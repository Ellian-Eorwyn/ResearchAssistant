"""Attached repository service for persistent source/citation expansion."""

from __future__ import annotations

import csv
import io
import json
import shutil
import tempfile
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from backend.models.bibliography import BibliographyArtifact, BibliographyEntry
from backend.models.export import ExportArtifact, ExportRow
from backend.models.repository import (
    RepositoryActionResponse,
    RepositoryHealth,
    RepositoryImportResponse,
    RepositoryScanSummary,
    RepositoryStatusResponse,
)
from backend.models.settings import AppSettings
from backend.models.sources import SourceManifestRow
from backend.pipeline.source_downloader import (
    SourceDownloadOrchestrator,
    build_manifest_csv,
    build_manifest_xlsx,
    dedupe_url_key,
    normalize_url,
)
from backend.pipeline.source_list_parser import parse_source_list_upload
from backend.pipeline.stage_bibliography import (
    build_entries_from_inline_urls,
    merge_inline_urls_into_entries,
    parse_bibliography,
)
from backend.pipeline.stage_export import write_csv
from backend.pipeline.stage_ingest import run_ingestion
from backend.pipeline.stage_references import detect_references_section
from backend.storage.file_store import FileStore

try:  # pragma: no cover - POSIX only
    import fcntl
except Exception:  # pragma: no cover - Windows fallback
    fcntl = None


SCHEMA_VERSION = 2
INTERNAL_DIR_NAME = ".ra_repo"
META_FILE_NAME = "repository.json"
STATE_FILE_NAME = "repository_state.json"
LOCK_FILE_NAME = "repository.lock"
MANIFEST_CSV_NAME = "manifest.csv"
MANIFEST_XLSX_NAME = "manifest.xlsx"
CITATIONS_CSV_NAME = "citations.csv"

TRACKING_PARAM_EXACT = {"gclid", "fbclid", "msclkid"}
TRACKING_PARAM_PREFIXES = ("utm_",)

FILE_FIELDS = [
    "raw_file",
    "rendered_file",
    "rendered_pdf_file",
    "markdown_file",
    "summary_file",
    "metadata_file",
]

SUPPORTED_DOCUMENT_IMPORT_EXTENSIONS = {".pdf", ".docx", ".md"}


@dataclass
class _MergedScan:
    rows: list[SourceManifestRow]
    citations: list[ExportRow]
    next_source_id: int
    duplicate_urls_removed: int


class AttachedRepositoryService:
    def __init__(self, store: FileStore):
        self.store = store
        self._path: Path | None = None
        self._mutex = threading.RLock()
        self._download_thread: threading.Thread | None = None
        self._download_state = "idle"
        self._download_message = ""
        self._last_scan: RepositoryScanSummary | None = None

    @property
    def is_attached(self) -> bool:
        return self._path is not None

    @property
    def path(self) -> Path:
        if self._path is None:
            raise ValueError("No repository attached")
        return self._path

    def attach(self, path_value: str) -> RepositoryStatusResponse:
        resolved = self._resolve_path(path_value)
        self._path = resolved
        self._ensure_internal_dirs()

        with self._writer_lock():
            meta = self._load_meta_locked()
            if not self._meta_path().exists():
                self._create_backup_snapshot_locked("first_attach")
                meta = self._default_meta()
            elif int(meta.get("schema_version") or 1) < SCHEMA_VERSION:
                self._create_backup_snapshot_locked(
                    f"schema_upgrade_v{int(meta.get('schema_version') or 1)}"
                )
                meta["schema_version"] = SCHEMA_VERSION

            merged, scan = self._scan_and_merge_locked()
            self._save_state_locked(
                sources=merged.rows,
                citations=merged.citations,
                imports=self._load_state_locked().get("imports", []),
            )
            self._save_meta_locked(
                {
                    **meta,
                    "schema_version": SCHEMA_VERSION,
                    "next_source_id": merged.next_source_id,
                    "last_scan_at": scan.scanned_at,
                    "updated_at": _utc_now_iso(),
                }
            )
            self._rebuild_outputs_locked(merged.rows, merged.citations)
            self._last_scan = scan
            self._download_state = "idle"
            self._download_message = "Repository attached"

        return self.get_status()

    def get_status(self) -> RepositoryStatusResponse:
        if not self.is_attached:
            return RepositoryStatusResponse(attached=False, message="No repository attached")

        with self._writer_lock():
            state = self._load_state_locked()
            meta = self._load_meta_locked()
            rows = _load_source_rows(state.get("sources", []))
            citations = _load_citation_rows(state.get("citations", []))
            health = self._compute_health(rows, citations)
            queued_count = sum(1 for row in rows if (row.fetch_status or "") in {"", "queued"})

            return RepositoryStatusResponse(
                attached=True,
                path=str(self.path),
                schema_version=int(meta.get("schema_version") or SCHEMA_VERSION),
                next_source_id=int(meta.get("next_source_id") or 1),
                total_sources=len(rows),
                total_citations=len(citations),
                queued_count=queued_count,
                download_state=self._download_state,
                message=self._download_message,
                last_scan_at=str(meta.get("last_scan_at") or ""),
                last_updated_at=str(meta.get("updated_at") or ""),
                health=health,
                scan=self._last_scan,
            )

    def import_source_list(self, filename: str, content: bytes) -> RepositoryImportResponse:
        if not self.is_attached:
            raise ValueError("Attach a repository before importing")
        parsed = parse_source_list_upload(filename=filename, content=content)
        return self._import_entries(
            entries=parsed.entries,
            import_type="spreadsheet",
            provenance_label=filename,
            default_source_document=filename,
        )

    def import_document(self, filename: str, content: bytes) -> RepositoryImportResponse:
        if not self.is_attached:
            raise ValueError("Attach a repository before importing")

        ext = Path(filename or "").suffix.lower()
        if ext not in SUPPORTED_DOCUMENT_IMPORT_EXTENSIONS:
            raise ValueError("Unsupported document type. Use .pdf, .docx, or .md.")

        with tempfile.TemporaryDirectory(prefix="repo-import-doc-") as tmp:
            tmp_path = Path(tmp)
            file_path = tmp_path / (filename or f"document{ext}")
            file_path.write_bytes(content)

            ingestion = run_ingestion(tmp_path)
            if not ingestion.documents:
                raise ValueError("No usable document content found")

            doc = ingestion.documents[0]
            section = detect_references_section(doc)
            sections = [section] if section else []
            artifact = parse_bibliography(sections)

            if doc.inline_citation_urls:
                if artifact.entries:
                    artifact.entries = merge_inline_urls_into_entries(
                        artifact.entries,
                        doc.inline_citation_urls,
                    )
                else:
                    artifact.entries = build_entries_from_inline_urls(doc.inline_citation_urls)

            entries = artifact.entries
            if not entries:
                raise ValueError(
                    "No reference URLs found in document references/inline citation links"
                )

            for entry in entries:
                if not entry.source_document_name:
                    entry.source_document_name = doc.filename

            return self._import_entries(
                entries=entries,
                import_type="document",
                provenance_label=filename,
                default_source_document=doc.filename,
            )

    def start_download(self, settings: AppSettings) -> RepositoryActionResponse:
        if not self.is_attached:
            raise ValueError("Attach a repository before downloading")

        with self._writer_lock():
            if self._download_thread and self._download_thread.is_alive():
                raise ValueError("Repository download is already running")

            state = self._load_state_locked()
            rows = _load_source_rows(state.get("sources", []))
            queued_ids = [
                row.id for row in rows if (row.fetch_status or "") in {"", "queued"}
            ]

            if not queued_ids:
                return RepositoryActionResponse(
                    status="noop",
                    message="No queued URLs to download",
                    queued_count=0,
                    total_sources=len(rows),
                    total_citations=len(state.get("citations", [])),
                )

            self._download_state = "running"
            self._download_message = f"Downloading {len(queued_ids)} queued URLs"
            self._download_thread = threading.Thread(
                target=self._download_worker,
                args=(queued_ids, settings),
                daemon=True,
            )
            self._download_thread.start()

            return RepositoryActionResponse(
                status="started",
                message=self._download_message,
                queued_count=len(queued_ids),
                total_sources=len(rows),
                total_citations=len(state.get("citations", [])),
            )

    def rebuild(self) -> RepositoryActionResponse:
        if not self.is_attached:
            raise ValueError("Attach a repository before rebuilding")

        with self._writer_lock():
            state = self._load_state_locked()
            rows = _load_source_rows(state.get("sources", []))
            citations = _load_citation_rows(state.get("citations", []))
            rows = self._sort_rows(rows)
            citations = self._sort_citations(citations)
            next_source_id = _next_source_id_from_rows(rows)

            self._save_state_locked(
                sources=rows,
                citations=citations,
                imports=state.get("imports", []),
            )
            self._save_meta_locked(
                {
                    **self._load_meta_locked(),
                    "schema_version": SCHEMA_VERSION,
                    "next_source_id": next_source_id,
                    "updated_at": _utc_now_iso(),
                }
            )
            self._rebuild_outputs_locked(rows, citations)
            health = self._compute_health(rows, citations)
            self._download_message = (
                f"Rebuilt manifest/citations ({health.missing_files} missing files detected)"
            )

            queued_count = sum(
                1 for row in rows if (row.fetch_status or "") in {"", "queued"}
            )
            return RepositoryActionResponse(
                status="completed",
                message=self._download_message,
                queued_count=queued_count,
                total_sources=len(rows),
                total_citations=len(citations),
            )

    def manifest_csv_path(self) -> Path:
        if not self.is_attached:
            raise ValueError("No repository attached")
        return self.path / MANIFEST_CSV_NAME

    def manifest_xlsx_path(self) -> Path:
        if not self.is_attached:
            raise ValueError("No repository attached")
        return self.path / MANIFEST_XLSX_NAME

    def citations_csv_path(self) -> Path:
        if not self.is_attached:
            raise ValueError("No repository attached")
        return self.path / CITATIONS_CSV_NAME

    def _download_worker(self, queued_ids: list[str], settings: AppSettings) -> None:
        try:
            with self._writer_lock():
                state = self._load_state_locked()
                rows = _load_source_rows(state.get("sources", []))
                by_id = {row.id: row for row in rows}
                queued_rows = [by_id[row_id] for row_id in queued_ids if row_id in by_id]

            if not queued_rows:
                with self._writer_lock():
                    self._download_state = "completed"
                    self._download_message = "No queued rows were available"
                return

            job_id = self.store.create_job()
            entries: list[BibliographyEntry] = []
            ref_counter = 1
            for row in queued_rows:
                ref_number = _parse_int(row.citation_number)
                if ref_number is None:
                    ref_number = ref_counter
                    ref_counter += 1
                entries.append(
                    BibliographyEntry(
                        ref_number=ref_number,
                        raw_text=row.original_url or row.final_url,
                        source_document_name=row.source_document_name,
                        url=row.original_url,
                        parse_confidence=1.0,
                        parse_warnings=[],
                        repair_method="repository_download",
                    )
                )

            bib = BibliographyArtifact(
                sections=[],
                entries=entries,
                total_raw_entries=len(entries),
                parse_failures=0,
            )
            self.store.save_artifact(job_id, "03_bibliography", bib.model_dump(mode="json"))

            orchestrator = SourceDownloadOrchestrator(
                job_id=job_id,
                store=self.store,
                rerun_failed_only=False,
                use_llm=settings.use_llm,
                llm_backend=settings.llm_backend,
                research_purpose=settings.research_purpose,
            )
            orchestrator.run()

            downloaded_raw = self.store.load_artifact(job_id, "06_sources_manifest") or {}
            downloaded_rows = _load_source_rows(downloaded_raw.get("rows", []))
            output_dir = self.store.get_sources_output_dir(job_id)

            download_map = {
                repository_dedupe_key(row.original_url or row.final_url): row
                for row in downloaded_rows
                if repository_dedupe_key(row.original_url or row.final_url)
            }

            with self._writer_lock():
                state = self._load_state_locked()
                rows = _load_source_rows(state.get("sources", []))
                row_by_id = {row.id: row for row in rows}

                for row_id in queued_ids:
                    row = row_by_id.get(row_id)
                    if not row:
                        continue
                    key = repository_dedupe_key(row.original_url or row.final_url)
                    downloaded = download_map.get(key)
                    if not downloaded:
                        row.fetch_status = "failed"
                        row.error_message = "download_failure: missing_result"
                        row.fetched_at = _utc_now_iso()
                        continue

                    self._apply_download_result(
                        target=row,
                        downloaded=downloaded,
                        output_dir=output_dir,
                    )

                merged_rows = self._sort_rows(list(row_by_id.values()))
                citations = self._sort_citations(_load_citation_rows(state.get("citations", [])))
                self._save_state_locked(
                    sources=merged_rows,
                    citations=citations,
                    imports=state.get("imports", []),
                )
                self._save_meta_locked(
                    {
                        **self._load_meta_locked(),
                        "next_source_id": _next_source_id_from_rows(merged_rows),
                        "updated_at": _utc_now_iso(),
                    }
                )
                self._rebuild_outputs_locked(merged_rows, citations)
                self._download_state = "completed"
                self._download_message = (
                    f"Completed repository download for {len(queued_ids)} queued URLs"
                )
        except Exception as exc:  # noqa: BLE001
            with self._writer_lock():
                self._download_state = "failed"
                self._download_message = f"Repository download failed: {type(exc).__name__}: {exc}"

    def _apply_download_result(
        self,
        target: SourceManifestRow,
        downloaded: SourceManifestRow,
        output_dir: Path,
    ) -> None:
        target.repository_source_id = target.id
        target.final_url = downloaded.final_url
        target.fetch_status = downloaded.fetch_status
        target.http_status = downloaded.http_status
        target.content_type = downloaded.content_type
        target.detected_type = downloaded.detected_type
        target.fetch_method = downloaded.fetch_method
        target.title = downloaded.title
        target.notes = downloaded.notes
        target.error_message = downloaded.error_message
        target.fetched_at = downloaded.fetched_at or _utc_now_iso()
        target.canonical_url = downloaded.canonical_url
        target.sha256 = downloaded.sha256
        target.extraction_method = downloaded.extraction_method
        target.markdown_char_count = downloaded.markdown_char_count

        source_dest_dir = self.path / "sources" / target.id
        source_dest_dir.mkdir(parents=True, exist_ok=True)

        for field in FILE_FIELDS:
            rel_value = getattr(downloaded, field)
            if not rel_value:
                setattr(target, field, "")
                continue
            src = output_dir / rel_value
            if not src.exists():
                setattr(target, field, "")
                continue
            dest = source_dest_dir / src.name
            shutil.copy2(src, dest)
            setattr(target, field, (Path("sources") / target.id / src.name).as_posix())

        metadata_rel = Path("sources") / target.id / f"{target.id}_metadata.json"
        metadata_abs = self.path / metadata_rel
        metadata_abs.write_text(
            json.dumps(target.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        target.metadata_file = metadata_rel.as_posix()

    def _import_entries(
        self,
        entries: list[BibliographyEntry],
        import_type: str,
        provenance_label: str,
        default_source_document: str,
    ) -> RepositoryImportResponse:
        import_id = uuid.uuid4().hex[:12]
        imported_at = _utc_now_iso()

        with self._writer_lock():
            state = self._load_state_locked()
            rows = _load_source_rows(state.get("sources", []))
            citations = _load_citation_rows(state.get("citations", []))
            imports = list(state.get("imports", []))
            meta = self._load_meta_locked()
            next_source_id = int(meta.get("next_source_id") or _next_source_id_from_rows(rows))

            by_key: dict[str, SourceManifestRow] = {}
            for row in rows:
                key = repository_dedupe_key(row.original_url or row.final_url)
                if key:
                    by_key[key] = row

            total_candidates = 0
            accepted_new = 0
            duplicates = 0

            for entry in entries:
                url = _entry_url(entry)
                if not url:
                    continue
                total_candidates += 1

                dedupe_key = repository_dedupe_key(url)
                if not dedupe_key:
                    dedupe_key = dedupe_url_key(url)

                existing = by_key.get(dedupe_key)
                if existing:
                    duplicates += 1
                    citations.append(
                        self._placeholder_citation_row(
                            entry=entry,
                            source_id=existing.id,
                            import_type=import_type,
                            imported_at=imported_at,
                            provenance_ref=f"{import_id}:{provenance_label}",
                            default_source_document=default_source_document,
                        )
                    )
                    continue

                source_id = f"{next_source_id:06d}"
                next_source_id += 1

                row = SourceManifestRow(
                    id=source_id,
                    repository_source_id=source_id,
                    import_type=import_type,
                    imported_at=imported_at,
                    provenance_ref=f"{import_id}:{provenance_label}",
                    source_document_name=entry.source_document_name or default_source_document,
                    citation_number=str(entry.ref_number or ""),
                    original_url=url,
                    fetch_status="queued",
                    notes="queued_for_download",
                )
                rows.append(row)
                by_key[dedupe_key] = row
                accepted_new += 1

                citations.append(
                    self._placeholder_citation_row(
                        entry=entry,
                        source_id=source_id,
                        import_type=import_type,
                        imported_at=imported_at,
                        provenance_ref=f"{import_id}:{provenance_label}",
                        default_source_document=default_source_document,
                    )
                )

            deduped_citations = self._dedupe_citations(citations)
            sorted_rows = self._sort_rows(rows)
            sorted_citations = self._sort_citations(deduped_citations)

            imports.append(
                {
                    "import_id": import_id,
                    "import_type": import_type,
                    "provenance": provenance_label,
                    "imported_at": imported_at,
                    "total_candidates": total_candidates,
                    "accepted_new": accepted_new,
                    "duplicates_skipped": duplicates,
                }
            )

            self._save_state_locked(
                sources=sorted_rows,
                citations=sorted_citations,
                imports=imports,
            )
            self._save_meta_locked(
                {
                    **meta,
                    "schema_version": SCHEMA_VERSION,
                    "next_source_id": next_source_id,
                    "updated_at": _utc_now_iso(),
                }
            )
            self._rebuild_outputs_locked(sorted_rows, sorted_citations)
            self._download_message = (
                f"Imported {accepted_new} new URLs ({duplicates} duplicates skipped)"
            )

            queued_count = sum(
                1 for row in sorted_rows if (row.fetch_status or "") in {"", "queued"}
            )
            return RepositoryImportResponse(
                import_id=import_id,
                import_type=import_type,
                total_candidates=total_candidates,
                accepted_new=accepted_new,
                duplicates_skipped=duplicates,
                total_sources=len(sorted_rows),
                queued_count=queued_count,
                message=self._download_message,
            )

    def _placeholder_citation_row(
        self,
        entry: BibliographyEntry,
        source_id: str,
        import_type: str,
        imported_at: str,
        provenance_ref: str,
        default_source_document: str,
    ) -> ExportRow:
        return ExportRow(
            repository_source_id=source_id,
            import_type=import_type,
            imported_at=imported_at,
            provenance_ref=provenance_ref,
            source_document=entry.source_document_name or default_source_document,
            citation_ref_numbers=str(entry.ref_number or ""),
            cited_authors="; ".join(entry.authors),
            cited_title=entry.title,
            cited_year=entry.year,
            cited_source=entry.journal_or_source,
            cited_doi=entry.doi,
            cited_url=_entry_url(entry),
            cited_raw_entry=entry.raw_text,
            match_method="repository_placeholder",
            warnings="placeholder_row",
        )

    def _dedupe_citations(self, rows: list[ExportRow]) -> list[ExportRow]:
        deduped: list[ExportRow] = []
        seen: set[tuple[str, ...]] = set()
        for row in rows:
            key = (
                row.repository_source_id,
                row.import_type,
                row.provenance_ref,
                row.citation_ref_numbers,
                row.cited_url,
                row.citation_raw,
                row.citing_sentence,
                row.cited_raw_entry,
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _scan_and_merge_locked(self) -> tuple[_MergedScan, RepositoryScanSummary]:
        state = self._load_state_locked()
        sources = _load_source_rows(state.get("sources", []))
        citations = _load_citation_rows(state.get("citations", []))

        manifests_scanned = 0
        artifacts_scanned = 0
        citations_scanned = 0

        manifest_paths = sorted(self._iter_paths_named(MANIFEST_CSV_NAME))
        for manifest_path in manifest_paths:
            manifests_scanned += 1
            provenance = _relative_or_absolute(self.path, manifest_path)
            sources.extend(self._read_manifest_csv(manifest_path, provenance))

        artifact_paths = sorted(self._iter_paths_named("06_sources_manifest.json"))
        for artifact_path in artifact_paths:
            artifacts_scanned += 1
            provenance = _relative_or_absolute(self.path, artifact_path)
            sources.extend(self._read_sources_artifact_json(artifact_path, provenance))

        citation_paths = sorted(self._iter_paths_named(CITATIONS_CSV_NAME))
        for citation_path in citation_paths:
            citations_scanned += 1
            provenance = _relative_or_absolute(self.path, citation_path)
            citations.extend(self._read_citations_csv(citation_path, provenance))

        merged = self._merge_source_rows(sources)
        merged_citations = self._merge_citation_rows(citations, merged.rows)
        merged = _MergedScan(
            rows=merged.rows,
            citations=merged_citations,
            next_source_id=merged.next_source_id,
            duplicate_urls_removed=merged.duplicate_urls_removed,
        )

        scan = RepositoryScanSummary(
            scanned_at=_utc_now_iso(),
            total_sources=len(merged.rows),
            total_citations=len(merged.citations),
            next_source_id=merged.next_source_id,
            manifests_scanned=manifests_scanned,
            artifacts_scanned=artifacts_scanned,
            citations_scanned=citations_scanned,
            duplicate_urls_removed=merged.duplicate_urls_removed,
        )
        return merged, scan

    def _merge_source_rows(self, rows: list[SourceManifestRow]) -> _MergedScan:
        best_by_key: dict[str, SourceManifestRow] = {}
        duplicate_urls_removed = 0

        for row in rows:
            if not row.id:
                row.id = row.repository_source_id or ""
            if not row.repository_source_id:
                row.repository_source_id = row.id
            if not row.import_type:
                row.import_type = "legacy_scan"
            if not row.imported_at:
                row.imported_at = row.fetched_at or _utc_now_iso()
            if not row.provenance_ref:
                row.provenance_ref = "legacy_scan"

            url = row.original_url or row.final_url
            key = repository_dedupe_key(url)
            if not key:
                key = dedupe_url_key(url)
            if not key:
                continue

            existing = best_by_key.get(key)
            if not existing:
                best_by_key[key] = row
                continue

            if _row_priority(row) > _row_priority(existing):
                best_by_key[key] = row
            duplicate_urls_removed += 1

        merged_rows = list(best_by_key.values())

        used_ids: set[int] = set()
        pending: list[SourceManifestRow] = []
        for row in merged_rows:
            parsed = _parse_numeric_id(row.id or row.repository_source_id)
            if parsed is None or parsed in used_ids:
                pending.append(row)
                continue
            used_ids.add(parsed)
            row.id = f"{parsed:06d}"
            row.repository_source_id = row.id

        next_id = (max(used_ids) + 1) if used_ids else 1
        for row in pending:
            while next_id in used_ids:
                next_id += 1
            used_ids.add(next_id)
            row.id = f"{next_id:06d}"
            row.repository_source_id = row.id
            next_id += 1

        merged_rows = self._sort_rows(merged_rows)
        return _MergedScan(
            rows=merged_rows,
            citations=[],
            next_source_id=_next_source_id_from_rows(merged_rows),
            duplicate_urls_removed=duplicate_urls_removed,
        )

    def _merge_citation_rows(
        self,
        rows: list[ExportRow],
        sources: list[SourceManifestRow],
    ) -> list[ExportRow]:
        source_by_key: dict[str, str] = {}
        source_ids = {source.id for source in sources}

        for source in sources:
            key = repository_dedupe_key(source.original_url or source.final_url)
            if key:
                source_by_key[key] = source.id

        normalized: list[ExportRow] = []
        for row in rows:
            if not row.repository_source_id:
                key = repository_dedupe_key(row.cited_url)
                if key and key in source_by_key:
                    row.repository_source_id = source_by_key[key]
            if not row.import_type:
                row.import_type = "legacy_scan"
            if not row.imported_at:
                row.imported_at = _utc_now_iso()
            if not row.provenance_ref:
                row.provenance_ref = "legacy_scan"
            if row.repository_source_id and row.repository_source_id not in source_ids:
                key = repository_dedupe_key(row.cited_url)
                if key and key in source_by_key:
                    row.repository_source_id = source_by_key[key]
            normalized.append(row)

        return self._sort_citations(self._dedupe_citations(normalized))

    def _rebuild_outputs_locked(
        self,
        sources: list[SourceManifestRow],
        citations: list[ExportRow],
    ) -> None:
        self.manifest_csv_path().write_text(
            build_manifest_csv(sources),
            encoding="utf-8-sig",
        )
        self.manifest_xlsx_path().write_bytes(build_manifest_xlsx(sources))

        citation_artifact = ExportArtifact(
            rows=citations,
            total_citations_found=len(citations),
            total_bib_entries=len(citations),
            matched_count=0,
            unmatched_count=0,
        )
        self.citations_csv_path().write_text(
            write_csv(citation_artifact),
            encoding="utf-8-sig",
        )

    def _compute_health(
        self,
        rows: list[SourceManifestRow],
        citations: list[ExportRow],
    ) -> RepositoryHealth:
        missing_files = 0
        source_ids = {row.id for row in rows}
        orphaned_citations = 0

        for row in rows:
            for field in FILE_FIELDS:
                rel = getattr(row, field)
                if not rel:
                    continue
                if not (self.path / rel).exists():
                    missing_files += 1

        for citation in citations:
            if citation.repository_source_id and citation.repository_source_id not in source_ids:
                orphaned_citations += 1

        return RepositoryHealth(
            missing_files=missing_files,
            orphaned_citation_rows=orphaned_citations,
        )

    def _read_manifest_csv(self, path: Path, provenance_ref: str) -> list[SourceManifestRow]:
        try:
            text = path.read_text(encoding="utf-8-sig")
        except Exception:
            text = path.read_text(encoding="utf-8", errors="replace")

        reader = csv.DictReader(io.StringIO(text))
        rows: list[SourceManifestRow] = []
        for raw in reader:
            if not raw:
                continue
            payload = dict(raw)
            payload.setdefault("id", str(payload.get("repository_source_id") or ""))
            payload.setdefault(
                "repository_source_id",
                str(payload.get("id") or payload.get("repository_source_id") or ""),
            )
            payload.setdefault("import_type", "legacy_scan")
            payload.setdefault("imported_at", str(payload.get("fetched_at") or _utc_now_iso()))
            payload.setdefault("provenance_ref", provenance_ref)
            row = _safe_manifest_row(payload)
            if row:
                rows.append(row)
        return rows

    def _read_sources_artifact_json(
        self,
        path: Path,
        provenance_ref: str,
    ) -> list[SourceManifestRow]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []

        rows: list[SourceManifestRow] = []
        for raw_row in payload.get("rows", []):
            if not isinstance(raw_row, dict):
                continue
            candidate = dict(raw_row)
            candidate.setdefault("repository_source_id", str(candidate.get("id") or ""))
            candidate.setdefault("import_type", "legacy_scan")
            candidate.setdefault("imported_at", str(candidate.get("fetched_at") or _utc_now_iso()))
            candidate.setdefault("provenance_ref", provenance_ref)
            row = _safe_manifest_row(candidate)
            if row:
                rows.append(row)
        return rows

    def _read_citations_csv(self, path: Path, provenance_ref: str) -> list[ExportRow]:
        try:
            text = path.read_text(encoding="utf-8-sig")
        except Exception:
            text = path.read_text(encoding="utf-8", errors="replace")

        reader = csv.DictReader(io.StringIO(text))
        rows: list[ExportRow] = []
        for raw in reader:
            if not raw:
                continue
            payload = dict(raw)
            payload.setdefault("import_type", "legacy_scan")
            payload.setdefault("imported_at", _utc_now_iso())
            payload.setdefault("provenance_ref", provenance_ref)
            row = _safe_export_row(payload)
            if row:
                rows.append(row)
        return rows

    def _iter_paths_named(self, filename: str):
        for path in self.path.rglob(filename):
            if self._is_internal_path(path):
                continue
            if path.is_file():
                yield path

    def _is_internal_path(self, path: Path) -> bool:
        try:
            return path.resolve().is_relative_to(self._internal_dir().resolve())
        except Exception:
            return False

    def _resolve_path(self, value: str) -> Path:
        candidate = Path((value or "").strip()).expanduser()
        if not str(candidate):
            raise ValueError("Repository path is required")
        if not candidate.is_absolute():
            raise ValueError("Repository path must be absolute")
        if candidate.exists() and not candidate.is_dir():
            raise ValueError("Repository path must point to a directory")
        candidate.mkdir(parents=True, exist_ok=True)
        test_file = candidate / ".ra_repo_write_test"
        try:
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
        except Exception as exc:
            raise ValueError(f"Repository path is not writable: {exc}") from exc
        return candidate

    def _ensure_internal_dirs(self) -> None:
        self._internal_dir().mkdir(parents=True, exist_ok=True)
        self._backups_dir().mkdir(parents=True, exist_ok=True)
        self._lock_path().touch(exist_ok=True)

    def _default_meta(self) -> dict[str, Any]:
        now = _utc_now_iso()
        return {
            "schema_version": SCHEMA_VERSION,
            "created_at": now,
            "updated_at": now,
            "last_scan_at": "",
            "next_source_id": 1,
        }

    def _load_meta_locked(self) -> dict[str, Any]:
        path = self._meta_path()
        if not path.exists():
            return self._default_meta()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return self._default_meta()
            merged = self._default_meta()
            merged.update(data)
            return merged
        except Exception:
            return self._default_meta()

    def _save_meta_locked(self, data: dict[str, Any]) -> None:
        payload = self._default_meta()
        payload.update(data)
        self._meta_path().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_state_locked(self) -> dict[str, Any]:
        path = self._state_path()
        if not path.exists():
            return {"sources": [], "citations": [], "imports": []}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {"sources": [], "citations": [], "imports": []}
            return {
                "sources": data.get("sources", []),
                "citations": data.get("citations", []),
                "imports": data.get("imports", []),
            }
        except Exception:
            return {"sources": [], "citations": [], "imports": []}

    def _save_state_locked(
        self,
        sources: list[SourceManifestRow],
        citations: list[ExportRow],
        imports: list[dict[str, Any]],
    ) -> None:
        payload = {
            "sources": [row.model_dump(mode="json") for row in sources],
            "citations": [row.model_dump(mode="json") for row in citations],
            "imports": imports,
        }
        self._state_path().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _sort_rows(self, rows: list[SourceManifestRow]) -> list[SourceManifestRow]:
        return sorted(
            rows,
            key=lambda row: (
                _parse_numeric_id(row.id) if _parse_numeric_id(row.id) is not None else 10**9,
                row.id,
                row.original_url,
            ),
        )

    def _sort_citations(self, rows: list[ExportRow]) -> list[ExportRow]:
        return sorted(
            rows,
            key=lambda row: (
                _parse_numeric_id(row.repository_source_id)
                if _parse_numeric_id(row.repository_source_id) is not None
                else 10**9,
                row.repository_source_id,
                row.imported_at,
                row.cited_url,
            ),
        )

    def _create_backup_snapshot_locked(self, reason: str) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_dir = self._backups_dir() / f"{timestamp}_{reason}"
        backup_dir.mkdir(parents=True, exist_ok=True)
        candidates = [
            self.path / MANIFEST_CSV_NAME,
            self.path / MANIFEST_XLSX_NAME,
            self.path / CITATIONS_CSV_NAME,
            self._meta_path(),
            self._state_path(),
        ]
        for src in candidates:
            if src.exists():
                shutil.copy2(src, backup_dir / src.name)

    def _internal_dir(self) -> Path:
        return self.path / INTERNAL_DIR_NAME

    def _meta_path(self) -> Path:
        return self._internal_dir() / META_FILE_NAME

    def _state_path(self) -> Path:
        return self._internal_dir() / STATE_FILE_NAME

    def _lock_path(self) -> Path:
        return self._internal_dir() / LOCK_FILE_NAME

    def _backups_dir(self) -> Path:
        return self._internal_dir() / "backups"

    @contextmanager
    def _writer_lock(self):
        with self._mutex:
            lock_file = self._lock_path() if self.is_attached else None
            handle = None
            try:
                if lock_file is not None:
                    lock_file.parent.mkdir(parents=True, exist_ok=True)
                    handle = lock_file.open("a+")
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                yield
            finally:
                if handle is not None:
                    if fcntl is not None:
                        try:
                            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                        except Exception:
                            pass
                    handle.close()


def repository_dedupe_key(url: str) -> str:
    normalized, err = normalize_url(url)
    candidate = normalized or (url or "").strip()
    if not candidate:
        return ""
    if err and "://" not in candidate:
        candidate = f"https://{candidate}"

    try:
        parsed = urlsplit(candidate)
    except Exception:
        return candidate.lower()

    if not parsed.netloc:
        return candidate.lower()

    filtered_params: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower in TRACKING_PARAM_EXACT:
            continue
        if any(key_lower.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            continue
        filtered_params.append((key, value))

    filtered_params.sort(key=lambda item: (item[0].lower(), item[1]))
    query = urlencode(filtered_params, doseq=True)

    return urlunsplit(
        (
            parsed.scheme.lower() or "https",
            parsed.netloc.lower(),
            parsed.path or "/",
            query,
            "",  # strip fragments
        )
    )


def _entry_url(entry: BibliographyEntry) -> str:
    url = (entry.url or "").strip()
    if url:
        normalized, _ = normalize_url(url)
        return normalized or url
    doi = (entry.doi or "").strip()
    if not doi:
        return ""
    normalized, _ = normalize_url(f"https://doi.org/{doi}")
    return normalized or f"https://doi.org/{doi}"


def _row_priority(row: SourceManifestRow) -> tuple[int, int, int, int]:
    status_rank = {
        "success": 3,
        "partial": 2,
        "queued": 1,
        "": 1,
        "failed": 0,
    }.get((row.fetch_status or "").strip().lower(), 0)
    return (
        status_rank,
        1 if row.markdown_file else 0,
        1 if row.raw_file else 0,
        1 if row.summary_file else 0,
    )


def _load_source_rows(payload: list[Any]) -> list[SourceManifestRow]:
    rows: list[SourceManifestRow] = []
    for item in payload:
        if isinstance(item, SourceManifestRow):
            rows.append(item)
            continue
        if isinstance(item, dict):
            row = _safe_manifest_row(item)
            if row:
                rows.append(row)
    return rows


def _load_citation_rows(payload: list[Any]) -> list[ExportRow]:
    rows: list[ExportRow] = []
    for item in payload:
        if isinstance(item, ExportRow):
            rows.append(item)
            continue
        if isinstance(item, dict):
            row = _safe_export_row(item)
            if row:
                rows.append(row)
    return rows


def _safe_manifest_row(payload: dict[str, Any]) -> SourceManifestRow | None:
    try:
        if payload.get("http_status") in {"", None}:
            payload["http_status"] = None
        else:
            payload["http_status"] = _parse_int(str(payload.get("http_status")))
        payload["markdown_char_count"] = _parse_int(str(payload.get("markdown_char_count") or "0")) or 0
        return SourceManifestRow.model_validate(payload)
    except Exception:
        return None


def _safe_export_row(payload: dict[str, Any]) -> ExportRow | None:
    try:
        confidence_val = payload.get("match_confidence")
        if confidence_val in {"", None}:
            payload["match_confidence"] = 0.0
        else:
            payload["match_confidence"] = float(confidence_val)
        return ExportRow.model_validate(payload)
    except Exception:
        return None


def _parse_numeric_id(value: str | None) -> int | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    return None


def _parse_int(value: str) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _next_source_id_from_rows(rows: list[SourceManifestRow]) -> int:
    numeric_ids = [_parse_numeric_id(row.id) for row in rows]
    valid = [item for item in numeric_ids if item is not None]
    if not valid:
        return 1
    return max(valid) + 1


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _relative_or_absolute(root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(root.resolve()))
    except Exception:
        return str(path)
