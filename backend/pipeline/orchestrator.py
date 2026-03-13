"""Pipeline orchestrator: runs all stages, saves artifacts, updates job status."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from backend.models.bibliography import BibliographyArtifact, ReferencesSection
from backend.models.citations import CitationArtifact
from backend.models.common import PipelineStage, ProcessingConfig
from backend.pipeline.stage_bibliography import (
    build_entries_from_inline_urls,
    merge_inline_urls_into_entries,
    parse_bibliography,
)
from backend.pipeline.stage_citations import detect_citations
from backend.pipeline.stage_export import build_export, write_csv
from backend.pipeline.stage_ingest import run_ingestion
from backend.pipeline.stage_matching import match_citations
from backend.pipeline.stage_references import detect_references_section
from backend.pipeline.stage_sentences import extract_citing_sentences
from backend.storage.file_store import FileStore

logger = logging.getLogger(__name__)

STAGE_ORDER = [
    PipelineStage.INGESTING,
    PipelineStage.DETECTING_REFERENCES,
    PipelineStage.PARSING_BIBLIOGRAPHY,
    PipelineStage.DETECTING_CITATIONS,
    PipelineStage.EXTRACTING_SENTENCES,
    PipelineStage.MATCHING_CITATIONS,
    PipelineStage.EXPORTING,
]


class PipelineOrchestrator:
    def __init__(self, job_id: str, store: FileStore, config: ProcessingConfig):
        self.job_id = job_id
        self.store = store
        self.config = config

    def run(self) -> None:
        """Execute all pipeline stages sequentially."""
        try:
            # Stage 1: Ingestion
            self._update_stage(PipelineStage.INGESTING, "running")
            upload_dir = self.store.get_upload_dir(self.job_id)
            ingestion = run_ingestion(upload_dir)
            self.store.save_artifact(
                self.job_id, "01_ingestion", ingestion.model_dump()
            )
            self._update_stage(
                PipelineStage.INGESTING,
                "completed",
                item_count=len(ingestion.documents),
                warnings=[w for d in ingestion.documents for w in d.warnings],
            )

            if not ingestion.documents:
                self._fail("No documents found to process")
                return

            documents = ingestion.documents

            # Stage 2: References detection
            self._update_stage(PipelineStage.DETECTING_REFERENCES, "running")
            refs_sections_by_doc: list[ReferencesSection | None] = []
            sections = []
            refs_warnings: list[str] = []
            for doc in documents:
                refs_section = detect_references_section(doc)
                refs_sections_by_doc.append(refs_section)
                if refs_section is None:
                    refs_warnings.append(
                        f"{doc.filename}: No references section detected. Citation matching will be limited."
                    )
                    continue
                sections.append(refs_section)

            refs_artifact = {"sections": [s.model_dump() for s in sections]}
            self.store.save_artifact(self.job_id, "02_references", refs_artifact)
            self._update_stage(
                PipelineStage.DETECTING_REFERENCES,
                "completed",
                item_count=len(sections),
                warnings=refs_warnings,
            )

            # Stage 3: Bibliography parsing
            self._update_stage(PipelineStage.PARSING_BIBLIOGRAPHY, "running")
            all_bib_sections = []
            all_bib_entries = []
            doc_bib_entries = []
            total_raw_entries = 0
            total_parse_failures = 0

            for doc, refs_section in zip(documents, refs_sections_by_doc):
                doc_sections = [refs_section] if refs_section else []
                doc_bib = parse_bibliography(doc_sections)

                # Enrich/create entries from inline citation URLs (Markdown docs)
                if doc.inline_citation_urls:
                    if doc_bib.entries:
                        doc_bib.entries = merge_inline_urls_into_entries(
                            doc_bib.entries, doc.inline_citation_urls
                        )
                    else:
                        doc_bib.entries = build_entries_from_inline_urls(
                            doc.inline_citation_urls
                        )
                    doc_bib.total_raw_entries = len(doc_bib.entries)

                for entry in doc_bib.entries:
                    if not entry.source_document_name:
                        entry.source_document_name = doc.filename

                all_bib_sections.extend(doc_bib.sections)
                all_bib_entries.extend(doc_bib.entries)
                doc_bib_entries.append(doc_bib.entries)
                total_raw_entries += doc_bib.total_raw_entries
                total_parse_failures += doc_bib.parse_failures

            bib_artifact = BibliographyArtifact(
                sections=all_bib_sections,
                entries=all_bib_entries,
                total_raw_entries=total_raw_entries,
                parse_failures=total_parse_failures,
            )

            self.store.save_artifact(
                self.job_id, "03_bibliography", bib_artifact.model_dump()
            )
            bib_warnings = []
            if bib_artifact.parse_failures > 0:
                bib_warnings.append(
                    f"{bib_artifact.parse_failures} entries had low parse confidence"
                )
            self._update_stage(
                PipelineStage.PARSING_BIBLIOGRAPHY,
                "completed",
                item_count=len(bib_artifact.entries),
                warnings=bib_warnings,
            )

            # Stage 4: Citation detection
            self._update_stage(PipelineStage.DETECTING_CITATIONS, "running")
            citations_by_doc = []
            all_citations = []
            for doc, refs_section in zip(documents, refs_sections_by_doc):
                refs_start_offset = None
                if refs_section and refs_section.start_block_index < len(doc.blocks):
                    refs_start_offset = doc.blocks[
                        refs_section.start_block_index
                    ].char_offset_start
                doc_citations = detect_citations(doc, refs_start_offset)
                citations_by_doc.append(doc_citations)
                all_citations.extend(doc_citations)
            self._update_stage(
                PipelineStage.DETECTING_CITATIONS,
                "completed",
                item_count=len(all_citations),
            )

            # Stage 5: Sentence extraction
            self._update_stage(PipelineStage.EXTRACTING_SENTENCES, "running")
            sentences_by_doc = []
            all_sentences = []
            for doc, doc_citations in zip(documents, citations_by_doc):
                doc_sentences = extract_citing_sentences(doc, doc_citations)
                sentences_by_doc.append(doc_sentences)
                all_sentences.extend(doc_sentences)
            self._update_stage(
                PipelineStage.EXTRACTING_SENTENCES,
                "completed",
                item_count=len(all_sentences),
            )

            # Stage 6: Citation matching
            self._update_stage(PipelineStage.MATCHING_CITATIONS, "running")
            matches = []
            unmatched_cit = 0
            unmatched_bib = 0
            bib_entry_offset = 0
            for doc_citations, doc_sentences, doc_entries in zip(
                citations_by_doc, sentences_by_doc, doc_bib_entries
            ):
                doc_matches, doc_unmatched_cit, doc_unmatched_bib = match_citations(
                    doc_citations, doc_sentences, doc_entries
                )
                for match in doc_matches:
                    if match.matched_bib_entry_index is not None:
                        match.matched_bib_entry_index += bib_entry_offset
                matches.extend(doc_matches)
                unmatched_cit += doc_unmatched_cit
                unmatched_bib += doc_unmatched_bib
                bib_entry_offset += len(doc_entries)
            match_warnings = []
            if unmatched_cit > 0:
                match_warnings.append(
                    f"{unmatched_cit} citations could not be matched to bibliography entries"
                )
            if unmatched_bib > 0:
                match_warnings.append(
                    f"{unmatched_bib} bibliography entries were never cited in the text"
                )

            citation_artifact = CitationArtifact(
                citations=all_citations,
                sentences=all_sentences,
                matches=matches,
                unmatched_citations=unmatched_cit,
                unmatched_bib_entries=unmatched_bib,
            )
            self.store.save_artifact(
                self.job_id, "04_citations", citation_artifact.model_dump()
            )
            self._update_stage(
                PipelineStage.MATCHING_CITATIONS,
                "completed",
                item_count=len(matches),
                warnings=match_warnings,
            )

            # Stage 7: Export
            self._update_stage(PipelineStage.EXPORTING, "running")
            export_artifact = build_export(
                matches,
                all_citations,
                all_sentences,
                bib_artifact.entries,
                research_purpose=self.config.research_purpose,
            )
            self.store.save_artifact(
                self.job_id, "05_export", export_artifact.model_dump()
            )
            csv_content = write_csv(export_artifact)
            self.store.save_export(self.job_id, csv_content)
            self._update_stage(
                PipelineStage.EXPORTING,
                "completed",
                item_count=len(export_artifact.rows),
            )

            # Mark overall completion
            self._set_current_stage(PipelineStage.COMPLETED)

        except Exception as e:
            logger.exception(f"Pipeline failed for job {self.job_id}")
            self._fail(f"{type(e).__name__}: {e}")

    def _update_stage(
        self,
        stage: PipelineStage,
        status_val: str,
        item_count: int = 0,
        warnings: list[str] | None = None,
        errors: list[str] | None = None,
    ) -> None:
        """Update the status of a specific stage."""
        job_status = self.store.get_job_status(self.job_id) or {}
        stages = job_status.get("stages", [])
        now = datetime.now(timezone.utc).isoformat()

        for s in stages:
            if s["stage"] == stage.value:
                s["status"] = status_val
                if status_val == "running":
                    s["started_at"] = now
                if status_val in ("completed", "failed"):
                    s["completed_at"] = now
                s["item_count"] = item_count
                if warnings:
                    s["warnings"] = warnings
                if errors:
                    s["errors"] = errors
                break

        if status_val == "running":
            job_status["current_stage"] = stage.value

        # Calculate progress
        completed_stages = sum(1 for s in stages if s.get("status") == "completed")
        job_status["progress_pct"] = round(
            (completed_stages / len(STAGE_ORDER)) * 100, 1
        )
        job_status["stages"] = stages
        self.store.save_job_status(self.job_id, job_status)

    def _set_current_stage(self, stage: PipelineStage) -> None:
        job_status = self.store.get_job_status(self.job_id) or {}
        job_status["current_stage"] = stage.value
        job_status["progress_pct"] = 100.0
        job_status["completed_at"] = datetime.now(timezone.utc).isoformat()
        self.store.save_job_status(self.job_id, job_status)

    def _fail(self, message: str) -> None:
        job_status = self.store.get_job_status(self.job_id) or {}
        job_status["current_stage"] = PipelineStage.FAILED.value
        job_status["completed_at"] = datetime.now(timezone.utc).isoformat()

        # Add error to the current running stage
        for s in job_status.get("stages", []):
            if s.get("status") == "running":
                s["status"] = "failed"
                s["errors"] = s.get("errors", []) + [message]
                s["completed_at"] = datetime.now(timezone.utc).isoformat()
                break

        self.store.save_job_status(self.job_id, job_status)
