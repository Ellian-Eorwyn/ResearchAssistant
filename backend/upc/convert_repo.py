"""Convert an attached ResearchAssistant repository into a UPC corpus.

Read-only with respect to the input, by construction:

* the output directory may not live inside the input;
* input files are opened ``"rb"`` and no writable path is ever built under it;
* RA's ``flock`` is deliberately **not** taken -- acquiring
  ``.ra_repo/repository.lock`` is itself a write and would race a running app.
  Instead the state file's bytes are fingerprinted at the start and re-checked at
  the end, so a mid-run edit is reported rather than silently half-converted;
* ``.ra_repo/agent_tokens.json`` is never read. It holds plaintext tokens and must
  not reach a corpus that is meant to be handed to someone.

Run ``--plan`` first, every time: it writes nothing and prints the anomalies that
decide whether the conversion will be clean.

    python -m backend.upc.convert_repo --in <ra-repo> --out <dir> --plan
    python -m backend.upc.convert_repo --in <ra-repo> --out <dir>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from backend.upc import projector as P
from backend.upc import writer as W
from backend.upc.node_bridge import NodeBridge, UpcUnavailable

RA_INTERNAL = ".ra_repo"
STATE_FILE = "repository_state.json"
META_FILE = "repository.json"
NEVER_READ = {"agent_tokens.json"}

# RA schema versions this converter has been checked against.
SUPPORTED_RA_SCHEMA = {5}

HASH_CHUNK = 1 << 20


# --------------------------------------------------------------------------
# Input model
# --------------------------------------------------------------------------


@dataclass
class RaRepo:
    root: Path
    meta: dict[str, Any]
    sources: list[dict[str, Any]]
    citations: list[dict[str, Any]]
    column_configs: list[dict[str, Any]]
    state_fingerprint: str

    @property
    def schema_version(self) -> int:
        try:
            return int(self.meta.get("schema_version") or 0)
        except (TypeError, ValueError):
            return 0


def load_ra_repo(root: Path) -> RaRepo:
    internal = root / RA_INTERNAL
    state_path = internal / STATE_FILE
    if not state_path.is_file():
        raise SystemExit(f"not an attached RA repository (no {RA_INTERNAL}/{STATE_FILE}): {root}")
    raw = state_path.read_bytes()
    state = json.loads(raw)
    meta_path = internal / META_FILE
    meta = json.loads(meta_path.read_bytes()) if meta_path.is_file() else {}
    return RaRepo(
        root=root,
        meta=meta,
        sources=list(state.get("sources") or []),
        citations=list(state.get("citations") or []),
        column_configs=list(state.get("column_configs") or []),
        state_fingerprint=hashlib.sha256(raw).hexdigest(),
    )


# --------------------------------------------------------------------------
# File inventory + hashing
# --------------------------------------------------------------------------


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class Inventory:
    """Every file the conversion might touch, hashed exactly once."""

    hash_of: dict[str, str] = field(default_factory=dict)      # rel path -> sha256
    size_of: dict[str, int] = field(default_factory=dict)
    missing: list[str] = field(default_factory=list)
    by_hash: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))

    def total_bytes(self) -> int:
        return sum(self.size_of.values())

    def duplicate_groups(self) -> list[list[str]]:
        return [paths for paths in self.by_hash.values() if len(paths) > 1]


def build_inventory(repo: RaRepo, rel_paths: Iterable[str], *, workers: int = 8,
                    cache: dict[str, dict[str, Any]] | None = None) -> Inventory:
    inv = Inventory()
    cache = cache or {}
    todo: list[tuple[str, Path]] = []
    for rel in sorted(set(rel_paths)):
        abs_p = repo.root / rel
        if not abs_p.is_file():
            inv.missing.append(rel)
            continue
        st = abs_p.stat()
        inv.size_of[rel] = st.st_size
        hit = cache.get(rel)
        if hit and hit.get("size") == st.st_size and hit.get("mtime_ns") == st.st_mtime_ns:
            inv.hash_of[rel] = hit["sha256"]
            continue
        todo.append((rel, abs_p))

    if todo:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for rel, digest in zip(
                (r for r, _ in todo), pool.map(lambda t: sha256_file(t[1]), todo)
            ):
                inv.hash_of[rel] = digest
                st = (repo.root / rel).stat()
                cache[rel] = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "sha256": digest}

    for rel, h in inv.hash_of.items():
        inv.by_hash[h].append(rel)
    for paths in inv.by_hash.values():
        paths.sort()
    return inv


def collect_paths(repo: RaRepo) -> tuple[list[str], dict[str, list[str]]]:
    """Declared representation paths, plus the image files each source indexes."""
    paths: list[str] = []
    images_by_source: dict[str, list[str]] = {}
    for row in repo.sources:
        for plan in P.plan_representations(row):
            paths.append(plan.rel_path)
        idx_rel = (row.get("image_index_file") or "").strip()
        if not idx_rel:
            continue
        idx_abs = repo.root / idx_rel
        if not idx_abs.is_file():
            continue
        try:
            idx = json.loads(idx_abs.read_bytes())
        except json.JSONDecodeError:
            continue
        img_dir = (row.get("images_dir") or os.path.dirname(idx_rel)).strip()
        rels: list[str] = []
        for img in idx.get("images") or []:
            fname = (img.get("file") or "").strip()
            if fname:
                rels.append(f"{img_dir}/{fname}")
        images_by_source[row.get("id", "")] = rels
        paths.extend(rels)
    return paths, images_by_source


# --------------------------------------------------------------------------
# P0 -- the plan
# --------------------------------------------------------------------------


@dataclass
class Anomaly:
    kind: str
    detail: str
    count: int = 1


def plan(repo: RaRepo, bridge: NodeBridge | None = None) -> dict[str, Any]:
    """Inspect the repository and report everything that will shape the conversion.

    Writes nothing. Everything reported here is a fact about the input, not a
    prediction about the output.
    """
    anomalies: list[Anomaly] = []
    if repo.schema_version not in SUPPORTED_RA_SCHEMA:
        anomalies.append(
            Anomaly(
                "ra_schema_unverified",
                f"repository schema_version={repo.schema_version}; this converter is "
                f"checked against {sorted(SUPPORTED_RA_SCHEMA)}",
            )
        )

    paths, images_by_source = collect_paths(repo)
    inv = build_inventory(repo, paths)

    # -- sources ---------------------------------------------------------
    kinds = Counter((r.get("source_kind") or "").strip() or "(blank)" for r in repo.sources)
    no_sha = [r["id"] for r in repo.sources if not (r.get("sha256") or "").strip()]
    if no_sha:
        anomalies.append(
            Anomaly("source_without_sha256", f"rows {', '.join(no_sha)}", len(no_sha))
        )

    # A source with no readable representation cannot satisfy rule 0.4, so the
    # metadata sidecar has to be emitted for it. Check that it exists.
    repless: list[str] = []
    for row in repo.sources:
        plans = P.plan_representations(row)
        if not any(p.rel_path in inv.hash_of for p in plans):
            repless.append(row.get("id", "?"))
    if repless:
        anomalies.append(
            Anomaly(
                "source_without_representation",
                f"rows {', '.join(repless)} have no readable declared file; the metadata "
                "sidecar must be emitted so rule 0.4 is satisfied",
                len(repless),
            )
        )

    # -- byte-identical files -------------------------------------------
    dup_groups = inv.duplicate_groups()
    same_source = 0
    cross_source = 0
    raw_eq_rendered = 0
    for group in dup_groups:
        owners = {p.split("/")[1] for p in group if p.startswith("sources/") and len(p.split("/")) > 1}
        if len(owners) > 1:
            cross_source += 1
        else:
            same_source += 1
        fields = {os.path.basename(p) for p in group}
        if any("_source." in f for f in fields) and any("_rendered." in f for f in fields):
            raw_eq_rendered += 1
    extra_records = sum(len(g) - 1 for g in dup_groups)
    if extra_records:
        anomalies.append(
            Anomaly(
                "shared_representation",
                f"{extra_records} extra records across {len(dup_groups)} byte-identical groups "
                f"({same_source} same-source, {cross_source} cross-source, {raw_eq_rendered} raw==rendered). "
                "These become one representation each; UPC >= 1.6.0 reports representation_shared "
                "(advisory). Under 1.5.0 every one of them was an id_duplicate error.",
                extra_records,
            )
        )

    # -- images ----------------------------------------------------------
    img_paths = [p for rels in images_by_source.values() for p in rels]
    img_hashes = {inv.hash_of[p] for p in img_paths if p in inv.hash_of}
    if img_paths:
        anomalies.append(
            Anomaly(
                "image_dedupe",
                f"{len(img_paths)} image records collapse to {len(img_hashes)} distinct img- ids",
                len(img_paths) - len(img_hashes),
            )
        )

    # -- columns ---------------------------------------------------------
    triage = P.triage_columns(repo.column_configs)
    values = Counter()
    for row in repo.sources:
        for cid, v in (row.get("custom_fields") or {}).items():
            if (v or "").strip():
                values[cid] += 1

    # -- text anchoring outlook ------------------------------------------
    md_paths = [
        (row.get("markdown_file") or "").strip()
        for row in repo.sources
        if (row.get("markdown_file") or "").strip()
    ]
    long_lines: list[tuple[str, int]] = []
    total_cp = 0
    for rel in md_paths:
        abs_p = repo.root / rel
        if not abs_p.is_file():
            continue
        try:
            text = abs_p.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            anomalies.append(Anomaly("markdown_not_utf8", rel))
            continue
        total_cp += len(text)
        longest = max((len(l) for l in text.split("\n")), default=0)
        if longest > 10_000:
            long_lines.append((rel, longest))
    if long_lines:
        worst = max(long_lines, key=lambda t: t[1])
        anomalies.append(
            Anomaly(
                "very_long_line",
                f"{len(long_lines)} markdown file(s) have a line over 10k codepoints "
                f"(worst: {worst[0]} at {worst[1]:,}); line-number navigation is useless there",
                len(long_lines),
            )
        )

    # -- url identity risk ------------------------------------------------
    ad_param_urls: list[str] = []
    if bridge:
        urls = [(r.get("original_url") or "").strip() for r in repo.sources]
        canon = bridge.canonical_urls([u for u in urls if u])
        for u, c in zip([u for u in urls if u], canon):
            if c and any(p in c for p in ("gad_source=", "gad_campaignid=", "gbraid=", "srsltid=")):
                ad_param_urls.append(c)
    if ad_param_urls:
        anomalies.append(
            Anomaly(
                "url_identity_depends_on_ad_params",
                f"{len(ad_param_urls)} source id(s) hash a URL that still carries ad-tracking "
                "parameters UPC's closed strip-list does not remove; a cleaned bibliographic.url "
                "is the escape hatch (spec/06)",
                len(ad_param_urls),
            )
        )

    return {
        "input": str(repo.root),
        "ra_schema_version": repo.schema_version,
        "sources": {
            "total": len(repo.sources),
            "by_kind": dict(kinds),
        },
        "files": {
            "declared": len(paths),
            "found": len(inv.hash_of),
            "missing": len(inv.missing),
            "distinct_hashes": len(inv.by_hash),
            "bytes": inv.total_bytes(),
        },
        "columns": {
            "total": len(repo.column_configs),
            "triage": triage.counts(),
            "detail": [
                {
                    "id": c.get("id"),
                    "label": c.get("label"),
                    "kind": P.triage_column(c),
                    "allowed_values": len((c.get("output_constraint") or {}).get("allowed_values") or []),
                    "values_present": values.get(c.get("id"), 0),
                }
                for c in repo.column_configs
            ],
        },
        "text": {
            "markdown_representations": len(md_paths),
            "total_codepoints": total_cp,
        },
        "legacy": {
            "citations_rows": len(repo.citations),
        },
        "anomalies": [{"kind": a.kind, "count": a.count, "detail": a.detail} for a in anomalies],
    }


def render_plan(report: dict[str, Any]) -> str:
    out: list[str] = []
    w = out.append
    w(f"UPC conversion plan for {report['input']}")
    w(f"  RA schema version : {report['ra_schema_version']}")
    s = report["sources"]
    kinds = ", ".join(f"{k}={v}" for k, v in sorted(s["by_kind"].items()))
    w(f"  sources           : {s['total']}  ({kinds})")
    f = report["files"]
    w(
        f"  files             : {f['found']:,} found / {f['declared']:,} declared, "
        f"{f['distinct_hashes']:,} distinct hashes, {f['bytes'] / 1e6:,.0f} MB"
    )
    t = report["text"]
    w(
        f"  text              : {t['markdown_representations']} markdown reps, "
        f"{t['total_codepoints']:,} codepoints"
    )
    c = report["columns"]
    w(f"  columns           : {c['total']}  ({', '.join(f'{k}={v}' for k, v in c['triage'].items())})")
    for d in c["detail"]:
        w(
            f"      {d['kind']:17} {str(d['label'])[:44]:46} "
            f"allowed={d['allowed_values']:<3} values={d['values_present']}"
        )
    w("")
    if not report["anomalies"]:
        w("  no anomalies")
    else:
        w(f"  anomalies ({len(report['anomalies'])}):")
        for a in report["anomalies"]:
            w(f"    - [{a['kind']}] {a['detail']}")
    return "\n".join(out)


# --------------------------------------------------------------------------
# P1-P6 -- the conversion
# --------------------------------------------------------------------------

TOOL = "researchassistant"
CONVERTER = "ra-upc-converter"

# Only strings at least this long are candidates for an extraction. RA's stored
# "evidence" is a mix of real prose and one-word field values; minting a
# needs_review extraction for the word "Home" is noise, not provenance. Short
# values become field_evidence with no locator instead.
MIN_EVIDENCE_CODEPOINTS = 24


def _ext_for(rel_path: str) -> str:
    ext = os.path.splitext(rel_path)[1].lower()
    return ext if ext and len(ext) <= 6 else ""


@dataclass
class ConvertState:
    """Resumable progress. Tool-private (§10): under .upc/, never a corpus member."""

    schema: int = 1
    input_fingerprint: str = ""
    hash_cache: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "ConvertState":
        if not path.is_file():
            return cls()
        try:
            d = json.loads(path.read_bytes())
        except json.JSONDecodeError:
            return cls()
        return cls(
            schema=d.get("schema", 1),
            input_fingerprint=d.get("input_fingerprint", ""),
            hash_cache=d.get("hash_cache") or {},
        )

    def save(self, path: Path) -> None:
        W.write_json(path, {
            "schema": self.schema,
            "input_fingerprint": self.input_fingerprint,
            "hash_cache": self.hash_cache,
        })


class Converter:
    """Turns one RA repository into one UPC corpus.

    Deterministic by construction: every id is content-addressed and every mapping
    is a pure function of the input, so running twice over unchanged input produces
    byte-identical objects. The only value that legitimately varies between runs is
    the conversion's own journal timestamp.
    """

    def __init__(self, repo: RaRepo, out: Path, bridge: NodeBridge, *, copy: bool = False,
                 allow_rewrite_anchors: bool = False) -> None:
        self.repo = repo
        self.out = out
        self.bridge = bridge
        self.copy = copy
        self.allow_rewrite_anchors = allow_rewrite_anchors
        self.state = ConvertState.load(out / ".upc" / "convert-state.json")

        self.inv: Inventory | None = None
        self.images_by_source: dict[str, list[str]] = {}
        # ra source id -> {src_id, slug, dir, reps: {field: rep_id}, rep_paths, digest}
        self.src: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.report: dict[str, Any] = {"phases": {}, "notes": []}

    # -- helpers ---------------------------------------------------------

    def _note(self, text: str) -> None:
        self.report["notes"].append(text)

    def _fallback_time(self, row: dict[str, Any]) -> str:
        for k in ("fetched_at", "imported_at"):
            v = (row.get(k) or "").strip()
            if v:
                return v
        return self.repo.meta.get("created_at") or "1970-01-01T00:00:00Z"

    # -- P1: sources + representations ------------------------------------

    def p1_sources(self) -> None:
        paths, self.images_by_source = collect_paths(self.repo)
        self.inv = build_inventory(self.repo, paths, cache=self.state.hash_cache)

        # Mint every source id in one batch. URL sources hash their canonical URL;
        # an upload hashes its primary bytes. Both recipes live in Node.
        mint_args: list[dict[str, Any]] = []
        slug_seeds: list[str] = []
        for row in self.repo.sources:
            url = (row.get("original_url") or row.get("final_url") or "").strip()
            if url:
                mint_args.append({"canonicalUrl": None, "__url": url})
            else:
                sha = (row.get("sha256") or "").strip()
                if not sha:
                    # No URL and no recorded hash: fall back to the bytes of the
                    # first representation we can actually read, so identity is
                    # still content-addressed rather than invented.
                    sha = self._first_available_hash(row) or ""
                mint_args.append({"primaryBytesSha256": sha})
            slug_seeds.append(P.slug_seed(row))

        urls = [a.pop("__url") for a in mint_args if "__url" in a]
        canon = dict(zip(urls, self.bridge.canonical_urls(urls))) if urls else {}
        for row, arg in zip(self.repo.sources, mint_args):
            if "canonicalUrl" in arg:
                u = (row.get("original_url") or row.get("final_url") or "").strip()
                arg["canonicalUrl"] = canon.get(u) or u
        src_ids = self.bridge.mint("src", mint_args)
        slugs = self.bridge.slugs(slug_seeds)

        seen_slug: dict[str, int] = {}
        written = 0
        for row, src_id, slug in zip(self.repo.sources, src_ids, slugs):
            slug = slug or "source"
            n = seen_slug.get(slug, 0)
            seen_slug[slug] = n + 1
            if n:
                slug = f"{slug}-{n + 1}"
            written += self._write_source(row, src_id, slug)
        self.report["phases"]["P1"] = {"sources": written, "representations": sum(
            len(v["reps"]) for v in self.src.values())}

    def _first_available_hash(self, row: dict[str, Any]) -> str | None:
        assert self.inv is not None
        for plan in P.plan_representations(row):
            h = self.inv.hash_of.get(plan.rel_path)
            if h:
                return h
        return None

    def _write_source(self, row: dict[str, Any], src_id: str, slug: str) -> int:
        assert self.inv is not None
        ra_id = row.get("id") or ""
        plans = P.plan_representations(row)
        kept, suppressed = P.dedupe_representations(plans, self.inv.hash_of)
        if not kept:
            self._note(f"source {ra_id}: no readable representation; skipped")
            return 0

        src_dir = self.out / "sources" / slug
        rep_ids: dict[str, str] = {}
        rep_records: list[dict[str, Any]] = []
        phase_meta = row.get("phase_metadata") or {}

        mint_batch = [{"sha256": self.inv.hash_of[p.rel_path]} for p in kept]
        ids = self.bridge.mint("rep", mint_batch) if mint_batch else []

        for plan, rep_id in zip(kept, ids):
            digest = self.inv.hash_of[plan.rel_path]
            dst_name = f"{plan.role}-{rep_id[4:]}{_ext_for(plan.rel_path)}"
            dst = src_dir / "representations" / dst_name
            W.place_bytes(self.repo.root / plan.rel_path, dst, copy=self.copy)
            rec: dict[str, Any] = {
                "representation_id": rep_id,
                "role": plan.role,
                "media_type": plan.media_type,
                "path": f"sources/{slug}/representations/{dst_name}",
                "sha256": "sha256:" + digest,
            }
            if plan.produced_by:
                rec["produced_by"] = plan.produced_by
            pm = phase_meta.get(plan.phase or "") or {}
            rec["provenance"] = P.provenance_stamp(
                pm, tool=TOOL, fallback_created_at=self._fallback_time(row),
                method=plan.produced_by or "import",
            )
            ext_extra: dict[str, Any] = {"ra_field": plan.field, "ra_path": plan.rel_path}
            if plan.rel_path in suppressed:
                ext_extra["also"] = suppressed[plan.rel_path]
            rec["ext"] = {TOOL: ext_extra}
            rep_ids[plan.field] = rep_id
            rep_records.append(rec)

        # Parent links resolve after every id exists, so ordering cannot matter.
        for plan, rec in zip(kept, rep_records):
            if plan.parent_field and plan.parent_field in rep_ids:
                parent = rep_ids[plan.parent_field]
                if parent != rec["representation_id"]:
                    rec["parent_representation_ref"] = parent

        rep_records.extend(self._image_representations(row, slug, rep_ids))

        source: dict[str, Any] = {
            "source_id": src_id,
            "source_kind": P.SOURCE_KIND.get((row.get("source_kind") or "").strip().lower(), "url"),
            "title": P.source_title(row),
            "aliases": {TOOL: ra_id},
            "retrieval": P.retrieval_block(row),
            "representations": rep_records,
            "extractions_path": f"sources/{slug}/extractions.jsonl",
            "provenance": P.provenance_stamp(
                phase_meta.get("fetch") or {}, tool=TOOL,
                fallback_created_at=self._fallback_time(row), method="import",
            ),
        }
        bib = P.bibliographic_block(row)
        if bib:
            source["bibliographic"] = bib
        tags = [t.strip() for t in (row.get("tags_text") or "").split(";") if t.strip()]
        if tags:
            source["tags"] = tags
        notes = (row.get("notes") or "").strip()
        if notes:
            source["notes"] = notes

        # RA-private fields that have no shared meaning stay namespaced (§00 writer
        # discipline) rather than inventing top-level members.
        private = {k: row.get(k) for k in (
            "import_type", "provenance_ref", "date_signals", "error_message",
            "fetch_verification", "discovery_depth", "citation_number",
        ) if (row.get(k) or "") != ""}
        if private:
            source["ext"] = {TOOL: private}

        W.write_json(src_dir / "source.json", source)
        W.write_jsonl(src_dir / "extractions.jsonl", [])

        self.src[ra_id] = {
            "src_id": src_id, "slug": slug, "dir": src_dir,
            "reps": rep_ids, "row": row,
            "digest": self.inv.hash_of.get((row.get("markdown_file") or "").strip()),
        }
        self._collect_events(row, src_id, rep_ids)
        return 1

    def _image_representations(self, row: dict[str, Any], slug: str,
                               rep_ids: dict[str, str]) -> list[dict[str, Any]]:
        assert self.inv is not None
        idx_rel = (row.get("image_index_file") or "").strip()
        if not idx_rel or not (self.repo.root / idx_rel).is_file():
            return []
        try:
            idx = json.loads((self.repo.root / idx_rel).read_bytes())
        except json.JSONDecodeError:
            return []
        img_dir = (row.get("images_dir") or os.path.dirname(idx_rel)).strip()
        parent = rep_ids.get("rendered_file") or rep_ids.get("raw_file")

        entries: list[tuple[dict[str, Any], str, str]] = []
        for img in idx.get("images") or []:
            fname = (img.get("file") or "").strip()
            rel = f"{img_dir}/{fname}" if fname else ""
            digest = self.inv.hash_of.get(rel)
            if not digest:
                continue
            entries.append((img, rel, digest))
        if not entries:
            return []

        ids = self.bridge.mint("img", [{"sha256": d} for _, _, d in entries])
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for (img, rel, digest), img_id in zip(entries, ids):
            if img_id in seen:
                # Two index entries over identical bytes are one representation.
                continue
            seen.add(img_id)
            dst_name = f"img-{img_id[4:]}{_ext_for(rel)}"
            dst = self.out / "sources" / slug / "representations" / "images" / dst_name
            W.place_bytes(self.repo.root / rel, dst, copy=self.copy)
            rec: dict[str, Any] = {
                "representation_id": img_id,
                "role": "image",
                "media_type": img.get("mime") or P.media_type_for(rel, "image/png"),
                "path": f"sources/{slug}/representations/images/{dst_name}",
                "sha256": "sha256:" + digest,
                "produced_by": "vision" if img.get("analysis") == "vision" else "import",
            }
            if parent:
                rec["parent_representation_ref"] = parent
            if img.get("width") and img.get("height"):
                rec["dimensions"] = {"width": img["width"], "height": img["height"]}
            desc = (img.get("description") or "").strip() or (img.get("reason") or "").strip()
            if desc:
                rec["description"] = desc
            # `has_text` is deliberately NOT inferred. §05 makes it an indication of
            # whether the image contains legible text; RA's index has no such field,
            # and guessing it from `category` would manufacture the claim.
            rec["provenance"] = P.provenance_stamp(
                {"model": idx.get("vision_model"), "prompt_version": idx.get("prompt_version"),
                 "completed_at": idx.get("generated_at")},
                tool=TOOL, fallback_created_at=self._fallback_time(row), method="vision",
            )
            rec["ext"] = {TOOL: {k: img[k] for k in (
                "image_id", "classification", "category", "confidence", "reason",
                "relevance", "original_url", "alt", "origin",
            ) if img.get(k) not in (None, "")}}
            out.append(rec)
        return out

    def _collect_events(self, row: dict[str, Any], src_id: str, rep_ids: dict[str, str]) -> None:
        for phase, pm in (row.get("phase_metadata") or {}).items():
            if not isinstance(pm, dict):
                continue
            ev = P.phase_event(
                phase, pm, tool=TOOL,
                inputs={"source_ids": [src_id]},
                outputs={"representation_refs": sorted(set(rep_ids.values()))} if phase in ("fetch", "convert", "cleanup") else None,
            )
            if ev:
                self.events.append(ev)

    # -- P2: generations ---------------------------------------------------

    def p2_generations(self) -> None:
        made = 0
        for ra_id, info in self.src.items():
            row = info["row"]
            for field_name, gen_type in (
                ("catalog_file", "catalog"),
                ("summary_file", "summary"),
                ("rating_file", "rating"),
                ("image_descriptions_file", "description"),
            ):
                rel = (row.get(field_name) or "").strip()
                if not rel or not (self.repo.root / rel).is_file():
                    continue
                made += self._write_generation(info, row, rel, gen_type, field_name)
        self.report["phases"]["P2"] = {"generations": made}

    def _write_generation(self, info: dict[str, Any], row: dict[str, Any], rel: str,
                          gen_type: str, field_name: str) -> int:
        src_id, slug = info["src_id"], info["slug"]
        data = (self.repo.root / rel).read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        phase = {"catalog_file": "catalog", "summary_file": "summary",
                 "rating_file": "rating", "image_descriptions_file": "images"}[field_name]
        pm = (row.get("phase_metadata") or {}).get(phase) or {}

        # RA records content_digest as "" for almost every phase. Copying that empty
        # string into input_digest would block L2 rule 2.1, so the digest is computed
        # from the bytes the generation actually consumed.
        inputs = sorted(set(info["reps"].values()))
        input_digest = None
        md_rep = info["reps"].get("markdown_file") or info["reps"].get("llm_cleanup_file")
        if md_rep and self.inv:
            md_path = (row.get("markdown_file") or row.get("llm_cleanup_file") or "").strip()
            h = self.inv.hash_of.get(md_path)
            if h:
                input_digest = "sha256:" + h

        gen: dict[str, Any] = {
            "type": gen_type,
            "title": {"catalog": "Catalogue metadata", "summary": "Summary",
                      "rating": "Relevance rating", "description": "Image notes"}[gen_type],
            "provenance": P.provenance_stamp(
                pm, tool=TOOL, fallback_created_at=self._fallback_time(row),
                method="model" if pm.get("model") else "conversion",
                input_digest=input_digest,
                derived_from={"source_ids": [src_id], "representation_refs": inputs},
            ),
        }
        if pm.get("stale"):
            gen["stale"] = True
        gen_id = self.bridge.mint("gen", [gen])[0]
        gen["generation_id"] = gen_id
        # The record lives at <gen-id>.json, so the OUTPUT must not also claim that
        # name or the two overwrite each other. `.output` also keeps the loader from
        # mistaking a JSON payload for a second generation record.
        out_name = f"{gen_id}.output{_ext_for(rel)}"
        out_rel = f"sources/{slug}/generated/{out_name}"
        W.place_bytes(self.repo.root / rel, self.out / out_rel, copy=self.copy)
        gen["output"] = {
            "path": out_rel,
            "media_type": P.media_type_for(rel, "application/json"),
            "sha256": "sha256:" + digest,
        }
        gen["ext"] = {TOOL: {"ra_field": field_name, "ra_path": rel}}
        W.write_json(self.out / "sources" / slug / "generated" / f"{gen_id}.json", gen)
        info.setdefault("generations", []).append(gen_id)
        return 1

    # -- P3: codebooks + codings -------------------------------------------

    def p3_codes(self) -> None:
        triage = P.triage_columns(self.repo.column_configs)
        codebooks: dict[str, dict[str, Any]] = {}   # column id -> codebook object

        for kind in (P.COLUMN_CODEBOOK_CLOSED, P.COLUMN_CODEBOOK_OPEN):
            for cfg in triage.by_kind.get(kind, []):
                slug = self.bridge.slugs([cfg.get("label") or cfg.get("id") or "column"])[0] or "column"
                cbk = P.codebook_for_column(cfg, TOOL, slug)
                cbk["revision"] = 1
                cbk["revision_digest"] = self.bridge.call(
                    "codebookRevisionDigest", {"codes": cbk["codes"]})
                cbk["provenance"] = {
                    "produced_by": {"tool": TOOL, "method": "manual"},
                    "created_at": cfg.get("last_run_at") or self.repo.meta.get("created_at")
                    or "1970-01-01T00:00:00Z",
                }
                cbk_id = self.bridge.mint("cbk", [cbk])[0]
                cbk["codebook_id"] = cbk_id
                ordered = {"codebook_id": cbk_id, **{k: v for k, v in cbk.items() if k != "codebook_id"}}
                W.write_json(self.out / "codebooks" / f"{cbk_id}.json", ordered)
                codebooks[cfg["id"]] = ordered

        # One coding per non-empty column value. These are DOCUMENT-level judgements:
        # RA runs a column against a whole source, so target.kind is "source". Nothing
        # here is span-level, and pretending otherwise would invent a locator.
        records: list[dict[str, Any]] = []
        coder = "ra-column-v1"
        for ra_id, info in self.src.items():
            row = info["row"]
            for cid, raw_value in (row.get("custom_fields") or {}).items():
                value = (raw_value or "").strip()
                cbk = codebooks.get(cid)
                if not value or not cbk:
                    continue
                closed = cbk.get("closed") is not False
                cod: dict[str, Any] = {
                    "codebook_ref": cbk["codebook_id"],
                    "target": {"kind": "source", "id": info["src_id"]},
                    "coder": coder,
                    "status": "active",
                    "codebook_revision": cbk.get("revision"),
                    "codebook_revision_digest": cbk.get("revision_digest"),
                }
                if closed:
                    token = P.code_token(value)
                    if not any(c["code"] == token for c in cbk["codes"]):
                        # A stored value outside its own allowed list: keep it as a
                        # note rather than silently coercing it to a legal code.
                        self._note(
                            f"source {ra_id} column {cid}: value {value!r} is not in the "
                            "closed codebook; recorded in ext, not coded"
                        )
                        continue
                    cod["code"] = token
                else:
                    cod["value"] = value
                cod["provenance"] = {
                    "produced_by": {"tool": TOOL, "method": "model",
                                    "prompt_version": "source_column.v1"},
                    "created_at": self._fallback_time(row),
                    "derived_from": {"source_ids": [info["src_id"]]},
                }
                records.append(cod)

        if records:
            ids = self.bridge.mint("cod", records)
            for cod, cid in zip(records, ids):
                cod["coding_id"] = cid
            ordered = [{"coding_id": r.pop("coding_id"), **r} for r in records]
            set_id = "cds-ra-columns"
            items_rel = f"codings/{set_id}/items.jsonl"
            W.write_jsonl(self.out / items_rel, ordered)
            W.write_json(self.out / "codings" / set_id / "manifest.json", {
                "set_id": set_id,
                "title": "ResearchAssistant column values",
                "items_path": items_rel,
                "codebook_refs": sorted({c["codebook_ref"] for c in ordered}),
                "coders": [{
                    "coder": coder, "kind": "model", "display_name": "RA column runner",
                    "tool": TOOL, "prompt_version": "source_column.v1",
                }],
                "provenance": {
                    "produced_by": {"tool": CONVERTER, "method": "import"},
                    "created_at": self.repo.meta.get("updated_at") or "1970-01-01T00:00:00Z",
                },
            })

        self.report["phases"]["P3"] = {
            "codebooks": len(codebooks),
            "codings": len(records),
            "columns_not_coded": {k: len(v) for k, v in triage.by_kind.items()
                                  if k not in (P.COLUMN_CODEBOOK_CLOSED, P.COLUMN_CODEBOOK_OPEN)},
        }

    # -- P4: extractions ---------------------------------------------------

    def p4_extractions(self) -> None:
        """Anchor RA's stored evidence strings into real, gated extractions.

        RA records evidence as bare strings with no position, so every one has to be
        re-found. A string occurring exactly once in a representation becomes a
        char_range extraction that passes hop B by construction; anything else is
        reported and never written as a verified quotation.
        """
        tally = Counter()
        for ra_id, info in self.src.items():
            row = info["row"]
            cat_rel = (row.get("catalog_file") or "").strip()
            if not cat_rel or not (self.repo.root / cat_rel).is_file():
                continue
            try:
                cat = json.loads((self.repo.root / cat_rel).read_bytes())
            except json.JSONDecodeError:
                continue
            candidates = self._evidence_strings(cat)
            if not candidates:
                continue
            minted = self._anchor_into(info, candidates, tally)
            if minted:
                W.write_jsonl(info["dir"] / "extractions.jsonl", minted)
        self.report["phases"]["P4"] = dict(tally)

    def _evidence_strings(self, cat: dict[str, Any]) -> list[str]:
        out: list[str] = []
        for s in cat.get("evidence_snippets") or []:
            if isinstance(s, str):
                out.append(s)
        for cand in (cat.get("citation_candidates") or {}).values():
            if not isinstance(cand, dict):
                continue
            for e in cand.get("evidence") or []:
                if isinstance(e, str):
                    out.append(e)
            for fe in (cand.get("field_evidence") or {}).values():
                if isinstance(fe, dict) and isinstance(fe.get("evidence"), str):
                    out.append(fe["evidence"])
        # Preserve order, drop repeats and anything too short to be evidence.
        seen: set[str] = set()
        keep: list[str] = []
        for s in out:
            t = s.strip()
            if len(t) < MIN_EVIDENCE_CODEPOINTS or t in seen:
                continue
            seen.add(t)
            keep.append(t)
        return keep

    def _anchor_into(self, info: dict[str, Any], candidates: list[str],
                     tally: Counter) -> list[dict[str, Any]]:
        row = info["row"]
        # Search order matters. The deterministic extraction is the trustworthy
        # target; the raw HTML holds <title>/JSON-LD values that are metadata rather
        # than prose; the LLM rewrite is excluded unless explicitly allowed, because
        # a quote found only there is the model's wording, not the source's.
        order: list[tuple[str, str]] = [("markdown_file", "quote")]
        order += [("raw_file", "entity"), ("rendered_file", "entity")]
        if self.allow_rewrite_anchors:
            order.insert(1, ("llm_cleanup_file", "quote"))

        texts: list[tuple[str, str, str, str]] = []   # field, rep_id, ext_type, text
        for field_name, ext_type in order:
            rep_id = info["reps"].get(field_name)
            rel = (row.get(field_name) or "").strip()
            if not rep_id or not rel:
                continue
            abs_p = self.repo.root / rel
            if not abs_p.is_file():
                continue
            try:
                texts.append((field_name, rep_id, ext_type, abs_p.read_text(encoding="utf-8")))
            except UnicodeDecodeError:
                continue

        minted: list[dict[str, Any]] = []
        pending: list[dict[str, Any]] = []
        for quote in candidates:
            placed = False
            for field_name, rep_id, ext_type, text in texts:
                hits = text.count(quote)
                if hits != 1:
                    continue
                start = text.index(quote)
                ext: dict[str, Any] = {
                    "source_id": info["src_id"],
                    "representation_ref": rep_id,
                    "type": ext_type,
                    "status": "active",
                    "direct_quote": quote,
                    "locator": {
                        "type": "char_range",
                        "representation_ref": rep_id,
                        "value": {"start": start, "end": start + len(quote)},
                    },
                    "provenance": {
                        "produced_by": {"tool": CONVERTER, "method": "import"},
                        "created_at": self._fallback_time(row),
                        "derived_from": {"source_ids": [info["src_id"]],
                                         "representation_refs": [rep_id]},
                    },
                    "ext": {TOOL: {"origin": "catalog_evidence", "anchored_in": field_name}},
                }
                if ext_type == "entity":
                    # Found only in the markup: this is a metadata value lifted from
                    # the page head, not a quotation of the document's prose.
                    ext["secondary_locators"] = [
                        {"type": "css_selector", "representation_ref": rep_id, "value": "head"}
                    ]
                pending.append(ext)
                tally[f"anchored_{field_name}"] += 1
                placed = True
                break
            if not placed:
                total = sum(t.count(quote) for _, _, _, t in texts)
                tally["ambiguous" if total > 1 else "not_found"] += 1

        if pending:
            ids = self.bridge.mint("ext", pending)
            seen: set[str] = set()
            for ext, eid in zip(pending, ids):
                if eid in seen:
                    continue
                seen.add(eid)
                minted.append({"extraction_id": eid, **ext})
        return minted

    # -- P5: journal --------------------------------------------------------

    def p5_journal(self) -> None:
        events = sorted(self.events, key=lambda e: (e.get("started_at") or "", e.get("activity_type", "")))
        events.append({
            "activity_type": "import",
            "tool": CONVERTER,
            "started_at": self._now,
            "ended_at": self._now,
            "status": "success",
            "notes": (
                f"converted {len(self.src)} sources from a ResearchAssistant repository "
                f"(schema {self.repo.schema_version}) at {self.repo.root}"
            ),
        })
        for i, ev in enumerate(events, start=1):
            ev["event_id"] = f"evt-{i:06d}"
        ordered = [{"event_id": e.pop("event_id"), **e} for e in events]
        W.write_jsonl(self.out / "provenance" / "events.jsonl", ordered)
        self.report["phases"]["P5"] = {"events": len(ordered)}

    # -- P6: manifest + projections -----------------------------------------

    def p6_projections(self) -> None:
        W.write_json(self.out / "corpus.json", {
            "upc_spec_version": self.bridge.info.spec_version if self.bridge.info else "1.6.0",
            "corpus_id": "cor-" + hashlib.sha256(
                str(self.repo.root).encode("utf-8")).hexdigest()[:12],
            "title": self.repo.root.name,
            "sections": {
                "sources": "sources/",
                "extractions": "extractions/",
                "codebooks": "codebooks/",
                "codings": "codings/",
                "provenance": "provenance/events.jsonl",
                "sources_csv": "sources.csv",
                "extractions_csv": "extractions.csv",
                "index_html": "index.html",
            },
        })
        proc = self.bridge.cli(["regen", str(self.out)])
        if proc.returncode != 0:
            raise SystemExit(f"upc regen failed: {proc.stderr.strip() or proc.stdout.strip()}")
        self.report["phases"]["P6"] = json.loads(proc.stdout)

    # -- driver --------------------------------------------------------------

    def run(self) -> dict[str, Any]:
        from datetime import datetime, timezone

        self._now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.state.input_fingerprint = self.repo.state_fingerprint
        self.out.mkdir(parents=True, exist_ok=True)

        self.p1_sources()
        self.p2_generations()
        self.p3_codes()
        self.p4_extractions()
        self.p5_journal()
        self.p6_projections()

        self.state.save(self.out / ".upc" / "convert-state.json")

        # The input must not have moved under us. Taking RA's flock would itself be
        # a write, so this is the honest alternative: notice and say so.
        after = load_ra_repo(self.repo.root).state_fingerprint
        if after != self.repo.state_fingerprint:
            self.report["notes"].append(
                "WARNING: the RA repository changed while converting; re-run for a consistent corpus"
            )
        return self.report


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _assert_output_outside_input(inp: Path, out: Path) -> None:
    inp_r, out_r = inp.resolve(), out.resolve()
    try:
        if os.path.commonpath([inp_r, out_r]) == str(inp_r):
            raise SystemExit(
                f"refusing to write inside the input repository ({out_r} is under {inp_r}). "
                "The converter is read-only with respect to its input; choose a sibling directory."
            )
    except ValueError:
        pass  # different drives: trivially outside


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m backend.upc.convert_repo",
        description="Convert an attached ResearchAssistant repository into a UPC corpus (read-only).",
    )
    ap.add_argument("--in", dest="inp", required=True, help="attached RA repository root")
    ap.add_argument("--out", dest="out", help="output corpus directory (must be outside --in)")
    ap.add_argument("--plan", action="store_true", help="inspect only; write nothing")
    ap.add_argument("--json", action="store_true", help="emit the plan as JSON")
    ap.add_argument("--upc-home", help="path to the UPC package (else $UPC_HOME, else vendored)")
    ap.add_argument("--copy", action="store_true",
                    help="copy bytes instead of hardlinking (forced across filesystems)")
    ap.add_argument("--allow-rewrite-anchors", action="store_true",
                    help="also anchor evidence in the LLM-rewritten markdown. Off by default: "
                         "68%% of its substantive lines are not verbatim in the deterministic "
                         "extraction, so a quote found only there is the model's wording, not "
                         "the source's. Anything anchored there badges verified-to-rewrite.")
    args = ap.parse_args(argv)

    inp = Path(args.inp).expanduser()
    if not inp.is_dir():
        raise SystemExit(f"no such directory: {inp}")
    if args.out:
        _assert_output_outside_input(inp, Path(args.out).expanduser())
    elif not args.plan:
        raise SystemExit("--out is required unless --plan is given")

    repo = load_ra_repo(inp)

    try:
        bridge: NodeBridge | None = NodeBridge(args.upc_home)
    except UpcUnavailable as exc:
        if not args.plan:
            raise SystemExit(f"cannot convert: {exc}")
        sys.stderr.write(f"warning: {exc}\n         planning without URL-identity checks\n")
        bridge = None

    report = plan(repo, bridge)
    if bridge and bridge.info:
        report["upc"] = {"spec_version": bridge.info.spec_version, "home": str(bridge.info.home)}

    if args.plan:
        print(json.dumps(report, indent=2) if args.json else render_plan(report))
        return 0

    if bridge is None:
        raise SystemExit("cannot convert without the UPC package")
    out = Path(args.out).expanduser()
    conv = Converter(repo, out, bridge, copy=args.copy,
                     allow_rewrite_anchors=args.allow_rewrite_anchors)
    result = conv.run()
    print(json.dumps(result, indent=2))
    for note in result.get("notes", []):
        sys.stderr.write(f"note: {note}\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
