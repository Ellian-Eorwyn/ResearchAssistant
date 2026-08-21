"""Pure RA -> UPC mapping. No I/O, no clock, no randomness.

This module is deliberately side-effect free so that the offline converter
(``convert_repo.py``) and the live in-app projection can share it *exactly*. If
they ever grow separate mapping logic, the predictable result is a corpus that
validates when built offline and fails when refreshed in the app, which is the
worst kind of bug to chase. Everything here takes RA records and returns plain
dicts; the callers own reading bytes, hashing, minting ids, and writing files.

References are to the UPC spec sections in ``<upc>/spec/``.
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

# --------------------------------------------------------------------------
# Representations (spec/02, spec/05)
# --------------------------------------------------------------------------

# RA's file fields are polymorphic: `raw_file` is HTML for a url source, a PDF for
# an uploaded document, and the WebVTT transcript for most video sources. Keying
# the role off the field name alone -- which the published crosswalk does -- gets
# 6 of 7 videos wrong, so every rule below also inspects the extension and the
# row's own detected_type.
_EXT_MEDIA = {
    ".html": "text/html",
    ".htm": "text/html",
    ".pdf": "application/pdf",
    ".md": "text/markdown",
    ".txt": "text/plain",
    ".json": "application/json",
    ".vtt": "text/vtt",
    ".m4a": "audio/mp4",
    ".mp3": "audio/mpeg",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".webp": "image/webp",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".doc": "application/msword",
    ".rtf": "application/rtf",
    ".csv": "text/csv",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}

# When one file is reachable through several RA fields (28 rows in the sample
# repository have byte-identical raw_file and rendered_file), the surviving record
# takes the role highest in this list and records the others in `ext`.
ROLE_PRIORITY = [
    "document_pdf",
    "raw_html",
    "rendered_html",
    "transcript",
    "clean_markdown",
    "ocr_pdf",
    "rendered_pdf",
    "metadata",
    "audio",
    "video",
    "thumbnail",
    "image",
    "original",
]


def media_type_for(path: str, default: str = "application/octet-stream") -> str:
    ext = posixpath.splitext(path.lower())[1]
    return _EXT_MEDIA.get(ext, default)


@dataclass(frozen=True)
class RepPlan:
    """One representation to emit, before its bytes have been hashed."""

    field: str            # the RA manifest field this came from
    rel_path: str         # repo-relative source path
    role: str             # UPC representation_role
    media_type: str
    parent_field: str | None = None   # RA field of the parent representation, if derived
    produced_by: str | None = None    # UPC produced_by_method
    phase: str | None = None          # RA phase_metadata key that stamps it


# (field, role, parent_field, produced_by, phase) for the non-polymorphic fields.
_SIMPLE_FIELDS: list[tuple[str, str, str | None, str | None, str | None]] = [
    ("rendered_file", "rendered_html", "raw_file", "playwright", "fetch"),
    ("rendered_pdf_file", "rendered_pdf", "rendered_file", "playwright", "fetch"),
    ("ocr_pdf_file", "ocr_pdf", "raw_file", "ocr", None),
    ("markdown_file", "clean_markdown", "raw_file", "conversion", "convert"),
    # The LLM "cleanup" is a model REWRITE of the deterministic extraction, not a
    # normalization of it: measured across 94 pairs, 68% of substantive lines are
    # not verbatim in the parent. Declaring produced_by=model with a textual parent
    # is what earns it the `verified-to-rewrite` badge instead of plain `verified`
    # (spec/09, spec/11), so a quote taken from it can never silently claim to be
    # the source's own wording.
    ("llm_cleanup_file", "clean_markdown", "markdown_file", "model", "cleanup"),
    ("metadata_file", "metadata", None, "import", None),
    ("audio_file", "audio", None, "x-yt-dlp", "media"),
    ("video_file", "video", None, "x-yt-dlp", "media"),
    ("thumbnail_file", "thumbnail", "video_file", "x-yt-dlp", "media"),
]


def plan_representations(row: dict[str, Any]) -> list[RepPlan]:
    """Every representation an RA source row declares, in emit order.

    Excludes images, which come from the per-source image index rather than from a
    manifest field, and excludes the artifacts that are generations rather than
    representations (catalog, summary, rating, image descriptions).
    """
    plans: list[RepPlan] = []

    raw = (row.get("raw_file") or "").strip()
    if raw:
        role, produced = _raw_role(row, raw)
        plans.append(
            RepPlan(
                field="raw_file",
                rel_path=raw,
                role=role,
                media_type=media_type_for(raw),
                parent_field=None,
                produced_by=produced,
                phase="fetch",
            )
        )

    for fld, role, parent, produced, phase in _SIMPLE_FIELDS:
        p = (row.get(fld) or "").strip()
        if not p:
            continue
        plans.append(
            RepPlan(
                field=fld,
                rel_path=p,
                role=role,
                media_type=media_type_for(p),
                parent_field=parent,
                produced_by=produced,
                phase=phase,
            )
        )
    return plans


def _raw_role(row: dict[str, Any], path: str) -> tuple[str, str]:
    """`raw_file` means three different things depending on the source."""
    ext = posixpath.splitext(path.lower())[1]
    detected = (row.get("detected_type") or "").strip().lower()
    method = (row.get("fetch_method") or "").strip().lower()

    if ext == ".vtt" or detected == "video":
        # A transcript is DERIVED text (spec/05): a quotation verified against it is
        # verified to the transcript, never to the recording.
        return "transcript", "x-yt-dlp"
    if ext == ".pdf" or detected == "pdf":
        # A PDF that was uploaded is the document itself, not a render of a page.
        return "document_pdf", ("manual" if "upload" in method or "manual" in method else "http")
    if ext in (".html", ".htm") or detected == "html":
        return "raw_html", ("playwright" if method == "playwright" else "manual" if "manual" in method else "http")
    return "original", "import"


def dedupe_representations(
    plans: Sequence[RepPlan], hash_of: dict[str, str]
) -> tuple[list[RepPlan], dict[str, list[dict[str, str]]]]:
    """Collapse plans whose files are byte-identical.

    A ``rep-`` id is the file's byte hash, so two plans over the same bytes are the
    same representation and emitting both would be a duplicate id. Keep the
    highest-priority role and record what was suppressed, so nothing is silently
    lost. Returns (kept, suppressed_by_path).
    """
    by_hash: dict[str, list[RepPlan]] = {}
    for p in plans:
        h = hash_of.get(p.rel_path)
        if not h:
            continue
        by_hash.setdefault(h, []).append(p)

    kept: list[RepPlan] = []
    suppressed: dict[str, list[dict[str, str]]] = {}
    for _h, group in by_hash.items():
        group_sorted = sorted(
            group,
            key=lambda p: (
                ROLE_PRIORITY.index(p.role) if p.role in ROLE_PRIORITY else len(ROLE_PRIORITY),
                p.field,
            ),
        )
        winner = group_sorted[0]
        kept.append(winner)
        if len(group_sorted) > 1:
            suppressed[winner.rel_path] = [
                {"role": o.role, "path": o.rel_path, "field": o.field} for o in group_sorted[1:]
            ]
    # Emit in the original declaration order for a stable, readable source.json.
    order = {p.rel_path: i for i, p in enumerate(plans)}
    kept.sort(key=lambda p: order.get(p.rel_path, 0))
    return kept, suppressed


# --------------------------------------------------------------------------
# Columns (spec/12 and spec/04)
# --------------------------------------------------------------------------

# RA's "custom columns" are heterogeneous, and the tempting mechanical rule
# ("closed value set -> codebook, else generation") over-produces junk: a column
# holding a URL or an internal workflow flag is neither a code nor generated
# content. Columns are triaged by what they actually hold.
COLUMN_CODEBOOK_CLOSED = "codebook_closed"
COLUMN_CODEBOOK_OPEN = "codebook_open"
COLUMN_BIBLIOGRAPHIC = "bibliographic"
COLUMN_REFERENCE = "reference"
COLUMN_WORKFLOW = "workflow"

# Matched against the column label, lowercased. Order matters: the first hit wins.
_LABEL_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^citation$|^year published$|^publication year$"), COLUMN_BIBLIOGRAPHIC),
    (re.compile(r"\b(url|link|pdf|source id)\b"), COLUMN_REFERENCE),
    (re.compile(r"^flag\b|\bpossible deletion\b|\bpossible duplicate\b"), COLUMN_WORKFLOW),
]


def triage_column(cfg: dict[str, Any]) -> str:
    """Decide what a repository column becomes in UPC."""
    label = (cfg.get("label") or "").strip().lower()
    for pattern, kind in _LABEL_RULES:
        if pattern.search(label):
            return kind
    constraint = cfg.get("output_constraint") or {}
    if constraint.get("allowed_values"):
        # A closed value set IS a coding scheme; a {Yes, No, Not Sure} codebook is
        # degenerate but perfectly coherent.
        return COLUMN_CODEBOOK_CLOSED
    return COLUMN_CODEBOOK_OPEN


def codebook_for_column(cfg: dict[str, Any], namespace: str, slug: str) -> dict[str, Any]:
    """Build a codebook object (without its id) from an RA column config."""
    constraint = cfg.get("output_constraint") or {}
    allowed = list(constraint.get("allowed_values") or [])
    fallback = (constraint.get("fallback_value") or "").strip()
    closed = bool(allowed)

    codes: list[dict[str, Any]] = []
    for value in allowed:
        codes.append({"code": code_token(value), "label": value})
    if closed and fallback and code_token(fallback) not in {c["code"] for c in codes}:
        # RA substitutes the fallback when a model answer fails the constraint, so
        # the value really does occur in the data and must be a legal code -- but it
        # means "the coder declined", which is worth saying out loud.
        codes.append(
            {
                "code": code_token(fallback),
                "label": fallback,
                "definition": "No determination was made. RA substitutes this when a model answer falls outside the allowed values.",
            }
        )
    if not codes:
        codes = [{"code": "seed", "label": "(open scheme)", "definition": "Placeholder: this codebook takes free-text values."}]

    cbk: dict[str, Any] = {
        "namespace": namespace,
        "slug": slug,
        "title": cfg.get("label") or slug,
        "closed": closed,
        "multi_label": False,
        "unit": "source",
        "codes": codes,
    }
    prompt = (cfg.get("instruction_prompt") or "").strip()
    if prompt:
        # The prompt IS the coding instruction, so it is the codebook's question --
        # the single most useful thing for a human deciding whether to trust a code.
        cbk["question"] = prompt if len(prompt) <= 400 else prompt[:397] + "..."
    if cfg.get("id"):
        cbk["aliases"] = {"researchassistant": cfg["id"]}
    return cbk


_TOKEN_STRIP = re.compile(r"[^a-z0-9]+")


def code_token(value: str) -> str:
    """A stable machine token for a human-written allowed value.

    The token, not the label, is what enters the ``cod-`` identity recipe, so
    renaming a label later must not re-mint codings.
    """
    t = _TOKEN_STRIP.sub("-", (value or "").strip().lower()).strip("-")
    return t or "unlabelled"


# --------------------------------------------------------------------------
# Provenance (spec/07)
# --------------------------------------------------------------------------

# RA phase -> the journal activity_type it becomes. `activity_type` is a closed
# enum with no x- escape, so every phase must land on an existing value.
PHASE_ACTIVITY = {
    "fetch": "fetch",
    "convert": "render",
    "cleanup": "render",
    "title": "generate",
    "catalog": "generate",
    "summary": "generate",
    "rating": "generate",
    "images": "generate",
    "image_descriptions": "generate",
    "media": "fetch",
    "citation_verify": "validate",
}

# RA phase status -> UPC event status (a closed enum).
PHASE_STATUS = {
    "completed": "success",
    "success": "success",
    "failed": "failed",
    "error": "failed",
    "skipped": "skipped",
    "stale": "needs_review",
    "needs_review": "needs_review",
}


def provenance_stamp(
    phase_meta: dict[str, Any] | None,
    *,
    tool: str,
    fallback_created_at: str,
    method: str = "import",
    input_digest: str | None = None,
    derived_from: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Turn one RA ``SourcePhaseMetadata`` into a UPC provenance stamp.

    ``profile_name`` deliberately does NOT become ``prompt_version``: they are two
    different things (which rubric ran vs. which prompt text ran), and collapsing
    them would make the stamp lie. The profile goes in ``ext``.
    """
    pm = phase_meta or {}
    produced_by: dict[str, Any] = {"tool": tool, "method": method}
    if pm.get("model"):
        produced_by["model"] = pm["model"]
        produced_by["method"] = "model"
    if pm.get("prompt_version"):
        produced_by["prompt_version"] = pm["prompt_version"]

    stamp: dict[str, Any] = {
        "produced_by": produced_by,
        "created_at": pm.get("completed_at") or pm.get("started_at") or fallback_created_at,
    }
    if derived_from:
        stamp["derived_from"] = {k: v for k, v in derived_from.items() if v}
    if input_digest:
        stamp["input_digest"] = input_digest
    if pm.get("profile_name"):
        stamp["ext"] = {"researchassistant": {"profile_name": pm["profile_name"]}}
    return stamp


def phase_event(
    phase: str,
    pm: dict[str, Any],
    *,
    tool: str,
    inputs: dict[str, list[str]] | None = None,
    outputs: dict[str, list[str]] | None = None,
) -> dict[str, Any] | None:
    """Reconstruct one journal event from an RA phase record.

    Uses the phase's real timestamps, so the journal reads as a history rather than
    a wall of "now". Phases that never ran produce no event.
    """
    status = PHASE_STATUS.get((pm.get("status") or "").strip().lower())
    if status is None:
        return None
    started = pm.get("started_at")
    if not started:
        return None
    ev: dict[str, Any] = {
        "activity_type": PHASE_ACTIVITY.get(phase, "generate"),
        "tool": tool,
        "started_at": started,
        "status": status,
    }
    if pm.get("completed_at"):
        ev["ended_at"] = pm["completed_at"]
    if inputs:
        ev["inputs"] = {k: v for k, v in inputs.items() if v}
    if outputs:
        ev["outputs"] = {k: v for k, v in outputs.items() if v}
    note = (pm.get("error") or "").strip() or (pm.get("error_code") or "").strip()
    if note:
        ev["notes"] = note
    if pm.get("model"):
        ev["model"] = pm["model"]
    return ev


# --------------------------------------------------------------------------
# Source (spec/02)
# --------------------------------------------------------------------------

SOURCE_KIND = {
    "url": "url",
    "uploaded_document": "document",
    "document": "document",
    "video": "video",
    "audio": "audio",
    "image": "image",
    "dataset": "dataset",
}

FETCH_STATUS = {
    "success": "success",
    "queued": "queued",
    "partial": "partial",
    "blocked": "blocked",
    "paywall": "paywall",
    "login_required": "login_required",
    "failed": "failed",
    "not_applicable": "not_applicable",
    "": "not_applicable",
}

FETCH_METHOD = {
    "http": "http",
    "playwright": "playwright",
    "manual_capture": "manual",
    "manual_upload": "manual",
    "local_upload": "manual",
    "yt_dlp": "x-yt-dlp",
}


def retrieval_block(row: dict[str, Any]) -> dict[str, Any]:
    ret: dict[str, Any] = {}
    for src_key, dst_key in (
        ("original_url", "original_url"),
        ("final_url", "final_url"),
        ("canonical_url", "canonical_url"),
        ("fetched_at", "fetched_at"),
        ("content_type", "content_type"),
        ("extraction_method", "extraction_method"),
    ):
        v = (row.get(src_key) or "").strip()
        if v:
            ret[dst_key] = v
    status = (row.get("fetch_status") or "").strip().lower()
    ret["fetch_status"] = FETCH_STATUS.get(status, "not_applicable")
    method = (row.get("fetch_method") or "").strip().lower()
    if method:
        ret["fetch_method"] = FETCH_METHOD.get(method, "x-" + re.sub(r"[^a-z0-9-]+", "-", method))
    http_status = row.get("http_status")
    if http_status not in (None, "", 0):
        try:
            ret["http_status"] = int(http_status)
        except (TypeError, ValueError):
            pass
    sha = (row.get("sha256") or "").strip()
    if sha:
        ret["sha256"] = sha if sha.startswith("sha256:") else "sha256:" + sha
    return ret


def bibliographic_block(row: dict[str, Any]) -> dict[str, Any]:
    """CSL-aligned bibliographic metadata from the flat manifest row."""
    bib: dict[str, Any] = {}
    title = (row.get("title") or "").strip()
    if title:
        bib["title"] = title
    authors = _split_authors(row.get("author_names") or "")
    if authors:
        bib["authors"] = authors
    issued = _issued(row)
    if issued:
        bib["issued"] = issued
    doc_type = (row.get("document_type") or "").strip()
    if doc_type:
        bib["item_type"] = _csl_item_type(doc_type, row)
    else:
        bib["item_type"] = _csl_item_type("", row)
    org = (row.get("organization_name") or "").strip()
    if org:
        bib["publisher"] = org
    url = (row.get("final_url") or row.get("original_url") or "").strip()
    if url:
        bib["url"] = url
    doi = (row.get("seed_doi") or "").strip()
    if doi:
        bib["doi"] = doi
    return bib


_CSL_BY_KIND = {"url": "webpage", "video": "motion_picture", "uploaded_document": "report", "document": "report"}


def _csl_item_type(doc_type: str, row: dict[str, Any]) -> str:
    d = doc_type.strip().lower()
    if "journal" in d or "article" in d:
        return "article-journal"
    if "report" in d:
        return "report"
    if "book" in d:
        return "book"
    if "thesis" in d or "dissertation" in d:
        return "thesis"
    if "standard" in d:
        return "standard"
    if "video" in d:
        return "motion_picture"
    return _CSL_BY_KIND.get((row.get("source_kind") or "").strip().lower(), "webpage")


_AUTHOR_SPLIT = re.compile(r"\s*(?:;|\band\b|\|)\s*", re.I)


def _split_authors(text: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for part in _AUTHOR_SPLIT.split(text or ""):
        p = part.strip().strip(",")
        if not p:
            continue
        if "," in p:
            family, _, given = p.partition(",")
            family, given = family.strip(), given.strip()
            if family and given:
                out.append({"family": family, "given": given})
                continue
        # An organizational or single-token name is a CSL literal, not a split name.
        out.append({"literal": p})
    return out


_YEAR = re.compile(r"\b(1[6-9]\d{2}|20\d{2})\b")
_ISO = re.compile(r"^(\d{4})-(\d{2})(?:-(\d{2}))?")


def _issued(row: dict[str, Any]) -> dict[str, Any] | None:
    date = (row.get("publication_date") or "").strip()
    m = _ISO.match(date)
    if m:
        parts = [int(m.group(1)), int(m.group(2))]
        if m.group(3):
            parts.append(int(m.group(3)))
        return {"date_parts": [parts]}
    year = (str(row.get("publication_year") or "")).strip()
    m2 = _YEAR.search(year) or _YEAR.search(date)
    if m2:
        return {"date_parts": [[int(m2.group(1))]]}
    return None


def source_title(row: dict[str, Any]) -> str:
    for key in ("title", "source_document_name", "original_url", "id"):
        v = (row.get(key) or "").strip()
        if v:
            return v
    return "Untitled"


def slug_seed(row: dict[str, Any]) -> str:
    """The human-readable text a source directory name is derived from.

    Author + year + title, per spec/06's slug guidance, so a folder listing reads
    like a bibliography rather than a list of hashes.
    """
    bits: list[str] = []
    authors = _split_authors(row.get("author_names") or "")
    if authors:
        first = authors[0]
        bits.append(first.get("family") or first.get("literal") or "")
    issued = _issued(row)
    bits.append(str(issued["date_parts"][0][0]) if issued else "n-d")
    bits.append(source_title(row))
    return " ".join(b for b in bits if b).strip()


@dataclass
class ColumnTriage:
    """How every repository column was classified, for the conversion report."""

    by_kind: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def add(self, kind: str, cfg: dict[str, Any]) -> None:
        self.by_kind.setdefault(kind, []).append(cfg)

    def counts(self) -> dict[str, int]:
        return {k: len(v) for k, v in sorted(self.by_kind.items())}


def triage_columns(configs: Iterable[dict[str, Any]]) -> ColumnTriage:
    t = ColumnTriage()
    for cfg in configs:
        t.add(triage_column(cfg), cfg)
    return t
