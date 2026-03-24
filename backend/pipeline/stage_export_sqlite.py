"""Build a Wiki-Claude compatible SQLite export database."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from backend.models.export import ExportRow
from backend.models.sources import SourceManifestRow

WIKICLAUDE_SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY,
    page_id INTEGER UNIQUE NOT NULL,
    title TEXT NOT NULL,
    namespace INTEGER DEFAULT 0,
    is_redirect INTEGER DEFAULT 0,
    redirect_target TEXT,
    is_disambiguation INTEGER DEFAULT 0,
    is_list_page INTEGER DEFAULT 0,
    is_date_page INTEGER DEFAULT 0,
    is_stub INTEGER DEFAULT 0,
    categories TEXT DEFAULT '[]',
    lead_text TEXT DEFAULT '',
    wikitext TEXT DEFAULT '',
    byte_offset INTEGER DEFAULT 0,
    text_length INTEGER DEFAULT 0,
    markdown_content TEXT DEFAULT '',
    ingested_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_articles_title ON articles(title);
CREATE INDEX IF NOT EXISTS idx_articles_page_id ON articles(page_id);
CREATE INDEX IF NOT EXISTS idx_articles_namespace ON articles(namespace);
CREATE INDEX IF NOT EXISTS idx_articles_redirect ON articles(is_redirect);
CREATE INDEX IF NOT EXISTS idx_articles_type_filter
  ON articles(namespace, is_redirect, is_disambiguation, is_date_page, is_list_page, is_stub, text_length);

CREATE TABLE IF NOT EXISTS domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS subdomains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_id INTEGER NOT NULL REFERENCES domains(id),
    slug TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT DEFAULT '',
    UNIQUE(domain_id, slug)
);

CREATE INDEX IF NOT EXISTS idx_subdomains_domain ON subdomains(domain_id);

CREATE TABLE IF NOT EXISTS classifications (
    article_id INTEGER NOT NULL REFERENCES articles(id),
    subdomain_id INTEGER NOT NULL REFERENCES subdomains(id),
    confidence REAL NOT NULL DEFAULT 0.0,
    signals TEXT DEFAULT '{}',
    is_manual INTEGER DEFAULT 0,
    PRIMARY KEY (article_id, subdomain_id)
);

CREATE INDEX IF NOT EXISTS idx_class_subdomain ON classifications(subdomain_id);
CREATE INDEX IF NOT EXISTS idx_class_article ON classifications(article_id);
CREATE INDEX IF NOT EXISTS idx_class_confidence ON classifications(confidence);
CREATE INDEX IF NOT EXISTS idx_class_subdomain_article ON classifications(subdomain_id, article_id);

CREATE TABLE IF NOT EXISTS ingest_state (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

DOMAIN_SLUG = "research-assistant"
DOMAIN_NAME = "Research Assistant"
SUBDOMAIN_SLUG = "imported-citations"
SUBDOMAIN_NAME = "Imported Citations"


@dataclass
class AggregatedArticle:
    page_id: int
    title: str = ""
    lead_text: str = ""
    cited_url: str = ""
    cited_doi: str = ""
    source_ids: set[str] = field(default_factory=set)
    source_documents: set[str] = field(default_factory=set)
    import_types: set[str] = field(default_factory=set)
    match_methods: set[str] = field(default_factory=set)
    warnings: set[str] = field(default_factory=set)
    citation_ref_numbers: set[str] = field(default_factory=set)
    citation_sentences: list[str] = field(default_factory=list)
    citation_paragraphs: list[str] = field(default_factory=list)
    cited_entries: list[str] = field(default_factory=list)
    max_confidence: float = 0.0
    markdown_content: str = ""



def build_wikiclaude_sqlite_db(
    db_path: Path,
    export_rows: list[ExportRow],
    source_rows: list[SourceManifestRow] | None = None,
    markdown_by_source_id: dict[str, str] | None = None,
) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    source_by_repo_id: dict[str, SourceManifestRow] = {}
    for row in source_rows or []:
        if row.repository_source_id:
            source_by_repo_id[row.repository_source_id] = row
        if row.id:
            source_by_repo_id.setdefault(row.id, row)

    grouped: dict[str, AggregatedArticle] = {}
    next_page_id = 1
    untitled_counter = 1

    for row in export_rows:
        key = article_group_key(row, untitled_counter)
        if key.startswith("row:"):
            untitled_counter += 1
        article = grouped.get(key)
        if article is None:
            article = AggregatedArticle(page_id=next_page_id)
            grouped[key] = article
            next_page_id += 1

        source_row = source_by_repo_id.get(row.repository_source_id or "")
        title_candidates = [
            row.cited_title,
            source_row.title if source_row else "",
            row.cited_url,
            row.cited_doi,
        ]
        for candidate in title_candidates:
            title = (candidate or "").strip()
            if title:
                article.title = article.title or title
                break

        if row.repository_source_id:
            article.source_ids.add(row.repository_source_id)
        if row.source_document:
            article.source_documents.add(row.source_document)
        if row.import_type:
            article.import_types.add(row.import_type)
        if row.match_method:
            article.match_methods.add(row.match_method)
        if row.warnings:
            article.warnings.add(row.warnings)
        if row.citation_ref_numbers:
            article.citation_ref_numbers.add(row.citation_ref_numbers)
        if row.citing_sentence:
            article.citation_sentences.append(row.citing_sentence)
        if row.citing_paragraph:
            article.citation_paragraphs.append(row.citing_paragraph)
        if row.cited_raw_entry:
            article.cited_entries.append(row.cited_raw_entry)
        if row.cited_url:
            article.cited_url = article.cited_url or row.cited_url
        if row.cited_doi:
            article.cited_doi = article.cited_doi or row.cited_doi

        summary_candidate = (row.cited_summary or "").strip()
        if summary_candidate and not article.lead_text:
            article.lead_text = summary_candidate
        elif not article.lead_text:
            article.lead_text = (row.citing_sentence or row.cited_raw_entry or "").strip()

        try:
            conf = float(row.match_confidence or 0.0)
        except Exception:
            conf = 0.0
        if conf > article.max_confidence:
            article.max_confidence = conf

    # Attach markdown content from source files if available
    if markdown_by_source_id:
        for article in grouped.values():
            if article.markdown_content:
                continue
            for sid in article.source_ids:
                md = markdown_by_source_id.get(sid, "")
                if md:
                    article.markdown_content = md
                    break

    with sqlite3.connect(str(db_path), timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA foreign_keys=ON")
        init_schema(conn)

        inserted_articles = insert_articles(conn, grouped)

        classified_articles, total_classifications = apply_default_classification(
            conn,
            inserted_articles,
        )

        refresh_domain_counts(conn)
        stats = build_cached_stats(
            total_articles=len(inserted_articles),
            classified_articles=classified_articles,
        )
        set_ingest_state(conn, "cached_stats", stats)
        set_ingest_state(conn, "ingest_complete", True)
        set_ingest_state(conn, "classification_complete", True)
        set_ingest_state(conn, "dump_file", "researchassistant_export")
        set_ingest_state(conn, "inserted_articles", len(inserted_articles))
        set_ingest_state(conn, "total_classifications", total_classifications)

        conn.commit()
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.DatabaseError:
            # Keep export robust even if checkpointing is unsupported.
            pass

    return db_path


def insert_articles(
    conn: sqlite3.Connection,
    grouped: dict[str, AggregatedArticle],
) -> list[tuple[int, AggregatedArticle, list[str]]]:
    inserted: list[tuple[int, AggregatedArticle, list[str]]] = []
    for _, article in sorted(grouped.items(), key=lambda item: item[1].page_id):
        title = article.title or f"Source {article.page_id}"
        categories = build_article_categories(article)
        wikitext = build_article_wikitext(article)
        markdown_text = article.markdown_content or ""
        if markdown_text:
            # Keep article body compatible with readers that only consume `wikitext`.
            wikitext = markdown_text
        lead_text = article.lead_text or derive_lead_text(wikitext)
        conn.execute(
            """INSERT INTO articles
               (page_id, title, namespace, is_redirect, redirect_target,
                is_disambiguation, is_list_page, is_date_page, is_stub,
                categories, lead_text, wikitext, byte_offset, text_length,
                markdown_content)
               VALUES (?, ?, 0, 0, NULL, 0, 0, 0, 0, ?, ?, ?, 0, ?, ?)""",
            (
                article.page_id,
                title,
                json.dumps(categories, ensure_ascii=False),
                lead_text[:2000],
                wikitext,
                len(wikitext),
                markdown_text,
            ),
        )
        article_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        inserted.append((article_id, article, categories))
    return inserted


def apply_default_classification(
    conn: sqlite3.Connection,
    inserted_articles: list[tuple[int, AggregatedArticle, list[str]]],
) -> tuple[int, int]:
    domain_id = upsert_domain(conn, DOMAIN_SLUG, DOMAIN_NAME, "Imported from ResearchAssistant")
    subdomain_id = upsert_subdomain(
        conn,
        domain_id,
        SUBDOMAIN_SLUG,
        SUBDOMAIN_NAME,
        "Citations exported from ResearchAssistant",
    )

    for article_id, article, _ in inserted_articles:
        conn.execute(
            """INSERT OR REPLACE INTO classifications
               (article_id, subdomain_id, confidence, signals, is_manual)
               VALUES (?, ?, ?, ?, 0)""",
            (
                article_id,
                subdomain_id,
                max(0.0, min(1.0, article.max_confidence or 0.0)),
                json.dumps(
                    {
                        "method": "researchassistant_export",
                        "source_ids": sorted(article.source_ids),
                        "citation_ref_numbers": sorted(article.citation_ref_numbers),
                    },
                    ensure_ascii=False,
                ),
            ),
        )

    return len(inserted_articles), len(inserted_articles)



def article_group_key(row: ExportRow, untitled_counter: int) -> str:
    if row.repository_source_id.strip():
        return f"source:{row.repository_source_id.strip()}"
    if row.cited_url.strip():
        return f"url:{row.cited_url.strip().lower()}"
    if row.cited_doi.strip():
        return f"doi:{row.cited_doi.strip().lower()}"
    if row.cited_title.strip():
        return f"title:{row.cited_title.strip().lower()}"
    return f"row:{untitled_counter:08d}"


def build_article_categories(article: AggregatedArticle) -> list[str]:
    categories: list[str] = []
    if article.import_types:
        categories.extend(f"import_type:{value}" for value in sorted(article.import_types))
    if article.source_documents:
        categories.extend(f"source_document:{value}" for value in sorted(article.source_documents))
    if article.match_methods:
        categories.extend(f"match_method:{value}" for value in sorted(article.match_methods))
    if article.warnings:
        categories.extend(f"warning:{value}" for value in sorted(article.warnings))
    return categories


def build_article_wikitext(article: AggregatedArticle) -> str:
    lines = [
        f"== {article.title or f'Source {article.page_id}'} ==",
    ]
    if article.cited_url:
        lines.append(f"URL: {article.cited_url}")
    if article.cited_doi:
        lines.append(f"DOI: {article.cited_doi}")
    if article.source_documents:
        lines.append("Source Documents: " + "; ".join(sorted(article.source_documents)))
    if article.import_types:
        lines.append("Import Types: " + "; ".join(sorted(article.import_types)))
    if article.match_methods:
        lines.append("Match Methods: " + "; ".join(sorted(article.match_methods)))
    if article.warnings:
        lines.append("Warnings: " + "; ".join(sorted(article.warnings)))

    if article.citation_sentences:
        lines.append("")
        lines.append("Citing Sentences:")
        for sentence in article.citation_sentences[:50]:
            lines.append(f"* {sentence}")

    if article.citation_paragraphs:
        lines.append("")
        lines.append("Citing Paragraphs:")
        for paragraph in article.citation_paragraphs[:20]:
            lines.append(f"* {paragraph}")

    if article.cited_entries:
        lines.append("")
        lines.append("Raw Bibliography Entries:")
        for entry in article.cited_entries[:20]:
            lines.append(f"* {entry}")

    return "\n".join(lines).strip()


def derive_lead_text(text: str) -> str:
    for line in (text or "").splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        candidate = re.sub(r"^#+\s*", "", candidate)
        candidate = re.sub(r"^[-*]\s+", "", candidate)
        candidate = candidate.strip()
        if candidate:
            return candidate
    return ""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(WIKICLAUDE_SCHEMA)
    for table in ("domains", "subdomains"):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN article_count INTEGER DEFAULT 0")
        except Exception:
            pass


def upsert_domain(
    conn: sqlite3.Connection,
    slug: str,
    name: str,
    description: str,
) -> int:
    conn.execute(
        "INSERT INTO domains (slug, name, description) VALUES (?, ?, ?) "
        "ON CONFLICT(slug) DO UPDATE SET name=excluded.name, description=excluded.description",
        (slug, name, description),
    )
    row = conn.execute("SELECT id FROM domains WHERE slug = ?", (slug,)).fetchone()
    return int(row["id"])


def upsert_subdomain(
    conn: sqlite3.Connection,
    domain_id: int,
    slug: str,
    name: str,
    description: str,
) -> int:
    conn.execute(
        "INSERT INTO subdomains (domain_id, slug, name, description) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(domain_id, slug) DO UPDATE SET name=excluded.name, description=excluded.description",
        (domain_id, slug, name, description),
    )
    row = conn.execute(
        "SELECT id FROM subdomains WHERE domain_id = ? AND slug = ?",
        (domain_id, slug),
    ).fetchone()
    return int(row["id"])


def refresh_domain_counts(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE subdomains SET article_count = (
            SELECT COUNT(*) FROM classifications c WHERE c.subdomain_id = subdomains.id
        )
        """
    )
    conn.execute(
        """
        UPDATE domains SET article_count = (
            SELECT COALESCE(SUM(s.article_count), 0)
            FROM subdomains s WHERE s.domain_id = domains.id
        )
        """
    )


def set_ingest_state(conn: sqlite3.Connection, key: str, value: object) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO ingest_state (key, value) VALUES (?, ?)",
        (key, json.dumps(value, ensure_ascii=False)),
    )


def build_cached_stats(total_articles: int, classified_articles: int) -> dict[str, int]:
    return {
        "total_pages": total_articles,
        "main_namespace": total_articles,
        "non_redirect_articles": total_articles,
        "classified_articles": classified_articles,
    }
