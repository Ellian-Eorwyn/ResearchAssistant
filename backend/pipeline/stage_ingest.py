"""Stage 1: File ingestion — extract structured text from uploaded documents."""

from __future__ import annotations

from pathlib import Path

from backend.models.ingestion import IngestedDocument, IngestionArtifact
from backend.parsers.docx_parser import extract_docx
from backend.parsers.md_parser import extract_md
from backend.parsers.pdf_parser import extract_pdf


SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".md"}


def run_ingestion(upload_dir: Path) -> IngestionArtifact:
    """Ingest all supported files in the upload directory."""
    documents: list[IngestedDocument] = []

    for file_path in sorted(upload_dir.iterdir()):
        ext = file_path.suffix.lower()
        if ext not in SUPPORTED_EXTENSIONS:
            continue

        if ext == ".pdf":
            doc = extract_pdf(file_path)
        elif ext == ".docx":
            doc = extract_docx(file_path)
        elif ext == ".md":
            doc = extract_md(file_path)
        else:
            continue

        documents.append(doc)

    return IngestionArtifact(documents=documents)
