from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.models.bibliography import BibliographyArtifact, BibliographyEntry, ReferencesSection
from backend.models.citations import CitingSentence, InTextCitation
from backend.models.common import PipelineStage, ProcessingConfig
from backend.models.ingestion import IngestedDocument, IngestionArtifact, TextBlock
from backend.pipeline.orchestrator import PipelineOrchestrator
from backend.storage.file_store import FileStore


def _doc(filename: str) -> IngestedDocument:
    return IngestedDocument(
        filename=filename,
        file_type="md",
        blocks=[
            TextBlock(
                text="Document body",
                block_index=0,
                char_offset_start=0,
                char_offset_end=13,
            )
        ],
        full_text="Document body",
        inline_citation_urls={},
    )


class PipelineOrchestratorTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="pipeline-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")

    def tearDown(self):
        self._tmp.cleanup()

    def test_processes_multiple_documents_in_one_job(self):
        job_id = self.store.create_job()
        doc_a = _doc("alpha.md")
        doc_b = _doc("beta.md")

        refs_a = ReferencesSection(
            document_filename=doc_a.filename,
            start_block_index=0,
            end_block_index=0,
            heading_text="References",
            raw_text="[1] Alpha source",
            detection_method="heading_match",
            confidence=1.0,
        )
        refs_b = ReferencesSection(
            document_filename=doc_b.filename,
            start_block_index=0,
            end_block_index=0,
            heading_text="References",
            raw_text="[1] Beta source",
            detection_method="heading_match",
            confidence=1.0,
        )

        def fake_parse_bibliography(sections, **kwargs):
            del kwargs
            if not sections:
                return BibliographyArtifact(
                    sections=[],
                    entries=[],
                    total_raw_entries=0,
                    parse_failures=0,
                )
            section = sections[0]
            return BibliographyArtifact(
                sections=sections,
                entries=[
                    BibliographyEntry(
                        ref_number=1,
                        raw_text=f"[1] source for {section.document_filename}",
                    )
                ],
                total_raw_entries=1,
                parse_failures=0,
            )

        def fake_detect_citations(doc, refs_start_offset):
            del refs_start_offset
            return [
                InTextCitation(
                    citation_id=f"{doc.filename}_cit_1",
                    document_filename=doc.filename,
                    raw_marker="[1]",
                    ref_numbers=[1],
                    char_offset_start=1,
                    char_offset_end=4,
                    style="bracket",
                )
            ]

        def fake_extract_sentences(doc, citations):
            return [
                CitingSentence(
                    sentence_id=f"{doc.filename}_sent_1",
                    document_filename=doc.filename,
                    text=f"Citation sentence in {doc.filename}",
                    citation_ids=[citations[0].citation_id],
                )
            ]

        with (
            patch(
                "backend.pipeline.orchestrator.run_ingestion",
                return_value=IngestionArtifact(documents=[doc_a, doc_b]),
            ) as mock_ingest,
            patch(
                "backend.pipeline.orchestrator.detect_references_section",
                side_effect=[refs_a, refs_b],
            ) as mock_refs,
            patch(
                "backend.pipeline.orchestrator.parse_bibliography",
                side_effect=fake_parse_bibliography,
            ) as mock_parse,
            patch(
                "backend.pipeline.orchestrator.detect_citations",
                side_effect=fake_detect_citations,
            ) as mock_citations,
            patch(
                "backend.pipeline.orchestrator.extract_citing_sentences",
                side_effect=fake_extract_sentences,
            ) as mock_sentences,
        ):
            orchestrator = PipelineOrchestrator(
                job_id=job_id,
                store=self.store,
                config=ProcessingConfig(),
            )
            orchestrator.run()

        self.assertEqual(mock_ingest.call_count, 1)
        self.assertEqual(mock_refs.call_count, 2)
        self.assertEqual(mock_parse.call_count, 2)
        self.assertEqual(mock_citations.call_count, 2)
        self.assertEqual(mock_sentences.call_count, 2)

        bibliography = self.store.load_artifact(job_id, "03_bibliography")
        self.assertEqual(len(bibliography["entries"]), 2)
        self.assertEqual(
            {entry["source_document_name"] for entry in bibliography["entries"]},
            {"alpha.md", "beta.md"},
        )

        citations = self.store.load_artifact(job_id, "04_citations")
        self.assertEqual(len(citations["citations"]), 2)
        self.assertEqual(len(citations["matches"]), 2)
        match_indices = [m["matched_bib_entry_index"] for m in citations["matches"]]
        self.assertEqual(match_indices, [0, 1])

        export = self.store.load_artifact(job_id, "05_export")
        self.assertEqual(len(export["rows"]), 2)

        status = self.store.get_job_status(job_id)
        self.assertEqual(status["current_stage"], PipelineStage.COMPLETED.value)


if __name__ == "__main__":
    unittest.main()
