from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend.models.settings import LLMBackendConfig
from backend.models.sources import SourceManifestRow
from backend.pipeline.source_downloader import SourceDownloadOrchestrator
from backend.storage.file_store import FileStore


class _FakeLLMClient:
    def __init__(self, response: str):
        self.response = response

    def sync_chat_completion(self, **_: object) -> str:
        return self.response


class SourceTitleGenerationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="source-title-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.job_id = self.store.create_job()
        self.output_dir = self.tmp_path / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self._tmp.cleanup()

    def test_title_generation_uses_front_matter_without_llm(self):
        markdown_rel = Path("markdown") / "000001_clean.md"
        markdown_abs = self.output_dir / markdown_rel
        markdown_abs.parent.mkdir(parents=True, exist_ok=True)
        markdown_abs.write_text(
            "---\n"
            "title: County Health Annual Report\n"
            "---\n\n"
            "Report body text.\n",
            encoding="utf-8",
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=self.job_id,
            store=self.store,
            use_llm=False,
            run_download=False,
            run_llm_title=True,
            output_dir=self.output_dir,
        )
        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/report",
            markdown_file=markdown_rel.as_posix(),
        )

        orchestrator._generate_source_title(row, [])

        self.assertEqual(row.title, "County Health Annual Report")
        self.assertEqual(row.title_status, "extracted")

    def test_title_generation_uses_llm_fallback_and_limits_length(self):
        markdown_rel = Path("markdown") / "000002_clean.md"
        markdown_abs = self.output_dir / markdown_rel
        markdown_abs.parent.mkdir(parents=True, exist_ok=True)
        markdown_abs.write_text(
            "The World Health Organization published an extensive update about avian influenza response planning.\n",
            encoding="utf-8",
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=self.job_id,
            store=self.store,
            use_llm=True,
            llm_backend=LLMBackendConfig(
                kind="openai",
                base_url="http://localhost:1234",
                api_key="",
                model="gpt-test",
                temperature=0,
                think_mode="default",
                num_ctx=8192,
                max_source_chars=0,
                llm_timeout=60,
            ),
            run_download=False,
            run_llm_title=True,
            output_dir=self.output_dir,
        )
        orchestrator._llm_client = _FakeLLMClient(
            '{"title":"World Health Organization Avian Influenza Global Response Situation Brief Update 2026","basis":"generated"}'
        )
        row = SourceManifestRow(
            id="000002",
            original_url="https://example.com/update",
            markdown_file=markdown_rel.as_posix(),
        )

        orchestrator._generate_source_title(row, [])

        self.assertEqual(row.title_status, "generated")
        self.assertEqual(
            row.title,
            "World Health Organization Avian Influenza Global Response Situation Brief Update",
        )
        self.assertEqual(len(row.title.split()), 10)


if __name__ == "__main__":
    unittest.main()
