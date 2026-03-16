from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import results
from backend.taxonomies import presets


class TaxonomyPresetTests(unittest.TestCase):
    def test_sep_preset_loads_domains(self):
        config = presets.get_taxonomy_config("sep")
        self.assertIsInstance(config, dict)
        self.assertIn("domains", config)
        self.assertTrue(len(config["domains"]) > 0)

    def test_wikipedia_preset_loads_from_env_path(self):
        try:
            import yaml  # noqa: F401
        except Exception:
            self.skipTest("PyYAML is not installed")

        with tempfile.TemporaryDirectory(prefix="taxonomy-presets-") as tmp:
            config_path = Path(tmp) / "domains.yaml"
            config_path.write_text(
                """
domains:
  wiki:
    name: Wiki
    subdomains:
      test:
        name: Test
        keywords:
          - sample
classification:
  min_confidence: 1.0
  max_subdomains_per_article: 3
""".strip()
                + "\n",
                encoding="utf-8",
            )
            with patch.dict("os.environ", {"WIKICLAUDE_TAXONOMY_PATH": str(config_path)}):
                config = presets.get_taxonomy_config("wikipedia")
                self.assertIn("domains", config)
                self.assertIn("wiki", config["domains"])

    def test_wikipedia_preset_missing_path_raises(self):
        with patch(
            "backend.taxonomies.presets._wikipedia_taxonomy_candidates",
            return_value=[Path("/tmp/definitely-missing-wikipedia-taxonomy.yaml")],
        ):
            with self.assertRaises(ValueError):
                presets.get_taxonomy_config("wikipedia")

    def test_taxonomy_presets_endpoint_lists_wikipedia_and_sep(self):
        app = FastAPI()
        app.include_router(results.router, prefix="/api")
        client = TestClient(app)

        resp = client.get("/api/taxonomy-presets")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        keys = {item["key"] for item in payload}
        self.assertIn("wikipedia", keys)
        self.assertIn("sep", keys)


if __name__ == "__main__":
    unittest.main()
