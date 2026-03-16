"""Built-in taxonomy presets and conversion utilities."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Callable


def slugify(label: str) -> str:
    """Convert a human-readable label to a URL-safe slug."""
    slug = label.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s]+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug.strip("-")


def _collect_descendant_labels(node: dict) -> list[str]:
    """Recursively collect labels from all descendants of a node."""
    labels: list[str] = []
    for child in node.get("children", []):
        label = (child.get("label") or "").strip()
        if label:
            labels.append(label)
        labels.extend(_collect_descendant_labels(child))
    return labels


def convert_sep_json_to_config(data: dict) -> dict:
    """Convert the nested SEP/InPhO JSON taxonomy into the YAML-compatible config dict.

    Mapping:
      - Level 1 children of root → domains
      - Level 2 children → subdomains
      - Level 3-4 descendant labels → keywords for their level-2 ancestor
      - Domains with no level-2 children get a single self-named subdomain
    """
    domains: dict[str, dict] = {}

    for domain_node in data.get("children", []):
        domain_label = (domain_node.get("label") or "").strip()
        if not domain_label:
            continue
        domain_slug = slugify(domain_label)

        level2_children = domain_node.get("children", [])

        if not level2_children:
            # Domain with no subdomains: create a single self-named subdomain
            domains[domain_slug] = {
                "name": domain_label,
                "subdomains": {
                    domain_slug: {
                        "name": domain_label,
                        "keywords": [domain_label],
                        "category_patterns": [],
                        "negative_keywords": [],
                    }
                },
            }
            continue

        subdomains: dict[str, dict] = {}
        for sub_node in level2_children:
            sub_label = (sub_node.get("label") or "").strip()
            if not sub_label:
                continue
            sub_slug = slugify(sub_label)

            # Collect this subdomain's own label + all descendant labels as keywords
            keywords = [sub_label]
            keywords.extend(_collect_descendant_labels(sub_node))

            subdomains[sub_slug] = {
                "name": sub_label,
                "keywords": keywords,
                "category_patterns": [],
                "negative_keywords": [],
            }

        domains[domain_slug] = {
            "name": domain_label,
            "subdomains": subdomains,
        }

    return {
        "domains": domains,
        "classification": {
            "min_confidence": 2.0,
            "max_subdomains_per_article": 3,
            "weights": {
                "title_keyword": 5.0,
                "category_match": 3.0,
                "lead_text_keyword": 1.5,
                "negative_title": -6.0,
                "negative_category": -4.0,
                "negative_lead": -2.0,
            },
        },
    }


def load_sep_preset() -> dict:
    """Load and convert the bundled SEP/InPhO taxonomy JSON."""
    json_path = Path(__file__).parent / "sep_inpho.json"
    data = json.loads(json_path.read_text(encoding="utf-8"))
    return convert_sep_json_to_config(data)


def _wikipedia_taxonomy_candidates() -> list[Path]:
    env_value = (os.getenv("WIKICLAUDE_TAXONOMY_PATH") or "").strip()
    candidates: list[Path] = []
    if env_value:
        candidates.append(Path(env_value).expanduser())
    candidates.extend(
        [
            Path.home() / "Obsidian" / "Dev" / "Wiki-Claude" / "config" / "domains.yaml",
            Path(__file__).resolve().parents[2].parent / "Wiki-Claude" / "config" / "domains.yaml",
        ]
    )
    return candidates


def _resolve_wikipedia_taxonomy_path() -> Path:
    for candidate in _wikipedia_taxonomy_candidates():
        if candidate.exists() and candidate.is_file():
            return candidate
    raise ValueError(
        "Wikipedia taxonomy config not found. Set `WIKICLAUDE_TAXONOMY_PATH` "
        "or place `domains.yaml` in `~/Obsidian/Dev/Wiki-Claude/config/`."
    )


def load_wikipedia_preset() -> dict:
    """Load the Wiki-Claude YAML domains taxonomy used for Wikipedia classification."""
    from backend.pipeline.stage_export_sqlite import load_domain_config

    return load_domain_config(_resolve_wikipedia_taxonomy_path())


BUILTIN_PRESETS: dict[str, dict] = {
    "wikipedia": {
        "name": "Wikipedia",
        "description": "Wiki-Claude domains.yaml taxonomy",
        "loader": load_wikipedia_preset,
    },
    "sep": {
        "name": "Stanford Encyclopedia of Philosophy",
        "description": "276-node InPhO taxonomy (22 philosophy domains, 70 subdomains)",
        "loader": load_sep_preset,
    },
}


def get_taxonomy_config(
    preset: str | None,
    custom_path: str | None = None,
) -> dict | None:
    """Dispatch taxonomy loading based on preset key.

    Returns the taxonomy config dict, or None for default classification.
    """
    effective_preset = (preset or "").strip().lower()

    # Backward compat: if no preset but a custom path is given, treat as "custom"
    if not effective_preset and custom_path and custom_path.strip():
        effective_preset = "custom"

    if not effective_preset or effective_preset == "none":
        return None

    if effective_preset == "custom":
        from backend.pipeline.stage_export_sqlite import load_domain_config

        raw_path = (custom_path or "").strip()
        if not raw_path:
            raise ValueError("Custom taxonomy requires a file path.")
        path = Path(raw_path).expanduser()
        if not path.exists() or not path.is_file():
            raise ValueError(f"Taxonomy config not found: {path}")
        return load_domain_config(path)

    entry = BUILTIN_PRESETS.get(effective_preset)
    if entry is None:
        valid = ", ".join(sorted(BUILTIN_PRESETS.keys()))
        raise ValueError(
            f"Unknown taxonomy preset: '{effective_preset}'. Valid presets: {valid}, custom, none"
        )

    loader: Callable[[], dict] = entry["loader"]
    return loader()
