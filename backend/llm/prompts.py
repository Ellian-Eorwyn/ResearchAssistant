"""Prompt templates for LLM-assisted pipeline tasks (Phase 2)."""

REFERENCES_DETECTION_SYSTEM = """You are analyzing an academic document excerpt.
Your task is to identify where the References, Bibliography, or Works Cited section begins.
Respond with JSON only."""

REFERENCES_DETECTION_USER = """The following is the last portion of an academic document.
Identify where the references/bibliography section begins.

Return JSON: {{"heading_text": "the heading text", "starts_at_line": N}}
If no references section is found, return: {{"heading_text": null, "starts_at_line": null}}

Document excerpt:
{text}"""

BIBLIOGRAPHY_REPAIR_SYSTEM = """You are parsing a bibliography entry from an academic paper.
The entry may have broken line wraps, malformed formatting, or missing fields.
Parse it into structured fields. Respond with JSON only."""

BIBLIOGRAPHY_REPAIR_USER = """Parse this bibliography entry into structured fields:

Entry:
{entry_text}

Return JSON with these fields (use empty strings for unknown fields):
{{
  "authors": ["Author One", "Author Two"],
  "title": "The title of the work",
  "year": "2024",
  "journal_or_source": "Journal Name",
  "volume": "",
  "issue": "",
  "pages": "",
  "doi": "",
  "url": ""
}}"""

SOURCE_SUMMARY_SYSTEM = """You are a research synthesis assistant.
Write one concise paragraph that is exactly 3 or 4 sentences.
Focus on the most salient issues from the source that are relevant to the stated research purpose.
Do not use bullet points, headings, or preambles."""

SOURCE_SUMMARY_USER = """Research purpose:
{research_purpose}

Source content:
{source_markdown}

Return exactly one paragraph with 3-4 sentences."""
