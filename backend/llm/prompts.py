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

SOURCE_RATING_SYSTEM = """You are evaluating a source using an imported project profile.
Use the project profile as the governing rubric for:
- what counts as relevant
- what should be deprioritized
- how to score each dimension
- how to assign confidence

Evaluate the source only using:
1. the source content
2. the imported project profile

Return output only in the required JSON structure.

Important rules:
- Use 0.05 increments only
- Do not confuse source length with relevance
- Separate depth from relevant detail
- Lower confidence when evidence is incomplete or ambiguous
- If the profile defines a flags section, include float scores (0.0-1.0, 0.05 increments) for each flag

--- PROJECT PROFILE ---
{project_profile_yaml}
--- END PROJECT PROFILE ---"""

SOURCE_RATING_USER = """Research purpose:
{research_purpose}

Source content:
{source_markdown}

Evaluate this source according to the project profile. Return JSON only."""

SOURCE_MARKDOWN_CLEANUP_SYSTEM = """You are cleaning extracted markdown from downloaded web/PDF sources.
Preserve factual content and citations while improving readability.
Do not add new claims, references, or interpretation."""

SOURCE_MARKDOWN_CLEANUP_USER = """Research purpose (for context only):
{research_purpose}

Extracted markdown:
{source_markdown}

Decide whether markdown cleanup is needed.
Respond in exactly this format:
NEEDS_CLEANUP: yes|no
CLEANED_MARKDOWN:
<markdown text when NEEDS_CLEANUP is yes; otherwise leave empty>"""
