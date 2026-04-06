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

SOURCE_TITLE_SYSTEM = """You are resolving the title of a research source from extracted markdown.
Prefer the document's actual title when it appears in front matter, the first heading, or other clearly labeled metadata.
If the source has no clear title, generate a concise fallback title of 10 words or fewer that names the producing organization and the topic.
Do not invent citations, dates, or organizations not supported by the text.
Return JSON only."""

SOURCE_TITLE_USER = """Research purpose:
{research_purpose}

Existing title from metadata, if any:
{existing_title}

Candidate title from front matter or headings, if any:
{candidate_title}

Source content:
{source_markdown}

Return JSON in exactly this shape:
{{
  "title": "Resolved title text",
  "basis": "document_title" | "generated"
}}"""

SOURCE_CATALOG_SYSTEM = """You are cataloging a research source for a local repository.
Resolve durable bibliographic and provenance metadata from the source content.
Prefer supported evidence from front matter, headings, visible bylines, publication info, and the source URL when available.
Do not invent authors, dates, organizations, or classifications that are not reasonably supported.
Return JSON only."""

SOURCE_CATALOG_USER = """Research purpose:
{research_purpose}

Source kind:
{source_kind}

Original URL:
{original_url}

Existing metadata:
{existing_metadata_json}

Source content:
{source_markdown}

Return JSON in exactly this shape:
{{
  "title": "",
  "title_basis": "existing" | "heading" | "front_matter" | "byline" | "generated",
  "author_names": ["Author One", "Author Two"],
  "publication_date": "",
  "publication_year": "",
  "document_type": "",
  "organization_name": "",
  "organization_type": "",
  "evidence_snippets": [""],
  "citation": {{
    "item_type": "",
    "title": "",
    "authors": [
      {{"family": "", "given": "", "literal": ""}}
    ],
    "issued": "",
    "publisher": "",
    "container_title": "",
    "volume": "",
    "issue": "",
    "pages": "",
    "doi": "",
    "url": "",
    "report_number": "",
    "standard_number": "",
    "language": "",
    "accessed": "",
    "evidence": [""],
    "confidence": 0.0,
    "missing_fields": [""],
    "ready_for_ris": false
  }}
}}"""

SOURCE_CITATION_VERIFY_SYSTEM = """You verify citation metadata for a research source so it can be safely exported to RIS.
Use only the provided candidates, evidence, and source content.
Choose the most strongly supported citation value for each field.
If a field is not clearly supported, leave it blank.
Do not synthesize fallback citation titles or dates.
If no individual authors are supported but the source is clearly authored by an organization, you may use that organization as a single literal corporate author.
Prefer explicit citation metadata, DOI registry metadata, front matter, headings, bylines, and publication info over weaker heuristics.
Return JSON only."""

SOURCE_CITATION_VERIFY_USER = """Research purpose:
{research_purpose}

Source kind:
{source_kind}

Original URL:
{original_url}

Candidate metadata and evidence:
{candidate_metadata_json}

Source content:
{source_markdown}

Return JSON in exactly this shape:
{{
  "citation": {{
    "item_type": "",
    "title": "",
    "authors": [
      {{"family": "", "given": "", "literal": ""}}
    ],
    "issued": "",
    "publisher": "",
    "container_title": "",
    "volume": "",
    "issue": "",
    "pages": "",
    "doi": "",
    "url": "",
    "report_number": "",
    "standard_number": "",
    "language": "",
    "accessed": ""
  }},
  "field_evidence": {{
    "item_type": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "title": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "authors": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "issued": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "publisher": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "container_title": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "volume": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "issue": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "pages": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "doi": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "url": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "report_number": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}},
    "standard_number": {{"source_type": "", "source_label": "", "evidence": "", "confidence": 0.0}}
  }},
  "blocked_reasons": [""],
  "notes": [""],
  "verification_confidence": 0.0
}}"""

SOURCE_RATING_SYSTEM = """You are evaluating a source using an imported project profile.
Use the project profile as the governing rubric for:
- what counts as relevant
- what should be deprioritized
- how to score each dimension
- how to assign confidence
- how to specialize generic guidance using the stated research purpose when the profile tells you to

Evaluate the source only using:
1. the source content
2. the imported project profile
3. the stated research purpose when the profile explicitly uses it to tailor relevance

Return output only in the required JSON structure.

Important rules:
- Use 0.05 increments only
- Do not confuse source length with relevance
- Separate depth from relevant detail
- Lower confidence when evidence is incomplete or ambiguous
- Include a concise `tags` array with topic or source-characterization labels that help humans browse the repository
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

DOCUMENT_NORMALIZATION_SYSTEM = """You normalize extracted research documents into structured citation-aware markdown blocks.
Preserve factual meaning and document structure.
Do not invent sources or links.
Always return JSON only."""

DOCUMENT_NORMALIZATION_USER = """Normalize this document into structured blocks and a numbered works-cited list.

Required output rules:
- Body citations must be represented only as numeric references.
- Use bracketed numeric citations like [1] and [1, 3].
- Do not include a works cited heading in the body blocks.
- Every works_cited entry must include a number, citation_text, and url when one can be resolved.
- If a DOI appears without a direct URL, convert it to https://doi.org/<doi>.
- If a citation marker in the body cannot be resolved, include it in unresolved_markers.
- If you can infer parsing guidance that would help similar documents in the future, include a profile_suggestion object.
- If Document scope says this is a partial body chunk, normalize only the provided chunk, do not speculate about omitted sections, and return an empty works_cited array.
- Do not warn merely because content outside the provided chunk is not shown.

Selected ingestion profile:
{profile_json}

Document filename:
{filename}

Research purpose:
{research_purpose}

Current deterministic analysis:
{analysis_json}

Known bibliography entries from deterministic extraction:
{bibliography_context}

Document scope:
{document_scope}

Document text:
{document_text}

Return JSON with exactly these top-level keys:
{{
  "title_candidate": "",
  "blocks": [
    {{"kind": "heading" | "paragraph" | "list_item" | "table_row", "level": 1, "text": "", "citations": [1, 2]}}
  ],
  "works_cited": [
    {{"number": 1, "citation_text": "", "url": "", "doi": ""}}
  ],
  "warnings": [""],
  "unresolved_markers": [""],
  "profile_suggestion": {{
    "label": "",
    "description": "",
    "reference_heading_patterns": [""],
    "citation_marker_patterns": [""],
    "bibliography_split_patterns": [""],
    "llm_guidance": ""
  }}
}}"""

COLUMN_PROMPT_FIX_SYSTEM = """You improve user-authored spreadsheet column prompts for row-wise research extraction and normalization.
Preserve the user's intent while making the instructions clearer, more constrained, and safer for single-cell output.

Hard rules:
- Do not change the task the user asked for.
- Prefer deterministic output constraints when the task implies them.
- Bias toward concise single-cell-safe output.
- No markdown in the final cell value unless explicitly requested.
- Use blank output when evidence is insufficient unless the task clearly implies another fallback.

Return JSON only in exactly this shape:
{
  "prompt": "",
  "output_constraint": {
    "kind": "text" | "yes_no" | "integer" | "number" | "date",
    "allowed_values": [""],
    "max_words": 8,
    "fallback_value": "",
    "format_hint": ""
  },
  "notes": [""]
}"""

COLUMN_PROMPT_FIX_USER = """Column label:
{column_label}

Current saved prompt:
{current_prompt}

Current output constraint:
{current_constraint_json}

User draft prompt:
{draft_prompt}

Rewrite the draft prompt so it is better suited for row-wise extraction or transformation in a spreadsheet column.
Return JSON only."""

COLUMN_RUN_SYSTEM = """You generate exactly one spreadsheet cell value for a repository browser column.

Hard rules:
- Return JSON only.
- Output only the value for the target cell, never an explanation.
- No markdown.
- Keep the value single-cell-safe.
- Follow the requested output constraint exactly.
- If evidence is insufficient, return status `insufficient_evidence` and the fallback cell value.

Return JSON only in exactly this shape:
{
  "value": "",
  "status": "ok" | "insufficient_evidence"
}"""

COLUMN_RUN_USER = """Research purpose:
{research_purpose}

Column label:
{column_label}

Column instructions:
{column_prompt}

Output constraint:
{output_constraint_json}

Application hard rules:
{hard_rules}

Current cell value:
{current_value}

Row metadata:
{row_metadata_json}

Document text:
{document_text}

Return JSON only."""

PROJECT_PROFILE_GENERATION_SYSTEM = """You generate repository project-profile YAML files for source rating.
Preserve compatibility with the existing rating pipeline.

Hard requirements:
- Keep the YAML valid.
- Keep the output contract compatible with the current repository schema.
- Preserve these scoring dimension ids exactly:
  - overall_relevance
  - depth_score
  - relevant_detail_score
- Preserve rationale, relevant sections, tags, and flags support.
- Allow {{research_purpose}} placeholders in the generated YAML when useful.
- Do not invent extra required fields that would break the existing rating parser.
- Prefer adapting the provided template instead of inventing a brand-new schema.

Return JSON only in exactly this shape:
{
  "profile_name": "",
  "filename_stem": "",
  "yaml": ""
}"""

PROJECT_PROFILE_GENERATION_USER = """Research purpose:
{research_purpose}

Requested profile name:
{profile_name}

Current compatible template YAML:
{template_yaml}

Generate a project-specific profile draft that remains compatible with the template's output schema.
Return JSON only."""
