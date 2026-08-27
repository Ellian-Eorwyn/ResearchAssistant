# Error codes

**Generated from the code by `scripts/generate_code_tables.py`.**
Do not edit by hand — regenerate instead, or the next change will silently
contradict it.

Every code below is emitted somewhere in the app. If you meet one that is
not here, report it to the user rather than guessing what it means.

## Fetch and convert failures

Read these from `phase_metadata.<phase>.error_code`; the code is already
split out for you. `ra triage` groups by them and gives you the remedy.

| code | what it means | worth retrying? |
|---|---|---|
| `blocked_challenge` | What came back is a bot-check or interstitial page, not the document -- a CDN 'access denied', a Cloudflare challenge, or similar. The browser retry was refused too. Save the page by hand and attach it. | no — get the document by hand |
| `blocked_fetch` | The phase did not run because the stored content is a block or challenge page rather than the document, and analysing it would describe the wall. Get the real document -- by hand, or through Resolve Fetches -- and the phase will run against it. | no — get the document by hand |
| `blocked_http` | The site answered with a status that means refusal (401, 402, 403, 407, 429 or 451) and the page body confirmed it. Save the page by hand and attach it. | no — get the document by hand |
| `blocked_request` | The site refused the request. The pipeline already retried it in a headless browser and was refused again, so another attempt will not help. Download the document by hand and attach it. | no — get the document by hand |
| `convert_missing_prerequisite` | There is nothing to convert because the fetched file is missing. Fetch first, or attach the document by hand. | yes, with convert |
| `extraction_failure` | The page was fetched but no readable text came out of it. Usually a viewer or an app shell. Save the document by hand and attach it. | no — get the document by hand |
| `internal_error` | The pipeline hit an error it did not expect and failed this one source rather than ending the whole run. The exception is in the detail. A retry is worth trying, since some causes are transient -- but if it repeats, this is a bug worth reporting rather than a source to fix. | yes |
| `invalid_url` | The address is not usable. Fix the cell in the source spreadsheet. | no — the URL is wrong |
| `llm_disabled` | The LLM is switched off in Settings, so this phase cannot run. | no — fix the environment |
| `llm_not_configured` | No usable LLM backend is configured. Set one in Settings. | no — fix the environment |
| `login_required` | The page is behind a sign-in. The pipeline does not hold credentials and will not enter any. Sign in yourself, save the page, and attach it. | no — get the document by hand |
| `media_download_failed` | The video or audio could not be downloaded. It may be private, removed, or region-locked. | no — get the document by hand |
| `missing_markdown` | A later phase needed the converted text and it was not there. Run convert. | yes, with convert |
| `missing_project_profile` | The phase needs a project profile and none was selected. | no — fix the environment |
| `network_failure` | An HTTP error. Whether it is worth retrying depends on the status code in the detail, so this is split further. | unknown |
| `not_applicable` | This phase does not apply to this source. | not a problem |
| `paywall` | What came back is a subscription prompt rather than the article. If you have access, open it yourself, save the page, and attach it. | no — get the document by hand |
| `playwright_not_installed` | The headless browser is not installed, so pages needing rendering will fail. | no — fix the environment |
| `rating_generation_failed` | The model call for the rating failed. Often transient. | yes |
| `rendering_failure` | The headless browser failed to render the page. | no — fix the environment |
| `resignal_missing_prerequisite` | Not classified. | unknown |
| `runtime_missing_yt_dlp` | yt-dlp is not installed, so video sources cannot be downloaded. | no — fix the environment |
| `summary_generation_failed` | The model call for the summary failed. Often transient. | yes |
| `timeout` | The site did not respond in time. Often transient, especially under load. | yes |
| `unsupported_content` | The response was a type the pipeline does not process. | not a problem |

`network_failure` covers every HTTP error that is not a 401/403/407/429,
so a 404 and a 503 arrive under the same code. The status is in the detail
as `http_status_<n>`, and `ra triage` splits them for you.

## Operation blockers and warnings

A blocker means nothing was changed. Fix it and run the command again.

### `attach_files`

Blockers: `ambiguous_slot`, `file_not_found`, `filename_id_not_found`, `id_claimed_twice`, `no_target_for_file`, `path_already_managed`, `path_is_internal_state`, `path_outside_repository`, `slot_claimed_twice`, `slot_not_writable`, `slot_occupied`, `symlink_not_allowed`, `unknown_role`, `unknown_source_id`, `unsupported_new_source_type`, `url_ambiguous`, `url_not_found`

Warnings: `already_attached`, `attached_file_is_a_block_page`, `duplicate_sha256`

### `create_columns`

Blockers: `column_label_exists`, `column_without_allowed_values`, `label_duplicate_in_request`, `label_required`, `prompt_required`

### `create_sources`

Blockers: `duplicate_url_in_request`, `id_duplicate_in_request`, `id_invalid`, `id_taken`, `url_already_present`, `url_invalid`, `url_required`

Warnings: `url_already_present`

### `remap_source_ids`

Blockers: `new_id_collides`, `new_id_duplicate`, `new_id_invalid`, `source_listed_twice`, `stray_manifest_would_override`, `target_dir_occupied`, `unknown_source_id`, `url_ambiguous`, `url_invalid`, `url_not_found`, `url_required`

Warnings: `artifact_outside_source_dir`, `remap_cycle`, `remap_noop`, `stray_manifest_present`, `stray_manifest_unreadable`, `url_matched_via_final_url`, `url_on_uploaded_document`

### `set_column_constraints`

Blockers: `column_without_allowed_values`, `constraint_already_set`, `unknown_column_id`

### `set_column_values`

Blockers: `column_label_ambiguous`, `column_required`, `unknown_column_id`, `unknown_column_label`, `values_required`

Warnings: `unknown_source_id`, `value_already_present`, `value_not_in_allowed`

## Spreadsheet notes

Reported by `ra plan-sheet`. None of these stop you on their own; they
tell the user what the sheet looks like and what was skipped.

`column_without_prompt`, `document_row`, `duplicate_provided_column`, `duplicate_source_id`, `duplicate_url`, `duplicate_urls_merged`, `empty_id_cell`, `mojibake_repaired`, `mojibake_unrepairable`, `no_header_row_found`, `no_prompts_row_found`, `no_sources_found`, `no_url_column_found`, `non_contiguous_ids`, `non_numeric_id`, `prompt_without_header`, `row_missing_url`

### Any operation

Blockers: `invalid_params`, `repository_busy`, `state_changed`

## Integrity checks

These run after every change. If one appears, the change was undone and
the repository is exactly as it was.

`artifact_id_prefix_mismatch`, `artifact_outside_repo`, `artifact_path_stale`, `attach_simulation_failed`, `dedupe_key_collision`, `duplicate_source_id`, `id_mismatch`, `id_missing`, `id_not_canonical`, `id_not_numeric`, `missing_artifact`, `next_attach_would_merge_rows`, `next_attach_would_renumber`, `next_source_id_too_low`, `orphan_citation`, `orphan_discovery_link`, `orphan_import_ref`, `stale_metadata_file`, `state_unparseable`, `stray_source_dir`

