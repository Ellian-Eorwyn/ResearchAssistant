/* ResearchAssistant - Frontend Application Logic */

(function () {
    "use strict";

    // ---- State ----
    const state = {
        jobId: null,
        files: [],
        settings: {},
        pollInterval: null,
        sourcePollInterval: null,
        repositoryPollInterval: null,
        hasSourceUrls: false,
        hasExportCsv: false,
    };

    // ---- DOM refs ----
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);

    const dom = {
        backendKind: $("#backend-kind"),
        baseUrl: $("#base-url"),
        apiKey: $("#api-key"),
        apiKeyRow: $("#api-key-row"),
        ollamaSettings: $("#ollama-settings"),
        ollamaTemperature: $("#ollama-temperature"),
        ollamaThinkMode: $("#ollama-think-mode"),
        modelSelect: $("#model-select"),
        btnLoadModels: $("#btn-load-models"),
        useLlm: $("#use-llm"),
        btnSaveSettings: $("#btn-save-settings"),
        settingsStatus: $("#settings-status"),
        uploadZone: $("#upload-zone"),
        fileInput: $("#file-input"),
        fileList: $("#file-list"),
        sourceListInput: $("#source-list-input"),
        btnUploadSourceList: $("#btn-upload-source-list"),
        sourceListStatus: $("#source-list-status"),
        repositoryPath: $("#repository-path"),
        btnAttachRepository: $("#btn-attach-repository"),
        repositoryStatus: $("#repository-status"),
        repositorySummary: $("#repository-summary"),
        repositorySourceListInput: $("#repository-source-list-input"),
        btnRepositoryImportList: $("#btn-repository-import-list"),
        repositoryDocumentInput: $("#repository-document-input"),
        btnRepositoryImportDocument: $("#btn-repository-import-document"),
        btnRepositoryDownload: $("#btn-repository-download"),
        btnRepositoryRebuild: $("#btn-repository-rebuild"),
        btnRepositoryManifestCsv: $("#btn-repository-manifest-csv"),
        btnRepositoryManifestXlsx: $("#btn-repository-manifest-xlsx"),
        btnRepositoryCitationsCsv: $("#btn-repository-citations-csv"),
        researchPurpose: $("#research-purpose"),
        btnProcess: $("#btn-process"),
        progressPanel: $("#progress-panel"),
        progressBar: $("#progress-bar"),
        progressPct: $("#progress-pct"),
        stageList: $("#stage-list"),
        resultsPanel: $("#results-panel"),
        warningsPanel: $("#warnings-panel"),
        warningsList: $("#warnings-list"),
        exportPanel: $("#export-panel"),
        exportSummary: $("#export-summary"),
        btnDownloadCsv: $("#btn-download-csv"),
        btnDownloadSqlite: $("#btn-download-sqlite"),
        btnDownloadSources: $("#btn-download-sources"),
        btnRerunFailed: $("#btn-rerun-failed"),
        btnCancelSources: $("#btn-cancel-sources"),
        sourcesProgress: $("#sources-progress"),
        sourcesProgressBar: $("#sources-progress-bar"),
        sourcesProgressText: $("#sources-progress-text"),
        sourcesSummary: $("#sources-summary"),
        sourcesRuntimeGuidance: $("#sources-runtime-guidance"),
        sourcesStatusList: $("#sources-status-list"),
        sourcesFiles: $("#sources-files"),
        btnDownloadManifestCsv: $("#btn-download-manifest-csv"),
        btnDownloadManifestXlsx: $("#btn-download-manifest-xlsx"),
        btnDownloadSourcesBundle: $("#btn-download-sources-bundle"),
        fetchDelay: $("#fetch-delay"),
        sourcesRunDownload: $("#sources-run-download"),
        sourcesRunCleanup: $("#sources-run-cleanup"),
        sourcesRunSummary: $("#sources-run-summary"),
        sourcesOutputRaw: $("#sources-output-raw"),
        sourcesOutputRenderedHtml: $("#sources-output-rendered-html"),
        sourcesOutputRenderedPdf: $("#sources-output-rendered-pdf"),
        sourcesOutputMarkdown: $("#sources-output-markdown"),
        sourcesForceDownload: $("#sources-force-download"),
        sourcesForceCleanup: $("#sources-force-cleanup"),
        sourcesForceSummary: $("#sources-force-summary"),
        sourcesRunRating: $("#sources-run-rating"),
        sourcesForceRating: $("#sources-force-rating"),
        projectProfileRow: $("#project-profile-row"),
        profileUploadRow: $("#profile-upload-row"),
        projectProfileSelect: $("#project-profile-select"),
        profileUploadInput: $("#profile-upload-input"),
        btnUploadProfile: $("#btn-upload-profile"),
        mergePrimaryPath: $("#merge-primary-path"),
        mergeSecondaryPath: $("#merge-secondary-path"),
        mergeOutputPathRow: $("#merge-output-path-row"),
        mergeOutputPath: $("#merge-output-path"),
        btnMergeRepos: $("#btn-merge-repos"),
        mergeStatus: $("#merge-status"),
        btnRepoSqlite: $("#btn-repo-sqlite"),
    };

    // ---- API helpers ----
    async function parseApiResponse(resp) {
        let data = null;
        try {
            data = await resp.json();
        } catch (e) {
            data = null;
        }
        if (!resp.ok) {
            const detail = data?.detail || data?.message || `API error: ${resp.status}`;
            throw new Error(detail);
        }
        return data;
    }

    async function apiGet(path) {
        const resp = await fetch(`/api/${path}`);
        return parseApiResponse(resp);
    }

    async function apiPost(path, body) {
        const resp = await fetch(`/api/${path}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        return parseApiResponse(resp);
    }

    async function apiPut(path, body) {
        const resp = await fetch(`/api/${path}`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        return parseApiResponse(resp);
    }

    // ---- Stage display names ----
    const STAGE_NAMES = {
        ingesting: "Ingesting document",
        detecting_references: "Detecting references section",
        parsing_bibliography: "Parsing bibliography entries",
        detecting_citations: "Detecting in-text citations",
        extracting_sentences: "Extracting citing sentences",
        matching_citations: "Matching citations to bibliography",
        exporting: "Exporting CSV",
    };

    // ---- Settings ----
    async function loadSettings() {
        try {
            const s = await apiGet("settings");
            state.settings = s;
            dom.backendKind.value = s.llm_backend?.kind || "ollama";
            dom.baseUrl.value = s.llm_backend?.base_url || "http://localhost:11434";
            dom.apiKey.value = s.llm_backend?.api_key || "";
            dom.ollamaTemperature.value = String(
                s.llm_backend?.temperature ?? 0
            );
            dom.ollamaThinkMode.value = normalizeThinkMode(
                s.llm_backend?.think_mode || "default"
            );
            dom.useLlm.checked = s.use_llm || false;
            dom.researchPurpose.value = s.research_purpose || "";
            dom.fetchDelay.value = s.fetch_delay ?? 2.0;
            if (dom.repositoryPath) {
                dom.repositoryPath.value = s.repository_path || "";
            }
            if (dom.sourcesRunSummary) {
                dom.sourcesRunSummary.checked = Boolean(s.use_llm);
            }
            if (dom.sourcesRunCleanup) {
                dom.sourcesRunCleanup.checked = false;
            }
            updateBackendSettingsVisibility();
            syncSourceTaskControls();
            if (s.llm_backend?.model) {
                addModelOption(s.llm_backend.model, true);
            }
            await loadRepositoryStatus(false);
        } catch (e) {
            console.error("Failed to load settings:", e);
        }
    }

    async function saveSettings() {
        const settings = {
            llm_backend: {
                kind: dom.backendKind.value,
                base_url: dom.baseUrl.value,
                api_key: dom.apiKey.value,
                model: dom.modelSelect.value,
                temperature: normalizeTemperature(dom.ollamaTemperature.value),
                think_mode: normalizeThinkMode(dom.ollamaThinkMode.value),
            },
            use_llm: dom.useLlm.checked,
            research_purpose: dom.researchPurpose.value,
            repository_path: (dom.repositoryPath?.value || "").trim(),
            fetch_delay: parseFloat(dom.fetchDelay.value) || 2.0,
        };
        try {
            const saved = await apiPut("settings", settings);
            state.settings = saved;
            dom.settingsStatus.textContent = "Saved";
            setTimeout(() => (dom.settingsStatus.textContent = ""), 2000);
        } catch (e) {
            dom.settingsStatus.textContent = "Error saving";
        }
    }

    function updateBackendSettingsVisibility() {
        const isOpenAi = dom.backendKind.value === "openai";
        dom.apiKeyRow.style.display =
            isOpenAi ? "" : "none";
        dom.ollamaSettings.style.display =
            isOpenAi ? "none" : "";
    }

    function addModelOption(model, selected) {
        const opt = document.createElement("option");
        opt.value = model;
        opt.textContent = model;
        opt.selected = selected;
        dom.modelSelect.appendChild(opt);
    }

    async function loadModels() {
        dom.btnLoadModels.disabled = true;
        dom.btnLoadModels.textContent = "Loading...";
        try {
            const params = new URLSearchParams({
                backend_kind: dom.backendKind.value,
                base_url: dom.baseUrl.value,
                api_key: dom.apiKey.value,
            });
            const resp = await apiGet(`models?${params}`);
            dom.modelSelect.innerHTML = "";
            if (resp.error) {
                const opt = document.createElement("option");
                opt.textContent = `Error: ${resp.error}`;
                dom.modelSelect.appendChild(opt);
            } else if (resp.models.length === 0) {
                const opt = document.createElement("option");
                opt.textContent = "No models found";
                dom.modelSelect.appendChild(opt);
            } else {
                const savedModel = state.settings?.llm_backend?.model || "";
                resp.models.forEach((m) => {
                    addModelOption(m, m === savedModel);
                });
            }
        } catch (e) {
            dom.modelSelect.innerHTML =
                '<option>Failed to connect</option>';
        }
        dom.btnLoadModels.disabled = false;
        dom.btnLoadModels.textContent = "Load Models";
    }

    // ---- Upload ----
    function setupUpload() {
        dom.uploadZone.addEventListener("click", () => {
            dom.fileInput.value = "";
            dom.fileInput.click();
        });

        dom.uploadZone.addEventListener("dragover", (e) => {
            e.preventDefault();
            dom.uploadZone.classList.add("dragover");
        });
        dom.uploadZone.addEventListener("dragleave", () => {
            dom.uploadZone.classList.remove("dragover");
        });
        dom.uploadZone.addEventListener("drop", (e) => {
            e.preventDefault();
            dom.uploadZone.classList.remove("dragover");
            handleFiles(e.dataTransfer.files);
        });
        dom.fileInput.addEventListener("change", () => {
            handleFiles(dom.fileInput.files);
        });
    }

    function fileSignature(file) {
        return [file.name, file.size, file.lastModified].join("::");
    }

    function handleFiles(fileList) {
        const incoming = Array.from(fileList || []);
        if (incoming.length === 0) {
            return;
        }

        const seen = new Set(state.files.map(fileSignature));
        incoming.forEach((file) => {
            const signature = fileSignature(file);
            if (seen.has(signature)) {
                return;
            }
            seen.add(signature);
            state.files.push(file);
        });

        renderFileList();
        dom.btnProcess.disabled = state.files.length === 0;
    }

    function renderFileList() {
        if (state.files.length === 0) {
            dom.fileList.style.display = "none";
            return;
        }
        dom.fileList.style.display = "";
        dom.fileList.innerHTML = state.files
            .map(
                (f, i) =>
                    `<div class="file-item">
                        <span>${escapeHtml(f.name)} (${formatBytes(f.size)})</span>
                        <span class="remove" data-index="${i}">&times;</span>
                    </div>`
            )
            .join("");

        dom.fileList.querySelectorAll(".remove").forEach((el) => {
            el.addEventListener("click", () => {
                const idx = parseInt(el.dataset.index);
                state.files.splice(idx, 1);
                renderFileList();
                dom.btnProcess.disabled = state.files.length === 0;
            });
        });
    }

    async function uploadSourceList() {
        const file = dom.sourceListInput.files && dom.sourceListInput.files[0];
        if (!file) {
            setSourceListStatus("Choose a CSV or XLSX file first.", true);
            return;
        }

        dom.btnUploadSourceList.disabled = true;
        dom.btnUploadSourceList.textContent = "Uploading...";
        setSourceListStatus("");

        try {
            const formData = new FormData();
            formData.append("file", file);
            if (state.jobId) {
                formData.append("job_id", state.jobId);
            }

            const resp = await fetch("/api/sources/upload-list", {
                method: "POST",
                body: formData,
            });
            const data = await resp.json();
            if (!resp.ok) {
                throw new Error(data.detail || "Upload failed");
            }

            state.jobId = data.job_id;
            state.hasSourceUrls = (data.total_urls_in_job || 0) > 0;

            dom.progressPanel.style.display = "none";
            dom.resultsPanel.style.display = "none";
            dom.warningsPanel.style.display = "none";
            dom.exportPanel.style.display = "";

            // The standalone source list flow doesn't produce citation CSV export.
            if (!data.merged_with_existing_job) {
                state.hasExportCsv = false;
                dom.btnDownloadCsv.disabled = true;
                dom.btnDownloadSqlite.disabled = true;
                    }

            resetSourceDownloadUI();
            dom.btnDownloadSources.disabled =
                !sourcePhasesSelected() ||
                (dom.sourcesRunDownload.checked && !state.hasSourceUrls);
            dom.exportSummary.innerHTML =
                `<p>Source list loaded: ${data.accepted_rows} URLs accepted ` +
                `(${data.missing_url_rows} rows missing URL, ` +
                `${data.estimated_duplicate_urls} estimated duplicates). ` +
                `Total URLs currently in job: ${data.total_urls_in_job}.</p>`;

            setSourceListStatus("Source list uploaded.");
            dom.sourceListInput.value = "";
        } catch (e) {
            setSourceListStatus(String(e.message || "Upload failed"), true);
        } finally {
            dom.btnUploadSourceList.disabled = false;
            dom.btnUploadSourceList.textContent = "Upload Source List";
        }
    }

    function setSourceListStatus(message, isError = false) {
        dom.sourceListStatus.textContent = message || "";
        dom.sourceListStatus.style.color = isError ? "var(--error)" : "";
    }

    function setRepositoryStatus(message, isError = false) {
        if (!dom.repositoryStatus) return;
        dom.repositoryStatus.textContent = message || "";
        dom.repositoryStatus.style.color = isError ? "var(--error)" : "";
    }

    function setRepositoryButtonsEnabled(enabled, status) {
        const attached = Boolean(status?.attached && enabled);
        const isRunning = status?.download_state === "running";
        const queued = status?.queued_count || 0;

        if (dom.btnRepositoryImportList) {
            dom.btnRepositoryImportList.disabled = !attached || isRunning;
        }
        if (dom.btnRepositoryImportDocument) {
            dom.btnRepositoryImportDocument.disabled = !attached || isRunning;
        }
        if (dom.btnRepositoryDownload) {
            dom.btnRepositoryDownload.disabled = !attached || isRunning || queued === 0;
        }
        if (dom.btnRepositoryRebuild) {
            dom.btnRepositoryRebuild.disabled = !attached || isRunning;
        }
        if (dom.btnRepositoryManifestCsv) {
            dom.btnRepositoryManifestCsv.disabled = !attached;
        }
        if (dom.btnRepositoryManifestXlsx) {
            dom.btnRepositoryManifestXlsx.disabled = !attached;
        }
        if (dom.btnRepositoryCitationsCsv) {
            dom.btnRepositoryCitationsCsv.disabled = !attached;
        }
        const hasCitations = attached && (status?.total_citations || 0) > 0;
        if (dom.btnRepoSqlite) {
            dom.btnRepoSqlite.disabled = !hasCitations;
        }
        if (dom.btnRepoSqliteTaxonomy) {
            dom.btnRepoSqliteTaxonomy.disabled = !hasCitations;
        }
    }

    function renderRepositoryStatus(status) {
        if (!dom.repositorySummary) return;
        if (!status || !status.attached) {
            dom.repositorySummary.textContent = "No repository attached.";
            setRepositoryButtonsEnabled(false, status);
            if (status?.message) {
                setRepositoryStatus(status.message);
            }
            return;
        }

        if (dom.repositoryPath && status.path) {
            dom.repositoryPath.value = status.path;
        }

        const health = status.health || {};
        const outputSummaryText = renderSourceOutputSummary(status.output_summary || {});
        dom.repositorySummary.textContent =
            `${status.total_sources || 0} sources | ` +
            `${status.total_citations || 0} citation rows | ` +
            `${status.queued_count || 0} queued | ` +
            `next ID ${status.next_source_id || 1} | ` +
            `${status.download_state || "idle"} | ` +
            `${health.missing_files || 0} missing files | ` +
            `${health.orphaned_citation_rows || 0} orphaned citation rows` +
            (outputSummaryText ? ` | ${outputSummaryText}` : "");

        setRepositoryButtonsEnabled(true, status);
        if (status.message) {
            setRepositoryStatus(status.message);
        }
    }

    function startRepositoryStatusPolling() {
        if (state.repositoryPollInterval) clearInterval(state.repositoryPollInterval);
        state.repositoryPollInterval = setInterval(async () => {
            await loadRepositoryStatus(false);
        }, 1500);
    }

    function stopRepositoryStatusPolling() {
        if (!state.repositoryPollInterval) return;
        clearInterval(state.repositoryPollInterval);
        state.repositoryPollInterval = null;
    }

    async function loadRepositoryStatus(allowPolling = true) {
        if (!dom.repositorySummary) return;
        try {
            const status = await apiGet("repository/status");
            renderRepositoryStatus(status);
            if (allowPolling && status.download_state === "running") {
                startRepositoryStatusPolling();
            } else if (status.download_state !== "running") {
                stopRepositoryStatusPolling();
            }
        } catch (e) {
            stopRepositoryStatusPolling();
            setRepositoryButtonsEnabled(false, null);
            if (dom.repositorySummary) {
                dom.repositorySummary.textContent = "Repository status unavailable.";
            }
        }
    }

    async function activateRepositoryExportJob(scope, importId = "") {
        const payload = { scope };
        if (scope === "import") {
            payload.import_id = (importId || "").trim();
        }
        const exportJob = await apiPost("repository/export-job", payload);

        state.jobId = exportJob.job_id || null;
        state.hasSourceUrls = (exportJob.total_urls || 0) > 0;
        state.hasExportCsv = false;

        dom.resultsPanel.style.display = "none";
        dom.warningsPanel.style.display = "none";
        dom.exportPanel.style.display = "";
        dom.btnDownloadCsv.disabled = true;
        dom.btnDownloadSqlite.disabled = true;

        resetSourceDownloadUI();
        dom.btnDownloadSources.disabled =
            !sourcePhasesSelected() ||
            (dom.sourcesRunDownload.checked && !state.hasSourceUrls);
        dom.btnRerunFailed.disabled = true;
        dom.exportSummary.innerHTML = `<p>${escapeHtml(exportJob.message || "")}</p>`;
        await loadSourceDownloadStatus();
        return exportJob;
    }

    async function attachRepository() {
        const path = (dom.repositoryPath?.value || "").trim();
        if (!path) {
            setRepositoryStatus("Enter an absolute path first.", true);
            return;
        }

        dom.btnAttachRepository.disabled = true;
        dom.btnAttachRepository.textContent = "Attaching...";
        setRepositoryStatus("");

        try {
            const status = await apiPost("repository/attach", { path });
            renderRepositoryStatus(status);
            if (status.download_state === "running") {
                startRepositoryStatusPolling();
            }
            try {
                const exportJob = await activateRepositoryExportJob("all");
                setRepositoryStatus(`Repository attached and scanned. ${exportJob.message}`);
            } catch (e) {
                setRepositoryStatus(
                    `Repository attached and scanned. ${String(e.message || "No URLs available for export tasks.")}`,
                    false
                );
            }
        } catch (e) {
            setRepositoryStatus(String(e.message || "Failed to attach repository"), true);
        } finally {
            dom.btnAttachRepository.disabled = false;
            dom.btnAttachRepository.textContent = "Attach + Scan";
        }
    }

    async function importRepositorySourceList() {
        const file = dom.repositorySourceListInput?.files?.[0];
        if (!file) {
            setRepositoryStatus("Choose a spreadsheet file first.", true);
            return;
        }

        dom.btnRepositoryImportList.disabled = true;
        dom.btnRepositoryImportList.textContent = "Importing...";
        setRepositoryStatus("");

        try {
            const formData = new FormData();
            formData.append("file", file);
            const resp = await fetch("/api/repository/import/source-list", {
                method: "POST",
                body: formData,
            });
            const data = await resp.json();
            if (!resp.ok) throw new Error(data.detail || "Import failed");
            dom.repositorySourceListInput.value = "";
            await loadRepositoryStatus(false);
            let detail = "";
            try {
                const exportJob = await activateRepositoryExportJob("import", data.import_id);
                detail = ` ${exportJob.message}`;
            } catch (e) {
                detail = ` ${String(e.message || "No new URLs available for export tasks.")}`;
            }
            setRepositoryStatus(
                `Imported ${data.accepted_new} new URLs (${data.duplicates_skipped} duplicates skipped).${detail}`,
                false
            );
        } catch (e) {
            setRepositoryStatus(String(e.message || "Import failed"), true);
        } finally {
            dom.btnRepositoryImportList.disabled = false;
            dom.btnRepositoryImportList.textContent = "Import Spreadsheet";
        }
    }

    async function importRepositoryDocument() {
        const file = dom.repositoryDocumentInput?.files?.[0];
        if (!file) {
            setRepositoryStatus("Choose a document file first.", true);
            return;
        }

        dom.btnRepositoryImportDocument.disabled = true;
        dom.btnRepositoryImportDocument.textContent = "Importing...";
        setRepositoryStatus("");

        try {
            const formData = new FormData();
            formData.append("file", file);
            const resp = await fetch("/api/repository/import/document", {
                method: "POST",
                body: formData,
            });
            const data = await resp.json();
            if (!resp.ok) throw new Error(data.detail || "Import failed");
            dom.repositoryDocumentInput.value = "";
            await loadRepositoryStatus(false);
            let detail = "";
            try {
                const exportJob = await activateRepositoryExportJob("import", data.import_id);
                detail = ` ${exportJob.message}`;
            } catch (e) {
                detail = ` ${String(e.message || "No new URLs available for export tasks.")}`;
            }
            setRepositoryStatus(
                `Imported ${data.accepted_new} new URLs from document (${data.duplicates_skipped} duplicates skipped).${detail}`,
                false
            );
        } catch (e) {
            setRepositoryStatus(String(e.message || "Document import failed"), true);
        } finally {
            dom.btnRepositoryImportDocument.disabled = false;
            dom.btnRepositoryImportDocument.textContent = "Import Document";
        }
    }

    async function startRepositoryDownload() {
        dom.btnRepositoryDownload.disabled = true;
        dom.btnRepositoryDownload.textContent = "Starting...";
        setRepositoryStatus("");

        try {
            const result = await apiPost("repository/download", {});
            setRepositoryStatus(result.message || "Repository download started.");
            await loadRepositoryStatus(false);
            if (result.status === "started") {
                startRepositoryStatusPolling();
            }
        } catch (e) {
            setRepositoryStatus(String(e.message || "Repository download failed"), true);
        } finally {
            dom.btnRepositoryDownload.textContent = "Download Queued Sources";
            await loadRepositoryStatus(false);
        }
    }

    async function rebuildRepositoryOutputs() {
        dom.btnRepositoryRebuild.disabled = true;
        dom.btnRepositoryRebuild.textContent = "Rebuilding...";
        setRepositoryStatus("");

        try {
            const result = await apiPost("repository/rebuild", {});
            setRepositoryStatus(result.message || "Repository spreadsheets rebuilt.");
            await loadRepositoryStatus(false);
        } catch (e) {
            setRepositoryStatus(String(e.message || "Rebuild failed"), true);
        } finally {
            dom.btnRepositoryRebuild.textContent = "Rebuild Spreadsheets";
            dom.btnRepositoryRebuild.disabled = false;
        }
    }

    // ---- Repository Merge ----
    function setMergeStatus(message, isError = false) {
        if (!dom.mergeStatus) return;
        dom.mergeStatus.textContent = message || "";
        dom.mergeStatus.style.color = isError ? "var(--error)" : "";
    }

    function syncMergeOutputPathVisibility() {
        const mode = document.querySelector('input[name="merge-output-mode"]:checked')?.value || "new";
        if (dom.mergeOutputPathRow) {
            dom.mergeOutputPathRow.style.display = mode === "new" ? "" : "none";
        }
    }

    async function mergeRepositories() {
        const primaryPath = (dom.mergePrimaryPath?.value || "").trim();
        const secondaryPath = (dom.mergeSecondaryPath?.value || "").trim();
        const outputMode = document.querySelector('input[name="merge-output-mode"]:checked')?.value || "new";
        const outputPath = (dom.mergeOutputPath?.value || "").trim();

        if (!primaryPath) {
            setMergeStatus("Enter the primary repository path.", true);
            return;
        }
        if (!secondaryPath) {
            setMergeStatus("Enter the secondary repository path.", true);
            return;
        }
        if (outputMode === "new" && !outputPath) {
            setMergeStatus("Enter an output directory for the merged repository.", true);
            return;
        }

        if (dom.btnMergeRepos) {
            dom.btnMergeRepos.disabled = true;
            dom.btnMergeRepos.textContent = "Merging...";
        }
        setMergeStatus("");

        try {
            const result = await apiPost("repository/merge", {
                primary_path: primaryPath,
                secondary_path: secondaryPath,
                output_mode: outputMode,
                output_path: outputPath,
            });
            setMergeStatus(result.message || "Merge started. Check repository status for progress.");
            // Start polling repository status to pick up completion
            startRepositoryStatusPolling();
        } catch (e) {
            setMergeStatus(String(e.message || "Merge failed"), true);
        } finally {
            if (dom.btnMergeRepos) {
                dom.btnMergeRepos.disabled = false;
                dom.btnMergeRepos.textContent = "Merge Repositories";
            }
        }
    }

    // ---- Processing ----
    async function startProcessing() {
        dom.btnProcess.disabled = true;
        dom.btnProcess.textContent = "Uploading...";

        // Reset UI
        state.hasSourceUrls = false;
        state.hasExportCsv = false;
        dom.resultsPanel.style.display = "none";
        dom.warningsPanel.style.display = "none";
        dom.exportPanel.style.display = "none";
        resetSourceDownloadUI();
        dom.btnDownloadCsv.disabled = true;
        dom.btnDownloadSqlite.disabled = true;

        try {
            // Upload files
            const formData = new FormData();
            state.files.forEach((f) => formData.append("files", f));

            const uploadResp = await fetch("/api/upload", {
                method: "POST",
                body: formData,
            });
            if (!uploadResp.ok) throw new Error("Upload failed");
            const uploadData = await uploadResp.json();
            state.jobId = uploadData.job_id;

            // Start processing
            dom.btnProcess.textContent = "Processing...";
            const config = {
                use_llm: dom.useLlm.checked,
                research_purpose: dom.researchPurpose.value,
            };
            await apiPost(`process/${state.jobId}`, config);

            // Show progress and start polling
            dom.progressPanel.style.display = "";
            startStatusPolling();
        } catch (e) {
            alert("Error: " + e.message);
            dom.btnProcess.disabled = false;
            dom.btnProcess.textContent = "Extract Citations";
        }
    }

    function startStatusPolling() {
        if (state.pollInterval) clearInterval(state.pollInterval);
        state.pollInterval = setInterval(async () => {
            try {
                const status = await apiGet(`status/${state.jobId}`);
                updateProgressUI(status);

                if (
                    status.current_stage === "completed" ||
                    status.current_stage === "failed"
                ) {
                    clearInterval(state.pollInterval);
                    state.pollInterval = null;
                    dom.btnProcess.disabled = false;
                    dom.btnProcess.textContent = "Extract Citations";

                    if (status.current_stage === "completed") {
                        await loadResults();
                    }
                }
            } catch (e) {
                console.error("Polling error:", e);
            }
        }, 800);
    }

    function updateProgressUI(status) {
        const pct = status.progress_pct || 0;
        dom.progressBar.style.width = pct + "%";
        dom.progressPct.textContent = Math.round(pct) + "%";

        const stages = status.stages || [];
        dom.stageList.innerHTML = stages
            .map((s) => {
                const name = STAGE_NAMES[s.stage] || s.stage;
                const dotClass = s.status || "pending";
                let info = "";
                if (s.status === "completed" && s.item_count > 0) {
                    info = `${s.item_count} items`;
                }
                if (s.warnings && s.warnings.length > 0) {
                    info += ` (${s.warnings.length} warnings)`;
                }
                return `<li>
                    <span class="stage-dot ${dotClass}"></span>
                    <span>${escapeHtml(name)}</span>
                    <span class="stage-info">${escapeHtml(info)}</span>
                </li>`;
            })
            .join("");

        // Collect warnings
        collectWarnings(stages);
    }

    function collectWarnings(stages) {
        const items = [];
        stages.forEach((s) => {
            (s.warnings || []).forEach((w) => {
                items.push({ type: "warning", stage: s.stage, message: w });
            });
            (s.errors || []).forEach((e) => {
                items.push({ type: "error", stage: s.stage, message: e });
            });
        });
        if (items.length > 0) {
            dom.warningsPanel.style.display = "";
            dom.warningsList.innerHTML = items
                .map(
                    (item) =>
                        `<li class="${item.type}">
                            <strong>${STAGE_NAMES[item.stage] || item.stage}:</strong>
                            ${escapeHtml(item.message)}
                        </li>`
                )
                .join("");
        }
    }

    // ---- Results ----
    async function loadResults() {
        try {
            // Load bibliography preview
            const bibData = await apiGet(
                `results/${state.jobId}?stage=bibliography`
            );
            const entries = bibData.entries || [];
            state.hasSourceUrls = entries.some(hasExtractedUrl);
            renderBibliography(bibData);

            // Load citations preview
            const citData = await apiGet(
                `results/${state.jobId}?stage=citations`
            );
            renderCitations(citData);
            renderSentences(citData);
            renderMatches(citData, bibData);

            // Load export summary
            const exportData = await apiGet(
                `results/${state.jobId}?stage=export`
            );
            renderExportSummary(exportData, entries);
            state.hasExportCsv = true;

            dom.resultsPanel.style.display = "";
            dom.resultsPanel.open = true;
            dom.exportPanel.style.display = "";
            dom.btnDownloadCsv.disabled = false;
            dom.btnDownloadSqlite.disabled = false;
            dom.btnDownloadSources.disabled =
                !sourcePhasesSelected() ||
                (dom.sourcesRunDownload.checked && !state.hasSourceUrls);
            dom.btnRerunFailed.disabled = true;
            await loadSourceDownloadStatus();
        } catch (e) {
            console.error("Failed to load results:", e);
        }
    }

    function renderBibliography(data) {
        const entries = data.entries || [];
        if (entries.length === 0) {
            $("#tab-bibliography").innerHTML =
                "<p class='muted'>No bibliography entries found.</p>";
            return;
        }
        let html =
            '<table class="preview-table"><thead><tr>' +
            "<th>#</th><th>Authors</th><th>Title</th><th>Year</th><th>URL</th><th>DOI</th><th>Confidence</th>" +
            "</tr></thead><tbody>";
        entries.forEach((e) => {
            const conf = confidenceBadge(e.parse_confidence);
            const urlCell = e.url
                ? `<a href="${escapeHtml(e.url)}" target="_blank" rel="noopener">${escapeHtml(e.url.length > 60 ? e.url.substring(0, 57) + "..." : e.url)}</a>`
                : "-";
            html += `<tr>
                <td>${e.ref_number || "-"}</td>
                <td>${escapeHtml((e.authors || []).join("; "))}</td>
                <td>${escapeHtml(e.title || e.raw_text.substring(0, 80))}</td>
                <td>${escapeHtml(e.year)}</td>
                <td>${urlCell}</td>
                <td>${escapeHtml(e.doi)}</td>
                <td>${conf}</td>
            </tr>`;
        });
        html += "</tbody></table>";
        $("#tab-bibliography").innerHTML = html;
    }

    function renderCitations(data) {
        const citations = data.citations || [];
        if (citations.length === 0) {
            $("#tab-citations").innerHTML =
                "<p class='muted'>No in-text citations found.</p>";
            return;
        }
        let html =
            '<table class="preview-table"><thead><tr>' +
            "<th>Marker</th><th>Ref Numbers</th><th>Page</th><th>Style</th>" +
            "</tr></thead><tbody>";
        citations.forEach((c) => {
            html += `<tr>
                <td>${escapeHtml(c.raw_marker)}</td>
                <td>${c.ref_numbers.join(", ")}</td>
                <td>${c.page_number || "-"}</td>
                <td>${c.style}</td>
            </tr>`;
        });
        html += "</tbody></table>";
        $("#tab-citations").innerHTML = html;
    }

    function renderSentences(data) {
        const sentences = data.sentences || [];
        if (sentences.length === 0) {
            $("#tab-sentences").innerHTML =
                "<p class='muted'>No citing sentences found.</p>";
            return;
        }
        let html =
            '<table class="preview-table"><thead><tr>' +
            "<th>Page</th><th>Sentence</th><th>Paragraph</th><th>Citations</th>" +
            "</tr></thead><tbody>";
        sentences.forEach((s) => {
            const para = s.paragraph && s.paragraph !== s.text
                ? escapeHtml(s.paragraph.length > 300 ? s.paragraph.substring(0, 297) + "..." : s.paragraph)
                : '<span class="muted">same as sentence</span>';
            html += `<tr>
                <td>${s.page_number || "-"}</td>
                <td>${escapeHtml(s.text)}</td>
                <td>${para}</td>
                <td>${s.citation_ids.length}</td>
            </tr>`;
        });
        html += "</tbody></table>";
        $("#tab-sentences").innerHTML = html;
    }

    function renderMatches(citData, bibData) {
        const matches = citData.matches || [];
        const entries = bibData.entries || [];
        if (matches.length === 0) {
            $("#tab-matches").innerHTML =
                "<p class='muted'>No matches found.</p>";
            return;
        }
        let html =
            '<table class="preview-table"><thead><tr>' +
            "<th>Ref#</th><th>Cited Entry</th><th>Confidence</th><th>Method</th>" +
            "</tr></thead><tbody>";
        matches.forEach((m) => {
            let entryText = "-";
            if (
                m.matched_bib_entry_index !== null &&
                m.matched_bib_entry_index < entries.length
            ) {
                const e = entries[m.matched_bib_entry_index];
                entryText = e.title || e.raw_text.substring(0, 60);
            }
            const conf = confidenceBadge(m.match_confidence);
            html += `<tr>
                <td>${m.ref_number}</td>
                <td>${escapeHtml(entryText)}</td>
                <td>${conf}</td>
                <td>${m.match_method}</td>
            </tr>`;
        });
        html += "</tbody></table>";
        $("#tab-matches").innerHTML = html;
    }

    function renderExportSummary(data, bibliographyEntries) {
        const rows = data.rows || [];
        const sourceUrlCount = countUniqueSourceUrls(bibliographyEntries || []);
        dom.exportSummary.innerHTML =
            `<p>${rows.length} rows exported | ` +
            `${data.matched_count || 0} matched | ` +
            `${data.unmatched_count || 0} unmatched | ` +
            `${data.total_bib_entries || 0} bibliography entries | ` +
            `${sourceUrlCount} URLs ready for Download Sources</p>`;
    }

    // ---- Source Download ----
    function sourcePhasesSelected() {
        return Boolean(
            dom.sourcesRunDownload?.checked ||
            dom.sourcesRunCleanup?.checked ||
            dom.sourcesRunSummary?.checked
        );
    }

    function sourceDownloadOutputsSelected() {
        return Boolean(
            dom.sourcesOutputRaw?.checked ||
            dom.sourcesOutputRenderedHtml?.checked ||
            dom.sourcesOutputRenderedPdf?.checked ||
            dom.sourcesOutputMarkdown?.checked
        );
    }

    function getSourceTaskPayload(rerunFailedOnly = false) {
        return {
            rerun_failed_only: rerunFailedOnly,
            run_download: Boolean(dom.sourcesRunDownload?.checked),
            run_llm_cleanup: Boolean(dom.sourcesRunCleanup?.checked),
            run_llm_summary: Boolean(dom.sourcesRunSummary?.checked),
            run_llm_rating: Boolean(dom.sourcesRunRating?.checked),
            force_redownload: Boolean(dom.sourcesForceDownload?.checked),
            force_llm_cleanup: Boolean(dom.sourcesForceCleanup?.checked),
            force_summary: Boolean(dom.sourcesForceSummary?.checked),
            force_rating: Boolean(dom.sourcesForceRating?.checked),
            project_profile_name: dom.projectProfileSelect?.value || "",
            include_raw_file: Boolean(dom.sourcesOutputRaw?.checked),
            include_rendered_html: Boolean(dom.sourcesOutputRenderedHtml?.checked),
            include_rendered_pdf: Boolean(dom.sourcesOutputRenderedPdf?.checked),
            include_markdown: Boolean(dom.sourcesOutputMarkdown?.checked),
        };
    }

    function syncSourceTaskControls() {
        const runDownload = Boolean(dom.sourcesRunDownload?.checked);
        const runCleanup = Boolean(dom.sourcesRunCleanup?.checked);
        const runSummary = Boolean(dom.sourcesRunSummary?.checked);

        [dom.sourcesOutputRaw, dom.sourcesOutputRenderedHtml, dom.sourcesOutputRenderedPdf, dom.sourcesOutputMarkdown]
            .forEach((el) => {
                if (el) el.disabled = !runDownload;
            });
        if (dom.sourcesForceDownload) dom.sourcesForceDownload.disabled = !runDownload;
        if (dom.sourcesForceCleanup) dom.sourcesForceCleanup.disabled = !runCleanup;
        if (dom.sourcesForceSummary) dom.sourcesForceSummary.disabled = !runSummary;

        if (!runDownload) {
            if (dom.sourcesForceDownload) dom.sourcesForceDownload.checked = false;
        }
        if (!runCleanup) {
            if (dom.sourcesForceCleanup) dom.sourcesForceCleanup.checked = false;
        }
        if (!runSummary) {
            if (dom.sourcesForceSummary) dom.sourcesForceSummary.checked = false;
        }

        if (runDownload && !sourceDownloadOutputsSelected()) {
            if (dom.sourcesOutputMarkdown) dom.sourcesOutputMarkdown.checked = true;
        }
    }

    function resetSourceDownloadUI() {
        if (state.sourcePollInterval) {
            clearInterval(state.sourcePollInterval);
            state.sourcePollInterval = null;
        }
        dom.sourcesProgress.style.display = "none";
        dom.sourcesFiles.style.display = "none";
        dom.sourcesProgressBar.style.width = "0%";
        dom.sourcesProgressText.textContent = "0/0";
        dom.sourcesSummary.textContent = "";
        dom.sourcesRuntimeGuidance.innerHTML = "";
        dom.sourcesRuntimeGuidance.style.display = "none";
        dom.sourcesStatusList.innerHTML = "";
        dom.btnDownloadSources.disabled = true;
        dom.btnRerunFailed.disabled = true;
        dom.btnCancelSources.disabled = true;
        syncSourceTaskControls();
    }

    async function startSourceDownload(rerunFailedOnly = false) {
        if (!state.jobId) return;
        syncSourceTaskControls();

        if (!sourcePhasesSelected()) {
            alert("Select at least one phase: download, LLM cleanup, or LLM summary.");
            return;
        }
        if (dom.sourcesRunDownload.checked && !sourceDownloadOutputsSelected()) {
            alert("Select at least one download output type.");
            return;
        }

        dom.btnDownloadSources.disabled = true;
        dom.btnRerunFailed.disabled = true;

        const btn = rerunFailedOnly ? dom.btnRerunFailed : dom.btnDownloadSources;
        const oldLabel = btn.textContent;
        btn.textContent = rerunFailedOnly ? "Re-running..." : "Starting...";

        try {
            const payload = getSourceTaskPayload(rerunFailedOnly);
            await apiPost(`sources/${state.jobId}/download`, payload);
            dom.sourcesProgress.style.display = "";
            dom.btnCancelSources.disabled = false;
            startSourceStatusPolling();
        } catch (e) {
            alert("Error starting source download: " + e.message);
            dom.btnDownloadSources.disabled =
                !sourcePhasesSelected() ||
                (dom.sourcesRunDownload.checked && !state.hasSourceUrls);
            dom.btnRerunFailed.disabled = true;
            dom.btnCancelSources.disabled = true;
        } finally {
            btn.textContent = oldLabel;
        }
    }

    async function cancelSourceDownload() {
        if (!state.jobId) return;
        dom.btnCancelSources.disabled = true;
        const oldLabel = dom.btnCancelSources.textContent;
        dom.btnCancelSources.textContent = "Cancelling...";
        try {
            await apiPost(`sources/${state.jobId}/cancel`, {});
        } catch (e) {
            alert("Error cancelling source download: " + e.message);
        } finally {
            dom.btnCancelSources.textContent = oldLabel;
        }
    }

    function startSourceStatusPolling() {
        if (state.sourcePollInterval) clearInterval(state.sourcePollInterval);
        state.sourcePollInterval = setInterval(async () => {
            try {
                const status = await apiGet(`sources/${state.jobId}/status`);
                updateSourceProgressUI(status);
                if (
                    status.state === "completed" ||
                    status.state === "failed" ||
                    status.state === "cancelled"
                ) {
                    clearInterval(state.sourcePollInterval);
                    state.sourcePollInterval = null;
                }
            } catch (e) {
                if (!String(e.message).includes("404")) {
                    console.error("Source status polling error:", e);
                }
            }
        }, 1000);
    }

    async function loadSourceDownloadStatus() {
        if (!state.jobId) return;
        try {
            const status = await apiGet(`sources/${state.jobId}/status`);
            updateSourceProgressUI(status);
            if (status.state === "running") {
                startSourceStatusPolling();
            }
        } catch (e) {
            const detail = String(e.message || "").toLowerCase();
            if (!detail.includes("404") && !detail.includes("not found")) {
                console.error("Failed to load source status:", e);
            }
        }
    }

    function updateSourceProgressUI(status) {
        const total = status.total_urls || 0;
        const processed = status.processed_urls || 0;
        const pct = total > 0 ? Math.min((processed / total) * 100, 100) : 0;
        const failedLike = (status.failed_count || 0) + (status.partial_count || 0);
        const isRunning = status.state === "running";
        const phases = [
            status.run_download ? "download" : "",
            status.run_llm_cleanup ? "LLM cleanup" : "",
            status.run_llm_summary ? "summaries" : "",
        ].filter(Boolean);
        const outputSummary = renderSourceOutputSummary(status.output_summary || {});

        dom.sourcesProgress.style.display = "";
        dom.sourcesProgressBar.style.width = `${pct}%`;
        dom.sourcesProgressText.textContent = `${processed}/${total} (${Math.round(pct)}%)`;
        dom.sourcesSummary.innerHTML =
            `<div>${escapeHtml(
                `${status.state || "pending"} | ` +
                `${status.success_count || 0} success, ` +
                `${status.partial_count || 0} partial, ` +
                `${status.failed_count || 0} failed` +
                ((status.duplicate_urls_removed || 0) > 0
                    ? `, ${status.duplicate_urls_removed} duplicates removed`
                    : "") +
                (status.message ? ` | ${status.message}` : "")
            )}</div>` +
            (phases.length > 0
                ? `<div>${escapeHtml(`Phases: ${phases.join(", ")}`)}</div>`
                : "") +
            (outputSummary ? `<div>${escapeHtml(outputSummary)}</div>` : "");

        renderRuntimeGuidance(resolveRuntimeGuidance(status));
        renderSourceStatusList(status.items || []);

        const runDownloadSelected = Boolean(dom.sourcesRunDownload?.checked);
        dom.btnDownloadSources.disabled =
            isRunning ||
            !sourcePhasesSelected() ||
            (runDownloadSelected && !state.hasSourceUrls);
        dom.btnRerunFailed.disabled =
            isRunning ||
            !runDownloadSelected ||
            failedLike === 0;
        dom.btnCancelSources.disabled = !isRunning;

        dom.sourcesFiles.style.display =
            status.state === "completed" || status.state === "cancelled" ? "" : "none";
    }

    function renderSourceOutputSummary(summary) {
        if (!summary || typeof summary !== "object") return "";
        const totalRows = summary.total_rows || 0;
        if (totalRows === 0) return "";
        return (
            `Outputs ${totalRows} rows | ` +
            `raw ${summary.raw_file_count || 0}, ` +
            `rendered HTML ${summary.rendered_html_count || 0}, ` +
            `rendered PDF ${summary.rendered_pdf_count || 0}, ` +
            `markdown ${summary.markdown_count || 0}, ` +
            `LLM cleanup ${summary.llm_cleanup_file_count || 0}` +
            ((summary.llm_cleanup_needed_count || 0) > 0
                ? ` (${summary.llm_cleanup_needed_count} flagged by LLM)`
                : "") +
            `, summaries ${summary.summary_file_count || 0}` +
            ((summary.summary_missing_count || 0) > 0
                ? ` (${summary.summary_missing_count} missing)`
                : "") +
            `, ratings ${summary.rating_file_count || 0}` +
            ((summary.rating_failed_count || 0) > 0
                ? ` (${summary.rating_failed_count} failed)`
                : "")
        );
    }

    function resolveRuntimeGuidance(status) {
        const structured = status.runtime_guidance;
        if (Array.isArray(structured) && structured.length > 0) {
            return structured;
        }

        const notes = Array.isArray(status.runtime_notes) ? status.runtime_notes : [];
        return notes
            .map((note) => {
                if (note === "runtime_missing_trafilatura") {
                    return {
                        code: note,
                        title: "Install missing parser dependency",
                        detail: "Trafilatura is unavailable, so HTML extraction uses fallback parsing.",
                        command: "./scripts/bootstrap_venv.sh",
                    };
                }
                if (note === "runtime_missing_playwright") {
                    return {
                        code: note,
                        title: "Install Playwright browser runtime",
                        detail: "Rendered HTML and visual webpage capture require Playwright. Run bootstrap from project root.",
                        command: "./scripts/bootstrap_venv.sh",
                    };
                }
                return null;
            })
            .filter(Boolean);
    }

    function renderRuntimeGuidance(items) {
        if (!Array.isArray(items) || items.length === 0) {
            dom.sourcesRuntimeGuidance.innerHTML = "";
            dom.sourcesRuntimeGuidance.style.display = "none";
            return;
        }

        dom.sourcesRuntimeGuidance.style.display = "";
        dom.sourcesRuntimeGuidance.innerHTML = items
            .map((item) => {
                const title = item.title || "Runtime setup suggested";
                const detail = item.detail || "";
                const command = item.command || "";
                return `<div class="runtime-guidance-item">
                    <div class="runtime-guidance-title">${escapeHtml(title)}</div>
                    ${detail ? `<div class="runtime-guidance-detail">${escapeHtml(detail)}</div>` : ""}
                    ${command ? `<pre class="runtime-guidance-command"><code>${escapeHtml(command)}</code></pre>` : ""}
                </div>`;
            })
            .join("");
    }

    function renderSourceStatusList(items) {
        if (!items || items.length === 0) {
            dom.sourcesStatusList.innerHTML = "";
            return;
        }
        const visible = items.slice(0, 200);
        dom.sourcesStatusList.innerHTML = visible
            .map((item) => {
                const label = item.fetch_status || item.status || "pending";
                const llmBits = [];
                if (item.llm_cleanup_status) {
                    llmBits.push(`cleanup: ${item.llm_cleanup_status}`);
                }
                if (item.summary_status) {
                    llmBits.push(`summary: ${item.summary_status}`);
                }
                if (item.rating_status) {
                    llmBits.push(`rating: ${item.rating_status}`);
                }
                const llmText = llmBits.join(" | ");
                return `<li>
                    <span class="source-id">${escapeHtml(item.id || "")}</span>
                    <span class="source-url" title="${escapeHtml(item.original_url || "")}">
                        ${escapeHtml(item.original_url || "")}
                        ${llmText ? `<br><span class="muted">${escapeHtml(llmText)}</span>` : ""}
                    </span>
                    <span class="source-status ${escapeHtml(item.status || "pending")}">${escapeHtml(label)}</span>
                </li>`;
            })
            .join("");
    }

    // ---- Tabs ----
    function setupTabs() {
        document.addEventListener("click", (e) => {
            if (!e.target.classList.contains("tab")) return;
            const tabName = e.target.dataset.tab;
            $$(".tab").forEach((t) => t.classList.remove("active"));
            $$(".tab-pane").forEach((p) => (p.style.display = "none"));
            e.target.classList.add("active");
            const pane = $(`#tab-${tabName}`);
            if (pane) pane.style.display = "";
        });
    }

    // ---- Helpers ----
    function escapeHtml(text) {
        if (!text) return "";
        const div = document.createElement("div");
        div.textContent = text;
        return div.innerHTML;
    }

    function formatBytes(bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
        return (bytes / (1024 * 1024)).toFixed(1) + " MB";
    }

    function confidenceBadge(value) {
        let cls = "high";
        if (value < 0.5) cls = "low";
        else if (value < 0.8) cls = "medium";
        return `<span class="confidence ${cls}">${(value * 100).toFixed(0)}%</span>`;
    }

    function normalizeTemperature(value) {
        const numeric = parseFloat(value);
        if (Number.isNaN(numeric)) return 0;
        return Math.max(0, Math.min(2, numeric));
    }

    function normalizeThinkMode(value) {
        const normalized = String(value || "").trim();
        if (normalized === "think" || normalized === "no_think") {
            return normalized;
        }
        return "default";
    }

    function hasExtractedUrl(entry) {
        if (!entry) return false;
        const url = (entry.url || "").trim();
        const doi = (entry.doi || "").trim();
        return Boolean(url || doi);
    }

    function normalizeSourceUrlForDedupe(value) {
        const cleaned = String(value || "")
            .trim()
            .replace(/^["'`<]+/, "")
            .replace(/[>"'`]+$/, "");
        if (!cleaned) return "";

        const candidate = cleaned.includes("://") ? cleaned : `https://${cleaned}`;
        try {
            const parsed = new URL(candidate);
            const filteredParams = Array.from(parsed.searchParams.entries())
                .filter(([key]) => {
                    const lower = key.toLowerCase();
                    return (
                        lower !== "gclid" &&
                        lower !== "fbclid" &&
                        lower !== "msclkid" &&
                        !lower.startsWith("utm_")
                    );
                })
                .sort((a, b) => {
                    if (a[0] === b[0]) return a[1].localeCompare(b[1]);
                    return a[0].localeCompare(b[0]);
                });
            const query = new URLSearchParams(filteredParams).toString();
            const canonicalPath = encodeURI(decodeURI(parsed.pathname || "/"));
            return (
                `${parsed.protocol.toLowerCase()}//${parsed.host.toLowerCase()}` +
                `${canonicalPath}` +
                (query ? `?${query}` : "")
            );
        } catch (e) {
            return candidate.toLowerCase();
        }
    }

    function countUniqueSourceUrls(entries) {
        const seen = new Set();
        entries.forEach((entry) => {
            const url = (entry?.url || "").trim();
            const doi = (entry?.doi || "").trim();
            const candidate = url || (doi ? `https://doi.org/${doi}` : "");
            if (!candidate) return;
            const key = normalizeSourceUrlForDedupe(candidate) || candidate.toLowerCase();
            seen.add(key);
        });
        return seen.size;
    }

    function syncProjectProfileControls() {
        if (!dom.projectProfileRow || !dom.sourcesRunRating) return;
        const show = dom.sourcesRunRating.checked;
        dom.projectProfileRow.style.display = show ? "" : "none";
        if (dom.profileUploadRow) dom.profileUploadRow.style.display = show ? "" : "none";
    }

    async function loadProjectProfiles() {
        if (!dom.projectProfileSelect) return;
        try {
            const profiles = await apiGet("project-profiles");
            if (!Array.isArray(profiles)) return;

            const current = dom.projectProfileSelect.value;
            dom.projectProfileSelect.innerHTML = '<option value="">-- Select a profile --</option>';
            profiles.forEach((profile) => {
                const filename = String(profile?.filename || "").trim();
                const name = String(profile?.name || "").trim();
                if (!filename || !name) return;
                const option = document.createElement("option");
                option.value = filename;
                option.textContent = name;
                dom.projectProfileSelect.appendChild(option);
            });

            if (current) {
                const available = Array.from(dom.projectProfileSelect.options).map((opt) => opt.value);
                if (available.includes(current)) {
                    dom.projectProfileSelect.value = current;
                }
            }
        } catch (e) {
            // Keep static fallback.
        }
    }

    async function uploadProjectProfile() {
        if (!dom.profileUploadInput?.files?.length) return;
        const file = dom.profileUploadInput.files[0];
        const formData = new FormData();
        formData.append("file", file);
        try {
            const resp = await fetch("/api/project-profiles/upload", {
                method: "POST",
                body: formData,
            });
            if (resp.ok) {
                const result = await resp.json();
                await loadProjectProfiles();
                if (result.filename && dom.projectProfileSelect) {
                    dom.projectProfileSelect.value = result.filename;
                }
                dom.profileUploadInput.value = "";
            }
        } catch (e) {
            // Upload failed silently.
        }
    }

    // ---- Init ----
    function init() {
        loadSettings();
        loadProjectProfiles();
        setupUpload();
        setupTabs();
        resetSourceDownloadUI();
        syncSourceTaskControls();
        syncProjectProfileControls();
        updateBackendSettingsVisibility();
        dom.btnDownloadCsv.disabled = true;
        dom.btnDownloadSqlite.disabled = true;
        setRepositoryButtonsEnabled(false, null);

        // Event listeners
        dom.sourcesRunRating?.addEventListener("change", syncProjectProfileControls);
        dom.btnUploadProfile?.addEventListener("click", uploadProjectProfile);
        dom.backendKind.addEventListener("change", updateBackendSettingsVisibility);
        dom.btnLoadModels.addEventListener("click", loadModels);
        dom.btnSaveSettings.addEventListener("click", saveSettings);
        dom.btnAttachRepository?.addEventListener("click", attachRepository);
        dom.btnRepositoryImportList?.addEventListener("click", importRepositorySourceList);
        dom.btnRepositoryImportDocument?.addEventListener("click", importRepositoryDocument);
        dom.btnRepositoryDownload?.addEventListener("click", startRepositoryDownload);
        dom.btnRepositoryRebuild?.addEventListener("click", rebuildRepositoryOutputs);
        dom.btnRepositoryManifestCsv?.addEventListener("click", () => {
            window.location.href = "/api/repository/manifest/csv";
        });
        dom.btnRepositoryManifestXlsx?.addEventListener("click", () => {
            window.location.href = "/api/repository/manifest/xlsx";
        });
        dom.btnRepositoryCitationsCsv?.addEventListener("click", () => {
            window.location.href = "/api/repository/citations/csv";
        });
        dom.btnRepoSqlite?.addEventListener("click", () => {
            window.location.href = "/api/repository/export/sqlite";
        });
        dom.btnMergeRepos?.addEventListener("click", mergeRepositories);
        document.querySelectorAll('input[name="merge-output-mode"]').forEach((radio) => {
            radio.addEventListener("change", syncMergeOutputPathVisibility);
        });
        syncMergeOutputPathVisibility();
        dom.btnUploadSourceList.addEventListener("click", uploadSourceList);
        dom.btnProcess.addEventListener("click", startProcessing);
        [
            dom.sourcesRunDownload,
            dom.sourcesRunCleanup,
            dom.sourcesRunSummary,
            dom.sourcesOutputRaw,
            dom.sourcesOutputRenderedHtml,
            dom.sourcesOutputRenderedPdf,
            dom.sourcesOutputMarkdown,
            dom.sourcesForceDownload,
            dom.sourcesForceCleanup,
            dom.sourcesForceSummary,
        ].forEach((el) => {
            el?.addEventListener("change", syncSourceTaskControls);
        });
        dom.btnDownloadCsv.addEventListener("click", () => {
            if (state.jobId && state.hasExportCsv) {
                window.location.href = `/api/export/${state.jobId}/csv`;
            }
        });
        dom.btnDownloadSqlite.addEventListener("click", () => {
            if (state.jobId && state.hasExportCsv) {
                window.location.href = `/api/export/${state.jobId}/sqlite`;
            }
        });
        dom.btnDownloadSources.addEventListener("click", () => {
            startSourceDownload(false);
        });
        dom.btnRerunFailed.addEventListener("click", () => {
            startSourceDownload(true);
        });
        dom.btnCancelSources.addEventListener("click", cancelSourceDownload);
        dom.btnDownloadManifestCsv.addEventListener("click", () => {
            if (state.jobId) {
                window.location.href = `/api/sources/${state.jobId}/manifest/csv`;
            }
        });
        dom.btnDownloadManifestXlsx.addEventListener("click", () => {
            if (state.jobId) {
                window.location.href = `/api/sources/${state.jobId}/manifest/xlsx`;
            }
        });
        dom.btnDownloadSourcesBundle.addEventListener("click", () => {
            if (state.jobId) {
                window.location.href = `/api/sources/${state.jobId}/bundle`;
            }
        });
    }

    // Start when DOM is ready
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
