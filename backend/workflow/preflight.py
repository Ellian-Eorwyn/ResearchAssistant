"""Check everything is ready before committing to a long job.

The motivating failure: a full workflow was set up, sources created, columns
written — and only when the analysis started did it emerge that the configured
LLM backend was unreachable. Nothing in the app checks that; `llm_backend_ready_for_chat`
inspects the config's shape and never touches the network.

So this returns three separate verdicts rather than one. Fetching 41 URLs and
running 574 model calls have different prerequisites, and an agent needs to know
it can do the first but not the second *before* it starts either.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from .models import Check, Preflight, na, nas

# The Playwright probe launches a real browser, so it is cached. Do not push this
# down into `check_playwright_runtime` -- other callers rely on it being live.
_CAPABILITY_TTL_SECONDS = 600.0
_capability_cache: tuple[float, dict[str, Any]] | None = None


def _in_worker_thread(func, *args):
    """Run a blocking call off the event loop.

    Both probes below are synchronous and refuse to run inside a running
    asyncio loop -- Playwright's sync API raises outright, and the LLM client's
    `list_models` is a coroutine that has to be driven by one. Since these
    endpoints are `async def`, both need their own thread.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(func, *args).result()


def _capabilities(*, refresh: bool = False) -> dict[str, Any]:
    global _capability_cache
    now = time.monotonic()
    if not refresh and _capability_cache and now - _capability_cache[0] < _CAPABILITY_TTL_SECONDS:
        return _capability_cache[1]

    from backend.pipeline.source_downloader import check_playwright_runtime

    python_ok, browser_ok, error = _in_worker_thread(check_playwright_runtime)
    result = {
        "playwright_python_available": python_ok,
        "playwright_browser_available": browser_ok,
        "error": error,
    }
    _capability_cache = (now, result)
    return result


def _stale_agent_cli(repo_path: Any) -> str:
    """Name of the installed `ra` if it differs from the shipped one, else ""."""
    from backend.storage.agent_skills import AGENTS_DIR_NAME, BIN_DIR_NAME, bundled_agent_cli_dir

    source_dir = bundled_agent_cli_dir()
    if not source_dir.is_dir():
        return ""
    target_dir = Path(repo_path) / AGENTS_DIR_NAME / BIN_DIR_NAME
    for source in sorted(source_dir.iterdir()):
        if not source.is_file() or source.name.startswith("."):
            continue
        target = target_dir / source.name
        try:
            if target.read_bytes() != source.read_bytes():
                return f".agents/{BIN_DIR_NAME}/{source.name}"
        except OSError:
            return f".agents/{BIN_DIR_NAME}/{source.name}"
    return ""


def run_preflight(
    service: Any,
    settings: Any,
    *,
    refresh_capabilities: bool = False,
) -> Preflight:
    """Aggregate every readiness signal into one verdict."""
    from backend.pipeline.source_downloader import llm_backend_ready_for_chat

    from . import WORKFLOW_CONTRACT_VERSION

    checks: list[Check] = []

    def add(check_id, ok, severity, detail, remedy=""):
        checks.append(Check(id=check_id, ok=ok, severity=severity, detail=detail, remedy=remedy))

    # --- repository -------------------------------------------------------
    attached = bool(getattr(service, "is_attached", False))
    add(
        "repository_attached",
        attached,
        "blocker",
        f"Repository: {service.path}" if attached else "No repository is attached.",
        "" if attached else "Open a repository in the app, or POST /api/repository/attach.",
    )

    status = None
    if attached:
        try:
            status = service.get_status()
        except Exception as exc:  # pragma: no cover - defensive
            add("repository_readable", False, "blocker", f"Could not read status: {exc}")

    if status is not None:
        busy = str(status.download_state or "") in {"running", "cancelling"}
        add(
            "repository_idle",
            not busy,
            "blocker",
            f"A job is {status.download_state}." if busy else "No job is running.",
            "Wait for it to finish, or cancel it." if busy else "",
        )
        missing = int(getattr(status.health, "missing_files", 0) or 0)
        add(
            "repository_health",
            missing == 0,
            "warning",
            f"{missing} artifact file(s) referenced but missing." if missing else "No missing artifacts.",
            "ra where, then re-fetch or attach the affected sources." if missing else "",
        )
        add(
            "sources_present",
            status.total_sources > 0,
            "warning",
            f"{status.total_sources} source(s), {status.queued_count} queued.",
            # No `--apply` here on purpose: a remedy is rendered outside `next`,
            # so it carries no gate, and pointing straight at `--apply` skips
            # the dry run the user is supposed to see first.
            "ra plan-sheet <file> then ra create-sources" if not status.total_sources else "",
        )

    # --- LLM --------------------------------------------------------------
    llm = getattr(settings, "llm_backend", None)
    use_llm = bool(getattr(settings, "use_llm", False))
    add(
        "llm_enabled",
        use_llm,
        "blocker_for_columns",
        "LLM is enabled." if use_llm else "LLM is switched off in Settings.",
        "" if use_llm else "Turn on 'use LLM' in the app's Settings page.",
    )

    shape_ok = bool(llm) and llm_backend_ready_for_chat(llm)
    add(
        "llm_config_shape",
        shape_ok,
        "blocker_for_columns",
        (
            f"Backend {llm.kind} at {llm.base_url}, model {llm.model!r}."
            if shape_ok
            else "The LLM backend is missing a base URL, a model, or a supported kind."
        ),
        "" if shape_ok else "Set the backend kind, base URL and model in Settings.",
    )

    if shape_ok:
        models, error = _list_models(llm)
        add(
            "llm_reachable",
            not error,
            "blocker_for_columns",
            f"{llm.base_url} answered with {len(models)} model(s)." if not error else error,
            "" if not error else f"Check the backend is running and reachable at {llm.base_url}.",
        )
        if not error:
            # Whether the model is *advertised* is only a hint: plenty of
            # servers expose one generic name and route whatever they are
            # given. Blocking a working setup on that would be worse than not
            # checking, so the advertised list is a warning and an actual
            # one-token completion is the verdict.
            listed = _model_present(llm.model, models)
            if not listed:
                add(
                    "llm_model_advertised",
                    False,
                    "warning",
                    f"Model {llm.model!r} is not in the backend's list "
                    f"({', '.join(models[:8]) or 'empty'}), but that does not always matter.",
                    "If the check below fails, set the model to one of those.",
                )

            answered, reply_error = _completion_probe(llm)
            add(
                "llm_responds",
                answered,
                "blocker_for_columns",
                (
                    f"Model {llm.model!r} answered a test prompt."
                    if answered
                    else f"Model {llm.model!r} did not answer: {reply_error}"
                ),
                ""
                if answered
                else (
                    "Set the model in Settings to one the backend offers: "
                    + (", ".join(models[:8]) or "(the backend listed none)")
                ),
            )

    # --- the tool the agent is holding ------------------------------------
    #
    # Provenance deliberately leaves an edited copy alone, but it reports that
    # only at sync time -- so a copy that diverged once stays stale for good and
    # nothing ever says so. The contract version does not catch it either: a
    # stale `ra` speaking the same contract just quietly lacks commands and
    # remedies added since.
    if attached:
        stale_cli = _stale_agent_cli(service.path)
        add(
            "agent_cli_current",
            not stale_cli,
            "warning",
            (
                f"{stale_cli} differs from the version this app ships."
                if stale_cli
                else "The bundled `ra` matches the one this app ships."
            ),
            (
                "It was edited, so it is left alone. Delete it and re-attach the "
                "repository to get the current one."
                if stale_cli
                else ""
            ),
        )

    # --- runtime ----------------------------------------------------------
    caps = _capabilities(refresh=refresh_capabilities)
    add(
        "browser_available",
        bool(caps["playwright_browser_available"]),
        "warning",
        (
            "Headless browser available for pages that need rendering."
            if caps["playwright_browser_available"]
            else f"Headless browser unavailable: {caps['error'] or 'unknown'}. "
            "Sites that need rendering will fail."
        ),
        ""
        if caps["playwright_browser_available"]
        else (
            ".venv/bin/python -m playwright install chromium"
            if "not installed" in (caps["error"] or "") or "import" in (caps["error"] or "")
            else "The browser is installed but failed to launch; the detail above says why."
        ),
    )

    report = Preflight(checks=checks, contract_version=WORKFLOW_CONTRACT_VERSION)
    blockers = [c for c in checks if not c.ok and c.severity == "blocker"]
    column_blockers = [c for c in checks if not c.ok and c.severity == "blocker_for_columns"]

    report.ok_to_fetch = not blockers
    report.ok_to_run_columns = not blockers and not column_blockers
    report.ok = report.ok_to_run_columns

    if blockers:
        report.summary = f"Not ready: {blockers[0].detail}"
        report.next = [na(c.remedy) for c in blockers if c.remedy][:3]
    elif column_blockers:
        report.summary = (
            "Ready to fetch, but not to run columns: " + column_blockers[0].detail
        )
        report.next = [na(c.remedy) for c in column_blockers if c.remedy][:3] or nas("ra where")
    else:
        warnings = [c for c in checks if not c.ok and c.severity == "warning"]
        report.summary = "Ready." + (f" {len(warnings)} warning(s)." if warnings else "")
        report.next = nas("ra where")
    return report


def _list_models(llm: Any) -> tuple[list[str], str]:
    """Ask the configured backend what it offers.

    The only probe in the app that actually touches the network -- everything
    else checks the config's shape and would happily pass against a machine that
    is switched off.
    """

    def probe() -> tuple[list[str], str]:
        import asyncio

        from backend.llm.client import UnifiedLLMClient

        client = UnifiedLLMClient(llm)
        try:
            # `list_models` is a coroutine, so it needs a loop of its own here.
            return list(asyncio.run(client.list_models()) or []), ""
        finally:
            close = getattr(client, "sync_close", None)
            if callable(close):
                close()

    try:
        return _in_worker_thread(probe)
    except Exception as exc:
        return [], f"Could not reach the backend: {type(exc).__name__}: {exc}"


def _completion_probe(llm: Any) -> tuple[bool, str]:
    """Send a one-token prompt. The only check that proves columns can run.

    Worth the single cheap call: everything upstream of this can pass while the
    configured model still refuses every request, which is exactly the failure
    that wastes a several-hundred-call job.
    """

    def probe() -> tuple[bool, str]:
        from backend.llm.client import UnifiedLLMClient

        client = UnifiedLLMClient(llm)
        try:
            reply = client.sync_chat_completion(
                system_prompt="Reply with the single word OK.",
                user_prompt="Reply with the single word OK.",
            )
            text = str(reply or "").strip()
            return bool(text), "" if text else "the model returned an empty response"
        finally:
            close = getattr(client, "sync_close", None)
            if callable(close):
                close()

    try:
        return _in_worker_thread(probe)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _model_present(configured: str, available: list[str]) -> bool:
    wanted = (configured or "").strip()
    if not wanted or not available:
        return False
    lowered = [m.lower() for m in available]
    if wanted.lower() in lowered:
        return True
    # Ollama reports `name:tag`; a config naming just the model should match.
    base = wanted.split(":", 1)[0].lower()
    return any(m.split(":", 1)[0] == base for m in lowered)
