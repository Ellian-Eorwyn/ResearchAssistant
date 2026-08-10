"""No workflow route may block uvicorn's event loop.

The workflow service is synchronous throughout, and its `wait_seconds` paths
end in `workflow.runs.wait_for`, which polls with `time.sleep` for up to two
minutes. An `async def` route that calls it inline sleeps the one thread uvicorn
answers every request on, so the whole app -- not just the workflow API -- stops
responding. The browser shows a blank page while the server sits there listening
and healthy.

That is exactly what happened during a 106-source fetch: `ra fetch --wait`, which
the workflow skill tells agents to run, re-issued the poll back to back and the
UI stayed dead for the length of the download.

The rule this file enforces: a route's call into the service goes through
`_offload`, which hands it to a worker thread.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import unittest

MODULE = pathlib.Path(__file__).resolve().parents[1] / "routers" / "workflow.py"

# Service methods that reach disk, the network, a lock, or `wait_for`. Anything
# here must be offloaded. Pure in-memory helpers are deliberately absent --
# `sheet_to_params` only reshapes a plan that is already parsed.
BLOCKING_METHODS = frozenset(
    {
        "preflight",
        "orientation",
        "triage",
        "parse_sheet",
        "run_operation",
        "run_source_phases",
        "run_column",
        "watch",
        "attach_files",
    }
)


def _direct_service_calls(tree: ast.AST) -> set[str]:
    """Service methods invoked directly, rather than passed to `_offload`.

    A call like `_service(request).triage(phase=phase)` is a `Call` whose func is
    an `Attribute` on another `Call`. Passing the bound method to `_offload`
    instead leaves an `Attribute` that is never itself called, which is the
    shape this looks for.
    """
    direct: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        inner = func.value
        if (
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Name)
            and inner.func.id == "_service"
        ):
            direct.add(func.attr)
    return direct


class EventLoopSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tree = ast.parse(MODULE.read_text(encoding="utf-8"))

    def test_no_blocking_service_call_runs_on_the_event_loop(self) -> None:
        offending = _direct_service_calls(self.tree) & BLOCKING_METHODS

        self.assertEqual(
            offending,
            set(),
            "These are called inline from an async route and will freeze the whole "
            f"server while they run: {sorted(offending)}. Wrap them in `_offload`.",
        )

    def test_offload_actually_leaves_the_event_loop(self) -> None:
        from backend.routers.workflow import _offload

        source = inspect.getsource(_offload)
        self.assertIn("run_in_threadpool", source)

    def test_every_blocking_method_exists_on_the_service(self) -> None:
        # Stops the guard list silently rotting into a set of names that no
        # longer match anything, which would make this file pass by default.
        from backend.workflow.service import WorkflowService

        for name in BLOCKING_METHODS:
            self.assertTrue(
                hasattr(WorkflowService, name), f"WorkflowService has no {name!r}"
            )

    def test_the_wait_helper_still_sleeps(self) -> None:
        # The premise of all of the above. If `wait_for` ever stops blocking,
        # this file's reasoning needs revisiting rather than quietly passing.
        import backend.workflow.runs as runs

        self.assertIn("time.sleep", inspect.getsource(runs.wait_for))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
