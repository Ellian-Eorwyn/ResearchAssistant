"""Workflow layer: the deterministic operations a small model drives.

Everything an agent needs to run the repository end to end lives behind this
module rather than in prose instructions. The rule that shaped it: a local model
should never have to write code, construct a request body, parse a spreadsheet,
poll a job, or work out which error is worth retrying. It runs a command and
reads the answer.

`WORKFLOW_CONTRACT_VERSION` is the handshake between the bundled `ra` CLI and
the running server. Bump it whenever a response shape the CLI reads changes; a
stale server or a stale CLI then says so on the next command instead of quietly
misbehaving.
"""

from __future__ import annotations

# 2: `next` entries became objects carrying their own `gate`, so a checkpoint
#    travels with the command instead of living only in a skill's prose.
WORKFLOW_CONTRACT_VERSION = 2

__all__ = ["WORKFLOW_CONTRACT_VERSION"]
