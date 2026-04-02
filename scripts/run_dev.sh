#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo "[run] .venv is missing; running bootstrap first"
  bash "${ROOT_DIR}/scripts/bootstrap_venv.sh" || {
    echo "[run] error: bootstrap failed." >&2
    exit 1
  }
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo "[run] error: ${VENV_PYTHON} not found after bootstrap." >&2
  exit 1
fi

cd "${ROOT_DIR}"

if ! command -v npm >/dev/null 2>&1; then
  echo "[run] error: npm is required to build the React frontend." >&2
  exit 1
fi

echo "[run] ensuring frontend dependencies"
(cd "${ROOT_DIR}/frontend" && npm install)

echo "[run] building frontend bundle"
(cd "${ROOT_DIR}/frontend" && npm run build)

exec "${VENV_PYTHON}" run.py
