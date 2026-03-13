#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"

echo "[bootstrap] project root: ${ROOT_DIR}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[bootstrap] error: python3 is not available on PATH." >&2
  exit 1
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo "[bootstrap] creating virtualenv at ${VENV_DIR}"
  python3 -m venv "${VENV_DIR}"
else
  echo "[bootstrap] using existing virtualenv at ${VENV_DIR}"
fi

echo "[bootstrap] upgrading pip tooling"
"${VENV_PYTHON}" -m pip install --upgrade pip setuptools wheel

echo "[bootstrap] installing project dependencies"
"${VENV_PYTHON}" -m pip install -r "${ROOT_DIR}/requirements.txt"

echo "[bootstrap] installing Playwright Chromium browser"
"${VENV_PYTHON}" -m playwright install chromium

echo "[bootstrap] setup complete"
echo "[bootstrap] next step: ./scripts/run_dev.sh"
