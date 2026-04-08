#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"
VENV_PYTHON="${VENV_DIR}/bin/python"

echo "=== ResearchAssistant Install ==="

# Check prerequisites
for cmd in python3 npm; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Error: $cmd is required but not found on PATH." >&2
    exit 1
  fi
done

# Python virtualenv
if [[ ! -x "${VENV_PYTHON}" ]]; then
  echo "[install] Creating Python virtualenv..."
  python3 -m venv "${VENV_DIR}"
else
  echo "[install] Using existing virtualenv at ${VENV_DIR}"
fi

echo "[install] Upgrading pip tooling..."
"${VENV_PYTHON}" -m pip install --upgrade pip setuptools wheel

echo "[install] Installing Python dependencies..."
"${VENV_PYTHON}" -m pip install -r "${ROOT_DIR}/requirements.txt"

echo "[install] Installing Playwright Chromium..."
"${VENV_PYTHON}" -m playwright install chromium

# Frontend
echo "[install] Installing frontend dependencies..."
(cd "${ROOT_DIR}/frontend" && npm install)

echo "[install] Building frontend..."
(cd "${ROOT_DIR}/frontend" && npm run build)

echo ""
echo "=== Install complete ==="
echo "Run with:  ${VENV_PYTHON} ${ROOT_DIR}/run.py"
echo "  or:      ${ROOT_DIR}/scripts/run_dev.sh"
