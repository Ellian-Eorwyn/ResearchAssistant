#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"

echo "=== ResearchAssistant Update ==="

# Pull latest code
echo "[update] Pulling latest changes..."
cd "${ROOT_DIR}"
git pull --ff-only

# Python deps
if [[ -x "${VENV_PYTHON}" ]]; then
  echo "[update] Updating Python dependencies..."
  "${VENV_PYTHON}" -m pip install -r "${ROOT_DIR}/requirements.txt"
  echo "[update] Updating Playwright..."
  "${VENV_PYTHON}" -m playwright install chromium
else
  echo "[update] Warning: .venv not found. Run install.sh first." >&2
  exit 1
fi

# Frontend rebuild
if command -v npm >/dev/null 2>&1; then
  echo "[update] Updating frontend dependencies..."
  (cd "${ROOT_DIR}/frontend" && npm install)
  echo "[update] Rebuilding frontend..."
  (cd "${ROOT_DIR}/frontend" && npm run build)
else
  echo "[update] Error: npm not found, cannot rebuild frontend." >&2
  exit 1
fi

echo ""
echo "=== Update complete ==="
echo "Restart the server to apply changes."
