#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"
HOST="${RA_HOST:-0.0.0.0}"
PORT="${RA_PORT:-7995}"
RUNTIME_DIR="${ROOT_DIR}/.run"
PID_FILE="${RUNTIME_DIR}/server.pid"
LOG_FILE="${RUNTIME_DIR}/server.log"

echo "=== ResearchAssistant Update ==="

process_command() {
  ps -p "$1" -o command= 2>/dev/null | sed 's/^[[:space:]]*//'
}

process_cwd() {
  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi
  lsof -a -p "$1" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p' | head -n 1
}

is_researchassistant_server() {
  local pid="$1"
  local cmd cwd
  cmd="$(process_command "${pid}")"
  cwd="$(process_cwd "${pid}")"
  [[ -n "${cmd}" ]] || return 1
  case "${cmd}" in
    *run.py*|*uvicorn*)
      ;;
    *)
      return 1
      ;;
  esac
  [[ "${cmd}" == *"${ROOT_DIR}"* || "${cwd}" == "${ROOT_DIR}" || "${cmd}" == *" run.py"* ]]
}

find_running_server_pid() {
  local pid cmd
  if [[ -f "${PID_FILE}" ]]; then
    pid="$(tr -d '[:space:]' < "${PID_FILE}")"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null && is_researchassistant_server "${pid}"; then
      echo "${pid}"
      return 0
    fi
    rm -f "${PID_FILE}"
  fi

  if ! command -v lsof >/dev/null 2>&1; then
    return 1
  fi

  while IFS= read -r pid; do
    [[ -n "${pid}" ]] || continue
    if is_researchassistant_server "${pid}"; then
      echo "${pid}"
      return 0
    fi
  done < <(lsof -tiTCP:"${PORT}" -sTCP:LISTEN 2>/dev/null || true)

  cmd=""
  pid="$(lsof -tiTCP:"${PORT}" -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
  if [[ -n "${pid}" ]]; then
    cmd="$(process_command "${pid}")"
    echo "[update] Error: port ${PORT} is already in use by a non-ResearchAssistant process: ${cmd:-pid ${pid}}" >&2
    exit 1
  fi

  return 1
}

stop_server() {
  local pid="$1"
  echo "[update] Stopping running server (pid ${pid})..."
  kill "${pid}"
  for _ in {1..20}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${PID_FILE}"
      return 0
    fi
    sleep 0.5
  done

  echo "[update] Server did not stop within 10 seconds; forcing shutdown..."
  kill -9 "${pid}" 2>/dev/null || true
  sleep 1
  if kill -0 "${pid}" 2>/dev/null; then
    echo "[update] Error: failed to stop server process ${pid}." >&2
    exit 1
  fi
  rm -f "${PID_FILE}"
}

start_server() {
  local pid
  mkdir -p "${RUNTIME_DIR}"

  echo "[update] Starting server on ${HOST}:${PORT}..."
  (
    cd "${ROOT_DIR}"
    RA_HOST="${HOST}" \
    RA_PORT="${PORT}" \
    RA_OPEN_BROWSER=0 \
      nohup "${VENV_PYTHON}" "${ROOT_DIR}/run.py" >>"${LOG_FILE}" 2>&1 < /dev/null &
    echo "$!" > "${PID_FILE}"
  )

  pid="$(tr -d '[:space:]' < "${PID_FILE}")"
  for _ in {1..20}; do
    if kill -0 "${pid}" 2>/dev/null; then
      if ! command -v lsof >/dev/null 2>&1 || lsof -tiTCP:"${PORT}" -sTCP:LISTEN 2>/dev/null | grep -qx "${pid}"; then
        echo "[update] Server restarted successfully (pid ${pid})."
        echo "[update] Log file: ${LOG_FILE}"
        return 0
      fi
    fi
    sleep 0.5
  done

  echo "[update] Error: server failed to start. Recent log output:" >&2
  tail -n 40 "${LOG_FILE}" >&2 || true
  exit 1
}

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

if SERVER_PID="$(find_running_server_pid)"; then
  stop_server "${SERVER_PID}"
else
  echo "[update] No running server detected on port ${PORT}; starting a fresh instance."
fi

start_server

echo ""
echo "=== Update complete ==="
echo "Server is running at http://127.0.0.1:${PORT}"
