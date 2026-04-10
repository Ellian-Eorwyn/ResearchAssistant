#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"
PORT="${RA_PORT:-7995}"
RUNTIME_DIR="${ROOT_DIR}/.run"
PID_FILE="${RUNTIME_DIR}/server.pid"

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
    echo "[run] error: port ${PORT} is already in use by a non-ResearchAssistant process: ${cmd:-pid ${pid}}" >&2
    exit 1
  fi

  return 1
}

stop_server() {
  local pid="$1"
  echo "[run] stopping existing server on port ${PORT} (pid ${pid})..."
  kill "${pid}"
  for _ in {1..20}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      rm -f "${PID_FILE}"
      return 0
    fi
    sleep 0.5
  done

  echo "[run] existing server did not stop within 10 seconds; forcing shutdown..."
  kill -9 "${pid}" 2>/dev/null || true
  sleep 1
  if kill -0 "${pid}" 2>/dev/null; then
    echo "[run] error: failed to stop server process ${pid}." >&2
    exit 1
  fi
  rm -f "${PID_FILE}"
}

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

if SERVER_PID="$(find_running_server_pid)"; then
  stop_server "${SERVER_PID}"
fi

exec "${VENV_PYTHON}" run.py
