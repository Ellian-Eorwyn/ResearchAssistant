#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_TEMPLATE="${ROOT_DIR}/systemd/researchassistant.service.template"
UNIT_NAME="researchassistant.service"
UNIT_DEST="/etc/systemd/system/${UNIT_NAME}"
DEFAULTS_FILE="/etc/default/researchassistant"
SERVICE_USER="${SUDO_USER:-${USER}}"
SERVICE_GROUP="$(id -gn "${SERVICE_USER}")"
VENV_PYTHON="${ROOT_DIR}/.venv/bin/python"

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "[service] error: run this script with sudo." >&2
    exit 1
  fi
}

validate_runtime() {
  if [[ ! -f "${UNIT_TEMPLATE}" ]]; then
    echo "[service] error: unit template missing: ${UNIT_TEMPLATE}" >&2
    exit 1
  fi
  if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "[service] error: runtime missing: ${VENV_PYTHON}" >&2
    echo "[service] run ./scripts/install.sh first." >&2
    exit 1
  fi
  if [[ ! -d "${ROOT_DIR}/frontend/dist" ]]; then
    echo "[service] error: frontend build missing: ${ROOT_DIR}/frontend/dist" >&2
    echo "[service] run ./scripts/install.sh first." >&2
    exit 1
  fi
}

write_unit() {
  sed \
    -e "s|__RA_USER__|${SERVICE_USER}|g" \
    -e "s|__RA_GROUP__|${SERVICE_GROUP}|g" \
    -e "s|__RA_ROOT__|${ROOT_DIR}|g" \
    -e "s|__RA_PYTHON__|${VENV_PYTHON}|g" \
    "${UNIT_TEMPLATE}" > "${UNIT_DEST}"
}

write_defaults() {
  if [[ -f "${DEFAULTS_FILE}" ]]; then
    return 0
  fi

  cat > "${DEFAULTS_FILE}" <<'EOF'
# Optional overrides for ResearchAssistant.
# Example:
# RA_HOST=127.0.0.1
# RA_PORT=7995
EOF
}

start_service() {
  systemctl daemon-reload
  systemctl enable "${UNIT_NAME}"
  systemctl restart "${UNIT_NAME}"
}

show_status() {
  systemctl --no-pager --full status "${UNIT_NAME}" || true
  systemctl is-enabled "${UNIT_NAME}"
}

require_root
validate_runtime
write_unit
write_defaults
start_service
show_status
