#!/usr/bin/env bash
# Preflight checks for the local lakehouse scaffold (Podman-first, local-only).
# Exit codes: 0 pass, 1 fail (with >>> FIX: guidance), 2 warnings only.
set -euo pipefail

FAIL=0
WARN=0

usage() {
  cat <<'EOF'
Usage: scripts/preflight.sh [--help]

Checks required to run the local lakehouse stack:
  - Podman >= 4 (or a Docker-compatible socket)
  - Compose provider: 'docker compose' via DOCKER_HOST -> Podman socket (primary),
    or 'podman-compose' fallback (health-gating caveat, see docs/runbook.md)
  - Free/total RAM (minimal profile needs >= 6 GB, full profile needs >= 8 GB)
  - CPU count (informational, warns below 2)
  - Required ports free: 8181 8182 9000 9001 8080 8088 8081

Exit codes: 0 pass, 1 fail, 2 warnings only.
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

fail() { echo "FAIL: $1"; echo ">>> FIX: $2"; FAIL=$((FAIL + 1)); }
warn() { echo "WARN: $1"; echo ">>> FIX: $2"; WARN=$((WARN + 1)); }
ok() { echo "OK: $1"; }

# --- 1. Container runtime: Podman >= 4 or a Docker-compatible socket ---
PODMAN_OK=0
if command -v podman >/dev/null 2>&1; then
  PODMAN_VER="$(podman --version 2>/dev/null || echo 'podman version unknown')"
  echo "Found: ${PODMAN_VER}"
  MAJOR="$(echo "${PODMAN_VER}" | grep -oE '[0-9]+' | head -n 1 || echo 0)"
  if [[ "${MAJOR}" -ge 4 ]]; then
    ok "Podman >= 4 (${PODMAN_VER})"
    PODMAN_OK=1
  else
    fail "Podman version < 4 (${PODMAN_VER})" "Upgrade Podman to >= 4 (see docs/runbook.md prerequisites)."
  fi
else
  echo "INFO: 'podman' not found on PATH."
fi

SOCKET_OK=0
for sock in "${DOCKER_HOST:-}" "${XDG_RUNTIME_DIR:-}/podman/podman.sock" /run/podman/podman.sock /var/run/docker.sock; do
  sock_path="${sock#unix://}"
  [[ -z "${sock_path}" ]] && continue
  if [[ -S "${sock_path}" ]]; then
    ok "Container socket present (${sock_path})"
    SOCKET_OK=1
    break
  fi
done

if [[ "${PODMAN_OK}" -eq 0 && "${SOCKET_OK}" -eq 0 ]]; then
  fail "No Podman >= 4 and no container socket found" "Install Podman >= 4 and enable the socket: systemctl --user enable --now podman.socket (see docs/runbook.md)."
fi

# --- 2. Compose provider: docker compose + DOCKER_HOST (primary) or podman-compose (fallback) ---
COMPOSE_PRIMARY_OK=0
COMPOSE_FALLBACK_OK=0
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  if [[ -n "${DOCKER_HOST:-}" ]]; then
    ok "Primary runner available (docker compose + DOCKER_HOST=${DOCKER_HOST})"
    COMPOSE_PRIMARY_OK=1
  else
    warn "docker compose works but DOCKER_HOST is unset (Podman socket not selected)" "Export DOCKER_HOST=unix://\$XDG_RUNTIME_DIR/podman/podman.sock so 'docker compose' drives Podman (see docs/runbook.md)."
  fi
fi
if command -v podman-compose >/dev/null 2>&1; then
  COMPOSE_FALLBACK_OK=1
  if [[ "${COMPOSE_PRIMARY_OK}" -eq 1 ]]; then
    ok "Fallback runner also available (podman-compose)"
  else
    warn "Only podman-compose found (no primary 'docker compose + DOCKER_HOST' runner)" "Prefer the primary runner for health-gated startup; podman-compose ignores depends_on health conditions (see docs/runbook.md)."
  fi
fi
if [[ "${COMPOSE_PRIMARY_OK}" -eq 0 && "${COMPOSE_FALLBACK_OK}" -eq 0 ]]; then
  fail "No compose provider found" "Set DOCKER_HOST to the Podman socket and install 'docker compose', or install podman-compose as fallback (see docs/runbook.md)."
fi

# --- 3. RAM: minimal needs >= 6 GB, full needs >= 8 GB ---
TOTAL_GB=0
if [[ -r /proc/meminfo ]]; then
  MEM_KB="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  TOTAL_GB=$((MEM_KB / 1024 / 1024))
  echo "INFO: Total RAM ${TOTAL_GB} GB"
  if [[ "${TOTAL_GB}" -lt 6 ]]; then
    fail "Only ${TOTAL_GB} GB RAM (minimal profile needs >= 6 GB)" "Free memory or add RAM/swap; run the minimal profile only."
  elif [[ "${TOTAL_GB}" -lt 8 ]]; then
    warn "Only ${TOTAL_GB} GB RAM (full profile needs >= 8 GB)" "Use the default minimal profile; add RAM before opting into --profile full."
  else
    ok "RAM ${TOTAL_GB} GB (>= 8 GB for full profile)"
  fi
else
  warn "Could not read total RAM (/proc/meminfo missing)" "Ensure the host has >= 6 GB for minimal, >= 8 GB for full."
fi

# --- 4. CPU count (informational) ---
if command -v nproc >/dev/null 2>&1; then
  CPUS="$(nproc)"
  echo "INFO: CPUs ${CPUS}"
  if [[ "${CPUS}" -lt 2 ]]; then
    warn "Only ${CPUS} CPU(s) detected" "2+ CPUs recommended; the stack will be slow but may still start."
  else
    ok "CPU count ${CPUS}"
  fi
else
  warn "Could not determine CPU count (nproc missing)" "2+ CPUs recommended for the stack."
fi

# --- 5. Required ports free ---
port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltnH 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${port}$"
  else
    (exec 3<>"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1
  fi
}

for port in 8181 8182 9000 9001 8080 8088 8081; do
  if port_in_use "${port}"; then
    fail "Port ${port} is already in use" "Stop the process holding port ${port} (e.g. ss -ltnp | grep ${port}) or remap the published port."
  else
    ok "Port ${port} free"
  fi
done

# --- Verdict ---
if [[ "${FAIL}" -gt 0 ]]; then
  echo "PREFLIGHT FAIL: ${FAIL} failure(s), ${WARN} warning(s)."
  exit 1
elif [[ "${WARN}" -gt 0 ]]; then
  echo "PREFLIGHT WARN: ${WARN} warning(s), no failures."
  exit 2
else
  echo "PREFLIGHT OK: host meets minimal-profile requirements."
  exit 0
fi
