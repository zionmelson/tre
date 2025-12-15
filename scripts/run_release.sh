#!/usr/bin/env bash
set -euo pipefail

# Resolve repository root so the script can be run from any directory.
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

# macOS SDK path is required by the Homebrew clang/lld toolchain.
DEFAULT_SDKROOT="$(/usr/bin/xcrun --sdk macosx --show-sdk-path)"
export SDKROOT="${SDKROOT:-$DEFAULT_SDKROOT}"

# Match the LLVM toolchain installed via Homebrew.
export LIBLTO_PATH="${LIBLTO_PATH:-/opt/homebrew/opt/llvm/lib/libLTO.dylib}"

# Ensure the release build always links with clang + lld using the newer LLVM bits.
REQUIRED_RUSTFLAGS=(
  "-Clinker=/opt/homebrew/opt/llvm/bin/clang"
  "-Clink-arg=-fuse-ld=/opt/homebrew/opt/lld/bin/ld64.lld"
  "-Clink-arg=-isysroot"
  "-Clink-arg=${SDKROOT}"
  "-Clink-arg=-Wl,-syslibroot,${SDKROOT}"
)

if [[ -n "${RUSTFLAGS:-}" ]]; then
  export RUSTFLAGS="${RUSTFLAGS} ${REQUIRED_RUSTFLAGS[*]}"
else
  export RUSTFLAGS="${REQUIRED_RUSTFLAGS[*]}"
fi

export RUST_LOG="${RUST_LOG:-info}"

DEFAULT_METRICS_BIND="127.0.0.1:9899"
METRICS_SNAPSHOT_DIR="${TRE_METRICS_SNAPSHOT_DIR:-/tmp}"
METRICS_SNAPSHOT_BASENAME="${TRE_METRICS_SNAPSHOT_BASENAME:-tre-metrics}"
METRICS_SNAPSHOT_ENABLED="${TRE_METRICS_SNAPSHOT_ENABLED:-1}"
METRICS_SNAPSHOT_TIMEOUT="${TRE_METRICS_SNAPSHOT_TIMEOUT:-5}"
METRICS_SNAPSHOT_DONE=0
METRICS_BIND_VALUE="$DEFAULT_METRICS_BIND"
METRICS_SNAPSHOT_URL=""
WATCHDOG_TRIGGER_FILE=""
WATCHDOG_SIGNAL=""

dump_metrics_snapshot() {
  if [[ "${METRICS_SNAPSHOT_ENABLED}" != "1" ]]; then
    return
  fi
  if [[ -z "${METRICS_SNAPSHOT_URL}" ]]; then
    return
  fi
  if [[ "${METRICS_SNAPSHOT_DONE}" == "1" ]]; then
    return
  fi
  if ! command -v curl >/dev/null 2>&1; then
    return
  fi

  local timestamp path
  timestamp="$(date +%Y%m%d-%H%M%S)"
  path="${METRICS_SNAPSHOT_DIR}/${METRICS_SNAPSHOT_BASENAME}-${timestamp}.prom"
  if mkdir -p "${METRICS_SNAPSHOT_DIR}" \
    && curl --silent --show-error --fail --max-time "${METRICS_SNAPSHOT_TIMEOUT}" \
      "${METRICS_SNAPSHOT_URL}" -o "${path}"; then
    echo "[run_release] Metrics snapshot saved to ${path}"
    METRICS_SNAPSHOT_DONE=1
  else
    echo "[run_release] Warning: failed to scrape metrics from ${METRICS_SNAPSHOT_URL}" >&2
    rm -f "${path}"
  fi
}

kill_stale_bot() {
  if ! command -v pgrep >/dev/null 2>&1; then
    return
  fi

  local -a stale_pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && stale_pids+=("$pid")
  done < <(pgrep -f "[t]re-arb-bot" || true)

  if [[ ${#stale_pids[@]} -eq 0 ]]; then
    return
  fi

  echo "[run_release] Killing stale tre-arb-bot PIDs: ${stale_pids[*]}"
  kill -TERM "${stale_pids[@]}" 2>/dev/null || true
  sleep 1
  for pid in "${stale_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
}

signal_process_group() {
  local pid="$1"
  local sig="$2"
  if [[ -z "$pid" ]]; then
    return
  fi

  local pgid=""
  if command -v ps >/dev/null 2>&1; then
    pgid="$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d '[:space:]' || true)"
  fi

  if [[ -n "$pgid" ]]; then
    kill "-${sig}" "-${pgid}" 2>/dev/null || true
  else
    kill "-${sig}" "$pid" 2>/dev/null || true
  fi
}

cleanup() {
  dump_metrics_snapshot
  if [[ -n "${WATCHDOG_PID:-}" ]]; then
    kill "${WATCHDOG_PID}" 2>/dev/null || true
    unset WATCHDOG_PID
  fi
  if [[ -n "${CARGO_PID:-}" ]]; then
    signal_process_group "${CARGO_PID}" TERM
    unset CARGO_PID
  fi
  if [[ -n "${WATCHDOG_TRIGGER_FILE:-}" ]]; then
    rm -f "${WATCHDOG_TRIGGER_FILE}"
    unset WATCHDOG_TRIGGER_FILE
  fi
  kill_stale_bot
}

trap cleanup EXIT INT TERM

cd "${REPO_ROOT}"
kill_stale_bot

# Optional timer: pass --duration <seconds> or export RUN_DURATION_SECONDS.
RUN_DURATION_SECONDS="${RUN_DURATION_SECONDS:-0}"
SCRIPT_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      shift
      RUN_DURATION_SECONDS="${1:-0}"
      shift
      continue
      ;;
    --duration=*)
      RUN_DURATION_SECONDS="${1#*=}"
      shift
      continue
      ;;
    --)
      SCRIPT_ARGS+=("$@")
      break
      ;;
    *)
      SCRIPT_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ${#SCRIPT_ARGS[@]} -gt 0 ]]; then
  for ((i = 0; i < ${#SCRIPT_ARGS[@]}; i++)); do
    arg="${SCRIPT_ARGS[$i]}"
    case "$arg" in
      --metrics-bind)
        next=$((i + 1))
        if [[ $next -lt ${#SCRIPT_ARGS[@]} ]]; then
          METRICS_BIND_VALUE="${SCRIPT_ARGS[$next]}"
        fi
        ;;
      --metrics-bind=*)
        METRICS_BIND_VALUE="${arg#*=}"
        ;;
    esac
  done
fi

set -- "${SCRIPT_ARGS[@]}"

METRICS_SNAPSHOT_URL="${TRE_METRICS_SNAPSHOT_URL:-http://${METRICS_BIND_VALUE}/metrics}"

# Default to the repo's config if no CLI arguments are supplied.
if [[ $# -eq 0 ]]; then
  set -- --config config.json
fi

start_cargo() {
  cargo run --release --bin tre-arb-bot -- "$@"
}

if [[ -n "$RUN_DURATION_SECONDS" && "$RUN_DURATION_SECONDS" -gt 0 ]]; then
  echo "[run_release] Limiting run to ${RUN_DURATION_SECONDS}s"
  WATCHDOG_TRIGGER_FILE="$(mktemp "${TMPDIR:-/tmp}/tre-watchdog.XXXXXX")"
  start_cargo "$@" &
  CARGO_PID=$!

  timeout_watchdog() {
    local child_pid="$1"
    local duration="$2"
    sleep "$duration"
    if kill -0 "$child_pid" 2>/dev/null; then
      dump_metrics_snapshot
      echo "[run_release] Time limit reached; sending SIGINT to cargo (pid ${child_pid})."
      if [[ -n "${WATCHDOG_TRIGGER_FILE:-}" ]]; then
        echo "SIGINT" >"${WATCHDOG_TRIGGER_FILE}"
      fi
      signal_process_group "$child_pid" INT
      sleep 2
      if kill -0 "$child_pid" 2>/dev/null; then
        echo "[run_release] Cargo still running; sending SIGTERM."
        if [[ -n "${WATCHDOG_TRIGGER_FILE:-}" ]]; then
          echo "SIGTERM" >"${WATCHDOG_TRIGGER_FILE}"
        fi
        signal_process_group "$child_pid" TERM
      fi
    fi
  }

  timeout_watchdog "$CARGO_PID" "$RUN_DURATION_SECONDS" &
  WATCHDOG_PID=$!

  wait "$CARGO_PID"
  STATUS=$?
  unset CARGO_PID
  if [[ -n "${WATCHDOG_TRIGGER_FILE:-}" && -f "${WATCHDOG_TRIGGER_FILE}" ]]; then
    WATCHDOG_SIGNAL="$(cat "${WATCHDOG_TRIGGER_FILE}" 2>/dev/null || true)"
  else
    WATCHDOG_SIGNAL=""
  fi
  if [[ -n "${WATCHDOG_TRIGGER_FILE:-}" ]]; then
    rm -f "${WATCHDOG_TRIGGER_FILE}"
    WATCHDOG_TRIGGER_FILE=""
  fi
  if [[ -n "${WATCHDOG_PID:-}" ]]; then
    kill "${WATCHDOG_PID}" 2>/dev/null || true
    unset WATCHDOG_PID
  fi
  kill_stale_bot
  if [[ "$STATUS" -eq 130 && "${WATCHDOG_SIGNAL}" == "SIGINT" ]]; then
    echo "[run_release] Watchdog-delivered SIGINT resulted in clean shutdown; treating as success."
    STATUS=0
  fi
  WATCHDOG_SIGNAL=""
  exit "$STATUS"
else
  start_cargo "$@" &
  CARGO_PID=$!
  wait "$CARGO_PID"
  STATUS=$?
  unset CARGO_PID
  kill_stale_bot
  exit "$STATUS"
fi
