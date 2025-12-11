#!/usr/bin/env bash
set -euo pipefail

# Verbose variant of run-dry that captures enriched logs to a timestamped file
# and mirrors them to the terminal. The script also exports a more detailed
# RUST_LOG default so watcher/CoreCast/planner telemetry is preserved.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/run-dry-verbose-${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}"

DEFAULT_RUST_LOG="info,tre=debug,tre::watch=trace,tre::planner=trace,tre::corecast=trace"
export RUST_LOG="${RUST_LOG:-${DEFAULT_RUST_LOG}}"

# Surface the Prometheus endpoint so callers know where to scrape live metrics.
export TRE_PROMETHEUS_ENDPOINT="${TRE_PROMETHEUS_ENDPOINT:-http://127.0.0.1:9898/metrics}"

echo "[run-dry-verbose] writing log to ${LOG_FILE}"
echo "[run-dry-verbose] Prometheus metrics: ${TRE_PROMETHEUS_ENDPOINT}"

touch "${LOG_FILE}"
exec > >(tee -a "${LOG_FILE}") 2>&1

"${SCRIPT_DIR}/run-bot.sh" --dry-run "$@"
