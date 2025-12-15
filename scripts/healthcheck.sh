#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
HEALTH_DIR="${TRE_HEALTHCHECK_DIR:-${REPO_ROOT}/healthchecks}"
mkdir -p "${HEALTH_DIR}"

timestamp="$(date +%Y%m%d-%H%M%S)"
log_path="${HEALTH_DIR}/health-${timestamp}.log"
metrics_basename="health-metrics"
run_duration="${TRE_HEALTHCHECK_DURATION:-360}"
guard_grace="${TRE_HEALTHCHECK_GUARD_GRACE:-10}"
metrics_bind="${TRE_HEALTHCHECK_METRICS_BIND:-127.0.0.1:9899}"
profit_floor="${TRE_HEALTHCHECK_PROFIT_FLOOR:-1}"

echo "[healthcheck] Starting ${run_duration}s run (timestamp ${timestamp}, profit floor ${profit_floor})"

cleanup() {
  local code=$?
  if [[ -n "${guard_pid:-}" ]]; then
    kill "$guard_pid" 2>/dev/null || true
  fi
  if [[ -n "${run_pid:-}" ]]; then
    kill "$run_pid" 2>/dev/null || true
  fi
  if [[ -n "${tee_pid:-}" ]]; then
    kill "$tee_pid" 2>/dev/null || true
  fi
  rm -f "${fifo_path:-}"
  exit $code
}

trap cleanup INT TERM

fifo_path="$(mktemp -u)"
mkfifo "$fifo_path"
tee "$log_path" <"$fifo_path" &
tee_pid=$!

RUN_DURATION_SECONDS="$run_duration" \
TRE_METRICS_SNAPSHOT_DIR="${HEALTH_DIR}" \
TRE_METRICS_SNAPSHOT_BASENAME="${metrics_basename}" \
RUST_LOG="${TRE_HEALTHCHECK_RUST_LOG:-jito::auth=debug,arbgraph=debug,worker::ingest::pools=debug,worker::planner=trace,worker::executor=debug}" \
TRE_MIN_PROFIT_LAMPORTS_OVERRIDE="${profit_floor}" \
./scripts/run_release.sh --config ./config.json --metrics-bind "${metrics_bind}" \
  >"$fifo_path" 2>&1 &
run_pid=$!

guard_pid=
(
  sleep "$((run_duration + guard_grace))"
  if kill -0 "$run_pid" 2>/dev/null; then
    echo "[healthcheck] Guard: duration exceeded, sending SIGINT to run (pid ${run_pid})." | tee -a "$log_path"
    kill -INT "$run_pid" 2>/dev/null || true
    sleep 5
    if kill -0 "$run_pid" 2>/dev/null; then
      echo "[healthcheck] Guard: run still alive, sending SIGTERM." | tee -a "$log_path"
      kill -TERM "$run_pid" 2>/dev/null || true
    fi
  fi
) &
guard_pid=$!

set +e
wait "$run_pid"
run_status=$?
set -e
if kill -0 "$guard_pid" 2>/dev/null; then
  kill "$guard_pid" 2>/dev/null || true
fi
set +e
wait "$tee_pid"
set -e
rm -f "$fifo_path"

metrics_file=""
if compgen -G "${HEALTH_DIR}/${metrics_basename}-*.prom" > /dev/null; then
  metrics_file="$(ls -t "${HEALTH_DIR}/${metrics_basename}-"*.prom | head -n1)"
fi

warn_count=$(grep -c " WARN " "${log_path}" || true)
error_count=$(grep -c " ERROR " "${log_path}" || true)

read_metric_sum() {
  local metric="$1"
  if [[ -z "${metrics_file}" || ! -f "${metrics_file}" ]]; then
    echo "n/a"
    return
  fi
  local lines
  lines=$(grep "^${metric}" "${metrics_file}" || true)
  if [[ -z "${lines}" ]]; then
    echo "0"
  else
    echo "${lines}" | awk '{sum+=$NF} END {if (sum=="") sum=0; print sum}'
  fi
}

planner_plans=$(read_metric_sum "planner_plans_total")
executor_plans=$(read_metric_sum "executor_plans_total")
trade_ingest=$(read_metric_sum "planner_trade_ingest_total")
trade_drops=$(read_metric_sum "planner_trade_drop_total")

cat <<EOF
[healthcheck] Run complete
  timestamp           : ${timestamp}
  log_file            : ${log_path}
  metrics_file        : ${metrics_file:-n/a}
  run_exit_status     : ${run_status}
  warn_lines          : ${warn_count}
  error_lines         : ${error_count}
  planner_plans_total : ${planner_plans}
  executor_plans_total: ${executor_plans}
  trades_ingested     : ${trade_ingest}
  trades_dropped      : ${trade_drops}
EOF

if [[ -n "${metrics_file}" ]]; then
  echo "[healthcheck] Detailed trade drop breakdown:"
  grep '^planner_trade_drop_total' "${metrics_file}" || echo "  (no drops recorded)"
  echo "[healthcheck] Planner trade ingest breakdown:"
  grep '^planner_trade_ingest_total' "${metrics_file}" || echo "  (no trades ingested)"
fi

if [[ ${run_status} -ne 0 ]]; then
  echo "[healthcheck] FAIL: run_release exited with ${run_status}" >&2
  exit ${run_status}
fi

echo "[healthcheck] PASS"
