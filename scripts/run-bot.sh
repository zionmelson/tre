#!/usr/bin/env bash
set -euo pipefail

# Default the metrics port + log level for local development.
: "${PROMETHEUS_METRICS_ADDR:=127.0.0.1:9898}"
: "${RUST_LOG:=debug}"
export PROMETHEUS_METRICS_ADDR RUST_LOG

dry_run=0
args=()
for arg in "$@"; do
	if [[ "$arg" == "--dry-run" ]]; then
		dry_run=1
		continue
	fi
	args+=("$arg")
done

if [[ $dry_run -eq 1 ]]; then
	export TRE_DRY_RUN_MODE=1
	echo "[run-bot] Dry-run mode enabled; submissions disabled"
fi
if [[ -z "${TRE_CORECAST_ADDRESS:-}" || -z "${TRE_CORECAST_AUTHORIZATION:-}" ]]; then
	echo "[run-bot] CoreCast credentials will be read from config.json (set TRE_CORECAST_ADDRESS/TRE_CORECAST_AUTHORIZATION to override)"
else
	echo "[run-bot] Using CoreCast credentials from environment overrides"
fi

echo "[run-bot] Bootstrap phase: optional static RPC fetch via bootstrap.rpc_endpoints, then CoreCast streaming begins"
echo "[run-bot] Trading phase: CoreCast ingest, planner, signer pool, dual submitters"

# Mirror stdout/stderr to the console while still persisting logs for tailing
cargo run --bin tre-arb-bot -- "${args[@]}" 2>&1 | tee -a output.txt
