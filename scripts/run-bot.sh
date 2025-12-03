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

echo "[run-bot] Hydration workers: template cache, ALT manager, flash planner"
echo "[run-bot] Fast execution workers: planner, signer pool, dual senders"

cargo run --bin tre-arb-bot -- "${args[@]}" >> output.txt 
