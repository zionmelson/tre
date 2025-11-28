#!/usr/bin/env zsh
set -euo pipefail

# Default the metrics port + log level for local development.
: "${PROMETHEUS_METRICS_ADDR:=127.0.0.1:9898}"
: "${RUST_LOG:=debug}"
export PROMETHEUS_METRICS_ADDR RUST_LOG

cargo run --bin tre-arb-bot -- "$@" >> output.txt 
