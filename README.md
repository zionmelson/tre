# tre-arb-bot

Ultra-light Solana arbitrage executor that prioritizes latency, simplicity, and modular upgrades. The bot consumes CoreCast streams, transforms parsed pool events into cached swap/flash-loan instructions, and submits atomic bundles to the Jito block engine while keeping the critical path as small as possible. The runtime now uses a flat chain of single-producer/single-consumer rings with CPU-pinned stages for deterministic latency.

## Status

This repository currently provides:

- Cargo project scaffolding with Tokio/tonic/prost wiring
- Proto compilation for Bitquery CoreCast and Jito MEV endpoints
- A flat SPSC runtime with lock-free rings, slab allocators, and CPU-affine stages (`runtime.rs`, `core/`, `stages/`)
- A fully wired CoreCast → Strategy → Bundle pipeline with config management, IDL caching, instruction template hydration, flash-loan planning, and Prometheus-friendly telemetry counters
- Bundle lifecycle tracking that streams `SubscribeBundleResults`, optionally taps the state-auction websocket feed, and publishes per-bundle alerts/metrics for accepted, rejected, finalized, or expired bundles
- Documentation of the target architecture and deployment assumptions (see `docs/architecture.md`)

## Quick Start

```bash
# Install dependencies (protoc, openssl, mold recommended for link speed)
brew install protobuf mold

# Ensure the Linux target exists for cross-compilation from Apple Silicon
rustup target add x86_64-unknown-linux-gnu

# Build the binary (native)
cargo build

# Build for production target (M1 -> Linux)
cargo build --release --target x86_64-unknown-linux-gnu

# Run locally with debug logging + Prometheus metrics
./scripts/run-bot.sh

# Use Docker (requires config volume mounted at /config)
docker build -t tre-arb-bot .
docker run --rm \
  -e ARBITRAGE_CONFIG=/config/config.json \
  -e PROMETHEUS_METRICS_ADDR=0.0.0.0:9898 \
  -v $(pwd)/config.json:/config/config.json:ro \
  -v $(pwd)/alt_registry.json:/data/alt_registry.json \
  -p 9898:9898 \
  tre-arb-bot
```

Environment variables:

- `ARBITRAGE_CONFIG`: optional path override for `config.json` (defaults to repo root).
- `RUST_LOG`: set to control tracing verbosity (defaults to `warn,tre-arb-bot=info`).
- `TRE_ALT_REGISTRY`: optional path override for the persisted ALT registry snapshot. Defaults to `alt_registry.json` in the working directory and allows restarts to reuse existing lookup tables without rebuilding them.
- `TRE_CORECAST_ADDRESS` / `TRE_CORECAST_AUTHORIZATION`: override `corecast.address` or `corecast.authorization` without editing JSON. Place `$ENV{TRE_CORECAST_AUTHORIZATION}` inside `config.json` or `config.template.json` to reference the variable directly.
- `TRE_JITO_ENDPOINTS`: comma/line separated list that overrides `jito.endpoints`. Individual URLs can also be specified via `$ENV{...}` tokens.
- `TRE_JITO_API_TOKEN`, `TRE_JITO_TIP_FLOOR_URL`, `TRE_JITO_TIP_STREAM_URL`, `TRE_JITO_CA_PEM`: override sensitive Jito parameters without storing them on disk.
- `TRE_BAM_VALIDATOR_CSV`: override the BAM validator CSV path.
- `TRE_QUICKNODE_RPCS`: override the Quicknode RPC list.
- `alerts.webhook_url`: optional HTTPS endpoint that receives JSON bundle/arbitrage lifecycle events; pair with `alerts.environment` to tag messages by deployment.

Submission tracking knobs live under the `submission` section of `config.json` (or `config.template.json`). You can safely check in the template that only contains `$ENV{VAR}` placeholders for secrets while keeping the real `config.json` ignored (see `.gitignore`). Use the template as the canonical reference in docs and CI.

### Docker & CI

- The repository ships a multi-stage `Dockerfile` that builds the release binary on Debian Bullseye and runs it inside a slim Debian runtime. Mount `/config` (for `config.json`) and `/data` (for persisted ALT state) when running the container. Default environment variables inside the image set `ARBITRAGE_CONFIG=/config/config.json`, `TRE_ALT_REGISTRY=/data/alt_registry.json`, `PROMETHEUS_METRICS_ADDR=0.0.0.0:9898`, and `RUST_LOG=warn,tre-arb-bot=info`.
- GitHub Actions workflow (`.github/workflows/ci.yml`) enforces `cargo fmt`, `cargo clippy -D warnings`, `cargo test --locked`, and a Docker build on every push/pull request targeting `main`/`master`. Use the same workflow locally to reproduce CI by running the listed commands and `docker build`.
- For Latitude.sh or other Ubuntu hosts, see `docs/deploy_latitude.md` for a step-by-step guide (server prep, env files, Docker run/systemd integration).

### Leader Schedule Gating

- Stage3 now consults a dedicated leader-schedule watcher that polls the configured `quicknode_rpcs` so bundles are only signed when the current slot’s leader is running Jito’s Blockspace Auction Mechanism (BAM).
- Populate `bam_validators.csv` with the canonical identity/vote accounts for every BAM-enabled validator (a starter list lives in the repo root). Use the top-level `bam_validator_csv` entry in `config.json` to point at refreshed snapshots when you regenerate the allowlist.
- When the watcher reports a non-BAM leader or the snapshot falls stale, Stage3 simply spins without dequeuing new bundles; logs make it obvious why submissions paused.
- Refresh the CSV whenever Jito publishes new validators or you want to prioritize a different subset. A runtime restart is required so the new allowlist can be loaded at startup.

### Alerting

Set `alerts.webhook_url` in `config.json` to mirror every arbitrage, bundle candidate, and Stage3 submission result to an external system (PagerDuty, Slack, etc.). The runtime always fans out to the logging sink, and will best-effort POST the same payload to the configured webhook with a short timeout so the hot path stays non-blocking. Use `alerts.environment` to distinguish between devnet, staging, and production fleets in downstream dashboards.

### Prometheus / Grafana

- Expose metrics by setting `PROMETHEUS_METRICS_ADDR="0.0.0.0:9898"` (or any open port). Prometheus can scrape the `/` endpoint directly.
- Track Stage1 capacity with `stage1_backpressure_events_total`. The `reason` label breaks events into `slab_exhausted`, `ring_full`, `overflow_queued`, `overflow_flushed`, and `overflow_dropped`. Sustained `slab_exhausted` or any non-zero `overflow_dropped` indicates the bundle pipeline is falling behind.
- Monitor Stage2 planning health with `stage2_plan_events_total`. Look for spikes in `event="plan_failed"` or `event="retries_exhausted"` (reason `plan` or `prepare`).
- Watch Stage3 send reliability via `stage3_retry_events_total`. Alerts should page when `event="retries_exhausted"` or `event="failure_recorded"` rises for `reason="sign"` (builder issues), `reason="submit"` (RPC/Jito), or `reason="context"` (Stage2 gaps). A healthy deployment should mostly emit `retry_flushed` when transient errors clear.
- Recommended alert (drop into your rules file):

  ```yaml
  - alert: Stage1BackpressureSustained
    expr: sum(rate(stage1_backpressure_events_total{reason=~"slab_exhausted|ring_full|overflow_dropped"}[5m])) > 0
    for: 2m
    labels:
      severity: critical
      service: tre-arb-bot
    annotations:
      summary: Stage1 bundle pipeline is dropping work
      description: "{{ $labels.environment }} has sustained Stage1 backpressure ({{ $value }} events/s)."
  ```

- Grafana panel tip: plot `sum by (reason)(rate(stage1_backpressure_events_total[1m]))` as a stacked area chart next to detection latency so you can correlate regressions with backpressure spikes.

## Directory Layout

```
bitquery/               # CoreCast protos (compiled via build.rs)
idls/pools/             # Pool IDLs → const instruction bytes (codegen emits pools_static.rs)
jito/                   # Jito block engine/searcher protos
src/
  core/                 # Lock-free SPSC rings + slab allocator
  stages/               # stage0_ingest → stage1_eval → stage2_submit implementations
  codegen/              # Generated instruction templates (include! at build time)
  runtime.rs            # Flat executor wiring + graceful shutdown
  config.rs             # Config loader + validation
  corecast.rs           # Credentialed CoreCast client + stream/channel pool management
  strategy.rs           # Opportunity evaluation, deduping, and dynamic tip policy
  bundle.rs             # Jito bundle client (single-endpoint today)
  notifier.rs           # Placeholder Gmail/alert plumbing
  proto.rs              # tonic include_proto! glue
```

## Deployment Targets

- **Local dev (macOS arm64):** Iterate quickly with `cargo build`; integration tests should mock CoreCast and Jito gRPC endpoints.
- **Production (Ubuntu x86_64):** Use release profile (LTO + panic abort). Recommended cross-compile locally, then copy the binary + `config.json`/IDL assets or build via the Dockerfile (to be filled with multi-stage build) on a CI runner.

## Next Steps

1. Extend bundle submission with multi-region failover plus bundle-status polling so retries and landed/failed state are observable.
2. Replace the logger-based `notifier` stub with the Gmail/OAuth flow and richer alerting hooks for successful or failed bundles.
3. Harden deployment (Dockerfile, CI, perf counters, health probes) while keeping the hot path logging-free.
4. Add integration tests/mocks for CoreCast + Jito flows to exercise the full pipeline under load.

For a deeper look at the intended architecture and latency goals, read `docs/architecture.md`.
