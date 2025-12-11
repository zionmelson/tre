# tre-arb-bot

Ultra-light Solana arbitrage executor built as a CoreCast-first pipeline. Everything runs inside one process, fed entirely by streamed pool/trade deltas and guarded by four pillars:

1. **CoreCast Watcher** – Boots each pool exactly once, decodes CoreCast deltas, and keeps `PoolStore` fresh without tiers or demotions.
2. **Template & Resource Builder** – Generates immutable swap templates, lookup tables, and flash-loan routes keyed by content hash.
3. **FastLane Planner** – Merges CoreCast deltas, trade hints, and strategy constraints to emit bundle candidates guarded by slot-lag checks.
4. **Bundle Builder & Submitter** – Signs bundles, enforces tip policy, streams results from the Jito block engine, and exports telemetry.

## Status

This repository currently provides:

- Cargo project scaffolding with Tokio/tonic/prost wiring
- Proto compilation for Bitquery CoreCast and Jito MEV endpoints
- CPU-affine runtime with lock-free channels + slabs wiring the four CoreCast-first pillars together (`runtime.rs`, `core/`, `hydration/`, `planner.rs`)
- Single-shot CoreCast → Watcher → Planner → Submitter flow with config management, IDL caching, template generation, flash-loan planning, and Prometheus-friendly telemetry
- Bundle lifecycle tracking that streams `SubscribeBundleResults`, taps the state-auction websocket feed, and publishes per-bundle alerts/metrics for accepted, rejected, finalized, or expired bundles
- Documentation covering the CoreCast-first pipeline and deployment assumptions (see `docs/architecture.md` and `docs/deploy_latitude.md`)
- An operational CLI with `run` (default) and `resign` subcommands so operators can either launch the executor or refresh archived bundles without spinning up the full runtime

## Runtime Overview

- `src/main.rs` exposes the user-facing CLI. `tre-arb-bot run` (default) spins up the executor, while `tre-arb-bot resign` reloads a logged bundle, fetches a fresh blockhash, and re-signs it with the configured payer.
- `runtime::ExecutorRuntime` bridges the config, CoreCast watcher, builder, planners, submission lane, and graceful shutdown (`SIGINT` stops every thread via an `AtomicBool`).
- `hydration/service.rs` (the watcher) owns CoreCast ingestion, the `AdapterBook`, template cache, ALT manager, flash-loan planner, and the shared `PoolStore`. It bootstraps pools once and keeps their static metadata pinned until restart.
- `planner/` hosts the graph service, CoreCast trade feed, and `FastLanePlanner`. The planner consumes `PoolStateUpdate`s, calls into `StrategyEngine`, gates on CoreCast slot lag, and emits `BundleCandidate` leases via an SPSC ring backed by slab allocators from `core/`.
- `submission.rs` and `bundle/` house the fast-lane submitter, Jito gRPC client, and `BundleTracker`. Successful sends go through `BundleClient`; lifecycle updates stream from result + auction feeds and are mirrored via `Notifier` + telemetry.
- Telemetry (`telemetry.rs`) exports Prometheus counters/histograms that cover CoreCast freshness, graph state, planner queues, submission success, and bundle outcomes. `pipeline.dry_run_mode` keeps the pipeline hot without submitting bundles so you can soak CoreCast freshness end-to-end.

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

# Non-submitting dry run (planner runs, submissions disabled)
# Always-on dry run helper
./scripts/run-dry.sh

# One-off dry run using the primary script
./scripts/run-bot.sh --dry-run

# Refresh a previously logged bundle payload with a fresh blockhash/signature
tre-arb-bot resign --config config.json --input bundle.b64 --output refreshed.b64

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

### Pool Inventory Validation

`./scripts/check-pools.sh` runs four sequential validations so you can confirm the local pool inventory is coherent before the CoreCast watcher spins up:

1. Config inventory (`config_inventory_valid`)
2. ALT registry readiness (`alt_registry_ready_and_consistent`)
3. On-chain ALT account existence (`alt_registry_accounts_exist_on_chain`)
4. IDL program IDs (`idl_program_ids_match_expectations`)
5. Instruction template coverage (`swap_template_sets_cover_both_directions`)

The script stops on the first failure, so the console output always highlights the exact validation that needs attention.

Use `TRE_RPC_URL=<https endpoint>` to override which Solana RPC the on-chain validation hits; it falls back to `quicknode_rpcs[0]` in `config.json` if unset.

Environment variables:

- `ARBITRAGE_CONFIG`: optional path override for `config.json` (defaults to repo root).
- `RUST_LOG`: set to control tracing verbosity (defaults to `warn,tre-arb-bot=info`).
- `TRE_ALT_REGISTRY`: optional path override for the persisted ALT registry snapshot. Defaults to `alt_registry.json` in the working directory and allows restarts to reuse existing lookup tables without rebuilding them.
- `TRE_CORECAST_ADDRESS` / `TRE_CORECAST_AUTHORIZATION`: override `corecast.address` or `corecast.authorization` without editing JSON. Place `$ENV{TRE_CORECAST_AUTHORIZATION}` inside `config.json` to reference the variable directly.
- `TRE_JITO_ENDPOINTS`: comma/line separated list that overrides `jito.endpoints`. Individual URLs can also be specified via `$ENV{...}` tokens.
- `TRE_JITO_API_TOKEN`, `TRE_JITO_TIP_FLOOR_URL`, `TRE_JITO_TIP_STREAM_URL`, `TRE_JITO_CA_PEM`: override sensitive Jito parameters without storing them on disk.
- `TRE_BAM_VALIDATOR_CSV`: override the BAM validator CSV path.
- `TRE_QUICKNODE_RPCS`: override the Quicknode RPC list.
- `TRE_DRY_RUN_MODE`: set to `true` to keep the CoreCast watcher + planner hot while discarding bundle submissions (the `--dry-run` script flag toggles this automatically). Use it for end-to-end soaks without touching the block engine.
- `alerts.webhook_url`: optional HTTPS endpoint that receives JSON bundle/arbitrage lifecycle events; pair with `alerts.environment` to tag messages by deployment.

### ALT Reuse & Testing

- `alt_manager.create_onchain` (in `config.json`) gates whether the ALT manager actually creates or extends lookup tables on Solana. Set it to `false` for dry runs so handles are allocated only from the local `alt_registry.json` snapshot—no SOL burned.
- `alt_manager.max_tables_per_pool` enforces how many lookup tables we keep per pool in the registry. The default of `1` guarantees we never juggle multiple LUTs for the same pool between runs.
- Seed `alt_registry.json` with the LUTs you care about (one per pool) when running with `create_onchain=false`. Any entry marked `Ready` with at least one table will be treated as reusable; entries missing a table will log a warning until you update the snapshot or re-enable on-chain creation.

Submission tracking knobs live under the `submission` section of `config.json`. Use the checked-in file as the canonical reference, copy it for each deployment, and swap any sensitive values with `$ENV{VAR}` placeholders so secrets stay outside of version control.

## Bundle Payload Re-Signing

`tre-arb-bot resign` is a maintenance-only subcommand implemented in `src/main.rs`. It:

1. Loads the same `config.json` that powers the runtime so it can locate the payer wallet and QuickNode (or custom) RPC endpoint.
2. Accepts a base64-encoded `VersionedTransaction` via `--input <path>` or stdin, decodes it, and fetches the latest blockhash (overrideable via `--rpc-url`).
3. Re-signs the message with the payer keypair referenced in `wallets.payer`, then emits the refreshed payload to `--output <path>` or stdout.

This flow is useful when you want to replay a logged bundle without restarting the executor or when you need to re-tip a payload for manual submission/debugging.

### Docker & CI

- The repository ships a multi-stage `Dockerfile` that builds the release binary on Debian Bullseye and runs it inside a slim Debian runtime. Mount `/config` (for `config.json`) and `/data` (for persisted ALT state) when running the container. Default environment variables inside the image set `ARBITRAGE_CONFIG=/config/config.json`, `TRE_ALT_REGISTRY=/data/alt_registry.json`, `PROMETHEUS_METRICS_ADDR=0.0.0.0:9898`, and `RUST_LOG=warn,tre-arb-bot=info`.
- GitHub Actions workflow (`.github/workflows/ci.yml`) enforces `cargo fmt`, `cargo clippy -D warnings`, `cargo test --locked`, and a Docker build on every push/pull request targeting `main`/`master`. Use the same workflow locally to reproduce CI by running the listed commands and `docker build`.
- For Latitude.sh or other Ubuntu hosts, see `docs/deploy_latitude.md` for a step-by-step guide (server prep, env files, Docker run/systemd integration).

### Leader Schedule Gating

- The submission lane consults a dedicated leader-schedule watcher that polls the configured `quicknode_rpcs` so bundles are only signed when the current slot’s leader is running Jito’s Blockspace Auction Mechanism (BAM).
- Populate `bam_validators.csv` with the canonical identity/vote accounts for every BAM-enabled validator (a starter list lives in the repo root). Use the top-level `bam_validator_csv` entry in `config.json` to point at refreshed snapshots when you regenerate the allowlist.
- When the watcher reports a non-BAM leader or the snapshot falls stale, the submission lane spins without dequeuing new bundles; logs make it obvious why submissions paused.
- Refresh the CSV whenever Jito publishes new validators or you want to prioritize a different subset. A runtime restart is required so the new allowlist can be loaded at startup.

### Alerting

Set `alerts.webhook_url` in `config.json` to mirror every arbitrage, bundle candidate, and submission-lane result to an external system (PagerDuty, Slack, etc.). The runtime always fans out to the logging sink, and will best-effort POST the same payload to the configured webhook with a short timeout so the hot path stays non-blocking. Use `alerts.environment` to distinguish between devnet, staging, and production fleets in downstream dashboards.

### Prometheus / Grafana

- Expose metrics by setting `PROMETHEUS_METRICS_ADDR="0.0.0.0:9898"` (or any open port). Prometheus scrapes `/` for counters, histograms, and gauges.
- CoreCast freshness: watch `corecast_delta_lag_ms`, `pool_bootstrap_total{status}`, and `corecast_stream_reconnects_total`. Sustained lag means the stream stalled or credentials expired.
- Planner & fast lane: track `watcher_update_age_ms`, `fastlane_ring_depth_pct`, `fastlane_ring_drop_events_total`, and `bundle_execution_retry_events_total{reason="sign|submit|context"}`.
- Template & flash readiness: keep `template_build_total{protocol}`, `template_cache_entries`, and `flash_route_ready_total` on a single dashboard to highlight upstream starvation.
- Recommended alert:

  ```yaml
  - alert: CoreCastLag
    expr: corecast_delta_lag_ms > 200
    for: 1m
    labels:
      severity: critical
      service: tre-arb-bot
    annotations:
      summary: CoreCast stream is stale
      description: "{{ $labels.environment }} corecast_delta_lag_ms is above 200 for >1m; planner will soon drop opportunities."
  ```

- Grafana tip: overlay `quantile_over_time(0.99, watcher_update_age_ms[1m])` with `bundle_execution_latency_ms` and `bundle_execution_tip_multiplier_milli` to correlate stale data with missed fills. Label series by adapter/protocol for faster triage.

## Directory Layout

```
bitquery/               # CoreCast protos (compiled via build.rs)
idls/pools/             # Pool IDLs → const instruction bytes (codegen emits pools_static.rs)
jito/                   # Jito block engine/searcher protos
src/
  core/                 # Lock-free rings + slab allocator primitives shared by watcher/planner/submitter lanes
  hydration/            # CoreCast watcher: ingest, adapter decoders, runtime pool map, template cache
  planner.rs            # Fast lane: subscribes to CoreCast feed, runs StrategyEngine
  runtime.rs            # Thread wiring + graceful shutdown for watcher, planner, submitter
  config.rs             # Config loader + validation (borrow pct, endpoints, runtime sizing)
  corecast.rs           # Credentialed CoreCast client + stream/channel pool management
  strategy.rs           # Opportunity evaluation, borrow sizing, and dynamic tip policy
  bundle.rs             # Bundle tracker, telemetry, and submission helpers
  submission.rs         # Builder + signer + dual-lane sender logic
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
