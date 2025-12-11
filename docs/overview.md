# TRE Runtime Overview

This single guide replaces the older architecture, deployment, and hydration notes so new contributors have one place to learn how the executor is wired, how to ship it, and what to watch in production.

## 1. Runtime Topology

```
CoreCast Streams ──▶ Pool Watcher ──▶ Pool Store ──▶ FastLane Planner ──▶ Bundle Builder ──▶ Jito Block Engine
                                         │                    │
                                  Templates (one-time)   Strategy + Tips
```

- **Four pillars:** the runtime collapses into four constantly-on services: (1) the CoreCast watcher that bootstraps pools once and keeps `PoolStore` fresh, (2) the Template & Resource Builder that manages immutable instruction templates, lookup tables, and flash routes, (3) the FastLane Planner that evaluates CoreCast deltas plus trade hints, and (4) the Bundle Builder & Submitter that signs, tips, and tracks bundles against the Jito block engine.

- **Pool Watcher** subscribes to CoreCast pool/trade streams, bootstraps static accounts once, and publishes decoded state into the in-memory `PoolStore`. There is no rolling hydration tier; once a pool’s static snapshot exists it stays “hydrated” for the whole run.
- **Template Builder** listens for new pools, creates swap templates exactly once per direction, and invalidates them only when underlying accounts change shape. Templates live entirely in RAM and are referenced by hash inside bundle contexts.
- **FastLane Planner** merges CoreCast deltas, trade feed hints, and strategy constraints to emit bundle candidates. Freshness checks happen immediately before submission using the difference between the latest CoreCast slot and the current tip slot; there are no TTL-based demotions.
- **Bundle Builder & Submitter** sign candidates with cached blockhashes, manage tip floors, and ship bundles through the configured Jito endpoints. The tracker consumes result streams and exposes `Accepted/Rejected/Expired` telemetry.

Key crates/modules:
- `src/hydration/` – now hosts the lightweight watcher, static snapshot registry, and template cache.
- `src/planner/` – graph manager, strategy engine, fast-lane scheduler.
- `src/builder/` – template builder, ALT manager, flash-loan planner, blockhash cache.
- `src/submission.rs`, `src/bundle/` – submitter, tracker, notifier.

## 2. Deployment Quickstart

1. **Provision hardware** – Ubuntu 22.04+, ≥4 vCPUs, ≥8 GB RAM, low-latency connectivity to CoreCast + Jito.
2. **Install dependencies**
   ```bash
   sudo apt-get update
   sudo apt-get install -y git curl build-essential pkg-config protobuf-compiler libssl-dev docker.io
   sudo usermod -aG docker $USER && newgrp docker
   ```
   Optionally install the Rust toolchain if you prefer native binaries over Docker.
3. **Clone and build**
   ```bash
   git clone https://github.com/zionmelson/tre.git /opt/tre
   cd /opt/tre
   docker build -t tre-arb-bot:latest .
   ```
4. **Prepare runtime paths**
   ```bash
   sudo mkdir -p /etc/tre-arb-bot /var/lib/tre-arb-bot /secrets
   sudo chown -R $USER /etc/tre-arb-bot /var/lib/tre-arb-bot
   cp config.json /etc/tre-arb-bot/config.json
   ```
   Adjust `config.json` to reference `$ENV{VAR}` placeholders for sensitive values.
5. **Create env file** (`/etc/tre-arb-bot/tre.env`)
   ```bash
   cat <<'EOF' > /etc/tre-arb-bot/tre.env
   TRE_AUTH_WALLET_PATH=/secrets/jito-auth.json
   TRE_PAYER_WALLET_PATH=/secrets/payer.json
   TRE_CORECAST_ADDRESS=corecast.bitquery.io
   TRE_CORECAST_AUTHORIZATION=<token>
   TRE_JITO_ENDPOINT_PRIMARY=https://ny.mainnet.block-engine.jito.wtf
   TRE_JITO_ENDPOINT_SECONDARY=https://la.mainnet.block-engine.jito.wtf
   TRE_JITO_CA_PEM=/secrets/jito_ca.pem
   TRE_JITO_API_TOKEN=<token>
   TRE_QUICKNODE_RPCS=https://...   # optional bootstrap RPC list
   PROMETHEUS_METRICS_ADDR=0.0.0.0:9898
   RUST_LOG=info,tre=debug
   EOF
   ```
6. **Run the container**
   ```bash
   docker run -d --name tre-arb-bot \
     --restart unless-stopped \
     --env-file /etc/tre-arb-bot/tre.env \
     -v /etc/tre-arb-bot/config.json:/config/config.json:ro \
     -v /var/lib/tre-arb-bot/alt_registry.json:/data/alt_registry.json \
     -v /secrets:/secrets:ro \
     -p 9898:9898 \
     tre-arb-bot:latest
   ```
   Use systemd to auto-start Docker if desired.
7. **Dry runs** – `./scripts/run-bot.sh --dry-run` (or the Docker equivalent) exercises CoreCast + planner without submitting bundles. Use `scripts/run-dry-verbose.sh` to capture detailed logs.

## 3. Operations & Telemetry

- **Metrics endpoint:** `http://<host>:9898/metrics` exposes Prometheus counters and gauges. Core metrics:
  - `pool_bootstrap_total{status}` – one-time RPC bootstrap results.
  - `corecast_delta_lag_ms` – staleness of streamed updates; alert if >100 ms for sustained periods.
  - `template_build_total{protocol}` – should roughly equal the number of pools seen per protocol.
  - `fastlane_ring_depth_pct`, `bundle_execution_latency_ms`, `bundle_result_events_total{status}` – health of planner and submission stages.
- **Logs:** structured log targets include `adapter::protocol`, `planner`, `submitter`, `bundle_tracker`. Verbose runs can filter per-module via `RUST_LOG=info,tre::hydration=trace` etc.
- **Alert ideas:**
  - `corecast_delta_lag_ms > 200` (critical) – stream stalled.
  - `pool_bootstrap_total{status="failed"}` increasing – RPC bootstrap issue.
  - `bundle_execution_retry_events_total` spiking – Jito endpoint instability.
- **Troubleshooting checklist:**
  - Pools stuck in bootstrap → inspect watcher logs to confirm static account fetch succeeded; ensure QuickNode (or equivalent) credentials are valid.
  - Empty metrics → confirm `PROMETHEUS_METRICS_ADDR` is set and port open.
  - `bundle contains expired blockhash` → verify system clock sync (`systemd-timesyncd`) and blockhash cache refresh cadence.

## 4. Lifecycle Modes

- **Production** – Default; watcher bootstraps pools, planner trades immediately.
- **Dry Run** – `./scripts/run-bot.sh --dry-run` keeps planner running but drains bundle candidates.
- **Bootstrap-only** – run `scripts/run-dry-verbose.sh` (or set `pipeline.dry_run_mode=true` / `./scripts/run-bot.sh --dry-run`) to rebuild template caches and watch telemetry until planners report readiness, then exit.

## 5. Reference Material

- Detailed CoreCast-first design: `docs/corecast-first.md`.
- Adapter-specific quirks remain in `docs/adapters/`.
- For any deployment variations (Kubernetes, bare-metal), duplicate the steps above but keep the same config/env separation.

This overview should be the single entry point for engineering, ops, and infra teams. All future changes should update this file plus any adapter-specific notes.
