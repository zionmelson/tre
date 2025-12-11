# Executor Runtime Architecture

## Objectives

- **CoreCast-first freshness:** Pools bootstrap once, stay memory-resident, and rely on CoreCast slot-lag guards before a bundle is signed. No TTL-based demotions or hydration tiers remain.
- **Single in-memory path:** CoreCast deltas are decoded once by the watcher and stay in RAM via `PoolStore`, rings, and slab allocators-no disk caches or multi-stage queues.
- **Deterministic latency:** CPU affinity, lock-free channels, and bounded allocators keep `CoreCast -> Planner -> Submitter` deterministic even under load.
- **Observability-backed safety:** Prometheus metrics (`telemetry.rs`) expose stream freshness, planner backlog, submission latency, bundle outcomes, and tip pressure so automation (or humans) can pause trading quickly.

## Four Pillars & Topology

```
CoreCast Streams -> Pool Watcher -> Pool Store -> FastLane Planner -> Bundle Builder -> Jito Block Engine
          |                     |
        Templates & ALTs        Strategy + Tips
```

- **CoreCast Watcher (`src/hydration/`)** - Subscribes to CoreCast pool/trade streams, bootstraps static accounts once, and keeps `PoolStore` updated. There is no intermediate hydration tier; once metadata exists it stays pinned until restart.
- **Template & Resource Builder (`src/builder/`)** - Records immutable instruction templates, lookup tables, flash-loan routes, and blockhash caches. Templates are keyed by `(pool, direction, content hash)` and never churn unless the hash changes.
- **FastLane Planner (`src/planner/`)** - Merges CoreCast deltas, trade hints, and strategy constraints to emit `BundleCandidate`s. Slot-lag and planner guardrails run immediately before scheduling a bundle.
- **Bundle Builder & Submitter (`src/submission.rs`, `src/bundle/`)** - Signs bundles, enforces tip policy, and streams results from the Jito block engine via `BundleClient` + `BundleTracker`.

`BuilderHandle` sits at the center so every pillar shares template state, ALT metadata, flash-loan routes, and blockhash caches.

## Stage Responsibilities

### 1. CoreCast watcher (`src/hydration/service.rs`)

- Owns the `AdapterBook`, template cache, ALT manager, flash-loan planner, and the shared `PoolStore`.
- `spawn_adapter_ingest` opens the credentialed `CoreCastClient`, consumes pool deltas, enriches them with adapter metadata, and applies them to `PoolStore`. The token-pair allowlist limits ingestion to configured mints.
- `spawn_pool_maintenance` coordinates RPC bootstrap when a pool first appears or when a structural change is detected. Once bootstrapped, the watcher never demotes pools-it only invalidates templates when hashes change.
- `pool_store.subscribe` fans updates out to graph builders, planners, metrics threads, and any consumer that needs live pool state. Shutdown is coordinated with a shared `AtomicBool` stop flag.

### 2. Template & resource builder (`src/builder/`)

- Seeds static templates from IDLs, lazily registers pool-specific templates, and stores them in `TemplateCache` keyed by `(pool, direction, template_hash)`.
- Manages the ALT registry (`alt_registry.json` by default), flash-loan planner, blockhash cache, token ATA map, and MEV guard heuristics.
- Exposes readiness helpers to planners/submitters via `BuilderHandle` without any TTL semantics; templates stay valid until the watcher publishes a new hash.

### 3. FastLane planner (`src/planner/`)

- `spawn_graph_service` maintains graph state and surfaces arbitrage cycles.
- `spawn_fast_lane_trade_feed` opens a CoreCast trade stream dedicated to the fast lane.
- `FastLanePlanner` merges pool updates and trade hints, evaluates each route with `StrategyEngine`, and emits `BundleCandidate` leases backed by slab allocators.
- `StrategyEngine` enforces `strategy.*` overrides (min edge, hop count, profit/tip floors, duplicate suppression, protocol borrow limits) and applies slot-lag guards immediately before a candidate is accepted.
- `Notifier` mirrors arbitrage cycles, candidate creation, and submission state to logs/webhooks tagged via `alerts.environment`.

### 4. Bundle builder & submission (`src/submission.rs`, `src/bundle/`)

- `submission::spawn_fast_lane_submitter` consumes `Lease<BundleCandidate>` entries from the ring, hydrates the immutable templates/ALTs referenced by each plan, and signs bundles with a fresh blockhash.
- `BundleClient` multiplexes across the configured Jito block-engine endpoints, handles auth token refresh, and applies endpoint-specific backoff. Successful sends return `{ endpoint, bundle_id }` for telemetry.
- `BundleTracker` listens to `SubscribeBundleResults`, optional auction streams, and REST fallbacks so it can mark bundles `Submitted/Accepted/Rejected/Expired`. Each change fans out via `Notifier` and feeds `tip_pressure` heuristics.
- `pipeline.dry_run_mode` routes planner output to a drain thread instead of the submitter while keeping every other pillar online. Use it for CoreCast soaks without risking submissions.

## Data Contracts

- **`PoolState` / `PoolStateUpdate`** - Represent decoded protocol state: slot, timestamps, liquidity summaries, protocol identifiers, and a monotonically increasing generation counter.
- **`TemplateContext`** - Metadata recorded alongside immutable instruction templates (tick-array coverage, remaining accounts, flash-loan hints). Context hashes guarantee the builder, planner, and submitter use the same bytes.
- **`BundleCandidate`** - Planner output containing `BundleBuildContext`, route metadata, estimated edge/profit/tip, flash-loan reservations, and detection timestamps. Candidates always live inside a `Lease` from `SlabPool` so memory stays deterministic.
- **`BundleInstructionPlan` / `BundleInstruction`** - Submission-stage records that pair template hashes with hydrated account metas, lookup tables, and compute budgets.
- **`BundleSubmissionEvent`** - Tracker output for `Submitted/Accepted/Rejected/Expired` states with endpoint + latency metadata.

## Operating Modes & Failure Handling

- **Dry run (`pipeline.dry_run_mode=true`)** keeps watcher, builder, and planner threads online while draining bundle candidates instead of submitting them. Metrics stay live, making it ideal for CoreCast/RPC soak tests and template rebuilds.
- **Production** simply disables the dry-run flag. Pools stay ready until their content hash changes, and slot-lag guards fire immediately before a candidate is sent.
- Backpressure surfaces via metrics/logs: `corecast_delta_lag_ms`, `watcher_update_age_ms`, `fastlane_ring_depth_pct`, and `fastlane_ring_drop_events_total` all spike before candidates are dropped.
- Template freshness is enforced via hash comparisons. If a pool's static accounts change, the watcher publishes a new hash and planners skip the pool until the builder finishes rebuilding.

## Telemetry & Alerting

- **CoreCast & watcher** - `pool_bootstrap_total{status}`, `corecast_delta_lag_ms`, `corecast_stream_reconnects_total`, `template_build_total{protocol}`, `template_cache_entries`.
- **Planner/graph** - `graph_pool_count`, `graph_cycle_count`, `watcher_update_age_ms{protocol}`, `fastlane_ring_depth_pct`, `fastlane_ring_drop_events_total`, `fastlane_slab_free_pct`.
- **Submission + tracking** - `bundle_execution_latency_ms`, `bundle_execution_attempts_total{status}`, `bundle_result_events_total{status}`, `bundle_execution_retry_events_total{reason}`, `bundle_result_latency_ms`, `bundle_execution_tip_multiplier_milli`.
- **Tip heuristics** - `tip_pressure`, `bundle_execution_tip_lamports`, and congestion gauges exported by `tip_pressure.rs`.

Recommended alert examples:

```
- alert: CoreCastLag
  expr: corecast_delta_lag_ms > 200
  for: 1m
  labels:
    severity: critical
    service: tre-arb-bot
  annotations:
    summary: CoreCast stream is stale
    description: "{{ $labels.environment }} corecast_delta_lag_ms is above 200 for >1m; planner will soon drop opportunities."
- alert: FastLaneBacklog
  expr: fastlane_ring_depth_pct > 80
  for: 30s
  labels:
    severity: warning
    service: tre-arb-bot
  annotations:
    summary: Fast lane backlog is growing
    description: "Ring occupancy is {{ $value }}%; inspect watcher_update_age_ms to confirm CoreCast freshness."
```

## Deployment & Testing

1. **Unit tests** - Focus on adapter decode paths, template hashing, borrow-cap math, and strategy/tip calculations.
2. **Integration tests** - Replay recorded CoreCast deltas through `PoolStore`, ensure the planner emits expected opportunities, and validate `StrategyEngine` rejects stale or duplicate trades.
3. **Dry runs** - Start the binary with `pipeline.dry_run_mode=true` (or `./scripts/run-bot.sh --dry-run`); confirm planner metrics stay below thresholds while submission threads remain idle.
4. **Live trading** - Flip dry-run off only when `corecast_delta_lag_ms < 100`, `watcher_update_age_ms p99 < 30 ms`, and `bundle_execution_retry_events_total` is flat. Keep Grafana dashboards for watcher/planner/submission on one page for quick triage.

## Future Work

- Add secondary consumers to the `PoolStore` broadcast channel (analytics, shadow planners) with backpressure isolation.
- Investigate shared-memory snapshots or QUIC feeds if multiple watcher binaries run on the same host.
- Layer adaptive borrow sizing (volatility-aware) over the current per-protocol `borrow_pct` defaults.
- Extend telemetry with per-route win/loss tracking and surface `tip_pressure` state through a dedicated API for downstream automation.
