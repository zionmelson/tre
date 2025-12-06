# Executor Runtime Architecture

## Objectives

- **Freshness first:** Pool snapshots, template caches, ALT tables, flash-loan routes, and blockhashes carry TTLs that cap end-to-end latency at ~45 ms. Anything older is rejected before the builder signs a bundle.
- **Single in-memory path:** CoreCast deltas are decoded once by the hydration service and stay in RAM via `PoolStore`, the lossy `SnapshotChannel`, and `SlabPool` structures—no RocksDB, JSON polling, or multi-stage disk queues.
- **Deterministic latency:** CPU affinity, lock-free channels, and bounded allocators keep `CoreCast → HydrationSnapshot → Planner → Submitter` deterministic even under load.
- **Observability-backed safety:** Prometheus metrics (`telemetry.rs`) expose queue depth, stale pools, planner backlog, submission latency, bundle outcomes, and tip pressure so automation (or humans) can pause trading quickly.

## Topology Overview

```
        CoreCast Streams  +  RPC refreshers
                      |
            +-----------------------+
            | HydrationService      |
            |  AdapterBook/ALT/etc. |
            +-----------------------+
                 |           |
        PoolStateUpdate   HydrationSnapshot
                 |           |
         Arc<PoolStore>     v
                 |    +----------------+
                 +--> | FastLanePlanner|
                      +----------------+
                              |
                 Ring<Lease<BundleCandidate>>
                              |
                    FastLane Submitter
                              |
                      BundleClient (Jito)
                              |
                      BundleTracker/Notifier
```

`BuilderHandle` (templates, ALT manager, flash-loan planner, blockhash cache, MEV guard) is shared across stages, ensuring that hydration, planning, and submission agree on accounts, ALTs, and signer state.

## Stage Responsibilities

### 1. Hydration ingress & resource managers (`src/hydration/service.rs`)

- Builds a single `HydrationService` around a shared `BuilderHandle`. It owns the `AdapterBook`, template cache, ALT manager, flash-loan planner, and the `PoolStore` (runtime map of hydrated pools subject to freshness/size policies from `config.watcher`).
- `spawn_adapter_ingest` opens the credentialed `CoreCastClient`, consumes pool deltas, enriches them with adapter-specific metadata, and applies them to `PoolStore`. An allowlist derived from `AppConfig::token_pair_allowlist` limits ingestion to configured mint pairs.
- `spawn_snapshot_worker` publishes lightweight `HydrationSnapshot`s into a single-slot `SnapshotChannel` every ~25 ms. Publishing overwrites the previous entry if the planner has not consumed it yet, ensuring the planner always reads the freshest state while telemetry counts any overwrites.
- `spawn_pool_maintenance` coordinates RPC refresh via `RpcRefresher` when CoreCast stalls, and `pool_store.collect_stale` ensures old entries are either refreshed or marked stale for the planner.
- `pool_store.subscribe` fans updates out to graph builders, planners, and metrics threads. Thread shutdown is coordinated through a shared `AtomicBool` stop flag owned by the runtime.

### 2. Graph + fast-lane planner (`src/planner/`)

- `spawn_graph_service` consumes `PoolStateUpdate`s directly from `PoolStore`, maintains graph state inside `GraphManager`, and surfaces arbitrage cycles (`ArbitrageOpportunity`).
- `spawn_fast_lane_trade_feed` opens a second CoreCast trade stream dedicated to the fast lane. Trades are filtered through the token-pair allowlist and queued via a bounded `crossbeam` channel.
- `FastLanePlanner` merges three inputs: pool updates, hydration snapshots, and trade updates. It waits for `PoolStore::wait_until_ready` and then loops, producing `Opportunity` structs that feed `StrategyEngine::evaluate`.
- `StrategyEngine` enforces `strategy.*` overrides (min edge, hop count, profit/tip floors, duplicate suppression, protocol borrow limits) and returns `BundleCandidate`s once routes are profitable and fresh. Candidates carry `BundleBuildContext` handles so the submitter can sign without rehydrating templates.
- `Notifier` mirrors arbitrage cycles, candidate creation, and submission state to logs/webhooks. Alerts are tagged via `alerts.environment` for downstream dashboards.

### 3. Submission & tracking (`src/submission.rs`, `src/bundle/`)

- `submission::spawn_fast_lane_submitter` consumes `Lease<BundleCandidate>` entries from the ring. Each lease tracks creation timestamps and optional flash-loan reservations so the builder can release them on errors.
- `BuilderHandle::sign_bundle` swaps in a fresh blockhash (from the cached RPC client) before handing the transaction to `BundleClient`.
- `BundleClient` multiplexes across the configured Jito block-engine endpoints, handles auth token refresh, and applies endpoint-specific exponential backoff. Successful sends result in `BundleSubmitResult { endpoint, bundle_id }` objects that feed telemetry + tracking.
- `BundleTracker` listens to `SubscribeBundleResults`, optional auction streams, and a REST poller so it can mark bundles `Submitted/Accepted/Rejected/Expired`. Every state change is emitted through `Notifier` and recorded for `tip_pressure` heuristics.
- `pipeline.dry_run_mode` routes planner output to a drain thread instead of the submitter. `pipeline.hydration_only_mode` drains snapshots immediately after hydration, skipping graph/planner entirely.

### 4. Support services

- **Builder (`src/builder/`)** – Seeds static templates from the IDL set, lazily registers pool-specific templates, owns the ALT manager (`alt_registry.json` by default), manages the flash-loan planner, blockhash cache, token ATA map, and MEV guard heuristics.
- **Strategy + tips (`src/strategy.rs`, `src/tip_pressure.rs`)** – Combine config-driven borrow limits with tip floors fetched from the Jito REST API. Wallet-level caps are optional via `strategy.wallet_tip_cap_bps`, and congestion heuristics track Stage3 latency.
- **Config (`src/config.rs`)** – Loads JSON, resolves `$ENV{VAR}` placeholders, populates derived sets (token pairs, ATAs, host-fee accounts, protocol defaults), and validates queue/buffer sizing before runtime startup.
- **Telemetry & notifier (`src/telemetry.rs`, `src/notifier.rs`)** – Spin up the Prometheus HTTP server (if `PROMETHEUS_METRICS_ADDR` is set) and multiplex structured events to logs plus optional webhooks.

## Data Contracts

- **`PoolState` / `PoolStateUpdate`** – Represent hydrated protocol state: slot, `Instant` timestamps, base/quote liquidity summaries, protocol identifiers, freshness flags, and a monotonically increasing generation counter.
- **`HydrationSnapshot`** – Cheap `Copy` struct delivered via the lossy `SnapshotChannel` that includes template counts, ALT manager stats, and flash-route readiness plus the `issued_at` timestamp. The channel always exposes the newest snapshot, dropping older ones and incrementing an overwrite metric so operators can see when freshness pressure is high. Callers compare `Instant::now()` against `snapshot.stale_at(freshness_budget)` to guard planners.
- **`TemplateReadiness`** – Returned by `BuilderHandle::template_readiness(pool, direction)` and combines template TTL + ALT TTL so route planning can skip pools missing a hydrated template/ALT table.
- **`BundleCandidate`** – Planner output containing `BundleBuildContext`, route metadata, estimated edge/profit/tip, flash-loan reservations, and detection timestamps. Candidates always live inside a `Lease` from `SlabPool` so memory stays deterministic.
- **`PendingSubmission`** – Tracker input with `bundle_id`, `seq`, endpoint, detection time, and optional tips. Tracker augments it with result streams and emits `BundleSubmissionEvent`s (submitted, accepted, rejected, expired, etc.).

## Operating Modes & Failure Handling

- `pipeline.hydration_only_mode` – Spins up hydration/graph threads but drains both snapshots and planner outputs immediately. Useful for soak-testing CoreCast or RPC refreshers.
- `pipeline.dry_run_mode` – Keeps planner/graph online but routes bundle candidates to a drain thread while recording planner metrics. Submission, tracker, and bundle client threads are skipped.
- Backpressure is surfaced via metrics and logs: `hydration_queue_depth_pct`, `watcher_pool_update_backlog_pct`, `fastlane_ring_depth_pct`, and `fastlane_ring_drop_events_total` all spike before candidates are dropped.
- Template/ALT freshness is enforced via TTLs (`TemplateCache` ~40 ms, ALT tables ~40 ms). Pools that fall behind are either refreshed via RPC or flagged stale, preventing planners from emitting bundles they cannot safely build.

## Telemetry & Alerting

- **Hydration** – `hydration_adapter_ingest_total{protocol}`, `hydration_rpc_refresh_total`, `hydration_stale_pool_events_total`, `hydration_template_cache_entries`, `hydration_alt_ready_tables`, and queue gauges measure ingest health plus template readiness.
- **Planner/graph** – `graph_pool_count`, `graph_cycle_count`, `watcher_update_age_ms{adapter}`, `watcher_cache_entries`, and `watcher_cycle_scan_pending` indicate whether CoreCast deltas are being consumed quickly enough.
- **Fast lane buffers** – `fastlane_ring_depth_pct`, `fastlane_ring_drop_events_total`, `fastlane_slab_free_pct`, and `fastlane_slab_exhaustion_events_total` highlight saturation inside the bundle ring/slab.
- **Submission + tracking** – `bundle_execution_latency_ms`, `bundle_execution_attempts_total{status}`, `bundle_result_events_total{status}`, `bundle_execution_retry_events_total{reason}`, and `bundle_result_latency_ms` capture send performance and downstream outcomes.
- **Tip heuristics** – `bundle_execution_tip_lamports`, `bundle_execution_tip_multiplier_milli`, and `tip_pressure`-derived gauges (exported via telemetry) show how congestion controls are behaving.
- Example PromQL alert:

  ```yaml
  - alert: FastLaneBacklog
    expr: fastlane_ring_depth_pct > 80
    for: 30s
    labels:
      severity: warning
      service: tre-arb-bot
    annotations:
      summary: Fast lane backlog is growing
      description: "Ring occupancy is {{ $value }}% which will drop bundle throughput; inspect watcher_update_age_ms and hydration_queue_depth_pct."
  ```

## Deployment & Testing

1. **Unit tests** – Focus on adapter decode paths, template cache hydration, borrow-cap math, and strategy tip calculations.
2. **Integration tests** – Replay recorded CoreCast deltas through `PoolStore`, ensure the planner emits expected opportunities, and validate `StrategyEngine` rejects stale or duplicate trades.
3. **Dry runs** – Start the binary with `pipeline.dry_run_mode=true` (or `./scripts/run-bot.sh --dry-run`); confirm planner metrics stay below thresholds while submission threads remain idle.
4. **Live trading** – Flip both pipeline flags off only when `hydration_queue_depth_pct < 50`, `watcher_update_age_ms p99 < 30 ms`, and `bundle_execution_retry_events_total` is flat. Keep Grafana dashboards for hydration/planner/submission on one page for quick triage.

## Future Work

- Add secondary consumers to the `PoolStore` broadcast channel (analytics, shadow planners) with backpressure isolation.
- Investigate shared-memory snapshots or QUIC feeds if multiple watcher binaries run on the same host.
- Layer adaptive borrow sizing (volatility-aware) over the current per-protocol `borrow_pct` defaults.
- Extend telemetry with per-route win/loss tracking and surface `tip_pressure` state through a dedicated API for downstream automation.
