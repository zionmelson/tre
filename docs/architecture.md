# Hydrator ↔ Watcher Architecture

## Objectives

- **Freshness first:** Derived price/liquidity, flash routes, templates, and blockhashes all carry timestamps plus TTLs. Anything older than 45 ms at use time is rejected.
- **Single in-memory path:** Hydrator produces runtime state directly in RAM and streams updates to the watcher; no RocksDB, JSON polling, or stage queues.
- **Deterministic latency:** CPU-pinned threads, lock-free channels, and slab allocators keep the decode→plan→submit chain well under 50 ms.
- **Observability-backed safety:** Telemetry surfaces queue saturation, stale pools, planner latency, and submission failures so the bot auto-pauses before leaking bad bundles.

## System Overview

```
              CoreCast Streams / RPC fallbacks
                              |
                    +---------------------+
                    | Hydrator Workers    |
                    |  (per protocol)     |
                    +---------------------+
                              |
                 decode + derive PoolState
                              |
                    +---------------------+
                    | Runtime Pool Map    |
                    | Arc<ShardMap<...>>  |
                    +---------------------+
                              |
                broadcast compact PoolUpdate
                              |
                    +---------------------+
                    | Lock-Free Channel   |
                    +---------------------+
                              |
                    apply updates / TTL check
                              |
                    +---------------------+
                    | Watcher (Planner)   |
                    | Strategy + Graph    |
                    +---------------------+
                              |
                     Bundle Slab / Tracker
                              |
                          Submitter
```

- Hydrator owns every external feed (CoreCast, template/ALT refreshers, flash-loan reserves) and is the only writer to the runtime pool map.
- Watcher subscribes to the broadcast channel, maintains a read-only view, evaluates routes, clamps borrow sizing, and pushes bundles into the tracker.
- Submission lane signs bundles and fires them down Jito + TPU the moment watcher declares a win.

## Detailed Flow

1. **Bootstrap** – Load `config.json`, wallets, IDLs, and the JSON ALT registry (templates only). Start CoreCast streams + RPC fallbacks, allocate pool map shards, and create the bounded MPMC channel.
2. **Hydrator ingest** – Each adapter worker decodes CoreCast deltas (or fallback RPC responses) into `PoolState { slot, ts, mid_price, bid_depth, ask_depth, fee_bps, volatility_flag }`. The pool map is updated in-place; a `PoolUpdate` struct is enqueued.
3. **Watcher refresh** – A lightweight thread drains the channel, refreshes its local view, drops stale entries, and wakes the planner when liquidity changed for any subscribed route.
4. **Strategy execution** – `planner.rs` pulls pool snapshots, applies borrow caps (`borrow_pct` per protocol), runs GraphManager/StrategyEngine, and, when profitable, allocates bundle handles inside the slab before hydrating instruction templates.
5. **Submission** – Bundle bytes are signed and submitted to both Jito gRPC and TPU fanout. Tip policy + BAM gating live entirely in the submission lane.
6. **Feedback** – Bundle tracker streams `SubscribeBundleResults` and state-auction websockets to mark accepted/rejected bundles. Telemetry and alerts reflect queue health, stale pools, planner latency, and submission outcomes.

## Component Responsibilities

- **`hydration/`** – CoreCast clients, adapter decoders, template cache, flash-loan + ALT managers, runtime pool map, broadcast queue, stale-pool scanners, and hydration telemetry.
- **`planner.rs` (Watcher)** – Subscribes to hydrator updates, manages the read-only pool view, enforces borrow config, runs StrategyEngine/GraphManager, and produces bundle candidates.
- **`submission.rs` / `bundle.rs`** – Sign bundles, push to Jito + TPU, manage retries, consume bundle-result streams, and expose send telemetry.
- **`config.rs`** – Validates hydrator queue sizes, borrow percentages, endpoint lists, BAM validator CSV path, and telemetry toggles.
- **`core/`** – Lock-free slabs, rings, and affinity helpers shared by hydrator + watcher.
- **`telemetry.rs`** – Registers gauges + histograms (`hydration_adapter_ingest_total`, `hydrator_queue_depth`, `hydration_stale_pool_total`, `watcher_update_age_ms`, `watcher_strategy_latency_ms`, `bundle_execution_retry_events_total`, etc.).

## Data Contracts

- **PoolState** – Canonical runtime struct containing slot, monotonic timestamp, current price/liquidity summary, adapter id, and health flags.
- **PoolUpdate** – Compact struct sent over the channel: `{ pool_id, slot, mid_price, bid_depth, ask_depth, health_flags, gen }`.
- **Borrow config** – `config.json` exposes `protocols.*.borrow_pct` (default 0.35) so StrategyEngine can clamp trade size deterministically.
- **Protocol RPC endpoints** – Each entry in `protocols.<name>.rpc_endpoints` must list the Solana RPC URLs the hydrator can use for one-off refreshes when CoreCast stalls. Provide at least two prioritized endpoints per supported protocol (Raydium, cp_amm, lb_clmm, Whirlpool) so the refresher can fetch vault balances and mark snapshots `RpcRefresh` instead of `Stale`. The template exposes env vars such as `$ENV{TRE_RPC_RAYDIUM_PRIMARY}` to keep credentials out of source; production configs should populate concrete HTTPS URLs.
- **Freshness** – `PoolState` and template/flash artifacts track `last_update_ms`; watcher refuses to trade if `now - last_update_ms > hydrator.max_staleness_ms` or if hydrator flagged the pool as stale.

## Telemetry & Alerting

- **Hydrator ingest** – `hydration_adapter_ingest_total{adapter}`, `hydrator_decode_latency_ms`, and `hydrator_queue_depth` highlight CoreCast speed + backlog.
- **Pool freshness** – `hydration_stale_pool_total{adapter}` counts pools that exceeded `max_staleness_ms`. Alert when non-zero for >1 minute.
- **Watcher health** – `watcher_update_age_ms` (per adapter/protocol) and `watcher_strategy_latency_ms` expose freshness + compute time. Page if p99 update age > 45 ms.
- **Submission** – `bundle_execution_retry_events_total{reason="sign|submit|context"}` and `bundle_execution_latency_ms` track signing/send loops.
- **Sample alert:**
  ```yaml
  - alert: WatcherStalePools
    expr: quantile_over_time(0.99, watcher_update_age_ms[1m]) > 0.045
    for: 2m
    labels:
      severity: critical
      service: tre-arb-bot
    annotations:
      summary: Watcher is trading on stale data
      description: "{{ $labels.environment }} watcher update age p99 >45ms for {{ $value }}s. Hydrator queue depth: {{ $labels.queue_depth }}."
  ```

## Deployment & Testing

1. **Unit tests** – Cover adapter decode paths with fixture accounts and verify borrow clamping math.
2. **Integration tests** – Simulate CoreCast deltas → hydrator queue → watcher to confirm update age + borrow sizing behavior.
3. **Dry runs** – Execute `scripts/run-bot.sh` with submissions disabled; inspect metrics to ensure hydrator queue depth <50%, watcher update age <30 ms, submission retries zero.
4. **Live trading** – Enable submissions once telemetry is green; keep Grafana panels for hydrator queue depth, watcher update age, and bundle results on the same dashboard.

## Future Work

- Add secondary broadcast consumers (e.g., analytics or shadow planners) without impacting hydrator latency.
- Experiment with shared-memory snapshots (mmap) for cross-process watchers if we ever split the binary.
- Layer adaptive borrow sizing on top of the global `borrow_pct` using volatility heuristics when we need finer control.
- Extend telemetry with per-route win/loss tracking to feed dynamic tipping policies.
