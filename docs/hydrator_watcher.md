# Hydrator ↔ Watcher Design

## Goals

- Eliminate legacy Stage1 planner path and keep only hydrator + watcher lanes.
- Maintain sub-45 ms end-to-end latency from CoreCast update to watcher decision.
- Keep all live pool state in volatile memory; JSON ALT registry remains template metadata only.
- Provide clear wiring, config knobs, and telemetry so we can iterate safely.

## High-Level Flow

```
          CoreCast Streams
                 |
        +-------------------+
        | Hydrator Workers  |
        | (per adapter)     |
        +-------------------+
                 |
        Decode & derive pool state
                 |
        +-------------------+
        | Runtime Pool Map  |  <-- Arc<ShardMap<PoolId, PoolState>>
        +-------------------+
                 |
      broadcast PoolUpdate structs
                 |
        +-------------------+
        | Lock-Free Channel |
        +-------------------+
                 |
         drain / apply updates
                 |
        +-------------------+
        | Watcher (FastLane) |
        |  StrategyEngine    |
        |  GraphManager      |
        +-------------------+
                 |
            Bundle Slab
                 |
            Submitter
```

## Component Details

### Hydrator

- **CoreCast ingest:** Each adapter worker subscribes to the relevant CoreCast topic. Messages include `(slot, account_pubkey, owner, data)`.
- **Adapter decode:** Existing modules under `src/hydration/adapters/` parse reserves, ticks, bins, and compute derived fields (mid-price, per-side liquidity, fees, volatility markers). Latency target <100 µs per update when hot.
- **Runtime pool map:** Shared in-memory structure (we can re-use `DashMap` or a sharded slab) keyed by pool id. Stores latest slot, derived price/liquidity, per-side depth, health flags, and timestamps.
- **Broadcast channel:** After updating the map, hydrator enqueues a compact `PoolUpdate` struct into a bounded lock-free MPMC queue. Fields: `pool_id`, `slot`, `mid_price`, `bid_depth`, `ask_depth`, `volatility_flag`, `gen_counter`.
- **Staleness handling:** A background task scans the map for pools that have not updated within `hydrator.max_staleness_ms` (config). Mark stale pools so watcher skips them or trigger on-demand RPC refresh.
- **Telemetry:** Per adapter counters (`hydration_adapter_ingest_total`), decode latency histogram, queue depth gauge, stale pool count, dropped update counter.

### Watcher (Fast Lane)

- **Feed subscription:** A lightweight thread drains the channel and applies updates to its local read-only view (can be the same map via `Arc` or a copy-on-write cache).
- **Strategy inputs:** StrategyEngine pulls the latest pools for route construction. Every pool entry carries slot/time metadata, so we can drop anything older than the 45 ms SLA.
- **Borrow sizing:** `config.json` gains per-protocol `borrow_pct` (default 0.35). Effective borrow = `available_liquidity * borrow_pct`, clamped by any protocol-specific caps already in StrategyEngine.
- **GraphManager integration:** Extract the reusable logic from `src/stages/stage1_eval.rs` into a new `planner/core.rs` module so watcher can invoke it without duplicating code. Watcher feeds it the runtime map snapshot.
- **Bundle flow:** Opportunities enter `bundle::tracker` (slab) and flow to the existing submitter/builder. JSON ALT registry remains the source of tx template metadata only.
- **Telemetry:** Histograms for hydrator→watcher latency (update age when processed), strategy evaluation time, bundle build time, and success/failure counts. Alerts trigger if queue lag > threshold.

## Interfaces & Data Structures

- `PoolState`: `{ slot, last_ts, mid_price, bid_depth, ask_depth, fee_bps, volatility_flag, adapter_id, extra }`.
- `PoolUpdate`: slim struct for channel use; contains only fields watcher needs to refresh local view quickly.
- Channel sizing: default 8192 entries, configurable. Drop-oldest policy to prevent hydrator stalls; emit metric.
- `PlannerCore`: new module exposing `quote_routes(pool_view, config) -> Vec<Opportunity>` used by watcher.

## Migration Steps

1. **Remove Stage1:** Delete `src/stages/stage1_eval.rs` and strip runtime wiring. Port any reusable logic into `planner/core.rs`.
2. **Build runtime pool map:** Implement shared struct + background staleness scanner inside `src/hydration/service.rs`.
3. **Add broadcast channel:** Wire hydrator workers to push `PoolUpdate` messages; add queue metrics.
4. **Watcher rewiring:** Update `src/planner.rs` (and downstream modules) to consume the new feed, load config borrow pct, and stop referencing legacy caches.
5. **Telemetry updates:** Extend `src/telemetry.rs` with new gauges/histograms; add Grafana panels.
6. **Testing:** Unit tests for adapter decode, channel fan-out, watcher staleness handling, and strategy borrow clamp. Integration dry-run to confirm <45 ms latency.
7. **Runbook:** Update `README.md`/docs to mention new startup sequence (hydrator priming signal before watcher trades).

## Risks & Mitigations

- **Queue saturation:** Bounded channel might overflow during market spikes. Mitigate with generous sizing, per-adapter backpressure metrics, and drop-oldest policy.
- **Stale pools:** Without persistence, restarts rely on CoreCast snapshots. Ensure hydrator waits for an initial snapshot before green-lighting watcher.
- **Contention:** Shared map must be sharded to avoid lock contention at high throughput. Evaluate `DashMap` vs custom sharded arrays.
- **Telemetry noise:** Add clear labels (adapter, protocol) so we can pinpoint laggy decoders quickly.

## Timeline Snapshot

- Day 0–1: Delete Stage1, stand up new `planner/core.rs` module.
- Day 1–2: Finish runtime pool map + broadcast channel + hydrator metrics.
- Day 2–3: Wire watcher to feed, integrate borrow pct config, re-enable bundle flow.
- Day 3–4: Add telemetry panels, run tests/dry runs, iterate on queue sizing.
- Day 4+: Live trading with observability guardrails.
