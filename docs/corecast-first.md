# CoreCast-First Runtime Redesign

This document captures the proposed simplification of the TRE executor so that pools hydrate exactly once, templates never churn, and CoreCast deltas become the only low-latency signal used for pricing and detection.

## Problem Statement

The current runtime promotes and demotes pools continuously based on short TTLs, RPC retries, and template epochs. Even though CoreCast streams already deliver the state we need, the planner still waits on RPC backfills and hydration timers that were designed for unreliable feeds. The result is:

- Pools bouncing between `ready`/`not-ready` despite no structural change.
- Template caches rebuilding on every small delay, burning CPU and RPC quota.
- Detection latency tied to RPC responsiveness instead of stream arrival time.

We want a design in which each pool performs a single bootstrap, then lives entirely off streamed deltas.

## Pillars

The CoreCast-first runtime collapses into four always-on pillars:

1. **CoreCast Watcher** – Boots a pool once, keeps `PoolStore` fresh, never demotes by TTL.
2. **Template & Resource Builder** – Generates immutable templates, lookup tables, and flash routes referenced by content hash.
3. **FastLane Planner** – Evaluates streamed state plus trade hints, enforces slot-lag guards, and emits bundle candidates.
4. **Bundle Builder & Submitter** – Signs candidates, enforces tip policy, and tracks results from the Jito block engine.

## Core Principles

1. **Single-shot bootstrap** – When a pool first appears we fetch only the static accounts that never change shape (tick arrays, vault metadata, IDL-derived constants). Once captured, these artifacts persist until the process restarts or the program definition changes.
2. **Stream-owned freshness** – CoreCast deltas are authoritative for prices, positions, and account balances. The planner never blocks on RPC after bootstrap; it simply consumes the live stream.
3. **Templates are immutable** – A swap template is keyed by `(program, pool address, direction)`. After it is generated the first time, it is reused forever unless the content hash changes.
4. **Planner-level guards** – Freshness checks shift to the planner/submitter; if the latest CoreCast slot is too old relative to the tip floor, we skip an opportunity. Hydration never demotes pools on its own.
5. **Optional watcher** – A tiny background watcher may refresh static accounts if they legitimately rotate (e.g., a protocol adds a new tick array). It cannot demote pools or alter templates directly; it only queues a rebuild when structural data clearly changed.

## Data Flow

```
           CoreCast (pool + trade streams)
                   |
             +-------------+
             | Pool Watcher|
             +-------------+
                   |
            in-memory Pool Store
                   |
            +-------------------+
            | FastLane Planner  |
            +-------------------+
                   |
            Bundle Builder/Signer
```

1. **Pool Watcher**
   - Subscribes to CoreCast pool deltas.
   - On first sighting of a pool: fetches static accounts (if not already cached) using throttled RPC. This is the *only* RPC interaction.
   - Writes a `PoolRecord { static_accounts, last_dynamic_state }` into the pool store. Once `static_accounts` exists, watcher marks the pool as `hydrated` and never revisits RPC.
   - Emits `TemplateNeeded(pool, direction)` events for the builder once per direction.

2. **Template Builder**
   - Listens for `TemplateNeeded` events.
   - Builds the Whirlpool/Raydium/etc. atomic instruction sequences once, persists them under their content hash, and reports success to the planner.
   - Any later structural change must emit `TemplateInvalidate(hash)` explicitly; there is no TTL.

3. **Planner + Strategy**
   - Consumes the continuously updated dynamic fields from CoreCast (liquidity, sqrt price, tick, fees).
   - Because templates are already pinned, planner can evaluate and build bundles as soon as profitability thresholds are met.
   - Before submission, the planner compares `tip_slot - pool.latest_slot` to guard against stale deltas.

4. **Submitter**
   - Signs bundles using cached templates and ALT data.
   - Does **not** trigger any hydration path; if a structural rebuild is necessary, the template builder handles it asynchronously.

## Watcher Responsibilities

- Maintain a key-value store: `pool_pubkey -> StaticPoolSnapshot`.
- Provide helper APIs for adapters to access decoded static data.
- Retry bootstrap RPC calls until success, backing off but never failing a pool permanently.
- Detect structural mutations by hashing the ordered account set; if a new hash appears, enqueue a template invalidate/rebuild but leave the pool marked `hydrated` until the new template is ready.

## Telemetry Expectations

- `pool_bootstrap_total{status}` – counts of pools that completed the one-time RPC bootstrap.
- `corecast_delta_lag_ms` – measures how fresh streamed updates are; replaces hydration TTL warnings.
- `template_build_total{protocol,status}` – reveals true rebuild rate (should be `1` per pool per direction most of the time).
- `watcher_static_miss_total` – alerts if static account hashes suddenly go missing, indicating a protocol upgrade.

## Migration Plan

1. **Freeze current hydration** – gate existing demotion logic behind a feature flag and default it off.
2. **Implement bootstrap cache** – add a `StaticPoolRegistry` with disk-backed persistence (optional) so restarts do not redo RPC unnecessarily.
3. **Adapt adapters** – ensure Raydium/Whirlpool/cp_amm adapters can derive every runtime value from CoreCast + cached static data.
4. **Switch planner gating** – rely solely on CoreCast slot lag checks instead of hydration tiers.
5. **Remove hydration service** – delete the old multi-tier hydration pipeline once the new watcher + bootstrap path is stable.

## Open Questions

- **Tick-array growth:** if a concentrated pool activates new tick arrays outside the cached set, can CoreCast signal the change? If not, we may need a daily watcher job that diff-checks RPC state for structural additions.
- **Cache persistence:** do we require templates/static data to survive process restarts? If yes, add a lightweight sled/JSON file to store the bootstrap artifacts.
- **Multi-process coordination:** when multiple bot instances run, ensure only one performs the bootstrap while others reuse the stored snapshot to avoid API rate limits.

## Summary

By trusting CoreCast for live data and constraining RPC to a one-time bootstrap, we cut the runtime back to its essentials: decode the stream, evaluate strategies, and submit bundles. Templates become immutable artifacts, pools never bounce between tiers, and detection latency returns to the <40 ms budget that motivated the project in the first place.
