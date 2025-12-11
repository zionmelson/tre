# Hydration Revamp: "When You're Ready, Fly"

## 🚀 Why
The current hydration pipeline optimizes for speed-by-deadline: every component must refresh inside a tight TTL or we instantly demote the pool, expire templates, and spam readiness warnings. In practice CoreCast delivers partial deltas fast, while RPC backfills take longer or occasionally fail. The result is constant short-circuiting—pools churn between `ready` and `not ready`, templates expire on repeat, and we never reach steady-state quoting.

This document proposes a more patient pipeline that still favors full hydration and hot swap templates, but only once per run. After a component succeeds we keep it until we *know* it is invalid, rather than until an arbitrary timer fires.

## 🎯 Goals
- **Fresh start every run:** caches are empty at boot, but stay warm in-memory for the duration of the run.
- **Eventual readiness:** pools move through predictable stages (Cold → Warming → Ready) without deadline churn.
- **Structural invalidation only:** templates and hydration components refresh when core data changes, not just because time passed.
- **Late freshness enforcement:** ensure slots are recent right before bundling, not hours earlier.
- **Clear operator signals:** metrics/logs track lag without blocking execution.

### CoreCast-first pillars

1. **CoreCast watcher** – boots pools once, records deltas, and never demotes by TTL.
2. **Template & resource builder** – mints immutable templates + lookup tables keyed by content hash.
3. **FastLane planner** – evaluates CoreCast state plus trade hints, applying slot-lag guards right before scheduling.
4. **Bundle builder & submitter** – signs bundles, enforces tip policy, and tracks the Jito lifecycle.

## 😬 Pain Today
| Pain | Cause | Effect |
| --- | --- | --- |
| Pools oscillate between ready/not-ready | Legacy readiness registry invalidates on slot/TTL deadlines | Planner never schedules quotes
| Templates expire every few seconds | Epoch counter advances even when data unchanged | Rebuild storm, wasted RPC
| Hydration WARN spam | Each pending component logs per pool per second | Hard to see real issues
| Startup inherits stale cache | On-disk template cache + readiness state reused | Bot re-enters with conflicting history
| End-to-end latency double counted | We enforce freshness twice (hydration + submission) | No net benefit, extra rejects

## 🧠 Mental Model
```
CoreCast delta ─► PoolSnapshot (Cold)
                 └─ Hydrator fetches components asynchronously
                         │
                         ├─ Meta + Liquidity ready → Warming
                         ├─ Tick arrays / fees ready → still Warming
                         └─ All required components successful once → Ready
```
- A pool never regresses unless **explicitly invalidated** (bad data, structure change, operator reset).
- Templates bind to a **content hash** (program id + account list + template params). When the hash changes, we rebuild; otherwise the template lives until process exit.
- Submission-time freshness check ensures we do not trade on slots older than a configured delta.
- These tiers bubble all the way up to the runtime architecture: stage-level docs, dashboards, and planners treat a pool as tradable once its readiness tier reaches `Ready`. Slot lag is evaluated at submission time via the execution guard (`MAX_EXECUTION_SLOT_LAG`), so hydration never demotes a pool just because time passed.

## 📐 Proposed Changes
### 1. Startup Reset
- Add `HydrationService::reset()` invoked during bot boot.
- Clear: `PoolStore`, adapter states, template caches (`TemplateCache`), readiness registry, snapshot files.
- Guarantee that each run starts as if we had never seen any pools.

### 2. Readiness Tiers
| Tier | Definition | Behavior |
| --- | --- | --- |
| Cold | CoreCast delta only | Stored in `PoolStore`, hydration kicked off
| Warming | ≥1 required component complete | Continue warming others, templates may pre-build if accounts known
| Ready | All required components succeeded at least once | Planner may quote/submit using cached data

Implementation:
- Extend the readiness registry with a `tier()` helper based on component completion.
- `ReadinessRegistry` records first transition to Ready and never flips back unless `invalidate(pool_id)` is called.

### 3. Explicit Invalidation Rules
Invalidate a pool only when:
1. **Structural change:** CoreCast reports new vault/tick-array/fee accounts → template hash changes.
2. **Logical corruption:** RPC hydration returns malformed data or a component checksum fails.
3. **Operator action:** manual command or runtime reset.
4. **Slot reversal:** CoreCast delivers slot < cached slot (rare but indicates reorg/out-of-order feed).

Otherwise, keep the last known-good component forever (until reset).

### 4. Submission-Time Freshness Gate
- Before planner enqueues a bundle, compare `current_tip_slot - pool.snapshot.slot`.
- If lag > `MAX_EXECUTION_SLOT_LAG` (e.g., 400), skip that pool for this attempt.
- This replaces hydrator TTL enforcement and keeps quotes honest without penalizing pools mid-warmup.

### 5. Template Lifetime = Content Hash
- Compute `TemplateContextHash = hash(program_id || ordered account pubkeys || swap params version)`.
- Cache templates by hash; reuse until hash changes.
- When hydration publishes a new account set (e.g., tick arrays rotated), emit `TemplateUpdate::Invalidate(hash)` so planner rebuilds only once.

### 6. Telemetry & Logging
- Replace WARN spam with counters:
  - `hydration_pool_tier{tier}`
  - `hydration_component_age_ms{component}`
  - `pool_execution_slot_lag`
- Log once when a pool enters Ready, once when invalidated.
- Surface a dashboard card: "Pools warming" vs "Pools ready".

## 🔧 Module Touchpoints
| Module | Changes |
| --- | --- |
| `hydration/types.rs` | Add tier enum, remove TTL-driven `SnapshotFreshness::Stale`, store first-ready timestamps |
| `hydration/pool_state.rs` | Keep snapshots resident until restart; readiness depends solely on the bootstrap registry |
| `hydration/service.rs` | Implement reset hook, orchestrate tier transitions |
| `hydration/template_cache.rs` | Key cache by content hash, expose invalidate signal |
| `planner/*` | Replace readiness TTL checks with tier gating + submission-time slot guard |
| `telemetry.rs` | New metrics for tiers and component ages |

## 🧭 Rollout Plan (status: shipped)
1. **Always on:** the `hydration.when_ready_fly` flag has been removed—startup cache resets and tiered readiness now run every time with no extra tag or config toggle.
2. Implement reset + tiering path, run in dry mode to validate metrics, and keep dashboards aligned (complete).
3. Enable in staging, compare `pool_ready_count`, `bundle_attempts`, and `rpc_errors` vs baseline (complete).
4. Ship to production permanently and delete the remaining TTL compatibility paths (complete).

## ✅ Success Criteria
- Pools no longer oscillate; `ready` pools stay ready until invalidated.
- Template rebuild rate drops to near structural change rate.
- Bundle submission success increases (fewer readiness rejects).
- Operator dashboards show fewer WARNs but retain visibility into lag via metrics.

---
**TL;DR:** let hydrator work at its own pace, keep everything in-memory once it succeeds, and defer freshness enforcement to the moment we actually trade. When the pool is ready, we fly.
