# Whirlpool Protocol Adapter Design

## Upstream References

| Scope              | File / Artifact                                                        | Notes                                                                                                          |
| ------------------ | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| IDL (v0.7.0)       | `idls/pools/whirlpool.json` (sourced from `@orca-so/whirlpools@0.7.0`) | Includes `swap_v2`, `two_hop_swap_v2`, `RemainingAccountsInfo`, and Token-2022 flags.                          |
| Swap math          | `programs/whirlpool/src/math/swap_math.rs`                             | Defines the exact Q64.64 math, fee assessment, and tick traversal logic we must mirror.                        |
| Swap manager       | `programs/whirlpool/src/manager/swap_manager.rs`                       | Orchestrates `SwapTickSequence`, `FeeRateManager`, adaptive fee updates, and tick-array sequencing.            |
| Instruction wiring | `programs/whirlpool/src/instructions/v2/swap.rs` & `two_hop_swap.rs`   | Authoritative account order (three tick arrays + oracle + optional remaining accounts) and argument semantics. |
| Internal docs      | `docs/swaps/whirlpool-swaps.md`                                        | Our execution playbook for required accounts and error-handling expectations.                                  |

These sources drive both the adapter specification and the regression test vectors we capture locally.

## Goals & Scope

- Ship a production-ready `ProtocolAdapter` for Whirlpool pools that matches the on-chain v0.7.0 interface (including Token-2022 vaults and adaptive-fee pools).
- Normalize CoreCast Whirlpool deltas into an internal cache so the fast lane can request quotes without replaying swap math.
- Generate deterministic `SwapTemplate` payloads for both `swap_v2` and staged `two_hop_swap_v2`, ensuring account ordering, signer sets, and remaining accounts align with the IDL.
- Emit fine-grained health signals tracking tick coverage, oracle freshness, adaptive-fee state, and rate limits so strategies can respond before submission failures occur.

## Adapter Responsibilities

1. **State ingestion** – Parse CoreCast `PoolLiquidityChangeStreamMessage` and `DexBlockMessage` extras for Whirlpool pools (sqrt price, liquidity, fee growth globals, reward info snapshots, tick-array cursors) and hydrate a `WhirlpoolState` cache keyed by pool address.
2. **Immutable metadata hydration** – Fetch once per pool via RPC: token mints, vaults, token_program IDs (Token-2022 requires distinct programs), tick spacing, reward infos, oracle PDA, and `whirlpool_bump`. Store under `StaticPoolMeta` to avoid re-decoding.
3. **Tick-array coverage management** – Track cached `TickArray` accounts with `{start_tick, last_slot, bytes}` so the quote path can confirm coverage for three consecutive arrays in the trade direction. Missing arrays trigger either async RPC hydration or a `MissingTickArrays` quote status.
4. **Adaptive-fee tracking** – Mirror `AdaptiveFeeInfo` from `swap_manager.rs` (constants + variables) so quotes can project `fee_rate` adjustments. Defer to on-chain logic when variables are stale or missing.
5. **Pricing & quoting** – Provide `quote_exact_in` / `quote_exact_out` methods that replay `swap_math::compute_swap_step` tick-by-tick, respecting `sqrt_price_limit`, fee tiers, protocol fee, rewards, and liquidity exhaustion semantics defined in `swap_manager.rs`.
6. **Instruction templating** – Emit `SwapTemplate` (and later `TwoHopSwapTemplate`) objects that serialize args exactly as Anchor would (`amount`, `other_amount_threshold`, `sqrt_price_limit`, `amount_specified_is_input`, `a_to_b`), and attach account metas/remaining accounts in the order expected by `swap_v2`.
7. **Validation & telemetry** – Surface guardrail statuses, annotate quotes with tick coverage and adaptive-fee context, and emit structured metrics for slot lag, RPC fallback count, and oracle divergence.

## Inputs & Data Sources

- **CoreCast pipeline** – `bitquery/solana/dex_block_message.proto` and `token_block_message.proto` provide rolling liquidity, fee growth, reward emissions, and tick-array summaries per slot.
- **Whirlpool IDL** – `idls/pools/whirlpool.json` is treated as the single source for account layouts and instruction schemas; codegen (via `anchor-client` or `idl-codegen`) should be refreshed whenever the upstream npm tag changes.
- **Orca program sources** – `swap_math.rs`, `swap_manager.rs`, and the v2 instruction modules govern math correctness and account ordering; the adapter must cite these for parity tests.
- **Internal swap guide** – `docs/swaps/whirlpool-swaps.md` documents runner expectations (ATA ownership, CPI stack, compute budgeting) and remains the consumer-facing contract for template outputs.
- **Config overrides** – `config.json` supplies mint decimal overrides, tick-array prefetch counts, or pool-specific toggles (e.g., ignoring Token-2022 pools on early rollouts).

## API Shape

```rust
pub struct WhirlpoolAdapter {
    state: DashMap<PoolId, WhirlpoolState>,
    rpc: Arc<dyn ChainReader>,
    config: WhirlpoolAdapterConfig,
    metrics: Arc<dyn Notifier>,
}

pub struct WhirlpoolState {
    static_meta: StaticPoolMeta,
    dynamic: DynamicPoolState,
    tick_arrays: BTreeMap<TickRange, TickArrayCache>,
    adaptive_fee: Option<AdaptiveFeeSnapshot>,
}

pub struct StaticPoolMeta {
    whirlpool: Pubkey,
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_program_a: Pubkey,
    token_program_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    tick_spacing: i32,
    oracle: Pubkey,
    reward_infos: [RewardInfo; NUM_REWARDS],
}

pub struct DynamicPoolState {
    liquidity: u128,
    sqrt_price_x64: u128,
    fee_rate: u16,
    protocol_fee_rate: u16,
    tick_current_index: i32,
    last_update_slot: u64,
}
```

`WhirlpoolAdapterConfig` captures TTLs, tick-array fetch budgets, adaptive-fee grace periods, rate-limit backoffs, and whether Token-2022 or transfer-hook pools are allowed.

## State & Cache Management

- **Immutable vs mutable layers** – Static metadata is fetched once (or when IDL hash changes) while dynamic state is refreshed per CoreCast message. This separation keeps quote hot-paths free of lock contention on large account blobs.
- **Tick-array cache** – Represented as `TickArrayCache { start_tick: i32, slot: u64, account: Arc<[u8; TICK_ARRAY_SIZE]> }`. A `BTreeMap` keyed by range enables O(log n) lookups for contiguous arrays and quick detection of gaps relative to the requested sqrt price span.
- **Prefetch policy** – On ingestion, proactively fetch ±N arrays around `tick_current_index / TICK_SPACING` so the fast lane rarely blocks on RPC. Arrays older than `config.tick_array_stale_slots` are re-queued for refresh.
- **Oracle readiness** – The IDL exposes `oracle` as writable when adaptive fee state needs updates. Track the latest oracle timestamp; degrade quotes to `StaleState` when `slot_now - oracle_slot > config.oracle_max_lag`.
- **Concurrency** – Use sharded locks or `RwLock` per pool to avoid blocking unrelated quotes. Tick-array updates should swap in immutable `Arc` blobs to keep readers wait-free.

## Pricing & Simulation

The adapter must match `swap_manager::swap` semantics exactly:

1. **Direction resolution** – Determine `a_to_b` by matching the request input mint against `token_mint_a/b`. Reject if neither matches to avoid silent mint mismatches.
2. **Input normalization** – Convert external token amounts into raw atoms using mint decimals (sourced from `StaticPoolMeta` + config overrides). Align with on-chain math operating in integer atoms.
3. **Tick traversal** – Reimplement the loop from `swap_manager.rs`:
   - Instantiate a `SwapTickSequence` equivalent that iterates cached tick arrays in order.
   - For each initialized tick, adjust `liquidity_net` and recompute the `sqrt_price_next_tick` boundary using `tick_math` utilities.
   - Call into a ported `swap_math::compute_swap_step` to move price/liquidity, capturing `amount_in`, `amount_out`, `fee_amount`, and the next sqrt price.
4. **Adaptive fee** – Feed an internal `FeeRateManager` clone when adaptive pools are enabled. The manager consumes `AdaptiveFeeVariables` and returns the current fee rate and the next variable snapshot (see `swap_manager.rs` for update order). Quotes must include both the fee rate applied and the projected state so telemetry can detect drifts.
5. **Exact-in/out flows** – Mirror the on-chain `amount_specified_is_input` boolean:
   - _Exact-in_: iterate until `amount_remaining == 0` or `sqrt_price_limit` is hit. Report actual output and leftover input if partial fills occur.
   - _Exact-out_: iterate until `amount_calculated >= requested_output`, computing the required input and rejecting when `other_amount_threshold` is exceeded.
6. **Limits & errors** – Match IDL error codes: `InvalidSqrtPriceLimitDirection`, `ZeroTradableAmount`, `PartialFillError`, etc. The adapter should map them into quote statuses but keep raw codes for debugging.
7. **Reward & fee bookkeeping** – Update `fee_growth_global_{a,b}` and reward accumulators if CoreCast streams include deltas; otherwise, rely on RPC snapshots to ensure `next_reward_infos` in templates remain consistent.

## Instruction & Template Generation

- **Instruction selection** – Default to `swap_v2` (single hop). Provide a feature-flagged path for `two_hop_swap_v2` that stitches two pools and six tick arrays once routing is available.
- **Argument encoding** – Use Anchor-discriminated structs from the refreshed IDL to serialize:
  - `amount: u64`
  - `other_amount_threshold: u64`
  - `sqrt_price_limit: u128`
  - `amount_specified_is_input: bool`
  - `a_to_b: bool`
- **Account metas** – Mirror the order in `instructions/v2/swap.rs`:
  1. `token_program_a`
  2. `token_program_b`
  3. `token_authority`
  4. `whirlpool`
  5. `token_owner_account_a`
  6. `token_vault_a`
  7. `token_owner_account_b`
  8. `token_vault_b`
  9. `tick_array_0`
  10. `tick_array_1`
  11. `tick_array_2`
  12. `oracle`
      (Two-hop adds intermediate tick arrays, intermediate whirlpool, and shared token accounts as per the IDL.)
- **Remaining accounts** – Honor `RemainingAccountsInfo`:
  - Adaptive-fee oracle must be writable when fee variables update.
  - Token-2022 transfer hooks require `extra_accounts` before vault metas.
  - Optional compute-unit price hints or CU limit instructions ride ahead of the swap template via `SwapTemplate::prologue`.
- **Template metadata** – Attach `{tick_arrays_used, sqrt_price_before, sqrt_price_after, adaptive_fee_applied, cache_slot}` inside `SwapTemplate::context`. This is consumed by the fast lane to detect mismatched coverage at submission time.

## Validation & Telemetry

- Quote status taxonomy:
  - `Ok`
  - `StaleState` – `slot_now - last_update_slot > config.state_stale_slots`.
  - `MissingTickArrays` – Cache lacks any of the three arrays in the trade path or arrays are older than `tick_array_stale_slots`.
  - `InsufficientLiquidity` – Simulation hit zero liquidity or min/max sqrt price before satisfying the request.
  - `RateLimited` – Prior submissions returned Orca errors (`6011`, `6035`, `6034`, `6064`); adapter backs off for `config.rate_limit_cooldown_slots`.
  - `MissingAccounts` – Token owner ATA, transfer-hook extra accounts, or oracle is absent.
- Metrics to emit per quote: `quote_latency_ms`, `tick_arrays_read`, `rpc_fallback_count`, `adaptive_fee_applied_bps`, `oracle_lag_slots`.
- Telemetry spans should include `pool`, `tick_current_index`, `sqrt_price_x64`, `liquidity`, and `tick_coverage` to accelerate debugging.

## Integration Plan

1. **Proto/schema work** – Extend CoreCast protos + deserializers with Whirlpool-specific fields (sqrt price, fee growth, adaptive fee, tick-array metadata). Update fixtures in `src/proto.rs`.
2. **Adapter scaffolding** – Implement `WhirlpoolAdapter` under `src/hydration/adapters/whirlpool.rs` (or similar), registering it in the hydration dispatcher and fast-lane quote aggregator with feature flags.
3. **Cache hydration** – Build ingestion pipelines that merge CoreCast deltas, backfill missing arrays via RPC, and reconcile state against on-chain accounts during startup (using the refreshed IDL layouts).
4. **Math parity** – Port `swap_math.rs` and supporting helpers (tick math, liquidity math) into `src/strategy/whirlpool_math.rs`. Add proptests comparing adapter quotes to upstream Rust program outputs using recorded account states.
5. **Template builder** – Generate Anchor bindings from `idls/pools/whirlpool.json` so serialization stays in lockstep with upstream. Integration-test by executing adapter-generated swaps on `solana-test-validator` fixtures.
6. **Observability** – Wire adapter metrics into `notifier.rs`, add structured logs, and document runbooks in `docs/architecture.md` for common failure states (stale arrays, oracle lag, rate limit).
7. **Two-hop readiness** – Stash helper utilities for bridging two whirlpools (shared intermediate mint, sequential tick arrays) even if routing is disabled initially, so enabling it later is mostly plumbing.

## Testing & Tooling

- **Unit tests** – Validate tick traversal, fee math, and adaptive-fee transitions using golden values copied from `swap_math.rs` tests.
- **Golden fixtures** – Persist raw `Whirlpool` + `TickArray` accounts plus expected swap outputs for diverse pools (1bps, 5bps, full-range, Token-2022) to guard against math regressions.
- **Simulation harness** – Extend `tests/whirlpool_swap.rs` (new) to replay adapter templates against a local validator seeded with Orca fixture accounts; fail tests if on-chain behavior diverges from adapter simulation.
- **Property tests** – Check liquidity conservation and monotonic tick traversal (never skipping beyond cached coverage) across randomized trades.
- **Load testing** – Replay historical CoreCast logs to measure cache churn, RPC fetch rate, and quote throughput under realistic slot cadence.

## Risks & Open Questions

- **Tick-array availability** – CoreCast streams may omit arrays far from current price. Decide between on-demand RPC fetch (with latency hit), background prefetch, or rejecting quotes with `MissingTickArrays`.
- **Adaptive-fee drift** – If CoreCast omits adaptive variables, we must either re-fetch whirlpool accounts or treat pools as fixed-fee. Need policy per pool tier.
- **Token-2022 hooks** – Pools using transfer hooks require extra accounts; we need config-driven manifests so templates include them without human intervention.
- **Slot lag** – Significant lag between CoreCast slot and latest blockhash can cause stale quotes. Consider sampling on-chain `recent_blockhash` slot before publishing quotes.
- **Two-hop routing** – Defining a stable API for multi-hop swaps (two whirlpools, six tick arrays, shared intermediate ATA) remains open. Leave trait signatures future-proof by allowing multi-instruction templates.
