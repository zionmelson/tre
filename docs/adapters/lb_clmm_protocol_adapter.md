# lb_clmm Protocol Adapter Design

## Goals & Scope

- Deliver a production-ready implementation of the shared `ProtocolAdapter` trait for the `lb_clmm` liquidity-bin CLMM program (`LBUZ…Pwxo`, IDL v0.10.1) documented in `docs/swaps/lb_clmm_swap.md`.
- Normalize CoreCast-derived pool state (bin configs, vault balances, oracle state, dynamic fee params) so the fast execution lane quotes against protocol-agnostic math.
- Emit deterministic `SwapTemplate` payloads for `swap`, `swap2`, `swap_exact_out*`, and `swap_with_price_impact*` inside the hydration lane so bundling requires no lb_clmm-specific knowledge.
- Provide adapter-level validation, risk flags, and telemetry so strategies can filter quotes that violate bin-range, activation, or price-impact constraints.

## Responsibilities

1. **State ingestion** – Parse CoreCast `PoolLiquidityChangeStreamMessage` extras (bin step, active bin id, reserves, oracle window, volatility, fee config) into an `LbClmmState` cache keyed by pool `Pubkey`.
2. **Static hydration** – Backfill immutable metadata (token mints/programs, reserve vault PDAs, bin array PDAs, host-fee settings) from `idls/pools/lb_clmm.json` or a one-time RPC fetch the first time we see a pool.
3. **Pricing** – Offer `quote_exact_in`/`quote_exact_out` solvers that walk bins, apply fee tiers & volatility adjustments, and enforce guardrails (`min_amount_out`, `max_price_impact_bps`).
4. **Instruction templating** – Produce `SwapTemplate` structs that target the correct instruction variant (`swap`, `swap2`, `swap_exact_out`, `swap_with_price_impact`, plus their `*2` hook-aware forms) with ordered account metas, bin-array/bitmap extension wiring, oracle mutability, and serialized args.
5. **Validation & telemetry** – Mark quotes with adapter-specific statuses (`PreActivation`, `PoolDisabled`, `InsufficientBins`, `VolatilityClamp`, etc.) and emit structured metrics for stale state, hook misconfigurations, or repeated error codes (6003/6042/6077).

## Inputs & Data Sources

- **CoreCast stream** (`bitquery/solana/token_block_message.proto`) for per-slot updates on reserves, bin bitmaps, oracle observations, and volatility-derived fee multipliers.
- **lb_clmm IDL** (`idls/pools/lb_clmm.json`) for authoritative layouts (`LbPair`, `BinArray`, `Oracle`, `SwapParameters`, `RemainingAccountsInfo`).
- **Swap reference** (`docs/swaps/lb_clmm_swap.md`) for account ordering, optional accounts, memo + transfer-hook behavior, host-fee rules, and error catalog.
- **Config overrides** (`config.json`) for adapter tuning (bin TTL, oracle horizon, compute-budget overrides, host-fee toggles).
- **Runtime RPC client** (shared by hydration + fast execution lanes) for lazy hydration of bin arrays or verifying Token-2022 hook metadata.

## API Shape

We keep the shared trait but add lb_clmm-specific data structures:

```rust
pub struct LbClmmAdapter {
    state: DashMap<Pubkey, LbClmmState>,
    config: LbClmmAdapterConfig,
    rpc: Arc<RpcClient>,
}

pub struct LbClmmState {
    lb_pair: Pubkey,
    token_x_mint: Pubkey,
    token_y_mint: Pubkey,
    token_x_program: Pubkey,
    token_y_program: Pubkey,
    reserve_x: Pubkey,
    reserve_y: Pubkey,
    bin_step: u16,
    active_bin_id: i32,
    bin_liquidity: BinSnapshotRing,
    oracle: OracleWindow,
    dynamic_fee_bps: u16,
    protocol_fee_bps: u16,
    host_fee_enabled: bool,
    last_update_slot: u64,
    requires_bitmap_extension: bool,
}
```

`LbClmmAdapterConfig` will expose:

- `max_state_age_slots`
- `max_oracle_lag_slots`
- `price_impact_floor_bps`
- `max_bin_walk` (limit to avoid expensive traversals)
- host fee behavior toggles (`enable_host_fee_accounts`, `host_fee_min_size`)

## State Management Plan

- **CoreCast ingestion**: Extend the hydration-lane parser to emit lb_clmm-specific payloads (bin deltas, oracle sample, dynamic fee scalars, pool flags). The adapter subscribes via `ingest_pool_update` and merges deltas into `BinSnapshotRing` (ringbuffer keyed by bin id).
- **Lazy hydration**: On first sight of a pool or whenever bin arrays fall out-of-sync, fetch `LbPair`, `BinArray` (active +- guard range), and `Oracle` accounts via RPC. Cache static PDAs + Token Program IDs so quoting remains lock-free.
- **Bitmap extension handling**: Track when `bin_array_bitmap_extension` is required (pool spans >512 bins) and cache the PDA to avoid deriving during every template build.
- **TTL & invalidation**: Evict or mark state as `StaleState` when `slot - last_update_slot > config.max_state_age_slots`, or when `oracle.exponent` drifts beyond `max_oracle_lag_slots`.
- **Concurrency**: Use `DashMap` for pool-level shards and let pricing functions grab immutable snapshots (Arc clones) to keep fast execution latency low.

## Pricing & Bin Walk Logic

1. **Direction resolution** – Determine swap direction via `QuoteRequest.sell_mint == token_x_mint`. Swap direction dictates whether we traverse bins upward or downward from `active_bin_id`.
2. **Bin traversal** – Walk bins using cached `BinSnapshotRing`, consuming liquidity per bin until `amount_in` exhausted (ExactIn) or target output satisfied (ExactOut). Skip empty bins quickly by checking liquidity bitmaps.
3. **Fee model** – Combine base fee (from `LbPair.fee_parameters`), dynamic fee derived from oracle volatility, and host fee (20% of taker fee when `host_fee_in` present). Apply per-step rounding consistent with on-chain math.
4. **Oracle guards** – Provide `max_price_impact_bps` enforcement by comparing price before first bin and after last bin. Emit `QuoteResponse::status = PriceImpactExceeded` when guard violated.
5. **Slippage** – For `ExactIn`, compute `min_amount_out` suggestion; for `ExactOut`, compute `max_amount_in` along the traversal path. Respect user-provided ceilings in the request.
6. **Partial-fill** – If liquidity exhausts before target satisfied, emit `QuoteResponse::status = InsufficientLiquidity` plus `filled_amount` so strategy can downsize or skip.
7. **Edge metrics** – Derive execution price vs oracle mid-point and compute `edge_bps`. Use this for fast execution ranking and to flag quotes that rely on stale volatility inputs.

## Swap Template Generation

- **Instruction selection**:
  - Default to `swap_with_price_impact2` when a request supplies `max_price_impact_bps` or when either mint uses Token-2022 hooks.
  - Downgrade to `swap2` for vanilla exact-in trades without price guards but still requiring hook plumbing.
- Offer `swap_exact_out` / `swap_exact_out2` for fast execution requests flagged as exact-out; adapter rejects unsupported modes early.
- **Accounts**: Follow the canonical order from `docs/swaps/lb_clmm_swap.md`:
  1. `lb_pair`
  2. Optional `bin_array_bitmap_extension`
  3. `reserve_x`, `reserve_y`
  4. `user_token_in`, `user_token_out`
  5. `token_x_mint`, `token_y_mint`
  6. `oracle`
  7. Optional `host_fee_in`
  8. `user` signer
  9. `token_x_program`, `token_y_program`
  10. Optional `memo_program`
  11. `event_authority`, `program`
- **Remaining accounts**: Build `RemainingAccountsInfo` slices when Token-2022 transfer hooks or reward hooks are required. The adapter inspects mint metadata (cached) to determine hook types.
- **Arguments**: Serialize the appropriate struct (`Swap`, `Swap2`, `SwapExactOut`, etc.) and embed computed guardrails (amounts, `active_id`, `max_price_impact_bps`).
- **Compute budget hints**: Annotate `SwapTemplate` with recommended CU target (e.g., 300k when wide bin walks expected) so the fast execution lane can prepend compute budget instructions.

## Validation & Telemetry

- `QuoteStatus::PreActivation` – pool not yet activated or caller not whitelisted.
- `QuoteStatus::PoolDisabled` – `LbPair` flagged disabled.
- `QuoteStatus::StaleState` – state older than TTL or oracle lagging.
- `QuoteStatus::InsufficientBins` – requested trade exhausts cached bin window; triggers background task to refresh bin arrays.
- `QuoteStatus::VolatilityClamp` – dynamic fee > configured ceiling; optionally skip quoting.
- Log metrics:
  - `lb_clmm.state_refresh_latency`
  - `lb_clmm.quote.bin_walk_bins`
  - `lb_clmm.swap_template.hook_account_count`
  - `lb_clmm.rejection.code` (surface 6003/6042/6077 counts)

## Integration Plan

1. **Schema extensions (Hydration lane)**

   - Update CoreCast proto parsing to include lb_clmm-specific fields (bin deltas, oracle payload, fee params).
   - Add unit tests covering new prost conversions.

2. **Adapter skeleton (Fast execution integration)**

   - Introduce `LbClmmAdapter` struct + trait impl stubs returning `None`.
   - Wire adapter registration in the hydration ingest map and fast execution quote router keyed by program id `LBUZ…Pwxo`.

3. **State cache & hydration**

   - Implement `ingest_pool_update` logic, TTL eviction, and lazy RPC fetches for bin arrays / oracle accounts.
   - Add integration tests that feed fixture CoreCast updates and assert state snapshots.

4. **Pricing engine**

   - Build `lb_clmm_math.rs` with bin-walk solvers (exact-in/out) + helpers for dynamic fee application.
   - Cross-validate results against recorded on-chain swaps (fixtures from `docs/swaps/lb_clmm_swap.md` examples or captured txs).

5. **Template builder**

   - Encode all swap instruction variants via Anchor-generated account structs or manual Borsh serialization.
   - Unit-test account ordering + remaining-accounts slices; run `solana-test-validator` sims to ensure instructions pass.

6. **Lane wiring (fast execution)**

   - Ensure the fast execution lane uses new templates to bundle transactions and respects adapter-provided guardrails (price impact, slippage, host fee address inclusion).
   - Update notifier paths for adapter telemetry.

7. **Docs & rollout**
   - Update `docs/architecture.md` and `docs/swaps/lb_clmm_swap.md` references to note adapter availability.
   - Provide playbook for enabling pools (config toggles, monitoring dashboards).

## Testing & Tooling

- **Unit tests** – Pricing math, fee application, and bin traversal with deterministic fixtures spanning: narrow-bin pools, pools requiring bitmap extension, Token-2022 hooks.
- **Golden fixtures** – Capture CoreCast payload + expected quotes for at least three pools (SPL/SPL, SPL/Token-2022, host-fee enabled). Use them in CI to guard against regressions.
- **Simulation harness** – Extend `runtime::simulate_swap` or add a new `tests/lb_clmm_swaps.rs` that executes adapter-generated `SwapTemplate`s on `solana-test-validator`.
- **Property tests** – Ensure bin traversal preserves invariants (liquidity never negative, price monotonic across bin walk) and that `ExactOut` solver never returns more than `max_in`.
- **Load tests** – Replay a day of CoreCast data to benchmark cache churn, bin-walk cost, and telemetry volume.

## Risks & Open Questions

1. **Bin array coverage** – CoreCast may not stream every bin touched by a large trade. Need fallback RPC fetch + caching policy; otherwise quotes may underestimate price impact.
2. **Dynamic fee accuracy** – On-chain fees depend on oracle observations we might not have yet; consider lag compensation or requiring a fresh oracle snapshot before quoting.
3. **Transfer-hook permutations** – Pools pairing Token-2022 mints with reward hooks increase remaining-account complexity; adapter must gracefully handle missing metadata without panicking.
4. **Activation windows** – Pools can be pre-activation or temporarily disabled. Decide whether adapter proactively polls `LbPair` flags or waits for error feedback.
5. **Compute limits** – Wide bin walks and hook-heavy swaps can exceed default CU. Adapter should surface estimated CU + optionally emit compute-budget instructions.
6. **Telemetry volume** – Bin-level metrics can explode; throttle or aggregate to avoid overwhelming observability sinks.

## Delivery Checklist

- [ ] CoreCast schema + parser updated with lb_clmm fields.
- [ ] `LbClmmAdapter` scaffolding merged with feature flag off by default.
- [ ] State cache hydrated via fixtures + RPC fallback.
- [ ] Exact-in/out pricing validated against on-chain txs.
- [ ] Swap template builder passes `solana-test-validator` sims for all four instruction variants.
- [ ] Hydration + fast execution wiring toggled on for canary pools.
- [ ] Observability dashboards + alerts updated.
- [ ] Documentation (this file + architecture guide) published.
