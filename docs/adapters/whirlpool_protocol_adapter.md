# Whirlpool Protocol Adapter Design

## Goals & Scope

- Implement a production-ready `ProtocolAdapter` for Whirlpool concentrated-liquidity pools as defined in `idls/pools/whirlpool.json`.
- Normalize CoreCast Whirlpool state into an internal cache so Stage1 strategies can request quotes without bespoke CLMM math.
- Generate deterministic `SwapTemplate` payloads for the Whirlpool `swap` instruction (and `twoHopSwap` later) so Stage2 can bundle transactions without recomputing ticks or PDAs.
- Expose adapter-level health/telemetry signals that describe when Whirlpool liquidity is actionable, stale, rate-limited, or missing tick coverage.

## Responsibilities

1. **State ingestion** – Parse CoreCast `PoolLiquidityChangeStreamMessage` + `DexBlockMessage` extras for Whirlpool pools (sqrt price, liquidity, fee growth, reward data, tick array summaries) and hydrate a `WhirlpoolState` cache.
2. **Metadata hydration** – On first sight of a pool, pull immutable data (token mints, vaults, tick spacing, reward infos, oracle PDA) from the on-chain `Whirlpool` account using the runtime RPC client and store alongside stream-driven deltas.
3. **Tick coverage management** – Track which `TickArray` PDAs are currently cached/loaded so quotes know whether they can traverse the requested price range without fetching additional accounts.
4. **Pricing & quoting** – Provide `quote_exact_in`/`quote_exact_out` implementations that simulate Whirlpool swaps (Q64.64 math, fee tiers, tick crossing) and report slippage, min/max bounds, and remaining liquidity.
5. **Instruction templating** – Produce `SwapTemplate` objects that serialize Whirlpool `Swap` args, order account metas exactly as in `docs/swaps/whirlpool-swaps.md`, and attach any `remaining_accounts` (extra tick arrays, oracle as writable, transfer-hook accounts).
6. **Validation & telemetry** – Emit guardrail statuses (`Ok`, `StaleState`, `MissingTickArrays`, `InsufficientLiquidity`, `RateLimited`) plus structured metrics so strategies can triage failures.

## Inputs & Data Sources

- **CoreCast pipeline** (`bitquery/solana/dex_block_message.proto`, `token_block_message.proto`): Supplies per-slot liquidity, swap fees, reward growth, and tick array summaries for Whirlpool pools.
- **Whirlpool IDL** (`idls/pools/whirlpool.json`): Authoritative account layouts, discriminator bytes, and instruction schemas for `swap`, `twoHopSwap`, `updateFeesAndRewards`, etc.
- **Swap playbook** (`docs/swaps/whirlpool-swaps.md`): Operational checklist for required accounts, error codes, and tick-array semantics the adapter must honor.
- **Config overrides** (`config.json`): Optional hints for mint decimals, custom oracle programs, or fallback tick-array prefetch parameters per pool.

## API Shape

The adapter conforms to the shared trait used by Stage1/Stage2:

```rust
pub struct WhirlpoolAdapter {
    state: DashMap<PoolId, WhirlpoolState>,
    config: WhirlpoolAdapterConfig,
    rpc: Arc<dyn ChainReader>,
}

pub struct WhirlpoolState {
    whirlpool: Pubkey,
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    tick_spacing: i32,
    liquidity: u128,
    sqrt_price_x64: u128,
    fee_rate: u16,
    protocol_fee_rate: u16,
    reward_infos: [RewardInfo; 3],
    last_update_slot: u64,
    tick_arrays: TickArraySet,
    oracle: Pubkey,
}
```

`WhirlpoolAdapterConfig` will capture state TTLs, max tick-array prefetch, fallback fee tiers, and rate-limiter backoff windows.

## State Management Plan

- **Cache layering** – Maintain immutable pool metadata (`StaticPoolMeta`) and mutable liquidity snapshot (`DynamicPoolState`) so hot paths avoid re-parsing large accounts.
- **Tick array tracking** – Model tick coverage as `BTreeMap<TickRange, TickArrayMeta>` storing start tick, last touched slot, and whether the raw on-chain account bytes are cached. This lets quotes verify coverage and request RPC hydration when missing.
- **TTL enforcement** – Reject quotes when `current_slot - last_update_slot` exceeds `config.state_stale_slots` or when required tick arrays have not been refreshed within `config.tick_array_stale_slots`.
- **Oracle readiness** – Record adaptive-fee oracle timestamps; degrade to `StaleState` if `trade_enable_timestamp` is in the future or missing.

## Pricing & Simulation

1. **Direction resolution** – Determine trade direction (`a_to_b`) by matching the request’s input mint with `token_mint_a/b`. Align with the IDL convention (`token A` vs `token B`).
2. **Slot-aligned liquidity** – Combine CoreCast vault deltas with last known on-chain vault balances to derive accurate reserves before simulation. If deltas are missing, perform a lazy RPC fetch.
3. **Tick-walk engine** – Implement a deterministic tick traversal routine mirroring on-chain logic:
   - Start from `tick_current_index` and `sqrt_price_x64`.
   - Load the current `TickArray` slice; iterate ticks in direction (`a_to_b`), consuming liquidity net as we cross initialized ticks.
   - For each step, simulate swap segment using Q64.64 math, subtracting fees and moving price until either `amount` is satisfied or `sqrt_price_limit` is reached.
4. **Exact-in solver** – Treat `amount` as input, deduct trading fees upfront, and iteratively consume liquidity segments until exhausted or tick coverage ends. Return actual output, fee breakdown, next `sqrt_price`, and `edge_bps` relative to mid price.
5. **Exact-out solver** – Reverse the solver by computing the input required to achieve the desired output, rejecting requests that exceed `other_amount_threshold` or run out of liquidity prior to completion.
6. **Slippage & guardrails** – Respect caller-provided `sqrt_price_limit` (set to `0`/`u128::MAX` for open) and short-circuit with `InvalidSqrtPriceLimitDirection` equivalent when incompatible.
7. **Reward & fee accounting** – Update fee growth / reward trackers (if provided) so Stage1 can expose net APR or reward accrual insights without re-reading on-chain state.

## Instruction Templating

- **Instruction choice** – Default to the single-hop `swap` instruction; keep scaffolding ready for `twoHopSwap` once routing is enabled.
- **Args serialization** – Use our Anchor-generated bindings or manual Borsh to encode:
  - `amount`, `other_amount_threshold`, `sqrt_price_limit`, `amount_specified_is_input`, `a_to_b`.
- **Account metas** – Follow the order documented in `docs/swaps/whirlpool-swaps.md`:
  1. `token_program`
  2. `token_authority`
  3. `whirlpool`
  4. `token_owner_account_a`
  5. `token_vault_a`
  6. `token_owner_account_b`
  7. `token_vault_b`
  8. `tick_array0`
  9. `tick_array1`
  10. `tick_array2`
  11. `oracle`
- **Remaining accounts** – Append adaptive-fee oracle (writable), supplemental tick arrays, transfer-hook programs, or memo program entries exactly as described in the IDL `RemainingAccountsInfo` hints.
- **Template metadata** – Embed signer expectations, compute budget recommendations, and tick-array coverage summary inside `SwapTemplate::context` so Stage2 can react when coverage changes mid-bundle.

## Validation & Telemetry

- Quote statuses:
  - `Ok` – State fresh, sufficient liquidity, template built.
  - `StaleState` – Pool/tick data older than configured slot threshold.
  - `MissingTickArrays` – Required arrays absent or stale.
  - `InsufficientLiquidity` – Simulation exhausted liquidity before satisfying request.
  - `RateLimited` – Recent on-chain errors (`6011`, `6035`, `6034`, `6064`) observed; adapter imposes cooldown.
  - `MissingAccounts` – Token owner ATAs, transfer-hook deps, or oracle data unavailable.
- Emit structured metrics (`whirlpool.quote_latency_ms`, `whirlpool.tick_fetch_count`, `whirlpool.rate_limit_hits`) via the notifier module.
- Attach debug context (current sqrt price, tick index, coverage ranges) to log spans for faster incident triage.

## Integration Plan

1. **Proto/schema updates** – Extend `PoolUpdate` and CoreCast parsers to expose Whirlpool-specific fields (sqrt price, liquidity, fee growth, tick array metadata). Update `src/proto.rs` tests.
2. **Adapter scaffolding** – Add `WhirlpoolAdapter` module under `src/strategy.rs` (or `src/core/adapters/whirlpool.rs`) implementing the trait with `todo!()` bodies initially.
3. **State cache + hydration** – Wire `ingest_pool_update` to merge stream deltas, fetch on-chain accounts as needed, and maintain tick-array coverage. Unit-test using fixture accounts extracted from mainnet RPC.
4. **Pricing engine** – Implement `quote_exact_in/out` using a reusable `whirlpool_math` module; validate via proptests comparing against recorded on-chain swaps (`docs/swaps/whirlpool-swaps.md` examples).
5. **Template builder** – Encode account metas and args via the generated IDL bindings; integration-test by simulating swaps on `solana-test-validator` with known pools.
6. **Stage plumbing** – Register the adapter in Stage0 ingestion (pool detection), Stage1 quote aggregator, and Stage2 submission, ensuring config toggles exist in `config.json`.
7. **Observability** – Add tracing spans + metrics; document adapter in `docs/architecture.md` and add runbooks for tick-array cache misses.

## Testing & Tooling

- **Unit tests** – Cover tick traversal, fee calculation, sqrt math, and guard rails using deterministic fixtures.
- **Golden fixtures** – Store raw `Whirlpool` + `TickArray` accounts and expected quote outputs for multiple pools (different tick spacing, Token-2022 vaults, adaptive-fee pools) to guard against regressions.
- **Simulation harness** – Extend existing `runtime::simulate_swap` (or create `tests/whirlpool_swap.rs`) to execute adapter-generated templates against a local validator using idempotent whirlpool accounts.
- **Property tests** – Ensure liquidity conservation (post-swap sqrt price + liquidity match expectations) and that partial coverage errors surface as `MissingTickArrays` rather than panics.
- **Load testing** – Replay historical CoreCast streams to evaluate cache churn, tick-array fetch volume, and quote throughput under mainnet-scale load.

## Risks & Open Questions

- **Tick-array availability** – CoreCast may not stream all arrays needed for a swap path; we need a policy for on-demand RPC fetch vs. prefetch vs. quote rejection.
- **Adaptive-fee oracle writes** – Some pools require writable oracle accounts; determine whether Stage2 supplies them via `remaining_accounts` or the adapter marks them writable by default.
- **Transfer-hook pools** – Token-2022 mints with hooks demand extra accounts; we must standardize how config/IDL hints inject those dependencies.
- **Slot lag** – Delayed CoreCast messages could cause stale sqrt price vs. on-chain reality; consider cross-checking `recent_blockhash` slot before publishing quotes.
- **Two-hop routing** – Future multi-hop support needs a canonical API for passing intermediate whirlpools and tick arrays; design should leave room without breaking trait signatures.
