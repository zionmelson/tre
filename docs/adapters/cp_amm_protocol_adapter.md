# cp_amm Protocol Adapter Design

## Goals & Scope

- Ship a purpose-built implementation of the shared `ProtocolAdapter` trait for the `cp_amm` constant-product pools documented in `docs/swaps/cp_amm_swap.md`.
- Transform CoreCast stream updates + on-chain account fetches into normalized pool state so the fast execution lane’s quoting logic no longer performs protocol-specific math.
- Generate deterministic `SwapTemplate` payloads targeting the `swap2` instruction (supports `ExactIn`, `PartialFill`, `ExactOut`) inside the hydration lane so the fast execution lane can bundle without re-deriving cp_amm knowledge.
- Surface adapter-specific validation + telemetry so strategies understand when cp_amm quotes are actionable, stale, or liquidity constrained.

## Responsibilities

1. **State ingestion** – Parse CoreCast `PoolLiquidityChangeStreamMessage` extras (vault balances, amp/fee config, mint ordering) into a `CpAmmState` cache keyed by `pool_id`, hydrating any missing vault/mint metadata directly from the on-chain `Pool` account via RPC.
2. **Pool hydration** – Optionally backfill immutable metadata (pool authority, token programs, referral config) from `idls/pools/cp_amm.json` or a lightweight on-chain RPC fetch the first time a pool appears.
3. **Pricing** – Offer `quote_exact_in` / `quote_exact_out` calculators that run cp_amm XYK math (including amp curve + dual fee schedule) and emit `edge_bps`, `min_out`, `max_in` guidance.
4. **Instruction templating** – Produce `SwapTemplate` structs for `swap2` with ordered account metas, PDA bumps, serialized `SwapParameters2`, and direction-aware vault wiring.
5. **Validation & telemetry** – Mark quotes with health enums (`Ok`, `StaleState`, `InsufficientLiquidity`, `RateLimited`) and emit structured metrics when guardrails trigger so we can tune strategy thresholds.

## Inputs & Data Sources

- **CoreCast stream** (`bitquery/solana/dex_block_message.proto` + `token_block_message.proto`): supplies per-slot deltas for vault balances, pool amp/fee snapshots, and referral state.
- **cp_amm IDL** (`idls/pools/cp_amm.json`): authoritative account layouts for `Pool`, `SwapParameters2`, and discriminators.
- **Swap guide** (`docs/swaps/cp_amm_swap.md`): operational knowledge (account ordering, SwapMode semantics, constant pool authority) used by the template builder.
- **Config overrides** (`config.json`): optional hints for pools missing CoreCast metadata (mint decimals, referral recipients, token program selection).

## API Shape

We continue to implement the shared trait:

```rust
pub trait ProtocolAdapter {
    fn program_id(&self) -> Pubkey;
    fn supports(&self, program_id: &Pubkey) -> bool;
    fn ingest_pool_update(&self, update: &PoolUpdate);
    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse>;
    fn quote_exact_out(&self, req: QuoteRequest) -> Option<QuoteResponse>;
    fn build_swap_template(&self, quote: &QuoteResponse) -> Option<SwapTemplate>;
}
```

Adapter-specific structs:

```rust
pub struct CpAmmAdapter {
    state: DashMap<PoolId, CpAmmState>,
    config: CpAmmAdapterConfig,
}

pub struct CpAmmState {
    pool: Pubkey,
    pool_authority: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    token_a_mint: Pubkey,
    token_b_mint: Pubkey,
    token_a_program: Pubkey,
    token_b_program: Pubkey,
    amp: u64,
    lp_fee_bps: u16,
    protocol_fee_bps: u16,
    referral_bps: u16,
    vault_a_balance: u128,
    vault_b_balance: u128,
    last_update_slot: u64,
    referral_token_account: Option<Pubkey>,
}
```

`CpAmmAdapterConfig` will hold TTL parameters, fallback decimals, and referral behavior toggles.

## State Management Plan

- Extend `PoolUpdate` to expose cp_amm-specific extensions (amp curve params, fee schedule, referral account). These fields originate from CoreCast metadata; when absent we lazily fetch the on-chain `Pool` account using the runtime RPC client.
- Maintain a `DashMap` cache keyed by pool `Pubkey` so fast execution read paths are lock-free.
- Add TTL eviction (`slot` drift and `Instant` age) plus per-pool dirty flags to avoid quoting from state that predates the latest hydration ingest cycle.
- Record an `Arc<PoolStaticMeta>` alongside mutable balances so serialization (account metas, token program IDs) is O(1).

## Pricing & Simulation

1. **Direction resolution** – Determine whether the strategy is selling `token_a` or `token_b` using mint equality checks against the request. cp_amm does not store canonical direction; we must mirror the caller’s choice.
2. **Normalize decimals** – Convert user inputs to 128-bit fixed-point amounts to avoid overflow when amp > 1.
3. **Exact-in solver**:
   - Start from cached vault balances (adjusted for protocol + referral fees).
   - Apply cp_amm invariant `xy = const` (or `xy = const + amp` depending on pool config) to solve for output after fees.
   - Subtract protocol + referral fees, then enforce `minimum_amount_out` guard.
4. **Exact-out solver** – Rearrange the invariant to solve required input for a target output; bail if exceeding request max or if vault lacks liquidity (`InsufficientLiquidity`).
5. **Partial-fill heuristic** – For `SwapMode::PartialFill`, cap input by `amount_0`, recompute leftover, and emit `amount_left` inside the quote context so the fast execution lane can decide to retry.
6. **Edge computation** – Compare quoted execution price vs mid-price derived from vault balances to produce `edge_bps` for prioritization.
7. **Risk flags** – Track rate-limiter hints (errors 6048/6049) from recent logs and mark quotes as `RateLimited` when we expect rejections.

## Swap Template Generation

- Default to `swap2` so all SwapMode cases are supported; downgrade to `swap` only when the request is `ExactIn` and the target cluster rejects `SwapMode` (unlikely).
- Required accounts (in order) follow the table in `docs/swaps/cp_amm_swap.md`:
  1. `pool_authority` (constant `HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC`).
  2. `pool`, `input_token_account`, `output_token_account`.
  3. `token_a_vault`, `token_b_vault`, `token_a_mint`, `token_b_mint` (matching direction).
  4. `payer`, respective token programs (Token-2022 aware), optional `referral_token_account`, Anchor `event_authority`, cp_amm program ID.
- `SwapParameters2` serialization uses Borsh; we reuse the generated Anchor struct or encode manually via our codec helpers.
- Embed instruction context (payer signer, compute budget suggestions, expected fee) in `SwapTemplate::context` so the fast execution planner knows how to bundle.

## Validation & Telemetry

- `QuoteResponse::status` enumerations:
  - `Ok` – fresh state and solvable trade.
  - `StaleState` – last update slot older than threshold.
  - `InsufficientLiquidity` – vault cannot satisfy request.
  - `RateLimited` – last attempt failed with limiter error, wait for cooldown.
  - `MissingAccounts` – we lack referral ATA or token program hints.
- Attach structured fields: `slots_stale`, `vault_balance_diff`, `fee_bps_applied`, `amount_left`, `expected_compute_units`.
- Emit metrics via `notifier` when we drop CoreCast updates or when quoting falls back to generic XYK math without amp data.

## Integration Plan

1. **Schema updates** – Augment `PoolUpdate` + CoreCast parser with cp_amm metadata fields; add quick unit tests in `src/proto.rs`.
2. **Adapter skeleton** – Introduce `CpAmmAdapter` in `src/strategy.rs` (or dedicated module) with trait stubs returning `None`.
3. **State cache & hydration** – Implement `ingest_pool_update` + lazy RPC fetch for missing metadata; write tests using fixture `cp_amm_pool.json`.
4. **Pricing engine** – Port invariant math into a dedicated `cp_amm_math.rs`; cover `ExactIn`, `ExactOut`, and `PartialFill` via proptests fed by IDL-based fixtures.
5. **Template builder** – Encode `swap2` instruction using our `proto::accounts::Swap2` helper; integration-test against a local validator or RPC simulation harness.
6. **Lane wiring** – Register the adapter inside the hydration lane ingestion map, feed its normalized quotes to the fast execution aggregator, and ensure submission policies consume the emitted templates end-to-end.
7. **Observability** – Add tracing spans + metrics counters; update `docs/architecture.md` with adapter references.

## Testing & Tooling

- **Unit tests** – Math module tests comparing against recorded on-chain swaps (see `tests/corecast_connect.rs` harness for fetching logs).
- **Golden fixtures** – Capture CoreCast payloads + expected quotes for at least three pools (legacy SPL/SPL, SPL/Token-2022, referral-enabled) and run them in CI.
- **Simulation harness** – Extend existing `runtime::simulate_swap` helper to execute cp_amm `swap2` instructions via `solana-test-validator` for regression coverage.
- **Property tests** – Validate invariant preservation (post-swap reserves satisfy XYK within rounding tolerance) and that `PartialFill` never exceeds max input.

## Risks & Open Questions

- **Referral routing** – Not all pools expose referral ATAs via CoreCast; do we maintain a static map or skip referral payouts (risking rejection)?
- **Token program mismatches** – Pools pairing Token-2022 with legacy SPL require dual program IDs; need deterministic selection logic per mint.
- **Rate limiter feedback** – Lack of direct limiter state may cause repeated failures; consider parsing recent transaction logs (fast execution lane) to back off.
- **Compute budget** – cp_amm `swap2` occasionally exceeds default CU when amp is high; decide whether the adapter emits compute-budget instructions inside templates or leaves it to fast execution policy.
