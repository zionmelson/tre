# Lifinity AMM v2 Protocol Adapter Design

## Goals & Scope

- Deliver a protocol-specific implementation of `ProtocolAdapter` targeting the Lifinity AMM v2 program (`2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c`).
- Translate CoreCast updates plus on-chain metadata into deterministic quotes so Stage1 no longer embeds Lifinity-specific math.
- Produce pre-built `SwapTemplate` payloads (accounts, bumps, serialized args) so Stage2 can submit swaps without recomputing oracle spread logic.
- Surface adapter-specific validation, health signals, and telemetry to gate strategies when pools are frozen, stale, or oracle-constrained.

## Responsibilities

1. **State ingestion** – Decode Lifinity-specific CoreCast extras (vault balances, config spreads, oracle hints) into an adapter-owned cache keyed by pool `Pubkey`.
2. **Oracle alignment** – Track the three oracle accounts (`oracle_main/sub/pc`) and their slot/confidence limits so quotes can flag mismatches before submission.
3. **Pricing & quoting** – Implement Lifinity curve math (fee/trade spread, price buffers, regression coefficients) for `quote_exact_in` / `quote_exact_out` flows.
4. **Instruction templating** – Generate deterministic `SwapTemplate` structs for the `swap` instruction, including ordered `AccountMeta`s and serialized `(amount_in, minimum_amount_out)` payloads.
5. **Validation & telemetry** – Emit adapter-specific health statuses (`Freeze`, `OracleMismatch`, `StaleState`, etc.) plus structured logs when guardrails trigger.

## Inputs & Data Sources

- **CoreCast stream** (`bitquery/solana/dex_block_message.proto`, `parsed_idl_block_message.proto`): provides slot-by-slot vault deltas, fee config, oracle info, and freeze bits from the Lifinity AMM state.
- **IDL artefacts** (`idls/pools/lifinity_amm_v2.json`): authoritative account layouts, fee curves, oracle limits, and instruction discriminators.
- **Swap playbook** (`docs/swaps/lifinity_amm_v2_swap.md`): operational account ordering, error codes, and pre-flight requirements.
- **Static config** (`config.json` / per-strategy overrides): TTL thresholds, oracle grace windows, and pool allowlists.

## API Surface

The adapter implements the shared trait already documented in `docs/architecture.md`:

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

Planned structs:

```rust
pub struct LifinityAdapter {
    state: DashMap<Pubkey, LifinityPoolState>,
    config: LifinityAdapterConfig,
}

pub struct LifinityPoolState {
    amm: Pubkey,
    authority: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    pool_mint: Pubkey,
    fee_account: Pubkey,
    token_program: Pubkey,
    oracle_main: Pubkey,
    oracle_sub: Pubkey,
    oracle_pc: Pubkey,
    fees: AmmFees,
    curve: AmmCurve,
    config: AmmConfig,
    vault_a_balance: u128,
    vault_b_balance: u128,
    last_update_slot: u64,
    freeze_trade: u8,
}
```

`LifinityAdapterConfig` will carry TTLs, oracle tolerance multipliers, and per-pool overrides (e.g., custom spread coefficients or token decimals when CoreCast omits them).

## State Management Plan

- **Ingestion** – Extend `PoolUpdate::from_corecast` to capture Lifinity-only fields (freeze bits, `std_spread`, `std_spread_buffer`, oracle slot limits). `ingest_pool_update` mutates/creates `LifinityPoolState` entries and marks them "fresh" for quoting.
- **Hydration** – On first sighting of a pool (or when CoreCast omits static data), fetch the on-chain `Amm` account via the runtime RPC client and hydrate immutable fields (`authority`, vault pubkeys, oracle keys, mints).
- **TTL/consistency** – Reject quotes when `slot_now - state.last_update_slot` exceeds `config.max_slot_lag` or when we have not seen consistent oracle tuples for `config.min_confirmed_updates` consecutive slots.
- **Concurrency** – Use `DashMap` (or `slab::Slab`) to allow lock-free reads during Stage1 evaluation; writes happen inside Stage0 ingestion tasks guarded by per-pool entries.

## Pricing & Quote Semantics

1. **Direction resolution** – Match the user's `token_in` mint to either `token_a` or `token_b` vault; determine whether the trade is `A→B` or `B→A` to pick the correct oracle reference price.
2. **Oracle alignment** – Pull cached oracle prices (from Stage0 ingest or on-demand Pyth client) and ensure they satisfy the thresholds in `config.oracleMainSlotLimit`, `oracleSubConfidenceLimit`, and `oraclePcSlotLimit`. Mark quotes as `OracleMismatch` if any feed is stale or out-of-band.
3. **Curve math** – Start from vault balances, apply `AmmFees` (trade + owner + host) and the dynamic spread defined by `config.std_spread`, `std_spread_buffer`, and `spread_coefficient`. Use the oracle-driven regression price (`config.last_balanced_price`, `rebalance_ratio`) to translate `amount_in` into `amount_out` while respecting pool caps.
4. **Slippage guard** – Compute `minimum_amount_out = quote_out * (1 - user_slippage_bps)`, but never below the protocol-required floor derived from `oracle_pc_account` price buffer.
5. **Exact-out solver** – Implement an inverse solver that iteratively guesses input based on oracle price and pool ratio, backing off if estimated input exceeds request max or pool cap; flag `InsufficientLiquidity` when the solver cannot converge.
6. **Result packaging** – Return `QuoteResponse` with: `amount_in`, `amount_out`, `edge_bps` (vs oracle mid), recommended `minimum_amount_out`, `fee_paid`, oracle snapshot metadata, and adapter health status.

## Instruction Template Builder

- Base the template on the `swap` instruction described in `docs/swaps/lifinity_amm_v2_swap.md`.
- Account ordering (read/write) exactly mirrors the IDL table: `authority`, `amm`, `user_transfer_authority`, `source_info`, `destination_info`, `swap_source`, `swap_destination`, `pool_mint`, `fee_account`, `token_program`, `oracle_main`, `oracle_sub`, `oracle_pc`.
- Serialize instruction data as two little-endian `u64`s: `amount_in`, `minimum_amount_out`. The adapter supplies `minimum_amount_out` from the quote result so Stage2 does not recompute.
- Include a `TemplateContext` struct carrying signer requirements, compute budget estimates (Lifinity swaps are lightweight but we track historical CU), and hints for Stage2 (e.g., `requires_approval = true`).
- Provide `AccountMeta::new_readonly` vs `new` decisions based on the swap guide; ensure PDAs such as `authority` are derived once and cached alongside state.

## Validation, Guard Rails & Telemetry

- Quote statuses: `Ok`, `FreezeTrade`, `StaleState`, `OracleMismatch`, `InsufficientLiquidity`, `ExceededPoolCap`, `MissingMetadata`.
- Per-quote telemetry payload: pool pubkey, slot age, vault balances, oracle prices/confidence, applied spread, computed edge, and reason for any downgrade.
- Hook into existing notifier metrics so we can alert when:
  - Freeze bits toggle from 0→1 mid-session.
  - Oracle feeds fall outside configured slot/confidence bounds for >N slots.
  - Adapter falls back to generic XYK math due to missing oracle data.

## Integration Plan

1. **Schema extension** – Update `PoolUpdate` + protobuf bindings (`bitquery/solana/dex_block_message.proto`) to expose Lifinity-specific config/fee/oracle fields.
2. **Adapter scaffolding** – Add `lifinity` module under `src/strategy.rs` (or new `src/protocols/lifinity.rs`) implementing the trait with stubbed pricing; wire it into the adapter registry keyed by program id.
3. **State hydration** – Implement RPC-backed fetch of the on-chain `Amm` account for new pools and persist immutable metadata.
4. **Pricing engine** – Port Lifinity math (oracle-informed spread, fee schedule) into a dedicated helper (e.g., `lifinity_math.rs`) with deterministic tests derived from recorded swaps.
5. **Template builder** – Encode the `swap` instruction using our existing serialization helpers; add golden tests comparing produced `AccountMeta`s to the table in `docs/swaps/lifinity_amm_v2_swap.md`.
6. **Stage wiring** – Register the adapter in Stage0 ingestion, Stage1 quoting, and Stage2 submission pathways so opportunities referencing Lifinity pools automatically leverage it.
7. **Observability** – Add tracing spans + metrics counters specific to Lifinity (e.g., `lifinity.oracle_stale`, `lifinity.freeze_tripped`) and document alert thresholds.

## Testing Strategy

- **Unit tests** – Deterministic math tests validating `quote_exact_in`/`quote_exact_out` across representative pool configurations (balanced, imbalanced, cap-bound). Use fixtures built from real CoreCast payloads.
- **Property tests** – Ensure oracle-informed price solves remain monotonic (increasing input produces non-decreasing output) and that curve invariants remain within tolerance.
- **Golden template tests** – Snapshot the serialized `SwapTemplate` (accounts + data) and compare against known-good swaps in CI.
- **Simulation harness** – Extend `runtime::simulate_swap` (or Stage2 forknet harness) to execute Lifinity swaps with recorded accounts to validate instruction correctness end-to-end.
- **Soak tests** – Replay historical CoreCast logs to confirm adapter keeps up with Stage0 throughput and does not leak state.

## Risks & Open Questions

- **Oracle availability** – We depend on three oracle feeds; do we ingest their data inside Stage0 or pull lazily per quote? Latency vs staleness tradeoff needs clarification.
- **Curve parameters** – Some pools may change `curve.curve_parameters` or use non-standard spread coefficients; we may need a migration plan for new curve types.
- **Freeze semantics** – When `freeze_trade` flips during bundle construction, Stage2 may submit stale instructions; consider sampling the `Amm` account again in Stage2 or adding a short-lived cache invalidation signal.
- **Token program variants** – Lifinity pools can point to Token-2022 program IDs; adapter must respect per-mint token program hints to avoid `IncorrectTokenProgramId (6024)` errors.
- **Pool cap enforcement** – `OverCapAmount (6036)` requires tracking `config.deposit_cap` vs outstanding LP supply; decide whether adapter enforces this pre-quote or allows Stage2 to handle failures.
