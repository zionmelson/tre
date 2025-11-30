# Raydium AMM v3 Protocol Adapter Design

## Goals & Scope

- Provide a protocol-specific implementation of the upcoming `ProtocolAdapter` trait for Raydium AMM v3 (CLMM) pools.
- Surface precise pricing data (mid, bounds, liquidity-aware slippage) derived from CoreCast pool updates and protocol math documented in `docs/swaps/amm_v3_swaps.md`.
- Generate deterministic swap instruction templates inside the hydration lane so the fast execution planner can serialize them without re-deriving protocol knowledge.
- Deliver confidence scores + validation hooks so downstream strategy code knows whether an arbitrage quote is executable.

## Responsibilities

1. **State ingestion** – Parse `PoolLiquidityChangeStreamMessage` extras (tick index, sqrt price, vault balances, fee growth) into a `RaydiumV3State` cache keyed by `pool_id`.
2. **Pricing** – Convert cached state + incoming trade amounts into quote functions: `quote_exact_in`, `quote_exact_out`, `edge_bps`, `sqrt_price_limit` suggestions.
3. **Instruction templating** – Produce `SwapTemplate` structs containing: Raydium program id, discriminator (`swap_v2`), ordered account metas, PDA bumps, and argument payloads for a given direction.
4. **Validation** – Simulate pool math (tick traversal, liquidity net checks) to ensure the predicted fill stays within slippage + liquidity bounds.
5. **Telemetry** – Emit structured logs / metrics when adapter falls back (missing metadata, stale state) to aid observability.

## Inputs

- **CoreCast Stream**: `bitquery/solana/dex_block_message.proto` + `parsed_idl_block_message.proto` provide the raw liquidity change and metadata fields (pool config index, tick spacing, sqrt_price_x64, vault balances). We will extend `PoolUpdate::from_corecast` to capture these adapter-specific fields.
- **Swap Playbook**: `docs/swaps/amm_v3_swaps.md` outlines PDA derivations and instruction arguments.
- **Config Overrides**: Optional static hints (fee tiers, mint decimals) from config or IDL for pools where CoreCast lacks data.

## API Shape

```rust
pub trait ProtocolAdapter {
    fn program_id(&self) -> Pubkey;
    fn supports(&self, program_id: &Pubkey) -> bool;
    fn ingest_pool_update(&self, update: &PoolUpdate);
    fn quote_exact_in(&self, req: QuoteRequest) -> Option<QuoteResponse>;
    fn build_swap_template(&self, quote: &QuoteResponse) -> Option<SwapTemplate>;
}
```

Adapter-specific structs:

```rust
pub struct RaydiumV3Adapter {
    state: DashMap<String, RaydiumV3State>,
    config: RaydiumAdapterConfig,
}

pub struct RaydiumV3State {
    pool_id: String,
    amm_config: Pubkey,
    tick_spacing: i32,
    sqrt_price_x64: u128,
    current_tick: i32,
    liquidity: u128,
    vault_base: u64,
    vault_quote: u64,
    last_update_slot: u64,
    observation_bump: u8,
    pool_bump: u8,
    vault_bumps: [u8; 2],
    mint_base: Pubkey,
    mint_quote: Pubkey,
}
```

## State Management

- Extend `PoolUpdate` to carry adapter-specific extensions (from CoreCast metadata fields) and feed them into `RaydiumV3State::apply_update`.
- `DashMap` (or lock-free slab) keyed by pool ensures thread-safe reads in the fast execution lane planner.
- Implement TTL eviction to avoid quoting from stale pools (e.g., `slot` drift or `Instant` age > N ms).

## Pricing Logic

1. **Normalize direction** – Determine whether the trade is base→quote or quote→base based on mint ordering vs trade intent.
2. **Exact-in simulation** – Starting from `sqrt_price_x64` and `current_tick`, iterate Raydium CLMM math:
   - For each tick boundary, compute max available liquidity, fees, and new sqrt price using formulas from Raydium open-source or IDL.
   - Accumulate output until input exhausted or liquidity depleted.
3. **Exact-out** – Reverse the solver to compute required input, respecting tick spacing and `sqrt_price_limit`.
4. **Edge calculation** – Compare quoted price vs observed trade price (fast execution heuristics) or vs other adapters to produce `edge_bps`.
5. **Slippage guard** – Emit recommended `sqrt_price_limit_x64` derived from price bounds encountered during the simulation.

## Swap Template Generation

- **Account seeds**: use `amm_config`, `pool_state`, `pool_vault_*`, `observation` seeds as documented.
- **Template struct**:

```rust
pub struct SwapTemplate {
    pub program_id: Pubkey,
    pub discriminator: [u8; 8], // swap_v2
    pub accounts: Vec<AccountMeta>,
    pub data: Vec<u8>,
    pub context: TemplateContext, // hints for the fast execution lane (payer, signer reqs)
}
```

- Fill `amount`, `other_amount_threshold`, `sqrt_price_limit_x64`, `is_base_input` from the `QuoteResponse`.
- Include derived bumps to let the fast execution lane re-derive addresses if needed.

## Validation Hooks

- `QuoteResponse::status` with enums: `Ok`, `StaleState`, `InsufficientLiquidity`, `MissingAccounts`.
- Attach audit info (ticks crossed, fee paid, estimated compute budget) to help the fast execution lane decide bundling priority.

## Integration Plan

1. **State Enrichment** – add Raydium-specific metadata extraction to `PoolUpdate` and confirm via logging.
2. **Adapter Skeleton** – implement trait with stubbed quote/template methods returning `None` until math filled in.
3. **Pricing Engine** – port the sqrt-price/tick math from Raydium reference or our existing tooling; unit-test with fixtures.
4. **Template Builder** – encode swap instructions using `raydium_amm_v3` IDL or manual Borsh layout; validate against known-good swaps.
5. **Fast execution wiring** – instantiate the adapter, feed pool updates from the hydration lane, and replace generic `Opportunity::from_trade` with adapter-derived quotes when `program_id` matches.
6. **Testing** – add regression tests with recorded CoreCast updates + expected quotes; optionally run against a forknet harness to ensure instruction templates execute.

## Open Questions

- Do we house adapter configs (tick spacing overrides, memo requirements) in `config.json` or derive entirely from CoreCast metadata?
- How do we prioritize between multiple adapters producing quotes for the same trade (e.g., fallback to reserve ratio when adapter unavailable)?
- Should swap template generation live in the hydration lane (attach to bundle candidate) or the fast execution lane (lazy evaluation when we decide to submit)?
