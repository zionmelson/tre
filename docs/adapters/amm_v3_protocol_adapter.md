# Raydium AMM v3 Protocol Adapter Design

This document is the single source of truth for our Raydium CLMM adapter. It ties local components to the exact Solana programs they mirror, cites the upstream GitHub sources, and lists every assumption the pricing engine makes.

## Upstream References (GitHub)

- `swap.rs`: full on-chain CLMM math (`swap_internal`) and fee accounting – <https://github.com/raydium-io/raydium-clmm/blob/master/programs/amm/src/instructions/swap.rs>
- `swap_v2.rs`: user-facing instruction, Token-2022 fee handling, memo requirement – <https://github.com/raydium-io/raydium-clmm/blob/master/programs/amm/src/instructions/swap_v2.rs>
- `swap_router_base_in.rs`: multi-hop router semantics (how outputs from hop _n_ become inputs for hop _n + 1_) – <https://github.com/raydium-io/raydium-clmm/blob/master/programs/amm/src/instructions/swap_router_base_in.rs>
- React to updates in those files first; every adapter invariant below is derived directly from them.

## Adapter Goals & Scope

1. Quote Raydium CLMM pools with the same tick-by-tick math that `swap_internal` uses, including liquidity-net transitions, protocol/fund fee debits, and sqrt-price limits.
2. Generate deterministic `swap_v2` instruction templates so the fast execution lane never needs to re-derive PDAs or account order.
3. Provide validation + telemetry so strategies can see when a quote was simulated vs. when we fell back to XYK reserves.
4. Remain router-compatible: our context must satisfy both single-hop `swap_v2` and chained `swap_router_base_in` executions.

## Data Inputs & State Enrichment

| Source                                      | Purpose                                                                    | Implementation Notes                                                          |
| ------------------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| CoreCast `PoolLiquidityChangeStreamMessage` | Vault balances, mint pairs, slot, fee deltas                               | Parsed in `PoolUpdate::from_corecast` (`src/hydration/types.rs`).             |
| Protocol extras (`RaydiumV3PoolExtras`)     | Tick spacing, sqrt_price_x64, liquidity, PDA bumps                         | Populated via `parse_raydium_v3_extras` (`src/hydration/types/protocols.rs`). |
| On-chain hydration (`RaydiumPoolHydrator`)  | Ground-truth pool accounts, AMM config fees, tick arrays, mint program IDs | See `src/hydration/adapters/raydium_v3.rs`, section `RaydiumPoolHydrator`.    |

`RaydiumV3State` caches:

- Pool + mint pubkeys, associated token program IDs (needed for Token-2022 transfers copied from `swap_v2.rs`).
- Vault balances (`vault_base`, `vault_quote`) for snapshot pricing and XYK fallback.
- Tick metadata (`tick_spacing`, `current_tick`, `sqrt_price_x64`, `liquidity`).
- Fee tiers (`trade_fee_bps`, and soon `protocol_fee_rate` / `fund_fee_rate`).
- Hydrated tick-array cache (3 accounts per direction) plus TTL, matching router expectations.

Eviction happens whenever `slot` drift exceeds `config.stale_slot_threshold` or wall-clock age exceeds `config.stale_after_ms`.

## Pricing Architecture

### Simulation Path (preferred)

1. Clamp request size via `clamp_request_amount` (max `RAYDIUM_V3_INPUT_UTILIZATION_BPS`, currently 40% of the input-side vault).
2. Determine direction with `classify_swap_direction` (mirrors mint ordering logic from `swap_v2.rs`).
3. Fetch tick arrays through `RaydiumPoolHydrator::preferred_tick_arrays_for_swap`, ensuring the same 3-array window `swap_internal` expects.
4. Run `simulate_clmm_exact_in`, our Rust port of `swap_internal` + `swap_math::compute_swap_step`:
   - Iterates up to 512 steps, same guard as upstream.
   - Applies `liquidity_net` when crossing initialized ticks.
   - Computes `sqrt_price_after` so we can set `QuoteResponse.sqrt_price_limit`.
5. Return `QuoteOutcome` with `amount_out` + post-trade sqrt price. This is the only path that sets `sqrt_price_limit`.

### Fallback Path (XYK)

If hydration is missing (no tick arrays, liquidity zero, or math fails), we compute `xyk_amount_out` using current vault balances and fee tier. This mirrors Raydium’s pool reserve invariant but ignores ticks, so mark quotes with reduced confidence.

### Fees & Limits

- Trade fee tier comes from AMM config (`trade_fee_rate` / `fee_bps`).
- Protocol/fund fees are available on-chain (see `swap.rs`), but we currently ignore them in simulation. TODO: copy the proportional deductions so `amount_out` matches on-chain reality even when those knobs are non-zero.
- `sqrt_price_limit` defaults to 0 (meaning “best effort”) unless simulation provides a bound. This behavior matches `swap_v2.rs`’s clamp to MIN/MAX tick when 0 is supplied.

## Instruction Templates & Routing

- Accounts and arguments must match the `SwapSingleV2` struct exactly; `docs/swaps/amm_v3_swaps.md` stays in sync with upstream.
- Template context includes:
  - Program IDs: Raydium v3, SPL Token, Token-2022, Memo (see `SWAP_MEMO_MSG`).
  - User ATAs resolved via `BuilderHandle::token_account_for_mint`.
  - Tick-array accounts in the order produced by hydrator (current + two lookahead/behind depending on direction).
  - PDA bumps for `pool`, `vault_0/1`, `observation`.
- Templates are registered with the builder (`register_amm_v3_pool`) once fee tier + PDA data exist. The builder also seeds ALT entries so the fast lane discovers the same account layout the router expects.
- For router-style multi-hop flows, the outgoing token account from hop _n_ becomes the input ATA for hop _n + 1_. Because our context stores both mint + ATA for every leg, we can chain templates without recomputation.

## Validation, Telemetry, and Error Modes

- `QuoteResponse.status` enumerations:
  - `Ok` – simulation succeeded.
  - `MissingTickArrays` – hydrator could not fetch 3 arrays (quote will be XYK-based).
  - `StaleState` – TTL eviction flagged the pool before quoting.
  - `InsufficientLiquidity` – `simulate_clmm_exact_in` exhausted liquidity before finishing.
- Metrics/logs (tracing target `adapter::raydium_v3`): hydration retries, cache hits, XYK fallback rate, ALT registration failures.
- When hydrator repeatedly fails to fetch AMM configs/mints, `missing_*` maps enforce an exponential backoff so we do not DOS RPC.

## Integration & Testing Checklist

1. **State plumbing** – Confirm CoreCast messages include Raydium extras, and hydration RPC endpoints are configured (`RaydiumV3AdapterConfig.rpc_endpoints`).
2. **Simulation parity** – Unit tests in `src/hydration/adapters/raydium_v3.rs` already assert PDA parsing and math helpers. Add fixtures comparing `simulate_clmm_exact_in` outputs to a recorded on-chain swap (use https://github.com/raydium-io/raydium-clmm test vectors).
3. **Template smoke test** – Build a `SwapTemplate`, send via `swap_v2`, and verify Memo + Token-2022 accounts are accepted.
4. **Router coverage** – Feed multiple quotes into `swap_router_base_in`, ensuring our context orders remain valid when multiple pools share accounts.
5. **Regression harness** – Replay CoreCast deltas + trades to confirm `bidirectional_quotes` align with actual fills (edge BPS > 0 triggers strategy alerts).

## Known Gaps / TODOs

- Model protocol + fund fees exactly as `swap.rs` does (they currently reduce pool-level fee growth but not our quote output).
- Add `sqrt_price_limit` clamping for strategies that want partial-fill protection instead of best-effort.
- Improve visibility when hydrator re-registers templates because builder ALTs missed a pool.
- Consider caching router-ready account lists (input/output mint order) to reduce per-quote decoration time.

Keep this document updated whenever upstream Raydium commits touch `swap.rs`, `swap_v2.rs`, or router semantics; those files are the canonical spec.
