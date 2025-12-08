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

| Source                                      | Purpose                                                                                  | Implementation Notes                                                                                      |
| ------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| CoreCast `PoolLiquidityChangeStreamMessage` | Vault balances, mint pairs, slot, fee deltas                                             | Parsed in `PoolUpdate::from_corecast` (`src/hydration/types.rs`).                                         |
| Protocol extras (`RaydiumV3PoolExtras`)     | Tick spacing, sqrt_price_x64, liquidity, PDA bumps, AMM config fee numerators            | Populated via `parse_raydium_v3_extras` (`src/hydration/types/protocols.rs`).                             |
| On-chain hydration (`RaydiumPoolHydrator`)  | Ground-truth pool accounts, AMM config fees, tick arrays, mint program IDs, Token-2022 fee configs | See `src/hydration/adapters/raydium_v3.rs`, section `RaydiumPoolHydrator`.                                |
| Mint metadata cache                         | Token-2022 transfer-fee config + owner program, reused by adapter + transfer-hook resolver | Stored in `RaydiumPoolHydrator::mint_metadata`; avoids repeated RPCs and keeps hints aligned with SDK.    |

`RaydiumV3State` caches:

- Pool + mint pubkeys, associated token program IDs (needed for Token-2022 transfers copied from `swap_v2.rs`).
- Vault balances (`vault_base`, `vault_quote`) for snapshot pricing and XYK fallback.
- Tick metadata (`tick_spacing`, `current_tick`, `sqrt_price_x64`, `liquidity`).
- Fee tiers (`trade_fee_bps`, `protocol_fee_bps`, `fund_fee_bps`) exactly as parsed from AMM config PDA.
- Tick-array bitmap extension address and `pool_status` byte (needed for SDK-style remaining-account slices).
- Transfer-fee summaries for each mint (epoch/max/bps); used both for hinting template hydration and for the simulation math.
- Hydrated tick-array cache (3 accounts per direction) plus TTL, matching router expectations.

Eviction happens whenever `slot` drift exceeds `config.stale_slot_threshold` or wall-clock age exceeds `config.stale_after_ms`.

### Configuration knobs

- `config.json -> protocols.raydium_v3.{hydration_slot_lag_tolerance, hydration_lag_ms_tolerance}` let operators widen or tighten the slot and wall-clock windows before the adapter considers a pool hydrated. Slot lag still governs when we refresh on-chain state, while the ms tolerance reuses the hydrator's `last_hydrated_at` timestamps so quotes/templates can proceed when RPC is briefly behind even though the existing meta is only a few seconds old.

## Pricing Architecture

### Simulation Path

The adapter now mirrors the Raydium TypeScript SDK (`@raydium-io/raydium-sdk`, `Clmm.fetchMultiplePoolInfos` + `Clmm.computeAmountOut`) in addition to the on-chain `swap_internal` routine. The flow is:

1. Clamp request size via `clamp_request_amount` (max `RAYDIUM_V3_INPUT_UTILIZATION_BPS`, currently 40% of the input-side vault). This matches the SDK’s `computeMaxAmountIn` guard so we will never issue quotes that the signer cannot submit.
2. Determine direction with `classify_swap_direction` (mirrors mint ordering logic from `swap_v2.rs` / SDK’s `ClmmPoolInfo`).
3. Fetch tick arrays through `RaydiumPoolHydrator::preferred_tick_arrays_for_swap`, ensuring the identical 3-array window (`current`, `next`, `prev`) that `swap_internal` and the SDK require.
4. Run `simulate_clmm_exact_in`, our Rust port of `swap_internal` + `swap_math::compute_swap_step`, but now instrumented to match SDK bookkeeping:
  - Iterates up to 512 steps, same guard as upstream.
  - Applies `liquidity_net` when crossing initialized ticks.
  - Tracks pool-input consumed vs. swap-budget so partial fills surface the exact `executed_input_amount` the SDK would emit.
  - Computes per-step trade fees, then allocates protocol + fund splits using `RAYDIUM_FEE_RATE_DENOMINATOR`, identical to `swap.rs`.
  - Applies Token-2022 transfer fees both pre-swap (input mint) and post-swap (output mint) using cached configs, reproducing the SDK’s `TokenAmount.applyFee` helpers.
  - Computes `sqrt_price_after` so we can set `QuoteResponse.sqrt_price_limit` and propagate router hints.
5. Emit `ClmmSwapSimulation` containing `executed_input_amount`, `net_output_amount`, protocol/fund fee totals, and the post-trade sqrt price. The adapter declines to quote rather than approximating when simulation prerequisites fail (missing hydration, zero liquidity, incomplete tick arrays), keeping parity with SDK fallbacks.

### Fees & Limits

- Trade fee tier comes from AMM config (`trade_fee_rate` / `fee_bps`).
- Protocol + fund fees are applied per-step and surfaced via `TemplateContext.expected_fee_amount`. Downstream legs subtract this value when deriving min-output/slippage, so bundle plans match on-chain behavior even when Raydium’s operator toggles those knobs.
- Token-2022 transfer fees are modeled by `apply_post_fee_amount` / `apply_pre_fee_amount`; quoted outputs always represent the *net* amount that final token accounts will receive.
- `sqrt_price_limit` defaults to 0 (meaning “best effort”) unless simulation provides a bound. We now always populate it with `sqrt_price_after` so router callers can enforce deterministic slippage clamps the same way the SDK does.

## Instruction Templates & Routing

- Accounts and arguments must match the `SwapSingleV2` struct exactly; `docs/swaps/amm_v3_swaps.md` stays in sync with upstream.
- Template context now carries every hint the SDK requires to build router hops:
  - Program IDs: Raydium v3, SPL Token, Token-2022, Memo (see `SWAP_MEMO_MSG`).
  - User ATAs resolved via `BuilderHandle::token_account_for_mint`.
  - Tick-array accounts in the order produced by hydrator (current + two lookahead/behind depending on direction) plus the optional tick-array bitmap extension PDA.
  - PDA bumps for `pool`, `vault_0/1`, `observation`.
  - Token-2022 transfer-fee config hints (epoch/max_fee/bps) for both input and output mints so Stage 2 can pre-build CPI metadata without re-reading mints.
  - Raydium pool status byte, protocol/fund fee bps, and `expected_fee_amount` (protocol + fund aggregate) allowing the planner to subtract non-trade fees before computing slippage tolerances.
  - Remaining-account slices for LB/transfer-hook parity are untouched, but we now reuse the same `MintAccountMeta` cache the transfer-hook resolver uses, ensuring Token-2022 owners stay consistent across components.
- Templates are registered with the builder (`register_amm_v3_pool`) once fee tier + PDA data exist. The builder also seeds ALT entries so the fast lane discovers the same account layout the router expects.
- For router-style multi-hop flows, the outgoing token account from hop _n_ becomes the input ATA for hop _n + 1_. Because our context stores both mint + ATA for every leg, we can chain templates without recomputation.

## Validation, Telemetry, and Error Modes

- `QuoteResponse.status` enumerations:
  - `Ok` – simulation succeeded.
  - `HydrationPending` – hydrator could not fetch contiguous tick arrays (quote will be XYK-based until arrays land).
  - `StaleState` – TTL eviction flagged the pool before quoting.
  - `InsufficientLiquidity` – `simulate_clmm_exact_in` exhausted liquidity before finishing.
  - `TokenFeeUnsupported` – raised when transfer-fee math produces zero executable budget (mirrors SDK guardrails).
- Metrics/logs (tracing target `adapter::raydium_v3`): hydration retries, cache hits, XYK fallback rate, ALT registration failures.
- Transfer-hook telemetry (`hydration_transfer_hook_events_total`) now records mint-owner lookup failures, validation account fetch errors, and success paths so we can alert when Token-2022 hooks degrade quoting.
- When hydrator repeatedly fails to fetch AMM configs/mints, `missing_*` maps enforce an exponential backoff so we do not DOS RPC.

## Integration & Testing Checklist

1. **State plumbing** – Confirm CoreCast messages include Raydium extras, and hydration RPC endpoints are configured (`RaydiumV3AdapterConfig.rpc_endpoints`).
2. **Simulation parity** – Unit tests in `src/hydration/adapters/raydium_v3.rs` already assert PDA parsing and math helpers. Add fixtures comparing `simulate_clmm_exact_in` outputs to a recorded on-chain swap (use https://github.com/raydium-io/raydium-clmm test vectors).
3. **Template smoke test** – Build a `SwapTemplate`, send via `swap_v2`, and verify Memo + Token-2022 accounts are accepted.
4. **Router coverage** – Feed multiple quotes into `swap_router_base_in`, ensuring our context orders remain valid when multiple pools share accounts.
5. **Regression harness** – Replay CoreCast deltas + trades to confirm `bidirectional_quotes` align with actual fills (edge BPS > 0 triggers strategy alerts).

## Known Gaps / TODOs

- Export the SDK-style “remaining accounts bitmap” info so the dashboard can show when pools are ejected for maintenance.
- Add `sqrt_price_limit` overrides for strategies that want tighter-than-SDK clamps (we currently mirror Raydium defaults).
- Improve visibility when hydrator re-registers templates because builder ALTs missed a pool.
- Consider caching router-ready account lists (input/output mint order) to reduce per-quote decoration time.

Keep this document updated whenever upstream Raydium commits touch `swap.rs`, `swap_v2.rs`, or router semantics; those files are the canonical spec.
