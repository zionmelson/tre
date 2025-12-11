# Meteora lb_clmm Protocol Adapter

This note documents how our lb_clmm adapter mirrors Meteora’s dynamic liquidity-bin market maker. Keep it synced with the upstream references below plus `docs/swaps/lb_clmm_swap.md` for account layouts.

## Upstream References (GitHub)

| Component                                                     | Purpose                                                                                                               | Link                                                                                 |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `commons/src/math/price_math.rs`                              | `get_price_from_id` and SCALE math that convert bin ids/bin step into Q64.64 prices                                   | <https://github.com/MeteoraAg/dlmm-sdk/blob/main/commons/src/math/price_math.rs>     |
| `commons/src/math/u64x64_math.rs`                             | Fixed-point exponentiation (`pow`) and rounding bounds that gate bin id ranges                                        | <https://github.com/MeteoraAg/dlmm-sdk/blob/main/commons/src/math/u64x64_math.rs>    |
| `commons/src/math/utils.rs` & `u128x128_math.rs`              | `safe_*` helpers plus 256-bit mul/div kernels used by bin math and fee calculations                                   | <https://github.com/MeteoraAg/dlmm-sdk/tree/main/commons/src/math>                   |
| `commons/src/extensions/lb_pair.rs`                           | `LbPairExtension` trait covering volatility updates, fee computation, token program resolution, and bitmap navigation | <https://github.com/MeteoraAg/dlmm-sdk/blob/main/commons/src/extensions/lb_pair.rs>  |
| `commons/src/extensions/bin.rs`                               | `BinExtension::swap`, price caching, and host-fee splitting logic per bin                                             | <https://github.com/MeteoraAg/dlmm-sdk/blob/main/commons/src/extensions/bin.rs>      |
| `commons/src/extensions/bin_array.rs` & `bin_array_bitmap.rs` | Helpers for mapping bin ids to PDAs plus bitmap scans for liquidity discovery                                         | <https://github.com/MeteoraAg/dlmm-sdk/tree/main/commons/src/extensions>             |
| `commons/src/extensions/position.rs`                          | Range lookups for liquidity chunks; informs hydration of which bin arrays to cache                                    | <https://github.com/MeteoraAg/dlmm-sdk/blob/main/commons/src/extensions/position.rs> |
| `commons/src/conversions/*.rs`                                | `PairStatus`, `PairType`, `ActivationType`, and token-program flag decoding mirrored inside adapter validation        | <https://github.com/MeteoraAg/dlmm-sdk/tree/main/commons/src/conversions>            |

Any upstream change across those modules should be mirrored here and in the hydration/pricing code paths that depend on them.

## Adapter Goals & Scope

1. Quote lb_clmm pools exactly like `LbPairExtension::swap` would, including variable fee adjustments, host/referral fee distribution, and bitmap-assisted bin traversal.
2. Emit deterministic `SwapTemplate` payloads for all supported instructions (`swap2`, `swap_with_price_impact2`, `swap_exact_out2`) so fast execution never re-derives PDAs or account order.
3. Mirror Meteora SDK semantics for SPL Token vs Token-2022 pools, including transfer-hook account resolution for swap legs and reward harvests.
4. Surface adapter-specific health signals (`PreActivation`, `VolatilityClamp`, `HydrationPending`, `MissingBitmapExtension`) plus structured telemetry for state freshness, template composition, and transfer-hook success/failure.

## Data Inputs & Cached State

| Field                                                                   | Source                                                                             | Notes                                                                                               |
| ----------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| Vault balances, bin liquidity deltas, active bin id                     | CoreCast `PoolLiquidityChangeStreamMessage` extras                                 | Ingested via `extensions.lb_clmm` payload, hydrated into `BinSnapshotRing`.                         |
| `LbPair` parameters (fees, volatility refs, bin step, activation flags) | Cached on-chain account fetched lazily, schema from `idls/pools/lb_clmm.json`      | Provide base/variable fee math mirroring `LbPairExtension`.                                         |
| `BinArray`/`BinArrayBitmapExtension` PDAs                               | Derived using `extensions/bin_array.rs` + `bin_array_bitmap.rs` helpers            | Needed for swaps that span multiple bin arrays or require bitmap extension (pool width > 512 bins). |
| Oracle window                                                           | CoreCast extras, fallback to `Oracle` account via RPC                              | Drives volatility accumulator + guardrails for out-of-date data.                                    |
| Token program flags + host fee ATA                                      | Stored in `LbClmmState.pool_meta` (mint + program IDs + optional host fee account) | Uses `TokenProgramFlagWrapper` conversions to decide SPL vs Token-2022.                             |

`LbClmmState` keeps a snapshot of those items plus timestamps (`last_update_slot`, `last_oracle_slot`). Freshness is no longer governed by a slot TTL; caches stay live once populated and inherit readiness between snapshots. Operators now lean on the standard `stale_slot_threshold`/`stale_after_ms` guards in `LbClmmAdapterConfig` plus explicit invalidations (structural changes, corrupt RPC reads, or manual resets), while `max_bin_walk` and `max_price_impact_bps` still enforce execution hygiene.

## Core On-Chain Extensions

- `LbPairExtension` (`extensions/lb_pair.rs`) exposes fee math (`compute_fee`, `compute_protocol_fee`, `compute_variable_fee`), volatility updates (`update_references`, `update_volatility_accumulator`), bitmap navigation (`next_bin_array_index_with_liquidity_internal`), and token-program decoding. The adapter mirrors these methods to avoid drift; any future change in that trait must be ported.
- `BinExtension` (`extensions/bin.rs`) encapsulates per-bin swap math, including host-fee splitting and safe mul/div operations. Our simulator must call the same flow: cache bin price via `get_or_store_bin_price`, respect `max_amount_out`, then call `Bin::swap` with host-fee basis points pulled from state.
- `BinArrayExtension` and `BinArrayBitmapExtExtension` provide PDA derivations plus bitmap scans; we rely on them when building template remaining accounts or when the cached ring buffer misses a bin that the swap path requires.
- `PositionExtension` is primarily used during hydration to backfill bin arrays for new pools or after we invalidate old snapshots; it informs how many arrays to fetch given a `(lower_bin_id, upper_bin_id)` range.

## Math Building Blocks (commons/src/math)

- `price_math.rs::get_price_from_id` converts bin ids + bin step into Q64.64 prices. We cache those results per bin to avoid recomputation.
- `u64x64_math.rs::pow` defines the exponential limits; its `MAX_EXPONENTIAL` guard is why we cap bin traversal ranges and reject quotes when a request spans beyond the supported exponent window.
- `u128x128_math.rs` plus `utils.rs` provide `mul_div`, `mul_shr`, `shl_div`, and the `safe_*` wrappers used heavily inside `Bin::swap`. Our pricing code sticks to those helpers (or reuses identical Rust equivalents) so rounding/overflow behavior matches on-chain outcomes bit-for-bit.

## SDK Logic Parity in Rust

The Rust adapter now mirrors the Meteora SDK entry points directly instead of re-implementing simplified math:

- `LbClmmState::simulate_lb_clmm_exact_in` calls the same sequence of helpers as `LbPairExtension::swap`, including `update_volatility_accumulator`, `compute_fee`, `compute_protocol_fee`, and the host-fee split. Every intermediate calculation is clamped with the same `safe_*` arithmetic to avoid drift.
- Bin traversal respects the SDK’s `next_bin_array_index_with_liquidity_internal` contract. When the CoreCast snapshot lacks a required array, we hydrate it through the same PDA derivations defined in `extensions/bin_array.rs` so bin windows line up with on-chain expectations.
- Price projections reuse `price_math::get_price_from_id` for the pre/post trade guardrails. We cache the Q64.64 outputs just like `BinCache` inside the SDK so hot pools avoid redundant pow/mul work without changing math results.
- The host-fee, referral, and volatility adjustments now follow the SDK’s switch statements verbatim. Each branch in the Rust enum wrappers points to the corresponding upstream variant so any divergence shows up as a unit-test failure against the golden SDK fixtures.

Whenever upstream adds a new helper in `commons/src/extensions`, we stub a Rust adaptor that forwards to the same semantics before wiring it into simulation or template building. The `tests/hydration::adapters::lb_clmm::*` suite executes captured SDK fixtures to spot regressions quickly.

## Pricing Architecture

1. **Direction resolution** – Compare `QuoteRequest.input_mint` against `token_x_mint` cached in state. This maps directly to the `swap_for_y` boolean used by `Bin::swap` and determines whether we decrement or increment `active_bin_id`.
2. **Bin discovery** – Start with the `LbPair`’s `bin_array_bitmap` (from CoreCast) to jump to the next non-empty bin. If liquidity is missing or the trade requires bins beyond our cached window, request fresh `BinArray` accounts using the offsets provided by `BinArrayExtension::bin_id_to_bin_array_key`.
3. **Per-bin execution** – For each bin, call the simulated `Bin::swap` to compute `amount_out`, `fee`, `protocol_fee`, and optional host fee. Respect `max_bin_walk` (default 800 bins) so we do not traverse unbounded ranges; once the walk exhausts that budget we fail the quote instead of degrading accuracy.
4. **Fees** – Combine `LbPairExtension::get_base_fee()` and `get_variable_fee()` (which depends on `volatility_accumulator`) and apply host-fee overrides when `host_fee_in` is configured. Variable fee clamps must be mirrored from upstream to avoid quoting trades that on-chain would reject via `MAX_FEE_RATE`.
5. **Guards** – Enforce `max_price_impact_bps` by comparing the execution price after bin walk with the pre-trade price computed via `price_math::get_price_from_id(active_bin_id, bin_step)`. If impact exceeds config, mark status `PriceImpactExceeded`.
6. **Partial fills & insufficiency** – If the simulator runs out of bins (`next_bin_array_index_with_liquidity_internal` returns empty) before satisfying the request, set `QuoteResponse.status = HydrationPending` and surface the filled amount. Strategies can downsize or refresh after hydration fetches more arrays. XYK fallbacks are no longer allowed; missing bin data now aborts the quote rather than approximating with a constant-product model.

## Swap Template Generation

- Always target the `swap2` discriminant family (plain, `swap_with_price_impact2`, `swap_exact_out2`) so we align with Meteora’s latest telemetry pipeline.
- Account ordering follows `docs/swaps/lb_clmm_swap.md` exactly. Include the optional `bin_array_bitmap_extension` PDA when the pair spans >512 bins (check `LbPairExtension::is_overflow_default_bin_array_bitmap`).
- Token program IDs (`spl_token::ID` vs `spl_token_2022::ID`) come straight from `TokenProgramFlagWrapper`; template builders must use those rather than inferring from mint decimals.
- Remaining accounts: assemble `BinArray` PDAs covering the highest/lowest bin touched, append oracle account, optional host-fee ATA, memo program, and any Token-2022 transfer-hook accounts that the hydration cache marked as required.
- Arguments (`SwapParameters`, `SwapExactOutParams`, etc.) should include the `active_id`, `max_price_impact_bps`, and the computed guardrails our simulator derived. We also attach a compute-budget recommendation (300k–500k CU range) when the bin walk crosses many arrays or when Token-2022 hooks are active.

## Transfer-Hook Resolution & Telemetry

- **Mint-program discovery** – The adapter caches the owner program for every mint via `TransferHookAccountsResolver::mint_program`. We hit the same RPC endpoints used for hydration; failures increment the `hydration_transfer_hook_events_total{reason="mint_owner_lookup"}` counter so operators can spot missing metadata before swaps fail on-chain.
- **Swap contexts** – For each swap leg we build `TransferHookAccountContext` structs that match the SDK’s execute instruction inputs (payer ➝ reserve for input, reserve ➝ ATA for output). `ExtraAccountMetaList::add_to_instruction` runs inside a sandboxed Tokio executor so we get identical CPI account ordering.
- **Reward hooks** – Reward harvests now mirror Meteora’s `collect_reward` flow. We derive (or fetch from the builder) the payer’s ATA for each reward mint, generate the bidirectional hook contexts (vault ➝ user and optional user ➝ vault), and deduplicate the remaining-accounts slices so swap templates reuse them across rewards with identical hooks.
- **Telemetry** – Every resolve attempt emits either `reason="success"` or a failure label (`validation_missing`, `account_fetch`, `extra_account_build`). We intentionally keep the `event="resolve"` dimension fixed so Grafana queries can aggregate across swap vs reward contexts. Pool- and mint-specific context still lives in tracing logs, but the metric lets us alert on sustained failures quickly.

If both mint legs are SPL Token (non-2022), the resolver returns `None` and the template builder omits transfer-hook slices entirely, keeping CU limits tight for legacy pools.

## Validation & Telemetry Hooks

- `PairStatus` / `PairType` / `ActivationType` conversions: reuse the logic in `commons/src/conversions` to decide whether a pool is tradable (`PairStatus::Enabled`) and whether activation is slot- or timestamp-gated. Quotes for disabled or pre-activation pools should immediately return `QuoteStatus::PreActivation`.
- `TokenProgramFlags` inform whether memo + remaining accounts are required for transfer fees; missing hooks should return `QuoteStatus::MissingAccounts` to avoid on-chain failures.
- Structural invalidation: quotes remain available on the latest known-good snapshot until hydration explicitly invalidates the pool (new vaults/bin arrays, corrupt data, or operator reset). Slot drift alone no longer marks pools as stale; readiness tiers stay latched once achieved.
- Bin coverage: when bin traversal hits an uncached segment or `BinArrayBitmapExtExtension::next_bin_array_index_with_liquidity` returns `(idx, false)`, emit `HydrationPending` and trigger a hydration refresh.
- Telemetry fields: `pool`, `slot`, `swap_mode`, `bin_walk`, `dynamic_fee_bps`, `host_fee_applied`, `bitmap_extension_used`, plus rejection counters grouped by on-chain error (6003 missing accounts, 6042 insufficient liquidity, 6077 state_too_old).

## Integration / Testing Checklist

1. **State plumbing** – Extend CoreCast schema and ingestion to carry `bin_step`, bitmap slices, oracle payload, and volatility parameters. Add fixtures under `tests/fixtures/lb_clmm/` mirroring `LbPairExtension` JSON dumps.
2. **Adapter coverage** – Unit-test `quote_exact_in`, `quote_exact_out`, and `quote_with_price_impact` against golden swaps produced by running the upstream SDK (call into `BinExtension::swap` with recorded bins).
3. **Template smoke tests** – Use `solana-test-validator` + real lb_clmm program to execute adapter-generated templates across SPL/SPL, SPL/Token-2022, host-fee enabled, and bitmap-extension pools.
4. **Bitmap regression** – Add tests that cross the default 512-bin boundary to ensure `bin_array_bitmap_extension` accounts are inserted whenever `LbPairExtension::is_overflow_default_bin_array_bitmap` says so.
5. **Oracle/fee drift** – Replay captured oracle updates and verify our volatility accumulator outputs match upstream `update_references`/`update_volatility_accumulator` behavior; alert when drift exceeds 1 bps.

## Known Gaps / TODOs

- Model Meteora’s activation handlers fully (slot vs timestamp) so we can predict activation without trial swaps.
- Persist `next_bin_refresh_slot` per pool to avoid spamming RPC when we already know a refresh is pending.
- Explore caching precalculated bin prices for hot pools to reduce Q64.64 math overhead under load.
- Add automated alerts when `BinArrayBitmapExtExtension` indicates liquidity holes that exceed `max_bin_walk`, since those pools may require manual intervention or config tuning.
- Document compute budget heuristics in `docs/architecture.md` once we stabilize CU estimates for bitmap-heavy swaps.

Keep this document synchronized with the upstream SDK modules and `docs/swaps/lb_clmm_swap.md`; together they define the lb_clmm adapter contract.
