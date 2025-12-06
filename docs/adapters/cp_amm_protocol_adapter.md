# Meteora cp_amm Protocol Adapter

This note documents how our cp_amm adapter mirrors Meteora’s on-chain program. Use it alongside the upstream sources below whenever you touch pricing, templates, or validation logic.

## Upstream References (GitHub)

| Component              | Purpose                                                                 | Link                                                                                                        |
| ---------------------- | ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `swap` module          | Entry points and shared plumbing                                        | <https://github.com/MeteoraAg/damm-v2/tree/main/programs/cp-amm/src/instructions/swap>                      |
| `ix_swap.rs`           | Account layout, `SwapMode`, referral handling, rate-limiter guard       | <https://github.com/MeteoraAg/damm-v2/blob/main/programs/cp-amm/src/instructions/swap/ix_swap.rs>           |
| `swap_exact_in.rs`     | Exact-in math and transfer-fee normalization                            | <https://github.com/MeteoraAg/damm-v2/blob/main/programs/cp-amm/src/instructions/swap/swap_exact_in.rs>     |
| `swap_exact_out.rs`    | Exact-out solver + slippage checks                                      | <https://github.com/MeteoraAg/damm-v2/blob/main/programs/cp-amm/src/instructions/swap/swap_exact_out.rs>    |
| `swap_partial_fill.rs` | Partial-fill semantics and consumed-amount reporting                    | <https://github.com/MeteoraAg/damm-v2/blob/main/programs/cp-amm/src/instructions/swap/swap_partial_fill.rs> |
| `swap/mod.rs`          | `ProcessSwapParams`/`ProcessSwapResult` definitions reused by all modes | <https://github.com/MeteoraAg/damm-v2/blob/main/programs/cp-amm/src/instructions/swap/mod.rs>               |

Any upstream change in those files should be reflected here and in `docs/swaps/cp_amm_swap.md`.

## Adapter Goals & Scope

1. Quote Meteora cp*amm pools with the same curve math (`ProcessSwapParams` → `process_swap*\*`) that on-chain `swap2` uses, including transfer-fee adjustments and referral splits.
2. Produce deterministic `swap2` templates (covering `ExactIn`, `PartialFill`, `ExactOut`) so the fast execution lane never needs to re-derive account order or serialize `SwapParameters2`.
3. Surface adapter-specific health signals (`RateLimited`, `MissingReferral`, `StaleState`) and structured telemetry so strategies know when to trust cp_amm quotes.

## Data Inputs & Cached State

| Field                                 | Source                                               | Notes                                                           |
| ------------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------- |
| Vault balances, mints, slot           | CoreCast `PoolLiquidityChangeStreamMessage`          | Parsed via `PoolUpdate::extensions.cp_amm`.                     |
| Amp curve + dynamic fee data          | CoreCast extras, fallback to on-chain `Pool` account | `CpAmmPoolExtras` in `src/hydration/types/protocols.rs`.        |
| Block time (unix)                     | CoreCast snapshot → `QuoteRequest.block_time`        | Drives activation gating via `ActivationHandler::get_current_point`. |
| Mint metadata + owner program         | Mint-metadata hydrator (`HydratedMintMeta`)          | Provides decimals + canonical SPL vs Token-2022 program IDs.    |
| Transfer-fee program hints            | Transfer-fee hydrator                                | Secondary source when metadata misses owner program.            |
| Token program IDs (SPL vs Token-2022) | Builder hint, metadata fallback, then RPC            | Matches Anchor `InterfaceAccount` requirements in `ix_swap.rs`. |
| Referral ATA                          | CoreCast metadata or config override                 | Optional account in `SwapCtx`.                                  |

`CpAmmState` stores mutable balances + fee params along with a static `PoolMeta` bundle (pool authority `HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC`, vault/mint pubkeys, referral ATA). When metadata is missing we hydrate decimals + owner program directly from mint accounts, then store them on the state so every subsequent quote has the same inputs the Meteora SDK would have pulled. The cache is backed by a `DashMap<Pubkey, CpAmmState>`; eviction is based on `config.stale_slot_threshold` and `config.stale_after_ms`.

## Instruction Semantics (`swap2`)

- `SwapParameters2` encodes two amounts plus a `swap_mode` byte. `amount_0` is `amount_in` for `ExactIn`/`PartialFill`, `amount_out` for `ExactOut`. `amount_1` is `min_out` or `max_in` respectively.
- `SwapMode` ordering: `ExactIn` = 0, `PartialFill` = 1, `ExactOut` = 2 (see `SwapMode` enum in `ix_swap.rs`).
- `handle_swap_wrapper` enforces:
  - Pool access validator (can disable swap per pool).
  - Single-swap-per-transaction via `validate_single_swap_instruction` (instructions sysvar + sibling inspection). Our templates must include the instructions sysvar when rate limiter is active.
  - Rate limiter guard derived from `pool.pool_fees.base_fee`. When triggered, the instruction requires `validate_single_swap_instruction` to ensure no batching; our adapter should surface `RateLimited` status if we detect previous failures for the same pool.
- Referral payouts: if `referral_token_account` is present, `transfer_from_pool` pays `referral_fee` in whichever token the fee mode indicates. Missing referral ATA will cause `requires` failures, so quotes must mark status `MissingAccounts` when the state lacks a referral target for referral-enabled pools.
- Activation handler parity: `ActivationHandler::get_current_point` evaluates both slot and Unix timestamp. We normalize `QuoteRequest.block_time` (seconds) and pass it through `CpAmmState::current_point`, ensuring pools that are time-gated (e.g., Meteora launches) only quote after both constraints clear. If CoreCast omits a block time we still fall back to slot-only behavior, matching on-chain defaulting.

## Pricing Architecture

1. **Direction resolution** – `SwapCtx::get_trade_direction` compares input ATA mint vs `token_a_mint`. The adapter mirrors this by inspecting `QuoteRequest.input_mint` and ordering vaults accordingly.
2. **Transfer-fee normalization** – All swap modes call `calculate_transfer_fee_excluded_amount`/`included_amount` before/after curve math. Our simulator must do the same for Token-2022 pools so quotes match on-chain behavior.
3. **Exact-in** (`swap_exact_in.rs`):

- Convert user amount to “fee-excluded” basis, run `Pool::get_swap_result_from_exact_input`, then re-apply token-program transfer fees on the output vault.
- Enforce `minimum_amount_out` on the fee-excluded output.

4. **Partial-fill** (`swap_partial_fill.rs`):

- Similar to exact-in but uses `Pool::get_swap_result_from_partial_input` and may consume less than the requested `amount_in`.
- Report the actually consumed input (`transfer_fee_included_consumed_in_amount`) so the caller can retry with the remainder. Our `QuoteResponse.context.partial_fill_remaining` should expose that delta.

5. **Exact-out** (`swap_exact_out.rs`):

- Convert desired output to “fee-included” units, run `Pool::get_swap_result_from_exact_output`, then ensure the required input (after adding token-program transfer fees) is ≤ `maximum_amount_in`.

6. **Fees** – `FeeMode::get_fee_mode` selects protocol/LP/referral fee configuration per direction. `Pool::apply_swap_result` mutates reserves; our adapter mirrors this in simulation so `vault_a/b_balance` stay aligned. We also sanitize Meteora’s dynamic fee schedule (clamping numerators/denominators to on-chain bounds) so quotation never exceeds what the program would accept, mirroring the SDK’s fallback to “legacy” fees when metadata is malformed.
7. **Edge calculation** – Mid-price comes from the hydrated CLMM state, not raw vault ratios. Convert `sqrt_price_x64` (already cached in `CpAmmState`) to an f64, square it, then adjust for the mint decimal delta:

```
let sqrt_price = sqrt_price_x64 as f64 / 2_f64.powi(64);
let decimals_adjustment = 10_f64.powi((token_b_decimals as i32) - (token_a_decimals as i32));
let mid_price = (sqrt_price * sqrt_price) * decimals_adjustment;
```

This mirrors `Pool::get_price()` in Meteora’s program and stays accurate even when most liquidity sits outside the active band. The vault-balance ratio (`vault_b / vault_a`) is only a reasonable proxy when liquidity is entirely in-range; note that explicitly so downstream consumers treat it as a diagnostic value rather than a pricing source. Compare the CLMM-derived mid-price to the execution price to populate `edge_bps`.

## Template Generation

1. Always target `swap2` (discriminator from Anchor IDL) because it covers all swap modes and emits `EvtSwap2` telemetry.
2. Required accounts (exact order from `SwapCtx`):

- `pool_authority` (constant PDA), `pool` (writable), user `input_token_account`, user `output_token_account`.
- `token_a_vault`, `token_b_vault`, `token_a_mint`, `token_b_mint` (Anchor constraints tie vault ↔ mint ↔ token program).
- `payer` signer, `token_a_program`, `token_b_program`, optional `referral_token_account` (writable), plus event authority + cp_amm program ID appended by Anchor.

3. Include `instructions` sysvar and `access_validator` PDA only when the on-chain accounts expect them (rate limiter or activation states). Verify via `docs/swaps/cp_amm_swap.md`.
4. Serialize `SwapParameters2` via Anchor’s struct or manual Borsh, ensuring `swap_mode` matches the requested direction.
5. Template context should include:

- Suggested compute budget (cp_amm swaps with high amp often need > 50k CU).
- `expected_fee_tokens` (protocol/referral sum) for downstream accounting.
- `partial_fill_expected_remaining` for `SwapMode::PartialFill`.
- `input_token_program`/`output_token_program` derived from builder hints, mint metadata, or transfer-fee hydrator so downstream signers never have to guess SPL Token vs Token-2022. This matches how Meteora’s SDK (`@meteora-ag/damm-sdk`) derives interface accounts and prevents Token-2022 pools from defaulting to the legacy program.

## SDK Parity & Token Program Handling

Everything in this adapter is now sourced the same way Meteora’s TypeScript SDK does:

1. **Builder-first token programs** – When the template builder provides explicit token programs we trust them, mirroring SDK behavior. If the builder is absent (dashboard, tests, dry runs) we fall back to hydrated mint metadata, then transfer-fee records, and finally RPC introspection.
2. **Mint metadata cache** – `HydratedMintMeta` stores decimals + owner program alongside the mint pubkey so every future quote reuses identical inputs. This avoids “first quote wins” drift when CoreCast lags behind.
3. **Transfer-fee synchronization** – The transfer-fee hydrator exposes the mint’s token program plus per-direction fee config, which we apply before/after curve math exactly like the SDK’s `simulateExactIn/Out` helpers.
4. **Template context completeness** – `TemplateContext` always includes both token programs, referral requirements, and memo/compute hints so any consumer (bot, planner, dashboard) can take the output blob and hit `swap2` without copying SDK logic.

If you change how the SDK derives mint/program metadata, update `CpAmmAdapter::decorate_template_context` and this section in lockstep.

## Validation & Telemetry

- `QuoteResponse::status` values:
  - `Ok` – simulation succeeded and template assembled.
  - `StaleState` – state older than TTL.
  - `InsufficientLiquidity` – simulator detected vault shortfall or amp math failure.
  - `MissingReferral` – referral ATA required but absent.
  - `RateLimited` – recent swap failure logged with `PoolError::FailToValidateSingleSwapInstruction` or limiter triggered.
- Emit tracing fields: `pool`, `slot`, `swap_mode`, `amount_in/out`, `fee_mode`, `referral_fee`, `rate_limiter_applied`.
- Count XYK fallback events separately (should never happen if hydration is healthy since cp_amm math is simple enough to keep in-process).

## Integration / Testing Checklist

1. **State plumbing** – Ensure `PoolUpdate.extensions.cp_amm` includes amp + fee data; add fixtures under `tests/fixtures/cp_amm/`.
2. **Adapter coverage** – Unit-test `CpAmmAdapter::quote_exact_in/out/partial` against recorded swap logs pulled from Meteora pools.
3. **Template smoke tests** – Use `solana-test-validator` to invoke `swap2` with our generated account metas (cover SPL/SPL and SPL/Token-2022 pairs, plus referral vs non-referral cases).
4. **Rate limiter regression** – Simulate rapid swaps to trigger `validate_single_swap_instruction` and confirm our templates include the instructions sysvar + sibling guard only when necessary.
5. **Router compatibility** – If/when we stitch cp_amm pools into multi-leg strategies, ensure template metadata clearly marks input/output ATAs for reuse.

## Known Gaps / TODOs

- Persist rate-limiter cooldown timing in state so strategies can skip pools for a few slots after a limiter trip.
- Improve referral ATA discovery (maybe query Meteora’s registry) to avoid repetitive `MissingReferral` statuses.
- Add end-to-end tests that run `swap_partial_fill` to verify `amount_left` arithmetic under real Token-2022 fee schedules.
- Backfill block-time data for historical CoreCast snapshots so activation gating stays accurate even when the upstream feed omits timestamps.

Keep this document synchronized with the GitHub references above and with `docs/swaps/cp_amm_swap.md`; they collectively define the cp_amm adapter contract.
