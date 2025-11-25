# Whirlpool Swap Playbook

This document explains how to submit swaps against Whirlpool pools using the on-chain IDL at `protocol/pools/whirlpool.json` (program `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`, version `0.3.6`). It is intended for searchers/bots that already ingest pool state via the proto pipeline but need a precise checklist for composing `swap` instructions.

## Instruction recap

`swap` is defined in the IDL with the following accounts and arguments:

| Account              | Mut   | Signer | Purpose                                                                                                                               |
| -------------------- | ----- | ------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `tokenProgram`       | false | false  | SPL Token program that processes the vault transfers.                                                                                 |
| `tokenAuthority`     | false | true   | User or delegate that owns the source token ATA(s); signs for both source and destination transfers.                                  |
| `whirlpool`          | true  | false  | The concentrated-liquidity pool account storing price, liquidity, fee rates, vault pubkeys, and reward trackers (`Whirlpool` struct). |
| `tokenOwnerAccountA` | true  | false  | User-owned token account for mint A (source or destination depending on direction).                                                   |
| `tokenVaultA`        | true  | false  | Program-owned vault that custody mint A liquidity (address stored in `Whirlpool.tokenVaultA`).                                        |
| `tokenOwnerAccountB` | true  | false  | User-owned token account for mint B.                                                                                                  |
| `tokenVaultB`        | true  | false  | Program-owned vault for mint B (`Whirlpool.tokenVaultB`).                                                                             |
| `tickArray0/1/2`     | true  | false  | Three consecutive `TickArray` accounts that cover the entire swap path. Each array contains 88 ticks starting at `startTickIndex`.    |
| `oracle`             | false | false  | Oracle PDA that tracks the last trade timestamps and adaptive fee metadata; ensures trading is enabled and records price data.        |

| Argument                 | Type   | Meaning                                                                                                                              |
| ------------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `amount`                 | `u64`  | When `amountSpecifiedIsInput=true` this is the exact input amount; otherwise it is the desired output amount.                        |
| `otherAmountThreshold`   | `u64`  | Safety bound for the other leg (min output or max input). Triggers `AmountOutBelowMinimum` (6036) or `AmountInAboveMaximum` (6037).  |
| `sqrtPriceLimit`         | `u128` | Q64.64 square-root price boundary. Must move in the same direction as the swap or `InvalidSqrtPriceLimitDirection` (6034) is thrown. |
| `amountSpecifiedIsInput` | `bool` | `true` = fixed input, `false` = fixed output quote.                                                                                  |
| `aToB`                   | `bool` | `true` swaps token A into token B. `false` swaps B into A.                                                                           |

## Preparing the accounts

1. **Load the Whirlpool account** (`Whirlpool` struct in the IDL). Extract:
   - `tokenMintA/B` to know the mint order.
   - `tokenVaultA/B` for the CPI accounts list.
   - `tickSpacing`, `tickCurrentIndex`, and `sqrtPrice` for routing and slippage logic.
2. **Select tick arrays.** Each `TickArray` PDA covers `tickSpacing * 88` ticks. Compute the array index by floor-dividing the target tick by `tickSpacing * 88`, then derive PDAs for `tickArray0/1/2` so the swap never leaves the provided coverage. Failing to pass a contiguous sequence yields `InvalidTickArraySequence` (6023) or `TickArraySequenceInvalidIndex` (6038).
3. **Ensure token accounts exist.** `tokenOwnerAccountA/B` must be associated token accounts that match the whirlpool mints. If the pool enforces transfer-hook extensions, include the required remaining accounts as outlined in the IDL `AccountsType` hints.
4. **Choose the oracle PDA.** For legacy pools the oracle PDA seeds are `[b"oracle", whirlpool_pubkey]`. Adaptive-fee pools also require the oracle to store `tradeEnableTimestamp`, so confirm `tradeEnableTimestamp <= current_slot_time` or you will hit `TradeIsNotEnabled` (6064).

## Building the instruction

Below is a minimal Anchor-style call that mirrors the IDL:

```rust
let ix = whirlpool::instruction::Swap {
    amount,
    other_amount_threshold,
    sqrt_price_limit,
    amount_specified_is_input,
    a_to_b,
};
let accounts = whirlpool::accounts::Swap {
    token_program: spl_token::ID,
    token_authority,
    whirlpool,
    token_owner_account_a,
    token_vault_a,
    token_owner_account_b,
    token_vault_b,
    tick_array0,
    tick_array1,
    tick_array2,
    oracle,
};
anchor_lang::solana_program::program::invoke(
    &ix,
    accounts.to_account_metas(Some(true)).as_slice(),
)?;
```

Implementation notes:

- `sqrt_price_limit` should be encoded as `u128` in Q64.64. Set it to `0` or `u128::MAX` to replicate Uniswap v3 style “no limit” behavior in each direction.
- If you specify output (`amountSpecifiedIsInput=false`) make sure the `amount` field is the desired amount out, and update your price impact check to account for fees taken before verifying `otherAmountThreshold`.
- Always refresh fees via `updateFeesAndRewards` or read `Whirlpool.feeGrowthGlobal{A,B}` before quoting to avoid quoting stale liquidity state.
- The on-chain program asserts that `tokenOwnerAccountA/B` use the whirlpool’s mints (`token_mint_a/b`). Be sure every signer owns the correct ATAs and that they are delegated (if needed) to the same `tokenAuthority` that signs the swap.
- Pools backed by Token-2022 / transfer-hook mints may require `spl_token_2022::ID` (plus memo CPIs) instead of the legacy SPL token program. Inspect the IDL (`RemainingAccountsInfo`) or the upstream `programs/whirlpool` repo to determine which token program IDs to pass in and include any memo program requested by the pool.

### Extra / remaining accounts

The Anchor instruction definitions in the upstream repo note a few optional-but-required-in-context accounts that are delivered via the `remaining_accounts` slice:

- **Adaptive fee pools:** `swap.rs` documents that the oracle PDA must be passed as writable through the remaining accounts list so the handler can mutate adaptive fee variables. You can also mark the oracle as writable in the fixed account list, but the cheaper pattern is to add it (and in `two_hop_swap`, both oracles) to `remaining_accounts` ahead of time.
- **Transfer-hook or Token-2022 pools:** When a pool advertises transfer hooks or supplemental tick arrays in `RemainingAccountsInfo`, append the slices in the same order as declared (`AccountsType::TransferHookA`, `TransferHookInput`, etc.). The program validates the counts, so mismatches will raise `RemainingAccountsInvalidSlice`/`RemainingAccountsInsufficient`.
- **Supplemental tick arrays:** Some pools emit `AccountsType::SupplementalTickArrays{,One,Two}` when their effective tick range exceeds three PDA windows. Supply the additional tick-array PDAs in the provided order or swaps will halt with `InvalidTickArraySequence`.

## Monitoring and error handling

- Successful swaps emit the `Traded` event (see IDL `events.Traded`) with before/after sqrt price, actual input/output, LP fee, and protocol fee. Subscribe to this log to reconcile PnL.
- Common failure codes during swaps:
  - `6011 SqrtPriceOutOfBounds` – limit crosses beyond initialized tick ranges.
  - `6035 ZeroTradableAmount` – swap encountered zero liquidity in the provided arrays.
  - `6034 InvalidSqrtPriceLimitDirection` – limit price does not lie in the trade direction.
  - `6055 TooManySupplementalTickArrays` or `6049 RemainingAccountsInsufficient` – occurs when transfer-hook extra accounts do not match the declared `RemainingAccountsInfo` slices.

## Advanced paths

- Use `twoHopSwap` when routing through an intermediate mint; the IDL shows duplicated tick-array groups plus separate `sqrtPriceLimitOne/Two` arguments.
- Adaptive-fee pools may require the `memoProgram` and mint-specific token programs (`tokenProgramA/B`) when using the V2 liquidity instructions. These requirements are spelled out in the same IDL file so keep it handy when constructing multi-instruction bundles.

Keep `protocol/pools/whirlpool.json` under version control and regenerate this doc whenever the upstream IDL changes so downstream bots stay in sync with the on-chain contract signature.
