# lb_clmm Swap Reference

Program: `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` (IDL v0.10.1, see `idls/pools/lb_clmm.json`). All account layouts, events, errors, and constants referenced below are copied from that IDL.

## Core accounts touched by every swap

- `lb_pair`: central pool state (holds static + dynamic fee params, bin step, active bin id, reserves, oracle, creator controls, pre-activation settings). Must stay writable across the whole swap.
- `bin_array_bitmap_extension` (optional): extra bitmap state required once the pool spans more than the 512 bins covered by the inline bitmap on the `lb_pair`. Supply it whenever `lb_pair.relations` demands it; Anchor will skip it for small pools.
- `reserve_x` / `reserve_y`: SPL token vaults owned by the pool PDA. They must match the `token_x_mint` and `token_y_mint` found inside the `lb_pair` account.
- `user_token_in` / `user_token_out`: Associated token accounts controlled by the taker. Their mints determine swap direction (X→Y when token_in mint == token_x_mint, else Y→X).
- `token_x_mint` / `token_y_mint`: Pure metadata accounts so the program can verify mint/program flags (Token Program vs Token-2022) as stored on `lb_pair`.
- `oracle`: sliding-window oracle buffer that tracks crossed bins and volatility. It must remain writable so dynamic fee math can push a fresh observation.
- `host_fee_in` (optional): ATA controlled by an integrator if you want to capture partner fees. When present, 20% (`HOST_FEE_BPS = 2000`) of the taker fee is redirected here before protocol fees are computed. The mint must match `user_token_in`.
- `user`: signer that authorizes debiting `user_token_in` and receiving `user_token_out`.
- `token_x_program` / `token_y_program`: Either the legacy SPL Token program (`Tokenkeg…`) or the Token-2022 program, matching the per-mint flags stored in `lb_pair`.
- `event_authority`, `program`: Anchor boilerplate so `Swap` events can be emitted and cross-program invocations can verify the origin.

## Instruction menu

### `swap`

Exact-in swap with simple slippage.

- Args: `amount_in` (u64), `min_amount_out` (u64).
- Accounts: base set listed above. No memo, no extra remaining accounts.
- Use when both legs are SPL Token classic and no transfer hooks are needed.

### `swap2`

Exact-in swap plus transfer-hook plumbing.

- Adds `memo_program` (`MemoSq4…`).
- Args: `amount_in`, `min_amount_out`, `remaining_accounts_info`.
- You must also append the actual extra accounts (one flat `remaining_accounts` vector) after the canonical accounts, matching the `RemainingAccountsInfo` layout described later.
- Required whenever either mint carries a Token-2022 transfer hook, or when an integrator wants an on-chain memo for compliance.

### `swap_exact_out`

Exact-out swap.

- Args: `max_in_amount` (u64 ceiling), `out_amount` (u64 target).
- Reverts with `NotExactAmountOut (6057)` if the produced amount does not match `out_amount` or `ExceededAmountSlippageTolerance (6003)` if the input cap is pierced.

### `swap_exact_out2`

Exact-out + transfer hooks.

- Same as `swap_exact_out`, but mirrors the `memo_program` and `remaining_accounts_info` requirements of `swap2`.

### `swap_with_price_impact`

Exact-in swap with on-chain price guards.

- Args:
  - `amount_in`
  - `active_id: Option<i32>`: assert the pool’s current `active_id` before the swap starts. Supplying `Some(id)` guards against races; `None` skips the check.
  - `max_price_impact_bps: u16`: upper bound (in BPS) on the price move measured between the first and last crossed bins. Trips `ExceededAmountSlippageTolerance (6003)` if violated.
- Useful for MEV-sensitive flows where `min_amount_out` alone is not enough.

### `swap_with_price_impact2`

Same functionality as above plus `memo_program` and `remaining_accounts_info` so hooks/memos can run.

## Remaining account plumbing

Transfer-hook aware instructions (`swap2`, `swap_exact_out2`, `swap_with_price_impact2`) accept:

```rust
pub struct RemainingAccountsInfo {
    pub slices: Vec<RemainingAccountsSlice>,
}

pub struct RemainingAccountsSlice {
    pub accounts_type: AccountsType,
    pub length: u8,
}

pub enum AccountsType {
    TransferHookX,
    TransferHookY,
    TransferHookReward,
    TransferHookMultiReward(u8),
}
```

Implementation notes:

- After the fixed account list, push every extra account needed by your token extensions in the same order that the slices describe. Each slice contracts a contiguous chunk of the trailing accounts.
- `length` is counted in accounts, not bytes. Keep it `<= 255` (u8).
- Use `TransferHookX`/`TransferHookY` for Token-2022 mints that expect extra accounts during transfers. The hook program is read from the mint itself; omitting the corresponding slice yields `MissingRemainingAccountForTransferHook (6077)`.
- `TransferHookReward` and `TransferHookMultiReward(u8 reward_count)` are for reward-vault transfers that also rely on hooks. The additional `u8` carried by `TransferHookMultiReward` is baked into the instruction data (not the slice) and tells the program how many reward mints share that account chunk.
- The runtime enforces `InvalidRemainingAccountSlice (6075)` and `DuplicatedRemainingAccountTypes (6076)` if slices overlap or repeat.

## Host-fee behavior

- Pass `host_fee_in` only if you operate an integrator wallet. When present, `host_fee = taker_fee * HOST_FEE_BPS / BASIS_POINT_MAX` and is transferred before protocol fees (`protocol_share`) are skimmed.
- The emitted `Swap` event exposes `fee`, `protocol_fee`, `fee_bps`, and `host_fee` so you can reconcile taker vs host earnings off-chain.

## Price, liquidity, and activation guards

- Exact-in swaps (`swap`, `swap2`) enforce `min_amount_out`; exact-out swaps (`swap_exact_out*`) enforce `max_in_amount`; price-impact variants enforce `max_price_impact_bps`. Breaching any guard aborts the instruction—no partial fills.
- Pools can be disabled (`PoolDisabled`, code 6042) or still inside their activation window. Before `lb_pair.activation_point`, only the `lb_pair.pre_activation_swap_address` may trade once the clock passes `activation_point - pre_activation_duration`. Otherwise you will see `AlreadyPassPreActivationSwapPoint (6065)` or `UnauthorizedAddress (6048)` when trying to front-run activation.
- The program also guards against runaway volume via `ExceedMaxSwappedAmount (6053)` and enforces quote-side invariants with `InsufficientOutAmount (6039)`.

## Swap event payload

Every successful swap emits the `Swap` event (discriminator `[81, 108, 227, 190, 205, 208, 10, 196]`):

```
struct Swap {
    lb_pair: Pubkey,
    from: Pubkey,
    start_bin_id: i32,
    end_bin_id: i32,
    amount_in: u64,
    amount_out: u64,
    swap_for_y: bool,
    fee: u64,
    protocol_fee: u64,
    fee_bps: u128,
    host_fee: u64,
}
```

Use the event to reconstruct executed price ranges (`start_bin_id`→`end_bin_id`) and reconcile the various fee buckets.

## Example: guarded Token-2022 swap with hooks

1. Build `RemainingAccountsInfo` with two slices: one for the X-side transfer hook (include the mint’s hook program + required PDAs) and one for the reward hook if your integrator receives LM rewards mid-swap.
2. Append those extra accounts to the instruction after the canonical account list.
3. Call `swap_with_price_impact2` with:
   - `amount_in` sized to your order.
   - `active_id = Some(observed_active_bin)` from your off-chain price oracle.
   - `max_price_impact_bps` tuned to your tolerance (e.g., 50 = 0.5%).
   - Simulate the route off-chain and cancel the transaction locally if the predicted `amount_out` would fall below your tolerance; the on-chain instruction does not take a `min_amount_out` argument.
4. Provide `host_fee_in` only if you expect to earn the 20% host rebate; otherwise omit it to save an account write.
5. Verify on-chain success by watching the `Swap` event and ensuring the `start_bin_id` you crossed lines up with your routing simulation.

## Troubleshooting checklist

- `NoTransferHookProgram (6078)`: you passed hook accounts but the mint lacks the transfer-hook extension. Remove the slice or upgrade the mint metadata.
- `ExceededAmountSlippageTolerance (6003)`: either your `min_amount_out`/`max_in_amount` is too strict for current liquidity, or volatility increased between simulation and execution—check `oracle` history.
- `InvalidPoolAddress (6089)`: confirms that `lb_pair` doesn’t match the PDA derived from the provided token mints/bin step. Double-check your seeds before submitting the instruction.
- `UnauthorizedAddress (6048)`: pool creator locked the pair; wait for activation or use the whitelisted `pre_activation_swap_address`.

With these references you can build swap transactions straight from the IDL without reverse-engineering the on-chain binary, while still taking advantage of the dynamic fee, host fee, and transfer-hook features baked into `lb_clmm`.
