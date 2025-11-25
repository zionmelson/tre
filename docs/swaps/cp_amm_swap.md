# cp_amm Swap Guide

This document distills the Anchor IDL at `protocol/pools/cp_amm.json` (program version 0.1.5) into a practical guide for executing swaps against cp_amm pools.

## Instruction Overview

**`swap`** → compatibility wrapper that always performs an _exact-in_ trade. On-chain it immediately re-encodes your `SwapParameters` as `SwapParameters2` with `SwapMode::ExactIn` (see `programs/cp-amm/src/lib.rs`).
**`swap2`** → generalized interface that exposes the full `SwapMode` enum so you can opt into exact-in, partial-fill-in, or exact-out behaviors. It also emits the richer `SwapResult2` payload used by downstream indexers (see `programs/cp-amm/src/instructions/swap/ix_swap.rs`).

Both instructions share the same account set and differ only in the argument struct that is serialized into the instruction data.

## Required Accounts

| Account                  | Writable | Signer | Notes from IDL                                                                                                                                                                                                  |
| ------------------------ | -------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pool_authority`         | ✗        | ✗      | Must be the fixed authority `HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC` baked into the program. Used to validate the CPI signer seeds for pool vaults.                                                       |
| `pool`                   | ✓        | ✗      | cp_amm pool state account. Determines vaults, fee config, and pool status (`PoolDisabled` will abort with error 6003).                                                                                          |
| `input_token_account`    | ✓        | ✗      | User-owned SPL token account that holds the asset you are selling. Must match the mint passed as `token_a_mint` for this instruction invocation.                                                                |
| `output_token_account`   | ✓        | ✗      | User-owned SPL token account that receives the asset you are buying. Must match `token_b_mint` for this invocation.                                                                                             |
| `token_a_vault`          | ✓        | ✗      | Pool vault that currently treats its mint as the _input_ side for this swap. Swap direction is enforced by how you order the vaults (`token_a_vault` == input vault, `token_b_vault` == output vault) per call. |
| `token_b_vault`          | ✓        | ✗      | Pool vault that releases the output tokens.                                                                                                                                                                     |
| `token_a_mint`           | ✗        | ✗      | Mint of the asset flowing from the user into the pool for this swap.                                                                                                                                            |
| `token_b_mint`           | ✗        | ✗      | Mint of the asset flowing from the pool back to the user.                                                                                                                                                       |
| `payer`                  | ✗        | ✓      | User or program authority executing the swap. Pays rent/fees and authorizes token transfers.                                                                                                                    |
| `token_a_program`        | ✗        | ✗      | SPL Token program ID that owns `token_a_mint`. Use `spl-token-2022` when interacting with Token-2022 mints, otherwise `spl-token`.                                                                              |
| `token_b_program`        | ✗        | ✗      | SPL Token program ID that owns `token_b_mint`.                                                                                                                                                                  |
| `referral_token_account` | ✓        | ✗      | Optional account that receives referral fees when the pool is configured with a referral share. Omit or pass `None` if you do not use referrals.                                                                |
| `event_authority`        | ✗        | ✗      | PDA derived from the literal seed `"__event_authority"`. Required so the program can emit Anchor events (`EvtSwap`, `EvtSwap2`).                                                                                |
| `program`                | ✗        | ✗      | cp_amm program ID.                                                                                                                                                                                              |

> **Directionality reminder:** cp_amm does not infer direction from the pool state. The caller decides which vault/mint pair is `token_a` (input) or `token_b` (output) _per swap_. Always align the four accounts (`input_token_account`, `token_a_vault`, `token_a_mint`, `token_a_program`) with the asset you are sending in.

## Argument Layouts

### `swap` → `SwapParameters`

| Field                | Type  | Meaning                                                                                                                                          |
| -------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `amount_in`          | `u64` | Exact number of `token_a_mint` units to debit from `input_token_account`.                                                                        |
| `minimum_amount_out` | `u64` | Lower bound on tokens to credit to `output_token_account`. The program throws error 6002 (`ExceededSlippage`) if the computed output is smaller. |

### `swap2` → `SwapParameters2`

| Field       | Type  | Meaning                                                                                                                                                                             |
| ----------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `amount_0`  | `u64` | Acts as `amount_in` for exact-in swaps; acts as `amount_out` target for exact-out swaps.                                                                                            |
| `amount_1`  | `u64` | Acts as `minimum_amount_out` (exact-in) or `maximum_amount_in` (exact-out).                                                                                                         |
| `swap_mode` | `u8`  | Serialized `SwapMode` enum defined in `programs/cp-amm/src/instructions/swap/ix_swap.rs` (0 = `ExactIn`, 1 = `PartialFill`, 2 = `ExactOut`). Any other value causes `InvalidInput`. |

#### SwapMode semantics

- **`ExactIn` (0)** – deterministic swap: `amount_0` is debited (after applying token-transfer fees) and the program enforces `amount_1` as the minimum output (`swap_exact_in.rs`). Fails with `ExceededSlippage` if the computed out amount is lower.
- **`PartialFill` (1)** – upper-bounded swap: the engine treats `amount_0` as the _maximum_ input, spends just enough to hit the pool’s next price boundary, and returns any unused input in `SwapResult2.amount_left` (`swap_partial_fill.rs`). `amount_1` still guards the minimum output after transfer-fee adjustments.
- **`ExactOut` (2)** – goal-seeking swap: `amount_0` becomes the desired output (after transfer fees) and `amount_1` bounds how much input you are willing to spend. The program solves for the input requirement and aborts if it exceeds `amount_1` (`swap_exact_out.rs`).

Because `swap` always wraps to `ExactIn`, use `swap2` when you need partial fills or exact-out guarantees.

### Events and telemetry

- `EvtSwap` carries `{ pool, payer, input_mint, output_mint, params: SwapParameters, swap_result: SwapResult }` where `swap_result` exposes total output, next sqrt price, and fee breakdowns.
- `EvtSwap2` emits the richer `SwapResult2` which additionally surfaces `included_fee_input_amount`, `excluded_fee_input_amount`, and `amount_left` (unspent input returned during `PartialFill`).

Subscribing to these events (via `event_authority`) is the canonical way to observe fills and fees without re-simulating the swap logic.

## Building a Swap Instruction

1. **Fetch pool + vault metadata.** Read the `Pool` account to confirm status, vault addresses, and supported mints. Abort if `pool.status != Active` or if rate-limiter constraints (errors 6048–6049) require throttling.
2. **Select direction.** Decide which asset you sell. Pass its mint, vault, token program, and your ATA as all of the `token_a_*` + `input_token_account` entries.
3. **Compute amounts.** Use local math to predict output given pool reserves and fees. Populate either `SwapParameters` or `SwapParameters2` accordingly.
4. **Assemble accounts.**
   - Add `pool_authority` (constant), `pool`, both vault PDAs, both token programs.
   - Optionally include `referral_token_account` if you expect referral fees (must be writable & owned by referral recipient).
5. **Serialize arguments.** Use `SwapParameters::try_to_vec()` or a borsh-compatible serializer. Anchor clients can create instruction data via the generated `cp_amm::instruction::Swap { params }` helpers.
6. **Sign and send.** The `payer` must sign. Include any prerequisite compute budget or price-oracle accounts your client logic needs; the cp_amm instruction itself only consumes the accounts above.
7. **Verify output.** Parse the returned `TransactionMeta`. If logs mention `ExceededSlippage`, your `minimum_amount_out` (or `maximum_amount_in`) was too aggressive.

## Example (Rust + Anchor client)

```rust
use anchor_lang::{prelude::*, InstructionData, ToAccountMetas};
use solana_sdk::instruction::Instruction;

let params = cp_amm::instruction::SwapParameters {
    amount_in: 50_000_000,            // 50 USDC with 6 decimals
    minimum_amount_out: 1_230_000,    // require at least 1.23 SOL (9 dp)
};

let accounts = cp_amm::accounts::Swap {
    pool_authority: CP_AMM_POOL_AUTHORITY,
    pool,
    input_token_account: user_usdc_ata,
    output_token_account: user_sol_wrapped_ata,
    token_a_vault: pool_usdc_vault,
    token_b_vault: pool_sol_vault,
    token_a_mint: usdc_mint,
    token_b_mint: sol_wrapped_mint,
    payer: user,
    token_a_program: spl_token::ID,
    token_b_program: spl_token::ID,
    referral_token_account: None, // or Some(referral_ata)
    event_authority: cp_amm::EVENT_AUTHORITY,
    program: cp_amm::ID,
};

let ix = Instruction {
    program_id: cp_amm::ID,
    accounts: accounts.to_account_metas(None),
    data: cp_amm::instruction::Swap { params }.data(),
};
```

Switch to `Swap2` by importing `cp_amm::instruction::Swap2` and populating `SwapParameters2` (remember to set `swap_mode`).

## Common Failure Modes

| Error                                           | Code      | When it triggers                                                                            |
| ----------------------------------------------- | --------- | ------------------------------------------------------------------------------------------- |
| `ExceededSlippage`                              | 6002      | Pool output `< minimum_amount_out` (or exact-out attempt needs more input than `amount_1`). |
| `PoolDisabled`                                  | 6003      | Pool status halted by governance.                                                           |
| `InsufficientLiquidity`                         | 6023      | Vaults cannot satisfy the requested output.                                                 |
| `InvalidInput`                                  | 6018      | Account ordering/mints/token programs do not match the pool configuration.                  |
| `FailToValidateSingleSwapInstruction`           | 6049      | Rate limiter rejected the swap (e.g., suspicious volume). Wait or reduce size.              |
| `InvalidFeeRateLimiter` / `InvalidFeeScheduler` | 6048/6050 | Pool misconfiguration—alert operators.                                                      |
| `UnsupportNativeMintToken2022`                  | 6026      | Attempted to pair Token-2022 mint with the legacy token program (or vice versa).            |

Handle these errors explicitly so your client can retry with adjusted parameters or mark the pool unhealthy.

## Operational Tips

- **Simulation first:** Use `simulateTransaction` with the exact instruction payload to preview `SwapResult` logs before submitting mainnet transactions.
- **Referrals:** If you omit `referral_token_account`, referral fees (if any) accrue to the pool’s configured default.
- **Rate limits:** Pools using dynamic fees or rate limiting may change `lp_fee`/`protocol_fee` between slots; rebuild the instruction shortly before sending to avoid stale math.
- **Event decoding:** Anchor event discriminators for swaps are `0x1b3c15d58aaa bb93` (`EvtSwap`) and `0xbd4233a826507799` (`EvtSwap2`). Use these to filter websocket logs when indexing fills.

With these guidelines, you should be able to construct, send, and monitor cp_amm swap transactions directly from the IDL without reverse-engineering the on-chain program.
