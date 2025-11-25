# Lifinity AMM v2 Swap Guide

Use this reference when constructing `swap` instructions against the `lifinity_amm_v2` program (`2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c`). Every field below is derived from `protocol/pools/lifinity_amm_v2.json`.

## Instruction surface

| Arg | Type | Notes |
| --- | --- | --- |
| `amount_in` | `u64` | Exact input amount taken from the user source token account. |
| `minimum_amount_out` | `u64` | Slippage guard; the swap aborts with `ExceededSlippage (6016)` if the quote falls below this value. |

## Account map

| Account | Mut | Signer | Purpose |
| --- | --- | --- | --- |
| `authority` | ❌ | ❌ | Program-derived authority stored on the AMM state; signs for pool vaults via PDA seeds. |
| `amm` | ✅ | ❌ | On-chain `Amm` account that stores curve config, fee params, vault mints, oracle config, and freeze flags. Must match the pool you intend to trade. |
| `user_transfer_authority` | ❌ | ✅ | Delegated authority that has an SPL allowance on `source_info`. Typically derived via `spl_token::instruction::approve`. |
| `source_info` | ✅ | ❌ | User-owned SPL token account providing the input asset. Owner must equal `user_transfer_authority` (delegated) or the signing key. |
| `destination_info` | ✅ | ❌ | User-owned SPL token account receiving the output asset. Owner must equal the wallet signer. |
| `swap_source` | ✅ | ❌ | Pool vault that holds the same mint as `source_info`; recorded in the AMM as `token_a_account` or `token_b_account`. |
| `swap_destination` | ✅ | ❌ | Pool vault that holds the opposite side of the pair. |
| `pool_mint` | ✅ | ❌ | Pool LP mint; must match the AMM’s `pool_mint`. Used internally for fee/price accounting. |
| `fee_account` | ✅ | ❌ | SPL account receiving owner/host fees, recorded in `fees`. Must match AMM config or `IncorrectFeeAccount (6019)` triggers. |
| `token_program` | ❌ | ❌ | Usually `spl_token::ID`. Must match `token_program_id` stored on the AMM or `IncorrectTokenProgramId (6024)` fires. |
| `oracle_main_account` | ❌ | ❌ | Primary price feed account; the address lives in `oracleMainAccount`. |
| `oracle_sub_account` | ❌ | ❌ | Secondary oracle feed (confidence check). |
| `oracle_pc_account` | ❌ | ❌ | Optional PC/Pyth oracle enforcing price buffers. |

## Required pre-flight checks

- Read the `Amm` account once and verify: addresses of both vaults, `pool_mint`, `fee_account`, and oracle keys. Reject stale caches because the program enforces strict equality checks (`IncorrectSwapAccount`, `IncorrectPoolMint`, `IncorrectOracleAccount`).
- Ensure `freeze_trade == 0`; otherwise the program will hard-fail with `InvalidInstruction` even if the instruction compiles.
- Confirm the SPL allowance on `source_info` covers `amount_in`; the program does not handle short allowances.
- When routing via smart order logic, recompute `minimum_amount_out` per hop to avoid `ExceededSlippage` cascades.
- Ensure oracle accounts share the same slot/price regime the AMM expects (`oracleStatus`, `oracleMainSlotLimit`, `oracleSubConfidenceLimit`). Off-spec feeds result in `InvalidInput` or `CalculationFailure`.

## Building the instruction

1. **Fetch pool metadata** via RPC (`getProgramAccounts` on the AMM program or cached registry) and deserialize using the `Amm` struct layout in the IDL.
2. **Assemble accounts** exactly as listed above. `authority` is PDA-only; do **not** sign with it. Anchor-style clients can call `program.derived_address(&[b"authority", amm.key().as_ref()])` or reuse on-chain metadata.
3. **Approve spending**: issue an SPL `Approve` so `user_transfer_authority` can debit `source_info` for `amount_in`.
4. **Quote output** using your simulator. Apply spread + platform fee from `amm.fees` and respect `amm.config.std_spread/std_spread_buffer` for aggressive routing.
5. **Set slippage guard**: `minimum_amount_out = quote * (1 - user_slippage_bps)`. The on-chain program enforces `minimum_amount_out <= actual_out` using safe math (underflows map to `CalculationFailure`).
6. **Serialize instruction data** as two little-endian `u64` values (`amount_in`, `minimum_amount_out`).
7. **Send transaction** with signer set { wallet, `user_transfer_authority` }. Anchor example:

```rust
let ix = Instruction {
    program_id: lifinity_program_id,
    accounts: vec![
        AccountMeta::new_readonly(authority, false),
        AccountMeta::new(amm, false),
        AccountMeta::new_readonly(user_transfer_authority, true),
        AccountMeta::new(source_info, false),
        AccountMeta::new(destination_info, false),
        AccountMeta::new(swap_source, false),
        AccountMeta::new(swap_destination, false),
        AccountMeta::new(pool_mint, false),
        AccountMeta::new(fee_account, false),
        AccountMeta::new_readonly(spl_token::ID, false),
        AccountMeta::new_readonly(oracle_main, false),
        AccountMeta::new_readonly(oracle_sub, false),
        AccountMeta::new_readonly(oracle_pc, false),
    ],
    data: lifinity_amm_v2::instruction::Swap { amount_in, minimum_amount_out }.data(),
};
```

## Failure modes to monitor

| Code | Name | Typical trigger |
| --- | --- | --- |
| 6000 | `AlreadyInUse` | You passed the same AMM account to two writes within a single atomic transaction. |
| 6009 | `InvalidInput` | Source/destination vault mismatch or unsupported oracle combination. |
| 6013 | `CalculationFailure` | Overflow/underflow when computing output; generally indicates stale pool state. |
| 6016 | `ExceededSlippage` | Realized price worse than `minimum_amount_out`. |
| 6019 | `IncorrectFeeAccount` | Fee account mismatch versus `amm.fee_account`. |
| 6024 | `IncorrectTokenProgramId` | Using Token-2022 on a Token-2020 pool (or vice versa). |
| 6025 | `IncorrectOracleAccount` | Any oracle key not matching the AMM config. |
| 6029 | `UnsupportedCurveOperation` | Attempting to trade on a pool running a curve that does not permit the direction or size requested.

## Operational checklist

- Keep the latest AMM snapshot cached client-side to minimize RPC calls and avoid race conditions.
- Run a dry-run simulation (e.g., `simulateTransaction`) before broadcasting when routing large trades.
- Alert if `freeze_trade`, `freeze_deposit`, or `freeze_withdraw` toggles mid-block; this often precedes maintenance.
- Log oracle slots and confidence intervals alongside swap fills for faster post-mortems when trades revert.
