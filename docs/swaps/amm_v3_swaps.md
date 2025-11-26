# Raydium AMM v3 Swap Playbook

This note explains how to construct "swap" instructions for Raydium's concentrated liquidity program (`CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`). The source of truth is `idls/pools/amm_v3.json` in this repo; every detail below is pulled directly from that IDL.

## Instruction to call

Use `swap_v2` (IDL discriminator `2b 04 ed 0b 1a c9 1e 62`). The legacy `swap` instruction is still present but does not support Token-2022 mints, so the bot should always invoke v2.

`swap_v2` arguments:

| Argument                      | Meaning                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `amount: u64`                 | When `is_base_input = true`, this is the exact number of input tokens to send. When `false`, it is the desired output amount.                       |
| `other_amount_threshold: u64` | Slippage limiter paired with `amount`. If base-in, this is the minimum output we will accept. If base-out, this is the maximum input we will pay.   |
| `sqrt_price_limit_x64: u128`  | Q64.64 square-root price limit. Set to `0` to allow the pool to move in the favorable direction until liquidity runs out, otherwise clamp movement. |
| `is_base_input: bool`         | `true` = exact-in swap (token vault of the input side is debited). `false` = exact-out swap.                                                        |

## Required accounts

| Account                | Signer | Writable | Derivation / Notes                                                                                                |
| ---------------------- | ------ | -------- | ----------------------------------------------------------------------------------------------------------------- |
| `payer`                | ✅     | ❌       | User/signing authority for token transfers.                                                                       |
| `amm_config`           | ❌     | ❌       | Factory/fee config PDA (`["amm_config", index]`). Must match the pool you are hitting.                            |
| `pool_state`           | ❌     | ✅       | Pool PDA `["pool", amm_config, token_mint_0, token_mint_1]`. Token mints are sorted (mint0 < mint1).              |
| `input_token_account`  | ❌     | ✅       | User-owned SPL/Token-2022 account providing liquidity. Must hold the mint that corresponds to the swap direction. |
| `output_token_account` | ❌     | ✅       | User-owned destination account for proceeds.                                                                      |
| `input_vault`          | ❌     | ✅       | Pool vault PDA `["pool_vault", pool_state, input_mint]`. Choose vault 0 or 1 depending on direction.              |
| `output_vault`         | ❌     | ✅       | Pool vault PDA `["pool_vault", pool_state, output_mint]`.                                                         |
| `observation_state`    | ❌     | ✅       | Oracle PDA `["observation", pool_state]` that tracks TWAP data.                                                   |
| `token_program`        | ❌     | ❌       | Usually `Tokenkeg...`. Required even when the mint is Token-2022.                                                 |
| `token_program_2022`   | ❌     | ❌       | `TokenzQdB...`; still supplied when neither side is Token-2022.                                                   |
| `memo_program`         | ❌     | ❌       | `MemoSq4gq...` as mandated by the IDL.                                                                            |
| `input_vault_mint`     | ❌     | ❌       | Mint that backs the `input_vault`.                                                                                |
| `output_vault_mint`    | ❌     | ❌       | Mint that backs the `output_vault`.                                                                               |

> The v1 `swap` instruction also takes a `tick_array` account. `swap_v2` relies on the pool's internal tick bitmap + observation account, so no explicit tick-array PDAs are needed for single-hop swaps.

## Building the instruction

1. **Order the token mints** – fetch the pool metadata from CoreCast (or your cache) and sort the two mints lexicographically to reproduce `token_mint_0` / `token_mint_1`.
2. **Derive `pool_state`** – use the PDA seeds `[b"pool", amm_config, token_mint_0, token_mint_1]`. The bump lives inside `PoolState.bump` and is used everywhere else.
3. **Derive vaults** – `token_vault_0` and `token_vault_1` use seeds `[b"pool_vault", pool_state, token_mint_x]`. Pick which one is the input/output vault depending on `is_base_input` and which mint you’re swapping in.
4. **Observation account** – PDA `[b"observation", pool_state]`. This must be writable because the AMM records the latest oracle sample during every swap.
5. **Select the correct token program** – Raydium always wants both `token_program` (classic SPL) and `token_program_2022`. When neither side is a Token-2022 mint, both remain the canonical program IDs from the IDL.
6. **Fill user accounts** – `input_token_account` must allow `payer` as authority (either directly or via delegate). `output_token_account` can be any existing associated token account.
7. **Set arguments** –
   - For exact-in: `amount = amount_in`, `other_amount_threshold = min_amount_out`, `is_base_input = true`.
   - For exact-out: `amount = desired_amount_out`, `other_amount_threshold = max_amount_in`, `is_base_input = false`.
   - `sqrt_price_limit_x64` can be left at `0` for “best effort” swaps or set to a clamp computed from your slippage math (convert price boundary to √P and left-shift by 64).
8. **Serialize and send** – use the IDL discriminator provided above, append the encoded arguments (little-endian), and pass the account metas in the exact order defined by the IDL.

## Example (pseudocode)

```rust
use raydium_amm_v3::{SwapV2Args, ID as AMM_V3_ID};

let accounts = raydium_amm_v3::accounts::SwapV2 {
    payer,
    amm_config,
    pool_state,
    input_token_account,
    output_token_account,
    input_vault,
    output_vault,
    observation_state,
    token_program: spl_token::id(),
    token_program_2022: spl_token_2022::id(),
    memo_program: spl_memo::id(),
    input_vault_mint,
    output_vault_mint,
};

let args = SwapV2Args {
    amount: amount_in,
    other_amount_threshold: min_amount_out,
    sqrt_price_limit_x64: 0,
    is_base_input: true,
};

let ix = raydium_amm_v3::instruction::swap_v2(
    &AMM_V3_ID,
    accounts,
    args,
);
```

## Operational tips

- **Tick coverage** – make sure the pool has liquidity spanning your price limit; `InsufficientLiquidityForDirection (6041)` means you tried to cross uninitialized ticks without the proper observation state.
- **Vault/mint alignment** – the mint you pass in `input_vault_mint` must match the vault PDA. Mixing them will trip the program’s vault checks before any token is moved.
- **Token-2022 fees** – if either side is a Token-2022 mint with transfer fees, the program verifies the `TokenzQdB...` program id and recomputes transfer fees internally. Passing the classic token program for a 2022 mint will fail.
- **Memo requirement** – even though we are not writing a memo, the instruction expects the memo program’s ID in the account list. Supply `MemoSq4gq...` or the call will be rejected.

With these pieces in place, Stage 2 can assemble Raydium CLMM swaps deterministically and drop them straight into Jito bundles.
