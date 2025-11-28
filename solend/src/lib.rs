//! Lightweight Solend helpers extracted from the upstream token-lending SDK.
//! Only the pieces required by `tre-arb-bot` are retained here so we can
//! avoid vendoring the full Solana Program Library repository.

use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::sysvar;

/// Canonical Solend program id on mainnet.
pub const PROGRAM_ID: Pubkey = pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo");

/// SPL Token program id. Exposed here to keep Solend-specific code together.
pub const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

/// Opcode used by the Solend flash borrow instruction.
pub const FLASH_BORROW_OPCODE: u8 = 19;

/// Opcode used by the Solend flash repay instruction.
pub const FLASH_REPAY_OPCODE: u8 = 20;

/// Builds the canonical Solend flash-borrow instruction.
pub fn flash_borrow_instruction(
    program_id: Pubkey,
    amount: u64,
    reserve_liquidity_supply: Pubkey,
    destination_liquidity: Pubkey,
    reserve: Pubkey,
    lending_market: Pubkey,
    lending_market_authority: Pubkey,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(reserve_liquidity_supply, false),
        AccountMeta::new(destination_liquidity, false),
        AccountMeta::new(reserve, false),
        AccountMeta::new_readonly(lending_market, false),
        AccountMeta::new_readonly(lending_market_authority, false),
        AccountMeta::new_readonly(sysvar::instructions::ID, false),
        AccountMeta::new_readonly(SPL_TOKEN_PROGRAM_ID, false),
    ];

    let mut data = Vec::with_capacity(1 + std::mem::size_of::<u64>());
    data.push(FLASH_BORROW_OPCODE);
    data.extend_from_slice(&amount.to_le_bytes());

    Instruction {
        program_id,
        accounts,
        data,
    }
}

/// Builds the canonical Solend flash-repay instruction.
pub fn flash_repay_instruction(
    program_id: Pubkey,
    amount: u64,
    source_liquidity: Pubkey,
    reserve_liquidity_supply: Pubkey,
    reserve_liquidity_fee_receiver: Pubkey,
    host_fee_receiver: Pubkey,
    reserve: Pubkey,
    lending_market: Pubkey,
    authority: Pubkey,
    borrow_instruction_index: u8,
) -> Instruction {
    let accounts = vec![
        AccountMeta::new(source_liquidity, false),
        AccountMeta::new(reserve_liquidity_supply, false),
        AccountMeta::new(reserve_liquidity_fee_receiver, false),
        AccountMeta::new(host_fee_receiver, false),
        AccountMeta::new(reserve, false),
        AccountMeta::new_readonly(lending_market, false),
        AccountMeta::new_readonly(authority, true),
        AccountMeta::new_readonly(sysvar::instructions::ID, false),
        AccountMeta::new_readonly(SPL_TOKEN_PROGRAM_ID, false),
    ];

    let mut data = Vec::with_capacity(1 + std::mem::size_of::<u64>() + 1);
    data.push(FLASH_REPAY_OPCODE);
    data.extend_from_slice(&amount.to_le_bytes());
    data.push(borrow_instruction_index);

    Instruction {
        program_id,
        accounts,
        data,
    }
}
