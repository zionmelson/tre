#![allow(dead_code)]

#[derive(Debug, Clone, Copy)]
pub enum SwapMintRole {
    Mint0,
    Mint1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SwapTemplateDirection {
    BaseIn,
    BaseOut,
}

#[derive(Debug)]
pub enum SeedSpec {
    Bytes(&'static [u8]),
    Account(&'static str),
    Arg(&'static str),
}

#[derive(Debug)]
pub struct PdaSpec {
    pub name: &'static str,
    pub seeds: &'static [SeedSpec],
}

#[derive(Debug)]
pub struct AccountSpec {
    pub name: &'static str,
    pub is_signer: bool,
    pub is_writable: bool,
    pub is_optional: bool,
    pub address: Option<&'static str>,
    pub pda: Option<&'static PdaSpec>,
    pub mint_role: Option<SwapMintRole>,
}

#[derive(Debug)]
pub struct ArgSpec {
    pub name: &'static str,
    pub ty: &'static str,
    pub fixed_value: Option<&'static str>,
}

#[derive(Debug)]
pub struct AmmV3SwapTemplate {
    pub direction: SwapTemplateDirection,
    pub program_id: &'static str,
    pub discriminator: [u8; 8],
    pub accounts: &'static [AccountSpec],
    pub args: &'static [ArgSpec],
}

#[derive(Debug)]
pub struct CpAmmSwapTemplate {
    pub name: &'static str,
    pub direction: SwapTemplateDirection,
    pub program_id: &'static str,
    pub discriminator: [u8; 8],
    pub accounts: &'static [AccountSpec],
    pub args: &'static [ArgSpec],
}

#[derive(Debug)]
pub struct LbClmmSwapTemplate {
    pub name: &'static str,
    pub direction: SwapTemplateDirection,
    pub program_id: &'static str,
    pub discriminator: [u8; 8],
    pub accounts: &'static [AccountSpec],
    pub args: &'static [ArgSpec],
}

#[derive(Debug)]
pub struct LifinitySwapTemplate {
    pub direction: SwapTemplateDirection,
    pub program_id: &'static str,
    pub discriminator: [u8; 8],
    pub accounts: &'static [AccountSpec],
    pub args: &'static [ArgSpec],
}

#[derive(Debug)]
pub struct WhirlpoolSwapTemplate {
    pub direction: SwapTemplateDirection,
    pub program_id: &'static str,
    pub discriminator: [u8; 8],
    pub accounts: &'static [AccountSpec],
    pub args: &'static [ArgSpec],
}

#[derive(Debug)]
pub struct StructSpec {
    pub name: &'static str,
    pub fields: &'static [ArgSpec],
}

include!(concat!(env!("OUT_DIR"), "/amm_v3_swap_templates.rs"));
include!(concat!(env!("OUT_DIR"), "/cp_amm_swap_templates.rs"));
include!(concat!(env!("OUT_DIR"), "/lb_clmm_swap_templates.rs"));
include!(concat!(env!("OUT_DIR"), "/lifinity_swap_templates.rs"));
include!(concat!(env!("OUT_DIR"), "/whirlpool_swap_templates.rs"));
