use std::{env, fmt::Write, fs, path::PathBuf};

use heck::ToSnakeCase;
use serde::Deserialize;
use sha2::{Digest, Sha256};

const BITQUERY_PROTOS: &[&str] = &[
    "bitquery/solana/block_message.proto",
    "bitquery/solana/dex_block_message.proto",
    "bitquery/solana/parsed_idl_block_message.proto",
    "bitquery/solana/token_block_message.proto",
    "bitquery/solana/corecast/corecast.proto",
    "bitquery/solana/corecast/request.proto",
    "bitquery/solana/corecast/stream_message.proto",
];

const JITO_PROTOS: &[&str] = &[
    "jito/auth.proto",
    "jito/block_engine.proto",
    "jito/block.proto",
    "jito/bundle.proto",
    "jito/packet.proto",
    "jito/relayer.proto",
    "jito/searcher.proto",
    "jito/shared.proto",
    "jito/shredstream.proto",
];

const IDL_POOL_DIR: &str = "idls/pools";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=bitquery");
    println!("cargo:rerun-if-changed=jito");
    println!("cargo:rerun-if-changed={IDL_POOL_DIR}");

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let proto_out = out_dir.clone();

    let cfg = tonic_build::configure()
        .build_server(false)
        .out_dir(proto_out)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf.Any", "::prost_types::Any")
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp");

    let includes = vec![
        PathBuf::from("bitquery"),
        PathBuf::from("bitquery/solana"),
        PathBuf::from("bitquery/solana/corecast"),
        PathBuf::from("jito"),
    ];

    cfg.compile(&[BITQUERY_PROTOS, JITO_PROTOS].concat(), &includes)?;

    emit_pool_templates(&out_dir)?;
    emit_swap_templates(&out_dir)?;

    Ok(())
}

fn emit_pool_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let mut entries = fs::read_dir(IDL_POOL_DIR)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map(|ext| ext == "json")
                .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.path());

    let mut buffer = String::new();
    buffer.push_str("#[derive(Debug)]\npub struct PoolTemplate {\n    pub name: &'static str,\n    pub bytes: &'static [u8],\n}\n\n");
    buffer.push_str("pub static POOL_TEMPLATES: &[PoolTemplate] = &[\n");

    for entry in entries {
        let path = entry.path();
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("unknown");
        let data = fs::read(&path)?;
        buffer.push_str("    PoolTemplate { name: \"");
        buffer.push_str(name);
        buffer.push_str("\", bytes: &[\n        ");
        for chunk in data.chunks(16) {
            let row = chunk
                .iter()
                .map(|byte| format!("0x{:02x}", byte))
                .collect::<Vec<_>>()
                .join(", ");
            buffer.push_str(&row);
            buffer.push_str(",\n        ");
        }
        buffer.push_str("], },\n");
    }

    buffer.push_str("];\n");

    fs::write(out_dir.join("pools_static.rs"), buffer)?;
    Ok(())
}

fn emit_swap_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    emit_amm_v3_swap_templates(out_dir)?;
    emit_cp_amm_swap_templates(out_dir)?;
    emit_lb_clmm_swap_templates(out_dir)?;
    emit_lifinity_swap_templates(out_dir)?;
    emit_whirlpool_swap_templates(out_dir)?;
    Ok(())
}

fn emit_amm_v3_swap_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let idl = load_idl("amm_v3")?;
    let program_id = idl
        .address
        .as_deref()
        .ok_or("amm_v3 idl missing program id")?;
    let Some(ix) = idl.instructions.iter().find(|ix| ix.name == "swap_v2") else {
        return Err("missing swap_v2 instruction in amm_v3 idl".into());
    };

    let mut buffer = String::new();
    buffer.push_str("pub static AMM_V3_SWAP_TEMPLATES: &[AmmV3SwapTemplate] = &[\n");

    for direction in [TemplateDirection::BaseIn, TemplateDirection::BaseOut] {
        let rendered = render_amm_v3_template(ix, program_id, direction)?;
        buffer.push_str(&rendered);
    }

    buffer.push_str("];\n");

    fs::write(out_dir.join("amm_v3_swap_templates.rs"), buffer)?;
    Ok(())
}

fn render_amm_v3_template(
    ix: &IdlInstruction,
    program_id: &str,
    direction: TemplateDirection,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    writeln!(snippet, "    AmmV3SwapTemplate {{")?;
    writeln!(
        snippet,
        "        direction: {},",
        direction.direction_expr()
    )?;
    writeln!(snippet, "        program_id: {},", quote(program_id))?;
    let discriminator = discriminator_bytes(ix)?;
    writeln!(
        snippet,
        "        discriminator: {disc},",
        disc = format_bytes(&discriminator)
    )?;
    writeln!(snippet, "        accounts: &[")?;
    for account in &ix.accounts {
        let rendered = render_account(account, direction)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    writeln!(snippet, "        args: &[")?;
    for arg in &ix.args {
        let fixed_value = match arg.name.as_str() {
            "is_base_input" => match direction {
                TemplateDirection::BaseIn => Some("true"),
                TemplateDirection::BaseOut => Some("false"),
            },
            _ => None,
        };
        let rendered = render_arg(arg, fixed_value)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    snippet.push_str("    },\n");
    Ok(snippet)
}

fn emit_cp_amm_swap_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let idl = load_idl("cp_amm")?;
    let program_id = idl
        .address
        .as_deref()
        .ok_or("cp_amm idl missing program id")?;

    const CP_SWAP_INSTRUCTIONS: &[&str] = &["swap", "swap2"];

    let mut buffer = String::new();
    buffer.push_str("pub static CP_AMM_SWAP_TEMPLATES: &[CpAmmSwapTemplate] = &[\n");

    for instr_name in CP_SWAP_INSTRUCTIONS {
        let Some(ix) = idl.instructions.iter().find(|ix| ix.name == *instr_name) else {
            return Err(format!("missing instruction {instr_name} in cp_amm idl").into());
        };

        match *instr_name {
            "swap" => {
                let rendered = render_cp_amm_template(ix, program_id, TemplateDirection::BaseIn)?;
                buffer.push_str(&rendered);
            }
            _ => {
                for direction in [TemplateDirection::BaseIn, TemplateDirection::BaseOut] {
                    let rendered = render_cp_amm_template(ix, program_id, direction)?;
                    buffer.push_str(&rendered);
                }
            }
        }
    }

    buffer.push_str("];\n");

    fs::write(out_dir.join("cp_amm_swap_templates.rs"), buffer)?;
    Ok(())
}

fn render_cp_amm_template(
    ix: &IdlInstruction,
    program_id: &str,
    direction: TemplateDirection,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    writeln!(snippet, "    CpAmmSwapTemplate {{")?;
    writeln!(snippet, "        name: {},", quote(&ix.name))?;
    writeln!(
        snippet,
        "        direction: {},",
        direction.direction_expr()
    )?;
    writeln!(snippet, "        program_id: {},", quote(program_id))?;
    let discriminator = discriminator_bytes(ix)?;
    writeln!(
        snippet,
        "        discriminator: {disc},",
        disc = format_bytes(&discriminator)
    )?;
    writeln!(snippet, "        accounts: &[")?;
    for account in &ix.accounts {
        let rendered = render_account(account, direction)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    writeln!(snippet, "        args: &[")?;
    for arg in &ix.args {
        let rendered = render_arg(arg, None)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    snippet.push_str("    },\n");
    Ok(snippet)
}

fn emit_lb_clmm_swap_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let idl = load_idl("lb_clmm")?;
    let program_id = idl
        .address
        .as_deref()
        .ok_or("lb_clmm idl missing program id")?;

    const LB_INSTRUCTIONS: &[&str] = &[
        "swap",
        "swap2",
        "swap_exact_out",
        "swap_exact_out2",
        "swap_with_price_impact",
        "swap_with_price_impact2",
    ];

    let mut buffer = String::new();
    buffer.push_str("pub static LB_CLMM_SWAP_TEMPLATES: &[LbClmmSwapTemplate] = &[\n");

    for instr_name in LB_INSTRUCTIONS {
        let Some(ix) = idl.instructions.iter().find(|ix| ix.name == *instr_name) else {
            return Err(format!("missing instruction {instr_name} in lb_clmm idl").into());
        };
        for direction in [TemplateDirection::BaseIn, TemplateDirection::BaseOut] {
            let rendered = render_lb_clmm_template(ix, program_id, direction)?;
            buffer.push_str(&rendered);
        }
    }

    buffer.push_str("];\n");

    fs::write(out_dir.join("lb_clmm_swap_templates.rs"), buffer)?;
    Ok(())
}

fn emit_lifinity_swap_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let idl = load_idl("lifinity_amm_v2")?;
    let program_id = idl
        .address
        .as_deref()
        .ok_or("lifinity idl missing program id")?;
    let Some(ix) = idl.instructions.iter().find(|ix| ix.name == "swap") else {
        return Err("missing swap instruction in lifinity idl".into());
    };

    let accounts = lifinity_accounts_with_pdas(&ix.accounts);

    let mut buffer = String::new();
    buffer.push_str("pub static LIFINITY_SWAP_TEMPLATES: &[LifinitySwapTemplate] = &[\n");

    for direction in [TemplateDirection::BaseIn, TemplateDirection::BaseOut] {
        let rendered = render_lifinity_template(ix, &accounts, program_id, direction)?;
        buffer.push_str(&rendered);
    }

    buffer.push_str("];\n");

    fs::write(out_dir.join("lifinity_swap_templates.rs"), buffer)?;
    Ok(())
}

fn emit_whirlpool_swap_templates(out_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let idl = load_idl("whirlpool")?;
    let program_id = idl
        .address
        .as_deref()
        .ok_or("whirlpool idl missing program id")?;
    let Some(ix) = idl.instructions.iter().find(|ix| ix.name == "swap") else {
        return Err("missing swap instruction in whirlpool idl".into());
    };

    let accounts = whirlpool_accounts_with_pdas(&ix.accounts);

    let mut buffer = String::new();
    buffer.push_str("pub static WHIRLPOOL_SWAP_TEMPLATES: &[WhirlpoolSwapTemplate] = &[\n");

    for direction in [TemplateDirection::BaseIn, TemplateDirection::BaseOut] {
        let rendered = render_whirlpool_template(ix, &accounts, program_id, direction)?;
        buffer.push_str(&rendered);
    }

    buffer.push_str("];\n");

    fs::write(out_dir.join("whirlpool_swap_templates.rs"), buffer)?;
    Ok(())
}

fn lifinity_accounts_with_pdas(accounts: &[IdlAccount]) -> Vec<IdlAccount> {
    accounts
        .iter()
        .map(|account| {
            let mut cloned = account.clone();
            if cloned.name == "authority" && cloned.pda.is_none() {
                cloned.pda = Some(IdlPda {
                    seeds: vec![
                        IdlSeed {
                            kind: "const".to_string(),
                            value: Some(serde_json::Value::String("authority".to_string())),
                            path: None,
                        },
                        IdlSeed {
                            kind: "account".to_string(),
                            path: Some("amm".to_string()),
                            value: None,
                        },
                    ],
                });
            }
            cloned
        })
        .collect()
}

fn whirlpool_accounts_with_pdas(accounts: &[IdlAccount]) -> Vec<IdlAccount> {
    accounts
        .iter()
        .map(|account| {
            let mut cloned = account.clone();
            if cloned.name == "oracle" && cloned.pda.is_none() {
                cloned.pda = Some(IdlPda {
                    seeds: vec![
                        IdlSeed {
                            kind: "const".to_string(),
                            value: Some(serde_json::Value::String("oracle".to_string())),
                            path: None,
                        },
                        IdlSeed {
                            kind: "account".to_string(),
                            value: None,
                            path: Some("whirlpool".to_string()),
                        },
                    ],
                });
            }
            cloned
        })
        .collect()
}

fn render_lifinity_template(
    ix: &IdlInstruction,
    accounts: &[IdlAccount],
    program_id: &str,
    direction: TemplateDirection,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    writeln!(snippet, "    LifinitySwapTemplate {{")?;
    writeln!(
        snippet,
        "        direction: {},",
        direction.direction_expr()
    )?;
    writeln!(snippet, "        program_id: {},", quote(program_id))?;
    let discriminator = discriminator_bytes(ix)?;
    writeln!(
        snippet,
        "        discriminator: {disc},",
        disc = format_bytes(&discriminator)
    )?;
    writeln!(snippet, "        accounts: &[")?;
    for account in accounts {
        let rendered = render_account(account, direction)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    writeln!(snippet, "        args: &[")?;
    for arg in &ix.args {
        let rendered = render_arg(arg, None)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    snippet.push_str("    },\n");
    Ok(snippet)
}

fn render_whirlpool_template(
    ix: &IdlInstruction,
    accounts: &[IdlAccount],
    program_id: &str,
    direction: TemplateDirection,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    writeln!(snippet, "    WhirlpoolSwapTemplate {{")?;
    writeln!(
        snippet,
        "        direction: {},",
        direction.direction_expr()
    )?;
    writeln!(snippet, "        program_id: {},", quote(program_id))?;
    let discriminator = discriminator_bytes(ix)?;
    writeln!(
        snippet,
        "        discriminator: {disc},",
        disc = format_bytes(&discriminator)
    )?;
    writeln!(snippet, "        accounts: &[")?;
    for account in accounts {
        let rendered = render_account(account, direction)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    writeln!(snippet, "        args: &[")?;
    for arg in &ix.args {
        let fixed_value = match arg.name.as_str() {
            "aToB" => match direction {
                TemplateDirection::BaseIn => Some("true"),
                TemplateDirection::BaseOut => Some("false"),
            },
            _ => None,
        };
        let rendered = render_arg(arg, fixed_value)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    snippet.push_str("    },\n");
    Ok(snippet)
}

fn render_lb_clmm_template(
    ix: &IdlInstruction,
    program_id: &str,
    direction: TemplateDirection,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    writeln!(snippet, "    LbClmmSwapTemplate {{")?;
    writeln!(snippet, "        name: {},", quote(&ix.name))?;
    writeln!(
        snippet,
        "        direction: {},",
        direction.direction_expr()
    )?;
    writeln!(snippet, "        program_id: {},", quote(program_id))?;
    let discriminator = discriminator_bytes(ix)?;
    writeln!(
        snippet,
        "        discriminator: {disc},",
        disc = format_bytes(&discriminator)
    )?;
    writeln!(snippet, "        accounts: &[")?;
    for account in &ix.accounts {
        let rendered = render_account(account, direction)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    writeln!(snippet, "        args: &[")?;
    for arg in &ix.args {
        let rendered = render_arg(arg, None)?;
        snippet.push_str(&rendered);
    }
    writeln!(snippet, "        ],")?;
    snippet.push_str("    },\n");
    Ok(snippet)
}

fn render_account(
    account: &IdlAccount,
    direction: TemplateDirection,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    let name_key = account.name.to_snake_case();
    let mint_role = mint_role_literal(&name_key, direction);
    writeln!(snippet, "            AccountSpec {{")?;
    writeln!(snippet, "                name: {},", quote(&account.name))?;
    writeln!(snippet, "                is_signer: {},", account.signer)?;
    writeln!(
        snippet,
        "                is_writable: {},",
        account.writable
    )?;
    writeln!(
        snippet,
        "                is_optional: {},",
        account.optional
    )?;
    writeln!(
        snippet,
        "                address: {},",
        option_str(account.address.as_deref())
    )?;
    if let Some(pda) = account.pda.as_ref() {
        writeln!(snippet, "                pda: Some(&PdaSpec {{")?;
        writeln!(
            snippet,
            "                    name: {},",
            quote(&account.name)
        )?;
        writeln!(snippet, "                    seeds: &[")?;
        for seed in &pda.seeds {
            writeln!(snippet, "                        {},", render_seed(seed)?)?;
        }
        writeln!(snippet, "                    ],")?;
        writeln!(snippet, "                }}),")?;
    } else {
        writeln!(snippet, "                pda: None,")?;
    }
    if let Some(role) = mint_role {
        writeln!(snippet, "                mint_role: Some({role}),")?;
    } else {
        writeln!(snippet, "                mint_role: None,")?;
    }
    snippet.push_str("            },\n");
    Ok(snippet)
}

fn render_arg(
    arg: &IdlInstructionArg,
    fixed_value: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut snippet = String::new();
    writeln!(snippet, "            ArgSpec {{")?;
    writeln!(snippet, "                name: {},", quote(&arg.name))?;
    writeln!(
        snippet,
        "                ty: {},",
        quote(&describe_type(&arg.ty))
    )?;
    match fixed_value {
        Some(value) => writeln!(
            snippet,
            "                fixed_value: Some({}),",
            quote(value)
        )?,
        None => writeln!(snippet, "                fixed_value: None,")?,
    }
    snippet.push_str("            },\n");
    Ok(snippet)
}

fn render_seed(seed: &IdlSeed) -> Result<String, Box<dyn std::error::Error>> {
    match seed.kind.as_str() {
        "const" => {
            let value = seed.value.as_ref().ok_or("const seed missing value")?;
            let bytes = seed_bytes(value)?;
            Ok(format!("SeedSpec::Bytes(&[{}])", bytes.join(", ")))
        }
        "account" => {
            let path = seed.path.as_deref().ok_or("account seed missing path")?;
            Ok(format!("SeedSpec::Account({})", quote(path)))
        }
        "arg" => {
            let path = seed.path.as_deref().ok_or("arg seed missing path")?;
            Ok(format!("SeedSpec::Arg({})", quote(path)))
        }
        other => Err(format!("unsupported seed kind: {other}").into()),
    }
}

fn discriminator_bytes(ix: &IdlInstruction) -> Result<[u8; 8], Box<dyn std::error::Error>> {
    if let Some(bytes) = ix.discriminator.as_ref() {
        return bytes_to_u8_8(bytes);
    }
    Ok(anchor_discriminator(&ix.name))
}

fn bytes_to_u8_8(bytes: &[u8]) -> Result<[u8; 8], Box<dyn std::error::Error>> {
    if bytes.len() != 8 {
        return Err("instruction discriminator must be 8 bytes".into());
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Ok(arr)
}

fn anchor_discriminator(name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(b"global:");
    hasher.update(name.as_bytes());
    let hash = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

fn seed_bytes(value: &serde_json::Value) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if let Some(array) = value.as_array() {
        let mut out = Vec::with_capacity(array.len());
        for elem in array {
            let num = match elem.as_i64() {
                Some(value) => value,
                None => return Err("seed byte must be integer".into()),
            };
            if !(0..=255).contains(&num) {
                return Err("seed byte outside u8 range".into());
            }
            out.push((num as u8).to_string());
        }
        return Ok(out);
    }

    if let Some(s) = value.as_str() {
        return Ok(s.as_bytes().iter().map(|byte| byte.to_string()).collect());
    }

    Err("unsupported const seed value".into())
}

fn option_str(value: Option<&str>) -> String {
    match value {
        Some(v) => format!("Some({})", quote(v)),
        None => "None".to_string(),
    }
}

fn quote(value: &str) -> String {
    format!("{:?}", value)
}

fn format_bytes(bytes: &[u8]) -> String {
    let inner = bytes
        .iter()
        .map(|byte| byte.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

fn mint_role_literal(name: &str, direction: TemplateDirection) -> Option<&'static str> {
    match name {
        n if is_base_token(n) => Some("SwapMintRole::Mint0"),
        n if is_quote_token(n) => Some("SwapMintRole::Mint1"),
        "user_token_in" => direction.input_role(),
        "user_token_out" => direction.output_role(),
        "host_fee_in" => direction.input_role(),
        "input_token_account" => direction.input_role(),
        "output_token_account" => direction.output_role(),
        "input_vault" => direction.input_role(),
        "output_vault" => direction.output_role(),
        "input_vault_mint" => direction.input_role(),
        "output_vault_mint" => direction.output_role(),
        "token_a_account" => direction.input_role(),
        "token_b_account" => direction.output_role(),
        "token_owner_account_a" => direction.input_role(),
        "token_owner_account_b" => direction.output_role(),
        "token_a_vault" => direction.input_role(),
        "token_b_vault" => direction.output_role(),
        "token_vault_a" => direction.input_role(),
        "token_vault_b" => direction.output_role(),
        "token_a_mint" => direction.input_role(),
        "token_b_mint" => direction.output_role(),
        "source_info" => direction.input_role(),
        "destination_info" => direction.output_role(),
        "swap_source" => direction.input_role(),
        "swap_destination" => direction.output_role(),
        _ => None,
    }
}

fn is_base_token(name: &str) -> bool {
    name.ends_with("_x") || name.ends_with("_0") || name.contains("token_x_mint")
}

fn is_quote_token(name: &str) -> bool {
    name.ends_with("_y") || name.ends_with("_1") || name.contains("token_y_mint")
}

fn describe_type(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Object(map) => {
            if let Some(option) = map.get("option") {
                return format!("Option<{}>", describe_type(option));
            }
            if let Some(vec_value) = map.get("vec") {
                return format!("Vec<{}>", describe_type(vec_value));
            }
            if let Some(defined_value) = map.get("defined") {
                return match defined_value {
                    serde_json::Value::String(inner) => inner.clone(),
                    serde_json::Value::Object(obj) => obj
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown")
                        .to_string(),
                    _ => "Unknown".to_string(),
                };
            }
            if let Some(array_value) = map.get("array") {
                if let Some(parts) = array_value.as_array() {
                    if parts.len() == 2 {
                        let ty = describe_type(&parts[0]);
                        let len = parts[1].as_u64().unwrap_or(0);
                        return format!("[{ty}; {len}]");
                    }
                }
            }
            "Unknown".to_string()
        }
        _ => "Unknown".to_string(),
    }
}

fn load_idl(name: &str) -> Result<Idl, Box<dyn std::error::Error>> {
    let path = PathBuf::from(IDL_POOL_DIR).join(format!("{name}.json"));
    let bytes = fs::read(path)?;
    let idl: Idl = serde_json::from_slice(&bytes)?;
    Ok(idl)
}

#[derive(Debug, Deserialize)]
struct Idl {
    address: Option<String>,
    instructions: Vec<IdlInstruction>,
}

#[derive(Debug, Deserialize)]
struct IdlInstruction {
    name: String,
    discriminator: Option<Vec<u8>>,
    accounts: Vec<IdlAccount>,
    args: Vec<IdlInstructionArg>,
}

#[derive(Debug, Deserialize, Clone)]
struct IdlAccount {
    name: String,
    #[serde(default, alias = "isMut")]
    writable: bool,
    #[serde(default, alias = "isSigner")]
    signer: bool,
    #[serde(default)]
    optional: bool,
    address: Option<String>,
    pda: Option<IdlPda>,
}

#[derive(Debug, Deserialize, Clone)]
struct IdlPda {
    seeds: Vec<IdlSeed>,
}

#[derive(Debug, Deserialize, Clone)]
struct IdlSeed {
    kind: String,
    value: Option<serde_json::Value>,
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IdlInstructionArg {
    name: String,
    #[serde(rename = "type")]
    ty: serde_json::Value,
}

#[derive(Clone, Copy)]
enum TemplateDirection {
    BaseIn,
    BaseOut,
}

impl TemplateDirection {
    fn direction_expr(&self) -> &'static str {
        match self {
            TemplateDirection::BaseIn => "SwapTemplateDirection::BaseIn",
            TemplateDirection::BaseOut => "SwapTemplateDirection::BaseOut",
        }
    }

    fn input_role(&self) -> Option<&'static str> {
        match self {
            TemplateDirection::BaseIn => Some("SwapMintRole::Mint0"),
            TemplateDirection::BaseOut => Some("SwapMintRole::Mint1"),
        }
    }

    fn output_role(&self) -> Option<&'static str> {
        match self {
            TemplateDirection::BaseIn => Some("SwapMintRole::Mint1"),
            TemplateDirection::BaseOut => Some("SwapMintRole::Mint0"),
        }
    }
}
