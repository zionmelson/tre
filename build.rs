use std::{env, fs, path::PathBuf};

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
