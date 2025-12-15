use std::{env, path::PathBuf};

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

const IDL_POOL_DIR: &str = "idls";

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

    cfg.compile_protos(&[BITQUERY_PROTOS, JITO_PROTOS].concat(), &includes)?;

    Ok(())
}
