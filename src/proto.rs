#![allow(clippy::all)]
#![allow(dead_code)]

pub mod solana_corecast {
    tonic::include_proto!("solana_corecast");
}

pub mod solana_messages {
    tonic::include_proto!("solana_messages");
}

pub mod bundle {
    tonic::include_proto!("bundle");
}

pub mod searcher {
    tonic::include_proto!("searcher");
}

pub mod packet {
    tonic::include_proto!("packet");
}

pub mod shared {
    tonic::include_proto!("shared");
}

pub mod block_engine {
    tonic::include_proto!("block_engine");
}

pub mod block {
    tonic::include_proto!("block");
}

pub mod relayer {
    tonic::include_proto!("relayer");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod shredstream {
    tonic::include_proto!("shredstream");
}
