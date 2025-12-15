#![allow(clippy::all)]
#![allow(rustdoc::broken_intra_doc_links)]
#![allow(unused_qualifications)]

/// Bitquery CoreCast service bindings.
pub mod solana_corecast {
    include!(concat!(env!("OUT_DIR"), "/solana_corecast.rs"));
}

/// Shared Solana message schemas (pools, trades, IDL snippets).
pub mod solana_messages {
    include!(concat!(env!("OUT_DIR"), "/solana_messages.rs"));
}

/// Jito bundle-level protobufs.
pub mod bundle {
    include!(concat!(env!("OUT_DIR"), "/bundle.rs"));
}

pub mod packet {
    include!(concat!(env!("OUT_DIR"), "/packet.rs"));
}

pub mod shared {
    include!(concat!(env!("OUT_DIR"), "/shared.rs"));
}

pub mod searcher {
    include!(concat!(env!("OUT_DIR"), "/searcher.rs"));
}

pub mod relayer {
    include!(concat!(env!("OUT_DIR"), "/relayer.rs"));
}

pub mod block_engine {
    include!(concat!(env!("OUT_DIR"), "/block_engine.rs"));
}

pub mod shredstream {
    include!(concat!(env!("OUT_DIR"), "/shredstream.rs"));
}

pub mod auth {
    include!(concat!(env!("OUT_DIR"), "/auth.rs"));
}

pub mod block {
    include!(concat!(env!("OUT_DIR"), "/block.rs"));
}
