# tre-arb-bot

Ultra-light Solana arbitrage executor that prioritizes latency, simplicity, and modular upgrades. The bot consumes CoreCast streams, transforms parsed pool events into cached swap/flash-loan instructions, and submits atomic bundles to the Jito block engine while keeping the critical path as small as possible. The runtime now uses a flat chain of single-producer/single-consumer rings with CPU-pinned stages for deterministic latency.

## Status

This repository currently provides:

- Cargo project scaffolding with Tokio/tonic/prost wiring
- Proto compilation for Bitquery CoreCast and Jito MEV endpoints
- A flat SPSC runtime with lock-free rings, slab allocators, and CPU-affine stages (`runtime.rs`, `core/`, `stages/`)
- Module skeletons for config management, IDL caching, CoreCast ingestion, bundle submission, Gmail-style notifications, and generated instruction templates
- Documentation of the target architecture and deployment assumptions (see `docs/architecture.md`)

## Quick Start

```bash
# Install dependencies (protoc, openssl, mold recommended for link speed)
brew install protobuf mold

# Ensure the Linux target exists for cross-compilation from Apple Silicon
rustup target add x86_64-unknown-linux-gnu

# Build the binary (native)
cargo build

# Build for production target (M1 -> Linux)
cargo build --release --target x86_64-unknown-linux-gnu
```

Environment variables:

- `ARBITRAGE_CONFIG`: optional path override for `config.json` (defaults to repo root).
- `RUST_LOG`: set to control tracing verbosity (defaults to `warn,tre-arb-bot=info`).

## Directory Layout

```
bitquery/               # CoreCast protos (compiled via build.rs)
idls/pools/             # Pool IDLs → const instruction bytes (codegen emits pools_static.rs)
jito/                   # Jito block engine/searcher protos
src/
  core/                 # Lock-free SPSC rings + slab allocator
  stages/               # stage0_ingest → stage1_eval → stage2_submit implementations
  codegen/              # Generated instruction templates (include! at build time)
  runtime.rs            # Flat executor wiring + graceful shutdown
  config.rs             # Config loader + validation
  corecast.rs           # CoreCast stream subscriber placeholder
  strategy.rs           # Opportunity evaluation stub
  bundle.rs             # Bundle submission stub
  notifier.rs           # Gmail notification skeleton
  proto.rs              # tonic include_proto! glue
```

## Deployment Targets

- **Local dev (macOS arm64):** Iterate quickly with `cargo build`; integration tests should mock CoreCast and Jito gRPC endpoints.
- **Production (Ubuntu x86_64):** Use release profile (LTO + panic abort). Recommended cross-compile locally, then copy the binary + `config.json`/IDL assets or build via the Dockerfile (to be filled with multi-stage build) on a CI runner.

## Next Steps

1. Fill in the CoreCast stream loop to decode parsed pool events into the `Opportunity` struct and feed the SPSC ring directly.
2. Implement the strategy module to stitch cached instructions and compute dynamic tips using the Jito tip floor/stream endpoints.
3. Flesh out the bundle client to send signed bundles, poll status streams, and forward success events to Gmail via OAuth service credentials.
4. Harden deployment (Dockerfile, CI, perf counters, health probes) while keeping the hot path logging-free.

For a deeper look at the intended architecture and latency goals, read `docs/architecture.md`.
