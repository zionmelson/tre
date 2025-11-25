# Ultra-Light Flat Executor Architecture

## Goals & Constraints

- **Latency:** Sub-50 ms from detection to bundle submission by precomputing swap templates, pinning executor threads, and removing blocking I/O from the hot path. The hot path hops across three lock-free SPSC rings with zero allocator churn.
- **Simplicity:** Single binary with well-defined modules; no unnecessary services or heavyweight logging. Tokio is kept for orchestration (ctrl-c, gRPC clients) while the data plane is fully synchronous.
- **Portability:** Develop on macOS (arm64) but deterministically target Ubuntu x86_64 via cross-compilation and/or Docker.
- **Reliability:** Atomic bundle construction with retry-safe submission, Gmail success notifications, and external watchdogs for health.

## High-Level Flow

1. **Bootstrap:** Load `config.json`, hydrate wallet paths, parse pool IDLs in `idls/pools`, and emit `src/codegen/pools_static.rs` at build time. Templates are exposed as const byte slices during runtime startup.
2. **Ingress (Stage 0):** Subscribe to Bitquery CoreCast streams (gRPC) and decode events directly into `Opportunity` structs that live inside a pre-allocated `SlabPool`. A dedicated CPU core runs this stage and pushes slab leases into Ring 0.
3. **Strategy Evaluation (Stage 1):** Pull leases from Ring 0, run strategy heuristics against cached templates, and allocate bundle objects from another slab when firing. Successful bundles are enqueued onto Ring 1.
4. **Bundle Submission (Stage 2):** Consume bundle leases, marshal transactions, apply tip policy, submit via `SearcherService`, and notify observers. This stage is also pinned to its own core and writes status metrics out-of-band.
5. **Control Plane:** Tokio remains responsible for ctrl-c shutdown, asynchronous gRPC clients, and future health probes, but it does not sit in the hot path.

## Module Overview

- `config` – JSON loader with tilde expansion, validation hooks, and perf tuning knobs (ring/slab sizes, CPU pinning).
- `idl` – Reads all pool IDLs once so codegen can emit const instruction bytes consumed by the runtime.
- `core` – Lock-free primitives (`core::ring`, `core::slab`) shared by every stage.
- `codegen` – Build-time generated `pools_static.rs` that exposes `PoolTemplate` slices without runtime JSON parsing.
- `corecast` – Holds tonic clients + streaming logic; publishes `Opportunity` structs directly into the Stage 0 slab.
- `stages` – `stage0_ingest`, `stage1_eval`, and `stage2_submit`, each owning a dedicated thread/core and communicating via SPSC rings.
- `runtime` – Wires slabs, rings, and stages together, handles ctrl-c shutdown, and guarantees deterministic teardown.
- `strategy` – Stateless engine that consumes opportunities, references cached IDLs, runs risk checks, and emits `BundleRequest`s.
- `bundle` – Manages long-lived gRPC channels to each Jito endpoint, sends bundles synchronously from Stage 2, and feeds success notifications to `notifier`.
- `notifier` – Lightweight Gmail API integration triggered by bundle success events; isolated from hot path via stage-local calls.

## Runtime Practices

- **Tokio Runtime:** Restrict Tokio to orchestration (ctrl-c handling, async gRPC). The data plane uses standard threads managed by `runtime.rs`.
- **Stages & Pinning:** Each stage runs on a named OS thread and optionally pins to a dedicated core via `core_affinity`. Avoid migrating stages across cores to limit cache thrash.
- **Rings:** Communication happens through lock-free SPSC rings sized via `config.perf.ring_capacity`. Busy wait with exponential backoff, yielding when needed to keep latency predictable.
- **Slabs:** `SlabPool` owns fixed-size arenas for opportunities and bundles so no `Vec`/`Arc` allocations happen in the hot path. Size via config and watch saturation metrics.
- **Tracing:** Only log on initialization, errors, or infrequent events; rely on counters for steady-state visibility to keep the stages quiet.
- **Networking:** Enable TCP_NODELAY, keep TLS warm, and consider region-aware CoreCast endpoints to reduce RTTs.

## Build & Deployment

- **Protos:** `build.rs` compiles Bitquery + Jito protos via `tonic-build`; ensure `protoc` is installed in both dev and CI environments.
- **Cross-Compile:** Add `x86_64-unknown-linux-gnu` target (already done) and build with `cargo build --release --target x86_64-unknown-linux-gnu`. Optionally use `cargo zigbuild` for hermetic linking.
- **Docker:** Planned multi-stage build (`rust:1.77-slim` builder + distroless runner) that copies the release binary plus configs/IDLs.
- **Perf Tooling:** Use `perf`, `flamegraph`, and network RTT probes on Ubuntu; instrument only via counters exported to an external collector (sidecar or host metrics).

## Future Work

1. Implement the actual CoreCast stream decode and integrate Bitquery credentials.
2. Flesh out strategy heuristics (pool graph, reserve depth, risk caps) now that slabs expose reusable objects.
3. Build bundle submission, polling, and failover across multiple Jito regions with dynamic tip estimation.
4. Wire Gmail OAuth service account + email templating for bundle successes.
5. Fill the Dockerfile, add CI (lint + clippy + fmt + cargo test), capture perf counters (ring occupancy, slab pressure), and script deployment to the Ubuntu host.
