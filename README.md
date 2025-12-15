# tre Arbitrage Executor

`tre` is an ultra-low-latency Solana arbitrage executor that ingests Bitquery CoreCast market data, detects deterministic opportunities, and expresses them as Solend flash-loan bundles submitted through the Jito Block Engine. This README gives a practical overview for building, configuring, and operating the binary. For deep architectural details, see `docs/architecture.md`.

## Feature Highlights

- **Deterministic pipeline:** dedicated ingest, planner, executor, and bundle monitor workers running on a single-threaded Tokio runtime with lock-free queues.
- **Zero-copy market data:** TLS CoreCast consumers normalize CLMM, Whirlpool, DAMM, and DLMM state into arena-backed structs for ArbGraph.
- **Flash-loan execution:** Solend reserves fund multi-hop swaps and repay flows using manual instruction templates.
- **Jito integration:** tonic clients talk to multiple Block Engine regions, maintain fresh tip accounts, and stream bundle results for telemetry.
- **First-class observability:** Prometheus exporter surfaces queue pressure, ingest health, planner/executor throughput, and bundle outcomes.

## Prerequisites

- Rust `1.77` or newer (Rustup recommended).
- `protoc` in `PATH` for compiling Bitquery/Jito protobufs (required by `build.rs`).
- TLS CA bundle for Jito (`certs/jito_block_engine_ca.pem` checked in by default) and API credentials for CoreCast & RPC providers.
- Solana keypairs for the Jito auth wallet and payer wallet referenced in `config.json`.

## Building & Running

1. Install dependencies and verify `cargo --version` matches the required toolchain.
2. Copy `config.json` and update secrets (API tokens, wallet paths, endpoint lists, Solend reserves).
3. On macOS with the Homebrew LLVM toolchain, run everything through the helper script so the correct linker, SDK, and LTO bits are exported automatically:

```bash
./scripts/run_release.sh --config ./config.json --metrics-bind 0.0.0.0:9898
```

Pass any additional CLI flags after `--config` as usual (the script forwards them directly to `cargo run --release -- …`).
Add `--duration <seconds>` (or export `RUN_DURATION_SECONDS=<seconds>`) when you only want a short debugging run—the helper script will send `SIGINT`/`SIGTERM` for you once the timer expires so the bot never runs indefinitely.

On other platforms you can still run manually:

```bash
cargo build --release
./target/release/tre-arb-bot --config ./config.json --metrics-bind 0.0.0.0:9898
```

Use `--log <RUST_LOG directive>` to override tracing filters when debugging.

The process starts all workers, opens the Prometheus exporter on the supplied address, and streams bundle results to the log.

## Configuration Reference (`config.json`)

- `corecast`: host, API token, queue sizing for Bitquery streams, plus an optional `ca_pem` that can point to a single PEM/CRT file or an entire directory of certs. When unset, the built-in rustls root store is used; when set, the supplied certificates are appended so future CA rotations can be handled by simply dropping a new file into the directory and sending a config reload.
- `quicknode_rpcs`: fallback HTTPS RPC endpoints for pulling account or tip data outside CoreCast.
- `wallets`: filesystem paths for the Jito auth cert (`auth`) and transaction `payer` keypair.
- `jito`: HTTPS/WebSocket endpoints, CA PEM path, and tip floor feed URLs (the executor polls `tip_floor_url` and never bids below the returned lamports value).
- `tokens` / `atas`: canonical mint addresses and pre-derived associated token accounts used during swaps.
- `token_pairs`: ordered mint pairs that ArbGraph should consider when searching for 2- or 3-hop loops.
- `planner`: knobs for ArbGraph heuristics (`min_profit_lamports`, optional `max_hops`), a `max_queue_depth` safety valve that caps the deduplicated opportunity queue, plus an `emit_plan_memos` flag that adds lightweight memo transactions after each bundle for on-chain bookkeeping. Optional supplemental-cache tuning fields (`supplemental_prefetch_per_tick`, `raydium_max_supplemental_arrays`, `orca_max_supplemental_arrays`, `supplemental_cache_ttl_secs`) let you raise/lower how aggressively Raydium/Orca tick-array snapshots are prefetched and how long they stay hot before refresh.
- `program_addresses`: allowlist of DEX program IDs touched by generated bundles.
- `solend_flash_loans`: per-asset Solend reserve metadata (program id, market, reserve accounts, fee receivers, and receiver allowlists). Each asset mirrors Solend’s on-chain account layout so the executor can borrow/repay deterministically.

Keep secrets such as API tokens, wallet paths, and host fee receivers out of version control. On production hosts, load `config.json` from a secure volume and keep permissions restrictive.

## Runtime Components

- **Ingest worker:** maintains independent CoreCast streams (`dex_pools`, `dex_trades`), applies ordering/filters, and publishes normalized updates. Tracks queue depth and drop counters.
- **Planner (ArbGraph):** consumes fresh pool states, recomputes profitable paths per dirty pool, and emits `ArbPlan` structs tagged with pool hashes and Solend borrow sizing while honoring the `planner` config (min profit floor, hop count, optional memo emission).
- **Executor:** rebuilds Solana instruction templates (Solend borrow, swap hops, repay), inserts compute budget + tip transfers, maintains a cache of allowed Jito tip accounts, and now fans out optional post-flash batches (e.g., memos) alongside the atomic flash-loan transaction.
- **Bundle monitor:** subscribes to Jito `SubscribeBundleResults`, tallies `Pending/Landed/Failed/Invalid` outcomes, and feeds those into metrics to guide tip adjustments.
- **Telemetry server:** exposes Prometheus `/metrics` alongside structured tracing (via `tracing-subscriber`).

## Telemetry

Metrics live under `/metrics` on the address supplied via `--metrics-bind`. Notable series include:

- `queue_depth{queue="ingest|planner|executor|bundle"}` and matching `queue_drops_total{...}` for backpressure visibility.
- `ingest_messages_total{stream="dex_pools|dex_trades"}`, `ingest_errors_total{...}`, and `ingest_backoff_ms{...}` for stream health.
- `planner_plans_total` / `executor_plans_total` for throughput.
- `planner_supplemental_cache_total{curve="raydium|orca",status="hit|miss|store"}` and `planner_prefetch_backlog{curve="raydium|orca"}` to watch cache hit rates and the backlog of supplemental prefetch tasks waiting on RPC.
- `executor_bundle_packets_total`, `executor_bundle_bytes_total`, and `executor_bundle_tip_lamports` to understand actual bundle composition, payload volume, and effective tips hitting Jito.
- `bundle_results_total{result="accepted|processed|finalized|rejected|dropped|unknown"}` mirroring the Jito result stream, paired with `bundle_latency_ms{result="..."}` for send→result delay, `bundle_failure_reason_total{reason="state_auction_bid|winning_batch_bid|simulation_failure|internal_error|dropped_bundle|blockhash_expired|partially_processed|not_finalized|unknown"}`, a `bundle_failure_streak` gauge for consecutive failures, and `bundle_inflight` to highlight how many UUIDs are outstanding without a terminal result.
  Scrape these metrics into Prometheus/Grafana and set alerts on queue drops or prolonged ingest backoff to catch data-plane issues early.

## Further Reading

Consult `docs/architecture.md` for deeper dives into pool math, instruction templates, and the long-term roadmap. The document covers protocol-specific quirks (Raydium CLMM, Orca Whirlpool, Meteora DAMM/DLMM) and is the canonical reference when extending planners or executor logic.
