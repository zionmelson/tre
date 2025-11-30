# Fast-Lane Executor Architecture

## Objectives

- **Freshness first:** Every data artifact (trade, pool snapshot, template, ALT, flash reserve, blockhash) carries a source timestamp plus a hard time-to-live. Objects older than 40 ms are rejected immediately, guaranteeing sub-50 ms detection-to-submit latency.
- **Simple pipeline:** Collapse to two hot paths (fast vs slow) instead of four blocking stages. The fast path owns planning, signing, and submission on a single core; the slow path runs hydrators and warmers without touching live trades.
- **Always-on submissions:** No gating on validator identity or global throttling. Multiple submission lanes (Jito + TPU fanout) fire in parallel the moment a plan is ready.
- **Observability-enforced safety:** A watchdog samples freshness and latency continuously. Any breach tears down trading automatically rather than allowing stale bundles to leak.

## High-Level Flow

1. **Bootstrap:** Load config, wallets, and IDLs; hydrate static templates; spin the freshness bus (shared channels that broadcast updates + timestamps to subscribers).
2. **Ingress:** A single Tokio runtime consumes CoreCast trade/pool streams and Solana account subscriptions. Each message is timestamped at arrival; if it is already too old (>30 ms) it is dropped. Fresh updates enter the planner ring immediately.
3. **Fast Path Planner:** Runs on one pinned thread. It maintains pool state, graph search, flash-loan sizing, and template readiness metadata. When a trade arrives it either emits a fully hydrated `BundlePlan` within ~20 ms or discards the trade if any dependency is missing. No retry queues are used.
4. **Signing & Submission:** The same thread (or a pair of co-located threads) signs bundles using prebuilt skeletons and immediately dispatches them through both submission lanes. Each bundle carries `fresh_until = min(all resource TTLs)`; if `now > fresh_until` the plan is dropped before signing.
5. **Slow Path Support:** Background tasks (template hydrators, ALT updates, flash-reserve monitors) stream account changes and push freshness events into the bus. They never block trades; they only update readiness state so future trades can succeed.
6. **Watchdog & Telemetry:** A guard thread samples detection-to-submit latency, data ages, and submission outcomes every few milliseconds. Any SLA violation toggles the system to degraded, halting trading until freshness recovers.

## Component Responsibilities

- `config` – Adds freshness TTLs, fast-/slow-path CPU pinning, submission lane settings, and watchdog thresholds.
- `corecast` – Tokio streams with bounded reconnect backoff (≤25 ms) and explicit timestamping. Emits `TradeUpdate` and `PoolUpdate` events with source slot/ts.
- `hydrators` (new) – Subscribe to Solana accounts (templates, ALTs, flash-loan reserves) and emit `ResourceReady` or `ResourceInvalidated` messages containing slot/time metadata.
- `planner` – Replaces `stage1_eval` + `stage2_submit`. Performs graph search, flash-loan checks, template assignment, and message construction in one pass. Rejects trades whose dependencies exceed TTL.
- `submission` – Replaces `stage3_send`. Manages signer pool, blockhash cache (TPU gossip), telemetry, and dual-lane submission (Jito gRPC + raw TPU broadcast). No throttling or leader gating exists.
- `watchdog` – Continuously verifies `now - resource.timestamp <= ttl` for all active artifacts and `bundle_latency_ms <= 50`. When breached it flips a shared atomic that stops the planner and emits alerts.
- `telemetry` – Exports freshness histograms (`data_age_ms` per resource type), bundle latency, drop reasons, and submission outcomes. Alerts fire off the watchdog plus Prometheus rules.

## Data Freshness Contract

- Every struct implements `FreshResource { timestamp: Instant, ttl: Duration }`.
- `TradeUpdate`, `PoolSnapshot`, `InstructionTemplate`, `AltHandle`, `FlashReserve`, `Blockhash`, and `RoutePlan` embed this metadata.
- Planner logic enforces `now <= resource.fresh_until()` before using it; otherwise the trade is skipped.
- Subsystems that mutate shared state must issue invalidate events whenever accounts change.
- Bundles carry `fresh_until = min(resource fresh_until)` so Stage3 can drop stale work without re-checking each dependency.

## Submission Strategy

- **Blockhash acquisition:** Replace the RPC polling cache with a TPU listener that streams new blockhashes every slot, keeping age <10 ms.
- **Multi-lane send:** After signing, push bundle bytes to (a) Jito gRPC and (b) direct TPU fanout simultaneously. Failures on one lane never block the other.
- **No BAM gate:** Validator allowlists now inform tipping heuristics, not gating. If a leader is hostile, we adjust tips or disable that lane via config, but the pipe stays open.
- **Circuit breakers:** If submissions fail for >N ms, watchdog pauses trading and raises an alert.

## Implementation Roadmap

1. **Telemetry + watchdog scaffolding** (new crate modules, metrics, config toggles).
2. **Builder refactor:** add freshness metadata, streaming hydrators, and TTL enforcement inside `builder::template_cache`, `alt_manager`, `flash_loan`, and `blockhash`.
3. **Planner rewrite:** merge current `stage1_eval` + `stage2_submit` into `planner.rs`, add fast/slow path separation, remove retry queues, and enforce freshness at selection time.
4. **Submission rewrite:** new `submission.rs` hosting signing plus dual-lane sending, eliminating `stage3_send` throttles and gates.
5. **Runtime wiring:** update `runtime.rs` to spawn the new planner, submission worker(s), hydrators, and watchdog; remove legacy stage threads and rings.
6. **Doc/tests/config:** update configs, scripts, README, and integration tests to exercise the new limits and SLA alerts.

## Operational Guardrails

- **Auto-halt on SLA breach:** Watchdog flips the `trading_enabled` atomic when any metric exceeds its budget. Manual override requires an operator flag.
- **Instrument everything:** Each drop path records `drop_reason` (stale_trade, stale_template, stale_blockhash, watchdog_pause, etc.). Sustained drops indicate systemic freshness problems.
- **Rehearsal plan:** Provide feature flags to run the new fast path in shadow mode, mirroring trades but not sending bundles until confidence is built.

## Future Extensions

- Add automated lane scoring (Jito vs TPU latency comparisons) to route hot bundles toward the fastest path dynamically.
- Explore co-locating the planner on the same machine as the validator to reduce network hop count.
- Integrate hardware timestamping (SO_TIMESTAMPING) for even tighter freshness accounting once supported by the RPC stack.

# Ultra-Light Flat Executor Architecture

## Goals & Constraints

- **Latency:** Sub-50 ms from detection to bundle submission by precomputing swap templates, pinning executor threads, and removing blocking I/O from the hot path. The hot path hops across three lock-free SPSC rings with zero allocator churn.
- **Simplicity:** Single binary with well-defined modules; no unnecessary services or heavyweight logging. Tokio is kept for orchestration (ctrl-c, gRPC clients) while the data plane is fully synchronous.
- **Portability:** Develop on macOS (arm64) but deterministically target Ubuntu x86_64 via cross-compilation and/or Docker.
- **Reliability:** Atomic bundle construction with retry-safe submission, Gmail success notifications, and external watchdogs for health.

## High-Level Flow

1. **Bootstrap:** Load `config.json`, hydrate wallet paths, parse pool IDLs in `idls/pools`, and emit `src/codegen/pools_static.rs` at build time. Templates are exposed as const byte slices during runtime startup.
2. **Ingress (Stage 0):** Subscribe to Bitquery CoreCast streams (gRPC) and decode events into normalized trade + pool update structs (e.g., `TradeUpdate`, `PoolSnapshot`). A dedicated CPU core runs this stage and pushes raw update leases into Ring 0 without performing strategy math. `TradeUpdate` captures execution-level facts (program, pool, signature, buy/sell mints, amounts), while `PoolSnapshot` caches the latest reserves plus spot price derived from those balances.
3. **Strategy Evaluation (Stage 1):** Pull updates from Ring 0, maintain pool state, synthesize `Opportunity` objects, then run strategy heuristics against cached templates. When a strategy fires it allocates bundle objects from another slab and enqueues them onto Ring 1.
4. **Bundle Submission (Stage 2):** Consume bundle leases, hydrate instruction templates (including flash-loan borrow/repay legs), attach tips, and pass completed bundles to the next stage along with telemetry about detection latency.
5. **Bundle Send (Stage 3):** Sign bundles, submit over the Jito `SearcherService` gRPC channel, record per-bundle metrics, and surface errors to the logger/notifier without blocking earlier stages.
6. **Bundle Tracking:** Dedicated tracker threads subscribe to `SubscribeBundleResults`, optionally listen to the state-auction websocket feed, and maintain a TTL-governed map of pending bundle IDs so accepted/rejected/finalized events and auction wins are logged, alerted, and exported via telemetry in near real time.
7. **Control Plane:** Tokio remains responsible for ctrl-c shutdown, asynchronous gRPC clients, and future health probes, but it does not sit in the hot path.

## Module Overview

- `config` – JSON loader with tilde expansion, `$ENV{VAR}` placeholder hydration, optional environment overrides (see `config.template.json`), validation hooks, and perf tuning knobs (ring/slab sizes, CPU pinning).
- `idl` – Reads all pool IDLs once so codegen can emit const instruction bytes consumed by the runtime.
- `core` – Lock-free primitives (`core::ring`, `core::slab`) shared by every stage.
- `codegen` – Build-time generated `pools_static.rs` that exposes `PoolTemplate` slices without runtime JSON parsing.
- `corecast` – Holds tonic clients + streaming logic; feeds Stage 0 with decoded CoreCast trade/pool updates backed by the ingestion slab.
- `stages` – `stage0_ingest`, `stage1_eval`, `stage2_submit`, and `stage3_send`, each owning a dedicated thread/core and communicating via SPSC rings. Stage 0 ingests + normalizes CoreCast data, Stage 1 builds opportunities and bundles, Stage 2 plans transactions, Stage 3 signs and transmits them to Jito.
- `runtime` – Wires slabs, rings, and stages together, handles ctrl-c shutdown, and guarantees deterministic teardown.
- `strategy` – Stateful tip-aware engine invoked from Stage 1 after an opportunity is synthesized; references cached IDLs, runs risk checks, and emits `BundleCandidate`s with tip policy applied.
- `bundle` – Manages the long-lived gRPC client used by Stage 3 to submit signed bundles (single configured endpoint today) and surfaces send outcomes for telemetry.
- `notifier` – Currently logs arbitrage/bundle signals; Gmail/OAuth wiring is still pending but isolated from the hot path.

## Runtime Practices

- **Tokio Runtime:** Restrict Tokio to orchestration (ctrl-c handling, async gRPC). The data plane uses standard threads managed by `runtime.rs`.
- **Stages & Pinning:** Each stage runs on a named OS thread and optionally pins to a dedicated core via `core_affinity`. Avoid migrating stages across cores to limit cache thrash.
- **Rings:** Communication happens through lock-free SPSC rings sized via `config.perf.ring_capacity`. Busy wait with exponential backoff, yielding when needed to keep latency predictable.
- **Slabs:** `SlabPool` owns fixed-size arenas for opportunities and bundles so no `Vec`/`Arc` allocations happen in the hot path. Size via config and watch saturation metrics.
- **Tracing:** Only log on initialization, errors, or infrequent events; rely on counters for steady-state visibility to keep the stages quiet.
- **Networking:** Enable TCP_NODELAY, keep TLS warm, and consider region-aware CoreCast endpoints to reduce RTTs.

### Telemetry & Alerting

- **Exporter:** Set `PROMETHEUS_METRICS_ADDR="0.0.0.0:9898"` (or similar) so Prometheus can scrape the process directly. Metrics remain cheap because they are lock-free histograms/counters updated in the hot path only when necessary.
- **Stage1 Backpressure:** `stage1_backpressure_events_total{reason="slab_exhausted|ring_full|overflow_queued|overflow_flushed|overflow_dropped"}` tracks every attempt to enqueue bundle candidates. Sustained `slab_exhausted` or `ring_full` signals that the opportunity slab or Ring1 is undersized for current traffic. `overflow_dropped` should stay at zero outside of intentional chaos tests. Use the following baseline alert to page when capacity disappears:

  ```yaml
  - alert: Stage1BackpressureSustained
  	expr: sum(rate(stage1_backpressure_events_total{reason=~"slab_exhausted|ring_full|overflow_dropped"}[5m])) > 0
  	for: 2m
  	labels:
  		severity: critical
  		service: tre-arb-bot
  	annotations:
  		summary: Stage1 bundle pipeline is backpressured
  		description: "{{ $labels.environment }} has sustained Stage1 backpressure ({{ $value }} events/s). Increase ring/slab capacity or reduce opportunity volume."
  ```

- **Dashboards:** A Grafana stacked-area panel using `sum by (reason)(rate(stage1_backpressure_events_total[1m]))` highlights which bottleneck hit first. Pair it with the existing detection/stage3 latency histograms so you can correlate latency spikes with backpressure. Annotate deployments via `alerts.environment` so on-call engineers can filter prod/stage/dev easily.
- **Stage2 Planning:** `stage2_plan_events_total` emits labels for `plan_failed`, `prepare_failed`, `retry_queued`, `retry_flushed`, and `retries_exhausted` (with `reason` equal to `plan`, `prepare`, or `retry`). Alert when `sum(rate(stage2_plan_events_total{event="retries_exhausted"}[5m])) > 0` so you notice when Stage2 consistently discards work after exhausting its retry budget.
- **Stage3 Sending:** `stage3_retry_events_total{event="retry_queued|retry_flushed|retries_exhausted|failure_recorded",reason="context|sign|submit"}` captures the send loop’s health. Sustained `retries_exhausted` on `submit` implies the Jito endpoint or network is unhealthy; `sign` faults indicate the builder hot path needs attention. Track `retry_flushed` to confirm the queue is draining after temporary issues.

## Build & Deployment

- **Protos:** `build.rs` compiles Bitquery + Jito protos via `tonic-build`; ensure `protoc` is installed in both dev and CI environments.
- **Cross-Compile:** Add `x86_64-unknown-linux-gnu` target (already done) and build with `cargo build --release --target x86_64-unknown-linux-gnu`. Optionally use `cargo zigbuild` for hermetic linking.
- **Docker:** Multi-stage build (`rust:1.77-slim` builder + distroless runner/Ubuntu base) that copies the release binary plus configs/IDLs. CI builds re-use the same container definition so prod deployment stays reproducible.
- **Perf Tooling:** Use `perf`, `flamegraph`, and network RTT probes on Ubuntu; instrument only via counters exported to an external collector (sidecar or host metrics).

## Future Work

1. Persist bundle tracker state across restarts and add multi-region bundle-status reconciliation so retries and landed/failed state remain observable even through process upgrades.
2. Replace the logger-based notifier stub with the Gmail/OAuth flow and richer alerting hooks for successful or failed bundles.
3. Harden deployment (Dockerfile, CI, perf counters such as ring occupancy/slab pressure, health probes) while keeping the hot path logging-free.
4. Add integration tests/mocks for CoreCast + Jito flows to exercise the full pipeline and guard against regressions.
