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
