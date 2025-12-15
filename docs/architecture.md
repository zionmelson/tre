# tre Arbitrage System Architecture

## Mission Profile

- **Objective:** Execute Solana flash-loan-backed arbitrage across Raydium CLMM, Orca Whirlpool, Meteora DAMM, and Meteora DLMM with sub-millisecond reaction time and deterministic behavior.
- **Core Principles:** Avoid heavy SDKs, prefer manual instruction templates, maintain zero-copy data paths, and minimize allocations during hot execution.
- **Execution Stack:** Bitquery gRPC ingestion → ArbGraph opportunity detector → Solend flash-loan pipeline → Jito Block Engine bundle submission.

## High-Level Topology

```
+-----------------+     +----------------+     +------------------+     +--------------------+
| Bitquery gRPC   | --> | Ingest Thread  | --> | ArbGraph Thread  | --> | Executor Thread    |
| (Pools, Trades) |     | (Zero-copy RX) |     | (Deterministic   |     | (Flash loan + Jito |
|                 | <-- | Backpressure   | <-+ | search)          | <-+ | bundle submit)     |
+-----------------+     +----------------+   | +------------------+   | +--------------------+
                                             |                        |
                                             |  Prometheus Telemetry  |
                                             +------------------------+
```

- Each stage pins to a dedicated core (using `core_affinity`) and communicates through lock-free `crossbeam_queue::SegQueue` with 64-byte cache-line padding.
- Opportunity plans (`ArbPlan`) include timestamp, pool state hashes, and planned swap path to ensure downstream validation stays deterministic.

## Data Ingestion & Market State

- **Inputs:** Bitquery Solana CoreCast streams `stream.type = "dex_pools"` and `stream.type = "dex_trades"`, both delivered via TLS gRPC.
- **Configuration mechanics (per docs):** subscription requests are `SubscribePoolsRequest` / `SubscribeTradesRequest` defined in `bitquery/solana/corecast/request.proto`. Each request supports `program`, `pool`, `token`, and (for trades) `trader` `AddressFilter`s so we can whitelist the exact Raydium/Orca/Meteora program IDs plus reserve mints. Filters are pushed to Bitquery so the server only ships relevant accounts.
- **Available fields:** payload mirrors `dex_block_message.proto` — slot, signature, fee and account metadata; pool/trade section contains protocol name/family, market address, base/quote currency structs (mint, decimals, owner), balance deltas (`PreBalance`, `PostBalance`, `ChangeAmount`) and parsed instruction arguments (e.g., `base_amount_in`, `min_quote_amount_out`). `dex_trades` omits explicit price so we derive it from token amounts + decimals.
- **Implementation Notes:**
  - `tonic` clients with per-request interceptors to inject API keys, gzip enabled; we maintain separate channels for pools vs trades to avoid head-of-line blocking.
  - Messages decoded directly into arena-backed structs; CLMM ticks and DLMM bins parsed by slicing `prost::bytes::Bytes` without reallocation.
  - Each update normalized into `PoolUpdate { pool_id, curve_kind, tick_state, liquidity_net, fee_bps, lamports_per_signature, unix_ns }` / `TradeUpdate { pool_id, base_change, quote_change, trader, slot }`.
  - Sequence numbers tracked per pool; stale or out-of-order updates dropped before queuing. Trade feed is advisory only; pool feed is authoritative for state changes.
  - Schema upgrades tracked by pinning the exact protobufs published at https://github.com/bitquery/streaming_protobuf (already vendored under `bitquery/`).

### CoreCast Stream Consumption

- `dex_pools` events represent liquidity adds/removes, swaps, and pool parameter updates; we care about swaps/liquidity moves to keep liquidity curves current. Transaction metadata (signers, token accounts, fee payer) is used to sanity-check suspicious pools.
- `dex_trades` events provide per-trade `Buy`/`Sell` sections with amounts and accounts; we mainly use these as a heartbeat plus to infer recent realized prices when evaluating slippage risk.
- Both streams emit `InstructionIndex` so we can correlate multiple messages within the same transaction if needed.
- Bitquery guarantees ordered delivery per slot per stream; we still attach `unix_ns` monotonic timestamps locally for latency metrics.

### Internal State Layout

- `TokenId (u16)` dense mapping for all supported mints.
- `PoolEdge` SoA (struct-of-arrays) for each protocol: arrays of reserves, fees, tick anchors, invariants.
- `DirtySet` bitmask marks pools touched since last scan; ArbGraph uses it to limit recomputation.

## Arbitrage Engine (ArbGraph)

- Maintains price graph across pools where nodes are tokens and edges are swap routes.
- **2-Hop Search:** For each dirty pool `P`, iterate `outgoing[src(P)] × outgoing[dst(P)]`, evaluate profit using fixed-point Q64.64 arithmetic.
- **3-Hop Search:** DFS depth=3 with pruning when cumulative fees exceed projected gains; deterministic order by sorted pool IDs for reproducibility.
- **Simulation:**
  - CLMM/Whirlpool simulate tick crossing by iterating inlined tick arrays; Meteora DLMM uses bin interpolation; DAMM uses additive liquidity curves.
  - Profit calculation subtracts pool fees, flash-loan fee, compute budget fee, and planned Jito tip. Opportunity emitted only if `net_profit > tip_buffer`.

## Instruction Template Library

- Each program interaction defined as immutable template: opcode, static account metas, and data layout with placeholder bytes.
- Templates reside in `templates/` module, generated (or hard-coded) from protocol IDLs during build.
- Runtime patching updates only the dynamic fields (amounts, sqrt price limits, bin selections) before instruction emission.
- Benefits: eliminates per-op allocation, avoids Anchor/SDK overhead, and guarantees byte-for-byte determinism.

### Key Templates

1. **Solend Flash Borrow/Repay** (already in `solend/` crate) — opcodes 19 and 20.
2. **Raydium CLMM Swap** (`idls/amm_v3.json`)
   - On-chain program: `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK` (`amm_v3`).
   - Entrypoints: `swap` (deprecated) discriminator `f8c69e91e17587c8` and `swap_v2` discriminator `2b04ed0b1ac91e62`; both share args `{ amount: u64, other_amount_threshold: u64, sqrt_price_limit_x64: u128, is_base_input: bool }` (little-endian, bool as u8).
   - Account metas in declared order:
     1. `payer` (signer)
     2. `amm_config`
     3. `pool_state`
     4. `input_token_account`
     5. `output_token_account`
     6. `input_vault`
     7. `output_vault`
     8. `observation_state`
     9. `token_program` (SPL Token)
     10. For `swap` only: inline `tick_array` account; `swap_v2` instead lists `token_program_2022`, `memo_program`, `input_vault_mint`, `output_vault_mint`.
         11+. Additional accounts (supplied via `remaining_accounts`) include tick arrays (multiple allowed), optional `tick_array_bitmap_extension`, token-2022 mint/program data, etc. Ordering from code: supply tick arrays descending for `zero_for_one`, ascending otherwise so the runtime pops correctly.
   - Runtime requirements:
     - Provide consecutive tick arrays covering any ticks we may cross; each `tick_array` spans 88 ticks at `tick_spacing` increments.
     - `observation_state` must equal `pool_state.observation_key` (Anchor enforces).
     - `token_program_2022` + `memo_program` (constant `MemoSq4...`) are mandatory for `swap_v2`; memo can be a no-op but must be present.
     - `input_vault_mint`/`output_vault_mint` accounts are read-only mint metas required by IDL; we keep them static in the template to avoid lookups.
     - `sqrt_price_limit_x64` must be strictly within `(MIN+1, MAX-1)` when direction matches; setting to boundary disables limit. We derive limit per plan to cap price impact.
   - Template plan: embed the first 10 metas statically (per IDL), include unconditional metas for SPL Token, Token-2022, Memo, and vault mints, then append a dynamic slice of tick arrays / bitmap extension addresses. At runtime we only patch the 41-byte arg blob and splice the precomputed account tail.
3. **Orca Whirlpool Swap** (program `whirLbqfDq161ZtYqPiz5Er1ENYyKqS5zsHhJ1bAuS`, repo reference: `orca-so/whirlpools`, see `ts-sdk/client/src/generated/instructions/swap*.ts` and `legacy-sdk/whirlpool/src/instructions`). Two production entrypoints exist:

- Legacy `swap` discriminator `f8c69e91e17587c8` (same as Raydium but different program) with data `{ amount: u64, other_amount_threshold: u64, sqrt_price_limit: u128, amount_specified_is_input: bool, a_to_b: bool }`. Accounts (all writable unless noted): `token_program` (SPL Token), `token_authority` signer, `whirlpool`, `token_owner_account_a`, `token_vault_a`, `token_owner_account_b`, `token_vault_b`, `tick_array0`, `tick_array1`, `tick_array2`, `oracle`. SDK still uses it for token-2022-disabled pools but it lacks memo + transfer-hook plumbing.
- `swap_v2` discriminator `2b04ed0b1ac91e62` extends the account list to support token-2022 and supplemental tick arrays. Ordered metas: `token_program_a`, `token_program_b` (choose SPL Token vs Token-2022 per mint), `memo_program` (`MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr`), `token_authority` signer, `whirlpool` (w), `token_mint_a`, `token_mint_b`, `token_owner_account_a` (w), `token_vault_a` (w), `token_owner_account_b` (w), `token_vault_b` (w), `tick_array0/1/2` (w), `oracle` (w, PDA derived from `b"oracle"` + whirlpool). Instruction data matches legacy `swap` plus an optional `remaining_accounts_info: Option<RemainingAccountsInfo>` describing additional metas appended after the canonical list.
- `RemainingAccountsInfo` is a vector of `RemainingAccountsSlice { accounts_type: AccountsType, length: u8 }`. The enum in the IDL includes `TransferHookA`, `TransferHookB`, `TransferHookReward`, `TransferHookInput`, `TransferHookIntermediate`, `TransferHookOutput`, `SupplementalTickArrays`, `SupplementalTickArraysOne`, and `SupplementalTickArraysTwo`. After the fixed metas we must provide the account metas for each slice in the same order and length declared in the vector. The official SDK (`legacy-sdk/whirlpool/src/utils/remaining-accounts-util.ts`) constructs this with `RemainingAccountsBuilder`, typically adding: (a) token-2022 transfer-hook PDAs returned by `spl_transfer_hook_interface` for whichever mint(s) need them, and (b) up to two supplemental tick-array slices when a swap crosses more than three 88-tick bundles. Omitting either slice when the pool expects it triggers `ErrorCode::MissingRemainingAccounts` on-chain.
- **Tick arrays:** always pass exactly three primary arrays that cover the current tick index plus the next two in the swap direction. If the quote shows we’ll traverse >264 ticks, include additional bin coverage via the supplemental slice. Direction is governed by `a_to_b`: if true (A→B) provide arrays in descending tick index; if false supply ascending order (same as SDK’s `WhirlpoolIx.buildSwapV2`).
- **Oracle mutability:** On adaptive-fee pools the program mutates the `oracle` account; the IDL marks it read-only for compatibility, so the SDK rewrites `ix.keys[oracle_index].isWritable = true` before sending. We should do the same whenever we build instructions manually or simply mark the meta writable up front to avoid runtime mutations.
- **Template plan:** Pre-store both swap discriminators, static metas (program ids, vaults, tick-array PDA prefixes), and the memo program address. At runtime we patch (a) the 34-byte arg blob, (b) the three tick arrays chosen by the ArbGraph, (c) any token-2022 remaining accounts, and (d) optional supplemental tick arrays. Reference builders live in `ts-sdk/client/src/generated/instructions/swap*.ts` and `legacy-sdk/whirlpool/src/instructions/v2/swap-ix.ts` if we need sanity checks.

4. **Meteora DAMM Swap** (`cp_amm` program `cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG`, IDL in `idls/cp_amm.json`) — Anchor `swap2` is the production path (legacy `swap` just wraps `swap2` with `SwapMode::ExactIn`). The on-chain layout exactly matches the IDL structs: `SwapParameters { amount_in: u64, minimum_amount_out: u64 }` for the deprecated entrypoint and `SwapParameters2 { amount_0: u64, amount_1: u64, swap_mode: u8 }` where `swap_mode ∈ {0=ExactIn,1=PartialFill,2=ExactOut}`. We patch all 17 bytes for `swap2`; legacy `swap` shares the same discriminator data ( `f8c69e91e17587c8` ) but we only keep it for parity tests.

- **Account metas (fixed order from IDL/`SwapCtx`):** (1) `pool_authority` PDA (hard-coded address `HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC`, also exposed by `const_pda::pool_authority::ID`), (2) `pool` `AccountLoader<Pool>`, (3) user `input_token_account`, (4) user `output_token_account`, (5) `token_a_vault`, (6) `token_b_vault`, (7) `token_a_mint`, (8) `token_b_mint`, (9) `payer` signer, (10) `token_a_program`, (11) `token_b_program`, (12) optional `referral_token_account`, (13) `event_authority` (PDA derived from seed `"__event_authority"` + program id), (14) the program id itself. The template keeps metas 1–11 static per pool; we derive `event_authority` at startup and toggle metas 12–14 based on referral usage. Anchor clients auto-append the last two metas, but our manual builder must do it explicitly.
- **Remaining accounts:** When a pool enables the rate limiter (`pool.pool_fees.base_fee` encodes it), the very first remaining account must be `SYSVAR_INSTRUCTIONS` so the on-chain `validate_single_swap_instruction` gate can scan sibling instructions. If omitted, the ix fails with `FailToValidateSingleSwapInstruction`. The same validator also rejects two `cp_amm::swap*` instructions that touch the same pool within one transaction, so our flash-loan plan must limit DAMM to one hop per tx.
- **Runtime behaviour:** `pool.get_trade_direction()` infers side by matching the mint on `input_token_account`. `token_a_program` / `token_b_program` can be SPL Token or Token-2022; the contract calls `calculate_transfer_fee_{included,excluded}_amount`, so we must route through the mint’s actual program ID to keep transfer-fee math consistent. Referral payouts (if `referral_token_account` is passed) are denominated in whichever side `fee_mode.fees_on_token_a` selects; we only opt in when we want Meteora referral rebates.
- **Template wiring:** we precompute the serialized `SwapParameters2` blob for each swap mode plus placeholders for `amount_0/amount_1`. `pool_authority` and `event_authority` are constant per program, so the executor only swaps in: (a) dynamic user token accounts (derived from runtime plan), (b) the amount fields, and (c) the optional referral meta. No other CPIs, SDKs, or instruction sysvars are needed beyond the conditional `SYSVAR_INSTRUCTIONS` handle.

5. **Meteora DLMM Swap** (`lb_clmm` program `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`, IDL vendored as `idls/lb_clmm.json`, cross-checked against Meteora’s public SDK). DLMM exposes three swap entrypoints that share the same meta order:

- `swap2` discriminator `414b3f4ceb5b5b88` takes `{ amount_in: u64, min_amount_out: u64, remaining_accounts_info: RemainingAccountsInfo }`.
- `swapExactOut2` discriminator `2bd7f784893cf351` takes `{ max_in_amount: u64, out_amount: u64, remaining_accounts_info }`.
- `swapWithPriceImpact2` discriminator `4a62c0d6b1334b33` takes `{ amount_in: u64, active_id: Option<i32>, max_price_impact_bps: u16, remaining_accounts_info }` to hard-cap slippage.
- `RemainingAccountsInfo` is a vector of `RemainingAccountsSlice { accounts_type: AccountsType, length: u8 }` that tells the program how many of the subsequent `remaining_accounts` belong to each token-2022 transfer-hook bucket (`AccountsType::{TransferHookX, TransferHookY, TransferHookReward, TransferHookMultiReward(n)}`). Even when we have no transfer hooks we serialize an empty vector so the on-chain parser immediately treats the trailing remaining accounts as bin arrays.
- **Account metas (exact order from IDL `swap2`):** `lb_pair` (w), optional `bin_array_bitmap_extension` PDA derived from `["bitmap", lb_pair]`, `reserve_x` (w), `reserve_y` (w), `user_token_in` (w), `user_token_out` (w), `token_x_mint`, `token_y_mint`, `oracle` (w), optional `host_fee_in` (w, set to our host fee ATA to earn shares or pass `None`), `user` signer, `token_x_program`, `token_y_program` (must match mint program: SPL Token vs Token-2022), `memo_program` (`MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr`), `event_authority` PDA derived from `"__event_authority"`, and finally the program id itself. All metas are immutable per pool except the user ATAs, host-fee ATA, and optional bitmap extension (some pools have not initialized it yet).
- **Remaining accounts:** after Anchor metas we append (1) any transfer-hook extra account metas for token-2022 mints as described by `RemainingAccountsInfo`, followed by (2) the bin-array PDAs that cover the bins we plan to traverse. Each bin array covers `MAX_BIN_PER_ARRAY` bins (IDL constant) and lives at `PDA(["bin_array", lb_pair, bin_array_index_i16])`. Swapping `token_x → token_y` requires arrays descending from the current active id, while the opposite direction walks ascending arrays (`ts-client/src/dlmm/helpers/binArray.ts#getBinArrayForSwap`). We also prefetch the bitmap-extension bitset so we can prove which arrays are initialized.
- **Oracle + price impact:** `oracle` (writable) tracks the logarithmic moving average used for fee calculus; we must read it from state when quoting because `swapWithPriceImpact2` optionally pins `active_id` (the bin we expect to enter) and enforces `max_price_impact_bps`. Quotes should estimate exactly which bins we will deplete, then clamp `active_id`/`max_price_impact_bps` to avoid surprise rejections.
- **Compute budget + memo requirement:** Meteora’s CLI always prefixes swaps with `ComputeBudget::set_compute_unit_limit(1_400_000)` and optionally `set_compute_unit_price`; we should mirror that because DLMM’s per-swap path crosses many bins. The memo program must still appear in the account metas even though we usually send an empty memo instruction; omitting it triggers `ConstraintRaw` failures.
- **Template wiring:** embed all static metas plus serialized discriminators at build time. Runtime only needs to (a) splice user input/output ATAs, host-fee ATA (if desired), and actual token program IDs, (b) patch the 16–24 byte arg payload per swap flavor, and (c) append the dynamic `remaining_accounts` slice consisting of token-2022 extras + ordered bin arrays. We precompute bin-array selections using the same helpers as Meteora’s SDK (`dlmm::client::accounts::Swap2` / `BinArray::bin_id_to_bin_array_index`) so instruction construction stays allocation-free.

## Execution Pipeline

1. `SolanaLeaderWatcher` tracks upcoming leader slots (via Jito slot subscription) to schedule bundles for `slot`, `slot+1`, `slot+2`.
2. `ArbPlan` triggers borrow sizing step: binary search borrow amount respecting pool depth and Solend reserve caps.
3. Build instruction list:
   - `ComputeBudget::set_compute_unit_price`
   - `ComputeBudget::set_compute_unit_limit`
   - `flash_borrow`
   - Swap instructions per hop
   - `flash_repay`
4. Construct `MessageV0` with ALT(s) and durable nonce; sign using hot key isolated to executor core.
5. Serialize into `VersionedTransaction`, replicate into `Bundle` proto, submit via multiple Jito gRPC `BundleServiceClient`s with varied tips.
6. Track acknowledgements through `SubscribeBundleResults`; failed bundles (account-in-use, slot mismatch) trigger adaptive retries with tweaked fee/tip ordering.

### Jito Block Engine Integration (per docs)

- **Endpoints:** JSON-RPC over HTTPS at `https://{region}.mainnet.block-engine.jito.wtf/api/v1/bundles` (send/get), `.../transactions` (single tx). Regions include `amsterdam`, `dublin`, `frankfurt`, `london`, `ny`, `slc`, `singapore`, `tokyo`. We'll maintain parallel clients across at least two regions for redundancy.
- **Methods used:**
  - `sendBundle` (max 5 fully signed base64 txs). Returns `bundle_id` (SHA256 of signatures). Jito enforces `skip_preflight=true` semantics and requires a tip ≥ 1_000 lamports paid to approved accounts.
  - `getBundleStatuses` for landed slot + confirmation, `getInflightBundleStatuses` for `Pending/Landed/Failed/Invalid` within last 5 minutes, `getTipAccounts` for dynamic validation.
- **Tip accounts (static per latest docs & provided list):**
  - `ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49`
  - `Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY`
  - `HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe`
  - `DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh`
  - `96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5`
  - `ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt`
  - `3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT`
  - `DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL`
- **Tip placement:** embed SOL transfer instruction directly inside the primary transaction (preferred) so failure cancels both trade and tip; avoid ALT usage for tip accounts per docs. When bundling, only the Jito tip matters (priority fee still paid via compute budget). We'll randomize tip account selection each bundle to reduce contention and pull current tip floors from `https://bundles.jito.wtf/api/v1/bundles/tip_floor` + websocket stream.
- **Leader coordination:** Block Engine runs 50 ms auctions keyed by account locking; we therefore (a) ensure bundle account sets minimal overlaps, (b) watch `getInflightBundleStatuses` to detect `Pending` spikes and adapt tip levels, and (c) expire bundles if targeted leader slot changes (bundles cannot cross slots).
- **Anti-sandwich:** if we need protection we can include `jitodontfront*` read-only account as first instruction account per mitigation doc, ensuring our arb tx stays first within the bundle.

## Performance Considerations

- Single-threaded executor runtime using `tokio::runtime::Builder::new_current_thread()` to prevent cross-thread wakeups.
- `ArbGraph` thread uses non-async loop with manual sleeping via `std::thread::park_timeout` when queues empty.
- Arena allocators (`bumpalo`) per opportunity for transient buffers; drop entire arena after bundle emission.
- Fixed-point arithmetic via `uint` crate to avoid floating-point nondeterminism; custom `u256` type for CLMM math safety margin.
- Extensive use of `#[inline(always)]`, `#[repr(u8)]` enums, and prefetching (via `core::arch::x86_64::_mm_prefetch`) where available to keep tick data hot.

## Failure Handling

- **Stale State:** executor revalidates pool hash before submission; mismatch leads to plan discard.
- **Flash Loan Revert:** maintain reserve health monitor; backoff faulty reserve for `N` slots.
- **Leader Reorg/Drop:** watch Jito slot feed; if leader changes unexpectedly, rebuild bundles with new tip targets.
- **Account Contention:** maintain multiple bundle variants differing in compute budget order/tips to increase landing odds.

## Possible (Not Guaranteed) Pitfalls

These issues are not currently observed but could surface later in development if we are not careful:

- **Bitquery stream fragility:** Listener threads can stall on TLS/interceptor hiccups, starving the state graph until watchdogs reconnect.
- **Queue backpressure:** `corecast_rx → state_graph → arb_engine` lock-free queues can overflow if any stage slows down, forcing us to drop freshest updates.
- **Normalization drift:** Tick math or DLMM bin interpolation bugs may inject phantom profits or hide real ones; continuous validation against on-chain state is required.
- **Cycle search blind spots:** Precomputed 2–3 hop sets might miss profitable longer paths, while widening scope risks blowing latency budgets.
- **Fixed-point precision traps:** Q64.64 arithmetic can overflow on large notionals or underflow on tiny moves; saturation guards and fuzz tests should catch this early.
- **Template/IDL drift:** Hard-coded instruction layouts can silently diverge when protocols upgrade, yielding invalid swaps exactly when opportunities appear.
- **Solend liquidity variability:** Flash borrows may revert when utilization spikes mid-slot; executor needs fallback reserves and fast retry logic.
- **Bundle contention:** Other searchers can copy or front-run bundles; we need fee randomization and staggered timing to keep fill probability high.
- **Blockhash freshness gaps:** Relying on Jito slot streams alone can produce expired blockhashes if reconnects take too long; add redundancy.
- **Observability debt:** Without fine-grained metrics/tracing, diagnosing microsecond-level regressions across stages will become painful as complexity grows.

## Telemetry & Ops

### Pool Seed Snapshot Monitoring

- **Grafana panel:** Import `docs/observability/grafana_pool_seed_snapshot.json` (Grafana → Dashboards → Import) to plot `planner_seed_snapshot_age_secs` with a constant 21 600 s (6 h) threshold. The saved panel already wires a Prometheus datasource selector plus `$job`/`$instance` filters. Sample query if you want to inline it elsewhere:
  ```promql
  planner_seed_snapshot_age_secs{job="tre"}
  ```
  The exported panel maps `0` values to “snapshot missing” and colors the series orange/red once it crosses the stale line.
- **Prometheus alert rule:** Drop `docs/observability/prometheus_pool_seed_snapshot.yml` into your Prometheus `rule_files`. It defines both `TrePoolSeedSnapshotStale` (fires when the metric stays above 6 h for >10 m) and `TrePoolSeedSnapshotMissing` (fires when the exporter stops exposing the gauge). Point the alerts at the same pager/on-call channel you use for other planner health alarms.
- **Stale snapshot runbooks:** Link the Grafana panel to a runbook that instructs ops to refresh the pool-seed snapshot or verify Bitquery ingest if the metric climbs while trades continue flowing.

## Implementation Roadmap

1. **Proto & Client Layer:** finalize Bitquery + Jito tonic modules in `build.rs`, generate stubs into `bitquery/` and `jito/` dirs.
2. **Ingestion Core:** implement `spawn_ingest` worker, unify pool/trade updates, benchmark queue latency.
3. **ArbGraph Engine:** build SoA data model, dirty-set incremental detector, unit tests for tick-cross accuracy.
4. **Template Library:** encode Raydium/Orca/Meteora layouts, add Solend reserve config loader.
5. **Executor & Bundles:** implement Solend flash path, message builder, Jito bundle submission with retry + metrics.
6. **End-to-End Harness:** simulate pools via recorded updates, ensure deterministic plan reproduction, measure latency under load.

This document should serve as the single-source blueprint for the initial implementation and future optimization passes.
