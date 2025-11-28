# Tip Funding Plan

## Background

Stage 2 currently assumes the bundle payer has enough SOL on hand to cover the Jito tip that Strategy assigns in Stage 1. That assumption breaks down whenever:

- the wallet balance drifts close to zero (e.g., after several failed bundles that still tipped), or
- the arb path relies on a flash-loaned asset (WSOL or something else) yet the strategy never reserves cash to repay the validator tip.

As a result, Stage 3 builds signed bundles that immediately fail with `InsufficientFundsForFee`, even though the route itself could have produced enough profit to reimburse the tip had we budgeted for it. We need to make tips first-class citizens in both the risk model and the planner so we only submit bundles whose validator payments are actually fundable.

## Goals

1. **Profit Accounting:** Treat the planned tip (and the carry cost of borrowing it) as a deterministic liability when evaluating opportunities. A candidate should only be accepted if `net_profit = projected_profit - (tip + funding_fee + conversion_slippage)` still clears `strategy.min_profit_lamports`.
2. **Deterministic Funding:** When Stage 2 sets up a flash-loan, it must over-borrow enough native lamports to cover the upcoming tip. If the route starts in a non-WSOL mint we need a deterministic "tip conversion" suffix that mints SOL right before the tip instruction executes.
3. **Wallet Safety Net:** Even after we borrow ahead, the wallet must enforce a "max % of balance" guardrail so we never strand ourselves with a zero SOL balance because the conversion leg failed or Solend liquidity dried up.

## Design Overview

### 1. Strategy / Stage 1

- Add two new knobs under `strategy`: `wallet_tip_cap_bps` (optional, default `null`) and `tip_carry_fee_bps` (implicitly the Solend 5 bps fee for now).
- `StrategyEngine::evaluate` will:
  1. Estimate the raw profit in lamports (only when `start_mint == WSOL`).
  2. Ask `TipOracle` for the recommended tip, but allow the oracle to clamp the amount down to at most `wallet_balance * wallet_tip_cap_bps / 10_000` using a background balance monitor (polls `getBalance(payer)` on whichever QuickNode RPC is configured). The oracle also enforces a configurable `tip_floor_min_lamports`, so even if the live Jito tip floor API briefly reports an unusually low value we continue bidding with a healthy baseline.
  3. Deduct the requested tip plus the expected flash-loan fee for that slice before checking `min_profit_lamports`. When the net becomes negative, the candidate is rejected immediately.
  4. Annotate the bundle with `tip_funding_plan` metadata (`DirectWallet`, `FlashBorrowed`, or `PostRouteConversion`) so Stage 2 can apply the right prefix/suffix mutations.
  5. Apply a congestion-aware multiplier sourced from the Stage 3 latency/expiry feed. When `bundle_stage3_latency_ms` drifts above the configured target or expirations spike, the multiplier nudges tips upward (and telemetry tracks the exact factor via `bundle_stage3_tip_multiplier_milli`). This keeps our bids competitive during block-engine backlogs while still clamping to `strategy.max_tip_lamports` and the wallet-cap guardrail.

### 2. Stage 2 Flash-Loan Planner

- Extend `InstructionPlanner::plan(...)` so it receives both the `BundleCandidate` and its chosen `tip_funding_plan`.
- For `FlashBorrowed` plans (route starts in WSOL):
  - Increase the Solend `borrow_amount` by `tip_lamports`.
  - Insert a prefix scaffold that siphons `tip_lamports` of WSOL into a scratch native account and immediately closes it so the payer system account now owns those lamports before the tip instruction fires.
  - Make sure we reserve the additional Solend fee in the profitability model so repayment still clears.
- For `PostRouteConversion` plans (route starts or ends in a non-WSOL mint):
  - Append conversion legs that swap the reserved amount into WSOL (whichever pool in the strategy allowlist has liquidity) and then use the same unwrap scaffold above.
  - The conversion cost (slippage + fees) becomes part of the strategy’s net-profit check so we never submit if the round trip can’t pay the validator.

### 3. Wallet-Aware Tip Clamp

- `TipOracle` now tracks an optional `WalletBalanceTracker` which polls RPC every few seconds. When `wallet_tip_cap_bps` is configured, `recommended_tip` becomes `min(strategy_tip, balance * cap)`.
- The oracle clamps every live tip floor update to `tip_floor_min_lamports` before applying the markup and congestion multipliers. This keeps tips “set-and-forget”: transient dips in the public floor feed no longer drag our base bids below the configured safety level.
- When the balance falls to zero, the oracle refuses to emit a tip which causes Stage 1 to drop the candidate (better to skip the bundle than to send an unfunded tip).

## Implementation Notes

- The balance monitor reuses existing QuickNode RPC URLs; it round-robins through the list and logs failures so we can alert if the wallet stops refreshing.
- The flash-loan over-borrow and unwrap scaffolding lives entirely inside Stage 2 so Stage 3 remains unchanged – it still signs whatever instruction list it receives and extracts the tip amount for telemetry.
- Non-WSOL conversion logic reuses the template planner. We model the conversion as an extra `RouteLegPlan` with `template_ready=false` so Stage 2 rehydrates the right swap template without Stage 1 having to understand the details.
- Profitability math stays in lamports for now; if we later add SPL-native opportunities we can extend `lamports_if_wsol` to other stable wraps (mSOL, jitoSOL, etc.) and express the tip reserve in whichever units Stage 2 needs.

## Rollout Plan

1. Land the configuration/doc changes plus the balance monitor & profit accounting so we stop accepting unfundable routes immediately.
2. Teach Stage 2 to over-borrow WSOL and emit the unwrap scaffold (flash-loan-only path).
3. Add the non-WSOL conversion suffix with a per-token routing policy (likely reuse the best Raydium CLMM pair vs. WSOL).
4. Expand telemetry: expose `strategy_tip_cap_hits_total` and `wallet_balance_lamports` gauges so we can alert when the clamp throttles us.

## Open Questions

- Which pools are canonical for the conversion suffix? (Raydium vs. Orca vs. Jupiter route)
- Do we want to move the tip instruction to the end of the bundle once we can deterministically fund it (safer for validators, but they may prefer it first)?
- Should we persist wallet-balance samples for postmortems, or is Prometheus enough?

Until those are settled we implement the WSOL-first path plus the wallet clamp; the conversion suffix can follow as an incremental change.
