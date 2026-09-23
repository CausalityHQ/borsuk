# One Million Source-Vector Final-Range Diagnostic Plan

**Goal:** Determine whether source-vector row scores, with the frozen 1M
group plans, page layout and range rule, meet the fixed final-range gate.

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-exact-source-range-diagnostic-design.md`

## Task 1: Streaming source-score priority

- [x] Add a pure streaming reducer for per-page minima and stable top-100
  row scores within each arm's selected groups.
- [x] Test its priority against the full-score reference on synthetic
  layouts with ties, base/delta roles and different arm group sets.
- [x] Add authenticated source Parquet traversal and float64 squared-L2
  scoring with a fixed arithmetic identity; enforce resource limits in
  the Spot worker before launch.
- [x] Commit this slice after focused tests and scoped lint.

## Task 2: Phase-separated source-only cell

- [x] Reuse the closed generation, maps, group plans and range planner;
  enforce a truth-free planning phase and seal both plans before truth.
- [x] Evaluate ordered GT100/GT10 owner pages with the unchanged fixed
  pass rule and paired OPQ8/control comparisons.
- [x] Independently validate source identities, score replay, plan seals,
  resource receipts and every truth hit mask on the execution host.
- [x] Run focused tests and scoped lint; commit and fast-forward push.

## Task 3: Causality Spot closeout

- [x] Check active jobs and EC2 instances, create/read back the immutable
  source archive and launch one create-only Spot attempt.
- [x] Monitor terminal and instance health only, collect terminal status,
  and confirm immediate compute termination.
- [x] Independently authenticate and recount terminal artifacts off-host,
  record the decision and update the architecture contract.
- [ ] Run final proportional verification and fast-forward push the
  closed evidence to `origin/main`.
