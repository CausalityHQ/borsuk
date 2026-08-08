# Paired Global-Cell Stripe Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:executing-plans and superpowers:test-driven-development.

**Goal:** Select a robust uncached S3 global-cell transfer shape using five
paired repetitions over one immutable index, without changing recall, logical
work, durable formats, or production gates.

**Design:**
`docs/superpowers/specs/2026-08-08-paired-global-cell-stripe-qualification-design.md`

## Global constraints

- Use TDD and observe each regression fail for the intended reason.
- Keep Arrow/Parquet durable formats and exact candidate/rerank behavior.
- Never use cache-assisted latency as the production selection result.
- Never inspect incomplete measurement CSV files.
- Use AWS profile `causality`, one exclusive worker, fresh disjoint prefixes,
  and retained marker/health monitoring.
- Commit verified coherent slices and fast-forward push directly to
  `origin/main`; no PR and no force push.

### Task 1: Make stripe width a bounded open-time policy

**Files:** `crates/borsuk/src/index.rs`

- [x] Add failing tests proving that 1/2/4 MiB widths produce 4/2/1 planned
  stripes for a 4 MiB envelope and that zero or greater than 4 MiB is rejected
  before an index can perform I/O.
- [x] Add `OpenOptions::global_pq_prefetch_stripe_bytes`, default it to 1 MiB,
  validate it at open, and share it through `CollectionReadRuntime`.
- [x] Pass the width into `global_pq_code_read_plans` and
  `Storage::read_striped_range`; retain the 8 MiB/16-stripe stage budgets.
- [x] Run all global-PQ, open-options, and affected group-commit tests.

### Task 2: Add the exact read-only benchmark protocol

**Files:** `crates/borsuk/examples/group_commit_bench.rs`

- [x] Add failing example tests for accepted frozen read configuration,
  rejected widths/repetitions/query counts, and deterministic sample-to-dataset
  query mapping.
- [x] Add an early `read-qualification` protocol that reads a terminal
  `samples.csv`, validates all identities and fixed dimensions/search knobs,
  opens a fresh cache, and performs no mutations.
- [x] Emit `reads.csv`, `summary.csv`, environment identity, and
  `READ_QUALIFICATION_COMPLETE`; reject recall misses and write-like requests.
- [x] Run the complete example test target and a local immutable-index smoke.

### Task 3: Add fail-closed paired runner and validator

**Files:**
`docs/research/global-cell-stripe-qualification.json`,
`scripts/bench_global_cell_stripes.sh`,
`scripts/validate_global_cell_stripes.py`, and their tests.

- [x] Write RED Python tests for exact 1/2/4 MiB arms, five repetitions,
  cyclic order, unique caches, immutable identities, 100 queries, recall 1.0,
  zero writes, terminal markers, raw traces, and resource telemetry.
- [x] Implement a runner that refuses reused output, caches, non-S3 production
  inputs, source mismatch, nonterminal base evidence, and competing work.
- [x] Implement the validator and paired selection report. The validator must
  fail closed before reading arm CSVs when the root is incomplete or failed.
- [x] Run shell syntax, complete runner/validator tests, and a local
  structurally valid smoke.

### Task 4: Full assurance and direct delivery

- [x] Run format, diff check, repository policy, strict all-feature/all-target
  Clippy, full locked workspace tests, and pinned Python tests once.
- [x] Record verification in the group-commit attempt ledger.
- [x] Commit coherent slices, fetch `origin/main`, prove it is an ancestor of
  `HEAD`, push `HEAD:main`, verify equality, and require a clean tree.

### Task 5: Run and act on paired AWS evidence

- [x] Prove Causality identity, worker health/exclusivity, terminal v67 base
  markers, base artifact checksums, and fresh result/cache locations.
- [x] Launch exactly one five-repetition paired campaign and retain its
  process/session identity.
- [x] Observe 15-minute retained waits followed by marker/process/EC2-health
  checks only; never inspect incomplete CSV files.
- [x] At terminality, run the validator first, then inspect and record only
  defensible terminal measurements.
- [x] Promote the winner only if the preregistered selection rule passes;
  otherwise return to TDD using the failed terminal evidence.
