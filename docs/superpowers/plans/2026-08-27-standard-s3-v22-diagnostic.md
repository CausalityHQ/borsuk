# Standard-S3 V22 Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:executing-plans` and `superpowers:test-driven-development`.
> Every production behavior is RED, observed, minimally GREEN, and focused
> before the next behavior.

**Goal:** Determine whether routing plus semantic exact-row packing can place a
real high-recall candidate prefix in at most four Standard-S3 ranges/1 MiB,
then test the smallest resident residual-code ranker that realizes it, before
building a new format.

**Architecture:** Reuse V21's historical-index dual authority and Spot
lifecycle. Stage L makes one authenticated corpus pass to compute true exact
prefixes, routing ranks, and layout/run censuses. Only a passing layout advances
to factorized G2 residual-code scans and D2 real S3 wave replay. Every artifact
is immutable, independently recomputed, and `claim_eligible:false`.

**Spec:**
`docs/superpowers/specs/2026-08-27-standard-s3-v22-semantic-layout-design.md`

## Constraints

- Standard S3 only; disk cache zero; no Express/local serving tier/persistent
  query cache.
- Recall@10 `>=0.975`, GT coverage `>=0.990`, every query `<=4` GET and
  `<=1_048_576` network bytes, 10M serving RSS `<=768 MiB`, 100M RSS `<=3 GiB`.
- Stage L, G2, and D2 are ordered gates. Failure never relaxes a gate.
- No persistent-format object or ordinary publication result is written here.

### Task 1: Freeze Stage-L authority and routing-rank primitives

**Files:** `crates/borsuk/src/v22_feasibility.rs`, `crates/borsuk/src/lib.rs`

- [x] RED literal arms for exact prefixes `{10,256,512,1024,1536,2048}` over
  V20 physical, V20 two-pivot repacked 32/64, semantic-within-cell 32/64, and
  semantic-cross-cell 32/64.
- [x] Implement exact validation, canonical ordering, one-based routing rank,
  duplicate-safe per-row coverage, authenticated dynamic cell-count bounds,
  and checked arithmetic. Assert 4,096 only for the Deep Image 10M authority.
- [x] Prove one rank vector reproduces any bounded probe-coverage sweep.
- [x] Run `rtk proxy cargo test -p borsuk v22_layout_census --lib -- --nocapture`,
  strict library Clippy, rustfmt, and diff check.

### Task 2: Implement exact layouts and range census

**Files:** `crates/borsuk/src/v22_feasibility.rs`, existing exact encoder/planner

- [x] RED hand fixture that separates the existing 1-D two-pivot order,
  metric microclustering within cells, and neighboring-cell placement.
- [x] Implement a precisely registered bounded microcluster algorithm: canonical
  ID order, deterministic metric pivots/ties, balanced recursion, and bounded
  per-cell work; cross-cell nearest-neighbor ordering is bounded by the
  authenticated cell count and corpus-wide quadratic clustering is rejected.
- [x] Reuse/generalize the production exact-range planner rather than fork its
  byte/request logic. Cover both `truncate` and `skip` only in G2; Stage L uses
  the complete exact prefix.
- [x] Emit exact primary useful/selected/physical/speculative bytes, amplification,
  eligibility, limiting bound, ranges, rows-per-range and contiguous-run
  histograms, plus projected-object path/length/checksum authority. Duplicate
  fetches are physical bytes. Budget-negative arms still emit their complete
  measured census and exact limiting bound.
- [ ] Prove sizing equals the proposed encoder for singleton, skewed,
  maximum-width, and compressed fixtures.
- [x] Run `rtk proxy cargo test -p borsuk v22_layout_oracle --lib -- --nocapture`.

### Task 3: Add the authenticated read-only Stage-L index census

**Files:** `crates/borsuk/src/index.rs`, `crates/borsuk/src/v22_feasibility.rs`

- [ ] RED tiny instrumented-index test around a wished-for hidden Stage-L API;
  snapshot manifest bytes and object roster.
- [ ] Stream/decode each authenticated V20 row once. Compute exact top-2048 for
  all frozen queries with bounded per-query heaps; verify exact top-10 equals
  frozen GT.
- [ ] Record each prefix row's primary cell, complete query routing rank, and
  routed-row curve; bind the decoded generation cell count and fail on
  missing/duplicate GT or mismatched authority.
- [ ] Project all seven layout authorities (four families) and run the exact
  42-arm range census over six prefixes.
- [ ] Enforce Stage-L eligibility: `>=0.995` GT cell coverage, every query
  `<=512,000` routed rows, `<=4` primary ranges, `<=1 MiB`, `<=2x` primary
  amplification. Report negative completion if none passes.
- [ ] Prove the call is mutation-free and report order is exact.

### Task 4: Publish and independently validate Stage-L evidence

**Files:** `crates/borsuk/examples/production_bench.rs`,
`scripts/run_publication_v3_cell.py`, publication execution/controller/launcher
and matching tests

- [ ] RED no-clobber/canonical/cardinality tests for arm, query-prefix sample,
  routing-rank, histogram, and summary artifacts.
- [ ] Add exclusive V22 diagnostic mode and a bounded Python validator that
  mutation-tests every field and recomputes all aggregates from raw evidence.
- [ ] Generalize V21's base-index authority without weakening it. Use distinct
  `diagnose-v22-layout` and result namespaces.
- [ ] Worker compiles current source, authenticates the explicit historical
  terminal/index, uses build-class Spot with zero swap/disk cache, uploads raw
  evidence before result/terminal receipt, and always terminates.
- [ ] Bind source/archive/binary/base authority/artifact digests/result/instance/
  cgroup/Spot/termination. No eligible layout is an honest completed negative.
- [ ] Run full production-bench, runner, execution, controller, launcher tests,
  Ruff, `py_compile`, `bash -n`, rustfmt, strict Clippy, and diff check.
- [ ] Review, freeze, fast-forward `origin/main`, run one repository assurance,
  and launch exactly one monitored Stage-L Spot attempt.

### Task 5: Implement G2 only after Stage L passes

**Files:** quantizer seam, V22 module/index/benchmark/publication files and tests

- [ ] Freeze only Stage-L-viable routing/layout/prefix factors.
- [ ] RED direct-vs-prepared residual scoring; add one shared existing-kernel
  seam, never another distance implementation.
- [ ] Fit diagnostic-only eligible widths `{8,12,16,24,32}` plus the 64-byte
  memory-ineligible positive control deterministically from training authority;
  bind each fitted codebook digest.
- [ ] One scan per `(width, routed-cell count)` produces one full ranking; all
  prefixes/layouts are views. Add explicit admission `{truncate,skip}`.
- [ ] Record representation/layout/final losses, run histograms, and measured
  CPU. Require p99 routing+scan+decode+dedup+exact `<=15 ms`.
- [ ] Compute RSS as measured preparation baseline minus only enumerated live
  allocations actually replaced plus real V22 capacities/transients. Enforce
  10M `768 MiB` and 100M `3 GiB` separately; 64 bytes is never eligible.
- [ ] Publish/recompute G2 evidence, freeze at most three Pareto arms over
  resident bytes/ranges/bytes/CPU, review, assure, and run one Spot attempt.

### Task 6: Replay real D2 S3 waves

- [ ] Run every Pareto arm on the registered serving instance/cgroup, not the
  diagnostic host. Replay exact measured range shapes against immutable S3
  objects.
- [ ] Record real end-to-end latency plus stage decomposition. Historical
  four-wide 64-KiB p99 `57.950 ms` is a bound input, not evidence for V22.
- [ ] Test hedge `off` for every arm. Add `{20,35}` only when the primary plan
  has at most three GETs and reserves one request plus the largest hedgeable
  range inside 1 MiB total network bytes. Four-primary-GET arms remain unhedged.
  Primary amplification excludes the separately reported hedge.
- [ ] Require end-to-end `60/80/100 ms`; bind raw evidence and terminate.

### Task 7: Authorize or reject production V22

- [ ] If any D2 arm passes all gates, freeze the lowest-resource Pareto winner
  only after latency evidence and write a separate persistent-format plan.
- [ ] Restore explicit fresh-open+prepare+first-query latency/RSS evidence,
  five strict-cold repetitions, concurrency throughput, build/mutation
  non-regression, and paired competitor methodology in that plan.
- [ ] If no arm passes, record immutable rejection and change architecture;
  never raise GET/byte/RAM gates or lower quality.
