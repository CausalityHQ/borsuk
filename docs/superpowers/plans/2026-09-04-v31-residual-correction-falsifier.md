# V31 Residual-Correction Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Determine in one fast 100K run whether one to three corpus-only bytes per row can close the sole variable-rate PQ ranking miss without changing page count or S3 volume.

**Architecture:** Reuse the authenticated V30 reproduction loader and fixed page layout. Evaluate none/u8-error/sign8/sign16/exact-error/exact-cross-term arms over identical candidates, emit raw Parquet evidence and canonical claim-ineligible JSON, and stop before persistent format work unless an arm reaches 320/320.

**Tech Stack:** Python 3.12, NumPy, PyArrow/Parquet, AWS S3, Causality EC2 Spot.

**Spec:** `docs/superpowers/specs/2026-09-04-v31-residual-correction-falsifier-design.md`

### Task 1: Implement pure correction and evidence contracts

**Files:**
- Create: `scripts/run_v31_residual_correction_falsifier.py`
- Create: `scripts/test_run_v31_residual_correction_falsifier.py`

- [x] Write and preserve missing-module/API REDs.
- [x] Implement query-independent u8 error quantization, manifest-seeded fixed
  projections, exact/scalar/sign corrections, bounded page reduction, six-arm
  canonical reduction, and typed Parquet evidence.
- [x] Require exact-cross-term equality with direct squared distance on
  synthetic rows and a fixed-layout evaluator control.
- [x] Run the six focused tests, scoped Ruff, pycompile, and diff-check.

### Task 2: Bind the one-shot scientific boundary

**Files:**
- Modify: `scripts/run_v31_residual_correction_falsifier.py`
- Modify: `scripts/test_run_v31_residual_correction_falsifier.py`

- [x] Add CLI/runner mutation tests proving exact four prerequisite identities,
  page prefix, evidence output, fixed shape/arms, and no local corpus/D3 mode.
- [x] Reuse only the strict V30 authenticated loader; fit the identical PQ8
  models, reproduce the primary-plus-nearest-secondary candidate membership
  used by the frozen exact control, simulate exact persisted arm bytes, and
  write evidence only after independent final reduction.
- [x] Run focused tests, the complete V30 reproduction regression, scoped Ruff,
  pycompile, and diff-check. Commit and push the exact source.

### Task 3: Run one fast 100K Spot falsifier

**Files:**
- Modify after terminal: `docs/research/publication-v3-attempt-ledger.md`

- [ ] Archive the clean source, launch one monitored Causality Spot worker, and
  stream only the registered 46,761,076 bytes of Arrow pages plus small
  Arrow/Parquet authorities. Preserve heartbeat, result, evidence, resources,
  log, and terminal under one immutable S3 attempt prefix.
- [ ] Require baseline 319/320 and exact-cross-term 320/320 or void the run.
  Freeze the smallest shippable arm reaching 320/320; otherwise follow only the
  preregistered u16/sign32 diagnostic or close the family.
- [ ] Terminate the instance immediately, validate and commit the evidence
  ledger, and update the active architecture plan. Do not run 9.99M, 100M, D3,
  persistent sidecar TDD, or coalescing unless the causal gate passes.
