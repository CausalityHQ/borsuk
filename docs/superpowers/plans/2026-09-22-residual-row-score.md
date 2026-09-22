# Residual Row-Score Falsifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one authenticated 100k decision cell testing whether source-trained residual PQ48+PQ24 row codes recover the page-nomination quality lost to PQ48.

**Architecture:** Retain the frozen tree, physical pages, exact-distance control, nomination rule, queries, truth and limits. Replace the 48-byte row plane with a 72-byte interleaved first-stage and residual-stage plane, sealed before query access. Fetch authenticated S3 ranges, replay every result with independent scoring, and terminate one-time Spot compute at the terminal marker.

**Tech Stack:** Python 3.12, NumPy 2.4.2, PyArrow 24.0.0, boto3, existing BORSUK research scripts, Causality EC2 Spot and S3.

**Spec:** `docs/research/algorithm-first-page-layout-ledger.md`, sections “Row-score a0001” and “Next 100k falsifier: two-stage residual row codes.”

## Global Constraints

- Source-only construction precedes access to the frozen queries, truth, tree and page representatives.
- Fixed first PQ48x8 and second residual PQ24x8, seed from `FROZEN_INPUTS.layout.seed`, 100,000 training rows and ten iterations per stage; no sweep.
- Interleaved 72-byte codes in physical page order; 16 pages per code block, actual S3 Range GETs, 32 GETs and 16,777,216 bytes maximum.
- Final data wave at most 32 pages and 16,777,216 encoded bytes; quality gates mean GT100 97.5%, p05 GT100 90%, R@10 96% on all 1,000 frozen queries.
- Single-stage PQ48 a0001, exact row scores, retained leaves and restricted oracle are paired controls; a pass is `quality-advance-memory-pending` only.
- Controller monitors terminal and instance health only while incomplete; upload immutable artifacts, terminate Spot at terminal, and never rewrite a closed prefix.
- No local Rust build while the devbox memory-pressure incident remains relevant.

## Review Focus

- An altered membership row must fail the source/physical-order seal before evaluation.
- A one-byte-corrupt or short S3 Range response must fail before nomination.
- A residual score must include cross-stage inner products and agree with direct reconstructed-vector distance on a small fixture.
- A code block crossing the 16-MiB or 32-GET cap must fail closed.
- A failed/interrupted worker must emit a noneligible terminal, preserve its prefix, and stop compute.

---

### Task 1: Residual code artifacts

**Files:** Create `scripts/native_residual_row_score_codes.py` and `scripts/test_native_residual_row_score_codes.py`; reuse `_physical_order`, `fit_pq`, `encode_pq` and `PQ48X8` from the existing scripts.

**Interfaces:** `construct_residual_codes(ids, vectors, membership, *, seed, sample_rows=100_000, iterations=10) -> ResidualCodes`; `write_residual_codes(root, artifacts, source_sha, membership_sha) -> CodeIdentities`; `read_residual_codes(root, identities, ids, membership, source_sha, membership_sha, *, dimensions, seed) -> ResidualCodes`. `ResidualCodes` carries first books `(48,256,16)`, residual books `(24,256,32)`, row codes `(N,72)`, physical source ordinals and page row counts.

- [ ] Write a tiny 48-dimensional fixture test that constructs twice, checks byte stability, verifies `codes.shape == (N,72)`, and rejects a membership-order change on read.
- [ ] Run only `python -m unittest scripts.test_native_residual_row_score_codes` and observe the expected missing-function failure.
- [ ] Implement first-stage PQ48, reconstruct source rows using their first codes, fit PQ24 on `vectors - first_reconstruction`, encode the residual, and interleave code columns. Store two books and one plane as canonical little-endian binary, with source, membership, order, dimensions, seed and all artifact SHA-256 values in a new schema seal.
- [ ] Run the focused test green and Ruff on the two files; commit this tested slice.

### Task 2: Exact residual lookup and paired page nomination

**Files:** Create `scripts/native_residual_row_score_evaluation.py` and `scripts/test_native_residual_row_score_evaluation.py`; reuse `plan_code_blocks`, `nominate_pages`, `S3CodeRangeReader` and fixed layout controls.

**Interfaces:** `residual_scores(query, first_books, residual_books, interleaved_codes) -> np.ndarray`; `evaluate_residual_query(..., read_code_range) -> ResidualSample`; `aggregate_residual_samples(samples) -> dict[str,int|str]`. Samples carry retained, restricted oracle, exact, old PQ48 and residual arms plus actual code-block coordinates and bytes.

- [ ] Add a test comparing lookup scores to direct `||q - (C1 + C2)||²` for rows with nonzero cross-stage terms; test stable source-ordinal ties, S3 short/corrupt response rejection, and a code-wave cap violation.
- [ ] Run the focused test red for the intended missing interface.
- [ ] Implement lookup with query-dependent stage tables and source-only precomputed cross terms; do not approximate the cross term. Reuse the exact page nomination and truth-count logic from a0001, with a 72-byte block planner and authenticated range callback.
- [ ] Run the focused test green and Ruff; commit the scoring slice.

### Task 3: Independent evidence and replay

**Files:** Create `scripts/native_residual_row_score_evidence.py`, `scripts/validate_native_residual_row_score_result.py` and focused tests. Follow the canonical JSON and strict identity pattern in `scripts/native_row_score_evidence.py`.

**Interfaces:** `write_residual_evidence(path, samples) -> ArtifactIdentity`; `read_residual_evidence(path, identity) -> (samples, metrics)`; `validate_residual_samples(samples, metrics, queries, truth, retained, artifacts, ids, vectors, page_bytes, limits) -> dict`.

- [ ] Add a test that tampers one selected page, one code block, one aggregate and one truth hit; each must fail validation. Add a test where a complete small fixture independently replays.
- [ ] Run those tests red for missing interfaces.
- [ ] Implement a separate block planner and direct independently coded residual score formula, recompute every retained leaf, page set, hit count, wave bound and aggregate. Reject noncanonical JSON and mismatched hashes.
- [ ] Run tests and Ruff green; commit the independent replay slice.

### Task 4: Phase-separated cell and Spot controller

**Files:** Create `scripts/native_residual_row_score_cell.py`, `scripts/launch_native_residual_row_score_spot.py` and focused tests; derive the worker/controller flow from the completed a0001 scripts without changing its archived source.

**Interfaces:** CLI phases `construct`, `evaluate`, `validate`; launcher takes immutable source archive identity, requirements digest and a unique `a0001` attempt prefix under `research/native-residual-row-score/<commit>/runs/`.

- [ ] Add tests that compile embedded terminal Python, `bash -n` the worker, assert query/truth downloads happen after source-only seal, and assert the validator gets `PYTHONPATH` under system and virtualenv Python.
- [ ] Run launcher/cell tests red for missing module behavior.
- [ ] Implement the three phases, actual authenticated S3 code range reader, immutable reservation, one-time Spot token, terminal roster, and immediate instance termination after terminal.
- [ ] Run the focused Python suite and Ruff, inspect generated launch specs, commit and push the tested source.

### Task 5: One immutable measured decision

**Files:** Update `docs/research/algorithm-first-page-layout-ledger.md` after terminal; keep raw artifacts in immutable S3.

- [ ] Freeze `git archive HEAD`, embed its exact commit marker, hash/read back the archive and upload with `If-None-Match: *`.
- [ ] Check the run prefix and tagged EC2 inventory are empty, then launch one Causality one-time Spot cell. Keep the original controller session handle; watch only terminal and infrastructure health while incomplete.
- [ ] At terminal, confirm instance termination and read back every artifact digest. Independently compare all 1,000 paired samples against the fixed gates and strongest a0001 query-blind control.
- [ ] Record a pass or kill decision, 100M two-generation resident worksheet status, and the exact next 1M locality or redesign gate. Commit and fast-forward push the ledger update.

## Self-review

- The plan covers source capability separation, persistent format, score correctness, two-wave I/O, independent replay, Spot interruption/termination and evidence publication.
- Rust production tests remain a separate red gate; this scientific cell does not claim production serving readiness.
