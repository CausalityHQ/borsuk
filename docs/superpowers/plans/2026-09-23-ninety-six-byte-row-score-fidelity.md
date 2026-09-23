# Ninety-six-byte row-score fidelity implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task. Do not create subagents for this plan.

**Goal:** Measure whether a real 96-byte row scorer preserves the fixed ReLAION-1M source-range result.

**Architecture:** Implement the exact 94-byte sign plus binary16 scale record as a versioned, authenticated code plane. Screen it against source scores on the fixed OPQ8 plans; build standard PQ96 on those same inputs only if sign96 fails. Seal source-only construct/plan phases before truth-bearing evaluation.

**Tech Stack:** Python 3, NumPy 2.4.2, PyArrow 24.0.0, pytest, Ruff, boto3, AWS Causality Spot, Lean 4.33.0.

**Spec:** `docs/superpowers/specs/2026-09-23-ninety-six-byte-row-score-fidelity-design.md`

## Global Constraints

- Record width is exactly 96 bytes: 752 little-endian sign bits and one little-endian binary16 scale.
- Keep the rotation seed 20260923 and coordinates where `j % 48 != 47`.
- Every score uses the stored rounded scale, pinned reduction order, and physical-row-ordinal ties.
- The 1M selected groups, physical layout and 32 merged ranges/16,777,216-byte rule stay fixed.
- Process-tree RSS stays below 3 GiB minus 64 MiB; swap stays zero on Spot.
- No overlapping remote campaigns or local full suite during swap pressure.

## Review Focus

- Zero, NaN and infinity coordinates: zero sign and invalid source/query values must have explicit behavior.
- Binary16 scale overflow or underflow: construction must reject a value it cannot round to finite nonnegative storage.
- Physical-order corruption: a reader must reject a changed row count, order, record byte or group digest.
- Candidate-row batching: scoring must not materialize a full 1M-by-752 matrix.
- Cohort leakage: truth must be unavailable to construct and plan, and holdout results must not tune the code.

---

### Task 1: Sign96 encoding and scoring kernel

**Files:** Create `scripts/native_rotated_sign96.py` and `scripts/test_native_rotated_sign96.py`.

**Interfaces:** `encode_records(vectors: np.ndarray, mean: np.ndarray, *, rotation_seed: int) -> np.ndarray` consumes physical-order float32 source batches and returns `uint8[n,96]`; `score_records(query: np.ndarray, mean: np.ndarray, records: np.ndarray, *, rotation_seed: int) -> np.ndarray` returns float32 scores in record order.

- [x] Write focused tests for byte count, bit order, coordinate selection, zero sign, stored-scale scoring, invalid inputs, and a 2,048-row batch boundary.
- [x] Run the focused tests and observe the missing-module failure.
- [x] Implement the encoder and scorer with the pinned three-block Hadamard function, 94+2 record, float64 reductions and bounded batches.
- [x] Run the focused tests, scoped Ruff and Lean proof; inspect peak RSS of the focused test process (128,220 KiB, zero swaps).
- [x] Commit and fast-forward push this independently reviewable kernel slice.

### Task 2: Authenticated sign96 code-plane and truth-separated 100k cell

**Files:** Create focused code-plane, cell, validator, worker, launcher and test modules under `scripts/`; reuse the existing source-range plan/evaluation helpers only through their public boundaries.

**Interfaces:** Construction writes a versioned mean, groups object and seal; plan consumes only source, queries and selected groups; evaluate consumes a sealed plan and truth; validator independently rereads group bytes and recounts all query masks.

- [ ] Test corruption of group bytes, wrong format/version, changed physical order, wrong source/query identity and truth access during planning; observe failures against the missing integration.
- [ ] Implement the sign96 authenticated group reader/writer in bounded batches and the paired source/sign candidate-row scoring pipeline.
- [ ] Run focused cell tests and scoped lint; independently review generated remote worker artifacts and terminal roster.
- [ ] Commit, fast-forward push and archive the exact source commit before the Spot launch.
- [ ] Run one sealed paired 100k Spot attempt; monitor only terminal markers and infrastructure health while incomplete.
- [ ] Close the terminal attempt with independent artifact hashes and mask recount; stop Spot compute immediately; apply the preregistered screen stop rule.

### Task 2b: PQ96 rescue only if sign96 fails

**Files:** Create PQ96 code-plane, cell and validator modules under `scripts/` only after the closed sign96 screen fails.

**Interfaces:** Reuse the sealed 100k source identity, OPQ8 group plans, layout, query order, source-score control and fixed 100k thresholds; write a distinct versioned PQ96 object and plan.

- [ ] Authenticate the completed sign96 failure and freeze the PQ96 rescue source commit.
- [ ] Implement and narrowly test 96-by-8D training, physical-order 96-byte codes, authenticated groups and truth-separated planning.
- [ ] Run one Spot rescue attempt, close it independently and advance only a passing scorer.

### Task 3: Fixed 1M gate for survivors

**Files:** Extend the paired cell and validation modules with the frozen 1M group/layout identities; append the closeout to the research ledger and architecture contract.

**Interfaces:** Each surviving scorer returns 1,000 truth-free plans and a separately derived per-query hit mask for the exact frozen 1M development cohort.

- [ ] Test that changed OPQ8 groups, layout, range caps, query order or code-object identity are rejected.
- [ ] Run the narrow affected tests and scoped lint, then one repository assurance command on a remote machine with recovered memory.
- [ ] Commit and fast-forward push the frozen implementation and source archive.
- [ ] Run one sealed 1M Spot attempt for surviving arms; collect terminal state and terminate compute.
- [ ] Authenticate, recount and compare fixed gates; record failures without tuning on development queries.
- [ ] If a scorer passes, preregister untouched-query actual code/data reads and serving latency/resource gates.
