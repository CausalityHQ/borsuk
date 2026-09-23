# Page-Centered Group Codes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one authenticated 100k decision cell for page-centered PQ48 row codes read through fixed four-page groups.

**Architecture:** Build source-only page means, a global PQ48 book over page residuals, and one immutable S3 code object with independently hashed four-page ranges. The fixed geometric tree nominates the first 32 distinct groups from its 128-leaf order. Score fetched rows by page mean plus PQ reconstruction, pair them with exact row scores over those same groups, and independently replay all 1,000 queries.

**Tech Stack:** Python 3.12, NumPy 2.4.2, PyArrow 24.0.0, boto3, the existing BORSUK research scripts, Causality EC2 Spot and S3.

**Spec:** `docs/superpowers/specs/2026-09-23-page-centered-group-codes-design.md`.

## Global Constraints

- Freeze the current authenticated 100k source, 480-KiB two-means membership, geometric tree, 1,000 development queries and GT100; never use query/truth during construction.
- One source-only PQ48x8 book over `x - mean(page(x))`, seed 20260921, 100,000 training rows and ten iterations; no width/seed/group sweep.
- Use exactly the first 32 distinct groups of four consecutive pages encountered in the fixed 128-leaf tree order; read every row in each selected group.
- Actual authenticated S3 code GETs ≤32 and code bytes ≤16,777,216; planned final data pages ≤32 and encoded bytes ≤16,777,216. No silent truncation or SDK retry.
- Quality on all 1,000 frozen queries: mean GT100 ≥97.5%, p05 GT100 ≥90%, GT10 page containment ≥96%. A code pass is `quality-advance-memory-pending` only.
- Pair an exact row scorer over precisely the same groups. Kill grouping if exact misses; otherwise kill page-centered codes if they miss.
- Immutable Spot attempt, terminal-closed artifacts, immediate compute termination, and no inspection of incomplete scientific files. No local heavy Rust build.

## Review Focus

- A membership reorder or centroid/code alteration must fail its physical-order/source seal.
- A short, duplicated, reordered or one-byte-corrupt S3 group range must fail before scoring.
- The page-centered lookup must agree with direct `||q - (mean(page)+decoded_residual)||²` under stable float32 tie ordering.
- The exact control must use the same full grouped candidate set, with the identical data-page nomination rule and byte cap.
- An interrupted worker must produce a noneligible terminal and terminate the one-time Spot instance without rewriting the attempt.

---

### Task 1: Source-only group code artifact

**Files:** Create `scripts/native_page_centered_group_codes.py` and `scripts/test_native_page_centered_group_codes.py`.

**Interfaces:** `construct_group_codes(ids, vectors, membership, *, seed, sample_rows=100_000, iterations=10) -> GroupCodes`; `write_group_codes(root, codes, source_sha, membership_sha, tree_sha) -> GroupCodeIdentities`; `read_group_codes(root, identities, ids, membership, source_sha, membership_sha, tree_sha, *, dimensions, seed) -> GroupCodes`. `GroupCodes` carries float32 PQ48 books, float32 page means, physical row codes, source ordinals, page counts, group offsets and group digests.

- [ ] Write tiny source fixture tests for deterministic page-centering, physical code order, group boundaries, book/plane stability, and rejection after a one-row membership reorder or group corruption.
- [ ] Run only `python -m unittest scripts.test_native_page_centered_group_codes` and observe the missing-module/interface RED.
- [ ] Implement source-only means, residual fit/encode, four-page group object, canonical source/membership/tree-bound seal, per-group SHA-256 and strict readback.
- [ ] Run focused tests GREEN, Ruff and `git diff --check`; commit the artifact slice.

### Task 2: Group shortlist and paired nomination

**Files:** Create `scripts/native_page_centered_group_evaluation.py` and `scripts/test_native_page_centered_group_evaluation.py`.

**Interfaces:** `plan_groups(retained_pages, group_manifest, *, maximum_groups=32, maximum_bytes=16_777_216) -> GroupPlan`; `evaluate_group_query(..., read_group_range) -> GroupSample`; `aggregate_group_samples(samples) -> dict`. `GroupSample` contains the fixed group IDs/ranges, actual successful GET/byte deltas, exact and coded page selections/hits, and old paired controls.

- [ ] Add a fixture where the first 32 distinct groups differ from the first 128 pages, and a direct reconstruction scoring comparison with exact source distances and stable ties. Test short/corrupt group reads and read caps.
- [ ] Run focused tests RED for missing interfaces.
- [ ] Implement one range per group, group digest authentication, page mean plus PQ residual scoring with float64 accumulation/one float32 cast, exact scorer on the same candidates, and unchanged top-100-row page nomination.
- [ ] Run focused tests GREEN and Ruff; commit the scorer slice.

### Task 3: Independent evidence and replay

**Files:** Create `scripts/native_page_centered_group_evidence.py`, `scripts/validate_native_page_centered_group_result.py`, and focused tests.

**Interfaces:** `write_group_evidence(path, samples) -> ArtifactIdentity`; `read_group_evidence(path, identity) -> (samples, metrics)`; `validate_group_samples(samples, metrics, queries, truth, membership, source, group_artifacts, page_bytes, limits) -> dict`.

- [ ] Add tests that tamper a group coordinate, selected page, hit count, aggregate, source/membership identity, and one group byte; each must fail independent validation. Add a complete tiny fixture.
- [ ] Run those tests RED for missing interfaces.
- [ ] Replay group choice, every source vector and decoded code score, exact/coded page nomination, hits, actual read coordinates, both wave caps, and aggregate in separate code. Preserve canonical JSON and hash checks.
- [ ] Run focused tests GREEN and Ruff; commit replay slice.

### Task 4: Phase-separated cell and Spot controller

**Files:** Create `scripts/native_page_centered_group_cell.py`, `scripts/launch_native_page_centered_group_spot.py`, and focused tests. Reuse frozen-input readers and the proven residual-cell/controller flow without changing archived source.

**Interfaces:** CLI phases `construct`, `evaluate`, `validate`; launcher accepts frozen archive URI/SHA/bytes, requirements SHA, and unique `a0001` prefix under `research/native-page-centered-groups/<commit>/runs/`.

- [ ] Test that construction completes and seals before query/truth download; compile embedded terminal Python, `bash -n` the worker, verify validator `PYTHONPATH` under both Python paths, and reject duplicate attempt reservations.
- [ ] Run focused launcher/cell tests RED for missing interfaces.
- [ ] Implement source-only networkless construct, non-root evaluator, actual S3 group GETs with no retry, independent validator, canonical terminal/artifact roster, phase high-water RSS gate below 3 GiB, and controller termination after terminal.
- [ ] Run focused Python suite, Ruff and generated launch-spec inspection; commit and fast-forward push frozen source.

### Task 5: One immutable measured decision

**Files:** Update `docs/research/algorithm-first-page-layout-ledger.md` after terminal; keep raw artifacts in immutable S3.

- [ ] Freeze `git archive HEAD` with exact commit marker; compare extracted files/modes to `git archive`, hash/read back, upload with `If-None-Match: *`.
- [ ] Check prefix and tagged EC2 inventory empty, launch one Causality one-time Spot attempt, keep its original controller session and monitor terminal/infrastructure only.
- [ ] At terminal, confirm instance termination, read back every artifact size/SHA, compare all 1,000 paired query samples, and independently verify exact grouped and coded quality/resource gates.
- [ ] Record kill or limited pass, source and artifact identities, two-generation resident worksheet status, and the precise next 1M or redesign gate. Commit and fast-forward push the decision ledger.

## Self-review

- The exact grouped control isolates locality loss from code fidelity in the same cell.
- The group manifest carries a bounded 1M read schedule, while the 100M memory envelope remains unqualified until the centroid-free production loader and concurrent allocation inventory are measured.
- The cell measures code-wave S3 reads and page containment; it does not claim data-wave S3 latency, SQ8 serving recall, product readiness, or competitor parity.
