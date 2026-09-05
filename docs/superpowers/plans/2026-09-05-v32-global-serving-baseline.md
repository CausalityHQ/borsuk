# V32 Global Serving Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify physical serving parity before measuring the existing1M index.
**Architecture:** A pure Python global-receipt validator precedes a separately
authenticated local/Spot controller. Keep source/query/truth authority outside
the truth-free serving executable.
**Tech Stack:** Rust qualifier, Python unittest, Arrow/Parquet, canonical JSON, Causality Spot.
**Spec:** `docs/superpowers/specs/2026-09-05-v32-global-serving-baseline-design.md`

## Global Constraints

-32 rows, global768,scan262144,candidates12288,pages16,k10.
- Preserve raw output SHA; no float parse-reserialize equality requirement.
- No S3/science during validator implementation; no compatibility aliases.

### Task 1: Pure physical-parity validator

**Files:** create `scripts/v32_global_serving.py` and
`scripts/test_v32_global_serving.py`.
**Interface:** `validate_global_serving_batch(payload: bytes, *, expected: tuple[GlobalQueryExpectation, ...], pages: tuple[GlobalPageIdentity, ...], source_rows: int) -> dict`.
Dataclasses are frozen. Query expectation fields are `query_ordinal`,
`candidate_replay_sha256`, `page_ordinals`; page fields mirror Rust
`ordinal,sha256,encoded_bytes,primary_rows,replica_rows`.

- [x] Stage six tests: coherent32-row pass; mixed schema/config/claim rejection;
  coherent-looking replay/page identity drift rejection against fixed expected;
  page count/order/row/byte drift; match identity/order/type/nonfinite drift;
  timing/work concrete types, phase sums and ceiling drift. Hand-derived fixture:
 16 pages×2 rows×100 bytes=32 rows/1600 bytes; ten sorted matches;32 query hashes.
- [x] Run `uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt python -m unittest scripts.test_v32_global_serving` for missing-module RED.
- [x] Implement exact dataclasses and validator with strict JSON parsing,
  concrete ints (never bool), finite distances, source bounds, distinct ordered
  matches, exact expected replay/page equality, recomputed row/bytes, phase sums,
  observed counts within frozen budgets, and explicit global batch schema.
- [x] Rerun identical six-node GREEN; scoped Ruff/pycompile/diff-check.
- [x] Commit verified validator and design (`34a6ed9f7c5cd3d8a74ca89ec2484342a6ced0a0`).

Review corrections: retain exactly min(12288,codes_scanned), consider exactly
sixteen distinct pages, and score at least one root. Three mutations reproduced
the missing gates, then the full six-node gate passed. Oversized integer distance
also reproduced an OverflowError and now fails at the finite-value boundary.

### Task 2: Authority loader and execution controller

**Files:** extend `scripts/v32_global_serving.py`, its tests, and the existing
Spot launcher only after Task1 is accepted.

First bounded substep: `load_global_replay_authority(terminal: bytes,
manifest: bytes, page_locations: bytes, registration: GlobalReplayRegistration)`
returns frozen `GlobalReplayAuthority(expected, pages, source_rows, query_start)`.
Registration independently pins terminal/manifest SHA and byte length, query,
truth and truth-receipt SHA, source count and query start. Check both byte hashes
before parsing; the manifest pins page-location Parquet hash and length. Decode
the Rust four-column nonnullable schema (u32 ordinal, fixed binary32 SHA, u32
length, u16 rows), contiguous ordinals and total unique source rows. Extract
original first-distinct page identities from control.queries and replay hashes
from virtual_geometric.queries, paired by the exact registered32 ordinals. Do
not take the virtual page selections. Cross-bind query/truth/manifest hashes and
fixed global configuration. This is an immutable historical evidence projection,
not a production old-index reader or full science-result revalidation. Exact
registered terminal bytes root unused historical scientific fields.

- [x] Test byte/hash drift before parse; physical Parquet schema/count/ordinal
  drift; fixed query order/hash and original-page identity cross-binding.
- [x] Implement projection only, then run affected tests and scoped static gate.
  No network, qualifier invocation or page body in this substep. Actual query/
  truth materialization and executable identity remain the following controller
  boundary; the projection alone cannot admit a scientific result.

Projection verification: three-node missing-API RED, three-node GREEN, then
ten total tests including physical-schema/coverage mutations; scoped Ruff,
py_compile and diff-check GREEN. Astra read-only review READY. Before the next
quality campaign, independently reproduced a truth distance summation-order
gap: NumPy axis reduction differs from Rust's sequential f64 accumulation for
an adversarial near-tie. Correct it under separate TDD and new truth authority;
preserve all historical truth objects and original control comparisons.

Controller must separate historical control truth identities (v2 hashes inside
the immutable replay terminal) from active quality truth (new v3 receipt and
Parquet). Both bind the same corpus/query/window, but their hashes must not be
forced equal or overwritten. Frozen replay digest/page sequence is truth-free
routing evidence; current quality is independently reduced from active truth.
Report whether old/new truth IDs differ before interpreting recall changes.

- [ ] Inspect exact terminal/manifest/Parquet layouts and write the bounded
  controller implementation subplan before edits. It must name exact artifact
  roles, URI/SHA/length bindings, output schema and frozen execution command.
- [ ] TDD exact artifact graph and32-query physical parity using real loaders;
  malformed authority must prevent invoking the qualifier.
- [ ] TDD independent recall/timing reduction and terminal preservation; failed
  quality remains evidence, not an exception that discards metrics.
- [ ] Test direct CLI and existing Spot lifecycle, review with Astra, commit;
  then register one1M physical-read attempt. Do not claim cold-cache isolation,
  sustainable QPS or write performance from that single batch.
