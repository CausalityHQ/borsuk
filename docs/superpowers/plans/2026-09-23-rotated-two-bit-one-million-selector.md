# ReLAION-1M Eight-Page Centroid Selector Implementation Plan

> Execute inline in the existing isolated `devbox/prod-ready-v9` worktree. The persistent user goal authorizes iterative research; current developer instructions prohibit spawning subagents. Use focused TDD, then one frozen Causality Spot attempt.

**Goal:** Decide whether one source-only float16 group centroid per adjacent eight-page V85 group can select 32 groups containing enough frozen ReLAION-1M truth to justify building the 200-byte row-code plane.

**Architecture:** Authenticate the frozen 1M source and V85 generation, parse base/delta page membership without loading all source vectors, stream source batches to fit float16 group centroids, then open frozen development queries/truth and score all centroids. An independent reducer reconstructs the source-only artifacts and per-query selections. The Spot controller reserves an immutable prefix, writes a terminal, and terminates its one instance.

**Tech stack:** Python 3.12, NumPy, PyArrow, boto3, S3 Range/whole-object reads, EC2 Spot. Use the existing pinned `scripts/requirements-format-bench.txt`.

**Spec:** `docs/superpowers/specs/2026-09-23-rotated-two-bit-one-million-bridge-design.md`, especially its centroid-selector screen.

## Global constraints

- Frozen V98 ReLAION-1M source/query/truth/generation/base/delta identities come from `scripts/launch_v98_hierarchical_row_router_spot.py:FROZEN_INPUTS`; the generation-bound router identity is in the spec. No validation or holdout query is read.
- Source-only build sees source, generation, base, delta and router. Query/truth download begins only after a canonical source seal is written and uploaded.
- Groups are exactly consecutive same-role pages of width eight. Compute one float16 centroid from source vectors per group and rank all groups by squared L2 with stable `(score, role, ordinal)` ties. Select 32 groups, with no alternate representation or sweep.
- Per-query code-wave projection is exact `sum(200*group_rows + 4 + 4*group_pages)` over chosen groups; require ≤32 groups and ≤16,777,216 bytes.
- Group truth containment must reach mean GT100 ≥97.5%, p05 GT100 ≥90% and GT10 ≥96% on all 1,000 frozen development queries. A pass is routing feasibility only, not a code, S3-serving, or 100M-memory claim.
- Every artifact is canonical, length/SHA-256-bound and read back after the terminal. Worker phases stay below 3 GiB RSS and have zero swaps. Causality Spot is default, no overlapping attempt, immediate termination at terminal.

## Review focus

- A page ID missing from source or repeated across base/delta must invalidate the seal; test both.
- A short or re-ordered Arrow page must fail before queries become available; test physical roster mismatch.
- A corrupted centroid, membership map, query or truth file must fail independent replay; test one bit flip for each relevant artifact class.
- Equal centroid distances must break ties by role then group ordinal; test a synthetic tie at 768 dimensions.
- A source-ordinal batch boundary must not change centroid bytes; test two batch sizes on the same fixture.

## Task 1: source-only page map and centroid artifact

**Files:** Create `scripts/native_one_million_group_selector.py` and `scripts/test_native_one_million_group_selector.py`.

**Interfaces:** `build_selector(source: Path, generation: Path, base: Path, delta: Path, router: Path, out: Path, identities: Mapping[str,ObjectIdentity], *, batch_rows: int=4096) -> dict[str,object]` writes `centroids.bin`, `membership.bin`, `seal.json`. `read_selector(out: Path, identities: Mapping[str,ObjectIdentity]) -> SelectorArtifact` validates exact schema and hashes. `SelectorArtifact` exposes ordered `(role,first_page,last_page,row_count,code_bytes)` groups, centroid matrix and sorted ID→group map.

- [ ] Write synthetic tests for base/delta page grouping, strict ID coverage, source-only access, deterministic float16 centroid bytes, batch-size stability, 200-byte code projection and seal mutation. Use small Arrow pages and 768D source Parquet.
- [ ] Run the focused test RED, then implement generation/object SHA checks, page parsing using the narrow `_read_page_run` primitive from `v97_row_width_screen.py`, streaming `ParquetFile.iter_batches`, stable grouped float64 summation, fixed little-endian serialization, and canonical source seal. Do not call `load_screen_inputs`.
- [ ] Run the focused test GREEN, Ruff, `git diff --check`, and commit the verified slice.

## Task 2: frozen query screen and independent replay

**Files:** Create `scripts/native_one_million_selector_evaluation.py`, `scripts/validate_native_one_million_selector.py`, and focused tests.

**Interfaces:** `evaluate_selector(artifact: SelectorArtifact, queries: Path, truth: Path, out: Path, identities: Mapping[str,ObjectIdentity]) -> dict[str,object]` writes canonical `evidence.json` and `result.json`. `validate_selector(...) -> dict[str,object]` recreates centroids from source and all 1,000 query plans without calling the producer scorer or query planner, writes `validation.json`.

- [ ] Test deterministic top-32 equal-score role/ordinal ties, all-truth denominators, p05 nearest-rank, exact projected bytes including headers, a missed truth group, altered sample and altered aggregate.
- [ ] Run RED; implement the single centroid L2 selector, 32-group code projection, per-query GT10/GT100, three quality thresholds, and the `group-centroid-selector-killed` versus `selector-feasible` decision. Write all 1,000 ordered samples and aggregate in canonical JSON.
- [ ] Run GREEN; independently recompute source group sums and query ranking with a distinct evaluation path in the validator. Reject any producer/validator mismatch. Run Ruff and diff check; commit the slice.

## Task 3: one immutable Spot decision

**Files:** Create `scripts/native_one_million_selector_cell.py`, `scripts/launch_native_one_million_selector_spot.py`, focused tests; append the terminal-closed decision to `docs/research/algorithm-first-page-layout-ledger.md`.

**Interfaces:** Worker phases are source-only construct, seal, query/truth evaluation, independent validation, resource gate and terminal. The controller accepts source commit/archive identity and uses `profile=causality`; it reserves one distinct `research/native-one-million-group-selector/<source-commit>/runs/relaion-1m-dev1000-a0001/` prefix with create-only writes.

- [ ] Test worker user-data shell syntax/size, source/query capability boundary, exact frozen identities, digest failures, 3-GiB/zero-swap enforcement, failed terminal exit, prefix collision and guaranteed Spot termination.
- [ ] Run RED; implement worker and controller using the tested rotated-two-bit Spot lifecycle. Hash/upload source-only artifacts before downloading query/truth; save full diagnostic text on failure and a canonical terminal roster on completion. Run focused tests and Ruff.
- [ ] Obtain read-only implementation review, fix verified findings, run one final focused gate and diff check. Commit/push the source to `origin/main` after a fast-forward ancestry check.
- [ ] Archive exactly `git archive HEAD` with `.borsuk-source-commit`, verify tracked blob bytes/modes, upload with `If-None-Match:*` and read back. Check no active matching instance or existing attempt prefix. Start one Spot controller session and monitor terminal/infrastructure only.
- [ ] After terminal, collect the original controller exit, prove instance termination, read back and hash every terminal-listed artifact, independently recompute query metrics and resource caps, then record the single fixed decision and exact next full-cell or redesign gate in the ledger; commit/push it.

## Stop rule

If selected groups miss any quality threshold or projected code-wave bound, stop this centroid design. A failure is evidence only against the fixed single-centroid selector on the frozen V85 layout, not against every eight-page grouping or the two-bit code. If it passes, implement the spec's separate 1M code-group and actual adjacent-range serving cell from a new frozen revision; do not claim production readiness from this screen.
