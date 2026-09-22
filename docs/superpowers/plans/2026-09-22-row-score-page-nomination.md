# Row-Score Page Nomination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task by task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Test whether query-blind PQ48 row scores recover the frozen ReLAION 100k page-containment gate behind one bounded code read.

**Architecture:** Reuse the sealed two-means physical pages and 128-leaf geometric tree. Construct a page-ordered PQ48 code plane from the source alone, read complete 16-page code blocks for retained leaves, and nominate at most 32 data pages by approximate top-100 row counts. The same nomination with exact row distances isolates code distortion from shortlist or objective failure. A separate validator authenticates all artifacts and replays every route.

**Tech Stack:** Python 3, NumPy 2.4.2, PyArrow 24.0.0, unittest, the existing PQ48 implementation, AWS EC2 Spot and S3 with profile `causality`.

**Spec:** `docs/research/algorithm-first-page-layout-ledger.md` § “Next 100k decision: row-score page nomination behind a code read”.

## Global Constraints

- Frozen source/query/truth SHA-256: `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`, `4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac`, `ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7`.
- Geometric membership and tree are exact immutable artifacts from source commit `67c88488fb17a9f02715c6d262a22225cf950de5`; authenticate bytes and source bindings.
- Fixed 128 retained leaves, PQ48x8 codes, 16 consecutive pages per code block, 100 nominated rows, 32 code GETs/16 MiB, and 32 data pages/16 MiB. No sweep.
- Require mean GT100 containment >=97.5%, p05 >=90%, R@10 >=96%, both wave budgets, and two-generation 100M resident-memory projection <3 GiB.
- Construct and upload source-only codes before query or truth capability. Keep producer `claim_eligible=false` until independent validation.
- Use Causality Spot, a new immutable source archive/attempt prefix, terminal artifact sync, interruption discard/restart, and immediate instance termination.

## Review Focus

- A code block crossing the 32-GET or 16-MiB bound must fail planning rather than shrink the tree shortlist.
- Duplicate, missing, or misordered stable IDs must fail code construction before sealing.
- Equal row scores and equal page nominations must use source ordinal and page ordinal respectively.
- Mutated codebooks, codes, tree, membership, evidence, or source identity must fail independent replay.
- A failed worker validation must write a failed terminal and terminate compute; controller replay must never recast it as remotely complete.

---

### Task 1: Pure code planner and row-score nomination

**Files:** Create `scripts/native_row_score_nomination.py` and `scripts/test_native_row_score_nomination.py`.

**Interfaces:** `plan_code_blocks(retained_pages, page_row_counts, block_pages=16, maximum_gets=32, maximum_bytes=16_777_216) -> CodePlan`; `nominate_pages(row_scores, row_source_ordinals, row_page_ordinals, page_byte_sizes, limits) -> tuple[int, ...]`.

- [ ] Write failing tests for a straddled code block, an over-budget plan, exact ties, top-100 page counts, and byte-cap skipping. Run `uv run --no-project --with numpy==2.4.2 --with pyarrow==24.0.0 python -m unittest scripts.test_native_row_score_nomination` and confirm the expected failures.
- [ ] Implement the planner using page-ordered 48-byte row offsets and contiguous 16-page blocks. Count complete fetched blocks, including their unretained rows, in GET and byte totals.
- [ ] Implement the fixed top-100 nomination rule with source-ordinal row ties, page-count ranking, minimum-score and page-ordinal ties, then fill remaining page slots by minimum row score.
- [ ] Run focused tests, Ruff, and `git diff --check`; commit the deterministic core.

### Task 2: Authenticated 100k cell and independent replay

**Files:** Create `scripts/native_row_score_cell.py`, `scripts/validate_native_row_score_result.py`, and focused test modules; reuse `scripts/v102_two_wave_pq48_refinement.py` for fixed PQ48 training and encoding.

**Interfaces:** Construction emits source/membership-bound codebooks and page-ordered code plane identities. Evaluation emits per-query retained pages, code blocks/GETs/bytes, exact-control and PQ48 selected pages and GT hits, data bytes, and aggregate decision. Validation reads sealed identities and independently recomputes all 1,000 routes and results.

- [ ] Write failing tests for source-only construction, canonical code-plane length/order, one changed code byte, one changed route, and one changed aggregate.
- [ ] Implement source-only PQ48 training and encoding with the frozen layout seed; seal codebooks and codes before query/truth access.
- [ ] Implement evaluator with exact-control row scores and PQ48 scores over the same retained leaves; emit both arms and the two independent wave budgets.
- [ ] Implement independent artifact authentication, reconstruction, and 1,000-query replay without invoking the producer's nomination function.
- [ ] Run focused tests and Ruff; commit the evidence slice.

### Task 3: Immutable Spot launch and decision

**Files:** Create `scripts/launch_native_row_score_spot.py` and focused launcher tests; update `docs/research/algorithm-first-page-layout-ledger.md` with terminal identities and decision.

- [ ] Write failing generated-worker tests for source readability, query/truth isolation, exact artifact roster, `PYTHONPATH` in every Python phase, failed terminal cleanup, Spot, and new-attempt client tokens.
- [ ] Implement a capped worker and controller using the repaired microcluster launcher pattern; code construction uploads before evaluate, remote validation follows evaluation, and terminal sync precedes shutdown.
- [ ] Run focused tests, Ruff, `git diff --check`, and one full repository assurance gate when the diff is stable. Check the original full Cargo test process and reuse its final result rather than launch another overlapping copy.
- [ ] Freeze and push source only after checking `origin/main` is an ancestor; check EC2 and the target S3 prefix for active work before launching one Spot cell.
- [ ] Monitor terminal markers and instance health only during the incomplete cell. After terminal, terminate compute, independently read and hash artifacts, record paired metrics, and either authorize SQ8/cold-S3 100k checks or reject the architecture with a root-cause decision.
