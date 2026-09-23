# One Million OPQ8 Data-Range Selector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decide whether query-only OPQ8 scores can select final data ranges that meet the fixed 1M page-containment gate within 32 GETs and 16 MiB.

**Architecture:** Reuse authenticated OPQ8 codes, page maps and frozen group plans. A pure interval planner computes the minimum-byte cover for any target page set; a phase-separated cell seals score-driven plans before truth evaluation. A Spot worker publishes an immutable terminal and the independent closeout recomputes all masks and budgets.

**Tech Stack:** Python 3.12, NumPy, PyArrow, pytest, pinned Lean 4.33.0, AWS Causality Spot/S3.

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-opq8-data-range-selector-design.md`

## Global Constraints

- Preserve the frozen 1M source, plan, model, code and cohort identities.
- Query-only construction and planning precede any truth access.
- Every arm and query must use at most 32 role-preserving ranges and 16,777,216 encoded bytes.
- One create-only Causality Spot attempt at a time; terminal-close and terminate immediately.
- Local swap pressure rules out a full local suite until pressure recovers.

## Review Focus

- Gap ties must resolve deterministically by role and physical ordinal.
- Target pages in different objects must never merge into one range.
- Bridged pages count toward I/O bytes and GT containment.
- Malformed page lengths, ordinals, source digests and nonfinite scores fail closed.
- Truth must be inaccessible before both arm plans are sealed.

---

### Task 1: Pure minimum-byte interval cover

**Files:** Create `scripts/native_one_million_data_ranges.py`; create `scripts/test_native_one_million_data_ranges.py`.

**Interfaces:** `minimum_cover(page_lengths, target_pages, maximum_gets)` returns role, inclusive start, exclusive end intervals and exact bytes. `admit_ranked_pages(page_lengths, ranked_pages, maximum_gets, maximum_bytes)` scans priorities and skips nonfitting targets.

- [x] Write focused tests for independent gap costs, deterministic ties, role boundaries, bridged byte accounting, skips, and invalid pages.
- [x] Run only the focused pytest target; confirm initial import failure.
- [x] Implement the minimum-byte cover and admission with exact integer accounting.
- [x] Run the same focused tests and scoped Ruff check.
- [ ] Commit the independently reviewable planner.

### Task 2: Phase-separated 1M source-only cell

**Files:** Create `scripts/native_one_million_data_range_cell.py` and `scripts/validate_native_one_million_data_range_cell.py`; create focused tests.

**Interfaces:** `construct` authenticates old source and page/code maps, `plan` reads queries and emits both sealed arm plans, `evaluate` reads truth only after plan seals and writes masks/metrics, `validate` independently rebuilds page maps and recomputes plans and results.

- [ ] Add a synthetic fixture with base/delta roles, selected groups, OPQ8 scores and known truth owner pages.
- [ ] Run its test red, implement phase boundaries and deterministic ranking, then run focused tests green.
- [ ] Test malformed identities, nonfinite scores, missing plan seals, bridging, GET/byte overflow and historical-plan mismatch.
- [ ] Add explicit resource and phase receipts; check no source-only result claims actual S3 data reads.
- [ ] Commit the cell and validator after scoped lint and focused tests.

### Task 3: Spot execution and independent closeout

**Files:** Create `scripts/native_one_million_data_range_worker.py`; adapt `scripts/launch_native_one_million_selector_spot.py`; create focused launcher tests; update `docs/research/algorithm-first-page-layout-ledger.md` only after closure.

- [ ] Add create-only source/terminal upload, readback authentication, Causality Spot default, zero-swap/3-GiB resource gate and terminal-driven shutdown.
- [ ] Test the worker and launcher using a tiny synthetic fixture; run scoped Ruff.
- [ ] Commit and fast-forward push to `origin/main` before creating the archive.
- [ ] Check durable consultation and instance lists for an existing attempt; launch only if none is active.
- [ ] Monitor terminal and instance health without opening incomplete measurement files.
- [ ] Authenticate terminal-listed artifacts, independently recompute all page hit masks and budget counts, record the decision, and stop compute.
