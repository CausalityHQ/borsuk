# One Million OPQ8 Original Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce one terminal-closed, paired 1M containment decision for the fixed 100k-trained OPQ8 route.

**Architecture:** Stream the frozen 1M source into an eight-byte physical code plane under a 3-GiB process-tree cap. Seal source artifacts before queries, seal all query plans before truth, and compare the OPQ8 group route with the historical PQ96 projection using the same physical groups and projected 96-byte range planner. Run one Spot attempt and independently close out its evidence.

**Tech Stack:** Python 3.12, NumPy, PyArrow, pytest, AWS CLI/S3/EC2, existing Spot controller.

**Spec:** `docs/superpowers/specs/2026-09-23-one-million-opq8-original-layout-design.md`

## Global Constraints

- Use the seven identities in `scripts/native_one_million_selector_cell.py` and the sealed 100k model at `87236c5765186fb8ab977ef86f87a6b1047effb1`.
- Preserve 7,278 pages, 910 original eight-page groups, 1,000 development queries, top-four ADC group scoring, 96-byte projected rows, at most 32 merged ranges and 16,777,216 bytes.
- The 1M source stage must not admit queries or truth; plan stage may admit queries but not truth; evaluation admits truth only after plan seal.
- Use one Causality Spot attempt, create-only/readback S3 artifacts, 3-GiB RSS cap with 64-MiB allowance, zero swap, terminal-only incomplete monitoring, and immediate termination after terminal.

## Review Focus

- Duplicate or missing source IDs must fail construction before a seal; test a repeated stable ID and a missing physical position.
- A base-to-delta boundary must never merge into one GET; test adjacent ordinals across roles.
- A skipped over-budget group must not stop consideration of later fitting groups; test the nonmonotonic case.
- Model/code/source mismatches must fail before queries; tamper one byte of each fixture.
- A matching aggregate with a changed historical per-query row must fail replay; mutate one control sample without changing totals.

---

### Task 1: Streaming code plane and group ranking

**Files:** Create `scripts/native_one_million_opq8.py`; modify `scripts/native_hundred_thousand_opq8_router.py`; create `scripts/test_native_one_million_opq8.py`.

**Interfaces:** `encode_opq8_rows(vectors: np.ndarray, model: Opq8Model) -> np.ndarray` encodes one bounded batch in source order. `build_1m_opq8(root: Path, out: Path) -> dict` authenticates the model/source and emits `codes.bin` and `seal.json`. `rank_group_scores(scores: np.ndarray, groups: Sequence[Group]) -> tuple[int,...]` yields top-four-mean order with role/ordinal ties.

- [x] **Step 1: Write failing tests.** The existing 10-row reversed source fixture exercises physical scattering, batch equality, missing/duplicate IDs, finite scores and top-four order. Ruling: reuse that fixture instead of creating a second synthetic page format; risk is lower fixture diversity.
- [x] **Step 2: Confirm new imports fail.** Focused `unittest` first failed at missing `encode_opq8_rows`, then missing `rank_opq8_row_groups`, then missing `native_one_million_opq8`.
- [x] **Step 3: Implement bounded encoding and construction.** PyArrow batches at most 4,096 rows scatter into a memory-mapped eight-byte plane, with exact source/model/page authentication and seen-position tracking.
- [x] **Step 4: Run focused tests and Ruff.** Five focused tests passed; scoped Ruff passed; no full local suite started.
- [x] **Step 5: Commit the code-plane slice.** The implementation and this checkpoint commit together; operator identity and attribution policy apply.

### Task 2: Phase-separated containment cell and independent validator

**Files:** Create `scripts/native_one_million_opq8_cell.py` and `scripts/test_native_one_million_opq8_cell.py`. Ruling: the replay validator stays in the cell because it reuses its phase entry points in a fresh staging directory; cost if wrong is weaker independence against a shared implementation defect, addressed by the final adversarial review and separate terminal closeout.

**Interfaces:** `run_construct(root)`, `run_plan(root,out)`, `run_evaluate(root,out)`, `run_validate(root,out)` write canonical sealed artifacts. The plan contains all 1,000 OPQ8 and source-distance diagnostic `ranked_groups`, `selected_groups`, merged `intervals`, projected GET and byte counts; evidence adds ordered GT masks and paired historical control fields.

- [x] **Step 1: Write failing phase tests.** The source fixture checks truth exclusion, model/code/plan tampering, control mismatch, phase order and fresh-stage replay; existing range-selector tests cover nonmonotonic skip and base/delta coalescing.
- [x] **Step 2: Confirm missing entry points fail.** The focused cell test failed at the missing module, then the missing `run_validate` entry point.
- [x] **Step 3: Implement plan and evaluate.** The query-only phase seals candidate, source-distance diagnostic and historical control plans before truth. Evaluation recomputes ordered GT masks, exact control fields and fixed thresholds.
- [x] **Step 4: Rebuild validation.** A fresh staging directory reconstructs the source code plane, reruns all query plans and truth outcomes, and compares every canonical artifact byte. This is a deterministic replay, not an independent algorithm proof.
- [x] **Step 5: Run focused tests and Ruff, then commit.** Three new focused tests passed and scoped Ruff passed; no local full suite started.

### Task 3: Spot execution and closeout

**Files:** Modify `scripts/launch_native_one_million_selector_spot.py` and `scripts/native_hundred_thousand_opq8_worker.py`; modify `scripts/test_native_one_million_selector_spot.py`; append to `docs/research/algorithm-first-page-layout-ledger.md` after terminal.

**Interfaces:** Add one distinct `opq8_1m` selector kind with its own prefix, terminal schema, source/query/truth downloads and artifact roster. Reuse the controller's create-only reservation, terminal readback and guaranteed EC2 termination.

- [x] **Step 1: Write failing launcher tests.** The focused test pins 1M prefix/schema, 13 artifacts, model/control downloads, source/plan phase order, 14,400-second CLI deadline, Spot specs and shell syntax.
- [x] **Step 2: Confirm the new kind is rejected.** The new focused test first failed at `build_plan` rejecting `opq8_1m`.
- [x] **Step 3: Add the worker/controller mode.** The worker shares create-only terminal, readback, resource receipts and termination with the 100k worker; the old 100k focused script test still passes.
- [x] **Step 4: Run focused tests and read-only cross-provider review.** Fifteen focused tests, scoped Ruff and diff checks passed. The reviewer found no Critical/High blocker; the preregistration now defines Spot interruption restart, the 1% material-expansion threshold and diagnostic verdict. A closed PQ96 query-0 preflight replayed exactly. Source archive checks follow after this reviewed revision is committed. No local full suite ran while swap remained charged.
- [x] **Step 5: Commit and fast-forward push, then launch exactly one Spot attempt.** Monitor only terminal and infrastructure until closure. Read back and authenticate all terminal artifacts, recompute all 1,000 results, confirm instance termination, record the pass/fail decision and push the ledger.

## Self-review

The tasks cover the source-only scope, phase boundaries, strongest historical control, resource policy and exact closeout. Actual 1M code reads, serving latency and 100M runtime remain separate gates by design. The plan assumes the already authorized native execution path in this active production goal.
