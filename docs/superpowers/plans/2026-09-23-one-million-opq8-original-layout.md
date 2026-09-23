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

- [ ] **Step 1: Write failing tests.** Use a 16-row two-group synthetic source and sealed model. Assert batch encoding equals `encode_opq8(vectors, np.arange(len(vectors)), model)`; assert scatter follows page physical order when parquet order differs; assert duplicate and missing IDs fail; assert a four-score mean rank and finite-score rejection.
- [ ] **Step 2: Run only `scripts/test_native_one_million_opq8.py`; confirm the new imports fail.** Use `uv run --no-project --with-requirements scripts/requirements-format-bench.txt python -m pytest -q scripts/test_native_one_million_opq8.py`.
- [ ] **Step 3: Implement bounded encoding and construction.** Stream PyArrow batches of at most 4,096 rows, use an ID-to-physical-ordinal map from authenticated base/delta page order, scatter into a `np.memmap` 8-byte plane, track a one-byte seen vector, and seal the immutable model/code/source/group authority. Reject any source cardinality or shape mismatch.
- [ ] **Step 4: Run the focused test and Ruff on only modified Python files.** Confirm the test passes and no local full suite starts.
- [ ] **Step 5: Commit the code-plane slice.** Commit only the reviewed files; preserve operator Git identity and omit AI attribution.

### Task 2: Phase-separated containment cell and independent validator

**Files:** Create `scripts/native_one_million_opq8_cell.py`, `scripts/validate_native_one_million_opq8.py`, and `scripts/test_native_one_million_opq8_cell.py`.

**Interfaces:** `run_construct(root)`, `run_plan(root,out)`, `run_evaluate(root,out)`, `run_validate(root,out)` write canonical sealed artifacts. The plan contains all 1,000 OPQ8 and source-distance diagnostic `ranked_groups`, `selected_groups`, merged `intervals`, projected GET and byte counts; evidence adds ordered GT masks and paired historical control fields.

- [ ] **Step 1: Write failing phase tests.** Assert queries cannot exist during construct, truth cannot exist during plan, tampering with model/code/seal/plan rejects downstream phases, and the control sample mismatch fails even when aggregates still match.
- [ ] **Step 2: Run only the new cell test; confirm it fails at missing entry points.** Use the same `uv run` pattern as Task 1.
- [ ] **Step 3: Implement plan and evaluate.** Authenticate the sealed code plane; score each query with `row_adc_scores`; rank top-four means; call `plan_group_ranges` with 96-byte projected `Group.code_bytes`. Make a second bounded source pass for source squared-L2 top-four diagnostic scores and seal their plans before truth. Compare each control sample with terminal-closed PQ96 evidence; resolve ordered truth IDs to original physical group owners only after the plan seal. Report selected-group count and projected bytes for each arm.
- [ ] **Step 4: Implement independent validation.** Re-encode the source in bounded batches, recompute all 1,000 ADC rankings and range plans, recompute both hit masks and metrics from source/truth, and compare exact canonical artifact bytes. Failure never claims a scientific quality miss.
- [ ] **Step 5: Run both new focused tests and scoped Ruff, then commit.** Include nonmonotonic skip and base/delta boundary fixtures.

### Task 3: Spot execution and closeout

**Files:** Modify `scripts/launch_native_one_million_selector_spot.py` and `scripts/native_hundred_thousand_opq8_worker.py`; modify `scripts/test_native_one_million_selector_spot.py`; append to `docs/research/algorithm-first-page-layout-ledger.md` after terminal.

**Interfaces:** Add one distinct `opq8_1m` selector kind with its own prefix, terminal schema, source/query/truth downloads and artifact roster. Reuse the controller's create-only reservation, terminal readback and guaranteed EC2 termination.

- [ ] **Step 1: Write failing launcher tests.** Validate the exact 1M prefix/schema, download phase order, 100k model identity, immutable artifact roster, Spot reservation and terminal parse.
- [ ] **Step 2: Run only launcher tests; confirm the new kind is rejected.** Use the existing focused test module.
- [ ] **Step 3: Add the worker/controller mode.** Freeze the source archive and script before launch; resource receipts cover construct, plan, evaluate and validation. Keep the original 100k worker behavior unchanged.
- [ ] **Step 4: Run focused tests, source archive checks and a read-only cross-provider review.** Repair findings before any launch. Do not run a local full suite while swap remains charged.
- [ ] **Step 5: Commit and fast-forward push, then launch exactly one Spot attempt.** Monitor only terminal and infrastructure until closure. Read back and authenticate all terminal artifacts, recompute all 1,000 results, confirm instance termination, record the pass/fail decision and push the ledger.

## Self-review

The tasks cover the source-only scope, phase boundaries, strongest historical control, resource policy and exact closeout. Actual 1M code reads, serving latency and 100M runtime remain separate gates by design. The plan assumes the already authorized native execution path in this active production goal.
