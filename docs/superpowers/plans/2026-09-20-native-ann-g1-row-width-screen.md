# Native ANN G1 Row-Width Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce one independently validated, immutable 1M development cell that selects the G2 routing bytes/row without changing production code.

**Architecture:** A new V97 research harness reuses V85's authenticated dataset/page loading but reproduces the bounded native two-stage route: one shared two-summary 1,024-page fence, row scoring within that fence, and at most 32 one-page GETs. V104/V105 independently killed smaller retained capacities in the V99 hierarchy and retained 1,024 as the qualified capacity boundary; they did not authenticate this distinct two-summary ranker, so its fresh exact-f32 diagnostic remains a mandatory fail-fast gate. The harness implements generic 8-bit and packed 4-bit PQ arms plus a summary-only control. A separate rescorer treats the result as untrusted and recomputes samples, aggregates, paired confidence intervals, memory eligibility, and the winner.

**Tech Stack:** Python 3.12, NumPy, PyArrow/Parquet/Arrow IPC, boto3 launch wrapper, canonical JSON evidence.

**Spec:** `docs/superpowers/specs/2026-09-20-native-ann-g1-row-width-screen-design.md`

## Global Constraints

- Production Rust is frozen throughout G1.
- Use all 1,000 ReLAION-1M development queries under exactly 32 GET / 16 MiB.
- Training is source-only; validation and holdout artifacts are forbidden.
- Heavy execution uses one Causality AWS Spot attempt; local runs are narrow tests only.
- The final result binds the single G1 dual-critique result hash.
- Rust evidence types use Serde structs; `serde_json::json!` is forbidden in native ANN code.

## Review Focus

- Packed PQ32x4 nibble order or scorer drift must fail differential tests.
- Every arm must use the identical page map, planner, budget and query order.
- Base and delta must both be routed/charged; no truth hit is resident for free.
- A malformed sample must not survive aggregate-only validation.
- Resident projections must include both codebooks and summary codes.
- Summary-only must remain query-blind and must not silently use a distinct planner.
- No global row-code scan or dense traceback planner may enter serving-eligible evidence.

---

### Task 1: Generic arm and memory contracts

**Files:**
- Create: `scripts/v97_row_width_screen.py`
- Create: `scripts/test_v97_row_width_screen.py`

**Interfaces:**
- Produces: `PqSpec`, `pack_pq4`, `unpack_pq4`, `score_packed_pq4`, and `project_resident_bytes_100m`.

- [ ] **Step 1: Write the failing contract tests**

```python
def test_pq32x4_pack_round_trip_and_scores_match_unpacked_reference():
    codes = np.arange(64, dtype=np.uint8).reshape(2, 32) % 16
    packed = pack_pq4(codes)
    assert packed.shape == (2, 16)
    assert np.array_equal(unpack_pq4(packed, 32), codes)
    assert np.array_equal(
        score_packed_pq4(packed, tables), score_unpacked(codes, tables)
    )

def test_resident_projection_recomputes_every_term_and_rejects_over_budget():
    assert project_resident_bytes_100m(PQ16X8).eligible
    assert project_resident_bytes_100m(PQ32X4).eligible
    assert not project_resident_bytes_100m(PQ24X8).eligible
    assert not project_resident_bytes_100m(PQ32X8).eligible
```

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest scripts.test_v97_row_width_screen.RowWidthContractTests`

Expected: import failure for the missing V97 contract.

- [ ] **Step 3: Implement only the typed arm, packing, scoring, and worksheet helpers**

Use checked integer arithmetic and reject wrong shape, values above 15, odd
subspace count, nonfinite tables, or totals at/above `3 * 1024**3`.

- [ ] **Step 4: Verify GREEN**

Run: `python3 -m unittest scripts.test_v97_row_width_screen.RowWidthContractTests`

Expected: all contract tests pass.

### Task 2: Shared-planner five-arm evaluation

**Files:**
- Modify: `scripts/v97_row_width_screen.py`
- Modify: `scripts/test_v97_row_width_screen.py`

**Interfaces:**
- Consumes: V85 authenticated readers, not its free-delta evaluator or dense planner.
- Produces: `evaluate_width_arms(inputs: ScreenInputs) -> ScreenResult`.

- [ ] **Step 1: Add synthetic RED tests**

Construct one query-blind base/delta page map where all five arms have known
rankings. Assert the shared 1,024-page fence, exact shortlist-boundary ties,
identical planner budgets, literal per-query hit IDs derived from selected-page
membership, and a summary-only arm built from exactly two contiguous means per
page. Leave one delta truth row on an unfetched page and assert it is a miss.
Mutate an arm's page map and assert rejection before scoring.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest scripts.test_v97_row_width_screen.RowWidthEvaluationTests`

Expected: missing evaluator failure.

- [ ] **Step 3: Implement sequential bounded arm training and scoring**

First rerun the exact-f32 ceiling behind the shared summary fence with both
tiers charged. Then train one arm at a time, write its canonical codebook/code
identity, retain
only its 1,000-query result before moving to the next arm, and never allocate a
query-by-million-row matrix. Score in row blocks with deterministic total order.

- [ ] **Step 4: Verify GREEN**

Run the same test class; expected all tests pass with no warnings.

### Task 3: Independent rescorer and winner

**Files:**
- Create: `scripts/v97_row_width_rescore.py`
- Create: `scripts/test_v97_row_width_rescore.py`

**Interfaces:**
- Consumes: canonical V97 result bytes plus expected result SHA-256.
- Produces: canonical rescore summary with paired 10,000-draw CIs and winner.

- [ ] **Step 1: Add RED mutation tests**

Mutate each sample field, aggregate, p05, budget, arm identity, worksheet term,
CI seed/count, query ordinal, and winner. Each mutation must reach and fail its
own semantic gate.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest scripts.test_v97_row_width_rescore`

Expected: missing validator failure.

- [ ] **Step 3: Implement independent recomputation**

Parse no producer helper output. Recompute hits from literal truth/hit IDs,
nearest-rank p05, maxima, point gates, all worksheet terms, paired bootstrap
draws, dominance, eligibility, and the deterministic winner.

- [ ] **Step 4: Verify GREEN**

Run the same test module; expected all mutation families pass.

### Task 4: One-attempt Spot runner

**Files:**
- Create: `scripts/v97_row_width_screen_run_remote.sh`
- Create: `scripts/launch_v97_row_width_screen_spot.py`
- Create: `scripts/test_launch_v97_row_width_screen_spot.py`

**Interfaces:**
- Consumes: frozen V85 source/query/truth/generation/base/delta identities and commit archive.
- Produces: immutable result, independent summary, terminal receipt, logs, and instance identity.

- [ ] **Step 1: Add RED launcher tests**

Assert profile `causality`, Spot market, one attempt, exact seven input
identities, all 1,000 queries, sequential arms, 10,000 resamples, source commit,
critique hash, terminal-before-shutdown, interruption discard, and no validation
or holdout URI.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest scripts.test_launch_v97_row_width_screen_spot`

Expected: missing launcher/runner failure.

- [ ] **Step 3: Implement the minimal launcher and remote runner**

The instance downloads only registered inputs, runs the producer once and
rescorer once, uploads canonical evidence, records pressure/RSS/wall/spend, and
terminates. No scientific retry occurs inside the cell.

- [ ] **Step 4: Run the complete local assurance gate**

Run: `python3 -m unittest scripts.test_v97_row_width_screen scripts.test_v97_row_width_rescore scripts.test_launch_v97_row_width_screen_spot`

Then: pinned Ruff on the six V97 files, `python3 -m py_compile` on them, `python3 scripts/validate_research_docs.py`, and `git diff --check`.

### Task 5: Freeze and execute G1

**Files:**
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: verified V97 commit and completed dual-critique result/hash.
- Produces: G1 gate receipt and G2 winner/fence.

- [ ] **Step 1: Reconcile the dual critique**

Record each critic separately, independently verify actionable findings, and
hash the canonical combined critique result. Apply only corrections needed
before execution; do not start another review.

- [ ] **Step 2: Commit and push the verified harness fast-forward**

Verify `origin/main` is an ancestor, push once, and record the full commit.

- [ ] **Step 3: Launch exactly one Spot attempt and monitor its terminal**

Do not inspect partial scientific output. On interruption, record the failed
attempt and stop; any replacement requires a separately preregistered attempt.

- [ ] **Step 4: Independently authenticate and ledger the terminal result**

Record dataset/split, every measured arm, CIs, worksheet, spend, instance,
artifacts, winner or blocker, competitor basis as unavailable/not measured at
G1, and the exact next G2 action.
