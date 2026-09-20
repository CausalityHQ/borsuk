# V99 Ranked-Gap Range Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decide G1 row width on all 1,000 ReLAION-1M development queries by replacing V98's impossible one-page-per-GET planner with deterministic adjacent same-object range GETs.

**Architecture:** Reuse V98's authenticated hierarchy, retained rows, exact scorer, PQ arms, and memory worksheet. A new V99 module converts ranked row pages into at most 32 nonoverlapping byte intervals by greedily merging the cheapest adjacent same-object gap, and a separate hostile reducer recomputes every range, metric, gate, projection, and winner before one immutable Spot result is ledgered.

**Tech Stack:** Python 3.12, frozen dataclasses, NumPy 1.26.4, PyArrow 17.0.0, unittest, boto3, Bash, Causality AWS Spot, S3.

**Spec:** `docs/superpowers/specs/2026-09-20-v99-ranked-gap-range-router-design.md`

## Global Constraints

- Use all 1,000 frozen ReLAION-1M development queries; never read validation or sealed holdout queries.
- Keep 8-page roots, 4,096 exposed pages, 1,024 retained pages, 262,144 scanned rows, 8,192 shortlisted rows, 32 GETs, and 16,777,216 bytes exact.
- Use query-independent training only; exact-f32 must pass before PQ16, PQ24, PQ32, PQ32x4, or summary-only executes.
- Typed dataclasses own JSON; large arrays remain Arrow/Parquet. No untyped `json!`-style construction.
- No production Rust, benchmark-manifest serving fallback, 10M, 100M, G2, or parity claim before a winner.
- Heavy science runs once on Causality AWS Spot; local work is narrow and stops if RSS exceeds 3 GiB or PSI full avg10 exceeds 0.5.
- Reuse critique SHA-256 `edcde2149f98bc38c5121b387b165fe9baf0ca6df79658ac624abf86866bcb87`; do not launch another G1 critique.

## Review Focus

- A range at an object tail includes one page, never crosses into the other object, and uses the exact last-page length.
- Rejecting an over-budget ranked page must not prevent a later page already inside or cheaply adjacent to an accepted interval.
- Cheapest-gap ties must be stable across Python/hash iteration order and preserve rank evidence.
- Selected pages must equal the complete interval union, including unranked gap pages; GET count is interval count, not page count.
- The reducer must reject producer range, byte, aggregate, classification, projection, and winner drift without importing producer helpers.

---

### Task 1: Typed ranked-gap planner

**Files:**
- Create: `scripts/v99_ranked_gap_range_router.py`
- Create: `scripts/test_v99_ranked_gap_range_router.py`

**Interfaces:**
- Consumes: V97 `PageKey`, `RoutedPage`; ranked `PageKey` values.
- Produces: `PageRange`, `RangeSelection`, `select_ranked_gap_ranges(ranked_pages, pages, max_gets, max_bytes)`.

- [ ] **Step 1: Write planner RED tests**

Add `V99RangePlannerTests` with literal page offsets/lengths. Tests require:

```python
selection = select_ranked_gap_ranges(
    ranked_pages, pages, max_gets=2, max_bytes=24
)
self.assertEqual(selection.ranges, expected_ranges)
self.assertEqual(selection.pages, expected_complete_union)
self.assertEqual(selection.gets, len(expected_ranges))
self.assertEqual(selection.bytes, sum(item.bytes for item in expected_ranges))
```

Cover same-role isolation, cheapest-gap and tie order, partial tail, duplicate
ranked pages, a rejected expensive candidate followed by a cheap candidate,
the 32/16-MiB boundaries, overlap/order mutations, and equality with a literal
scalar planner.

- [ ] **Step 2: Run RED**

```bash
uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_v99_ranked_gap_range_router.V99RangePlannerTests
```

Expected: import failure for the three missing V99 symbols.

- [ ] **Step 3: Implement the minimal planner**

Use frozen slots dataclasses:

```python
@dataclass(frozen=True, slots=True, order=True)
class PageRange:
    object_role: str
    first_page: int
    last_page: int
    offset: int
    bytes: int

@dataclass(frozen=True, slots=True)
class RangeSelection:
    ranges: tuple[PageRange, ...]
    pages: tuple[PageKey, ...]
    gets: int
    bytes: int
```

Validate page registration and contiguity, insert singleton candidates, merge
the least extra-byte adjacent same-role pair with deterministic ties, revert
only the current candidate when a cap fails, and return the exact interval
union.

- [ ] **Step 4: Run GREEN and static checks**

Run the Task-1 command, scoped Ruff, py_compile, and `git diff --check`.
Expected: all planner tests pass with no lint or syntax finding.

- [ ] **Step 5: Commit**

```bash
git add scripts/v99_ranked_gap_range_router.py scripts/test_v99_ranked_gap_range_router.py
git commit -m "research: add V99 ranked-gap planner"
```

### Task 2: Exact ceiling and width evidence

**Files:**
- Modify: `scripts/v99_ranked_gap_range_router.py`
- Modify: `scripts/test_v99_ranked_gap_range_router.py`

**Interfaces:**
- Consumes: V98 hierarchy/scoring APIs and Task-1 range planner.
- Produces: `RangeSample`, `V99Result`, `evaluate_v99(inputs, authority, config)`, `canonical_v99_result_bytes(result)`.

- [ ] **Step 1: Write evaluation RED tests**

Add `V99EvaluationTests`. Literal fixtures must prove containment stop, exact
range stop, exact pass followed by all five arms, 1,000 unique query ordinals,
identical hierarchy/retained rows across stages, 8,192 shortlist authority,
range GET/byte accounting, complete 100M projections, paired intervals, winner,
and canonical `borsuk-v99-ranked-gap-range-router-v1` bytes.

- [ ] **Step 2: Run RED**

```bash
uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_v99_ranked_gap_range_router.V99EvaluationTests
```

Expected: missing V99 result/evaluator symbols.

- [ ] **Step 3: Implement typed evaluation**

Reuse V98's authority, hierarchy, exact/PQ scorers, and projection arithmetic.
Define `RangeSample` with truth IDs/pages, ordered `PageRange` values, complete
selected page union, hit IDs, recall, GETs, bytes, and root/page/row counts.
Run exact-f32 first; only an exact pass fits/evaluates PQ16, PQ24, PQ32,
PQ32x4, and summary-only through the identical Task-1 planner.

- [ ] **Step 4: Run GREEN and affected V98 regressions**

```bash
uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_v99_ranked_gap_range_router \
    scripts.test_v98_hierarchical_row_router
```

Expected: V99 and unchanged V98 tests pass.

- [ ] **Step 5: Commit**

```bash
git add scripts/v99_ranked_gap_range_router.py scripts/test_v99_ranked_gap_range_router.py
git commit -m "research: add V99 range evaluation"
```

### Task 3: Independent hostile reducer

**Files:**
- Create: `scripts/v99_ranked_gap_range_router_rescore.py`
- Create: `scripts/test_v99_ranked_gap_range_router_rescore.py`

**Interfaces:**
- Consumes: canonical V99 result path, registered SHA-256, expected V97 authority, expected V99 configuration.
- Produces: `validate_v99_result`, `rescore_v99_result`, `canonical_v99_rescore_bytes`.

- [ ] **Step 1: Write reducer RED and mutation tests**

Require independent recomputation of every sample hit/recall, complete interval
union, exact range offset/bytes, aggregates, quality/resource gates, bootstrap
matrix, paired intervals, memory terms, eligibility, classification, and
winner. Mutate every immutable identity, range field, cap, sample field,
aggregate, projection term, interval, and decision in isolation.

- [ ] **Step 2: Run RED**

```bash
uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_v99_ranked_gap_range_router_rescore
```

Expected: missing reducer symbols.

- [ ] **Step 3: Implement reducer without producer imports**

Import only shared authority dataclasses and `HierarchyConfig`. Parse concrete
types/schema manually, derive range unions and byte spans from result-bound
page metadata, reproduce the seed-7216 matrix locally, and emit canonical
`borsuk-v99-ranked-gap-range-router-rescore-v1` bytes.

- [ ] **Step 4: Run GREEN and combined gate**

Run both V99 test files, scoped Ruff, pycompile, docs validation, and
`git diff --check`. Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add scripts/v99_ranked_gap_range_router_rescore.py \
  scripts/test_v99_ranked_gap_range_router_rescore.py
git commit -m "research: add V99 hostile reducer"
```

### Task 4: Single-attempt Spot boundary

**Files:**
- Create: `scripts/v99_ranked_gap_range_router_run_remote.sh`
- Create: `scripts/launch_v99_ranked_gap_range_router_spot.py`
- Create: `scripts/test_launch_v99_ranked_gap_range_router_spot.py`

**Interfaces:**
- Consumes: clean source archive, six frozen objects, V99 evaluator/reducer, Causality AWS profile.
- Produces: create-only reservation/launch receipts and terminal-bound result, rescore, resources, and log identities.

- [ ] **Step 1: Write launcher RED tests**

Copy no V98 implementation. Test a thin typed V99 plan that reuses only shared
Spot primitives, binds the V99 schema/config, generates user data below 16,384
bytes, downloads/authenticates six inputs, executes producer then reducer,
publishes terminal last, enforces working PSI/swap/wall stops, launches one
c7i.8xlarge Spot instance serially across registered zones, and terminates on
every path. Add the executable awk 0.00/0.51 regression from V98.

- [ ] **Step 2: Run RED**

```bash
uv run --offline --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_launch_v99_ranked_gap_range_router_spot
```

Expected: missing V99 launch boundary.

- [ ] **Step 3: Implement the minimal boundary**

Use frozen V99 dataclasses and factor only genuinely campaign-neutral S3/EC2
helpers from V98. The runner uses typed producer/reducer APIs, verifies exact
length/SHA-256 before semantic use, uploads immutable evidence, then terminal,
and never reads partial measurement output while active.

- [ ] **Step 4: Run full local assurance once**

Run the three V99 test modules, V98 regression modules, scoped Ruff,
pycompile, shell syntax, docs validation, and `git diff --check`. Expected: all
green without a full repository suite.

- [ ] **Step 5: Commit and push**

Commit the verified V99 harness, fetch `origin/main`, require it is an ancestor
of HEAD, and push `HEAD:main` fast-forward.

### Task 5: Execute and ledger the sole V99 cell

**Files:**
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: clean verified V99 source and frozen inputs.
- Produces: one authenticated G1 winner or fail-fast rejection.

- [ ] **Step 1: Preregister exact metadata**

Record source/archive identities, never-reused prefix, inputs, configuration,
gates, stop rules, instance/AZ order, expected spend, and no-rerun rule. Validate
docs and push the preregistration before launch.

- [ ] **Step 2: Launch exactly one Spot attempt**

Run the V99 launcher once with AWS profile `causality`. Preserve its original
session; observe only terminal marker and EC2 health; terminate immediately at
terminal or registered stop.

- [ ] **Step 3: Authenticate terminal evidence**

Download terminal first into one explicit mktemp. Only if complete, download
its named evidence, verify exact length/hash, rerun the current-source reducer,
require byte-identical rescore, verify instance termination, unlink named files,
and remove scratch.

- [ ] **Step 4: Record the decision**

Ledger dataset/split, every arm and paired CI, ranges/GETs/bytes/work,
projection, winner/rejection, instance/region/AZ, elapsed/RSS/PSI/swap/cost,
artifact identities, competitor basis, and the exact G2 or next-G1 action.
Run docs validation and `git diff --check`.

- [ ] **Step 5: Commit and push the ledger**

Commit only the validated ledger, fast-forward push to `origin/main`, and mark
Task 5 complete. No automatic retry, 10M, 100M, or production change follows.
