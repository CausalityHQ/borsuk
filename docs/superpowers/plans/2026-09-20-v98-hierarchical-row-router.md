# V98 Hierarchical Row-Score Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and independently validate a fail-fast ReLAION-1M development screen that replaces V97's failed flat summary fence with a bounded hierarchy and selects the smallest qualifying row-code width.

**Architecture:** New V98 research modules preserve V97 as immutable historical evidence. A typed producer authenticates the frozen generation, builds an eight-page/root hierarchy, applies containment and exact-f32 ceilings, and only then evaluates five fixed row-width arms. A separate reducer treats producer bytes as hostile and recomputes all samples, gates, paired intervals, memory terms, and the winner; a one-attempt Spot wrapper executes the frozen screen.

**Tech Stack:** Python 3.12, frozen dataclasses, NumPy, PyArrow/Arrow IPC/Parquet, boto3, canonical JSON evidence, unittest, Ruff.

**Spec:** `docs/superpowers/specs/2026-09-20-v98-hierarchical-row-router-design.md`

## Global Constraints

- Preserve V97 files and artifacts unchanged; V98 uses schema `borsuk-v98-hierarchical-row-router-v1` with no compatibility reader.
- Use the frozen ReLAION-1M source, all 1,000 development queries, exact top-100 truth, and the same native generation/base/delta/page map identities as V97.
- Fix eight same-role pages per root, at most 4,096 exposed pages, 1,024 retained pages, 262,144 scanned rows, and 2,048 shortlisted rows.
- Enforce at most 32 physical GETs and 16 MiB of encoded page bytes per query.
- Require average Recall@10 >= 960,000 ppm, average Recall@100 >= 975,000 ppm, and p05 Recall@100 >= 900,000 ppm.
- Use exactly 10,000 paired bootstrap resamples with one registered seed and index matrix.
- Use frozen typed dataclasses for evidence. Canonical JSON is a wire encoding only; do not construct untyped producer evidence incrementally.
- Use Arrow IPC or Parquet for large cross-language hierarchy/vector artifacts with exact non-null schemas.
- Production Rust remains frozen until G1 selects a winner.
- Heavy science runs once on Causality AWS Spot from a clean commit; local work is narrow and stops or moves if RSS exceeds 3 GiB or PSI full avg10 exceeds 0.5.
- Reuse the completed G1 dual-critique hash; do not launch another G1 consultation.

## Review Focus

- A partial final root or base/delta role boundary must not expose more than 4,096 child pages or mix object roles.
- Truth containment, exact-f32 scoring, and every compressed arm must use the identical retained-page set and page planner.
- Duplicate, stale, tombstoned, or superseded rows must not enter hierarchy summaries or row codes.
- Blockwise top-k must exactly match scalar full sort for ties, non-contiguous IDs, partial blocks, packed nibbles, and fewer-than-k inputs.
- The reducer must derive every gate and memory term without importing producer computations or trusting producer aggregates.

---

### Task 1: Typed hierarchy authority and layout

**Files:**
- Create: `scripts/v98_hierarchical_row_router.py`
- Create: `scripts/test_v98_hierarchical_row_router.py`

**Interfaces:**
- Consumes: V97's authenticated `ObjectIdentity`, `PageKey`, `RoutedPage`, `ScreenAuthority`, and input loader contracts without accepting V97 result bytes.
- Produces: `HierarchyConfig`, `HierarchyArtifact`, `RootGroup`, `build_hierarchy(inputs, config)`, and `validate_hierarchy(artifact, inputs, config)`.

- [ ] **Step 1: Write the failing authority/layout tests**

Add `V98HierarchyAuthorityTests` with literal fixtures. Cover:

```python
config = HierarchyConfig(
    pages_per_root=8,
    maximum_root_groups=65_536,
    maximum_exposed_pages=4_096,
    retained_pages=1_024,
    maximum_scanned_rows=262_144,
    shortlist_rows=2_048,
    maximum_gets=32,
    maximum_bytes=16 * 1024**2,
)
artifact = build_hierarchy(inputs, config)
self.assertEqual(
    tuple(group.role for group in artifact.roots),
    ("base", "base", "delta", "delta"),
)
self.assertLessEqual(max(len(group.pages) for group in artifact.roots), 8)
self.assertEqual(artifact.page_summary_codes.shape[1], 16)
self.assertEqual(artifact.root_summary_codes.shape[1], 16)
```

Use nine base pages plus nine delta pages so each role has one full and one
partial root. Assert page halves, singleton duplication, root halves, exact
Arrow schemas, deterministic role/ordinal order, codebook/code identities, and
round-trip bytes. Mutation cases change each cap, group role, child ordinal,
offset/count, code length, schema nullability, digest, and visible-row roster.

- [ ] **Step 2: Run the authority/layout RED**

Run:

```bash
python3 -m unittest scripts.test_v98_hierarchical_row_router.V98HierarchyAuthorityTests
```

Expected: import failure for the missing V98 hierarchy types/functions.

- [ ] **Step 3: Implement the minimal typed hierarchy**

Define frozen `slots=True` dataclasses. Import only stable V97 data/loading and
PQ primitives; do not import V97 result production. Split pages independently
per object role, produce two contiguous-half means for every page/root, train
the shared PQ16 summary codebook from base page-summary vectors only, encode all
page/root means, and serialize hierarchy arrays through an exact Arrow IPC
schema. Validate all counts with checked arithmetic before allocating.

- [ ] **Step 4: Run the authority/layout GREEN**

Run the same unittest class. Expected: all tests pass with no warnings.

- [ ] **Step 5: Commit the hierarchy contract**

```bash
git add scripts/v98_hierarchical_row_router.py scripts/test_v98_hierarchical_row_router.py
git commit -m "research: define V98 hierarchy authority"
```

### Task 2: Deterministic bounded routing and row scoring

**Files:**
- Modify: `scripts/v98_hierarchical_row_router.py`
- Modify: `scripts/test_v98_hierarchical_row_router.py`

**Interfaces:**
- Consumes: `HierarchyArtifact`, one float32 query, visible row vectors/codes, page membership, and `HierarchyConfig`.
- Produces: `HierarchyFence`, `route_hierarchy(query, artifact, config)`, `blockwise_top_rows(scores, ids, k)`, and `score_retained_rows(...)`.

- [ ] **Step 1: Write routing and differential RED tests**

Add `V98BoundedRoutingTests`. Pin literal root/page distances and verify:

```python
fence = route_hierarchy(query, artifact, config)
self.assertLessEqual(len(fence.exposed_pages), 4_096)
self.assertLessEqual(len(fence.retained_pages), 1_024)
self.assertLessEqual(fence.scanned_rows, 262_144)
self.assertEqual(fence.retained_pages, expected_total_order)
```

Exercise exactly 4,096 children, the next root crossing the cap, fewer than
1,024 total pages, a partial final root, equal distances, base/delta ties, and a
singleton root. Differentially compare bounded blockwise top-2,048 against
`sorted(zip(scores, ids))[:k]` for random blocks, reversed blocks, duplicate
scores, subnormal finite values, non-contiguous IDs, k=1, k=n, and each V97 PQ
format including packed PQ32x4. Reject NaN, infinity, duplicate IDs, wrong code
width, and any sample claiming counts beyond a configured cap.

- [ ] **Step 2: Run the bounded-routing RED**

```bash
python3 -m unittest scripts.test_v98_hierarchical_row_router.V98BoundedRoutingTests
```

Expected: missing routing/scoring boundary failure.

- [ ] **Step 3: Implement bounded routing/scoring**

Score root and page summaries with V97's independently tested ADC primitive.
Select roots/pages with `numpy.argpartition`, then establish exact total order
only within the bounded retained set. Scan row codes in fixed blocks, maintain a
2,048-entry `(distance, row_id)` maximum heap, and never allocate or sort all
262,144 pairs. Return actual evaluation counts in `HierarchyFence` and reject a
count above its limit. Summary-only returns the retained page order without a
row-score path.

- [ ] **Step 4: Run the bounded-routing GREEN**

Run the same test class. Expected: all differential and cap cases pass.

- [ ] **Step 5: Commit bounded routing**

```bash
git add scripts/v98_hierarchical_row_router.py scripts/test_v98_hierarchical_row_router.py
git commit -m "research: add bounded V98 routing"
```

### Task 3: Fail-fast full-query producer and memory worksheet

**Files:**
- Modify: `scripts/v98_hierarchical_row_router.py`
- Modify: `scripts/test_v98_hierarchical_row_router.py`

**Interfaces:**
- Produces: `ContainmentSample`, `ArmSample`, `V98Projection`, `V98Result`, `evaluate_v98(inputs, authority, config)`, and `canonical_v98_result_bytes(result)`.

- [ ] **Step 1: Write fail-fast/result RED tests**

Add `V98ProducerTests` with three coherent fixtures:

1. containment fails and fitting/scoring spies remain untouched;
2. containment passes but exact-f32 fails and arm-fitting spies remain untouched;
3. both ceilings pass and exactly five arms execute in registered order.

Assert 1,000 unique query ordinals in the large-shape contract, literal truth
page hits, exact-f32 hit IDs, selected page order, GETs/bytes, actual work
counts, and classifications `hierarchy-containment-rejected`,
`hierarchy-exact-ceiling-rejected`, or `widths-evaluated`. Assert canonical
sorted compact JSON plus one LF. Mutate every configuration field, sample
field, artifact identity, stop class, array identity, and projection term.

Pin worksheet arithmetic with:

```python
projection = project_v98_resident_bytes_100m(PQ16X8, config)
self.assertEqual(projection.page_summary_bytes, 390_625 * 2 * 16)
self.assertEqual(projection.root_summary_bytes, 48_829 * 2 * 16)
self.assertLess(projection.total_bytes, 3 * 1024**3)
self.assertFalse(project_v98_resident_bytes_100m(PQ32X8, config).eligible)
```

- [ ] **Step 2: Run the producer RED**

```bash
python3 -m unittest scripts.test_v98_hierarchical_row_router.V98ProducerTests
```

Expected: missing fail-fast evaluator/result types.

- [ ] **Step 3: Implement fail-fast evaluation and projection**

Authenticate/load once. Build the hierarchy once. First produce all containment
samples and reduce them locally; return immediately on failure. Then compute
exact-f32 samples through the identical retained pages and planner; return on
failure. Fit/evaluate one compressed arm at a time and retain only its code
identity plus per-query evidence before freeing codes. Derive the V98 worksheet
from counts and integer widths, adding each hierarchy and workspace term once.
Serialize only frozen dataclasses through `dataclasses.asdict` and canonical
JSON; no untyped evidence assembly.

- [ ] **Step 4: Run producer GREEN and complete producer module**

```bash
python3 -m unittest scripts.test_v98_hierarchical_row_router
```

Expected: all V98 producer tests pass.

- [ ] **Step 5: Commit producer**

```bash
git add scripts/v98_hierarchical_row_router.py scripts/test_v98_hierarchical_row_router.py
git commit -m "research: add V98 fail-fast producer"
```

### Task 4: Independent reducer and winner decision

**Files:**
- Create: `scripts/v98_hierarchical_row_router_rescore.py`
- Create: `scripts/test_v98_hierarchical_row_router_rescore.py`

**Interfaces:**
- Consumes: exact V98 result path, registered SHA-256, and expected authority/configuration.
- Produces: `validate_v98_result(...)`, `rescore_v98_result(...)`, and canonical `borsuk-v98-hierarchical-row-router-rescore-v1` bytes.

- [ ] **Step 1: Write complete reducer mutation RED tests**

Construct one typed valid result and independently expected summary. Mutation
tables cover missing/extra/type drift; bool-as-int; query order/cardinality;
truth/hit/page identities; containment/exact/arm recalls; p05; GETs/bytes; all
work caps; hierarchy/codebook/code array identities; every worksheet term;
bootstrap seed/count/matrix; each paired interval; classification; eligibility;
and winner. Include coherent-looking producer aggregate and winner forgeries so
validation proves it recomputes rather than compares shallow fields.

- [ ] **Step 2: Run reducer RED**

```bash
python3 -m unittest scripts.test_v98_hierarchical_row_router_rescore
```

Expected: missing V98 reducer boundary.

- [ ] **Step 3: Implement independent validation/reduction**

Do not import producer aggregate, projection, bootstrap, classification, or
winner functions. Strictly parse schema and concrete types. Recompute sample
hits from literal truth and selected-page membership, aggregates with nearest
rank, resource maxima, hierarchy gates, worksheet arithmetic, and one seeded
10,000-row bootstrap index matrix shared by all comparisons. Apply absolute
gates first, memory eligibility second, paired non-inferiority third, then the
deterministic smallest-row winner rule.

- [ ] **Step 4: Run reducer GREEN**

```bash
python3 -m unittest scripts.test_v98_hierarchical_row_router_rescore
```

Expected: all reducer and mutation tests pass.

- [ ] **Step 5: Commit reducer**

```bash
git add scripts/v98_hierarchical_row_router_rescore.py scripts/test_v98_hierarchical_row_router_rescore.py
git commit -m "research: independently validate V98 evidence"
```

### Task 5: One-attempt Causality Spot execution boundary

**Files:**
- Create: `scripts/v98_hierarchical_row_router_run_remote.sh`
- Create: `scripts/launch_v98_hierarchical_row_router_spot.py`
- Create: `scripts/test_launch_v98_hierarchical_row_router_spot.py`

**Interfaces:**
- Consumes: exact commit archive and six frozen object identities.
- Produces: immutable raw result, independent summary, timing/resource record, terminal receipt, logs, and instance identity under one attempt prefix.

- [ ] **Step 1: Write launcher RED tests**

Assert profile `causality`, Spot-only market, x86_64 AMI/instance agreement,
one attempt, exact source/query/truth/generation/base/delta identities, all
1,000 queries, fixed hierarchy caps, 10,000 bootstrap samples, G1 critique
hash, no validation/holdout URI, terminal upload before shutdown, and instance
termination. Assert interrupted or missing-terminal cells are ineligible and
partial result bytes are never downloaded or inspected.

- [ ] **Step 2: Run launcher RED**

```bash
python3 -m unittest scripts.test_launch_v98_hierarchical_row_router_spot
```

Expected: missing V98 launch plan/runner.

- [ ] **Step 3: Implement remote runner and launcher**

Derive a unique attempt prefix from campaign/source/time. Claim it before EC2
launch. Download only registered objects and verify length/SHA-256 before use.
Run producer once and reducer once, upload immutable evidence, record wall/RSS,
PSI/swap, instance AZ/type/ID, Spot price and estimated spend, then write the
terminal receipt and terminate. Trap interruption to upload a failed terminal;
never restart inside the attempt.

- [ ] **Step 4: Run complete local assurance**

```bash
python3 -m unittest \
  scripts.test_v98_hierarchical_row_router \
  scripts.test_v98_hierarchical_row_router_rescore \
  scripts.test_launch_v98_hierarchical_row_router_spot
uv run --offline --python 3.12 --with ruff==0.15.20 ruff check \
  scripts/v98_hierarchical_row_router.py \
  scripts/test_v98_hierarchical_row_router.py \
  scripts/v98_hierarchical_row_router_rescore.py \
  scripts/test_v98_hierarchical_row_router_rescore.py \
  scripts/launch_v98_hierarchical_row_router_spot.py \
  scripts/test_launch_v98_hierarchical_row_router_spot.py
python3 -m py_compile \
  scripts/v98_hierarchical_row_router.py \
  scripts/test_v98_hierarchical_row_router.py \
  scripts/v98_hierarchical_row_router_rescore.py \
  scripts/test_v98_hierarchical_row_router_rescore.py \
  scripts/launch_v98_hierarchical_row_router_spot.py \
  scripts/test_launch_v98_hierarchical_row_router_spot.py
python3 scripts/validate_research_docs.py
git diff --check
```

- [ ] **Step 5: Commit and push the verified harness**

```bash
git add scripts/v98_hierarchical_row_router_run_remote.sh \
  scripts/launch_v98_hierarchical_row_router_spot.py \
  scripts/test_launch_v98_hierarchical_row_router_spot.py
git commit -m "research: add V98 Spot execution boundary"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

### Task 6: Execute and ledger G1 V98

**Files:**
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: clean verified V98 commit, existing G1 critique hash, and frozen registered inputs.
- Produces: one authenticated G1 decision or fail-fast rejection, with no production-code change.

- [ ] **Step 1: Preregister exact attempt metadata**

Record campaign/attempt prefix, source commit/archive, six identities,
configuration, absolute gates, stop classifications, bootstrap seed/count,
instance choices, timeout/pressure stops, cleanup, and expected maximum spend.

- [ ] **Step 2: Launch exactly one Spot attempt**

Use `launch_v98_hierarchical_row_router_spot.py` with AWS profile `causality`.
Monitor only S3 terminal markers and EC2 health while incomplete. Terminate the
instance immediately after terminal. An interrupted attempt is recorded as
failed; any replacement is a separately registered attempt.

- [ ] **Step 3: Independently authenticate the terminal evidence**

Download only complete terminal/result/summary/resource artifacts. Verify exact
lengths and hashes, rerun the current-source reducer, and require byte-identical
derived claims. Explicitly remove named scratch files after PID clearance.

- [ ] **Step 4: Update and validate the evidence ledger**

Record dataset/split, per-arm recall and paired CIs, work/GET/byte maxima,
100M worksheet, winner or rejection, instance/region/AZ, wall/RSS/PSI/swap,
spend, artifact hashes, competitor basis as not measured at G1, and the exact
next G2 action.

Run:

```bash
python3 scripts/validate_research_docs.py
git diff --check
```

- [ ] **Step 5: Commit and push the G1 decision**

```bash
git add docs/research/algorithm-first-page-layout-ledger.md
git commit -m "docs: record V98 G1 decision"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```
