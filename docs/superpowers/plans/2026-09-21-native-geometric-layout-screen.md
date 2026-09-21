# Native Geometric Layout Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decide on frozen ReLAION development-100k whether a query-blind, capacity-balanced geometric page layout has enough exact 32-page recall headroom to justify a new native ANN format.

**Architecture:** A constructor process that cannot access queries or truth emits authenticated Parquet row-to-page membership for four preregistered layouts. A separate evaluator computes the exact GT100 optimum under 32 pages and 16 MiB, and a hostile reducer independently reloads immutable inputs and recomputes every sample, aggregate, and decision. This plan ends at the layout decision; it does not implement the production router or format.

**Tech Stack:** Python 3.12, NumPy, PyArrow/Parquet, frozen typed dataclasses, `unittest`, AWS EC2 Spot and S3 through existing Causality launch primitives.

**Spec:** `docs/superpowers/specs/2026-09-21-native-geometric-ann-redesign-design.md`

## Global Constraints

- Construction receives source vectors, opaque stable IDs, dimensions, metric, seed, and page limits; it receives no query or truth path.
- Evaluate exactly ID-order/256, balanced-random-projection/256, balanced-two-means/256, and balanced-two-means/480-KiB.
- Enforce at most 32 pages and 16,777,216 encoded page bytes per query.
- Record R@10 plus mean, p05, and worst R@100; decide layout headroom on R@100.
- Invalidate unless ID-order reproduces 61.377% mean, 47% p05, and 41% worst R@100 within one GT hit per query.
- Kill below 97.5% mean or 90% p05. Advance only at or above 99% mean and 95% p05.
- Use stable `(score, stable_id)` ties, fixed seeds, finite float32 inputs, and no query-derived training or selection.
- Evidence is typed Parquet plus canonical JSON receipts from dataclasses. Production code must not use `serde_json::json!`.
- Run heavy construction/evaluation only on Causality AWS Spot. Local work is narrow synthetic tests and static checks.
- Do not change the production native format, router, mutation path, or compaction path in this plan.

## Review Focus

- Variable page sizes: Task 3 pins an exact count-and-byte optimizer against brute force and a case where greedy loses.
- Capability leakage: Task 2 proves the constructor CLI has no query/truth surface and rejects such flags.
- Duplicate or missing stable IDs: Tasks 1 and 4 require exact single-owner source coverage.
- Degenerate geometry: Task 2 covers equal scores, empty splits, non-finite vectors, and unsplittable rows.
- Evidence drift: Task 4 mutates membership, samples, aggregates, decisions, and registered identities.

---

### Task 1: Typed authority and membership format

**Files:**
- Create: `scripts/native_geometric_layout_screen.py`
- Create: `scripts/test_native_geometric_layout_screen.py`

**Interfaces:**
- Consumes: authenticated source Parquet plus explicit URI/SHA-256/length, dimensions, metric, seed, and page limits.
- Produces: `ArtifactIdentity`, `LayoutMethod`, `LayoutAuthority`, `MembershipRow`, `write_membership_parquet(path: Path, authority: LayoutAuthority, rows: Sequence[MembershipRow]) -> ArtifactIdentity`, and `read_membership_parquet(path: Path, authority: LayoutAuthority, source_ids: Sequence[bytes]) -> list[MembershipRow]`.

- [ ] **Step 1: Write the failing authority tests**

Add frozen dataclass fixtures and four tests named
`test_layout_authority_rejects_schema_type_identity_and_limit_drift`,
`test_membership_requires_exact_single_owner_source_coverage`,
`test_membership_parquet_has_one_strict_physical_schema`, and
`test_membership_bytes_are_deterministic_for_the_same_rows`.

Use a literal 12-row, 3-dimensional source with non-monotonic 16-byte IDs. Mutate missing/extra keys, bool-as-int, zero/non-finite limits, URI/SHA/length, duplicated IDs, missing IDs, page/in-page order, encoded lengths, nullable columns, column order, and integer widths.

- [ ] **Step 2: Run the narrow RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_native_geometric_layout_screen.AuthorityAndMembershipTests
```

Expected: import failure for the missing typed authority and membership APIs.

- [ ] **Step 3: Implement the minimal authority and Parquet codec**

Use frozen slots dataclasses and an enum:

```python
class LayoutMethod(str, enum.Enum):
    ID_ORDER_256 = "id-order-256"
    RANDOM_PROJECTION_256 = "balanced-random-projection-256"
    TWO_MEANS_256 = "balanced-two-means-256"
    TWO_MEANS_480K = "balanced-two-means-480k"

@dataclasses.dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    role: str
    uri: str
    sha256: str
    encoded_bytes: int

@dataclasses.dataclass(frozen=True, slots=True)
class LayoutAuthority:
    schema: str
    source: ArtifactIdentity
    rows: int
    dimensions: int
    metric: str
    seed: int
    method: LayoutMethod
    maximum_page_rows: int
    maximum_page_bytes: int
```

The Parquet schema is exactly: `stable_id: binary not null`, `source_ordinal: uint32 not null`, `page_ordinal: uint32 not null`, `in_page_ordinal: uint16 not null`, `page_rows: uint16 not null`, `encoded_page_bytes: uint32 not null`, `method: string not null`, `source_sha256: fixed_size_binary[32] not null`, `seed: uint64 not null`, and `construction_sha256: fixed_size_binary[32] not null`. Validate complete single-owner coverage and deterministic `(page_ordinal, in_page_ordinal)` order before writing and after reading.

- [ ] **Step 4: Run GREEN and static checks**

Run Step 2, then:

```bash
uv run --offline --python 3.12 --with ruff==0.15.20 \
  ruff check scripts/native_geometric_layout_screen.py \
             scripts/test_native_geometric_layout_screen.py
python3 -m py_compile scripts/native_geometric_layout_screen.py \
                          scripts/test_native_geometric_layout_screen.py
git diff --check
```

Expected: four tests pass and all static checks exit 0.

- [ ] **Step 5: Commit the authority slice**

```bash
git add scripts/native_geometric_layout_screen.py \
        scripts/test_native_geometric_layout_screen.py
git commit -m "research: define native geometric layout authority"
```

### Task 2: Query-blind deterministic layout constructors

**Files:**
- Modify: `scripts/native_geometric_layout_screen.py`
- Modify: `scripts/test_native_geometric_layout_screen.py`

**Interfaces:**
- Consumes: `LayoutAuthority`, source ID array, and finite float32 matrix.
- Produces: `construct_layout(authority: LayoutAuthority, stable_ids: Sequence[bytes], vectors: numpy.ndarray) -> list[MembershipRow]` and the `construct` CLI subcommand.

- [ ] **Step 1: Write failing constructor tests**

Add four tests named
`test_id_and_random_projection_controls_are_literal_and_deterministic`,
`test_balanced_two_means_obeys_capacity_and_stable_ties`,
`test_two_means_rejects_nonfinite_empty_and_unsplittable_geometry`, and
`test_constructor_cli_has_no_query_truth_or_evaluation_surface`.

The balanced fixture has two obvious clusters plus boundary ties. Assert exact page membership literals, not only counts. Run the CLI subprocess with source/authority/output arguments, inspect `--help`, and require query/truth/GT flags to be absent and rejected.

- [ ] **Step 2: Run the constructor RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_native_geometric_layout_screen.QueryBlindConstructorTests
```

Expected: missing `construct_layout`/`construct` boundary.

- [ ] **Step 3: Implement the deterministic constructors**

Implement a fixed SRHT-like signed Hadamard projection with power-of-two zero padding and take the first `min(dimensions, 192)` permuted coordinates. Derive signs and permutation from the registered seed. At every two-means node:

```python
score = squared_l2(projected_row, right_centroid) \
      - squared_l2(projected_row, left_centroid)
order = numpy.lexsort((stable_id_bytes, score))
left, right = order[:cut], order[cut:]
```

Use two deterministic farthest-point seeds, exactly eight Lloyd iterations, float32 inputs with float64 centroid sums, and a capacity-balanced cut satisfying both child capacities. ID order sorts only by stable ID. Random projection uses one registered vector and the same stable tie. The 480-KiB arm measures actual Arrow IPC page bytes and recursively splits until every page is at most 491,520 bytes.

- [ ] **Step 4: Run GREEN and affected authority tests**

Run Task 1 and Task 2 classes plus Ruff, pycompile, and `git diff --check` from Task 1.

- [ ] **Step 5: Commit the constructor slice**

```bash
git add scripts/native_geometric_layout_screen.py \
        scripts/test_native_geometric_layout_screen.py
git commit -m "research: build query-blind geometric pages"
```

### Task 3: Exact page-count and byte-budget evaluator

**Files:**
- Modify: `scripts/native_geometric_layout_screen.py`
- Modify: `scripts/test_native_geometric_layout_screen.py`

**Interfaces:**
- Consumes: sealed membership, frozen ordered GT10/GT100 stable IDs, and `EvaluationLimits(maximum_pages=32, maximum_bytes=16_777_216)`.
- Produces: `QueryCoverageSample`, `LayoutEvaluation`, `exact_page_coverage(page_hits: Mapping[int, int], page_bytes: Mapping[int, int], limits: EvaluationLimits) -> QueryCoverageSample`, and the `evaluate` CLI subcommand.

- [ ] **Step 1: Write failing exact-evaluator tests**

Add four tests named
`test_equal_page_oracle_matches_literal_top_hit_counts`,
`test_variable_page_oracle_matches_bruteforce_not_greedy_ratio`,
`test_oracle_ties_choose_lexicographically_smallest_page_tuple`, and
`test_evaluation_recomputes_r10_mean_p05_worst_and_decision`.

The variable-size fixture makes greedy hit/byte selection lose. For small randomized fixtures, enumerate every allowed subset and assert equality with `exact_page_coverage`.

- [ ] **Step 2: Run the evaluator RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_native_geometric_layout_screen.ExactCoverageTests
```

Expected: missing coverage/evaluation boundary.

- [ ] **Step 3: Implement the exact bounded oracle**

For each query, count GT IDs per page. Use dynamic programming indexed by `(selected_count, covered_hits)` whose value is minimum encoded bytes plus lexicographically smallest page tuple. Since GT100 caps hits at 100, this is exact and bounded independently of the 16-MiB integer range. Reject duplicate GT IDs, unknown IDs, malformed ranks, and membership drift. Emit all 1,000 per-query samples to Parquet with selected pages, hits@10, hits@100, bytes, and recall ppm.

Use nearest-rank p05 at sorted index `ceil(0.05 * query_count) - 1`. Classify each non-control arm as `killed`, `insufficient`, or `advance`; invalidate all decisions if the ID control is outside registered one-hit tolerance.

- [ ] **Step 4: Run GREEN and affected tests**

Run Task 1–3 classes plus Ruff, pycompile, and `git diff --check`.

- [ ] **Step 5: Commit the evaluator slice**

```bash
git add scripts/native_geometric_layout_screen.py \
        scripts/test_native_geometric_layout_screen.py
git commit -m "research: compute exact geometric page ceilings"
```

### Task 4: Independent hostile reducer

**Files:**
- Create: `scripts/validate_native_geometric_layout_result.py`
- Create: `scripts/test_validate_native_geometric_layout_result.py`

**Interfaces:**
- Consumes: source identity, four membership Parquets, ordered GT, per-query evidence Parquet, canonical result bytes, and fixed configuration.
- Produces: `validate_result(paths: ValidationPaths, expected: LayoutScreenAuthority) -> ValidatedLayoutDecision` and zero stdout on success.

- [ ] **Step 1: Write failing mutation tests**

Add table-driven mutations for source/membership/GT/result URI, SHA-256, or length; method/seed/limits; missing/duplicate ownership; page bytes; selected pages; hits; recall; p05/worst/mean; control reproduction; arm decision; claim eligibility; and canonical key order/newline. Include a coherently reserialized result with recomputed self-digests but changed registered identity.

- [ ] **Step 2: Run the reducer RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_validate_native_geometric_layout_result
```

Expected: import failure for `validate_result`.

- [ ] **Step 3: Implement independent recomputation**

Do not call producer aggregate or decision helpers. Re-read strict Parquet schemas, independently map GT IDs to pages, rerun the count/hit dynamic program, recompute every sample and aggregate, and compare them with the canonical result. Accept only `claim_eligible = false` because this is a development screen.

- [ ] **Step 4: Run GREEN and the complete file gate**

Run both complete unittest files, scoped Ruff, pycompile, docs validation, and `git diff --check`.

- [ ] **Step 5: Commit the reducer slice**

```bash
git add scripts/validate_native_geometric_layout_result.py \
        scripts/test_validate_native_geometric_layout_result.py
git commit -m "research: validate geometric layout evidence"
```

### Task 5: One-attempt Causality Spot execution

**Files:**
- Create: `scripts/launch_native_geometric_layout_spot.py`
- Create: `scripts/test_launch_native_geometric_layout_spot.py`
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: clean pushed source SHA, exact frozen ReLAION development-100k source and GT identities, AWS profile `causality`, region `eu-central-1`, and one attempt ID.
- Produces: terminal receipt, four membership artifacts, per-query evidence, canonical result, independent validation receipt, and ledger decision.

- [ ] **Step 1: Write failing launch-plan tests**

Test a frozen `SpotLayoutPlan` requiring Spot, one attempt, explicit instance/volume/wall/RSS limits, exact input identities, S3 output prefix, source SHA, dependency lock, terminal upload on every path, and termination. Reject On-Demand, retry/restart, unclean source, mutable refs, missing cleanup, query/truth exposure to constructor, and any 1M/10M/100M input.

- [ ] **Step 2: Run the launcher RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_launch_native_geometric_layout_spot
```

Expected: missing typed launch boundary.

- [ ] **Step 3: Implement the smallest existing-pattern launcher**

Reuse only campaign-neutral EC2/S3 helpers from `scripts/launch_v100_page_residual_range_spot.py`. Build/upload one clean source archive, request a `c7i.8xlarge` Spot instance with interruption handling, execute constructor and evaluator as separate commands with disjoint input mounts, validate before uploading `COMPLETE.json`, and terminate immediately after any terminal marker. Never inspect partial scientific output.

- [ ] **Step 4: Verify and commit the executable harness**

Run all three unittest files, scoped Ruff, pycompile, docs validation, and `git diff --check`. Commit and fast-forward push to `origin/main`; record the full source SHA before launch.

- [ ] **Step 5: Execute exactly one 100k cell**

Launch once with the registered source SHA. Monitor terminal markers and instance health only. On terminal, download small result/evidence artifacts, verify exact identities, run the independent reducer, and terminate/verify instance clearance. Do not rerun a scientific terminal.

- [ ] **Step 6: Record the decision**

Append four measured arms, per-query artifact identities, exact mean/p05/worst, resource/cost evidence, validation receipt, and decision to the ledger. Run:

```bash
python3 scripts/validate_research_docs.py
git diff --check
```

Commit and push only the ledger update. If no arm reaches 99% mean and 95% p05, stop this plan and write a new architecture spec for graph partitioning; do not tune the same tree or start a 1M run.
