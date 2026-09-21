# Native Geometric Router Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decide with one cheap authenticated ReLAION-100k cell whether a query-independent bounded tree router can turn the passing balanced-two-means physical layout into release-gate SQ8 recall.

**Architecture:** Preserve the query-blind 192-dimensional SRHT balanced-two-means construction, but retain its exact capacity-cut hyperplanes as a typed tree artifact. At query time, best-first traversal orders sibling branches by squared distance to the actual capacity-cut hyperplane, retains at most 128 leaves, refines those leaves using full-dimensional float32 page representatives, and chooses at most 32 pages and 16 MiB. The evaluator separately reports selected-page GT containment and top-k recall after page-local SQ8 reconstruction; this is a fail-fast scientific gate, not a production format or latency claim.

**Tech Stack:** Python 3.12, NumPy, PyArrow/Parquet, boto3, canonical JSON, Causality AWS Spot.

**Spec:** `docs/superpowers/specs/2026-09-21-native-geometric-ann-redesign-design.md`

## Global Constraints

- Frozen workload: ReLAION development-100k, 100,000 rows × 768 float32 dimensions, all 1,000 development queries, exact GT100, squared Euclidean distance, seed 20260921.
- Constructor has no query or truth capability; tree and page artifacts are sealed before evaluation.
- Exactly one router hypothesis: 128 retained leaves, full-dimensional page-centroid refinement, at most 32 output pages and 16 MiB of encoded page bodies.
- No leaf-count, page-count, seed, representation, or score ladder is selected after viewing truth.
- Router pass requires page-local SQ8 average R@10 at least 96%, average R@100 at least 97.5%, and p05 R@100 at least 90%.
- Every scientific artifact is typed, canonical, content-addressed, and independently recomputed. Production JSON macros and benchmark-manifest serving dependencies are forbidden.
- No production Rust format work, 1M run, or compatibility adapter is allowed before this 100k gate passes.
- Heavy execution uses one Causality AWS Spot attempt. Local execution is limited to narrow synthetic tests, static checks, and independent reduction.

## Review Focus

- Capacity-balanced splits use a nonzero cut threshold; routing must reproduce that threshold rather than assuming the centroid Voronoi bisector.
- Stable-ID tie-breaking must affect construction deterministically without becoming query/truth input to serving.
- Best-first priorities must be finite, deterministic, and independent of heap insertion order for ties.
- Page selection must enforce actual per-page encoded bytes, not `page_count × maximum_page_bytes` or an average.
- SQ8 recall must score reconstructed page-local vectors and deduplicate stable IDs; selected-page containment is not a substitute for ranked recall.

---

### Task 1: Typed tree construction and bounded routing

**Files:**
- Modify: `scripts/native_geometric_layout_screen.py`
- Modify: `scripts/test_native_geometric_layout_screen.py`

**Interfaces:**
- Consumes: `LayoutAuthority`, source stable IDs, source float32 vectors, and the existing deterministic SRHT/two-means construction.
- Produces: `GeometricTreeNode`, `GeometricPageRepresentative`, `GeometricRouterArtifacts`, `construct_geometric_router`, and `route_geometric_query`.

- [ ] **Step 1: Write failing split-authority tests**

Add tests whose hand-computed two-dimensional split has a capacity cut different from the centroid bisector. Require the returned authority to contain a finite normal, adjusted offset, positive squared normal norm, exact left/right row sets, and deterministic stable-ID tie behavior. Mutate the offset, normal, norm, child kind, child ordinal, and leaf/page coverage independently and require rejection.

- [ ] **Step 2: Run the narrow RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_native_geometric_layout_screen.GeometricRouterTests.test_capacity_cut_tree_authority_is_exact_and_query_blind
```

Expected: import or missing-symbol failure for the typed tree boundary.

- [ ] **Step 3: Implement exact tree authority**

Refactor the recursive two-means constructor so each internal node retains the split function used to partition rows:

```python
signed(query) = dot(normal, srht(query)) + adjusted_offset
boundary_distance_squared = signed(query) ** 2 / normal_norm_squared
```

Choose the near child from the sign of `signed`; push the far child with `max(parent_penalty, boundary_distance_squared)`. Store children as explicit `(is_leaf, ordinal)` pairs. Assign contiguous internal-node and leaf-page ordinals and validate that every page is reachable exactly once.

- [ ] **Step 4: Write and run bounded-router differential tests**

Test a scalar exhaustive reference against heap traversal for balanced, skewed, tied, reversed-input, subnormal, and non-finite cases. Require at most 128 unique leaves, deterministic `(priority, kind, ordinal)` ties, exact full-dimensional page-centroid refinement, at most 32 unique pages, and exact byte-budget enforcement.

Run:

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest scripts.test_native_geometric_layout_screen.GeometricRouterTests
```

Expected: all `GeometricRouterTests` pass.

- [ ] **Step 5: Commit the core slice**

Run scoped Ruff, `py_compile`, and `git diff --check`, then commit the two files with message `research: add bounded geometric router screen`.

### Task 2: Typed artifacts, SQ8 evaluation, and independent validator

**Files:**
- Modify: `scripts/native_geometric_layout_screen.py`
- Modify: `scripts/test_native_geometric_layout_screen.py`
- Modify: `scripts/validate_native_geometric_layout_result.py`
- Modify: `scripts/test_validate_native_geometric_layout_result.py`

**Interfaces:**
- Consumes: sealed membership, tree, page-representative, source, query, and truth artifacts.
- Produces: strict Parquet router artifacts, one per-query evidence Parquet, a canonical claim-ineligible result, and an independently recomputed decision.

- [ ] **Step 1: Write failing artifact and SQ8 tests**

Add strict Parquet schemas for internal nodes and page representatives. Add a coherent synthetic corpus where selected-page containment and reconstructed SQ8 top-k recall differ. Require evidence columns for query ordinal, visited internal nodes, retained leaves, selected pages, encoded bytes, containment hits at 10/100, ranked SQ8 hits at 10/100, and routing nanoseconds. Mutate every schema field, identity binding, tree edge, page byte, selected-page order, hit count, aggregate, and decision.

- [ ] **Step 2: Run the focused RED**

```bash
uv run --offline --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest \
  scripts.test_native_geometric_layout_screen.GeometricRouterArtifactTests \
  scripts.test_validate_native_geometric_layout_result.GeometricRouterValidationTests
```

Expected: missing artifact/evaluation/validation boundary.

- [ ] **Step 3: Implement typed serialization and evaluation**

Serialize recursively validated tree nodes and representatives with exact source, seed, construction, membership, dimensions, metric, and encoded-page bindings. Evaluate all queries using only sealed router artifacts before reading truth. For selected pages, reconstruct each page from its own SQ8 low/step authority, compute squared Euclidean distances in bounded NumPy blocks, deduplicate by stable ID, and sort by `(distance, stable_id)`.

- [ ] **Step 4: Implement independent recomputation**

The validator must authenticate raw bytes, rebuild the SRHT query projection, rerun tree traversal and representative refinement, reconstruct selected page SQ8 vectors, recompute every per-query sample, and derive mean/p05/worst aggregates and the pass/kill decision without calling producer aggregate or decision helpers.

- [ ] **Step 5: Run complete local assurance and commit**

Run both complete unittest files, scoped Ruff, `py_compile`, docs validation, and `git diff --check`. Commit the four-file slice with message `research: validate geometric router evidence`.

### Task 3: One immutable 100k Spot decision cell

**Files:**
- Modify: `scripts/launch_native_geometric_layout_spot.py`
- Modify: `scripts/test_launch_native_geometric_layout_spot.py`
- Modify: `docs/research/algorithm-first-page-layout-ledger.md`

**Interfaces:**
- Consumes: one clean pushed source SHA and the exact frozen source/query/truth identities.
- Produces: sealed membership/tree/representative/evidence artifacts, canonical result, validation receipt, terminal receipt, resource receipts, and a ledger decision.

- [ ] **Step 1: Write the launch-plan RED**

Require one c7i Spot attempt, constructor/evaluator capability separation, exact input identities, 3-GiB RSS and wall caps, terminal upload on every path, explicit termination, and no retry. Reject any 1M/10M/100M input, truth mounted during construction, missing cleanup, or mutable source reference.

- [ ] **Step 2: Implement the smallest worker change**

Extend the existing launcher to run the router constructor and evaluator as separate commands, upload all sealed artifacts before validation, and preserve a terminal on interruption or failure. Do not add a new launcher framework.

- [ ] **Step 3: Verify and push the executable revision**

Run the three complete unittest files, scoped Ruff, `py_compile`, docs validation, and `git diff --check`. Commit and fast-forward push; record the exact source archive SHA-256 and size.

- [ ] **Step 4: Run one Spot cell and reduce it independently**

Monitor only terminal markers and EC2 health while active. After terminal, independently validate all samples and the complete 100M two-generation resident-memory worksheet. Terminate compute and clear explicit scratch. Never restart a scientific terminal.

- [ ] **Step 5: Apply the preregistered decision**

If ranked SQ8 misses any quality gate, preserve the exact causal split between routing containment and ranking and materially redesign the responsible layer at 100k. If all three gates pass and the complete 100M worksheet is below 3 GiB, freeze the algorithm and authorize exactly one unchanged 1M qualification. Append identities, metrics, resource/cost evidence, and the decision to the ledger; validate docs, commit, and push.
