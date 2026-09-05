# V34 Hierarchical Low-Rank Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify and productionize an exact hierarchical rank-four routing path that preserves perfect Euclidean-neighbour containment while bounding resident memory, query CPU, S3 objects/bytes, and update cost.

**Architecture:** Persist one compact rank-four Arrow generation, order its leaves with an exact 16-way best-first tree using conservative signed-score bounds, and fetch only the complete storage groups and final pages admitted by fixed budgets. Reject the path with synthetic correctness gates and an authenticated exposed-1M performance screen before creating fresh capability-separated development/holdout evidence; replace the experimental V32/V33 reader only after the fresh holdout and selective-S3 gates pass.

**Tech Stack:** Rust, nalgebra, Arrow IPC, Parquet, serde JSON, SHA-256, Python 3.12, unittest, boto3, AWS profile `causality`, EC2 Spot, S3.

**Spec:** `docs/superpowers/specs/2026-09-05-v34-hierarchical-low-rank-routing-design.md`

## Global Constraints

- Rank two remains rejected; exposed V33 rank-four evidence is hypothesis-generating only.
- Freeze 64 groups, 262,144 rows, 12,288 candidates, first-distinct 64 pages, and at most 8 MiB actual encoded code-object payload per query.
- Rank four is the sole eligible candidate; fine-leaf centroid and equal-byte six-center paths are controls.
- The optimized route must exactly match exhaustive rank-four group order, overflow identity, selected rows, and derived object identities.
- Stop before fresh data unless p95 exact leaf evaluations are at most 25%, router CPU p95 is at most 5 ms on one pinned target core, complete query CPU is at most half the exhaustive control, and checked process memory is below 3 GiB.
- Authenticate immutable generations once when mapping, not once per query.
- Admit at most 1,040 MiB each for active and retiring generations, 128 MiB
  shared caches, 160 MiB runtime/fixed state, 512 MiB query workspaces, and
  96 MiB unallocated headroom: 2,976 MiB total below the 3,072-MiB hard limit.
- Use Arrow IPC for rank-four summaries, Parquet for bulk corpus/query/truth, and canonical JSON only for manifests and receipts.
- Never materialize or download the whole corpus, reconstructed population, PQ plane, or page corpus on the serving node.
- Use AWS profile `causality` and Spot by default; terminate instances immediately at terminal markers.
- Persistent V34 layouts replace experimental predecessors; add no compatibility reader, alias, migration, or duplicate write path.

---

### Task 1: Freeze Rank-Four Authority, Algebra, and Memory Projection

**Files:**
- Create: `crates/borsuk/src/v34_rank4.rs`
- Create: `crates/borsuk/tests/v34_rank4.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/src/v33_group_shape.rs`

**Interfaces:**
- Consumes: `V33GroupShapeBuildRequest` and the decoded V33 rank-four reference from `v33_group_shape`.
- Produces: `V34Rank4Leaf`, `V34Rank4Generation`, `V34ServingMemoryProjection`, `build_v34_rank4_generation`, `build_v34_rank4_generation_from_v33`, `score_v34_rank4_leaf`, and `project_v34_serving_memory`.

- [ ] **Step 1: Write the authority and algebra RED tests**

  Add `v34_rank4_` tests covering exact field ordering, concrete finite types,
  contiguous logical intervals, group bounds, population positivity, residual
  nonnegativity, descending eigenvalues, deterministic signs, decoded
  trace/trace-square/spectral recomputation, nonorthogonal rounded directions,
  negative scores, zero covariance, singular populations, and ordinal ties.
  Assert the score against a literal hand calculation of
  `D+t-a*sqrt(2*h+4*u_sigma_u)` rather than calling the production helper.

- [ ] **Step 2: Run the narrow RED**

  Run: `cargo test -p borsuk --test v34_rank4 -- --nocapture`

  Expected: compilation fails only because the V34 types and functions are
  absent; no fixture or unrelated compiler failure is accepted.

- [ ] **Step 3: Implement the minimal immutable representation**

  Define the exact serving fields:

  ```rust
  pub struct V34Rank4Leaf { /* immutable validated serving state */ }
  ```

  Keep fields crate-private and expose immutable getters so callers cannot
  construct or mutate unauthenticated cached moments. Reuse V33 reconstruction
  and decomposition through `build_v34_rank4_generation_from_v33`, but copy
  only decoded rank-four serving state. Validate `spectral_bound >= max(d) +
  sum(lambda_k*dot(v_k,v_k))`. Score with ordered f64 reductions.

- [ ] **Step 4: Add checked memory/work projection**

  Return checked byte counts for 1,040-MiB active and retiring generations,
  128-MiB caches, 160-MiB runtime, sixteen 32-MiB query workspaces, and 96-MiB
  headroom. Lock the admitted sum at 2,976 MiB and the strict hard limit at
  3,072 MiB. Lock the observed-density arithmetic
  `414_100*2_320 == 960_712_000` and reject totals
  at or above `3*1024*1024*1024` bytes. Report exhaustive directional work as
  `leaf_count*4*96` MACs without claiming it is complete query CPU.
  Derive the provisional complete 16-way tree through terminal buckets of at
  most sixteen leaves as `1+16+256+4_096+65_536 == 69_905` nodes, rather than
  estimating node count from leaf density.

- [ ] **Step 5: Run GREEN and mechanical checks**

  Run:

  ```bash
  cargo test -p borsuk --test v34_rank4 -- --nocapture
  cargo test -p borsuk --lib v33_group_shape::tests::v34_rank4_from_v33_binds_authenticated_reconstruction_and_rank_four -- --exact --nocapture
  cargo fmt --all -- --check
  git diff --check
  ```

  Expected: all V34 tests pass, formatting is unchanged, and no whitespace
  errors remain.

- [ ] **Step 6: Commit and push the authority slice**

  Commit only `v34_rank4.rs`, `tests/v34_rank4.rs`, `v33_group_shape.rs`,
  `lib.rs`, and this plan correction; verify `origin/main` is an ancestor,
  push fast-forward to `origin/main`, and record the full SHA.

### Task 2: Persist a Rank-Four-Only Arrow Generation

**Files:**
- Modify: `crates/borsuk/src/v34_rank4.rs`

**Interfaces:**
- Consumes: `V34Rank4Generation`.
- Produces: `encode_v34_rank4_arrow`, `decode_v34_rank4_arrow`, and `V34Rank4ArtifactIdentity { uri, sha256, length }`.

- [ ] **Step 1: Write Arrow round-trip and mutation RED tests**

  Require one record batch, exact non-null fields and order, fixed-size lists of
  96 and 4, nested four-by-96 directions, exact leaf order, one trailing
  generation identity, and rejection of extra/missing/null/wrong-width/wrong-
  type/nonfinite/reordered rows. Mutate every scalar and vector family and
  require SHA/length authentication before semantic use.

- [ ] **Step 2: Run the narrow RED**

  Run: `cargo test -p borsuk --lib v34_rank4_arrow_ -- --nocapture`

  Expected: only the missing encoder/decoder boundary fails.

- [ ] **Step 3: Implement encode/authenticate/decode**

  Encode directly from compact leaves; do not serialize the V33 ladder or
  reconstructed rows. Decode into one serving generation, recompute all cached
  scalars, and reject any mismatch. Hash the exact Arrow bytes once and bind the
  URI, SHA-256, length, source archive, metric, dimensions, reconstruction,
  codebooks, normalization, and scorer version in a canonical manifest.

- [ ] **Step 4: Prove V33-reference equivalence**

  On coherent synthetic leaves, compare decoded V34 rank-four scores and full
  exhaustive group order with the existing V33 decoded rank-four reference,
  including negative scores, rounded nonorthogonal directions, and ties.

- [ ] **Step 5: Run GREEN, fmt, and diff-check**

  Run the `v34_rank4_arrow_` and `v34_rank4_reference_` filters serially, then
  `cargo fmt --all -- --check` and `git diff --check`.

- [ ] **Step 6: Commit and push the artifact slice**

  Commit only the verified rank-four artifact changes and push fast-forward.

### Task 3: Implement Exact Exhaustive Group Admission

**Files:**
- Create: `crates/borsuk/src/v34_route.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: a mapped `V34Rank4Generation`, query `[f32; 96]`, and immutable per-group row/code-byte identities.
- Produces: `V34RouteBudget`, `V34SelectedGroup`, `V34RoutePrefix`, and `exhaustive_v34_route`.

- [ ] **Step 1: Write admission RED tests**

  Require minimum leaf score per group; `(score,group_ordinal)` ordering; exact
  64-group, 262,144-row, and 8-MiB limits; checked sums; no skipping; and the
  first overflowing group recorded but not admitted. Cover a row-only overflow,
  byte-only overflow, simultaneous overflow, duplicate group leaves, empty
  inputs, nonfinite query, and exact-limit admission.

- [ ] **Step 2: Run RED**

  Run: `cargo test -p borsuk --lib v34_route_exhaustive_ -- --nocapture`

  Expected: missing route types/functions only.

- [ ] **Step 3: Implement the exhaustive authority path**

  Score each leaf exactly once, retain one minimum per group, sort groups, and
  apply the complete-prefix rule. Keep required-owner frontiers out of this API;
  truth joins only in the evaluator.

- [ ] **Step 4: Run GREEN and commit**

  Run the focused filter, fmt check, and diff-check; commit `v34_route.rs` and
  `lib.rs`, then push fast-forward.

### Task 4: Build the Conservative 16-Way Tree

**Files:**
- Modify: `crates/borsuk/src/v34_route.rs`

**Interfaces:**
- Consumes: immutable rank-four leaves.
- Produces: `V34TreeNode`, `V34RouteTree`, `build_v34_route_tree`, `bound_v34_node`, and `hierarchical_v34_route`.

- [ ] **Step 1: Write deterministic construction RED tests**

  Freeze recursive construction: for each node choose the largest population-
  weighted variance dimension (lowest dimension tie), stable-sort leaves by
  `(mean[dimension],leaf_ordinal)`, split into at most sixteen contiguous slices
  whose sizes differ by at most one, and recurse until at most sixteen leaves.
  Node center is the ordered f64 mean of descendant means rounded once to f32;
  radius is the outward-rounded maximum decoded distance. Aggregate
  `t_min/h_max/a_max/L_max` outward. Test reordered input, ties, singleton,
  constant means, and exact child coverage.

- [ ] **Step 2: Write bound RED tests**

  Compare every node bound against every descendant exact score for random,
  adversarial, singular, negative-score, subnormal, large-coordinate, rounded-
  direction, and zero-covariance fixtures. The bound must never exceed the
  minimum descendant score. Validate the interior minimizer and both interval
  endpoints against a dense scalar sampling oracle; ambiguous floating-point
  comparisons must expand.

- [ ] **Step 3: Run construction/bound RED**

  Run: `cargo test -p borsuk --lib v34_route_tree_ -- --nocapture`

  Expected: missing tree/bound functions only.

- [ ] **Step 4: Implement construction and directed bounds**

  Use explicit next-down/next-up helpers for persisted f32/f64 aggregates. For
  `L_max == 0`, minimize the remaining monotone expression at an endpoint. For
  positive `L_max`, evaluate the clamped stationary point and both endpoints,
  then round the minimum downward.

- [ ] **Step 5: Write exact traversal differential RED tests**

  Require hierarchical and exhaustive routes to agree on every selected group,
  score bits, overflow identity, selected rows/bytes, and empty/error outcome.
  Cover shuffled tree storage, repeated group leaves, all-equal bounds, an
  adversarial no-pruning tree, and every prefix limit. Require the first leaf of
  each emitted group to be certified against all unresolved node bounds.

- [ ] **Step 6: Implement best-first traversal**

  Maintain a min-heap of node bounds and a min-heap of evaluated leaves. Emit a
  leaf only when its exact `(score,ordinal)` precedes every unresolved bound;
  otherwise expand the best unresolved node. The first emitted leaf per group
  fixes its minimum. Continue until the first overflowing group is certified.

- [ ] **Step 7: Run GREEN and commit**

  Run `v34_route_tree_` and `v34_route_differential_`, fmt check, diff-check,
  then commit and push the verified tree slice.

### Task 5: Add the Authenticated Exposed-1M Performance Falsifier

**Files:**
- Create: `crates/borsuk/examples/v34_route_falsifier.rs`
- Modify: `crates/borsuk/Cargo.toml`

**Interfaces:**
- Consumes: seven explicit local authenticated metadata/PQ/query roles and no corpus/page role.
- Produces: one canonical claim-ineligible `borsuk-v34-route-falsifier-result-v1` receipt.

- [ ] **Step 1: Write CLI/capability RED tests**

  Require explicit local paths and identities, `--execute-v34-route`, output
  path, pinned core, repetitions, and no bucket/page-prefix/endpoint/credential
  flags. Reject missing, duplicate, unknown, storage, page, D3, and malformed
  flags. Prove the request type has no page/body client.

- [ ] **Step 2: Write performance-receipt RED tests**

  Freeze 1,024 warmups and at least 10,000 raw timed queries in deterministic
  order. Recompute canonical p50/p95/max exact leaf evaluations, bounds, bytes
  touched, CPU nanoseconds, and wall nanoseconds. Pass only when exact parity,
  p95 leaf fraction `<=250_000 ppm`, router CPU p95 `<=5_000_000 ns`, complete
  CPU `<=500_000 ppm` of exhaustive, and projected memory `<3 GiB` all hold.

- [ ] **Step 3: Run RED, then implement the thin executable**

  Run the example filter and accept only missing executable symbols. Implement
  authentication, mapping, pinned-core timing, exhaustive pairing, hierarchical
  timing, and canonical receipt without networking.

- [ ] **Step 4: Run the reduced scientific gate once**

  Build release offline and execute against the authenticated exposed-1M V33
  rank-four summary plus the 128 burned V33 queries. Run 1,024 deterministic
  warmups and at least 10,000 timed query invocations with a 1-GiB RSS cap and
  pressure monitoring. Preserve the sole terminal and stop permanently on any
  correctness or performance failure. This screen is claim-ineligible: it may
  reject the implementation but cannot establish quality. Do not tune the tree
  or thresholds after observing the receipt.

- [ ] **Step 5: Commit only on pass**

  Record the exact binary/result identities and metrics in the evidence ledger,
  validate docs, commit code and evidence as separate coherent slices, and push
  fast-forward. A failure records rejection and ends V34.

### Task 6: Create and Seal the Fresh Development/Holdout Cohort

**Files:**
- Create: `scripts/build_v34_fresh_cohort.py`
- Create: `scripts/test_build_v34_fresh_cohort.py`

**Interfaces:**
- Consumes: registered source Parquet objects and the complete exposed-query exclusion manifest.
- Produces: separate authenticated development-query, development-truth, holdout-query, and holdout-truth Parquet objects plus a canonical split receipt.

- [ ] **Step 1: Write capability and split RED tests**

  Require exact 600/600 counts, deterministic sampling, exact-hash exclusion,
  frozen near-duplicate threshold, source-family exclusion, cross-split
  exclusion, independent exact GT@10, concrete non-null Parquet schemas, and
  distinct role credentials. A split column in one readable object must fail.

- [ ] **Step 2: Implement the bounded builder/controller**

  Stream source batches; never download a full corpus. Generate GT in an
  independently invoked exact kernel sharing no PQ/router code. Upload separate
  immutable roles, validate SHA-256/length/schema, revoke holdout access from
  the router role, and write the terminal receipt.

- [ ] **Step 3: Verify locally without source data**

  Run the complete script unittest file, scoped Ruff, py_compile, and
  `git diff --check`. Commit/push before any cohort creation.

- [ ] **Step 4: Run one bounded cohort-build Spot cell**

  Prebuild artifacts, use `causality` Spot, record instance/zone/AMI/source SHA,
  stream source data, publish only terminal artifacts, and terminate. An
  interruption repeats the identical registration under a new attempt; a
  scientific failure does not rerun.

### Task 7: Run Development and Freeze the Holdout Candidate

**Files:**
- Create: `scripts/run_v34_route_campaign.py`
- Create: `scripts/test_run_v34_route_campaign.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: compact generation, tree, controls, development queries/truth, and fixed budgets.
- Produces: authenticated development route/control receipts and a frozen holdout registration.

- [ ] **Step 1: TDD the campaign state machine**

  Freeze states `PRECHECK -> ORACLE -> ROUTE -> FREEZE -> HOLDOUT_GRANT`, exact
  artifact bindings, one-burn prohibition, terminal interruption semantics,
  pressure stops, and denial of holdout credentials before the freeze receipt.

- [ ] **Step 2: Implement and locally verify the controller**

  Run focused unittest, Ruff, py_compile, and diff-check; commit/push before
  execution.

- [ ] **Step 3: Run the development oracle**

  Reject the program if any exact truth-owner layout exceeds 64 complete groups
  or 262,144 rows. Keep the oracle output separate from query-only route cost.

- [ ] **Step 4: Run development once**

  Compare rank four, fine centroid, and equal-byte six-center control. Require
  every owner/query, all caps, control dominance, exact hierarchical parity,
  and all pruning/CPU/memory gates. Record actual selected groups/rows/bytes;
  never substitute truth-informed frontier counts.

- [ ] **Step 5: Freeze or reject**

  On failure, record and terminate. On pass, commit the exact candidate,
  artifacts, budgets, tree, binary, result, and holdout command before granting
  holdout access.

### Task 8: Run the Sealed Holdout and Selective-S3 Replay

**Files:**
- Modify: `scripts/run_v34_route_campaign.py`
- Modify: `scripts/test_run_v34_route_campaign.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: frozen holdout registration and read-only role credentials.
- Produces: one authenticated holdout receipt and one paired V34/V32 selective-S3 receipt.

- [ ] **Step 1: TDD holdout immutability and paired measurement**

  Require one holdout execution, no parameter override, fixed page prefixes,
  complete candidate/page/exact-rerank causal metrics, identical host/network
  settings for V34 and V32, and canonical request/byte/latency/QPS reductions.

- [ ] **Step 2: Execute holdout once**

  Reject on one missing owner, imperfect query, identity mismatch, prefix
  mismatch, or resource breach. Do not reopen development or run a second
  holdout.

- [ ] **Step 3: Execute paired selective S3**

  Fetch only selected code groups and pages with bounded concurrency. Require
  aggregate and minimum recall of 1,000,000 ppm at 64 pages, non-worse p95 GETs,
  bytes, latency, and QPS, and a strict GET or byte improvement over V32.

- [ ] **Step 4: Record and terminate**

  Preserve terminal S3 evidence, terminate the Spot instance immediately, add
  the exact ledger entry, validate docs, and commit/push. A pass authorizes
  production integration but is not yet a 100M or competitor claim.

### Task 9: Replace the Experimental Reader and Add Segment Writes

**Files:**
- Create: `crates/borsuk/src/v34_index.rs`
- Create: `crates/borsuk/src/v34_segment.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/segment.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/tests/production_workload.rs`
- Modify: `crates/borsuk/tests/storage_access_trace.rs`

**Interfaces:**
- Consumes: validated V34 base generation, tree, storage client, and ordered immutable delta manifests.
- Produces: `V34Index::open`, `V34Index::search`, `V34SegmentWriter::append`, `seal`, `publish`, and `compact`.

- [ ] **Step 1: TDD snapshot and search semantics**

  Require generation pinning, one-time authentication, global budgets across
  base/deltas, bounded async code/page fetches, newest-version resolution,
  tombstones before final top-k, deterministic exact rerank, and reader-safe
  reclamation. Reject incompatible versions rather than dispatching to V32/V33.

- [ ] **Step 2: TDD segment write semantics**

  Require append-only immutable records, idempotent sealing, conditional
  manifest publication, at most four runs/one million delta rows, backpressure,
  bounded leaf workers, deterministic summary construction, crash recovery,
  and compaction from authenticated PQ codes.

- [ ] **Step 3: Implement the smallest production path**

  Keep base and deltas in separate immutable Arrow/Parquet/S3 objects. Reuse the
  qualified route and exact-rerank kernels. Remove superseded experimental
  production defaults and formats; retain historical benchmark code only as
  evidence fixtures, not runtime dispatch.

- [ ] **Step 4: Verify quality and write/read concurrency**

  Run focused unit/integration tests first, then one repository full assurance
  gate. Measure sustained ingestion, visibility delay, compaction debt, write
  amplification, pinned-generation memory, and concurrent read p50/p95/p99.

- [ ] **Step 5: Commit and push production integration**

  Commit only after focused and full gates pass. Push fast-forward and record
  the exact source SHA.

### Task 10: Qualify 100M Before Freezing Defaults

**Files:**
- Create: `scripts/run_v34_100m_campaign.py`
- Create: `scripts/test_run_v34_100m_campaign.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: exact production SHA and immutable 100M dataset roles.
- Produces: authenticated construction, write-throughput, memory, recall, S3, latency, and QPS receipts.

- [ ] **Step 1: TDD the 100M campaign contract**

  Require actual leaf/group/page counts, checked `<3 GiB` process memory,
  complete construction cost, selective object counts/bytes, cold/warm
  distributions, sustained write and compaction metrics, fixed concurrency,
  exact recall, Spot interruption rules, and paired controls.

- [ ] **Step 2: Run local static and controller gates**

  Verify complete unittest discovery under pinned dependencies, Ruff,
  py_compile, shell syntax, Rust fmt, strict workspace/all-targets Clippy, and
  the full locked workspace/all-targets test once.

- [ ] **Step 3: Run one preregistered 100M Spot campaign**

  Stream S3 inputs; never stage the corpus locally. Stop on authority,
  correctness, memory, pressure, deadline, or selective-read breaches. Preserve
  terminal repetitions only and terminate compute immediately.

- [ ] **Step 4: Freeze or reject production defaults**

  Freeze V34 defaults only if all quality, scalability, latency/QPS, write, and
  memory gates pass. Otherwise record the exact rejected boundary and return to
  architecture design without weakening the completed result.
