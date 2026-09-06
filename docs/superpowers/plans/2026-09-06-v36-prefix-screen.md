# V36 Physical Prefix Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject an infeasible V36 projection, posting, coarse-code, or complete fine-object layout on a separately registered, hash-object-sampled 1M real-768D population before the globally selected 787-GB dataset freeze.

**Architecture:** A bounded same-region Spot/NVMe freezer authenticates complete ReLAION objects selected by a query-independent hash and produces role-separated Parquet plus exact GT. Rust then evaluates projection, posting, score, coarse-code, and complete-object admission as sequential causal stages; it never treats logical micro-pages, raw vector bytes, or this diagnostic sample as qualification evidence.

**Tech Stack:** Rust, Arrow IPC, Parquet, serde JSON, SHA-256, BLAKE3, SRHT, bounded centered principal-subspace iteration, residual PQ4, SQ8, AVX2/VNNI, NEON dot-product, Python 3.12, boto3, AWS Spot profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-06-v36-funnel-three-qualification-design.md`

## Global Constraints

- The screen population is `borsuk-v36-prefix-screen-population-v1`; it is not the globally hash-selected V36 corpus and cannot satisfy G0--G5 or a release claim.
- Order registered complete source objects by
  `(SHA-256("borsuk-v36-screen-object-v1" || utf8(path) || u64le(length)),path)`,
  then freeze the first 1,100,000 distinct valid feature IDs in that object and
  physical-row order, keeping the first occurrence. Stop after the complete
  object that reaches the target and record every consumed identity and byte.
- The object-sample stream is capped at 16 complete source objects and 6 GiB;
  reaching either cap before 1,100,000 distinct valid rows is terminal
  `screen-source-insufficient`, never permission to fetch object 17.
- Within that frozen population, select disjoint development, validation, sealed holdout, and performance roles by registered SHA-256 rankings; remove all 13,000 query rows before selecting exactly 1,000,000 corpus rows. Leave the remaining rows unused.
- Construction receives corpus only. Evaluation receives named corpus-derived artifacts, query vectors, and truth but no source/list/discovery capability.
- Bulk cross-language artifacts are strict Parquet or Arrow IPC. Small authorities, terminals, and results are strict canonical newline JSON.
- Remote execution uses profile `causality`, `eu-central-1`, Spot by default,
  encrypted ephemeral NVMe, one original process per attempt, authenticated
  checkpoint upload every 16 complete objects or 300 active seconds, at most
  three attempts, a 43,200-second active cap, a registered Spot price ceiling
  of $3/hour and total campaign ceiling of $90, and immediate termination. The
  per-attempt effective cap is the smaller of 43,200 seconds and the remaining
  dollar budget divided by the admitted hourly rate and multiplied by 3,600
  seconds/hour, so three attempts cannot project past $90.
- The normal envelope is 14 complete GETs and 7 MiB per wave; the hard retry-inclusive envelope is 16 GETs and 8 MiB. Offline object admission uses the same complete encoded identities and limits.
- Screen development selects exactly one complete arm. Validation rejection is
  terminal, and sealed holdout opens once only after validation passes. Passing
  authorizes unchanged full-source replay; failure records the causal rejection
  and stops V36 before further corpus spend. A population-diagnostic shift is
  indeterminate rather than an architecture rejection.

---

### Task 1: Prefix-Screen Authority and Revised V36 Arm Identities

**Files:**
- Modify: `crates/borsuk/src/v36_funnel.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/tests/v36_funnel_authority.rs`
- Create: `crates/borsuk/tests/v36_prefix_screen_authority.rs`

**Interfaces:**
- Produces `V36ProjectionArm::{Srht192,CenteredSubspace192}`.
- Extends `V36ShapeScore` with `Prototype6`, meaning six total f32[192] vectors including the posting centroid.
- Produces `V36PrefixPopulationAuthority`, `V36PrefixScreenManifest`, `validate_v36_prefix_screen_manifest`, and canonical serializers.
- Extends the checked resource ledger to sixteen 32-MiB streaming query
  workspaces; no decoded object set may exceed one workspace.

- [ ] **Step 1: Write projection and score identity REDs**

  Add tests named `v36_prefix_projection_identity_is_closed` and
  `v36_prototype_six_fits_the_equal_summary_slot`. Require seed 36 for SRHT;
  exact mean/reservoir/seed-36/two-iteration/orthogonalization/sign identities
  for the centered subspace; retained-energy reports at M64/96/128/192; six
  total vectors; 4,608 vector bytes plus 128 metadata bytes; unknown variants,
  seven-vector summaries, and post-result projection substitution rejected.

- [ ] **Step 2: Write population authority REDs**

  Add `v36_prefix_population_is_distinct_from_full_source_authority`. Require
  the exact population marker, source revision, ordered source-manifest digest,
  hash-ranked complete-object sampling algorithm,
  digest, 1,100,000 distinct candidates, 1,000,000 corpus rows, role counts
  1,000/1,000/1,000/10,000, consumed-object identities, first-occurrence rule,
  and `claim_eligible=false`. Mutation-test every count, URI, digest, length,
  role seed, ordering rule, capability, 32-MiB workspace, and overflow.

- [ ] **Step 3: Run the focused RED**

  Run:

  ```bash
  CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_prefix_screen_authority -- --nocapture
  ```

  Expected: compilation fails only on the missing prefix/projection/prototype
  boundary.

- [ ] **Step 4: Implement the minimal closed authority**

  Add closed enums and strict structs with `deny_unknown_fields`. Canonicalize
  recursively sorted JSON with one trailing LF. Validate all counts and exact
  cross-object identities; do not add defaults, aliases, legacy variants, or a
  conversion that can label prefix evidence as full-source evidence.

- [ ] **Step 5: Verify and commit**

  Rerun the focused test, scoped Clippy, `cargo fmt --all -- --check`, docs
  validation, and `git diff --check`. Commit the authority slice only after the
  diff proves no runtime query or storage path changed.

### Task 2: Bounded Population Freezer and Exact Truth

**Files:**
- Create: `crates/borsuk/src/v36_prefix_dataset.rs`
- Create: `crates/borsuk/examples/v36_prefix_freeze.rs`
- Create: `crates/borsuk/tests/v36_prefix_dataset.rs`
- Create: `scripts/run_v36_prefix_screen.py`
- Create: `scripts/test_run_v36_prefix_screen.py`
- Create: `docs/research/v36-prefix-screen-authority.json`

**Interfaces:**
- Produces strict source/query/GT Parquet, source-ordinal mapping, and a canonical population receipt.
- Consumes only the registered finite source-object prefix and Task 1 authority.

- [ ] **Step 1: Write dataset and GT REDs**

  Test object selection by registered hash then path, and first occurrence by
  `(selected_object_ordinal,row_offset)`, nonnegative u64 feature
  IDs, non-null finite nonzero f32[768], exact physical schema, role priority,
  query removal, exact counts, source ordinal distinct from feature ID, and
  binary64 no-FMA GT@100 ordered by `(distance,unsigned_feature_row_id)`.
  Produce exact GT@100 for development, validation, and sealed holdout only;
  the 10,000 performance vectors are removed from corpus membership and bound
  for later timing but do not incur a prefix-screen GT scan. Mutation-test
  nullability, child type/name, dimension, NaN, infinity, zero
  norm, duplicate winner, role overlap, ordinal drift, GT distance, and tie.
  Report GT@10 exact/near-duplicate rates, nearest-neighbour distance quantiles,
  and centered spectrum retained-energy fractions for comparison with the
  later globally selected 1M population.

- [ ] **Step 2: Write launcher and lifecycle REDs**

  Require `causality`, Spot, three registered `eu-central-1` AZ candidates,
  encrypted ephemeral NVMe, the 16-object/6-GiB source cap, $3/hour Spot and
  $90 campaign caps, disk preflight above raw
  population plus 25% workspace, no devbox corpus path, terminal conditional
  upload, durable authenticated checkpoint upload every 16 complete objects or
  300 active seconds, at most three Spot attempts, interrupted-cell checkpoint
  validation, and unconditional instance termination. A failed or missing
  marker is terminal failure, never an unbounded poll or fourth attempt.
  Require a non-mutating `--dry-run` that prints exact object/byte/disk/wall/
  attempt/cost caps and creates no instance, bucket object, or local corpus.

- [ ] **Step 3: Run focused REDs**

  Run the Rust test, then:

  ```bash
  python3 -m unittest scripts.test_run_v36_prefix_screen
  ```

  Expected: missing freezer/launcher boundaries only.

- [ ] **Step 4: Implement bounded streaming materialization**

  Stream complete authenticated objects to ephemeral NVMe in bounded Arrow
  batches. Stop only after the complete object containing the 1,100,000th
  distinct valid ID. Rust owns validation, membership, Parquet, and blocked
  no-FMA GT; Python owns provisioning, monitoring, terminal sync, and
  termination. Checkpoint only complete source objects and complete GT query
  tiles. Authenticate and upload each checkpoint before deleting its prior
  durable checkpoint; resume only from complete registered object/tile
  boundaries.

- [ ] **Step 5: Execute one dry run and one Spot freeze**

  The dry run must print instance candidates, maximum source objects/bytes,
  disk, wall, and cost caps without AWS mutation. Then run one original Spot
  cell, preserve its terminal, terminate it, and authenticate every produced
  object before any oracle consumes it.

- [ ] **Step 6: Verify and commit evidence**

  Run focused Rust/Python tests, pinned Ruff, py_compile, scoped Clippy, fmt,
  docs validation, and diff-check. Commit the code plus completed authority and
  receipt; confirm no instance remains before pushing.

### Task 3: Sequential Offline Funnel and Complete Fine Objects

**Files:**
- Create: `crates/borsuk/src/v36_funnel_geometry.rs`
- Create: `crates/borsuk/src/v36_funnel_code.rs`
- Create: `crates/borsuk/src/v36_funnel_fine.rs`
- Create: `crates/borsuk/src/v36_prefix_oracle.rs`
- Create: `crates/borsuk/tests/v36_prefix_oracle.rs`
- Modify: `crates/borsuk-fma/src/lib.rs`

**Interfaces:**
- Produces deterministic projection, posting assignment, closure, score, coarse-code heap, local fine-object packing, physical admission, rerank, and causal checkpoint results.
- The full qualification plan reuses these modules; this task does not create a prefix-only production format.

- [ ] **Step 1: Write projection/geometry/score REDs**

  Compare SRHT192 and centered-subspace192 on identical source rows. Pin the
  binary64 corpus mean, exact 768-by-768 reservoir covariance, the lockfile-
  pinned single-threaded `nalgebra 0.33` `SymmetricEigen` implementation,
  eigenpair residual and covariance-reconstruction bounds, eigenpair
  ordering/sign convention, and retained energy at M64/96/128/192. Test
  terminal decomposition-bound failure as numerical failure. Test
  B4096/B8192, single and closure epsilon 0.05/0.15/0.30, cap eight, exact
  assignment, deterministic reductions, occupancy stops, centroid, diagonal,
  rank-two, rank-four, and prototype-six. Require byte-identical results across
  worker counts and block sizes; every score consumes the same 4,736 resident
  bytes. Prototype-six uses the exact seed-36 ChaCha8 k-means++ rule
  and ten Lloyd iterations from the spec; singleton and duplicate-vector tests
  require `min(5,primary_rows)` active sub-prototypes and centroid padding in
  inactive slots. Require lower-bounded Hamilton posting allocation: one per
  non-empty run, exact apportionment of `P-R` by run row count, integer-
  remainder/run-ordinal ties, and exactly 123 postings for the skewed
  `[999997,1,1,1]` case.
  After score freeze, require HNSW whenever posting count exceeds 1,024, with
  exact `M=32`, `efConstruction=200`, seed 36, registered efSearch ladder, and
  999,000-ppm parity to the exhaustive selected-scorer prefix.

- [ ] **Step 2: Write coarse-code and identity REDs**

  Test projected-f32, sign24, PQ4-32, and PQ4-48 over identical routed rows.
  Retain explicit u64 dense ordinal and u64 source feature ID in every coarse
  replica. Require owner-relative codes, unique-live admission, K in
  512/1024/1536/2048, deterministic ties, scalar/SIMD parity, and exact
  candidate containment. Treat any residual-energy byte as a separately named
  heuristic arm and compare it with uncorrected PQ; never label it exact.
  Require strict-prefix atomic coarse admission: the first non-fitting posting
  stops the suffix, so cumulative byte frontiers remain monotone.

- [ ] **Step 3: Write complete fine-object REDs**

  Build one unreplicated fine plane in primary-posting/local-cluster/local-
  chain/source order. For each posting train
  `min(primary_rows,ceil(B/64))` projected-space centroids by deterministic
  farthest-first plus five Lloyd iterations; sort assigned rows by centroid
  distance/source ordinal; slice blocks of at most 64; and run the exact
  smallest-source-ordinal-start nearest-unvisited chain with source-ordinal
  ties inside each block. Assert the construction-work bound
  `7*N*ceil(B/64)*192 + N*64*192`, including initialization/final assignment,
  measured work, and registered preflight stop.
  Test conservative 512-KiB SQ8 targets of 1,280/640/320/160
  rows for D384/768/1536/3072, then serialize and reduce rows until the complete
  Arrow object fits. Require candidate-to-object mapping, exact checked vote
  mass `sum(K-r)` for zero-based unique-candidate ranks, deterministic order
  `(negative_vote_mass,best_candidate_rank,object_ordinal)`, at most 14 normal
  GETs/7 MiB, at most
  16 hard GETs/8 MiB including retries/metadata, and no range reads or
  query-specific repacking. Mutation-test scattering where 16 truth-bearing
  objects must fail a 14-object budget.

- [ ] **Step 4: Write sequential causal-oracle REDs**

  Freeze checkpoints in this order: projection/route, score, projected-f32 K,
  coarse-code K, complete-object admission, same-row source-f32, SQ8 recall.
  Later stages cannot change earlier candidates. Require useful/fetched rows,
  object scattering, bytes, GETs, duplicates, excluded objects, unreachable
  frontiers, CPU, RSS, and construction work.

- [ ] **Step 5: Run REDs, implement scalar authorities, then SIMD**

  Run:

  ```bash
  CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_prefix_oracle -- --nocapture
  ```

  Implement the scalar deterministic path first. Rerun it GREEN, then add
  native kernels confined to `borsuk-fma` and require differential equality on
  ties, odd tails, saturation, reversed blocks, and non-multiple dimensions.

- [ ] **Step 6: Verify and commit**

  Run focused tests, scoped strict Clippy, fmt, and diff-check. Audit that
  allocation is bounded by postings, K, object count, or configured workspaces,
  never corpus rows. Commit only after the complete-object planner is GREEN.

### Task 4: Execute the Prefix Screen and Decide

**Files:**
- Create: `crates/borsuk/examples/v36_prefix_oracle.rs`
- Modify: `scripts/run_v36_prefix_screen.py`
- Create: `docs/research/v36-prefix-screen-result.json`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes the Task 2 diagnostic population and Task 3 executable.
- Produces one claim-ineligible causal decision that either authorizes the full-source freeze or rejects V36.

- [ ] **Step 1: Write campaign/result REDs**

  Require exact source, binary, arm, query, GT, object-layout, and terminal
  identities. The serializer independently recomputes every checkpoint and
  rejects outcome-dependent arm insertion, validation/holdout tuning, missing
  candidates, raw-byte substitution, or logical-page GET accounting.

- [ ] **Step 2: Freeze the sequential screen ladder**

  Use development to compare projections first, then geometry, then all five
  registered posting scores, then coarse codes, then physical layouts.
  Select exactly one complete winner by the global lexicographic rule.
  Validation evaluates only that identity and rejection is terminal. Open
  sealed holdout once only after validation passes. Do not execute a Cartesian product after an
  earlier causal stage rejects an arm.

- [ ] **Step 3: Run one offline Spot oracle**

  Require route, selected-score, projected-f32, coarse-code, and post-I/O
  containment at least 998,000 ppm; SQ8 loss versus same-row source-f32 at most
  1,000 ppm; development recall@10 at least 997,000 ppm; holdout recall@10 at
  least 995,000 ppm; minimum at least 800,000 ppm; mean replication at most 3;
  RSS below 2 GiB; and complete-object admission inside 14 GETs/7 MiB. Start no
  cold S3 query cell unless the offline replay passes.
  Bind exact/near-duplicate ppm, nearest-neighbour squared-L2 p10/p50/p90, and
  centered M192 retained-energy ppm. Later full-source comparison uses the
  spec's exact 20,000-ppm/10% material-shift rules and cannot turn a shifted
  population into architecture evidence.

- [ ] **Step 4: Record the terminal decision**

  On failure, name the first causal stage and stop the full V36 plan. On pass,
  freeze the one winning combination of projection, geometry, score, coarse
  code, K, local packing, object ceiling, and admission rule. Full-source
  qualification only replays this identity. Authorize Task 2 of the full plan
  without treating screen metrics as release evidence.

- [ ] **Step 5: Verify and commit**

  Run result mutation tests, docs validation, and diff-check. Terminate the
  instance before committing the immutable result and ledger update.
