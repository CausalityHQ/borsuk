# V36 Physical Prefix Screen Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject an infeasible V36 projection, posting, coarse-code, or complete fine-object layout on a separately registered, hash-object-sampled 1M real-768D population before the globally selected 787-GB dataset freeze.

**Architecture:** A bounded same-region Spot/NVMe freezer authenticates complete ReLAION objects selected by a query-independent hash and produces role-separated Parquet plus exact GT. Rust then evaluates projection, posting, score, coarse-code, and complete-object admission as sequential causal stages; it never treats logical micro-pages, raw vector bytes, or this diagnostic sample as qualification evidence.

**Tech Stack:** Rust, Arrow IPC, Parquet, serde JSON, SHA-256, BLAKE3, SRHT, deterministic centered covariance eigendecomposition, residual PQ4, SQ8, AVX2/VNNI, NEON dot-product, Python 3.12, boto3, AWS Spot profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-06-v36-funnel-three-qualification-design.md`

## Global Constraints

- The screen population is `borsuk-v36-prefix-screen-population-v2`; it is not the globally hash-selected V36 corpus and cannot satisfy G0--G5 or a release claim.
- Order registered complete source objects by
  `(SHA-256("borsuk-v36-screen-object-v1" || utf8(path) || u64le(length)),path)`
  and authenticate exactly the first 16, whose frozen total is 5,485,265,954
  bytes. Deduplicate by minimum `(selected_object_ordinal,row_offset)`, then
  take the 1,100,000 lowest population-specific row hashes across all sixteen
  objects. Fewer distinct rows is terminal `screen-source-insufficient`.
- Each object-sample cohort remains capped at its registered 16-object window
  and 6 GiB. Cohort A never fetches zero-based ranked object 16; cohort B never
  fetches outside zero-based ranks 16--31. Identity runs and selection are
  external-memory and bounded; no decoded multi-object vector set is resident.
- Within that frozen population, select disjoint development, validation,
  sealed holdout, and performance roles using the population-specific labels
  `borsuk-v36-prefix-screen-{role}-query-v2`; never reuse full-source role
  seeds. Remove all 13,000 query rows before selecting exactly 1,000,000 corpus
  rows using `borsuk-v36-prefix-screen-corpus-v2`. Leave the remaining rows
  unused. All four query artifacts from every executed screen cohort become
  exact exclusion dependencies of every later full-source role selection;
  prove zero overlap and reject omission of cohort B when it ran.
- Construction receives corpus only. Evaluation receives named corpus-derived artifacts, query vectors, and truth but no source/list/discovery capability.
- Bulk cross-language artifacts are strict Parquet or Arrow IPC. Small authorities, terminals, and results are strict canonical newline JSON.
- V36 campaign code is research scaffolding, not a generic runtime dependency.
  Before release the V36 freezer/controller/evaluation modules and executables
  move into a separate workspace research crate. Production Parquet and
  object-store dependencies remain, but generic consumers receive no campaign
  checkpoint, AWS policy, or frozen-dataset authority surface.
- Remote execution uses profile `causality`, `eu-central-1`, Spot by default,
  encrypted ephemeral NVMe, one original process per attempt, authenticated
  immutable checkpoint publication after every complete object and after a
  complete all-query source block once 300 active seconds have elapsed, at most
  three attempts, a 43,200-second active cap, a registered Spot price ceiling
  of $3/hour and total campaign ceiling of $90, and immediate termination. The
  per-attempt effective cap is the smaller of 43,200 seconds and the remaining
  dollar budget divided by the admitted hourly rate and multiplied by 3,600
  seconds/hour, so three attempts cannot project past $90.
- The normal envelope is 14 complete GETs and 7 MiB per wave; the hard retry-inclusive envelope is 16 GETs and 8 MiB. Offline object admission uses the same complete encoded identities and limits.
- Screen development ranks complete families. Validation evaluates only the
  leader, and sealed holdout opens once only after validation passes. Passing
  advances projection/geometry/score to full-source replay; K, code width, and
  layout remain open until 10M. A population-diagnostic shift is indeterminate,
  and one-cohort failure cannot reject the generic architecture.
- The screen also evaluates diagnostic posting occupancies
  64/82/128/256/512/1024/2048/4096/8192 with actual unique admitted fractions,
  GT@1 containment, rank-100 reconstruction error, rank-100/101 gaps, zero-gap
  counts, and finite positive-gap ratios; none is a scale-equivalence gate.
  It advances projection/geometry/score only. K, code width, and object layout
  remain open until 10M. One-cohort failure requires a disjoint 16-object
  confirmation before a family is rejected. Confirmation cohort B uses ranked
  objects 16--31 (5,483,342,562 bytes) and excludes the exact cohort-A
  selected-ID artifact before ranking, so duplicate logical IDs cannot leak
  across cohorts.

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
- Produces `V36PrefixRegisteredSourceObject`, `V36PrefixPopulationAuthority`,
  `V36PrefixScreenManifest`, `validate_v36_prefix_screen_manifest`, and
  canonical serializers.
- The prefix manifest binds each complete candidate arm (projection, geometry, score,
  coarse code, fine codec, chunk ceiling, and K); the breaking full-source v2
  manifest embeds the same projection authority rather than only a seed.
- Extends the checked resource ledger to sixteen 32-MiB streaming query
  workspaces; no decoded object set may exceed one workspace.

- [x] **Step 1: Write projection and score identity REDs**

  Add tests named `v36_prefix_projection_identity_is_closed` and
  `v36_prototype_six_fits_the_equal_summary_slot`. Require seed 36 for SRHT;
  exact mean/reservoir/covariance, pinned eigensolver, residual/reconstruction,
  eigenpair order/sign identities for the centered subspace; retained-energy
  reports at M64/96/128/192; six
  total vectors; 4,608 vector bytes plus 128 metadata bytes; unknown variants,
  seven-vector summaries, and post-result projection substitution rejected.

- [x] **Step 2: Write population authority REDs**

  Add `v36_prefix_population_is_distinct_from_full_source_authority`. Require
  the exact population marker, source revision, ordered source-manifest digest,
  hash-ranked complete-object sampling algorithm,
  digest, 1,100,000 distinct candidates, 1,000,000 corpus rows, role counts
  1,000/1,000/1,000/10,000, population-specific role labels and digests,
  consumed-object identities, first-occurrence rule,
  and `claim_eligible=false`. Mutation-test every count, URI, digest, length,
  role seed, ordering rule, capability, 32-MiB workspace, and overflow.

- [x] **Step 3: Run the focused RED**

  Run:

  ```bash
  CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_prefix_screen_authority -- --nocapture
  ```

  Expected: compilation fails only on the missing prefix/projection/prototype
  boundary.

- [x] **Step 4: Implement the minimal closed authority**

  Add closed enums and strict structs with `deny_unknown_fields`. Canonicalize
  recursively sorted JSON with one trailing LF. Validate all counts and exact
  cross-object identities; do not add defaults, aliases, legacy variants, or a
  conversion that can label prefix evidence as full-source evidence.

- [x] **Step 5: Verify and commit**

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
- Create: `docs/research/v36-prefix-source-registry.json`

**Interfaces:**
- Produces strict source/query/GT Parquet, source-ordinal mapping, and a canonical population receipt.
- Consumes `V36PrefixFreezeAuthority`, which independently binds the complete
  registry count, encoded-byte total, ordered-manifest digest, caps, policies,
  and role seeds without future consumed-object evidence.
- Produces `V36PrefixPopulationAuthority` only after all sixteen selected
  objects authenticate; the receipt records every completed object
  independently of whether it contributes a surviving selected ID.

The post-review v2 boundary replaces the cutoff-object rule before execution.
`V36PrefixFreezeAuthority` binds the exact upstream dataset-authority SHA-256,
the cohort ordinal, zero-based object-window start/count and exact window byte
total, population-row and corpus seed labels, the four future-exclusion roles,
and an exclusion artifact required for nonzero cohorts. No v1 reader, alias, or
conversion remains because BORSUK is prerelease.

- [ ] **Step 0: Implement the v2 representativeness authority**

  Mutation-test upstream authority digest, object algorithm/version, exact
  cohort ordinal/start/count/window byte total, population-row and corpus
  label/digest, source-manifest binding, exclusion-artifact rules, and four
  query-exclusion roles. Cohort A binds 5,485,265,954 bytes; cohort B binds
  5,483,342,562 bytes for zero-based ranks 16--31. Test
  that late high-ranked rows from every selected object can enter the
  population, a physical prefix cannot substitute, duplicates retain the
  minimum physical occurrence, and the bounded external merge equals an
  in-memory scalar oracle on reduced shapes. Regenerate both checked-in inputs
  and require byte-identical Python/Rust canonicalization.

  Keep the executable fail-closed before source acquisition until the complete
  v2 path below is GREEN. Generic reduced-shape validation remains separate
  from the registered campaign validator. The registered validator pins the
  2,298-object/787,439,811,692-byte registry, its exact manifest digest, and
  cohort A's 0--15 window; it rejects smaller self-consistent windows and
  rejects cohort B until the selected-ID exclusion artifact is authenticated
  and consumed.

  Replace every persisted physical-cutoff field rather than retaining a v1
  alias. Checkpoint and receipt schemas advance to v2. Identity runs bind the
  global ranked object ordinal (A 0--15, B 16--31) plus the window start/count;
  checkpoint completion is a count of complete objects, never an inferred
  prefix cutoff. Add an explicit `Selected` phase carrying the complete
  population-score cutoff `(sha256,feature_row_id)`, pre/post-exclusion counts,
  and the immutable 1,100,000-row selected-identity Arrow artifact. Later
  phases must bind that selection byte-for-byte.

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
  Add GT@1 containment, exact rank-101 margin evidence, per-query own-object
  GT@10 incidence, and the occupancy 64/82/128/256/512/1024/2048/4096/8192
  route curve. Mutation-test separately registered cohort-failure
  classification and forbid a one-cohort generic-architecture rejection.

- [x] **Step 2: Write launcher and lifecycle REDs**

  Require `causality`, Spot, three registered `eu-central-1` AZ candidates,
  encrypted ephemeral NVMe, the 16-object/6-GiB source cap, $3/hour Spot and
  $90 campaign caps, disk preflight above simultaneous source downloads,
  identity spools, final population Parquet, and 25% writer/query/GT workspace,
  no devbox corpus path, output-artifact
  publication before terminal conditional upload, immutable authenticated
  checkpoint upload after each complete object and each complete all-query
  source block once 300 active seconds elapse, at most three Spot attempts,
  interrupted-cell checkpoint
  validation, a controller deadline, and unconditional instance termination.
  Checkpoints and terminals bind the exact freeze authority, registry, source
  archive, executable, source commit, attempt, and referenced artifacts. A
  failed or missing marker is terminal failure, never an unbounded poll or
  fourth attempt. Closed statuses are `complete`,
  `screen-source-insufficient`, `infrastructure`, and `interrupted`;
  scientific insufficiency never retries.
  Require a non-mutating `--dry-run` that prints exact object/byte/disk/wall/
  attempt/cost caps and creates no instance, bucket object, or local corpus.

- [ ] **Step 3: Run focused REDs**

  Run the Rust test, then:

  ```bash
  python3 -m unittest scripts.test_run_v36_prefix_screen
  ```

  Expected: missing freezer/launcher boundaries only.

- [ ] **Step 4: Implement bounded streaming materialization**

  Stream complete authenticated objects from their registered immutable HTTPS
  URIs to encrypted ephemeral NVMe, authenticate SHA-256 before decoding, and
  process bounded Arrow batches. Validate all sixteen selected objects, then
  externally deduplicate and row-hash rank the exact 1,100,000 candidates; any
  invalid gated row rejects the source revision rather than being skipped.
  Rust owns validation, membership, Parquet, and blocked
  no-FMA GT; Python owns provisioning, monitoring, terminal sync, and
  termination. Store population checkpoint bulk state as per-object Arrow IPC
  identity runs and GT state as Arrow IPC top-101 heaps. Eight-query tiles are
  scheduling units only: checkpoint GT at a source-row block after every query
  heap has incorporated it, preserving one corpus scan. Publish immutable
  campaign-scoped dependencies before the checkpoint manifest, then replace
  the run-scoped pointer with `If-None-Match`/`If-Match`; resume only from complete
  registered object or all-query source-block boundaries. Upload every
  authenticated output plus a manifest
  before publishing the terminal. Capture binary exit, timeout, and SIGTERM
  explicitly so `set -e` cannot bypass the terminal path.

  The population implementation is a bounded external merge, not a resident
  cross-object vector set or a target-sized hash heap. For each authenticated
  object, stream strict `(feature_row_id,row_offset)` records into capped
  sorted Arrow IPC spills; retain the minimum row offset per ID; anti-join the
  sorted stream against earlier committed runs so the globally earliest
  physical occurrence wins; then publish one immutable, possibly empty,
  first-occurrence run. Keep only file-backed authenticated dependencies in the
  writer and restore path. After all sixteen runs exist, externally score-sort
  fixed records by `(population_sha256,unsigned_feature_row_id)` with bounded
  fan-in, apply any authenticated prior-cohort exclusion, and emit exactly the
  first 1,100,000 identities. A complete window with insufficient eligible IDs
  is `screen-source-insufficient`; a spill/row/disk bound is infrastructure,
  never scientific insufficiency.

  Mutation-test multi-batch spills, duplicate-only empty runs, cross-object
  duplicates, a winning row from the last object after the target was already
  reached, equal-score ID ties, malicious IPC metadata/nulls/counts, byte-cap
  overflow, writer failure followed by retry, interruption after every object,
  corrupt-newest-checkpoint rejection without fallback, and external/scalar
  equality with a three-row buffer. The selected-ID artifact binds seed,
  manifest, cohort/window, exclusion identity, count, winning positions and
  cutoff. Role selection consumes that authenticated selection and never
  silently performs population selection again.

  Population state advances `Population(1)..Population(16) -> Selected ->
  Materialized -> GroundTruth -> Complete`. Acquisition is capability-limited
  to the registered window; resume downloads no completed population object,
  while final vector materialization may reacquire exactly that window. Remove
  the fail-closed execution gate only after a reduced-shape end-to-end
  scan/checkpoint/restore/select/materialize test and the controller's v2
  dependency closure are GREEN.

  Implement replacement attempts as one authenticated handoff. The controller
  first confirms the preceding EC2 instance is terminal, reads the one
  run-scoped head, and binds its pointer length/SHA-256, generation, and
  manifest identity into the next execution authority. The guest stages only
  that manifest and its ordered dependency closure. Rust authenticates both
  digests, canonical JSON, Arrow IPC schemas, producer transition, restored
  completed object window, selection threshold, and all population counters
  before any source GET.
  Rust alone chooses fresh versus resume and the next generation; the Python
  publisher receives that exact generation. A resumed scan downloads only the
  incomplete suffix. Because population identity runs deliberately omit vector
  payloads, final role-Parquet materialization reacquires exactly the consumed
  prefix after selection; this bounded replay is not a second population scan
  and never expands to the registered full source.

  Test the handoff in narrow layers before any Spot work: manifest/run replay
  disagreement and corrupt-newest rejection with zero acquisition; partial and
  sixteen-object completion resume; selected-population materialization after suffix-only
  scan; head-plus-one sidecar publication; a two-attempt interrupted lifecycle;
  conflicting terminals; and confirmed instance termination before replacement
  launch. Completion requires both science success and a successful final
  checkpoint drain.

  Implement the GT phase rather than treating its manifest variant as
  evidence: fix corpus row groups at 65,536 rows and serialize every query's
  ordered top-101 heap plus exact next source ordinal as Arrow IPC at the first
  complete row group after at least 300 active seconds since the preceding
  publication;
  bind source/query identities and arithmetic authority; restore the suffix;
  prove bit-identical uninterrupted/resumed GT@100 output, rank-101 identity,
  distance bits, and margin. A reduced-shape exact-kernel
  preflight with the production thread count must project completion below half
  the active wall cap by the exact row-times-query ratio, and 900
  seconds without a completed row group or checkpoint is an infrastructure
  stop.

  Mutation-test pointer races, lost acknowledgements, corrupt newest
  generations, cross-object duplicates, duplicate-only objects, a selection
  threshold contributed by the final object, interruption before object commit,
  tied GT distances,
  interruption during a source block, and uninterrupted-versus-resumed exact
  identity/distance-bit equality. Existing identical immutable objects succeed
  only after exact length/SHA-256/BLAKE3 verification; conflicting bytes fail.

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
- Produces one claim-ineligible causal decision that advances one
  projection/geometry/score family, requests cohort-B confirmation, or rejects
  a twice-failed family. It does not freeze scale-sensitive representation or
  layout choices.

- [ ] **Step 1: Write campaign/result REDs**

  Require exact source, binary, arm, query, GT, object-layout, and terminal
  identities. The serializer independently recomputes every checkpoint and
  rejects outcome-dependent arm insertion, validation/holdout tuning, missing
  candidates, raw-byte substitution, or logical-page GET accounting.

- [ ] **Step 2: Freeze the sequential screen ladder**

  Use development to compare projections first, then geometry, then all five
  registered posting scores, then coarse codes, then physical layouts.
  Select exactly one leading projection/geometry/score family by the global
  lexicographic rule. Coarse-code and layout measurements are diagnostic here.
  Validation evaluates only that family and rejection cannot try the runner-up. Open
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
  centered M192 retained-energy ppm. Also bind the complete occupancy curve
  with actual unique admitted rows/fractions, GT@1 containment, rank-101
  margins, true-rank-100 coarse reconstruction error, zero-gap counts, and
  finite positive-gap error/margin quantiles. Treat every occupancy, including
  82, and every ratio as diagnostic rather than a rejection gate. Later full-source comparison uses the
  spec's exact 20,000-ppm/10% material-shift rules and cannot turn a shifted
  population into architecture evidence.

- [ ] **Step 4: Record the terminal decision**

  On cohort-A failure, name the first causal stage and authorize only the
  pre-registered object/ID-disjoint cohort-B confirmation. Reject a family only
  if B fails the same stage. A B pass is terminal `cohort-discordant` and
  neither advances nor rejects; B source insufficiency or infrastructure
  failure is indeterminate, no cohort C exists, and B has its own three-attempt
  $90 cap. On A pass, advance only projection, geometry, and score. Before
  opening the 1M holdout, preregister the finite 10M candidate set
  and deterministic development-only selection rule for coarse code, K, local
  packing, object ceiling, and admission. Freeze those once on 10M development;
  validation and the 1M scale-transfer holdout cannot retune them. Authorize
  full-source materialization without treating
  screen metrics as release evidence.

- [ ] **Step 5: Verify and commit**

  Run result mutation tests, docs validation, and diff-check. Terminate the
  instance before committing the immutable result and ledger update.
