# V36 Funnel-3 Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Causally qualify or reject a dimension-independent, S3-native posting funnel at 1M and 10M before implementing a breaking production format or spending on 100M.

**Architecture:** A Rust oracle separates posting geometry, posting score, optional graph traversal, projected-f32 containment, lossy coarse containment, physical fine-chunk admission, and fine-codec recall. A thin Python launcher enforces phase capabilities, immutable artifacts, AWS Spot lifecycle, and outcome-blind stops. The falsifier uses complete authenticated Arrow IPC objects; source, queries, and truth use Parquet; small manifests and receipts use strict canonical newline JSON.

**Tech Stack:** Rust, Arrow IPC, Parquet, serde JSON, SHA-256, BLAKE3, SRHT, bounded centered principal-subspace iteration, residual PQ4, SQ8/f16, AVX2/VNNI, NEON dot-product, Python 3.12, boto3, AWS Spot profile `causality`.

**Spec:** `docs/superpowers/specs/2026-09-06-v36-funnel-three-qualification-design.md`

## Global Constraints

- BORSUK is pre-release: V36 is breaking and has no legacy reader, alias, or migration path.
- Construction cannot access queries/truth; serving cannot access source objects or list S3.
- Local gates are narrow and serial with `CARGO_BUILD_JOBS=1`; corpus work runs only on bounded same-region Spot/NVMe.
- Every stage preserves the original terminal or canonical stop receipt. No silent retry or overlapping duplicate is allowed.
- The normal planner admits at most 14 complete-object GETs/7 MiB per wave; the
  hard envelope including retries and metadata is 16 GETs/8 MiB. An arm cannot
  widen either envelope.
- Representation freezes once on object-sample-screen development. Screen
  validation evaluates only that winner and rejection is terminal; its sealed
  holdout opens once. Full-source 1M/10M only replay the unchanged identity.
- No production-format or write-lifecycle implementation begins unless one unchanged arm passes 1M and 10M.
- No full-source materialization or paid qualification cell starts until
  `docs/superpowers/plans/2026-09-06-v36-prefix-screen.md` reaches its terminal
  pass decision. That screen uses a distinct diagnostic population and cannot
  satisfy a full-source qualification gate. Full qualification may select only
  only the one complete screen-frozen arm and may not reopen a screen-rejected
  component.
- Bulk cross-language artifacts are Arrow IPC/Parquet. Compact authority and evidence are canonical JSON with one trailing newline.

---

### Task 0: Frozen Dataset Authority

**Files:**
- Modify: `docs/research/standard-datasets.md`
- Modify: `docs/research/methods.md`
- Create: `docs/research/v36-funnel-dataset-authority.json`

**Interfaces:**
- Produces the exact ReLAION source revision/object manifest, duplicate-ID rule, nested scale membership, query roles, GT contract, instance/cost limits, and artifact schemas.

- [x] **Step 1: Register immutable source metadata**

  Freeze revision `bfc7465dcf1245bd605d35dcaf5d2177bbc2025a`, 514,367,913 rows, 2,298 objects, 787,439,811,692 bytes, and ordered-manifest SHA-256 `76ac61cf2821a331419ad40d5eb94d2cdafccccf39af17af1d328b7a2f0bc6c7`.

- [x] **Step 2: Register nested membership and evaluation roles**

  Freeze duplicate resolution, query removal, nested 1M/10M/100M membership,
  1,000 development/validation/holdout queries, 10,000 performance queries,
  and the v1 planned GT output set. Task 2 replaces the unmaterialized v1 GT
  authority before execution.

- [x] **Step 3: Validate and commit**

  Research-doc validation and diff checks passed at commit `9a1beeaf999df4d496e62bf77eb1fb887f3edbd2`.

### Task 1: Authority, Algorithms, and Checked Resource Ledger

**Files:**
- Modify: `crates/borsuk/src/v36_funnel.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/tests/v36_funnel_authority.rs`

**Interfaces:**
- Produces `V36GeometryArm`, `V36ShapeScore`, `V36CoarseCode`, `V36FineCodec`, `V36ChunkCeiling`, `V36FunnelManifest`, `V36ResourceLedger`, `validate_v36_manifest`, `project_v36_resources`, and `plan_v36_transport`.

- [x] **Step 1: Write strict authority REDs**

  Pin baseline SRHT seed 36 and M192; the ten geometry arms; closure epsilon,
  RNG-pruning, and cap-eight identities; B4096/B8192 primary-row semantics;
  the exact generalized V33/V34 shape estimator/equation; projected-f32,
  sign-code24/record44, PQ4-code32/record48 and PQ4-code48/record64 sizes;
  K512/1024/1536/2048 unique source
  IDs; 512-KiB coarse fragments; 64/256/512-KiB fine chunks; SQ8/f16/source-f32
  roles; strict schemas; unique role/URI identities; SHA-256/BLAKE3 algorithms;
  no unknown fields; and `claim_eligible=false`.

- [x] **Step 2: Write checked arithmetic REDs**

  Assert posting counts 245/2,442/24,415 for B4096 and
  123/1,221/12,208 for B8192; exact
  single/mean-two/mean-three/cap-eight coarse projections; no dense row
  locator or unchanged-base ID table; interval-directory and mutation-overlay
  arithmetic; every resident allocation and encoded/decoded overlap;
  byte-based delta admission; a 128-MiB recent-fine arena; checked overflow;
  atomic whole-posting admission; normal 14-GET/7-MiB and hard
  16-GET/8-MiB limits per wave; and explicit excluded-object evidence.

- [x] **Step 3: Run the focused RED**

  Baseline authority RED/GREEN was delivered at commit `543295cd`; prefix-
  screen Task 1 separately adds centered projection and prototype-six
  identities before this plan resumes.

  Expected for later mutations: rejection at the exact changed authority,
  ledger, or planner boundary rather than a missing baseline symbol.

- [x] **Step 4: Implement the minimal closed boundary**

  Use closed enums and exact constants. Make `plan_v36_transport` count complete encoded object lengths, retries, metadata, decoder capacity, and distinct identities. Reject resource projections above 2 GiB at 1M/10M or 3 GiB at 100M. Do not add runtime scoring or storage I/O.

- [x] **Step 5: Verify and commit**

  Run the focused test, scoped strict Clippy, `cargo fmt --all -- --check`, and
  `git diff --check`. Baseline authority and ledger are committed at
  `543295cde6bdb05d93d34919543bcce4396a9645`; revised screen identities are
  owned by prefix-screen Task 1.

### Task 2: Materialize Nested Corpora, Queries, and Exact Truth

**Files:**
- Modify: `scripts/run_v36_dataset_freeze.py`
- Modify: `scripts/test_run_v36_dataset_freeze.py`
- Create: `crates/borsuk/examples/v36_dataset_freeze.rs`
- Create: `crates/borsuk/tests/v36_dataset_freeze.rs`
- Create: `docs/research/v36-funnel-dataset-materialization.json`

**Interfaces:**
- Consumes the Task 0 source registry.
- Produces authenticated 1M/10M/100M source Parquet, four query-role Parquets,
  source-ordinal mapping, exact GT@100, and construction/performance receipts.

- [ ] **Step 1: Write membership and schema REDs**

  Test the first-occurrence duplicate rule, query exclusion before ranking,
  nested SHA membership, source-ordinal mapping, non-null f32[768] physical
  schema, full-stream null/nonfinite/zero rejection, exact row counts,
  query-role separation, distinct source/dense ordinal meanings, and GT tie order by
  `(distance,unsigned_feature_row_id)`. Mutation-test every
  URI/digest/length/schema/binding. Produce the same GT@10 exact/near-duplicate
  rates, nearest-neighbour distance quantiles, and centered M192 retained-energy
  diagnostics as the object-sample screen.

- [ ] **Step 2: Extend launcher/capability REDs**

  Require profile `causality`, Spot, `eu-central-1`, exact source revision, one
  bounded S3 streaming pass, ephemeral NVMe only, no devbox corpus
  materialization, 1.9-TB disk preflight, 43,200-second active cap, checkpoint
  upload every 16 complete objects or 300 active seconds, at most three Spot
  attempts, terminal upload, authenticated resume, and immediate instance
  termination. The baseline launcher is delivered at
  `f0ed2281a4052321924ab02ffc1947b52ca27db5`; this step adds REDs for the new
  durable cadence and three-attempt boundary before changing it.

- [ ] **Step 3: Run focused REDs**

  Run the Rust test, then `python3 -m unittest scripts.test_run_v36_dataset_freeze`.

  Expected: missing dataset freezer and launcher boundaries only.

- [ ] **Step 4: Implement streaming freeze and GT**

  Rust owns membership, Parquet validation/writing, source ordinals, and
  blocked no-FMA binary64 GT. Load the 3,000 quality queries once (about 9 MiB)
  and scan each source row block once across successive eight-query tiles;
  checkpoint the completed source-row prefix plus all canonical top-100 heaps.
  Resume at the first incomplete source-row block. Upload an authenticated
  checkpoint every 16 complete source objects or 300 active seconds, whichever
  comes first; reject a fourth Spot attempt. Before execution, replace Task 0's
  planned query-major checkpoint with a v2 row-major authority and explicitly
  remove `performance-gt100` from its output set, leaving GT only for the 3,000
  quality queries, and
  mutation-test its exact schema; no materialized artifact exists under v1.
  Python authenticates inputs, provisions one Spot cell, monitors
  pressure/progress/cost, syncs terminal artifacts, and terminates compute. No
  scientific math lives in Python.

- [ ] **Step 5: Run dry-run, then one bounded Spot materialization**

  Preserve the exact dry-run command/cost/bytes. On approval already granted by the standing goal, launch one original cell. Publish artifacts only after all counts, hashes, nested membership, and GT relations validate. An interrupted cell is discarded and restarted from its last authenticated construction checkpoint.

- [ ] **Step 6: Verify and commit evidence**

  Run focused Rust/Python gates, pinned Ruff, py_compile, docs validator, fmt, and diff-check. Commit code plus completed registry identities; terminate the instance before pushing.

### Task 3: Posting Geometry, Closure, and Shape Oracle

**Files:**
- Modify: `crates/borsuk/src/v36_funnel_geometry.rs`
- Create: `crates/borsuk/tests/v36_funnel_geometry.rs`
- Create: `crates/borsuk/examples/v36_geometry_oracle.rs`

**Interfaces:**
- Produces `V36PostingModel`, `V36PostingAssignment`, `V36PostingSummary`, `train_v36_geometry`, `assign_v36_closure`, and `score_v36_postings`.

- [ ] **Step 0: Bind the authenticated model handle and external admissions**

  Load the strict super-cell Arrow object once into a privately constructible
  handle that retains its full registered identity and training authority.
  Reject any API accepting a separately supplied model and identity. Add a
  conservative pre-assignment admission and an exact post-count admission;
  execution accepts only these opaque admitted plans. Project assignment,
  fixed-shard sorting, merge generations, local initialization, ten Lloyd
  passes, worst-case repair rescans, disk overlap, bounded queues, aggregate
  worker memory, active wall, and cost. Unit-test 1M/10M/100M arithmetic and
  each limit one unit below its required value without allocating the projected
  population.

- [ ] **Step 0a: Build and merge authenticated external training runs**

  Write provisional uncompressed Arrow shards of fixed 65,536 logical rows,
  independent of callback block and worker scheduling, sorted by
  `(supercell_ordinal,source_ordinal)`. Use a fixed fan-in merge schedule to
  publish canonical 65,536-row per-super-cell chunks. Strictly validate Arrow
  FlatBuffer allocation envelopes before decode, exact schemas/nullability,
  complete unique row coverage, corpus replay digest, model identity, scratch
  ownership, symlink/path traversal refusal, and byte budgets. Keep worker,
  timing, retry, and host evidence outside deterministic result bytes. Require
  final chunk and root-manifest equality across 1/2/4 workers and three callback
  sizes; scratch-fragment bytes may differ.

- [ ] **Step 0b: Train local postings with external sidecars**

  Apply the checked Hamilton allocation to committed super-cell counts and
  reject `P<R`, `k_i>n_i`, overflow, or an incorrect total. Persist bounded
  nearest-distance and assignment sidecars for farthest-first initialization
  and each Lloyd iteration. Repair empties in posting order with updated donor
  counts, then replay repaired assignments in source order for binary64 sums;
  never subtract from an accumulated sum or merge block/worker partial sums.
  Keep the resident trainer only as the reduced-shape scalar oracle. Mutation-
  test corruption/stale checkpoint recovery, skewed one-run populations,
  duplicate/gap substitution, ties, empty repairs, and equality to the resident
  oracle before publishing authenticated posting-centroid Arrow artifacts.

- [ ] **Step 1: Write frozen-arm scale-transfer REDs**

  Consume the prefix-screen module rather than recreating its REDs. Require the
  exact frozen projection/geometry/score identity, super-cell counts 4/64/512,
  exact B4096/B8192 posting counts, bounded external runs, corpus-only training,
  exact assignment at 1M/10M, closure order/cap, and byte-identical results
  across 1/2/4 workers and three block sizes. Recheck lower-bounded Hamilton
  allocation and the `[999997,1,1,1]`, `P=123` case at this boundary.
  Require HNSW for posting counts above 1,024, its exact construction/search
  identity, 999,000-ppm parity to the exhaustive selected scorer, and checked
  graph-byte plus score-work projection at the 100M posting count.

- [ ] **Step 2: Write occupancy and construction-stop REDs**

  Recompute mean/p50/p95/p99/max replication and primary/stored occupancy. Reject mean replication above 3, primary p99 above 2B, primary max above 4B, stored p99 above 6B, stored max above 8B, arithmetic overflow, projected active time above 12 hours, or cost above the manifest cap.

- [ ] **Step 3: Write frozen shape-score replay REDs**

  Recompute only the frozen score over globally selected rows. Require its
  equal 4,736-byte slot, corpus-only fitting, exact initialization/iteration or
  covariance identity, finite checks, scalar/SIMD tolerances, and
  `(score,posting_ordinal)` ties. Mutation-test substitution by any non-frozen
  score.

- [ ] **Step 4: Run RED, implement, and run GREEN**

  Run `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_geometry -- --nocapture`. Implement only the typed streaming oracle, rerun it, then scoped Clippy/fmt/diff-check.

- [ ] **Step 5: Commit the geometry oracle**

  Commit code/tests/example only after a read-only numeric and capability audit.

### Task 4: Execute the 1M Geometry/Shape Falsifier

**Files:**
- Create: `scripts/run_v36_geometry_campaign.py`
- Create: `scripts/test_run_v36_geometry_campaign.py`
- Create: `docs/research/v36-funnel-g1-geometry-authority.json`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes the frozen 1M development corpus/query/GT.
- Produces the frozen geometry/score curve on globally selected rows without
  coarse-code or fine-code confounding.

- [ ] **Step 1: Write campaign and result REDs**

  Require the exact prefix-frozen input/binary/arm identity, construction-only and
  evaluation-only capability sandboxes, every logical integer L through
  `min(postings,1,024)`, power-of-two summaries, deterministic canonical
  aggregates, occupancy/work/resource stops, and no validation/holdout access.
  Before geometry can reject, compare the object-sample and global-1M duplicate
  rates, nearest-neighbour p10/p50/p90, and centered-energy diagnostics using
  the spec's exact shift thresholds; a shift returns population-indeterminate.
  This stage reports logical-prefix containment only; physical coarse transport
  is evaluated after Task 5 defines each code layout.

- [ ] **Step 2: Implement thin replay orchestration**

  Replay only the prefix-frozen geometry/score identity on the globally
  hash-selected 1M corpus. Evaluation opens development only to measure the
  unchanged arm; it cannot select, substitute, or reopen another arm.

- [ ] **Step 3: Run 1M development on Spot**

  Reject the frozen arm at construction gates before query scoring. Require
  998,000 ppm logical route and selected-score containment. Record its equal-
  slot score evidence without comparing or selecting another shape. On failure,
  record Funnel-3 rejection and stop this plan only after population
  comparability passes; otherwise record population-indeterminate.

- [ ] **Step 4: Validate and commit**

  Validate result recomputation, docs, and diff; commit/push the immutable geometry evidence. Do not implement codecs on failure.

### Task 5: Coarse Representation, Unique Heap, and Fine Layout

**Files:**
- Modify: `crates/borsuk/src/v36_funnel_code.rs`
- Modify: `crates/borsuk/src/v36_funnel_fine.rs`
- Create: `crates/borsuk/tests/v36_funnel_code.rs`
- Create: `crates/borsuk/tests/v36_funnel_fine.rs`
- Modify: `crates/borsuk-fma/src/lib.rs`

**Interfaces:**
- Produces projected-f32, sign-code24/record44, PQ4-code32/record48, and
  PQ4-code48/record64 scorers; unique live-ID admission; the fine interval
  directory; complete Arrow chunk layouts; and SQ8/f16/source-f32 rerankers.

- [ ] **Step 1: Write code-layout and SIMD REDs**

  Consume the prefix-screen code module and test only the globally selected
  corpus delta: the exact frozen code identity, owner-relative codebooks,
  explicit u64 dense ordinal/source ID, complete Arrow schemas, bounded
  external construction, and scalar/SIMD parity at 1M/10M. Mutation-test any
  code, K, or kernel substitution.

- [ ] **Step 2: Write unique-heap REDs**

  Require duplicate and visibility filtering during admission, K unique source
  IDs, deterministic best-score replacement, bounded ID-position storage, no
  allocation proportional to corpus rows, and counters for every scanned
  duplicate. Base records carry source ID; mutation tests prove an overlay
  replacement/tombstone suppresses the base candidate before it consumes K.

- [ ] **Step 3: Write fine-layout REDs**

  Consume the prefix-screen fine-object module and require the unchanged local
  partition, chain, complete-object schema, strict-prefix coarse admission,
  vote mass/order, SQ8/f16/source-f32 representation, and normal/hard budgets
  on globally selected 1M/10M rows. Test measured Arrow-envelope shrinkage,
  candidate scattering, encoded/decoded overlap, and final feature-ID tie
  order. Mutation-test query-time repacking, range reads, hidden quantization
  side objects, and any reopened layout.

- [ ] **Step 4: Run focused REDs and implement**

  Run the code test and fine test separately. Implement scalar authorities first, native kernels second, then chunk planning/rerank. The main crate stays unsafe-free; native unsafe remains confined to the kernel crate.

- [ ] **Step 5: Run GREEN and commit**

  Run both focused gates, scalar/SIMD differentials, scoped strict Clippy, fmt, and diff-check. Commit the coherent representation/layout slice.

### Task 6: Complete 1M Causal Oracle and Freeze

**Files:**
- Create: `crates/borsuk/src/v36_funnel_eval.rs`
- Create: `crates/borsuk/tests/v36_funnel_eval.rs`
- Create: `crates/borsuk/examples/v36_funnel_oracle.rs`
- Create: `scripts/run_v36_funnel_oracle.py`
- Create: `scripts/test_run_v36_funnel_oracle.py`
- Create: `docs/research/v36-funnel-g1-authority.json`

**Interfaces:**
- Produces serialized route, shape, projected-f32, lossy-coarse, post-I/O,
  fine-codec, and source-f32 checkpoints for the one prefix-frozen arm identity.

- [ ] **Step 1: Write checkpoint/result REDs**

  Mutation-test every ordinal prefix, aggregate, gate, arm identity, transport counter, classification, and cross-object binding. Serializer independently recomputes all metrics. A later checkpoint cannot change an earlier row set.

- [ ] **Step 2: Write launcher REDs**

  Prove construction/query/truth/source capabilities are phase-specific; every
  phase requires the exact prefix-frozen winner; holdout additionally requires
  its validation-passed receipt; unknown flags fail closed;
  RSS/PSI/swap/progress/wall stops are canonical and outcome-blind.

- [ ] **Step 3: Implement the oracle and run development**

  Replay only the prefix-frozen projection, geometry, score, code, K, layout,
  and codec. Evaluate projected-f32 on its physically admitted rows first, then
  its lossy code, fine layout, serving codec, and same-row source-f32. Stop at
  the first failed causal gate. Development requires 998,000 ppm containment
  checkpoints, fine loss <=1,000 ppm, and 997,000/800,000 ppm
  aggregate/minimum recall. Compare the registered population diagnostics
  before causal classification; a material shift is population-indeterminate,
  not architecture rejection. No selection occurs in this task.

- [ ] **Step 4: Validate and open holdout once**

  Validation evaluates only the unchanged arm; rejection terminates the
  campaign and cannot try another screen candidate. On validation pass, open
  the 1M holdout once and require 995,000/800,000 ppm. Do not alter the 10M
  policy after this point.

- [ ] **Step 5: Verify and commit**

  Run affected Rust/Python gates, pinned Ruff, pycompile, scoped strict Clippy,
  fmt, docs validation, and diff-check. Commit the complete 1M replay evidence.

### Task 7: Frozen 10M Scale-Transfer Gate

**Files:**
- Create: `docs/research/v36-funnel-g2-authority.json`
- Modify: `scripts/run_v36_funnel_oracle.py`
- Modify: `scripts/test_run_v36_funnel_oracle.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Replays the unchanged winner and diagnostic prefixes at 10M.

- [ ] **Step 1: Stage authority/preflight REDs**

  Pin the 10M source/query/GT, measured posting occupancy/packing, exact work/disk/RAM/cost, 12-hour construction cap, 7,200-second measurement cap, and canonical stop receipt. Undefined frontier projections are infinite failures.

- [ ] **Step 2: Run one Spot construction/evaluation campaign**

  Replay every distinct cumulative complete-object byte boundary through 7 MiB
  with the 14-GET normal cap, plus frozen L/K/chunk diagnostics, without
  changing representation. Compute `C1`, `C10`,
  `Chat100=C10*C10/C1`, and the seed-36 10,000-resample paired one-sided 99%
  upper bound. Undefined frontiers are infinite. Require the upper bound and
  the measured-occupancy/fragment-packing 100M projection to fit 7 MiB and 14
  GETs, leaving the registered hard retry reserve intact.
  Before accepting that projection, require the registered HNSW scorer to match
  the exhaustive selected-scorer prefix at 999,000 ppm traversal parity and
  include its measured graph bytes in the 100M RAM projection.

- [ ] **Step 3: Apply validation and holdout**

  Require the complete 10M quality, construction, RSS, and transport gates on validation. Only then open 10M holdout once and require 995,000/800,000 ppm. No post-result adjustment is permitted.

- [ ] **Step 4: Validate and commit evidence**

  Commit the terminal result and ledger. On failure, record architecture rejection and do not implement the production format.

### Task 8: Minimal Executable S3 Query Path

**Files:**
- Create: `crates/borsuk/src/v36_funnel_query.rs`
- Create: `crates/borsuk/tests/v36_funnel_query.rs`
- Create: `scripts/run_v36_funnel_query.py`
- Create: `scripts/test_run_v36_funnel_query.py`

**Interfaces:**
- Consumes the frozen 1M/10M arm and complete named-object reader with no list/discovery API.
- Produces offline-identical results and honest compute/hot/shared-cache/cold counters.

- [ ] **Step 1: Write storage/query REDs**

  Require identical local/S3-mock ordinals, no unselected access, two
  sequential waves, normal <=14 complete GETs/7 MiB and hard <=16 GETs/8 MiB
  each, exact retries and HTTP bytes, `transport-indeterminate` classification
  beyond the reserve, exact digest before decode, bounded buffers/workspaces,
  and queueing-inclusive latency.

- [ ] **Step 2: Implement the breaking query path**

  Read only registered complete Arrow objects. Use the interval directory, unique heap, fixed workspace pool, and selected SIMD kernels. Do not add arbitrary range authentication or legacy storage aliases.

- [ ] **Step 3: Measure 1M and 10M**

  Require offline/executable equality, decoded-hot p99 <=15 ms, <=2 GiB RSS, and transport gates. Report hot-object QPS, 10,000-distinct-query shared-cache QPS/hit rate, and cold S3 percentiles separately. Cold S3 has no 15-ms promise.

- [ ] **Step 4: Verify and commit**

  Run affected tests, strict scoped Clippy, full dependency-complete Python discovery, fmt, docs validation, and diff-check once. Commit code and evidence together.

### Task 9: Delta Publication and Sustained Lifecycle

**Files:**
- Create: `crates/borsuk/src/v36_funnel_delta.rs`
- Create: `crates/borsuk/tests/v36_funnel_delta.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Produces a process-wide byte-bounded active buffer, immutable microsegments,
  conditional publication, visibility state, bounded delta arena/CSR,
  128-MiB recent-fine arena, continuous fine coalescer, and compaction receipts.

- [ ] **Step 1: Write lifecycle REDs**

  Require latest-wins insert/replace/delete visibility, timer/byte seals,
  selected-codec preservation, conditional `HEAD`, retry/indeterminate
  receipts, reader-safe reclamation, old/new generation overlap, byte-based
  delta admission, same-wave fine objects, and exact write amplification. Pin a
  byte-bounded changed-ID overlay `(source_id,latest_sequence,live,current_delta_ordinal)`;
  prove unchanged base IDs need no resident entry, replacements/tombstones
  suppress base rows before K, and delta records bind their sequence. Require
  published fine rows to enter the recent-fine arena, coalesce by owner posting
  every five seconds or 64 MiB, atomically switch to authenticated chunks, stop
  before 128 MiB, and charge microsegment plus coalesced bytes to amplification.

- [ ] **Step 2: Write long-lived rebalancing REDs**

  Separate short delta coalescing from base absorption. Require bounded split/reassignment work, immutable fine-chunk sharing where valid, no lost live row, exact snapshot truth, and a terminal stop when skew or replication exceeds the frozen gates.

- [ ] **Step 3: Implement minimal lifecycle and run GREEN**

  Reuse algorithms but no V35 serialized type. Run focused lifecycle/query tests, scoped strict Clippy, fmt, and diff-check.

- [ ] **Step 4: Commit lifecycle**

  Commit only after a read-only concurrency, durability, and byte-accounting review.

### Task 10: 100M Query and Write Qualification

**Files:**
- Create: `scripts/run_v36_funnel_campaign.py`
- Create: `scripts/test_run_v36_funnel_campaign.py`
- Create: `docs/research/v36-funnel-g4-g5-authority.json`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Produces three terminal 100M read repetitions, saturation/mixed-write results, and sustained-rebalancing evidence.

- [ ] **Step 1: Write campaign dry-run REDs**

  Require profile `causality`, same-region Spot by default, exact commit/binary/input/arm identities, instance candidates, complete object/GET/byte projections, committed cost/wall caps, terminal upload, interruption discard/restart, and immediate termination.

- [ ] **Step 2: Execute G4 only after a GREEN dry run**

  Run three terminal repetitions of the unchanged 100M/768D arm. Require 995,000/800,000 ppm holdout recall, <3 GiB RSS, and the two-wave transport cap. Require <=15 ms p99 and >=3,000 QPS only on decoded-hot concurrency 64; report concurrency 1/16/64 and honest distinct-query/cold S3 metrics.

- [ ] **Step 3: Execute saturation and mixed G5**

  Measure 5k/10k/20k/40k mutations/s for 60 seconds each and require >=20k with zero rejects and visibility p99 <=1 s. Restore the frozen base, then measure ten minutes at 5k accepted mutations/s, eight writers, 64 readers, and 70/20/10 insert/replace/delete. Require read p99 <=110% baseline, complete write amplification <=3.0, recall gates at every registered snapshot, and no deleted result.

- [ ] **Step 4: Execute sustained rebalancing gate**

  Run a separately budgeted long-lived skew/absorption cell long enough to force posting splits and base absorption. Require bounded replication/occupancy, no visibility gap, no unbounded resident growth, and the same query/write gates. Short G5 compaction alone cannot qualify this behavior.

- [ ] **Step 5: Final assurance and decision**

  After the complete diff is stable, run one locked workspace/all-targets test, strict workspace/all-targets Clippy, full dependency-complete Python discovery, fmt, docs validation, and diff-check. Commit all terminal evidence. Freeze the breaking release format only if every gate passes; otherwise record the causal rejection and start a new architecture spec.
