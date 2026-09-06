# V36 Funnel-3 Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a causal, fail-fast oracle and staged qualification harness that accepts or rejects the V36 Funnel-3 architecture before any production-format rewrite.

**Architecture:** A Rust oracle freezes traversal, route, coarse-code, transport-pruned fine-prefix, fine-rerank, and offline f32-control checkpoints over authenticated Arrow/Parquet artifacts. A small Python launcher enforces capability separation, sealed development/validation/holdout phases, resource stops, canonical receipts, and later AWS Spot lifecycle without placing scientific math in Python.

**Tech Stack:** Rust, Arrow IPC, Parquet, serde JSON, BLAKE3, SHA-256, AVX2/VNNI, NEON dot-product, Python 3.12 orchestration, boto3 only in later AWS phases.

**Spec:** `docs/superpowers/specs/2026-09-06-v36-funnel-three-qualification-design.md`

## Global Constraints

- BORSUK is pre-release: use a breaking V36 format and add no compatibility reader, alias, or migration path.
- Construction cannot access queries or truth; serving cannot access source-corpus objects or list S3.
- Bulk cross-language artifacts are strict Arrow IPC or Parquet; compact manifests and receipts are canonical newline JSON.
- Local scientific stages precede AWS; AWS uses profile `causality` and Spot by default.
- Local gates are narrow and serial with `CARGO_BUILD_JOBS=1`; run the full workspace gate once only after the complete diff is stable.
- No arm reaches 100M until the unchanged G1-selected
  assignment/coarse-code/fine-codec representation passes G1, G2, and G3.
  L/K are diagnostic prefix curves under one fixed capacity policy, not new
  representation arms.

---

### Task 0: Freeze Real Scale Corpus and Absolute Baseline Contract

**Files:**
- Modify: `docs/research/standard-datasets.md`
- Modify: `docs/research/methods.md`
- Create: `docs/research/v36-funnel-dataset-authority.json`

**Interfaces:**
- Consumes: immutable metadata for `andropar/relaion2b-natural-embeddings`.
- Produces: exact hash-nested 1M/10M/100M 768D source identities, separate
  development/validation/holdout/performance splits and GT@100 at each scale,
  and an absolute V36 baseline manifest.

- [ ] **Step 1: Verify dataset availability and license without bulk download**

  Record the immutable dataset revision, license text, 768-dimensional non-null f32 vector schema, `feature_row_id` semantics, shard URIs, lengths, and cryptographic digests. Fail this task if any object lacks stable byte authority or the embedding-use license is incompatible.

- [ ] **Step 2: Freeze nested scale and query membership**

  Use increasing `SHA-256(source_identity || little_endian(feature_row_id))` for nested source membership after excluding query rows selected by a committed counter-based hash seed. Freeze one development/validation/holdout/performance query-vector identity shared across 1M, 10M, and 100M, with separate exact GT at each scale: 1,000 queries in each quality role and 10,000 in the performance role.

- [ ] **Step 3: Define absolute execution authority**

  Bind source/query/GT objects, instance family, region, cache size, concurrency, SDK, and repetitions to V36. Historical V35 evidence is context only and cannot substitute for a same-data executable.

- [ ] **Step 4: Validate and commit dataset authority**

  Run the research-doc validator and `git diff --check`, then commit the two existing research docs plus the authority JSON. Do not begin Task 1 unless this task is GREEN.

### Task 1: Qualification Authority and Checked Budgets

**Files:**
- Create: `crates/borsuk/src/v36_funnel.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Create: `crates/borsuk/tests/v36_funnel_authority.rs`

**Interfaces:**
- Consumes: the authenticated 192-dimensional projection algorithm and canonical JSON helpers, re-expressed in V36 identities rather than legacy serialized types.
- Produces: `V36FunnelArm`, `V36FunnelManifest`, `V36FunnelBudget`, `validate_v36_funnel_manifest`, and `project_v36_funnel_budget`.

- [ ] **Step 1: Write authority and arithmetic REDs**

  Test exact arms and ladders, unique role/URI identities, concrete digest algorithms, strict schemas, no unknown fields, checked overflow, 100M projected resident bytes below 3 GiB, coarse bytes for one/two assignments including the explicit u64 row ordinal, and fine lower bounds for D384/768/1536/3072 at 1,024/1,536/2,048 rows. Require one shared transport planner to count distinct complete-object identities, actual encoded lengths, retries, decoder capacity, and excluded candidates under 16 GETs/8 MiB per wave. Pin `claim_eligible=false` for G0–G3 receipts.

- [ ] **Step 2: Run the focused RED**

  Run: `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_authority -- --nocapture`

  Expected: compile failure only at the missing V36 types and functions.

- [ ] **Step 3: Implement the minimal typed boundary**

  Use closed enums for assignment, code, fine codec, and phase. Reject any arm outside the spec matrix. Compute every byte with checked integer arithmetic; require remote coarse bytes `rows * (code_bytes + 8) * assignments` for explicit row ordinals. Treat `fine_rows * (D + 8)` for SQ8, `fine_rows * 2D` for f16, and `fine_rows * 4D` for f32 only as raw lower bounds; serving admission uses complete encoded Arrow object lengths and decoder capacities.

- [ ] **Step 4: Run GREEN and static gates**

  Run the focused test, scoped Clippy, `cargo fmt --all -- --check`, and `git diff --check`.

- [ ] **Step 5: Commit the authority slice**

  Commit `v36_funnel.rs`, its export, and authority tests after a read-only diff review.

### Task 2: Deterministic Leaf Assignment and Orthogonal-Spill Control

**Files:**
- Create: `crates/borsuk/src/v36_funnel_build.rs`
- Create: `crates/borsuk/tests/v36_funnel_build.rs`

**Interfaces:**
- Consumes: authenticated projected corpus blocks and `V36FunnelArm`.
- Produces: `V36LeafModel`, `V36Assignment`, `train_v36_leaf_model`, and `assign_v36_rows`.

- [ ] **Step 1: Write construction REDs**

  Require `ceil(rows/256)` primary leaves; `min(4,096,next_power_of_two(ceil(leaf_count/64)))` coarse cells; exact 1M/10M/100M counts; a `min(rows,1_048_576)` deterministic reservoir; 25 coarse and 10 local fixed Lloyd iterations; increasing-row f64 reductions; leaf-ordinal ties; primary search over all leaves in the two nearest coarse cells and orthogonal-spill selection over the next eight; primary occupancy near 256 and double-assignment occupancy near 512; no query/truth role; bounded block liveness; exact construction work counters; and identical artifacts across block sizes and worker counts. Pin coarse-object order to `(coarse_cell_ordinal,leaf_ordinal)`, greedy complete-object packing at 512 KiB, and useful/fetched-byte accounting.

- [ ] **Step 2: Run the focused RED**

  Run: `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_build -- --nocapture`

  Expected: missing construction boundary only.

- [ ] **Step 3: Implement streaming training and assignment**

  Train coarse cells from a deterministic bounded reservoir, stream one external partition by coarse cell, then train local leaves one coarse run at a time. Retain only one bounded run window and registered accumulators, emit Arrow leaf summaries plus sorted assignment runs, and externally merge by `(leaf,row_ordinal,assignment_role)`. Permit a logical leaf to span multiple independently authenticated 512-KiB coarse objects in the fixed order; record all identities in its directory and charge all of them to the planner. A resident global `rows * leaves` Lloyd matrix is forbidden.

- [ ] **Step 4: Run GREEN and determinism gates**

  Compare 1/2/4 workers and at least three input block sizes byte-for-byte; run scoped Clippy, fmt, and diff-check.

- [ ] **Step 5: Commit the construction slice**

  Commit only construction code/tests and exports.

### Task 3: Dimension-Independent Coarse Code Arms

**Files:**
- Create: `crates/borsuk/src/v36_funnel_code.rs`
- Create: `crates/borsuk/tests/v36_funnel_code.rs`
- Modify: `crates/borsuk-fma/src/lib.rs`

**Interfaces:**
- Consumes: 192-dimensional projected rows and authenticated training ordinals.
- Produces: `V36CoarseCodebook`, `V36CoarsePlane`, `train_v36_coarse_code`, `encode_v36_coarse_rows`, and scalar/SIMD ADC functions.

- [ ] **Step 1: Write codec and SIMD REDs**

  Cover exact 24/32/48-byte layouts, complete logical checksums, Arrow schemas, saturation, finite inputs, ties-to-even, odd tails, shuffled blocks, scalar/SIMD equality, bounded 1,024/1,536/2,048-entry heaps, and no allocation proportional to corpus rows.

- [ ] **Step 2: Run the focused RED**

  Run: `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_code -- --nocapture`

  Expected: missing code and kernel boundaries only.

- [ ] **Step 3: Implement scalar authority and native kernels**

  Implement the 24-byte sign control and 32/48-byte anisotropic PQ4 plus an explicit u64 row ordinal for every assignment. Use 16-entry shuffle LUTs for PQ4 and exact `(distance,row_ordinal)` heap order. Native kernels live in the existing unsafe-permitted kernel crate; the main crate remains unsafe-free.

- [ ] **Step 4: Run GREEN, differential, and lint gates**

  Require all ladder arms and adversarial numeric families to match their scalar authority, then scoped Clippy, fmt, and diff-check.

- [ ] **Step 5: Commit the coarse-code slice**

  Commit code, kernels, tests, and exports together.

### Task 4: Fine Chunk Rerank

**Files:**
- Create: `crates/borsuk/src/v36_funnel_fine.rs`
- Create: `crates/borsuk/tests/v36_funnel_fine.rs`

**Interfaces:**
- Consumes: bounded coarse heap, resident leaf/chunk directory, and registered fine Arrow IPC objects.
- Produces: `V36FineChunkPlan`, `plan_v36_fine_chunks`, `rerank_v36_sq8`, `rerank_v36_f16`, and `rerank_v36_f32_control`.

- [ ] **Step 1: Write fine-tier REDs**

  Require row-ID deduplication before capacity, a frozen `(distance,row_ordinal)` fine prefix before physical ordering, a separate post-I/O containment checkpoint, limits of 1,024/1,536/2,048 rows and 16 complete independently authenticated Arrow IPC files totaling at most 8 MiB, strict physical schema, SQ8 scale/offset binding, exact object/decoder-capacity accounting, scalar/SIMD equality, and an offline f32 control on the identical prefix exempt only from serving I/O caps.

- [ ] **Step 2: Run the focused RED**

  Run: `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_fine -- --nocapture`

  Expected: missing fine-chunk/rerank boundary only.

- [ ] **Step 3: Implement bounded planning and rerank**

  Deduplicate and select by `(distance,row_ordinal)`, admit registered chunks in that order until the GET/byte cap, freeze the admitted rows, and only then reorder by `(chunk,row_ordinal)` for decode. Use int8/i32 kernels for SQ8 and fixed-order controls for f16/f32.

- [ ] **Step 4: Run GREEN and resource gates**

  Assert exact GET/range/byte counters and no unselected object access; run scoped Clippy, fmt, and diff-check.

- [ ] **Step 5: Commit the fine-tier slice**

  Commit implementation and tests.

### Task 5: Six-Checkpoint Offline Oracle

**Files:**
- Create: `crates/borsuk/src/v36_funnel_eval.rs`
- Create: `crates/borsuk/examples/v36_funnel_oracle.rs`
- Create: `crates/borsuk/tests/v36_funnel_eval.rs`

**Interfaces:**
- Consumes: authenticated model, assignment, coarse plane, fine Arrow chunks, query Parquet, and GT@100.
- Produces: `V36FunnelCheckpoint`, `V36FunnelResult`, `evaluate_v36_funnel`, and canonical result bytes.

- [ ] **Step 1: Write causal checkpoint REDs**

  Freeze route-owner containment before coarse scoring, coarse containment before I/O pruning, post-I/O fine-prefix containment before decode, fine-codec recall next, and same-row-set offline f32 recall last. Mutation-test every aggregate, ordinal list, arm identity, gate, and causal classification. A stage may read only capabilities admitted by its phase.

- [ ] **Step 2: Run the focused RED**

  Run: `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_eval -- --nocapture`

  Expected: missing evaluator/result boundary only.

- [ ] **Step 3: Implement evaluation and canonical serialization**

  Recompute every per-query hit/containment/recall metric and aggregate during serialization. Emit separate `routing-rejected`, `coarse-code-rejected`, `fine-layout-rejected`, `fine-codec-rejected`, `executable-path-required`, and `qualified-for-g2` outcomes; never call containment recall.

- [ ] **Step 4: Run GREEN and affected Rust gates**

  Run evaluator, authority, build, code, and fine tests serially; then scoped Clippy, fmt, and diff-check.

- [ ] **Step 5: Commit the oracle slice**

  Commit the library evaluator, thin example, tests, and exports.

### Task 6: Capability-Separated Launcher and Dataset Freeze

**Files:**
- Create: `scripts/run_v36_funnel_oracle.py`
- Create: `scripts/test_run_v36_funnel_oracle.py`
- Create: `scripts/run_v36_funnel_campaign.py`
- Create: `scripts/test_run_v36_funnel_campaign.py`
- Modify: `docs/research/standard-datasets.md`
- Modify: `docs/research/methods.md`

**Interfaces:**
- Consumes: local registered artifacts and the release-built oracle binary.
- Produces: construction receipt, development result, validation result, sealed-holdout result, and terminal receipt.

- [ ] **Step 1: Write launcher REDs**

  Use subprocess tests to prove construction cannot open query/truth paths, evaluation cannot open source paths, validation requires a development-selected arm, holdout requires that exact validation-passed arm, every input has exact URI/digest/length, unknown flags fail closed, and pressure/timeout stops emit canonical outcome-blind receipts. Campaign dry-run tests require profile `causality`, Spot by default, same-region objects, explicit cost/wall caps, terminal-receipt upload, interrupted-cell discard, and immediate termination.

- [ ] **Step 2: Run the focused RED**

  Run: `python3 -m unittest scripts.test_run_v36_funnel_oracle`

  Expected: missing launcher boundary only.

- [ ] **Step 3: Implement the thin launcher**

  Use explicit file descriptors/capability roots, a single child process group, 2 GiB RSS stop, memory PSI full avg10 stop at 0.5, any swap-growth stop, progress timeout, and an external 7,200-second wall cap. The campaign wrapper streams bounded S3 inputs to ephemeral NVMe on same-region Spot and terminates the instance after syncing its receipt. The Python layer authenticates and orchestrates; Rust performs all scientific scoring.

- [ ] **Step 4: Materialize registered splits and validate authority**

  Consume Task 0's registered membership rule in one bounded, same-region AWS Spot construction cell. Stream the immutable 514M source once in bounded blocks directly from S3, retain no complete corpus on local disk or in RAM, and write the hash-nested 1M/10M/100M Parquet shards plus scale-specific query/GT artifacts back to S3. Pin a dry-run byte/work/cost projection and reject above its registered cap before launch. Record exact URIs, SHA-256, lengths, dimensions, row counts, split seeds, instance/interruption identities, and licensing. Generate GT@100 by a separately authenticated blocked f32 scan in the same bounded campaign. Neither validation nor holdout is opened during development.

- [ ] **Step 5: Run GREEN and static gates**

  Run both complete launcher test files, pinned Ruff, py_compile, docs validator, and diff-check.

- [ ] **Step 6: Commit launcher and registry slice**

  Commit the launcher, tests, and registry/method entries.

### Task 7: G1 Spot 1M Causal Matrix

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md`
- Create: `docs/research/v36-funnel-g1-authority.json`

**Interfaces:**
- Consumes: release oracle, four frozen 1M corpora, and development/validation/holdout capabilities.
- Produces: authenticated per-arm development results, one validation result, and one sealed holdout result.

- [ ] **Step 1: Run sub-second preflight**

  Validate all authorities, projected work, disk/RAM headroom, scalar/SIMD parity, and exact arm count without opening scientific artifacts.

- [ ] **Step 2: Execute development arms with early stops**

  On same-region Spot, sweep the full diagnostic L/K curves through the identical transport planner. Executable evaluation always uses the longest deterministic prefix fitting each wave's fixed object/byte cap and records each dimension/codec raw-capacity ceiling plus useful/fetched bytes. Stop an assignment arm if route containment misses 998,000 ppm inside that envelope; stop a code arm below 998,000 ppm at the consumed K; reject the layout below 998,000 ppm after fine-chunk admission; reject each fine codec independently if loss versus same-row-set offline f32 exceeds 1,000 ppm. Preserve every terminal result and terminate compute after sync.

- [ ] **Step 3: Freeze winner, validate, then open holdout once**

  Require development recall at least 997,000 ppm aggregate and 800,000 ppm minimum; the 2,000-ppm margin applies only to aggregate recall because per-query recall@10 is quantized by 100,000 ppm. Do not add it to traversal or containment thresholds. Apply the exact lexicographic selection rule and serialize the representation winner identity. Evaluate validation once without retuning; only on pass evaluate all 1,000 holdout queries once. Holdout requires 995,000 ppm aggregate and 800,000 ppm minimum recall; failure rejects the architecture.

- [ ] **Step 4: Validate evidence and commit**

  Run docs validator and diff-check, commit only the ledger and canonical authority result, and fast-forward push.

### Task 8: G2 Executable 1M Path

**Files:**
- Create: `crates/borsuk/src/v36_funnel_query.rs`
- Create: `crates/borsuk/tests/v36_funnel_query.rs`
- Create: `scripts/run_v36_funnel_g2.py`
- Create: `scripts/test_run_v36_funnel_g2.py`

**Interfaces:**
- Consumes: unchanged G1 winner and a bounded range-reader with no list/discovery API.
- Produces: query results and counters for recall, GETs, bytes, latency, and RSS.

- [ ] **Step 1: Write query and transport REDs**

  Require identical local/S3-mock results, at most 32 complete-object GETs and 16 MiB, actual HTTP byte accounting, no unselected object access, exact retry accounting, immutable object identity, bounded buffers, and truthful warm/cold labels.

- [ ] **Step 2: Implement the executable path**

  Use at most 16 concurrent coarse-group reads followed by at most 16 complete fine Arrow IPC objects. Authenticate BLAKE3 before decoding and SHA-256 at publication boundaries. Measure compute on all performance queries. Measure hot-object p99 on the first 32 registered performance queries by warming each query's own bounded working set before its 1,024 warmups and 10,000 timed executions; use the 10,000-query second pass for realistic shared-cache behavior. Replay holdout only for offline/executable equality. Never download or persist the corpus.

- [ ] **Step 3: Run 1M G2**

  Run every real-data dimension band admitted by G1. Require recall gates, warm cached p99 at most 15 ms, RSS at most 2 GiB, and the arm's registered GET/byte cap. Report cold S3 separately without a 15 ms promise. Mark any synthetic-only band unqualified rather than promoting it through another band's result.

- [ ] **Step 4: Verify and commit**

  Run affected tests, strict scoped Clippy, full Python discovery, fmt, docs validation, and diff-check; commit/push the coherent G2 slice.

### Task 9: G3 10M Promotion Gate

**Files:**
- Modify: `docs/research/publication-v3-attempt-ledger.md`
- Create: `docs/research/v36-funnel-g3-authority.json`

**Interfaces:**
- Consumes: unchanged G2 implementation and arm.
- Produces: authenticated 10M frontier-growth result.

- [ ] **Step 1: Preflight exact projected work**

  Pin separate 10M development/validation/holdout/performance identities and GT, rows, leaf counts, expected objects, GET/byte maxima, scratch lifecycle, RSS/PSI/swap stops, and terminal output before execution.

- [ ] **Step 2: Run 10M once per registered repetition**

  Replay every L/K diagnostic through the identical transport planner without selecting a new representation. Compute exact integer-prefix `L*` on the same preregistered development query vectors with scale-specific GT. Require `L*(10M) <= 1.6 * L*(1M)`. Compute `Lhat100 = L10^2/L1` and its paired, seed-36, 10,000-resample nearest-rank one-sided 99% bootstrap bound; require that bound to fit the coarse GET/byte envelope. Apply every 10M executable gate to validation without retuning; only then open the 10M holdout once and require 995,000 ppm aggregate and 800,000 ppm minimum recall. Stop immediately on the first failed promotion gate.

- [ ] **Step 3: Validate and commit evidence**

  Preserve immutable results, update the ledger, validate docs/diff, and commit/push. Do not launch G4 on failure.

### Task 10: V36 Delta, Publication, and Compaction Lifecycle

**Files:**
- Create: `crates/borsuk/src/v36_funnel_delta.rs`
- Create: `crates/borsuk/tests/v36_funnel_delta.rs`
- Modify: `crates/borsuk/src/lib.rs`

**Interfaces:**
- Consumes: authenticated V36 generation/chunk manifest, mutation Arrow batches, and conditional object sink.
- Produces: `V36DeltaManifest`, `V36SnapshotVisibility`, `seal_v36_delta`, `publish_v36_delta`, and `compact_v36_generation`.

- [ ] **Step 1: Write lifecycle REDs**

  Port the already-verified V35 latest-wins/base-horizon/run-range authority into breaking V36 identities, then require one process-wide buffer and microsegment sealing/publication every 250 ms or at codec byte capacity, insert/replace/delete visibility, conditional publication conflict and indeterminate-outcome receipts, bounded scratch, chunk sharing, reader-safe reclamation, and checked write-amplification counters. Preserve the exact G1-selected SQ8 or f16 codec for pending and sealed rows. Reserve 32 MiB and derive the checked active row cap as `min(65_536, floor(224 MiB/(fine_row_bytes+120)))`; prove the D3072 caps of 65,536 SQ8 or 37,496 f16 rows. Query nodes ingest immutable published coarse slices into one resident append-only delta arena plus a checked CSR leaf-to-row directory capped at 64 MB for four million double assignments; queries score only selected-leaf delta rows. Cap G5 at 4,000,000 stored mutation entries, route base/pending/delta together, and charge delta fine objects to the same 16-GET/8-MiB fine wave. The three registered compactions coalesce only delta coarse metadata/tombstones/duplicate IDs and share existing fine objects; base absorption is forbidden in G5. Define mutation truth by exact f32 scan of the live snapshot sequence. Mutation-test stale updates, uncovered live rows, later tombstones, 4M-entry admission, codec drift, byte/timer seals, retries, and interrupted compaction.

- [ ] **Step 2: Run the focused RED**

  Run: `CARGO_BUILD_JOBS=1 cargo test -p borsuk --test v36_funnel_delta -- --nocapture`

  Expected: missing V36 lifecycle symbols only.

- [ ] **Step 3: Implement the minimal lifecycle**

  Reuse algorithms, not V35 serialized types. Emit immutable V36 delta routing/coarse/fine microsegments, one complete visibility directory, conditional `HEAD`, and outcome-blind terminal receipts. Seal the shared buffer on timer or checked codec capacity. Compact only delta metadata/coarse slices and share fine objects.

- [ ] **Step 4: Run GREEN and lifecycle static gates**

  Run the focused test, query regressions, scoped Clippy, fmt, and diff-check.

- [ ] **Step 5: Commit the lifecycle slice**

  Commit V36 lifecycle code/tests/exports before any write campaign.

### Task 11: G4/G5 AWS Spot Qualification

**Files:**
- Modify: `scripts/run_v36_funnel_campaign.py`
- Modify: `scripts/test_run_v36_funnel_campaign.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: exact G3 source/implementation/arm identities and the verified Task 10 V36 lifecycle.
- Produces: three terminal 100M query repetitions and three write/compaction repetitions.

- [ ] **Step 1: Write campaign lifecycle REDs**

  Require AWS profile `causality`, Spot request by default, same-region S3, exact source commit and binary digest, terminal receipt upload, interrupted-cell discard/restart, immediate instance termination, cost cap, and no idle retention.

- [ ] **Step 2: Run campaign dry-run GREEN**

  The dry-run prints exact instance candidates, regions, object authorities, commands, expected bytes/GETs, cost envelope, stop rules, and cleanup without creating infrastructure.

- [ ] **Step 3: Execute G4 after explicit preflight record**

  Run three terminal V36 query repetitions on the registered 100M/768D source/query/GT, host, cache, and concurrency. Open the sealed 100M holdout once and require 995,000 ppm aggregate and 800,000 ppm minimum recall. Require RSS below 3 GiB and the 32-GET/16-MiB cap. On the first 32 performance queries require per-query hot-object compute p99 at most 15 ms and at least 3,000 hot-object QPS at concurrency 64. Report concurrency 1/16/64 and the separate 10,000-distinct-query shared-cache/cold metrics without applying the hot QPS gate or relabelling cold S3 as a 15-ms result. Preserve every metric, instance identity, interruption history, and cost.

- [ ] **Step 4: Execute G5 only after G4 passes**

  First measure no-reader saturation at fixed 5k/10k/20k/40k offered-rate steps for 60 seconds each; capacity is the highest zero-rejection step whose visibility p99 is at most one second, and V36 must reach at least 20,000 mutations/s. Discard the saturation cell and restore the exact frozen base. Then, after a two-minute warmup, run 10 minutes at 5,000 accepted mutations/s with eight writers, 64 concurrent readers, and the fixed 70/20/10 mix through delta-only compactions at minutes 3, 6, and 9. Enforce visibility, complete-byte amplification, and concurrent-read gates. The preflight accounts for every initial fine/coarse, visibility, retry, arena, and compaction byte and must project amplification below 3.0. At warmup end, every measured minute, and immediately before/after each compaction, block a snapshot and run exact f32 GT@10 for the fixed 1,000 mutation queries; require 995,000 ppm aggregate, 800,000 ppm minimum, and no deleted result at every pending/sealed/compacted snapshot.

- [ ] **Step 5: Final verification and decision**

  Run one locked workspace/all-targets test, strict workspace/all-targets Clippy, full dependency-complete Python discovery, fmt, docs validation, and diff-check. Commit all terminal evidence. Only then write the breaking production-format plan or record Funnel-3 rejection.
