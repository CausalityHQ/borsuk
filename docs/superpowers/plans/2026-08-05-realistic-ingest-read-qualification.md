# Realistic Ingest and Read Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Qualify and improve BORSUK until durable writes scale from sub-200 ms single-record acknowledgement to competitor-class batched throughput while real 768D and 1536D corpora preserve high recall and low read latency.

**Architecture:** Keep the immutable object-store WAL as the acknowledgement boundary and vary only explicit batching and independent commit lanes. Use official VectorDBBench Cohere 1M/768D and DBpedia OpenAI 1M/1536D artifacts with shipped or recomputed pinned ground truth; build a fresh immutable base per repetition, preserve every raw sample and resource trace, and reject campaigns before reading measurements unless terminal markers and structural validation pass.

**Tech Stack:** Rust, `BorsukIndex`, `GroupCommitWriter`, Arrow/Parquet, Python fail-closed validators, AWS S3 and EC2 via profile `causality`, systemd, official VectorDBBench datasets.

## Current checkpoint (2026-08-06)

- The immutable base plus materialized-delta query path is the selected serving
  architecture. Revision `771e29b` added a versioned coverage certificate and
  removed one full routing-tree validation walk.
- Terminal local r16 preserved recall@10 1.0 and measured 165.893 ms post-drain
  read p95, but active-tail read p95 remained 202.953 ms and drain-inclusive
  ingest remained 2,714 records/s. The production gates therefore remain open;
  BORSUK is not yet qualified at 100M vectors.
- The active implementation slice makes current-manifest coverage authoritative
  in both the outer global dispatcher and recursive materialized-delta search,
  while retaining checksum-walk validation for stale coverage. Its focused RED
  reproduction was 11 GETs plus 5 HEADs; GREEN is 7 GETs plus 1 HEAD with delta
  correctness, shared budgets, and WAL-only telemetry preserved.
- After this checkpoint is committed and fast-forward pushed, do not infer 100M
  readiness from the 1M local run. Resume with a fresh terminal local causal run
  from the exact committed revision. If active read p95 is still at or above
  200 ms, prioritize bounded searchable quantized WAL extents; if
  drain-inclusive throughput remains below 10,000 records/s, remove synchronous
  drain/global-delta construction from the acknowledgement workload before AWS
  scale qualification.
- Terminal r17 from `d4b5ee8` closes the local read gates: recall@10 remained
  1.0, active-tail p95 fell to 97.701 ms, and post-drain p95 fell to 3.137 ms.
  Drain remained 5.572 s (2,772 end-to-end records/s). Phase profiling found a
  redundant 1.919 s persisted fallback-quantizer rebuild immediately before a
  separate 1.847 s global-delta build. The TDD r19 factor invalidates the stale
  fallback reference in the segment publication and lets exact/metadata-aware
  searches fall back to current routing pages until maintenance rebuilds it.
  It reduced drain to 3.857 s and raised end-to-end throughput to 3,893
  records/s without changing recall or read gates. Keep this factor; next fuse
  segment/global-delta construction and avoid serial manifest republishes.

## Global Constraints

- Do not create pull requests; commit verified slices and fast-forward push directly to `origin/main` without force.
- Use AWS profile `causality`; verify no competing workload before launch.
- Never inspect an incomplete cell's measurement CSV. Monitor only phase markers, process/service health, and resource telemetry until that cell is terminal.
- Preserve frozen source archives, manifests, raw artifacts, dataset descriptors, SHA-256 identities, and resource traces.
- Use five repetitions for a publication campaign; three repetitions are sufficient only for a labelled architecture qualification.
- Report single-record latency and batched throughput as separate workload points. Do not extrapolate either one from the other.
- Compare external products only with commercial first-party or paper numbers, or with honest paired reproductions under disclosed equivalent conditions.
- Production gates are write p95 below 200 ms, recall@10 at least 0.95 at the selected serving point, and read p95 below 200 ms. The initial durable bulk-throughput parity target is 10,000 vectors/s at 768D.

---

### Task 1: Make durable batch size and mutation count explicit

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Modify: `scripts/validate_benchmark_artifacts.py`
- Test: `scripts/test_validate_benchmark_artifacts.py`
- Modify: `docs/research/market-benchmark-matrix.md`

**Interfaces:**
- Consumes: `BORSUK_BENCH_WRITE_BATCH_SIZE`, `BORSUK_BENCH_WRITE_OPS`.
- Produces: `configured_batch_records` in `bench_write_costs.csv` and `bench_lifecycle.csv`, plus unchanged raw `bench_write_samples.csv` rows.

- [x] **Step 1: Add RED tests for explicit batch length and fail-closed mutation count**

  Assert `write_batch_len(5000, 4900, 1024) == 100`, default one-million-row mutations equal 50,000, an explicit 3,200 remains 3,200, and an override above the dataset size fails.

- [x] **Step 2: Run the RED test**

  Run: `cargo test -p borsuk --example production_bench lifecycle_write_batch_size_is_an_explicit_experiment_factor --no-run`

  Expected: compile failure because `write_batch_len` does not exist.

- [x] **Step 3: Implement and record the two experiment factors**

  Parse positive `BORSUK_BENCH_WRITE_BATCH_SIZE`, optional positive `BORSUK_BENCH_WRITE_OPS`, use the batch size for mutation source decoding and insert/upsert/delete calls, and reject a mutation count above the pinned source size.

- [x] **Step 4: Require the batch factor in structural validation**

  Add `configured_batch_records` to the required aggregate write and lifecycle columns and update the validator fixture.

- [x] **Step 5: Verify**

  Run: `cargo test -p borsuk --example production_bench`

  Run: `cargo clippy -p borsuk --example production_bench --all-features -- -D warnings`

  Run: `PYTHONPATH=scripts python3 -m unittest scripts.test_validate_benchmark_artifacts scripts.test_validate_research_docs`

- [x] **Step 6: Commit**

  Commit: `464a8b9 bench: expose durable write batch factors`

### Task 2: Add a bounded insert-only qualification phase

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Modify: `scripts/validate_benchmark_artifacts.py`
- Test: `scripts/test_validate_benchmark_artifacts.py`
- Test: `scripts/test_bench_production_lifecycle_aws.py`

**Interfaces:**
- Consumes: `BORSUK_BENCH_INSERT_ONLY=1`, a previously built immutable base URI, `BORSUK_BENCH_WRITE_BATCH_SIZE`, and `BORSUK_BENCH_WRITE_OPS`.
- Produces: `bench_write_costs.csv`, `bench_write_samples.csv`, `INSERT_VISIBILITY_COMPLETE`, and no fabricated fully-indexed or consolidation fields.

- [x] **Step 1: Write a RED phase-selection test**

  Add `validate_insert_only(insert_only, build_only, read_only)` and assert insert-only rejects build-only/read-only combinations but accepts an ordinary mutable run.

- [x] **Step 2: Run the RED test**

  Run: `cargo test -p borsuk --example production_bench insert_only_is_a_distinct_mutation_phase --no-run`

  Expected: unresolved import/function failure for `validate_insert_only`.

- [x] **Step 3: Extract aggregate/sample artifact writing**

  Move the existing write-cost and write-sample serialization into `write_cost_artifacts(config: &ResolvedConfig, rows: &[WriteRow]) -> BenchResult<()>` without changing headers or numeric formulas.

- [x] **Step 4: Implement insert-only execution**

  After build-only handling and before recall/query phases, open the base, call `measure_inserts`, validate all sampled inserted IDs with one `get_records` call, write only the insert row/raw samples, create `INSERT_VISIBILITY_COMPLETE`, and return.

- [x] **Step 5: Verify**

  Run: `cargo test -p borsuk --example production_bench`

  Run: `cargo clippy -p borsuk --example production_bench --all-features -- -D warnings`

- [x] **Step 6: Commit and fast-forward push**

  Commit message: `bench: isolate durable insert qualification`

### Task 3: Add a fail-closed realistic durable-write campaign

**Files:**
- Create: `docs/research/realistic-durable-write-campaign.json`
- Create: `scripts/bench_realistic_durable_write.sh`
- Create: `scripts/validate_realistic_durable_write.py`
- Create: `scripts/test_validate_realistic_durable_write.py`
- Create: `scripts/test_bench_realistic_durable_write.py`

**Interfaces:**
- Consumes: frozen source SHA-256, dataset descriptor SHA-256, fresh S3 base/result prefixes, batch sizes `1,32,128,1024`, and repetitions.
- Produces: per-cell phase markers, raw write samples, resource telemetry, aggregate summaries, `REALISTIC_DURABLE_WRITE_COMPLETE`, or `REALISTIC_DURABLE_WRITE_FAILED`.

- [x] **Step 1: Write RED validator fixtures**

  Cover missing root completion, a root failure marker, source/dataset/manifest hash mismatch, missing raw samples, unreconciled batch counts, batch records above the configured factor, write p95 at or above 200 ms, visibility below 1.0, and a missing resource exit status.

- [x] **Step 2: Run RED tests**

  Run: `PYTHONPATH=scripts python3 -m unittest scripts.test_validate_realistic_durable_write`

  Expected: import failure for `validate_realistic_durable_write`.

- [x] **Step 3: Implement the validator**

  Require each planned `(dataset, repetition, batch_records)` cell, exact manifest copy, exact hashes, at least 100 raw durable batches, p95 below 200 ms, all sampled inserts visible, reconciled object-store request totals, time-aligned resource rows, and successful process exit.

- [x] **Step 4: Implement the runner**

  Refuse reused outputs and indexes; create phase markers only after successful phases; wrap each cell with `benchmark_with_resources.py`; upload raw artifacts after the cell becomes terminal; install a trap that writes/uploads the root failure marker.

- [x] **Step 5: Run local structural smoke**

  Use a generated 64-row/8D local Parquet fixture, two batch factors, and one repetition. Run the validator over the terminal smoke directory.

- [x] **Step 6: Commit and fast-forward push**

  Commit: `6f5ba5d bench: preregister realistic durable writes`

### Task 4: Pin real 768D and 1536D datasets

**Files:**
- Modify: `scripts/fetch_vdbbench_dataset.py`
- Test: `scripts/test_fetch_vdbbench_dataset.py`
- Modify: `docs/research/reproducibility.md`

**Interfaces:**
- Consumes: official unshuffled VectorDBBench Parquet objects and the public DBpedia OpenAI source.
- Produces: `dataset.json`, `meta.json`, original Parquet files, per-file bytes/SHA-256, aggregate source SHA-256, row/dimension/query/neighbor validation.

- [x] **Step 1: Finish and validate Cohere Medium 1M/768D**

  Require 1,000,000 train rows, 768 float32 dimensions, aligned test/neighbor counts, at least ten ground-truth neighbours, original filenames, and immutable hashes.

- [ ] **Step 2: Add a RED DBpedia descriptor test**

  Assert the alias identifies 1,000,000 source rows, 990,000 indexed rows after the seeded 10,000-query split, 1,536 dimensions, cosine distance, source URL, license, and exact preparation parameters.

- [ ] **Step 3: Implement checksum-pinned DBpedia preparation**

  Preserve source embeddings, seed and split identities, recompute exact ground truth only for the pinned indexed/query split, and record preparation CPU/time separately from BORSUK build/query timing.

- [ ] **Step 4: Verify and commit**

  Run: `PYTHONPATH=scripts python3 -m unittest scripts.test_fetch_vdbbench_dataset scripts.test_market_benchmark_matrix`

  Commit message: `data: pin realistic embedding workloads`

### Task 5: Qualify commit-lane scalability without weakening durability

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`
- Create: `docs/research/realistic-group-commit-campaign.json`
- Modify: `scripts/bench_group_commit_scalability.sh`
- Test: `scripts/test_bench_group_commit_scalability_runner.py`

**Interfaces:**
- Consumes: Cohere train Parquet, worker lanes `1,2,4,8`, writers `1,8,32`, pipeline depth `4`, 768 dimensions, and real vectors.
- Produces: per-ticket durable latency, acknowledgement and drain-inclusive records/s, MiB/s, group size, lane, request counts, visibility, and sampled resource telemetry.

- [x] **Step 1: Write RED tests for dataset-backed tickets and lane factors**

  Require the runner to reject random-vector production mode, require the dataset descriptor/hash, and enumerate worker lanes independently of writer count.

- [x] **Step 2: Implement bounded Parquet vector streaming**

  Decode vectors before starting each ticket's latency clock, retain original vectors only until visibility validation, and never include dataset I/O in durable acknowledgement latency.

- [x] **Step 3: Preserve safety invariants**

  Run prepare-failure, crash-recovery, same-ID last-write-wins, drain barrier, and request-reconciliation gates for every worker-lane factor.

- [ ] **Step 4: Verify and commit**

  Run: `cargo test -p borsuk --example group_commit_bench`

  Run: `cargo test -p borsuk --test group_commit`

  Run: `cargo clippy -p borsuk --all-targets --all-features -- -D warnings`

  Commit message: `bench: qualify realistic commit lanes`

  The initial three-repetition burst protocol was rejected after a GPT-5.6 Sol
  adversarial review: it excluded final drain from throughput, changed record
  IDs across lane treatments, omitted the production-default eight-lane factor,
  and trusted derived summaries. The replacement preregistration uses five
  repetitions, identical record IDs, `1/2/4/8` lane factors, 1,000 operations
  per writer to cross repeated materialization boundaries, raw-derived
  validation, and a 10,000 vectors/s drain-inclusive gate for 32-writer cells.
  Its synthetic-base inserted-ID check remains visibility evidence only;
  production corpus recall/read latency remains Task 7. A factor-spanning
  integration gate now verifies acknowledged reopen visibility, sequential
  last-write-wins, and drain/reopen correctness for `1/2/4/8` worker lanes.

### Task 6: Run isolated AWS qualifications and react to terminal evidence

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Create: `docs/research/realistic-durable-write-attempt-ledger.md`
- Modify: `docs/research/market-benchmark-matrix.md`

**Interfaces:**
- Consumes: committed source revision, frozen manifests, dataset hashes, fresh prefixes, idle Causality host.
- Produces: immutable terminal artifacts and exact failure/root-cause records.

- [x] **Step 1: Verify launch preconditions**

  Fetch `origin/main`; verify exact source archive SHA-256 and manifest SHA-256 remotely; require empty result/index prefixes, no benchmark process, at least 32 GiB available memory, and enough disk for dataset plus peak scratch.

- [x] **Step 2: Launch sustained architecture qualification**

  Run Cohere 1M/768D batch and commit-lane matrices from a committed source. Preserve raw artifacts and resource telemetry.

- [ ] **Step 3: Monitor periodically**

  Use an attached 15-minute shell sleep, observe its completion, then check markers, systemd/process health, memory, disk, and newly terminal cells. Never read an active cell's CSV.

- [ ] **Step 4: Validate terminal artifacts before measurements**

  Run repository fail-closed validators. On failure, record exact markers and diagnose from logs/resource/request telemetry; on success, inspect raw/aggregate results and report uncertainty.

- [ ] **Step 5: Iterate causally**

  Change one architecture factor per campaign. Any storage/layout change gets a new format marker and fresh base indexes; old artifacts remain historical evidence only.

  v30 failed only the explicit write-p95 gate at the first one-writer cell.
  The next architecture replaces the two-serial-PUT lane acknowledgement with
  one conditional HEAD containing the complete inline block. Background spill
  externalizes inline blocks before a fenced HEAD replacement. Pure LIST
  discovery is rejected because it needs a larger epoch-sealing and zombie-
  fencing protocol; acknowledgement-before-asynchronous-HEAD is the same LIST
  design in disguise.

  Format v26 now makes that boundary explicit. Index creation writes every
  empty ownership-lane HEAD before publishing `CURRENT`, and readers fail
  closed if any HEAD is later missing. Each foreground group acknowledges one
  inline HEAD CAS. At 8 MiB of inline payload, the owning worker returns
  the receipt first, uploads checksum-addressed blocks, then installs external
  descriptors with one fenced HEAD CAS; a failed upload leaves the inline copy
  authoritative and must be retried before that lane accepts another append.
  Raw group samples now record exact acknowledgement HEAD bytes, and the
  validator reconciles total and maximum bytes from those samples. Every cell
  also preserves the complete physical storage trace and fails above the
  preregistered 16x write-amplification ceiling. Pre-drain refresh-plus-search
  probes expose mutable-HEAD read cost under the same 200 ms p95 bound, factor
  rotations cover every lane position, and spill-failure gates are part of the
  terminal correctness artifact. Multi-lane API failures now return structured
  committed and failed lane sets. The local structural smoke, full Rust suite,
  448 Python tests, strict Clippy, formatting, and policy checks passed at the
  committed v31 source revision.

  v31 then terminated in its first `c2000/r01/l1/w1` cell after every phase
  marker completed. Fail-closed root and terminal-cell validation rejected the
  campaign before measurement inspection. The complete terminal cell recorded
  1,000 records in 836 groups, 2.210 acknowledged records/s, 1.849
  drain-inclusive records/s, 603.825 ms write p95, 951.618 ms active-tail read
  p95, 73.770 ms post-drain read p95, and inserted-ID recall@10 of 1.0. Its
  physical trace recorded 920,705,087 write bytes for 3,072,000 input vector
  bytes (~299.71x). Causal inspection found a synchronous four-block spill in
  the owning append worker and a corpus-wide global-PQ rebuild during drain.
  The next revision removes the tiny-block trigger and the drain rebuild. It
  does not claim the still-open whole-query base/delta/WAL budget, bounded
  delta-compaction, or fully asynchronous spill gates identified by the
  subsequent GPT-5.6 Sol review. Background materialization demand is now
  sticky: a threshold crossed during an active pass schedules another pass
  before the sole worker relinquishes ownership, with deterministic state-race
  tests covering both arrival-during-pass and arrival-after-exit orderings. The
  materialized-delta query view also strips both WAL implementations and its
  shared live-tail cache: the outer query remains the single owner of exact
  fresh-record scoring. A differential regression proves one newly published
  WAL record adds exactly one scored record when a stable global base and a
  materialized delta coexist. Materialized-delta segments are now exact-searched
  as a correctness overlay and charged first against one declared
  `max_segments` budget; the stable resident base receives the remainder, and
  the query fails closed if no base probe remains. A real base-plus-two-delta
  regression previously measured six searched segments under a limit of four
  and now remains within four while returning the fresh nearest record. Shared
  byte budgeting and a hard materialized-delta bound remain open architecture
  gates. The resident-global base now honors its own best-effort `max_bytes`
  boundary between code-read waves: it always permits one useful chunk, stops
  before scheduling the next wave after exhaustion, reranks that useful work,
  and reports `MaxBytes` with the actual overshoot. A one-byte regression used
  to scan the full eight-chunk probe set and now scans exactly one. Delta/WAL
  byte charging and resident-global latency-budget enforcement remain open.

  The unbounded materialized-delta gate is now frozen to a two-ANN-layer
  design. A manifest may reference one stable base artifact and at most one
  delta artifact; segments covered by neither are the exact fringe. Delta
  coverage must be disjoint from base coverage, every referenced checksum must
  still be active, and a nested delta is invalid. The embedded reference graph
  carries required layout marker `1`, so unreleased single-layer JSON is
  rejected with a rebuild instruction instead of being guessed compatible.
  Artifact objects are written content-addressed before the manifest CAS and GC
  traces both descriptors and all of their chunks/graphs. A subsequent
  first-principles check rejected geometric full-delta rebuilds: a growing
  trigger permits a growing exact fringe, while a fixed trigger repeatedly
  rebuilds the growing delta and becomes quadratic. Instead, the delta trains
  codebooks once after a bounded bootstrap fringe, then maintenance encodes
  only new vectors into immutable chunks and publishes a new content-addressed
  descriptor that reuses every old chunk. The packed location layout remains
  valid while its declared segment/row bit capacity holds; crossing that remote
  boundary requires a deliberate rebuild and new evidence. Query work remains
  one stable ANN, one appendable delta ANN, one bounded bootstrap fringe, and
  one WAL overlay under shared segment/candidate/byte/deadline accounting. A
  GPT-5.6 Sol review found that immutable appended generations were filtered
  only after the top-k distance boundary, so stale near-query rows could hide
  live neighbours. The persisted regression now repeats an upsert across delta
  appends and proves MVCC filtering and identity deduplication happen before
  that boundary. The same review correctly identified stale delta checksums
  after compaction and the still-open unbounded growth/retraining problem.

  The append primitive now reconstructs both scan and coarse quantizers from
  the persisted descriptor, rejects any new segment/row ordinal that exceeds
  its packed-location capacity, preserves every old chunk reference byte for
  byte, and validates contiguous appended row ranges through the normal
  descriptor constructor. Real sidecar tests cover quantizer-identical encoding,
  layout overflow, old-chunk reuse, and descriptor serialization. Index-level
  WAL materialization now bootstraps after one configured segment capped at
  1,024 vectors, then publishes append descriptors that preserve the old chunk
  prefix and encode only uncovered segments. A one-record regression proves the
  system does not freeze a degenerate one-cell quantizer; reaching the local
  16-vector bootstrap creates coverage for both fringe segments and the next
  flush appends a third. Refresh is an optimization after the durable segment
  manifest and cannot turn a successful flush into a reported failure. Both
  flat and paged compaction now rebuild the delta from the replacement segment
  set and publish coverage atomically; a persisted regression verifies that no
  compacted checksum remains referenced. The hard rollover is geometric: once
  the materialized delta plus uncovered fringe reaches half the stable base's
  vector count, the background refresh writes one content-addressed ANN over
  the active segment set, atomically promotes it to the base, and resets the
  delta. Below that boundary refresh remains append-only. This caps the two ANN
  layers at 1.5x the stable vector count and makes every full retraining grow
  the base by at least 50%, avoiding fixed-size quadratic rebuilds without
  moving work onto the durable foreground acknowledgement path. Base and delta
  ANN execution now subtract predecessor bytes and elapsed time before entering
  the recursive layer, stops instead of constructing an invalid zero budget,
  and preserves the layer that exhausted the request in the merged termination
  report. Search now scores the bounded live WAL once and reserves its exact
  persisted cell-run and lane-block bytes plus elapsed time before entering the
  immutable layers. If that consumes the request, the fresh WAL top-k returns
  directly with degraded recall and no immutable reads; otherwise base and
  delta receive only the remainder. Lane-log snapshots retain exact descriptor
  byte counts and expose their lanes, runs, and records in query telemetry.
  The shared-budget slice passed 477 runnable library tests, all 27 real
  group-commit integration tests, strict all-target/all-feature Clippy, and
  formatting at checkpoint `29a8711` plus its mechanical lint cleanup.
  Realistic latency and recall qualification remains open.

  A terminal local Cohere Medium 1M/768D qualification at checkpoint
  `2041b34` isolated the resident-global scan amplification. The scan-only
  control used 256-byte SRHT-PQ codes. At candidates 320, nprobe 8/16/32
  produced recall@10 0.906/0.978/0.997 and disk-cached p95
  158.686/237.433/307.617 ms while reading 35.52/62.46/108.54 MB per query.
  Bytes tracked selected physical chunks, and source arithmetic accounts for
  approximately 1.42 MB of product codes per full 5,461-row chunk before
  identity and exact-vector reranking.

  The existing per-cell graph path was then rebuilt as a one-factor arm with
  degree 32, construction ef 128, and a declared 512 MiB decoded graph cache.
  The fresh index added 400,665,664 graph bytes and took 678.381 seconds for
  final ANN construction after 151.222 seconds of ingest. The terminal
  100-query artifact passed the repository structural validator. Its
  disk-cached graph coverage was exactly 1.0 at every point, so fallback scans
  cannot explain the result. At nprobe 8/16/32 it produced recall@10
  0.874/0.939/0.956, p95 156.227/245.966/412.460 ms, and
  20.79/35.33/64.34 MB per query. The first recall-qualified point therefore
  missed the latency gate by more than 2x. Per-cell graphs are rejected as the
  production default for this format; their per-chunk traversal and identity
  reads do not remove enough work. The next causal factor is reducing the
  oversized 256-byte scan code while keeping the same coarse routing and exact
  rerank contract.

  Terminal same-corpus code-width arms then established that factor causally.
  A 128-byte SRHT-PQ index preserved recall@10 0.975 at nprobe 16 and
  candidates 128 while reducing disk-cached p95 to 151.363 ms, but concurrency
  saturated near 25 QPS and p95 exceeded 200 ms at two clients. The 64-byte
  index completed ingest plus ANN construction in 311.126 seconds. Its
  terminal recall curve passed structural validation and found the minimal
  qualified point at nprobe 16/candidates 128: recall@10 0.952 and
  disk-cached p95 24.960 ms. A separate terminal serving run at that exact
  point also passed structural validation. Its homogeneous disk-cached row
  recorded recall@10 0.952 and p95 131.994 ms. The raw concurrency artifact
  recorded 72.781/78.493/80.920/80.628/81.192 QPS and p95
  26.981/54.105/79.862/143.261/240.922 ms at 1/2/4/8/16 clients,
  respectively, with 12,540,249.64 average measured bytes per query in the
  concurrency phase. The serving point therefore satisfies the local recall
  and read-p95 gates through eight clients, but 16-client tail latency and the
  approximately 81 QPS saturation remain explicit scalability gaps. No
  production default is frozen from this single local repetition.

  The next source slice promotes that terminal evidence only for the matched
  regime: adaptive 768D angular indexes with at least 100,000 vectors resolve
  to a 64-byte SRHT-PQ code, 16 flat-cell probes, and a 128-row exact rerank.
  Explicit code-width overrides remain authoritative, and the independently
  measured 960D Euclidean GIST default remains 256 bytes. A TDD regression
  failed before the metric-aware resolver existed and now pins all four
  decisions. Publication defaults remain unfrozen until repeated qualification
  from the committed revision; this source selection is the architecture
  candidate, not a publication claim.

  Control-plane checkpoint (2026-08-06): the already-active isolated-target
  release build for `production_bench` completed successfully at commit
  `740f27a` in 9m18s. It used
  `/data/target/agents/borsuk-prod-ready-v9`; `/data` retained 68 GiB free at
  completion. No benchmark or further qualification arm was launched. The
  preserved terminal code64 artifacts and results above remain the latest
  measurement evidence for restart/resume.

  The first committed-default repetition at `da2fad1` completed its fresh
  Cohere Medium 1M build in 318.792 seconds, but fail-closed build validation
  rejected it before any read phase. The writer reported 6,234,415 bytes of
  collection metadata plus two 268,434,876-byte runtime capacities under a
  536,870,912-byte budget. The same validator reproduces the defect on the
  earlier explicit code64 build: creation split the nearly empty manifest's
  budget once, while later manifest publication grew resident metadata without
  resizing the clone-shared runtime. The terminal arm is preserved with
  `LOCAL_BUILD_VALIDATION_FAILED`; it is not qualification evidence.

  The causal correction gives each finite runtime an immutable metadata
  partition equal to at least one quarter of its total RAM budget and splits
  only the remainder between retained and transient work. Publication and
  refresh reject metadata that would cross that partition, avoiding any race
  with outstanding clone-shared permits. At 512 MiB this leaves 128 MiB for
  manifests/routing/cell centroids and 384 MiB for serving work; the raw
  preregistered 16K-by-768D float32 centroid matrix is 49,152,000 bytes before
  bounded routing metadata. A public TDD regression failed
  with the exact overcommit and then passed. The complete 483-test library
  gate (477 passed, 6 ignored), all Rust integration-test binaries, the
  named-vector memory suite, strict all-target/all-feature Clippy, and
  formatting passed. A fresh build from the corrected commit remains required
  before read qualification resumes.

  The fresh committed-default Cohere Medium 1M build at `9b437a7` then reached
  `LOCAL_BUILD_COMPLETE` and passed the repository's fail-closed artifact
  validator. It indexed all 1,000,000 768-dimensional vectors in 162.312
  seconds and completed compaction in 155.396 seconds. The resulting active
  index is 6.701 GB. Under the 512 MiB governed RAM budget, collection metadata
  was 6,234,415 bytes and the corrected retained/transient capacities were
  201,326,592 bytes each, with no measured retained or transient overrun. This
  closes the mutable-manifest budget defect, but it is a build-only repetition:
  it does not qualify recall, read latency, concurrent scalability, or the AWS
  write path. Per the control-plane checkpoint instruction, no subsequent read
  or qualification arm was started in this session.

  The corrected read arm for that index subsequently reached
  `LOCAL_READ_COMPLETE` and passed fail-closed structural validation. At the
  persisted 16-probe/128-candidate serving point it measured recall@10 0.952,
  27.065 ms disk-cached p95, and 70.778/76.065/74.889/71.341/78.375 QPS at
  1/2/4/8/16 clients. Corresponding p95 latency was
  30.076/48.373/83.292/155.823/249.069 ms, so the 16-client scalability gate
  failed. Each query selected 42.44 physical chunks and measured about 12.54
  MB from the all-cached concurrency profile. Descriptor diagnostics found a
  73,515-row largest cell, 18.8 times the 3,906-row mean, identifying
  undertrained coarse routing rather than CPU count as the primary excess-work
  cause.

  Commit `e8713bf` increases the dimension-byte-bounded coarse-training
  reservoir from 16 MiB to 64 MiB: 768D training rises from 5,461 to 21,845
  rows while the source and rotated copies remain bounded by 128 MiB inside
  the 192 MiB transient partition. Its fresh Cohere 1M build completed ingest
  plus compaction in 313.957 seconds and passed fail-closed validation. A
  terminal boundary sweep found the first measured qualified serving point at
  28 probes/128 candidates: recall@10 0.952 and 19.263 ms disk-cached p95.
  The terminal concurrency arm at that point measured
  88.977/98.331/102.411/101.622/101.157 QPS and
  16.926/44.357/62.147/95.353/192.205 ms p95 at 1/2/4/8/16 clients. It selected
  38.79 chunks and measured about 9.47 MB per all-cached query. Thus this single
  local architecture repetition preserves the recall gate, passes the read
  p95 gate through 16 clients, and improves 16-client throughput by about 29%
  versus the corrected baseline. One earlier identical 32-probe arm contained
  a terminal 16-client 952 ms outlier; its repeat was 210 ms, so no publication
  claim or frozen default is inferred from one repetition. Commit `c803a27`
  persists the measured 28-probe default. The complete 483-test library gate,
  strict all-target/all-feature Clippy, formatting, and diff checks passed
  before both source commits were fast-forwarded to `origin/main`.

- [ ] **Step 6: Run five repetitions at the selected revision**

  Freeze defaults only after write latency/throughput and realistic read recall/latency gates pass in the architecture qualification.

### Task 7: Production-read qualification and completion audit

**Files:**
- Modify: `docs/research/market-benchmark-matrix.md`
- Modify: `docs/research/publication-notes.md`
- Modify: `docs/research/production-hardening-audit-2026-07-28.md`

**Interfaces:**
- Consumes: frozen Cohere/DBpedia indexes, shipped/recomputed ground truth, uncached/disk-cached/mixed cache profiles, concurrency `1,2,4,8,16`.
- Produces: recall@10 curves, p50/p95/p99, QPS, GETs/bytes, memory, index size, startup, drain/indexing costs, and defensible production claims.

- [ ] **Step 1: Run recall/candidate curves**

  Select a serving point only where recall@10 is at least 0.95; retain the full curve and exact control.

- [ ] **Step 2: Run cache and concurrency profiles**

  Measure uncached, disk-cached, and 0/25/50/75/100 mixed coverage with independent cache directories and raw per-query samples.

- [ ] **Step 3: Audit every objective requirement**

  Require: sub-200 ms write p95 for the declared single/batched points, at least 10,000 vectors/s durable 768D bulk ingest or an explicitly documented unresolved gap, sub-200 ms read p95 at the selected cache/concurrency scope, recall@10 at least 0.95, bounded memory, recoverability, and complete public API/documentation gates.

- [ ] **Step 4: Commit and fast-forward push terminal history**

  Commit message: `docs: record realistic production qualification`
