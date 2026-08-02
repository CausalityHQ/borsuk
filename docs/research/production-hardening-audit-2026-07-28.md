# Production hardening audit — 2026-07-28

This audit separates implemented feature breadth from production and
performance readiness. It records verified code behavior at
`a1864e4a9661f41cd32d6f893a26293629e13137`; hypotheses are labelled and need
measurement before they become product claims.

The release target remains stronger than feature parity: BORSUK must provide
correct dense, sparse, text, hybrid, and late-interaction lifecycle semantics,
then demonstrate competitive writes, reads, recall, memory, and storage
behavior on comparable, disclosed conditions.

## Verified strengths

- The primary and named dense lifecycle supports float32, float16, bfloat16,
  E4M3FN, E5M2, int8, and packed binary vectors.
- Sparse float32/float16, BM25, every non-empty dense/sparse/BM25
  combination, and late-interaction float32/float16 have mutable lifecycle
  coverage.
- The Rust feature matrix passes all nine tests.
- WAL runs are immutable and cell/lane sharded; one fenced transaction marker
  publishes a multi-cell primary append.
- Normal segment and WAL table defaults are Parquet. Two independent
  qualifications rejected Vortex as the automatic default for those object
  roles; Vortex remains an explicit research backend.
- The finalized production dense path is graph-free `srht-pq-scan`: bounded
  IVF routing, paged product-code scans, and exact Arrow-sidecar reranking.
- Publication v7's cache accumulation defect is fixed at `a1864e4`: validated
  hybrid cells delete only their cache/scratch directories, and publication
  execution requires 32 GiB of free disk before each repetition and hybrid
  phase.

## Correctness blockers

### P0: collection-wide multimodal atomicity

Primary and named indexes own independent manifests and WAL transactions.
Add, upsert, delete, flush, compaction, and refresh advance the primary and
children sequentially. A child failure can therefore expose a partially
advanced collection even though public comments describe modality atomicity.

Evidence:

- `crates/borsuk/src/index.rs:2568`
- `crates/borsuk/src/index.rs:2818`
- `crates/borsuk/src/index.rs:2911`
- `crates/borsuk/src/index.rs:2943`
- `crates/borsuk/src/index.rs:3044`
- `crates/borsuk/src/index.rs:6011`
- `crates/borsuk/src/index.rs:7031`

Required gate: a collection transaction/snapshot design plus crash and fault
injection at every primary/child publish and refresh boundary. No production
readiness claim is allowed while partial publication is possible.

Resolved update (2026-07-29): `a679259`, `adca46f`, and their preceding checked
control-record commits establish one collection `CURRENT`, exact
primary/child manifest snapshots, and root-authorized multimodal WAL commits.
Fault injection proves invisibility before the root-head CAS and complete
visibility after it even when post-commit automatic flush fails. The complete
crate suite and all-target Clippy pass on the resolved implementation.

## Resolved correctness gates

### Non-negative sparse retrieval semantics

Commit `69e3146896d95ec840d774fdb87f3b5959a2f52e` resolves the signed
sparse ambiguity by defining the inverted-index retrieval domain explicitly:

- named sparse stored and query weights must be non-negative;
- negative record weights are rejected before publication;
- negative direct and hybrid query weights are rejected before planning; and
- exact sparse results are the top strictly positive inner-product matches.
  Zero-score nonmatches are outside the sparse-match result universe.

The generic `SparseVector` math type remains signed so primary-vector sparse
encoding and dense/sparse metric helpers keep their existing mathematical
behavior. The non-negative restriction applies specifically to named sparse
inverted retrieval. The regression covers rejected writes without visibility,
direct queries, and hybrid queries. The full Rust crate suite and
`clippy --all-targets -D warnings` pass.

Evidence:

- `crates/borsuk/src/index.rs:4498`
- `crates/borsuk/src/index.rs:4985`
- `crates/borsuk/tests/sparse_named_shard.rs:99`

## Scale and efficiency blockers

### P0: WAL memory and read amplification are not collection-bounded

- Flush thresholds apply per logical cell, so total unflushed bytes can scale
  with the number of cells.
- Search decodes every unconsumed record run before selecting the global fast
  path.
- The decoded-tail cache is not byte-budgeted or single-flighted.
- Each query clones the tail into a new newest-generation map/vector.

At the code's 100M/96D sizing, 2,289 logical cells are selected. The
theoretical local-tail envelope is therefore tens of GiB rather than the
nominal 32 MiB cell threshold.

Evidence:

- `crates/borsuk/src/index.rs:5915`
- `crates/borsuk/src/index.rs:6150`
- `crates/borsuk/src/index.rs:6220`
- `crates/borsuk/src/index.rs:6243`
- `crates/borsuk/src/index.rs:10753`
- `crates/borsuk/src/index.rs:18049`

Required gate: query only relevant WAL cells, enforce a collection-wide byte
cap, and single-flight byte-accounted decode. Test many cold cells just below
their local threshold with four concurrent queries and strict RSS/object-GET
bounds.

Implementation update (2026-07-29):

- `1c2f194` replaces the unbounded whole-frontier decoded cache with immutable
  per-run single-flight decode and one 16 MiB retained / 16 MiB in-flight
  runtime shared by primary and named modalities.
- `c9c680e` selects active logical WAL cells before materialization for bounded
  approximate queries. Exact and guaranteed-recall reads intentionally retain
  complete-tail semantics.
- The subsequent aggregate-ceiling checkpoint adds a 256 MiB durable
  collection WAL cap divided across modalities, so many cold cells cannot each
  retain one 32 MiB local allowance.

The implementation blockers in this subsection are closed, but the promotion
gate remains open until the specified many-cold-cell, four-query RSS/GET
measurement runs on the production revision.

### P0: open/refresh coordination reads scale as cells × lanes

Stable snapshot isolation double-collects every cell/lane head with sequential
object-store reads. At 2,289 cells and eight lanes this implies at least
36,624 coordination GETs per modality before frontier, descriptor, and marker
reads.

Evidence:

- `crates/borsuk/src/cell_wal.rs:759`
- `crates/borsuk/src/storage.rs:1760`
- `crates/borsuk/src/index.rs:2186`
- `crates/borsuk/src/index.rs:2568`

Required gate: a sparse active-lane directory or bounded set of aggregate head
shards, measured at 100M for p95 latency, GET count, cost, and concurrent
writer stability.

Implementation update (2026-07-29):

- Reader discovery now uses 64 collection-level frontier shards. A mutation
  reserves its transaction-hashed bounded HEAD before lane preparation,
  publishes all modality descriptors, and acknowledges only after a CAS
  replaces the reservation with their checked collection commit.
- Open/refresh brackets a parallel double-collect of those fixed heads with
  stable `collection/CURRENT` reads and directly reads their embedded active
  commits. Per-cell lane heads remain the
  append/prune/GC layout and are no longer scanned to establish visibility.
- A deterministic object-store counter test proves exactly 128 coordination
  GETs for both one and 10,000 logical cells. Root-head
  fault tests prove pre-publication invisibility; WAL, named-vector,
  concurrent-writer/GC/search, and storage-role suites pass.
- Fully consumed transactions are CAS-rebased out of the collection heads
  only after every modality has materialized them, preventing both
  resurrection and partial-modality hiding.
- Each shard triggers cooperative materialization at eight commits and refuses
  admission at 64 combined reservations and commits. Because commits live in
  the bounded mutable HEAD, no immutable root history accumulates or requires a
  racy GC pass.
- Reservations expire after one hour. Actual GC removes expired reservations,
  verifies a stable root-authorization set around each lane snapshot, and
  detaches unrooted lane runs before object deletion, so process-crash debris
  cannot grow forever.

The implementation blocker is closed. The promotion gate remains open until
the production revision is measured at large scale for p95 open/refresh
latency, request cost, active-chain depth, and concurrent-writer stability on
the target object store.

Qualification update (2026-08-01): the preregistered logical-cell routing v3
campaign reached a terminal failure during the first 2,000-cell, 32-writer
flat-routing cell. The runner reported `ConcurrentModification` at
`collection/wal-frontier/57/HEAD/CAPACITY`; its failure marker is present and
its completion marker is absent. No partial measurement CSV was inspected.
The promotion gate therefore remains open, and bounded-head capacity under
concurrent writers is again an implementation blocker until admission is
made progress-safe and covered by a focused concurrency regression before a
fresh campaign is launched.

Resolution update (2026-08-01, `2f50aa2`): collection transactions now reserve
root capacity before publishing immutable lane history and retry admission
with a fresh transaction id when its candidate frontier shard is saturated.
The transaction id is still retry-safe at this boundary, no mutation payload
exists yet, and the 64-entry per-shard reader bound is unchanged. A
deterministic regression fills one shard to its exact hard limit and proves
that admission selects an available shard without exposing the internal
`CAPACITY` sentinel. The complete `borsuk` crate suite, 32-writer/GC/search
concurrency tests, performance smoke, recall gate, fault injection, and
all-target Clippy pass. Write reports include the early reservation bytes and
bound transient coordination overwrite amplification. The implementation
blocker is closed; the AWS promotion gate remains open pending a fresh
immutable paired campaign from this or a descendant revision.

### P1: memory accounting is not process-wide

The 512 MiB check covers the manifest estimate, while independent default
caches can retain roughly 560 MiB before global PQ, WAL, query working sets,
and named-vector child caches. Children clone options and own separate caches
and admission gates.

Evidence:

- `crates/borsuk/src/index.rs:143`
- `crates/borsuk/src/index.rs:446`
- `crates/borsuk/src/index.rs:523`
- `crates/borsuk/src/index.rs:1266`
- `crates/borsuk/src/index.rs:16186`

Required gate: one collection-level byte governor shared by primary/children,
all caches, WAL, and query reservations, verified with multimodal hybrid load.

Implementation update (2026-07-30, `4d8cd28`):

- one shared collection read runtime now owns the primary and named modalities'
  retained cache pool, transient decode admission, cache instances,
  single-flight maps, count gates, and WAL runtime;
- open preflights the complete pinned collection, while compare-and-swap
  publication and refresh enforce an aggregate estimate carried in checksummed
  collection references for both paged and resident routing;
- dense, projected-vector, graph, sparse/BM25, WAL, and late-interaction decode
  paths take owned transient permits; decoded segment, graph, lexical, sidecar,
  and WAL retention takes owned shared-pool reservations;
- Rust, Python, TypeScript, and CLI telemetry now reports collection resident
  bytes plus retained/transient current, capacity, and peak counters.

The implementation blocker is closed. The promotion gate remains open until
the frozen production revision passes the specified concurrent multimodal AWS
run with governor peaks, process RSS, result equality/recall, latency, QPS, and
object-store requests published together.

### P1: maintenance and flush amplification

- Background maintenance reloads the manifest but can retain a stale WAL
  snapshot when GC is disabled or assigned elsewhere.
- Incremental split/merge reads `manifest.segments`, which is intentionally
  empty for modern paged indexes.
- Flush emits physical segments per selected record run rather than coalescing
  runs by cell toward the target segment size.

Evidence:

- `crates/borsuk/src/index.rs:3358`
- `crates/borsuk/src/index.rs:3486`
- `crates/borsuk/src/index.rs:6024`
- `crates/borsuk/src/index.rs:6076`
- `crates/borsuk/src/index.rs:7073`
- `crates/borsuk/src/index.rs:8116`

Required gates: multi-node remote-tail maintenance, routing-page-aware
incremental maintenance, and coalesced flush measurements covering object
count, PUTs, bytes, flush p95, search GETs, and later compaction amplification.

Implementation update (2026-07-30):

- maintenance refreshes the complete collection snapshot before planning work,
  and incremental split/merge resolves the active segment summaries from
  routing pages rather than assuming resident `manifest.segments`;
- WAL flush now groups selected record runs by logical cell and streams them
  into target-sized batches, retaining at most one bounded pending batch per
  cell instead of emitting one physical segment per immutable run; and
- a regression proves four independent one-record transactions in one cell
  materialize as one segment and remain exact after reopen.

The identified implementation defects are closed. The promotion gate remains
open until the frozen multi-node and flush-amplification measurements publish
object count, PUTs, bytes, flush p95, search GETs, and later compaction
amplification together.

### P1: write routing and explicit-ID concurrency

- Post-freeze ingest routes each vector by a flat scan of every logical-cell
  centroid even though query routing already has a persisted coarse
  quantizer.
- Current explicit-ID ingest evidence is single-writer and is explicitly not
  competitive.

Evidence:

- `crates/borsuk/src/index.rs:5377`
- `crates/borsuk/src/cell_wal.rs:1541`
- `crates/borsuk/tests/cell_wal.rs:417`
- `crates/borsuk/tests/cell_wal.rs:574`
- `docs/research/batch-id-ingest-diagnostic-2026-07-27.md:69`

Required gates: flat versus quantizer routing at 2K/16K cells and 1/8/32
writers; ordered gate-free shard acquisition versus striped gates at
1/8/32/128 writers, including duplicate races and fault recovery.

Implementation update (2026-07-30): explicit-ID batches no longer acquire the
collection-wide claim `GATE`. Writers acquire their fixed claim shards in
ascending order and version-safely release partial acquisitions on contention
or error. Duplicate-race, failed-batch release, stale-checkpoint, crash, and
fault suites remain mandatory. The performance promotion gate remains open
until the frozen 1/8/32/128-writer matrix completes.

Commit `009fcf5` also removes the flat-scan-only write-routing path
for large frozen cell catalogs. Each handle lazily builds one HNSW over the
immutable logical-cell centroids, caches it by routing epoch, and reuses it
across ordinary manifest-version changes. Empty, malformed, and small catalogs
retain the bootstrap or exact flat path. Focused tests pin Euclidean cell
selection, angular query normalization, and cache identity across a manifest
advance; the complete cell-WAL, WAL, crash, and fault suites remain green.
This is an implementation result only. No write-throughput improvement is
claimed before the preregistered 2K/16K-cell and 1/8/32-writer paired matrix.
The preregistered harness uses a hidden open-time flat-routing control, keeping
the source, persisted catalog, WAL append path, and coordination behavior
identical between arms; production defaults always retain quantizer routing.
The completed harness freezes five paired repetitions at 2K/16K cells and
1/8/32 writers, alternates arm order, records every append and storage-request
count, preserves per-cell process resource telemetry, runs duplicate/failure/
crash gates, and validates fail-closed before its completion sync. A separately
labelled 64-cell local smoke passed the complete structural validator; its
measurements are ineligible for product claims.

Qualification update (2026-08-02): v4 became terminal and ineligible during
`c2000/r01/w32/flat`. Two writers returned during warmup/preflight, while the
remaining 30 writers and main thread blocked in a 33-party start barrier. The
main thread entered that barrier before joining writer handles, permanently
hiding both initiating errors. CPU time stayed fixed at 22m39s for more than
three hours; native backtraces then proved the barrier wait. The child was
terminated through the runner, which published `LOGICAL_CELL_ROUTING_FAILED`;
the completion marker is absent. No partial measurement CSV was inspected.

The harness now reports each writer's preflight outcome over a channel and
uses an independent per-writer start channel only after every writer is ready.
Any preflight error or panic closes the start channels, releases already-ready
writers, joins them, and returns the initiating error instead of deadlocking.
Each campaign cell also has a preregistered 1,800-second fail-closed timeout,
with a bounded TERM-to-KILL interval, so a different liveness failure cannot
hold the matrix indefinitely.
Focused regressions cover both partial readiness and cancellation of waiting
writers. A structurally valid local campaign and the fail-closed validator
pass. This closes the harness liveness defect only; the two AWS preflight
errors remain unidentified until a fresh immutable attempt reports them, and
the production performance gate remains open.

Qualification update (2026-08-02, v5): failure-aware startup exposed the next
production gap instead of deadlocking. The first one-writer flat and quantizer
cells completed, but `c2000/r01/w8/flat` reached the frozen 1,800-second cell
timeout and exited 124. Preserved resource telemetry reports 30m00.05s wall
time, 111.53s user time, 47.68s system time, 8% aggregate CPU, and 9,400,048
voluntary context switches. The terminal failure marker is present and the
completion marker is absent, so the fail-closed validator was not run and no
performance comparison is eligible. No partial measurement CSV was inspected.
The evidence establishes an I/O-wait-dominated write-path qualification
failure, but does not localize the remote operation. Add non-measurement
operation-stage progress telemetry and qualify the remote WAL/object-store
path before spending another full paired matrix.

Diagnostic update: the paired runner now emits a low-overhead, non-measurement
stderr heartbeat every 30 seconds. Aggregate started/completed counters cover
index opens, warmup appends, routing precomputation, and measured appends, with
ready/done writer counts. The same atomics and reporter run in both arms; CSV
schemas, operation order, inputs, and measured timing boundaries are unchanged.
Reporter shutdown is channel-driven and emits an immediate final snapshot, so
successful and failed cells do not wait for the heartbeat interval. The local
paired smoke completed with balanced stage counters in both arms and passed the
structural validator. This makes the next bounded AWS diagnostic actionable;
it is not evidence that the remote write-path performance gap is fixed.

The follow-up bounded diagnostic is explicitly claim-ineligible and separate
from the frozen matrix: 2K cells, flat routing, eight writers, two warmups and
five measured appends per writer, one repetition, and a 600-second timeout.
It uses fresh index/result prefixes and distinct diagnostic terminal markers.
A fresh local filesystem execution completed all 40 measured appends with
balanced progress counters and preserved raw and process-resource artifacts.
That proves runner structure only; remote qualification remains required.

AWS diagnostic v1 completed with balanced stage counters and structurally
valid terminal artifacts, but failed the production-viability gate. Forty
successful single-record appends issued 13,362 storage requests and took
21.553 seconds measured wall time with 3.17 CPU-seconds; p50/p95 append latency
was 2.718/6.549 seconds and throughput was 1.856 appends/s. The independent
resource envelope recorded 28.49 seconds elapsed, 19% CPU, and 241,961
voluntary context switches. These claim-ineligible numbers isolate remote
request amplification as the next write-performance blocker; they do not
compare flat routing with the quantizer or authorize product claims.

Implementation update: storage format v19 makes each ID-claim shard a durable
write epoch for every WAL mutation, including generated-ID writes. An absent or
checkpoint-matching shard now proves that no potentially conflicting write has
occurred since the handle's pinned view, allowing an insert-only writer to skip
the 64-shard double collect. A changed shard still forces refresh and duplicate
validation. The cold explicit-ID regression fell from 146 GETs to fewer than
30; a separately opened generated-ID writer still invalidates a stale explicit
writer, which refreshes and rejects the duplicate. Cell-WAL, WAL lifecycle,
crash-recovery, fault-injection, and format suites remain green. This is local
request-bound evidence; fresh EC2/S3 qualification is still required.

AWS diagnostic v2 completed and validated, but format v19 alone did not reduce
the eight-writer bottleneck: 40 appends issued 14,656 requests, throughput was
1.437 appends/s, and p95 was 7.278 seconds. Relative to claim-ineligible v1,
requests rose 9.7%, throughput fell 22.6%, and p95 rose 11.1%; one repetition
per revision is insufficient for stable effect sizes, but the production gate
clearly remains failed. The remaining mechanism is frequent collision in the
16-way claim epoch space, which invalidates other writers and re-enters the
collection-wide refresh path.

Implementation update: format v20 expands claims to 4,096 logical epochs using
12 BLAKE3 digest bits and packs their sparse state into 22 lazily created
coordination pages. Generation allocation remains independently bounded to 16
shards. The deterministic local analogue—eight pre-opened writers performing
40 disjoint single-record appends—is guarded below 1,000 GETs, while a
500-record explicit-ID batch remains guarded below 100 PUTs and the stale
generated-ID/explicit-ID duplicate test still forces refresh and rejects the
conflict. Fresh EC2/S3 evidence remains required before claiming the collision
fix is effective remotely.

### P1: common query features leave the qualified global path

Filters or metadata return disable global PQ and fall back to normal segment
execution. A pre-finalized approximate request with no explicit segment bound
can also be rejected by coarse routing.

Evidence:

- `crates/borsuk/src/index.rs:8875`
- `crates/borsuk/src/index.rs:9872`
- `crates/borsuk/src/record.rs:1987`

Required gate: separate pre-finish, finalized, filtered, metadata-returning,
and post-delta lifecycle benchmarks, followed by filter-aware global serving
or a compact global filter layer.

## Datatype and retrieval performance truth

- Float32 uses portable `wide::f32x8` arithmetic.
- Float16, bfloat16, E4M3FN, E5M2, and int8 exact scoring decode into owned
  float32 buffers before using common float32 kernels.
- Packed binary storage is unpacked bit-by-bit into float32; Hamming/Jaccard
  do not currently use direct packed XOR/AND/popcount kernels.
- There is no explicit runtime multiversion dispatch for AVX2, AVX-512,
  F16C, VNNI, NEON, SVE, or architecture popcount.
- Sparse float16 postings expand to resident float32.
- Late-interaction float16 is scored as float32, and exact MaxSim performs a
  full child search per query token before reranking.
- Exact sidecar candidates are decoded into owned float32 vectors; dense
  candidate sets can decode the full sidecar.

Evidence:

- `crates/borsuk/src/metric.rs:482`
- `crates/borsuk/src/scalar_decode.rs:5`
- `crates/borsuk/src/float8.rs:59`
- `crates/borsuk/src/arrow_vector_sidecar.rs:40`
- `crates/borsuk/src/arrow_vector_sidecar.rs:764`
- `crates/borsuk/src/sparse_index.rs:126`
- `crates/borsuk/src/late_interaction.rs:196`
- `crates/borsuk/src/index.rs:4610`

These are correctness-preserving implementations, not yet evidence of
datatype-specific end-to-end speedups. Promotion requires the frozen ARM/x86
SIMD-on/off matrix in the post-reset plan.

## Benchmark and release ordering

1. Preserve local and cloud evidence.
2. Resolve the collection-wide multimodal atomicity contract with
   fault-injected tests; keep the non-negative sparse retrieval regression
   mandatory.
3. Bound collection-level WAL/open/memory behavior.
4. Fix maintenance, flush, and write-routing/concurrency paths.
5. Qualify filtered/metadata/global query behavior.
6. Run the frozen cross-architecture datatype SIMD matrix.
7. Run production lifecycle and 100M gates from one identified revision.
8. Freeze and run a fresh publication prefix.
9. Independently validate the complete tree before reading aggregate outcomes.

External reported numbers remain context-only. Direct claims require the same
dataset/query cohort, exact ground truth, pinned recall definition, cache
state, hardware/client disclosure, repetitions, and raw sample preservation.
