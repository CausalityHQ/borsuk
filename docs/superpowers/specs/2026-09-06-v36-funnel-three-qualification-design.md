# V36 Funnel-3 Architecture Qualification Design

## Objective

Qualify or reject a dimension-independent, S3-native three-stage vector-query
funnel before changing the production format. The candidate must reach
competitive recall on real high-dimensional data, keep the resident serving
index below 3 GiB at 100 million rows, bound remote work to two waves of at
most 16 GETs and 8 MiB each, and preserve append-only writes with observable
snapshot semantics.

This is a research qualification, not a release claim. BORSUK is pre-release:
the winning design may introduce a breaking format with no legacy reader or
migration path. Sub-second contracts run locally; corpus work runs on bounded
same-region AWS Spot capacity under profile `causality`. No 100M build starts
until one frozen arm passes 1M and 10M without retuning.

## Evidence and unresolved mechanism

Authenticated results establish both the opportunity and the risk:

- the 100M x 96D production baseline reached 0.992 recall, 44.4 ms cached p95,
  910.5 ms uncached p95, 70.82 GETs, 76.7 MiB/query, and 559 MiB RSS;
- the strongest local 9.99M Deep Image result reached 997,708 ppm aggregate,
  800,000 ppm minimum, and 11.47 ms p99, but did not execute its page stage;
- the strongest exact S3 result reached 320/320 hits at 100K rows with 16
  pages, 2,928,808 returned bytes, and 144.07 ms cold p99;
- V32 at 1M showed 302/320 hits with 16 pages and 320/320 with 64 pages, using
  3.1 versus 12.5 MiB/query and roughly 85--180 ms median;
- the 1M rank-four lower-tail shape diagnostic improved 1,277/1,280 centroid
  owner hits to 1,280/1,280 on a burned cohort, but has no fresh high-D result;
- exact global ADC over the historical width-12 PQ plane reproduced the routed
  result, so that generation was representation-limited rather than router-
  limited;
- the 1M Cohere-768 write cell sustained 46,656 records/s drain-inclusive with
  66 ms write p95, two PUTs and zero GETs per acknowledgement, but did not
  establish distributed compaction or mixed-load behavior.

The repeated failure is candidate starvation combined with lossy coarse
ordering and poor physical locality. Tiny 256-primary leaves create 390,625
leaves at 100M, make boundary ownership brittle, and provide too few samples
for useful covariance estimates. Merely increasing shape complexity cannot
repair an insufficient unique row set. Conversely, larger replicated postings
can exceed byte, write-amplification, or construction budgets. V36 therefore
tests posting geometry, replication, scoring, coarse representation, and fine
layout as separate causal layers instead of assuming any one is the solution.

## Frozen corpus and leakage boundary

The scale-transfer source is
[`andropar/relaion2b-natural-embeddings`](https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings)
at revision `bfc7465dcf1245bd605d35dcaf5d2177bbc2025a`: 514,367,913
physical rows, 2,298 Parquet objects, 787,439,811,692 bytes, and ordered
manifest SHA-256
`76ac61cf2821a331419ad40d5eb94d2cdafccccf39af17af1d328b7a2f0bc6c7`.
Its exact membership, duplicate-ID rule, query roles, schemas, and construction
limits are frozen in `docs/research/v36-funnel-dataset-authority.json`.

The source produces nested 1M/10M/100M 768D corpora after removing query rows.
Development, validation, and holdout contain 1,000 disjoint queries each;
performance contains 10,000. Exact GT@100 uses binary64 accumulation in source
dimension with no fused multiply-add and orders by
`(distance,unsigned_feature_row_id)`, matching the frozen dataset authority.
`feature_row_id` maps explicitly to `source_ordinal`, its zero-based position in
the materialized source Parquet order. Duplicate feature IDs keep the first
`(shard_path,row_offset)` occurrence. Construction reservoirs, Lloyd
reductions, and training ties use `source_ordinal`. After primary assignment,
`dense_ordinal` means the separate primary-posting storage position used by
coarse heaps and the fine interval directory. The two names are never aliases.

The complete 10M policy is frozen before opening the 1M holdout. Because query
identities are shared across scales, the 10M result is a frozen scale-transfer
test, not a second fresh cohort. Development may select once. Validation may
reject but not retune. Holdout opens once and rejection cannot trigger another
arm. Reused historical query sets are diagnostic only. Construction receives
no query or truth capability; serving receives no source-corpus or list-S3
capability.

The Task 0 registry's `block_queries=8` GT checkpoint is a planned,
unmaterialized query-major contract and is not executable at 100M within the
12-hour cell cap. Before materialization, a new authenticated materialization
manifest supersedes only that checkpoint: all 13,000 query vectors are loaded
once (about 40 MiB), each source row block is read once, distance work proceeds
in successive eight-query tiles, and the checkpoint binds the completed
source-row prefix plus every query's canonical top-100 heap. Resume starts at
the first incomplete source-row block. Source membership, query identities,
distance arithmetic, and GT ordering remain unchanged.

Bulk cross-language data is strict Arrow IPC or Parquet with exact physical
schema, concrete types, nullability, row count, ordinal semantics, and byte
identity. Small manifests and receipts are canonical JSON with one trailing
newline. Publication binds SHA-256; independently fetched objects additionally
bind BLAKE3. ETag is never treated as a digest.

## Qualification hypothesis

### Projection and construction

The smallest falsifier freezes the existing deterministic SRHT projection,
seed 36, from source dimension to `M=192`. PCA is not an automatic rescue arm.
The source-f32 metric remains the truth authority. A deterministic corpus-only
reservoir and fixed reductions train
`min(4,096,next_power_of_two(ceil(rows/262,144)))` super-cell centroids: 4 at
1M, 64 at 10M, and 512 at 100M. Postings are trained within bounded external
super-cell runs. The super cells are a construction partition only and never
replace the exhaustive posting-score authority. Exact assignment against all
posting centroids is required at 1M and 10M so construction approximation
cannot contaminate the geometry result. At 100M, construction routing may be
introduced only after parity with the frozen exact assignment is measured.

The construction reservoir contains the

`min(rows,1,048,576)`

smallest `(SHA-256("borsuk-v36-geometry-reservoir-v1" ||
little_endian(source_ordinal)),source_ordinal)` rows. Super-cell Lloyd runs exactly 25
iterations; local posting Lloyd runs exactly 10. Initialization is deterministic
farthest-first: the first centroid is the smallest reservoir source ordinal, each
next centroid maximizes distance to its nearest retained centroid, and ties use
row ordinal. Assignment ties use centroid ordinal. Sums accumulate binary64 in
increasing row-ordinal order and centroid components round once to binary32.
An empty centroid takes the row farthest from its assigned centroid, breaking
ties by source ordinal, and that row is removed from its previous accumulation
before recomputation.

Each non-empty super-cell run receives at least one posting. Initial local
posting counts are `floor(run_primary_rows/B)`; the remaining postings needed
to reach `ceil(rows/B)` are assigned by decreasing fractional remainder, then
super-cell ordinal. If the floor is zero, its mandatory one posting is charged
before remainder distribution. A checked precondition rejects a requested
posting count smaller than the number of non-empty super cells. This allocation
and every local Lloyd artifact are serialized before the exact global primary
and closure assignment.

Posting size `B` always means target **primary rows**, not stored assignments.
The exact posting counts are:

| Primary target | 1M | 10M | 100M |
|---|---:|---:|---:|
| 4,096 | 245 | 2,442 | 24,415 |
| 8,192 | 123 | 1,221 | 12,208 |

The historical 256-primary single and double-assignment geometries remain
controls. Candidate geometries use `B in {4,096,8,192}` with either single
assignment or closure replication at `epsilon in {0.05,0.15,0.30}`. There are
ten geometry arms total.

For projected row `x`, primary posting `c0` minimizes squared L2. A posting
centroid `c` is closure-eligible when
`d2(x,c) <= (1+epsilon)^2 * d2(x,c0)`. The primary is retained first. Eligible
centroids are visited by increasing `(distance,posting_ordinal)`. A candidate
replica is rejected when an already retained centroid is closer to the
candidate centroid than `x` is. At most eight total owners are retained.
Construction records mean/p50/p95/p99/max replication, primary and stored
occupancy, distance work, spill work, and exact deterministic reduction
evidence.

An arm is rejected before query evaluation when mean replication exceeds 3,
primary p99 exceeds `2B`, primary maximum exceeds `4B`, stored-assignment p99
exceeds `6B`, or stored-assignment maximum exceeds `8B`. These are gates, not
promised clustering properties. Exact global assignment at 100M would cost
roughly 468.768T MACs for B4096 or 234.394T for B8192 before training, so every
later construction method requires a reduced-shape throughput/cost preflight
and a 12-hour active-cell cap.

### Posting score and traversal

Exhaustive scoring of every posting is the representation authority at 1M and
10M. Every integer prefix through `min(posting_count,1,024)` is evaluated;
`{1,2,4,8,16,32,64,128,256,512,1,024}` is the reported summary.

For B4096/B8192 only, compare four posting scores under the same geometry and
equal 4,736-byte resident summary slot:

1. centroid squared-L2;
2. diagonal Gaussian lower-tail heuristic;
3. rank-two Gaussian lower-tail heuristic;
4. rank-four Gaussian lower-tail heuristic.

These are query-ranking heuristics, not certified enclosing ellipsoids. The
diagonal/rank-two/rank-four arms use the V33/V34 Gaussian distance-moment
algorithm generalized from 96 to 192 dimensions. For unique primary rows in
increasing ordinal order, compute the population mean and covariance with
binary64 sums and divisor `n`. Decompose the complete symmetric covariance,
sort eigenpairs by decreasing eigenvalue then original index, canonicalize
repeated eigenspaces by projected coordinate axes, and choose the sign whose
greatest-absolute component is positive (lowest dimension breaks ties).
Negative eigenvalues below `-max(trace*1e-12,1e-15)` reject the arm; smaller
negative values clamp to zero. After rounding retained eigenvectors/eigenvalues
to binary32, residual diagonal is
`max(0,cov[i,i]-sum_j(lambda[j]*v[j,i]^2))`; a negative residual below
`-max(abs(cov[i,i])*1e-5,1e-7)` rejects the arm.

For rank `r in {0,2,4}`, define `C=diag(residual)+sum_j lambda[j] v[j]v[j]^T`,
`delta=q-mean`, and
`score=delta^T delta+trace(C)-sqrt(2*ln(n))*sqrt(max(0,2*trace(C^2)+4*delta^T C delta))`.
Rank zero is the diagonal arm; centroid scoring is only `delta^T delta`.
All scoring uses fixed dimension/component order in binary64 and
`(score,posting_ordinal)` ties. The persisted maximum arm contains population,
mean f32[192], residual f32[192], four f32 eigenvalues and four f32[192]
directions (4,628 raw bytes); every arm is charged the equal 4,736-byte slot.
The receipt reports used bytes as well as the equal-byte slot. A fresh-query
pass decides whether shape helps; burned V33 results are context only.

A resident HNSW over posting summaries is considered only after geometry and
score freeze. Its topology is built by centroid squared-L2; query traversal
orders candidates by the frozen selected posting score. It uses `M=32`,
`efConstruction=200`, seed 36, and
`efSearch=max(L,e)` for `e in {64,128,256,512,1,024}`. It must reproduce the
exhaustive **selected scorer's** prefix at 999,000 ppm traversal parity. HNSW
is an accelerator, never the representation authority.

### Coarse records and unique-candidate admission

Each immutable-base coarse replica carries both a dense logical row ordinal and
its u64 source feature ID. Unique primary order defines the ordinal; no resident
100M-row physical-locator or ID table is allowed. Every scanned replica counts
toward CPU and transferred bytes, but duplicate or non-live rows are filtered
**during** bounded heap admission. Capacity therefore means K unique live
source IDs, not K assignment records.
Deterministic coarse replacement and prefix order are
`(distance,dense_ordinal)`. Final fine results map ordinals back to their bound
source feature IDs and use `(source_f32_distance,unsigned_feature_row_id)` for
the exact control, or `(codec_distance,unsigned_feature_row_id)` for the serving
codec. Recall compares the returned source feature IDs with the frozen GT IDs.

Coarse evidence runs in this causal order:

1. projected-f32 scoring over the routed rows, diagnostic only;
2. a 44-byte sign control: 24 sign bytes, one f32 residual norm, one u64 dense
   ordinal, and one u64 source feature ID. A zero residual has norm zero and
   all-zero bits. Otherwise bit `i` is one iff residual component `i >= 0`;
   reconstruction component is `+/- norm/sqrt(192)`, and the score is binary64
   squared-L2 from the projected query residual to that reconstruction;
3. residual PQ4-32: 64 three-dimensional subquantizers, 16 centroids each,
   32 code bytes plus ordinal and source ID = 48 bytes;
4. residual PQ4-48: 96 two-dimensional subquantizers, 16 centroids each,
   48 code bytes plus ordinal and source ID = 64 bytes.

Every assignment is encoded relative to the centroid of the **owner posting
whose coarse object contains it**. A replicated row therefore has a distinct
sign/PQ code in each owner and needs no primary-centroid lookup at query time.
The projected-f32 checkpoint still represents the same source row exactly and
deduplicates by source ID. Each arm has one global PQ codebook trained from all
retained assignments of the registered corpus-only reservoir, ordered by
`(source_ordinal,owner_posting_ordinal)`. Each residual-PQ subspace initializes
its 16 centroids by the construction farthest-first rule, runs exactly 20 Lloyd
iterations, accumulates binary64 in increasing source-ordinal order, rounds
centroids once to binary32, and uses codeword-ordinal ties. Coarse K is
`{512,1,024,1,536,2,048}` unique rows. Projected-f32
separates projection/routing loss from coarse-code loss; source-f32 on the
identical final row set separates fine-codec loss.

### Physical S3 layout and fine rerank

The falsifier uses one shared, unreplicated fine plane in dense primary-row
order. Each coarse replica carries the dense ordinal. A compact resident
interval directory maps ordinal ranges to fine chunks, avoiding a 400--800 MB
per-row locator table.

Fine layouts are constructed without query access and compare complete,
independently authenticated Arrow IPC chunks whose **encoded** ceilings are
64, 256, or 512 KiB. A chunk contains one non-null fixed-size-list vector
column, one non-null u64 dense ordinal, and one non-null u64 source feature ID. Candidate
chunks are admitted by best candidate order until the normal 14-GET/7-MiB wave
is full, while the retry-inclusive hard limit remains 16 GETs/8 MiB; then
decoded rows are reordered physically. No arbitrary row-range is
treated as authenticated. Metadata, retries, full encoded bytes, decoded
capacity, and encoded-plus-decoded overlap are charged.

Serving fine codecs are SQ8 and f16, selected independently. SQ8 stores D bytes
plus registered per-chunk scale/offset metadata; f16 stores `2D` bytes. The
offline source-f32 control reranks the exact same admitted rows. Selection and
layout freeze precede codec evaluation.

Coarse objects are also complete authenticated Arrow IPC files. Each object is
one posting fragment; its manifest, rather than a repeated per-row column,
binds `posting_ordinal:u32`. Its strict columns are non-null
`dense_ordinal:u64`, `source_feature_id:u64`, and the arm's fixed-size-binary
code. Rows are ordered by `dense_ordinal` and greedily fragmented without
exceeding 512 KiB encoded. The resident posting directory binds every fragment's URI,
SHA-256, BLAKE3, length, first ordinal, and row count. The planner visits
postings by `(posting_score,posting_ordinal)` and admits **all** fragments of a
posting atomically only if their combined GETs/bytes fit; otherwise it records
the posting as excluded and continues. Partial postings are never scored. The
projected-f32 diagnostic scores the exact rows admitted by a candidate code
layout using offline projected rows; it does not introduce f32 serving objects.

## Causal gates and selection

The serialized checkpoints are:

1. exhaustive route-owner containment;
2. selected-scorer containment;
3. graph traversal parity, if graph is enabled;
4. projected-f32 unique-candidate containment;
5. lossy coarse-code unique-candidate containment;
6. post-I/O admitted-row containment;
7. fine-codec recall and same-row source-f32 recall.

Each checkpoint freezes row ordinals before the next stage. Later stages cannot
change an earlier prefix. Report duplicate scans, unique rows, useful/fetched
bytes, GETs, excluded chunks, and unreachable frontiers.

Development requires 998,000 ppm route, selected-scorer, projected-f32,
coarse-code, and post-I/O containment; fine-codec loss versus same-row f32 is at
most 1,000 ppm. End-to-end development requires 997,000 ppm aggregate recall@10
and 800,000 ppm minimum. Holdout requires 995,000 ppm aggregate and 800,000 ppm
minimum. Per-query p1 is reported; minimum remains a gate because recall@10 is
quantized in 100,000-ppm steps.

The normal planner admits at most 14 complete-object GETs and 7 MiB of payload
in the coarse wave and the same in the fine wave. The hard envelope, including
retries and response metadata, remains 16 GETs and 8 MiB per wave. Widening it
is not a rescue. If retries exceed the reserve, the query and complete
measurement repetition are classified `transport-indeterminate`; they are not
scored as zero recall and cannot select or retune an arm. A campaign may run at
most three separately registered replacement repetitions of the already-frozen
arm before blocking on infrastructure. Selection is lexicographic: pass every authority,
quality, construction and transport gate; minimize total returned bytes, GETs,
distinct-query CPU p99, measured resident bytes, then arm ID.

At 10M replay only the frozen winner and registered diagnostic prefixes. Sweep
the common coarse-wave byte ceiling at every distinct cumulative complete-
object boundary from zero through 7 MiB while retaining the 14-GET normal cap.
`C*(N)` is the smallest such byte ceiling reaching 998,000 ppm aggregate
containment. Posting count `L*`, returned bytes, and GETs per query at that
ceiling are reported but are not substituted for `C*`. With seed 36, 10,000
paired query resamples, and the nearest-rank one-sided 99% quantile, recompute
the aggregate frontiers and
`Chat100=C10*C10/C1`; undefined or unreachable frontiers are infinite failures.
The 99% upper bound of `Chat100` must fit the 7-MiB normal payload budget, and
the 100M projection using measured occupancy and fragment packing must fit 14
normal GETs. This byte frontier avoids treating a two-versus-three-posting integer
jump as a continuous scaling signal. It quantifies query-sampling uncertainty,
not certainty of the 100M scaling model.

## Latency, throughput, and memory contracts

Cache protocols remain distinct:

- decoded-hot compute p99 after 1,024 warmups and 10,000 measurements/query;
- distinct-query shared-cache latency and QPS over 10,000 performance queries;
- cold S3 latency with actual GET/byte/retry counters;
- an NVMe-warm arm only if separately registered, never relabelled as RAM-hot.

Only decoded-hot has a 15 ms p99 gate. Cold S3 has no impossible 15 ms claim.
The 3,000-QPS concurrency-64 gate applies only to the decoded-hot microbenchmark;
distinct-query QPS is reported until a production gate is justified by data.
Queueing from the fixed workspace pool is included in latency.

Every executable measurement must remain below 2 GiB RSS at 1M/10M, and every
100M projection and measurement below 3 GiB. The checked ledger separately
charges projection, every scale's registered super centroids, posting summaries, HNSW if present,
posting/object and fine-interval directories, liveness, active and retiring
generations, decoded cache, encoded-plus-decoded overlap, fixed query
workspaces, runtime, delta coarse records, CSR, compaction old/new arenas, and
retry buffers. Shared `Arc` buffers count once; allocations still reachable
after eviction count until released.

The initial fixed caps are projection 3 MiB, super centroids 4 MiB, posting
summaries 128 MiB, all directories 128 MiB, liveness 64 MiB, decoded cache
256 MiB, sixteen 16-MiB query workspaces, and runtime/headroom 512 MiB. These
are admission caps, not achieved measurements. Delta coarse plus CSR has a
512-MiB byte cap and recent fine rows have a separate 128-MiB cap; row admission derives from the selected encoded record size
and measured replication rather than a fixed four-million-row promise.

Before physical Arrow directories exist, the checked ledger uses a
non-borrowable lower bound: 80 bytes per complete object identity, 16 bytes per
posting entry, and 16 bytes per fine ordinal interval. Minimum coarse and fine
object counts are the raw projected payload divided upward by the applicable
complete-object ceiling. The materialized directory must report at least this
floor, and its greater measured capacity replaces the floor in resident
admission. The 80-byte object entry is two binary 32-byte digests plus an exact
u64 encoded length and u64 object ordinal; URI keys are derived from the
authenticated generation prefix rather than held as resident strings.

At 100M, raw remote coarse storage before Arrow envelopes is 4.4 GB for the
44-byte sign control, 4.8 GB for PQ4-32, or 6.4 GB for PQ4-48 at single
assignment; multiply by measured mean replication. Mean replication 2--3 gives
8.8--13.2, 9.6--14.4, or 12.8--19.2 GB respectively; cap-eight worst cases are
35.2, 38.4, or 51.2 GB. Fine SQ8 is approximately 39/78/154/308 GB at
384/768/1536/3072D plus metadata; f16 is approximately twice that. S3 capacity
is acceptable, but none of these planes may become resident per-row state.

## Write and compaction qualification

The selected generation is immutable. One process-wide active mutation buffer
seals every 250 ms or at a checked byte limit. Conditional `HEAD` publication,
latest-wins visibility, insert/replace/delete semantics, and outcome-blind
receipts are mandatory. Pending and sealed rows use the selected projection,
coarse representation, replication policy, and fine codec.

Query nodes ingest published coarse slices into a bounded resident delta arena
and posting-to-delta CSR. Admission is byte-based and fails closed before the
3-GiB process bound. A byte-bounded resident mutation overlay contains only IDs
changed since the immutable base, with their latest sequence, live/tombstone
state, and current delta ordinal. A base candidate is admitted only when its
source ID is absent from this overlay. A delta candidate additionally carries
u64 sequence and is admitted only when it equals the overlay's latest live
entry. Thus stale or deleted base rows cannot consume K. Overlay allocation,
hash-table slack, rebuild overlap, and lookup CPU are measured and charged;
there is no resident map for unchanged base IDs. Newly published fine rows are
also ingested into a byte-bounded 128-MiB recent-fine arena, so visibility does
not create query-time microsegment GETs. A continuous coalescer flushes every
five seconds or 64 MiB, whichever comes first, grouping rows by owner posting
and rewriting them once into the selected 64/256/512-KiB fine layout. It must
finish before the recent arena reaches 128 MiB or ingestion stops. Publication
switches atomically from resident rows to the coalesced chunk directory; the
same row is never charged to both query sources. Coalesced delta fine objects
consume the same fine-wave budget as base objects. Initial durable
microsegment bytes, coalesced bytes, retries, and later rewrites all count
toward write amplification. Short G5 compactions may coalesce delta
routing/coarse state and tombstones while sharing already coalesced immutable
fine chunks. They do not prove long-lived base absorption or split/reassignment.
That production lifecycle requires a later sustained rebalancing gate.

Saturation uses offered rates 5k/10k/20k/40k mutations/s for 60 seconds each
and must reach 20k with zero rejects and visibility p99 <=1 s. The mixed cell
restores the frozen base, warms for two minutes, then measures ten minutes at
5k accepted mutations/s, eight writers, and 64 readers with a fixed 70/20/10
insert/replace/delete mix. Read p99 must be <=110% of the no-write baseline;
complete S3 write amplification, including scratch/retry/compaction bytes, is
<=3.0. Exact blocked source-f32 GT over live IDs at every registered snapshot
must meet the holdout recall gates and never return a deleted ID.

## Execution funnel and decision

1. G0 proves schemas, deterministic arithmetic, capability isolation, bounded
   unique heaps, transport accounting, scalar/SIMD parity, and resource stops.
2. G1 materializes the registered 1M corpus/GT on same-region Spot, runs the
   ten geometry arms, shape scores, representation controls, and fine layouts,
   then freezes one arm on development and evaluates validation/holdout once.
3. G2 replays the frozen arm at 10M and applies frontier-growth, quality,
   construction, RSS, and transport gates before any production format work.
4. G3 builds the minimal executable S3 query path at 1M/10M and proves offline
   equality plus honest hot/shared-cache/cold measurements.
5. G4 runs three terminal 100M/768D query repetitions on Spot only if G2/G3
   pass unchanged.
6. G5 qualifies saturation and mixed read/write behavior; a later sustained
   gate qualifies base absorption and rebalancing.

If geometry fails route containment, reject or replace the geometry. If the
selected score fails while centroid passes, reject shape complexity. If
projected-f32 fails, neither wider PQ nor fine rerank may rescue it. If lossy
coarse coding fails against projected-f32, replace the code only. If post-I/O
containment fails, reject the physical layout or byte budget. If a fine codec
loses more than 1,000 ppm, reject that codec independently. If 10M frontier
growth fails, reject Funnel-3 before 100M. Passing the 768D path qualifies only
that dimension band; 384/1536/3072 require their own real-data qualification.
