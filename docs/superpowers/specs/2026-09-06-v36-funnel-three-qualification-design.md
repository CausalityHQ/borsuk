# V36 Funnel-3 Architecture Qualification Design

## Objective

Qualify or reject a dimension-independent three-stage S3 query funnel before
changing the production format. The candidate must preserve high recall while
keeping resident index state below 3 GiB at 100 million rows, remote query work
bounded, and writes append-only. It is not allowed to inherit V35's Morton-run
leaves, per-source-dimension SQ4 coarse plane, fixed eight-page final tier, or
full f32 page rerank.

This is a research qualification, not a release claim. Sub-second contracts and
reduced-shape falsifiers run locally; corpus-scale scientific stages use bounded
AWS Spot capacity under profile `causality` so the devbox never materializes or
thrashes on high-dimensional corpora. No 100M build starts until the same frozen
arm passes the 1M and 10M gates.

## Evidence that motivates the change

The strongest authenticated local result is 997,708 ppm aggregate recall with
800,000 ppm minimum recall and 11.47 ms p99 on 9.99M Deep Image rows, but its
codes and vectors were local and its page stage did not execute. The strongest
authenticated S3-native exact result is 320/320 hits on 100,000 rows with 16
pages and 2,928,808 returned bytes; cold p99 was 144.07 ms. The authenticated
100M baseline reached 0.992 recall with 70.82 GETs, 76.7 MB, 44.4 ms cached p95,
and 910.5 ms uncached p95.

The repeated failure is candidate starvation. Existing routers expose only
0.03% to 0.26% of the corpus and then ask a lossy page summary to miss almost no
true neighbor. Deep1M contains queries whose ten truth rows occupy nine or ten
leaves, so a fixed eight-page final tier is structurally insufficient. Exact
global ADC over the old width-12 PQ plane reproduced its routed recall, proving
that routing was not the causal defect in that generation. K32 prototypes,
incidence postings, witnesses, and per-page minimum ADC likewise failed their
registered quality gates. Low-rank covariance patches can improve a boundary
score but do not repair a starved row-level candidate set.

## Candidate architecture

### Query-independent geometry

Project source vectors once into `M=192` dimensions using the authenticated V35
projection contract. Set primary leaf count to `ceil(rows/256)`: 3,907 at 1M,
39,063 at 10M, and 390,625 at 100M. Train compact leaves hierarchically from
corpus-only data: a deterministic reservoir trains the coarse centroids, one
stream partitions rows into bounded external coarse runs, and each run trains
and assigns its registered number of local leaves. This forbids an impossible
resident 400,000-way global Lloyd step. Compare single assignment with a fixed
orthogonal-spill double assignment. For projected row `x`, score every leaf
owned by its two nearest coarse cells. Primary `p` minimizes squared L2 in that
union. Among the eight next-nearest leaves, replica `r` minimizes
`d(x,r) + dot(x-c[p],x-c[r])^2 / max(d(x,p),2^-126)`; ties use leaf ordinal.
Leaf count and the 256-row target count primaries, so the double-assignment arm
targets 512 stored assignments per leaf. Queries and truth are unavailable to
construction.

Use `min(4,096,next_power_of_two(ceil(leaf_count/64)))` coarse centroids: 64 at
1M, 1,024 at 10M, and 4,096 at 100M. The qualification oracle scores every
coarse centroid and then every leaf in the best `{8,16,32,64}` coarse-prefix
ladder; this is the traversal authority. A resident HNSW over leaf centroids is
evaluated only after exhaustive leaf-prefix parity is frozen, so graph
approximation cannot be confused with representation quality.

Training uses a fixed `min(rows,1,048,576)` reservoir, 25 coarse Lloyd
iterations, and 10 local-leaf Lloyd iterations. The receipt records exact
distance/MAC counts. At 100M the upper bounds are `rows*D*192` projection MACs
(14.75T at D768; 58.98T at D3072), `reservoir*4096*192*25 = 20.62T` coarse
training MACs, `rows*4096*192 = 78.64T` coarse-assignment MACs,
`rows*ceil(ceil(rows/4096)/256)*192*10 = 18.432T` local-leaf MACs, and at most
`rows*2*ceil(leaf_count/coarse_count)*192 = 3.6864T` two-cell primary/spill
assignment MACs. Before G3/G4, a registered reduced-shape
throughput preflight projects wall time and cost; an estimate above 12 hours or
the campaign's preregistered cost cap blocks launch rather than silently
changing iterations or sampling.

### Three-stage funnel

1. **Route:** exhaustively score every resident leaf summary as the representation
   authority. Separately apply the coarse-prefix `{8,16,32,64}` approximation and
   require at least 999,000 ppm traversal containment against exhaustive top-L at each
   `L in {32,64,128,256,512,1024}` before reading row codes. Record both
   traversal parity and truth-owner containment.
2. **Coarse row rank:** read leaf-contiguous, dimension-independent projected
   codes. Compare fixed 24-byte sign/RaBitQ-style, 32-byte anisotropic PQ4, and
   48-byte anisotropic PQ4 arms. Maintain a bounded `(distance,row_ordinal)` heap
   in the `{1,024,1,536,2,048}` ladder consumed by the fine stage; no
   full-corpus pair allocation is permitted.
3. **Fine rerank:** deduplicate the coarse heap by row ID, freeze its
   `(distance,row_ordinal)` prefix, then map it to independently authenticated
   fine chunks. Admit chunks in best-candidate order until the 16-GET/8-MiB
   envelope is full; only then reorder admitted rows by physical location.
   Record containment after this I/O pruning. Serving compares SQ8 and f16. A
   same-row-set f32 control runs offline with bounded memory and is exempt from
   serving GET/byte caps.

The six immutable causal checkpoints are coarse/HNSW traversal parity against
exhaustive leaf scoring, route-owner containment, coarse-code top-candidate
containment at the consumed K, I/O-admitted fine-prefix containment, fine-codec
recall, and offline f32-control recall.
Each checkpoint serializes its row ordinals before the next stage. A later stage
cannot change or reinterpret an earlier prefix.

## Data and authority

Each dimension band uses a real 1M-row corpus where licensing permits:
384-dimensional, 768-or-960-dimensional, 1,536-dimensional, and 3,072-dimensional.
If a band lacks a real corpus, a counter-based power-law-spectrum generator is
allowed but is labelled synthetic and cannot establish real-data quality.
Synthetic-only bands remain unqualified for release.

The scale-transfer corpus candidate is the 768-dimensional, approximately
514M-row
[`andropar/relaion2b-natural-embeddings`](https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings)
Parquet dataset. Before implementation,
G0 must verify its current license, immutable revision, physical schema, and
object identities, then freeze hash-nested 1M/10M/100M subsets by increasing
`SHA-256(source_identity || little_endian(feature_row_id))` after removing all
query ordinals. Each scale receives separate development, validation,
sealed-holdout, performance-query, and exact-GT artifacts. The historical V35
100M/96D result is context only; no nonexistent V35 768D executable is treated
as a qualification authority. If this source cannot be immutably registered or licensed,
the 10M/100M promotion is blocked until another real 768D source is registered.

For every corpus, construction receives only source Parquet and a corpus-only
sample manifest. Evaluation receives a separate query Parquet and blocked f32
GT@100. Development, validation, and sealed holdout each contain 1,000 disjoint
queries. All representation choices use development only. Development aggregate
recall must exceed the final threshold by 2,000 ppm (997,000 ppm). Minimum
per-query recall remains 800,000 ppm because recall@10 is quantized in
100,000-ppm steps; containment, traversal-parity, resource, and transport
thresholds are unchanged rather than receiving an impossible
blanket margin. Validation may reject but cannot retune the winner. Holdout is opened
exactly once after validation; failure rejects the architecture rather than
triggering re-selection. Reusing a historical burned query set is diagnostic
only.

Bulk cross-language artifacts are Arrow IPC or Parquet with exact physical
schemas, concrete types, nullability, row counts, ordinals, SHA-256 byte identity,
and logical checksums. Compact manifests and receipts are canonical JSON with one
trailing newline. S3 object manifests use SHA-256; independently addressable hot
chunks additionally bind BLAKE3 for serving authentication. ETag is never a
digest.

## Fixed parameter ladder

The representation matrix is the Cartesian product of assignment, coarse code,
and serving fine representation below, pruned only by an earlier causal stop.
Coarse cells, leaves, K, and fine-row counts are diagnostic curves, not tunable
representation arms. Executable serving always takes the longest deterministic
prefix admitted by the fixed 16-object/8-MiB planner in each wave.

- assignment: single, orthogonal-spill double;
- routed coarse cells: 8, 16, 32, 64, capped at the scale's coarse-cell count;
- routed leaves: 32, 64, 128, 256, 512, 1,024, always constrained by the
  identical executable coarse-object GET/byte planner;
- coarse code: 24-byte projected sign, 32-byte projected PQ4, 48-byte projected
  PQ4;
- coarse/fine prefix: 1,024, 1,536, 2,048 rows;
- serving fine representation: SQ8, f16;
- diagnostic fine prefixes: 1,024, 1,536, 2,048, while executable serving uses
  the longest prefix admitted by actual chunk GET/byte caps;
- offline authority: f32 on the exact same frozen fine-prefix rows.

Selection is lexicographic: pass all authority and quality gates; minimize
returned bytes; then GETs; then warm CPU p99; then resident bytes; then arm ID.
No outcome-specific manual tuning is allowed.

Coarse objects have one non-tunable packing order: coarse-cell ordinal, then
leaf ordinal, greedily packed without exceeding 512 KiB. Receipts record useful
routed-leaf bytes divided by fetched coarse-object bytes. A scattered top-L set
that exceeds the fixed planner is truncated and recorded at the I/O checkpoint;
construction may not repack after observing queries.

Before Arrow overhead and the 16-object limit, raw 8-MiB fine capacity is at
most 21,399/10,922 rows at D384, 10,810/5,461 at D768, 5,433/2,730 at D1536,
and 2,723/1,365 at D3072 for SQ8/f16. The executable planner may admit fewer
complete objects. High-dimensional bands therefore use smaller admitted
prefixes rather than falsely claiming K=2,048.

## Fail-fast gates and causal interpretation

### G0: authority and arithmetic

Sub-second tests pin schemas, checksums, tie order, bounded heaps, no query access
during construction, no source access during serving, and checked memory/byte/GET
arithmetic. The same deterministic transport planner used by G1/G2 must compute
distinct coarse and fine object identities, enforce at most 16 GETs and 8 MiB in
each wave from complete encoded lengths, and expose every candidate excluded by
either budget. Concurrency is not counted as fewer GETs. Any failure blocks
scientific execution.

### G1: 1M offline causal oracle

Run development first on same-region Spot while streaming bounded source blocks
from S3 to ephemeral NVMe; the corpus is never resident in RAM. Route containment
below 998,000 ppm within the executable coarse GET/byte plan rejects the leaf
geometry. Coarse containment below 998,000 ppm at the exact consumed K rejects
that code arm. I/O-prefix containment below 998,000 ppm rejects the physical
layout or byte budget independently of the fine codec. Fine recall loss above
1,000 ppm relative to offline f32 on the exact same frozen row set rejects that
fine-codec arm; f16 is evaluated independently under its own identical transport
planner. Report all curves; do not collapse containment into
end-to-end recall.

Open holdout only after freezing one arm. It must reach aggregate recall@10 at
least 995,000 ppm, minimum per-query recall at least 800,000 ppm, and final
rerank recall no lower than fine-codec recall. The target is competitive recall, not a promise
of perfect recall.

### G2: 1M executable path

Run on same-region Spot against S3 Standard. The selected arm must remain under
32 complete-object GETs and 16 MiB: at most 16 coarse GETs/8 MiB plus 16 fine
GETs/8 MiB. The envelope charges actual HTTP bytes, retries, and decoded-buffer
capacity. Compute-only p99 runs over all performance queries. Hot-object p99
uses the first 32 registered performance queries. Before each query's block,
only its at-most-16-MiB working set is authenticated and warmed into the
256-MiB cache, followed by 1,024 warmups and 10,000 timed executions. Hot-object
p99 must be at most 15 ms and is not labelled shared-cache or S3 latency. A
second pass over 10,000 distinct registered performance
queries, disjoint from development, validation, and holdout, uses the same fixed
cache and records hit rate. Process RSS must remain at most 2 GiB. Cold S3
latency is reported honestly and has no 15-ms promise. The opened 1M holdout may be
replayed only to prove offline/executable equality and cannot tune anything. No
unselected object may be opened.

### G3: 10M frontier growth

Run on same-region Spot. At 1M and again at 10M, record `L*(N)`, the smallest
integer prefix in `1..=1,024` reaching 998,000 ppm route containment under the
identical executable transport planner; the power-of-two ladder remains the
reported diagnostic summary, not the resolution of `L*`. G3 replays the complete
L/K diagnostics while keeping the G1-selected assignment/coarse-code/fine-codec
representation and max-capacity policy frozen; it cannot select a new
representation. The same preregistered 1,000 development query vectors, with
scale-specific exact GT, measure the 1M and 10M curves. Require
`L*(10M) <= 1.6 * L*(1M)`. Estimate the 100M frontier with the fixed geometric
scale extrapolation `Lhat100 = L10 * L10 / L1`. A paired query bootstrap with seed 36,
10,000 resamples, and the nearest-rank one-sided 99% quantile recomputes both
frontiers and `Lhat100`; that upper bound must still fit the coarse GET/byte
envelope. No alternative fit, confidence level, seed, or query subset is allowed
after results are observed. Apply the actual 10M route, coarse, I/O-prefix,
recall, CPU, RSS, GET, and byte gates to validation without retuning; only then
open the separate 10M holdout once and require the G1 holdout recall gates.
Failure rejects 100M promotion.

### G4: 100M AWS Spot qualification

Only the unchanged G3 winner runs under AWS profile `causality` on preregistered
Spot capacity. Three terminal repetitions must retain at least 995,000 ppm
aggregate and 800,000 ppm minimum recall, remain below 3 GiB RSS, and obey the
32-GET/16-MiB envelope. The sealed 100M holdout is opened once for the recall
gates. On the first-32 per-query hot-object protocol, compute-only and hot-object
p99 must remain at most 15 ms after 1,024 warmups and 10,000 timed executions
per query. On the second pass over 10,000 distinct performance queries with the
fixed 256-MiB cache, record cache hit rate, QPS, and every warm/cold percentile
without an absolute QPS gate. Require at least 3,000 QPS at concurrency 64 only
for the hot-object protocol; concurrency 1 and 16 are reported. These are
absolute V36 gates, not comparisons to a V35 executable that does not exist.
Results record
latency, QPS, actual HTTP GETs and
range bytes, RSS, CPU, and cost. Interruptions preserve terminal receipts in S3,
discard the interrupted measurement cell, and restart it; compute terminates
immediately after the terminal marker.

### G5: write and compaction qualification

First measure saturation capacity without readers at fixed offered-rate
steps of 5k, 10k, 20k, and 40k mutations/s, 60 seconds per step. Capacity is the
highest completed step with zero rejected mutations and mutation-visibility p99
at most one second; it must reach at least 20,000 mutations/s. Discard this
saturation cell and restore the exact frozen base before the mixed-load cell;
its mutations cannot leak into G5 recall or capacity. Then run 10
minutes after a two-minute warmup at 5,000 accepted mutations/s with eight
writers and 64 concurrent readers. The mutation mix is fixed at 70/20/10
insert/replace/delete, and IDs for replace/delete are uniform without replacement
from the live set. Run compactions at measured minutes 3, 6, and 9. A
microsegment seals and publishes every 250 ms or at the checked byte/row cap,
whichever occurs first. Require visibility p95 at
most one second, total S3 write amplification at most 3.0 using every final,
scratch, retry, and compaction byte divided by the uncompressed canonical Arrow
mutation-batch bytes, and concurrent-read p99 at most 110% of no-write p99.
Before execution, checked D768 projections must include every initial fine/code,
visibility, arena, retry, and three compaction writes and remain below 3.0; the
runtime counter is still authoritative and fails closed if actual bytes exceed it.
At the end of warmup, once per measured minute, and immediately before and after
each of the three compactions, block a snapshot and compute exact f32 GT@10 for
the same fixed 1,000 mutation-evaluation queries over live IDs. Every snapshot,
including pending, sealed-run, and compacted states, must reach 995,000 ppm
aggregate and 800,000 ppm minimum recall; deleted IDs must never be returned.

## 100M storage and memory projection

The resident target is deliberately conservative and independent of source
dimension: projection 0.3–2.4 MB; 4,096 coarse centroids about 3.1 MB; leaf
summaries about 90 MB; 32-neighbor leaf graph about 51 MB; leaf/object directory
about 20 MB; liveness and sealed-run state about 60 MB; active mutation buffer
256 MB; object cache 256 MB; 16 query workspaces 256 MB; runtime plus headroom
512 MB. Expected immutable-query total is about 1.5 GiB and must remain below
2 GiB in G2. G5 additionally budgets the 448-MB delta coarse arena and 64-MB
delta CSR directory; every 100M projection and measurement must remain below
3 GiB.

The remote coarse plane is at most 48 code bytes plus one explicit eight-byte
row ordinal per assignment. With orthogonal-spill double assignment this is at
most 11.2 GB before object envelopes. Fine SQ8 storage is
approximately 39/78/154/308 GB at 384/768/1536/3072 dimensions for 100M primary
rows, plus eight bytes per row for registered scale/offset metadata. Source f32
Parquet remains the rebuild/return authority in S3 and is never resident as a
serving index.

## S3 and write layout

The candidate layout is immutable and chunk-shared:

```text
gen/<manifest-sha>/routing.arrow
gen/<manifest-sha>/coarse-groups/<ordinal>.arrow
gen/<manifest-sha>/fine/<chunk-ordinal>.arrow
source/<shard>.parquet
delta/<sequence>/<artifact>
HEAD
```

Coarse groups target at most 512 KiB. A logical leaf may span multiple
independently authenticated coarse objects; its resident directory lists them
in the fixed packing order, and the transport planner charges every complete
object. Oversized or duplicate-heavy leaves therefore reach G1 and fail the
transport/containment gate rather than making construction undefined. Fine Arrow IPC files contain one non-null
fixed-size-list batch and are independently addressable and authenticated at at
most 512 KiB including schema and footer. A logical leaf may span several fine
chunks; its directory is resident. Sixteen complete fine objects therefore
enforce the 8-MiB bound without pretending arbitrary Parquet row runs are
independently readable. Query reads are complete registered objects only;
listing, arbitrary endpoints, and whole-corpus downloads are not serving
capabilities. Source and construction data remain Parquet. Writers append to one
process-wide active buffer; eight writers do not independently create runs. One
microsegment seals and conditionally publishes every 250 ms or at checked
codec-dependent capacity. Sealed fine objects remain immutable. Query nodes
ingest each authenticated published coarse slice into one resident append-only
delta coarse arena plus a CSR leaf-to-delta-row directory. The directory uses
checked u64 row references, at most 64,000,000 bytes for four million double
assignments, and queries score only delta rows owned by selected leaves. Query
work is independent of microsegment count and
spends no query-time GETs. Readers refresh from the exact `HEAD` and its complete
visibility directory in a background publication phase with named-object GET
capabilities but no list API. At each of three registered compaction points, compaction rewrites only
delta coarse metadata/arena slices, coalesces tombstones and duplicate IDs, and
references existing fine objects. It cannot rewrite the base fine plane or
unchanged delta fine objects. Base absorption is outside G5 and requires a
separately budgeted generation build. Every publication uses one conditional
head update.

The active mutation buffer is query-visible before seal and preserves the exact
fine codec selected at G1; it may not silently substitute SQ8 for a selected
f16 arm. Exactly 32 MiB is reserved for double 48-byte coarse codes, IDs,
directories, and bounded scratch. The byte-triggered row capacity is
`min(65,536, floor(224 MiB / (fine_row_bytes(D, codec) + 120)))`, where
`fine_row_bytes = D + 8` for SQ8 and `2D` for f16. Thus D3072 admits all 65,536
SQ8 rows but only 37,496 f16 rows before sealing. Tests use checked arithmetic for every supported
dimension/codec pair and reject a mutation larger than the remaining buffer.
After seal, full fine chunks move to S3 while their slices append to the single
resident delta coarse arena. G5 caps stored mutation entries at 4,000,000,
covering all 3,600,000 warmup-plus-measurement mutations with headroom, so the
worst 48-byte double-assignment arena including explicit row ordinals is 448,000,000
bytes, within the global 3-GiB measured gate. Queries score base, pending, and
the delta arena together; delta fine chunks
share the same 16-fine-GET/8-MiB envelope rather than adding a hidden budget.
G5 truth is a blocked exact f32 scan over the live IDs at the pinned snapshot
sequence, performed outside timed serving.

## SIMD and determinism

Projected PQ4 uses 16-entry shuffle LUTs. Router and SQ8 rerank use int8 dot
products with i32 accumulation via AVX2/VNNI or NEON dot-product kernels. Scalar
controls must be bit-identical on integer stages; f32 controls use fixed source
order and registered tolerance. Heap ordering is always `(distance,row_ordinal)`.
Scalar/SIMD differential tests cover ties, subnormals, saturation, reversed
blocks, odd tails, and every selected parameter arm.

## Decision

If G1 fails at route containment, investigate leaf geometry or orthogonal spill
rather than code width. If routing passes and coarse containment fails, widen or
replace the projected code without changing the router. If coarse containment
passes but I/O-prefix containment fails, reject the chunk layout or byte budget.
If the prefix passes and one fine codec fails, reject that codec; SQ8 and f16 are
independent arms and neither is an automatic fallback for the other.
If G3 frontier growth fails, reject Funnel-3 before 100M. G4/G5 at 768 dimensions
qualifies only the 768-or-960 band; every other band requires its own real-data
G1/G2 quality and executable gates before it becomes a supported release band.
