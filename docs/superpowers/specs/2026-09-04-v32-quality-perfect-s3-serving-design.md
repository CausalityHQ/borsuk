# V32 Quality-Perfect S3 Serving Design

## Decision

V32 restores the authenticated row-PQ candidate router that reached 320/320
Recall@10 with 16 pages, and removes the rejected page-centroid router from the
pre-release production path. It then optimizes serving around the proven
quality boundary. S3 Standard remains the durable authority; an optional
same-AZ S3 Express directory-bucket replica is the low-latency serving tier.
No serving process downloads or persists the corpus.

## Frozen quality boundary

The governing 100,000-row Deep Image development terminal is source
`2bce312c1bc7759efc1e540e2787750775ff85e8`, SHA-256
`f7ca28d37e1fe1d2cc08790d7155980bdeede8b6ce8fd78faf8635373ca2641f`.
Its route uses root beam 8, leaf beam 64, candidate depth 12,288, a 24-byte
PQ8 base code with query-independent 5-percent 48-byte refinement, exactly 16
unique pages, and exact reranking after page decode. It reached 320/320 hits,
32/32 perfect queries, at most 33,001 scanned codes and 2,928,808 page bytes.

The single-page-centroid route reached only 273/320 and is deleted rather than
retained behind a mode or compatibility alias. The claim-ineligible V31
residual experiments remain historical evidence only.

## Pre-release format

V32 replaces the experimental V30 manifest with one schema version and no
legacy reader. The canonical JSON manifest binds the source commit, dataset,
and exact identities of every resident and page artifact. Resident roots and
trained leaves use non-null fixed-size-list `float16[96]` Arrow IPC centroids;
the decoded trained-leaf value is the exact residual origin used by both
construction and query ADC. PQ codebooks also use Arrow IPC. The pre-release layout format
replaces each old leaf range with a routing-microleaf row:
`routing_leaf_ordinal:uint32`, `code_parent_leaf_ordinal:uint32`,
`routing_centroid:non-null fixed-size-list<float16>[96]`, `logical_start:uint64`,
`row_count:uint64`, `page_start:uint32`, and `page_count:uint32`. Page ranges
and refinement ranks use Parquet. Page bodies are packed continuously across
routing-microleaf boundaries; a page has no single-leaf owner, and diagnostics
resolve a truth logical ordinal through the routing range table instead. Page
bodies remain non-null Arrow IPC rows
containing an eight-byte source ordinal and fixed-size-list `float32[96]`
vector. There is one strict reader and no alias for the superseded leaf schema.

Construction first assigns each row to its nearest trained root and leaf and
encodes its PQ residual against that trained leaf. It then partitions every
nonempty trained-leaf population into exactly `ceil(rows / 1,024)`
deterministic geometric microleaves using four-round margin bisection with
source-ordinal ties. Empty trained leaves emit no routing row and cannot consume
a query slot. Every emitted routing centroid, split or unsplit, is the raw
float64-accumulated mean of its complete assigned population, cast through
float32 to float16 without unit normalization. All routing centroids therefore
use one squared-L2 geometry. Every child retains the original trained leaf as its
`code_parent_leaf_ordinal`; query routing therefore uses the microleaf centroid
while PQ lookup tables use the same parent centroid that encoded the codes.
Every routing microleaf has at most 1,024 rows.

Before PQ encoding, a count-only assignment preflight streams the corpus once
and records every trained-root and trained-leaf population. It rejects a parent
above 131,072 rows before the expensive construction phase. The actual builder
raises the obsolete 65,536-row/64-page trained-parent limits and allows one
parent population up to that preregistered 131,072-row bound, approximately
62 MiB of decoded construction records. Pages belong to routing microleaves, so
the old trained-parent page-count cap is removed. The format admits only
nonempty dense routing ordinals. Construction reports routing-microleaf fan-out
below each root as diagnostic evidence; the global resident and measured
routing-work equations are authoritative rather than a per-root population
abort.

Pages have at most 480 rows and, more importantly, each encoded Arrow body is
authenticated at construction to be at most 196,608 bytes. Oversized output is
rejected before upload. Exactly 16 selected objects therefore imply the
3,145,728-byte query bound by construction rather than by a row-size estimate.
Microleaf and page boundaries are independent. The globally ordered logical
stream is packed into consecutive 480-row pages, so adjacent microleaves may
share a page and no per-microleaf tail consumes an extra request slot.

The serving-location table is a strict Parquet artifact with one row per page:
`page_ordinal:uint32`, `sha256:string`, `encoded_bytes:uint64`,
`standard_uri:string`, and nullable `express_uri:string`. A copied Express
object must be byte-identical to the Standard object and retain the same
SHA-256 and length. The reader accepts exactly one selected tier per request;
it never silently falls back across tiers because that would make latency and
availability evidence ambiguous.

Construction also emits `logical-sources.arrow`, a diagnostic-only Arrow IPC
permutation from router logical ordinal to source ordinal. Its exact SHA-256,
length, role, and filename are bound by the manifest. Construction writes it
in bounded batches and the no-page diagnostic authenticates it before mapping
source-ordinal truth into router space. Normal serving validates the manifest
entry but does not load this object; it is not resident serving state.

## Query path

1. Normalize one finite 96-dimensional query.
2. Score the frozen root beam, then every nonempty routing microleaf beneath
   it, in deterministic `(distance, routing leaf ordinal)` order.
3. Take the arm's exact leaf beam (64, 128, or 256), extending down the same
   order only when its population is below 12,288. Reject before page access if
   the complete selected ranges cross the paired 65,536, 131,072, or 262,144
   scan budget. Retain the best 12,288 rows with deterministic
   `(score, source ordinal)` ties. Each selected
   microleaf builds its query table from its trained code-parent centroid. That
   centroid is the authoritative stored float16 value used when construction
   encoded the residual, so query and construction coordinates are identical.
4. Reduce the ranked rows to exactly 16 unique physical pages. Sum their
   authenticated manifest lengths and reject above 3,145,728 bytes before any
   object request.
5. Issue all page reads concurrently through one persistent async client and
   connection pool. Responses remain reference-counted byte buffers; no
   `Bytes -> Vec<u8>` copy is allowed before Arrow validation.
6. Validate length, SHA-256, Arrow schema, row counts, and source ordinals;
   exact-rerank the decoded rows and return ten deterministic matches.

The process may cache only bounded page bodies under an explicit byte limit.
The cache is optional acceleration and never authority. Resident metadata plus
cache must stay below 3 GiB. Full-corpus staging, local corpus paths, discovery
of latest objects, D3 access, and query-derived construction are absent.

## Latency and throughput

Standard S3 cold latency and compute latency are separate products. The
authenticated Standard result measured 144,065,141 ns cold p99, 74,808,007 ns
process CPU p99, 8,185,812 ns maximum routing elapsed, and 11,159,727 ns
maximum exact-rerank elapsed. V32 does not relabel that result as 15 ms.

Before every scientific run a metadata-only simulator consumes an injected
request-p99 and aggregate-throughput profile and computes
`routing + request_p99 + ceil(bytes / throughput) + decode_rerank`. It rejects
any arm whose lower-bound projection misses its tier gate. The simulator is a
fail-fast estimate; only a same-AZ measured run can pass a release gate.

A memory-resident CPU preflight measures the complete routing/PQ/reducer path
for the preregistered scan ladder 65,536, 131,072, and 262,144. It runs before
truth or S3 access. A budget is removed when its measured process projection
cannot fit the 64 ms total CPU gate; the 1M development containment chooses the
smallest surviving budget with 320/320 containment. That choice is then frozen
for the disjoint 9.99M and 100M cohorts. A cohort failure does not reopen the
ladder on that cohort.

Scale does not preserve a fixed fraction of corpus codes; locality rank is the
quantity being tested. After the routing-microleaf implementation exists, a
page-free V32 build and containment diagnostic first run on the authenticated
100K development cohort with frozen geometry 128 roots, 4,096 trained leaves,
and root beam 8. The same diagnostic then runs on the 1M development cohort.
Those two newly produced maximum truth-microleaf ranks define a preregistered
two-point power envelope:
`alpha = max(0, ln(rank_1m / rank_100k) / ln(10))` and
`projected_rank(N) = ceil(rank_1m * (N / 1m)^alpha)`. Ranks are clamped upward
to one, rounded upward to the next ladder leaf beam 64/128/256, and never fitted
on either disjoint cohort. If the projection exceeds 256 or its paired scan arm
fails the CPU gate, the architecture is rejected before the scale run. The
100K rank is produced before the 1M scale decision, not by the later Express
cell. The 32-query development set can choose this bound but cannot qualify release;
the later 1,000-query sealed holdout only validates or rejects it.

The frozen 100K implementation is not fast enough for the 15 ms tier: its
8.19 ms maximum routing plus 11.16 ms maximum decode/rerank already exceeds the
end-to-end target, and its 74.81 ms process CPU p99 exceeds 64 ms. V32 therefore
has an explicit compute-optimization gate before any Express run. It reuses one
PQ query table per distinct trained parent across sibling microleaves, scans
code planes in allocation-free fixed blocks, vectorizes centroid/PQ/exact L2
with scalar differential tests, and exact-reranks the 16 decoded pages in
parallel into deterministic per-page top tens followed by a stable merge. A
pinned, memory-resident 10,000-query benchmark must measure routing plus
decode/rerank no-load p99 at most 12 ms and total process CPU p99 at most 64 ms.
It also runs the identical path at a fixed offered load of 1,000 queries/s with
64 concurrent clients, includes server queueing in elapsed latency, and rejects
unless compute p99 remains at most 12 ms and sustained achieved throughput is
at least 1,000 queries/s. Any query-batching wait is included in the 12 ms. The
remaining 3 ms is reserved for same-AZ object latency and transfer. Failure
stops before Express provisioning.

The targets are:

- 1,000,000-ppm aggregate and minimum Recall@10 and 32/32 perfect development
  queries;
- exactly 16 page selections and no more than 3,145,728 fetched bytes;
- hot/local-page p99 at most 15 ms both for one no-load query and at fixed
  1,000-query/s offered load with 64 concurrent clients, including queueing;
- same-AZ S3 Express end-to-end p99 at most 15 ms under those same two modes;
- Standard S3 cold p99 at most 150 ms, reported separately;
- process CPU p99 at most 64 ms and sustained achieved throughput at least
  1,000 queries/s on 64 vCPUs; throughput and loaded latency are measured
  together rather than inferred from CPU division;
- projected 100-million-row resident bytes at most 3 GiB and observed RSS at
  most 3 GiB.

At 100 million rows, 95 million 24-byte codes plus 5 million 48-byte codes are
2,520,000,000 bytes. The refinement bitmap is 12,500,000 bytes. At least 97,657
routing microleaves are required by the 1,024-row cap. The projection uses the
actual emitted microleaf count, actual page count, and serialized resident
metadata widths; it may not substitute a representative count. With 65,536
trained leaves, skew cannot increase microleaf count beyond
`ceil(100,000,000 / 1,024) + 65,535 = 163,192`. Page count is bounded by
`ceil(N / 480)`, or 208,334 at 100 million rows, because packing crosses
microleaf boundaries. Page
locations store a fixed 32-byte digest, integer length/row fields, and an
ordinal; Standard/Express URI prefixes occur once in the manifest rather than
as heap strings per page. Roots, trained leaves, microleaf/page ranges, compact
page locations, and any cache must keep the complete projection below 3 GiB.
Using 194 bytes per trained leaf, 224 per routing microleaf, 64 per page range,
and 48 per page location gives a conservative subtotal of 2,605,102,400 bytes
including codes and bitmap, leaving 616,123,072 bytes below 3 GiB for roots,
codebooks, Arrow alignment, allocator overhead, and the explicitly bounded
cache. The implementation computes this projection with checked integers from
the actual schema widths and rejects overflow or a total above 3 GiB.

Scale geometry is explicit: 128 roots/4,096 trained leaves/root beam 8 at 1M;
256/16,384/16 at 9.99M; and 1,024/65,536/64 at 100M. Each keeps the root beam at
6.25 percent while the trained-leaf mean stays near 1,526 rows or below. The
geometry and all work bounds are frozen before each sealed cohort and
construction remains query-blind.

Router construction validates every routing row's parent against the trained
hierarchy and derives its root only through
`code_parent_leaf_ordinal -> leaf_roots`. Arm validation bounds routing work by
the number of eligible microleaves under the selected roots, not by the trained
leaf count. Selection, PQ scanning, page ownership, and miss diagnostics all use
the dense routing-microleaf ordinal; only residual lookup uses the trained
parent ordinal.

## Qualification order

All code changes use narrow synthetic RED/GREEN tests first. The first corpus
gate is a query-blind 1,000,000-row construction with 128 roots, 4,096 leaves,
32,768 training rows, and 480-row pages. A claim-ineligible 32-query development
diagnostic then requires 320/320 truth IDs to be contained by the selected 16
pages while reading zero page bodies. It also requires at most 3,145,728
selected-page bytes, at most 65,536 scanned codes, and at most 1,024 rows
in any routing microleaf for the first ladder arm; later arms retain their exact
131,072 or 262,144 limit. Per-root microleaf counts are recorded rather than
used as a population abort. The published
9.99-million-row neighbor table is not valid truth
for this prefix. A separate query-enabled preparation phase therefore streams
the same six authenticated corpus shards one at a time, computes exact top-10
neighbors for the fixed 32 queries with deterministic distance/source-ordinal
ties, and persists only a canonical 32-row Parquet truth artifact and receipt.
It receives no construction or page capability and never materializes the
complete prefix. Failure stops before any page-latency measurement.

The immediate fail-fast ladder is:

1. Local adversarial duplicate/noise fixtures prove the 1,024-row cap, exact
   `ceil(n/1,024)` subdivision, raw-mean centroid convention,
   parent/code-coordinate binding, empty-parent omission, artifact determinism,
   worker-count invariance, contiguous logical coverage, the count-only
   assignment preflight, the global resident projection, and the 196,608-byte page-object
   stop. They also mutation-lock `RootFrontier`, `LeafFrontier`,
   `CandidateRetention`, and `PageReducer` diagnostics with exact rank margins.
2. The existing query-blind 1M build is repeated once. It must turn the observed
   2,930-row tail into bounded microleaves while enforcing the page-byte cap;
   failure stops before truth preparation.
3. Only then does authenticated streaming truth preparation run, followed by
   the no-page 320/320 containment gate. A miss is classified by whether its
   truth row was outside the root beam, absent from the selected routing
   microleaves, evicted from the
   12,288 PQ candidates, or lost by the 16-page reduction. Page ranges carry
   routing-microleaf ordinals, so this classifier remains exact. Only the
   preregistered development ladder may increase the code budget.

Only perfect 1M containment permits a 16-object same-AZ S3 Express
microbenchmark using already-selected page objects; it is not a corpus copy.
Then one 100K end-to-end Spot run verifies quality and timing. A later decisive
quality gate uses at least 1,000 held-out queries and 10,000 independently
frozen truth IDs; the 32 development queries cannot qualify a release. Before
that gate, a separate query-enabled, page-blind `causality` Spot phase streams
the authenticated 100M source shards without construction or page capability,
computes exact float32 squared-L2 top ten with `(distance, source_ordinal)` ties
for a preregistered sealed 1,000-query cohort, and writes only a canonical
10,000-row Parquet truth artifact plus a canonical receipt binding the query
cohort, source shards, row count, algorithm, worker topology, and output digest.
The producer is independently sharded by query, has no router/layout input, and
must be terminal before serving begins. Its cost, bytes, CPU time, and cleanup
are reported as a bounded campaign phase. Only
passing stages proceed to the disjoint 9.99-million-row cohort and then a
100-million-row construction/serving run. Each campaign uses `causality` Spot,
writes canonical JSON plus typed Arrow/Parquet evidence, terminates immediately,
and keeps D3 and competitor claims fenced.
