# V156 production graph generation and resource contract

## Purpose and evidence boundary

V154 and V155 established a development winner for ReLAION-1M
validation-1000: cached sparse planning reduced paired offline planner
p95 from 6.489573 to 0.759366 ms and returned 99.567% exact-source
Recall@100 versus 99.607% for the flat control, with fewer planned bytes
and GETs. V153 established exact D96 plan parity against the prior flat
planner on deep-image-96-angular random100k. These are used cohorts.
They do not qualify a production generation, fresh quality, live-S3
latency, or 10M/100M memory.

The next software increment makes the graph route a generation-bound
serving component. It must preserve the measured scorer and page-plan
semantics while adding authenticated persistence, explicit resource
admission and observable bounds. No default recall or memory setting is
frozen by this design. No vector-count threshold chooses the route or
caps RAM.

## Considered routes

1. Patch graph search into the existing `BorsukIndex` global search
   branches. This reuses broad API plumbing but entangles a new SQ8
   generation with legacy global sidecars and makes identity and resource
   accounting difficult to audit.
2. Build a separate versioned graph generation beside the existing
   `ServingGeneration`, then put it behind the public search dispatch
   after qualification. This isolates all new object identities and
   permits an atomic format replacement before release. **Selected.**
3. Rewrite the full persistent index and mutation path at once. This is
   ultimately possible without compatibility constraints, but would
   put too many unmeasured changes into the next quality comparison.

## Generation format and query path

One immutable generation manifest names and authenticates the source
router, source-ID map, exact source tier, SQ8 object/page digest table,
local SQ8 mirror, centroid blob and graph adjacency. It includes one
generation number, metric, dimensions, row count, physical page rows,
unit rows, SQ8 object ETag and each artifact's exact byte length and
SHA-256. The graph adjacency initially retains its fixed-parameter
`BORSUKG1` encoding; graph build parameters are not selectable in a
profile until the encoding is versioned. The enclosing manifest receives
a new version marker and rejects older experimental generations. Its
trusted digest comes from a conditionally updated generation pointer in
the authorized object store. Each object uses a content-addressed key
and is never overwritten. The builder streams and authenticates the
SQ8 object once, derives the deterministic centroid blob, and binds its
digest to the SQ8 digest, affine low/step digest, source layout digest,
metric, geometry and generation in the trusted manifest. The loader
checks every artifact length and SHA-256 before decoding. It rejects a
layout mismatch, wrong object key/ETag, mirror format or nominee limit,
non-L2 geometry without an explicit normalization contract, and any
page-row/unit-row combination the planner cannot implement. The first
slice requires 256 rows/page and 32 rows/unit; changing these requires
a format and planner revision. A replacement generation is built and
checked separately, then published through a conditional pointer
write. An owned `Arc` generation keeps old readers pinned until they
finish. Production has no non-CAS publication fallback; garbage
collection waits for pinned readers and a declared grace period.

For one query, the source-only PQ64 router emits its bounded, score-ordered
nominee roster. V114 selected the 100 primary rows only **after exact SQ8
scoring of all 512 nominees** through its authenticated local mirror; V154
and V155 reused those captured exact-primary rosters. Taking the first 100
PQ nominees would change the method and has no returned-quality evidence.
Fetching every nominee page from S3 before graph planning may consume the
request's transport cap. The registered V157 cover gate decides whether
that exact-primary step is feasible under the same charged cap, and records
PQ-primary overlap without treating overlap as a recall result. No serving
query may silently substitute PQ-primary for the V154/V155 method.
The current full-code router is a
100M latency blocker: `nominate` scores every page summary, then scans
the selected PQ codes. V156 initially qualifies only the graph/planner
stage after this router. A measured bounded-work hierarchical
source-only route must replace it before the full 100M search gate. The V154
page-seeded HNSW
search uses a graph work cap proportional to the distinct primary-page
count. It caches every evaluated unit distance. Exact eight-unit page
scoring refines the retained candidate pages, and the V153
primary-first planner admits pages only under the request's byte and
GET caps. The existing authenticated SQ8 range scorer emits the ordered
top-512 IDs. These join the source-only nominees for exact source
rerank. In live serving the SQ8 scorer consumes the authenticated bytes
returned by the S3 range fetches; a local mirror is an optional
authenticated cache and diagnostic control, not a substitute for the
charged fetch path. The exact source tier likewise needs bounded
authenticated block access rather than a requirement to keep all float32
source vectors resident. The response reports planned/actual GETs and bytes, graph and
exact unit work, primary retention, target shortfall, SQ8/exact union
width, latency phases and the pinned generation ID. Every wire attempt,
including a retry, consumes the configured GET and byte allowance;
the coordinator may stop with a partial or failed result when the cap
is exhausted. Request caps are clamped to the operator's maxima.
Cancellation, timeouts, retries and errors release transient permits.

## Recall and memory policy

The input is a requested quality/service profile plus explicit upper
bounds for resident RAM, transient RAM, planned SQ8 bytes, GETs and
concurrency. A profile is a versioned table of measured,
cross-corpus operating points: unit rows,
candidate multiplier, graph work multiplier, page admission multiplier
and transport caps. Its admissible points are monotone in requested
quality after accounting for measurement uncertainty. Each point carries
an evidence domain: metric, dimension range, corpus-size interval,
query role, and paired lower confidence bounds on Recall@100 and its
tail. The domain limits what evidence can support; it is never an
algorithm switch or memory ceiling at a vector-count threshold. If no
validated point covers the request, return `UnsupportedObjective`;
never imply an unmeasured recall guarantee. The table is calibrated on
development data, frozen before one fresh confirmation per requested
objective, and independent of dataset name. A user may raise the RAM ceiling at
100M to select a higher-recall point; the implementation never raises
it silently. V154 sparse missed its 4P page target on 776/1,000 used
queries, so increasing the admission multiplier alone is unlikely to
raise recall under the same transport cap. Bytes, GETs, routing work
and their marginal quality must be measured jointly.

The generation's modeled resident envelope sums router PQ codes and
summaries, decoded centroid arrays, graph adjacency, source-ID map,
source-tier/page digest tables, codebooks and pinned metadata. Add
bounded caches and concurrency-scaled transient buffers. Build and
hydration have separate peak envelopes, including the simultaneous
encoded and decoded centroid blobs and the old generation while a new
one loads. The current graph builder copies every centroid; its peak
must be admitted or the builder redesigned. Local SQ8 mirror placement
requires disk admission and is optional in production. Every term uses
checked arithmetic from authenticated header geometry and declared
format maxima. Reject an over-budget load **before** allocating large
arrays. The graph decoder must check a manifest-bound maximum encoded
length and decoded-heap reservation before allocating adjacency; a
flat/CSR graph layout is preferred for an exact resident bound. The
report distinguishes modeled payload, measured heap/RSS,
charged cgroup memory and cache. A 100M estimate is a planning input,
not a measurement. Page bytes and GETs are separately capped per query;
they may increase with the requested quality profile.

The older V143 15.2 GB D768 single-generation example uses the compact
f16 centroid payload. The current `UnitCentroidPages::decode` keeps f32
coordinates and one f32 norm per 32-row unit. Its array formula is
`ceil(N/32) × (4D + 4)` bytes: 9,612,500,000 bytes at 100M D768, versus
4,800,000,000 compact centroid bytes. Under V143's other stated terms
(64-byte PQ code, 24-byte page summaries and 16-byte ID map per row),
the corrected modeled single-generation payload is 20,012,500,000
bytes at 100M D768 before graph, headers, allocator, cache and query
scratch; two complete generations need at least twice that payload.
These figures are arithmetic projections, not charged-memory measurements.
At 100M D768 the current PQ64 codes account for 6.4 GB of the corrected
20.0125 GB model, and the two f32 page summary blocks account for about
2.4 GB. The exact source digest table
depends strongly on authenticated block size: a 4-KiB block choice for
roughly 307 GB of source data adds about 2.4 GB of digests, while 1-MiB
blocks add about 9.4 MB. The latter trades verification granularity
for RAM and I/O; neither choice is a frozen default. The digest table,
graph adjacency, loading peaks and transient buffers are additional to
the corrected 20.0125 GB payload model.

## Formal and empirical obligations

Lean should state conditional bounds for graph visits, candidate pages,
GETs, bytes, resident payload and concurrent transient permits using
the chosen profile as an explicit parameter. It must include source
router work separately until a bounded hierarchical router is proved.
A Rust refinement test
must check the actual planner/graph work counters against the model on
adversarial small geometries and authenticated 100k/1M records. The
proof cannot establish graph neighbor usefulness, Recall@100,
hardware/S3 latency or charged RAM. Those require fresh paired tests.

The ordered gate is: (1) versioned binding and fail-closed identity and
pre-allocation admission tests; (2) exact reproduction of the recorded
V153/V154 sparse page plans and V155 returned IDs, plus resource
accounting on the closed used cohorts; (3) define a mutation-visible
delta policy, its query merge and compaction/rebuild threshold before
freezing query semantics; (4) preregistered fresh D96/D768 paired
returned quality, including SQ8-only p05 and exact-source tails,
against the strongest flat BORSUK baseline, preserving V36's existing
sealed holdout until final qualification; (5) matched live-S3 latency,
throughput, retries, actual bytes/GETs and charged RAM; (6) bounded
hierarchical source routing and frozen 10M, then 100M build, query,
mutation/compaction and cost gates.
Only a method that passes each gate becomes the production default.

## First implementation slice

Add a versioned owned generation loader that takes a trusted manifest
digest and immutable artifact byte sources. It validates manifest schema,
object identities, all byte lengths and hashes, generation/geometry,
router layout, fixed graph parameters, 256/32 page/unit rows and L2 or
an explicit angular normalization flag. It computes a checked resident,
hydration and transient envelope from headers before decoding and
returns `Arc<GraphServingGeneration>` only after every binding succeeds.
Wrong generation, layout, SQ8 hash, centroid hash, graph hash, ETag,
nominee limit, geometry or one byte below the required RAM envelope
must fail before a query can run. A small deterministic fixture tests
successful load and each failure. Closed V154/V155 records then test
bit-exact reproduction of the **sparse** page plans and returned IDs.
The pointer publisher, public dispatch, profile calibration, S3 query
coordinator and mutation delta remain explicit subsequent gates; this
slice does not claim them by merely loading artifacts.
