# Native Geometric ANN Redesign

## Purpose

The current production-native format cannot meet BORSUK's quality target. On
the frozen ReLAION development-100k corpus (100,000 rows, 768 dimensions,
1,000 queries), it measured 51.267% mean Recall@100. An authenticated
truth-aware oracle proved that its fixed pages cannot exceed 61.377% mean,
47% p05, or 41% worst Recall@100 under the 32-page limit. The failure is
structural: the builder sorts by stable ID and then cuts that unrelated order
into 256-row pages.

This design replaces ID-ordered pages and the population-sized resident PQ
router. It first proves that one query-blind geometric layout has enough
physical-page recall headroom on 100k. Only a passing layout earns a production
format and router implementation.

Research labels such as V98 or V108 remain evidence-ledger campaign IDs. They
are never format versions. The replacement root uses the explicit schema name
`borsuk-native-geometric-ann-v1`; its local `format_version = 1` is scoped to
that schema. No compatibility reader for `native-bounded-v3` is retained.

## Success contract

The serving contract remains:

- query-blind construction: corpus vectors and opaque stable IDs only;
- at most 32 remote page GETs and 16 MiB of page bodies per query;
- average Recall@10 at least 96%, average Recall@100 at least 97.5%, and p05
  Recall@100 at least 90%;
- object-storage-native pages; the corpus and row codes are not resident;
- complete serving resident memory below 3 GiB at 100 million rows;
- stable `(distance, id)` result order and no flat/PQ-scan serving fallback;
- immutable authenticated generations, immediate mutation visibility,
  deterministic compaction, and bounded recovery;
- no nested Rayon or per-query work-stealing pool; fixed CPU and I/O admission;
- typed Serde authority, Arrow IPC page bodies, and Parquet evidence. Production
  code does not construct JSON with `serde_json::json!`.

The 100k development screen is a falsifier, not release evidence. Promotion
requires an unchanged 1M run and later independent 9.99M evidence.

## Alternatives considered

### Capacity-balanced two-means tree — selected first falsifier

Recursively split projected corpus vectors with deterministic balanced
two-means until every leaf fits the physical page byte cap. This directly
optimizes geometric coherence while enforcing bounded pages. It has a compact
tree representation and deterministic external construction. Forced balance
can cut natural clusters, so the exact page-coverage oracle must decide whether
it is viable.

### Capacity-bounded neighborhood-graph partitioning

Build a corpus-neighbor graph and partition it while minimizing cut edges.
This objective matches page containment more directly and is the next
representation hypothesis if balanced two-means fails. It is not the first
screen because graph construction, external-memory partitioning, mutation, and
compaction are substantially more expensive and harder to bound at 100M.

### Multiple PQ-locality orders

Spatially order subquantizer centroids, derive fixed interleavings, and store
two independent physical page orders. This can reduce boundary cuts, but it
doubles vector storage and compaction traffic while both layouts still share
one 32-GET budget. Naive lexicographic PQ-code order is invalid because codeword
ordinals have no geometric meaning. This option remains closed unless the
single-layout geometric hypotheses fail and a two-order oracle proves decisive
headroom.

## Phase 1: unbiased page-layout screen

The first implementation is a disposable evaluator, not production serving
code. It uses the already authenticated ReLAION development-100k source and
GT100 objects. This cohort has informed earlier work, so results are labelled a
development screen rather than a holdout.

Construction and evaluation are capability-separated processes:

1. The constructor receives only source vectors, opaque IDs, dimensions,
   metric, seed, and physical byte limits. Query and truth paths are absent
   from its environment and filesystem namespace.
2. It writes a typed Parquet membership artifact containing stable ID, page
   ordinal, in-page ordinal, encoded page length, layout method, source
   identity, seed, and construction hash.
3. The membership artifact is closed and authenticated before the evaluator is
   launched.
4. The evaluator receives membership plus frozen GT100. It never receives the
   constructor's mutable state or chooses a seed/layout after observing truth.

The preregistered arms are deliberately small:

1. lexicographic-ID pages at 256 rows, as the reproduction control;
2. deterministic balanced random-projection pages at 256 rows, as a cheap
   non-learned geometric control;
3. deterministic balanced two-means pages at 256 rows;
4. the same two-means method with a 480-KiB serialized page ceiling, expected
   to admit at most roughly 512 SQ8 rows at 768 dimensions.

The two-means implementation uses a fixed, query-independent SRHT projection
to `min(dimensions, 192)` float32 coordinates. Each split trains two centroids
with a fixed seed and iteration count, computes the signed difference in
squared distance, and assigns the sorted scores at the capacity-balanced cut.
Ties use stable ID bytes. IDs are sorted only inside the final page. Empty
clusters and non-finite arithmetic fail the arm; they do not silently change
the method.

For a single-owner layout with equal-size pages, the exact best 32-page GT100
coverage is the sum of the 32 largest per-page hit counts for each query. For
variable-size pages, the evaluator instead solves the joint 32-page and
16-MiB encoded-byte problem exactly with deterministic dynamic programming;
greedy hit/byte ranking is not authority. It records per-query selected page
ordinals, hits, bytes, and recall in Parquet and recomputes mean, p05, and
worst values independently.

The screen decisions are:

- invalid if the ID control does not reproduce 61.377% mean, 47% p05, and 41%
  worst Recall@100 within one hit per query where ties permit;
- kill an arm when its exact mean is below 97.5% or p05 is below 90%;
- advance only when exact mean is at least 99% and p05 at least 95%, providing
  margin for router and SQ8 ranking loss;
- classify values between the release gate and headroom gate as insufficient;
  do not implement a production router for them;
- report worst recall without inventing a new worst-query release threshold.

No latency claim is produced by this phase.

## Production page layout

If and only if the two-means screen passes, the production builder streams
source vectors into a bounded external partitioner. Partition state lives on
local build scratch, never in serving RAM. Each finalized leaf is encoded as
one Arrow IPC object whose actual encoded length is at most 480 KiB. Long IDs
or high dimensions therefore reduce rows per page rather than violating the
query byte cap.

Each page contains stable binary ID, mutation sequence, row state, and a
non-null fixed-size-list SQ8 vector. A page-local SQ8 low/step authority is
bound by the generation root. Page ordinals follow deterministic tree-leaf
order; rows within a page follow stable ID order. Explicit offsets, row counts,
and byte lengths replace every `row / 256` ownership assumption.

The builder preserves authoritative source vectors for future compaction.
Compaction never retrains from lossy SQ8 reconstructions.

## Resident page router

Serving keeps only a compact balanced tree and page-level representatives:

- one global deterministic SRHT description;
- 192-dimensional float32 split planes and thresholds for internal nodes;
- one full-dimensional SQ8 representative per page;
- a 64-byte page directory entry with object/range/row authority;
- fixed-capacity best-first heaps and per-query workspaces.

There is no resident per-row PQ array. A query is projected once and explores
the tree under a fixed node budget. The split-plane distance and page
representatives produce a deterministic routing priority, not a claimed metric
lower bound. The router scores a bounded page frontier and selects pages by
`(routing_score, page_ordinal)` until either 32 pages or 16 MiB would be
exceeded. Any later distance bound must be stored and proved separately. A
page-level bounded-degree graph may be tested later, but is not part of the
first production candidate.

At 100M rows and 256 rows/page there are at most 390,625 pages. The preliminary
resident estimate is:

- split planes: about 300 MiB for 390,624 × 192 × 4 bytes;
- page SQ8 representatives: about 286 MiB for 390,625 × 768 bytes;
- page directory: about 24 MiB at 64 bytes/page;
- thresholds, topology, heaps, quantizer authority, and alignment: below
  128 MiB by explicit worksheet;
- one generation total: below 0.75 GiB before caches and runtime reserve.

The final worksheet must include allocator capacity, two-generation refresh
overlap, bounded delta, decoded cache, concurrent response bodies, query
workspaces, and runtime reserve. A 100M build cannot start unless the complete
worksheet remains below 3 GiB.

## Query and I/O execution

Each query pins one authenticated generation and obtains one fixed CPU permit.
Routing runs synchronously on that permit or a collection-scoped fixed
executor. It never creates a Rayon pool and never schedules long scans on Tokio
I/O workers.

The reader coalesces only adjacent registered ranges inside one immutable
object. The scheduler independently enforces 32 logical pages, 32 physical
attempts including retries, and 16 MiB of response bodies. It authenticates
each slice before Arrow decode, applies latest-wins visibility, scores SQ8 rows,
merges the bounded mutable overlay, and returns stable `(distance, id)` order.
Budget exhaustion and executor saturation are typed observable outcomes, not
fallback triggers.

## Generations, mutation, and compaction

Writes first enter the durable WAL. A tightly bounded resident exact delta
provides immediate visibility. Flush creates immutable geometric micro-runs;
base plus all micro-runs share the same 32-GET/16-MiB query budget. Run count,
delta bytes, ID-directory bytes, and compaction backlog have explicit limits
and backpressure.

Latest sequence wins across every run. Tombstones remain until all older
overlapping runs retire. Compaction merges authoritative vectors for bounded
tree regions, splits overfull pages, rebuilds affected router nodes, validates
the complete replacement snapshot, and only then conditionally publishes the
new root. Old generations remain readable until pinned readers release them.

## Authentication and failure behavior

The canonical typed root binds schema name, local format version, generation,
predecessor digest, source identity, dimensions, metric, projection authority,
tree, page directory, page quantizers, mutation directory, base/micro-runs,
limits, and every object URI/SHA-256/encoded length. It contains no query,
truth, campaign, or benchmark-manifest role.

Open validates canonical bytes, exact schemas and concrete types, finite
values, tree topology, leaf coverage, unique artifact roles/URIs, byte caps,
page counts, mutation ranges, and the complete resident worksheet before
installing a snapshot. Failed staging or validation cannot publish visibility.
Previous experimental roots are rejected explicitly.

## Promotion sequence

1. Run the four-arm 100k page-layout screen and independently recompute it.
2. If one arm passes the headroom gate, test only its compact router containment
   on the same sealed pages; then add SQ8 ranking and the complete quality gate.
3. Prove 100k create/reopen, 1/10/100-run equivalence, mutation, crash-safe
   publication, and compaction.
4. Freeze the revision and run one ReLAION-1M Spot cell with cold/warm
   p50/p95/p99, QPS, GETs, bytes, RSS, build time, index bytes, and cost.
5. Reproduce the unchanged revision at 9.99M. Qualify sustained writes and
   visibility before considering 100M.

Every measured cell records source commit, seed, command, hardware, raw
per-query path, confidence/repetition method, artifact hashes, spend, and one
terminal receipt. S3 Vectors and TurboPuffer are compared only through matched
authenticated runs or clearly labelled first-party published numbers.
