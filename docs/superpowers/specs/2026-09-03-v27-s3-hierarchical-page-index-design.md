# V27 S3-Native Hierarchical Page Index Design

## Decision

Replace the unreleased local-full-corpus PQ4 fan-out serving design with an
S3-native page index. Serving keeps only a compact query-independent router,
page summaries, immutable identities, and a bounded optional cache locally. It
never materializes the complete corpus, complete vector plane, or one code per
corpus row. Each query chooses at most ten immutable Arrow pages, reads them in
one concurrent S3 wave, and exact-reranks only their vectors.

This is a format replacement, not a compatibility layer. V27 has new artifact
schemas and rejects V23--V26 experimental indexes. Historical results remain
immutable evidence for their original architectures.

## Evidence driving the replacement

The reduced V26 ten-shard integration searched 100,000 real rows successfully
but exposed that its result serialized a fixed 100,000,000-row work claim. More
fundamentally, the V26 production design requires every PQ code and exact vector
snapshot to be downloaded before serving. That contradicts the required
object-store architecture even if its local query latency passes.

The authenticated Deep-Image snapshot contains seven objects and
4,249,296,061 bytes. A bounded S3 probe from the current host measured 41.814 ms
median GET startup, 88.24 MiB/s serial transfer, and 324.05--360.07 MiB/s over
four concurrent streams. Cold materialization projects to 46.218 seconds
serially or 11.338--12.589 seconds with four streams. This is acceptable only
as startup evidence; it cannot be hidden inside an 11.466 ms resident-search
claim.

Earlier page experiments establish two constraints. Existing fixed layouts
can achieve high oracle containment but their routers require hundreds of page
candidates, while one-mean and fixed-mode page summaries do not concentrate a
small enough frontier. V27 therefore co-designs the page layout and hierarchy
instead of adapting another selector to a frozen failed layout.

## Corpus and leakage boundary

Construction is query-independent. It may stream the training corpus from S3
in bounded batches, but it receives no query, truth, evaluation ordinal, or
result capability. Evaluation artifacts become available only after every
router, page, checksum, and layout receipt is immutable.

Deep-Image development and sealed cohorts are disjoint and fixed before
construction. Architecture choices use only the development cohort. The sealed
cohort is run once from a committed source and frozen artifacts. The synthetic
100M campaign uses a separate dataset identity and cannot support a Deep-Image
quality claim.

## Query-independent construction

V27 trains a two-level balanced IVF hierarchy from a deterministic hash sample
of corpus rows:

- 1,024 root centroids;
- 65,536 leaf centroids, exactly 64 children per root;
- f16 centroid storage with f32 accumulation and deterministic ordinal ties;
- fixed seed, sample rule, iteration count, and empty-cluster repair.

A second bounded corpus stream assigns each row to its nearest primary leaf.
Rows whose second-leaf distance margin is below the preregistered boundary are
replicated once, subject to an exact global 15% replica ceiling. Records are
externally sorted by `(leaf, projection_key, source_ordinal)` and packed into
pages of at most 1,024 primary-plus-replica rows. No complete row collection is
resident during construction.

Each page stores up to four deterministic f16 modes derived only from its own
rows. Leaf-to-page postings contain page ordinals, primary/replica counts, and
the exact page identity. The layout receipt proves that every source ordinal
has exactly one primary owner, at most one replica, and that the complete
primary union equals the corpus authority.

## Cross-language artifacts

The persistent format is deliberately small and explicit:

- `roots.arrow`: 1,024 nonnullable `f16[96]` centroids;
- `leaves.arrow`: 65,536 nonnullable `f16[96]` centroids plus root ownership;
- `page-postings.parquet`: sorted leaf-to-page rows with concrete integer
  types, counts, and identities;
- `page-modes.arrow`: up to four nonnullable `f16[96]` modes per page;
- `pages/<sha256>.arrow`: nonnullable `id:fixed-binary[8]` and
  `vector:fixed-list<element:f32;96>` columns;
- canonical compact newline JSON manifests and terminal receipts;
- Parquet query, truth, samples, resource, and latency evidence.

Schemas are exact and versioned. There are no aliases, legacy readers,
runtime-loader manipulation, generated linker paths, or identity inference
from filenames. SHA-256 plus encoded length authenticates every role before
semantic use.

## Resident memory bound

At 100M rows and a 15% replica ceiling, at most 115M stored rows produce at
most 112,305 pages. The conservative resident projection is:

- root centroids: `1,024 * 96 * 2 = 196,608` bytes;
- leaf centroids: `65,536 * 96 * 2 = 12,582,912` bytes;
- root/leaf offsets and ownership: at most 1 MiB;
- four page modes: `112,305 * 4 * 96 * 2 = 86,250,240` bytes;
- page/posting identities and decoded metadata: at most 32 MiB;
- scoring, heaps, exact-rerank buffers, and allocator reserve: 128 MiB;
- optional content-addressed hot cache: 256 MiB hard cap.

The serving process must project below 512 MiB before opening and remain below
768 MiB observed RSS. The release-wide 3 GiB ceiling remains an outer bound,
not a target. No row-proportional code or vector plane is resident.

## Query algorithm

For one normalized f32 query:

1. score all 1,024 roots with deterministic f32 accumulation;
2. retain a preregistered development-selected beam from `{8,16,32}` roots;
3. score only their leaf children and retain leaves from `{64,128,256}`;
4. gather leaf-to-page postings into a bounded page accumulator;
5. score the associated page modes and order by `(distance,page_ordinal)`;
6. choose the first ten unique pages;
7. issue exactly one concurrent S3 wave;
8. authenticate and decode each Arrow page, then exact-rerank at most 10,240
   rows by `(squared_distance,source_ordinal)`.

The selected root beam, leaf beam, and page count are chosen on development
data only by the smallest lexicographic arm that passes all quality and work
gates. The sealed holdout receives that one arm. Empty, duplicate, reordered,
unauthenticated, late, or partially failed page reads fail the whole query.

## S3 and cache contract

The ordinary cold path performs no more than ten GETs and reads no more than
4,587,520 encoded page bytes. GETs begin together; there is no dependent second
wave. Standard S3 is tested first. If its same-region cold p99 cannot meet the
registered cold gate, the identical immutable page format is qualified in an
S3 Express One Zone directory bucket colocated with the serving instance. The
store is frozen before the sealed run; results from different stores are never
combined.

The optional hot cache is content-addressed, bounded at 256 MiB, and evicts by
deterministic LRU. Cold qualification starts with an empty cache and reports
zero hits. Hot-cache latency is a separate secondary result and never replaces
the cold result. No full-index warmup or persistent local corpus is allowed.

## Gates

The development fail-fast gate requires all 32 registered queries to recover
all ten official neighbors. The sealed Deep-Image gate requires:

- exactly 1,000,000 ppm aggregate and minimum Recall@10;
- exactly ten or fewer selected pages and one S3 wave per query;
- no more than 4,587,520 page bytes per query;
- router plus exact-rerank CPU p99 at most 15 ms;
- Standard-S3 cold end-to-end target p99 at most 100 ms and hard ceiling
  150 ms, or an explicitly separate S3 Express qualification;
- resident projection below 512 MiB, observed RSS below 768 MiB, zero swap
  growth, and memory PSI full avg10 at most 1%;
- exact recomputation of every query result and aggregate from typed Parquet
  evidence.

The 100M synthetic scale gate additionally requires 100,000,000 unique primary
ordinals, the 15% replica ceiling, complete page/authentication coverage, and
the same resource/request bounds. It is not a Deep-Image or competitor claim.

## Fail-fast ladder

1. Unit contracts cover hierarchy, page packing, exact identities, bounded
   work, and truthful counters.
2. A reduced 100K separate-process test builds pages and runs real local object
   reads with deterministic injected latency; it must finish in seconds.
3. A no-page-body Deep quality screen reuses immutable row/assignment evidence
   to reject any arm that cannot contain all truth rows in ten selected pages.
4. A bounded real S3 test reads only registered small pages and measures GET
   waves, bytes, and cold latency before any corpus-scale build.
5. One 9.99M Deep construction and sealed quality run occurs on `causality`
   Spot only after steps 1--4 pass.
6. One 100M synthetic construction and serving run follows only after Deep
   quality and S3 latency pass.

No full workspace suite runs between scientific iterations. Focused selectors
and the seconds-long affected gate run after each repair; strict Clippy and the
full locked workspace suite run once at a stable release milestone.

## Monitoring and termination

The controller polls every original Spot attempt at 30-second intervals and
reports phase, rows/pages, GET waves, bytes, elapsed time, RSS, PSI, swap,
instance checks, and spend estimate. It never duplicates quiet work. It stops
on impaired checks, three PSI breaches above 1%, any swap growth, five minutes
without build progress, one minute without query progress, the registered wall
cap, or any authority failure. Only an explicit Spot interruption permits a
fresh attempt prefix. Every terminal instance uploads its receipt and is
terminated immediately.

## Release disposition

V27 replaces the current local-full-corpus production candidate. Until its
sealed Deep and 100M gates pass, BORSUK has no release-ready S3 ANN claim. The
existing PQ4 local result remains useful evidence about code fidelity and CPU
speed but cannot be described as S3-serving performance.
