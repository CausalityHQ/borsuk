# V32 selective code serving design

## Decision

Replace production residency of the global PQ code planes with an authenticated,
code-free routing directory and bounded Arrow code objects fetched only for the
routing microleaves selected by the query. The resident V32 router remains a
diagnostic oracle until exact differential qualification succeeds; it is not a
compatibility reader and is removed from the production construction path before
release.

This is the next serving slice, not a claim that the complete product is ready.
It proves that the existing quality-preserving route can execute without loading
the corpus or all PQ codes. Incremental write visibility and tiered compaction
remain a following architecture slice and must receive their own throughput and
failure-atomicity evidence.

## Why this approach

Three approaches were considered:

1. Keep the 2.52 GB projected 100M code planes resident. This preserves the
   proven route but consumes nearly the whole 3 GiB limit before routing
   metadata, pages, cache, concurrency, and allocator overhead.
2. Fetch one object per code parent or microleaf. This is bounded but creates
   excessive S3 request amplification and cannot pack skewed parents well.
3. Pack complete routing-microleaf ranges into root-local bounded objects while
   keeping only routing centroids and object identities resident. This preserves
   exact routing and PQ coordinates, admits deterministic packing across parent
   fragments, and bounds both resident memory and fetched bytes. Use this
   approach.

The format is new and strict. There is no legacy reader, alias, automatic
fallback, or old-manifest migration because BORSUK is pre-release.

## Existing verified dependencies

The implementation reuses these already-qualified components without changing
their arithmetic:

- `borsuk-v32-bounded-code-object-v1`, an authenticated uncompressed Arrow IPC
  object containing at most 8,192 rows, 32 parent records, 128 ranges, and
  524,288 encoded bytes;
- `V32ParentCursor`, which traverses validated mixed-width codes without copying
  or allocating a logical-row map;
- the shared `score_parent_codes` kernel, which is bitwise-equivalent to the
  resident base24/high48 ADC path and retains at most 12,288 candidates;
- the strict V32 page-location Parquet registry, page decoder, page reducer, and
  exact reranker.

The higher layer changes the global meaning of a parent record: one code parent
may have fragments in several objects, always with identical original f16
centroid bits. A routing microleaf range is indivisible and belongs to exactly
one object. This interpretation does not change the local Arrow object bytes,
but supersedes the earlier experimental statement that a whole code parent must
fit one object.

## Persistent authority

### Manifest

One canonical newline-terminated JSON manifest with exact key set and concrete
types is the commit point. It uses format
`borsuk-v32-selective-serving-manifest-v1` and binds:

- source commit, source archive SHA-256, dataset identity, exactly 96 dimensions,
  squared-L2 metric, and the frozen normalization rule;
- source row count, root count, nonempty code-parent count,
  routing-microleaf count, code object count, and physical page count;
- exact SHA-256 and encoded length for roots, base24 codebook, high48 codebook,
  every ordered parent/leaf/object-directory shard, and the page registry;
- one immutable generation ID below a fixed Standard S3 index prefix and an
  optional distinct same-AZ Express prefix; every artifact key is derived from
  the validated generation, role, ordinal, and authenticated digest;
- the exact serving arm: root beam, routing-leaf beam, scan budget, candidate
  depth 12,288, selected page count 16, maximum code-object requests, maximum
  fetched code bytes, and maximum page bytes 3,145,728;
- encoded-directory, decoded-directory, metadata-cache, and aggregate active
  query byte budgets.

The loader authenticates manifest bytes before semantic use. Fixed keys are
`roots.arrow`, `pq-base24.arrow`, `pq-high48.arrow`, and
`page-locations.parquet`; the three directory families use
`directory/{parents|leaves|objects}/{shard_ordinal:05}.arrow`. Code and page
objects use 256 digest-derived prefix shards under the immutable generation:
`codes/{sha256[0..2]}/{object_ordinal:08}-{sha256}.arrow` and
`pages/{sha256[0..2]}/{page_ordinal:08}-{sha256}.arrow`. It authenticates each
referenced resident artifact
before parsing and rejects an unrecognized
field, format, role, filename, digest, length, prefix, or configuration. Object
and page URIs are never accepted from directory rows.

### Normalized code directory

The directory is normalized into three Arrow tables so repeated object fragments
do not duplicate 192-byte parent centroids. Each table is split into ordered
uncompressed IPC shards of at most 64 MiB. A shard has one record batch, no
dictionaries, exact role/ordinal metadata, and no nullable field or child.
Shard boundaries occur only between rows.

`code-parents` has one dense row per nonempty parent, sorted by ordinal:

```text
code_parent_ordinal: UInt32
root_ordinal: UInt32
centroid: FixedSizeList<Float16, 96>
population: UInt64
```

`routing-leaves` has one dense row per microleaf, sorted by ordinal:

```text
routing_leaf_ordinal: UInt32
code_parent_ordinal: UInt32
object_ordinal: UInt32
routing_centroid: FixedSizeList<Float16, 96>
logical_start: UInt64
row_count: UInt32
```

`code-objects` has one dense row per object, sorted by ordinal:

```text
object_ordinal: UInt32
root_ordinal: UInt32
sha256: FixedSizeBinary(32)
encoded_bytes: UInt32
row_count: UInt32
first_routing_leaf_ordinal: UInt32
routing_leaf_count: UInt16
```

All new root, parent, leaf, and object ordinals use 32 bits; the legacy
hierarchy's 16-bit root ownership is not reused. Parent roots and object roots
are nondecreasing. Each object is root-local and owns one contiguous leaf-ordinal
span. Every production routing leaf contains 1..1,024 rows and every represented
parent is nonempty. This serving slice does not invent a balancing algorithm,
merge cells, or change routing geometry merely to satisfy storage limits.

Repeated code-parent records inside fetched objects must bind the one directory
parent's root and bit-identical finite centroid. Every routing-leaf ordinal occurs
once. Routing centroids are finite, nonzero, and bit-identical to construction.
Leaf logical ranges form a nonoverlapping exhaustive partition of
`[0, source_rows)` in leaf-ordinal order. Parent populations, object populations,
and global source rows are independently recomputed from leaves.

Each directory leaf tuple must occur exactly once in its fetched code object as
the matching parent range with identical parent ordinal, parent centroid bits,
logical start, and row count. The object may contain unselected leaves; only
selected directory leaves enter ADC. A seekable borrowed cursor precomputes the
base/high byte offsets at range boundaries from the fidelity bitmap, so skipping
an unselected range neither scores nor walks every row and creates no per-row map.

No second persisted root-to-object authority exists. Validation streams each
table's shards in manifest order, compacts flat columns, and drops the Arrow
source before opening the next shard. It derives root parent/leaf spans and
validates the explicit leaf-to-object column against object spans. The manifest
authenticates every shard; construction reconciliation proves the directory and
page/PQ build describe one logical population.

Selecting a root authorizes only in-memory scoring of its contiguous leaf span.
It never authorizes fetching every object in that root. Objects are derived
solely from final selected leaves, so growing root populations cannot silently
become a whole-root code download.

### Page registry and logical mapping

The existing page registry remains ordered by dense page ordinal and binds each
page SHA-256, encoded length, and positive row count. Validation derives page
logical starts by checked prefix sum and requires total rows equal
`source_rows`. Candidate logical ordinals are mapped by binary search over this
derived prefix table. The directory cannot introduce a second page assignment.
Exactly 16 distinct selected page identities and at most 3,145,728 encoded page
bytes are required before any page request.

## Global bounds and memory

Untrusted counts are rejected before allocation. The supported 1B envelope is:

- `1 <= source_rows <= 1,000,000,000`;
- `1 <= roots <= 4,096`;
- `1 <= code_parents <= 262,144`;
- `ceil(source_rows / 8,192) <= objects <= routing_microleaves <= 4,000,000`;
- `objects <= parent_fragments <= routing_microleaves`, and the total number of
  directory ranges is exactly the routing-microleaf count;
- `ceil(source_rows / 480) <= pages <= 2,400,000`;
- each object satisfies the existing 8,192-row, 32-parent, 128-range, and
  524,288-byte bounds;
- each directory family has 1..32 shards, each shard is at most 64 MiB, all
  authenticated directory bytes are at most 1,280 MiB, and the checked compact
  decoded projection is at most 1,280 MiB.

The code-parent count covers only nonempty parents represented by directory
ranges; empty trained parents are neither selectable nor assigned an object.
The compact resident representation uses flat arrays and offsets, not one heap
allocation per row. At the envelope maxima, parent rows at 208 bytes are about
52 MiB, four million routing rows at a conservative 216 bytes are about
824 MiB, four million object identities at an aligned 56 bytes are about
214 MiB, and
2.4M page identities at 48 bytes are about 110 MiB. These are deliberately
loose format ceilings, not expected production counts. Roots, codebooks,
offsets, and lookup tables are small relative to those terms. The implementation
computes checked encoded, decoded, cache, and active-query projections from
actual counts and struct widths; it does not claim the arithmetic subtotal is
observed RSS.

Startup validates raw Arrow metadata and buffer extents before materialization.
Only one shard's source buffer and nested arrays coexist with the growing compact
directory. A build is rejected unless the checked sum
`resident metadata + one decode shard + cache + admitted queries + refresh
overlap + runtime headroom` remains below 3 GiB. Opening a new manifest
generation reserves old-and-new snapshot overlap before allocating it.

## Query data flow

1. Acquire a byte-weighted admission reservation covering the query's declared
   code-object bytes, one decode slot, candidate state, and page bytes. The
   shared reservation pool, resident metadata, and bounded cache have a checked
   total below 3 GiB.
2. Normalize one finite 96-dimensional query. Score every authenticated root and
   select the exact root beam by `(distance, root_ordinal)`.
3. Score all routing centroids below those roots and select the frozen leaf beam
   by `(distance, routing_leaf_ordinal)`, extending exactly as the resident route
   specifies to reach candidate depth. Reject if selected population exceeds the
   frozen scan budget.
4. Derive the unique ordered code-object identities containing selected leaves.
   Starting from the frozen leaf beam, extend by the same total order as the
   resident route until reaching candidate depth, exhausting eligible leaves, or
   selecting 256 leaves. The 256-leaf ceiling is part of both the resident oracle
   and selective production arm, so it is deterministic approximation rather
   than a storage error. Selected objects cannot exceed selected leaves. Before any GET,
   require no more than 256 objects, 64 MiB, 256 selected ranges, or 262,144
   selected code rows. At build/open time, the sum of the 256 largest object
   lengths must be at most 64 MiB, making the byte cap unreachable by a valid
   query rather than a data-dependent service error.
5. Fetch objects in ordinal order through waves no wider than the manifest's
   frozen 16/32/64/128/256 arm. A store response
   preserves request order and returns only after every member succeeds. Validate
   length and SHA-256 before Arrow parsing, match the complete decoded object to
   its directory row, score only selected ranges through the shared parent
   scorer, then release decoded state before the next wave. There is one base24
   and one high48 lazy table pair per query, reused across objects. Consecutive
   fragments of the same parent reuse its current tables; a parent transition
   rebuilds them. Receipts record distinct parents, parent transitions, and
   actual table builds so fragmentation cost is visible.
6. Reduce the bounded candidates to exactly 16 physical pages. Authenticate and
   fetch those pages through the existing selected serving tier, decode and
   exact-rerank, and return ten deterministic matches.
7. Release the admission reservation after all response buffers and decoded
   arrays are dropped.

An object cache may retain authenticated encoded `Bytes` under its own manifest
budget. It is acceleration only, never authority. There is no whole-code-plane
cache, corpus path, page discovery, latest-object lookup, or implicit Standard
to Express fallback.

## Store and failure contract

The production boundary is asynchronous and intentionally narrow. The old
synchronous `V32PageStore` remains only on the resident diagnostic path:

```rust
#[async_trait::async_trait]
trait V32SelectiveStore: Send + Sync {
    async fn read_code_wave(
        &self,
        objects: &[V32CodeObjectIdentity],
    ) -> Result<Vec<bytes::Bytes>>;

    async fn read_page_wave(
        &self,
        pages: &[V27PageIdentity],
    ) -> Result<Vec<bytes::Bytes>>;
}
```

Each code input wave has 1..the frozen maximum of 256 identities; the page wave
has exactly 16. Results have the same length and order,
and every body has exactly its registered length before return. The production
adapter runs wave members concurrently through one persistent client and
   connection pool. It reads at most the registered length plus one byte and
   aborts an overlength body before unbounded collection. On any request,
   authentication, decode,
directory-match, or scoring failure, it cancels or drains outstanding work,
returns no partial search result, requests no later wave or page, and preserves
the first causal error. Retrying is an explicit caller policy and cannot silently
mix serving tiers or manifest generations.

## Construction and write boundary

The production builder streams query-independent corpus rows once after frozen
training. It emits deterministic pages and root-local code objects concurrently
through bounded queues. Pages partition the single global logical row axis;
page packing never restarts at a parent or routing-leaf boundary. Complete
routing microleaves are the code-object packing units; a
code parent may continue in later objects with identical centroid bits. Before
the sealed holdout, a burned development-only metadata simulator compares
exactly 1, 2, and 4 consecutive leaves per object and code wave widths
16/32/64/128/256 using exact selected leaf sets and encoded-size formulas. It
prunes dominated arms without claiming hash-shard distribution or construction
throughput. The real writer then encodes and hashes every surviving arm on the
same burned development layout and freezes the smallest-byte arm satisfying the
request/wave, hash-shard-skew, and measured construction-throughput gates. The
holdout cannot reopen that ladder. Production packing uses
deterministic `(root, routing_leaf_ordinal)` order, never exceeds four leaves per
object, and flushes earlier when any physical object bound would be crossed.

After all bodies are durably written and authenticated below a unique immutable
generation prefix, the builder emits the page registry, directory shards, and
canonical generation manifest. A failed build has no visible manifest and is
unreachable garbage eligible for generation-aware cleanup; it never overwrites
an older generation. An optional current-generation pointer is the final
conditional write.
Readers open through a trusted manifest digest and pin one immutable generation
for a complete query. If a mutable current-generation pointer is added, the
publisher updates it conditionally against the expected predecessor. Garbage
collection cannot reclaim an unreachable generation until active readers and
failed publications are accounted for. This establishes snapshot atomicity for
bulk construction. It does not establish low-latency incremental writes;
immutable delta segments, tombstone/version resolution, bounded fan-out,
backpressure, and compaction are the next design after selective serving
qualifies.

## Verification

Development is strict TDD and uses narrow gates before broader checks:

1. Directory mutation tests cover raw Arrow schema, nullability, buffer bounds,
   count/allocation caps, ordinals, roots, repeated-parent centroid equality,
   routing-leaf uniqueness, range coverage, object identities, page prefix sums,
   and decoded-memory projection.
2. Code-object matching tests cover fragment repetition, missing/extra/reordered
   parents and ranges, centroid-bit drift, selected-range filtering, and an
   unselected range that would otherwise change the candidate winner.
3. Store tests cover 1/16/17 requests, request-order preservation despite reverse
   completion, truncation, substitution, oversize, a failure after a successful
   wave, no later wave, and no page read after code failure.
4. Differential tests compare every retained candidate score bit and logical
   ordinal, 16/64-page prefixes, and final exact top ten between resident and
   selective paths for mixed widths, gapped fragments, parent/object delivery
   orders, ties, and multiple roots.
5. A structural test constructs the selective index without `V30CodePlanes`, a
   global fidelity bitmap, or `V32Router`. Production modules cannot expose a
   whole-code accessor.
6. One same-host ABBA diagnostic uses frozen 128 queries and identical physical
   pages. It requires exact candidate/page/result parity and records code GETs,
   code bytes, cache hits, logical requests, retry-inclusive physical attempts,
   fetch/decode/score/page/rerank elapsed, process CPU, peak RSS, and request
   concurrency. It is not a release qualification.
7. Only after the local fail-fast gates pass, run one preregistered 1M Spot
   selective-serving experiment with the `causality` profile. A later 100M build
   and holdout are authorized only when request, byte, CPU, RSS, recall, and
   throughput gates survive unchanged.

The simulator and serving receipt also report object/page key-shard distribution,
retry-inclusive physical requests, throttles, and maximum connections. The
digest-derived 256-way key layout must not concentrate more than the registered
skew bound; a throttled run fails throughput qualification rather than silently
raising retries.

## Shape-aware routing experiment after parity

Selective storage preserves the existing routing score first. It does not assume
that one centroid is the best representation of a 96-dimensional anisotropic or
multimodal leaf. After exact selective/resident parity, run a separate
query-independent routing diagnostic over identical leaf populations, row scan
budget, candidate depth, and 16-page reducer:

- centroid squared-L2 baseline;
- enclosing sphere/ellipsoid boundary distance, reported cautiously because a
  large radius can systematically favor diffuse cells;
- a quantized diagonal or low-rank covariance score;
- 1, 2, and 4 query-independent micro-prototypes per leaf, with int8/f16 memory
  projections. Eight f16 prototypes already cost roughly 1.5 GiB per million
  leaves and exceed 6 GiB at the four-million-leaf envelope, so they are not
  compatible with the complete 3 GiB process budget.

Use about 2,000 fresh source-distribution queries never used by prior V32
experiments. Hash the cohort, burn one development split to choose at most one
shape arm, and evaluate the sealed split exactly once. Exact GT@10 and the
selected leaf/page containment stages attribute misses. The experiment passes
only if it improves minimum and aggregate containment without increasing the
frozen scored-row, selected-page, code-byte, CPU, or resident-memory limits.
Repeated-query adaptation, truth-derived prototypes, and tuning on the sealed
split are forbidden. A shape arm that passes receives a new format marker and
new production qualification; it is never smuggled into the storage parity run.

No quality gate is loosened: development still requires 1,000,000 ppm aggregate
and minimum Recall@10 for the frozen perfect-recall cohort. Standard S3 cold
latency is measured honestly rather than held to the withdrawn 15 ms veto.
Warm/same-AZ latency, sustained read throughput, bulk write throughput, request
and byte amplification, visibility delay, and memory are independent reported
axes. A selective-serving pass does not qualify incremental write throughput,
1B operation, or release readiness by itself.
