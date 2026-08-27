# Standard-S3 V21 Cold-Read Design

**Status:** Approved direction; corrected written design pending operator review.

**Supersedes:** `docs/research/cold-read-latency-design.md` for the next
pre-release dense-ANN format. V20 results remain immutable evidence tied to
their exact source and format.

## Objective

Make strict-cold dense-vector search fast on ordinary Amazon S3 while retaining
bounded RAM, high recall, deterministic builds, and the existing production
write authority.

The Deep Image 10M release gate is:

- S3 Standard is the only exact-vector backing store; no S3 Express, local
  serving tier, or query-populated object cache;
- recall@10 at least `0.975` on the untouched publication query set;
- cold p50/p95/p99 at most `60/80/100 ms`, with p99 `75 ms` the engineering
  target;
- zero query-time selector/head GETs and one exact wave containing at most four
  physical S3 GETs, including any hedge, and at most `1 MiB` backing bytes;
- process peak RSS at most `768 MiB` under the registered runtime contract;
- decoded selector authority at most exactly `40,000,000 bytes` (4 bytes per
  Deep Image row), charged to the retained pool;
- no swap, OOM, detached request, unaccounted allocation, or request that
  outlives its query;
- a primary paper claim only if paired BORSUK cold p99 is at least 20% below
  the fastest valid competitor at matched recall.

Other registered dense profiles retain the universal four-GET, 1-MiB, memory,
and recall contracts, but do not inherit Deep Image's latency claim without a
separate frozen qualification. Sparse, text, and late-interaction layouts are
not rewritten in this slice.

## Evidence and Root Cause

The frozen V20 Deep Image 10M cell at source
`bb283705b995becf9d2f6cc0f6817c1c45d8666f` measured recall `0.9773`,
p50/p95/p99 `232.649/299.246/337.712 ms`, `54.232` GETs and `29.788 MB`
per query, and peak RSS `639,549,440` bytes. The query spent about 42.5 GETs
and 29.3 MB on row-code heads before a second wave of about 11.7 GETs and
0.48 MB for exact rows.

A same-host S3 Standard experiment measured 64-KiB wave p99 of `43.333 ms`
at width one, `57.950 ms` at width four, and `100.881 ms` at width twelve.
The smallest credible Standard-only architecture therefore removes the head
wave and caps the remaining physical wave at four requests.

## Selected Architecture

V21 keeps the existing top-level IVF router and bounded global-leaf builder.
It removes the per-row PQ code plane from group objects. Instead, a compact,
mandatory resident directory stores a few representative PQ codes and a
calibrated spread for each physical exact bundle. Queries rank bundles in
RAM, issue one scoped exact wave, authenticate and decode the returned Arrow
ranges, and use the existing SIMD exact kernel.

The format is `cell-card-leaf-v21`. There is no V20 reader, migration, alias,
or dual-write path.

## Physical Format

### Exact bundle

One bundle contains rows from exactly one logical cell and one deterministic
builder wave. Its target row limit is derived, never assumed:

```text
row_limit = min(256, configured_exact_payload_bytes / authoritative_row_bytes)
```

The encoder may shorten the range to satisfy the existing 96-KiB exact-vector
payload target and 128-KiB encoded-page cap. Adjacent fitted page ranges are
greedily merged only while both hard byte caps and `row_limit` still hold;
this avoids the half-empty occupancy produced by recursive binary halving.

Each bundle is one independently authenticated, self-describing Arrow IPC
range. Its root reference contains group dictionary ordinal and checksum,
offset, Arrow metadata bytes, body bytes, total bytes, row count, cell and
bundle ordinals, bundle checksum, and schema fingerprint. A query never reads
a group footer or schema separately.

Group objects are content addressed and capped at 48 MiB. A bundle never
crosses a group boundary.

### Selector representatives

Bundle rows remain in the current deterministic two-pivot locality order. They
are divided into contiguous selector regions with a preregistered maximum span
of either 32 or 64 rows. For each region the root stores:

- one code from the generation's authenticated global quantizer;
- one finite `f16` spread estimate in the quantizer's score domain;
- its row span.

The score used to rank a bundle is the minimum of
`approximate_distance(query, centroid) - spread` over its regions. Stable
ordinal fields break ties. Because product-quantized distance is not a metric
lower bound, the spread is explicitly a ranking calibration term, not proof
that the bundle contains or excludes a neighbour. Coverage is established only
by the claim-ineligible ground-truth qualification.

### Resident directory

The decoded directory is structure-of-arrays: a dictionary of unique group
paths/checksums, fixed-width bundle-reference columns using group ordinals, a
contiguous code slab, spread and span slabs, and `cell_count + 1` offsets. It
contains no per-bundle `String`, `Vec`, `Box`, `Arc`, map, or tree node.
Memory reports use allocated capacities, including dictionary payloads.

Directory shards are canonical Arrow IPC objects over contiguous cell ranges,
each at most 128 MiB. A small catalog authenticates cell ranges, checksums,
encoded and decoded byte counts, and aggregate rows/bundles/regions. Open
validates aggregate counts before allocation and rejects decoded authority over
the exact configured byte cap.

Preparation reserves final retained slabs, decodes one shard at a time, and
holds encoded and batch scratch under a transient permit. The directory is
immutable for a pinned manifest generation and shared by `Arc` among clones
and read scopes. Named-vector roots are distinct but share the collection-wide
retained pool.

No bundle-count or directory-size estimate is derived from `rows / 256`.
Qualification records the real fitted bundle histogram and computes decoded
bytes from actual capacities. Deep Image must fit in `40,000,000` bytes; a
100M profile must fit in `600,000,000` bytes and the registered 3-GiB process
budget.

## Deterministic Construction

V21 reuses the current bounded global-leaf pipeline:

1. Greatest-version resolution emits the authenticated deterministic spool
   waves used by V20.
2. Rows are grouped by logical cell within a wave and ordered with
   `sort_global_leaf_rows_by_two_pivot_locality`; all ties use canonical row
   authority.
3. `fit_global_leaf_page_ranges` applies the derived row and byte limits, then
   greedily merges adjacent fitted ranges when legal.
4. Each resulting range becomes one exact bundle and receives the next stable
   per-cell ordinal.
5. Each selector region centroid is accumulated sequentially in f64 by
   component over canonical row ordinal and metric-normalized exactly as the
   current page-centroid path. Its spread is the maximum finite region-row
   score from that centroid, rounded outward to f16.
6. The existing `GlobalScanQuantizer::encode` produces the representative
   code. Exact rows are encoded into the group object; per-row codes are then
   released and are never published in V21.

The reduction order is defined by global canonical row ordinal, not spill
chunk or thread completion order. Every bounded region is reduced from its
canonical row sequence after the deterministic wave/page boundaries are fixed,
so spill scheduling cannot change the floating-point operation order. Build
scratch remains the existing bounded spool/page/group working set; V21 does
not add recursive object-store scratch or a corpus-sized clustering pass.

The global quantizer is generation authority. The current complete-generation
rebuild may refit it; every directory and group reference in that generation
uses the same codebook digest.

## Query Execution

The existing IVF router selects logical cells. Query setup prepares the global
ADC tables once. The SIMD selector scans only directory regions in routed
cells and ranks bundles by `(adjusted_score, cell, bundle, group, offset)`.

The exact planner consumes that order incrementally. It retains the longest
prefix satisfying:

- at most four physical S3 GETs, counting speculative hedges;
- at most `1 MiB` physical bytes;
- at most 2x physical amplification over selected bundle bytes;
- every physical range at most the 128-KiB encoded-bundle cap.

Adjacent selected bundle ranges may coalesce, but an unselected lower-ranked
bundle cannot change the range or cost of an earlier prefix. The planner
reports candidate rows, a configured candidate target, and the literal
`limiting_bound`; the target is not a feasibility assertion. If bytes or
requests prevent the target, telemetry reports candidate starvation rather
than silently representing it as a complete candidate plan. If the first
bundle does not fit, the query fails closed.

All admitted ranges launch in one scoped parallel wave. Each task retains its
I/O and transient permits through authentication and decode. Error and
cancellation paths drain or abandon siblings before return. No selector read,
group footer read, or second exact wave is permitted.

The existing S3-Standard range-hedge primitive remains available behind a
typed delay whose default is off. A claim cell may select `off`, `20 ms`, or
`35 ms` only during preregistered tuning. Enabling it reserves one of the four
physical-request slots, so the primary plan admits at most three ranges and the
duplicate hedge can never produce a fifth GET. The actual V21 range-width floor
must be remeasured before this arm is frozen.

Fetched rows are authenticated before Arrow decode, charged by actual decoded
bytes, filtered by the pinned manifest and mutation authority, and reranked by
the existing SIMD exact kernel and stable top-k tie break.

## Mutation and Generation Authority

This latency slice does not invent base/delta selector directories. Current
foreground writes continue into the resident WAL overlay and are visible to a
pinned handle without an S3 request. Current materialization/compaction builds
a complete new global ANN generation from all authoritative segments, refits
the generation quantizer if required, constructs its complete V21 directory,
and publishes it atomically.

Therefore the immutable portion of every query has exactly one generation
directory and one four-request wave. Foreground append remains off the bundle
construction path. Full-generation materialization cost is measured honestly
against V20 and is a rejection gate; V21 does not claim incremental immutable
delta publication.

Group objects and selector shards are written and authenticated first, then the
catalog/root, then manifest and collection snapshot under existing CAS
authority. Partial failures leave only unreachable content-addressed objects.
Pinned old handles remain readable until ordinary GC authority permits
deletion.

Open rejects V20, noncanonical ordering, duplicate or overlapping ranges,
row/byte/region limits, non-finite or inconsistent spreads, code width or
codebook mismatch, aggregate row disagreement, schema mismatch, and every
checksum mutation.

## Memory and Scalability

No new pool is introduced. Directory slabs are retained; ranking vectors,
range bytes, Arrow decode, and exact scoring scratch are transient. Permits
outlive the allocations they account for.

Deep Image limits are the exact 40,000,000-byte directory cap, at most 1 MiB
physical exact bytes/query, an actual decoded-row bound derived from selected
row width plus Arrow overhead (not a fixed 8-MiB allowance), and 768 MiB peak
RSS. The 100M directory cap is 600,000,000 bytes under the 3-GiB registered
process budget. Query CPU scales with routed directory entries, not corpus
entries. Build memory remains bounded by existing wave, page, group, and root
writers.

`OpenOptions` gains one typed V21 selector-retention cap, clamped by the total
memory budget. Zero rejects V21 rather than falling back to query-time heads.

## Telemetry and Measurement Boundary

`SearchReport` records routed bundles/regions, resident directory bytes,
selector CPU, selected bundles/rows, candidate target, candidate starvation,
exact requests and selected/physical/speculative bytes, queue/service/decode/
rerank durations, termination stage, and limiting bound. Storage counters must
reconcile every GET and byte.

Mandatory directory load is startup, not query work. Publication reports its
GETs, bytes, wall time, decode peak, and resident bytes separately. Strict-cold
query latency uses a fresh prepared handle per query, no disk cache, and must
show positive query-scoped Standard-S3 activity. For honesty against managed
competitors, the paper also publishes an end-to-end fresh-open + prepare +
first-query distribution; it is not substituted for the query-only cold number.

## Qualification

### Phase 0: claim-ineligible feasibility

Before implementing the full format, a deterministic directory simulator runs
over frozen training-only authority using real logical-cell histograms and
exact rows. It tests bundle row targets `{128, 256}`, selector spans `{32, 64}`,
and hedge delays `{off, 20 ms, 35 ms}`. Request count remains fixed at four;
the hedge consumes one of those physical requests.

For every point it records actual bundle/region histograms, decoded directory
capacity, GT-top-10 bundle coverage before exact scoring, candidate rows,
coalescing, selected/physical bytes, and request count. A point is eligible only
if projected directory fits the exact cap, every query fits four GETs/1 MiB,
selector GT coverage is at least `0.990`, and final training-only recall is at
least `0.975`.

The lowest-memory eligible point is frozen. If no point passes, implementation
stops before the paid build and the next architecture iteration makes selector
regions independently fetchable; gates are not loosened. This explicit
feasibility stage prevents a full-format build around an unproven coarse
selector.

### Paid Deep Image 10M

After source freeze, build a new V21 index and run five serial Spot repetitions
over the untouched 1,000-query publication set. Terminal attempts are receipt
bound; interrupted attempts remain separate. Every objective gate must pass.

The same source runs non-regression gates:

- bulk ingest at least 95% of V20;
- full finalization/materialization wall at most 125% of V20 within the same
  memory/storage contract;
- foreground mutation throughput at one and eight writers at least 95% of V20
  and batch p99 at most 110% of V20;
- four-worker strict-cold throughput at least 1.5x V20 with p99 at most 150 ms
  and the same request/byte bounds.

### Competitors

Run identical corpus, query order, ground truth, k, recall floor, client region,
and disclosed first-pass event against S3 Vectors and Turbopuffer. Preserve
first and repeated passes separately. No superiority claim is allowed without
the 20% paired cold-p99 margin.

## Required RED-to-GREEN Verification

1. Stable bundle bytes across input permutation, concurrency, and an explicit
   in-memory-versus-spilled canonical reduction that crosses 64 MiB.
2. Derived row/page limits for every registered element width and dimension.
3. Region centroid, outward spread, and adjusted scoring for every supported
   metric and global quantizer type.
4. Root/group round trips plus exhaustive schema, cap, order, overlap, spread,
   checksum, codebook, and aggregate-count mutation rejection.
5. Exact incremental planning never exceeds four actual GETs, 1 MiB, 2x
   amplification, or the selected prefix; candidate starvation and every
   limiting bound are tested.
6. Strict-cold search performs zero selector/head/footer GETs and reconciles
   positive exact GETs/bytes against an instrumented object store.
7. All fetch tasks and permits terminate on success, storage/decode error,
   cancellation, admission rejection, and hedge win/loss.
8. One immutable generation plus WAL overlay preserves current MVCC results;
   sequential materialization publishes a complete new generation.
9. Retained/transient accounting covers real capacities for one and concurrent
   searches without double ownership.
10. Foreground append/WAL paths never invoke bundle construction.
11. Creation, open, full materialization, named vectors, GC, and pinned reads
    remain correct.
12. Publication rejects cache service, head/footer GETs, excess requests/bytes/
    RSS, startup omission, incomplete telemetry, recall or latency failure.
13. Focused tests, format, strict workspace Clippy, repository assurance, and
    read-only cross-provider review are green before any paid build.

## Non-Goals

- No S3 Express, local serving tier, CDN, or shared query-result cache.
- No V20 compatibility or migration.
- No warm number in the primary cold comparison.
- No recall weakening to buy latency.
- No incremental immutable delta-directory design in this slice.
- No sparse, lexical, text, or late-interaction rewrite.
