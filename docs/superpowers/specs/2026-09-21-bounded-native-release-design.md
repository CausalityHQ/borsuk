# Bounded Native Reader Release Design

## Purpose

BORSUK needs one honest production reader, not another experimental router.
The two-summary PQ16 hierarchy is closed by V108: even exact-f32 scoring of
its selected pages reached only 83.747% average Recall@100 on the frozen
ReLAION-1M development split. It must not remain the advertised native path.

The strongest already-measured BORSUK mode is the single-wave bounded reader:
on the same 1M corpus and 1,000 queries it measured 99.280% Recall@10,
99.026% average Recall@100, 97% p05 Recall@100, and 82% worst-query
Recall@100. Its reused-client latency was 41.77/65.51/89.75 ms at
p50/p95/p99 and its measured peak was 182.39 QPS at concurrency 128. This
design turns that mode into the pre-release library reader while retaining the
native generation, mutation, and compaction semantics already verified in the
main crate.

This is a bounded release, not a 100M claim. The reader rejects an index whose
authenticated resident-state worksheet exceeds its configured process budget.
Historical benchmark artifacts remain evidence only and are not production
inputs.

## Release contract

The qualified operating envelope is frozen before the paid 1M cell:

- Euclidean float32 vectors with an authenticated dimensionality;
- 256 physical rows per page;
- page-summary routing followed by PQ64 row-code ranking and exact SQ8 page
  scoring;
- average Recall@10 at least 96%, average Recall@100 at least 97.5%, and p05
  Recall@100 at least 90% on the frozen 1,000-query ReLAION-1M development
  split;
- one bounded page-fetch wave, no serving fallback, and exact `(distance,id)`
  final ordering;
- cold p95 at most 250 ms, with cold and reused-client measurements reported
  separately;
- all resident state, decoded caches, in-flight responses, and workspaces
  admitted under an explicit byte budget;
- no nested Rayon or per-query work-stealing pool. Query concurrency owns a
  fixed CPU permit and a fixed range-I/O permit;
- visible pending puts and deletes, immutable generation refresh, deterministic
  compaction, and one/ten/one-hundred-run equivalence;
- query and ground-truth files are never index artifacts.

The 1M cell records actual GETs, bytes, resident bytes, peak RSS, QPS, and
cost. Any larger corpus remains unsupported until its complete worksheet and
unchanged-revision qualification pass.

## Production format

One sorted compact canonical JSON root with exactly one trailing LF binds all
objects by role, URI, SHA-256, encoded length, format version, generation,
previous-generation digest, source identity, dimensions, metric, page rows,
and quantizer identity. It names:

1. `route.json`, a typed canonical root that records the fixed page-summary,
   row-code, shortlist, range-coalescing, and memory limits;
2. `route-summaries.parquet`, page-major rows with a non-null page ordinal,
   block ordinal, and non-null fixed-size-list float32 summary;
3. `route-codebooks.parquet`, PQ64 codewords with exact subspace ownership and
   non-null fixed-size-list float32 centroids;
4. `route-row-codes.parquet`, exactly one non-null fixed-size-list u8[64]
   value per base physical row in page-major order;
5. the existing typed mutation directory;
6. ordered immutable base and delta run objects; and
7. exact SQ8 low/step authority for page decoding.

Base and delta pages are Arrow IPC streams. Each row contains a stable binary
ID, mutation sequence, state, and a non-null fixed-size-list u8 SQ8 vector.
Page objects remain remote. The root never embeds benchmark queries, truth, or
dataset-specific paths. Native Rust layouts, NumPy, pickle, and inferred path
identity are forbidden.

There is no compatibility reader. The format version changes and the library
rejects previous experimental native roots.

## Open and admission

Opening a generation authenticates the root before reading child objects. It
then validates complete Arrow/Parquet physical schemas, exact row counts and
ordering, finite codebooks/summaries, quantizer bindings, page/run ranges, and
all object identities.

Before allocating resident routing data, the loader computes the complete
worksheet from authenticated counts:

- page summaries;
- PQ64 row codes;
- codebooks and SQ8 scalars;
- mutation directory and resident delta;
- decoded router allocations;
- the configured decoded-page cache;
- all active I/O response permits;
- fixed CPU workspaces; and
- a declared runtime reserve.

The open fails before allocation when checked arithmetic overflows or the
total exceeds the configured resident budget. The release default is sized by
the measured 1M cell rather than extrapolated to 100M.

## Query execution

Each query pins one immutable generation and obtains one search admission
permit. Routing proceeds as follows:

1. score every registered page summary with deterministic float32 arithmetic
   and retain a bounded page frontier by `(distance,page)`;
2. score PQ64 row codes only in that frontier using a fixed-block SIMD kernel
   with scalar differential tests;
3. retain a bounded row shortlist, map rows to unique pages, and sort pages by
   registered physical order;
4. coalesce only adjacent ranges inside the same immutable object, never
   crossing a registered run or exceeding the per-response byte permit;
5. issue one bounded asynchronous range wave;
6. authenticate and decode every returned Arrow page, apply snapshot
   visibility, score SQ8 rows exactly under the registered decoder, merge
   resident delta and pending WAL records, and return stable `(distance,id)`
   order.

CPU work runs on a fixed dedicated executor or synchronously under a bounded
permit. It never creates a Rayon pool per query and never lets Tokio I/O
workers execute long scans. Admission rejects excess work rather than growing
an unbounded thread or task queue.

## Mutation, generations, and compaction

The reader reuses the main crate's verified semantics:

- pending live WAL rows are scored beside the pinned snapshot;
- pending tombstones shadow committed rows immediately;
- immutable delta rows use latest-sequence-wins semantics;
- refresh installs a complete authenticated generation atomically;
- failed refresh leaves the previous snapshot available;
- compaction output is byte-independent of input enumeration order; and
- publication acknowledges only after the conditional generation-head update.

The router is rebuilt only for a new compacted base generation. Bounded delta
and WAL overlays do not mutate base routing objects.

## Observability and failure behavior

Every search report exposes mode, generation, summary blocks scored, row codes
scored, pages selected, physical GETs, response bytes, decoded bytes, cache
hits, queue wait, route time, I/O time, decode/score time, and total time.

The reader fails closed for schema additions or omissions, URI/digest/length
drift, non-canonical roots, non-finite data, invalid physical nullability,
unordered or duplicate pages/runs, range escape, mutation sequence ties,
memory-budget overflow, response-budget overflow, executor saturation, and
incomplete generation publication. It never falls back to `pq-scan`, flat
scan, a benchmark manifest, or a different persisted format.

## Verification and promotion

1. Unit fixtures: exact schemas, canonical authority, memory worksheet,
   scalar/SIMD equality, deterministic ties, bounded queues, latest-wins,
   pending puts/deletes, refresh, and compaction.
2. Real 100k gate: exact quality, 1/10/100-run equivalence, mutation and
   compaction equivalence, memory accounting, and a forced admission failure.
3. One immutable ReLAION-1M Spot cell: the frozen 1,000 queries, cold/reused
   latency, QPS at concurrency 1 and the registered batch concurrency, GETs,
   bytes, RSS, build time, index bytes, and cost.
4. Only if the 1M cell passes may the same revision be considered for a larger
   corpus. A new scale requires a complete resident worksheet and does not
   inherit a 100M claim from the 1M result.

The machine-readable benchmark table and concise operator table are updated
from authenticated per-query samples. Estimated and blocked fields remain
explicit; projections are never reported as measurements.
