# Vortex Object-Role Investigation — 2026-07-26

Status: historical revision-bound evidence. The schedule-locked v13 AWS normal-segment qualification is complete
and independently reproduced. Every cross-backend Vortex arm failed the frozen
promotion rule, so Parquet remains the production normal-segment default.
The later cell-WAL qualification also rejected promotion, and the unreleased
backend and current-tree harnesses were removed. No partial campaign or local
diagnostic is promotion evidence.

## Final schedule-locked v13 decision

Campaign `storage-layout-schedule-locked-20260727-v13` completed all 100
scheduled cases on 27 July 2026: two public datasets, local disk and S3, five
arms, five repetitions, and 100 immutable query identities per case. The
frozen source archive SHA-256 is
`ca2fca375c3340afade6b293bf5719c62d838f82f82dc202d4e26a4f5b4fe6da`;
the frozen dataset-identity manifest SHA-256 is
`be7912fb8c69f54200b77dad1d123afd718505f25149633ba0817ceae88b0e1c`.
Canonical evidence is under
`s3://borsuk-bench-453182569524-euc1/layout-qualification/results/storage-layout-schedule-locked-20260727-v13/`.

An independent fresh sync admitted exactly 100 completion markers and 10,000
query samples. Running the frozen assembler and analyzer again produced
byte-identical outputs: qualification samples SHA-256
`e02451cf334dd8450c0b8e13f21bb57d296c1071e7960a30c2d4ffbc2b8d9960`
and decisions SHA-256
`254f56df4eae3b3f4fd70c934f12b95e3dcb86e43679d19896277083d94141d3`.

All four final `backend=all,dataset=all` rows are `no-promotion`.
`mixed-vortex-full` passed the isolated S3 Fashion-MNIST subcase, but failed
GloVe and the cross-backend confidence gate. The worst paired p95 ratios for
the four candidates ranged from 1.283 to 1.292, with family-wise confidence
upper bounds from 1.300 to 1.319. Recall loss was zero, and Vortex often
improved segment bytes, CPU, or individual subcases; none of those partial
wins overrides the preregistered latency and cross-dataset rule.

Campaign `storage-layout-20260726-v5` was stopped after four cases. Its
resident global-PQ query path read Arrow product-code and exact-vector
sidecars, not the normal segment tables changed by the experiment. Those rows
are retained as diagnostic evidence and are explicitly forbidden for
promotion or publication. The corrected campaign forces the normal-segment
path and rejects any raw sample with global scan chunks or zero searched
segments.

Campaign `storage-layout-forced-segment-20260727-v6` proved the corrected
query-path contract, then stopped in its first range-aware case. Concurrent
header and row scans could observe a range-cache file while another thread was
writing it directly to the final pathname. Authenticated chunk verification
caught the partial read. Cache publication now uses atomic rename, and a
failed authenticated cache read evicts both full-object and range entries
before one backing-store retry. The two completed v6 cases are diagnostic only;
the unchanged frozen gates will be applied to a fresh campaign.

Campaign `storage-layout-forced-segment-20260727-v7` then cleared the failed
range case, but a capacity preflight after its first five cases showed that
retaining every disposable index/cache tree would exhaust the worker volume
before the larger GloVe repetitions completed. v7 is therefore diagnostic
only. The runner now syncs all result evidence and the completion marker, then
sequentially removes only that case's cache, scratch directory, and local
index before starting the next measurement. Campaign v8 kept the same arms,
datasets, seeds, repetitions, queries, and frozen decision gates.

The first complete v8 local arm set exposed that the 2,048-row adaptive
threshold selected Vortex for every 4,096-row-capped Fashion segment, making
the nominal mixed arms physically identical to fixed Vortex. v8 is also
diagnostic only. Campaign v9 uses an inclusive 4,096-row Vortex threshold, so
full cells use Vortex and the non-full tail uses Parquet, and rejects a mixed
case unless both persisted extensions are present. No correctness, latency,
confidence, or operational decision gate changed.

After v9 repetition r01 completed, native inspection of the real normal-segment
schema found that ten segment constants were repeated in every logical row.
The fixed-list centroid and PQ-bound columns dominated the Vortex object while
Parquet compressed the repetition much more effectively. Format v12 therefore
stores the checked constants once in a nullable packed `segment_header` value
at row zero and projects only row-varying columns for candidate scans.

The same audit found two measurement/runtime defects. Concurrent Vortex reads
of the same authenticated 1 MiB integrity chunk could redundantly fetch that
chunk, so the range reader now singleflights per chunk while allowing distinct
chunks to proceed in parallel. Also, the old logical `bytes_read` accumulator
could double-count overlapping concurrent segment scopes. Qualification now
uses the isolated query-scoped `backing_bytes_read` counter for the cold
backing-byte guard; the benchmark's aggregate byte field is the sum of its
query-scoped disk-cache and backing counters.

For the request guard, `physical_requests` now means query-scoped backing read
operations on local disk and actual query-scoped network GETs on S3. The
earlier assembler used `network_gets` for both, which was always zero locally;
the correction activates the existing 1.05 local request-amplification gate
without changing its threshold and can only withhold a promotion.

The runner's `uncached` label has a precise cache boundary: it deletes the
read-through payload cache before every query and disables decoded-segment
retention, while routing summaries, the coarse quantizer, and bounded sidecar
indexes remain resident after open as steady production serving metadata. It
does not evict the host kernel page cache. Local-disk evidence is therefore
application-cache-cold and may be kernel-page-cache warm; S3 still crosses the
measured backing-object boundary. This is not a cold-process or physical
local-disk-cache claim. Startup is measured separately.

An additional pre-v10 audit found that the provisional one-row header still
serialized its value as JSON bytes. Before the v10 archive was created, format
v12 was finalized as a deterministic little-endian `BSH1` record with explicit
lengths, nanosecond timestamp preservation, canonical metric text, and an
internal BLAKE3 checksum. The earlier 20,000-row smoke therefore remains only a
disclosed reason to investigate the schema; it is not an exact performance
measurement of the final v10 candidate.

One final-candidate functional smoke then rebuilt all three fixed-format arms
from scratch on that same 20,000-row synthetic corpus. All arms had recall@10
0.960; cold p95 was 11.607 ms Parquet, 7.744 ms Vortex full, and 8.045 ms
Vortex range. Segment bytes were 858,682 Parquet and 2,034,600 for both Vortex
arms, and every sample's aggregate byte field equaled its query-scoped disk
plus backing counters. This single local 20-query run is recorded to prevent
selective disclosure and is forbidden for promotion or publication.

The analyzer also now emits a final `backend=all` row. Because one production
normal-segment default cannot silently vary by local versus S3 storage, an arm
is promotable only if every dataset gate passes independently on both required
backends. This executable scope correction can only withhold a promotion and
does not change a numeric threshold.

Paired latency evidence now also carries and joins on the benchmark's
`query_source_index` within each repetition rather than trusting only row
position. Duplicate or incomplete source identities fail the sample gate. The
current deterministic seeds already made the two equivalent, but the assembled
evidence now proves that assumption explicitly.

The four eligible candidate arms also share a family-wise confidence budget.
Their hierarchical-bootstrap upper bounds use quantile 0.9875
(`1 - 0.05 / 4`) rather than four unadjusted 0.95 quantiles. The p95 point
threshold and strict upper-bound-below-one rule are unchanged; this
multiplicity correction can only prevent a post-hoc best-arm false positive.

These corrections were preregistered as campaign
`storage-layout-normalized-header-20260727-v10` before any v10 AWS case. The
100-case v9 campaign remains the independent v11/current-schema baseline and
must finish; its samples will not be pooled with v10. A 20,000-row synthetic
local smoke is recorded in the protocol solely to disclose why v12 was worth
testing and is explicitly forbidden for promotion or publication.

## Current conclusion

The maximum-speed candidate is a mixed physical layout. Replacing every
Parquet object with Vortex would apply a general table format to tiny atomic
pointers, fixed-width ANN buffers, and sparse row takes where it is unlikely to
be the best representation.

The earlier corrected real-segment replay makes Vortex worth investigating for
selective table workloads, but it does not justify a default change:

- Vortex selective reads were much faster through its native reader;
- Parquet was 2.6–3.3 times smaller on the real normal-segment schema;
- Parquet won complete materialization;
- BORSUK now gives Vortex an authenticated object-store range reader instead
  of preloading the object;
- finalized SRHT-PQ queries normally use Arrow product-code and exact-vector
  objects instead of normal segment tables.

This is consistent with Vortex's own format documentation: its default layout
uses column partitioning, zone pruning, roughly 2 MiB uncompressed chunks, and
up to roughly 1 MiB localized compressed buffers for analytical scans. The
official compact strategy explicitly prioritizes size at the expense of read
performance, while custom layouts can target other block-storage access
patterns. Accordingly, BORSUK measures the default analytical layout through
its native ranged reader instead of assuming that either the default or
compact strategy is suitable.

That normal-segment gate is now closed: retain Parquet. The remaining
format-selection gate in the current implementation is the separate
cell-WAL v2 campaign. Other object roles retain their checked Parquet, Arrow
IPC, or packed defaults until their own real access traces justify a candidate.

## Implemented production boundary

- layout policy v3 inventories all 14 object roles; normal-segment and cell-WAL
  record writers resolve it dynamically, while the checked
  `storage-object-roles.csv` remains authoritative for routing, lexical,
  control, graph, and sidecar writers;
- normal-segment references persist role, codec, policy version, and 1 MiB
  BLAKE3 integrity chunks;
- one index can contain mixed Parquet/Vortex normal segments and WAL runs while
  exact vectors remain Arrow IPC and control objects keep their role codecs;
- Vortex issues real `Storage::read_range` calls, singleflights verified
  integrity chunks, and rejects corrupt ranges before the codec sees them;
- full-object Vortex remains only as the qualification control through
  `BuildConfig::vortex_range_reads = false`;
- replay requires at least 30 paired materialized samples, immutable source
  checksums, equal logical-value checksums, and identical cache state.

## Role triage

| Object role | Initial candidate | Vortex priority | Reason |
|---|---|---:|---|
| Catalog and `CURRENT` | purpose-built packed pointer | exclude initially | Tiny complete reads and conditional writes dominate; a table runtime adds overhead. |
| WAL run | Parquet, Vortex, or Arrow | high | Sequential immutable write plus projected tail replay may benefit, but build CPU, bytes and tail materialization must be measured together. |
| Lane head | purpose-built packed pointer | exclude initially | Tiny CAS object with a fixed schema. |
| Commit marker | purpose-built packed marker | exclude initially | Atomic create and checksum validation, not analytical access. |
| Routing page | packed or range-aware table | medium | Small projected reads may benefit, but one additional request can dominate decode savings. |
| Normal segment | Parquet or range-aware Vortex | highest | It has the strongest existing selective-read evidence and the clearest current access-path defect. |
| Product-code bundle | Arrow IPC or packed fixed-width | low | The production operation is bounded fixed-width range scanning, already separated from table metadata. |
| Exact-vector sidecar | Arrow IPC or packed fixed-width | exclude initially | Candidate row ordinals are already known; sparse fixed-width takes are the workload. |
| Filter-index sidecar | packed or Vortex | medium | Metadata predicates need compact negative lookup; trace selectivity and bytes before choosing a general table. |
| Sparse/BM25 block | Parquet, Vortex, or specialized postings | highest | Term lookup and selective postings/row projections are plausible Vortex wins and important product paths. |
| Late-interaction sidecar | Arrow IPC or packed token blocks | low | Entity and token ranges are already known before SIMD MaxSim. |
| Tombstone run | Parquet, Vortex, or sorted packed generations | high | Sorted ID/generation lookup and consolidation need direct evidence. |
| ID ownership directory | Vortex or sorted/hash packed blocks | high | New role with selective lookup, batch update and compaction; no compatibility constraint favors Parquet. |

## Required investigations

### 1. Normal segments

Compare full-object Parquet, ranged Parquet, full-object Vortex, and ranged
Vortex for:

- lean candidate scan;
- metadata filter;
- point and bounded row selection;
- exact/full decode;
- cold S3, disk-cached, and decoded-cache conditions;
- build, compaction, storage size, CPU, RSS, requests and transferred bytes.

The end-to-end arms must use fresh indexes. Read-time conversion is not valid.

### 2. WAL runs

Trace actual tail sizes and projections for dense, FP8, sparse, BM25, hybrid
and late-interaction mutations. Measure single-run append latency, 32-writer
throughput, replay latency, flush amplification and object footprint. A faster
reader that substantially slows durable acknowledgement is not a WAL win.

### 3. Lexical and sparse blocks

Capture term-page, postings and row-metadata reads separately. Compare Vortex
pushdown with Parquet row-group projection and a purpose-built sorted postings
layout. Measure complete hybrid queries rather than isolated term scans.

### 4. Tombstones and ID ownership

Compare sorted table lookup against hash-partitioned packed blocks. Include
negative lookup, hot-ID updates, consolidation, crash recovery and repair.
Vortex is eligible, but a compact indexed layout may be faster.

### 5. Routing pages

Measure total request latency before decoder CPU. Keep routing pages in a tiny
packed format if Vortex reduces decode time but adds range requests or bytes.

## Promotion rule

Vortex wins an object role only from fresh end-to-end evidence with identical
logical results. This layout-selection stage includes paired single-query
p95/p99, CPU, RSS, requests, bytes, role and total storage size,
build/compaction time, and at least two representative datasets. Concurrent
throughput is deliberately left to the subsequent full production benchmark;
the layout campaign must not be cited as throughput evidence.

The checked
`docs/research/storage-layout-qualification-protocol.json` freezes the numeric
gates. Promotion requires a p95 ratio of at most 0.95 with the paired bootstrap
upper bound below 1.0, p99 at most 1.02, and mean recall loss at most 0.005.
The bootstrap is hierarchical: it resamples independent repetitions first and
then paired query positions within each selected repetition.
Request count, bytes read, normal-segment bytes, active index bytes, and peak
RSS may regress by at most 5%; build time and measured CPU core-time may
regress by at most 10%. Missing operational evidence is a failed sample gate,
not an ignored field. The role-specific segment-byte guard prevents unchanged
sidecars from diluting a size regression in the object family being qualified.

The operational gates were added after two local Fashion-MNIST cases had
completed, before any S3, GloVe, cross-dataset, or promotable result existed.
They can only turn a promotion into a no-promotion. The amendment and the
partial observation are recorded explicitly in the protocol to prevent
undisclosed post-result threshold selection.

The segment-byte guard was added after the v12 source freeze but before any
v13 AWS case. It reuses the already-emitted `segment_bytes` measurement and can
only withhold a promotion. Its timing and unchanged 1.05 no-regression bound
are recorded separately in the protocol.

No single global storage-format default will be introduced. The frozen
layout-policy version will map each object role and size/schema class to its
qualified representation.

Before the v10 source archive, the object-role audit also found JSON and
unchecked-text control objects on the sharded WAL write/recovery path. Those
objects now use the checked packed `BWH1`, `BWN1`, `BWD1`, `BWC1`, `BID1`, and
`BCN1` codecs, with `BMM1` and `BTM1` for their nested mutation metadata,
documented in `docs/storage-format.md`. This is a shared
production-schema correction across every layout arm, not a Vortex
optimization; its timing and unchanged decision gates are recorded in the
qualification protocol.

## Primary references

- [Vortex file-format layout strategy](https://docs.vortex.dev/concepts/file-format)
- [Vortex layouts and block-storage abstraction](https://docs.vortex.dev/concepts/layouts)
- [Vortex I/O and compact/default write strategies](https://docs.vortex.dev/api/python/io)
