# Planet-Scale S3 Review

Date: 2026-07-05

Scope:

- 100M-vector local attempt shape for S3-like object count and publish cadence.
- Read path: paged routing, segment payload budget, graph dispatch, cache behavior.
- Write path: generated-id ingest, explicit-id ingest, publish/manifest churn.
- Storage model: object layout, optimistic publish, GC, transient object-store behavior.

## Current Assessment

BORSUK has the right core storage direction for S3: immutable Parquet segment
objects, immutable graph objects, fixed binary `CURRENT`, versioned manifests,
paged routing, local read-through cache, and out-of-place compaction. The
million-vector read regression was real and is fixed by skipping graph reads
when the candidate budget already covers the whole segment.

The method is not yet fully proven for "planet scale" until a production-shaped
S3 soak is run. Local 100M evidence is useful, and the completed 100M local run
shows the storage shape works at that scale, but it does not prove S3 request
rate, backend retry behavior, LIST consistency under GC, or multi-writer publish
contention.

## Findings

1. **128-vector large-scale attempts are not representative of S3 production.**
   They create too many tiny segment/graph objects. Use 4096-vector or larger
   ingest/compaction leaves for S3-scale evidence.

2. **Small add batches create excessive publish churn.**
   An 8192-record batch would publish more than 12k versions for 100M records.
   The attempt harness now defaults to 1,048,576-record add batches, reducing
   100M publish cycles to roughly 96.

3. **Generated-id ingest is the fast write path.**
   `add_vectors` reserves monotonic ids without scanning existing segments.
   This is the path used by the 100M attempt.

4. **Explicit-id ingest can become read-heavy.**
   `add` and `add_vectors_with_ids` validate duplicates against existing
   segment/page blooms. That is correct for safety, but platform-scale users
   with external ids need either generated internal ids plus an external id map,
   or a first-class trusted-unique ingest mode with clear risk semantics.

5. **Paged routing is necessary for large readers.**
   `OpenOptions { resident_routing: false, .. }` keeps segment summaries out of
   resident RAM and resolves them from routing pages. Large S3 deployments
   should use paged routing by default.

6. **Pure append-only ingest does not by itself prove unbounded scale.**
   The append path writes routing pages, but the active handle keeps resident
   segment summaries until compaction/paged publication clears them. Planet-scale
   ingest needs periodic compaction, or a direct paged-add publish mode, so a
   long-running writer does not accumulate an unbounded resident summary table.

7. **Graph reads are now budget-aware.**
   Graph Parquet is read only when `k < min(max_candidates_per_segment,
   segment_len) < segment_len`. Full-segment candidate budgets exact-score every
   row without graph I/O.

8. **Retries are delegated to `object_store` cloud backends.**
   Fault-injection tests prove typed error mapping and fail-fast behavior, not
   successful retry in the mock layer. Real S3/MinIO soak testing remains
   required.

9. **Segment and graph object writes are sequential today.**
   Ingest and compaction write each segment plus its graph block before moving to
   the next chunk, then publish `CURRENT` after all payload objects are durable.
   That is simple and correct, but high-throughput S3 ingest should add a
   bounded concurrent payload-write phase before final manifest publication.

10. **Graph traversal is not automatically faster on 16D / 4096-row leaves.**
    On the completed 100M artifact, `hybrid` with 32 segments and 512 candidates
    exact-scored fewer rows than the full-candidate probe, but it still took
    2,859 ms from cache because graph traversal dominated. `pq-scan` found the
    same inserted vector in 106 ms at 8 segments and 335 ms at 32 segments.
    High-scale defaults should prefer budgeted PQ scan or graph-skipping hybrid
    unless real workload evidence shows graph traversal wins.

## Required Evidence Before Claiming Planet Scale

- 100M local production-shaped attempt: 16D, 4096-vector segments,
  1,048,576-record add batches, generated ids.
- 100M post-ingest stats: records, segment count, routing pages, segment bytes,
  graph bytes, manifest version, resident metadata bytes.
- A read benchmark against that 100M artifact using paged routing and bounded
  approximate search.
- A compaction/paged-publication run at 100M showing that resident metadata
  stays bounded after write-optimized L0 ingest.
- A real S3-compatible run, preferably MinIO first and then AWS S3, measuring
  request rate, write throughput, p50/p95 reads, object-cache hit ratio, and GC
  listing behavior.
- A bounded parallel payload-write design for S3 ingest/compaction, including
  concurrency limits, retry semantics, and cleanup behavior for failed publishes.
- A decision on explicit-id write semantics: safe duplicate scan only, generated
  internal ids, or trusted-unique bulk ingest.

## Evidence Collected

Completed 100M local production-shaped attempt:

- records: 100,000,000 requested / 100,000,000 completed;
- dimensions: 16;
- segment size: 4096 vectors;
- add batch size: 1,048,576 records;
- elapsed: 5,907,443 ms;
- temp bytes observed: 19,289,357,703;
- pre-compaction segments: 24,415;
- routing: 191 leaf pages / 194 total pages, max level 2;
- bytes: 12,558,258,954 segment bytes and 6,002,204,522 graph bytes at ingest
  completion;
- resident metadata: 32,179,245 bytes in the write handle, 275 bytes when
  opened later with paged routing;
- manifest version after ingest: 97;
- RSS: 7,159,808 before, 557,432,832 peak, 493,568,000 after.

Post-ingest paged stats after six bounded L0-to-L1 compaction batches:

- manifest version: 103;
- active records: 100,000,000;
- active segments: 24,415;
- routing pages: 191 leaf pages / 194 total pages;
- segment bytes: 12,587,513,374;
- graph bytes: 5,995,998,131;
- resident metadata: 275 bytes.

Bounded compaction evidence:

- six batches completed;
- each batch read 512 L0 segments and wrote 512 L1 segments;
- total records rewritten: 12,582,912;
- each batch read about 263.7 MB and wrote about 393.0 MB;
- each batch read and wrote 1 routing page index and 6 routing pages;
- old graph payload reads stayed at 0;
- the full 100M pass was stopped after six batches because the current
  single-process compactor is serial and would tie up the session for a long
  time. This is a throughput gap, not a correctness failure.

100M read probes after the first bounded compaction batch:

| Mode | max_segments | max_candidates_per_segment | Found seed id | elapsed_ms | bytes_read | graph_bytes_read | records_scored |
|---|---:|---:|---:|---:|---:|---:|---:|
| pq-scan | 512 | 4096 | true | 5446 | 265,995,130 | 0 | 2,097,152 |
| pq-scan | 32 | 4096 | true | 335 | 17,191,066 | 0 | 131,072 |
| pq-scan | 8 | 4096 | true | 106 | 4,845,036 | 0 | 32,768 |
| hybrid | 32 | 4096 | true | 277 | 17,191,066 | 0 | 131,072 |
| hybrid | 32 | 512 | true | 2859 | 17,191,066 | 7,866,523 | 16,384 |

GC dry-run after six compaction batches:

- objects scanned: 55,901;
- reclaimable bytes: 3,051,351,375;
- routing page indexes read: 1;
- routing pages read: 194;
- bytes read for dry-run metadata: 15,173,908;
- no objects deleted because the command was dry-run only.

## Remaining Work

- Run the same 100M+ ingest/compact/read flow on a real S3-compatible endpoint.
- Add bounded concurrent segment/graph payload writes before final manifest
  publication.
- Decide explicit-id semantics for platform-scale ingest.
- Improve graph-mode cost selection or graph traversal performance so
  `vamana-pq` / `hybrid` are not slower than PQ scan on low-dimensional
  production-shaped leaves.
- Complete a full 100M compaction pass when wall-clock time is acceptable, then
  rerun the read-probe CSV against the fully compacted artifact.
