# BORSUK Architecture

BORSUK uses immutable external segments plus a small in-memory routing layer.
The current implementation keeps these invariants:

- one physical index has one fixed metric;
- durable tables use Arrow schemas and Parquet storage;
- local files and S3-compatible object stores share the same object layout;
- inserted vectors are written to immutable L0 Parquet segment files;
- compaction rewrites selected source-level segments into new target-level
  Parquet segments and publishes a new manifest without mutating old objects;
- garbage collection can dry-run or delete inactive segment objects that are no
  longer referenced by the active manifest;
- `CURRENT` is a tiny binary pointer to the active manifest/checksum;
- manifests and segment summaries are binary Parquet tables, not JSON;
- each segment row stores a small `routing_code` sketch alongside the exact
  vector;
- each active segment summary references a segment-local graph Parquet block
  under `graphs/L*/`;
- search loads one segment at a time and updates a top-k heap;
- exact mode can stop early when a segment lower bound cannot improve the kth
  result.

## Storage Layout

```text
index-root/ or s3://bucket/prefix/
  CURRENT
  manifests/
    manifest-00000000000000000001.parquet
  routing/
    segments-00000000000000000001.parquet
    pivots-00000000000000000001.parquet
  segments/
    L0/
      ab/
        seg-<uuid>.parquet
    L1/
    L2/
  graphs/
    L0/
      cd/
        graph-<uuid>.parquet
    L1/
    L2/
  objects/
```

The segment prefix comes from a stable hash/checksum so object-store backends
can avoid concentrating requests in one path prefix.

The current backend uses full-object `put`, `head`, and byte-range `get`
operations via the Rust `object_store` crate. Full-object reads are implemented
as `head` plus `0..size` range reads so the same primitive can later read
Parquet footers and selected row groups. An optional local read-through cache
can mirror fetched objects under a cache directory while keeping RAM usage
bounded to the active query. Concurrency limits and retry tuning are separate
storage phases.

## Search Flow

1. Load the active manifest.
2. Score segment summaries with a lower bound when the metric supports it.
3. Sort segment candidates by lower bound.
4. Fetch and decode candidate segments one at a time.
5. In approximate mode, optionally rank rows inside the segment by the
   `routing_code` sketch, use the best ranked rows as graph entry points,
   traverse segment-local graph neighbors by query distance, and exact-score at
   most `max_candidates_per_segment` records.
6. Compute exact vector distances for the selected rows.
7. Maintain only the current top-k hits in memory.

For metrics where the centroid/radius lower bound is not safe, BORSUK falls
back to a zero lower bound and performs a segment scan.

The current segment-local sketch is intentionally small: one deterministic
scalar routing code per row, stored in Parquet. BORSUK also writes a
segment-local graph block as a Parquet edge table with source id, neighbor id,
and neighbor distance. Approximate search currently uses the scalar routing
code to choose entry points and performs bounded query-guided traversal through
the segment-local graph while respecting the per-segment exact-scoring budget.
Richer vector sketches are a later phase.

## Compaction Flow

Inserts append immutable L0 segments. `BorsukIndex::compact` selects active
segments from a source level, reads their Parquet payloads, rewrites the records
into new target-level Parquet segments, and publishes a new manifest version
that references the compacted outputs.

Old segment objects are deliberately left in place during compaction. They are
no longer active once the new manifest is current, but deletion happens only via
an explicit garbage-collection call so object-store readers do not observe
in-place mutation.

## Garbage Collection Flow

`BorsukIndex::gc_obsolete_segments` lists objects under `segments/` and
`graphs/`, compares them with the active manifest's referenced segment and
graph paths, and treats unreferenced Parquet objects as candidates. Dry-run is
the default in public APIs and CLI. When deletion is explicitly requested,
BORSUK deletes only those inactive objects and reports the reclaimed bytes.

Current compaction rebuilds exact vectors, routing codes, graph blocks, and
segment summaries. GC treats inactive segment and graph objects as reclaimable
only after they are no longer referenced by the active manifest.
