# Production Benchmark Contract

This page defines how to qualify a BORSUK production profile. Deep method
evaluations, standard-dataset tables, recall curves, configuration ablations,
resource graphs, scale studies, and external comparisons live in the dedicated
[research section](research/README.md).

## Required production profile

Production uses:

- graph-free `srht-pq-scan + scan`: resident global/coarse codebooks and chunk metadata,
  paged product codes from selected global coarse cells, parallel
  asymmetric-distance scan, fixed-width cell-aligned exact vectors, and late
  top-k ID materialization;
- dimension-aware 32 MiB bulk-ingest checkpoints, with row capacity decreasing
  as dimensions increase;
- locality sorting inside each bounded checkpoint and ingest-preserving
  `finish_bulk_load()` by default;
- a handle-wide bounded rerank-read gate;
- at most four admitted searches by default;
- at most 16 requested object reads per query under the shared 24-read gate;
- four process-wide CPU workers and 24 process-wide small-stack I/O waiters;
- optional byte-bounded local disk caches; and
- full-corpus reclustering only as an explicitly measured layout override.

Uncapped query or decode concurrency is research-only.

## Cache-state terminology

- `startup`: open, validate, and prepare serving metadata; excluded from query
  latency.
- `uncached`: global/coarse PQ serving metadata is prepared. Selected product-code
  chunks and lossless vector ranges are absent from local disk and require
  backing-store I/O. Global exact pages are fixed-width and need no index;
  physical record-sidecar indexes used for late top-k IDs share the hard
  128 MiB cache.
- `disk_cached`: identical query data is served through local disk and reports
  zero backing-store GETs. Logical bytes served by the disk layer remain
  reported; they must not be mistaken for network transfer.
- `memory_preloaded`: `WarmReport.coverage_complete=true` proves every active
  decoded segment, and every required graph, remains in the byte-bounded RAM
  cache. It is reported separately; a partial warm is instead a mixed-cache
  profile. The graph-free production default has no graph allocation.

Do not use “cold” or “warm” without one of these precise definitions.

## Qualification gate

A shippable point must use a full standard corpus and shipped ground truth,
reach strict recall@10 ≥ 0.95, and report:

- dataset, records, dimensions, metric, and query count;
- global-PQ subspaces and candidates, segment rows, rerank-read cap, and any
  reclustering choice;
- query admission cap;
- startup and p50/p95/p99 for every cache state;
- QPS and p95 under the declared concurrency;
- backing GETs, bytes/query, and request-cost assumptions;
- peak CPU, RSS/VMS, disk reads/writes, and cache footprint; and
- repetitions or an explicit single-run limitation.

Latency comparisons at unmatched recall are rejected.

Current million-vector evidence is pending a fresh current-format run. Earlier
large-scale artifacts predate the typed Arrow sidecar/global-artifact boundary
and cannot qualify the current production profile.

## Run

```bash
BORSUK_BENCH_DATASET=/tmp/borsuk-datasets/sift-128 \
BORSUK_BENCH_URI=s3://bucket/sift-128 \
BORSUK_BENCH_OUTPUT_DIR=/tmp/sift-results \
BORSUK_BENCH_GLOBAL_SCAN_CODEC=srht-pq-scan \
BORSUK_BENCH_RECALL_LEAF_MODE=srht-pq-scan \
BORSUK_BENCH_NPROBES=1,2,4,8,16,32,64 \
BORSUK_BENCH_CANDIDATES=16,32,64,128 \
BORSUK_BENCH_MAX_ACTIVE_SEARCHES=4 \
BORSUK_BENCH_MAX_WAITING_SEARCHES=16 \
BORSUK_BENCH_LEAF_READ_WIDTH=32 \
BORSUK_BENCH_MAX_INFLIGHT_LEAF_READS=48 \
BORSUK_CPU_THREADS=3 \
BORSUK_IO_THREADS=88 \
BORSUK_BACKING_GET_CONCURRENCY=64 \
  cargo run --locked --release -p borsuk --example production_bench
```

Fresh benchmark builds default to the locality-sorted, ingest-preserving layout.
Set `BORSUK_BENCH_RECLUSTER_BUILD=1` only for the explicit full-reclustering
ablation; it is not the production default.

Wrap the command with `scripts/benchmark_with_resources.py` for CPU/RAM/disk
telemetry. Full commands, the six-corpus runner, the all-method matrix runner,
chart generation, raw artifacts, and validation gates are in
[research reproducibility](research/reproducibility.md).

## Research navigation

- [Evidence and coverage](research/README.md)
- [Six standard datasets](research/standard-datasets.md)
- [All leaf methods](research/methods.md)
- [Configuration and concurrency](research/configuration-ablation.md)
- [Scale and workloads](research/scale-and-workloads.md)
- [Systems comparison and publication position](research/systems-comparison.md)
