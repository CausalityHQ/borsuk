# Publication position and related work

> Canonical research interpretation now lives in
> [`research/systems-comparison.md`](research/systems-comparison.md). This file
> remains as a redirect target for existing links and retains the detailed
> primary-reference notes below.

This note separates what BORSUK implements from what a paper can defensibly
claim. It is not a priority claim.

## System under evaluation

The production query path is:

1. load and validate the active immutable snapshot;
2. load a compact rotated product-PQ codebook, a bounded full-dimensional
   hierarchical coarse router, and immutable
   code/exact-page references; global exact pages are fixed-width and need no
   resident offset indexes;
3. route to global coarse cells, page their code chunks in fixed waves, and
   scan product codes in parallel with asymmetric-distance tables;
4. keep the configured whole-query candidate budget and group rows by their
   cell-aligned fixed-width lossless-vector page;
5. range-read and exact-score those float32 vectors through a handle-wide
   bounded admission gate;
6. materialize IDs/generations only for the final top-k and exact-distance ties
   from the original physical record sidecars; and
7. return top-k in deterministic distance/ID order.

Compute and network waiting have independent process-wide limits: four CPU
workers perform PQ scan, decode, and exact scoring, while 24 small-stack I/O
waiters overlap blocking object-store reads. Handle-wide admission gates remain
the memory limit across users. This split is part of the bounded multi-tenant
systems design and must be ablated; it is not an ANN-algorithm novelty claim.

The former v8 evidence must be relabeled `srht-pq-scan`; it is historical
baseline evidence, not evidence for the newly distinct `pq-scan` or
`fast-turboquant-scan` codecs. Publication text must describe that engine as
**adaptive-IVF-routed, paged SRHT-rotated product-PQ scan with
lossless reranking**. Ordinary angular corpora use the measured flat coarse
layout; large angular and Euclidean corpora use the hierarchical
full-dimensional layout. The product layout is a rejected ablation. It
uses the seeded structured Hadamard rotation idea associated with TurboQuant,
then classical learned product codebooks with one byte per subspace. It is not
the segment-local TurboQuant-4b scalar codec and must not be presented as an
unmodified implementation of the TurboQuant paper. The segment-routed fallback
still uses the separately documented corpus-fitted rotated scalar codes.

`fast-turboquant-mse-scan` is the separate MSE-only control: normalize the vector,
apply a seeded normalized randomized Hadamard transform, quantize coordinates
with a fixed dimension-derived Lloyd-Max table, and store the vector norm.
`fast-turboquant-scan` is the full structured two-stage control: its scalar stage
uses `b-1` bits and its full-width residual stage stores one sign bit per padded
coordinate plus the residual norm. The structured projection is not described
as an i.i.d. Gaussian matrix. No historical SRHT-PQ measurement may be
relabeled as a TurboQuant result.

New angular/cosine indexes normalize both build and query routing geometry.
Exact vectors remain unchanged in the lossless sidecar. The unreleased v8
format uses the corrected geometry directly; benchmark indexes are recreated
from source data and no migration path is claimed.

## What is established prior work

The following are prior art and are not individually novel:

- IVF/cell routing and compressed candidate ranking;
- HNSW or another graph over centroids;
- random rotation followed by scalar quantization, including
  [TurboQuant](https://arxiv.org/abs/2504.19874) and related quantized ANN work;
- structured randomized Hadamard/FWHT rotation for multiplier-free TurboQuant,
  now explicit in [Fast-TurboQuant](https://arxiv.org/abs/2606.21448), and a
  TurboQuant-backed retrieval index, now evaluated by
  [TurboVec](https://arxiv.org/abs/2607.16973);
- exact reranking after approximate candidate generation;
- SSD-resident ANN, including
  [DiskANN](https://www.microsoft.com/en-us/research/publication/diskann-fast-accurate-billion-point-nearest-neighbor-search-on-a-single-node/);
- partition-based dynamic ANN, including
  [SPFresh](https://arxiv.org/abs/2410.14452);
- out-of-place LSM plus disk-based ANN composition, including
  [LSM-VEC](https://arxiv.org/abs/2505.17152);
- immutable object-storage files with memory/NVMe caches. Turbopuffer documents
  an object-storage source of truth, a centroid-based SPFresh index, and
  memory/NVMe cache tiers in its
  [architecture](https://turbopuffer.com/docs/architecture). Pinecone serverless
  documents immutable object-storage “slabs” and query executors that populate
  SSD/memory caches in its
  [architecture](https://docs.pinecone.io/guides/get-started/database-architecture);
- managed object-storage vector search itself, provided by
  [Amazon S3 Vectors](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors.html);
- distributed-storage ANN, including
  [DSANN](https://arxiv.org/abs/2510.17326), and immutable ANN indexes attached
  to object-store table snapshots, including
  [Puffin-backed vector indexes](https://arxiv.org/abs/2606.04196);
- immutable-version/LSM-style publication and compaction.

## Defensible contribution

The strongest paper is a systems and measurement paper, not a new-ANN-algorithm
paper. The contribution to test is the composition of:

- content-addressed immutable object-store segments;
- range-addressed immutable bundles whose contiguous vector-less code region is
  paired with a separate fixed-width lossless-vector region and late top-k ID
  materialization;
- persisted serving metadata that makes query data I/O the only measured
  uncached network path;
- vector-level global coarse cells externally partitioned without a
  corpus-sized RAM build or rewriting the lossless physical segments;
- an adaptive persisted coarse topology: measured flat routing for ordinary
  angular corpora and full-dimensional hierarchical leaves for large angular
  and Euclidean corpora, with resident centroids byte-capped independently of corpus size;
  these are engineering choices and ablation targets, not new clustering
  algorithms;
- graph-free rotated low-bit shortlist scoring with exact metric rerank;
- deterministic codebook/row locality ordering that changes physical range
  coalescing but not the learned centroids, ADC scores, or result semantics;
- a split concurrency envelope: bounded cell-read width inside a query, a
  bounded global search-admission cap, a global active-read/decode cap,
  separate process-wide CPU/I/O worker budgets, and non-retaining same-cell
  single-flight sharing across users; and
- a reproducible cache-state methodology that reports recall, p50/p95/p99,
  object requests/bytes, CPU, RSS, disk I/O, cache footprint, and overload
  throughput together.

A safe claim is: “We design and evaluate an object-store-native vector-search
layout that combines bounded quantized cell scans with random-access lossless
reranking.” Avoid “first”, “novel IVF”, “novel TurboQuant”, “novel HNSW”, or
“novel exact rerank” unless a formal literature review establishes a narrower
priority claim.

The July 2026 literature check makes the boundary stricter: neither the SRHT/
FWHT rotation nor “TurboQuant used inside a vector index” is defensibly novel.
DSANN and the June 2026 Puffin/Iceberg work also make broad claims such as
“object-store ANN” or “compute-disaggregated vector index” indefensible.
The paper must test the systems contribution instead: an embedded library with
immutable object-store publication, vector-less paged code cells, fixed-width
cell-aligned lossless reranking with late ID reads, bounded external index construction, and a
process-wide multi-user resource envelope. Even that should be phrased as a
design/evaluation contribution until a broader scholarly and patent search is
complete.

## SIMD and compute-path boundary

The current SIMD claim is deliberately narrow. Euclidean, cosine, angular, and
inner-product hot loops share the `wide::f32x8` reduction in `metric.rs`, with a
scalar tail for dimensions not divisible by eight. The `wide` crate selects the
available compile-target implementation (including Arm NEON and x86 vector
instructions where supported); BORSUK does not currently contain custom CPUID
or `is_*_feature_detected!` runtime dispatch. Unsupported targets retain the
portable scalar fallback supplied by that abstraction. Tests compare SIMD and
scalar squared-distance results across tail widths.

The TurboQuant-4b SRHT transform itself is an in-place scalar Walsh–Hadamard
butterfly over a power-of-two padded buffer. Its shortlist scoring eventually
uses exact rerank through the shared metric kernel, but the project must not
claim that the SRHT or 4-bit unpacking is already hand-vectorized. A publication
should report compiler target flags and CPU model, and describe runtime dispatch
as future optimization rather than measured novelty.

## Nearest production systems

| System | Durable/search layout | Cache/compute model | Evidence used here |
|---|---|---|---|
| BORSUK | Adaptive flat or full-dimensional hierarchical IVF over range-addressed immutable product-PQ/lossless-vector bundles in user-controlled object storage; product routing is a rejected ablation | Embedded compute; coarse cells group matching chunks across ingest checkpoints, 32 MiB/query code waves, 128 MiB sidecar-index LRU, optional local disk cache, and global query/I/O admission | Fresh descriptor-v8 source recreations, recall/latency curves, and three selected-point clean-process repetitions on all six public corpora; exact 1.0 remains a separately labelled exhaustive guarantee |
| Amazon S3 Vectors | Managed vector buckets/indexes; internal ANN layout is not public | Fully managed and opaque to the client | Direct Fashion-MNIST run on the same client/region/query set; AWS documents sub-second infrequent and as-low-as-100-ms frequent queries plus 90%+ average recall on most datasets ([query docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-query.html)) |
| turbopuffer | Object-storage source of truth; centroid-based SPFresh vector index | Stateless query nodes with NVMe/memory tiers; consistent reads check object storage | Vendor-reported only: the architecture reports 874-ms cold p50 and 14-ms cached p50 on an otherwise unspecified 1M-document workload; the introduction separately reports 1,214-ms cold p90 and greater-than-90% vector-search recall ([architecture](https://turbopuffer.com/docs/architecture), [introduction](https://turbopuffer.com/docs)). Dataset, dimensions, metric, `k`, hardware, and network scope are not reported, so these are context only. |
| Pinecone serverless | Immutable object-storage slabs plus a write memtable/log | Query executors cache slabs on local SSD/memory; dedicated read nodes can keep the set warm | Architecture comparison only; no credentials/identical-data direct run ([architecture](https://docs.pinecone.io/guides/get-started/database-architecture)) |
| DiskANN | SSD-resident Vamana graph with compressed vectors | Single-node SSD + DRAM cache | Algorithmic/disk baseline from the published paper, not an object-store service or identical run |

Do not put vendor-reported values on the same plotted series as direct results.
The direct S3 Vectors comparison is limited to Fashion-MNIST and labels its
states `first_pass` and `repeated_pass`; the managed service does not expose
enough internals to assert that they are equivalent to BORSUK `uncached` and
`disk_cached`.

The following layout numbers are historical v6 evidence, not current v8
defaults or results. On the legacy fixed-4096-row layout, the direct recall frontier is 0.981 at
`nprobe=22, candidates=11` and 0.986 at 12 candidates, around S3 Vectors'
measured 0.985. Its width-22 run is retained as the motivating failure: four
admitted queries allowed 88 concurrent cell decodes and produced 2.63 GiB peak
RSS.

The Fashion layout campaign uses 512 rows at `nprobe=6, candidates=11`:
recall 0.989, 3.52 MB and 12 backing GETs/query. Cap-12 experiments measured
87.6–132 ms uncached p95 and 182.6–242 MiB peak RSS. The final graph-free cap-24
default was then rerun three times on the v6 engine: median
uncached/disk-cached p95 was 88.7/11.9 ms, median four-worker throughput was
310.7 QPS, and worst serving RSS was 193.2 MiB. This is a 27.1× query-byte
reduction and roughly an
order-of-magnitude peak-RSS reduction from the legacy recall-matched failure.
The final engine additionally caps active cell decodes globally at 24 and
single-flights overlapping reads of the same immutable checksum.

The layout ablation must accompany this result: 1024 rows used 7.01 MB/query
and 407 MiB peak RSS; 256 rows reached 95.7 ms uncached p95 but needed 18
GETs/query and 244.8 MiB peak RSS; 128 rows needed 30 GETs/query and 233.9 MiB.
This established 512 as the balanced v6 default, not a universal optimum. The
current v8 policy instead targets roughly 16 MiB of lossless float32 vectors
per physical ingest segment, clamped to 64–131,072 rows. Vector-level coarse
cells are independent of those physical segments, and search pages only their
selected compact product-code chunks in bounded waves.

The historical 960D GIST layout experiment established why rows per physical
segment must be dimension-aware, but its 31.2 GiB build peak is not a current
claim. The descriptor-v8 external builder now peaks at 369.9 MiB process RSS
and 3.80 GiB disposable scratch on the same one-million-row shape. More
importantly, the controlled code-width curve shows a different optimization:
code128 needs `32 probes / 608 candidates` for 0.985 recall, while code256
reaches 0.995 at `24 / 96`, adds only 1.5% to the whole index, and measures a
three-run 427.3/29.2 ms uncached/disk-cached p95 median. The paper must publish
the complete curve: 0.997 at `24 / 384` costs 376.70 GETs/query, and 0.999 at
64 probes costs still more scan time. This is empirical ANN quality, not the
formal exact-mode 1.0 guarantee.

## Experimental graph optimization result

The current graph path uses a deterministic score-once best-first heap rather
than repeatedly rescanning a raw frontier. A dense state table prevents
duplicate queue entries, and decoded/validated immutable graphs share the
existing byte-accounted segment cache. On graph-enabled indexes, `warm()`
prepares that metadata before measured queries.

On the full 100-query Fashion set at `nprobe=22, candidates=512`, this changed
p95 from 1,951.5 ms to 28.0 ms at the same 0.970 recall. The first
recall-matched graph configuration (`nprobe=32, candidates=2560`) reached 0.986
recall and 56.4–57.7 ms p95 across three fresh runs, compared with pq-scan at
0.986/89.2 ms and Vamana-PQ at 0.988/96.2–96.4 ms on the same memory-preloaded
graph-enabled index. Query-time GETs were zero and worst process RSS was about
379 MiB for all three methods.

This belongs in an experimental-method ablation, not the main six-corpus
production table. It does not establish a new Vamana/HNSW algorithm, covers one
corpus, uses a far wider graph candidate budget, and includes an intentional
full-cell scan fallback at sufficiently large budgets. The former 0.990/0.995
recall claim for widths 512/1,024 was based on 20 profiling queries; full-100
results were 0.970/0.973 and are the only publication-safe values.

## Evaluation rules

- All current BORSUK rows use graph-free `pq-scan` with the persisted
  SRHT-rotated learned product-PQ configuration. The segment-local
  TurboQuant-4b codec is a fallback, not the descriptor-v8 global engine.
- Recall is computed from the dataset's shipped ground truth on the full corpus.
- A production point must meet recall@10 >= 0.95 before latency is reported as
  qualified.
- `uncached` means serving metadata is already resident, while the query data
  pages are absent from the local disk cache and therefore require object-store
  I/O.
- `disk_cached` repeats the identical query working set from local disk and must
  report zero backing-store GETs and zero backing-store bytes.
- Full decoded-vector preload is a separate `memory_preloaded` research state,
  not “cold” or “warm”.
- Startup/open plus serving-metadata preparation is measured separately and is
  excluded from query latency.
- Production uses bounded per-query width, the default four-query admission
  cap, and the default 24-cell global decode cap. Uncapped fan-out and multi-user
  runs are labelled “research ceiling”.
- Vendor-reported numbers and directly measured numbers appear in separate
  columns. Dataset, corpus size, dimensions, metric, recall definition, cache
  state, consistency mode, client location, and concurrency must accompany any
  latency comparison.

## Publication risk

Algorithmic novelty is low because the routing, quantization, and reranking
ingredients are established. Systems novelty is plausible but depends on a
careful nearest-system comparison and ablations showing that the separated
scan/rerank layout and process-wide resource caps materially improve the recall/
latency/memory/object-I/O envelope. A systems workshop or industry track is the most
credible initial target; a top systems venue would need larger-scale results,
failure/recovery and update experiments, cost normalization, and direct
baselines on identical hardware and data.
# Evidence wording gate

External numbers may come only from first-party commercial documentation or a
primary paper and must appear in
[`research/reported-comparisons.csv`](research/reported-comparisons.csv).
Reported values are context, never controlled measurements, and are not plotted
on the direct BORSUK series. Missing dataset, hardware, metric, `k`, cache, or
latency-scope fields force `context-only` wording.

The only planned superiority decision is
`lower-latency-at-matched-recall` for paired BORSUK/Amazon S3 Vectors
Fashion-MNIST requests. It requires at least five fresh repetitions, 1,000
queries per repetition, a hierarchical-bootstrap latency-ratio confidence
interval wholly below one, and a recall-difference interval whose lower bound
is non-negative. A numerically favorable point estimate that fails either
interval is reported as `no-superiority-claim`.
