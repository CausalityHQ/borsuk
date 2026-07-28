# Systems Comparison and Publication Position

This page separates direct measurements, vendor-reported context, established
prior art, and defensible BORSUK contributions.

## Direct Amazon S3 Vectors comparison

The same Frankfurt `c7g.8xlarge`, Fashion-MNIST corpus, 100 queries, and shipped
ground truth were used for BORSUK and Amazon S3 Vectors. Source:
[`aws-s3vectors-fashion-comparison.csv`](../web/assets/benchmarks/aws-s3vectors-fashion-comparison.csv)
and
[`aws-v8-vector-ivf-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-v8-vector-ivf-production-repetitions-2026-07-21.csv).

| System/profile | recall@10 | first/uncached p95 | repeated/disk p95 |
|---|---:|---:|---:|
| BORSUK descriptor-v6 hierarchy, `8/320`, first diagnostic | 0.988 | 178.2 ms | 1.40 ms, zero backing GETs |
| BORSUK historical pre-fixed-page v8, `8/320`, bounded profile (three-run median) | 0.989 | 191.1 ms | 2.45 ms, zero backing GETs |
| BORSUK historical v6 graph-free default | 0.989 | 88.7 ms | 11.9 ms, zero backing GETs |
| BORSUK historical v6 cap-12 ablation | 0.989 | 87.6 ms | 11.8 ms, zero backing GETs |
| BORSUK historical v6 recall-match, 4096 rows | 0.986 | 198–229 ms | 79–83 ms, zero backing GETs |
| Amazon S3 Vectors | 0.985 | 275 ms first pass | 105 ms repeated pass |

S3 Vectors does not expose server CPU/RAM/disk or a zero-network local-cache
state, so `first_pass` and `repeated_pass` are not renamed to BORSUK `uncached`
and `disk_cached`. The direct comparison is limited to Fashion-MNIST. The
descriptor-v6 row remains diagnostic until fresh repetitions and bounded-load
measurements complete.

## Experimental graph result

The graph path has a separate memory-preloaded Fashion result and is not placed
in the S3-network table above. A score-once best-first frontier plus a shared,
byte-accounted decoded-graph cache reduced the historical full-query p95 from
1,951.5 to 28.0 ms at the same 0.970 recall and width 512. After recall matching,
three `nprobe=32, candidates=2560` graph repetitions reached 0.986 recall and
56.4–57.7 ms p95, versus pq-scan at 0.986/89.2 ms and Vamana-PQ at
0.988/96.2–96.4 ms on the same graph-enabled index. Every selected measured
query issued zero GETs because graph decode/validation occurred during
`warm()`.

This is a strong implementation and caching ablation, not a new graph-search
algorithm claim. It covers one public corpus, uses a much wider graph candidate
budget, and explicitly labels the full-cell fallback. See
[leaf methods](methods.md) for the full 100-query curve, resource envelopes, and
the correction of an earlier 20-query profiling result.

## Nearest production systems

| System | Durable/search layout | Cache/compute model | Evidence class here |
|---|---|---|---|
| BORSUK | immutable adaptive-IVF product-code cells plus bounded Arrow IPC exact-vector batches in user object storage | embedded compute, byte-capped resident serving metadata, optional local disk, bounded decode | typed-Arrow empty-prefix recreations in progress; historical six-corpus AWS retained only as invalidated evidence |
| Amazon S3 Vectors | managed vector buckets; internal ANN layout opaque | managed service | direct Fashion client result |
| turbopuffer | object-storage source of truth and centroid-based SPFresh index | stateless query nodes with NVMe/memory tiers | vendor documentation only |
| Pinecone serverless | immutable object-storage slabs plus write memtable/log | executors cache slabs in SSD/memory | vendor documentation only |
| DiskANN | SSD-resident Vamana graph with compressed vectors | single-node SSD and DRAM cache | published algorithm/system baseline |

Vendor values are not plotted on the direct-result series. Their consistency,
hardware, dataset, cache state, and concurrency are not identical.

The machine-readable
[reported-comparison registry](reported-comparisons.csv) is the only source for
external numeric context. It accepts first-party commercial documentation and
primary papers, records every unknown comparison field, and permits
`context-only` wording. Those rows cannot authorize “faster”, “better”, or
“lower latency” claims about BORSUK. Only paired raw rows from the frozen
confirmatory protocol can do that.

## Recall guarantee and resource objective

There are two deliberately separate products, because one setting cannot
truthfully promise formal perfect recall and sublinear work on every possible
high-dimensional corpus:

| Path | Recall statement | RAM policy | CPU/I/O consequence |
|---|---|---|---|
| `pq-scan` production default | empirical recall@10, reported on every complete query set | byte-capped metadata, code waves, rerank cache, and global admission | bounded approximate work selected from the measured recall/latency curve |
| exact / `guaranteed_recall` | formal exact top-k under the configured metric | streaming and bounded; it does not retain the corpus in RAM | may score/read the full eligible corpus when safe lower bounds cannot prune it |

The engineering objective is therefore **exactness when requested, and the
highest measured ANN recall inside explicit RAM/CPU/I/O limits by default**.
An empirical `1.000` on 100 benchmark queries is not renamed “guaranteed.” The
NYTimes exact control makes the unavoidable distinction visible: exact and ANN
both returned 1.000 on the ten-query subset, but exact consumed 405.6 MB/query
and 5.19 s disk-cached p95. Claims of simultaneously perfect recall, negligible
CPU, and negligible I/O would require a proof-preserving pruning index and
dataset-dependent evidence; they are not inferred from quantization accuracy.

A promising follow-up is to persist conservative bounds for the vector-level
global cells and visit those cells in lower-bound order before exact
verification. That could reduce average exact work while preserving the formal
guarantee, but it is not implemented or claimed by the descriptor-v8 results.

Cost follows the same comparison boundary. Every option needs an application
or client process. BORSUK performs search inside that process; managed products
perform it behind a remote API. The comparison therefore does not add the
benchmark EC2 host to BORSUK while treating managed-product clients as free.
See the dated [cost and deployment model](cost-and-deployment.md) for measured
index footprints, GET-derived costs, current list prices, and formulas.

Official context current at the time of this research:

- [Amazon S3 Vectors query documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-query.html)
  describes warm responses as low as 100 ms and 90%+ average recall for most
  datasets, while explicitly recommending workload-specific evaluation.
- [turbopuffer architecture](https://turbopuffer.com/docs/architecture) describes
  object storage as durable state and NVMe/memory cache tiers. On its
  unspecified 1M-document workload it reports 874 ms cold p50 and 14 ms cached
  p50. Its current [introduction](https://turbopuffer.com/docs) separately
  reports 1,214 ms cold p90 and greater-than-90% vector-search recall. Dataset,
  dimensions, metric, `k`, hardware, and client-network scope remain
  unspecified, so these values are context only.
- [Pinecone architecture](https://docs.pinecone.io/guides/get-started/database-architecture)
  describes immutable object-storage slabs and query executors that populate
  local SSD/memory caches.

## Established prior work

The following components are prior art and are not individually novel:

- IVF/cell routing and graphs over centroids;
- HNSW and Vamana graph search;
- random rotation followed by scalar quantization, including TurboQuant;
- structured Hadamard/FWHT rotation for TurboQuant-style quantization, including
  [Fast-TurboQuant](https://arxiv.org/abs/2606.21448), and using TurboQuant in a
  retrieval index, including [TurboVec](https://arxiv.org/abs/2607.16973);
- exact reranking after approximate candidate generation;
- SSD-resident ANN such as DiskANN;
- hierarchical inverted-file disk ANN such as SPANN;
- partition-based dynamic ANN such as SPFresh;
- out-of-place LSM/disk ANN composition such as
  [LSM-VEC](https://arxiv.org/abs/2505.17152);
- distributed-storage ANN such as [DSANN](https://arxiv.org/abs/2510.17326);
- compute-disaggregated immutable vector indexes attached to object-store table
  snapshots, such as [Puffin-backed vector indexes](https://arxiv.org/abs/2606.04196);
- immutable object-store files with memory/NVMe caches; and
- LSM-style immutable publication, compaction, and garbage collection.

Primary research references include
[TurboQuant](https://arxiv.org/abs/2504.19874),
[Fast-TurboQuant](https://arxiv.org/abs/2606.21448),
[TurboVec](https://arxiv.org/abs/2607.16973),
[LSM-VEC](https://arxiv.org/abs/2505.17152),
[DiskANN](https://www.microsoft.com/en-us/research/?p=634449),
[SPANN](https://arxiv.org/abs/2111.08566), and
[SPFresh](https://www.microsoft.com/en-us/research/publication/spfresh-incremental-in-place-update-for-billion-scale-vector-search/),
[DSANN](https://arxiv.org/abs/2510.17326), and
[Puffin-backed vector indexes](https://arxiv.org/abs/2606.04196).

## Defensible contribution

The strongest contribution is systems composition and measurement:

- content-addressed immutable object-store cells;
- a vector-less quantized scan table separated from a lossless,
  footer-addressable bounded Arrow IPC exact-rerank batches;
- persisted serving metadata that removes first-query library initialization
  from the uncached data-path measurement;
- byte-accounted sharing of immutable decoded/validated graph blocks for the
  experimental graph-enabled path;
- graph-free, coarse-routed paged product-code shortlist scoring with exact
  metric rerank (the rotation and PQ components themselves are prior art);
- a bounded external-construction path whose full-dimensional hierarchical leaf
  table has a corpus-independent byte ceiling (hierarchical k-means itself is
  prior art);
- a three-part concurrency envelope: per-query width, global query admission,
  and global active decode, plus non-retaining same-cell single-flight; and
- a reproducible cache-state protocol reporting recall, latency, requests,
  bytes, CPU, RSS, disk I/O, cache footprint, and overload together.

A safe paper claim is: “We design and evaluate an object-store-native vector
search layout that combines bounded quantized cell scans with random-access
lossless reranking.” Avoid “first”, “novel IVF”, “novel TurboQuant”, “novel
HNSW”, or “novel exact rerank” without a formal priority review.

## Publication risk and missing evidence

Algorithmic novelty is low; systems novelty is plausible but unproven. The July
2026 Fast-TurboQuant and TurboVec publications specifically rule out novelty
claims based on FWHT rotation or merely embedding TurboQuant in a vector index.
DSANN and the June 2026 Puffin/Iceberg work also rule out a broad “first
object-store/disaggregated ANN” claim. A systems workshop or
industry track is the credible first target. A stronger venue needs:

- the full standard-dataset × method matrix now enumerated by the runner;
- direct identical-hardware baselines such as DiskANN where licensing and build
  practicality permit;
- larger dimension-aware layout results beyond Fashion-MNIST;
- repeated confidence intervals rather than two-run ranges;
- update/failure/recovery experiments under sustained load; and
- broader workload-normalized dollar analysis beyond the dated storage/request
  model already published.

The compatibility path [`docs/publication-notes.md`](../publication-notes.md)
retains the longer claim audit until a formal bibliography is introduced.
