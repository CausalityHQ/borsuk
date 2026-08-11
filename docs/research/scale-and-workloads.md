# Scale and Workload Evaluations

These studies use controlled synthetic workloads to isolate engine mechanisms
or reach sizes that are impractical for repeated public-corpus sweeps. They are
not substitutes for the [standard-dataset results](standard-datasets.md).

## Artifact catalog

| Artifact | Scope | Principal variables and reported resources |
|---|---|---|
| [`metric-pruning.csv`](../web/assets/benchmarks/metric-pruning.csv) | 125-segment metric pruning | metric, prunable flag, recall, segments, bytes, p50 variance |
| [`filtering.csv`](../web/assets/benchmarks/filtering.csv) | selective metadata filtering | selectivity, pruned segments, rows, bytes, recall, p50/p95 |
| [`sparsity.csv`](../web/assets/benchmarks/sparsity.csv) | within-segment filter-first crossover | rejection, rows scored, bytes, recall |
| [`mixture-workload.csv`](../web/assets/benchmarks/mixture-workload.csv) | dense/sparse/text mixtures | ingest, p50/p95, bytes, write rate, recall |
| [`sparse_inverted.csv`](../web/assets/benchmarks/sparse_inverted.csv) | high-vocabulary sparse search | vocabulary scale, postings work, latency, memory |
| [`production_workload.csv`](../web/assets/benchmarks/production_workload.csv) | upserts, deletes, filters, compaction, restart | write rate, search tails, bytes/GETs, live rows |
| [`production_write_costs.csv`](../web/assets/benchmarks/production_write_costs.csv) | isolated mutations | p50/p95, operations/s, bytes read/written |
| [`sequential.csv`](../web/assets/benchmarks/sequential.csv) | seven-method controlled matrix | recall, p50/p95, bytes, routing, rows, cache |
| [`parallel.csv`](../web/assets/benchmarks/parallel.csv) | method × worker scaling | QPS, tails, recall, bytes, RSS delta |
| [`routing-overfetch.csv`](../web/assets/benchmarks/routing-overfetch.csv) | metadata lookahead 1–32 | recall, routing reads, bytes, cache states |
| [`dataset-scaling.csv`](../web/assets/benchmarks/dataset-scaling.csv) | 10k–10M scaling | ingest/compaction/query, recall, RSS, bytes |
| [`memory-scale.csv`](../web/assets/benchmarks/memory-scale.csv) | 64–1024 readers | capped/uncapped RSS, QPS, p50/p95 |
| [`large-scale.csv`](../web/assets/benchmarks/large-scale.csv) | 1M method comparison | pq/Vamana/hybrid recall, latency, bytes, RSS |
| [`hundred-million-build.csv`](../web/assets/benchmarks/hundred-million-build.csv) | historical pre-v8 100M build | elapsed time, footprint, routing, graphs, RSS |
| [`hundred-million-read.csv`](../web/assets/benchmarks/hundred-million-read.csv) | historical partial-layout 100M reads | selected cells, latency, bytes, cache, resident metadata |
| [`lifecycle.csv`](../web/assets/benchmarks/lifecycle.csv) | ingest and compaction | throughput, segments, routing I/O, graph I/O |
| [`scale.csv`](../web/assets/benchmarks/scale.csv) | early controlled size sweep | build/search time, bytes, recall |
| [`workload.csv`](../web/assets/benchmarks/workload.csv) | early mixed-workload sweep | operation mix, latency, throughput |

## Metric pruning

Cosine, angular, Euclidean, Manhattan, Chebyshev, Canberra, Bray-Curtis, and
Hamming can use safe bounds in the measured workload; inner product cannot use
the centroid/radius lower bound. The gate checks recall 1.0 while reporting how
many of 125 segments are avoided. Never compare the latency row without its
metric and prunability flag.

## Filtering and mixtures

Segment statistics prune whole objects for selective filters. When matching
rows are distributed across every segment, filter-first ranking switches to
scoring the actual matches once they fit the candidate budget. This preserves
recall while reducing scoring work; bytes remain flat if every segment object
must still be read.

The mixture workload measures dense, sparse, text, dense+text, and all-modality
queries independently. Sparse inverted search is sublinear in total vocabulary
for the checked workload and is documented separately from dense ANN results.

## Updates and lifecycle

The production workload interleaves upserts, deletes, filtered reads,
compaction, and reopen. It reports live-record correctness alongside search and
write rates. Write-cost rows are not query results: their bytes and p95 describe
mutation publication and tombstone behavior.

## Parallel memory

The memory-scale experiment shows why per-query limits are insufficient. At
100k vectors and 64 readers, the uncapped row added about 170 MB RSS and reached
208 QPS / 412 ms p95. A 16-query cap added about 2.8 MB and reached 169 QPS /
114 ms p95. The current production path further adds the global cell-decode cap
and same-cell single-flight sharing evaluated on AWS.

## One million vectors

The large-scale gate uses 1,000,000 vectors, 16 dimensions, 128-row cells, a
512-segment budget, routing overfetch 8, and 128 candidates/cell. pq-scan,
Vamana-PQ, and hybrid all reached tie-aware and id recall 1.0 in the constructed
workload, reading about 13.79 MB. These are mechanism checks, not public ANN
leaderboard claims.

Projected pq-scan reads lean code/metadata columns and range-reads only selected
lossless vector rows. The dedicated memory study measured 1.74 GB peak RSS for
full segment decode versus 694 MB for projected reads at 256 concurrent readers
on its original configuration.

## One hundred million vectors

The v8 production design treats 100M as a default constraint, not an opt-in
large-memory mode. At 96 dimensions the dimension-aware default is 43,690 rows
per physical segment, yielding 2,289 bounded ingest/object units for 100M rows.
Vectors are assigned independently through a full-dimensional 64-by-256
hierarchy with at most 16,384 global coarse cells; the persisted probe rule
selects 256 cells at the ceiling (no more than 1/64 of the routing space). This
keeps rows scored per query bounded as the corpus grows from 10M to 100M. Code
payloads are scanned in at most 32-chunk waves. Global
lossless exact pages are fixed-width and require no per-row offset table; only
late top-k physical-ID materialization can use the bounded 128 MiB sidecar-index
LRU. Benchmark build batches hold at most 32 MiB of input vectors. Build/query
compute stops at four threads, while 24 process-wide 256 KiB-stack I/O waiters
overlap object-store latency without increasing the scoring CPU budget. Four
searches and twenty-four active decodes are shared
process-wide defaults. Build partitioning uses disk scratch rather than RAM;
`BORSUK_BUILD_SCRATCH_DIR` can place it on a provisioned volume. These are implementation invariants and scale
calculations; they define the acceptance envelope for measured runs.

### Fresh 100M AWS SRHT-PQ baseline (2026-07-22)

The first complete 100M×96D angular AWS recreation is now archived as a v8
SRHT-PQ baseline, not relabelled as a v9 codec result. It used 43,690 rows per
physical segment, a 512 MiB configured resident budget, four compute workers,
24 shared I/O/decode permits, 100 full-corpus queries, and a fresh S3 prefix in
Frankfurt. Build ingest took 7,835,789 ms and finalization 6,557,166 ms (about
4.00 hours combined). The whole run lasted 4.16 hours, peaked at 559.4 MiB RSS
and 461.8% sampled CPU, used at most 42.47 GiB of build scratch, and grew the
local read-through cache to 5.74 GiB. Full-process RSS therefore exceeded the
configured resident-state budget by about 47 MiB; this baseline does not pass a
strict 512 MiB process-envelope gate.

Recall plateaued at 0.992 by 64 probes:

| probes / candidates | recall@10 | uncached p95 | disk-cached p95 | logical bytes/query | backing GETs/query |
|---:|---:|---:|---:|---:|---:|
| 64 / 200 | 0.992 | **910.5 ms** | **44.4 ms** | 76.7 MB | 70.82 |
| 128 / 200 | 0.992 | 1,235.5 ms | 79.3 ms | 150.2 MB | 136.21 |
| 256 / 200 | 0.992 | 1,704.3 ms | 146.5 ms | 292.6 MB | 267.93 |

The higher-probe rows add I/O and latency without recall and are rejected. The
64-probe row proves bounded-memory 100M operation and high empirical recall,
but its 0.91 s S3 p95 and four-hour build are a baseline to beat, not a
production-latency claim. The raw build, recall, and 8 MiB resource timeline are
under
`synthetic-clustered-100m-96-r2-default-build` (historical raw artifact not distributed),
with [recall/latency](../web/assets/charts/v8-vector-ivf/synthetic-clustered-100m-96-r2-default-build/recall-latency-synthetic-clustered-100m-96.svg)
and [CPU/RAM/disk/cache](../web/assets/charts/v8-vector-ivf/synthetic-clustered-100m-96-r2-default-build/resources-experiment.svg)
charts.

The disk-cached counter makes the first bottleneck separable without estimating
it: the code-range path reports its logical spans on a cache hit, while cached
late row ranges report zero backing bytes. Thus 76.19 MB/query belongs to the
code scan and only 0.48 MB/query is additional uncached exact-rerank and final-ID
traffic. The v8 packed reader merged every selected slice sharing an object into
one range from the minimum to maximum selected offset. That also transferred
unselected cells between distant slices. For scale, a balanced 100M layout with
64 code bytes, a four-byte row location, 16,384 cells, and 64 probes contains
about 26.56 MB of selected code rows before headers; the observed 76.19 MB span
is 2.87 times that balanced lower bound. The v9 reader now forms bounded,
gap-aware groups (32 reads and 32 MiB retained code bytes per wave; at most a
64 KiB unselected gap per physical range). Its fresh lower-probe AWS curve is a
required acceptance result before changing the production default.

### Historical pre-v8 experiment

The checked-in build completed 100,000,000 16D vectors in 5,907,443 ms
(approximately 98.5 minutes), creating 24,415 pre-compaction segments. It
reported 12.56 GB of segment tables, 6.00 GB of graph blocks, about 32 MB of
resident metadata, and roughly 550 MB peak-RSS delta during build.

The read artifact is explicitly labeled after the first bounded 2M-row L0→L1
compaction batch; it is not a fully compacted 100M production index. Query rows
report both 32- and 512-segment budgets so latency cannot be separated from
coverage.

## Reproduction

Each artifact's producing command and acceptance gate is documented in
[reproducibility](reproducibility.md). Local/shared-machine absolute latency is
used for regression and relative analysis, not compared directly to AWS.
