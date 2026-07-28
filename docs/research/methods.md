# Leaf-Method Evaluation

This page evaluates every public search method without implying that all methods
have equivalent full-corpus AWS coverage.

## Methods

| Method | Candidate generation | Graph payload | Exact rerank | Intended role |
|---|---|---:|---:|---|
| exact | metric-safe routing bounds followed by exhaustive eligible scoring | no | inherently exact | ground truth / small indexes |
| flat-scan | exact distances over a capped row window | no | yes | uncompressed approximate baseline |
| SQ-scan | persisted scalar-bound ordering over a capped row window | no | yes | legacy scalar approximation baseline |
| pq-scan | classical learned product-PQ scan without rotation | no | yes | identity-rotation control |
| srht-pq-scan | seeded SRHT/FWHT rotation followed by learned product-PQ | no | yes | current production default pending qualification |
| fast-turboquant-mse-scan | structured rotation plus fixed Lloyd-Max scalar levels | no | yes | MSE-only TurboQuant ablation |
| fast-turboquant-scan | `b-1`-bit MSE stage plus full structured residual-sign correction | no | yes | full TurboQuant scan control |
| graph | segment-local graph traversal | yes | yes | graph-only approximation |
| Vamana-PQ | TurboQuant-guided graph traversal | yes | yes | compressed graph candidate generation |
| hybrid | combines scan and graph candidates | yes | yes | recall-oriented experimental composition |

Historical artifacts that used ambiguous labels remain historical and are not
relabeled. New experiments always rebuild from the source vectors and record
their exact codec discriminator. All scan codecs use the same paged global
routing and exact lossless rerank pipeline.

## Controlled comparison design

The checked-in method matrix is
[`sequential.csv`](../web/assets/benchmarks/sequential.csv). It contains seven
methods on:

- sklearn-digits: 1,797 records;
- synthetic uniform, clustered, and adversarial distributions;
- 10,000 and 100,000 records for every synthetic family.

Synthetic datasets use 64 dimensions; every row uses 256-row cells,
`max_segments=8`, `routing_page_overfetch=8`, `max_candidates_per_segment=64`,
and 100 queries. Recall is tie-aware recall@10, with strict id recall retained in
the artifact. This configuration deliberately isolates method behavior; it is
not the public-corpus production layout.

## sklearn-digits result

| Method | recall@10 | p50 | p95 | bytes/query | rows scored |
|---|---:|---:|---:|---:|---:|
| exact | 1.000 | 8.31 ms | 8.95 ms | 230.0 KB | 1,793 |
| flat-scan | 0.462 | 8.37 ms | 9.06 ms | 234.0 KB | 453 |
| SQ-scan | 0.462 | 7.16 ms | 7.59 ms | 234.0 KB | 453 |
| pq-scan | 1.000 | 9.83 ms | 10.37 ms | 234.0 KB | 453 |
| graph | 0.983 | 28.54 ms | 31.03 ms | 234.0 KB | 453 |
| Vamana-PQ | 1.000 | 26.65 ms | 31.19 ms | 234.0 KB | 453 |
| hybrid | 1.000 | 27.22 ms | 31.26 ms | 234.0 KB | 453 |

At this small scale, graph startup/traversal overhead dominates. `pq-scan`
observes 1.000 recall on this complete query set at the same candidate budget
where flat and SQ fall to 0.462. This is an empirical controlled observation,
not a formal guarantee or a universal graph-vs-scan claim.

## Recall across all seven controlled datasets

| Method | observed recall@10 range | Qualification result |
|---|---:|---|
| exact | 1.000–1.000 | exact reference |
| flat-scan | 0.462–1.000 | fails on digits and clustered 10k |
| SQ-scan | 0.462–1.000 | same capped-window recall limitation |
| pq-scan | 0.994–1.000 | passes every controlled dataset |
| graph | 0.963–1.000 | passes every controlled dataset |
| Vamana-PQ | 0.994–1.000 | passes every controlled dataset |
| hybrid | 0.994–1.000 | passes every controlled dataset |

The bytes and rows-scored columns are equal for approximate modes because the
experiment holds the routing and candidate budgets fixed; it isolates candidate
quality and method CPU rather than optimizing every method independently.

## Full-corpus Fashion graph optimization

The AWS graph campaign uses one graph-enabled legacy Fashion-MNIST index, the
same Frankfurt `c7g.8xlarge`, and all 100 shipped queries. It preserves the
historical failure instead of overwriting it. At `nprobe=22` and 512 candidates
per cell, graph recall was 0.970 and p95 was **1,951.5 ms**. Profiling found two
independent repeated-work defects:

1. a raw frontier vector was linearly rescanned at every expansion, recomputing
   exact distance for already-discovered rows; and
2. every query decoded the immutable graph Parquet again and revalidated every
   edge, including recomputing each stored full-vector edge distance.

The first fix uses a deterministic best-first binary heap plus a dense
unseen/queued/selected state table. Every discovered row is scored at most once,
while distance then record-id ordering remains unchanged. The second stores the
decoded, validated graph beside its decoded segment in the existing shared,
byte-accounted cache. `warm()` now prepares graphs for graph-enabled indexes, so
initialization absorbs graph GET/decode/validation and the first measured graph
query has no metadata surprise. Graph-free production indexes build and retain
no graphs.

For bounded production without a decoded retention budget, checksum-keyed
single-flight now shares one immutable decoded graph among overlapping users
and releases it after the last traversal. Each query keeps only its mutable
frontier/visited/result state. A FIFO global admission gate prevents newly
arriving or rapidly repeating callers from starving older waiters when offered
concurrency exceeds the production cap. The publication matrix measures this
separately from the persistent-cache and memory-preloaded profiles.

Graph construction was tightened independently of query traversal. Locality
projection keys are computed once per row instead of inside sort comparisons;
source-row edge work is partitioned across available CPUs; stored vectors use
the already-validated SIMD distance kernel without repeating dimension/finite
checks; and cells above the exact-build threshold draw neighbors from bounded
locality/routing-order windows instead of an all-pairs candidate set. Build
CPU, peak RSS, read/write bytes, and wall time remain separate from serving
telemetry in the current promotion matrix.

### Same-query, same-index ablation

| Stage | recall@10 | p95 | p99 | Query GETs | Peak RSS |
|---|---:|---:|---:|---:|---:|
| historical implementation | 0.970 | 1,951.5 ms | 1,967.3 ms | 0 | 330.6 MiB |
| score-once best-first traversal | 0.970 | 208.8 ms | 209.6 ms | 0 | 353.6 MiB |
| shared decoded graph, lazy first use | 0.970 | 28.5 ms | 66.9 ms | 0.46 average | 338.8 MiB |
| graph-aware initialization preload | 0.970 | **28.0 ms** | **28.5 ms** | **0** | 369.0 MiB |

This is a 69.6× p95 reduction at unchanged recall from the historical row to
the initialized current engine. Raw data and profiles are in
[`graph-optimization`](../web/assets/benchmarks/raw/2026-07-21/graph-optimization/),
with the compact ablation in
[`aws-graph-optimization-fashion-2026-07-21.csv`](../web/assets/benchmarks/aws-graph-optimization-fashion-2026-07-21.csv).

### Multi-user admission and immutable graph sharing

The production handle admits four searches at once. The original counting
semaphore was not fair: a newly awakened waiter could repeatedly lose its
permit, so a 16-user graph trial produced tails far beyond four orderly service
waves. The current ticketed FIFO gate prevents reacquisition ahead of existing
waiters. Checksum-keyed single-flight also lets overlapping readers traverse one
immutable decoded graph allocation without turning it into a retained cache;
frontier, visited, distance, and result state remain query-local.

Three Fashion repetitions on the same selected graph point (`16 / 256`, recall
0.980) isolate the effect:

| Engine | 16-user p95 range | 16-user max range | worst peak RSS |
|---|---:|---:|---:|
| unfair admission baseline | 2,836.8–2,916.1 ms | 4,358.0–4,935.9 ms | 386.7 MiB |
| FIFO + immutable graph single-flight | **1,196.5–1,227.1 ms** | **1,228.9–1,257.1 ms** | 425.0 MiB |

The corrected maximum is approximately four times the 299–318 ms disk-cached
single-user p95, as expected from four FIFO waves. Single-query medians did not
improve: uncached p95 remained about 1.46 s and disk-cached p95 about 314 ms.
Peak RSS also did not fall on this diverse-query workload, where simultaneous
queries rarely request the same graph at the same instant and full cell/vector
decode buffers dominate adjacency memory. Single-flight is therefore a hot-cell
burst safeguard, not evidence for a lower universal RSS claim.

### Recall-matched current-engine methods

The graph width was then swept on all 100 queries. The first point meeting the
0.985 direct-comparison target is `nprobe=32, candidates=2560`. Three fresh
process/cache repetitions give:

| Method | Configuration | recall@10 | p95 range | Mean p95 | Worst RSS | Query GETs |
|---|---|---:|---:|---:|---:|---:|
| graph | 32 cells, 2,560 candidates/cell | 0.986 | 56.4–57.7 ms | **56.9 ms** | 378.5 MiB | 0 |
| pq-scan | 22 cells, 12 candidates/cell | 0.986 | 89.2–89.2 ms | 89.2 ms | 379.0 MiB | 0 |
| Vamana-PQ | 22 cells, 32 candidates/cell | 0.988 | 96.2–96.4 ms | 96.3 ms | 379.0 MiB | 0 |

These are recall-matched, not work-matched: graph needs a much wider candidate
budget and 32 routed cells, while pq-scan uses a tiny TurboQuant shortlist.
The shared RSS/cache figures are whole-process envelopes from the same
graph-enabled, memory-preloaded index; they must not replace the six-corpus
graph-free production RSS table. Hybrid at the same 22/32 point measured 0.988
recall and 96.4 ms p95, matching Vamana-PQ because this compacted index dispatches
to that stored leaf mode.

The complete curve is
[`aws-graph-recall-latency-2026-07-21.csv`](../web/assets/benchmarks/aws-graph-recall-latency-2026-07-21.csv),
the repetitions are
[`aws-graph-selected-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-graph-selected-repetitions-2026-07-21.csv),
and every repetition has CPU, RSS, process-disk, and cache telemetry. At budgets
that cover a whole cell, BORSUK intentionally skips graph expansion and scans
that cell; this explains the non-monotonic 4,096/8,192 rows and is explicitly
labelled in the artifact.

### Shared adjacency regression removal (local qualification only)

The AWS graph rows above still included one avoidable query-time cost: each
query rebuilt `Vec<Vec<usize>>` adjacency from the complete immutable edge
table. A 43k-row segment therefore scanned and allocated around 700k edges
before traversing a handful of them. The current decoder prepares one compact
CSR offset array beside the shared edge vector. Concurrent searches share that
immutable structure; only frontier, visited state, and results remain
query-local.

On a deterministic local 100k-vector, 96-dimensional clustered qualification
set, the same memory-preloaded graph point (`nprobe=2`, 16 candidates, 100
queries) retained 0.992 recall while moving from 7.455 ms p50 / 12.271 ms p95
to **0.590 ms p50 / 0.950 ms p95**. At 16 callers with a global four-search
cap, the current local run reached 6,748.8 QPS, 2.300 ms p50, 2.642 ms p95,
and 2.962 ms maximum; average graph expansion added 9.36 candidates/query and
query-time graph reads were zero.

These numbers are an engineering smoke test, not publication evidence: the
run is local, synthetic, single-process, and does not include the mandatory
AWS CPU/RSS/disk envelope. The six-corpus AWS matrix must reproduce the result
before the old AWS graph rows can be superseded. It now includes separate
storage-scan, direct disk-cached graph, memory-preloaded graph, incomplete-cache
fallback, and `auto` tier-selection controls. A row called `memory_preloaded`
is emitted only when decoded graph execution is requested and `warm()` proves
complete segment-and-graph coverage; partial preload and global scan-code I/O
are not relabelled as resident memory.

The corrected direct graph control also shows why NVMe presence alone is not a
hot graph tier. On a five-query diagnostic at `nprobe=2`, 16 candidates, direct
graph measured 1,586 ms p50 / 2,425 ms p95 from the source path and 1,663 ms
p50 / 2,542 ms p95 after the same bytes were disk-cached. It read about 31.8 MB
and 1.55 MB of graph data/query. Removing network requests did not remove
Parquet/vector decode, checksum validation, or adjacency preparation.

A 256 MiB shared decoded cache on the three-cell version of the same local
index produced the opposite result. Across 100 queries at 0.992 recall, the
fill pass measured 0.547 ms p50 / 1.007 ms p95 but exposed cold misses at
1,331 ms p99 and 1,644 ms maximum. The immediately repeated steady pass was
0.600/0.977/1.244/1.266 ms at p50/p95/p99/max, with zero query-time reads and a
276 MiB process-RSS peak. The cold tail must not be hidden by the fast median.

### Mixed hot/cold cache coverage

The cache matrix therefore emits a per-query `bench_cache_coverage.csv` for
bounded decoded-graph profiles. It records the requested hot-query mix
separately from observed decoded-memory, disk-cache, and backing-storage access
fractions. A query ID outside the seeded hot set is not assumed cold: it may
route to the same immutable cells, while a nominally hot query may miss after
LRU eviction.

The first local coverage qualification used 25 physical cells, a 64 MiB
decoded cache, 40 queries, and five 20-query mixes. It deliberately does not
qualify a default, but confirms that the instrumentation detects partial
coverage:

| requested hot-query mix | observed decoded | observed disk | observed backing | p50 | p95 |
|---:|---:|---:|---:|---:|---:|
| 100% | 55.0% | 45.0% | 0.0% | 88.3 ms | 235.5 ms |
| 75% | 42.5% | 50.0% | 7.5% | 103.9 ms | 235.4 ms |
| 50% | 32.5% | 55.0% | 12.5% | 116.7 ms | 224.4 ms |
| 25% | 35.0% | 52.5% | 12.5% | 119.2 ms | 209.3 ms |
| 0% | 37.5% | 50.0% | 12.5% | 108.4 ms | 226.9 ms |

The non-monotonic observed coverage is real overlap/eviction behavior, not a
charting error. A fixed-index follow-up on the same 100k-vector, 96D synthetic
corpus and 25-cell layout isolated the decoded-cache budget:

| budget | steady p95 | 16-caller p95 | 100% requested-hot observed decoded | 100% requested-hot p95 | peak RSS |
|---:|---:|---:|---:|---:|---:|
| 128 MiB | 117.3 ms | 400.8 ms | 85.0% | 120.6 ms | 222.1 MiB |
| 256 MiB | **0.285 ms** | **1.831 ms** | **100.0%** | **0.236 ms** | 227.3 MiB |
| 512 MiB | 0.403 ms | 7.064 ms | 100.0% | 0.246 ms | 225.4 MiB |

The 256 MiB budget is the smallest tested value that covers this snapshot;
512 MiB reserves more capacity but does not improve the measured resident set.
This is local synthetic evidence, not a cross-corpus default decision. A query
outside the seeded hot set still causes a 112–129 ms p95 miss even with the
larger cap, which is why the publication chart keeps hot and outside-hot-set
latencies separate.

The matching `auto` control proves the runtime decision rather than inferring
it from configuration. With a 64 MiB preload, only 12/26 segments and graphs
remained resident (39.8 MB decoded, `coverage_complete=false`); open succeeded
and `auto` used `srht-pq-scan`, measuring 4.05 ms disk-cached p95 over the
20-query control. At 256 MiB all 26/26 segments and graphs remained resident
(93.1 MB decoded, `coverage_complete=true`), so the same `auto` setting selected
the graph and measured 0.380 ms memory-preloaded p95. Preload itself remained a
separate 2.7–2.8 s startup cost. These subset recalls are not used as the
full-query-set publication recall.
The compact local artifact is archived as
[`local-cache-budget-control-2026-07-22.csv`](../web/assets/benchmarks/local-cache-budget-control-2026-07-22.csv);
the complete per-query CSVs and resource timelines remain mandatory for AWS
publication runs.

Publication runs sweep 64/128/256/512 MiB decoded budgets on
all six corpora and retain each query's actual tier fractions, recall, latency,
query class (`hot` or `outside_hot_set`), execution engine, bytes, GETs, CPU,
RSS, process disk I/O, and cache footprint. Render the
stacked observed-residency bars plus all/hot/outside p95 lines with
`scripts/render_cache_coverage_charts.py`; requested hot mix and observed cache
coverage remain different axes.

The older claim that graph recall reached 0.990/0.995 at widths 512/1,024 came
from a 20-query profiling subset. On the full 100-query set those old widths
were 0.970/0.973. Only the 100-query numbers above are publication results.

The older seven-method diagnostic remains available in
[`aws-methods-fashion-2026-07-20.csv`](../web/assets/benchmarks/aws-methods-fashion-2026-07-20.csv)
and its raw sweep under
[`final-engine/standard-method-sweeps-v1`](../web/assets/benchmarks/raw/2026-07-20/final-engine/standard-method-sweeps-v1/).
It is a historical baseline, not current graph performance.

## Parallel and routing configurations

[`parallel.csv`](../web/assets/benchmarks/parallel.csv) repeats all seven modes
at worker counts 1, 2, 4, and 8 across all seven controlled datasets, reporting
QPS, p50/p95, recall, bytes, routing reads, cache counters, and RSS delta.

[`routing-overfetch.csv`](../web/assets/benchmarks/routing-overfetch.csv) sweeps
overfetch 1, 2, 4, 8, 16, and 32 for pq-scan, Vamana-PQ, and hybrid. Payload
budgets stay fixed, so the experiment isolates the cost and recall effect of
decoding more routing metadata rather than silently reading more cells.

[`large-scale.csv`](../web/assets/benchmarks/large-scale.csv) compares pq-scan,
Vamana-PQ, and hybrid at 1M vectors, 16 dimensions, 128-row cells, 512 selected
segments, and 128 candidates/cell. All three reached recall 1.0 in that
constructed workload; their single-query times were 208, 199, and 195 ms.

## Missing six-corpus method matrix

Only pq-scan has full AWS results on Fashion-MNIST, GloVe, SIFT, NYTimes, GIST,
and Deep-Image. Fashion now also has the diagnostic rows above, but the other
five corpora remain **not measured** for graph/SQ/flat modes, not negative
results.
Graph-backed modes require graph-enabled indexes and cannot be evaluated using a
pq-scan-only production index.

Run a dry enumeration with:

```bash
DATASETS=/tmp/borsuk-datasets \
OUT=/tmp/borsuk-method-matrix \
BORSUK_S3_BUCKET=s3://bucket/research \
  scripts/bench_standard_method_matrix.sh
```

The checked-in
[`coverage.csv`](../web/assets/benchmarks/standard-method-matrix/coverage.csv)
contains 42 planned cells. Paid execution requires
both `BORSUK_MATRIX_EXECUTE=1` and `BORSUK_RUN_STANDARD_MATRIX=1`; see
[reproducibility](reproducibility.md).
