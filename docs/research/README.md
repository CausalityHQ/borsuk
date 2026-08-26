# BORSUK Research

This section contains experimental evidence, non-default configurations,
historical baselines, external comparisons, and publication analysis. The
normal [API guide](../api.md) and [architecture guide](../architecture.md)
describe only the supported production path and its defaults.

## Current release qualification

Publication V3 is the only active release campaign. Its checked
[manifest](publication-v3-manifest.json) covers standard ANN datasets,
realistic 1M/10M/100M dense datasets, synthetic stress families, hybrid and
late-interaction retrieval, datatype kernels, and write/update/delete/compact
lifecycle behavior. All manifest datasets have durable staged authority.
Lifecycle qualification has completed, while the standard read campaign's
first source-bound cold cell completed and its warm cell failed closed. That
read campaign is paused until the replacement warm-cache source is frozen;
results from the superseded source remain historical and are never pooled with
the replacement campaign.

Publication V2 and all earlier campaign artifacts are immutable historical
evidence for their recorded source revisions. They are not current-release
results and are not combined with future V3 measurements.

## Production system under evaluation

The default query path keeps the compact global product-PQ descriptor/codebook
and IVF metadata resident, pages only selected cell code chunks, range-reads
fixed-width cell-aligned lossless float32 vectors, exact-reranks them, and then
materializes IDs/generations only for final top-k rows. Global exact pages need
no resident offset table; physical record-sidecar indexes used for late ID reads
share the capped 128 MiB cache. The
public mode name is `pq-scan`; it is graph-free and uses a TurboQuant-inspired
structured rotation before product quantization.
Production defaults admit four searches and share one bounded rerank-read gate
across callers. Cell routing remains the fallback for filters, exact mode,
unflushed WAL data, and indexes that have not completed full compaction.

The v8 campaign always creates a new empty prefix and ingests the source corpus
again. Pre-v8 indexes are rejected; v7 rows remain explicitly historical and
are never presented as v8 evidence.

## Evidence classes

Results in this section are separated into five classes:

1. **Direct standard-dataset AWS evidence** — six full public ANN corpora on an
   AWS client with Amazon S3 backing.
2. **Controlled method evidence** — all seven leaf methods on sklearn-digits
   and uniform, clustered, and adversarial synthetic families.
3. **Scale/workload evidence** — targeted synthetic studies up to 100 million
   vectors.
4. **Direct external evidence** — identical Fashion-MNIST queries against
   Amazon S3 Vectors.
5. **Vendor-reported context** — primary vendor documentation, never presented
   as an identical-data direct benchmark.

Numbers from different classes are not merged into one ranking.

## Cache-state contract

- `startup`: open, validate, and prepare serving metadata; excluded from query
  latency.
- `uncached`: serving metadata is resident, but query cell data is absent from
  local disk and must be fetched from object storage.
- `disk_cached`: the disk cache is reset, one handle is prepared, and the
  disk-resident product of excluded startup is cleared while RAM-resident
  serving metadata is deliberately retained. The complete 1,000-query set is
  then primed once. Recall clears decoded query state
  before each measured query; concurrency clears it before measuring each
  steady worker profile. Its 64 GiB cache authority reserves 16 GiB inside the
  cache budget and funds 1,024 queries at 48 MiB each; the 96 GiB volume leaves
  another 32 GiB outside the cache. Every measured query must report a
  local-disk read and zero backing GETs and backing bytes.
  The 64 GiB read-through cache applies only to BORSUK's remote object index;
  Amazon S3 Vectors has opaque managed caching and FAISS serves an admitted
  resident index, so their disclosed 1 GiB client cache is staging/control
  capacity rather than an equivalent index-data cache.
- `memory_preloaded`: the warm report proves complete decoded segment/vector
  coverage in the bounded RAM cache; graph-enabled indexes also require every
  immutable graph resident. Partial coverage is labeled as a mixed-cache state,
  not silently promoted to memory-preloaded.

Managed services whose internal state is opaque retain vendor-neutral labels
such as `first_pass` and `repeated_pass`.

The bounded-cohort protocol preserves the configured disk-cache limit even
when the union of all 1,000 queries exceeds it. It pays one untimed cold prime
per query; that setup cost is excluded from latency but remains visible in the
attempt's storage/cost ledger. Current read results report timed backing I/O as
`storage_gets` / `storage_bytes_read`, timed local-disk bytes as
`disk_cache_bytes_read`, timed decoded-RAM bytes as
`decoded_cache_bytes_read`, and the backing I/O excluded for open, verification,
and priming as `excluded_setup_storage_gets` /
`excluded_setup_storage_bytes_read`. Older whole-query-set `disk_cached`
artifacts are historical and are not latency-comparable to bounded-cohort
results.

## Standard datasets

| Dataset id | Corpus | Dimensions | Metric | Full-corpus AWS pq-scan |
|---|---:|---:|---|---|
| `fashion-mnist-784` | 60,000 | 784 | Euclidean | measured |
| `glove-100` | 1,183,514 | 100 | cosine | measured |
| `sift-128` | 1,000,000 | 128 | Euclidean | measured |
| `nytimes-256` | 290,000 | 256 | cosine | measured |
| `gist-960` | 1,000,000 | 960 | Euclidean | measured |
| `deep-image-96` | 9,990,000 | 96 | cosine | measured |

Recall is strict recall@10 against each dataset's shipped full-corpus ground
truth. See [standard datasets](standard-datasets.md).

## Method coverage

The coverage table prevents a controlled local result from being mistaken for
a six-corpus AWS result.

| Method | Controlled digits/synthetic matrix | Six public AWS corpora | Production default |
|---|---|---|---|
| exact | measured | exact reference rows exist inside recall sweeps; no production serving profile | no |
| flat-scan | measured | not measured | no |
| SQ-scan | measured | not measured | no |
| SRHT-rotated product-PQ `pq-scan` | measured | measured on all six | **yes** |
| graph | measured | Fashion measured; other five not measured | no |
| Vamana-PQ | measured | Fashion measured; other five not measured | no |
| hybrid | measured | Fashion measured; other five not measured | no |

The missing public-corpus cells are deliberate, visible gaps. The
[method-matrix runner](../../scripts/bench_standard_method_matrix.sh) enumerates
all 42 cells and requires explicit flags before paid AWS execution. See
[method evaluation](methods.md).

## Research map

- [Standard public datasets](standard-datasets.md): recall/latency frontiers,
  selected profiles, repetition ranges, and resource graphs.
- [Leaf methods](methods.md): semantics, controlled seven-method comparison,
  limitations, and the missing full-corpus matrix.
- [Configuration ablations](configuration-ablation.md): layout, probes,
  candidates, width, caps, cache states, and overload.
- [Scale and workloads](scale-and-workloads.md): filtering, metrics,
  mixtures, updates, parallelism, 1M, and 100M.
- [Dense, sparse, and text retrieval](hybrid-retrieval.md): shared-qrels BEIR
  and synthetic evaluations for all seven signal combinations, fusion, mixed
  cache coverage, distributions, and resources.
- [Lexical Parquet evaluation](lexical-parquet-evaluation.md): exact BM25 and
  named-sparse hierarchy, bounded range-read/resource contract, cache mixtures,
  and the fresh-publication acceptance gates.
- [SIMD query kernels](simd-kernels.md): implementation coverage,
  scalar-equivalence gates, and ARM64/x86 measurement protocol.
- [SIMD end-to-end manifest](simd-e2e-manifest.json): frozen same-source,
  same-host SIMD/scalar-control matrix and fail-closed evidence contract.
- [Stabilization and release-readiness matrix](release-readiness-2026-07-26.md):
  declared type/kind lifecycle coverage and the exact local gates that must
  precede confirmatory benchmarking.
- [WAL layout qualification v5 decision](wal-layout-qualification-v5-decision.json):
  exact source, protocol, schedule, environment, result, and independent
  reproduction hashes for the rejected compact-Vortex WAL promotion.
- [Physical GET admission Cohere 1M decision](physical-get-admission-cohere1m-aws-v1-decision.json):
  five paired AWS repetitions showing that process-wide admission protects
  overload but V9's roughly 104 backing GETs/query still fails the 8- and
  32-client latency gate, requiring the V10 bounded Arrow leaf layout.
- [Parquet versus Vortex tables](table-format-ab.md): corrected
  materialized-Arrow real-segment replay, storage/resource evidence, and the
  end-to-end default-selection gate. Its checked aggregate evidence is
  [`aws-vortex-segment-replay-2026-07-24.csv`](../web/assets/benchmarks/aws-vortex-segment-replay-2026-07-24.csv)
  with the matching
  [`aws-vortex-segment-replay-resources-2026-07-24.csv`](../web/assets/benchmarks/aws-vortex-segment-replay-resources-2026-07-24.csv).
- [Arrow IPC versus Vortex ANN buffers](vector-format-ab.md): typed candidate
  take/range access, compatibility blockers, and the separate ANN-container
  decision.
- [Systems comparison](systems-comparison.md): S3 Vectors, related systems,
  defensible novelty, and publication risk.
- [Cost and deployment](cost-and-deployment.md): measured index/GET costs,
  managed-service list-price context, fair client-compute accounting, and the
  exact license boundary.
- [Reproducibility](reproducibility.md): commands, gates, artifact schemas,
  raw telemetry, and chart generation.

## Qualification rules

A production point must:

- run the shipped full corpus and ground truth;
- use strict recall@10 and meet at least 0.95;
- identify method, quantizer, layout, `nprobe`, candidate budget, width, query
  cap, and decode cap;
- report startup separately from query latency;
- report p50/p95/p99, GETs, bytes, CPU, peak RSS, disk I/O, and cache footprint;
- prove `disk_cached` using storage-boundary counters; and
- retain repeats and rejected/obsolete rows rather than silently selecting the
  fastest trial.

The production campaign root is
`raw/2026-07-20` (historical raw artifact not distributed); the graph profile,
optimization ablation, recall curve, repetitions, and resource traces are under
`raw/2026-07-21` (historical raw artifact not distributed).

The current-result evidence manifest is
[`current-results.csv`](../web/assets/benchmarks/current-results.csv). Current
v7 GloVe layout and serving repetitions are consolidated in
[`aws-global-pq-v7-glove-layouts-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-glove-layouts-2026-07-21.csv)
and
[`aws-global-pq-v7-glove-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-glove-production-repetitions-2026-07-21.csv).
The cross-dataset v7 serving table is
[`aws-global-pq-v7-production-repetitions-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-production-repetitions-2026-07-21.csv).
Candidate recall/latency sweeps are in
[`aws-global-pq-v7-candidate-sweeps-2026-07-21.csv`](../web/assets/benchmarks/aws-global-pq-v7-candidate-sweeps-2026-07-21.csv).
