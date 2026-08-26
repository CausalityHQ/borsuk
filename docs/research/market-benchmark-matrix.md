# Market-comparable benchmark matrix

The machine-readable contract is
[`market-benchmark-matrix.csv`](market-benchmark-matrix.csv). A publication
result is incomplete unless every applicable row is measured or explicitly
marked blocked with the missing source artifact. A historical BORSUK result
cannot satisfy a row after a storage descriptor, codec, default, or query-path
change.

## Required workloads

| Goal | Required corpus | Primary comparison |
|---|---|---|
| Dense ANN baseline | DBpedia Entities OpenAI 1M, 1536D | LanceDB-style 1M RAG baseline |
| Dense QPS/cost curve | Cohere Medium 1M and Cohere Large 10M | VectorDBBench |
| Write-to-serve lifecycle | LAION 100M | insert/search-under-load and freshness |
| Very large dense | MS MARCO V2 138M | object-storage economics at 100M+ |
| Filter-heavy | seeded synthetic 15M/256D | narrow/wide filters and exact fallback |
| Multi-tenant | namespaces from 10K to 1M rows | isolation, locality, noisy neighbours |
| Hybrid | dense+sparse, dense+BM25, sparse+BM25 | BEIR NDCG/MRR/recall, not ANN recall alone |
| Late interaction | MS MARCO passage/ColBERT | MaxSim quality and rerank cost |
| Object-store path | every scalable workload | first prepared-open query and cache mixtures |

VectorDBBench is used as a dataset/workload compatibility anchor, not as a
license to compare differently provisioned systems. Its public repository
includes Cohere 1M/10M, LAION 100M, filtering, and insertion-under-load
workloads: [Zilliz VectorDBBench](https://github.com/zilliztech/VectorDBBench).
DBpedia is retained for continuity with published LanceDB 1M comparisons.
Hybrid evaluation uses the public [BEIR](https://github.com/beir-cellar/beir)
loaders and qrels. Late interaction follows the MaxSim model in the public
[ColBERT](https://github.com/stanford-futuredata/ColBERT) implementation.

Current VectorDBBench 1.0.20 defines Cohere Medium and Large as 1M/768D and
10M/768D. Its 1024D 1M/10M cases are BioASQ, not Cohere; the machine-readable
matrix deliberately keeps those identities separate. BORSUK consumes the
official unshuffled Parquet shards directly so that vector precision, row ids,
queries, and ground truth remain identical to the control suite.

MS MARCO V2's 138M passages are a text corpus, not a canonical 768D dense-vector
artifact. That row remains blocked until the experiment pins an encoder name,
model revision, tokenizer revision, pooling/normalization rule, query set, and
qrels, then publishes checksums for the resulting standard Parquet shards.
Labeling an arbitrary embedding export “MS MARCO V2 138M” would make the recall
and cost comparison irreproducible.

The conventional ANN-Benchmarks DBpedia generator takes the 1M source rows,
uses a seeded 10K query split, and therefore indexes 990K rows. Publication
tables must show both numbers instead of presenting “1M source” as one million
indexed vectors. Its exact-neighbour build is a separate measured preparation
phase and is never charged to query latency.

## Executable workload adapters

[`scripts/market_benchmark_runner.py`](../../scripts/market_benchmark_runner.py)
plans three fresh index repetitions, independent cache directories, bounded
production concurrency, and an explicitly labelled research ceiling. Dense
ANN/lifecycle uses `production_bench`; BEIR hybrid uses
`hybrid_retrieval_bench`; filter, namespace, and late-interaction rows use
`market_workload_bench`. The runner refuses to reuse an output directory and
builds one immutable index per repetition before read-only cache profiles.

The filter adapter generates only the matrix's declared seeded synthetic
corpus; `scale` must equal `tenants × records_per_tenant`. Ten duplicate rows
form a deterministic top-10 oracle, and tenant separation is larger than a
binary16 ULP so f16 storage cannot create artificial cross-tenant ties. The
query output contains selectivity, recall@10, exact-fallback ratio, pruned
segments, rows evaluated/passed, cache/backing bytes, GETs, raw latency samples,
mean, sample standard deviation, and p50/p95/p99/max.

The namespace adapter creates physically independent BORSUK index URIs and
cache/admission state for every declared namespace size. Baseline and
noisy-neighbour phases use independent cache directories with identical
priming; otherwise baseline traffic would warm the noisy phase and fabricate a
speedup. It reports recall, latency distributions, cache locality, bytes/GETs,
and slowdown by namespace. BORSUK is an embedded library, so authentication is
outside this data-plane boundary: adapter rows record zero in-library auth
failures/overhead instead of inventing an auth implementation. Deployment auth
must be benchmarked in the actual service wrapper.

Late interaction consumes normalized standard Parquet, not a private token
file:

- `documents.parquet`: `document_id: utf8`,
  `tokens: list<fixed_size_list<float32|float16, N>>`;
- `queries.parquet`: `query_id: utf8`, the same `tokens` type, and
  `relevant_ids: list<utf8>`.

Each declared token frontier builds the MRR@10/recall@50 versus latency curve
and records token-search time, SIMD MaxSim rerank time, query-token count,
token-hit/entity amplification, bytes, cache tiers, GETs, and raw
distributions. MS MARCO/ColBERT remains publication-blocked until the
encoder/checkpoint and those normalized Parquet files are checksum-pinned; the
adapter never replaces them with a synthetic dataset under the MS MARCO label.

## Cache-state contract

“Cold” and “warm” are not publication labels because they hide different
states. Every query row uses one of:

- `uncached`: library open and mandatory index metadata preparation are
  complete; the local data cache is empty; required object bytes may come from
  S3/object storage.
- `disk_cached`: the same query data is in the configured local disk cache and
  the measured query issues zero backing-store GETs.
- `mixed_coverage`: 0/25/50/75/100% byte coverage is prepared independently,
  with separate in-cache, out-of-cache, and mixed query-locality bins. Coverage
  is measured from bytes required by the trace, not from whole-index bytes.

Each cache-state point emits per-query raw samples, mean, standard deviation,
p50/p95/p99/max, QPS, logical bytes, backing GET/HEAD count, returned S3 bytes,
cache bytes, and useful-byte ratio.

## Resource and lifecycle contract

Every experiment has a time-aligned resource trace and phase markers for
download, ingest, WAL publication, searchable, background indexing,
compaction, open/prepare, and each query phase. Required measurements are:

- process mean/peak CPU, RSS and VMS;
- physical process read/write bytes;
- local cache and build-scratch disk bytes over time;
- S3 GET/HEAD/PUT and returned/written bytes;
- ingest vectors/s, time-to-searchable, time-to-fully-indexed, and write
  amplification;
- concurrency at bounded production defaults and a separately labelled
  uncapped research ceiling;
- at least three fresh-prefix/fresh-process repetitions, with standard
  deviation shown on charts.

Lifecycle write measurements must record the configured durable batch size.
`production_bench` accepts `BORSUK_BENCH_WRITE_BATCH_SIZE` (default `1024`) as
the maximum durable batch size and emits it in both aggregate write-cost and
lifecycle artifacts; raw samples bind each actual batch size. When a mutation
cohort would otherwise create fewer batches than configured writers, the
cohort is split into balanced partial batches so every writer participates.
Single-record latency and batched throughput are separate workload points:
neither may be extrapolated from the other. Production qualification sweeps
`1`, `32`, `128`, and `1024` records per durable publish, preserving raw
per-batch latency and object-store request samples for each point.
`BORSUK_BENCH_WRITE_OPS` fixes the mutation count when set and fails closed if
it exceeds either the pinned dataset or the product's maintenance-free online
delta envelope. Otherwise the sample is the smaller of the historical five
percent corpus cohort and that envelope. Consequently the measured row count
may differ across vector dimensions; every receipt reports the exact count.

Client compute is an explicit, equal line for every system. BORSUK's library
executes inside that client; S3 Vectors, TurboPuffer, and other services also
need a request-generating client, so client compute is not charged only to
BORSUK. Managed-service server compute and service fees remain separate rows.

For BORSUK, `searchable` means the immutable Parquet WAL object and the new
manifest frontier have been durably published; another process can refresh and
read the row. `fully indexed` means the bounded WAL tail has been materialized
into immutable segment-local indexes. It does **not** mean that the whole
corpus-wide scan artifact was rebuilt. That optional maintenance boundary is
reported separately as `consolidation_ms` and
`consolidation_amplification`. The lifecycle CSV marks its data-byte write
amplification as a lower bound because metadata/routing table writes are
measured separately by the object-store/resource trace rather than guessed.

## Recall/latency policy

The complete curve is published, not one cherry-picked point. Dense rows sweep
the routing/probe budget, approximate-code width, shortlist size, and exact
rerank budget. Hybrid rows sweep per-leg candidate depth and fusion/rerank
budget. The table identifies:

- exact 1.0-recall control;
- first point meeting each recall threshold;
- bounded production choice;
- dominated points;
- latency, resource, S3-request, and cost deltas for each recall increment.

Dataset-specific best settings are evidence, not user defaults. Production
defaults must come from persisted heuristics over metric, dimensions, declared
element type, corpus size, measured density/token statistics, available RAM,
storage backend, and observed filter/tenant distribution.
