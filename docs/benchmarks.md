# Benchmarks

BORSUK has two benchmark layers:

- Criterion functions in `crates/borsuk/benches/local_search.rs` for local
  repeatable timing.
- `crates/borsuk/examples/benchmark_report.rs` for developer-facing tables,
  CSV artifacts, write/compaction lifecycle timing, parallel-query pressure,
  RSS sampling, dataset-size scale sweeps, routing-overfetch sweeps, and web
  charts.

The hosted docs page renders the CSV outputs interactively.

## Timing variance (repetitions and standard deviation)

Latency is noisy, so a single run can mislead. Timing benchmarks re-measure each
data point across several repetitions and report the metric as a **mean plus a
sample standard deviation** in a `<metric>_std` column; the correctness and I/O
columns (recall, bytes read, segments searched) are deterministic for a fixed
query set, so they are computed once and only the latency is repeated. On the web
charts the std renders as a **± error-bar whisker** on each point or bar for the
timing metrics.

This is wired through the shared chart renderer, so every chart shows whiskers
once its CSV carries the `_std` columns. It is live today for the
`metric-pruning`, `workload`, `filtering`, and `sparsity` sweeps (all test-based
and regenerable in seconds–minutes). The larger `benchmark_report`-driven sweeps
(sequential / parallel / scale / routing-overfetch / lifecycle) and the
million-plus-vector gates share the same rendering path and are being moved onto
the repeated-measurement harness; until each is regenerated its chart simply
omits the whisker.

`benchmark_report` measures the read-optimized query path. Each dataset is bulk
inserted through the append-only L0 path, explicitly compacted into
vector-local L1 leaves, and then queried. Compaction time is intentionally not
included in query latency; the report writes `lifecycle.csv` so ingest and
compaction throughput stay visible as their own measurement.

## Run

```bash
cargo bench --locked -p borsuk
cargo test --locked -p borsuk --test performance_smoke
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 100 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench
```

The report exposes query and layout knobs so recall can be evaluated as a
budget curve instead of a single hard-coded point:

```bash
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --synthetic-records 100000 \
  --queries 20 \
  --parallelism 1 \
  --segment-max-vectors 256 \
  --max-segments 32 \
  --routing-page-overfetch 8 \
  --max-candidates-per-segment 256
```

Use `--segment-max-vectors` to change ingest and compaction leaf size,
`--max-segments` to change the expensive segment-payload budget,
`--routing-page-overfetch` to change cheap routing metadata lookahead, and
`--max-candidates-per-segment` to change local exact-rerank work inside each
fetched leaf.

The smoke test and Criterion benchmark assertions use tie-aware recall so
duplicate/equal-distance vectors do not fail only because their ids differ.
They enforce `0.95` tie-aware recall@10 for the high-recall modes `pq-scan`,
`vamana-pq`, and `hybrid`; strict id recall stays a diagnostic for duplicate
or tied vectors.

To generate dataset-size scaling artifacts for the web charts, pass a
comma-separated synthetic record-count sweep. Dataset names are suffixed with
`-n<count>` so the interactive selector can distinguish each size:

```bash
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --synthetic-records-list 10000,100000 \
  --queries 100 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench-scale
```

That command writes `scale.csv` and `routing-overfetch.csv` in addition to
`sequential.csv`, `parallel.csv`, and `lifecycle.csv`. The scale artifact
normalizes names such as `synthetic-uniform-n100000` into a
`family=synthetic-uniform` column while preserving `records` as a numeric x-axis
for web charts.

Large-scale runs are intentionally outside default CI. Run the ignored release
gate explicitly when validating million-vector behavior:

```bash
BORSUK_LARGE_SCALE_OUTPUT=/tmp/borsuk-bench/large-scale.csv \
cargo test --locked --release -p borsuk --test large_scale \
  million_vector_local_search_scale_gate -- --ignored --nocapture
```

The large-scale gate defaults to 1,000,000 vectors, 16 dimensions,
`segment_max_vectors=128`, `routing_page_overfetch=8`, and batched ingest. Override with
`BORSUK_LARGE_SCALE_RECORDS`, `BORSUK_LARGE_SCALE_DIMENSIONS`,
`BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS`, and
`BORSUK_LARGE_SCALE_BATCH_RECORDS`. Query routing lookahead can be overridden
with `BORSUK_LARGE_SCALE_ROUTING_PAGE_OVERFETCH`. When
`BORSUK_LARGE_SCALE_OUTPUT` is set,
the gate writes one CSV row per high-recall mode so the release artifact can be
copied to `docs/web/assets/benchmarks/large-scale.csv`. The artifact includes
both tie-aware recall@10 and strict id recall@10, matching the smaller
benchmark CSVs.

## Pruning Across Metrics (RAG Fitness)

RAG stacks overwhelmingly rank by **cosine** similarity, so the question that
decides whether BORSUK fits a RAG workload is: does exact cosine search prune the
index the way Euclidean does, or does it read everything? The
`metric_pruning_bench` gate builds one clustered dataset (3,000 vectors, 32-d, 24
clusters → 125 segments over a multi-level routing tree) under several metrics,
runs 40 exact top-k queries each, and records how much of the index each query
proves skippable (`prune_pct`), the object bytes it reads, and latency. Exact
results are checked against an independent brute-force top-k, so recall is `1.0`
by construction for every metric. **Latency is measured across 8 repetitions and
reported as mean ± sample standard deviation** (the pruned/recall/bytes columns
are deterministic for a fixed query set, so only latency varies).

| Metric | Prunes? | Segments read (of 125) | Pruned | Bytes/query | p50 (mean ± std) | recall@10 |
|---|---|---:|---:|---:|---:|---:|
| `cosine` | yes | 6.2 | **95.1 %** | 396 KB | 13.3 ± 0.2 ms | 1.0000 |
| `angular` | yes | 6.2 | 95.1 % | 396 KB | 13.2 ± 0.3 ms | 1.0000 |
| `euclidean` | yes | 6.1 | 95.1 % | 395 KB | 13.5 ± 0.6 ms | 1.0000 |
| `manhattan` | yes | 7.1 | 94.4 % | 406 KB | 13.6 ± 0.4 ms | 1.0000 |
| `inner-product` | no | 125.0 | 0.0 % | 1.78 MB | 39.5 ± 0.4 ms | 1.0000 |

**Cosine and angular prune 95 %+ of the index — the same as the Lp family.** That
is the payoff of measuring their bubble geometry as Euclidean distance over
unit-normalized vectors (`‖a−b‖² = 2(1−cosine)`, so a Euclidean lower bound
converts to a sound cosine bound; see [api.md](api.md#the-pruning-tradeoff-read-this-before-picking-a-metric)).
A cosine query reads **~4.5× fewer bytes and runs ~3× faster** than
`inner-product`, which has no sound lower bound and must scan every segment to
stay exact. The tight latency std (sub-millisecond across repetitions) shows the
timing is stable, not a lucky single run. Your original vectors are stored and
returned unchanged — the normalization is only an internal detail of the routing
geometry. Regenerate (release build; the gate repeats the timed sweep 8×):

```bash
BORSUK_METRIC_PRUNING_OUTPUT=docs/web/assets/benchmarks/metric-pruning.csv \
  cargo test --locked --release -p borsuk --test metric_pruning_bench metric_pruning_gate -- --ignored
```

The fast `metric_pruning_is_sound` test asserts the contract on every commit:
cosine/angular/euclidean/manhattan each skip segments, `inner-product` scans all,
and all five return the exact top-k.

## Retrieval-Mode Mixtures (dense · sparse · text)

A real deployment rarely uses one retrieval mode. `mixture_workload_bench` builds
the same 5,000-record corpus under every combination of the three legs BORSUK
fuses — a **dense** vector, a **sparse** (SPLADE-style) named vector, and **BM25**
full text — and reports ingest time and query p50 (mean ± std over 6 repeats) so
the cost of adding a leg is legible. Every record always carries a primary dense
vector (BORSUK's model); a mixture's *query* uses only the legs it names.

| Mixture | Ingest | Query p50 (mean ± std) | Bytes/query |
|---|---:|---:|---:|
| `dense` | 108 ms | 8.5 ± 0.1 ms | 1.16 MB |
| `sparse` | 113 ms | 0.003 ± 0.002 ms | 0 B |
| `dense+sparse` | 114 ms | 9.4 ± 0.3 ms | 1.16 MB |
| `dense+text` | 127 ms | 18.8 ± 0.2 ms | 1.43 MB |
| `sparse+text` | 142 ms | 7.7 ± 0.1 ms | 183 KB |
| `dense+sparse+text` | 158 ms | 18.8 ± 0.2 ms | 1.43 MB |

The **sparse** leg is nearly free to query (an inverted index touched by only its
non-zeros — microseconds, no vector bytes), so adding it to a dense query barely
moves latency. The **dense** and **BM25 text** legs dominate query cost, and
ingest grows monotonically as legs are added (108 → 158 ms). Every leg fuses on
the same near-zero-RAM object-storage engine. Regenerate:

```bash
BORSUK_MIXTURE_OUTPUT=docs/web/assets/benchmarks/mixture-workload.csv \
  cargo test --locked --release -p borsuk --test mixture_workload_bench mixture_workload_gate -- --ignored
```

## Production Workload (Upserts + Deletes + Filter + Compaction + Restart)

Vector databases are chosen for how they behave on a Monday morning, not for a
3 % ANN edge. The `production_workload` gate runs the mix a real deployment
runs — versioned upserts (inserts *and* overwrites), deletes, metadata-filtered
search, compaction, and a process restart — rather than a static ANN sweep, and
checks the index for correctness the whole way (every live record resolves to
its newest value; deleted records are gone; a bucket filter returns exactly the
matching live set):

```bash
BORSUK_WORKLOAD_OUTPUT=docs/web/assets/benchmarks/production_workload.csv \
cargo test --locked --release -p borsuk --test production_workload \
  production_workload_gate -- --ignored --nocapture
```

40 rounds of a 200-record batch (~half fresh ids, half overwrites) with deletes
each round and compaction every fifth, then a restart and 200 bucket-filtered
top-10 searches:

| metric | value |
|---|---:|
| live records (after churn) | 2,189 |
| upserts / deletes applied | 8,000 / ~440 |
| write throughput | 166 ops/s |
| filtered search p50 / p95 | 83 ms / 105 ms |
| avg bytes read / query | 1.4 MB |
| avg GET requests / query | 225 |
| segment storage | 0.95 MB |

The point of this gate is **correctness under churn**, which it asserts on every
run; `production_workload_is_sound` keeps a fast version in the normal test run.
The latency reflects a deliberately fragmented index (many small segments from
one-batch-per-round writes) read cold after restart with paged routing — it is a
worst-case shape for read cost, not a tuned serving configuration, and larger
segments plus a warm cache move it substantially. Building this benchmark
surfaced and fixed a data-loss bug where `delete` on a paged index wiped every
record (`tests/paged_delete_compaction.rs`).

## Sparse Inverted Index (High-Vocabulary Lexical)

Lexical and learned-sparse vectors (BM25, SPLADE) live over huge vocabularies —
tens of thousands to millions of terms — but each vector carries only a few
dozen non-zeros. BORSUK stores a `VectorKind::Sparse` named vector as raw sparse
rows and searches them through an inverted index (`term -> [(row, weight)]`): a
query gathers candidates from its terms' posting lists and scores only those
rows with an exact sparse dot product. Nothing is ever densified, so cost tracks
the number of rows that actually share a term — not the vocabulary size.

The `sparse_inverted_bench_gate` gate contrasts this against scoring every row
(brute force) and against the densify-on-read approach BORSUK abandoned, which
would materialize each row as a dense `[f32; D]`:

```bash
BORSUK_SPARSE_BENCH_OUTPUT=docs/web/assets/benchmarks/sparse_inverted.csv \
cargo test --locked --release -p borsuk --test sparse_inverted_bench \
  sparse_inverted_bench_gate -- --ignored --nocapture
```

4,000 rows, 32 non-zeros each, top-10, 60 queries per vocabulary point:

| vocabulary | inverted p50 | brute p50 | speedup | rows scored | densify RAM |
|-----------:|-------------:|----------:|--------:|------------:|------------:|
|     10,000 |    211.8 µs  | 1329.9 µs |    6.3× |       9.8 % |     0.1 GiB |
|    100,000 |     39.3 µs  | 1333.1 µs |   34.0× |       1.0 % |     1.5 GiB |
|  1,000,000 |     13.5 µs  | 1358.5 µs |  100.9× |       0.1 % |    14.9 GiB |
|  5,000,000 |     10.4 µs  | 1347.4 µs |  129.9× |       0.0 % |    74.5 GiB |

The shape of the result: **as the vocabulary grows the inverted index gets
_faster_, not slower.** A larger term space means two vectors are less likely to
share a term, so each query touches a smaller slice of the corpus (9.8 % → 0.0 %)
and its exact result is returned in single-digit microseconds. Brute force stays
flat near 1.3 ms because it scans every row regardless. Meanwhile the densify
column is the wall BORSUK refused to hit: holding this corpus as dense rows would
need 74.5 GiB at a 5M-term vocabulary — and a query would have to touch all of
it. `sparse_inverted_is_sublinear` keeps a fast version of the check (exact
agreement with brute force, plus a bound on candidates touched) in the normal
test run.

## Dataset Scaling (10k → 10M)

The `dataset_scaling` gate answers one question directly: as the collection
grows, what happens to recall, latency, and memory? It sweeps a list of record
counts, and at each one runs the full production path — batched ingest,
compaction to L1, obsolete-segment GC, then paged (`resident_routing=false`,
near-zero-RAM) `pq-scan` search graded against exact search — with a *fixed*
per-query budget so the trend is not confounded by a moving target.

```bash
BORSUK_SCALING_OUTPUT=docs/web/assets/benchmarks/dataset-scaling.csv \
BORSUK_SCALING_RECORDS=10000,100000,1000000,10000000 \
cargo test --locked --release -p borsuk --test dataset_scaling \
  dataset_scaling_gate -- --ignored --nocapture
```

Defaults: 16 dimensions, `segment_max_vectors=256`, 32 queries per point,
`max_segments=128`, `routing_page_overfetch=16`, `max_candidates_per_segment=256`
(equal to the segment size, so each read segment is scanned in full — no PQ
approximation on the rows we score). A warm-up pass precedes the timed queries so
the reported p50/p95 are steady-state, not first-touch cold-start noise. The
synthetic vectors are **clustered** (a
vector's cluster id is `hash(seed) % num_clusters`, so cluster-mates are its true
neighbours and are scattered through the insert order) — the manifold structure
real embeddings have. Uniform-random data is a pathological ANN worst case where
neighbours are barely separated, and is not representative. Override the record
list with `BORSUK_SCALING_RECORDS`, and each knob with the matching
`BORSUK_SCALING_*` variable (`_DIMENSIONS`, `_QUERIES`, `_SEGMENT_MAX_VECTORS`,
`_MAX_SEGMENTS`, …). One CSV row is written per record count, with tie-aware and
id recall@10, p50/p95 query latency, resident-metadata bytes, the RSS peak delta
observed during the query loop, and average bytes/segments read per query. The
checked-in `docs/web/assets/benchmarks/dataset-scaling.csv` covers 10k, 100k, 1M,
and 10M.

The shape of the result: **recall stays 1.0 and cold query latency stays
sub-second while resident metadata holds flat at a few hundred bytes** across the
entire 1000× growth. Because the vectors are clustered, routing pinpoints the
query's cluster and reads only the handful of segments that hold it, so bytes read
per query barely grow — holding more vectors costs no resident RAM and little
extra I/O. `dataset_scaling_point_is_sound` keeps a tiny version of the sweep in
the normal (non-`--ignored`) test run.

## Object-Store Network Overhead (1M on SeaweedFS)

The same million-vector gate runs against an S3-compatible object store, so you
can measure exactly how much the network read/write path costs versus local
files. Point it at a bucket with `BORSUK_LARGE_SCALE_URI`:

```bash
BORSUK_LARGE_SCALE_URI=s3://your-bucket/large-scale \
BORSUK_LARGE_SCALE_BATCH_RECORDS=65536 \
BORSUK_LARGE_SCALE_OUTPUT=/tmp/borsuk-bench/large-scale-s3.csv \
cargo test --locked --release -p borsuk --test large_scale \
  million_vector_local_search_scale_gate -- --ignored --nocapture
```

Measured on the bundled SeaweedFS stack (`examples/seaweedfs`) at one million
16D vectors, against the same gate on local files on the same machine:

| Stage | Local files | SeaweedFS (network) | Overhead |
| --- | --- | --- | --- |
| Query, pq-scan | 368 ms | 515 ms | 1.40x |
| Query, vamana-pq | 386 ms | 524 ms | 1.36x |
| Query, hybrid | 379 ms | 546 ms | 1.44x |
| Recall@10 (all modes) | 1.000000 | 1.000000 | equal |
| Bytes read per query | 14.46 MB | 14.46 MB | equal |
| Resident metadata | 283 B | 254 B | equal |
| Ingest 1,000,000 | 62.0 s | 105.8 s | 1.71x |
| Compaction | 93.9 s | 160.2 s | 1.71x |
| Garbage collection | 6.4 s | 41.9 s | 6.5x |

Queries stay sub-second with about 40% network overhead and identical recall,
because a query is a bounded number of object reads: the routing pages plus the
selected segments (33 routing pages and 512 segments, 14.46 MB in this run).
Ingest and compaction cost about 1.7x more because every new object is a network
PUT. Garbage collection is the one operation dominated by per-object round trips,
since it deletes roughly sixteen thousand objects with one network call each.

## Completed 100M Build

`docs/web/assets/benchmarks/hundred-million-build.csv` is a checked-in
measurement of a completed local build of 100,000,000 16D vectors using
4096-vector segments and 1,048,576-record add batches. It ingested and compacted
in 5,907,443 ms into 24,415 segments reachable through 194 routing pages, with a
12.56 GB segment footprint, 6.00 GB of graph blocks, and 32 MB of resident index
metadata. The paired read probe is checked in at
`docs/web/assets/benchmarks/hundred-million-read.csv`.

## Concurrent Readers and Memory

Many readers share a single open index. The resident index metadata is loaded
once and is not duplicated per reader, so a thousand concurrent users do not
consume a thousand copies of the index. The parallel headroom gate opens one
1,000,000-vector index and runs a growing pool of concurrent hybrid searches
against it with a 512-segment / k=10 budget:

```bash
BORSUK_LARGE_SCALE_PARALLELISM=1,16,64,256,1024 \
BORSUK_LARGE_SCALE_PARALLEL_MAX_SEGMENTS=512 \
BORSUK_LARGE_SCALE_PARALLEL_OVERFETCH=8 \
BORSUK_LARGE_SCALE_PARALLEL_MAX_CANDIDATES=128 \
cargo test --locked --release -p borsuk --test large_scale \
  parallel_search_headroom_reports_rss_peak_against_budget -- --ignored --nocapture
```

| Concurrent readers | Resident index metadata | Peak RSS added | Per active query |
| ---: | ---: | ---: | ---: |
| 1 | 283 B | 0.85 MB | 0.9 MB |
| 16 | 283 B | 48.7 MB | 3.0 MB |
| 64 | 283 B | 172.9 MB | 2.7 MB |
| 256 | 283 B | 1.04 GB | 4.1 MB |
| 1024 | 283 B | 4.18 GB | 4.1 MB |

The resident index metadata stays flat at 283 bytes from one reader to a
thousand — the index itself is never copied per reader. What grows is the
transient working set of queries that are running at the same instant: each
in-flight search holds roughly 4 MB while it fetches and scores its segments,
independent of how large the collection is (a query touches a bounded number of
segments, not the whole index). Peak memory is therefore
`shared_index + simultaneous_queries x per_query_working_set`, not
`readers x index_size`.

Two open-time options bound that peak directly, without a caller-side worker
pool. `max_concurrent_searches` caps how many searches run their decode/score
phase at once, so peak working memory tracks the permit count rather than the
caller thread count -- the memory-for-latency tradeoff is that searches beyond
the permit count queue, raising tail latency when concurrency exceeds the cap.
`segment_cache_max_bytes` adds a shared, byte-bounded LRU of decoded segments:
concurrent queries that touch the same segment share one decoded copy instead of
each decoding its own, spending memory to save wall-time on hits (the opposite
tradeoff). With a 256 MB decoded-segment cache and a 64-permit admission gate,
the same sweep at 1,000,000 vectors:

| Concurrent readers | Peak RSS, default | Peak RSS, cache + gate |
| ---: | ---: | ---: |
| 64 | 489 MB | 601 MB |
| 256 | 1.48 GB | 694 MB |
| 1024 | 5.65 GB | 1.43 GB |

At a thousand concurrent readers the peak drops about 4x (5.65 GB to 1.43 GB)
and the run also finishes about 8x faster, because the gate stops a thousand
threads from thrashing memory and CPU at once. The sweep uses a distinct query
per worker, so segment overlap is low and most of the win here comes from the
gate; a realistic hot-key workload shares far more through the decoded cache.
Reproduce with the same command plus
`BORSUK_LARGE_SCALE_SEGMENT_CACHE_BYTES=268435456` and
`BORSUK_LARGE_SCALE_MAX_CONCURRENT_SEARCHES=64`.

For a self-contained, checked-in version of this claim, the `memory_scale`
example runs the same reader × concurrency sweep on a smaller build and writes
`docs/web/assets/benchmarks/memory-scale.csv`. On 100,000 `pq-scan` vectors the
admission gate keeps peak RSS flat as readers grow — 1024 concurrent readers add
only ~20 MB with `max_concurrent_searches = 16`, versus ~1.2 GB uncapped, while
p95 latency stays near 94 ms instead of thrashing to ~7.9 s:

```bash
cargo run --release -p borsuk --example memory_scale
# BORSUK_MEMSCALE_VECTORS=1000000 for the million-vector point
```

| Concurrent readers | Peak RSS added, uncapped | Peak RSS added, cap 16 | p95 uncapped | p95 cap 16 |
| ---: | ---: | ---: | ---: | ---: |
| 64 | 170 MB | 2.8 MB | 412 ms | 114 ms |
| 256 | 497 MB | 6.3 MB | 1999 ms | 105 ms |
| 1024 | 1200 MB | 20.7 MB | 7893 ms | 94 ms |

### Projected pq-scan on large segments

pq-scan and sq-scan select candidates from the compact PQ/routing codes and
persisted PQ bounds, then re-rank on full vectors. When a segment is larger than
the candidate budget, BORSUK decodes the segment column-projected (skipping the
vector column entirely) and reads back only the chosen candidates' vectors, so
per-query decode memory tracks the candidate budget rather than the segment
size. Results are identical to a full decode. Measured at 1,000,000 vectors with
4096-vector segments, pq-scan, a 128-candidate budget, and 256 concurrent
readers:

| Decode path | Peak RSS added |
| --- | ---: |
| Full segment decode | 1.74 GB |
| Column-projected + row-selective | 0.52 GB |

This is a deliberate memory-for-latency tradeoff: about 3.3x less peak memory
(128 candidate vectors decoded per segment instead of 4096) in exchange for
roughly 15% more wall-time, because the projected path makes a second
column-projected read to fetch the candidate vectors. Recall is unchanged --
results are identical to a full decode. It is automatic for pq-scan/sq-scan when
the candidate budget is below the segment length and the shared decoded cache is
off, and can be disabled per process with `BORSUK_DISABLE_PROJECTED_SCORING=1`.
Reproduce by adding
`BORSUK_LARGE_SCALE_SEGMENT_MAX_VECTORS=4096`,
`BORSUK_LARGE_SCALE_PARALLEL_LEAF_MODE=pq-scan`, and
`BORSUK_LARGE_SCALE_PARALLEL_MAX_CANDIDATES=128` to the parallel headroom command
(`BORSUK_DISABLE_PROJECTED_SCORING=1` measures the full-decode baseline).

To include the real-data smoke dataset used by the web docs:

```bash
curl -L https://raw.githubusercontent.com/scikit-learn/scikit-learn/main/sklearn/datasets/data/digits.csv.gz \
  -o /tmp/digits.csv.gz
gzip -dc /tmp/digits.csv.gz > /tmp/digits.csv
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 100 \
  --parallelism 1,2,4,8 \
  --csv /tmp/digits.csv \
  --csv-name sklearn-digits \
  --csv-dimensions 64 \
  --artifacts-dir /tmp/borsuk-bench
```

The scikit-learn digits dataset is documented by scikit-learn as 1,797 rows
with 64 features, each row representing one 8x8 handwritten digit image:
https://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_digits.html

## What Is Measured

Sequential rows:

- tie-aware recall@10 against exact mode, where any hit at or inside the exact
  kth distance counts even when duplicate vectors have different ids;
- strict id recall@10 as a diagnostic for duplicate-vector and tie behavior;
- termination-reason counts from `SearchReport`, so complete, exact-pruned,
  and budget-limited searches are visible in the artifact;
- dataset records, dimensions, segment size, routing overfetch, and
  approximate query budgets;
- p50/p95 latency;
- average segment bytes read;
- average graph bytes read;
- average resident metadata bytes reported by `SearchReport`;
- average segments searched, rows considered, rows exact-scored;
- object-cache hits and misses.

Scale rows:

- dataset family, concrete dataset name, mode, record count, dimensions,
  routing overfetch, and approximate budgets;
- tie-aware recall@10 and strict id recall@10 for each size;
- termination-reason counts for each dataset/mode/size row;
- p50/p95 latency, query bytes, graph bytes, resident metadata, segments
  searched, rows considered, exact-scored rows, and object-cache hits/misses
  as record count changes.

Routing-overfetch rows:

- high-recall modes only: `pq-scan`, `vamana-pq`, and `hybrid`;
- `routing_page_overfetch` values `1, 2, 4, 8, 16, 32`;
- tie-aware recall@10, strict id recall@10, p95 latency, query bytes, graph
  bytes, routing page/index reads, resident metadata, exact-scored rows, and
  cache misses for each value.

`routing_page_overfetch` is cheap metadata lookahead, not a segment-payload
multiplier. If persisted vector bounds are decisive, larger values can leave
routing page reads unchanged. If bounds are tied or close, larger values allow
the query to decode more cheap routing metadata before spending the expensive
segment and graph payload budgets. At each routing layer, the setting also acts
as a page-level floor for close pages, so one dense page cannot consume the
whole lookahead by leaf-segment count alone.

Lifecycle rows:

- ingest wall time and vectors/sec for the append-only L0 write path;
- compaction wall time and rewritten vectors/sec;
- pre/post segment counts, source segments read, output segments written, and
  records rewritten;
- compaction bytes read/written and byte throughput;
- routing page/index read/write counts and old graph payload reads during
  compaction.

Parallel rows:

- the same per-query recall, dataset size, latency, bytes, resident metadata,
  cache hits/misses, and termination-reason counts;
- worker count 1, 2, 4, and 8;
- total query throughput;
- process RSS before, sampled peak, after, and peak delta.

RSS is sampled from the benchmark process. `resident_bytes_estimate` is the
BORSUK metadata estimate. They answer different questions: RSS shows observed
process pressure during a parallel batch, while resident bytes shows the index
metadata that BORSUK budgets.

Large-scale rows:

- record count, dimensions, segment size, `max_segments`,
  `routing_page_overfetch`, and `max_candidates_per_segment`;
- pre/post segment counts, ingest time, compaction time, exact reference time,
  and compaction bytes read/written;
- delete-mode GC evidence measured right after compaction on the quiescent
  gate index: GC latency (`gc_ms`), objects scanned, objects deleted, and
  bytes reclaimed;
- mode, tie-aware recall@10, termination reason, approximate query time,
  segment payload count, bytes read, graph bytes read, RSS before/peak/after,
  RSS peak delta, resident bytes, rows considered, rows scored, and graph
  candidates.

Hundred-million build rows:

- completed inserted record count, dimensions, segment size, and batch size;
- elapsed build time and observed temporary bytes;
- pre-compaction segment count, routing leaf/page count, segment bytes, graph
  bytes, resident metadata bytes, manifest version, and RSS before/peak/after
  with peak delta.

`hundred-million-build.csv` records a completed local build of 100,000,000 16D
vectors — measured scale evidence for the write and compaction path.

Hundred-million read-probe rows:

- 100M record count, dimensions, compaction state, deterministic query seed,
  leaf mode, `max_segments`, `routing_page_overfetch`, and
  `max_candidates_per_segment`;
- whether the probe found the inserted seed id, termination reason, elapsed
  time, total/searched segments, routing page reads, segment bytes, graph bytes,
  cache hits/misses, considered rows, exact-scored rows, graph candidates, and
  resident metadata bytes.

## Current Local Results

Measured on Apple M3 Max, 16 cores, 128 GB RAM, Darwin 25.2.0, Rust 1.95.0.
The checked-in web artifacts were regenerated with `--queries 100`,
`--synthetic-records-list 10000,100000`, `--parallelism 1,2,4,8`, and the
scikit-learn digits CSV. Synthetic datasets use 64 dimensions,
`segment_max_vectors=256`, `max_segments=8`, `routing_page_overfetch=8`, and
`max_candidates_per_segment=64`. They are compacted into vector-local leaves
before query timing.

Lifecycle timing is reported separately from query latency:

| Dataset | Records | Ingest vectors/sec | Compaction vectors/sec | Ingest ms | Compaction ms | Segments read/written | Compaction bytes read/written |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | 12,782 | 4,160 | 782.3 | 2403.6 | 40/40 | 1.19 MB / 803.0 KB |
| synthetic-clustered | 10,000 | 12,000 | 2,975 | 833.3 | 3361.3 | 40/40 | 683.3 KB / 684.4 KB |
| synthetic-adversarial | 10,000 | 13,171 | 4,177 | 759.2 | 2394.1 | 40/40 | 351.6 KB / 458.3 KB |
| synthetic-uniform | 100,000 | 12,694 | 3,832 | 7877.6 | 26098.0 | 391/391 | 11.70 MB / 5.05 MB |
| synthetic-clustered | 100,000 | 11,757 | 2,820 | 8505.9 | 35461.1 | 391/391 | 6.54 MB / 4.97 MB |
| synthetic-adversarial | 100,000 | 13,021 | 3,118 | 7679.9 | 32071.9 | 391/391 | 3.35 MB / 4.32 MB |
| sklearn-digits | 1,797 | 10,314 | 2,501 | 174.2 | 718.5 | 8/8 | 229.0 KB / 291.9 KB |

| Dataset | Records | Mode | Tie Recall@10 | Id Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | exact | 1.00 | 1.00 | 3.2 | 76.1 KB | 0 B | 283 B |
| synthetic-uniform | 10,000 | flat-scan | 0.96 | 0.96 | 8.4 | 174.3 KB | 0 B | 283 B |
| synthetic-uniform | 10,000 | sq-scan | 0.96 | 0.96 | 9.8 | 174.3 KB | 0 B | 283 B |
| synthetic-uniform | 10,000 | pq-scan | 1.00 | 1.00 | 10.7 | 174.3 KB | 0 B | 283 B |
| synthetic-uniform | 10,000 | graph | 0.97 | 0.97 | 22.4 | 174.3 KB | 48.3 KB | 283 B |
| synthetic-uniform | 10,000 | vamana-pq | 1.00 | 1.00 | 21.8 | 174.3 KB | 48.3 KB | 283 B |
| synthetic-uniform | 10,000 | hybrid | 1.00 | 1.00 | 21.8 | 174.3 KB | 48.3 KB | 283 B |
| synthetic-clustered | 10,000 | exact | 1.00 | 1.00 | 11.9 | 118.0 KB | 0 B | 283 B |
| synthetic-clustered | 10,000 | flat-scan | 0.94 | 0.91 | 8.5 | 148.8 KB | 0 B | 283 B |
| synthetic-clustered | 10,000 | sq-scan | 0.94 | 0.91 | 8.4 | 148.8 KB | 0 B | 283 B |
| synthetic-clustered | 10,000 | pq-scan | 0.99 | 0.98 | 11.1 | 148.8 KB | 0 B | 283 B |
| synthetic-clustered | 10,000 | graph | 0.96 | 0.95 | 24.6 | 148.8 KB | 51.8 KB | 283 B |
| synthetic-clustered | 10,000 | vamana-pq | 0.99 | 0.98 | 24.0 | 148.8 KB | 51.8 KB | 283 B |
| synthetic-clustered | 10,000 | hybrid | 0.99 | 0.98 | 24.0 | 148.8 KB | 51.8 KB | 283 B |
| synthetic-adversarial | 10,000 | exact | 1.00 | 1.00 | 4.0 | 58.0 KB | 0 B | 283 B |
| synthetic-adversarial | 10,000 | flat-scan | 1.00 | 1.00 | 8.1 | 102.3 KB | 0 B | 283 B |
| synthetic-adversarial | 10,000 | sq-scan | 1.00 | 1.00 | 8.1 | 102.3 KB | 0 B | 283 B |
| synthetic-adversarial | 10,000 | pq-scan | 1.00 | 1.00 | 11.8 | 102.3 KB | 0 B | 283 B |
| synthetic-adversarial | 10,000 | graph | 1.00 | 1.00 | 22.4 | 102.3 KB | 33.0 KB | 283 B |
| synthetic-adversarial | 10,000 | vamana-pq | 1.00 | 1.00 | 24.1 | 102.3 KB | 33.0 KB | 283 B |
| synthetic-adversarial | 10,000 | hybrid | 1.00 | 1.00 | 24.0 | 102.3 KB | 33.0 KB | 283 B |
| sklearn-digits | 1,797 | exact | 1.00 | 1.00 | 8.9 | 224.6 KB | 0 B | 283 B |
| sklearn-digits | 1,797 | flat-scan | 0.46 | 0.46 | 9.1 | 228.5 KB | 0 B | 283 B |
| sklearn-digits | 1,797 | sq-scan | 0.46 | 0.46 | 7.6 | 228.5 KB | 0 B | 283 B |
| sklearn-digits | 1,797 | pq-scan | 1.00 | 1.00 | 10.4 | 228.5 KB | 0 B | 283 B |
| sklearn-digits | 1,797 | graph | 0.98 | 0.98 | 31.0 | 228.5 KB | 85.7 KB | 283 B |
| sklearn-digits | 1,797 | vamana-pq | 1.00 | 1.00 | 31.2 | 228.5 KB | 85.7 KB | 283 B |
| sklearn-digits | 1,797 | hybrid | 1.00 | 1.00 | 31.3 | 228.5 KB | 85.7 KB | 283 B |

The synthetic-uniform and synthetic-clustered generators intentionally include
duplicate/tied nearest vectors. Tie-aware recall avoids treating a different id
with the same exact kth-distance as a miss. Id recall remains in the artifacts
so duplicate-id effects stay visible.

These checked-in numbers must be regenerated whenever routing, compaction,
leaf-mode, storage, or cache behavior changes. Low recall on synthetic-uniform
after compaction is a regression because query vectors are present in the
dataset and should route to their vector-local leaf blobs. The benchmark report
fails if `pq-scan`, `vamana-pq`, or `hybrid` fall below `0.95` tie-aware
recall@10; `flat-scan`, `sq-scan`, and plain `graph` stay visible as
diagnostic/baseline modes but are not the high-recall release gate.
Checked-in scale-sweep artifacts cover 10k and 100k synthetic vectors for the
three synthetic families. Use the ignored large-scale gate as the separate
correctness and I/O check for the million-vector case.

The checked-in `scale.csv` now includes 10k and 100k synthetic sweeps generated
with `--synthetic-records-list 10000,100000` and 100 queries per dataset. At
100k vectors, all high-recall modes reached `1.000` tie-aware recall@10 with
the strict `max_segments=8` payload budget. A previous diagnostic probe showed
that clustered misses came from routing metadata breadth, not per-segment
candidate scoring: raising `max_candidates_per_segment` increased exact-scored
rows without improving recall, while decoding one more close sibling L0 routing
page restored the missing neighbors. Routing overfetch now has a page-level
floor at every routing layer, so the default `routing_page_overfetch=8` can
keep sibling metadata pages eligible for final segment ranking without raising
vector payload reads.

| Dataset | Records | Mode | Tie Recall@10 | p95 ms | Bytes/query | Graph bytes/query | Resident bytes |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 100,000 | pq-scan | 1.00 | 10.8 | 220.4 KB | 0 B | 283 B |
| synthetic-uniform | 100,000 | vamana-pq | 1.00 | 21.6 | 220.4 KB | 33.9 KB | 283 B |
| synthetic-uniform | 100,000 | hybrid | 1.00 | 21.8 | 220.4 KB | 33.9 KB | 283 B |
| synthetic-clustered | 100,000 | pq-scan | 1.00 | 12.8 | 246.2 KB | 0 B | 283 B |
| synthetic-clustered | 100,000 | vamana-pq | 1.00 | 24.1 | 246.2 KB | 34.9 KB | 283 B |
| synthetic-clustered | 100,000 | hybrid | 1.00 | 23.7 | 246.2 KB | 34.9 KB | 283 B |
| synthetic-adversarial | 100,000 | pq-scan | 1.00 | 11.1 | 158.5 KB | 0 B | 283 B |
| synthetic-adversarial | 100,000 | vamana-pq | 1.00 | 22.4 | 158.5 KB | 32.1 KB | 283 B |
| synthetic-adversarial | 100,000 | hybrid | 1.00 | 22.0 | 158.5 KB | 32.1 KB | 283 B |

The checked-in `routing-overfetch.csv` sweeps
`routing_page_overfetch=1,2,4,8,16,32` for the high-recall modes across the same
datasets. On the 100k synthetic rows, synthetic-uniform and synthetic-adversarial
stayed at `1.000000` tie-aware recall@10 across the sweep. Synthetic-clustered
was `0.970000` at `routing_page_overfetch=1` and reached `1.000000` from
`routing_page_overfetch=2` upward. Average routing page reads stayed around
`2.0` for decisive bounds and rose to `2.42` on the clustered 100k sweep,
showing that overfetch spends extra metadata reads only when routing bounds are
close enough to matter.

The latest million-vector gate was run with 1,000,000 synthetic vectors,
16 dimensions, `segment_max_vectors=128`, `max_segments=512`,
`routing_page_overfetch=8`, and `max_candidates_per_segment=128`. After
compaction into 7,813 vector-local segments, `pq-scan`, `vamana-pq`, and
`hybrid` all reached `1.000`
tie-aware recall@10 and strict id recall@10 while reading at most 512 segment
payloads. All three modes read 14.46 MB/query and no graph bytes in this run:
the 128-row candidate budget already covers each 128-row compacted segment, so
graph-backed modes skip graph traversal instead of paying for graph I/O that
cannot reduce local exact-rerank work. The checked-in `large-scale.csv` run
ingested in 34.1s, compacted in 57.2s, and ran the exact recall reference in 0.68s
on the same machine. Compaction read 161.77 MB and
wrote 224.70 MB, counting both new segment and graph payload bytes. The
delete-mode GC that ran right after compaction scanned 32,180 objects, deleted
16,487 obsolete ones, and reclaimed 769.78 MB in 3.5s. RSS peak
delta stayed below 256 KB for each measured
single-query mode. The latency fix is budget-aware graph dispatch: graph blocks
are read only when `k < min(max_candidates_per_segment, segment_len) <
segment_len`, so full-segment candidate budgets get the same exact-rerank
coverage without the graph overhead.

| Records | Mode | Tie Recall@10 | Id Recall@10 | Query ms | Segments searched | Bytes/query | Graph bytes/query | Routing pages | RSS peak delta | Resident bytes |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000,000 | pq-scan | 1.00 | 1.00 | 208.0 | 512 | 13.79 MB | 0 B | 33 | 64.0 KB | 283 B |
| 1,000,000 | vamana-pq | 1.00 | 1.00 | 199.0 | 512 | 13.79 MB | 0 B | 33 | 16.0 KB | 283 B |
| 1,000,000 | hybrid | 1.00 | 1.00 | 195.0 | 512 | 13.79 MB | 0 B | 33 | 16.0 KB | 283 B |

The checked-in `hundred-million-build.csv` records a completed local build that
inserted 100,000,000 of 100,000,000 requested 16D vectors with 4096-vector
segments and 1,048,576-record add batches. It finished in 5,907,443 ms, observed
19.29 GB in the temp directory, published 24,415 pre-compaction segments, used
191 routing leaf pages / 194 routing pages, and reported 31.8 MB of resident
metadata in the write handle while paged readers report 275 B resident metadata
from the same manifest.

The checked-in `hundred-million-read.csv` probes the completed 100M artifact
with paged routing and a local read-through cache after the first bounded
L0-to-L1 compaction batch rewrote 2,097,152 records. The deterministic query is
the inserted vector with id `42`, so `hit_own_id=true` is a correctness smoke
check for the selected read budget. The 8-segment `pq-scan` probe found id `42`
in 106 ms with 4.85 MB of segment bytes and 32,768 exact-scored rows. The
32-segment `pq-scan` probe found the same id in 335 ms with 17.19 MB and
131,072 exact-scored rows. A 32-segment `hybrid` probe with a full 4096-row
candidate budget skipped graph reads and took 277 ms. The same 32-segment
`hybrid` probe with a 512-row candidate budget exact-scored only 16,384 rows,
but graph traversal read 7.87 MB of graph payload and took 2,859 ms even from
cache. For this 16D/4096-row leaf shape, graph traversal is currently a poor
read-latency tradeoff unless the caller has evidence that lower exact-rerank
work matters more than graph payload and traversal cost.

The same local artifact was then advanced through six bounded L0-to-L1
compaction batches before stopping the loop to keep the review session
bounded. Those six batches rewrote 12,582,912 records total. Each batch read
512 L0 segments, wrote 512 L1 segments, read/wrote 6 routing pages, and read
zero old graph payloads. After batch 6, paged stats still showed 100,000,000
records, 24,415 active segments, 191 routing leaf pages / 194 routing pages,
and 275 B resident metadata. A dry-run GC with `--min-age-seconds 0` scanned
55,901 objects and reported 3.05 GB reclaimable without deleting anything.
The full 100M compaction pass remains a wall-clock throughput task for the
current serial compactor, not a correctness prerequisite for the checked-in
read-probe artifact.

## Parallel Graph Pressure

The table below shows the graph-heavy modes at 8 workers. The web page exposes
all modes and every parallelism point.

| Dataset | Records | Mode | Workers | QPS | p95 ms | RSS peak delta | Graph bytes/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| synthetic-uniform | 10,000 | graph | 8 | 325.0 | 29.4 | 1.16 MB | 48.3 KB |
| synthetic-uniform | 10,000 | vamana-pq | 8 | 315.3 | 30.7 | 528.0 KB | 48.3 KB |
| synthetic-uniform | 10,000 | hybrid | 8 | 326.7 | 29.1 | 912.0 KB | 48.3 KB |
| synthetic-clustered | 10,000 | graph | 8 | 285.8 | 32.8 | 512.0 KB | 51.8 KB |
| synthetic-clustered | 10,000 | vamana-pq | 8 | 346.9 | 27.4 | 464.0 KB | 51.8 KB |
| synthetic-clustered | 10,000 | hybrid | 8 | 304.4 | 31.8 | 544.0 KB | 51.8 KB |
| synthetic-adversarial | 10,000 | graph | 8 | 324.7 | 29.0 | 976.0 KB | 33.0 KB |
| synthetic-adversarial | 10,000 | vamana-pq | 8 | 307.1 | 31.4 | 608.0 KB | 33.0 KB |
| synthetic-adversarial | 10,000 | hybrid | 8 | 340.0 | 28.2 | 432.0 KB | 33.0 KB |
| synthetic-uniform | 100,000 | graph | 8 | 330.9 | 28.0 | 448.0 KB | 33.9 KB |
| synthetic-uniform | 100,000 | vamana-pq | 8 | 314.6 | 29.6 | 400.0 KB | 33.9 KB |
| synthetic-uniform | 100,000 | hybrid | 8 | 325.1 | 29.1 | 768.0 KB | 33.9 KB |
| synthetic-clustered | 100,000 | graph | 8 | 302.5 | 31.5 | 496.0 KB | 34.9 KB |
| synthetic-clustered | 100,000 | vamana-pq | 8 | 285.8 | 34.2 | 576.0 KB | 34.9 KB |
| synthetic-clustered | 100,000 | hybrid | 8 | 284.5 | 33.9 | 800.0 KB | 34.9 KB |
| synthetic-adversarial | 100,000 | graph | 8 | 313.1 | 30.9 | 432.0 KB | 32.1 KB |
| synthetic-adversarial | 100,000 | vamana-pq | 8 | 294.9 | 31.4 | 416.0 KB | 32.1 KB |
| synthetic-adversarial | 100,000 | hybrid | 8 | 312.7 | 29.8 | 432.0 KB | 32.1 KB |
| sklearn-digits | 1,797 | graph | 8 | 239.1 | 39.3 | 464.0 KB | 85.7 KB |
| sklearn-digits | 1,797 | vamana-pq | 8 | 238.0 | 40.0 | 432.0 KB | 85.7 KB |
| sklearn-digits | 1,797 | hybrid | 8 | 245.5 | 38.9 | 400.0 KB | 85.7 KB |

## Criterion Entries

- `local_exact_search_10k_x_64`
- `local_approx_report_10k_x_64`
- `local_flat_scan_approx_report_10k_x_64`
- `local_sq_scan_approx_report_10k_x_64`
- `local_pq_scan_approx_report_10k_x_64`
- `local_vamana_pq_approx_report_10k_x_64`
- `local_hybrid_approx_report_10k_x_64`
- `local_warm_cache_approx_report_10k_x_64`
- `local_clustered_approx_report_10k_x_64`
- `local_adversarial_approx_report_10k_x_64`

The smoke test keeps exact search plus every implemented approximate leaf mode
under the local one-second target and validates recall, byte counters, segment
budgets, graph reads, and resident metadata counters.
