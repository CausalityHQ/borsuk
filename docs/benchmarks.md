# Benchmarks

BORSUK has two benchmark layers:

- Criterion functions in `crates/borsuk/benches/local_search.rs` for local
  repeatable timing.
- `crates/borsuk/examples/benchmark_report.rs` for developer-facing tables,
  CSV artifacts, write/compaction lifecycle timing, parallel-query pressure,
  RSS sampling, dataset-size scale sweeps, routing-overfetch sweeps, and web
  charts.

The hosted docs page renders the CSV outputs interactively.

`benchmark_report` measures the read-optimized query path. Each dataset is bulk
inserted through the append-only L0 path, explicitly compacted into
vector-local L1 leaves, and then queried. Compaction time is intentionally not
included in query latency; the report writes `lifecycle.csv` so ingest and
compaction throughput stay visible as their own gate.

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
