# Production Readiness

This page explains what "production-ready" means for BORSUK: the areas that are
verified, the exact command that verifies each one, and the artifact it leaves
behind. It is written so you can judge a build's maturity for yourself and
reproduce the evidence — whether you are evaluating BORSUK, operating it, or
cutting a release. A build is **not production-ready** until every area below
checks out on that build's own artifacts; local smoke checks are useful during
development but do not certify production use on their own. The
[Evidence Map](#evidence-map) at the end ties each area to its checked-in
artifact and the command that regenerates the evidence.

## Correctness

The full Rust workspace checks:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
```

The suite covers:

- create/open compatibility for existing indexes;
- generated ids and explicit ids;
- duplicate-id rejection without full payload scans;
- append after non-resident open that preserves existing routing page refs,
  lets generated-id appends avoid unrelated parent routing pages, reuse the rightmost append parent when readable, and use routing id blooms for explicit-id
  duplicate checks;
- `get_vector(id)` lookup through the segment bloom filters;
- exact search, id search, vector search, batch search, and report search;
- `flat-scan`, `sq-scan`, `pq-scan`, `graph`, `vamana-pq`, and `hybrid`;
- compaction from L0 to L1+ and dry-run/delete garbage collection;
- vector-local compaction that keeps strict-budget recall high after append
  ingest;
- scoped compaction that reads only selected source leaf payloads, never old
  graph blocks or unrelated leaves, exposes routing page/index and graph
  payload read counters, and reuses unchanged routing page objects;
- scoped compaction from routing page metadata whenever routing pages exist,
  including handles that started with resident summaries, using page-level `level_mask` metadata to skip unrelated routing pages without reading
  unselected segment, graph, or routing page blobs;
- scoped compaction that promotes oversized top routing indexes into higher
  parent layers from page-ref metadata, without reading unrelated parent page
  bodies;
- scoped compaction that patches dirty leaf page refs by persisted ordinal, so
  sparse leaf ordinals do not trigger full branch scans or dense-array rewrites;
- persisted leaf-level routing page indexes/content pages, approximate page
  drill-down through persisted vector bounds with centroid/radius fallback,
  routing-metadata overfetch before strict segment-payload caps, page-level id blooms for non-resident `get_vector(id)`, a resident segment-summary vector empty open/search path, GC protection of active segment/graph objects through
  routing page metadata, and computed multi-level routing pages;
- top-down parent-to-leaf page-walk search and compaction candidate selection
  from persisted routing pages;
- strict `ram_budget` enforcement with no silent segment skipping;
- local-file and S3-compatible object-store paths.

## Packages

The Python package is built and tested from its wheel, not from the source tree:

```bash
(cd python && uvx maturin build --locked --out dist)
wheel="$(ls -t python/dist/borsuk-*.whl | head -1)"
BORSUK_WHEEL_PATH="$wheel" uv run --with "./$wheel" python -m unittest discover python/tests
```

CI runs that matrix on Python 3.12, 3.13, and 3.14 across Linux x64, Linux
arm64, Windows x64, macOS arm64, and macOS Intel.

The TypeScript package is built and tested from the native N-API bridge:

```bash
(cd packages/borsuk && npm ci && npm run build:native && npm test)
```

CI runs the npm matrix on Node 22, 24, and 26 across Linux x64, Linux arm64,
Windows x64, macOS arm64, and macOS Intel.

## Storage format

Persistent index data is binary and efficient:

- `CURRENT` is the only non-Parquet persistent object and remains a fixed
  binary pointer;
- manifests, segment summaries, routing bloom filters, pivot/routing tables,
  vector records, scalar codes, PQ codes, and graph blocks are Parquet;
- no persistent JSON table is allowed in the index format;
- ids use compact binary/numeric storage primitives internally; long external
  string ids must not be repeated in hot graph/routing structures;
- manifest publication is append-only and out-of-place;
- obsolete segment and graph deletion is explicit and dry-run by default;
- garbage collection derives active segment and graph paths from routing page
  metadata when resident segment summaries are empty, without reading payload
  blobs;
- checksums catch stale or corrupt manifest/routing/pivot tables.

Avro and Protobuf can be reconsidered for future append logs or control-plane
messages, but they are not production storage for vector/index tables.

## Performance

The benchmark suite regenerates fresh artifacts:

```bash
cargo bench --locked -p borsuk
cargo test --locked -p borsuk --test performance_smoke
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --synthetic-records-list 10000,100000 \
  --queries 100 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench
BORSUK_LARGE_SCALE_OUTPUT=/tmp/borsuk-bench/large-scale.csv \
BORSUK_LARGE_SCALE_ROUTING_PAGE_OVERFETCH=8 \
cargo test --locked --release -p borsuk --test large_scale \
  million_vector_local_search_scale_gate -- --ignored --nocapture
```

The 10k smoke check uses tie-aware recall for equal-distance vectors and
enforces at least `0.95` tie-aware recall@10 for `pq-scan`, `vamana-pq`,
and `hybrid`.

The benchmark report includes synthetic uniform, clustered, and adversarial
datasets at 10k and 100k record counts, plus at least one real dataset such as
`sklearn-digits`. It compares exact search with every leaf mode and reports
recall, p50/p95 latency, bytes read, graph bytes read, records scored, cache
hits/misses, and `resident_bytes_estimate`. Million-vector evidence comes from
the separate ignored large-scale test and is checked in as
`docs/web/assets/benchmarks/large-scale.csv`.

Benchmark artifacts include dataset record count, dimensions, segment
size, routing overfetch, query budgets, tie-aware recall, strict id recall, and
termination reasons.
Routing-overfetch artifacts sweep `routing_page_overfetch` for the
high-recall modes and report recall, latency, routing page reads, bytes,
resident metadata, and cache counters.
Lifecycle artifacts report append ingest time, ingest throughput,
compaction time, rewritten records, source/output segment counts, compaction
bytes read/written, routing page/index read/write counts, and old graph payload
reads.
The ignored large-scale test publishes `large-scale.csv` with million-vector
tie-aware recall, strict id recall, termination reason, routing overfetch,
latency, segment, byte, graph-byte, RSS before/peak/after, RSS peak-delta,
resident-byte, compaction, and delete-mode GC counters (`gc_ms`,
`gc_objects_scanned`, `gc_objects_deleted`, `gc_bytes_reclaimed`) for
`pq-scan`, `vamana-pq`, and `hybrid`.
The scale-attempt path also produces a 100M+ write-shaped artifact before
any planet-scale claim is made. That artifact uses generated ids,
4096-vector or larger ingest segments, large add batches, paged routing stats,
segment/graph/routing byte counters, manifest version, RSS before/peak/after,
and an explicit stop reason. The write-shaped attempt is followed by bounded
compaction into read-shaped L1+ leaves and paged-routing read probes against
the compacted artifact.
The benchmark command fails if the high-recall modes `pq-scan`,
`vamana-pq`, or `hybrid` report less than `0.95` tie-aware recall@10.
Parallel graph pressure reports worker count, QPS, p95 latency,
`rss_peak_delta`, graph bytes per query, resident bytes, and cache
hits/misses for `graph`, `vamana-pq`, and `hybrid`. The hosted web docs
render the lifecycle, sequential, routing-overfetch, scale, large-scale, and
parallel CSV files interactively.

## Memory behavior

Memory failures are explicit:

- create/open/add/compact fail if resident metadata exceeds `ram_budget`;
- queries report resident metadata through `SearchReport` and `IndexStats`;
- `resident_bytes_estimate` covers index metadata kept resident by the handle
  only: manifest/config fields, resident pivots, and resident segment summaries
  when routing is opened in resident mode. It does not include per-query segment,
  graph, routing-page, Arrow decode, object-store client, cache, allocator, or
  thread-stack memory.
- open with `cache_dir` reads fresh `CURRENT` and invalidates stale cached
  active manifest/routing/pivot metadata tables before returning a handle;
- cached segment, graph, and routing page payloads are validated by checksum
  and repaired from backing storage when only the local cache copy is corrupt;
- query reports count segments skipped by routing-page pruning before leaf page
  decode, not only segments skipped after segment summaries are loaded;
- query reports expose recall guarantee semantics: exact mode reports `exact`,
  complete approximate coverage reports `budget-complete`, and any routing
  preselection skip, epsilon stop, byte/latency/segment budget stop, or
  per-segment candidate truncation reports `degraded`;
- `guaranteed_recall` / `guaranteedRecall` approximate searches disable silent
  recall-loss paths where possible and return a typed
  `recall_guarantee_violated` error when a hard budget would violate the
  guarantee;
- `IndexStats` reports active segment, record, segment-byte, graph-byte, and
  routing-topology counters from routing page index aggregates when resident
  summaries are empty;
- Python, TypeScript, and CLI stats calls propagate corrupt stats metadata
  errors instead of silently reporting partial counters;
- large parallel graph queries report RSS growth in benchmark artifacts, and
  production `ram_budget` settings should leave explicit headroom above
  `resident_bytes_estimate` for concurrent query payloads, graph expansion, page
  decoding, local cache buffers, and runtime overhead;
- query budgets can stop additional I/O, but they do not hide active data or
  return partial results as if the full index had been searched; `SearchReport`
  exposes a typed termination reason for complete, pruned, epsilon-stopped,
  and budget-stopped queries;
- at large scale, routing metadata is paged or hierarchical enough to stay
  inside the configured RAM budget without loading a flat summary row for every
  leaf.

### Recall guarantee semantics

The recall guarantee contract holds in Rust, Python, and TypeScript. Exact mode
returns true k-NN under the index metric for the active snapshot and reports
`exact`. Approximate mode is an empirical ANN path, and its report conservatively
classifies any known recall-loss condition as `degraded`, including routing
preselection pruning, budget or epsilon stops, and per-segment candidate
truncation.

When approximate search returns `budget-complete`, the query completed
without skipped segments or candidate truncation. When callers request
`guaranteed_recall` / `guaranteedRecall`, the implementation avoids silent
degradation and returns the typed `recall_guarantee_violated` error if a hard
budget would prevent that complete coverage.

## Language consistency

Rust, Python, and TypeScript expose the same public model:

- typed metrics, search modes, and leaf modes;
- vectors with generated ids or caller-provided ids;
- compact arbitrary ids in the storage model, with typed string/number/binary
  convenience shapes in Python and TypeScript;
- no `payload_refs` public parameter;
- no `stringDistance` or string-specific search API;
- separate searches for ids and vectors;
- load-vector-by-id API;
- report APIs for tuning create-time `segment_max_vectors`, compaction
  `target_segment_max_vectors` / `targetSegmentMaxVectors`, `max_segments`,
  `max_candidates_per_segment`, `max_bytes`, and cache behavior;
- documented create-time versus query-time parameters.

## Object storage

S3-compatible checks run against a real endpoint:

```bash
export AWS_ENDPOINT=http://127.0.0.1:8333
export AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_REGION=us-east-1
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes

cargo test --locked -p borsuk s3_compatible_index_round_trip_when_configured \
  --test s3_compatible
```

For the repository-local full endpoint smoke, run the SeaweedFS or MinIO stack.
Each brings up a real S3 endpoint, then runs the Rust S3-compatible test, the
request-rate soak, and (SeaweedFS) the Rust, Python, and TypeScript S3 examples
against it:

```bash
./examples/seaweedfs/run-smoke.sh
./examples/minio/run-smoke.sh
```

The same index layout works on AWS S3, MinIO, SeaweedFS, and local files.

### Request-rate soak

The `s3_soak` integration test measures what a backing store actually serves:
object-store requests per query and per add, query throughput, p50/p95 read
latency, and the cache hit ratio. It builds an index on the endpoint, runs a
paged (minimal-RAM) query pass, then a pass with a warm decoded-segment cache to
show the RAM-for-request-rate tradeoff. It is gated on `BORSUK_S3_TEST_URI` and
tunable with `BORSUK_SOAK_VECTORS` / `BORSUK_SOAK_QUERIES`.

```bash
BORSUK_S3_TEST_URI=s3://borsuk-test/indexes \
  cargo test --locked -p borsuk --test s3_soak -- --nocapture
```

The soak shows that requests/query tracks per-query work (routing pages plus
fetched segments), not dataset size, and that the warm segment cache never
increases the request count. Every `SearchReport` and `AddReport` also carries
the same `requests` breakdown, so request rate is observable in production
without the soak harness.

## Evidence Map

Candidate evidence is not the same as a release decision. Each area below maps to
a checked-in artifact and the command that regenerates the evidence from the
exact build you are evaluating.

| Gate | Checked-in artifact | Fresh command evidence | Release decision |
|---|---|---|---|
| Correctness | Rust unit/integration tests under `crates/borsuk/tests/`, compaction tests in `crates/borsuk/src/index.rs`, and policy anchors in `scripts/check_repo_policy.py`. | `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, and `cargo test --locked --workspace --all-targets`. | Pass only if all commands exit 0 and no scoped-compaction, RAM-budget, routing, search, or object-store invariant is skipped. |
| Packages | Python package metadata/tests in `python/`, TypeScript package metadata/tests in `packages/borsuk/`, and publish workflow matrix in `.github/workflows/`. | Build a wheel with `uvx maturin build --locked`, test that wheel with `python -m unittest discover python/tests`, then run `npm ci`, `npm run build:native`, and `npm test` in `packages/borsuk`. | Pass only if the tested artifacts are native package artifacts, not source-tree imports or CLI shell-outs. |
| Storage | `docs/storage-format.md`, Arrow/Parquet readers and writers, `CURRENT` pointer code, package license files, and storage-format tests. | Full workspace tests plus package tests that read/write indexes through local files and S3-compatible object stores. | Pass only if every persistent index table except `CURRENT` remains binary Parquet and no JSON manifest/table path is introduced. |
| Performance | `docs/web/assets/benchmarks/*.csv`, `docs/benchmarks.md`, `crates/borsuk/examples/benchmark_report.rs`, and the ignored `large_scale` gate. | Regenerate benchmark artifacts with `benchmark_report`, run `cargo bench --locked -p borsuk`, run `performance_smoke`, and run `million_vector_local_search_scale_gate` with `BORSUK_LARGE_SCALE_OUTPUT`. | Pass only if high-recall modes stay at or above 0.95 tie-aware recall@10, routing-overfetch sweeps are published, termination reasons are visible, and query I/O/memory counters are published. |
| Memory | RAM-budget tests, `SearchReport`/`IndexStats` resident metadata counters, cache invalidation tests, and benchmark RSS artifacts. | Full workspace tests plus benchmark artifacts that include resident bytes, `rss_peak_delta`, cache hits/misses, and explicit query termination reasons. | Pass only if memory overflow fails explicitly and no query or compaction path silently skips active data to fit memory. |
| API | Rust public types, Python stubs and tests, TypeScript declarations and tests, CLI help/tests, and docs/api.md. | Rust, Python-wheel, npm-native, and CLI smoke tests from the release candidate. | Pass only if Rust, Python, and TypeScript expose typed metrics, leaf modes, ids, searches, vector lookup, compaction/rebuild, stats, and reports consistently. |
| Object store | S3-compatible example code, SeaweedFS and MinIO example stacks, `s3_compatible` and `s3_soak` tests, and docs. | `BORSUK_S3_TEST_URI=... cargo test --locked -p borsuk --test s3_compatible` and `--test s3_soak`, plus Python and TypeScript S3 examples against the same endpoint. | Pass only if the exact release candidate works against a real S3-compatible endpoint with the same Parquet object layout as local files, and the soak shows bounded requests/query with a warm-cache request-rate reduction. |

## What ships today

The repository contains CI, publish workflows, cross-language tests,
performance smoke tests, benchmark generation, checked-in benchmark CSV
artifacts, and interactive web charts. That evidence is necessary but is not a
blanket production-ready claim on its own.

A build is not production-ready until the checks above pass on that exact build
and its benchmark artifacts are published alongside it.
