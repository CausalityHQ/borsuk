# Production Readiness

BORSUK should not be called production-ready for a release candidate until every
gate below has passed on the candidate artifacts. Local smoke checks are useful,
but they are not enough to certify production use.

## 1. Correctness Gate

Run the full Rust workspace checks:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
```

The suite must cover:

- create/open compatibility for existing indexes;
- generated ids and explicit ids;
- duplicate-id rejection without full payload scans;
- `get_vector(id)` lookup through the segment bloom filters;
- exact search, id search, vector search, batch search, and report search;
- `flat-scan`, `sq-scan`, `pq-scan`, `graph`, `vamana-pq`, and `hybrid`;
- compaction from L0 to L1+ and dry-run/delete garbage collection;
- vector-local compaction that keeps strict-budget recall high after append
  ingest;
- scoped compaction that reads only selected source leaf payloads, never old
  graph blocks or unrelated leaves, and reuses unchanged routing page objects;
- scoped compaction from routing page metadata when resident segment summaries
  are empty, using page-level `level_mask` metadata to skip unrelated routing
  pages without reading unselected segment, graph, or routing page blobs;
- persisted leaf-level routing page indexes/content pages, approximate page
  drill-down through page centroid/radius metadata, page-level id blooms for
  non-resident `get_vector(id)`, a resident segment-summary vector empty
  open/search path, GC protection of active segment/graph objects through
  routing page metadata, plus computed multi-level routing pages and page-walk
  search before billion-scale certification;
- strict `ram_budget` enforcement with no silent segment skipping;
- local-file and S3-compatible object-store paths.

## 2. Package Gate

Build and test the Python package from its wheel, not from the source tree:

```bash
(cd python && uvx maturin build --locked --out dist)
wheel="$(ls -t python/dist/borsuk-*.whl | head -1)"
BORSUK_WHEEL_PATH="$wheel" uv run --with "./$wheel" python -m unittest discover python/tests
```

The release matrix must pass on Python 3.12, 3.13, and 3.14 across Linux,
Windows, macOS arm64, and macOS Intel.

Build and test the TypeScript package from the native N-API bridge:

```bash
(cd packages/borsuk && npm ci && npm run build:native && npm test)
```

The npm release matrix must pass on Node 22, 24, and 26 across Linux, Windows,
macOS arm64, and macOS Intel.

## 3. Storage Gate

Persistent index data must stay binary and efficient:

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

## 4. Performance Gate

Run the benchmark suite and publish fresh artifacts for the release candidate:

```bash
cargo bench --locked -p borsuk
cargo test --locked -p borsuk --test performance_smoke
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 100 \
  --parallelism 1,2,4,8 \
  --artifacts-dir /tmp/borsuk-bench
cargo test --locked --release -p borsuk --test large_scale \
  million_vector_local_search_scale_gate -- --ignored --nocapture
```

The benchmark report must include synthetic uniform, clustered, and adversarial
datasets plus at least one real dataset such as `sklearn-digits`. It must
compare exact search with every leaf mode and report recall, p50/p95 latency,
bytes read, graph bytes read, records scored, cache hits/misses, and
`resident_bytes_estimate`.

Benchmark artifacts must include dataset record count, dimensions, segment
size, query budgets, tie-aware recall, and strict id recall. Parallel graph
pressure must report worker count, QPS, p95 latency, `rss_peak_delta`, graph
bytes per query, and resident bytes for `graph`, `vamana-pq`, and `hybrid`.
The hosted web docs must render the sequential and parallel CSV files
interactively before a production-ready release is tagged.

## 5. Memory Gate

Memory failures must be explicit:

- create/open/add/compact fail if resident metadata exceeds `ram_budget`;
- queries report resident metadata through `SearchReport` and `IndexStats`;
- large parallel graph queries report RSS growth in benchmark artifacts;
- query budgets can stop additional I/O, but they must not hide active data or
  return partial results as if the full index had been searched.
- billion-scale releases must demonstrate that routing metadata is paged or
  hierarchical enough to stay inside the configured RAM budget without loading a
  flat summary row for every leaf.

## 6. API Gate

Rust, Python, and TypeScript must expose the same public model:

- typed metrics, search modes, and leaf modes;
- vectors with generated ids or caller-provided ids;
- compact arbitrary ids in the storage model, with typed string/number/binary
  convenience shapes in Python and TypeScript;
- no `payload_refs` public parameter;
- no `stringDistance` or string-specific search API;
- separate searches for ids and vectors;
- load-vector-by-id API;
- report APIs for tuning `segment_max_vectors`, `max_segments`,
  `max_candidates_per_segment`, `max_bytes`, and cache behavior;
- documented create-time versus query-time parameters.

## 7. Object Store Gate

S3-compatible smoke checks must pass against a real endpoint before release:

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

The same index layout must work on AWS S3, MinIO, SeaweedFS, and local files.

## Current Evidence

The repository contains CI, publish workflows, cross-language tests,
performance smoke tests, benchmark generation, checked-in benchmark CSV
artifacts, and interactive web charts. That evidence is necessary but not a
blanket production-ready claim.

A release is not production-ready unless the gates above pass on that exact
release candidate and the benchmark artifacts are published with the release.
