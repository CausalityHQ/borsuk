# Research Reproducibility

## Fetch standard datasets

```bash
python3 scripts/fetch_ann_dataset.py --output /tmp/borsuk-datasets
```

Each dataset directory must contain `train.f32`, `test.f32`, `neighbors.i32`,
and `meta.json`. Full-corpus runs use shipped ground truth. A limited corpus
forces the harness to recompute ground truth for the actual subset.

The market-comparison runner also reads VectorDBBench's official Parquet layout
directly; it does not create a second raw-vector copy. Plan or execute a source
recreation with:

```bash
python3 scripts/fetch_vdbbench_dataset.py \
  --dataset cohere-medium-1M \
  --output-root /data/borsuk-market

python3 scripts/fetch_vdbbench_dataset.py \
  --dataset cohere-medium-1M \
  --output-root /data/borsuk-market \
  --execute-download
```

If every source Parquet object finished downloading but local schema/hash
validation was interrupted (for example, the validation environment was
missing `pyarrow`), install `scripts/requirements-format-bench.txt` in an
isolated environment and resume without replacing source bytes:

```bash
python3 scripts/fetch_vdbbench_dataset.py \
  --dataset cohere-medium-1M \
  --output-root /data/borsuk-market \
  --validate-existing
```

Resume mode requires every exact source file to be non-empty, rejects partial
or unexpected Parquet files, reruns the full Arrow schema/row/ground-truth
contract, and only then writes `meta.json` and `dataset.json` with hashes.

Available aliases are `cohere-medium-1M` (1M/768D),
`cohere-large-10M` (10M/768D), and `laion-100M` (100M/768D). The fetcher
selects only unshuffled `train*.parquet`, `test.parquet`, and
`neighbors.parquet`, validates physical Arrow types and row counts, hashes
every downloaded object, and writes `dataset.json` plus `meta.json`.
Shuffled train files are rejected because positional generated ids would no
longer match the supplied ground-truth ids.

The terminal Causality preparation of `cohere-medium-1M` validated exactly
1,000,000 768-dimensional float32 train vectors, 1,000 test queries, and 1,000
shipped neighbours per query. Its frozen descriptor SHA-256 is
`54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` and
its aggregate source SHA-256 is
`c0c572f0265181a182ae904383f97d0e3137521eb52bd3c05d1a3935bab0273b`.
The descriptor, metadata, validation script, and exact dependency pins are
preserved below
`s3://borsuk-bench-453182569524-euc1/research/datasets/cohere-medium-1M/preparation/3040883/`.
Dataset descriptors are provenance artifacts, not benchmark measurements.

Run the local Parquet structural smoke for the durable-write campaign with an
environment containing the pinned NumPy and PyArrow versions:

```bash
BORSUK_BENCH_PYTHON=/path/to/pinned-venv/bin/python \
  BORSUK_REALISTIC_DURABLE_WRITE_SMOKE=1 \
  scripts/bench_realistic_durable_write.sh
```

Paid execution additionally requires the explicit execution flag, validated
dataset directory, fresh disjoint index/result prefixes, architecture and
instance identity, and a frozen source SHA-256. The runner rejects prefix
reuse and validates each terminal cell before advancing.

Build all market executables, create a checksum-gated plan, then execute paid
rows only after inspecting its `status`/`blocker` columns:

```bash
cargo build --locked --release -p borsuk \
  --example production_bench \
  --example hybrid_retrieval_bench \
  --example market_workload_bench

python3 scripts/market_benchmark_runner.py plan \
  --dataset-root /data/borsuk-market \
  --output-root /data/borsuk-market-results \
  --plan /data/borsuk-market-results/run-001-plan.csv \
  --run-id run-001 \
  --repetitions 3 \
  --bucket s3://bucket/publication/run-001

python3 scripts/market_benchmark_runner.py execute \
  --plan /data/borsuk-market-results/run-001-plan.csv \
  --repo-root . \
  --allow-paid-execution
```

## Bounded Rust test-binary build

The workspace test-binary gate uses a low-memory test profile (`debug = 0`,
`incremental = false`, `split-debuginfo = "off"`) and caps Cargo at two build
jobs:

```bash
BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
```

On 2026-07-26, the complete locked workspace/all-targets `--no-run` gate
finished successfully in 87 seconds on the local Apple Silicon development
host after the focused debug suites had populated dependencies. The effective
peak was bounded to two concurrent compiler jobs. This timing is a build-health
measurement, not a benchmark result; clean CI timing is recorded separately by
the same script.

## Frozen confirmatory publication protocol

The v2 manifest fixes five independent repetitions, 1,000 queries per
repetition, Fashion-MNIST as the only direct Amazon S3 Vectors comparison,
`srht-pq-scan` with `nprobe=8`, 320 candidates, and `k=10`, plus the six dense
and three BEIR datasets. BORSUK dense measurements are frozen to the two
claim-relevant approximate cache phases and explicitly skip full-corpus exact,
startup, concurrency, and unrelated cache-coverage phases. Each BEIR dataset
uses all seven retrieval modes at candidate depth 256, 64 segments, RRF
(`k=60` for fused modes), and 0/50/100% requested hot fractions. Validate it
and inspect the deterministic schedule before any paid action:

Every paid-prefix attempt, including failures before a usable comparison, is
recorded in the
[publication v2 attempt ledger](publication-v2-attempt-ledger.md).

```bash
python3 scripts/publication_protocol.py validate \
  docs/research/publication-v2-manifest.json
python3 scripts/publication_protocol.py schedule \
  docs/research/publication-v2-manifest.json \
  --output /tmp/borsuk-publication-v2-schedule.csv
```

Each schedule row owns a fresh result prefix, index prefix, cache key, query
seed, independently seeded dense/hybrid dataset orders, and process. The
Fashion BORSUK and S3 Vectors measurements are adjacent, and their order
alternates across repetitions; unrelated dense datasets run only after that
direct pair. S3 Vectors writes one raw row per request to
`query_samples.csv`; BORSUK writes the matching `query_seed` and
`repetition_id` into `bench_query_samples.csv`. Both implementations use the
same checked SplitMix64/Fisher–Yates permutation and persist
`query_source_index`; the analyzer rejects a pair when either its position or
source index differs. Results are synced before cache/scratch cleanup, and the
full-corpus runner removes each completed dataset's disposable trees before
starting the next dataset.

Before measuring, the paid runner validates or prepares the three BEIR inputs
with `sentence-transformers==3.4.1`,
`BAAI/bge-small-en-v1.5` at commit
`5c38ec7c405ec4b44b94cc5a9bb96e735b38267a`, and the frozen retrieval query
prefix. It validates every artifact checksum and copies the source and prepared
dataset manifests into the result evidence. Preparation publishes through a
run-specific temporary directory, so an interrupted encoder cannot masquerade
as a reusable complete dataset.

The direct claim decision is paired and fixed in advance:

```bash
python3 scripts/analyze_publication_claims.py \
  --borsuk direct-borsuk.csv \
  --s3-vectors direct-s3-vectors.csv \
  --expected-repetitions 5 \
  --expected-queries 1000 \
  --output direct-claim.csv
```

The hierarchical bootstrap resamples repetitions, then paired query positions
inside each selected repetition. The analyzer refuses any cohort other than
the frozen 5 × 1,000 paired requests and reports both p95 and p99
latency-ratio intervals. “Lower latency at matched recall” is allowed only when
the primary 95% p95-latency-ratio interval is wholly below 1.0 and the
recall-difference interval has a non-negative lower bound. Otherwise the
machine-readable decision is `no-superiority-claim`; p99 remains a reported
secondary outcome.

After downloading the completed result prefix, audit the entire evidence tree
before using any number:

```bash
python3 scripts/validate_publication_v2_results.py \
  /path/to/publication-v2-results \
  --expected-source-sha256 "$BORSUK_SOURCE_SHA256"
```

The auditor reconstructs the schedule from the frozen manifest, verifies the
source archive, proves its embedded manifest is byte-identical to the published
manifest, and requires the exact `c7g.8xlarge`/32-vCPU/gp3/S3-Standard client
identity. It verifies the pinned hybrid dependencies and prepared-input
manifests, then requires every nonempty dense, S3 Vectors, and hybrid artifact.
For each repetition the hybrid proof is the complete frozen matrix: 21 query
cells plus one build row per BEIR dataset, or 66 rows across the three
datasets, with no duplicate or missing cell. It also requires completion
markers, checks every paired direct-query position and source identity, and
independently recomputes both claim CSVs. A failed, partial, drifted,
selectively copied, or coverage-only tree is rejected. Set
`BORSUK_SOURCE_SHA256` to the exact digest printed by the paid launcher.

The runner sets both `BORSUK_BENCH_QUERIES=1000` and
`BORSUK_BENCH_UNCACHED_QUERIES=1000`; the latter is explicit because the
interactive benchmark default otherwise caps the cold phase at 100 requests.

External numeric context is separately validated:

```bash
python3 scripts/validate_reported_comparisons.py
```

[`reported-comparisons.csv`](reported-comparisons.csv) accepts only first-party
commercial documentation and primary papers. Every current row is
`context-only`; reported values are never merged with the paired direct series.

The paid launcher is:

```bash
AWS_PROFILE=causality bash scripts/launch_aws_publication_v2.sh
```

It content-addresses the exact source and manifest, verifies the AWS account,
records the instance, RAM, accelerator, and cache-disk classes, and refuses to
start while any `borsuk-*` tmux campaign is active. BORSUK and S3 Vectors use
the same measured client instance. Amazon's managed service compute is
undisclosed, so the permitted hardware wording is exactly “same measured
client; managed service compute undisclosed,” never an unsupported
weaker-hardware claim. The remote
runner requires both `BORSUK_PUBLICATION_V2_EXECUTE=1` and
`BORSUK_RUN_PUBLICATION_V2=1`, syncs after every repetition, and never invokes
the unrelated external-control matrix.

Filter `dataset.json` additionally fixes `seed`, `tenants`,
`records_per_tenant`, `queries`, `segment_max_vectors`, and physical vector
type. Namespace descriptors fix `namespace_sizes`. Late-interaction
descriptors point to normalized `documents.parquet` and `queries.parquet` and
fix the `candidates_per_query_token` sweep. Their exact Arrow schemas and
reported metrics are specified in
[the market benchmark matrix](market-benchmark-matrix.md#executable-workload-adapters).

## Full-corpus production pq-scan matrix

```bash
BORSUK_S3_BUCKET=s3://bucket/prefix \
AWS_REGION=eu-central-1 \
DATASETS=/tmp/borsuk-datasets \
OUT=/tmp/borsuk-full \
  scripts/bench_s3_full.sh
```

Deep-Image is opt-in with `RUN_DEEP_IMAGE=1`. This command performs paid S3
writes and reads.

## Internal-method × six-dataset matrix

Dry-run enumeration, which creates only `coverage.csv`:

```bash
DATASETS=/tmp/borsuk-datasets \
OUT=/tmp/borsuk-method-matrix \
BORSUK_S3_BUCKET=s3://bucket/research \
  scripts/bench_standard_method_matrix.sh
```

Paid execution is deliberately double-gated:

```bash
BORSUK_MATRIX_EXECUTE=1 \
BORSUK_RUN_STANDARD_MATRIX=1 \
DATASETS=/tmp/borsuk-datasets \
OUT=/tmp/borsuk-method-matrix \
BORSUK_S3_BUCKET=s3://bucket/research \
  scripts/bench_standard_method_matrix.sh
```

The bounded grid is:

- methods: exact, flat-scan, SQ-scan, pq-scan, srht-pq-scan,
  fast-turboquant-mse-scan, fast-turboquant-scan, graph, and Vamana-PQ;
- candidates: 16, 32, 64, 128;
- dataset-specific `nprobe` frontiers recorded in `coverage.csv`;
- dimension-aware default cell rows;
- query cap 4 and global decode cap 24;
- startup, uncached, disk-cached, and memory-preloaded states; and
- CPU/RAM/disk/cache resource samples for each executed method.

The repository retains the dry-run coverage contract at
[`standard-method-matrix/coverage.csv`](../web/assets/benchmarks/standard-method-matrix/coverage.csv).
Rows remain `planned` until a result directory contains the method CSV and its
resource timeline; documentation never converts planned cells into measurements.

## Direct harness invocation

```bash
BORSUK_BENCH_DATASET=/tmp/borsuk-datasets/sift-128 \
BORSUK_BENCH_URI=s3://bucket/sift-128 \
BORSUK_BENCH_OUTPUT_DIR=/tmp/sift-results \
BORSUK_BENCH_RECALL_LEAF_MODE=pq-scan \
BORSUK_BENCH_NPROBES=1,2,4,8,16,32,64 \
BORSUK_BENCH_CANDIDATES=16,32,64,128 \
BORSUK_BENCH_RAM_BUDGET_BYTES=536870912 \
BORSUK_BENCH_SEGMENT_CACHE_MAX_BYTES=0 \
BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
  cargo run --locked --release -p borsuk --example production_bench
```

`BORSUK_BENCH_PRELOAD_SERVING=1` requests the separate memory-preloaded state.
Archive the resulting warm report and use that label only when
`coverage_complete=true`; otherwise the run is a bounded mixed-cache profile.
Cache-state production runs set the decoded-segment budget to zero so
`disk_cached` measures the local disk layer, not retained decoded RAM.
The library and production harness default the resident-memory budget to
512 MiB; set it explicitly in archived commands so the evidence remains
self-describing.
Setting either concurrency cap to zero is an explicitly uncapped research run.

For bounded decoded-cache runs, the harness writes one row per query to
`bench_cache_coverage.csv`, including requested hot-set membership and observed
decoded-RAM/disk/backing access fractions. Render every such artifact with:

```bash
python3 scripts/render_cache_coverage_charts.py \
  --experiment-root /tmp/borsuk-results \
  --output-dir /tmp/borsuk-results/charts
```

Every repetition creates a different empty URI and recreates the complete index
from the original corpus before measuring it. Cache/traffic profiles within the
same repetition query that immutable build read-only through independent cache
directories; they do not pay duplicate ingest and cannot mutate the artifact.
This captures construction variance without conflating it with cache-state
variance. Any change to library code, benchmark code, artifact schema, codec,
layout, cache policy, or resource sampler invalidates the complete result
matrix and requires another source recreation under a new run identifier.

## Exact-vector physical-format A/B

Install the benchmark-only dependencies into an isolated environment:

```bash
python3 -m pip install -r scripts/requirements-format-bench.txt
```

Run the identical-row local-disk matrix under the resource sampler:

```bash
python3 scripts/benchmark_with_resources.py \
  --output /tmp/vector-formats/resources.csv \
  --scratch-dir /tmp/vector-formats/scratch \
  -- python3 scripts/benchmark_vector_formats.py \
    --output-dir /tmp/vector-formats/results \
    --rows 1000000 \
    --dimensions 768 \
    --element-type float32 \
    --batch-rows 8192 \
    --selected-rows 10,100,1000 \
    --patterns clustered,scattered \
    --repetitions 30
```

Repeat from a fresh output directory for `float16`, `bfloat16`, `int8`, and
`binary`. `build.csv`, `open.csv`, raw `samples.csv`, distribution
`summary.csv`, strict `status.csv`, and `resources.csv` are one evidence unit.
An unsupported physical type is a blocked cell; the harness never silently
changes its representation. The `uncached` label is valid only when the
orchestrator evicts the host page cache or uses a fresh host/object-store path;
the script intentionally does not pretend that deleting an application cache
also clears the kernel page cache.

## Staged AWS format decision

The full product/publication matrix must not run until the durable-table format
decision is closed. Launch the format-only stage from an authenticated
workstation:

The product replay uses the same production benchmark binary for both
normal-segment formats:

```bash
BORSUK_SEGMENT_TABLE_FORMAT=parquet cargo run --release -p borsuk \
  --example production_bench
BORSUK_SEGMENT_TABLE_FORMAT=vortex cargo run --release -p borsuk \
  --example production_bench
```

Each row must use a new `BORSUK_BENCH_URI`, cache directory, and output
directory. The selector is persisted in `BuildConfig`; it is not a read-time
toggle, and changing it requires rebuilding/reingesting the index. Only
`segments/**/seg-*` changes container. Arrow IPC exact-vector and global ANN
sidecars plus WAL, routing, lexical, graph, and control tables remain identical
in scope. Vortex runs use its default layout. Parquet stays the product default
until corrected real-artifact local/NVMe/S3 distributions and CPU/RAM/disk/I/O
evidence justify a promotion.

```bash
AWS_PROFILE=causality \
  bash scripts/launch_aws_format_qualification.sh
```

The launcher verifies AWS account `453182569524`, packages the exact tracked
and untracked source state required to build the workspace, content-addresses
the archive by SHA-256, starts the dedicated `c7g.8xlarge`, and uses SSM to
start a detached remote `tmux` session. Turning off the workstation does not
stop the run.

The remote
[`bench_format_qualification_aws.sh`](../../scripts/bench_format_qualification_aws.sh)
executes only:

- Parquet versus Vortex-default/compact table workloads on EC2 local disk and
  native S3;
- Arrow IPC versus Vortex-default/compact ANN candidate-take workloads at
  128-d and 960-d on EC2 local disk and native S3;
- a no-coercion 15-type compatibility matrix covering the Arrow table shapes
  used by BORSUK, including `FixedSizeBinary`, fixed-size lists, variable
  lists, UTF-8, Binary, nullable primitives, and booleans;
- a no-coercion typed-vector compatibility matrix for f32, f16, physical bf16,
  i8, and fixed-size binary across Arrow IPC and both Vortex layouts.

Every performance case owns a fresh output directory and S3 prefix, runs under
the process-tree resource sampler, validates raw samples against its
distribution summary, renders CPU/RAM/disk/network charts, and syncs artifacts
to the result prefix. Native-S3 Vortex tests disable its segment cache; local
local-disk tests retain the normal disk-cached profile. The artifact must record
whether that disk is EBS, instance-store NVMe, or another class; those labels
are not interchangeable.

The stage-one artifact is `FORMAT_DECISION_REQUIRED`; it deliberately contains
no product benchmark. The first 24 July 2026 Vortex latency run was invalidated
because it did not materialize Vortex values to the same Arrow boundary used by
Parquet. A corrected run must report `materialized-arrow` and
`compressed-native` as distinct execution modes; the latter is valid only when
it performs the real downstream computation. No format decision is frozen.
The evidence and audit trail are in
[Parquet versus Vortex](table-format-ab.md) and
[ANN vector-buffer format A/B](vector-format-ab.md).

After that decision is frozen and all local gates pass, launch the first fresh
product phase with one command:

```bash
AWS_PROFILE=causality \
  bash scripts/launch_aws_publication_benchmarks.sh
```

This separate launcher content-addresses the exact Rust/scripts source,
verifies AWS account `453182569524`, starts the fixed `c7g.8xlarge`, validates
all six standard dense source directories, and runs
[`bench_publication_aws.sh`](../../scripts/bench_publication_aws.sh) in detached
`tmux`. The remote runner rebuilds all indexes under a new S3 prefix, measures
Fashion-MNIST, GloVe, SIFT, NYTimes, GIST, and Deep-Image, records the compiler,
hardware, selected physical formats, CPU/RAM/disk/network distributions, and
syncs evidence on both success and failure. It excludes local cache and scratch
payloads from result publication and shuts the worker down after the campaign.
Only `DENSE_DEFAULT_COMPLETE` marks a valid completed phase; a directory with
`status=failed` is diagnostic evidence, not benchmark data.

## Dense, sparse, and text matrix

Fetch and prepare a BEIR dataset with a checksum-pinned encoder:

```bash
python3 scripts/fetch_beir_dataset.py \
  --dataset scifact \
  --output /tmp/beir/scifact

python3 scripts/prepare_hybrid_dataset.py \
  --source /tmp/beir/scifact \
  --output /tmp/borsuk-hybrid/scifact \
  --dataset scifact \
  --split test \
  --dense-backend sentence-transformers \
  --dense-model BAAI/bge-small-en-v1.5 \
  --dense-revision 5c38ec7c405ec4b44b94cc5a9bb96e735b38267a \
  --dense-query-prefix "Represent this sentence for searching relevant passages: " \
  --publication

python3 scripts/validate_hybrid_dataset.py /tmp/borsuk-hybrid/scifact
```

The full paid matrix is double-gated and creates one new S3 prefix:

```bash
BORSUK_HYBRID_DATASETS_ROOT=/tmp/borsuk-hybrid \
BORSUK_HYBRID_RUN_ID=hybrid-source-recreation-r1 \
BORSUK_HYBRID_MATRIX_EXECUTE=1 \
BORSUK_RUN_HYBRID_MATRIX=1 \
BORSUK_S3_BUCKET=s3://bucket/research \
  scripts/bench_hybrid_retrieval_matrix.sh
```

The default grid contains dense, sparse, text, dense+sparse, dense+text,
sparse+text, and dense+sparse+text. It measures four scan codecs, three
candidate/probe points, five requested cache fractions, and repeated queries.
Each build and measured-query directory contains its own `resources.csv`.

Render nDCG@10, Recall@10, Precision@10, MRR@10, latency-distribution, and
observed cache-tier charts with:

```bash
python3 scripts/render_hybrid_retrieval_charts.py \
  --experiment-root /tmp/borsuk-hybrid-results \
  --output-dir /tmp/borsuk-hybrid-results/charts
```

The chart inputs retain latency mean, sample standard deviation, p50, p95, p99,
maximum, and raw per-query rows. Requested hot fraction is never substituted
for observed disk/backing byte coverage.

## Fashion graph optimization curve

The command below creates a graph-enabled full-corpus Fashion index from the
source vectors. Cached graph objects are prepared outside the measured cached
query phase; cells without a valid local graph use the configured storage scan.

```bash
BORSUK_BENCH_DATASET=/tmp/borsuk-datasets/fashion-mnist-784 \
BORSUK_BENCH_URI=s3://bucket/graph-enabled-fashion \
BORSUK_BENCH_CACHE=/tmp/borsuk-graph-cache \
BORSUK_BENCH_OUTPUT_DIR=/tmp/borsuk-graph-curve \
BORSUK_BENCH_LIMIT=0 \
BORSUK_BENCH_QUERIES=100 \
BORSUK_BENCH_NPROBES=32 \
BORSUK_BENCH_CANDIDATES=256,512,1024,1536,2048,2304,2560,2816,3000,3072,4096 \
BORSUK_BENCH_RECALL_LEAF_MODE=graph \
BORSUK_BENCH_RECALL_ONLY=1 \
BORSUK_BENCH_SKIP_EXACT_RECALL=1 \
BORSUK_BENCH_READ_ONLY=1 \
BORSUK_BENCH_MAX_CONCURRENT_SEARCHES=4 \
BORSUK_BENCH_MAX_CONCURRENT_CELL_DECODES=24 \
  python3 scripts/benchmark_with_resources.py \
    --output /tmp/borsuk-graph-curve/resources.csv \
    --cache-dir /tmp/borsuk-graph-cache \
    -- target/release/examples/production_bench
```

Repeat every selected point from source under a distinct object prefix and
fresh process/cache directory. The checked-in campaign keeps all repetitions
rather than selecting the fastest.

## Production write-to-serve lifecycle

Run the post-publication SIFT-1M lifecycle gate from a fresh object prefix:

```bash
BORSUK_LIFECYCLE_EXECUTE=1 \
BORSUK_RUN_PRODUCTION_LIFECYCLE=1 \
BORSUK_LIFECYCLE_RUN_ID=production-lifecycle-v16-r1 \
BORSUK_LIFECYCLE_DATASET=/home/ec2-user/borsuk-datasets/sift-128 \
BORSUK_LIFECYCLE_OUT=/tmp/production-lifecycle-v16-r1 \
BORSUK_SOURCE_SHA256="$BORSUK_SOURCE_SHA256" \
BORSUK_SOURCE_ARCHIVE=/path/to/frozen-source.tar.gz \
BORSUK_S3_BUCKET=s3://bucket/research \
BORSUK_LIFECYCLE_RESULT_URI=s3://bucket/evidence/production-lifecycle-v16-r1 \
  scripts/bench_production_lifecycle_aws.sh
```

The fixed gate uses the automatic all-Parquet production layout, float32 input,
`srht-pq-scan`, 100 queries, seed `20260729`, `nprobe=8`, 320 candidates, and
the same 512 MiB decoded-memory budget as publication. It builds from source,
then measures WAL publication and immediate visibility for a 5% insert cohort,
delta flush, global consolidation, upsert, delete, compaction, purge, and query
behavior at every mutation boundary. Exact brute-force recall is skipped
because this gate qualifies the production lifecycle, while approximate recall
is still measured against the dataset ground truth. The runner refuses both an
existing local result directory and a nonempty S3 index prefix, and it emits
`PRODUCTION_LIFECYCLE_COMPLETE` only after all raw distributions and lifecycle
artifacts pass validation.
`protocol.txt` binds both the frozen source-archive digest and the lifecycle
runner digest, so orchestration hardening performed after the source freeze
cannot be confused with a core-library change. The detached runner synchronizes
validated results to the fresh result URI; on any error it removes the
completion marker, writes `PRODUCTION_LIFECYCLE_FAILED`, and syncs the partial
evidence.

## 100M packed-code range qualification

The 100M qualification also rebuilds from the raw corpus:

```bash
BORSUK_100M_EXECUTE=1 \
BORSUK_RUN_100M_QUALIFICATION=1 \
BORSUK_S3_BUCKET=s3://bucket/research \
BORSUK_100M_RUN_ID=100m-source-recreation-r1 \
BORSUK_100M_DATASET=/data/synthetic-clustered-100m-96 \
BORSUK_100M_OUT=/tmp/borsuk-100m-code-ranges \
BORSUK_SOURCE_SHA256="$BORSUK_SOURCE_SHA256" \
BORSUK_SOURCE_ARCHIVE=/path/to/frozen-source.tar.gz \
BORSUK_100M_RESULT_URI=s3://bucket/evidence/100m-source-recreation-r1 \
  scripts/bench_100m_code_ranges.sh
```

The script assigns a new index URI below the supplied bucket. The default
campaign runs 100 uncached and 100 disk-cached queries at probes
`4,8,12,16,24,32,48,64` and candidate budgets `100,200`. It also records the
whole-process CPU/RAM/local-cache/scratch timeline and renders both resource and
recall/latency charts. Override `BORSUK_100M_PROBES`,
`BORSUK_100M_CANDIDATES`, or `BORSUK_100M_QUERIES` only for a separately named
experiment.
The runner refuses an existing local output directory or nonempty object
prefix, records the exact grid and physical formats in `protocol.txt`, and
writes `QUALIFICATION_100M_COMPLETE` only after raw artifacts and charts pass
validation. The protocol separately records the frozen core source and runner
digests. Failure similarly publishes `QUALIFICATION_100M_FAILED` with the
partial evidence and never leaves a completion marker.

## Resource telemetry

```bash
python3 scripts/benchmark_with_resources.py \
  --output /tmp/results/resources.csv \
  --cache-dir /tmp/results/cache \
  -- cargo run --locked --release -p borsuk --example production_bench
```

Resource CSVs contain:

```text
elapsed_ms,cpu_percent,rss_bytes,vms_bytes,
process_read_bytes,process_write_bytes,cache_disk_bytes
```

On Linux, zero physical process-read bytes can mean the filesystem page cache
served the disk-cached phase. Zero backing `GET` requests is the cache-state
authority; `SearchReport.bytes_read` is logical payload I/O and can remain
nonzero for full-cell reads served locally.

## Chart generation

```bash
python3 scripts/render_recall_latency_charts.py \
  --input docs/web/assets/benchmarks/aws-recall-latency-2026-07-20.csv \
  --output-dir docs/web/assets/benchmarks/recall-latency

python3 scripts/render_resource_charts.py \
  --experiment-root docs/web/assets/benchmarks/raw/2026-07-20 \
  --output-dir docs/web/assets/benchmarks/resources \
  --prefix research

python3 scripts/render_recall_latency_charts.py \
  --input docs/web/assets/benchmarks/aws-graph-recall-latency-2026-07-21.csv \
  --output-dir docs/web/assets/charts/graph-optimization \
  --subtitle 'memory_preloaded · optimized segment-local graph · exact rerank'
```

## Validation

```bash
python3 scripts/validate_research_docs.py

PYTHONPATH=scripts python3 -m unittest \
  scripts/test_benchmark_with_resources.py \
  scripts/test_render_resource_charts.py \
  scripts/test_render_recall_latency_charts.py \
  scripts/test_bench_s3_full.py \
  scripts/test_validate_research_docs.py \
  scripts/test_bench_standard_method_matrix.py
```

The validator requires all six standard datasets, all seven controlled methods,
resource schemas and graphs, working local research links, and a clean boundary
between default and research documentation.

## Artifact retention

- consolidated CSVs: `docs/web/assets/benchmarks/*.csv`;
- raw AWS experiments: `docs/web/assets/benchmarks/raw/2026-07-20/`;
- graph profiles and optimization: `docs/web/assets/benchmarks/raw/2026-07-21/`;
- recall curves: `docs/web/assets/benchmarks/recall-latency/`;
- resource timelines: `docs/web/assets/benchmarks/resources/`;
- experiment and publication interpretation: `docs/research/`.

Never overwrite a historical artifact to make it match a new default. Add a
dated artifact, label the old configuration, and update the coverage matrix.
