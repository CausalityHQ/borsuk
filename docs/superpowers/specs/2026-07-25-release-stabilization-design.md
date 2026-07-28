# BORSUK Release Stabilization Design

## Objective

BORSUK will not run a confirmatory publication benchmark until its supported
data types and retrieval kinds pass one common end-to-end lifecycle contract.
Because no public release exists, this stabilization may break persisted
formats and public APIs when doing so removes ambiguity or duplicated paths.

The release gate covers:

- dense vectors;
- sparse vectors;
- BM25 text;
- late-interaction token matrices;
- hybrid fusion across compatible retrieval legs;
- immutable object-store publication, WAL visibility, refresh, compaction,
  reopen, and garbage collection;
- Rust, CLI, Python, and Node entry points;
- bounded-memory and corruption behavior.

## Scalar type contract

The supported physical scalar types are:

| Type | Stable name | Durable Arrow physical type | Compute type |
|---|---|---|---|
| IEEE binary32 | `float32` | `Float32` | `f32` |
| IEEE binary16 | `float16` | `Float16` | `f32` |
| Brain float16 | `bfloat16` | `UInt16` plus extension metadata | `f32` |
| FP8 E4M3 finite | `float8-e4m3fn` | `UInt8` plus extension metadata | `f32` |
| FP8 E5M2 | `float8-e5m2` | `UInt8` plus extension metadata | `f32` |
| Signed integer | `int8` | `Int8` | `f32` or widened integer block |
| Packed bits | `binary` | `FixedSizeBinary` | packed machine words |

FP8 names refer to explicit formats, never a generic `fp8` whose exponent and
mantissa layout is unknown. `fp8` may be accepted as an input alias for
`float8-e4m3fn`, but manifests always persist the explicit stable name.

Ingest canonicalizes a vector exactly once to its declared physical type.
Routing, training, indexing, exact storage, exact reranking, and brute-force
controls observe that same canonical value. Queries are canonicalized to the
field type before scoring so measured recall is not inflated by comparing
stored low-precision values with unrounded query semantics.

E4M3FN and E5M2 conversion uses round-to-nearest, ties-to-even. NaN and
infinite input are rejected for every vector type because non-finite values do
not have well-defined distance semantics. Finite overflow saturates to the
largest finite representable magnitude: 448 for E4M3FN and 57,344 for E5M2.
Signed zero is preserved in the physical byte but compares as zero.

## Type and metric compatibility

Numeric dense types support squared Euclidean, Euclidean, cosine, angular, and
inner product. Packed binary supports Hamming and Jaccard only. Sparse fields
support float32 and float16 values with inner product/cosine semantics.
Late-interaction fields support float32 and float16 token matrices with MaxSim.
FP8 sparse postings and FP8 late-interaction matrices are excluded from the
first release gate because they require separate accuracy and accumulation
semantics; attempts to declare them fail at schema validation.

Metric/type incompatibility fails at index creation, not at the first query.

## One lifecycle for every field

Every supported matrix cell must prove:

1. create an index with an explicit schema;
2. add records with IDs, metadata, and the field payload;
3. search before flush;
4. reopen from another reader and observe only the published snapshot;
5. refresh and observe the new generation;
6. upsert and delete;
7. flush/materialize immutable segments;
8. compact;
9. reopen with an empty local cache;
10. search with exact and bounded approximate modes as applicable;
11. validate generations, IDs, distances/scores, and persisted physical type;
12. run garbage collection without deleting active objects.

The matrix is generated from a declarative case table so adding a type or kind
without lifecycle coverage fails repository policy.

## Retrieval-kind proof matrix

The core matrix contains:

- primary dense field for every dense scalar type;
- named dense field for every dense scalar type;
- primary sparse compatibility plus named sparse float32/float16;
- BM25-only and dense+BM25;
- sparse+BM25;
- dense+sparse;
- dense+sparse+BM25;
- late-interaction float32/float16;
- late interaction coexisting with ordinary dense/text fields without eager
  token-sidecar reads;
- binary dense Hamming/Jaccard;
- non-UTF8 and integer record IDs;
- local filesystem, in-memory object store, and S3-compatible storage
  contracts.

Python, Node, and CLI use smaller smoke matrices that exercise every public
type spelling and every retrieval kind. The complete lifecycle remains in the
Rust core to avoid multiplying expensive integration binaries.

## Storage contract

Parquet remains the durable record/WAL table because it is standard,
inspectable, and already qualified. Exact fixed-width vectors use bounded
standard Arrow IPC record batches for range-addressed reranking. Variable token
matrices use Arrow `List<FixedSizeList<T>>`. Sparse posting shards remain
immutable, checksum-addressed sidecars.

The stabilization removes any private or mislabeled vector container still
reachable from production code. Physical type metadata is checked against the
manifest on every open. A field cannot silently decode through float32 when its
manifest declares a smaller physical type.

FP8 Arrow arrays use `UInt8` values with schema metadata:

```text
ARROW:extension:name = borsuk.float8-e4m3fn
ARROW:extension:metadata = {"version":1}
```

and equivalently for E5M2. This is deliberately explicit because the Arrow
version in use has no portable FP8 primitive shared by all bindings.

Row-group and Arrow record-batch sizes remain bounded by target bytes, not a
fixed row count alone. Object-store reads deduplicate ranges and single-flight
immutable checksums.

## SIMD and hot-path policy

SIMD work follows correctness, not the other way around. Each optimized kernel
has an independent scalar reference and randomized bulk/tail equivalence tests.

Required optimized paths are:

- float32 dense dot product, norms, Euclidean, cosine, and angular;
- float16/bfloat16/FP8 decode in bounded blocks followed by vectorized f32
  accumulation;
- int8 widening dot and squared-difference blocks;
- binary XOR/AND/OR plus population count;
- sparse posting/block accumulation;
- blocked late-interaction MaxSim;
- lexical posting score accumulation where contiguous postings permit it.

Architecture-specific intrinsics are permitted behind runtime or compile-time
dispatch. The portable `wide` implementation remains the fallback. A kernel is
promoted only if it matches the scalar reference and improves a release-mode
microbenchmark on at least one supported architecture without regressing the
other.

## Test-build architecture

The current repository creates many large integration-test binaries, each
linking Arrow, Parquet, Vortex, object-store, TLS, and the whole BORSUK crate
with full debug information. This makes `cargo test --all-targets` a build
stress test rather than a useful correctness gate.

Stabilization will:

- disable full debuginfo and incremental compilation in the test profile;
- cap local/CI build jobs for the large workspace gate;
- consolidate new matrix coverage into one `feature_matrix` integration
  binary;
- keep narrow existing binaries where process isolation is meaningful;
- add a timed `--no-run` smoke gate that detects future test-binary explosion;
- retain release-profile performance tests separately.

No test is removed merely to make compilation faster.

## Release gates

Benchmarking is unblocked only when all of these pass from a clean test target:

1. format/schema unit tests;
2. generated Rust lifecycle matrix;
3. mutation, snapshot, compaction, GC, corruption, and fault-injection suites;
4. S3-compatible contract tests;
5. Python, Node, and CLI smoke matrices;
6. scalar/SIMD equivalence and release microbench thresholds;
7. bounded full workspace `--all-targets` build and test;
8. formatting, Clippy, documentation, and repository-policy checks;
9. a release-readiness manifest showing every matrix cell green.

Only after these gates pass will the publication methodology v2 manifest be
frozen and the paid AWS confirmatory campaign launched.
