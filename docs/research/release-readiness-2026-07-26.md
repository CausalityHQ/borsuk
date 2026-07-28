# Stabilization and release-readiness matrix — 2026-07-26

Status: the local implementation and compatibility gates are green. The
normal-segment and independently reproduced 220-case v5 WAL storage-layout
qualifications rejected Vortex, and the all-Parquet automatic production
default is frozen. Confirmatory publication execution is the next evidence
gate.

## Feature lifecycle matrix

Every positive cell below covers WAL visibility, declared-type query
canonicalization, exact and approximate ordering where applicable, flush,
reopen, upsert, delete, compaction, safe garbage collection, and final reopen.

| Retrieval field | Declared physical types | Result |
|---|---|---|
| Primary dense | float32, float16, bfloat16, E4M3FN, E5M2, int8 | green |
| Primary packed binary | binary with Hamming and Jaccard | green |
| Named dense | float32, float16, bfloat16, E4M3FN, E5M2, int8, binary | green |
| Primary sparse input | float32, float16 | green |
| Named sparse | float32, float16 | green |
| BM25 text | enabled and disabled validation | green |
| Hybrid signal sets | dense, sparse, BM25, and all non-empty multi-signal combinations | green |
| Late interaction | float32, float16 | green |
| Record IDs | UTF-8 and opaque non-UTF8 bytes | green |
| Unsupported scalar/kind pairs | sparse and late-interaction rejection cases | green |

Evidence command:

```bash
cargo test -p borsuk --test feature_matrix -j2
```

Result: 9 passed, 0 failed.

FP8 has an additional format-specific lifecycle suite covering both formats,
finite overflow saturation, non-finite rejection before WAL publication,
physical one-byte storage, exact/approximate search, mutations, compaction, and
reopen:

```bash
cargo test -p borsuk --test fp8_vectors -j2
```

Result: 3 passed, 0 failed.

## SIMD and low-precision matrix

| Kernel family | Correctness evidence | Result |
|---|---|---|
| Dense dot, norm, Euclidean, binary masks | scalar comparison over bulk and tail dimensions | green |
| Float16, bfloat16, int8 decode | exact scalar comparison over block boundaries and tails | green |
| E4M3FN and E5M2 decode | exhaustive byte-table stability plus bulk/tail comparison | green |
| Sparse and lexical scoring | scalar comparisons with repeated rows and signed values | green |
| Late-interaction MaxSim | scalar comparison at 7/8/9, 63/64/65, and 127/128 dimensions | green |
| Routing, PQ, TurboQuant and FWHT | deterministic scalar comparisons at production widths | green |

The targeted SIMD suite reported 15 passed and 0 failed. Release
microbenchmarks recorded a 1.23x float16 conversion improvement and 5.21x FP8
blocked-decode improvement on the local Apple Silicon host. These kernel
figures are diagnostic and are not used as product-comparison numbers.

## Complete local gates

| Gate | Command | Result |
|---|---|---|
| Formatting | `cargo fmt --all -- --check` | green |
| Rust lint | `cargo clippy --locked --workspace --all-targets -j2 -- -D warnings` | green |
| Locked dependency vulnerabilities | `cargo audit` | 0 vulnerabilities; one informational transitive maintenance notice |
| Rust all-target tests | `cargo test --locked --workspace --all-targets -j2` | green |
| Rust test-binary build | `BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh` | green |
| CLI end-to-end | included in Rust all-target run | 29 passed |
| Node native/API/package | `npm run build:native && npm test` | 105 passed, 2 configured-S3 skips |
| Python clean CPython 3.14 wheel/API/package | `BORSUK_WHEEL_PATH=... python -m unittest discover python/tests` | 147 passed, 3 optional integration skips |
| Script, protocol, and artifact contracts | `python -m unittest discover -s scripts -p 'test_*.py'` | 272 passed |
| Python public typing surface | `pyright tests/typing_usage.py` | 0 errors, 0 warnings |
| Web docs and example-source synchronization | `node scripts/test_docs_web.mjs && node scripts/sync_docs_examples.mjs --check` | green |
| Diff hygiene | `git diff --check` | green |

The all-target Rust run includes crash recovery, fault injection, consistency,
WAL, compaction/GC, Parquet/Vortex format, sparse, text, hybrid,
late-interaction, storage, package, examples, CLI, and benchmark-binary test
targets. Explicit heavy scale/soak tests and paid S3 scenarios remain opt-in;
they belong to the benchmark campaign rather than the local correctness gate.
The core library reported 359 tests: 354 passed and 5 explicit research or
microbenchmark tests ignored. The bench profile also built every workspace
benchmark target successfully.

The final WAL stabilization pass replaced JSON or unchecked-text lane heads,
frontier nodes, transaction descriptors, commit markers, mutation/tombstone
metadata, ID-directory deltas, and coordination counters with checked packed
binary codecs. It also removed the collection-wide generated-ID CAS from
ordinary explicit-ID appends and made partial multi-ID claim rollback
version-safe. Their focused unit, 19-test cell-WAL integration, 21-test WAL,
crash-recovery, upsert, all-target, and clippy coverage all pass.

The corrected full all-target gate also found that adversarial Parquet bytes
could make the upstream Arrow IPC schema converter panic while opening a
routing-page index. BORSUK now contains that complete untrusted decode boundary
and converts the unwind to `InvalidStorage`; all three format-fuzz suites pass
their deterministic whole-object mutation matrix without an escaping panic.

The final dependency audit found `crossbeam-epoch 0.9.18` and
`quick-xml 0.40.1` in the locked graph after RustSec published one pointer
formatting advisory and two XML denial-of-service advisories. The lockfile now
uses `crossbeam-epoch 0.9.20`, `object_store 0.14.1`, and `quick-xml 0.41.0`;
the previously yanked `spin 0.10.0` resolution is `0.10.1`. `cargo audit`
reports zero vulnerabilities. Its remaining `paste 1.0.15` notice is
informational (`unmaintained`), transitive through the qualified Parquet/Vortex
stack, and does not describe a known vulnerability.

The final storage-layout campaign is also fail-closed against evidence drift.
Its checked protocol fixes all 100 cases and five query seeds; the runner
rejects schedule, hardware, region, and layout deviations before measurement.
It validates public-dataset metadata and byte sizes, hashes every corpus,
query, ground-truth, and metadata input, and binds that manifest plus the exact
source, instance, AMI, EBS class, CPU, kernel, memory, and toolchain identity
into the evidence. The assembler requires the exact balanced schedule, exactly
100 unique query identities per case, raw seed/engine agreement, finite
measurements, and both required backends. Normal-segment bytes and total active
index bytes each have an independent 1.05 no-regression gate.

## Defects found by the full gate

The first full run found that corruption-test routing-page fixtures still wrote
the schema from before `segment_table_format` and `vector_size_bytes` became
durable fields. The shared rewriter now preserves both columns. The complete
151-test `local_index` suite and the full all-target workspace run passed after
the repair.

## Benchmark release condition

No external superiority claim follows from this matrix. It authorizes
methodology freeze, not selective result reporting. Confirmatory execution may
start only after the publication manifest, independent repetition schedule,
raw per-query artifacts, statistical decision rule, and reported-evidence
registry validate as one frozen protocol.
