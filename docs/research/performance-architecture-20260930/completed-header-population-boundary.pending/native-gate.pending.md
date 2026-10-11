# Population artifact boundary: native gate pending

Test-only source 18f36c212f5590ed628dd5a7299ff467b074adaf, parent 1fcf7231c03646bffd41f7f58b25d9c785c0c068. Sole changed file crates/borsuk/examples/compare_native_replay.rs; 38 test lines. Source SHA 5a65c2eb904cc253bfa20df123204127ca9d6f94dc6f5240da8488f07f60e04e. No production edit, no native compilation or test execution, no paid qualification launched.

Both completed independent reviews 9b597e48f7e344d7 agree that the smallest repair is `const HEADER_RESULT_CAP: u64 = FILE_CAP;`. Research critic ran as gpt-6.1-sol after Opus usage-limit fallback; engineering critic gpt-6-astra. Their complete separately labelled results are preserved alongside this document. The supplied observed 1,146,181-byte/32-query result only supports a size projection; it does not prove full1000 size or population performance.

Independent source verification: baseline producer OUTPUT_CAP and completed reducer FILE_CAP are 64MiB. Header-only binder separately rejects pins over32MiB, retains only two64KiB header buffers, hashes the entire result in8KiB blocks, and checks full SHA/EOF/length/FD/path/single-link identity. Full and untraced-panel paths retain the same core records; panel adds selection metadata. No row trimming/compression/schema change/ANN change is required.

## Ordered, separate causality EC2 Spot gate

Do not compile or run fixtures on the local host or on the compiler-disabled staging canary. Freeze exact source and transports before a compiler-enabled disposable attempt. Resources: compiler jobs2, CPU0-3/400%, memory8GiB/swap0/PID512; native tests serial, CPU0/100%, memory1GiB/swap0/PID128. Host shape c7i.2xlarge, finite per-stage and whole-attempt deadlines, external termination timer, root-volume deletion. Preserve interruptions as INVALID, no automatic repeat, no retained idle compute.

1. Compile exact test-only example with locked dependencies (`cargo test --locked -p borsuk --example compare_native_replay --no-run`).
2. Execute exact `completed_header_binding_tests::bind_completed_scale_authenticates_population_sized_result_tail` with --exact --test-threads=1. Require exactly one test and the original admission failure `bounded header artifact pin`. Preserve original status/body/resource evidence. A compiler failure is not the expected red result.
3. Only after verified red: freeze a new coherent candidate changing the one cap assignment to FILE_CAP and retaining this test. No other native change. Copy/authenticate that exact file and full source map on the SAME qualified compiler attempt at the next sequential stage; no algorithm or data changes.
4. Run the same exact test for green, then the entire example test suite (expected previous93 + new1 =94), and locked release build of the example. Capture named native test counts, actual exits, CPU/RSS/cgroup memory/OOM/swap, and source before/after.
5. Pass locked workspace/all-target Clippy correctness+suspicious. Run real `env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=2 CARGO_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh` to compile the complete workspace test surface. Reuse unchanged-source fresh gates only where their scope is identical and record that fact.
6. Authenticate and collect all original terminal bodies, terminate SAME instance, wait, prove root volume absent. Only then integrate the verified slice and retained release to origin/main with fast-forward source proof.

The fixture is exactly64MiB and its opaque tail is authenticated, not semantically reduced. Wrong whole-body SHA must reject. The final64MiB+1 check is a DECLARED oversized pin; do not label it physical oversized-file coverage. No population/cold/vendor win follows from this gate.

## Next measurement

Separate staging canary a0001 uses existing qualified ELF releases and immutable actual corpus/request/truth/generation pins. Full1000 measurement waits for this boundary repair and closed canary. Reuse the corpus, ground truth, retained generation and production baseline ELF; no recompilation or corpus/index rebuild for measurement. Report p50/p90/p95/p99 and sequential QPS from the qualified native full-population reducer, retained router/open costs, process peak and logical request/byte charges. Declare source/local payload cache off, resident router, SDK connection reuse, provider cache uncontrolled and query concurrency1. Do not label logical charges as physical/billed totals or sequential QPS as concurrent service capacity. No matched vendor win until equivalent S3 Vectors data/quality/region/concurrency evidence exists; Turbopuffer published numbers remain disclosed references.
