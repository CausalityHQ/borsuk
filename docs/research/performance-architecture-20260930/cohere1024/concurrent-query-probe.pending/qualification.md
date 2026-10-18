# Source qualification; no performance result

Compiled source commit: 89fb834bb51396b9580fa620fd2898526a23fa01. This final documentation commit preserves its exact Rust bytes.
Base: 3698e785cd72820493a2dfcf1fd85d7d74bdb13c. Library/API source is unchanged.
Evidence: /mnt/borsuk-workspace/evidence/concurrent-query-qualification-a0004. Instance: i-0a71976260f8bd031.
Original qualification unit: borsuk-concurrent-query-qualification-a0004.service; invocation 8714d7282d0d440e8b0c6b9d67c92cc5; launch SSM 8740c506-62e1-4aac-bcf0-9ddf23f41411.
Terminal VERIFIED, exit 0; unit inactive with MainPID and ControlPID zero.

The same bounded unit held cargo-target.lock. Cargo and test-build jobs were 8;
CPU affinity 8-15, quota 800%, memory 40 GiB, swap zero, TasksMax 2048, runtime 1800 seconds.

## Exact final-source gates

- affected: exit 0. `cargo test --locked -p borsuk --bin check_cohere_native_baseline --example compare_native_replay concurrent_probe -- --test-threads=1`
  Test results: 4 passed / 0 failed / 0 ignored, 5 passed / 0 failed / 0 ignored.
- library: exit 0. `cargo test --locked -p borsuk --lib source_auth_ -- --test-threads=1`
  Test results: 10 passed / 0 failed / 0 ignored.
- release: exit 0. `cargo build --locked --release -p borsuk --bin check_cohere_native_baseline --example compare_native_replay`
- clippy: exit 0. `cargo clippy --locked --workspace --all-targets -- -Dclippy::correctness -Dclippy::suspicious`
- testbuild: exit 0. `env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=8 CARGO_BUILD_JOBS=8 bash scripts/check_rust_test_build.sh`
  Real cargo --no-run workspace build; shim unset. 161 executable receipts; these are builds, not test executions.

The affected target executions comprise 4 runner tests plus 5 reader tests (the
reader includes the same 4 runner cases): 5 distinct test names, 9 executions.
They prove actual held payload overlap, Q1 serialization, ordered fault/drain,
both Q2 slots reusable after cancellation, real caller reservation boundaries
(model-1 rejects and model admits with zero SOURCE/SQ8 payload reads), exact
IDs/score bits/plans/charges, strict resource/header binding and the coherent
90ms/10ms to 50ms elapsed mutation refusal.

## Reused regression evidence and preserved failures

qualification-a0003 on 64d20af725ff54e4fbfdd12d44dbbbfae2a6104f finished its full
reader suite with 343 passed, 1 failed, 1 ignored (exit101). Only its passing
checks are reused; this was not a passing suite. The failed concurrent API case
assumed physical first GET implied first ordinal despite Offload2 scheduling.
The repaired test first holds ordinal zero at its GET, then starts the sibling,
preserving the first-query failure oracle and Q2 overlap requirement. Both final
target executions passed. No assertion was weakened and no library was changed.
The a0003 two exact serial refusal retests each passed 1 test. Its later gates
were never qualified; the parent-authorized stage-boundary disposition is retained.

qualification-a0001 preserves missing-vendor, missing-linker and compilation
failures. qualification-a0002 preserves the initial streaming-fixture failure,
focused success and the full-runner 66-pass/2-fail old error-text regression.
The exact error-text cases passed on a0003; the full reader includes them.
Every continuation began after the previous owned unit was terminal. No duplicate
build, local compiler, library edit, real-data/S3 query or performance run occurred.

## Release artifacts

- compare_native_replay: SHA256 63866d14f15a2a16119c912067df96f5ecf357b779fa5235884d2c9508c5de01; 14760064 bytes; /mnt/borsuk-workspace/evidence/concurrent-query-qualification-a0004/artifacts/compare_native_replay.
- check_cohere_native_baseline: SHA256 a03c60d63969c93d580ac0b471de2f61910c51f3660938687bc830473d59ed92; 18944888 bytes; /mnt/borsuk-workspace/evidence/concurrent-query-qualification-a0004/artifacts/check_cohere_native_baseline.

These are the newly qualified ELFs, not historical a046/3e86 runtime artifacts.
The source-index audit and final bundle preserve all unowned base trees.

## Direct next gate

Root owns actual-input model/resource admission and separate smoke, then its
prospective Full1000 Q1/Q2 ABBA descriptive repeated-cohort Pareto screen.
Freeze the same current_thread runtime and global Offload2 workers in both arms.
Require exact semantic parity before throughput interpretation. A Q2 overlap
witness requires sum_query_wall_ns > cohort elapsed_ns; otherwise report overlap
not established. Scope negative results to cooperative closed-loop batches.
Use the original full1000 native cohort receipt; do not fabricate a prefix receipt.
Connection establishment is included; each cell is a fresh process, backend/cache
state remains unknown, and no prewarm is introduced. No service-peak, significance,
variance, cold, vendor or arbitrary-threshold claim is authorized by these tests.
