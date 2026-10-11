# Qualified Rust paired leaf-metric coverage probe

2026-10-11. Native source `240a570dff26daa7ec556159c9f9ecd370ed0dce`, based on main `556e5bf147ef0ac3f8fa083212204ea4af560065`. Only `crates/borsuk/examples/compare_native_replay.rs` changes. Production leaf selection remains L2; no layout, scoring, fetch, normalization, thread, SIMD or dataset-library change.

The new `--leaf-metric-coverage CONFIG CONFIG_SHA NEW_SELECTION_SEAL NEW_REPORT` diagnostic authenticates the original completed producer, generation/router/membership and request authorities. It selects exactly 48 leaves under L2 and cosine, uses production SOURCE nomination/cover planning, and durably seals both full selections before first truth access. It then authenticates whole SQ8 bytes, norms and ID ordering and reports paired SOURCE-scored truth coverage separately from transport-only bridge coverage. It checks original returned IDs against the actual SQ8 fetch cover. Publication failures invalidate the original output inode without deleting a foreign replacement path.

This is a metadata-bound consumed32 mechanism diagnostic, not a full generation serving open or an ANN/cold benchmark. The data phase has NOT RUN. The historical control remains returned313/320 FAIL and SOURCE coverage314/320; a candidate needs at least315 coverage slots with net positive gain, unchanged SOURCE128GET/64MiB. A survivor alone earns unchanged-SQ8 returned-recall testing. Fresh unused holdout and matched cold measurements are still required.

## Qualification evidence

| Gate | Exact native result |
|---|---|
| Eight new `leaf_coverage_` tests | 8 passed, 0 failed/ignored |
| Four preserved `sealed_loss_` regressions | 4 passed, 0 failed/ignored |
| Locked optimized example build | exit0, retained SHA-bound binary |
| Locked workspace/all-target Clippy correctness+suspicious | exit0 |
| Real environment-unset workspace test-build script, jobs2 | exit0; complete workspace test surface compiled, not executed |

All five stages authenticated the same 463-file source tree before/after, matching Git; native source map430. Resource controls: CPUs0-3/400%,8GiB,swap0,pids512,jobs2. All native/manager exits0, all owned cgroups drained, swap peaks0 and OOM/kill counters0. Workspace test compilation reached the8GiB cap with809 memory.max events (reclaim); these are not zero and are not ANN RAM measurements.

One causality Spot instance, `i-0defaf8a335f362fd`, was reused through repairs and all gates, then terminated/waited. Root volume `vol-08e774940109dda3a` deletion was verified; external timer inactive. No other instance/job/worker was restarted or replaced.

## Preserved failed attempts and repairs

- `4ec3cb76`: native target compiled,6 tests passed/2 failed at the shared synthetic fixture's selected-request/reserved-query digest. Native exit101, unchanged source, drained cgroup; preserved.
- `607da22f`: corrected the one-query digest;6 passed/2 failed because the synthetic SQ8 plan omitted required accounting fields. Preserved.
- `240a570d`: completed the one-page plan and reconciled seal/terminal reserved counts and diagnostic-prefix scope. Test-only changes; strict production validators and assertions remain.
- One SSM shell attempt rejected `pipefail` in `/bin/sh` before overlay/native execution. Explicit Bash wrapper fixed the demonstrated environment defect; no duplicate instance or algorithm rerun was inferred.

Independent Opus/Astra review of the initial source identified publication and returned-ID coverage defects. Reconciliation and byte-preserved compressed original reviews are included. The subsequent fixture-only repairs do not loosen gates. Compiler warnings remain; the required correctness/suspicious Clippy gate passed.

## Next action

Integrate these verified bytes and receipts. Run the existing fixed48 paired coverage gate only after real-input native runtime admission, a separate disposable environment smoke, and frozen source/config. Do not repurpose this closed compiler campaign or claim a recall/QPS/latency/vendor win from synthetic tests.

Original full stage logs/rosters are retained create-only under `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/leaf-metric-native-qualification-a0001/stages/`. Per-stage EVIDENCE.txt explains this Git subset; the delivery manifest covers the subset, not the omitted full logs.
