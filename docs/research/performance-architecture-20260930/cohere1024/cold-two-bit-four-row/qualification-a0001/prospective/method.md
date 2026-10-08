# Four-row repaired runner: prospective native qualification

Candidate `04944c574ece595c43710974d89a75d759da2f7b` preserves kernel commit `3138bfb0` and adds a checked 512-byte batch allowance to the cold runner. Root reviewed its exact two-file delta: only runner admission/reporting, the existing runner regressions, scratch field documentation, and a cfg(test) observer in the planner boundary. The codec/kernel/ignored primitive bytes are identical to the independently audited a0004 source. The observer is absent from production builds and outside the primitive's direct scoring call.

The a0004 closure at `f6fbacf2` proves seven compiler/correctness stages and ordered packed accumulation; it does not prove actual production stack or performance. The independent Opus/Astra reviews in group `f9b98d02852a459f` apply to this unchanged kernel. Their required production-profile and nested-frame audit remains enforced. This narrow runner correction changes no scientific memory, CPU, GET, byte, cache, query, truth or population limit.

## Exact source and matched control

Both sources are sealed before launch. Candidate: 415 native files, 2454 total files, native identity `f4f34fd68c687249a914bf22ed58778d516dc31d8444b3625056b6c7b6da514c`.

Control: the candidate archive with only `rotated_two_bit.rs` and `two_bit_generation.rs` replaced by the authenticated `c49a2e6d2a035bfaf42358fbf7fb60abf11e4222` library blobs. Its identity is `4c5dff40e340f004b50799650284c0b205f4068ba970125fb1e6122afaa3c03b`. The corrected runner is identical in both, including its additional 512-byte allowance. Every other native/support byte is identical. Separate source directories are authenticated before/after their builds; no candidate source is modified for the control build.

Use one explicitly captured Rust 1.98.0/Cargo 1.98.0 environment, default release profile, empty RUSTFLAGS/CARGO_ENCODED_RUSTFLAGS, empty rustc wrappers, jobs1 and no Cargo configuration overrides. Share the target cache for dependency reuse, but copy and hash candidate production binaries before the control writes its binaries. Recheck those immutable candidate copies afterward. Scalar-control uses a separate target directory and release profile. Retain all six declared ELF roles with whole-body byte/SHA pins.

## Thirteen serial stages and limits

1. Runner debug, including all three mandatory scratch/startup pipeline regressions.
2. Planner debug, including the native paged false/minimum/+511 and host-qualified true/+512 dispatch witness.
3. Codec debug.
4. Release libtest compile and permissive packed-emission stop screen.
5. Codec release.
6. Planner release.
7. Runner release, using the optimized production library.
8. Affected integration tests.
9. Candidate production runner and HTTP example release build; copy/hash artifacts.
10. Matched control production release build; copy/hash artifacts and reauthenticate both source trees.
11. Locked workspace/all-target Clippy correctness and suspicious gates.
12. Actual workspace test compilation with BORSUK_TEST_BUILD_COMMAND removed and both job limits1.
13. Scalar-control codec release, separate target, then rehash the original default release libtest.

All mandatory names must pass once per owning debug/release stage. Gate commands/time/native+tee exit statuses are recorded separately. Missing/ignored/failed names or nonzero exits fail qualification; no primitive runs in this job.

Spot c7i.2xlarge, CPU2/cpuset0,1, 8GiB, swap0, pids512, jobs1, native7200s/machine9000s, EBS80GiB, compute reservation $1.50 plus ancillary $0.15, bid ceiling $0.50/hour. Preserve interruptions and execution errors as INVALID; no automatic duplicate or algorithm rejection. The terminal causes immediate termination/wait before collection. Native tests use synthetic local fixtures only; no real corpus, request/truth workload or service query occurs.

Source/archive/input admission and staging canary passed under CPU1/256MiB/no-swap/120s. Real checks include full source/pin authentication, Bash syntax, a tiny actual separate control transfer/patch/hash sequence with candidate unchanged, and missing frozen marker exit98 before Cargo. Scalar artifact positive/duplicate/wrong-feature/wrong-directory cases use explicitly mocked ELF metadata. These checks establish glue admission only, not native correctness.

## Decision after the original terminal

Authenticate every artifact, both full source maps, commands/counts, default ELF before/after identity, toolchain/profile/flags, cgroup/swap/OOM/drain and exact instance termination. Inspect actual retained candidate and control production serving functions and nested kernel frames. Require independent lane-order/no-reassociation/no-horizontal/no-FMA proof and cumulative incremental production stack no greater than the unchanged 512-byte batch allowance, including spills, outgoing call slots and any realignment. Any unresolved frame keeps timing on HOLD.

Only then admit the unchanged CPU1/256MiB/no-swap/pids128, 30 CPU-second/120 wall-second primitive. That primitive compares four-row to single-row scoring; it does not alone isolate SIMD against the previous four scalar chains or prove cold improvement. A survivor needs real-input runtime admission, disposable staging smoke, and paired cold measurement with the existing preregistered recall/parity and p90/p95/QPS gate. Reuse the saved matched S3 reference and label the published Turbopuffer population/query/cache/region/load mismatches. No new dataset, 100B, warm cache or service benchmark is authorized by this native qualification.
