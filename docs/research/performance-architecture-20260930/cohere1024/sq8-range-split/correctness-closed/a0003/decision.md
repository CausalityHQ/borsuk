# SQ8 range split: native correctness qualified, performance pending

Frozen native source: `8e7076951936b0434b7947afd5b23e192c98fe70`.
Original command: `e77d03c6-a011-4bec-9e23-1af966a780a8`.
Spot instance: `i-09f86aa4caf0d7e83`; volume `vol-09ef9f0f3b2a996d5`.

## Accepted scope

The optional SQ8 range-split implementation and current-v10 readers passed the original remote qualification and independent evidence verification. The default remains unsplit. This is compiler/synthetic correctness qualification, not full architecture qualification or scientific performance evidence.

- Original native command: Success / 0, 2026-10-16 04:06:37–05:02:04 UTC.
- All 184 native and 17 manager stages passed; exact source before/after authenticated.
- All 22 mandatory names selected once and passed in both debug and release; four originally failing examples also passed first, before broad regressions.
- Each profile passed 30 SQ8 range tests, 37 SOURCE walk tests, 51 runner tests and 281 example tests. These runs overlap mandatory tests; do not sum them as unique tests.
- Release production binaries retained and separately published with validated hashes and create-only upload receipts.
- Workspace correctness/suspicious Clippy and the real shim-unset complete workspace test build passed, compiler concurrency 2.
- Independent verifier accepted originals, rejected an isolated copy with original-command exit 7 (exit 7), and rejected a one-byte enlarged runner binary (exit 1, binary size/SHA mismatch). No downloaded ELF executed locally.
- Original cleanup watcher exited 0: exact instance terminated, root volume absent, external timer inactive, root background marker cleared.

## Verifier defects preserved

The first local verifier invocation failed because its jq helper placed an evidence filename before the filter program. The helper argument-order repair is committed at `58088bcce58a35b3a23d57b2d42922585c5f3a14`; bash syntax and ShellCheck passed. The second invocation passed on the SAME immutable collected evidence. Earlier source repairs also authenticated the exact SSM preflight line and kept collector sidecars outside the strict two-binary directory. No native test predicate was relaxed and no native run was repeated for these verifier repairs.

## Remaining gates

Builder-removal sensitivity is EXTERNAL / NOT_RUN. The prepared mutation probe, disabled/historical parity scripts and six-cell panel execution scripts are source-only pending runtime proof. Before scientific cells: exact real-input admission, separate disposable staging smoke, disabled-v9/current-v10 parity, builder-removal falsifier, independently frozen runtime headers and final protocol admission. No performance experiment has been launched by this closeout.

The terminal's `compute_closure_pending:true` is its immutable pre-termination state. The separate final cleanup receipts prove closure afterward. `full_qualification:false` remains true as a scope restriction; no recall/latency/RSS/vendor win is implied.

## Timing and retained authority

Debug full-example regression took 1130.98 seconds; release test-target compilation took approximately 981 seconds. These explain most of the 55m27s native duration; compilation is not a product acceptance metric.

Evidence bucket: `borsuk-bench-453182569524-euc1`.
Prefix: `research/semantic-router/20261016/sq8-split-correctness-a0003/terminal/`.
Metadata archive SHA256: `74d55e823b88430bdb96c66def556e501bf1b77083553c474c261b92342bd44e`.
Raw archive and both binaries remain in S3; the curated files here bind the original source, inputs, gates, results and closure. Historical a0001 preflight INVALID and a0002 execution timeout/failures remain preserved separately; neither is an algorithm rejection.
