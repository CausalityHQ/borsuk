# Centroid bulk conversion: paired development gate

## Intervention and authority

Replace scalar FP16-to-FP32 centroid conversion with the locked `half` crate
bulk slice API, using a fixed 1024-element u16 scratch buffer (2048 bytes).
Keep little-endian interpretation, format, checks, output order, norm
summation, hashes, routing and physical scoring unchanged. The sole native
source difference is `crates/borsuk/src/unit_centroid_pages.rs`; reconstruct
control from its authenticated body at `93b11ab4`. The graph duplicate-check
improvement qualified in graph-decode a0004 is present in BOTH arms.

Candidate whole-native identity:
`14fa7148e55d898ab79b1a8b4a208dfc02266f7e2610e1c084080550d78b882c`.
Control whole-native identity:
`46e5ca162da947f3596b00291211ecd05e2059b721938047b8c1487189aedb6d`.
Both cover exactly 395 Rust/Cargo files. No speedup is presumed.

## Mandatory qualification before measurement

Actual local affected tests, workspace Clippy and all-targets compilation
passed on the candidate; the one full workspace execution is still pending.
Authenticate its source-bound proof and complete raw log after terminal zero.
Do not reuse the earlier full suite as evidence for changed source. Disclose
local x86_64 CI scope: no repeated ARM full workspace suite is planned.

Build control and candidate fresh on the same ARM worker with byte-equal
toolchain/feature evidence and locked dependencies. Restore authenticated
control source locally, qualify it, clean the borsuk package, restore
candidate source and qualify it. Run the existing nine focused groups on
both arms (graph eight tests each), plus centroid tests (control three,
candidate four), candidate mirror target four tests and both release builds.
Capture both changed centroid bodies in the 21-file compiled source roster.
Recheck whole-native source before/after every build and restore candidate
source in cleanup. No current ARM full-suite pass claim.

## Fixed protocol and GO/KILL

Reuse the reviewed ABBA cold protocol: control queries 0–31, candidate 0–31,
candidate 32–63, control 32–63; ReLAION then CoHere in each block. Both
FIRST1M D768 cosine k10 panels are consumed development queries, using the
same immutable requests, resident references, truth and generation roots as
graph-decode a0004. All 256 calls are included. A new quality/generalization
claim requires a separate unopened representative panel.

Every call starts a fresh process and namespace. Client CPUs 4–5, native
0–3, Tokio four workers, AWS maximum attempts one, modeled native budget
1 GiB, application SQ8 cache off, S3 service cache uncontrolled, loopback
plain HTTP. Authenticate inputs before timing. Metadata uses 4 MiB ranges
and up to eight parallel GETs, with a 32 MiB staging payload bound. Source
caps remain 128 GETs /64 MiB /16 parallel, SQ8 32 GETs /16,773,120 bytes,
combined 160 GETs /83,881,984 bytes and zero failed reads.

Development GO requires all 64 successes per dataset and arm, exact ordered
resident-reference parity, mean recall@10 >=95%, and lower candidate decode
p50 AND end-to-end cold p90 on BOTH datasets against contemporaneous control.
Any failed requirement is FAIL; preserve a0004 and the failed arm evidence.
Report cold/decode p50/p90/p95/p99, paired differences, bytes/requests/RSS and
errors. Preserve tail regressions; do not add a retrospective p99 gate.
The 444 ms cold p90 published context is separate. Serial completion rate
is not offered or saturation QPS. No matched vendor or total-cost claim.

## Resources and terminal handling

One c7g.2xlarge Spot in eu-central-1a, existing subnet
`subnet-034528fbd6977848f`. Machine 5400 seconds; build 10 GiB/no swap/four
CPUs/3600 seconds plus 30 cleanup; profile 8 GiB/no swap/4 GiB address space/
1500 seconds plus 30 cleanup. Quote maximum $0.30/hour, estimated compute
cap $0.45 plus $0.15 allowance. Total lifecycle cost remains unmeasured.

Fsync every acknowledged instance ID. No automatic Spot replacement.
Monitor incomplete campaigns only through terminal markers and infrastructure.
Discard an interrupted cell, preserve its receipt, and stop this attempt.
Terminate and wait for every owned ID before collection; authenticate all
terminal bodies and independently reduce closed records. No launch before
actual source/config/runtime/controller/verifier gates pass.
