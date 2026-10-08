# One page-authentication backend candidate

Status: selected for bounded source implementation; native and speed UNVERIFIED.
Evidence: closed CPU profile at 6a7a43e3, and completed read-only planning
consultation 3197e4eb551c4e0d. This is one causal change, not a new layout.

Replace only the SHA-256 digest expression in PageAuthority::verify_payload
with aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, page_bytes).
Add exact already-locked aws-lc-rs 1.18.1 as a direct dependency and its
existing package edge in Cargo.lock; no package upgrades. Keep all bytes,
full/tail page extents, page-order errors, transport/ETag/generation checks,
digest comparisons, charges, index formats and scorer arithmetic identical.
There is no backend selector or cache. The existing SHA-256 dependency stays
for manifests/sidecars and a cfg(test)-only independent control verifier.

Root independently authenticated the locked sha2 crate checksum and the
exact profiled ELF. Disassembly of the sampled digest_blocks function has
SHA256RNDS2/MSG1/MSG2 instructions. The consultation's metadata-only backend
uncertainty is therefore narrowed: this is not missing hardware acceleration.
An alternate implementation may have zero benefit. No speedup is claimed.

## First falsifier

Before any new cold campaign, root must run actual affected native tests and
a release verifier microbenchmark on bounded remote compute. No local Cargo,
rustc, ANN, corpus, query or truth execution. Timed microbenchmark is CPU1,
256 MiB, swap0, at most 30 CPU seconds and 120 wall seconds; build separately
under the established CPU2/8GiB/jobs1 envelope. Five alternating paired
blocks compare the complete old and new verifier over identical buffers.
Keep the old verifier cfg(test)-only, exact old range/length/hash/error logic.
No generic backend abstraction in production.

Synthetic deterministic full source/SQ8 pages and tails are labelled as
verifier fixtures, not ANN or real-input admission. Use source page geometry
32*(padded_D/4+8) and SQ8 256*(D+12), including D1024. Report all raw block
CPU/wall times, byte counts, labels, order and digests. Weight full-class
ns/byte by the observed source 8,971,573,248 and SQ8 16,699,756,416 byte totals;
these weights are fixture policy only, not library constants. Reject if any
semantic mismatch, weighted CPU saving below20%, or either full class median
ratio exceeds1.05. Retain tail results separately. Deadline/resource/setup
failure is INVALID, not an algorithm rejection. Do not put timing assertions
in ordinary correctness tests or claim warmed verifier buffers are cold ANN.

Correctness must cover independent SHA known-answer/padding-boundary cases,
generic dimensions and full/tail extents, first/middle/last/unscored-byte
corruption, wrong sidecars, bad range, truncated/extra payload, same error
class/order, and existing transport plus scalar score-bit regressions.

## Later gates, only if the first falsifier survives

Pass release and workspace/all-target Clippy correctness+suspicious, and the
real BORSUK_TEST_BUILD_COMMAND-unset workspace test-build script on exact
source. Authenticate changed feature resolution without upgrading packages.
Then source-bound real-input admission and a separate disposable canary;
freeze exact source/binaries/config before one uninstrumented cold ABBA.

ABBA keeps the same100k/D1024/k10/1000-query bytes, generation, CPU1/512MiB,
swap0, width32, query concurrency1 and application caches OFF. Require exact
ordered IDs/score bits/traces/charges and 9723/10000 recall in every cell.
Prospective screen: aggregate query CPU at least5% lower, aggregate serial
QPS at least3% higher, p50 lower in both adjacent comparisons, and no pair
p90/p95 regression over3%. Root must preregister the concrete attempt before
launch. The saved S3 benchmark is not rerun. This does not establish a 10M
Turbopuffer comparison or the full product goal.
