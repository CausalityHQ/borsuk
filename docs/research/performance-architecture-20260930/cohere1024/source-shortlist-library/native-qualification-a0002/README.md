# SOURCE shortlist v9: native qualification

Qualified Rust source: `bf7ada2457d51cebb78d2295d0d14d841c24e8eb`.
This receipt proves source compilation and the listed native correctness gates.
It does not establish actual 1M input parity, cold performance, production
readiness, RSS under query load, total lifecycle cost, or a competitor win.

## Change

Generation v9 authenticates SQ8 in 32-row blocks. SOURCE discovery remains
256-row geometry, and encoded SOURCE units remain 32 rows. The ordinary serving
path retains the best 256 actually scored SOURCE rows, ordered by descending
finite f64 score and ascending physical ordinal for ties, then covers their SQ8
blocks under explicit request and byte budgets. Whole ranges, including bridges
and partial tails, remain authenticated. Build, open, publication, compaction,
CLI/configuration and resource accounting use the new proof geometry. Old
experimental generation formats are rejected by the current serving reader.

## Original run and verification

- AWS profile `causality`, eu-central-1 Spot m6a.2xlarge.
- Instance `i-0676750a554a602c4`, native SSM command
  `3e54793d-7d31-4a92-8e99-159fe92bfc14`.
- Unit: CPU0-3/400%, 14 GiB, swap0, pids512; compiler jobs1, test threads1.
- Native build/test/release interval: 2026-10-15 11:11:22–11:51:44 UTC.
- All 475 packaged source files unchanged before and after the run.
- Independent verifier exit0; instance terminated, volume absent, external timer
  inactive. Original logs and source pins remain under the immutable campaign
  prefix below.

| Gate | Result |
|---|---|
| Library test compilation | exit0 |
| Shortlist correctness | 7 passed |
| Exact repaired geometry falsifiers | 3 passed |
| Two-bit regressions | 46 passed |
| SQ8 authority regressions | 8 passed |
| Budget regressions | 10 passed |
| Baseline binary regressions | 48 passed |
| Generation integration | 10 passed |
| Application-ID integration | 13 passed, 1 ignored |
| Workspace Clippy correctness/suspicious | exit0 |
| Actual shim-unset workspace test build | exit0 |
| Release build and retention | exit0 |

The ignored test is `native_s3_conditional_fence_and_gc_smoke`; default-suite
success is not evidence of real S3 conditional-write/GC execution. That remains
a separate controlled-prefix product smoke gate. Counts above include repeated
falsifiers across test filters and must not be summed as distinct tests.

The cgroup reached 15,032,385,536 charged bytes, including compilation and file
cache, with 1,013 memory-max events, zero OOM kills and zero swap. This is not
query RSS or evidence that the build avoided memory pressure.

## Retained release

Bucket: `borsuk-bench-453182569524-euc1`.
Campaign prefix:
`research/semantic-router/20261015/source-shortlist-library-integration-geometry-a0002/`.
Native evidence: `stages/source-shortlist-integration-geometry/`.
Release archive: `retained-release/release.tar`, 54,886,400 bytes, SHA256
`974a03807e9fb1cef9873e445310d852b4751bf05c478d99e6589d1e9f237ab4`.

The original native job hashed the archive and five ELF files before create-only
upload. Root fetched the small retention manifest, binary SHA table and source
verification log and matched the archive HEAD length. The archive body has not
been independently downloaded and hashed locally. Any deployment must verify
its full SHA, exact member roster and binary hashes before execution.
`retained-release.json` keeps the original pending-verification wording and null
native command ID; `qualification.json` supplies the external original-job and
independent-verifier binding. Historical receipts are not rewritten.

## Decision and next falsifier

Keep 256 as the first integration candidate and preserve 512 as the higher
recall frontier point. The older, separately qualified count curve measured
97.86% recall@10 and 11,497,411,968 planned SQ8 bytes over 1,000 queries for 256;
512 measured 97.89% and 20,971,524,224 bytes. These are diagnostic quality and
planned charges, not new v9 cold measurements. Recall, latency, requests and
bytes are competing objectives; no simultaneous-improvement requirement is
imposed.

Before adopting defaults or publishing performance claims: authenticate the
unchanged real SOURCE/router/SQ8 inputs, construct v9 proofs without refitting,
pass the preregistered eight-query ID/score-bit and independent exact-range
oracle, run a separate staging smoke, then run frozen full-1,000-query cold
comparisons. Real S3 CRUD/fencing/GC, maintenance and generation-swap RSS,
matched vendor conditions and lifecycle dollars remain required product gates.
The final whole-branch independent review is a separate source-integration gate.
