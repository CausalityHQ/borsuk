# Publication V3 attempt ledger — 2026-08-25

This ledger records terminal Publication V3 attempts, including attempts that
cannot contribute to a publication claim. It prevents outcome-based selection
and preserves the exact boundary that must be repaired before another paid
attempt. Terminal marker and receipt objects are authoritative; a locally
observed process exit is not a substitute for those objects.

No artifact from a failed runtime attempt is merged into another attempt. A
completed cell remains bound to its source, manifest, protocol, build receipt,
arm, and attempt identity. A partial campaign does not authorize a comparative
or aggregate publication claim.

## Standard ANN read campaign

The first scheduled BORSUK cell of `standard-ann-read` used cell identity
`r01-f4b9090272773676be960bbd` and one immutable build:

- source archive SHA-256:
  `7fdcac7f3564265048e366cf4ca76fa4268aacb910d0bf53b3869515e82b718a`;
- manifest SHA-256:
  `7cba62c8cb04a04e5b42c4a324dc14d7cd65800a7482beadb8fe3375d61db3cb`;
- build protocol SHA-256:
  `1337e9a2d0eaa99c5c42c2f1bc8a591c5323e1f6cc32f6a93fb07690d6cf6cba`;
- binary SHA-256:
  `2e2e9ee5e006ccce4f6d487780ebd3d1aa0020913746f923ff23f00c81db985f`;
- index URI:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/indexes/build-attempts/0001/index-22ed55815e0f56c4daed6911`.

All attempts used EC2 Spot in AWS account `453182569524`. The controller
terminated each instance after its terminal marker; no campaign instance was
left running.

| Role / arm | Attempt | Instance | Terminal state | Terminal-marker SHA-256 | Boundary |
|---|---:|---|---|---|---|
| build | 1 | `i-003d0d872898399c6` | complete | `4992ce9f1f92bacacd00c88d5e2f8c935e899aea79db72f2d0720e32ae197e7f` | Immutable BORSUK index build completed and published its receipt. |
| recall arm 0 (`cold`) | 1 | `i-0879428218d03f90c` | complete | `f103648ae3858bf6ccce3cf2868dbad625c13fc8005914127e0074c10cf21d3e` | The exact cell passed result validation and published `RESULT_COMPLETE.json`. |
| recall arm 1 (`warm`) | 1 | `i-045d20cea081877e1` | failed | `0bee75e35f70ff35cb207f78f1d7eebf2d270a18f8f63ccad10f2b59de4d351b` | The disk-cached guard rejected query 20 after two backing GET/HEAD operations. The runtime failed closed before a result could be published. |

The terminal marker prefixes are:

- build:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f4b9090272773676be960bbd/build/attempts/0001/`;
- cold runtime:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f4b9090272773676be960bbd/runtime-recall/arms/0000/attempts/0001/`;
- warm runtime:
  `s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-f4b9090272773676be960bbd/runtime-recall/arms/0001/attempts/0001/`.

### Completed cold-cell evidence

The cold cell measured 1,000 queries and is valid evidence for that exact cell,
but not for a completed campaign or third-party comparison:

| Metric | Result |
|---|---:|
| recall@10 | 97.41% |
| latency p50 | 143.564 ms |
| latency p95 | 238.364 ms |
| latency p99 | 332.367 ms |
| throughput | 6.475 queries/s |
| peak RSS | 1,189,474,304 bytes |
| backing bytes read | 27,098,790,672 bytes |
| backing GETs | 54,625 |
| global leaf code requests | 42,887 |
| global leaf exact requests | 11,738 |

The cell used a 2 GiB RAM budget, no disk cache, S3 GET concurrency 64,
leaf-read width 32, 48 maximum in-flight leaf reads, four active searches, and
exact-read physical amplification 2. Its result object is bound to index
receipt SHA-256
`54c01a9131905f36001f517e3011cd9691f0447e99b2bb7c4ec0ac65fc8ee08a`.

### Warm failure decision

The failure is a benchmark-methodology defect, not a warm performance result.
The disk directory was cleared between bounded query cohorts while the same
open serving handle retained decoded and code-plane RAM state. Priming a later
cohort could therefore be satisfied by RAM without recreating its disk-cache
objects; subsequent RAM eviction allowed the measured query to reach S3. This
explains why the first cohort passed and the next cohort failed at query 20.

The failed attempt authorizes no latency, throughput, cost, or comparison
claim. A replacement attempt must use a frozen repair that makes the disk cache
the only state shared between priming and measurement, preserves the immutable
attempt numbering, and rebuilds under the repaired source authority.
