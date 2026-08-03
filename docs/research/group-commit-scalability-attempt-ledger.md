# Group-commit scalability attempt ledger

This campaign qualifies the production `GroupCommitWriter` at 2,000 and 16,000
logical cells with 1, 8, and 32 concurrent producers over five repetitions.
While an attempt is incomplete, inspect only terminal markers, service/process
health, non-measurement progress, and resource availability. Never inspect a
partial summary or samples CSV.

| Attempt | Revision | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| v1 | `3ea0335` | `1fb3282fe8b3ea8327c60c121e394f50dd2bb36ff866c7c1e4102af015dc891a` | `c9a3914d39ded8b119f19f61f6faf8c58068c9d8f99b53d5f0f4deadb2e727bf` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v1/index` | terminal failure at `c2000/r01/w8`: resource exit 1 after 9:47.29; journal reported the combined post-reopen visibility/recall gate. No campaign CSV was inspected. Investigation found that writer-count cells reused identical vectors under different IDs, making exact top-1 tie-sensitive, while 20 exhaustive S3 scans per cell made validation dominate the ingest experiment. |
| v2 | `a49d10b` | `caca6cdf273c125712cf2bc0e5218cc045b1a951817ff145222951c8a3fe2598` | `cadc28cc51d96ab0f4a26bed037836ca61d9950c13be8f3a75499150d4336a84` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v2/index` | terminal timeout at `c16000/r01/w8` after every 2K cell and `c16000/r01/w1` advanced: service and resource exit 124 at the frozen 30:00.04 bound, peak RSS 719,696 KiB, explicit failure marker. No incomplete campaign CSV was inspected. Point validation exposed repeated generation resolution and allocation of the complete live WAL tail for every ID lookup. |
| v3 | `565b6d1` | `ea77af985a0ea5daf3a93ece3aa57ebb80f18ebdcb8e4d52e81831808c8a21f2` | `ed84a8aa6cf2254d972f9dd99b488cbd7d23d60725f27e6e2f99ae250ce2acdf` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v3/results/` | `s3://borsuk-bench-453182569524-euc1/research/group-commit-scalability/20260803-v3/index` | terminal timeout at `c16000/r01/w8`: all fifteen 2K cells and `c16000/r01/w1` advanced, then resource/service exit 124 at the frozen 30-minute bound with an explicit failure marker. No incomplete CSV was inspected. Generation-indexed point lookup reduced live process memory versus v2, but the remaining exhaustive 16K-cell S3 recall scan still dominated validation. |
| v4 | `ee3049c` | not launched | not launched | not launched | not launched | superseded before launch: fail-fast write gates were added, but the protocol retained the exhaustive S3 recall scan exposed by v3 |
| v5 | pending | pending | pending | pending | pending | preregistered replacement uses 20 known-ground-truth queries through bounded production `SrhtPqScan`, requiring recall@1 1.0 and read/write p95 below 200 ms plus writer-scaled throughput; launch pending local gates |

The campaign is claim-ineligible until the root completion marker exists, no
failure marker exists, the service exits successfully, and the fail-closed
validator reconciles every matrix cell, raw sample, group receipt, request
total, resource exit, visibility result, exact-recall result, and correctness
gate.
