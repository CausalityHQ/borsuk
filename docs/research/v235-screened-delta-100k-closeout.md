# V235 screened mutation delta, ReLAION-100k

**Decision: quality passed, speed gate failed.** The conservative FP32
screen avoided 99.8875% of ordered FP64 delta scores and matched all
1,000 complete control ID lists, but lowered loaded p95 only 18.9%
and raised throughput 26.7%. The frozen gates required at least 25%
p95 reduction and 1.5× throughput. Every query still screens all
10,000 rows and the base graph work remains; another scoring-loop
variant would not establish the missing end-to-end product result.
Stop this local mutation-kernel series before another 1M latency cell.
Keep the screened path experimental until its floating-point bound is
reviewed; exact ID parity on one panel is not a general proof.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-0a6d4c83232d7c5cb` in `eu-central-1c`, independently confirmed
**terminated**. Source commit
`a923fc399a8777743b969bcae48d5e1a30b8abcd`, source archive
SHA-256 `64db92598d6907c4150f480798bb72983fad891dd6eeedf4407de918220aaf07`,
original terminal SHA-256
`9e5ebe96f4ee8b2d34c939377e9e3a34682e903f18ab6b146cf95170e77c2117`,
closeout SHA-256
`9d762e5bad8bc3e32da07d9097093d458a8ccf714315b8c30158236274f9245f`.
The terminal exited zero; all 12 artifact lengths/hashes and four
sealed raw streams passed independent replay. Loaded p50/p90/p95/p99
were recomputed from each sealed 1,000-query timing stream. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v235-screened-delta-100k/a923fc399a8777743b969bcae48d5e1a30b8abcd/runs/a0001/`.

Dataset/split: ReLAION-100k D768, development queries 0–255 and
previously used method-held-out queries 256–999, cosine k=100 and
exact GT100. Both arms used the authenticated V218 graph,
ef/shortlist 2,048/2,048, eight loaded workers, and every tenth
physical row upserted under the same ID and FP16 vector (10,000 rows).
The logical corpus and truth were unchanged. Arms ran sequentially in
separate processes on one host from the same binary/compiler flags.
Times below are **loaded in-process**, not HTTP product latency.

| Verified same-run cell | Decoded full scan | Screen + exact rescore |
| --- | ---: | ---: |
| Development GT100 hits / 25,600 | 25,540 | 25,540 |
| Previously used method-held-out GT100 hits / 74,400 | 74,238 | 74,238 |
| Combined GT100 hits / 100,000 | 99,778 | 99,778 |
| Complete ID lists identical to control | — | 1,000 / 1,000 |
| Loaded p50 / p90 / p95 / p99, ms | 10.584 / 11.585 / 11.908 / 12.468 | 8.345 / 9.344 / 9.661 / 10.152 |
| Loaded throughput, queries/s | 743.0 | 941.3 |
| Peak process RSS, bytes | 422,891,520 | 285,696,000 |
| Overlay-owned resident bytes | 30,892,500 | 30,892,500 |
| One-time decode/pack, ms | 10.563 | 12.412 |
| Delta rows screened / exact scored | 10,000,000 / 10,000,000 | 10,000,000 / 11,245 |
| Vector-body GETs | 0 | 0 |

The screened arm exact-scored a mean of 11.245 rows per query; p95
was 16 and maximum 21. Both arms had p05=99 and passed
split quality, ID parity, memory, packing-time and zero-GET checks.
Sealed control ID/loaded SHA-256 values were
`b2d1325d01457e3f086439a7e3b93b4eecae840a92ff2a30467cc3ba58538d47`
and `42730e4291954fcca5d33c79d89c7d51dec33bedba6fcc4c7b71db9d2c491933`;
screened values were
`d0e43b3da2f3da28e7e2b4e2493f5023ab6f3ed7efd054eefa68597907b197e5`
and `d7d7528458df690d9556458ccba2a81931d5b9f025a7c637d45b60ba27042fe9`.
Spot quote was $0.3631/hour; estimated compute to closeout was
$0.03215, excluding EBS, S3 and billing adjustments. The process RSS
difference is descriptive because arms used separate processes.

**Next product gate:** exercise a real-S3 ReLAION-1M collection-head
mutation publication and authenticated pinned-reader reload with the
exact decoded overlay, then a measured end-to-end HTTP serving cell
from that persisted state. The mutation admission/compaction envelope
must be derived from an explicit recall, latency and memory budget,
with no fixed vector-count knee. V233's 1M HTTP result remains the
current measured mutation-serving baseline; V235 does not promote to
1M. A further query-kernel design needs a material storage/tier or
cross-query batching change and its own cheap falsifier.
