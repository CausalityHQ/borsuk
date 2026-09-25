# V231 indexed mutation delta, ReLAION-100k

**Decision: reject the 256-candidate HNSW delta.** Its query and build
performance passed, but it lost 20 exact GT100 hits versus the same-run
linear mutation snapshot, with losses on both query subsets. It does
not advance to 1M. The responsible layer is approximate mutation
candidate selection: the authenticated base graph, query panel,
mutation IDs/vectors and exact FP16 rerank were held fixed. The next
100k candidate changes representation instead: predecode the FP16
mutation rows to resident FP32 and retain an exact scan. FP16 values
are exactly representable as FP32, so this can preserve the current
FP16-to-FP64 scoring path and ID order while spending RAM to avoid
per-query half conversion. This is a hypothesis until measured.

The sole `causality` c7i.4xlarge Spot attempt `a0001` ran on
`i-02593c80ff3d0b109` in `eu-central-1c`; the instance is
independently confirmed **terminated**. Source commit
`2678f8ea8de52c7a801dd6dbf1496f2fe4b44521`, source archive
SHA-256
`5191297420a4f2ac99849d8a5d85aaa04f1ef2aa60f03162a04f71afb48b042d`,
original terminal SHA-256
`c9348465dd670f070cb3f6ec0469316c28f876a165527f2e4a46513e33fd69a8`,
closeout SHA-256
`8974f1810dabaf733f14b69e109b41d8cfde645f45cf299e597b6b72658b0417`.
The terminal completed at exit zero; all ten artifact lengths/hashes
and both sealed raw ID streams passed independent S3 replay. Evidence:
`s3://borsuk-bench-453182569524-euc1/research/v231-indexed-delta-100k/2678f8ea8de52c7a801dd6dbf1496f2fe4b44521/runs/a0001/`.

Dataset/split: ReLAION-100k D768, development queries 0–255 and
previously used method-held-out queries 256–999, k=100, exact GT100.
The authenticated V218 graph and ef/shortlist 2,048/2,048 were fixed.
Every tenth physical row (10,000 rows) was upserted under the same ID
and vector, leaving the logical corpus and truth unchanged. Each arm
verified unmutated V218 ID replay before measurement. The linear and
indexed arms used the same binary and source revision, run in that
order on one Spot host. Times below are **loaded in-process** query
measurements, not network product latency.

| Verified cell | V218 baseline | V231 linear 10k | V231 indexed 10k |
| --- | ---: | ---: | ---: |
| Development GT100 hits / 25,600 | 25,537 | 25,540 | 25,535 |
| Method-held-out GT100 hits / 74,400 | 74,234 | 74,238 | 74,223 |
| Combined GT100 hits / 100,000 | 99,771 | 99,778 | 99,758 |
| p05 GT100 hits/query | 99 | — | 99 |
| Loaded p50/p90/p95/p99, ms | 5.750/6.673/6.915/7.380 | 21.928/22.903/23.152/23.668 | 6.455/7.547/7.887/8.403 |
| Loaded throughput, queries/s | 1,353.3 | 361.1 | 1,193.7 |
| Peak process RSS, bytes | 212,135,936 | 416,296,960 | 305,995,776 |
| Overlay-owned resident bytes | — | 15,532,500 | 49,584,668 |
| Exact delta rows scored over 1,000 queries | — | 10,000,000 | 256,000 |
| Vector-body GETs | 0 | 0 | 0 |

The indexed arm matched 982/1,000 full linear ID lists; the changed
lists lost 20 net GT100 hits. Its index build took 3.583 s, after
50.2 ms of mutation preparation; linear mutation preparation took
51.1 ms. The HNSW used fixed M=16, M0=32, ef construction=64,
256 candidates and exact FP16 rerank. Both arms masked 205,478
base-shortlist rows across the query panel. Each arm ran in a separate
process on the same host; the large difference in process RSS is
descriptive and cannot be attributed to index allocation alone. The
loaded query percentiles were program-reported from 1,000 timings;
the sealed per-query raw streams contain sequential query timings and
IDs, so those loaded percentiles cannot be independently recomputed
from the raw artifacts. A promoted gate must retain loaded per-query
samples too.

The preregistered quality gate failed; the p95 reduction, ≤60 s build,
≤512 MiB RSS, and zero-GET performance gates passed. The launch-time
Spot quote was $0.3631/hour; estimated compute to closeout was
$0.03209, excluding EBS, S3 and billing adjustments. V229's
1,000-row overlay and V218 are historical context; no S3 Vectors or
Turbopuffer product comparison is inferred.

**Next single gate:** same 100k/10k mutation panel and source graph,
compare the current linear FP16 decode loop with an exact predecoded
FP32 delta scan. Require identical returned ID lists on all 1,000
queries before assessing loaded p50/p90/p95/p99, QPS, resident bytes,
RSS and mutation preparation cost. If exactness holds and p95 falls
materially, promote one 1M network cell versus V230, then test
real-S3 1M mutation publication. No fixed vector-count RAM knee is
adopted; RAM cost belongs with the recall/latency operating point.
