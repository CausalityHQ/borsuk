# V133 locally hydrated SQ8: 100k development placement decision

V133 completed one immutable Causality Spot cell on `c7i.8xlarge` in
`eu-central-1c`, instance `i-0445873b347142877`, in 275 seconds. Its source
commit is `e6e99b58ec1e47a8445e7f2f3a96bb7407766246`; source archive
SHA-256 is
`94723a431e332bb91c90a0a39b1d9c8db283ff285c631ba218326125d777a16f`
(11,658,465 bytes). The complete exit-0 terminal at
`s3://borsuk-bench-453182569524-euc1/research/v133-local-sq8/e6e99b58ec1e47a8445e7f2f3a96bb7407766246/runs/v133-20260924T070622Z/a0001/terminal.json`
has SHA-256
`7fee39326963565f3c08df38ba702187b7a546ae4d96838ec4e4a879e3b5bbad`.
The instance was independently observed **terminated**. All 12
terminal-listed artifacts (1,838,876 bytes) were independently read back
from S3 and matched their recorded lengths and SHA-256 values. The
[preregistration](v133-local-sq8-transport-prereg.md) froze the decision
before this run.

Dataset: **deep-image-96-angular random 100k train subset**, with 1,000
already-used **publication-test ordinals 9000–9999** and GT100 recomputed
within the subset. Both arms used V122's same fixed physical ranges, 512
nominee rows, returned SQ8 top-512 expansion, source-ID map and exact F32
source cosine ranker. Arm order alternated by query. The SQ8 object was
downloaded and authenticated at startup, then page spans were read from a
pinned local file with 4 KiB block and page SHA-256 verification on every
query. This is a development placement replay, not a live router/planner or
concurrent service benchmark.

| Measured metric, 1,000 serial queries | Candidate | Paired capped control |
| --- | ---: | ---: |
| Exact-source Recall@100, hits / 100,000 | **99.942%, 99,942** | 98.827%, 98,827 |
| Complete replay p50 / p95 / p99, ms/query | 44.180 / **47.285** / 48.158 | 19.561 / **24.104** / 25.600 |
| Local SQ8 read p50 / p95 / p99, ms/query | 13.231 / 14.435 / 14.606 | 1.930 / 3.835 / 4.390 |
| Exact source rank p50 / p95 / p99, ms/query | 15.122 / 15.989 / 16.656 | 14.996 / 15.595 / 15.845 |
| Local SQ8 range reads | 1,000 | 24,674 |
| Local SQ8 bytes read | 9,428,126,976 | 1,535,701,248 |
| Verified local SQ8 4 KiB block reads | 2,302,408 | 393,102 |
| Authenticated source logical bytes / block reads | 23,653,893,440 / 361,040 | 23,443,671,808 / 357,828 |

The candidate won 335 paired source-hit queries, tied 665, and lost none.
The maximum per-query SQ8 read was 10,800,000 bytes and one range for the
candidate; 3,760,128 bytes and 32 ranges for the control. All 2,000 arms
respected the 32-range and 16,777,216-byte caps. Preparation staged 17
authenticated inputs (69,718,777 downloaded bytes) in 8.241 seconds,
outside the per-query timings; startup authentication inside evaluation took
another 0.397 seconds. The dedicated serving cgroup peaked at **113,475,584
charged bytes** (108.2 MiB), including file cache. Its after-exit current
charge was 73,367,552 bytes, mostly remaining cache. This is a 100k replay
peak, not a 1M/100M serving-RAM bound.

Independent closeout parsed all 1,000 rows of `replay.jsonl` (SHA-256
`c8a76409cd218935598fda21695398e4095fbcff41351725d69a48a6d5ecd684`),
verified contiguous query ordinals and source ordinals 9000–9999, unique
top-100 IDs, every cap, all summed counters, nearest-rank p50/p95/p99 and
paired outcomes against `summary.json` (SHA-256
`d02fd7e92d9e67a95cddf1c3383500aea709859f26a4c184618dc441f215700f`).
On every query in both arms, ordered source IDs, hit counts, candidate counts,
source logical bytes and source block reads were identical to the separately
authenticated V131 and V132 replays. The physical range counts and SQ8 bytes
were identical to V132's live S3 replay; only placement changed. The focused
production-reader test also passed on a separate terminated Causality Spot
instance `i-0e2da56b06e576080`, terminal SHA-256
`bf6fb24ba3bdd576c78a637728384f2c1cd927e4195bb77335d6889e5b31d77f`:
RAM/file page parity, byte and page cap rejection, and late file corruption
detection. All three test artifacts matched their terminal digests.

**Decision: promote locally hydrated SQ8 placement to the end-to-end 100k
gate, without freezing production defaults.** Candidate p95 / same-run local
control p95 was **1.9617**, below the preregistered 2.0 screen, and candidate
p95 was below both the preregistered 75.624654-ms V132 control threshold and
half V132's 152.858292-ms live S3 candidate p95. V133's candidate is 3.23
times faster at p95 than V132's S3 candidate in these separate runs on the
same instance type. The within-run 2× margin is only 0.923 ms; repeatability
must be tested at the next end-to-end gate rather than inferred from one cell.
The placement avoids query-time S3 GETs by paying startup hydration and local
storage; it is not a selective S3 GET win. The immutable S3 generation remains
the authority and the file is a replaceable authenticated serving copy.

The exact `N × (D + 12)` SQ8 byte payload grows smoothly with row count:
10.8 GB for 100M D96 and 78 GB for 100M D768 per generation. This payload
formula is not a measured charged-RAM, SSD-I/O, latency or cost result. The
file placement has no vector-count quality switch, but OS cache, source tier,
router, generation overlap and concurrency still require measured memory and
cost envelopes. The current flat PQ64 router's V121 9.99M offline nomination
was 184.76 ms/query; V133 excludes it. Next, freeze and measure complete
router/planner plus concurrent local serving at 100k, then one generic layout
and resource policy on fresh deep-image and ReLAION-1M builds. The router
must be changed materially before 10M/100M latency can be claimed. No S3
Vectors or Turbopuffer paired comparison was made here.
