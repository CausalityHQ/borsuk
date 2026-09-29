# CoHere FIRST1M prospective936 confirmation GO

2026-09-29. Unchanged source-only v4 roota4eb4851, qualified native/four-slot
HTTP binaries and fixed CoHere FIRST1M D768 cosine source. Fresh query source
rows1,005,000–1,005,999; prospective original ordinals64–999, no tuning.
Independent verifier35862 closed0: authenticated archive/terminal/artifacts,
sealed suffix/request/source-ordinal parity, real k10/k100 IDs, integer recall,
every timing/physical reduction, resource limits and actual termination.
Controller69115 closed0; Spot`i-0667fe5cc57cd32cb` **terminated**,965s elapsed.
Terminal`8c2eb4a2…`, archive`410c3e09…`, source commit`44b04832`.

**Verified** R@10 **9,048/9,360=96.6666667%** and R@100
**88,079/93,600=94.1014957%**, identical native and HTTP IDs. All four
936-offer cells succeeded fully:3,744/3,744 overall at8.0 successful QPS each.

| Rep | k | p50 ms | p90 ms | p95 ms | p99 ms | Successful QPS |
|---|---|---|---|---|---|---|
| 0 | 10 | 110.708 | 112.913 | 113.949 | 125.094 | 8.0 |
| 1 | 100 | 110.563 | 112.841 | 113.717 | 125.174 | 8.0 |
| 2 | 100 | 110.701 | 113.245 | 114.567 | 130.101 | 8.0 |
| 3 | 10 | 110.563 | 112.931 | 114.109 | 130.072 | 8.0 |

**GO** against the prospective preregistration: R@10>=95%, both k10
p90<444ms, all offered requests successful and>=8 successful QPS. R@100 is
reported separately; it would miss the historical98% stretch diagnostic,
which is not this prospective product gate. No retrospective threshold change.

Metadata resident, no application SQ8 cache, S3 service cache uncontrolled,
loopback HTTP, unfiltered. Namespace readiness4.71–4.81s is separate from
query timing. No measured vendor/control delta. Published TP1M D768 cold
p90=444ms and practical95% R@10 are [comparison context](published-product-targets-20260929.md),
with cache/transport differences. AWS90%+average/subsecond claims do not
provide matched p95/QPS. This result is **not namespace-cold end-to-end**.

119,556 GET and62,796,764,160 verified bytes across3,744 responses; every
response <=32 GET and<=16,773,120 verified bytes, zero failed data GET.
Cgroup peak4,116,770,816B<8GiB, zero swap/OOM,4GiB address-space cap and
four CPUs/threads. Compute$0.0480 is an **estimate excluding EBS/S3**, not
an invoice or total cost. Source construction and exact-GT costs are separate.

Descriptive combined fixed1000 queries: R@10=9,668/10,000=96.68%,
R@100=94,126/100,000=94.126%. Keep dev64 and prospective936 as separately
frozen gates. Scoped duplicate checks passed; complete historical freshness
remains unproven. Original dev startup failurea0001 is retained independently.

Next decisive measurement: [peer client](peer-transport-preparation.md) with
explicit server/client identities and cold-state declarations; measure
namespace startup separately. Closed dev native headers already show remote
metadata open4.487/4.511s, a real namespace-initialization gap. No discovery/
nomination/scoring intervention is justified by this passing R@10 panel.
Remaining: saturation/total cost,10M/100M memory/scale and generation swaps,
lifecycle mutation/pinning/recovery/compaction/GC and BOTH vendor comparisons.
The fixed train-query panel lies inside full CoHere10M: do not reuse it as a
disjoint10M split; freeze a separate split before that scale gate.
