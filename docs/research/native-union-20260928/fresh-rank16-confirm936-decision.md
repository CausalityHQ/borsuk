# ReLAION FIRST1M prospective confirmation: GO

2026-09-29. Frozen original rank16 query ordinals64–999 were opened only after
the protocol at commit `3860a377` was pushed. The original Spot attempt
`fresh-rank16-confirm936-a0001` completed, independent closed-artifact
verification passed, and EC2 `i-0bf7f63baa89c2e7b` is terminated. Source,
root `d8e7ccf…`, scorer, native reference binary `a2a9ca63…`, four-slot HTTP
binary `fe7084ce…`, exact f64 GT100, query order, physical caps and gates
were unchanged from the fresh development run. No prospective query was used
to tune them.

Dataset and split: ReLAION FIRST1M D768 cosine, **fresh rank16 prospective
64–999** (936 queries). Native and HTTP R@10=9,239/9,360=**98.7073%**;
R@100=92,206/93,600=**98.5107%**. The relevant preregistered floor was
95% mean R@10, so this candidate is **+3.7073 percentage points above that
floor**. This is not a delta against a matched fresh control or vendor.
The earlier fresh development0–63 result was R@10=99.375% and
R@100=98.421875%; the full fixed1,000-query panel descriptively totals
R@10=98.75% and R@100=98.505%. Historical consumed control and flat
results refer to a different query split.

Four fresh HTTP processes ran fixed `[k10,k100,k100,k10]`,936 offers per
cell,8 offered QPS,8 workers and5s request timeout. All3,744 offers
succeeded with zero503, client drops, timeouts, invalid identity or failed
data GETs. The independent verifier recomputed every native GT hit, HTTP
returned ID/plan/hit, physical count and incoming percentile from retained
records and sealed S3 ranges.

| Rep | k | All-offered recall | Incoming p50/p90/p95/p99 ms | Successful QPS | Namespace ready ms | Server max RSS KiB |
|---|---:|---:|---|---:|---:|---:|
|0|10|98.7073%|110.1623 / 112.8224 / 115.7277 / 144.8444|8.0|4712.782|390088|
|1|100|98.5107%|110.3549 / 113.7042 / 119.0015 / 145.8683|8.0|4912.305|399320|
|2|100|98.5107%|110.5050 / 113.2987 / 121.2015 / 146.5879|8.0|4811.898|402108|
|3|10|98.7073%|110.3789 / 116.9879 / 136.3771 / 176.5958|8.0|4811.927|398148|

Each cell used27,926 actual data GETs and15,697,843,200 verified bytes,
or29.835 GET and16,771,200B per query; all per-query32GET/16,773,120B
caps held. Total111,704 GETs and62,791,372,800B. Measurement cgroup
peak4,114,006,016B of8GiB, zero swap/OOM; process max RSS402,108KiB.
The namespace-ready 4.71–4.91s is separate from request latency.
Source/router metadata was resident, no application SQ8 cache was used,
S3 service cache remained uncontrolled, and the client used loopback
connections. The Spot machine ran951s; compute$0.0473 is an **estimate**
excluding EBS/S3, not measured lifecycle$/query or an invoice.

**Decision:** GO on the frozen prospective gate: native R@10>=95%, both
k10 cells all936 successful, all-offered R@10>=95%, incoming p90<444ms,
and successful QPS>=8. Turbopuffer's published1M D768 cold p90=444ms is
comparison context under different source/cache/region/protocol, not a
paired vendor result. No fresh matched control, vendor p95/QPS comparison,
S3 total bill, saturation/cost curve, CoHere,10M/100M or full mutation,
pin/recovery/compaction/GC lifecycle is established by this run. The
all-history query-artifact audit remains incomplete; the scoped selected-ID
lineage proof supported this panel and must be revised if authenticated reuse
appears. Historical strict R@100, one-slot and two-slot FAILs remain intact.

Next gate: build an independently sourced CoHere fixed split and repeat
native/HTTP quality, then measure a reproducible10M cold protocol and
saturation/cost curve on the exact object-native library revision. A
published-target milestone is possible under disclosed protocol; a claim
of beating either vendor requires paired reproduction. Receipts and the
independent result are in
[`fresh-rank16-confirm936/a0001/verification.json`](fresh-rank16-confirm936/a0001/verification.json).
