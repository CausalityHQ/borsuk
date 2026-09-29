# Current convergence checkpoint

2026-09-29. Latest results below are verified CLOSED measurements. Source and
scorer remain frozen; the qualified HTTP example uses four bounded slots.
The original BOTH-vendor production goal remains open.

| Baseline | Current candidate / actual quality delta | Measured latency, throughput, resources and cost | Remaining BOTH-vendor gap | Next decisive gate |
|---|---|---|---|---|
| ReLAION FIRST1M D768 cosine, **fresh rank16 prospective64–999**; practical competitor-based floor mean R@10>=95%. No matched fresh control. Consumed R@100control98.25% and flat99.421875% are stale/different split. | Native/HTTP R@10=9239/9360=98.7073%, +3.7073pp vs stated floor; R@100=92206/93600=98.5107% separately. No actual fresh matched-control delta. | All3,744 offers success at8.0QPS per cell. k10 incoming p90=112.8224/116.9879ms, p95=115.7277/136.3771ms; namespace ready4.71–4.91s separate. 111,704 actual GET/62,791,372,800B; RSSmax402108KiB, cgroup peak4,114,006,016B. Spot compute$0.0473 **estimate**, EBS/S3/lifecycle unknown. | **GO prospective confirmation.** Published Turbo1MD768 coldp90444ms is nonpaired context. CoHere,10M/100M, saturation/lifecycle and matched BOTH vendors remain open. | Independent CoHere fixed split, then runnable10M cold/saturation/cost and mutation lifecycle on frozen object-native revision. |

The prospective protocol uses fixed order[k10,k100,k100,k10],936 requests per cell, fresh HTTP process per cell,
8 workers and5s request timeout. Data fetches use no application SQ8 cache;
source/router metadata is resident, namespace hydration is excluded from request
timing, S3 service cache is uncontrolled, client is loopback with separate
connections. Both fresh k10 repetitions pass the frozen development gates:
all936 successful, R10>=95%, p90<444ms and successfulQPS>=8. Prior consumed
development R@10=635/640=99.21875%, R@100=6318/6400=98.71875%, k10
p90=302.778/146.643ms at8QPS are different-split historical evidence. Its
first k10 p95=449.209ms missed the old400ms engineering stretch. Original
strict R100, one-slot and two-slot FAILs remain immutable.
The fresh development0–63 R@10=99.375% and R@100=98.421875% passed its
separate gate before the prospective panel was opened; together the fixed
1,000-query panel descriptively gives R@10=98.75% and R@100=98.505%.

Published comparison context: Turbopuffer1M D768 coldp90=444ms; its published8QPS
homepage workload is10M D1024. These are disclosed differences, not paired vendor
measurements. AWS subsecond cold is a directional target; unknown vendor p95/QPS
remain unknown. No measured vendor win is claimed. Primary sources and dates are
in [the published target matrix](published-product-targets-20260929.md).

## Identity gate progress

CLOSED metadata/source reads now bind V36 validation1000 through V116 to
V154/V198/V279. They are one consumed query bank despite different encodings.
V273–V276 additionally authenticate CoHere raw-source rows100000–103999; V274–V276
add3,000 exclusions absent from the earlier named list. With V277 the contiguous
excluded range is100000–104999. Other known CoHere train ranges1000000–1000999 and
1001000–1001999 and the registered test1000 remain excluded.

All697 inventoried terminal bodies were reauthenticated:371 contain a direct
archive SHA,28 contain nested archive identities,298 lack an archive SHA in the
terminal. Original V114–V116 reservations bind five older archives separately.
Exact Git archive reconstruction closes149 source pairs/158 terminal references;
28 selected V130–V155 original archives were separately verified by terminal SHA.
The V85 archived converter/input checks bind V114's older formatted queries to
the original V36 development bank.

An ID-only scan full-SHA-verified the original16 frozen V36 source objects and
the next16 rank-selected candidate objects. Their3,579,759 and3,578,530 unique
physical feature IDs overlap by5,724. The candidate bank cannot be used whole
as a fresh panel; 3,572,806 IDs remain after excluding the original-bank IDs.
A preregistered ID-only selector chose 1,000 provisional IDs, and an independent
pass confirmed zero selected IDs in the original bank and every candidate row
locator. Further original source/archive and runtime-input audits support a
[scoped selected-ID GO](fresh-rank16-identity-decision.md) for sealed vector and
exact-GT construction. That [one Spot construction cell](fresh-rank16-seal-decision.md)
has now sealed all1,000 query vectors and exhaustive f64 GT100, verified16
candidate shards, exact original1M raw-source parity, three immutable S3
artifact SHA256s and actual instance termination. Cgroup peak5.865GB;
compute$0.0264 **estimated** excluding EBS/S3. The subsequent
[fresh development gate](fresh-rank16-dev64-decision.md) passed on ordinals0–63,
then the frozen [prospective confirmation](fresh-rank16-confirm936-decision.md)
passed on ordinals64–999 with no tuning. **Complete all-history query-artifact
closure remains false.** Reject all1,000 IDs if later authenticated input
proves reuse.

Recent code/measurement evidence: [prospective decision](fresh-rank16-confirm936-decision.md),
[fresh development decision](fresh-rank16-dev64-decision.md),
[four-slot consumed decision](four-slot-1m-offered-http-decision.md),
[frozen strict1M decision](source-completion-1m-decision.md),
[prior two-slot decision](two-slot-1m-offered-http-decision.md),
[prior one-slot decision](current-1m-offered-http-decision.md).
Unchanged2696-pass core assurance and five offered-protocol checks are reused.
Construction, development and prospective jobs are CLOSED; all instances
are terminated. No operator decision is needed for the next fixed gate.
