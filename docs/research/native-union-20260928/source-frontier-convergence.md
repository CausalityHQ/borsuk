# Current convergence checkpoint

2026-09-29. Latest results below are verified CLOSED measurements. Source and
scorer remain frozen; only the qualified HTTP example uses four bounded slots.
The original BOTH-vendor production goal remains active.

| Baseline | Current candidate / actual quality delta | Measured latency, throughput, resources and cost | Remaining BOTH-vendor gap | Next decisive gate |
|---|---|---|---|---|
| ReLAION FIRST1M D768 cosine, consumed external development0–63. Same-source offline R@100 control6288/6400=98.25%; flatSQ86363/6400=99.421875%. No matched1M HTTP control. | Each k10 repetition635/640=99.21875%; each k100 repetition6318/6400=98.71875%. R100 delta+.46875pp vs offline control,−.703125pp vs flat. | All256 incoming HTTP requests successful; each cell8.0 successfulQPS at offered8QPS. k10 reps p90=302.7780583/146.6433489ms, p95=449.20857815/180.09771570ms; k100 p90=112.4303208/117.4496405ms. Process maxRSS400248KiB; measurement cgroup peak682176512B. Total7784 actual dataGET /4293918720 verifiedB, zero failedGET. Worker compute$0.0233 **estimate**, excludes EBS/S3; lifecycle$/query unmeasured. | **GO consumed development1M only.** No fresh representative panel, CoHere1M, warm/saturation,10M/100M, lifecycle or paired vendor qualification. | Close query-bank/source exclusions; preregister wholly separate quality-blind1M queries, meanR10>=95%, R100 separate. Reuse exact qualified native code/binaries for incoming coldHTTP, namespace hydration, offered rate/cost curve. |

Fixed order[k10,k100,k100,k10],64 requests per cell, fresh HTTP process per cell,
8 workers and5s request timeout. Data fetches use no application SQ8 cache;
source/router metadata is resident, namespace hydration is excluded from request
timing, S3 service cache is uncontrolled, client is loopback with separate
connections. Both k10 repetitions pass the frozen development gates: all64
successful, R10>=95%, p90<444ms and successfulQPS>=8. First k10 p95 misses the old
400ms engineering stretch. Prior strict R100, one-slot and two-slot FAILs remain
immutable; the current candidate remains product-eligible.

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
compute$0.0264 **estimated** excluding EBS/S3. **Complete all-history
query-artifact closure remains false.** No fresh ANN recall or cold HTTP result
exists; reject all1,000 IDs if later authenticated input proves reuse. Next
run the frozen fresh development0–63 native quality and offered8QPS gate;
prospective64–999 stay sealed.

Recent code/measurement evidence: [four-slot decision](four-slot-1m-offered-http-decision.md),
[frozen strict1M decision](source-completion-1m-decision.md),
[prior two-slot decision](two-slot-1m-offered-http-decision.md),
[prior one-slot decision](current-1m-offered-http-decision.md).
Unchanged2696-pass core assurance and five offered-protocol checks are reused.
All original cloud jobs are closed and their instances terminated. No new paid
job, admission bump, architecture cycle, review or operator decision is needed
for the current metadata work.
