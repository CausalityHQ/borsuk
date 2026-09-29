# Four-slot current ReLAION1M — actual development GO

2026-09-29. Original71692 CLOSED0; independent29363 and90658 CLOSED0. Instance
 i-0157547066e2c9b14 actually terminated469s.395 source files/39 artifacts,
expected RED2vs4, both2HTTP example testsGREEN, compiledsource/releasebinary,
integer quality/IDs/root/head/physical bounds/timing/decision/cleanup verified.
Other394 native files/library2696 tests and five offered protocol checks reused.
Qualified HTTP source2adc14246cda4f7ff2318a985ba61b27e4e7e6db865be67260b0252e4b0d9e49
applied exactly. Binary12,546,280B SHAfe7084ce75e56788f156639f534d41fe9fb576ce771bc6f9f5094a1f20264f82.
Source87ab5ae926fd4a3b037c031e7e16d8c9286f1705a985b3fb0fc69f32593f6e78,
terminal2d4ed0cbe1f2d506eadba67ad14d087af33665476b4cfa6dff26aa518cfa35ac.

## Measured cells

ReLAION FIRST1M D768 cosine consumed external development0–63, fixed8offered
QPS,64requests/cell, fresh HTTP server each, metadata/source-plane resident,
application SQ8 cache absent, S3 service cache uncontrolled, loopback client.
CLOSED actual native k10/k100 references and S3 namespaces reused from previous
qualified serving; no new GT/source/native publication/query recomputation.

| Repetition | k | All-offered recall | Incoming HTTP p50/p90/p95/p99 ms | Successful QPS | Native max RSS KiB |
|---|---:|---:|---|---:|---:|
| 0 | 10 | 635/640=99.21875% | 110.538043 /302.7780583 /449.20857815 /572.19624164 | 8.0 | 400248 |
| 1 | 100 | 6318/6400=98.71875% | 109.685598 /112.4303208 /114.82344795 /126.48384054 | 8.0 | 362260 |
| 2 | 100 | 6318/6400=98.71875% | 111.197984 /117.4496405 /136.84866115 /160.01250970 | 8.0 | 362156 |
| 3 | 10 | 635/640=99.21875% | 113.415561 /146.6433489 /180.09771570 /260.02601524 | 8.0 | 362176 |

ALL256requests successful; no503,drop,timeout,integrity or dataGET error.
Each cell1946actual dataGET attempts/1,073,479,680 verifiedB; total7784GET/
4,293,918,720B,all counters complete,perquery32GET/16,773,120B held.
RSS maximum400248KiB; measurement cgroup peak682,176,512B,8GiBlimit/zeroSwap/
zeroOOM,4GiBAS/CPU0–3/fourthreads/1GiB library caller. Compiler memory is
separate. Four admission slots cover native tails while limiting allocation,
with at most128dataGET/67,092,480verifiedB across4active queries.

Both preregistered k10 cells meet all64success,R10>=95%,p90<444ms and achieved
successfulQPS>=8 using full8s window+drain. GO applies ONLY to this consumed
current1M development protocol. Firstcell p95=449.21ms would miss the older
400ms stretch diagnostic; it is reported, not hidden by a median or retroactive
threshold change. Previous1slot/2slot offered-loadFAILs and original strict
source-completion1MR100flat-gapFAIL remain immutable.

## Baselines, limits and next decisive gate

Same-source CLOSED offline R100control6288/6400=98.25%,flatSQ86363/6400=
99.421875%: candidate+.46875pp vscontrol,−.703125pp vsflat. These are quality
baselines, not matched1M control HTTP. Previous independent Spot single-slot
59/64success7.375QPS/p90116.83ms and2slot58/64success7.25QPS/p90245.00ms are
engineering history, not a controlled causal latency estimate for4slots.

Both k10p90s fall below Turbopuffer published1MD768444ms context under declared
differences. No matched vendor measurement/claim: our source/router metadata
resident; namespace hydration excluded from request timing and S3 caches/WAN
not matched. The external8QPS figure is published on10MD1024, not this1MD768.
AWS subsecond cold remains directional without vendor percentile/QPS guarantees.
Warm and saturation are unmeasured. Compute$0.0233 ESTIMATE from Spot quote/wall
excludesEBS/S3,notinvoice/query or total lifecycle cost.

Next: finish query-source identity exclusions and preregister a representative
fresh1M k10/k100 panel using this exact qualified library/binary and protocol;
include incoming HTTP, cold namespace open/hydration separately, failed offers,
physical counts/RSS and declared total cost. Then CoHere1M, rate/saturation curve,
10M/100M bounded scale, incremental/pin/recovery/compaction/GC lifecycle and
protocol-matched product qualification. No more admission bumps or architecture/
review cycles for this development result. No operator decision needed now.
