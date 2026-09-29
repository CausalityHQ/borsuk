# Practical published product targets

Operator correction authoritative; primary pages independently checked 2026-09-29 UTC.
These are vendor reports, not BORSUK reproductions or guarantees. Active frozen
experiments retain their original strict verdicts. This document changes no active
controller, scorer, work limit, identity gate or result.

|Reference|Published quality|Published cold latency|Published throughput|Unknowns / differences|
|---|---|---|---|---|
|[AWS query docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-query.html)|90%+ average recall for most datasets|Subsecond directional statement|Unknown|k, corpus, split, percentiles, concurrency, recall-specific configuration not given|
|[Turbopuffer concepts](https://turbopuffer.com/docs/concepts)|Aims90–95% recall@10, including filters|No benchmark here|Unknown|General target, not every-query guarantee|
|[Turbopuffer blog](https://turbopuffer.com/blog/turbopuffer), updated2026-03-05|Benchmark recall unknown|1M D768 (~3GB), p90=444ms|Unknown|p95, k, query split, protocol/concurrency and price unknown|
|[Turbopuffer homepage](https://turbopuffer.com/)|Benchmark recall unknown|10M D1024 (~40GB), topk10: p50=874,p90=1214,p99=1686ms|8QPS stated benchmark approach|p95, corpus/query identity, region/cold protocol and price unknown|

Preregister next representative fixed panel: mean recall@10>=95%, report recall@100
separately; 1M D768 cold incoming p90<444ms under disclosed BORSUK protocol;
10M D1024 p90<1214ms as published comparison context; AWS subsecond directional
cold target where applicable. Reproduce/exceed8QPS at declared quality and latency,
then measure saturation and total cost curve. These are practical initial goals,
not inferred vendor guarantees or a matched vendor win.

Warm is separate: blog1M p90=10ms; homepage10M p50/p90/p99=14/17/27ms;
AWS warm can be as low as100ms. Never require warm targets of no-cache cold runs.
Do not infer p95 from p90, QPS from reciprocal latency, universal100% from an
advertised recall range, or namespace cold throughput from global25000QPS or
namespace5000+ limits. Do not transplant1M444ms to100M.

Current98% recall@100,p05>=95,flatgap<=32/6400,250/400ms are preregistered
engineering/stretch diagnostics. Strict FAIL remains FAIL; it can end that strict
arm without establishing product nonviability. A95–97% candidate requires the new
fresh product panel rather than automatic architecture rejection. No retroactive
pass, tuning on rejected seals, or weakening correctness/source/control parity.
100M remains bounded-resource/scalability requirement; derive latency from measured
scale curve. Fresh identity, maintenance/pins/recovery and lifecycle dollars remain
required. Published-target attainment is a useful milestone; absent vendor account
access does not prevent a useful library release. Claim only attainment of reported
targets with disclosed differences until matched vendor measurements exist.

Current BORSUK100k evidence is k100, consumeddev0–63, loopback serial HTTP/resident
router/no client SQ8 cache; not a representative fresh k10 or matched8QPS test.
No current1M HTTP,10M,100M or total lifecycle dollar evidence has been accepted.
