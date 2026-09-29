# Fresh ReLAION FIRST1M development: native and HTTP GO

2026-09-29. Original Spot attempt `fresh-rank16-dev64-a0002` completed under
commit `8afdb8f1`; independent closed-artifact and integer/timing verification
passed. EC2 `i-03b77e1d45413297a` is terminated. The earlier a0001 ended
before any ANN query because its local generation metadata was incomplete;
that separate infrastructure failure remains in
`fresh-rank16-dev64-a0001-infra-decision.md`.

Frozen dataset/split: ReLAION FIRST1M D768 cosine, rank16 external **fresh
development ordinals0–63**, source/raw/root/scorer and retained native binaries
unchanged. Only the exact first64 byte ranges of sealed query, request and GT
objects were read. Prospective64–999 remained sealed. Native candidate
R@10=636/640=**99.375%**, R@100=6299/6400=**98.421875%**. No matched fresh
control was measured. The relevant practical floor was95% mean R@10:
candidate+4.375 percentage points above that floor. Consumed dev64 control
R@100=98.25% and flat SQ8=99.421875% refer to a different query split and
are **stale context**, not fresh deltas.

Four fresh HTTP processes ran fixed `[k10,k100,k100,k10]`,64 offers each,
8 offered QPS/8 client workers/5s timeout. Each used current four-slot
native serving, gen1/epoch1, metadata resident, no application SQ8 cache,
fresh namespace and a loopback client. S3 service cache was uncontrolled;
namespace readiness was measured separately from request latency.

| Rep | k | All-offered recall | Incoming HTTP p50/p90/p95/p99 ms | Successful QPS | Namespace ready ms | Native max RSS KiB |
|---|---:|---:|---|---:|---:|---:|
|0|10|636/640=99.375%|113.833834 /124.6767233 /139.2232651 /159.0735890|8.0|4813.566|368264|
|1|100|6299/6400=98.421875%|112.593562 /118.6747721 /140.6514049 /200.8653766|8.0|4712.025|366456|
|2|100|6299/6400=98.421875%|110.979737 /114.0201420 /115.2938495 /122.8226687|8.0|4712.338|367108|
|3|10|636/640=99.375%|112.357428 /115.9056346 /118.6152459 /128.6511137|8.0|4711.442|376420|

All256 offers succeeded; no503, client drop, timeout, invalid identity or
failed data GET. Each cell had1,927 actual physical GETs and1,073,479,680
verified bytes; total7,708 GETs and4,293,918,720B. Every returned ID, plan,
byte count and hit was independently reduced against the corresponding CLOSED
native reference and sealed exactGT prefix; per-query32GET/16,773,120B caps
held. Measurement cgroup peak4,000,931,840B of8GiB, zero swap/OOM,
process maxRSS376,420KiB. Spot compute$0.0159 **estimate**, excludes EBS/S3
and is not lifecycle$/query or invoice cost.

**Decision:** GO on the preregistered fresh **development** gate: native
R@10>=95%; both k10 HTTP cells all64 successful, offered R@10>=95%,
incoming p90<444ms and successfulQPS>=8. The444ms is Turbopuffer's published
1M D768 cold p90 context under different source, cache, region and protocol;
it is not a measured vendor win. AWS subsecond cold is directional. No
fresh matched control HTTP, saturation/cost curve, CoHere,10M/100M, mutation/
pin/GC lifecycle or vendor pairing exists. Prior strict consumed R@100,
one-slot and two-slot FAILs remain immutable.

Next decisive gate is the already selected prospective ordinals64–999 under
the exact same root, binaries, scorer, product thresholds and offered protocol,
with no tuning from those queries. Confirm the practical fresh mean R@10 and
report R@100 separately, cold HTTP tails, QPS, hydration, physical counts,
bounded RSS and cost. Then CoHere and scale/lifecycle. Exact receipts are in
[`fresh-rank16-dev64/a0002/verification.json`](fresh-rank16-dev64/a0002/verification.json).
