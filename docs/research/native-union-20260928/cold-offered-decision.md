# Namespace-cold offered curve, 2026-09-30

**Campaign terminal FAIL; complete records independently authenticated.**
The post-measurement capture helper expected8GiB while the frozen protocol
used7GiB. Preserve statusfailed/exit96. All12cells had closed before this
assertion; independent verification confirms the declared7GiB cgroup, zero
swap/OOM,768offers/628successes/140capacitydrops/zero transport errors.
This evidence is not a passing campaign or matched vendor win.

Original controller36593 CLOSED1, Spot i-02058168e768f0739 terminated1109s.
Independent failed-closeout verifier85705 CLOSED0: valid_closed_records=true,
valid_measurement=false, campaign_terminal_passed=false. Source9f1abe4e,
archive73b6542e061af216978b6c6669db23edbf770514b29a2bb99dd223c6a1e91510,
terminal43be3029353a7689336e35ebf0e036bce77fe732fa5cc5998a42630ce595aa6d.
All41artifacts/source395/compiled8/binary/frozen18/config/inputs authenticated.
Native binary reused, no rebuild or full assurance repetition.

FIRST1M D768 cosine k10, previously observed development0–63, ReLAION then
CoHere at each rate,64offers/cell. Six admitted fresh processes maximum,
ports18080–85, no client queue/retry/warmup/application SQ8cache. Samehost
loopback plainHTTP; client4–5/native0–3/Tokio4; S3 cache uncontrolled.
Timer spans process launch through full response; scheduled tails also recorded.
CoHere training-query rows1,005,000–1,005,999 unsuitable for disjoint full10M.

| Offered QPS | Dataset/split | Success/drop of64 | Successful R10 | Cold p90/p95 ms | Actual successful/full-span QPS | All-offer quality gate |
|---:|---|---:|---:|---:|---:|---|
|0.25|ReLAION development0–63|64/0|99.375000%|2063.683/2090.053|0.251938|PASS|
|0.25|CoHere development0–63|64/0|96.875000%|2028.528/2045.404|0.251936|PASS|
|0.5|ReLAION development0–63|64/0|99.375000%|2053.110/2096.138|0.499950|PASS|
|0.5|CoHere development0–63|64/0|96.875000%|2021.743/2053.561|0.499973|PASS|
|1|ReLAION development0–63|64/0|99.375000%|2037.488/2056.265|0.984663|PASS|
|1|CoHere development0–63|64/0|96.875000%|2016.007/2032.603|0.984857|PASS|
|2|ReLAION development0–63|63/1|99.365079%|2503.376/2708.289|1.875325|FAIL|
|2|CoHere development0–63|63/1|96.825397%|2275.847/2801.222|1.875091|FAIL|
|4|ReLAION development0–63|37/27|99.459459%|2460.598/2679.767|2.077397|FAIL|
|4|CoHere development0–63|40/24|97.250000%|2438.875/2516.952|2.228805|FAIL|
|8|ReLAION development0–63|21/43|99.047619%|2521.053/2607.067|2.116729|FAIL|
|8|CoHere development0–63|20/44|97.000000%|2813.687/2888.144|1.995387|FAIL|

Table values measured and independently verified within the failed closeout.
Latency and recall above are success-conditioned; dropped offers stay in
completion denominators. Largest tested all-offer operating point:1QPS BOTH
datasets, unchanged99.375%/96.875% R10 and0pp against immutable ordered-ID
references. At2QPS each dataset drops1/64; at8QPS drops43/64 and44/64.
Do not present the ~2 completed/full-spanQPS under overload as8QPS attainment.
No R100 or universal saturation maximum; all444ms context gates FAIL.

Relevant serial ranges baseline (different campaign/source epoch; not matched
load control): coldp90=2164.242/2202.817ms, R10=99.375%/96.875%. At1QPS
current p90=2037.488/2016.007ms. No causal latency delta inferred across these
campaigns. Offered-load source changes only Python protocol, native unchanged.

Profile cgroup peak3,470,827,520B under7GiB; process peak≤372,060,160B, zero swap/OOM.
Compute$0.054957EST from1109s at observed$0.1784/hour;
excludesEBS/S3, not invoice or total-dollar result. Successful logical startup
requests37metadataGET+9HEAD plus2controlGET from source; native SDK retries
explicitly disabled. Physical SDK transport and unsuccessful request cost
remain unmeasured; metadata bodies ~256.7MB per admitted namespace.

Root causes/decisions: (1) capture helper hardcodesprofile8GiB: fix the shared
helper with explicit declared-limit argument, preserve legacy8/10GiB defaults;
(2) six slots fill under≥2QPS: preserve drops and the actual1QPS operating
point; inspect measured staging/decode and size the next bounded intervention
from these records, rather than silently raising admission or queuing offers.
Retain the valid ranged-transfer candidate; this does not kill its architecture.

Next: finish resource-helper regression and publish this failed-closeout evidence;
then one causal startup reduction or explicitly priced larger concurrency envelope
with fresh fixed gates. No rerun solely to obtain a green terminal marker.
Remaining BOTH-vendor gap: ~2s cold latency vspublished444ms context,8QPS
all-offer completion, complete cost, protocol-matched vendor evidence,10M/100M
bounded scale and incremental/generation-swap/recovery lifecycle.

[Independent failed-closeout verification](cold-offered/a0001/verification.json),
[Measured phase/cost reduction](cold-offered/a0001/phase-and-cost.json),
[Frozen protocol](cold-offered-preregistration.md).
