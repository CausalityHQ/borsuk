# Two-slot ReLAION1M serving — bounded causal development fix

2026-09-29. Predecessor current-1m-offered-http/a0001 is valid FAIL: single
HTTP/library slot caused5HTTP503 at offered8QPS,59success/7.375QPS, successful
p90/p95116.8302302/127.8074947ms. Actual native R@10=99.21875%,R@100=98.71875%.
Same FIRST1M D768 cosine consumed external development0–63 panel; no fresh,
matched vendor/control HTTP, saturation or lifecycle qualification.

One causal change: shared `QUERY_SLOTS=2` for HTTP preallocation nonqueued
admission and library max_active_queries. Source/routing/scorer/GT/topk/root/
head/gen1/epoch1/perquery32GET16773120B/1GiB caller/CPU0–3/fourthreads unchanged.
Library already charges simultaneous payload and supports bounded concurrent
queries; no library modification or repeated full2696 gate. Each of2slots
holds until HTTP handler returns. Third arrivals receive503 before JSON/ANN;
no queue/retry/cache. Up to64 simultaneous native data GETs/33,546,240 verified
query bytes across2active queries, bounded by library memory and cgroup.

AWS-only focused HTTP example: expected assertion RED with sharedconstant1,
change only that literal to2 on worker, both existing example tests GREEN,
release build; retain exact compiled source and SHA-bound binary. Admission
test takes2permits,rejects3rd,releases/replacesone,rejects3rdagain, then returns
both slots. Reuse unchanged394 other native hashes/currentnative reference
and2696 library authority; reuse five unchanged offered protocol checks.

Then reuse CLOSED actualk10/k100 native reference records and immutable
published namespaces from current-1m-offered-http/a0001. No repeated native
query/publication/GT/source normalization or canonical3.08GB download. Head
still checked at every HTTP startup. Frozen config binds all inputs/authorities
and new Python wrapper; existing qualified lifecycle/measurement helper reused.
Repetitions[k10,k100,k100,k10],64queries each, fresh server, metadata resident,
application SQ8 cache absent. S3 service cache uncontrolled, potentially warm
from prior requests; client loopback separate connections, offered8QPS/8client
workers/5s timeout. Preserve all offered outcomes, source/IDs/counters parity,
all-offered/successful recall,tails,drain QPS,RSS/GETbytes/counter completeness.

Fixed same gate: every k10 cell all64success,all-offeredR@10>=95%,successful
incomingHTTPp90<444ms andsuccessfulQPS>=8 using8swindow+drain. First failedk10
ends remaining cells; GOrequires bothk10cells. k100 separate context. No
retroactive predecessor pass or oldstrictR@100diagnostic conversion. Candidate
remains eligible if servingfails; classify actual bottleneck. External444ms
is published1MD768p90 context;8QPSexternal is a different10MD1024protocol,
so no matched reproduction claim. Reference R@100 offlinecontrol98.25%,flat
99.421875% are quality-only context. One-slot actual HTTP is a matched current
engineering baseline, explicitly a separate frozen revision/admission bound.

One causality eu-central-1 c7g.2xlargeSpot/80GiB encrypteddisposable gp3;
2700s whole worker hard shutdown,1800s compiler1830s service/10GiBcgroupzeroSwap,
600smeasurement630s service/8GiBcgroupzeroSwap4GiBASCPU0–3. Maxquote$.30/hour,
compute ceiling$.225 plus$.15EBS/S3 allowance; no OnDemand fallback. No local
native/numerical tests. Controller immutable reservation/globalworker guard;
no overlappingjob. Completed repetitions syncS3 immediately, terminal syncs
bounded artifacts, actualtermination confirmed before independent verification.
Spot interruption discards cell; no automaticreplacement. Verify RED/GREEN/
compiledbinary/unchangedcore then integerGT/parity/timing/counter/decision/RSS.
Compute estimates excludeEBS/S3,notinvoice/totalcost. No operator decision needed.
