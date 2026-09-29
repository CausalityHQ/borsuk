# Startup profiling decision, 2026-09-29

Verified diagnostic; no ANN queries, first-query measurement or vendor comparison.
Frozen source `217a33df`, archive `4a8437ff664adbbd4cf8fb6e09382ac7dc923f886615a8a0be6e7990ab6cf6c0`.
Original controller25025 CLOSED0; independent verifier25471 CLOSED0.
Spot `i-0810780a8cdf0fa5b` terminated after762s; six health-only starts,
interleaved ReLAION/CoHere three times, FIRST1M D768 cosine index metadata.
Query split: none. Each start reads nine metadata objects,256,694,403/256,694,456 B.
No application cache; S3 cache uncontrolled. Three samples per dataset are
exploratory ranges, not population percentile estimates.

| Dataset | Namespace ready ms | Remote open ms | GET headers ms | Stream/output ms | Awaited writes ms (inside stream) | Authenticated load/decode ms |
|---|---:|---:|---:|---:|---:|---:|
| ReLAION |4714.177–4823.857|4501.334–4624.662|233.644–362.312|2420.223–2451.781|73.824–85.735|1792.048–1798.950|
| CoHere |4713.980–4714.631|4504.122–4591.923|245.411–398.368|2372.996–2448.731|89.024–103.790|1793.774–1795.993|

All numbers above are measured and independently verified. The staging total
is2685.884–2802.140ms. In first ReLAION/CoHere starts, plane/records.bin
stream/output is1985.041/1919.653ms; centroids431ms or less. Awaited writes
include executor waits; these are not exclusive disk hardware measurements.
Stream/output includes network and output waits, so CPU/network attribution
within that phase is not measured. Decode includes authentication, file reads,
centroid/graph decoding and validation; its finer attribution is unmeasured.

Build cgroup peak7,034,101,760B<10GiB; profiling639,819,776B<8GiB,
zero swap/OOM, four CPUs and4GiB address-space limit. Focused native tests
(object staging, generation integration, HTTP example) and locked ARM release
passed. Historical unaffected2696-test assurance is reused; no claim of a fresh
current full-workspace run. Observed Spot quote$0.1784/hour: compute ceiling
using762s is$0.037762 estimated, excludes EBS/S3 and is not invoiced cost.

Decision: buffered output alone addresses only74–104ms, so do not prioritize
it. The largest observed phase is streaming the200MB resident plane. Source inspection found an additional concrete cause: sha2 0.10.9 selects
its software SHA-256 backend on aarch64 unless its asm feature is enabled.
The resolved feature tree has only default/std. Authenticated plane loading
hashes200MB, and centroid/graph loading hashes the48MB centroid blob repeatedly.
Attribution of the1.8s load/decode cost to hashing is an inference, not measured.
Next causal experiment: enable the existing dependency hardware backend, with
runtime CPU detection and software fallback, leaving identity checks, transfer
schedule and scorer unchanged. Freeze this instrumented sequential run as
diagnostic control; measure a fresh matched control/candidate panel before any
improvement claim. Bounded parallel transfers remain subsequent work if needed.
True cold first-query, quality parity, saturation, total cost,10M/100M scale,
incremental lifecycle and matched BOTH-vendor comparisons remain open.
Historical uninstrumented peer query results are context, not a matched
startup performance baseline. No product-quality or comparator GO is awarded.

[Independent verification](startup-profile/a0001/verification.json).
