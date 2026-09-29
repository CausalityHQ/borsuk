# Peer HTTP development decision, 2026-09-29

GO for the frozen transport development gate. All512 offers succeed at8 QPS
per cell with exact ordered-ID/range/counter parity to closed native references.
Both FIRST1M D768 cosine development panels use previously observed ordinals0–63.
No new independent quality confirmation or live vendor win is claimed.

| Panel | R@10 | R@100 (separate) | k10 peer p90 ms (first/last) | k10 p95 ms (first/last) | Frozen native quality delta |
|---|---:|---:|---:|---:|---:|
| ReLAION dev0–63 |99.375%|98.421875%|141.123551 /118.2494215|195.68210595 /119.22407325|0 pp, verified exact IDs|
| CoHere dev0–63 |96.875%|94.484375%|118.3884053 /116.9580448|119.83735735 /118.39806055|0 pp, verified exact IDs|

These are measured peer incoming HTTP with new connections, metadata resident,
no application SQ8 cache, S3 cache uncontrolled, private-IP plain HTTP, no
filters. Four cells per panel use[k10,k100,k100,k10]. Per-cell8 QPS includes
scheduled offers/drain; campaign-wide throughput including startup is a different
quantity. Full p50/p90/p95/p99 and all k100 results are in verification.json.
The95%R10/<444ms published-context gate passes, but cache/startup and vendor
protocol differences prevent a matched competitor claim.

Measured physical totals15,848 GETs /8,587,837,440 verified bytes. Server cgroup
peak641,310,720 B (<8 GiB); client83,116,032 B (<512 MiB); both zero swap/OOM.
Both original Spot instances terminated: server i-0f272c689ed0b2f6f, client
i-01178d7ad6c290114. Controller74283 CLOSED0; independent verification37118
and report-enriched26387 CLOSED0. Initial verifier10167 hit an EC2 reporting
limit: terminated instance response omits private IP. Bind identity is instead
verified from authenticated root launch/IMDS role receipts and native ready log;
no measurement, identity requirement or acceptance gate changed.

Conservative compute estimate$0.0241 for243 s excludes EBS/S3, not billed cost.
Source1e735a0e /archive59f503f648be4bd3bd3c4bb13c570927fc764f465bb29e62125dbfd3ba8eb025;
configcd32e35343deff1b5006b83aa1be558a1821873c095856cce85fcde221d691aa.

Remaining decisive gap: remote namespace opening is4,513.984531–4,864.560267 ms
and readiness4,714.726247–5,016.004086 ms. Head reads89.00863–133.581705 ms.
True namespace-cold end-to-end performance therefore fails the subsecond/444ms
context despite fast resident-metadata queries. Next: decompose metadata GET,
stream-to-disk and authenticated decode costs using the same source/corpora;
make one causal startup change, then run a separately preregistered cold
first-query gate. Keep source/scorer/control parity and physical budgets.
Saturation/total cost,10M/100M bounded scale, lifecycle and BOTH vendor matched
quality/latency/QPS/$ comparisons remain open. Historical strict gates unchanged.
