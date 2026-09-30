# Bounded metadata ranges decision, 2026-09-30

**Matched development improvement GO; published-context latency FAIL.** Retain
bounded ranges as the valid native candidate. This is no matched vendor win.

Frozen source `0525dcb826e740411c115e8ece4ffb82a9ee3394`, archive
`924e396904fc1d1deae9dba45c77f21eec103c98b4c1d3f38951543944da5485`.
Original controller5122 CLOSED0; independent verifier81699 CLOSED0 without
reader repair or rerun. Spot `i-00a2dc34258cd1efe` terminated after1487s.
All53 artifacts,395 native source identities, the sole reviewed staging delta,
eight compiled snapshots, frozen control binary, ARM SHA backend, full toolchain
parity and focused ARM qualification independently authenticated. No current
full-workspace assurance claim.

FIRST1M D768 cosine k10, previously observed development ordinals0–63 per
dataset,64 calls per arm/dataset. Fixed ABBA uses fresh namespace/process per
call; timer spans process launch through full first HTTP response including
refused TCP connects. Plain loopback HTTP, client CPUs4–5/native0–3, no
application SQ8 cache, S3 service cache uncontrolled. CoHere training-query
rows1,005,000–1,005,999 cannot serve as disjoint queries for full10M.

| Dataset/split | Matched serial control R@10 | Ranges candidate R@10 / delta | Control cold p90/p95 ms | Candidate cold p50/p90/p95/p99 ms | Actual p90 delta |
|---|---:|---:|---:|---:|---:|
|ReLAION development0–63|99.375%|636/640=99.375% /0pp|3698.424/3765.954|2010.407/2164.242/2221.526/2780.698|−1534.182ms (−41.482%)|
|CoHere development0–63|96.875%|620/640=96.875% /0pp|3701.716/3723.752|2018.061/2202.817/2347.164/2726.740|−1498.899ms (−40.492%)|

All table values measured and independently verified;256/256 calls succeed,
exact ordered IDs and physical query accounting agree. All128 ordinal-paired
cold differences favor candidate. Both95% mean R@10 and strict matched-p90
improvement gates pass. Both444ms context gates fail; no retroactive relaxation.
R@100 unmeasured. Incoming HTTP candidate p90/p95 is218.244/311.251ms ReLAION,
176.185/212.245ms CoHere; excludes initialization, not the cold result.

| Dataset | Control→candidate staging median ms | Stream/output median ms | Decode median ms | Candidate records /centroids stream median ms |
|---|---:|---:|---:|---:|
|ReLAION|2692.213→1080.478|2485.709→833.150|648.227→644.841|591.966/192.943|
|CoHere|2693.173→1089.295|2488.113→836.739|647.601→644.641|592.638/193.235|

Component medians do not sum to a percentile. Candidate direct synchronous
scratch writes consume74.737/74.650ms inside stream/output; old control writes
are awaited. The range intervention reduces transfer time; decode remains
~645ms and staging~1.08s. No detailed transport CPU attribution measured.

Tradeoff per namespace: metadata logical GET9→37 plus9 HEAD, same authenticated
bytes (~256.7MB),32MiB staging payload bound excluding SDK transport. Across64
calls/dataset candidate576 HEAD/2368 logical GET versus control0/576. Counts
exclude physical SDK retries. Query GET totals1927/2035 and verified bytes
1,073,479,680 each dataset, failed GET0, unchanged32GET/16,773,120B caps.
Native peak RSS control371,322,880/371,331,072B versus candidate371,896,320/
371,851,264B; profile cgroup652,513,280B/build6,828,859,392B, zero swap/OOM.

Serial campaign256/full-span=0.345378calls/s includes cleanup and ABBA gaps;
not offered8QPS or saturation. Compute estimate$0.073689 at observed$0.1784/h
for1487s includes build/campaign, excludes EBS/S3, not invoice/lifecycle cost.

Next decisive gate: fresh cold offered-load/cost curve using this qualified
binary, with declared concurrent namespace memory admission and complete
request cost scope. First inspect measured startup/decode paths for the minimum
causal reduction; do not rebuild unchanged native code or repeat full assurance.
Remaining BOTH-vendor gap: cold latency, offered throughput/total cost,
protocol-matched vendor evidence,10M/100M scale, incremental/generation-swap/
recovery lifecycle. No unsupported scale extrapolation.

[Independent verification](metadata-ranges-cold/a0001/verification.json),
[phase/cost reduction](metadata-ranges-cold/a0001/phase-and-cost.json),
[frozen protocol](metadata-ranges-preregistration.md).
