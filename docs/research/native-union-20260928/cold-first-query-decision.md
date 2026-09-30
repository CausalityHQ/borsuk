# Namespace-cold first-query decision, 2026-09-30

Scientific quality/parity GO; published-context latency FAIL on BOTH datasets.
Retain the valid hardware-SHA candidate. No matched vendor win, offered-load
throughput, saturation, R@100 or total lifecycle cost was measured here.

Frozen source `2ea57d8f95d1808f3750a68b60561dc333773081`, archive
`38267b760cd6bf165cda31d4df2881594df245bbec3096213f248a3ce7e1a4c2`.
Original controller33764 CLOSED0, independent verifier11495 CLOSED0 without
reader repairs or rerun. Spot `i-0c3a8d79a76f28234` independently terminated547s.
All14 artifacts authenticated, unchanged395 Rust/Cargo and eight compiled
source files; qualified ARM binary reused, no rebuild/full-gate duplication.

FIRST1M D768 cosine, k10, fixed previously observed development ordinals0–63
per dataset. CoHere queries come from training rows1,005,000–1,005,999 and
cannot be reused as disjoint queries for full10M. Each call starts a fresh
process/namespace; preencoded request, timer before spawn through complete
first HTTP response, refused TCP connections included. Plain loopback HTTP,
client CPUs4–5/native0–3, no application SQ8 cache, S3 cache uncontrolled.
This differs from the published Turbopuffer1M D768 cold-query context.

| Dataset/split | Reference recall@10 | Candidate recall@10 / actual delta | Cold p50/p90/p95/p99 ms | Incoming HTTP p90/p95 ms | Serial cold calls/s |
|---|---:|---:|---|---|---:|
|ReLAION development0–63|99.375%|636/640 = 99.375% / 0pp|3621.950/3670.383/3736.774/4428.277|184.999/240.051|0.271688|
|CoHere development0–63|96.875%|620/640 = 96.875% / 0pp|3613.232/3684.872/3720.372/3930.581|196.773/234.378|0.272868|

All table values are measured and independently verified. Exact ordered IDs,
source/scorer/authority and physical counters match immutable native references;
these establish quality/control parity, not a matched cold timing baseline.
128/128 calls succeed. Both mean R@10 exceed95%; both cold p90 exceed444ms,
so the fixed published-context gate FAIL remains immutable. Serial calls/s
includes namespace initialization and cleanup gaps; it is not offered8QPS or
saturation throughput. Incoming HTTP excludes initialization and must not be
presented as the cold result. No R@100 measurement in this run.

| Dataset | Metadata staging median ms | Stream/output median ms | Awaited writes inside stream ms | Decode median ms | Head-read median ms |
|---|---:|---:|---:|---:|---:|
|ReLAION|2691.487|2484.850|94.303|637.869|90.901|
|CoHere|2693.923|2485.308|95.078|638.917|90.297|

Component medians are separate reductions and do not sum to a tail percentile.
Measured streaming is the largest component; writes account for only about95ms
of it. Prior closed per-object receipts locate most streaming in the200MB
plane/records.bin, followed by48MB centroids. No detailed transport CPU/network
attribution was measured; concurrent ranged transfer is a hypothesis to falsify.

Physical query accounting: ReLAION1927 GETs/1,073,479,680 verifiedB;
CoHere2035 GETs/1,073,479,680 verifiedB; zero failed GETs. Metadata576 objects
per dataset,16,428,441,792/16,428,445,184B. Metadata object counts are not complete
SDK physical request/retry accounting. Each query remains within32GET and
16,773,120B. Cgroup peak651,014,144B, native process maxRSS371,421,184/
371,339,264B; zero swap/OOM,8GiB cgroup/4GiB address-space bounds.
Compute estimate$0.027107 from547s and observed$0.1784/hour Spot quote,
excludesEBS/S3, not invoiced or a total per-query cost.

Next one causal intervention: bounded ranged transfer of large metadata,
retaining root authentication, exact bytes/content, cumulative admission,
owned scratch cancellation/cleanup and unchanged scorer/query caps. Freeze
explicit added startup request/memory accounting and a matched control before
launch. First prove shared staging correctness with narrow affected tests;
then qualify the ARM executable and compare startup/cold boundaries against
this frozen candidate. No new architecture/reviewer cycle.

Remaining BOTH-vendor gap: cold latency, matched protocol/vendor measurement,
saturation/total cost,10M/100M bounded-memory scale and incremental lifecycle/
generation-swap/recovery. Published-target attainment remains a legitimate
milestone with disclosed differences; no unsupported100M extrapolation.

[Independent verification](cold-first-query/a0001/verification.json),
[phase/cost reduction](cold-first-query/a0001/phase-and-cost.json),
[preregistered protocol](cold-first-query-preregistration.md).
