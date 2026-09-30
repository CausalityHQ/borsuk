# Paged source cold development gate

Frozen configuration: `cold-config.json`. No measurement has begun.

## Intervention and authority

Build the authenticated 395-file native source epoch in `native-source-manifest.json` on ARM. Generation v6 stages nine metadata objects and verifies the encoded source by bounded authenticated ranges instead of staging its entire 200 MB body. Preserve source, order, scorer, centroid/graph and SQ8 payloads. Input publication receipts authenticate every new generation body. Borrowed canonical and SQ8 bodies retain their previously qualified identities; publication HEAD checks do not requalify their contents.

The ReLAION and CoHere FIRST1M, D768, cosine, k10 panels each contain 64 already consumed development queries (ordinals 0–63). These are development gates, not fresh publication panels. Reference headers bind to the converted roots; all result rows and terminal bytes remain unchanged. Reference startup timings are historical and cannot measure this candidate.

## Protocol and gates

Run ReLAION first, then CoHere. Each query starts a new process and namespace and sends exactly one ANN POST. Timing spans process launch through the first complete HTTP response, including refused TCP connections. Loopback HTTP, native CPUs 0–3, client CPUs 4–5, no application SQ8 cache; S3 service cache is uncontrolled. Report p50/p90/p95/p99 and serial completion QPS, without calling this offered-load or saturation throughput.

Require all 64 calls per dataset, exact ordered-ID/SQ8 physical parity with the authenticated reference, zero failed source/SQ8 GETs, and mean recall@10 ≥95%. Enforce source ≤128 GETs/67,108,864 verified bytes, SQ8 ≤32 GETs/16,773,120 bytes, combined ≤160 GETs/83,881,984 bytes. Record metadata logical HEAD/GET counts, source HEAD and timing, payload buffer bounds, RSS, cgroup memory/swap/OOM, startup phases and source/SQ8 charges. Logical admissions and verified bytes are not independent packet-level wire accounting.

Any authority, correctness, transport, overflow or resource failure ends this arm, retains its raw failure evidence and prevents subsequent cells. A completed quality failure remains FAIL. Neither outcome alone establishes a universal architecture failure. Report the source discovery/nomination/scoring bottleneck supported by the evidence.

Cold p90 <444 ms is a separate published Turbopuffer 1M D768 comparison context, not a matched vendor measurement. AWS subsecond cold is directional context. No vendor p95 or throughput guarantee is inferred. Historical geometry offered-load results are an unmatched reference: at 1 offered QPS, ReLAION/CoHere R10=99.375%/96.875%, cold p90=2171.147223/1702.327560 ms; at 8 offered QPS only 24/22 of 64 succeeded. Do not claim a paired speedup from this serial arm.

## Execution and next decision

One Spot c7g.2xlarge; machine limit 4200 s. Focused build: 10 GiB, no swap, four CPUs, 2400 s plus 30 s cleanup. Profile: 8 GiB, no swap, 4 GiB address-space limit, 1500 s plus 30 s cleanup. Compute cap $0.35 plus $0.15 allowance is an estimate, not billed total cost. Freeze source/config/code/archive/artifact identities before launch. Build produces a resolved config containing the qualified binary identity; preserve both original and resolved config hashes. No full-workspace assurance claim.

On Spot interruption discard the interrupted measurement arm, retain terminal evidence, and authorize any fresh repetition only after closure. Authenticate the owned instance identity, terminate and wait for termination before collecting terminal artifacts. Never inspect incomplete measurement records or retain idle compute.

Attempt a0001 was rejected before instance creation for insufficient Spot capacity in eu-central-1c. Its empty owned-instance closeout is retained. The next distinct attempt uses the existing public subnet in eu-central-1b, in the same VPC with an active internet-gateway route. Region, machine type, resource bounds and protocol stay fixed; record the actual availability zone and subnet in its reservation. This is a capacity retry, not a measurement repetition.

Attempt a0002 completed its focused ARM qualification, then failed before any ANN HTTP request because the input bootstrap wrote `HEAD.json` instead of the native `head.json` key. Preserve this failed arm and its raw ordinal-zero record. Both lowercase head objects now authenticate to the same original head bodies; generation and query payloads are unchanged. The next attempt uses `--qualified` to authenticate and restore that closed binary, exact compiled snapshots, resolved configuration and historical build receipts. It performs no new Cargo build. Its source qualification binds current controller code and the frozen build terminal/artifact identities, and labels reuse explicitly. Only the new profile resources and closed query records measure the retry.

Independently authenticate and reduce closed records before accepting results. If correctness and quality survive, the next decisive test is 8 offered QPS and a saturation/cost curve from the same qualified binary. Warm, recall@100, disjoint publication panels, 10M/100M, incremental maintenance/recovery and matched vendor comparisons remain open.
