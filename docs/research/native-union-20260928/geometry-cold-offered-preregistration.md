# Qualified geometry cold offered-load curve

2026-09-30. Preparation; no launch authority until config, wrapper, controller
and independent verifier are integrated and their authority/lifecycle checks pass.

Reuse the closed metadata-geometry/a0002 candidate binary without Cargo or a
new native assurance gate. Source/native authority and all frozen bodies remain
immutable. Both matched geometry arms passed 19 focused ARM tests; current
full-suite pass is not claimed. Candidate binary SHA256
`256dcf6c9d3f893d5c709564a099bf440b482e617800598ddd6538e1ec0cbb21`,
12,470,768 bytes, native identity
`4bbc6c332e78e5a068001913597cdc82d6c13bd147acd96e019149c190a39a1c`.

## Fixed protocol and gates

FIRST 1M rows, D768, cosine, k10. ReLAION split `fresh rank16 development0-63`;
CoHere split `fresh CoHere development0-63`. Previously observed development
panels, not disjoint publication panels. Same immutable requests, ground truth,
source/scorer authority, ordered IDs and physical query counters as geometry.
Fresh process/namespace for every admitted offer; no application SQ8 cache,
no HTTP retry; S3 service cache uncontrolled; loopback plain HTTP.

Absolute open-loop rates 0.25, 0.5, 1, 2, 4, 8 offers/second, in that order;
64 offers per dataset/rate, ReLAION then CoHere, 768 total. Six owned ports,
18080–18085, no queue, immediate capacity drop if all are occupied. Ownership
lasts through process cleanup. Client CPU4–5, native CPU0–3/Tokio4, caller1GiB.
Preserve all offer/admission/completion/success/drop/error/abort denominators.
Report success-conditioned service/scheduled tails separately, and completed
full-span QPS including cleanup. Never infer saturation QPS as 1/latency.

Operating-point gate: all64 offers successful, mean recall@10>=95%, exact
ordered-ID/source/scorer/query-physical parity and correct metadata accounting.
Report the largest tested passing rate for BOTH datasets; 8QPS attainment is
separate from cold-p90<444ms published context. A capacity failure ends that
rate's attainment claim; it does not erase the valid candidate. Identity or
accounting failure stops the arm and retains raw partial records. Recall@100,
warm, vendor wins, 10M/100M and total lifecycle cost remain unmeasured here.

Candidate staging is eight4MiB ranges, 32MiB payload bound, 69 logical metadata
GETs+9HEADs per successful namespace. Two startup control GETs are inferred
from source, not measured SDK wire calls. Query32GET/16,773,120-byte limits
unchanged. Cost counts include increased metadata calls, with retries/invoice
scope explicitly unknown unless measured.

One causality/eu-central-1 c7g.2xlarge Spot worker; 7GiB profile cgroup, zero
swap, 4GiB address-space limit, 2400s+30s worker, 2700s machine, 80GiB encrypted
gp3 deleted on termination. Max Spot quote $0.30/hour, compute estimate cap
$0.225 plus $0.15 EBS/S3 allowance. Estimates are not invoice/total cost.
Prerequisite: no other live BORSUK campaign; unique attempt/ACK/client token,
no automatic replacement. Interrupted cell discarded; failed terminal preserved,
restart only a fresh preregistered attempt. Authenticate terminal and all bodies,
terminate owned ID and confirm termination before independent result reduction.

Old cold-offered/a0001 binary/source and its failed-closeout records are a
historical reference, not a matched control. New curve has no claimed paired
speedup against it. Current matched single-call baseline is geometry/a0002:
candidate p90 1763.117ms ReLAION /1712.617ms CoHere, R10 99.375%/96.875%.
Its 444ms context failed. Next after this curve: reduce substantive metadata
loading/decode work under identity, quality and bounded-memory gates; measured
median decode alone ~572ms exceeds444, so range tuning alone cannot close it.
