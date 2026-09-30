# Qualified geometry offered-load curve: 8 QPS FAIL

2026-09-30. Original controller45319 closed exit0; independent verifier24822
closed exit0 on its first attempt. All42 campaign bodies, source archive, frozen
native395/compiled9 authority, schedules/port ownership, immutable inputs/GT,
responses, physical counters and resources authenticate. Spot
`i-0d411ec33273ad4c1` is independently confirmed terminated and its owned
80GiB gp3 volume deleted. No Rust build or full-workspace gate was repeated.

Source `0c909c5735748bd413145672c262e4c8abf832dc`.
Archive `13e3fcc2e4b742848a010179575a8ef954d7bbb2cd02b96ac4f6ce3c0568565b`.
Config `6200cc0d4b1b6a88a6498ec16a865c7436814605976658114c59ad3416ab4f44`.
Terminal `280332b3ab785fd48f90d0b2dc19f3889a9d6f39caf63a57438410f49c34b14f`.

## Fixed protocol and actual results

FIRST1M rows/D768/cosine/k10; ReLAION split `fresh rank16 development0-63`;
CoHere split `fresh CoHere development0-63`. Previously observed development
panels, not disjoint publication. Fresh process/namespace per admitted offer,
six ports, no client queue or retry, absolute offers, loopback HTTP, no application
SQ8 cache, S3 service cache uncontrolled. CPUclient4–5/native0–3/Tokio4,
caller1GiB, profile7GiB/zero swap/4GiB address space. Candidate8×4MiB staging
uses the frozen geometry/a0002 binary and unchanged source/scorer.

**All numbers in the following table are verified measurements.** Tails and
recall are conditioned on successful offers; drops remain in all-offer gates.

| Dataset | offered QPS | successes/64 | drops | mean R10 success population | cold p90 ms | cold p95 ms | completed full-span QPS | all-offer quality gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| ReLAION | 0.25 | 64/64 | 0 | 99.375% | 2171.372 | 2693.404 | 0.2523 | PASS |
| CoHere | 0.25 | 64/64 | 0 | 96.875% | 2716.493 | 3864.267 | 0.2523 | PASS |
| ReLAION | 0.5 | 64/64 | 0 | 99.375% | 2308.666 | 3136.192 | 0.5013 | PASS |
| CoHere | 0.5 | 64/64 | 0 | 96.875% | 1807.431 | 2332.546 | 0.5014 | PASS |
| ReLAION | 1 | 64/64 | 0 | 99.375% | 2171.147 | 4480.263 | 0.9903 | PASS |
| CoHere | 1 | 64/64 | 0 | 96.875% | 1702.328 | 2104.020 | 0.9901 | PASS |
| ReLAION | 2 | 64/64 | 0 | 99.375% | 2111.871 | 3139.803 | 1.9235 | PASS |
| CoHere | 2 | 63/64 | 1 | 96.825% | 3082.180 | 3498.566 | 1.7965 | FAIL |
| ReLAION | 4 | 40/64 | 24 | 99.500% | 3387.805 | 3691.964 | 2.2750 | FAIL |
| CoHere | 4 | 41/64 | 23 | 97.073% | 2648.261 | 3137.096 | 2.3493 | FAIL |
| ReLAION | 8 | 24/64 | 40 | 100.000% | 2245.500 | 2253.639 | 2.4319 | FAIL |
| CoHere | 8 | 22/64 | 42 | 96.364% | 2573.773 | 3827.684 | 2.2043 | FAIL |

**768 offers:638 successes,130 capacity drops,0 errors.** Largest tested
passing BOTH-dataset operating point is **1 offered QPS**:64/64 successful
on each,99.375%/96.875% mean recall@10, ordered IDs/physical counters exact.
Quality delta against immutable reference is0pp for the complete cells.
At2QPS ReLAION completes64; CoHere completes63 and drops1. At8QPS only
24/64 ReLAION and22/64 CoHere complete; the8QPS gate is **FAIL**.
ReLAION100% success-conditioned R10 at8QPS covers24 calls, not all64 offers.
All444ms context gates fail; AWS subsecond directional target is unmet.

Frozen geometry/a0002 single-call p90=1763.117/1712.617ms is a different
protocol, not this load control. Old binary3d96 cold-offered/a0001 is a stale
historical reference, not a matched control. No paired speedup or regression
claim follows from either cross-campaign comparison. No recall@100, warm,
matched vendor win,10M/100M or saturation ceiling was measured here.

## Measured cause and resources

At1QPS median staging=785.200/773.727ms; decode=578.719/578.730ms;
incoming HTTP=105.446/103.847ms (ReLAION/CoHere). ReLAION stagep95
=3594.667ms, versus decodep95=599.456ms. Heavy cold staging tails fill
the six owned slots; the admission drops are measured. At8QPS the successful
population median staging=1097.723/1024.992ms, decode=673.168/635.207ms.
Component medians/tails are not additive, and rates run sequentially.

Peak native RSS393,359,360 bytes; profilecgroup2,984,062,976 bytes,
zero swap/OOM. Successful namespace metadata counters remain69GET+9HEAD
with32MiB payload bound. At1QPS each dataset verifies1,073,479,680 query
bytes/64 calls; queryGET totals1927/2035 with no failures, unchanged caps.

## Cost scope

Compute **$0.055601 estimated** from1122 controller
seconds and observedSpot$0.1784/hour, not invoiced billed lifecycle. Published
Frankfurt list-price inputs dated2026-09-30: standard GET-class$0.00000043
/request; gp3$0.0952/GB-month; publicIPv4$0.005/hour. Raw primary quotes
and actual infrastructure identity are preserved alongside the receipts.
Controller-wall EBS/IP estimates=$0.003297/$0.001558
(80GiB baselinegp3,30-day month proration, one publicIPv4). Successful logical
metadata+query request estimate=$0.029857; source-inferred
two startup controlGETs/success add$0.000549 separately.
These are list-price estimates, not physical SDK wire counts or total invoice.
Retained storage, setup/input/artifact requests, credits/taxes and full
incremental-maintenance/build/lifecycle costs are excluded. Per-cell scoped
estimates in phase-and-cost.json do not make failed4/8QPS operating points pass.

## Convergence decision and next executable change

| baseline | candidate | actual quality delta | measured latency/QPS | cost | remaining BOTH-vendor gap | next decisive test |
|---|---|---|---|---|---|---|
| immutable ID/reference quality; no matched load control | qualified8×4MiB binary, six cold slots |0pp complete-cell R10;99.375/96.875% | largest BOTH passing1QPS; p90=2171.147/1702.328ms,p95=4480.263/2104.020ms;8QPS FAIL | scoped estimates above; total cost unmeasured |444ms/subsecond missed,8QPS attainment missed;10M/100M and lifecycle open | authenticated source metadata reduction with exact scorer/IDs and predeclared combined request/byte/memory gates |

End this offered-load arm; no rerun solely for a better tail or green8QPS.
Retain the valid8×4MiB default and its prior matched development GO. Raising
slots alone does not close the latency gap or100M full-source residency.
Current cold open stages and loads the full200MB two-bit source plane plus
48MB centroid blob per1M namespace. Source-plane residence alone scales to
20GB at100M before centroid expansion/graphs, and this is a size extrapolation,
not a measured100M system.

Next implement authenticated on-demand source-plane access under a bounded
page/byte/concurrency plan, preserving existing router nomination and scoring
semantics. Establish per-page authority and combined startup/source/SQ8 limits
before a fresh falsifier; do not weaken digest, identity or quality checks.
A scalar-to-batch half conversion is only an unmeasured decode hypothesis,
not a substitute for reducing loaded metadata. Both-vendor product goal stays
ACTIVE; no operator decision is currently needed.
