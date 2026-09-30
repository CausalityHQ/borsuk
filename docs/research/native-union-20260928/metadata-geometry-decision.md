# Matched metadata geometry: development GO

2026-09-30. Original controller 99192 closed with exit 0. Spot
`i-0e74d46ec2126cab8` is verified terminated after 1669 controller seconds.
Independent reader 57227 closed with exit 0 on its first attempt, authenticating
all 60 artifacts, the source archive, both 395-file native identities, nine
compiled snapshots per arm, five focused ARM test classes per arm, toolchain
parity, all 256 responses, ground truth, clocks, counters and resource limits.
Each arm passed 19 focused tests; this is not a repeated full-workspace gate.

Source: `3315455568ea727f277911de96a2231289d6ae5b`.
Archive: `48bb877cdbe947401084ab56ec237eca1c4d9cff7bbb49a9c35d9380ae235959`.
Terminal: `37268e9f0125ef0a3f770271458a0378d27b018571cb4c701965833b9fa1e826`.
Config: `410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3`.

## Protocol and actual comparison

FIRST 1M rows, D768, cosine, k=10; 64 previously observed development queries
per arm/dataset. ReLAION split: `fresh rank16 development0-63`; CoHere split:
`fresh CoHere development0-63`. ABBA order, same inputs and immutable object
authority, fresh process/namespace per call, no application SQ8 cache or HTTP
retry, loopback HTTP, S3 service cache uncontrolled. Client CPUs 4–5, native
0–3, Tokio four workers, caller admission 1 GiB, profile 8 GiB cgroup/zero swap
and 4 GiB address-space limit.

Both binaries were freshly built on the same ARM worker with the graph digest
reuse. Matched control is four concurrent 8 MiB ranges; candidate eight
concurrent 4 MiB ranges. Both keep the 32 MiB staging payload bound. Earlier
binary timings are historical references and are not this control.

All values below are verified measurements, in milliseconds.

| Dataset / arm | cold p50 | cold p90 | cold p95 | cold p99 | mean recall@10 |
|---|---:|---:|---:|---:|---:|
| ReLAION control | 1957.225 | 2339.880 | 2537.490 | 3098.389 | 99.375% |
| ReLAION candidate | 1614.761 | 1763.117 | 1809.685 | 2462.588 | 99.375% |
| CoHere control | 1924.226 | 1989.650 | 2123.751 | 3299.136 | 96.875% |
| CoHere candidate | 1599.626 | 1712.617 | 1778.707 | 2140.125 | 96.875% |

Candidate p90 decreases **24.649% ReLAION / 13.924% CoHere**. Quality delta
is **0 percentage points** on both; ordered IDs and physical query counters
match for all 256 calls. Candidate is faster in 63/64 ReLAION pairs and 61/64
CoHere pairs. Both arms meet the preregistered 95% mean recall@10 floor.
The fixed development gate therefore passes. Keep the eight-by-four default.

The separate published 1M D768 cold-p90 context of 444 ms remains **FAIL**:
candidate exceeds it by 1319.117 / 1268.617 ms. The AWS subsecond cold
directional target is also unmet. This is no matched vendor win. Recall@100,
warm performance, offered/saturation QPS, total cost and 10M/100M scale were
not measured here; the serial campaign rate is not an offered-rate result.

## Measured cause and tradeoff

| Dataset | median stage control → candidate | median decode control → candidate |
|---|---:|---:|
| ReLAION | 1078.707 → 752.676 ms | 575.849 → 572.585 ms |
| CoHere | 1076.454 → 762.402 ms | 576.229 → 571.919 ms |

Median stream/output intervals decrease 833.109 → 556.775 ms and
830.843 → 558.090 ms. The 200 MB source records component decreases
588.537 → 340.882 / 588.662 → 342.376 ms; the 48 MB centroid component
192.410 → 117.719 / 191.729 → 117.160 ms. Writes remain approximately
72–73 ms, included in stream intervals. Component medians are not tail sums.
The measured improvement is in staging, with little matched decode change.
This experiment does not isolate the graph digest optimization's speedup.

Logical metadata calls per namespace increase **37 → 69 GETs**, with nine
HEADs unchanged and identical bytes. Two additional startup control GETs are
inferred from frozen source; these counts are not measured SDK wire retries.
Verified query GET totals per 64 calls are unchanged: ReLAION 1927, CoHere
2035, each 1,073,479,680 verified bytes, zero failures, within the unchanged
32 GET / 16,773,120-byte per-query caps.

Candidate peak native RSS is 393,117,696 / 393,375,744 bytes versus control
371,933,184 / 371,892,224 bytes. The payload bound excludes transport and
allocator overhead; the RSS increase is observed, not attributed by profiling.
Profile cgroup peak is 681,709,568 bytes; combined build peak 7,165,911,040
bytes, zero swap/OOM. CoHere's incoming-HTTP p95/p99 increased
175.638/273.182 → 188.469/451.997 ms despite improved full cold tails;
the broader load curve must retain that tail evidence.

Compute is **$0.082708 estimated** from 1669 controller seconds at the
observed $0.1784/hour Spot quote. It includes qualification and measurement;
it excludes EBS, S3 and public IPv4, and is not an invoice or operating-query
total cost. Higher GET counts must appear in subsequent cost curves.

## Next decisive gate

Reuse the qualified candidate binary and compiled proof for the preregistered
cold offered-load curve; do not rebuild unchanged Rust or repeat the full
assurance suite. Freeze its new binary/source authority and geometry accounting
before launch. Report all offered/admitted/completed/dropped/error denominators,
scheduled and service tails, all-offer quality, resource peaks and cost scope.
The older six-slot curve is a historical reference, not a matched control.

Cold latency still needs a substantive metadata/decode reduction: median
decode alone is above the 444 ms context, so further range tuning alone cannot
close that target under this loading protocol. Retain this valid improvement
while measuring throughput; then reduce loaded/authenticated metadata and
decode work under explicit quality, identity and memory gates. No new reviewer
or architecture cycle is needed to close the current curve.

Both-vendor performance/cost, disjoint publication quality, 10M/100M bounded
scale and incremental-maintenance/recovery qualification remain open.
