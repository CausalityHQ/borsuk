# Paged source cold development decision

`cold/a0004/verification.json` independently authenticates the closed campaign and reduces all 128 calls. Instance `i-0f85a45b112b1ec40` is terminated. The binary is the qualified ARM build from a0002; build evidence is historical, profile evidence is new.

| Dataset and split | Candidate R10 | Quality delta vs frozen ordered reference | Cold p90 / p95 (ms) | Serial QPS | Decision |
|---|---:|---:|---:|---:|---|
| ReLAION FIRST1M D768 cosine k10, consumed fresh rank16 development 0–63 | 99.375% | 0 pp, exact ordered IDs | 1404.494 / 1494.709 | 0.743629 | Quality/correctness PASS; 444 ms context FAIL |
| CoHere FIRST1M D768 cosine k10, consumed fresh CoHere development 0–63 | 96.875% | 0 pp, exact ordered IDs | 1397.579 / 1520.655 | 0.755502 | Quality/correctness PASS; 444 ms context FAIL |

Every number above is verified from all 64 successful calls per dataset. Serial QPS is not offered-load or saturation throughput. No matched control latency or vendor measurement exists. Earlier geometry offered-load results (R/C p90 2171.147/1702.328 ms at 1 offered QPS; only 24/22 successes at 8 QPS) are historical and unmatched; do not derive a paired speedup.

Measured startup remains the primary cold bottleneck: metadata-stage medians 448.661/437.959 ms, decode 351.522/351.837 ms, head read 91.032/90.605 ms. Incoming HTTP medians are 363.724/356.979 ms. These marginal phase medians are not additive tail decompositions. Source requests reached the fixed 128-GET cap in both datasets; maximum verified source bytes were 43,520,000/37,888,000. Combined GETs reached 160. All source/SQ8 failed-GET counts were zero. Per-call metadata uses 22 logical GETs, nine metadata HEADs and one source HEAD; inferred control reads and physical wire accounting are separate.

Measured native peak RSS was 173,961,216/173,416,448 bytes. The separate 951 MB conservative admission model is an estimate. Compute cost for this attempt is estimated at $0.016221 from 301 controller seconds and the $0.194/hour Spot quote; this excludes failed arms, EBS, IPv4, S3, storage, setup and invoice reconciliation, and is not total cost.

Retain the candidate: the preregistered quality and correctness gates pass. Preserve the published-context latency FAIL. Next decisive test is the same qualified binary at 8 offered QPS, then saturation and request-cost accounting. Cold startup work must target the measured staging/decode costs; no new router/scorer architecture is justified by this result. Both-vendor latency/throughput-per-total-dollar, warm, R100, disjoint publication panels, 10M/100M and lifecycle qualification remain open.
