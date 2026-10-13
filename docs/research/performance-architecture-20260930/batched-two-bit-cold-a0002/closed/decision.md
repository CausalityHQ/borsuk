# Scalar versus eight-row batch source scoring — 2026-10-13

Same frozen1M Cohere D1024/cosine/k10, all1,000 queries per cell, both widths64, client payload/local-drive cache off. Original scalar ELF3dfad916; batch ELFa62a1f45; comparatorc8056b5d. Exact source/config and archive pins accompany these native aggregates.

| Cell | Recall@10 | p90 ms | p95 ms | p99 ms | Sequential QPS |
|---|---:|---:|---:|---:|---:|
| Scalar A1 | 97.80% | 182.421 | 194.125 | 295.800 | 6.153 |
| Batch B1 | 97.80% | 183.189 | 196.615 | 331.455 | 5.959 |
| Batch B2 | 97.80% | 180.074 | 191.255 | 312.184 | 6.154 |
| Scalar A2 | 97.80% | 176.180 | 189.175 | 326.735 | 6.249 |

Decision: batch kernel UNSELECTED for cold performance. Paired p95 worsened1.28%/1.10%, QPS fell3.15%/1.52%, and planning time rose2.48%/4.60%. p99 worsened12.05% in pair1 and improved4.45% in pair2. No repeatable speed gain was measured. This is an observed regression, not rejection of a small positive gain. Restore scalar library behavior; preserve batch code and receipts as historical evidence.

All18 stages passed, zero swap/OOM, all four cells had9780/10000 hits and zero underfill. Both Rust paired reductions confirm exact ordered IDs/score bits, full traces, physical plans and logical GET/byte charges for every query. Original exec035e4fe2 closed0; i-040362a5805f62515 terminated, root volume deleted and external timer inactive. Archive and stage metadata independently verified before reading these aggregates. No raw query rows were processed locally.

Retained S3 Vectors reference: recall96.10%, p90197.550ms, p95215.023ms, p99243.048ms, sequential QPS7.358. Scalar BORSUK has higher measured recall and lower p95 in this pass, but lower QPS and worse p99. Cache eviction and original-f32 versus normalized-f32 ranking parity are unproved; no matched-quality or lifecycle-dollar win. No matched Turbopuffer reproduction.

Next measured bottleneck: SOURCE consumes about77s of160–163s query wall time per scalar cell, versus23s planning and58–60s SQ8. Investigate whether source fetches can finish in one wave with the same plan/bytes and a measured memory envelope. Do not assume SIMD or layout benefits from this negative result. Recall anchor97.80%/prospective algorithm floor97.60% remains fixed; numerical scheduling changes require exact parity.
