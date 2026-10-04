# Capacity-constrained partitioner: closed paired decision

Do not promote this arm or scale it to 1M. Execution and native admission passed; both frozen quality gates failed.

| Dataset/split | Original recall@100 | Partitioner recall@100 | Original p05 hits | Partitioner p05 hits | Selected-cell GT ceiling |
|---|---:|---:|---:|---:|---:|
| ReLAION FIRST100k, consumed64 | 64.9375% | 65.03125% | 45 | 47 | 87.109375% |
| CoHere FIRST100k, consumed64 | 68.875% | 69.125% | 50 | 50 | 77.484375% |

All cells used24 local reads/query, below the frozen32/16MiB envelope. Candidate mean payload was8,258,367.8125 bytes for ReLAION and7,939,120.3125 bytes for CoHere. These are local authenticated reads, not physical S3 query GETs. The selected-cell ceiling assumes perfect nomination/ranking and is an upper bound on this fixed selection, not measured improved recall.

Candidate local nomination misses1413/6400 ReLAION and535/6400 CoHere; residual selected-cell misses825/6400 and1441/6400. Final ranking misses were0 on the nominated GT population; this does not prove quantization is lossless on all corpus rows. Fixing nomination alone cannot reach98%.

Local nomination used mean9.383853 ms CPU ReLAION and9.064181 ms CoHere; routing used0.564646/0.625873 ms, ranking3.231518/3.238641 ms. SIMD could reduce compute, but cannot recover absent candidates. Complete-query latency and QPS were not measured. Stage-sum wall tails differ between arms and include scheduling effects; they are not end-to-end or S3 cold latency claims.

Next native decision: improve selected-cell recall with a source-derived routing summary, then validate nomination/recall before expensive scale runs. Reuse the retained layouts and existing research; do not repeat global-top24 centroid-only routing, which already failed. Keep the old source-neighborhood cut improvement as mechanism evidence, not a proxy for query recall. Build time is not a product gate.

The same instance terminated;15 calls completed, including4 actual positive admissions and1 expected-exit2 negative. Host peak reached2GiB with reclaim events but no swap/OOM. Observed scratch7,748,316,866 bytes exceeded the6,482,146,477-byte admission model while staying below16GiB; revise prospective accounting from this evidence. No vendor win or100M feasibility claim.
