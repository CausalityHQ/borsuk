# Boundary overlap: closed scientific FAIL

Original same instance terminated after 279 seconds. Terminal exit0 with all118 authenticated bodies; original controller exit2 was replay relocation. Absolute raw-body view replay passed under256MiB/CPU1/noSwap. Original collected evidence remains unchanged.

Consumed64 queries per dataset; FIRST100k, D768 cosine, k100. Matched no-overlap full-SQ8 control, identical routing and scoring.

| Dataset | Control recall@100 | Candidate recall@100 | Candidate coverage | Candidate p05 hits/100 | Mean local GETs | Mean payload bytes | Gate |
|---|---:|---:|---:|---:|---:|---:|---|
| relaion | 87.00000% | 89.34375% | 89.45312% | 64 | 24.00 | 8328279.8 | FAIL |
| cohere | 77.43750% | 80.70312% | 80.76562% | 61 | 24.00 | 8003251.3 | FAIL |

Both fail preregistered mean>=98% and p05>=95. Reject promotion to1M for this arm. The remaining loss is primarily routing/selected-cell coverage: candidate coverage nearly equals returned recall. SIMD cannot recover these missing candidates. Next native causal change must improve routing/coverage before optimizing scoring. These local serial payload operations are not physical S3 cold latency/QPS or matched vendor results. Large bodies remain bound by original terminal S3 roster and retained raw-body view; no benchmark rerun.
