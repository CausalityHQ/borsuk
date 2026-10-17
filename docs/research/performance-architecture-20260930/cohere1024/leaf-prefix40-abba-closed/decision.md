# Leaf40 versus48: valid Pareto tradeoff
Frozen Cohere1M D1024 cosine k10, 1000 identical requests, ABBA48/40/40/48. Exact native and strict-reader binaries reused. Actual admission, separate smoke, and each sequential create-only S3 checkpoint passed.

40 retains97.37% recall versus48 at97.83%:46 fewer hits across10000 truth positions,40 queries worsened and960 unchanged. Retain both; no default change.

Paired p90 ratios0.910609/0.936668; p95 ratios0.917248/0.973814; query wall ratios0.904768/0.922552.40 QPS6.7171/6.7562 versus48 at6.0774/6.2330. Logical bytes34229466368 versus39690200320 (-13.7584%); GET127811 versus139913 (-8.6497%). QueryCPU ratios0.879133/0.853821. Both temporal pairs support the directional faster/lower-read tradeoff; this is not a universal speedup or production default decision.

Application cacheOFF, backendUNKNOWN, same1000 queries repeated, first queries included. Physical counters are process native transport including IMDS, not billed requests or wire bytes. No concurrency/lifecycle-dollar/cold guarantee. S3Vectors96.17%/p95205.998ms is descriptive only: different run/normalization/cache/retry/timer conditions; no matched vendor win. Turbopuffer remains unmeasured.

The higher-quality48 option remains. Next substantive optimization should preserve quality and target measured SOURCE/planning stages. Do not repeat broad qualification for unchanged binaries.
