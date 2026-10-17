# Counterbalanced semantic leaf-prefix latency screen

Source a0463ed097296c19539d05529ddad615be760fdd; same qualified binaries and authenticated Cohere1M/D1024/cosine/k10 fixed1000. Four fresh-process cells A1=48,B1=44,B2=44,A2=48, after separate runtime admission and real-S3 smoke. No rebuild or requalification.

| Cell | Leaves | Recall@10 | p90 ms | p95 ms | Serial QPS |
|---|---:|---:|---:|---:|---:|
|A1|48|97.83%|199.020|218.011|6.350|
|B1|44|97.63%|191.371|207.753|6.660|
|B2|44|97.63%|199.243|215.558|6.155|
|A2|48|97.83%|199.273|212.188|6.411|

Tail-latency direction was not uniformly favorable across both temporal comparisons; report the mixed result without selecting a favorable run.

Both repeated same-arm runs had exact ordered result/plan/trace/logical-charge and per-query recall parity. Recall differs between arms and is reported, not forced equal. The incumbent48 default remains unchanged;44 remains a research tradeoff.

Serial data-uncached S3 workload with fresh processes; startup metadata warms connections and S3 server caches are uncontrolled. Two repetitions share1000 queries, not2000 independent samples. Pooled values are descriptive. No universal cold, concurrent QPS, equal-quality or competitor win claim.

See summary.json for all cell hashes, latency/CPU/RSS/read charges, stage decomposition, repeat parity, temporal ratios and quality deltas. All native full-file readers exited0.
