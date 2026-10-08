# Cold baseline/direct SQ8 pair a0001

Frozen Rust candidate 2fb771bdb31f9490a3341bdbfa2b4ad510cfb487, qualified binary SHA67c7eb51e3ff6bb9b77c1c9f12cac3b402501b708d91bd6d95ea20b029a8ab0b. Cohere100k/1000/D1024/cosine/k10; CPU1/512MiB/swap0/query1/fetch32. Application payload caches off; provider/backend caches uncontrolled. Order A then B on one Spot host; no order-balanced repetition or strict backend-cold claim. Instance i-09858a5757a8475f4 terminated/waited before collection.

| Native query metric | A baseline | B direct closure |
|---|---:|---:|
| Recall@10 |97.23%|97.24%|
| p50 ms |93.820|86.039|
| p90 ms |117.089|116.758|
| p95 ms |127.512|124.909|
| p99 ms |149.076|271.548|
| Serial query QPS |10.166|10.666|
| Query process CPU,1000 queries,s |49.208|62.519|
| Native HTTP GET attempts |51,943|26,938|
| Verified payload bytes |25,671,329,664|35,315,366,912|

Declared p90/p95/QPS/recall gate passed observationally: p90 -0.28%, p95 -2.04%, QPS +4.92%. Not an overall winner: p99 +82.15%, CPU +27.05%, and bytes +37.57%. No default promotion or scale qualification follows from this single ordered pair. All1000 baseline IDs/scorebits/traces/charges match historical B1/B2; all direct plans match closed GET/byte predictions. No query lost recall; one gained one hit. Native query transport has zero transport/stream failures, status206 for every GET, and attempts equal logical charges. These are native transport observations, not wire/billed S3 proof. Both prefixes sealed before truth. Cgroup peak96,972,800B, no swap/OOM/scratch leak; direct conservative modeled memory298,129,247B.

Stage scope sums,1000 queries: baseline discovery0.397s/SOURCE39.809s/planning6.089s/SQ852.039s; direct discovery0.203s/SOURCE0/planning0.017s/SQ893.531s. SQ8 includes fetch/authentication/scoring, so these totals do not isolate network from CPU or explain p99 causally. Worst direct wall times271-289ms with roughly59-88ms process CPU motivate a bounded fetch/body/authentication/scoring attribution before another code change.

Saved S3 Vectors reference on the same corpus/query bytes: R10 96.56%, p50 60.352ms, p90 118.479ms, p95 149.056ms, p99 215.429ms, serial QPS13.754. Conditions differ (first-pass idle/backend state); BORSUK has higher recall and lower observed p95, S3 higher median speed/QPS and lower p99 than direct. No matched vendor win. Turbopuffer published cache-disabled10M p50/p90/p99 874/1214/1686ms is a different population/load and does not establish a matched100k result. Retain original references; do not rerun S3.

Next: independent source/result interpretation and a bounded causal attribution gate for the direct SQ8 phase. Do not call this production-ready or claim throughput per total lifecycle dollar; build/write/maintenance/recovery and scale gates remain. Parquet/Arrow are not in this measured query path; serving already uses native binary objects. No blind SIMD, cache, routing or format change is authorized by this result alone.
