# Verified native reduction of completed S3 admission

All five serial remote gates passed on c5591b3807fa641fa0a12765947714ead2fe0089, including eight tests, release, workspace Clippy, real workspace test compilation and offline native reduction. The original instance was terminated and waited before collection.

| Completed 100k Cohere D1024 cosine k10 admission | Result |
| --- | --- |
| Recall@10 | 97.23% |
| p50 / p90 / p95 / p99 | 181.36 / 207.87 / 219.46 / 245.02 ms |
| Serial reciprocal throughput | 5.386 queries/s |
| Underfilled queries | 0 / 1000 |

These are descriptive measurements of the previously completed admission, not a new performance panel or service QPS claim. Mean stages: discovery 35.29 ms, source 66.44 ms, planning 6.17 ms, SQ8 fetch/rank 77.74 ms. Stage timings include computation and authentication.

Prospective comparisons use published vendor numbers and the same Cohere dataset and disclosed methodology. No separate vendor-service run is planned. Scale, cache, region, query selection and concurrency differences must be disclosed; this 100k admission does not establish a win against a published 10M benchmark.

Next gates: actual native D1024 incremental compaction/recovery correctness, then one causal Rust performance change selected from these timings before scaling.
