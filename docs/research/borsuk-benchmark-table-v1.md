# BORSUK measured benchmark checkpoint

All rows use 1,000 queries, `k=100`, 768 dimensions, and exact GT100. 
`blocked` means the authority did not measure the field; it is never treated as zero. 
The 1M product comparison is matched-workload, not paired query order.

| system / mode | corpus | R@10 | mean / p05 / worst R@100 | first p50/p95/p99 ms | reuse p50/p95/p99 ms | QPS c1 / batch | GETs mean / bytes mean | RSS | build | cost |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BORSUK / `native-production-exact-page-score` | ReLAION-100k development | 51.720% | 33.953% / 16.000% / 8.000% | blocked/blocked/blocked | 19.98/20.76/21.93 | 49.90 / blocked@blocked | 16.778 / 15.590 MiB | 2.16 GiB | 18.589 s | blocked USD |
| BORSUK / `bounded-native-sq8-v3` | ReLAION-100k development | 69.740% | 51.267% / 33.000% / 21.000% | blocked/blocked/blocked | 26.69/26.96/29.94 | 37.32 / blocked@blocked | 32.000 / 6.952 MiB | 1.97 GiB | 32.163 s | 0.0449 USD |
| BORSUK / `bounded-native-sq8` | ReLAION-1M development | 99.280% | 99.026% / 97.000% / 82.000% | 41.46/70.07/161.86 | 41.77/65.51/89.75 | 21.20 / 182.39@128 | 23.085 / 11.434 MiB | 10.81 GiB | 149.656 s | 0.0842 USD |
| Amazon S3 Vectors / `matched-managed-service` | ReLAION-1M development | 97.680% | 90.938% / 67.000% / 39.000% | 73.35/239.56/328.05 | 61.37/93.52/120.57 | 9.48 / blocked@blocked | blocked / 0.005 MiB | 0.53 GiB | 848.090 s | 0.2127 USD |

## Decision

- Current format-v3 ReLAION-100k routing is killed: mean R@100 is 51.267% at exactly 32 page GETs/query, below the 97.5% gate.
- It improves the prior 100k router (33.953% mean R@100) but remains far below the target.
- The historical 1M bounded reader remains non-promoted evidence: mean R@100 99.026%, reused p99 89.75 ms, peak 182.4 QPS; it is a different frozen architecture.
- Matched S3 Vectors reaches mean R@100 90.938%; its physical GETs and index bytes are service-opaque.
- No same-revision 100k→1M slope exists because the current 100k quality gate failed. No 10M/100M value is presented as measured.
- An exact brute-force latency cell was not run: exact GT100 already fixes the quality control, and brute-force latency would not choose between the current product integration options.
- Turbopuffer is blocked: no authenticated tenant/namespace credential was available, so no matched row is emitted.

The historical S3 Vectors ReLAION-1M **development** raw samples also
give nearest-rank **p90 193.866503 ms** on the fresh-index first pass and
**p90 79.397588 ms** on the immediate repeated pass (1,000 requests per
pass). These were recomputed on 2026-09-25 from the immutable
`samples.parquet` SHA-256
`870b24d95178f6087c762d248fec10ee7d885ac990346dfe91134e4f777aa346`,
bound to terminal SHA-256
`f502f9443b954a674a296896d138d7e039d58f37850c9f1ee46cbabd6f226488`.
The same raw samples reproduce the table's p50/p95/p99 and mean
Recall@100. These p90 values are measured historical context for that
development split, not a new V219 validation or same-revision comparison.

## Next production gate

The authenticated generation/delta/mutation/compaction integration is verified, but its router fails quality. Redesign the routing/index representation and falsify it at 100k; do not spend on 1M/10M/100M for this revision.
