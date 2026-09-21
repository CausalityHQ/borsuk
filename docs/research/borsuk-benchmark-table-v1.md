# BORSUK measured benchmark checkpoint

All rows use 1,000 queries, `k=100`, 768 dimensions, and exact GT100. 
`blocked` means the authority did not measure the field; it is never treated as zero. 
The 1M product comparison is matched-workload, not paired query order.

| system / mode | corpus | R@10 | mean / p05 / worst R@100 | first p50/p95/p99 ms | reuse p50/p95/p99 ms | QPS c1 / batch | GETs mean / bytes mean | RSS | build | cost |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BORSUK / `native-production-exact-page-score` | ReLAION-100k development | 51.720% | 33.953% / 16.000% / 8.000% | blocked/blocked/blocked | 19.98/20.76/21.93 | 49.90 / blocked@blocked | 16.778 / 15.590 MiB | 2.16 GiB | 18.589 s | blocked USD |
| BORSUK / `bounded-native-sq8` | ReLAION-1M development | 99.280% | 99.026% / 97.000% / 82.000% | 41.46/70.07/161.86 | 41.77/65.51/89.75 | 21.20 / 182.39@128 | 23.085 / 11.434 MiB | 10.81 GiB | 149.656 s | 0.0842 USD |
| Amazon S3 Vectors / `matched-managed-service` | ReLAION-1M development | 97.680% | 90.938% / 67.000% / 39.000% | 73.35/239.56/328.05 | 61.37/93.52/120.57 | 9.48 / blocked@blocked | blocked / 0.005 MiB | 0.53 GiB | 848.090 s | 0.2127 USD |

## Decision

- ReLAION-100k development production routing is killed: mean R@100 is 33.953% despite exact candidate scoring.
- The unchanged 1M bounded reader is the quality/latency baseline: mean R@100 99.026%, reused p99 89.75 ms, peak 182.4 QPS.
- Matched S3 Vectors reaches mean R@100 90.938%; its physical GETs and index bytes are service-opaque.
- The 100k→1M slope fields are descriptive only because the two BORSUK rows use different formats, routing, and storage paths. No 10M/100M value is presented as measured.
- An exact brute-force latency cell was not run: exact GT100 already fixes the quality control, and brute-force latency would not choose between the current product integration options.
- Turbopuffer is blocked: no authenticated tenant/namespace credential was available, so no matched row is emitted.

## Next production gate

Bound concurrent ranged-response memory, then attach the measured bounded reader to authenticated native generations, delta merge, mutation visibility, and compaction. Qualify those semantics at 100k before any larger spend.
