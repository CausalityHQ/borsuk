# V210 resident neighborhood 100k falsifier closeout

The fixed nearest-physical-neighborhood rule **fails** its joint
quality/latency gate on reused ReLAION-100k D768 development queries
0–255. The one complete Causality Spot `c7i.4xlarge` attempt `a0002`
used source commit `51f78a6f9f5c8cdf5fcfeccdab698d84b91799a8`,
archive SHA-256
`4450dbc49545cf9675a304dbdb0c6cd29126ca1877ba2da4abdd5f8402253bf1`,
and instance `i-00d93c6eecdd8dc0a`. Terminal SHA-256 is
`5c0dfdc972b968364b660742252ce2fee45b57e97f1051a1db93f73dde86a2ff`
at `s3://borsuk-bench-453182569524-euc1/research/v210-resident-neighborhood-100k/51f78a6f9f5c8cdf5fcfeccdab698d84b91799a8/runs/a0002/terminal.json`.
The launcher checked every terminal artifact digest and byte count
and confirmed the Spot instance terminated. Raw returned IDs and a
GT-blind seal were uploaded before truth and baseline were downloaded.

| ReLAION-100k D768, used development first 256 | GT100 hits / 25,600 | p05 hits / 100 | FP16 score-only p95 |
| --- | ---: | ---: | ---: |
| V193 full-rank SQ8, same 256 queries | 25,440 | not replayed here | not measured here |
| Resident nearest 512 | 24,896 | 86 | 0.440 ms |
| Resident nearest 2,048 | 25,198 | 92 | 1.680 ms |
| Resident nearest 8,192 | 25,433 | 96 | 6.869 ms |
| Resident nearest 32,768 | **25,516** | **99** | **37.853 ms** |

No budget simultaneously meets paired baseline hits, p05≥98 and
score-only p95≤10 ms. The 8,192 arm misses baseline by seven hits but
also has an unacceptable p05 of 96. The 32,768 arm gains 76 hits and
reaches p05 99 but misses the scoring latency gate by 27.853 ms.
The Python offline prepare process peaked at 2,112,892 KiB RSS and
took 14.66 s wall; these are source-load and diagnostic resources,
not charged Rust serving RAM or product request latency. The p95
timings include NumPy candidate gather and FP16 dot/rank, but exclude
nearest-position construction, network service and concurrency.

The negative result rejects direct FP16 scoring of a large physical
neighborhood at this budget. The next single design is a query-aware
compact-code screen over the expanded neighborhood, followed by a
bounded FP16 shortlist. It must face another frozen 100k falsifier
before any repeat 1M cell; the V209 1M quality failure remains
negative evidence. No matched Turbopuffer or S3 Vectors result exists.

The `a0001` phase-argument failure and its terminated instance are
recorded in the preregistration amendment. It produced no query
measurement and is not pooled with `a0002`.
