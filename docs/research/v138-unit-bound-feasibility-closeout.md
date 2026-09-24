# V138 source-only unit-bound feasibility closeout

**Decision: reject exact center/radius unit admission as the primary selective
S3 range policy.** The GT-free a0002 attempt reproduced the a0001 diagnostic
and crossed the frozen D96 stop threshold. This is a feasibility screen, not a
returned Recall@100 or live S3 latency measurement. The historical V132
candidate is a byte baseline from a different route revision, not a paired
performance comparison.

## Authenticated execution

The source commit was `b1afc2f8361d46317d2e6b34e394688f7a5396a6`, with
source archive SHA-256
`b459a99896a9b3187ee11da33d6c9f17882bc6a66eb299ba87eeaebbe83de8f8`.
The immutable a0002 prefix is
`s3://borsuk-bench-453182569524-euc1/research/v138-unit-bound/b1afc2f8361d46317d2e6b34e394688f7a5396a6/runs/v138-20260924T085601Z/a0002`.
Its terminal SHA-256 is
`2b619fb40b5ab0db56bfd60da044809efc7871e7993fc2b2fb7a9aaa8d81af2a`.
All eight terminal-listed artifacts were downloaded and checked against their
recorded byte lengths and SHA-256 values. The 1,000 raw query rows were
independently recounted. Spot instance `i-06a359a04274f8181` reached a
successful terminal in 29 seconds and was terminated. There is no active V138
worker. The frozen primary-only input had SHA-256
`04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3`;
the worker did not download the GT-bearing V122 evidence file.

## Measured D96 screen

The cohort is the **deep-image-96-angular random100k train subset**, with the
already-used **publication-test queries 9000–9999**. Units contain 32 SQ8 rows;
pages contain 256 rows. These are computed source-only bound-admission and
minimum-range byte counts, not actual S3 transfers.

| Measure | Authenticated a0002 result |
| --- | ---: |
| Queries / physical units / pages | 1,000 / 3,125 / 391 |
| Promising units, p50 / p95 | 3,084 / 3,120 |
| Promising pages, p50 / p95 | 391 / 391 |
| Queries admitting all 391 pages | 917 / 1,000 |
| Minimum bytes to cover all admitted pages in at most 32 ranges, mean | **10,781,973.504 B/query** |
| Same minimum bytes, p50 / p95 / maximum | 10,800,000 / 10,800,000 / 10,800,000 B/query |
| Queries within 16,777,216 B and 32 GETs | 1,000 / 1,000 |
| f16 center + f32 radius summary payload | 612,500 B |
| Evaluator maximum RSS / cgroup peak | 54,848 KiB / 70,668,288 B |

The preregistered D96 stop threshold was **4,714,063 B/query mean**. The
historical V132 broad candidate actually received **9,428,126.976 B/query
mean** from S3 on this dataset and split. This exact bound screen would select
about **1.144 times** that byte volume, because conservative radii of small
physical units overlap the query's 100th-witness threshold nearly everywhere.
The 16-MiB cap passes only because the whole 100k SQ8 object is 10.8 MB.
`decision.json` records `deep_stop=true` and `relaion_skipped=true`.

## Scope and next decision

The result rejects a flat sound bound as the main way to choose S3 ranges on
this layout. It does not invalidate the conditional Lean proof in
`formal/UnitBoundAdmission.lean`: that proof concerns containment if its
assumptions hold, not selectivity, observed recall, throughput, or latency.
The proof can still support an optional certificate or a score feature.

The next router candidate needs query-dependent approximate ranking or
nominee-informed expansion to reach currently unvoted D96 pages, coupled with
budgeted selection under the D768 1M cap. The same policy and tuning procedure
must work across both corpora, with memory allowed to scale with target recall
and corpus size. Preregister source-only diagnostics first, then paired
returned-quality and live S3 measurements from one frozen revision. Do not
adopt a fixed halo from the post-hoc V137 coverage ceiling.
