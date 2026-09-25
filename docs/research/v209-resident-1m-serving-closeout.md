# V209 resident ReLAION-1M serving closeout

V209 **fails quality** on the already-used ReLAION-1M D768
validation-1000 panel. The production Rust generation binding,
verified old/new physical row map, authenticated 1,544,000,064-byte
resident FP16 plane and eight-worker whole-query path worked. The
512-only source PQ64 nominee set is too narrow on this corpus. Do not
promote it as a product default or infer external-product parity.

The single Causality `c7i.12xlarge` Spot attempt `a0001`, instance
`i-09f1cc64a8e1c9e04`, used source commit
`c94c841cb40b064c7b88984d50249e74f58ce968`, archive SHA-256
`19e6db84122b74545147d7021cb77580e393a183a08d55bc674f39162367241a`.
The complete terminal SHA-256 is
`3abf58319064b5f7b2d130f47c1b715595c5bd82c8cda67dfae8d523aff3da0d`
at `s3://borsuk-bench-453182569524-euc1/research/v209-resident-1m-serving/c94c841cb40b064c7b88984d50249e74f58ce968/runs/a0001/terminal.json`.
Every terminal artifact passed SHA-256 and byte-count readback; the
instance terminated. Its verified row-map SHA-256 is
`fee37bc3367b97be1e7d21c7b2f240b6114c3be51cbf3d0ec037b89bd333703b`;
generation root SHA-256 is
`8734c83bf3e4fabb84b13535feedce7c7c7beafae594ae53dafbc9741e41dc8d`.
The plane's physical public IDs matched all one million relaid SQ8 IDs.

| Same 1,000 ReLAION real queries | Returned GT100 hits / 100,000 | p05 hits / 100 | Query latency p50 / p95 / p99 | Scope |
| --- | ---: | ---: | ---: | --- |
| V209 resident full 512-nominee Rust | **96,813** | **85** | **22.31 / 23.00 / 23.26 ms** | nomination, map and FP16 ranking, eight workers |
| V199 live S3 + resident FP16 | 99,605 | 98 | 46.57 / 92.23 / 137.87 ms | transport, SQ8 and FP16 only; route/plan excluded |
| V155 cached sparse + exact source | 99,567 | 98 | not established here | historical planned transport baseline |

V209 had 317/1,000 queries below 98 hits, minimum 58, and paired
wins/ties/losses of 12/466/522 against V199. Eight-worker throughput
was **358.48 completed queries/s** over 2.7896 s of query-stage wall.
Peak serving-process RSS was **1,692,241,920 bytes**, charged FP16
plane arrays **1,544,000,000 bytes**, and cold router/map/plane load
2.243 s. Vector-body GETs and bytes were zero. These timings exclude
network front end, process startup, concurrent generation swap,
mutation and durability. V209 and V199 latency scopes differ; V209's
smaller latency does not compensate for its quality failure.

An independent postterminal replay streamed and SHA-256 checked the
complete old SQ8 object, converted the V116 request's 512 old ordinals
to authenticated public IDs, and intersected those sets and V209
returned IDs with V198's closed per-query GT100 witness. On the same
1,000 queries, the V116 nominee roster itself contained only **96,849**
GT100 IDs. Every V209 returned ID lay within that roster, and its
96,813 GT hits missed only 36 hits that were nominated. V198's
99,873/100,000 *candidate-field* ceiling describes wider physical
neighborhood units, not the 512 nominees. This establishes a candidate
coverage bottleneck; no planner-loop or FP16 precision micro-tuning can
close the 2,792-hit gap to V199 while scoring only those 512 IDs.

The material design decision is to turn the authenticated relaid
physical neighborhood into a resident candidate source and rank its
FP16 rows directly, with a corpus-independent candidate budget chosen
from a recall/resource frontier. First run a cheap ReLAION-100k D768
development falsifier across a frozen budget ladder; do not run another
1M cell until it gives both sufficient candidate coverage and bounded
scoring time. The 1M memory budget remains tied to recall, corpus and
generation count, with no vector-count knee.

Spot launch-to-termination was about 282 seconds. AWS Spot price
history at launch showed **$0.962/hour** for `c7i.12xlarge` in
eu-central-1c, so compute charge is approximately **$0.075** before
EBS, S3 requests/storage and rounding; this is an estimate, not a
billing measurement. No external Turbopuffer or S3 Vectors comparison
has been measured under matched conditions.
