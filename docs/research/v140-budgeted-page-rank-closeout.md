# V140 budgeted centroid-page rank closeout

**Decision: β=4 passes the preregistered cross-corpus geometry gate and may
enter a returned-quality implementation.** This is one generic, source-only
page-ordering policy on two used historical layouts, not a qualified
production default, returned Recall@100 result, or live S3 latency result.
β=1 and β=2 miss the D96 physical-coverage floor; β=8 exceeds the D96 mean
byte screen. No β was tuned within a corpus.

## Execution and authentication

The frozen source commit was `3213ca34cf6a8c32824dec6967b39eb06dd5fba6`,
source archive SHA-256
`7be1f3c95280e6c7449308d41807f775037d5b4d022523a635c88ae41b36ba59`.
The immutable a0001 prefix is
`s3://borsuk-bench-453182569524-euc1/research/v140-budgeted-page/3213ca34cf6a8c32824dec6967b39eb06dd5fba6/runs/v140-20260924T092255Z/a0001`.
Terminal SHA-256 was
`d9e15698b83c6be6456aeaff2864763b91192c063f0bda5dfca0cdc392a1f3c4`.
It reported `complete`, exit 0 after 142 seconds. Causality Spot
`c7i.8xlarge` instance `i-078bf0d54c1e33e70` was independently observed
**terminated**. All 13 terminal-listed artifacts were authenticated by the
launcher and independently downloaded and checked again. Deep and ReLAION
raw SHA-256 values were
`e2a1b31161bb2190cb7dc36d0601c50449c6d6aba0cc1a4985c032d713bc54d5`
and `3dc54d101814b0f8e2b2d80f1b79e2762edcc885ad423ee14ae601669eb0804a`.
The standard-library `scripts/v140_postterminal_recount.py` independently
verified 2,000 query identities, per-β page inventories, range geometry,
GET/byte charges, primary retention and caps. It authenticated sealed V122
truth and layout before counting D96 physical coverage. No GT entered the
worker's page selection.

## Paired geometry on used splits

The D96 cohort is **deep-image-96-angular random100k train subset**, used
**publication-test ordinals 9000–9999**. The D768 cohort is **ReLAION-1M**,
used **validation-1000**. Each has 1,000 queries. MB below means decimal
million planned bytes. All variants had zero 32-GET/16-MiB cap violations.

| β | D96 physical GT100 covered /100,000 | D96 mean planned MB/query | D96 p05 covered hits/query | D768 primary pages retained / total | D768 mean planned MB/query | D768 queries short of page target |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 98,579 | 0.588 | 93 | 17,571 / 17,578 | 3.536 | 0 |
| 2 | 99,054 | 1.266 | 95 | 17,571 / 17,578 | 7.215 | 26 |
| **4** | **99,738** | **2.694** | **98** | **17,571 / 17,578** | **11.802** | **295** |
| 8 | 99,983 | 5.132 | 100 | 17,571 / 17,578 | 15.175 | 709 |

The registered gate required D96 physical coverage at least 99,500,
D96 planned mean at most 4,714,063 B/query, zero cap violations, and at
least 95% aggregate D768 primary-page retention. β=4 is the sole passing
setting. Its D96 exact counts were **99,738 GT positions**, **2,693,744.64
B/query** planned, **28.1 GETs/query** planned mean, and **5,408,640 B/query**
planned p95. It beat the V122 fixed capped control's physical GT coverage
on 298 queries, tied on 699, lost on 3; that control covered 98,827
positions at 1,535,701.248 B/query mean. The much broader V135 candidate
returned 99,942 exact-source hits, but its historical V132 S3 schedule
received 9,428,126.976 B/query. These are different route revisions; the
comparison only sets context for the next paired run.

For β=4 on D768, planned p95 was **16,773,120 B/query**, mean **23.581
GETs/query**, and all primary pages were retained on **999/1,000 queries**;
the other query dropped seven pages. Its 11,801,736.96
B/query mean is a route plan, not an actual S3 response or a matched
comparison with V126's earlier source replay. The summary payload was
600,000 B on D96 and 48,000,000 B on D768, excluding the existing router,
allocator, scratch and cache.

## Resource and next gate

The D96 evaluator took 7.29 seconds for 1,000 queries with 74,332 KiB
maximum RSS. The D768 evaluator took **93.73 seconds** for 1,000 queries
with **991,420 KiB** maximum RSS; charged worker cgroup peak was
**1,051,865,088 B**. These are offline batch measurements on one Spot
worker. The Python greedy cover and flat centroid scan have not met a live
query latency gate, and the current flat scan will not scale to 10M/100M.

The next decisive gate is a production Rust β=4 route and range planner on
the same authenticated generation and a paired returned-quality replay
against the strongest fixed control, on both used cohorts. It must measure
actual exact-source/SQ8 returned Recall@100, p05/sub-90 tails, complete
per-query latency, GET/bytes, and charged serving memory. Only a quality
winner earns fresh matched 1M validation and live S3 transport, followed
by hierarchical routing and 10M/100M scale gates. β=4 remains an experiment
setting until those gates pass. The production memory policy must scale
smoothly with requested recall and vector count; this fixed 32-row summary
screen alone does not prove that policy.
