# V112 precise nominee route ceiling

Status: complete offline **development** diagnostic on 2026-09-23. The
registered first-200 gate passed and the same source ran all 1,000
development queries. No validation query or live S3 query-path GET was
used. The candidate is **not a deployable one-wave reader**: it obtains
SQ8 scores for the 512 nominated rows from a local authenticated copy
before choosing ranges. Serving that information directly from S3 would
add a query wave.

## Authority and paired design

The single scientific attempt is
`s3://borsuk-bench-453182569524-euc1/research/v112-precise-nominee/1de19ee38c230e0af89c9ff70fe7e36870c6675a/runs/relaion-1m-dev1000-a0001/`.
It ran pushed source `1de19ee38c230e0af89c9ff70fe7e36870c6675a`
on c7i.12xlarge Spot instance `i-0a151ddc5d297eaae`, now verified
**terminated**. Terminal status was complete, exit 0, elapsed 911
seconds. The launcher independently read back and SHA-256 checked every
terminal artifact. Prefix evidence SHA-256 is
`420ca214518366d01ba86bf77244e3363e41b1c429451aa414247a88f8ed34c2`;
full evidence SHA-256 is
`bd92b68ea822897a6d58118c03ea17df6701800d13820a61b77d35087d6054a2`.
The full independent reduction SHA-256 is
`90001ce42fdf0ecc718dd1200a14856be4112ff0965bcdc8cbbafbcc9c9ab3fc`.
The source, queries, GT100, V63 permutation and V70 SQ8 object matched
their frozen hashes. The V77 manifest reproduced SHA-256
`131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874`.

All arms retain the V77 1,024-region page route, PQ64 top-512 row
nomination, V63 physical SQ8 page order and identical final SQ8 scorer.
The V77 gap-two control is uncapped. V109's control admits pages in
best-PQ64-row order under 32 GETs/16,777,216 bytes. V112 locally
SQ8-scores only the 512 nominated rows, gives its precise top 100
primary page votes and the other 412 rows secondary votes, then uses
V111's exact weighted interval optimizer. A 513-to-1 factor makes a
primary vote outrank every possible secondary vote; the factor and the
physical byte arithmetic were checked in Lean. GT100 is consulted only
after the route and returned IDs are fixed. The validator independently
recomputed nomination, nominee weights, physical optimum and witness,
all three arms' exact SQ8 returned rankings, GETs/bytes and GT hits.

| ReLAION-1M development cohort and arm | Returned Recall@100 | p05 hits | Sub-90 queries | GET p50/p95/max | Byte p50/max | Cap violations |
|---|---:|---:|---:|---:|---:|---:|
| First 200: V77 gap-two | 99.160% (19,832/20,000) | 97 | 0 | 18/61/80 | 10,583,040/41,932,800 B | 63/200 |
| First 200: V109 capped | 98.695% (19,739/20,000) | 95 | 4 | 23/32/32 | 9,784,320/16,773,120 B | 0/200 |
| First 200: V112 precise nominee | **99.110%** (19,822/20,000) | 97 | 0 | 14/32/32 | 15,974,400/16,773,120 B | **0/200** |
| All 1,000: V77 gap-two | 99.292% (99,292/100,000) | 98 | 0 | 21/63/109 | 11,381,760/49,720,320 B | 329/1,000 |
| All 1,000: V109 capped | 98.803% (98,803/100,000) | 95 | 19 | 25/32/32 | 10,183,680/16,773,120 B | 0/1,000 |
| All 1,000: V112 precise nominee | **99.234%** (99,234/100,000) | 98 | 1 | 17/32/32 | 15,974,400/16,773,120 B | **0/1,000** |

The prefix exceeded the 19,800-hit stop threshold by 22 hits and
triggered the preregistered full development replay. Over all 1,000
queries, the precise nominee route gained 431 returned GT hits over
V109 under the same caps. It was 58 returned hits below the uncapped
V77 arm, which violated the caps on 329 queries. These are planned
physical GETs and bytes against an authenticated local object, not live
S3 latency, QPS or node-cost measurements. The full replay used
986,196 KiB maximum RSS and 264.51 seconds wall time; the independent
full validator used 1,006,416 KiB and 257.75 seconds, both with zero
swaps. They are offline worker resources, not 100M serving memory.

## What the result isolates

Post-terminal cross-analysis of the SHA-checked full evidence with
V110's same-query GT-page roster gives these **physical page-containment**
counts, before final SQ8 scoring:

| All 1,000 development queries | V77 uncapped | V109 capped | V112 precise nominee |
|---|---:|---:|---:|
| GT hits on fetched pages | 99,719 | 99,190 | **99,658** |
| Returned GT hits | 99,292 | 98,803 | **99,234** |

V112 improved containment by 468 and returned hits by 431 relative
to V109. Its final scorer lost 424 GT positions relative to page
containment, compared with 387 for V109; the scorer was unchanged, and
the difference reflects the newly fetched row mix and rank competition.
The union of pages with a primary vote contained 99,346 GT positions;
the final interval plan contained 312 more in net, after secondary
votes, gap coverage and any primary pages displaced by the caps. The
precise score supplies page-value information that
PQ64 minimum-score and count-only weights lacked. The V110
truth-aware whole-page optimum contains 99,996, leaving a 338-hit
page-selection gap even for this nondeployable diagnostic.

## Production decision and next gate

Promote **the route concept**, not this diagnostic reader. Build one
resident finer row representation trained only from corpus rows, use
it to rerank V77's fixed PQ64 top-512 nominees, then apply the same
100-primary/412-secondary page votes, physical DP and SQ8 final scorer.
Start with a preregistered 16-byte-per-row residual code in addition
to PQ64; if its paired development gate fails because row ordering is
insufficient, compare one 32-byte width before changing the design.
Do not optimize weights or thresholds on the now fully observed
development split. A candidate must reproduce both physical caps,
reach at least 99.0% returned Recall@100 and p05 at least 90 on the
development cohort, and then pass untouched validation plus live S3
latency/QPS, node-charged memory and rollover tests before any default
is frozen. V112 does not measure those production properties.

Memory is a frontier `M(N,R,C,G)` over collection size, selected recall,
concurrency and pinned generations. A 16-byte extra row plane adds
1.6 GB per 100M generation, or 3.2 GB during a two-generation rollover,
before reserves. A 32-byte plane doubles those increments. These are
arithmetic projections, not measured peaks or a fixed 3-GiB ceiling.
The 10M and 100M nomination/locality, latency and memory slopes remain
unmeasured; no vector-count knee is assumed.

The checked Lean arithmetic establishes the vote ordering and byte
cap implications. A full DP refinement and a cohort recall proof would
still require authenticated page-weight, estimator/GT-overlap and SQ8
rank-displacement premises. It cannot prove unseen-query recall or live
S3 service time without those measurements.
