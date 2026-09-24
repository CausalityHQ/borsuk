# V152 cached page scores: terminal-closed reject

**Decision:** reject the cache-only architecture. It eliminated
duplicate centroid distance calculations but made paired CPU slower.
The experiment was an engineering comparison on the already used
deep-image-96-angular random100k subset, publication test ordinals
3000–3999. It does not establish any 1M, 10M or 100M result.

## Frozen attempt and verification

Source commit `1cd267782e52fb3cca2c9ce176b2e48970f85c7a`, archive
SHA-256 `c58ec56108c9e295eb09f00bff00ddfece743ffe4ebcf284898e8e9cb39d8e6b`.
One uninterrupted Causality Spot `c7i.8xlarge` instance,
`i-0344c58944ba4daf7`, ran 438 seconds and terminated after its
complete terminal. Terminal URI:
`s3://borsuk-bench-453182569524-euc1/research/v152-cached-page-graph/1cd267782e52fb3cca2c9ce176b2e48970f85c7a/runs/v152-20260924T134009Z/a0001/terminal.json`;
SHA-256 `e40cd496a573533ba962c3173918b6e146a52b6c1f3df59326478e470d22ad08`.
The launcher authenticated all 33 artifact lengths and SHA-256 values,
then independently recounted plans, unit work, cached and exact page
scores, returned quality, timing summaries and verdict. All eight
focused unit-centroid tests passed on the worker. The GT-blind audit
checked 4,000 ordered SQ8/source arm results and all 391 flat page
scores for every query; maximum independent flat-score discrepancy was
0.00000099957. Query, routing and GT hashes matched the terminal-closed
V151 development cohort exactly.

## Paired result

All rows are measured on the same 1,000 queries. CPU means
single-threaded wall time for page scoring/search and planning; bytes
and GETs are planned SQ8 ranges, not live S3 responses. V150 and V151
timings are diagnostic; flat and V152 occupied alternating first/last
positions.

| Arm | Source Recall@100 | p05 hits | Below 90 | CPU p95 (ms) | Planned bytes | GETs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Paired flat V146 | 99.720% | 98 | 0 | 0.554735 | 2,690,858,880 | 28,267 |
| V150 | 99.505% | 97 | 1 | 0.503021 | 1,995,667,200 | 25,277 |
| V151 | 99.646% | 98 | 0 | 0.638223 | 2,648,446,848 | 27,982 |
| V152 cached | 99.646% | 98 | 0 | 0.656683 | 2,648,446,848 | 27,982 |

V152 scored exactly the same candidate pages and returned the same
ordered IDs as V151. Its maximum page-score difference from flat was
0.00000099838, below the 0.0001 gate. Primary, 16P/4P, 32-GET and
16-MiB gates all passed. Source quality was 74 hits below flat, inside
the 100-hit gate. Planned bytes and GETs were lower than flat. The
strict CPU gate failed by 0.101948 ms (18.4%). Both order parities
failed separately: flat/V152 even p95 0.554735/0.656683 ms; odd p95
0.537876/0.656202 ms.

V151 did p95 1,933 unit-distance computations; V152 did 1,325,
matching the distinct union. V152 p95 graph work was 624 units and
new page-scoring work 717. The cache saved calculations but failed to
save wall time. Closed per-phase p95 timings isolate the failure:

| Arm | Search (ms) | Page scoring (ms) | Planning (ms) | Total (ms) |
| --- | ---: | ---: | ---: | ---: |
| Flat | — | 0.044669 | 0.513167 | 0.554735 |
| V151 | 0.104344 | 0.040397 | 0.497867 | 0.638223 |
| V152 | 0.104464 | 0.058314 | 0.503411 | 0.656683 |

Phase p95 values are marginal percentiles and therefore need not sum
to the total p95. V152 scored fewer units, but its hash lookups and
strict sequential squared-difference arithmetic were slower than
V151's repeated eight-lane norm-dot scorer. Search plus scoring still
costs about 0.16 ms at p95, while the sparse planner costs about
0.50 ms, nearly as much as flat's planner. The 100k flat score scan is
only about 0.045 ms. Thus simply removing duplicate distances cannot
pass the 100k CPU gate. V152 science peak RSS was 32,488 KiB and its
isolated cgroup peak was 76,042,240 B; graph build took 213.746 ms.

## Next architecture decision

Keep the authenticated centroid and graph format as a correctness
baseline. The next design must reduce routing and planning wall time
with generic D96/D768 arithmetic and a workload-based recall/cost
control, without a vector-count knee. A shared vectorized distance
kernel and a faster page planner are candidates; they require separate
equivalence and paired timing gates. Do not tune the 16P/4P budgets or
the used 3000–3999 queries to manufacture a pass. The reserved
4000–4999 cohort remains unopened for a winner confirmation. At 1M,
the historical V146 flat scorer alone measured 9.895993 ms p95 and
the full score-plus-planner path 11.071522 ms p95 on ReLAION-1M
validation-1000, so a sparse route may still win at scale, but no
V152 1M measurement exists. Test that hypothesis with a frozen paired
1M gate rather than extrapolating this 100k timing.
