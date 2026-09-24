# V154 ReLAION-1M graph CPU diagnostic: terminal-closed continuation

**Decision:** the unchanged generic seeded graph route clears the
paired ReLAION-1M offline CPU screen after V153's planner refactor.
Continue to an authenticated returned-ID and GT100 quality gate before
any architecture promotion. The used validation-1000 split is a
development diagnostic, not held-out confirmation or live S3 serving.

## Frozen attempt

Successful source commit `8f2c5ce4bffadc421fb4a48e4deccf989f803770`,
archive SHA-256
`5fe7bfcd84cc5fa6a3ecf66c45a6f265a6c0d12e9e04fcd2d92a5679c5e5badc`.
One uninterrupted Causality Spot `c7i.8xlarge`, instance
`i-09a52ffa1eb1975f4`, ran 357 seconds and terminated after its
complete terminal:
`s3://borsuk-bench-453182569524-euc1/research/v154-relaion-graph/8f2c5ce4bffadc421fb4a48e4deccf989f803770/runs/v154-20260924T143744Z/a0002/terminal.json`,
SHA-256 `09301ea5dfbb75aef49e7f34869903c15def2dfcd0eb787214f924fb191c5068`.
The launcher authenticated all 21 terminal-listed artifact sizes and
SHA-256 values, then independently recounted every query's four page
plans, V146 flat reference scores, bounded work, transport charges,
phase timings, percentile summaries and verdict. Eight focused
unit-centroid tests passed remotely. The a0001 premeasurement Python
failure is recorded separately in the attempt ledger and contributes
no scientific result.

## Paired measurement

The dataset is ReLAION-1M, validation-1000 already used in V116/V142,
D768, with 1,000 queries. CPU values are single-threaded offline wall
milliseconds for centroid search/scoring and page planning on the same
Spot worker. Bytes and GETs are planned SQ8 ranges, not live responses.
V150/V151 are diagnostic controls after the alternating flat/cached
timed pair.

| Arm | CPU p50 / p95 / p99 ms | Planned bytes, 1,000 queries | Planned GETs |
| --- | ---: | ---: | ---: |
| Paired flat | 5.511775 / 6.489573 / 7.048234 | 11,801,736,960 | 23,581 |
| Cached sparse | 0.286678 / **0.759366** / 0.929290 | 11,134,007,040 | 22,126 |

The sparse p95 is 8.55× lower than paired flat. Both order cohorts
pass separately: flat/sparse even 6.489573/0.778745 ms, odd
6.484324/0.725768 ms. V150 and V151 diagnostic p95s are 0.638319
and 0.671134 ms; they are not promotion candidates in this gate.
Flat score p95 is 5.696519 ms and flat planner p95 1.209425 ms.
Sparse search, page scoring and planner marginal p95s are 0.381143,
0.313456 and 0.065138 ms; marginal percentiles need not add to the
total p95. Maximum absolute sparse/flat page-score difference is
0.00000306964, within the 0.0001 preregistered gate.

All arms obey the 16P graph work, 5P candidate pages, 32 GET and
16,777,216 B transport limits and retain exactly the same primary
pages as paired flat. The unchanged cap cannot fit all primary pages
on one query in either arm; absolute all-query primary retention is
therefore false, and paired retention is true. Flat is short of its
4P target on 295 queries and sparse on 776. Cached sparse selects
41,417 of the 58,496 page occurrences selected by flat (70.803%).
This page capture is a quality warning, not a recall measurement.

At p95, cached graph search evaluates 608 unit centroids and total
distinct work is 1,240 units, versus a 31,250-unit full plane. Graph
build took 12,024.034 ms and encoded 4,265,768 bytes. The science
process peaked at 298,780 KiB RSS; its isolated cgroup peaked at
474,251,264 B including charged cache. The paired science command
took 19.86 seconds wall for graph build and all four 1,000-query arms.
These resource and CPU figures exclude primary routing, live S3,
source rerank, startup hydration, concurrency and pinned generations.

## Required next gate

Freeze a returned-quality replay from this exact V154 terminal. Fetch
the authenticated ReLAION source parquet, V63 layout, SQ8 bytes,
source-ID mapping, V116 nominees and GT100; seal ordered SQ8 and exact
source returned IDs for flat and cached sparse before opening GT. Check
Recall@100, p05, sub-90 queries, physical GT coverage, bytes/GETs and
charged memory against the strongest same-run flat result and historical
V142 β=4 context. Reject the sparse route if it loses unacceptable
quality despite its CPU win; revise the search/index or resource policy
generically, without a dataset threshold. A winner still needs a
reserved held-out cohort, then live S3 and larger-scale gates.
