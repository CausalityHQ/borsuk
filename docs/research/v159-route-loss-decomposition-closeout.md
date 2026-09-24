# V159 ReLAION-100k route-loss closeout

## Decision

The V158 100k failure is primarily a **physical admission/layout** failure
under the current 32-GET, 16,777,216-byte cap. The source-only PQ64 roster
contains 97.555% of exact GT100 rows, and pages touched by those nominees
contain 99.367%. Exact-SQ8 primary selection retains 97.344% in its 100
rows and 97.982% in its pages. The V114 weighted-interval plan fetches
only 77.302%. PQ-first-100 is independently rejected: it retains only
69.066% in its 100 rows, before planning.

The measured 100k SQ8 D768 mirror preserves source-file row order:
`v113_100k_score_screen.py::build_from_source` reads vectors and IDs in
Parquet order, `build_arrays` retains that order, and
`v114_exact_local_100k.py::build_from_sealed_artifact` writes the arrays
unchanged. It does **not** use the V63 clustered layout used by the separate
V114/V155 1M path. This source-order layout spreads each query's exact
primary over many pages. A separate postterminal, GT-blind arithmetic read
of all 1,000 closed V114 primary rosters found a median of 88 distinct primary
pages (p05 83, p95 93). Even the minimum page-aligned contiguous cover
with 32 GETs needs at least 27,955,200 bytes across the cohort; the
median is 31,150,080 bytes, p95 34,544,640 bytes, and **zero** queries fit
16,777,216 bytes. The existing capped planner cannot retain all exact
primary pages on this source-order layout, regardless of planner quality.

Prior experiments on the same ReLAION-100k development source already
tested query-blind geometric grouping. The historical 480-KiB balanced
two-means layout had a truth-aware 32-page/16-MiB GT100 coverage oracle of
99.844%, but its actual single-centroid router selected pages containing
only 94.417% and returned 94.349% after SQ8 ranking (p05 77%). Eight
page-local microcluster representatives reached 95.462% containment
(p05 79%). These are closed historical cells in
`algorithm-first-page-layout-ledger.md`, not V159 measurements. They show
that physical clustering can create headroom, while selecting the useful
pages without truth remains a separate unresolved problem. Repeating those
routers would not be a new architecture test.

Do not patch this with a dataset-specific recall switch or further PQ
primary tuning. The next material design must concentrate relevant rows
into fewer physical pages and/or use a narrower retrieval page format,
then perform exact-source reranking of a bounded union. Increasing the
charged byte/GET allowance is a measurable alternative, but it must face
live S3 latency, throughput and cost gates. The V156 graph can help only
if its page layout and admission policy clear the same physical ceiling.

## Verified decomposition

Each value below counts distinct exact GT100 IDs over 1,000 **used**
ReLAION-100k D768 development queries; 100,000 is the maximum possible.
These are offline coverage ceilings or measured fetched coverage, not
new returned-score results.

| Stage | GT100 IDs present | p05 / median hits per query |
| --- | ---: | ---: |
| 512 nominated rows | 97,555 | 88 / 100 |
| All pages touched by 512 nominees | 99,367 | 96 / 100 |
| 100 exact-SQ8 primary rows | 97,344 | 88 / 99 |
| Pages touched by exact primary | 97,982 | 91 / 99 |
| V114 exact-primary final physical plan | 77,302 | 71 / 78 |
| 100 PQ-first primary rows | 69,066 | 50 / 70 |
| Pages touched by PQ primary | 76,210 | 61 / 77 |
| PQ-primary final physical plan | 62,394 | 51 / 63 |

The final-plan counts reproduce V158's terminal-bound physical coverage
exactly on every query. The V159 independent checker recounted all eight
fields for every query and the summary from the frozen inputs.

## Provenance and limits

One Causality `c7i.xlarge` Spot instance `i-0f4e7d4ee518c6ad4`
completed from source commit `375d7a3be26735eebda43f30920e5eb2f31620a2`
and was confirmed terminated. Terminal SHA-256:
`0298f341f0eb64421d345959a569779f6de303000ee00c35327611e00eb5e02b`.
Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v159-route-loss/375d7a3be26735eebda43f30920e5eb2f31620a2/runs/a0001/`.
All four terminal-listed artifact lengths and SHA-256 values were read
back and checked. Raw SHA-256
`d72cd5bf220094c58af739fac50ab5a935c62cfde1e4c3f8efbc2b8c01af82d4`,
summary SHA-256
`0a4211a2800e96034e13715b8faeedf541f0dc30ad77fba0a0c2e6a71767bfeb`,
independent check SHA-256
`cc058d42e42c4007562cde6f64cad5f6544d50c2dd461946e9dd0ebfb03935a5`.

The 32-GET minimum-cover calculation was made after V159 closed from the
historical V114 primary roster and the existing V157 exact page-cover
algorithm. It is a deterministic explanatory calculation, not a
preregistered V159 promotion metric; a production format change must
re-run a frozen paired quality gate. None of these results measures live
S3 latency, serving RAM, fresh holdout recall or 10M/100M scale.

## Next gate

Specify a materially revised query-independent, source-only physical
grouping or narrow retrieval format, a bounded graph/page planner, and a single generic
recall/resource rule before opening any new quality split. Run the cheapest
paired 100k D768 returned-quality gate against the strongest currently
usable BORSUK control under equal bytes, GETs and exact-source scoring.
Keep the V159 decomposition as the falsification target: a new layout
must retain far more of the 97,982 exact-primary-page GT hits inside its
actual charged plan. Promote only a measured winner to 1M.
