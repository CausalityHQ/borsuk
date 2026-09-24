# V150 semantic unit-centroid graph closeout

**Decision: reject the frozen V150 candidate policy.** The graph reduced
paired CPU and distinct score work on the used 100k cohort, but failed
the preregistered selected-page capture and per-query shortfall gates.
This was a GT-free page-plan screen. It did not measure returned recall,
live S3 latency, or serving cost, and it does not promote to 1M.

## Immutable evidence

- Source commit: `1bb1559a775a241f1049f812f40a93d7f1c22fdc`, pushed to
  `origin/main`. Source archive SHA-256:
  `7a2b21341e8d435732b185ecee76ac9975bd1d88cdff1b89680b0b19a899c320`
  (16,825,716 bytes). The launcher verified it against the named Git archive.
- Causality Spot `c7i.8xlarge` `i-079853e2a7da4a2b0`, run
  `v150-20260924T121622Z/a0001`, terminated after a complete,
  uninterrupted exit-zero terminal. Terminal:
  `s3://borsuk-bench-453182569524-euc1/research/v150-unit-centroid-graph/1bb1559a775a241f1049f812f40a93d7f1c22fdc/runs/v150-20260924T121622Z/a0001/terminal.json`;
  SHA-256 `c00e15182688a17b94f2182c30f3144d6cb9c2a5e57ecfa0235eb842790a7e07`.
  All 16 artifact hashes and sizes were verified. The independent
  launcher replayed every page plan and the page-ID control from the
  authenticated V138 roster and V146 score matrix. The worker's three
  focused graph tests passed. Total instance elapsed time was 302 s.
- Cohort: deep-image-96-angular random100k train subset, 1,000 reused
  publication-test source query ordinals 9000–9999, D=96. Baselines:
  V140 β=4 raw selected pages and shortfall; V146 f16 unit-centroid
  scorer and flat Rust score matrix; V149 rejected physical hierarchy.

## Terminal measurements and gates

| Measure | V150 verified | Gate or baseline |
|---|---:|---:|
| V140 selected-page capture | 62,202 / 84,796 = **73.3549%** | ≥95%; V149 77.0956% |
| Same-budget page-ID control capture | 26,366 / 84,796 = **31.0934%** | V150 advantage ≥5 points; achieved 42.26 points |
| Queries with shortfall worse than V140 | **970 / 1,000** | 0 |
| p95 graph + exact page score + planner CPU | **0.495937 ms/query** | paired in-run V146 flat **0.547917 ms/query** |
| p95 distinct graph + exact scored units | **1,155** | <1,562; V146 flat 3,125 units |
| p95 graph-only distinct unit evaluations | **640** | hard per-query 16P cap |
| p95 exact page-score evaluations | **1,013** | recorded separate work |
| p95 total unit-distance computations | **1,661** | descriptive |
| p95 scored physical pages | **127** | descriptive |
| Search work exhausted | **1,000 / 1,000 queries** | diagnostic |
| Max absolute exact page-score difference vs V146 | **0** | ≤0.0001 |
| Graph artifact | **426,398 bytes** | adjacency only |
| Graph build | **198.863904 ms** | 100k host only |
| Centroid resident arrays | **1,212,500 bytes** | excludes graph and allocator |
| Science process peak RSS | **55,524 KiB** | measured |
| Science cgroup memory peak | **74,391,552 bytes** | measured |

The paired CPU comparison alternated flat-first and graph-first order
by query. V146's historical flat p95 was 0.530383 ms/query on another
host and is descriptive, not the paired gate. No result here projects
100M build speed, RSS, recall, or latency.

## Miss diagnosis

The frozen `8P` nearest-unit results covered a median of only 58
distinct physical pages; the final plan scored a median 59 including
primary pages. Repeated near units on the same page consumed result
slots. Among 84,796 V140 selected pages, 75,107 occurred in pages
whose units the graph evaluated, 62,203 were in returned-unit pages
plus required primary pages, 62,202 were scored, and 62,202 survived
the sparse planner. Of the missing nonprimary pages, 9,308 had no unit
evaluated and 13,285 had an evaluated unit but no returned unit. The
first loss is bounded graph discovery; the larger loss is conversion
from nearest *units* to distinct *pages*. Planner admission was not
the cause. All 1,000 searches hit the 16P graph cap.

The frozen candidate policy is rejected. Do not tune `M`, `ef`, `8P`,
`16P`, or a vector-count threshold on these reused publication-test
queries. The next architecture should discover distinct semantic
physical pages directly, or otherwise reserve candidate slots by page,
with a policy selected on a fresh development split. Its gate must
retain paired CPU, explicit graph/exact work, and true returned quality
before 1M promotion. RAM budgets should be chosen against measured
recall and latency, rather than a hard corpus-size knee. Lean can prove
conditional graph-work, adjacency-memory, and transport bounds, but
page discovery, recall, and elapsed CPU/S3 latency remain empirical.

## Postterminal truth-cover diagnostic

After the GT-free decision was frozen, the read-only
`scripts/v150_postterminal_truth_coverage.py` recounted V150's actual
fetched ranges against the authenticated V122 GT100 and SQ8 source-ID
map. The selected pages contained **99,414 / 100,000** truth positions;
the fetched ranges, including bridged pages, contained **99,523 / 100,000**.
The fetched-range p05 was **97 / 100**, with zero queries below 90.
V141/V140's previously verified fetched physical coverage on this same
used split was **99,738 / 100,000**. The V150 range-cover deficit was
therefore 215 truth positions, much smaller than its 22,594-page
V140-plan difference.

This is a postterminal diagnostic on already-used queries, not a
preregistered V150 pass or returned Recall@100. Router nominees and
source reranking can change returned hits relative to fetched-page
coverage. It shows that exact page-plan capture was an overly blunt
screen for a changed candidate router. The next decisive gate should
measure returned IDs, lower-tail recall, and S3 work directly before
rejecting or promoting any revised page-diverse policy.
