# V153 planner cover once: terminal-closed 100k reject

**Decision:** retain the generic planner refactor because it preserves every
plan and materially reduces planner CPU. Reject the current cached sparse
route on the deep-image-96-angular random100k development split: its paired
p95 CPU remains above flat. This does not establish performance at 1M or
larger scales.

## Frozen attempt and verification

The source commit is `0cf15985a352d5143d4c9dfb56ff5652a202c4e0`;
its exact Git archive has SHA-256
`9e8fc956619a63b6cf67d837b8b98110378210d2ede9d7e8a9c054149a3a195e`.
One uninterrupted Causality Spot `c7i.8xlarge`, instance
`i-09d6ae0791f54d713`, ran 441 seconds and terminated. Its complete
terminal is
`s3://borsuk-bench-453182569524-euc1/research/v152-cached-page-graph/0cf15985a352d5143d4c9dfb56ff5652a202c4e0/runs/v153-planner-once-20260924T135920Z/a0001/terminal.json`,
SHA-256 `16e74eed8353b586977883730eb4811fc91bdb2f2a29fd7b04bec5b1823c414e`.
The unchanged V152 runner retained its artifact schema, authenticated all
33 artifact lengths and SHA-256 digests, and independently recounted the
closed result. The GT-blind returned-ID audit checked 4,000 arm-query
results and all flat page scores on 1,000 queries; its maximum flat-score
discrepancy was 0.00000099957. All eight remote unit-centroid tests passed.

The split is publication test ordinals 3000–3999 on the authenticated
deep-image-96-angular random100k subset. It was already used for V151 and
V152 development. A separate full-row replay against the terminal-closed
V152 science artifact found zero plan mismatches in flat, V150, V151 and
V152 arms over all 1,000 queries. Each plan comparison included selected
pages, coalesced ranges, GETs, planned bytes, target and shortfall. There
were zero primary-roster and zero flat-score mismatches. Source Recall@100,
SQ8/source ordered results, planned bytes and GETs are consequently
unchanged from V152; the independent returned-quality recount confirmed
99.720% flat and 99.646% cached sparse source Recall@100, p05 98 hits
and zero sparse queries below 90 hits. The sparse route plans
2,648,446,848 bytes and 27,982 GETs versus flat's 2,690,858,880 bytes
and 28,267 GETs across 1,000 queries.
The two returned-quality evidence files have the identical SHA-256
`bea0686c8af9dd2e725f26ab982a735b8477b80143cb0f5b4a64ea655cbfae72`.
`scripts/v153_compare_closed_plan_parity.py` reproduces the full plan,
roster and flat-score comparison on authenticated closed science files.

## Paired CPU decision

All CPU values below are wall milliseconds per query on the new Spot host.
Each p95 is the 950th sorted value of 1,000. Phase p95 values are marginal
percentiles and do not sum to total p95. V152 historical values are from
a different instance and are only diagnostic; the new flat row is the
paired baseline.

| New-host arm | Total p95 ms | Search p95 ms | Score p95 ms | Planner p95 ms |
| --- | ---: | ---: | ---: | ---: |
| Flat | 0.129576 | — | 0.044795 | 0.088766 |
| V150 | 0.209179 | 0.120703 | 0.035215 | 0.057521 |
| V151 | 0.216348 | 0.102927 | 0.037478 | 0.077206 |
| V152 cached sparse | 0.235237 | 0.102474 | 0.057804 | 0.075996 |

Flat/cached p95 is 0.132506/0.230786 ms for even query order and
0.127664/0.237138 ms for odd order. Cached sparse is faster than flat
on 227 of 1,000 individual queries. The strict CPU screen fails by
0.105661 ms, or 81.5% relative to paired flat p95.

For diagnosis only, V152's old-host planner p95 was 0.513167 ms flat
and 0.503411 ms sparse, versus 0.088766 and 0.075996 ms on the V153
host. Exact plan parity and unchanged scoring/search methods support
that the refactor removed redundant cover work; cross-host ratios are
not a controlled performance estimate. The flat 100k centroid scan
itself costs only 0.044795 ms p95 on the paired V153 host, while the
sparse route spends 0.102474 ms in graph search and 0.057804 ms in
page scoring before planning. Its 100k CPU loss is therefore an
algorithmic cost at this small corpus size, not a planner defect.
Science maximum RSS was 32,488 KiB, isolated cgroup peak 76,042,240 B,
and graph size 426,398 bytes. No local full suite ran under devbox swap
pressure; local `cargo check --lib -j 2` and nine focused planner tests
passed before launch.

## Next scale gate

The selected production increment is one final cover materialization
after page admission, with no dataset or vector-count branch. Do not
promote the cached sparse route on this used 100k split. Freeze a paired
ReLAION-1M validation-1000 diagnostic using the same generic budgets and
an authenticated 1M layout, source-only router, graph and centroid plane.
Compare the new flat and sparse implementations on one Spot host with
source Recall@100, actual returned IDs, planned bytes/GETs, CPU phases,
and charged RAM. The relevant historical V146 flat score-plus-planner
p95 was 11.071522 ms on that already used ReLAION-1M split, but a new
paired flat baseline is required after this planner change. Confirm any
development winner on a reserved held-out cohort before freezing defaults.
Lean's conditional work/cover bounds remain useful for correctness and
scaling assumptions; recall, wall latency and charged RAM still need
measurement.
