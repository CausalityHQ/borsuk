# V144 production page-planner parity closeout

The frozen V144 Spot attempt at source `094fc86460da04e904a0e33625c70a3043870abb`
ran on `i-0e33d8e54153eb89e` and emitted terminal marker
`s3://borsuk-bench-453182569524-euc1/research/v144-page-parity/094fc86460da04e904a0e33625c70a3043870abb/runs/v144-20260924T102526Z/a0001/terminal.json`
(SHA-256 `2e73fee33b2b67c74059e342c1a27a2431f4a98e28ec0b7930313dd922d0fbb0`).
The terminal reports `failed`, phase `parity-relaion`, exit 1, elapsed 225 s.
The instance was terminated. The terminal's hashes and sizes independently
match both raw plans, both score matrices, both parity JSONL files and both
parity summaries. Both regenerated Python raw plans also matched their frozen
V140 hashes before Rust replay. The production crate `cargo check --lib`
and release adapter build succeeded.

| Frozen cohort and split | Rust/V140 parity | Rust planner p50 / p95 / p99 (ms/query) | Planned bytes / 1,000 queries | GETs / 1,000 queries | Decision |
|---|---:|---:|---:|---:|---|
| deep-image-96-angular random100k train subset; already-used publication-test ordinals 9000–9999 | 1,000/1,000 exact | 0.124907 / 0.442532 / 0.544069 | 2,693,744,640 | 28,100 | pass |
| ReLAION-1M; already-used validation-1000 | 1,000/1,000 exact | 0.279443 / 6.713421 / 6.868686 | 11,801,736,960 | 23,581 | **fail** p95 ≤5 ms |

All figures above are authenticated V144 measurements, not live S3 request
latency. V140 is the paired page-plan baseline, not a quality rerun. The
ReLAION result has 295 queries with a target-page shortfall; 272 query
planner calls exceeded 5 ms. A saturated physical-byte cap causes the
production loop to inspect many rejected score candidates. Each candidate
cloned the selected `BTreeSet` and rebuilt/sorted/allocated a complete GET
range cover. This repeated work is the generic CPU failure mechanism.

V145 changes the same production admission implementation to keep selected
pages sorted in a reusable vector, compute the exact minimum-cover charge
with reusable gap scratch for every candidate, and materialize ranges only
after an admission. The frozen V140 page plans, β=4, 32 GETs, 16 MiB budget,
and ≤5 ms p95 gate remain unchanged. No dataset-specific branch or relaxed
threshold is allowed.
