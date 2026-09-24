# V162 closed 1M route-loss decomposition: closeout

## Decision

V161's negative 1M transfer is a **layout-and-admission** failure, not a
single weighted-interval tuning issue. On the used ReLAION-1M D768
validation-1000 split, all pages touched by the 512 source-only nominees
contain 99.387% of exact GT100, but pages touched by the frozen exact-SQ8
primary 100 contain only **98.874%** (p05 94). That is already below
V161's 99.400% / p05 97 transfer floor. The actual 32-GET/16-MiB
physical plan contains **96.881%** (p05 81), compared with the closed
V155 cached sparse plan's **99.647%** (p05 98).

The page-aligned minimum 32-GET cover of all V161 primary pages breaches
16,777,216 bytes on **227/1,000** queries. No better optimizer can
retain every primary page on those queries under this page-aligned format
and cap. For the other 773 queries a complete primary-page cover fits,
but the aggregate and tail still require a separate planner analysis
before assigning any specific recoverable loss. The combined evidence
rejects the V161 512-row balanced-two-means one-wave layout/plan as a
production candidate at 1M. Keep V155 as the strongest measured 1M
point; do not sweep page width or cluster count on used validation truth.

## Authenticated stage counts

Each count is distinct GT100 IDs over the same 1,000 **used** queries;
100,000 is the maximum. Row/page stages are coverage ceilings, not new
returned-score results.

| Closed V161/V155 stage | GT100 / 100,000 | p05 hits/query |
| --- | ---: | ---: |
| V161 512 nominee rows | 96,849 | 85 |
| All relaid pages touched by nominees | 99,387 | 97 |
| V161 exact-SQ8 primary 100 rows | 96,634 | 85 |
| All relaid pages touched by exact primary | 98,874 | 94 |
| V161 final fetched ranges | 96,881 | 81 |
| V155 cached sparse final fetched ranges | 99,647 | 98 |

V161's exact-primary pages span a median 30 and p95 56 distinct pages.
The GT-blind minimum page-aligned 32-GET cover costs a median
11,980,800 bytes, p95 25,159,680 bytes and maximum 82,268,160 bytes;
773 queries fit 16,777,216 bytes. The final plan's 96,881 GT hits are
1,993 below the primary-page coverage total as a **net difference**;
the page and final sets are not nested, so this number is not a count
of particular neighbors lost by the optimizer. The result does not
rule out arbitrary row-addressable formats, a different source-only
layout, or a higher explicitly charged byte/GET profile.

## Provenance and limits

One Causality `c7i.xlarge` Spot instance `i-0ddda0a718af222dc` ran from
clean pushed source `e2b8294914096784493366946112b32b7ae7cae5`,
completed, and was confirmed terminated. Immutable prefix:
`s3://borsuk-bench-453182569524-euc1/research/v162-closed-route-loss/e2b8294914096784493366946112b32b7ae7cae5/runs/a0001/`.
Source archive SHA-256:
`65e3dd35e4feea812869eb09d1e2b799b8bc0611f26f6016915181e23bed7e74`.
Terminal SHA-256:
`1c0cfd3e1cfe7d26e83e43b2f45d029b4f8ec7f0029384fcf94c38bcfb03b5a9`.
The controller independently read back all four terminal-listed S3
artifacts and matched each length and SHA-256. Raw SHA-256:
`7fe469bb5bf8d9e00ffaf37b0ba1d0d2667c2346b76d144b7f31c71e763ca76e`;
summary SHA-256:
`5ee670da4317d90e9a2c07bbe0b341412619e6b145d5e23c90323a5eb9d17080`;
separate recount SHA-256:
`4d9bc5c468c0e6821b7a1ca4ba86639540b55afdf5ad6294867905cfa8e2c84e`.
The recount recomputed all 1,000 stage counts from authenticated V63
permutation, V161 membership/plans, V116 primary rosters and V36 GT,
and reproduced V161/V155 physical coverage and the classification.

This postterminal diagnostic makes no new quality, latency, 100M memory
or service-cost claim. The next design must change the source-only
layout/routing representation or the charged transport format/profile,
and compare against V155 on the same cohort. A generic profile may
spend more RAM for a higher requested recall; it must disclose the
resource cost and qualify on multiple corpora without a dataset-name
switch or vector-count knee. Lean can prove conditional cover/resource
bounds, not empirical recall or S3 latency.
