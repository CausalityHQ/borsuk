# V121 second attempt: D96 planner state limit

The immutable attempt at
`s3://borsuk-bench-453182569524-euc1/research/v121-deep-image-paired/8f8aa39e16871121ed97a8c9d5d78204f4f316f5/runs/v121-20260924T013245Z/a0002`
ended in `replay` with `BudgetTooLarge`, exit code 1. Terminal SHA-256 is
`bd3f61a9ff9e563bd432638bcf76625e01f8721aedbbc215b64abbe9b232319c`.
Spot `i-0e32f78a3c7767ea0` was tagged `a0002` and terminated. The original
launcher propagated exit 1. All 22 terminal-listed artifacts were downloaded
and independently checked by length and SHA-256. No GT object was downloaded,
and there is no returned-quality result.

The fixed candidate and control requests were composed for all 1,000 queries.
The Rust replay returned seven query records before the planner rejected query
ordinal 7. That query had 205 distinct weighted pages. D96's existing 32-row
unit is 3,456 bytes; 16,777,216 bytes permits 4,854 such units. The DP guard
calculates `(32+1) × (4,854+1) × 205 = 32,844,075` state transitions,
above its 32-million limit. The failure comes from a redundant budget lattice,
not an observed recall miss or a physical byte-cap violation.

For this 9,990,000-row D96 layout the final page has 112 rows, charged as
four 32-row units; a full page costs eight. Every plan's charged units are
therefore a multiple of `gcd(8,4)=4`. Divide the full-page and final-page
charges and budget by four while multiplying unit bytes by four. This is a
one-to-one relabeling of every feasible plan, including the same physical
ranges, exact bytes, weights and tie order. The failing query then requires
only `33 × 1,214 × 205 = 8,212,710` guarded transitions. Across the sealed
1,000 request records the maximum distinct weighted pages was 368, giving a
normalized bound of 14,742,816, below the existing guard. The optimization
uses the page geometry only, not query or GT values. Focused exhaustive
small-layout tests check unchanged plans and bounds.

This exact reduction is generic arithmetic, but an arbitrary final-page
length can have gcd one and leave the original complexity. It is a way to
finish the frozen V121 measurement, not a claim that the planner is already
scale qualified for all `(N,D)` or that its CPU latency is acceptable. A
future production planner needs a generic bounded-work algorithm, with any
conservative approximation separately qualified. The 1,000 query vectors and
seven returned-ID records have been processed, but no GT values were fetched
or inspected. An algorithmic change after this attempt will use a new held-out
query cohort; the exact lattice relabeling leaves this attempt's method intact.
