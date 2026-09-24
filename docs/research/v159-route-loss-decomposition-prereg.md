# V159 ReLAION-100k route-loss decomposition

## Decision

V158 rejected PQ-first-100 primary and found only 77.302% physical GT100
coverage for the exact-primary control on used ReLAION-100k D768. Locate
the missing GT IDs before changing architecture: are they outside the
source-only 512 nominees, inside nominee pages but discarded by primary
selection, or inside candidate pages but excluded by the capped physical
plan? This is a diagnostic, not a parameter search or a new quality claim.

## Frozen inputs and method

Reuse V158's terminal-complete `requests.jsonl`, V114 exact-primary
`reference.jsonl` and SQ8 object, V85 exact GT100, and V158's sealed plans
and returned-result rows. Authenticate each by the SHA-256 values in the
V158 preregistration and closeout, and bind the V158 terminal SHA-256
`09306fa1aca94748635eca32ac9259e934249ee1fb620c3ec333977517a2dd35`.
No source retraining, new nominees, page votes, or GT-informed choices.

For each of 1,000 used development queries, map stable int64 GT IDs to
physical SQ8 rows. Count unique GT100 IDs within: (1) the 512 nominated
rows; (2) all physical pages containing those nominees; (3) the 100
exact-SQ8 primary rows; (4) all pages containing those primary rows;
(5) the 100 PQ-first primary rows; (6) their pages; and (7) each V158
final physical plan. Require the final-plan counts to reproduce V158's
physical coverage exactly. Report raw per-query counts and aggregate
sum/p05/p50/p95/max in hits per 100 queries, with an independent recount.

Interpret a low 512-row ceiling as a nominee representation/router defect.
If nominee rows are high but their physical pages or selected primaries
lose GT, examine page admission and graph expansion. If nominee pages are
high but the final capped plan is low, prioritize page scoring/placement
and a quality-linked I/O allowance. Do not use a corpus-name or N knee.

Run one immutable Causality Spot cell, upload terminal-bound evidence,
terminate compute after terminal, and discard any interrupted attempt.
