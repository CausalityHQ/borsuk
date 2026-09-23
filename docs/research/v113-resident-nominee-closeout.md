# V113 resident nominee score screen: rejected

Status: terminal and independently validated, 2026-09-23. This is a
ReLAION-100k **development** score-fidelity result, not returned recall or a
live S3 latency result. The preregistered method and gates are in
`v113-resident-nominee-gate.md`.

The complete attempt used source commit
`9f4936fbb9bad5596921ff71004984f9b295bc57` and Causality EC2 Spot
`c7i.8xlarge` instance `i-079bb171f74c59b33`, now terminated. Its immutable
terminal marker is
`s3://borsuk-bench-453182569524-euc1/research/v113-resident-nominee/9f4936fbb9bad5596921ff71004984f9b295bc57/runs/v113-100k-score-20260923T203231Z/a0001/terminal.json`.
The marker reports `complete`, exit 0, 187 seconds. The score evidence
SHA-256 is `2af94d47966351da6dcfa451f4834658bab6fb1d3fb8fee736b007c479428fba`.
The independent validator recomputed source/SQ8 identities, all 1,000 query
rosters, primary sets, errors and reduction; its terminal-bound log reports
`validated_queries: 1000` and the same evidence hash. Local checks matched
the downloaded reduction and validation log to terminal artifact hashes.

The frozen corpus input is ReLAION-100k source SHA-256
`a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`;
the frozen development 1,000-query input is SHA-256
`4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac`.
Training used only corpus rows. All arms share exhaustive PQ64 top-512
nomination and compare their top 100 with SQ8 score ranking within that
roster. No ground truth, physical reads or returned recall entered this cell.

| Arm | Mean SQ8 primary overlap (of 100) | p05 overlap (of 100) | Mean absolute nominee score error | Mean maximum nominee score error |
| --- | ---: | ---: | ---: | ---: |
| Four-byte scalar correction | 72.538 | 58 | 0.099485 | 0.252543 |
| Sixteen-byte residual plane | 74.037 | 62 | 0.073077 | 0.209344 |

Each score-error value is in the frozen scorer's squared-L2 score units and
is averaged across the 1,000 query-level statistics over the 512 nominees.
The SQ8 scorer's maximum observed discrepancy from the corresponding
float64 mathematical expression was `5.946e-7` squared-L2 score units.
The residual plane improved mean overlap on 594 queries, tied on 125 and
lost on 281. These are verified completed-cohort observations; no unseen
query bound follows from them.

The preregistered promotion gate was mean overlap at least 95 and p05 at
least 90. Both planes fail by a wide margin. The residual plane gains only
1.499 mean primary positions over scalar correction, below the two-position
condition for a single 32-byte follow-up. The decision is **redesign the
representation**. Neither arm advances to the 1M returned-recall gate.
The recorded error reduction without comparable primary-set improvement
indicates that the 12-byte residual representation did not preserve enough
score ordering near the decision boundary; this is an interpretation of the
measured overlap, not a quantified boundary-margin result.

The measured Spot process peaks were 5,278,304 KiB for the source-only
builder, 205,696 KiB for the scorer and 1,668,240 KiB for independent
validation. Their respective wall times were 62.82, 47.86 and 43.23
seconds. These are offline process measurements on ReLAION-100k, not
100M memory, production query latency or QPS. The R16 payload projection
remains 16 bytes per vector per pinned generation but is now attached to
a rejected quality point. Future memory budgets should be chosen from
the measured `M(N,R,C,G)` frontier, without an arbitrary vector-count knee
or fixed 3-GiB ceiling.

The preceding immutable attempt under commit `406b2282` failed in the
score phase before query evidence because the frozen query Parquet uses
`vector`, while its reader requested `embedding`. Its terminal reports
`failed`, exit 1; instance `i-0c04e9c325f195a5d` is terminated. Commit
`9f4936f` fixed both the scorer and validator, and the complete attempt
above used that revision. The failed attempt contributes no quality result.

The next representation must be specified using corpus-only construction
and a width/recall resource frontier, then face the same source-frozen
100k primary-overlap gate before paired 1M development routing. An
untouched ReLAION validation split and deep-image-96-angular cross-corpus
test remain mandatory before a production default is selected. The Lean
primary-stability theorem remains conditional on authenticated score bounds
and boundary margins; this cell does not satisfy those premises or prove
unseen recall, S3 tail latency or 100M resource peaks.
