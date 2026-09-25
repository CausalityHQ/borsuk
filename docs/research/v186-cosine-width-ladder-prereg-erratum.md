# V186 preregistration byte-arithmetic erratum

The frozen V186 preregistration at source commit `3eb74be9` incorrectly calls
672 complete 32-row D768 SQ8 units **16,800,000 bytes**. The correct charge
is `672 × 32 × 780 = 16,773,120 bytes`, which is 4,096 bytes below 16 MiB
(`16,777,216 bytes`). The checked arithmetic is
`formal/SmoothPageBudget.lean::maximum_units_exact`.

This corrects only the explanatory byte count. The preregistered radii,
unit allowances, fit/holdout split, source-quality thresholds, and decision
rule are unchanged. A rank top-672 list is still not a contiguous interval
plan: mandatory units and bridges may make a physical plan exceed 672 units
or the GET cap. The campaign's frozen source archive and artifacts remain
identified by the original commit.
