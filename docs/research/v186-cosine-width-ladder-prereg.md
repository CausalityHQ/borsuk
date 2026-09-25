# V186 cosine PQ candidate-width ladder preregistration

V185's paired score screen showed that PQ-reconstructed cosine ranked the
same width-32 candidates better than squared L2, yet missed the 446-unit
source gate by seven truth rows. One holdout query caused 28 of 49 candidate
misses. V186 asks whether a **generic wider candidate neighborhood** fixes
the tail without relying on query IDs, data-set-specific routing or a fixed
corpus-size knee. Wider neighborhoods change candidate reach; the scored
unit allowance separately controls potential object bytes.

Use a new disjoint **ReLAION-1M D768 source pseudoquery SHA ranks
1665–1920** panel: fit ranks 1665–1792, holdout 1793–1920, 128 queries
each. Freeze the V115 nominees, V164 row order, 32-row SQ8 unit geometry,
SQ8 mandatory primary units and exact float64 cosine GT100 definition.
For candidate radii 32, 64 and 128 physical units, rank complete candidates
by the best row's PQ-reconstructed cosine score. Exclude the source query
row from PQ minima and exact source truth. Seal every GT-blind roster and
complete ranking to S3 before opening truth. One widest-radius score field
may be projected onto narrower nested rosters only if the identical scores
and roster identities are checked.

For each width and split, report candidate ceiling, candidate units per
query (mean, max and p95), exact-source containment at top 446, 672 and
1,344 ranked units, and per-query p05. Report Spot offline scoring resources.
The 446-unit point is the V155 mean complete-unit byte allowance; 672 is an
exploratory elastic allowance of **16,800,000 bytes/query before interval
bridges**, not 16 MiB or a serving memory measurement. Both are source-only
rank screens. A radius that approaches full-corpus candidate scoring must
be identified explicitly even if its truth ceiling is high.

The first decision is the smallest radius passing **at 446 units** on the
fresh holdout: at least 12,745/12,800 exact source truth rows and p05 at
least 98. If no radius passes at 446, apply the same gate at 672 and select
the smallest passing radius for an explicitly elastic physical-plan test.
If none passes, revise the candidate generator or representation; do not
keep increasing a hidden default budget to polish one panel. No radius is a
production default from this experiment. A positive result must pass a
fresh sealed GT-blind mandatory-cover interval plan and then paired used
ReLAION-1M validation-1000 returned Recall@100, S3 latency and charged
serving RAM before promotion. The strongest current returned-quality
comparator is V155 used validation-1000 99,567/100,000, p05 98 at
11,134,007,040 planned bytes and 22,126 GETs; source pseudoqueries are
unpaired and cannot be called a product win.

Run one immutable Causality Spot cell, seal all rankings before truth,
rehash complete artifacts and terminate immediately. An interrupted cell is
discarded and restarted under a new attempt ID.
