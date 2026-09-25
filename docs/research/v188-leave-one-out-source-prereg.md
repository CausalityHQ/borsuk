# V188 leave-one-out nominee source screen preregistration

V187 found that every one of its 256 source pseudoqueries had its own row
among V115's 512 nominated rows. Excluding that row from ranking and exact
GT100 did **not** remove its physical unit from the nominated candidate
neighborhood. V185–V187 source-only quality may therefore be optimistic for
external queries. V188 isolates this methodological effect before another
planner or production-default claim.

Use a fresh disjoint **ReLAION-1M D768 source pseudoquery SHA ranks
2177–2432** panel: fit ranks 2177–2304, holdout 2305–2432, 128 queries
each. Freeze V115 PQ router, V164 physical order, 32-row SQ8 units,
width-32 candidate expansion and PQ-reconstructed cosine unit scoring.
Request the deterministic top **513** V115 nominees once per query. Arm
`self_included` uses the first 512, exactly matching the historical
nominee rule. Arm `leave_one_out` removes the query's own old-physical row
from the 513 list, then takes the first 512 remaining. If the own row is
absent, both arms use the same 512. The replacement is the next PQ-ranked
row, so shortlist size remains 512. Validate the top-512 prefix against
the original 512-call rule on each query. Both arms exclude the query row
from PQ rank minima and exact float64 cosine GT100. No source ID or query
class changes the algorithm.

Seal both complete GT-blind candidate sets and unit rankings to S3 before
opening source truth. Report own-row nominee frequency and rank, candidate
unit counts, candidate truth ceiling, exact source truth in the top 446 and
672 ranked units, p05, and paired per-query win/tie/loss of leave-one-out
versus self-included on fit and holdout. These are source-only unit-rank
screens: no contiguous interval plan, returned recall, S3 latency or
charged serving RAM is measured.

The holdout decision uses `leave_one_out`, because it better approximates
an external query. If top 446 contains at least 12,745/12,800 source truth
rows with p05 at least 98, advance that roster policy to a fresh GT-blind
physical interval gate. Otherwise, if top 672 reaches the same gate,
advance only an explicit elastic resource arm. If neither reaches it,
change candidate generation or representation rather than restoring the
query's own row or tailoring exceptions. Record the paired loss or gain
even when the gate passes; this panel cannot erase V185's failed tail.
Any pass still requires paired used ReLAION-1M validation-1000 returned
Recall@100, live S3 latency and charged serving RAM. The V155 used-query
baseline remains 99,567/100,000 returned GT100 hits, p05 98, at
11,134,007,040 planned bytes and 22,126 GETs, unpaired with this panel.

Run one immutable Causality Spot cell, rehash complete artifacts and
terminate immediately. An interrupted cell is discarded and restarted
under a new attempt ID. No dataset-specific default or fixed 100M-vector
memory knee is inferred from this source experiment.
