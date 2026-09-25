# V187 cosine PQ physical interval-plan preregistration

V185 showed a material score-metric improvement but failed the width-32,
446-unit rank gate on one fresh source panel. V186 passed that rank gate on
another fresh panel; its easier tail does not erase V185. V183's GT-blind
squared-L2 interval cover needed 2.38 times V155-scaled bytes for its
quality-passing arm, while V184 proved a truth-aware layout witness could
pass at much less. V187 asks whether the generic cosine rank signal is
selective enough **after mandatory primaries, interval bridges and GET caps**.

Use a new disjoint **ReLAION-1M D768 source pseudoquery SHA ranks
1921–2176** panel: fit 1921–2048, holdout 2049–2176, 128 queries each.
Freeze V115 nominees, V164 physical order, 32-row SQ8 units, SQ8 primary
selection, width-32 candidate union and PQ-reconstructed cosine unit rank.
Exclude each source query row from ranking and exact float64 cosine GT100.
As in V185/V186, the query row may still be a V115 nominee and seed a
candidate neighborhood; report how often that happens. This source-proxy
optimism cannot be resolved by another source pseudoquery panel and is one
reason the used validation query gate remains mandatory.
Complete every GT-blind rank and interval plan, seal their hashes to S3,
then open source truth. Score **all fetched physical units**, including
bridges and units outside the candidate union, against exact GT100.

Use the existing deterministic mandatory-cover `CoverFrontier` admission
rule. Freeze three arms per query: (1) `v155_mean_floor_elastic`: 32 GETs,
base 446 complete units, with the unit cap raised only to the query's
exact mandatory-cover floor when necessary and never above 672 units;
(2) `get32_units446`: strict 32 GETs/446 units to expose the floor effect;
(3) `elastic_16m`: 32 GETs/672 units as the full 16-MiB ceiling. V183
already observed six of 128 holdout mandatory floors above a strict
22-GET/446-unit cap, so repeating that strict mean as a purported V155
resource gate would be predetermined. V155 itself allowed up to 32 GETs
and 16 MiB per query. The exact unit charge is 24,960 bytes. A mandatory
floor above an arm's allowed cap is resource infeasibility. Report the
floor, floors above base, actual interval units, planned bytes and GETs,
per-query maxima, source hits and p05 by fit/holdout, plus infeasible
counts. No arm assumes every rank-top unit can be fetched within its cap.

The main V155-resource gate for the floor-elastic arm on the fresh holdout
requires at least
12,745/12,800 exact truth rows, p05 at least 98, complete mandatory cover,
no 16-MiB/32-GET per-query cap violations, and aggregate planned bytes no greater
than `11,134,007,040 × 128/1000` and GETs no greater than
`22,126 × 128/1000`. This is a **source-only physical-plan screen**, not
paired returned recall. If it passes, replicate on another disjoint source
panel before used ReLAION-1M validation-1000 returned Recall@100, S3
latency and charged serving RAM. If only the 32/672 arm passes the quality
and per-query physical caps, pursue an explicit elastic recall/resource
policy and measure its GET/cost tradeoff; no hidden 100M-vector knee. If
none passes, change the GT-blind optional-unit utility, global resource
allocation or layout, using V184's feasible truth-aware witness as the
upper-bound diagnostic. A failed source gate cannot be cured by choosing
the best-looking query IDs or this panel's truth.

V155 used ReLAION-1M D768 validation-1000 actual returned exact-source
Recall@100 was 99,567/100,000, p05 98 at 11,134,007,040 planned bytes
and 22,126 GETs. It is unpaired with this source panel. No current paired
S3 Vectors or Turbopuffer comparison exists. Run one immutable Causality
Spot cell, rehash complete artifacts and terminate immediately; discard
and restart an interrupted cell under a new attempt ID.
