# V166 admission-method decision after V165

## Evidence and selected question

V164's smooth source-only 1M layout passed the used-cohort transfer floor
but missed the paired V155 quality/byte gate: 99.553% versus 99.567%
exact-source Recall@100 and 14.568 versus 11.134 GB planned SQ8 bytes
per 1,000 queries. V165 changed the fetch atom to 32 rows under the same
positive-vote objective; bytes fell to 13.545 GB but remained 21.65% over
V155. Its GT-blind plan-only gate therefore killed unit width alone.
The postterminal lower bound for fetching every frozen primary unit is
1.057 GB over 1,000 queries, but those rows alone contain only 96.634%
of GT100 IDs on this cohort. Additional unit selection determines quality.

The next cheapest question is **whether source-built unit statistics rank
new, locally scorable candidate rows better than the equal-vote objective
at a matched physical byte/GET allowance**. This is a score-surrogate
feasibility probe, not a recall or live-service gate. A promising model
would then justify a separately frozen returned-quality gate.

## Alternatives and corrections

A completed Fable research consultation `bc73698edb1547ed` suggested
expected-exceedance-mass admission. Its useful change is to stop spending
merely because one more low-ranked nominee carries a positive vote.
Before implementation, several claims require correction:

- A Gaussian exceedance estimate is **not a recall certificate**. It can
  only predict counts within a declared candidate universe. Units outside
  that universe and query-distribution shift remain unaccounted for.
- The V114 interval DP stores score/budget states but its public function
  reconstructs only a maximum-score plan. A minimum-cost state meeting a
  target mass needs a new, verified API and tie rule.
- Higher requested mass has a nondecreasing *minimum feasible cost* for a
  fixed universe/model. Independently selected optimal unit sets need not
  be nested. Lean should prove the cost statement, not set containment.
- The first source-moment formula applies to cosine. An L2 score-moment
  formula and a separate ReLAION-100k L2 transfer are required before
  claiming a generic metric policy.
- The model can select a quality profile only after fresh, cross-corpus
  calibration. `r` in a provisional mass threshold is an internal risk
  allowance, not a proven Recall@100 guarantee.

A completed Claude architecture check `f450b11225e04f44` found that an
authenticated local SQ8 mirror is already implemented and V133 measured a
100k D96 placement replay. Moving query bytes to NVMe changes the cost
accounting; it does not improve nomination or justify a new quality claim.
V121 measured 184.76 ms/query for the flat router at 9.99M, so bounded-work
nomination remains a separate 10M/100M gate. The local option may remain a
measured serving tier, with warm/cold hydration and generation-overlap costs
charged. We will not relabel planned S3 bytes as a local-tier speedup.

## V166 source-only surrogate probe

Keep V164's authenticated source-only cosine order and 32-row units.
For each unit, build a mean vector and residual second moment from only
source vectors. The base summary payload is `2D/32 + 4/32` bytes per row
if the mean is f16 and residual energy f32: **48.125 bytes/row at D768**,
or 4.8125 GB for 100M rows per generation before headers, maps, cache,
alignment and graph. This grows linearly with N and contains no N knee.
It is a modeled payload, not charged RAM. Optional directional refinement
is deferred until this base summary proves useful; its memory must be an
explicit quality-tier choice.

Choose source pseudoqueries by a fixed hash of stable ID. Fit the one
global residual-scale parameter only on one source-disjoint fit partition;
check ranking/calibration on a disjoint source partition. Do not use V36
validation GT or V155/V164 returned hits to fit it. Exclude a pseudoquery's
own source row from its neighbor roster and all scored exceedance counts.
For a pseudoquery,
the provisional threshold is the 100th exact local SQ8 score among the
frozen 512 nominees. The candidate universe is the union of each nominee's
32-row unit and its immediate physical neighbors, de-duplicated. This
universe is fixed by the same rule for any N and corpus; it is not the whole
index. Predict for each unit how many locally unscored rows exceed the
threshold from the source moments. Independently score all rows in that
candidate universe from the authenticated local SQ8 mirror to count actual
exceedances without opening GT. Record the model's per-unit calibration,
ranking and candidate-universe width.

Compare unit ranking against V165's equal-vote ranking at matched 32-GET
and encoded-byte charges. Freeze the planner objective and a holdout
decision before running: the model must improve captured actual
above-threshold rows on disjoint pseudoqueries, with a positive paired
bootstrap lower bound, while meeting every cap. Otherwise kill this
surrogate and redesign the route/objective. A pass permits a separate
used-cohort returned-quality screen against V155, then fresh data and
live service. It does not validate the requested-recall mapping, 100M
latency, or the cost of the local tier.

Lean can prove: authenticated 32-row unit counts imply GET/byte bounds;
for a fixed model and candidate universe, raising a required mass cannot
lower minimum feasible charged cost; and a sound per-unit *upper score*
bound below the current exact threshold permits safe pruning. Lean cannot
prove the Gaussian estimate is calibrated, that its candidate universe
contains enough true neighbors, or measured recall/latency/cost. Those
remain separate gates.
