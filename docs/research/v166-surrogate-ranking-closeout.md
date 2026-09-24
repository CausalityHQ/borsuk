# V166 source-surrogate ranking closeout

## Decision

**Kill the base isotropic source-moment surrogate.** The preregistered
GT-blind ReLAION-1M D768 source-pseudoquery holdout-128 gate failed its
matched-cap ranking test. The first 128 hash-selected source pseudoqueries
fit one variance scale (`alpha = 1.0`); the next 128 were evaluated without
ground truth or a fitted change. The comparison was the V165 equal-vote
32-row interval rule recomputed for the same pseudoqueries, with each
surrogate query capped at that baseline query's planned bytes and GETs.
This is neither Recall@100 nor live S3 latency.

| Closed holdout-128 measure | V165 equal-vote | V166 surrogate |
| --- | ---: | ---: |
| Actual above-threshold non-nominee rows captured in declared candidate universe | 434 / 465 | 380 / 465 |
| Planned SQ8 bytes | 1,700,275,200 | 927,962,880 |
| Planned GETs | 2,088 | 843 |
| Uncaptured above-threshold rows | 31 | 85 |
| Median planned bytes/query | 14,851,200 | 6,651,840 |
| Median GETs/query | 14.5 | 5 |

The paired captured-row difference was **−0.421875/query** (−54 total),
with preregistered 10,000-resample paired bootstrap 95% interval
**[−0.6796875, −0.2109375]**. Per-query wins/ties/losses were 1/106/21.
Every surrogate query used fewer planned bytes than its baseline, despite
being allowed the same maximum. Both arms passed the physical cap checker;
maximum bytes were 16,773,120 baseline and 16,149,120 surrogate, and
maximum GETs were 32 and 29. Resource savings without captured-row parity
do not pass this feasibility gate.

## Root cause and method decision

The fixed Gaussian tail model assigned zero **integer planner mass** to
units containing 322 of the holdout's 465 actual above-threshold rows:
283 of 361 positive units were rounded to zero at the preregistered
100× mass scale. The holdout's lowest predicted-probability bin contained
450 actual SQ8 exceedances among 987,503 eligible rows, while the model
predicted 42.16. It therefore stopped spending the matched allowance and
missed 54 rows that the equal-vote plan captured. The fit split had the
same directional issue: its lowest bin predicted 53.97 but contained 435
actual exceedances. Changing the scale, variance parameter, bins, or
threshold after seeing this holdout would be tuning on used evidence; no
such change is promoted.

Raw source distance and SQ8 score disagreed across the provisional
threshold for only 14 of 988,000 holdout eligible rows. Holdout mean
absolute source/SQ8 score difference was 0.0004861, maximum 0.0032481.
This rules out SQ8 quantization mismatch as the main reason for the
surrogate's failure on this closed panel. The remaining issue is its
isotropic tail assumption and positive-mass objective. The declared
candidate universe covered 32,923 distinct query-unit incidences and
1,053,536 row incidences across 128 queries; it is not the full corpus.
The source pseudoqueries were present during router and physical-order
training, so even a positive result would not have proved external-query
generalization.

The next method must change the selection signal or route, rather than
retune this Gaussian on the holdout. V138 already rejected flat exact
center/radius unit admission on deep-image-96-angular random100k because
nearly every unit remained promising; that old bound may serve only as an
optional correctness certificate, not be relaunched as the primary policy.
A bounded-work source route with a different query-dependent selection
signal is a candidate direction, provided it also avoids V98's failed
budgeted hierarchy ceiling. Any replacement needs
a new source-only construction rule, an explicit memory profile that
scales smoothly with recall and N, a cheapest decisive 100k gate against
the strongest 100k BORSUK point, then a frozen 1M returned-quality test
against V155. V155 remains the strongest used ReLAION-1M validation-1000
operating point: 99.567% exact-source Recall@100, p05 98,
11,134,007,040 planned SQ8 bytes and 22,126 GETs per 1,000 queries.
Those validation metrics are on a different cohort and are not a paired
comparison to the V166 source-pseudoquery counts.

## Authority and resources

The complete a0002 terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v166-surrogate-ranking/eda7dc6456c66a520cf161ff1001ca5c7e97671f/runs/a0002/terminal.json`
with SHA-256 `e30d429ac630b6897fc018c38158ca4c1dd0fc3d14a134ab176b534cd6e55a88`.
It binds pushed source commit `eda7dc6456c66a520cf161ff1001ca5c7e97671f`,
the source archive SHA-256
`14c284921940b10b9e2f135f243d61112d65d4025a0fb40c93fee04414c660e9`,
Spot `c7i.12xlarge` instance `i-09fccef0e8b55da76`, exit 0, phase
`complete`, and 12 artifacts. The controller streamed and rehashed each
terminal-listed S3 artifact and confirmed the EC2 instance terminated.
The independent interval/capture checker reported 128/128 pass and
`killed`. The sealed `cases.jsonl` SHA-256 is
`81c1c4c03c3d1eeadab68bd7c4e2fe44accfbc715aaf4f6c030d41486d2b45f9`;
`plans.jsonl` SHA-256 is
`1da05d75c864adb3b5dbcaec77b20b669b56386bf866eec0119dda05f2a11d16`.
The `prepare` cell took 50.64 seconds wall and peaked at 9,375,404 KiB
RSS; these are offline construction/probe resources, not serving memory.
The prior a0001 bootstrap failure is separately preserved in
`v166-a0001-bootstrap-closeout.md` and supplies no ranking measurement.
