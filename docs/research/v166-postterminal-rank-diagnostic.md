# V166 postterminal nominee-rank diagnostic

This is a read-only diagnostic on the **closed** V166 a0002 GT-blind
ReLAION-1M D768 source-pseudoquery holdout-128 evidence. It does not
measure Recall@100, validate a new planner, or change the killed V166
decision. The V166 complete terminal SHA-256 is
`e30d429ac630b6897fc018c38158ca4c1dd0fc3d14a134ab176b534cd6e55a88`.
The read-back `cases.jsonl` and `plans.jsonl` matched their terminal hashes
`81c1c4c03c3d1eeadab68bd7c4e2fe44accfbc715aaf4f6c030d41486d2b45f9`
and `1da05d75c864adb3b5dbcaec77b20b669b56386bf866eec0119dda05f2a11d16`.
The separately downloaded V63 old layout and V164 order matched their
published SHA-256 values
`32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b`
and `5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f`.
`scripts/v166_postterminal_rank_diagnostic.py` independently recounts this
note from those four authenticated files. Its committed canonical JSON
output is `v166-postterminal-rank-diagnostic.json`, SHA-256
`aed2f873c2a6e37fcf6128fbac5b8684487fa83a05a591a6c1862a836cbf04ca`.

For each candidate 32-row unit, map each of its nominated old physical
rows through V63 source order and V164 inverse order. Take the smallest
zero-based PQ64 nominee rank in the unit; units with no nominee but
included as immediate neighbors form a separate category. Count the
closed actual above-threshold non-nominee SQ8 rows and whether each
frozen interval plan fetched their unit.

| Minimum PQ64 nominee rank in unit | Actual rows | V165 equal-vote captured | V166 surrogate captured |
| --- | ---: | ---: | ---: |
| 0–99 | 173 | 173 | 171 |
| 100–199 | 66 | 66 | 57 |
| 200–299 | 43 | 42 | 32 |
| 300–399 | 30 | 25 | 23 |
| 400–499 | 31 | 30 | 21 |
| 500–511 | 1 | 1 | 1 |
| Neighbor only | **121** | **97** | **75** |
| **Total** | **465** | **434** | **380** |

The 121/465 useful rows in units with no nominee show that candidate-unit
selection cannot be reduced to nominee-rank weighting alone. Physical
neighbor expansion or another query-dependent signal is material even
within this narrow candidate universe. Equal-vote interval merging
incidentally captured 97 of those 121 rows. These are source pseudoqueries
seen by source-only construction, with no GT opened; the table cannot
calibrate a recall profile or justify tuning a halo width on this holdout.

An additional score-aware **oracle lower bound** requires every
exact-primary unit and every unit containing an actual above-threshold
non-nominee row in the declared candidate universe. For each query, merge
the smallest physical gaps until at most its V165 baseline GET count
remains. The resulting minimum contiguous 32-row-unit cover is
222,343,680 encoded bytes over 128 queries (median 1,123,200 B/query),
versus 1,700,275,200 bytes for the V165 plans. It fits the corresponding
baseline byte cap on 127/128 queries. This oracle sees the very SQ8
scores the policy is supposed to predict, so it is only a headroom
diagnostic; it cannot be implemented as a source-only query policy or
be interpreted as achieved recall. Within the declared universe, the
binding problem is selection information, not a physical lower bound
near V165's byte spend.
