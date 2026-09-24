# V166 postterminal equal-vote byte curve

This is an exploratory, GT-blind replay on the **closed** ReLAION-1M D768
V166 source-pseudoquery holdout-128. It is not a new preregistered gate,
returned Recall@100, or live S3 measurement. It uses only V166's sealed
per-unit SQ8 above-threshold counts, nominees and exact-primary rows,
plus the authenticated V63/V164 physical orders. The input hashes and
replay code are pinned in `scripts/v166_postterminal_iso_byte_curve.py`.
Its 16-MiB arm exactly reproduced all 128 recorded V165 equal-vote plans,
providing a same-method check. The committed canonical JSON output is
`v166-postterminal-iso-byte.json`, SHA-256
`103dc29cc644739284814ac6b5b40eda1bc35e80cb14cc39080641ce73208611`.

The replay keeps V165's 513/1 primary/nominee weights and at most 32 GETs.
It changes only a uniform per-query maximum encoded byte allowance.
`V155-mean-B` is 11,134,007 B/query, the floor of V155's used
validation-1000 mean planned bytes. These are **different query cohorts**;
this label sets a byte scale, not a paired quality comparison.

| Per-query byte cap | Actual rows captured / 465 | Planned bytes / 128 queries | GETs / 128 queries | Queries covering all exact-primary units |
| --- | ---: | ---: | ---: | ---: |
| 6 MiB | 395 | 714,604,800 | 2,655 | 127 |
| 8 MiB | 407 | 925,317,120 | 2,454 | 127 |
| V155 mean, 11,134,007 B | 420 | 1,198,704,000 | 2,263 | 128 |
| 13.5 MiB | 433 | 1,487,491,200 | 2,152 | 128 |
| 16 MiB | 434 | 1,700,275,200 | 2,088 | 128 |

At almost the same aggregate encoded bytes, the **unchanged** equal-vote
rule at 8 MiB captured 407 rows in 925.3 MB; V166's surrogate captured
380 in 928.0 MB. This independently supports killing the surrogate as a
selection signal. The equal-vote arm spent more GETs (2,454 versus 843),
so it is an iso-byte diagnostic, not an iso-GET or latency result.

Restricting equal-vote to the V155 mean byte scale retained 420/434
above-threshold rows captured at its full 16-MiB cap while cutting this
cohort's planned bytes by 29.5%. That suggests the positive-vote stopping
rule contributes materially to V165's byte excess. The 14 lost rows,
GET increase, pseudoquery leakage, and unobserved rows outside V166's
candidate universe prevent any inference that V155's Recall@100 or live
latency would be preserved. A new policy needs fresh, preregistered
returned-quality and resource gates; this used holdout cannot choose its
threshold.
