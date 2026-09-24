# V168 PQ64 adjacent-unit ranking closeout

## Decision

**Advance the direct PQ64 row-score field to a separately frozen,
GET-and-byte-aware physical planner experiment.** The decision concerns
ranking of adjacent-only units on a source pseudoquery proxy. It does
not promote a production index, establish returned Recall@100, or
measure serving latency.

The preregistered ReLAION-1M D768 panel used human source-hash ranks
513–640 (zero-based ordinals 512–639), disjoint from V166's ranks
1–256 and V167's ranks 257–512. The frozen source, V70 SQ8, V63 old
layout, V115 PQ64 router, and V164 source-only physical order were
authenticated by their preregistered S3 lengths and SHA-256 digests.
The comparison used the **same** adjacent-only candidate set and 16,
32, or 64 physical-unit budget for both features. The control ranked
units by the best PQ rank of a nominee in an immediately neighboring
unit. Direct ranking used the minimum PQ64 ADC score among the unit's
rows, excluding the query's own row. Labels were non-nominee SQ8 rows
at or below the query's exact-primary 100th SQ8 threshold, also
excluding the query row. The GT-blind plan was sealed before labels
were opened.

| Measure, 128 queries | Direct PQ64 | Neighbor-rank control |
| --- | ---: | ---: |
| Captured useful adjacent-only rows at 16 units/query | 61 | 16 |
| Captured useful adjacent-only rows at 32 units/query | 89 | 32 |
| Captured useful adjacent-only rows at 64 units/query | 133 | 61 |

There were 150 useful adjacent-only rows in 96 units. The 32-unit
paired gain was **57 rows**, with an exact preregistered 10,000-resample
PCG64(168) paired-bootstrap 95% interval of **17–121 rows** for the
128-query total. Per-query direct wins/ties/losses were 18/107/3.
All preregistered gates passed: gain at least eight, lower interval
endpoint above zero, and no aggregate direct regression at 16 or 64
units. Across the panel, the candidate field enumerated 35,586
physical units and scored 1,138,752 resident-code rows. The 49,152
row/3,145,728 PQ-lookup **per-query maximum** remains a conditional
code-geometry bound, not a measured per-query average or serving
latency.

The offline `prepare` process took 27.85 wall seconds and peaked at
9,261,340 KiB RSS; `plan` took 0.21 seconds and peaked at 78,760 KiB.
The later check-only replay took 28.06 seconds and peaked at
9,277,340 KiB. These include source-parquet loading and are **not**
charged resident serving RAM. A complete PQ64 code plane is 64N bytes
per generation, so one 100M-row plane would have 6.4 GB code payload
before other memory; that is a projection, not a 100M observation.

## Immutable execution and validation

- Source commit for the measurement: `9cb5a36a18b061d0a9f1cb5eedc4dfcd2b719b08`.
- Measurement: `s3://borsuk-bench-453182569524-euc1/research/v168-pq-neighbor-ranking/9cb5a36a18b061d0a9f1cb5eedc4dfcd2b719b08/runs/a0002/` on Causality Spot `c7i.12xlarge` instance `i-08319c2ddbe69ebe0`. The terminal SHA-256 is `5a9e8020c6be4daa65df0c48dcabe0079132eeba09aeea676ff9a6e2375b5d3a`. Its terminal status is `failed` solely because the checker compared integer budget keys in memory against string keys after JSON serialization. The sealed summary SHA-256 is `d54c4469d09769505f8a2674c5b6ceb9ab025f5cf1a47a20deba60df704962ce`; its plan-seal and label SHA-256 digests are `f335e211a1e0862be53ba10af056e34d829525781cbcb108504ba1ce6960af51` and `c95959bab234cd1f47d4f0e9924cd52689e407523936cf093e549496b6924c42`. The terminal-listed artifacts were streamed and rehashed before the instance was confirmed terminated.
- A prior `a0001` never emitted query rows. Its terminal is `failed` in `prepare` because the frozen V63 old-order array is authenticated `<i4`, while the new score-field validator initially demanded `<i8`. The terminal SHA-256 is `c8c3364442a846c09bee35255fce50d749fba42807efb29690ab06d935f1caa9`; its instance `i-00c004dc8c594ed01` was terminated. The narrow dtype repair was pushed before `a0002`.
- Checker repair commit: `ac0f991bceb9606b4c9b476123f9aba7f29fee28`. A **check-only**, not a new measurement, loaded the exact closed `a0002` artifacts and all frozen inputs, verified their lengths and SHA-256 digests, independently checked candidate geometry, replayed the source/score computation and sealed hashes, and compared the summary. Its Causality Spot instance `i-04ea1e9a6c1f385df` returned `status: pass`, 128 queries, `advance-to-planner`; it was terminated after the terminal and all listed artifacts were streamed and rehashed. Its terminal SHA-256 is `57eec6dc39d67309f5040b405aafd3b4a623af52d3067c4b790db59310714d91` at `s3://borsuk-bench-453182569524-euc1/research/v168-closed-check/ac0f991bceb9606b4c9b476123f9aba7f29fee28/runs/a0001/`.

The original `a0002` terminal remains failed and unchanged. Its
measured summary is treated as verified by the separately closed,
versioned check-only run; it is not relabeled as a successful original
campaign.

## Next gate

Freeze one **generic** planner before opening any new labels. Retain
all exact-primary units. Admit candidate units using the direct PQ64
field and optimize **both** remote bytes and GETs under a caller
resource profile, with the physical 16-MiB and 32-GET ceilings checked
separately. A unit utility must be calibrated without dataset names,
corpus-size knees, or the 128 used V168 labels. Couple any charged RAM
limit to corpus size and requested recall; measure the actual code,
router, generation, and concurrency charge. Compare the frozen plan on
fresh paired quality cohorts against the strongest current BORSUK
baselines: V163 ReLAION-100k D768 development-1000 returned Recall@100
99.322%, p05 98, 15,657,408,000 planned bytes and 15,275 GETs;
also show V160's 15,562,734,720-byte/9,894-GET resource point.
At ReLAION-1M D768 used validation-1000, compare against V155's
99.567%, p05 98, 11,134,007,040 planned bytes and 22,126 GETs.
These baselines are historical measurements from their own frozen
revisions; a new matched campaign must reproduce them under disclosed
equivalent conditions before a product comparison. Only a decisive
100k win should promote to the more expensive 1M gate, then to live
S3 latency/cost and 10M/100M resource qualification. The current
V115 flat summary route is not yet scale-qualified.
