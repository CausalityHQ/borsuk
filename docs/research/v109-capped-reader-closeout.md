# V109 precise reader with truth-free capped range admission

Status: stopped at the preregistered 200-query development prefix on
2026-09-23. No 1,000-query result or live query-latency claim exists.

## Authority and paired design

The sole scientific attempt is
`s3://borsuk-bench-453182569524-euc1/research/v109-capped-reader/382de458a49206b89d39c9c53165e2a69743b48e/runs/relaion-1m-dev1000-a0002/`.
It ran pushed source `382de458a49206b89d39c9c53165e2a69743b48e` on
one c7i.12xlarge Spot instance `i-00cc1dc8a9fdf3c86`, now verified
terminated. Terminal status was `complete`, exit 0, elapsed 298 seconds.
The launcher independently read back and SHA-256 checked every terminal
artifact. The source, query, GT100 and V63 physical permutation were checked
against their frozen source hashes. V70's 780,000,000-byte SQ8 object matched
SHA-256 `2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b`.
The regenerated V77 manifest was 92,669,264 bytes, SHA-256
`131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874`.
No query-path S3 reads were made; GETs and bytes below are **exact planned
SQ8 intervals**, not received bytes or measured latency.

The fixed ReLAION-1M **development** prefix contains 200 queries, each with
100 source-defined GT neighbors. Both arms use the same V63 256-row physical
pages, V77 1,024-region PQ192-reconstructed page summaries, PQ64 row codes,
and top-512 encoded row nomination. The historical arm coalesces nominated
pages with a two-page gap and precisely scores all fetched SQ8 rows. The
candidate ranks nominated pages by best PQ64 score, admits them under a total
32-GET/16,777,216-byte limit, bridges cheapest gaps when needed, and also
precisely scores every fetched SQ8 row. The independent reducer recomputed
physical plans, returned-ID interval membership and GT hits. Its first-200
control reproduced V77/V78: 19,832 returned GT hits, 61 GETs at p95.

| Paired arm | Returned Recall@100 | p05 hits | Sub-90 queries | GET p50/p95/max | Byte p50/max | Over-cap queries |
|---|---:|---:|---:|---:|---:|---:|
| V77 gap-2 control | **99.160%** (19,832/20,000) | 97 | 0 | 18/61/80 | 10,583,040/41,932,800 B | 63/200 |
| V109 capped range admission | **98.695%** (19,739/20,000) | 95 | 4 | 23/32/32 | 9,784,320/16,773,120 B | **0/200** |

The capped arm lost a net 93 paired GT hits, or 0.465 percentage point. It
missed the registered 99.0% returned-recall prefix filter by 61 GT hits, so
the worker did not run the remaining 800 development queries. The recorded
decision is `stop-capped-planner`. Prefix replay maximum RSS was 900,964 KiB,
elapsed 30.30 seconds, with zero swaps; manifest export took 235.248 seconds.
These are offline worker resource observations, not 100M serving forecasts.

The per-query evidence SHA-256 is
`d2d5b19add38863486f7c9a852c23b37003e96da3339b15f72f6d661317d72eb`.
The 200-query control and capped arm share query identities, row codes,
score books, SQ8 object and ground truth. This isolates the candidate's
physical-plan loss on this cohort.

## Failure localization

The historical plan already met both caps on 137/200 queries; the capped
candidate lost **zero net GT hits on those 137**. On the 47 queries where
the historical plan exceeded both the GET and byte caps, the capped plan
lost 89 of the 93 net hits. Thirteen queries exceeded only GET count and
lost two hits; three exceeded only bytes and lost two hits. Across all
queries, 77 capped plans used all 32 GETs and 55 landed within one full SQ8
page of 16 MiB. The worst query lost 18 hits as its historical plan fetched
71 GETs/34,344,960 bytes; the capped plan admitted 32 GETs/16,773,120
bytes and rejected 76 nominated pages.

Thus the failure is concentrated where **page scatter forces simultaneous
request and byte truncation**. It is not evidence that PQ64 nomination or SQ8
final scoring degraded: they are paired and unchanged. A different greedy
admission rule might recover some of the 93 hits, so the next decisive step
is an exact **truth-aware physical interval upper bound** on the same V63
layout and **whole-256-row-page** SQ8 read geometry under 32 GETs/16 MiB.
This oracle is diagnostic, never a query route. If even that upper bound
misses 99.0%/p05 90, change page geometry, row-granular I/O or physical
layout. It would not rule out sub-page ranges. If it passes, implement a materially different query-only
page selector/planner and test it against this fixed paired control. Do not
rerun the failed gap-merging arm with only a larger page shortlist.

The earlier setup-only attempt `a0001` under source `7a3c74a4` failed in
dependency setup before science; its complete failure terminal and stopped
instance are separate. Neither attempt opened a validation split.
