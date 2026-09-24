# V165 postterminal primary-unit cover audit

This is an exploratory, GT-blind analysis of **closed** V165/V164/V116
inputs. It was not a preregistered decision gate and measures no recall.
It asks how much of V165's 13,545,018,240 planned SQ8 bytes is compelled
by covering the 100 frozen exact-primary rows, before spending bytes on
secondary votes or nearby rows.

For each of the 1,000 used ReLAION-1M D768 validation queries, map every
V116 exact-primary old physical ordinal `p` to the V164 new 32-row unit
`floor(inverse_v164[v63_order[p]] / 32)`. De-duplicate and sort these
units. Cover every selected unit with at most 32 contiguous GETs by joining
the smallest gaps between consecutive selected-unit runs until the GET
count fits. This is the minimum charged-unit cover of **all** primary units
under that GET cap: any cover must include each selected unit, and joining
the cheapest required gaps minimizes additional units. Charge each unit
24,960 encoded SQ8 bytes.

The resulting GT-blind lower-bound cover costs **1,057,405,440 bytes total**
over 1,000 queries, median **923,520 bytes/query**, p95 **2,146,560**;
its GET total is **18,557**, median 18 and p95 32. No query exceeds the
16-MiB byte cap. This is about 7.8% of V165's planned byte total; the
remaining V165 bytes arise from its positive-vote objective and the
additional fetched intervals. This comparison does **not** establish that
the cheaper cover returns comparable neighbors: the V162 closed stage
count found only 96,634/100,000 GT100 IDs in the frozen exact-primary
100 rows on this cohort. Extra fetched rows and exact-source reranking
have a measured role in V164's 99,553/100,000 returned GT100 result.

Reproduction inputs are the authenticated V63 order
`32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b`,
V164 order
`5b5ef48d86570e5ca68fdaaac9aef231ec7368dd526baef00474cd0a2f59a06f`,
and V116 `rust-replay.jsonl`
`3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960`.
The arithmetic was recomputed once from those terminal-closed S3 objects;
the source archive and frozen historical campaign artifacts are unchanged.
The next experiment should value added fetched rows by a query-time score
bound or a predeclared quality surrogate, with bytes and GETs explicitly
charged, rather than treat every secondary nominee as a free positive vote.
