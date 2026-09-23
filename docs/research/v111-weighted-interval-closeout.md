# V111 truth-free weighted interval admission

Status: stopped at the registered ReLAION-1M **development** first-200
prefix on 2026-09-23. The remaining 800 development queries and the
untouched validation split were not run. There is no live S3 query-latency
measurement.

## Authority and paired method

The sole scientific attempt is
`s3://borsuk-bench-453182569524-euc1/research/v111-weighted-reader/8a665b12014ddd1e17cfe5b8146840ad8363d895/runs/relaion-1m-dev1000-a0001/`.
It ran pushed source `8a665b12014ddd1e17cfe5b8146840ad8363d895`
on c7i.12xlarge Spot instance `i-05cac99cba7f0ad54`, verified
**terminated**. The terminal reports `complete`, exit 0, 346 seconds.
The launcher independently read back and SHA-256 checked every terminal
artifact. The 1,015,978-byte per-query evidence has SHA-256
`ab40a28116850803f1342c30e8907994f7b6c0aefa26ca91ef3025ae1cd9e530`;
the 1,012-byte independent reduction has SHA-256
`e99168734a809f64465b580e8cf4c4c39888281587708698e49102db87ddff82`.
The authenticated V77 manifest reproduced its historical SHA-256
`131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874`.
The frozen SQ8 object was checked against SHA-256
`2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b`.

All three arms use the same ReLAION-1M development queries, V63 page
order, V77 PQ64 top-512 row nomination and SQ8 precise scorer. The
uncapped V77 control uses gap-two coalescing; V109's capped control greedily
admits pages ordered by their best PQ64 row score; V111 chooses whole-page
contiguous intervals by exact dynamic programming with a **truth-free**
page weight equal to the number of nominated top-512 rows on that page.
The validator independently rechecked each weight, the physical optimum,
the witness ranges, bytes/GETs, returned-ID range membership and GT100
hits. No query-path S3 GET took place: GET and byte figures are the exact
planned SQ8 ranges.

| Arm, first 200 development queries | Returned Recall@100 | p05 hits | Sub-90 queries | GET p50/p95/max | Byte p50/max | Cap violations |
|---|---:|---:|---:|---:|---:|---:|
| V77 gap-two, uncapped control | 99.160% (19,832/20,000) | 97 | 0 | 18/61/80 | 10,583,040/41,932,800 B | 63/200 |
| V109 best-row greedy capped control | 98.695% (19,739/20,000) | 95 | 4 | 23/32/32 | 9,784,320/16,773,120 B | 0/200 |
| V111 top-512 count-weighted exact intervals | **98.535%** (19,707/20,000) | 92 | 5 | 14/32/32 | 15,974,400/16,773,120 B | **0/200** |

V111 lost 32 returned GT hits to the paired V109 capped control and
missed the preregistered 99.0% prefix gate by 93 hits. The worker
recorded `stop-weighted-planner`. Its prefix replay took 58.27 seconds
wall time with maximum RSS 966,652 KiB and zero swaps; these are offline
worker resources, not serving latency or a 100M memory projection.

## Failure localization and decision

Post-terminal, SHA-checked per-query cross-analysis with V110's same-query
truth-page roster recomputed actual **range containment**:

| First-200 diagnostic | V77 uncapped | V109 capped | V111 weighted |
|---|---:|---:|---:|
| GT hits whose pages lie in fetched ranges | 19,925 | 19,826 | **19,791** |
| Returned GT hits after identical SQ8 scoring | 19,832 | 19,739 | **19,707** |

The raw top-512 nominated pages themselves contained 19,921 GT hits.
As a **diagnostic only**, a truth-aware exact interval plan restricted to
the nominated truth-bearing pages can include all 19,921 of those hits
under both caps on every query; one optimal witness had GET p95 17,
GET max 28 and charged-unit p95 328 of 336. This establishes room for
a better query-time page value estimate without expanding nomination,
although it cannot tell a serving router which pages carry truth.
V111's actual ranges lost 35 containment hits relative to V109 and its
returned result lost 32. Weighted planning was better on 12 queries,
tied on 164 and worse on 24 for containment; returned hits were better
on 11, tied on 166 and worse on 23. The count objective therefore made
the **page-choice information** worse under the same caps. The unchanged
SQ8 scorer did not cause this paired decline. The median **paired**
per-query difference was 2,995,200 more bytes and four fewer GETs for
V111. The byte/GET shift is consistent with longer coalesced ranges;
the range-level cause has not been separately decomposed.
The pairwise range difference lost 95 GT positions fetched only by V109
and gained 60 fetched only by V111. Among nominated truth pages in those
differences, the median best-PQ64 page rank was 34 for losses and 80 for
gains (zero-based). Fourteen discordant truth positions lay on bridged,
unnominated pages. The count utility traded higher-ranked pages for
lower-ranked ones on this fixed cohort.

This is a negative result for **top-512 row count as page utility**, not
for the exact optimizer or for the V63 layout's physical feasibility.
V110's truth-aware whole-page oracle still contains all 20,000 prefix
GT positions within both caps. Do not rerun V111 with another count
threshold or expand its page count. The next design must change the
query-time evidence for page value, and then test the resulting returned
recall against the same V109 capped control under both caps. If that
evidence cannot recover the missing pages, revise the resident router or
page representation. Memory is evaluated as a measured frontier over
`N`, recall target, concurrency and pinned generations, without a fixed
3-GiB 100M ceiling or an arbitrary vector-count knee.
