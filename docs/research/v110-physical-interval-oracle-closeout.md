# V110 whole-page physical interval oracle

Status: terminal-complete offline diagnostic on 2026-09-23. This is a
truth-aware upper bound on **page containment**, not a serving route,
returned-recall result, or latency measurement.

## Authority and scope

The single scientific attempt is
`s3://borsuk-bench-453182569524-euc1/research/v110-physical-oracle/831878a472298a636fa8778a688ef335897be444/runs/relaion-1m-dev1000-a0001/`.
It ran pushed source `831878a472298a636fa8778a688ef335897be444`
on c7i.8xlarge Spot instance `i-0b35ba589b8ab7e8b`, verified
**terminated** after a complete, exit-0 terminal in 32 seconds. The
launcher read back and SHA-256 checked every terminal artifact. The
209,912-byte per-query evidence has SHA-256
`7c7aeab4b1d9fe0b9fcd4cbaac5e509bf4edfaec65016d661e1ed707e9761b81`;
the 635-byte independent reduction has SHA-256
`f212176fd8e6c59f91d346cf62eb37544c5d63fdfee1ad6bca1e1f22e75e4c7a`.
The source IDs, GT100 and V63 physical permutation matched their frozen
SHA-256 identities. The reducer independently remapped every truth ID to
its physical page and recomputed every optimum. Small random layouts were
also checked by exhaustive interval enumeration in local unit tests.
Independent readback of the SHA-matched truth Parquet found exactly 100
distinct GT IDs in each of its 1,000 queries.

For each ReLAION-1M **development** query, the oracle sees all 100 truth
IDs and maximizes the number whose V63 256-row SQ8 pages fit in at most
32 contiguous GET ranges and 16,777,216 bytes. Each full page costs
199,680 bytes; the last short page costs 49,920 bytes. A dynamic program
charges every included gap page and searches GET count, byte units and
open/closed interval state. Its endpoints need only lie on truth-bearing
pages: moving an endpoint through zero-weight pages cannot increase covered
truth and can only consume bytes. This establishes exactness for the stated
whole-page geometry. It does not constrain subpage reads or a new layout.

| Fixed cohort | Oracle GT hits | Containment | p05 hits | Sub-90 queries |
|---|---:|---:|---:|---:|
| ReLAION-1M development, first 200 of 1,000 | 20,000/20,000 | 100.000% | 100 | 0 |
| ReLAION-1M development, all 1,000 | 99,996/100,000 | 99.996% | 100 | 0 |

Exactly 998 queries permit all 100 truth pages under both caps. Query
343 permits 97 and query 852 permits 99. The offline oracle process used
154,692 KiB maximum RSS in 2.72 seconds; validation used 148,712 KiB in
1.57 seconds. These are worker resources, not serving memory or latency.

## Paired failure localization

On the identical first 200 development queries, V109's query-only capped
reader returned 19,739/20,000 GT hits (98.695%) with no cap violations.
Its uncapped V77 control returned 19,832/20,000 (99.160%) but exceeded
one or both caps on 63 queries. The truth-aware V110 ceiling exceeds the
capped returned result by 261 hits and the uncapped result by 168 hits.
These are **different metrics**: V110 counts fetched truth pages, whereas
V109 precisely scored fetched SQ8 rows and counted returned truth IDs.

SHA-verified cross-analysis of V109's 610,564-byte prefix evidence
(`d2d5b19add38863486f7c9a852c23b37003e96da3339b15f72f6d661317d72eb`)
with V110's same-query truth pages gives these additional containment
diagnostics:

| V109 page set, first 200 | Truth pages contained | Missing truth hits |
|---|---:|---:|
| PQ64 top-512 nominated pages before cap | 19,921/20,000 | 79 |
| Capped admitted nominated pages before gap bridging | 19,815/20,000 | 185 |

The 106-hit drop between these two page sets localizes a substantial
loss to budgeted admission. A scored range can also contain truth pages
in bridged gaps, so these page-set counts are diagnostics rather than
exact returned-recall ceilings. The 79 misses in the original nomination
show that any complete solution must also improve nomination or exploit
truth pages read incidentally. The 76-hit difference between capped
admitted-page containment and capped returned hits mixes gap coverage
with SQ8 ranking displacement and must not be assigned to either cause
without a same-range paired analysis.

The experiment rejects a claim that the **existing whole-page physical
layout alone** makes the 99% threshold impossible under these caps. It
does not establish that a truth-free query planner can find the oracle
ranges, that SQ8 scoring would return all contained truth, or that any
route meets live S3 p95 latency/QPS. It says nothing measured at 10M or
100M.

## Next gate

The cheapest next arm replaces V109's greedy page admission with an exact
interval optimizer whose **truth-free** weight for a page is its number
of PQ64 top-512 nominated rows. This fixes nomination and scores and
tests whether physical-budget optimization alone can recover the gap;
no GT100 labels enter the planner. Admit intervals jointly under both
budgets, charging gap pages and the short final page. Compare to V109
on the same V63 manifest, SQ8 object and first 200 development queries.
Stop if it misses
99.0% returned Recall@100, p05 90, 32 GETs or 16 MiB; if it passes,
replay all 1,000 development queries and then reserve an untouched
validation gate. Report a reducer-only truth-weighted optimum over the
nominated pages to distinguish planner error from nomination error. If
the count weight is poorly aligned with truth, revise the page weight
using a preregistered score calibration; if nomination coverage remains
the main error, revise the resident representation. Report measured memory,
recall, latency and node cost at each `N` and target `R`. There is no
fixed 3-GiB 100M release gate or arbitrary vector-count knee.

The useful Lean target is a conditional theorem that the interval DP
returns the maximum weighted truth-page coverage for its authenticated
page weights and byte/GET arithmetic. Such a theorem proves the upper
bound given the inputs; it cannot prove unseen-query recall, live S3
latency or 100M resource peaks without measured premises.
