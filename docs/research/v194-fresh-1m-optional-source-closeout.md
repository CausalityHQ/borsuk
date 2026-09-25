# V194 fresh 1M optional utility gate: closed negative result

## Frozen decision and authority

The preregistered ReLAION-1M D768 source pseudoquery SHA ranks
**2945–3456** (512 new query identities) failed. The optional-risk arm
returned **50,692/51,200 exact-source GT100 IDs** after SQ8 reranking,
versus the frozen **50,979** screening threshold; it had one infeasible
query. Its nearest-rank p05 was 98, planned 3,864,556,800 bytes and
5,147 GETs, below the aggregate caps, but low aggregate I/O cannot
rescue the quality or feasibility failures. The full-rank contemporary
arm returned 50,694, also with one infeasible query. The decision is
`revise-representation-allocation-or-serving`; do not promote either
arm to live S3 or a production default on this evidence.

One Causality Spot `c7i.12xlarge` cell `a0001` completed from source
commit `72bf198eadfbaa0e438cc0871b9931900319cb2e` and archive
SHA-256 `eafb948160eea0f5508dcf3feb290cf245a180a6c47707818ae7ef598d474a44`.
Its complete terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v194-fresh-1m-optional-source/72bf198eadfbaa0e438cc0871b9931900319cb2e/runs/a0001/terminal.json`,
SHA-256 `4da29740e45057192646a36a045c412e90598e05df9ad95213241ea7350e9767`,
with `exit_code=0`. The controller rehashed every artifact and the
pretruth S3 feature and plan copies, then terminated instance
`i-055964b24c4e0fa59`. The worker re-read the immutable S3 copies
before opening exact-source GT100. Complete features, plans, raw and
summary SHA-256 values are respectively
`415dbb3a20e3ca3a0c77f327b0b457a049156fe0a9810199ee1b0da0c4de5b64`,
`ee5cb9a7eee6a05b753db13f52b9903349ff8d750312702e98b736a39bd5562d`,
`121104dc11819eeaaae810f70d96979dbe74f4bca65e3cd2a29c2557757f144c`,
and `fc081258d0563deb8f18b926d8948fee73759f45d727543663eed9108434fb01`.
The checked-in `scripts/check_v194_fresh_1m_optional_source.py`
independently replayed 512 candidate and physical interval witnesses,
2,048 arm/query rows, resource caps, GT100 unit coverage, tail arithmetic,
and the decision. It cannot independently recompute each returned-ID
intersection because the completed raw artifact contains GT100 **unit
masses**, not GT100 stable IDs. Returned-hit identity relies on the
frozen Spot evaluator and authenticated source, with hashes and
self-consistency checked in replay. A follow-up replay should carry
stable GT100 IDs in its completed output or independently recompute
them on remote compute.

## Measured result and matched arms

All counts are on the same **fresh V194 ReLAION-1M D768 source-512**
split. Returned hits are actual SQ8 top-100 intersections with exact
float64 cosine GT100, excluding each query's own source row. Physical
coverage counts GT100 IDs in fetched 32-row units; bytes and GETs are
planned charges, not live S3 measurements. Every arm treats the one
infeasible query as zero returned hits for aggregate and p05 reporting.

| V194 arm | Returned hits / 51,200 | p05 / 100 | Fetched GT100 / 51,200 | Planned bytes | Planned GETs | Infeasible queries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Optional-risk | 50,692 | 98 | 50,941 | 3,864,556,800 | 5,147 | 1 |
| Full-rank, separately fit price | 50,694 | 98 | 50,950 | 4,835,750,400 | 8,596 | 1 |
| Constant-risk | 50,680 | 98 | 50,930 | 4,808,319,360 | 5,091 | 1 |
| V187-style greedy control | 50,668 | 97 | 50,926 | 5,582,079,360 | 14,928 | 1 |

The candidate ceiling is **51,138/51,200**. The optional arm loses
62 GT100 IDs before the candidate field, then 97 candidate GT100 IDs
in the single infeasible query. Across the 511 feasible queries it
loses 100 between candidate ceiling 51,041 and fetched coverage
50,941, then 249 between coverage and SQ8 returned top-100 50,692.
These four losses sum to **508** missed rows. Full-rank uses 971,193,600
more planned bytes and 3,449 more GETs than optional to gain only two
returned hits on this split. Optional versus full-rank paired query
counts are 17 wins, 472 ties and 23 losses, net −2 hits. The greedy
control also exceeds the 11,328 aggregate GET screening cap.

One query, ordinal **3321** (display rank 3322), source ID **339868062**,
has 86 mandatory 32-row units scattered across the V164 layout. Its
minimum cover under 32 GETs is **766 units**, above the fixed 672-unit
cap. All four arms fail this query for the same physical reason. The
frozen hard planner correctly rejects it; this is an **admission and
layout/budget mismatch**, not a planner witness bug. Three query floors
are above 512 units; one is above 672. The mandatory-floor p50/p95/p99
are 37/85/155 units, with maximum 766. A generic dynamic cap based on
the mandatory floor and an explicit request budget would admit this
tail if the caller allows at least 19,119,360 bytes for 766 units; any
such policy requires new measured validation and a bounded-memory
implementation. A query-ID exception is not justified.

The optional arm has 21/512 queries below 98 returned hits, p05 98,
and a descriptive 95% Wilson interval **[2.70%, 6.19%]** for that
below-98 share. Its reported minimum is zero because the infeasible
query is counted as zero; the minimum among feasible queries is 93.
The bottom-decile mean including the infeasible query is 95.31. These
uncertainty numbers describe source pseudoqueries, not real user-query
performance. The strongest prior BORSUK 1M actual-returned comparator,
V155 ReLAION-1M D768 **used validation-1000 real queries**, was
99,567/100,000 hits, p05 98, 11,134,007,040 planned bytes and 22,126
GETs. It is **unpaired** with V194; no product win or loss versus V155
can be inferred from the difference alone.

Spot preparation used 2:15.59 wall, 295.91 user CPU seconds and
9,367,080 KiB peak RSS; planning used 1:56.05 wall, 111.39 user CPU
seconds and 212,260 KiB peak RSS; evaluation used 1:02.90 wall,
293.40 user CPU seconds and 11,030,896 KiB peak RSS. These are
offline research-process resources, not serving latency or charged
production RAM. No live S3 latency, 10M/100M, or matched current-revision
S3 Vectors/Turbopuffer result was measured.

## Next design decision

Stop promotion of the current SQ8-only return path. On this **used**
V194 panel, run one diagnostic that keeps the candidate field and
physical plans frozen while measuring (a) a general mandatory-floor
budget rule and (b) whether a small exact or FP16 rerank shortlist
recovers the 249 SQ8 return losses within a disclosed extra-read and
RAM budget. If those assumptions fail, revise representation or layout
rather than tuning the optional price. Any resulting policy must be
fixed before a new identity-disjoint source panel and real-query paired
live S3 gate. Extend Lean proofs to the generic dynamic-cap admission
and conditional rerank bound where useful; measured data must still
establish the premises and serving cost. This preserves the user's
requirement that memory may scale with recall and corpus size rather
than imposing a fixed 100M-vector knee.
