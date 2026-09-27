# V286 cosine page-centroid development falsifier

Status: frozen before reading V286 query coverage. V284's direct page scorer
used Euclidean distance to each source-only f16 unit mean on the V283 physical
layout and fetched 97.0625 GT100 mean/p05 92 on CoHere first100k D768 cosine
k100 development0–63. V285's exact cosine nearest-row page-order bound
fetched 100/100 with the same 84-page/32-range schedule. The source-only f16
unit-center norms vary from 0.621 to 0.937; the V284 Euclidean scorer does not
normalize them. This experiment changes **only the page score** to maximum
cosine similarity between the query and any of the eight existing f16 unit
means on a physical page. The generation archive, centroids, layout, queries,
GT and V284 greedy 32-range/84-page scheduler are otherwise unchanged. The
runner checks their pinned SHA-256 identities before use.

The fixed KILL gate is fetched mean GT100 <98.7 or p05 <95 on these 64 used
development queries, or any plan over 32 GET/16,777,216 bytes. A pass permits
one Rust scorer and sublinear page-router implementation, followed by a
same-query returned-SQ8 check, but neither the short development split nor a
source-only page-coverage result can promote 1M or a vendor claim. A failure
rejects simple unit-mean score normalization on this layout; the next design
must change descriptor information, not widen this used panel or tune page
count. One local run only, <=2 minutes and <=2 GiB peak process RSS; no
cloud job or full panel. Numerical results close to the gate require exact
Rust scorer replay before promotion.

## Completed development decision (2026-09-27 UTC)

**KILL cosine scoring of the existing f16 unit means.** The frozen
`0ef1e021` script completed exit 0 in 0.57 s with 59,436 KiB peak process
RSS. The [result](v286-cosine-centroid-dev64-result.json) SHA-256 is
`7dce58171803d67fd2f2491d9f28d18f7cae6c529073723fbfb8cf2c2d5b456e`.
On CoHere first100k D768 cosine k100 development0–63, it fetched **96.78125
GT100 mean/p05 89**, at most 31 GETs and 16,773,120 planned bytes. It misses
the 98.7/p05 95 gate by 1.91875 mean hits and 6 p05 hits, and is below V284's
Euclidean unit-mean result of 97.0625/p05 92. This is far from the gate, so
no exact Rust scorer replay is warranted. The mismatch between query metric
and centroid distance was not the root cause. The unit means themselves do
not retain enough query-neighbor geometry for this physical layout and budget;
the next candidate must encode more within-page detail rather than tune the
same mean scores or page count. This short fetched-coverage diagnostic is not
returned recall or live S3 performance. No full panel or paid run follows.
