# GO only to actual nomination: hierarchical semantic extents

2026-09-28 UTC. Base d2f898f8 plus archived source.patch; original Spark jobs
finished, no paid job or serving default. Original fixture red exit101, green
exit0 (2 source-order tests), optimized example build exit0. Remote/local Rust
source hashes agree. Both original corpus runs exit0; terminal/order/source/GT
hashes replayed before decision. Exact receipts and executable probe retained.

## Frozen development containment gate

Both corpora first100k, D768, cosine, GT100, previously used development
queries0–63 (64 each). Source-only fit: hierarchy target ceil(N/1024), source
reservoir<=64/cell, seed8201,12 updates, existing hierarchical Lloyd/HNSW,
cell/radius/ordinal order, hard1024-row extent cap. No query/GT enters fitting.
This is a hard row cap, not balanced full-corpus density or adaptive splitting.

| Dataset | Optimistic best32 extents mean / p05 (%) | Byte-feasible best21 extents mean / p05 (%) | Decision |
|---|---:|---:|---|
| CoHere |100 /100 |99.78125 /99 |GO to nomination only |
| ReLAION |100 /100 |99.53125 /99 |GO to nomination only |

Predeclared thresholds mean>=98.9%, p05>=96. GT-aware top32 ignores byte costs;
top21 is a feasible witness: at most21 GET and21504 SQ8 rows,16773120 bytes
under780B/row. Neither is a query algorithm or achieved recall. These extents
are larger than the previous32-page oracle; do not present their different
retrieval budgets as a paired product improvement. Full serving performance,
independent quality, matched vendors and100M resources remain unmeasured.

CoHere fitting elapsed3.49s, maxRSS40964KiB, source SHA270ed2889d11312f15246d6d98a0c7621fb38e714791e7c5f11b408de9a5552b;
order SHA57613395bc54810e6bf3cd778b129752aeca78d664b17f26801a39bef09dd2f0.
All elapsed/RSS numbers are local source fitting on Spark, not serving latency
or100M projections. ReLAION timing is in its exact fit.time receipt.

## Concrete code decision / next single gate

Keep the experimental authenticated streamed fitter and explicit hier-fit CLI;
no root-format or serving route change yet. The existing failed flat fitter is
only an immutable research control, not a fallback/default. New recipe and
extent boundaries are explicit; generation reads must authenticate them before
any production use. Public API returns order+extents; atomic CLI order writer
reuses the existing no-replacement publication primitive.

Next cheapest distinct falsifier: compute source-only per-extent centroid
geometry, nominate at most21 extents on these development queries, measure
fetched GT coverage and returned SQ8 recall against unchanged paired flat SQ8.
Exact centroid ranking is only a diagnostic upper/reference for that nomination
geometry, not a revived100M flat-scan product route. Kill before new format,
independent panel,1M HTTP or cloud if even this reference fails quality. A
survivor still needs bounded actual routing, authenticated extent metadata,
and the existing98% mean/p05>=95/<=.5pp paired-flat gate. No parameter sweep.

Both-vendor minimum release matrix stays OPEN on every final axis. No matched
S3 Vectors or Turbopuffer win, query p90/p95/QPS or lifecycle-cost claim follows
from this development diagnostic. No operator decision is needed.
