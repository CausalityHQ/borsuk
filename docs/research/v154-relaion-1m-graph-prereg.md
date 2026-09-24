# V154 ReLAION-1M graph scale diagnostic preregistration

**Decision:** does the dimension-generic seeded graph route become faster
than a paired flat scan at 1M and D768 after V153's generic planner
refactor, while retaining primary pages and bounded transport? This is
an already-used development split, not a held-out qualification or live
S3 latency result. A CPU win only authorizes a separate returned-quality
gate; it does not promote the route.

## Frozen inputs and route

Use ReLAION-1M validation-1000 V116 requests, SHA-256
`c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9`,
and V139 primary rosters, SHA-256
`71bfdf71f293ca1d23f58694866b3ba52ee8ee95ed9b02e676a2a8bf031f3162`.
The V154 seal converts them without changing query coordinates or
row IDs; the expected query and routing digests are
`4ef4734ff40aeda8b8e99ec01ca993be46e8ad9c337b5410259eeb171f57acfd`
and `2545c5762d04d28c91941607ae1801a8aa7d5a927f4a02aa92b28d43b1cdf1cf`.
Use the authenticated V146 `BORSUCP1` ReLAION unit-centroid plane,
48,000,032 bytes, SHA-256
`07ad4736d8eb6867d46523de71ed61d82220dabc923f3c88ead1cd13eb776651`.
Check the V146 complete terminal and its artifact digest before use.
No GT or source vectors enter this diagnostic.

The Rust runner uses the same production `UnitCentroidPages`,
`UnitCentroidGraph` and `budgeted_page_rank` implementations and the
same 32-row units, 256-row pages, β=4, 16P graph evaluation cap, 4P
nonprimary page cap, 32 GETs and 16,777,216 byte cap as V153.
P is the distinct primary-page count for each query; no budget depends
on the vector count or dataset name. Build graph adjacency once from
the authenticated centroid plane and record its digest, build time,
size and memory. Compare flat, V150, V151 and V152-cached arms on each
query; flat and cached occupy alternating first/second positions.
V150 and V151 run afterward as diagnostic controls so their identical
graph walks cannot warm the cached candidate. Record page scores, selected pages,
ranges, exact charged bytes, GETs, work, shortfalls, p50/p95/p99 CPU
and phase timing. The runner's V150/V151/V152 labels denote algorithms,
not historical 100k measurements.

## Decision and execution

First require every graph arm to retain exactly the same primary pages
as paired flat, and all arms to satisfy work, page, GET and byte caps.
V140 retained all ReLAION primary pages on 999/1,000 queries; query 574's
60 scattered primary pages cannot all fit the unchanged 16 MiB/32 GET
cover. Record that absolute shortfall separately, rather than treating
the shared flat/sparse cap effect as a CPU rejection. Require cached page scores to differ
from paired flat by at most 0.0001 absolute, and independently recount
the terminal-closed plans and percentile summaries. The CPU screen
passes only if cached sparse p95 search+score+planner time is strictly
below the same-host flat p95 score+planner time both pooled and in each
even/odd order cohort. Report planned bytes and GETs against flat,
including any quality-relevant page-capture loss. If CPU loses, reject
this implementation at 1M and do not spend another Spot run on a
returned-quality replay. Distinguish primary, cap, score and CPU
rejection reasons. If CPU wins, freeze its terminal artifact and
run a separate preregistered returned-ID and GT100 replay against the
same V116 source/layout/SQ8 inputs, including source Recall@100, p05,
sub-90, exact transport, and charged memory. The historical V142 β=4
99.607% source Recall@100 and V146 11.071522 ms flat CPU p95 are
context only; neither is a paired V154 baseline.

Use one immutable committed source archive and one Causality Spot
attempt. Record the instance identity. On Spot interruption, mark the
cell invalid, sync the terminal, and restart the entire cell under a
new attempt ID. Inspect incomplete work only through instance health
and terminal markers. Authenticate every terminal artifact, recount
independently after closure, and terminate compute immediately. Local
devbox swap pressure forbids a local full suite; use only the narrow
Rust typecheck and input-seal checks before remote execution.
