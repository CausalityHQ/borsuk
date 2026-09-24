# V134 native router and planner 100k preregistration

V133 established that a pinned local SQ8 file preserves the exact-source
result and meets its 100k placement screen. V134 asks whether the **production
Rust PQ64 router, exact nominee scorer and weighted physical planner** can
produce that same candidate route inside a measured serial request without
exhausting the latency headroom. This is the cheapest decisive route gate
before concurrency or a fresh 1M build. No layout, SQ8/source scorer,
parameter, physical budget or used query split changes.

Reuse the authenticated generation-131 package, **deep-image-96-angular
random 100k train subset**, and 1,000 already-used **publication-test
ordinals 9000–9999**. The 100k router policy is the already-frozen V122
`ceil(page_count × 1024 / 3907)` regions (103 pages), 512 PQ64 nominees,
100 exact-SQ8 primary rows, and nominee-count-plus-one primary vote weight.
The production planner uses 256-row pages, 32-row accounting units, at most
32 physical ranges, and at most 16,777,216 SQ8 bytes. Its output must equal
the sealed V122 candidate byte ranges on **every** query. PQ64 nominee sets,
exact primary rows must also match sealed evidence. Page votes are derived
from those sealed rows and the returned plan score is verified by the Rust
planner's witness; score-order differences from float32 ties are recorded
separately.
No GT data enters routing or planning.

The candidate timer begins before PQ64 nomination and includes exact local
nominee scoring, physical planning, local SQ8 page reads, returned SQ8
scoring, ID-map resolution and exact source ranking. Record separate router,
nominee, planner, SQ8 read and source subphase times. The paired capped
control replays V133's sealed control ranges and nominee roster on the same
query in alternating arm order. Its timer excludes offline control routing;
this is a **fixed-route latency reference**, not an end-to-end control
implementation. Compare V134 candidate to V133's same-quality fixed-route
candidate p95 47.285297 ms/query and V132's live-S3 capped-control p95
75.624654 ms/query with those scope differences explicit.

Fail fast on any candidate route or output mismatch, authentication error,
budget violation or source-ID/GT-hit drift against sealed V131/V133 rows.
Promote the serial route only if all 1,000 queries pass parity and candidate
complete p95 is at most 75.624654 ms/query, the registered available
development headroom. Report nearest-rank p50/p95/p99, all phase times,
per-query raw evidence, startup transfer/authentication, dedicated serving
cgroup charged peak, source commit/archive, terminal/artifact hashes and Spot
instance identity. Run one immutable Causality Spot attempt, discard any
interrupted cell and terminate immediately after its terminal. A passing
serial result permits a separate preregistered C1/C8/C32 concurrent serving
gate, then matched fresh deep-image and ReLAION-1M builds. This used 100k
screen cannot certify population recall, 1M generality or 10M/100M routing
cost; the latter already measured 184.76 ms/query offline at 9.99M rows in
V121. No commercial comparison is inferred.
