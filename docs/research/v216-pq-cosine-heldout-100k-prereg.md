# V216 frozen cosine PQ graph method-held-out 100k gate

V215 selected cosine PQ navigation ef_search=2048 and resident FP16
shortlist=2048 from development queries 0–255. Freeze that exact
source-built graph, source PQ64 books/codes, generation-pinned FP16
plane, physical row mapping, code and arm. Evaluate ReLAION-100k
D768 query ordinals **256–999** (744 queries, GT100). Those queries
were used in older BORSUK campaigns, but were held out from V215's
arm selection. Do not call them globally new or pristine.

Reuse exact verified V214 inputs and the V215 cosine method. Return
stable top-100 IDs for each request and seal the raw IDs to S3 before
opening GT or the V193 full-rank SQ8 paired baseline. Record aggregate
GT100 hits, p05 hits/query, paired wins/ties/losses, whole in-process
Rust p50/p95/p99, sequential QPS, p95 base visits, cold hydration,
peak serving RSS and vector-body GETs. The gate requires aggregate
hits≥V193's paired hits on these same 744 queries, p05≥98,
whole p95≤10 ms, peak RSS≤320,000,000 bytes, and zero vector-body
GETs. No arm selection, budget change or data-specific tuning is
allowed after this result.

Run one Causality Spot cell; restart interrupted cells at a new
attempt, write a terminal hash roster and terminate at terminal.
Only terminal and infrastructure health may be read before completion.
A pass authorizes one frozen ReLAION-1M in-process serving/recall and
resource comparison against the strongest paired BORSUK baseline,
then an end-to-end network/product and matched competitor campaign.
It does not itself prove 1M or external-product performance.
