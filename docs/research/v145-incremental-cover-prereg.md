# V145 incremental-cover production planner gate

**Decision:** does a generic exact-cover-cost optimization remove V144's
ReLAION CPU tail while preserving all frozen V140 β=4 selections, physical
ranges and budget fields on both used cohorts?

Source is a new immutable archive from one pushed commit. Use the same
authenticated V122 deep-image-96-angular random100k train subset and
already-used publication-test ordinals 9000–9999, and the same authenticated
V116 ReLAION-1M validation-1000 requests/layout and V115 router coefficients.
Use the same V140 terminal and raw plan hashes. Regenerate f16 centroid
score matrices from frozen inputs and require regenerated V140 Python raw
plan files to match their historical SHA-256 hashes byte-for-byte. No GT
download or quality evaluation occurs in this cell.

For each of 1,000 queries per cohort, require the Rust production planner
to match frozen V140 β=4 selected pages, half-open physical byte ranges,
planned bytes, GET count, target shortfall and primary-page retention exactly.
The generic limits remain 32 GETs and 16,777,216 planned bytes. Run
`cargo check --lib` on the production crate in the same source archive.
Measure isolated Rust planner CPU and require p95 **≤5 ms/query** separately
on each cohort. Stop at the first D96 failure; do not weaken this threshold
after observing results. Authenticate terminal artifact hashes and sizes,
record instance identity, and terminate compute at terminal.

The optimized cover cost equals selected physical-page bytes plus the
cheapest gaps required to reduce run count to at most 32. Gap tie order
cannot change the total charged bytes; the accepted-page range cover retains
the original deterministic lowest-gap-first rule. A failed cell requires
diagnosis and a material architecture/implementation revision. A successful
cell qualifies only the pure page-admission CPU and parity; production
centroid scoring, large-N routing, live S3 latency, recall outside the used
splits, and charged serving RAM require their own gates.

Use one Causality Spot attempt with a 5,400 s hard stop. Treat interruption
as a discarded measurement cell and restart from a new attempt marker. While
incomplete, inspect only terminal marker and instance health. On a terminal
marker, stop compute immediately and authenticate all artifacts.
