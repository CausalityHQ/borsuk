# V144 production page-planner parity preregistration

**Decision:** does the new production Rust `budgeted_page_rank` module
reproduce all 1,000 V140 β=4 page selections and physical ranges on both
already-used D96 and ReLAION cohorts, with enough planner CPU headroom to
justify integrating it into serving?

Use the authenticated V122 deep-image-96-angular random100k train subset
and already-used publication-test ordinals 9000–9999, and the authenticated
V116 ReLAION-1M validation-1000 layout and requests. The V122/V116
queries, GT-free primary rosters, SQ8 objects and source-derived
coefficients are frozen at their historical SHA-256 values in the V140
launcher and runner. Authenticate the complete V140 terminal and both
sealed raw plan files. No GT is downloaded or used in this cell.

Re-run only V140's f16 unit-centroid score calculation on each cohort
and export the per-query f32 page-score matrix. Require the Python raw
β=4 plans regenerated in this run to match the V140 sealed raw file
byte-for-byte before attributing any difference to Rust. Feed those
same scores and the same primary physical ordinals into the production
Rust planner with β=4, 32 GETs and 16,777,216 bytes. For each query,
require exact equality of selected pages, half-open physical ranges,
planned bytes, GET count, target shortfall and primary retention. Stop
after a D96 mismatch; do not spend on ReLAION in that case.

The CPU headroom screen is p95 **≤5 ms/query** for the isolated Rust
page planner on each cohort. Also run a production-crate `cargo check
--lib` from the same source archive. Measure score-export and planner
process RSS and charged cgroup memory separately. These offline times
exclude router nomination, source scoring, network and concurrent
serving, and cannot support a complete latency claim. The score
calculation remains Python for this parity gate; production Rust scoring
and a hierarchical large-`N` route are subsequent gates.

Run one source-frozen Causality Spot attempt, observe incomplete work by
terminal marker and instance health only, authenticate all terminal
artifacts after completion, discard/restart any Spot interruption cell,
and terminate compute immediately at terminal. A failure requires a
specific arithmetic or implementation root-cause decision, not a
weakened parity or CPU gate.
