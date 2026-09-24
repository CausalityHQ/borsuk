# V153 planner cover materialization

V152 closed with p95 page-planning time 0.503411 ms for the sparse
candidate route and 0.513167 ms for paired flat on deep-image-96
random100k, publication test ordinals 3000–3999. Its total CPU gate
failed, so planner cost is a concrete next target. These are frozen
V152 measurements, not V153 timings.

The planner already computes the exact charged byte count for every
proposed page with `cover_charge`, rejecting pages that exceed the
transport budget. Previously it also built the full coalesced range
cover after every accepted page, allocating a B-tree set, run list,
gap list, join set and output ranges each time. V153 defers that cover
construction until page admission finishes. The ranked page order,
incremental charge, reject decisions, target, and final shortest-gap
cover are unchanged. The refactor removes repeated allocations without
adding a vector-count or dataset switch.

`cargo check --lib -j 2` passed and all nine focused
`budgeted_page_rank::tests` passed after the change. In particular,
the tests check that incremental charge matches the exact cover over
varied page patterns and tail pages. A paired remote measurement and
terminal-closed plan replay are still required to establish latency
and full-cohort parity. The next campaign should use the unchanged
V152 development queries, primary rosters and flat/candidate scores,
then promote only a justified route to the decisive ReLAION-1M
quality and CPU comparison. Held-out deep-image ordinals 4000–4999
remain reserved.

## Frozen V153 diagnostic attempt

Run one Causality `c7i.8xlarge` Spot attempt from the exact committed
source archive, using the unchanged V152 four-arm runner and development
ordinals 3000–3999. The only production-code change from V152 is the
planner's deferred cover materialization. The runner retains its V152
artifact schema and its original quality/CPU verdict; the source commit
and unique `v153-planner-once-*/a0001` prefix identify this distinct
attempt. Record archive and terminal digests, instance identity and
terminal state. On interruption, discard the entire cell and restart
under a new attempt ID; never inspect partial measurement files. Stop
the instance immediately after the terminal marker.

Authenticate and independently recount every artifact after terminal
closure. Compare each query's selected-page and range plan in all four
arms against the closed V152 attempt, along with source quality,
planned bytes and GETs. Exact plan parity is the correctness gate for
this planner-only change. Compare paired flat and V152 p95 CPU on the
new host, including even and odd order and per-phase timing; the V152
historical CPU is diagnostic across hosts. Require all original V152
quality, transport, resource and score gates, exact plan parity, and
V152 p95 CPU strictly below paired flat to call this a development
winner. Report an unchanged or negative timing result as a rejection.
No tuning of page budgets or the used queries is permitted. A winner
still requires reserved 100k confirmation and a separately frozen
ReLAION-1M gate before promotion.
