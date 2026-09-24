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
