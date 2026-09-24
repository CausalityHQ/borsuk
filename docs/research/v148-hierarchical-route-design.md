# V148 hierarchical route design for the next decisive gate

## Decision and constraints

V146 proves the Rust f16-centroid builder and scorer reproduce every V140
β=4 physical page plan on both used cohorts, but its flat D768 1M
score+planner p95 is 11.071522 ms/query, above the 10 ms screen. The scan
evaluates `ceil(N/32) × D` centroid coordinates per query. At 100M D768
that is 2.4 billion coordinates/query before the planner. A source-only
route with sublinear query work under an explicit search budget is required; no
vector-count threshold or corpus-specific branch may select a different
method. A request's recall/resource objective may instead increase a
continuous candidate-work budget.

V147's authenticated diagnostic shows why two simple substitutions are
wrong. Local ±32-page expansion covers only 73.33% of ReLAION's
V140-selected pages. Conversely, byte-cap saturation can accept a
ReLAION page ranked 3,890/3,907 by centroid score. Exact V140 page-plan
parity is therefore a diagnostic for a new sparse route, not its quality
gate. Returned Recall@100, tails, bytes, GETs, CPU and charged RAM must
be paired against the current β=4 arm. New scores must retain V146's
min-distance-over-eight-unit-centroids definition on every page they
evaluate; primary pages are always evaluated and admitted first.

## Candidate architectures

| Route | Build and memory | Query work | Main risk |
|---|---|---|---|
| Contiguous physical-page hierarchy (first probe) | Deterministic bottom-up summaries over physical page order; O(ND/32) build; internal f16 means/radii plus existing f16 unit plane | Explicit node-expansion and candidate-page caps, then exact eight-unit scoring at visited pages | High-dimensional radii overlap; best-first traversal may miss useful distant pages under the cap |
| Compact unit-centroid graph | Approximate-neighbor graph over `N/32` unit centroids; f16 vectors plus bounded-degree adjacency | Explicit expansion cap × degree × D; graph may reach distant pages quickly | Graph build, generation updates, random-memory traffic and high-D neighbor recall; the current `centroid_hnsw.rs` allocates an O(N) visited bitmap per insertion and copies vectors, so its build scaling cannot be assumed |
| Layout-cluster IVF | Cluster centroids and physical membership ranges; probe nearby clusters then score their pages exactly | Coarse scan/search plus all units in chosen clusters | Frozen V116/V122 manifests do not yet establish retained cluster boundaries; refitting or a format rewrite is needed, and cluster-boundary misses may hurt quality |

The first probe is the physical-page hierarchy because it is the cheapest
to build, stores summaries in physical order, and needs no new training
data. Its internal nodes group 16 consecutive children. Leaves are one
256-row page, so all eight 32-row centroids of a visited page are scored
exactly. Each internal node stores a f16 mean and an outward-rounded
radius covering its descendant centroids. Best-first traversal uses the
triangle-inequality lower bound for priority, with deterministic node-ID
ties. This priority is a bound on possible centroid score, not a promise
that a fixed-width beam contains every useful page in high dimension.

The candidate-page cap is an explicit function of the requested
recall/service budget and primary-page count. The expanded-node cap may
also include tree height, which grows smoothly as `log₁₆(N/256)`. The first
offline probe will freeze one corpus-independent multiplier before any
GT use. The planner will accept a sparse ranked page list and preserve
its existing primary-first, 32-GET and 16-MiB admission arithmetic.
Its returned plan will expose candidate count, node expansions,
centroid evaluations and beam exhaustion. The initial flat PQ64
primary router remains a separate measured scaling risk; this gate
does not claim the full route is sublinear until that router is replaced
or jointly bounded.

## Ordered validation

1. Preregister a GT-free 100k D96 screen on the frozen V146 centroid
   file and score matrix. Compare page-score accuracy on evaluated pages,
   V140 selected-page capture, candidate/node work and planner bytes/GETs
   against the flat β=4 arm. Set a work cap below the 3,125-unit flat
   scan so this screen can reject an ineffective hierarchy. A page-plan
   mismatch alone is not a quality failure. Stop if candidate capture
   or work is decisively poor; do not tune the beam on this used cohort.
2. Replay returned IDs and exact-source Recall@100 on a fresh 100k
   held-out split under a paired frozen layout. Require the existing
   V141 quality floor and compare aggregate/tail quality, actual bytes
   and latency against the strongest β=4 BORSUK arm. Promote only a
   winner to the frozen 1M ReLAION split with the same global policy.
3. At 1M, require p95 local score+planner ≤10 ms, measure the primary
   route too, and record charged RAM and concurrent behavior. Then run
   the 9.99M D96 gate on V120's frozen layout. At 100M, choose RAM from
   measured recall/latency/cost tradeoffs without an N knee. Fresh
   held-out and live-S3 comparisons remain publication gates.

Lean can prove the hierarchy's structural work, payload and GET/byte
bounds given validated child spans, radii and explicit caps. It can prove
that fully evaluated pages have exact V146 scores and that a certified
lower bound excludes an unvisited node above a threshold. It cannot
prove that a budgeted traversal captures enough useful pages, that a
candidate-only returned list meets Recall@100 on unseen queries, or
that hardware/S3 meets latency and charged-memory targets without
measured premises. Existing conditional arithmetic is in
`formal/BudgetedPageAdmission.lean` and
`formal/UnitCentroidScaling.lean`.

The Fable consultation `fd25ab68ca094745` independently suggested a
unit graph. Its work/memory arguments informed the alternatives above;
its 100M build-time estimate and exact-plan-parity gate are not adopted
because the current graph builder and V147 rank tail contradict those
assumptions. A graph remains the next material alternative if the
physical hierarchy fails the 100k screen.
