# V149 contiguous hierarchy GT-free 100k preregistration

**Decision:** can a dimension-generic contiguous physical-page hierarchy
produce exact scores for its visited pages and reduce query coordinate
work below the V146 flat scorer on the frozen D96 random100k cohort,
without violating the V145 planner's GET/byte bounds? This is a work and
arithmetic gate only; it cannot qualify returned recall.

Use the authenticated V146 `deep.centroids.bin` (`BORSUCP1`), the V146
flat Rust f32 score matrix, the V140 raw β=4 page plans, the V122 deep
queries and frozen V138 primary rosters on already-used publication-test
ordinals 9000–9999. Authenticate source and all inputs by size and
SHA-256. Do not download GT or returned IDs.

Build a deterministic 16-way tree over consecutive 256-row physical
pages. Each leaf corresponds to one page's at most eight 32-row
centroids. Every node records a centroid mean and a radius
outward-bounding all descendant unit centroids using the child-radius
triangle inequality. Store the tree in a new versioned artifact and
reject malformed spans, radii, offsets or non-finite values. For a
query, use best-first traversal prioritized by `distance to node mean
- radius` with node-ID tie order. Score a node summary when it
enters the queue; evaluate every unit centroid of a leaf when that leaf
is popped, so its page score equals the V146 flat score up to
floating-point rounding. Always evaluate and include every primary
physical page.

**Pre-execution amendment (2026-09-24):** preserve the signed value
`distance to mean - radius` as the queue priority. Clamping negative
bounds to zero can turn a broad overlap into a page-ID traversal. No
V149 Spot attempt was launched under the prior wording.

Freeze β=4. If `P` distinct primary pages are present, the additional
leaf budget is `min(page_count, β × P)` and the expanded-node budget is
`2 × β × P × tree_height`, where `tree_height` counts levels including
leaves. Both formulas are corpus-independent and have
no vector-count switch. Count every scored node summary and unit
centroid as one coordinate-vector evaluation. Feed only visited page
scores, plus primary pages, to the same primary-first planner and retain
its 32-GET/16,777,216-byte caps. Record beam exhaustion, page count,
score differences on evaluated pages, selected-page capture against
V140, planned bytes/GETs, build time, CPU distribution and charged RAM.

The work threshold below is an implementation/resource check, not a
route-quality discriminator: with the frozen 391 pages and V138 primary
roster, even arbitrary page order is guaranteed to use fewer than 3,125
coordinate-vector evaluations at p95 under this beam cap. Before any
Spot run, add two independent route-quality screens. First, the
aggregate fraction of V140 β=4 selected pages retained by the sparse
plan must be at least **95%**. Second, this fraction must exceed a
deterministic page-ID control by at least **5 percentage points**. For
each query, the control visits every primary page plus the same number
of nonprimary pages as the hierarchy, in ascending page ID, scores
those pages with the authenticated V146 flat Rust matrix, and feeds
them to the identical sparse planner. Every query's target shortfall
must be no worse than V140. These are proxies for page-plan fidelity,
not returned recall; fresh held-out GT remains mandatory even after a
pass. These thresholds were fixed before observing V149 measurements.

The pass gate is: exact inclusion of all primary pages on all 1,000
queries; no plan cap violation; no malformed score; maximum absolute
visited-page score difference versus the V146 Rust matrix ≤0.0001; and
p95 vector evaluations **<3,125**, the flat D96 unit count. The latter
is a structural work screen, not a hardware latency claim. The three
route-quality requirements in the preceding paragraph also apply. A failure
requires a root-cause choice between summary quality, beam priority,
planner interface and implementation cost before another campaign. A
pass licenses one fresh held-out 100k returned-quality/live-cost gate
against the paired V141/V145 β=4 arm. Only a quality winner advances to
the frozen ReLAION-1M split and then 10M/100M. Do not select new beam
constants from this used cohort.

Run one source-frozen Causality Spot attempt with interruption
discard/restart as a new numbered attempt (`a0002`, etc.) after an
interrupted terminal or terminated-without-terminal state; never resume
or combine an interrupted measurement cell. Use terminal-only monitoring while incomplete, artifact
authentication after terminal and immediate instance termination. The
10 ms D768 CPU, full primary route, fresh quality and live-S3 gates
remain open regardless of this result.
