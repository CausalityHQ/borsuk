# Sparse query scratch correctness GO; scale qualification open

Shared CentroidHnsw/CatalogRouter beam search now creates a sparse per-query
visited set rather than clearing one mark for every catalog node. Initial
requested reserve is min(node_count, ef×32); actual HashSet capacity rounds up
and can grow with traversed nodes. Reusable construction epochs are unchanged.
One beam-search loop serves both marking policies; no hash-set iteration enters
ranking, so hashing order does not change neighbor traversal/tie semantics.

Original red exit101 rejects the placeholder node-sized set's capacity. Final
green exit0:13 affected centroid/router tests pass,2 existing tests ignored,
0.55s test execution (verification timing, not serving latency). At1M nominal
nodes and ef64 the new initial capacity is<=4096. On deterministic duplicated
vectors,3 queries×3 beam widths compare exact node IDs and f32 distance bits
against epoch-marked search. Existing epoch wrap, reachability, catalog routing,
nonfinite-query rejection and nearest-versus-brute-force checks also pass.
Original Spark compile/test logs/exits and source SHA are preserved. No local
Cargo build, overlap, paid job or corpus panel.

This removes an O(cell_count) scratch initialization from each assignment
query at fixed beam/degree. Construction already had epoch reuse in current
source; the historical V149 warning must not be treated as a current missing
fix. Worst-case traversal/dynamic set size may still cover all cells. No new
latency/RSS/throughput,100M or vendor claim follows. The new source layout is
not implemented/qualified by this helper change.

Next: reuse existing train_logical_cell_centroids plus this assignment search
for bounded hierarchical source fitting and multi-page semantic extents with
boundary coverage, using explicit sample/stream/extent admission. Do not add a
second Lloyd implementation or revive rejected traversal experiments. Current
native flat fitter remains KILL and both matched vendor gates remain OPEN.
