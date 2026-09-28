Decision, read-only, HEAD 83cc6cdb. Nothing was edited, built, launched, or sent.

## Audit of closures and the causal fact they hide

- **Copies are dead on cost, not quality.** Epsilon 0.15 closure hit rho ≥ 2.00001 before any query replay. That mechanism is SPANN's boundary closure (Chen et al., NeurIPS 2021), which accepts far higher replication. The ledger also rejected an epsilon ladder earlier (line 4590) and V38 rho 1.25 (98.855 at 14 postings). No copy arm remains.
- **Summary rankers are dead.** Extent means 91.8%, unit prototypes 93.1%, GT witness 99.78% on the same 1024-row layout. V139/V146/V149/V150 rejected centroid-page hierarchies.
- **The unmeasured decomposition is already half-measured, and it changes the target.** On ReLAION dev, native fetched coverage is 99.5625% and the bounded graph's 159-candidate coverage is also 99.5625% (centroid-discovery-diagnostic). Exhaustive unit-centroid ranking of the same 159 pages gives 99.875%. So the 200 B/row two-bit plane (192 packed + 8 scalar, rotated_two_bit.rs:135) lost **zero** GT hits when cutting 159 pages to the 84-page byte budget. The lossy layer at 100k is bounded discovery over the unit-centroid HNSW, which is built with `select_neighbours(.., diverse=false)` at centroid_hnsw.rs:945 and 1282. The plane is the RAM problem but is not the load-bearing quality component. The 744-query validation decomposition (discovery vs ranking) is not measured.

## One candidate: cut-edge layout, boundary edges instead of boundary copies

Build the page partition from the source row kNN graph rather than from Voronoi cells, and keep the residual cut edges as page adjacency. Locality comes from putting kNN edges inside pages; boundary neighbours are preserved as a ~0.4 B/row page adjacency list instead of 780 B copies (rho stays 1).

- **Layout (source-only):** exact kNN k=32 on the sealed normalized 100k source, then streaming balanced assignment into 256-row pages (LDG, Stanton and Kliot KDD 2012; Fennel variant, Tsourakakis et al. WSDM 2014): assign each row, in BFS order over the graph from ordinal 0, to the page maximising neighbours-already-present × (1 − fill/256), ties to lowest page id. Output is the existing `SemanticSourceLayout` shape (order + extents).
- **Resident router:** existing 32-row f16 unit centroids (48 B/row) plus, per page, the top-16 pages by cut-edge count (u32 id + u16 weight, ~96 B/page). Delete the two-bit plane. Predeclared envelope: ≤56 B/row resident, i.e. 5.6 GB at 100M as arithmetic, not RSS. The earlier 3 GiB figure would additionally require 8-bit or page-level centroids; that is a separate declared step, not a silent relaxation.
- **Query:** existing HNSW seed → candidate generation by adjacency expansion (frontier ordered by hops then min-unit-centroid distance) to 159 pages → rank by min-unit distance → unchanged `choose_budgeted_pages_sparse`, 32 GET, 16 MiB, single SQ8 wave, 780 B/row.
- **Distinct from closed arms:** V149–V154 navigated by centroid geometry over physical or unit summaries; V38/spill copied rows; V283 packed k-means cells; V239/V249 kept a row-level resident graph. Nothing resident here is row-level, and no pointer chasing touches S3.

## Lifecycle accounting

Bytes and requests per query are unchanged. Inserts stay in the mutation delta (two_bit_mutations.rs); compaction assigns new rows by kNN against fetched candidate pages only, appends to the best adjacent page with free capacity, and increments cut-edge weights, so no O(N) graph rebuild per compaction. Deletes are the existing logical tombstones; adjacency is a prior, not a correctness structure, so stale edges only cost recall. Pinned-generation memory extends the existing `graph_resident_bytes` model with the adjacency blob. Metric: the production bounded route accepts only SquaredEuclidean at D ≥ 64 (native_ann_build.rs:34, native_ann.rs:282); this arm inherits the two-bit route's normalized-cosine research status and proves nothing about production Cosine.

## Falsifier, one Spark job, no GT in construction

CoHere first100k, dev 0–63 only, ≤900 s, 4 GiB, BLAS 1, blocked GEMM for kNN.

1. **Step 0 (exploratory replay, existing artifacts):** on the consumed 744-query validation, split native loss into 159-candidate coverage vs 84-page cut. If the cut is also lossless there, plane removal is licensed regardless of layout.
2. **M1, layout only, GT-aware witness:** distinct pages holding GT-100 and the ≤32-range/84-page DP witness, paired against the native flat layout with the same script. KILL if the witness regresses below the flat layout.
3. **M2, decisive, GT-free:** fetched GT-100 coverage of the adjacency-expanded centroid route. KILL if mean or p05 is below the paired native two-bit dev result (99.766 / 99). Matching with zero per-row resident bytes is the whole claim.
4. ReLAION only if CoHere passes. Validation untouched.

PASS licenses one code action: `fit_cut_edge_layout` in source_order.rs, an `adjacency.bin` artifact in two_bit_build.rs, and replacing the plane ranking in `plan_inner` (two_bit_generation.rs:372). The subsequent frozen returned gate needs fresh queries. Ordinals 64–255 appear unused by run.py's splits. Verify that against the ledger before sealing them.

## Strongest counterargument and what the gate cannot show

At 100k the byte cap admits 21.5% of pages, so any sane layout contains GT-100. The gate discriminates nomination, not the K-growth wall at 1M–100M, and the adjacency prior can miss three-way boundaries exactly as V40's spill router did. A 100M kNN build is itself a lifecycle cost the project has avoided, and it is unmeasured here. The gate establishes nothing about cold S3 latency, RSS, QPS, or either vendor.

If no arm is wanted, the exact missing causal evidence is Step 0 plus the GT-100 page-spread growth 100k → 1M under the current layout.
