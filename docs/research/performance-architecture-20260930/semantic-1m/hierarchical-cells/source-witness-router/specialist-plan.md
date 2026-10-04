**Choose a source-witness router: replace each cell’s single mean with 16 source-derived SQ8 representatives, then select the globally best 24 cells.** Keep the retained partitioner layout unchanged. This is a falsifiable routing hypothesis, not a qualified production architecture.

The causal evidence supports changing representation next: partition repair barely changed recall, and global centroid-only routing already failed. Current selected-cell ceilings are only 87.109375%/77.484375%; better nomination alone cannot reach 98%. [Closed decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/capacity-constrained-partitioner/paired100k/a0001/decision.md), [global-leaf failure](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k/global-leaf-probe/a0002/decision.md).

1. **Freeze this policy before implementation.**

   Current routing compares the normalized query with one unnormalized cell mean. A mean can hide a cell’s distinct neighborhoods. Instead, build `min(16, cell_rows)` witnesses from each cell’s authenticated SQ8 rows:

   - Decode and normalize rows for **source-only** representative selection.
   - Start with the smallest source ID; repeatedly select the row farthest from its nearest selected witness, breaking ties by source ID.
   - Store each witness’s original SQ8 record plus cell ID/local row ordinal.
   - At query time, use the existing SQ8 scoring arithmetic. A cell’s score is its best witness score; select exactly 24 cells, ties by cell ID.

   Score witnesses from **every cell**, without the old hierarchy filtering them first. A nearby witness can therefore admit a previously excluded cell and recover residual selected-cell misses. Coverage is uncertain: sparse representatives may miss neighborhoods or overemphasize outliers. No query-derived training, parameter sweep, fallback widening or dataset-specific policy.

2. **Keep the implementation inside the existing Rust prototype.**

   Modify [hierarchical_semantic_cells.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:1414) and its [existing CLI](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:53). Proposed public seams:

   ```rust
   build_source_probes(layout: &Prototype, output: &Path)
       -> Result<Artifact>

   SourceProbeRouter::open(
       layout: &Prototype, artifact: &Artifact, max_payload_bytes: usize
   ) -> Result<Self>

   SourceProbeRouter::select(&self, query: &[f32])
       -> Result<Vec<usize>> // exactly 24 ordered cell IDs

   Prototype::search_probed(
       &self, router: &SourceProbeRouter, query: &[f32],
       top_k: usize, options: SearchOptions
   ) -> std::result::Result<SearchTrace, SearchFailure>
   ```

   Reuse selected-cell fetching, accounting, authentication and `rank_returned_ranges`. Give the probe sidecar a new schema binding layout-root SHA, codec coefficients, witness IDs and fixed policy. Its builder accepts no requests/truth descriptors. Add only build/diagnose modes to the existing binary.

3. **Run one bounded native falsifier before scale—later, not during this consultation.**

   Reuse the retained **partitioner** layouts and consumed64 panels for both datasets; do not regenerate sources, layouts or truth.

   - First verify deterministic witnesses, scalar scoring/top24 parity, ties, ID binding, corrupted artifacts and cap rejection on small fixtures.
   - Build witnesses with requests/truth unopened. Freeze sidecars and configuration.
   - Execute one routing-coverage pass over each consumed64 panel; freeze selected rosters before truth attribution.
   - **Stop if either dataset misses 98% mean selected-cell coverage or p05 ≥95 hits.** This rejects the fixed 16-witness/top24 policy, not every representative router.
   - Only if both pass, execute returned recall using the same selections and existing `blocks_per_cell=16`, which admits every fetched block. Require the same 98%/95 gates. This is an all-SQ8 diagnostic; it does not establish a faster implementation.

   Use one dataset/process at a time: prospective **one CPU, 512 MiB, no swap, ten minutes per dataset**, with exact input/output inventory admitted beforehand and the existing 16 GiB scratch safety ceiling preserved. Authentication, implementation or resource failure is **INVALID**, not a quality rejection. Before any paid execution, require exact-input runtime admission and the separate disposable canary.

   Consumed-panel success permits further qualification only; it is not held-out confirmation.

4. **Preserve the loss decomposition.**

   For truth set \(G\), selected-cell rows \(C\), nominated rows \(A\), and returned rows \(R\):

   \[
   \text{misses}=|G\setminus C|+|G\cap(C\setminus A)|+|G\cap(A\setminus R)|.
   \]

   Current ReLAION/CoHere counts are **825/1,441 routing misses**, **1,413/535 nomination misses**, and **0/0 final-ranking misses**. Report newly recovered **and newly lost** truth positions against the retained control. With all blocks admitted, nomination loss should become zero; final-ranking loss must be measured again because additional SQ8 competitors can displace truth.

5. **Measure these costs; do not borrow old timing claims.**

   Values below are ReLAION/CoHere. [Metrics](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/capacity-constrained-partitioner/paired100k/a0001/closed-metrics.json).

   | Stage/resource | Current measured evidence | Proposed arithmetic or unknown |
   |---|---|---|
   | Routing CPU | 0.565/0.626 ms | 4,816/4,928 witness scores; 3.70/3.78 million coordinate evaluations. CPU unknown. |
   | Fetch + nomination CPU | 9.384/9.064 ms | Selected occupancy changes; existing all-block path still performs SQ2 work. |
   | Ranking CPU | 3.232/3.239 ms | All-SQ8 scores every selected row; CPU unknown. |
   | Query local reads/bytes | 24; 8.258/7.939 MB mean | 24; at most \(24×512×988=12.141\) MB. Routing has zero query reads after preload. |
   | Directory memory | 3.038/3.101 MB tracked owned capacity; 26.269/26.324 MB modeled parsed allowance | Add approximately **3.795/3.883 MB** packed witnesses, plus containers/startup overlap. |
   | Query process HWM | 17.822/17.990 MB | Remeasure stage snapshots and externally monitored peaks. |

   Historical campaign scratch was **7.748 GB**, exceeding its **6.482 GB** model; host peak reached 2 GiB with reclaim and no OOM. Neither number is a universal product cap. New accounting must include retained inputs, staged/final sidecars, traces, binaries, temporary copies and cache. [Resource verification](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/capacity-constrained-partitioner/paired100k/a0001/root-verification.json).

6. **Treat 100M and maintenance as explicit qualification boundaries.**

   Let average cell occupancy be \(m\), cells \(C=N/m\), witnesses \(W=16C\), dimension \(D=768\):

   - Packed witness storage: \(W(D+20)\). Preserving observed occupancy gives approximately **3.8–3.9 GB at 100M**.
   - Flat routing: \(WD\), approximately **3.7–3.8 billion coordinate evaluations/query**. The 100k scan is a representation falsifier; its 100M performance is unqualified.
   - Existing cell payload: \(988N=\mathbf{98.8\ GB}\). Retaining current directory density also implies roughly 6.4 GB encoded directory—not a bounded root.
   - Witness build: \(O(16ND)\) work, one cell’s decoded vectors buffered, plus output. Scratch must account for inputs and simultaneously live staging/output.
   - Serving RSS: resident directory/probes or bounded cache **plus concurrent query buffers**, runtime and allocator overhead. Paging/indexing witnesses introduces new routing loss and fetch dependencies requiring another qualification.
   - Incremental maintenance can rebuild witnesses for touched cells: at most **12,608 witness bytes/cell**, alongside payload rewrites. Inserts, deletions, splits, pinned readers and atomic publication still require native lifecycle integration. Pinned storage is the union of reachable versions, with \(pV\) a conservative whole-generation bound.

   Total dollars must include build, storage, query requests/transfer, maintenance, overlap and recovery per successful query. None is measured here.

**SIMD ceiling:** current summed stage CPU is **13.180/12.929 ms/query**. Even eliminating the entire nomination stage—more than SIMD could accomplish—saves at most **9.384/9.064 ms**, giving **3.47×/3.35×** stage-CPU speedups. Halving that entire stage gives only **1.55×/1.54×**. Actual savings are \(K(1-1/s)\), with SIMD-eligible \(K\) unmeasured. These are CPU bounds, not S3 latency or QPS predictions.

The implementation boundary is the sidecar router and falsifier. Before handoff, qualify the changed target, required workspace Clippy and `scripts/check_rust_test_build.sh` on the exact revision. Preserve historical FAILs; defer paging, maintenance integration and performance optimization until this gate survives.

Read-only inspection at `56a639e`; no files edited, native tests run, truth bodies opened, cloud actions or children launched.
