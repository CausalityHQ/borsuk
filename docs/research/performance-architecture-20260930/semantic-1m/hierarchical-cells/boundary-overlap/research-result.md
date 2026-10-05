**Choose bounded geometric boundary replication into whole SQ8 cells.** Keep the primary partition and query router fixed; change which nearby rows each fetched cell contains.

The inspected Rust source matches `3bb71007577a7688830aed776ade5e6d215d2d03`. During inspection, the shared checkout advanced to `3e3319b5…` with documentation and an additional retained-evidence audit. That new evidence materially changes the decision:

| Perfect selector of unchanged cells | ReLAION mean / p05 | CoHere mean / p05 |
|---|---:|---:|
| 24 cells | 97.640625% / 87 | 92.625% / 80 |
| 32 cells | 99.421875% / 95 | **97.171875% / 88** |

These are truth-informed ceilings, not deployable routing or new ANN measurements. The audit’s top-count selections fit 16 MiB, so **any router returning at most 32 unchanged whole cells necessarily fails the CoHere target**. I inspected the audit code and receipt; I did not rerun it. [Supplemental oracle](./docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/a0001/layout-oracle.json)

The original witness arm remains scientific **FAIL**, with infrastructure **INVALID** preserved separately. Its coverage regression and the partitioner’s additional local-nomination losses remain relevant. [Witness decision](./docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/a0001/decision.md), [closed loss measurements](./docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/capacity-constrained-partitioner/paired100k/a0001/closed-metrics.json)

| Choice | Mechanism and evidence | Decision now |
|---|---|---|
| Resident compressed row graph → candidate cells | Avoids weak cell summaries, but cannot exceed the unchanged-layout ceiling. V239 also achieved 99.894% candidate containment while requiring 29.35 MB of minimum page payload at p95; V240 reordering worsened it. | Defer. A viable version needs changed fetching or layout as well. |
| Geometric boundary replication | Makes a row reachable from another cell, directly changing the coverage ceiling without query-time graph traversal. | **Implement one bounded falsifier.** Its achievable coverage remains unknown. |

Historical replication failures constrain expectations and lifecycle cost; their arm-specific limits do not universally prohibit overlap. [V239](./docs/research/v239-graph-candidate-containment-100k-closeout.md), [V240](./docs/research/v240-graph-local-order-100k-closeout.md), [existing research reconciliation](./docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/implementation-review/layout-research-result-20261003.md)

1. **Implement one deterministic overlap rule.**

   Preserve every primary row assignment, stored routing prototype, SQ8 code and coefficient. During deterministic primary-layout replay, retain each actual partition boundary—including capacity-adjusted thresholds and source-ID tie rules. An ordinary nearest-centroid bisector is insufficient because `capacity_partition` can move the cut.

   For each source row:

   - Find its nearest geometric boundary along its primary assignment path.
   - Cross that boundary and descend the sibling subtree deterministically to one alternate leaf.
   - Propose one replica, prioritized by normalized boundary distance, then source ID.
   - Admit at most **one extra copy per row**, **128 replicas per destination cell**, and **0.25N replicas globally**. Reject overflow; never widen the fetch plan.

   Keep query routing unchanged. Store primary rows and accepted replicas together in one authenticated SQ8 extent per cell. Score every fetched SQ8 row; deduplicate logical IDs **before top-k truncation**.

   This tests whether boundary separation caused recoverable misses. It does not presume that near-boundary rows explain enough of the remaining loss.

2. **Freeze a prospective envelope that accounts for the copies.**

   With at most 512 primary rows plus 128 replicas, D768 SQ8 records remain 780 bytes:

   \[
   32 \times 640 \times 780 = 15{,}974{,}400\text{ bytes}.
   \]

   That leaves **802,816 bytes** below 16 MiB for charged cell framing and related payload. Fetch at most 32 authenticated cell extents, with all locations known before payload reads. There is no S3 graph traversal or subsequent refinement fetch.

   Use an SQ8-only cell format for **both** the no-overlap control and overlap candidate. Otherwise removing SQ2 nomination would confound the overlap result. Preserve the original historical results unchanged.

   This envelope is **after router admission**. Cold router hydration, head operations, retries and mutation loading need separate accounting; it does not establish an all-inclusive cold 32-GET result.

   At 100M, the following are arithmetic only:

   - Primary SQ8: **78 GB**; maximum extra replicas: **19.5 GB**.
   - A PQ64 row graph needs 6.4 GB codes, approximately 0.4 GB norms and 0.4 GB row→cell mappings. Assuming 64 average base edges and 64-bit offsets adds approximately 26.4 GB: **33.6 GB before upper layers, allocator overhead and query workspaces**.
   - Overlap avoids that resident row graph, but the existing directory is still substantial: its binary prototype arithmetic alone is approximately **1.2–2.4 GB**, depending on occupancy. Current parsed-directory admission, pins and startup copies must also be charged.

   Neither architecture has measured 100M feasibility. Use
   `unique pinned generations + delta + concurrency × query scratch + maintenance + runtime`
   for admission, not a borrowed historical RAM cap.

3. **Keep the Rust scope explicit and reusable.**

   | File | Proposed change |
   |---|---|
   | [hierarchical_semantic_cells.rs](./crates/borsuk/src/hierarchical_semantic_cells.rs) | Emit actual `SplitBoundary` metadata; expose authenticated primary membership and a `select_cells` result. Assert replayed primary membership/prototypes match the retained layout. |
   | New `crates/borsuk/src/semantic_cell_overlap.rs` | Implement `build_overlap`, `OverlapIndex::open`, and `search_selected_sq8`; own replica admission, authenticated cell extents and resource receipts. |
   | [returned_sq8.rs](./crates/borsuk/src/returned_sq8.rs) | Add a unique-ID ranking entry point using unchanged SQ8 arithmetic; reject inconsistent duplicate bodies within a pinned generation. |
   | [bin/hierarchical_semantic_cells.rs](./crates/borsuk/src/bin/hierarchical_semantic_cells.rs) | Add bounded build/nominate/diagnose commands; seal selections before opening truth. |
   | `lib.rs` and new native integration tests | Export the library API and exercise correctness independently of the reporting CLI. |

   Give the overlap artifact its own version marker and reject incompatible artifacts. No new Python algorithm, controller or quantizer is needed.

4. **Run the native falsifier before any scale campaign.**

   Freeze one policy and both consumed64 FIRST100k panels; no parameter ladder.

   - **Correctness first:** small independent fixtures covering capacity-shifted boundaries, tied/degenerate cuts, replica quotas, corrupt spans, source-ID mappings, duplicate removal, nonunit queries, deletes and replacements. Independently reconstruct selected unique rows and compare SQ8 ranking.
   - **Paired native test:** identical primary layouts, routing selections and full-cell SQ8 scoring, with overlap disabled/enabled. Record unique selected-cell GT coverage, returned R@100, p05, actual local range reads/bytes, stage CPU, peak memory, build scratch and replica distribution.
   - **Proposed execution bounds:** datasets sequentially; build ≤4 workers, 8 GiB RAM, 8 GiB scratch and 20 minutes per panel; query/reference evaluation one worker, 512 MiB and five minutes per panel. These are new experiment bounds, not product limits. Compilation has a separate bounded remote allocation.
   - **Pass:** both panels achieve selected coverage **≥98%, p05≥95**, and returned recall **≥98%, p05≥95**, within every declared fetch and resource bound. Coverage passing while returned recall fails is insufficient. A valid execution missing these criteria rejects this overlap policy; authentication, code or resource-execution errors are **INVALID**.

   The consumed panels can falsify the mechanism, not establish generalization. Any survivor still needs fresh quality and later cold service qualification.

   Before integration, compile/run the affected native targets, run workspace Clippy with the prescribed correctness/suspicious denies, and run `bash scripts/check_rust_test_build.sh` on the exact revision. Record revision and exit statuses. Before any later paid experiment, perform exact-input runtime admission and a separate disposable canary.

5. **Make lifecycle behavior part of the same library change.**

   Persist primary owner and optional replica destination together. Queries pin one base/delta revision; pending replacements and tombstones suppress every older copy. Keep writes in a bounded query-visible delta until maintenance publishes affected primary/replica cells atomically.

   Compaction must rewrite affected cells and directory paths, preserve canonical originals, enforce replica quotas, and charge old/new generations simultaneously. Local split handling must also update incident replicas. Publish immutable objects before the conditional head update; reclaim them only after pins release.

   Reuse the existing mutation/publication foundations, but do not claim that current whole-generation compaction or the Fresh1m maintenance refusal already provides this behavior. Lifecycle correctness fixtures precede scale; measured update amplification and reclamation cost remain promotion gates.

**SIMD should wait.** Eliminating all measured witness-routing CPU could save at most its **2.38/2.45 ms mean CPU**, while leaving its coverage failure intact. The older fixed48 service has a separate **131.094 ms mean POST CPU ceiling**; eligible kernel CPU is unknown, and its “halve all CPU” p90 of 472.595 ms is conditional arithmetic. Neither supports prioritizing SIMD over changing coverage. Profile the surviving full-SQ8 path before selecting a kernel. [CPU evidence](./docs/research/performance-architecture-20260930/semantic-1m/fixed48/architecture-decision-20261002/critical-path-evidence.json)

The principal uncertainty is whether one alternate destination and 25% replication can repair diffuse neighborhoods and routing misses. Failure should stop this policy without retuning the consumed queries. No files were edited, native/data/cloud jobs run, children launched, or consultations started; no vendor advantage is established.
