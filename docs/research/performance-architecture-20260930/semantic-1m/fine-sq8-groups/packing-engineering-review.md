# Packing diagnostic engineering review

Reviewer: GPT-6 Astra. Consultation `2549d75f4ae34ad6`, group `ab44181ad4974985`. Completed read-only review of base `c79700e5e93440566e02c1fbbb1b7adfc6d61864`. Findings were sent to the same implementation worker. This review establishes no compilation, execution, recall or performance result.

**The method is distinct enough from RCM to justify one frozen diagnostic, but the proposed diagnostic is not yet run-ready.** At `c79700e5`, packing remains unimplemented. These are concrete implementation/admission blockers, not reproduced failures of a packer.

1. **P1 — Existing readers cannot enforce the required prefix boundary unchanged.**  
   [`verify_nomination_prefix`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:859) requires file length to equal prefix length. The closed trace is **9,064,136 bytes**, while the permitted prefix is **1,665,668 bytes**. The general artifact reader authenticates the whole file and probes EOF.

   Use a bounded read from one securely opened regular file, authenticate exactly the permitted bytes, then parse those same bytes. Put the byte limit beneath any buffering; never probe beyond it. Seal **both panel permutations before opening the shared prefix**, even with sequential graph processing.

   **Small falsifier:** an instrumented reader containing a valid prefix followed by forbidden bytes; fail on any read beyond the boundary. Truncation and one-byte prefix corruption must produce INVALID.

2. **P1 — Bind the graph, original ordinal domain, permutation and replay together.**  
   [`PqVectorGraph::open_authenticated`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/resident_vector_graph.rs:1043) checks its supplied identity, but the caller must obtain that identity and graph descriptor from the authenticated historical root. A same-sized, internally consistent replacement graph must not become admissible through configuration.

   The new seal should bind both original root hashes, graph hashes/lengths, full graph identities, geometry, algorithm version/rules, permutation hashes, source/config identity, and the expected historical seal/prefix identity. The final receipt must bind that seal and all 128 results. Require exactly 64 distinct ordinals per panel, valid unique nominees, and complete inverse-map round trips.

   I independently hashed both small root manifests: they match the historical nomination seal. That seal’s SHA-256 is `374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62`.

   **Small falsifier:** swap panel graphs, substitute a valid graph with a different layout identity, alter one permutation entry, or duplicate a replay ordinal. Each must be INVALID.

3. **P1 — Tail placement needs an explicit algorithmic exception, and replay must explicitly request 32 ranges.**  
   “Keep a short final group last” conflicts with unrestricted greatest-affinity selection unless the tail is excluded from seed/candidate selection. Reserving it initially and moving it afterward can produce different packs; freeze one rule. Reserving it and appending it once is simplest.

   [`cover_pages`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budgeted_page_rank.rs:28) assumes fixed-width groups with only the final group clipped. An interior tail invalidates its offsets and charges. Also, current [`MAX_GETS`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:30) is **256**; reusing `plan_nomination` unchanged would test the wrong envelope. Call the cover primitive with explicit `32`, retaining historical behavior elsewhere.

   **Small falsifiers:** N=33 with strongest affinity to the one-row tail; 33 separated selected groups requiring a bridge; and byte accounting where 1,344 full groups cost **16,773,120 bytes**, while 1,345 cost **16,785,600 bytes**. More than 32 touched packs alone must not reject a feasible exact cover.

4. **P2 — “Edges between groups” leaves materially different deterministic algorithms possible.**  
   The graph stores its base layer **last**, not first. It permits asymmetric adjacency. Specify whether reciprocal arcs count twice. The natural reading is: each stored directed base arc contributes one to the unordered group-pair weight; ignore intra-group arcs, then expose that weight symmetrically.

   This matters because the builder adds directed reachability and indegree-repair edges, including an ordinal cycle ([builder](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/centroid_hnsw.rs:838)). These are part of the frozen graph; silently filtering them would define another method.

   **Small falsifiers:** an incoming-only edge from group 2 to seed 0 must influence selection; upper-layer-only edges must not. Separately test summed affinity versus affinity to the last addition, equal-score ID ties, zero-affinity fallback, shuffled edge order, and score reset between packs.

5. **P1 — The 256 MiB/five-minute envelope lacks a construction peak model.**  
   The decoder retains encoded bytes while allocating nested adjacency, then allocates reachability scratch. Its `512N` cap is not a whole-process cap. CoHere’s manifest records **26,844,914 encoded graph bytes** and **31,415,732 graph-capacity bytes**, before packing adjacency, aggregation scratch, planner state and allocator/runtime overhead.

   “Bounded sparse adjacency” needs checked bounds on actual capacities and their coexistence. A lazy heap with stale entries is bounded by updates, not group count. At just **6,250 groups**, incremental scores plus a deterministic scan avoid that complication: ≤39.1 million score-array visits per panel, with roughly 25 KB of `u32` scores. This bounds selection work, not total runtime.

   **Small falsifiers:** reuse the existing authenticated nested-allocation rejection fixture, plus a dense cross-group fixture and an admission limit one byte below the modeled peak. Reject before exceeding capacity. Enforce CPU1, memory, swap and deadline externally; if the model cannot fit, report the required cap rather than treating resource failure as packing rejection.

6. **P1 — A visible success terminal is insufficient after sync failure or process death.**  
   [`Events::run`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:639) writes the terminal before its final `sync_all`. A failed final sync can leave readable success JSON despite an error exit. OOM/SIGKILL cannot reliably produce an in-process INVALID terminal.

   Create permutation/seal/output files without overwrite; sync permutation files, seal and parent directory before nomination access. Accept survival or rejection only with a matching successful exit/resource receipt and verified output bindings. Timeout, OOM, authentication, admission and persistence failures are INVALID.

   **Small falsifiers:** inject failure at permutation sync, seal sync, directory sync and final terminal sync; kill between sealing and replay; retry against existing files. Existing artifacts must remain unchanged, and no failed execution may count as a scientific rejection or survival.

The causal case is **plausible but limited**. Capacity-constrained, weighted group aggregation differs materially from RCM’s traversal ordering, and preserves the original 16-row neighborhoods. However, graph adjacency—including repair edges—has not been shown to predict joint nomination. CoHere’s containment result also means high returned quality cannot be attributed to nominees alone: packing preserves nominee-group members but can lose useful rows from old gap bridges. The proposed later returned-recall gate is therefore necessary.

No serving-format rewrite, mutation work, cloud benchmark or broader product qualification is needed to resolve these diagnostic blockers. I performed static inspection and small metadata hashing only; no files were edited and no native/Cargo/cloud experiments were run.

