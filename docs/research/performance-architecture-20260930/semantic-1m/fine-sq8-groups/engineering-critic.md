**Verdict: proceed with the bounded Rust implementation, but gate the 100k run on the checks below.** Finer fetch groups materially change the V239/V240 payload problem; this is a legitimate new falsifier, not evidence that those failures were overturned.

Reviewed `922b7c8`. During inspection, HEAD advanced to `fbdd355c`, changing only the protocol: its addendum resolves the original exhaustion-policy and 32 MiB admission ambiguities. Rust remains unchanged. No edits, builds, native execution, corpus/GT access, AWS, children or consultations.

1. **BLOCKER — enforce scorer calls, not graph visits.**  
   [`navigate_from_with_workspace`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/resident_vector_graph.rs:865) scores upper-layer neighbors before initializing `visits`, and scores the starting node again. Its frontier heap can also exceed `ef`; `ef4096` bounds retained results, not all workspace. The amended exhaustion policy is sound, but the existing counter cannot implement it. Preserve the actual exhausted shortlist and use that identical list for both layout comparisons; never describe a shorter list as the V239 1,024-candidate result.

   **Smallest native check:** a tiny multilayer graph with an independently counted scoring callback; exhaust during descent and during base expansion, assert no invocation beyond the cap, deterministic retained nominees, and counter reset on the next query.

2. **BLOCKER — serving admission must precede graph decoding, and stage memory exceeds one query’s memory.**  
   [`open_authenticated`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/resident_vector_graph.rs:161) allocates nested adjacency before authenticating the final digest, permits up to 17 layers and 256 edges per layer, and accepts no allocation budget. Checking `heap_bytes()` afterward cannot prevent an oversized decode. Separately, [`query_payload_bytes`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:81) charges approximately 53 MiB before graph workspace/planning. The amended 512 MiB **stage** admission must also include retained panel evidence, loader temporaries, resident generations and runtime allowance—even with one active query.

   **Smallest native check:** inject an allocation limit into opening and show that oversized adjacency is rejected before crossing it; independently check aggregate admission with retained evaluator state and two pinned generations.

3. **BLOCKER — normalize queries explicitly before reusing SQ8 ranking.**  
   [`score_nominees`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:47) computes squared L2 using the supplied query and stored SQ8 norm. It does not normalize. PQ cosine navigation normalizes scores, so passing raw nonunit queries to the returned-row scorer can make nomination and ranking disagree. PQ preparation also computes raw dot products in `f32` before applying its inverse query norm, exposing extreme finite inputs to overflow/underflow. Existing hierarchical serving already uses [`cosine_vector`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/sq8_source.rs:22).

   **Smallest native check:** independently hand-score a tiny SQ8 fixture with unequal reconstruction norms and nonzero coefficients; verify identical ordering for a query and a positive power-of-two scaling. Include finite extreme magnitudes, zero and NaN. Preserve the historical SQ8 kernel and bytes.

4. **BLOCKER — removing FP16 must preserve authenticated identity and bounded startup.**  
   Today both graph opening and [`ResidentPqCosineGraph`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/resident_vector_graph.rs:940) depend on the FP16 plane. Removing that argument alone loses its identity authority. The proposed replacement root must bind graph, PQ, physical order, geometry, coefficients and group hashes together. `PageAuthority` authenticates fetched groups; the ranker trusts its caller.

   The minimal productive API is the proposed codes-only scorer, bounded authenticated graph open, bounded nomination, and an opaque fetch plan tied to the query and pinned snapshot. Startup should require router metadata, not reading the entire SQ8 object.

   **Smallest native check:** reopen a tiny persisted generation in a fresh process with source/FP16 files absent; reject a same-sized swapped PQ/order artifact, altered plan, and corruption inside a **bridged** group. Verify short-tail authentication.

5. **BLOCKER — seal both datasets before either truth open.**  
   The new protocol requires this, but the existing [`paired_overlap` flow](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:319) seals and opens truth within one dataset invocation. Calling that pattern sequentially would expose the first dataset’s results before the second dataset’s nominations were sealed.

   **Smallest native check:** a two-panel synthetic diagnostic with guarded truth readers; assert neither reader opens until both durable plan seals exist. Preserve all 64 records, denominator 100, refusals and explicit FAIL/INVALID distinctions. This needs a thin native phase boundary, not another controller feature.

6. **ADVISORY — 3,997,696 bytes is guaranteed minimum bridging headroom, not a separate gap cap.**  
   The [calculation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/specialist-plan.md:26) assumes 1,024 distinct groups. Shared groups leave more room. For example, two nominees in each of 512 groups numbered `0,3,…,1533` require 6,389,760 bytes of bridging to reach 256 ranges, yet total payload is only 12,779,520 bytes: valid.

   **Smallest native check:** that fixture plus brute-force tiny interval covers. Reuse `cover_pages`; the neighboring `choose_budgeted_pages*` APIs intentionally discard candidates and would violate this arm.

7. **ADVISORY / serving acceptance gate — exclusions alone do not implement replacement visibility.**  
   [`rank_returned_ranges_excluding`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:151) suppresses old IDs but neither scores replacements nor pins a mutation revision. Bind execution to one immutable base-plus-delta snapshot, merge replacement scores before final truncation, and report underfill.

   **Smallest native check:** replace an ID outside the base shortlist with the best-scoring delta row, delete another ID, then verify both the new snapshot and an older held reader. Incremental graph maintenance can remain explicitly unsupported.

8. **ADVISORY / promotion gate — distinguish built capacity from reopened capacity.**  
   [`connect_row`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/centroid_hnsw.rs:1381) pushes then truncates adjacency; truncation retains capacity. Reachability repair adds edges. Thus neither `m0=64` nor reopened graph size bounds builder heap. Freeze the exact serial/batched builder and charge its actual capacities.

   The 58.2 GB router ceiling and 522.8 GB known build charge are arithmetic, with additional build terms excluded. If the intended product envelope cannot accommodate those costs plus pins, concurrency and maintenance, **redesign before promotion**. A passing 100k result cannot waive that requirement; full 100M qualification is unnecessary before this deliberately falsifying 100k test.

The prior overlap remains scientific FAIL. One dependency stage still permits eight scheduling batches at 32 slots; local range counts establish neither physical S3 latency nor vendor parity.
