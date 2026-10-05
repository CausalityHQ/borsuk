# Packing diagnostic research review

Consultation `380cd1fcb2874591`, dual group `ab44181ad4974985`, completed read-only. Effective reviewer GPT-6.1 Sol after the Claude Opus attempt failed OAuth refresh. This is the original automatic fallback, not a second review launch. Base reviewed: `c79700e5e93440566e02c1fbbb1b7adfc6d61864`.

Root reconciliation: retain one fixed native diagnostic, without a serving rewrite or panel tuning. Both critics agree on prefix isolation, root/graph bindings, explicit 32-range cover, preallocation admission and independent supervisor authority. Clarifications are preregistered in `packing-diagnostic-method.md` and sent to the same Rust worker. Native compilation/execution remains pending. The aggregate CoHere arithmetic is 6,305 returned hits minus 6,119 nominee-contained hits = at least 186 outside-nominee hits; mean gate 6,272 leaves 33 hits headroom. This is an aggregate lower bound, not an identification of which rows or ranges supplied them.

**The method is materially distinct from V240 and warrants one bounded locality falsifier. The plan still needs explicit rules for sealing, prefix reads, resource admission and failure handling before execution.** Survival would justify the returned-recall gate.

Reviewed HEAD `c79700e`. No files changed; no native/Cargo/cloud work or large data bodies were read. The retained panel manifests and nomination seal matched their recorded hashes.

The causal justification is narrower than [packing-plan.md](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/packing-plan.md:5) suggests. Physical scatter demonstrably defeats the current 32-range cover. That does **not** establish that HNSW edge affinity predicts joint nomination. Nor does CoHere nomination itself meet quality: containment is 95.609375%, whereas returned recall is 98.515625%. Across 64 queries, at least **186 returned GT hits** come from outside the nominee sets. Returned recall has only **33 hits of aggregate headroom** above the mean gate, and p05 already equals its threshold. Preserving nominees therefore provides weak reassurance about returned recall. [Native summary](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/native-phase-summary.json)

V240 used a row permutation from RCM, whose objective is matrix bandwidth. This proposal contracts indivisible groups, retains edge multiplicity, and greedily maximizes affinity to a capacity-limited pack. That is a substantive methodological difference. Graph-local ordering already has prior art such as [Gorder](https://github.com/datourat/Gorder); [recursive graph bisection](https://arxiv.org/abs/1602.08820) supplies another locality-oriented alternative. Neither establishes ANN cover or recall performance. The relevant RCM implementation operates on matrix structure, including symmetrization, rather than this accumulated weighted-pack objective. [SciPy documentation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csgraph.reverse_cuthill_mckee.html)

Important weaknesses remain: pairwise edge density substitutes for query-set locality; lowest-ID seeds retain dependence on the old ordering; and authenticated base edges include an ordinal cycle and indegree repair, not exclusively geometric neighbors. The latter is explicit in [centroid_hnsw.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/centroid_hnsw.rs:838). Keep the fixed rule, but do not interpret internal edge density as evidence that the cover will improve.

The execution blockers are:

1. **Seal both permutations before opening the shared prefix.**  
   The prefix contains both panels. Sequentially packing ReLAION, replaying the prefix, then packing CoHere would violate the intended separation. Construct and durably seal both maps sequentially, release construction storage, then open the prefix once.

   The existing [verify_nomination_prefix](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:859) requires the entire file length to equal the prefix length. It cannot directly validate the closed 9,064,136-byte trace. The new reader must read/hash exactly 1,665,668 bytes, without an EOF probe or buffered read into the remainder. Put the byte limiter **under** buffering.

2. **Make graph weighting and tail placement unambiguous.**  
   Base adjacency is `tower.last()`, not layer zero. Define symmetric group affinity as the count of stored directed edges in either direction: reciprocal edges contribute twice; within-group edges contribute nothing. Group IDs come from original graph ordinals divided by 16, not logical document IDs.

   Reserve a short final group before greedy selection and place it last. It still consumes a pack slot; appending it to an already full pack would violate capacity. This issue is invisible at exactly 100,000 rows.

   Nominee translation is `16 × old_to_new[old_ordinal / 16] + old_ordinal % 16`. Validate both permutation directions and round trips.

3. **Specify aggregate admission and a bounded algorithm.**  
   The retained manifests describe roughly 26.8 MB of encoded graph and 31.4 MB of graph allocation payload. These are not total RSS. The decoder holds encoded bytes alongside nested allocations and performs additional reachability allocations. [Decoder seam](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/resident_vector_graph.rs:1041)

   Charge encoded input, decoded capacities, edge aggregation, both adjacency directions, sorting/build scratch, scores, assigned marks, heap capacities, maps, serialization and runtime margin **before growth**. “Sparse” does not establish a memory bound. A stale-entry heap can retain far more entries than groups.

   Incremental affinity updates plus scanning the 6,250 group scores gives planner work bounded by approximately `O(M + g²)`, excluding aggregation. Repeatedly scanning raw edges for every selection gives `O(gE)` work and needs separate justification. The five-minute limit needs an external supervisor; allocator failure or SIGKILL cannot reliably produce an in-process receipt.

   A dense `u32` group matrix costs 156,250,000 bytes and is a possible simpler representation for this isolated 100k diagnostic. It still needs aggregate admission and measured runtime margin. Otherwise retain bounded sparse storage or preregister an honest larger cap.

4. **Use the cover seam without invoking the serving pipeline.**  
   [plan_nomination](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:648) uses `MAX_GETS=256`. The diagnostic must explicitly call the existing exact cover with 32; changing the shared serving constant would exceed its scope.

   `FineSq8Index::open` opens SQ8 records, and `paired_fine` opens requests. Neither is the diagnostic entry path. Authenticate capped metadata and the graph, expose borrowed validated base edges, and use a narrow wrapper around [cover_pages](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budgeted_page_rank.rs:28). No serving-root v2 or physical rewrite is needed for this test.

   The pack arithmetic is correct: 32 full packs cost 16,773,120 bytes, leaving **4,096 bytes**. Packing does not guarantee that a query touches at most 32 packs; exact cover remains the acceptance criterion.

5. **Bind identities and preserve durable failure evidence.**  
   Each packing seal should bind the authenticated original panel root, graph descriptor and complete graph identity, algorithm/version, capacity 42, map direction, map hash and diagnostic source/configuration identity. Keep the frozen trace’s source identity distinct from the modified diagnostic’s identity.

   Authenticate the original nomination seal itself, then enforce exactly 64 plan ordinals per panel, matching root bindings, bounded unique nominees and the fixed prefix hash. Prefix hashing alone does not validate parsed panel/ordinal semantics.

   Persist maps with no overwrite, fsync them, then persist/fsync seals and their directory before prefix access. Reuse [Events](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:582) for reserved INVALID terminals, with a separate supervisor receipt for timeout, OOM and write/sync failure. Evaluate all 128 covers before a completed REJECT decision.

The smallest independent synthetic falsifiers should be:

| Check | Minimal falsifier |
|---|---|
| Accumulated affinity | With test capacity 3 and weights `w01=5, w02=4, w03=3, w13=3`, require first pack `[0,1,3]`. A strongest-single-edge implementation incorrectly chooses 2. Also exercise ties, zero affinity, incoming-only edges and upper-layer exclusion. |
| Tail handling | `N=673`: make the one-row tail the seed’s strongest neighbor. Require it last, with 42 full groups in the first pack and the tail in a second pack. |
| Map direction | Use non-self-inverse `old_to_new=[1,2,0]`; check boundary ordinals and complete round trips. |
| Actual range cap | Select groups `0,3,…,1497`: 500 groups cost **12,330,240 bytes at 256 ranges**, but **17,921,280 at 32**. Require REJECT at 32. |
| Decoder admission | Reuse the existing three-row, 17-layer allocation-inflation fixture; test invalid degrees and identity substitutions. Encoded size alone must not admit it. |
| Planner admission | Use a small hub graph and deliberately insufficient adjacency/heap budgets. Require rejection before exceeding charged capacities. |
| Prefix isolation | Append a forbidden suffix to a valid synthetic prefix; instrument read offsets to prove zero suffix reads. Test truncation, tampering, FIFO and symlink rejection. |
| Durability and receipts | Preserve existing map/seal files on rerun; inject map/seal/directory sync failures and a supervisor timeout. Require INVALID and no premature prefix opening. |

These are proposed checks, not executed results.

I would retain this as **one fixed arm with no consumed-panel tuning**. A byte failure rejects this packing; authentication, admission or timeout failures are INVALID. Production insertion, maintenance, 100M residency, cold-total requests and vendor comparisons should not gate this isolated diagnostic. They remain outside what a survivor could establish.

