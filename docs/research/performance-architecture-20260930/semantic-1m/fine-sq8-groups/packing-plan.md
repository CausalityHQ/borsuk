Consultation `24306d4e129f41be`, completed exit 0. Read-only planning result; proposals remain unimplemented and unqualified.

**Select one source-graph packing experiment: reorder the existing 16-row SQ8 groups while preserving the frozen PQ64/HNSW router exactly.** Advance only to an offline falsifier; current evidence does not establish a layout meeting the product envelope.

Reviewed HEAD `221ec93c7e666197f7c8d8218a47dc8d97da0d58`. The inspected router, PQ, fine-layout and diagnostic Rust files match the campaign’s frozen source hashes. No files changed, large bodies opened, ANN/Cargo run, or cloud work launched.

The causal evidence is clear: nomination now supports high quality, while physical scatter prevents 32-range admission. Exact covering in the current order fits only **50/64 ReLAION and 32/64 CoHere** queries within 16 MiB. Raising the range cap would preserve the historical result but would not solve the product requirement. [Cover arithmetic](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/frozen-cover-arithmetic.json)

| Candidate | Decision |
|---|---|
| **Single-copy graph-affinity packing of fine groups** | Test next. Changes locality alone; no additional vectors, nomination loss or remote graph traversal. |
| Source-built overlapping posting objects, addressed through resident row-to-object memberships | Defer. Multiple placements could reduce boundary scatter, but require replication, a cover-selection algorithm and greater maintenance cost. With replication factor \(r\), SQ8 alone costs \(780rN\) bytes; neither \(r=2\) nor bounded object size guarantees a 32-GET cover. Existing centroid-based object routing would also change nomination. |

V240 already rejected reverse Cuthill–McKee ordering: its 256-row-page lower bound worsened. The proposed test therefore uses **weighted group affinity and a byte-derived packing capacity**, not another graph traversal permutation. This remains a hypothesis; HNSW adjacency need not predict joint nomination well. [V240 closeout](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v240-graph-local-order-100k-closeout.md)

1. **Freeze one deterministic packing rule.**

   Treat each original 16-row group as indivisible. Weight two groups by the number of authenticated base-layer graph edges between their rows; ignore upper layers. Build packs by starting with the lowest unassigned group ID, then repeatedly adding the unassigned group with greatest total affinity to the current pack; break ties by original group ID. Use the lowest remaining ID when affinity is zero. No refinement sweep.

   Capacity is **42 groups**, derived from the envelope:

   \[
   w=768+12=780,\quad p=16w=12{,}480,\quad
   L=\left\lfloor\frac{16\,\mathrm{MiB}}{32p}\right\rfloor=42.
   \]

   One full pack is **524,160 bytes**; 32 cost **16,773,120 bytes**. Concatenate packs into one range-readable object, retaining 16-row authentication and no padding. Keep a short final group last.

   Packs guide placement; queries still use exact smallest-gap covering over selected fine groups. Covering ≤32 packs is sufficient, not necessary. Preserve every frozen nominee; refuse an infeasible plan.

2. **Keep the Rust change narrow.**

   - [fine_sq8_groups.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:648): packing, authenticated group permutation, ordinal translation, revised admission and v2 root binding.
   - [resident_vector_graph.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/resident_vector_graph.rs:989): expose validated base-edge iteration for construction. Leave graph IDs, adjacency, entry point, PQ codes and nomination budgets unchanged.
   - [hierarchical_semantic_cells.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:82): one thin pack-and-replay diagnostic.

   Persist `old_group → new_group` as `u32`; derive and validate its inverse when opening. Translate fetched positions back through the inverse before checking logical IDs. The v2 root must bind the unchanged router identity separately from the new SQ8 permutation, object and group hashes. Reject incompatible serving roots.

   Reuse `cover_pages`, authentication, normalized SQ8 ranking and snapshot handling. **Preserved nominees do not guarantee preserved returned recall:** the current ranker scores incidental fetched rows too.

3. **Use these checked resource formulas.**

   Let \(g=\lceil N/16\rceil\), \(E\) be encoded graph bytes and \(K=786{,}432\) codebook bytes at D768.

   \[
   S=780N+64N+8N+36g+E+K+\text{headers/root}
   \]

   This includes records, PQ codes, existing logical-ID map, hashes and one persisted permutation. At 100M: **85.425 GB + encoded graph + fixed metadata**, excluding canonical originals and retained generations.

   Resident admission, including both permutation directions:

   \[
   R=G_{\rm heap}+68N+8N+40g+K+2\,\mathrm{MiB}.
   \]

   Under the existing \(G_{\rm heap}\le512N\) allowance, this projects **59.052884 GB at 100M**. Packing adds only **50 MB**, but does not solve router residency.

   The current query allowance, including epoch marks, is:

   \[
   Q=4N+3B+8\,\mathrm{MiB}
      +256\min(N,\lfloor B/780\rfloor)+4D,
   \quad B=16\,\mathrm{MiB}.
   \]

   At 100M, \(Q=\) **464,229,632 bytes per active query**. Admit unique pinned allocations, concurrent queries, opening transients, deltas, maintenance and runtime together. These are arithmetic projections: current code rejects \(N>100k\). **Keep that guard** until construction, opening and lifecycle residency qualify.

   For total requests, enforce \(H+F+A\le32\): metadata/recovery GETs \(H\), payload ranges \(F\), and additional attempts \(A\). Resident metadata and one-attempt execution can make \(H=A=0\) during serving; 32 payload ranges alone do not establish a cold-total-GET result.

4. **Run the smallest falsifier before rewriting SQ8.**

   Proposed later diagnostic: CPU1, 256 MiB, no swap, five-minute deadline, datasets sequentially. Use bounded sparse group adjacency; charge graph decoding, both adjacency directions and planner capacities before allocation.

   First authenticate the closed original graph and construct/seal the permutation **before opening nominees**. Then read only the authenticated **1,665,668-byte nomination prefix** of the original trace, whose seal records SHA-256 `7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025`. Requests, truth, SQ8 payloads and the remaining trace stay unopened. [Nomination seal](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/screen/fine/measurement/paired-fine.fine-seal.json)

   Replay all 128 frozen nominee sets through the permutation and existing exact cover operation. Verify bijection and complete nominee preservation. **Any query exceeding 16 MiB at 32 ranges falsifies this packing.** Report maximum and per-panel pass counts; no tuning on these panels. Authentication, timeout or resource failures are INVALID. Survival is only a necessary locality result.

5. **Require recall and maintenance gates next.**

   After falsifier survival, stream-copy complete groups and independently verify unchanged row bytes and IDs. Freeze both panels’ new plans and returned results before truth access. Require each panel’s returned mean R@100 ≥98%, p05 index 3 ≥95, and every query within the full request/byte envelope. Confirm identical native nominees and report containment, fetched coverage and returned recall separately. Fresh queries follow consumed-panel survival.

   Deletes/replacements can retain the existing pinned snapshot path. **New-ID inserts are currently unsupported**; production needs an admitted durable delta with query-visible candidates, replayable recovery and backpressure at its limit. Avoid in-place pack edits. Publish immutable replacement artifacts before switching the head; retain old artifacts until readers release their pins.

   Layout compaction reads and rewrites approximately \(780N\) payload bytes plus metadata; graph/PQ artifacts can remain shared when unchanged. New-source graph rebuilding has separate memory and maintenance costs. Test interrupted publication and old-reader recovery before promotion.

The main risks are insufficient graph locality, loss of helpful incidental rows, and unchanged graph/build residency. A passing packing replay warrants the next native recall gate—not 100M, cold-S3, QPS or vendor qualification.
