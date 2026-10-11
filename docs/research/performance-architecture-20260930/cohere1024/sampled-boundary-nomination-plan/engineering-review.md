**NO-GO for implementing the plan unchanged.** The sampled-row nomination arm is reasonable to falsify, but its edge semantics, gate, and resource admission need correction first.

I reviewed the [completed plan](/data/target/borsuk-boundary-adjacency-plan.pending/planning-answer.md), the committed screen evidence, and source objects at `3b70a8e3b863cca220ce1c15df5ac363f9e0aec3`. Source line references below refer to that revision, not the stale checkout.

**Must fix:**

1. **Remove the unsupported zero-loss requirement.**  
   Plan line 19 rejects any lost slot. The frozen screen requires candidate coverage ≥315/320 and `gained > lost`; `compare_native_replay.rs:5315` implements that rule. A candidate gaining two slots and losing one passes the existing gate but fails the proposed gate.

   Zero loss could be a separately justified prospective policy, but neither the layer attribution nor preserving 48 leaves establishes its necessity. Smallest repair: retain the existing gate, report gains/losses, and leave returned-recall qualification separate.

2. **Preserve the precise coverage metric.**  
   Plan line 19 says “owning nominated units,” but the historical 314/320 counts **SOURCE-scored page closure**. `coverage_slot` tests `page_closure.contains(physical / 256)` at `compare_native_replay.rs:5308`; `loss_coverage` requires every unit in that closure to be scored at lines 4674–4681.

   These differ: selecting unit 0 brings physical page 0 into closure, so unit 1 is SOURCE-scored even if its owning leaf was not selected. A fetched gap-bridge page is a third category.

   Smallest repair: retain the historical closure-based gate and distinguish direct owning-unit nomination only where needed to explain recovered slots. Do not reuse 314 as the baseline for a different metric. This distinction does **not** invalidate the established contrapositive proof that the six unscored targets’ leaves were absent.

3. **Freeze what an edge count means before choosing diversity.**  
   `build_hnsw_adjacency` explicitly selects `diverse=false` at `centroid_hnsw.rs:419–426`. Its result is a pruned insertion graph, **not each sampled row’s final nearest-32 list**. Reciprocal insertions are pruned independently at lines 1381–1417, so the graph is directed.

   The diverse sibling deliberately retains some farther candidates; its test selects nodes `[1,3]` over the nearer pair `[1,2]` at lines 1547–1568. Counting its stored edges measures a different prior. Conversely, nondiverse pruning does not guarantee accurate neighbor discovery or connectivity. Historical Fable evidence implicates the old discovery path; it does not establish that changing this flag improves this Cohere arm.

   **Smallest sound choice:** retain the proposed nondiverse builder for this single arm, but define weights as **counts of retained directed cross-leaf base arcs**. Extract base arcs from `tower.last()`, not index zero—tower storage is reversed (`:1216–1223`). Count each stored directed arc once, exclude same-leaf arcs, then retain top 16 by count/leaf ID.

   Disconnected components are a recall limitation here, not automatically an invalid artifact: every node’s adjacency can still be projected. Freeze empty-adjacency fallback to L2 and test it. Do not silently use `build_reachable_*`: its repair adds ordinal-cycle and physical-order edges (`:838–875`), which would contaminate geometric votes.

   Nearest-neighbor counts do not intrinsically require nondiverse navigation: diverse discovery followed by distance-ranked neighbor extraction is possible, but that is additional work and a different arm. Neither shortcut should be presented as qualified kNN.

4. **Make authentication and decoding an explicit pre-query boundary.**  
   Reusing the named functions is insufficient without changing their sequencing. `coverage_router` validates calibration but discards it (`compare_native_replay.rs:5088–5123`); the existing `loss_positions` runs **after truth opens** (`:5492–5519`).

   Construction must first authenticate the entire SQ8 stream and physical-ID order. Its records are `[i64 logical ID][f32 squared norm][D code bytes]`, not just codes (`sq8_source.rs:258–281`). Sample index `u` must map to physical unit `u`; never use logical ID divided by 32 as membership.

   Require exact length/EOF, duplicate/range checks under the example’s ordinal-ID contract, finite calibration and reconstructed coordinates, nonzero normalization, dimension agreement, and final file-identity checks before publishing adjacency. Bind its version and digest to generation, calibration, router, membership, SQ8, order, sampling rule, and algorithm parameters; include that digest in the paired selection seal.

   Keep ordinal-only IDs and filesystem/manifest handling in the example. The library should operate on admitted geometry and unit membership, without imposing benchmark ID or dataset conventions.

5. **Replace the resource estimate with an enforceable allocation model.**  
   The 122 MiB sample-vector payload is correct. “Graph and edge-count scratch should keep construction below 256 MiB” is not yet admission evidence.

   The builder uses nested vectors with spare capacities, levels, insertion order, visited marks, heaps, and pruning scratch (`centroid_hnsw.rs:113–175,440–450,1294–1337`). Projection, decoded-row copies, authenticated metadata, and serialization also need accounting. Search width 64 bounds retained results, not all traversal work or total distance evaluations.

   The current screen already reports a conservative modeled payload of **164,435,968 bytes**. Adding samples to that unchanged model exceeds 256 MiB; this does not prove actual RSS failure, but it requires explicit stage lifetimes and a replacement model. Drop samples and the row graph before retaining query selections.

   Also distinguish serialized adjacency bytes from resident capacity. `128 × leaf_count` describes 16 `(u32 ID,u32 weight)` entries; offsets, containers, query scratch, and simultaneous generation pins remain additional costs.

**Sampling and voting risks should remain disclosed, not trigger another arm search.** One fixed midpoint per unit can systematically miss a boundary elsewhere in that unit. Larger leaves contribute more sampled rows; raw cut counts therefore favor larger or denser leaves. The short final unit receives one sample too. Rank weights provide no distance-margin information.

Freeze these choices unchanged for the falsifier. Specify zero-based seed ranks giving weights 40…1, exclude the first 40 leaves from replacement candidates, deduplicate voted leaves, and fill shortages in original L2 order. The 640-link bound applies only to adjacency expansion; full L2 ranking and nomination still run.

The smallest implementation remains two files: generic adjacency construction/selection and tests in `semantic_unit_router.rs`; bounded authenticated IO, the distinct candidate entrypoint, and existing seal/control/cover orchestration in `compare_native_replay.rs`. Keep the HNSW helper private. No centroid hierarchy, new diagnostic framework, graph-repair mechanism, or serving-generation integration is needed for this screen.

**Required native falsifier and provisional envelope:**

| Stage | Required boundary |
|---|---|
| Remote build/tests | Causality EC2; two compiler jobs, 16 GiB RAM, 64 GiB build disk |
| Adjacency construction | One CPU, 512 MiB enforced memory ceiling, swap zero, 900-second deadline; checked allocation model before allocation |
| Staging | 2 GiB only after accounting for every staged file; SQ8 alone is 1,036,000,000 bytes |
| Query screen | Existing 48-leaf, SOURCE 128-GET/64-MiB and SQ8 limits; preserve separate screen resource controls |
| Lifecycle | Spot, exact instance/source/config receipts, terminal artifact sync, interrupted cells discarded, immediate compute cleanup |

Before measurement:

- Run affected library/example tests on EC2, including nonidentity order, cross-generation substitution, corrupt records, short tails, zero/nonfinite vectors, D1024 acceptance/D1025 rejection, reversed tower indexing, directed-edge counting, ties, isolated components, and exactly 48 distinct selections.
- Pass source-bound admission on authenticated real inputs and a separate disposable CLI/publication/cleanup smoke. Synthetic fixtures alone are insufficient.
- Run required workspace Clippy and `scripts/check_rust_test_build.sh` on the exact delivered revision.
- Construct and seal adjacency with requests/truth unopened. Then seal paired nominations before truth access. Reproduce original returned313/320 and SOURCE coverage314/320; apply candidate≥315 and gained>lost.
- A survivor proceeds to actual native returned-recall qualification with unchanged scoring and fetch limits. The coverage reducer explicitly does not call native ANN. Setup/protocol failures remain INVALID, distinct from a completed weak candidate or a demonstrated construction-resource limit.

The inspected inventory’s missing qualified FP32 graph does not establish global absence; the generation metadata even describes a canonical FP32 object. Its availability and qualification were not checked.

The six nomination misses justify testing this mechanism. They do not predict success; q537 remains an unexplained covered ranking loss. Neither outcome qualifies full-FP32 kNN, Fable’s layout proposal, cold performance, or a vendor win.

No files changed. No builds, data parsing, network/cloud actions, or child agents were used. Runtime correctness and the proposed resource envelope remain unverified.
