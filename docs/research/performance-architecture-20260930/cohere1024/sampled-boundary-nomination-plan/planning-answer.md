Recommend **one sampled SQ8 row boundary-adjacency arm over the existing semantic leaves**, keeping the 48-leaf budget and physical layout fixed. Plan against Git revision `3b70a8e3b863cca220ce1c15df5ac363f9e0aec3`.

The six SOURCE coverage omissions are established leaf-nomination losses. Fable’s full FP32 row-kNN layout needs prerequisites absent from the inspected screen inventory: authenticated canonical FP32 rows and a same-source row-neighbor graph, or a qualified graph build. The inventory already admits SQ8 rows, calibration and membership, so the smaller arm below is executable without obtaining another vector representation.

1. **Construct adjacency without queries or truth.** Stream and authenticate the existing SQ8 object, generation, plane, router root and membership. Verify physical-ID order against `native_order_sha256`. Select one actual reconstructed row per 32-row unit at physical position `min(32u+16, N−1)`; decode with authenticated `low/step`, normalize, and label it with `membership[u]`. Admit dimensions explicitly within the existing 1–1024 ceiling.

2. **Project sampled row edges onto leaves.** Reuse `centroid_hnsw::build_hnsw_adjacency` with fixed `m=16`, `m0=32`, construction/search width 64. Despite the helper’s name, its inputs here are individual reconstructed rows. Count cross-leaf base edges; retain each leaf’s top 16 neighbors by count, ties by leaf ID. Drop sampled vectors and the row graph after construction. This is approximate adjacency over sampled SQ8 rows—not full-source exact kNN.

3. **Replace nomination once, without a sweep.** Keep the first 40 leaves from the original L2 ranking. Fill eight positions from their adjacency using descending votes  
   `Σ (40 − seed_rank) × cut_edge_count`.  
   Break ties by original L2 rank, then leaf ID; fill shortages from the remaining original ranking. Pass the 48 distinct IDs through existing `nominate_selected`. SOURCE closure, scoring and cover limits stay fixed.

4. **Limit the implementation to two files for this screen.**
   - `crates/borsuk/src/semantic_unit_router.rs`: generic adjacency construction, authenticated decoding and boundary selection, with tests in the existing module. Use a separate versioned adjacency artifact bound to generation/root/membership/SQ8/order identities.
   - `crates/borsuk/examples/compare_native_replay.rs`: bounded streaming construction and a distinct candidate entrypoint, reusing `coverage_router`, producer-control parity, selection sealing, `loss_positions` and the existing cover planner. Preserve historical cosine-arm behavior and receipts.

**Bounded falsifier:** On causality Spot EC2, first test deterministic edges/ties, duplicate and corrupt identities, short final units, and dimension rejection. Then perform source-bound admission and a separate disposable smoke. Freeze the single arm, construct adjacency with requests/truth unopened, and durably seal paired original/candidate nominations before opening truth.

Require the original control to reproduce **returned313/320 and SOURCE coverage314/320**. Reject this arm if candidate SOURCE-scored coverage is below **315/320**, or if it loses any previously covered slot. Count owning nominated units, not fetched gap bridges. Protocol/setup errors are INVALID; preserve them separately. No parameter sweep follows rejection. Survival only licenses returned-recall qualification.

**Proposed envelopes, not measurements:** 31,250 sampled D1024 f32 rows occupy approximately **122 MiB**; graph and edge-count scratch should keep modeled construction below **256 MiB**. Preregister CPU1, RSS512 MiB, swap0, a 900-second construction deadline and 2 GiB staging disk. Adjacency costs at most `128 × leaf_count` bytes plus offsets/header—roughly **0.1 MB** for this root. Query expansion examines at most 640 links. Separately budget remote compilation at two compiler jobs, 16 GiB RAM and 64 GiB build disk; run affected targets, required workspace Clippy and `scripts/check_rust_test_build.sh` before handoff.

This differs from rejected centroid hierarchies because boundary evidence comes from individual rows, with no new mean hierarchy or row replication. Sampling can miss rare boundaries; cut counts can favor large leaves; replacements can scatter closure pages and exceed existing cover caps. Historical ReLAION scores provide no Cohere prediction.

Git/source reads only; no edits, native execution, data parsing, network, cloud actions or children.
A failed screen rejects this sampled SQ8 arm, not every FP32 boundary-adjacency design; survival establishes no latency or vendor win.
