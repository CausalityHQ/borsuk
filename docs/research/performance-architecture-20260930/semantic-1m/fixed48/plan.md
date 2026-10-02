**Preregister fixed top48 as the sole routing intervention, followed by one production SOURCE/SQ8 decomposition on a new fixed CoHere64 panel.** It is worth a bounded offline test; it is not ready for a cold campaign.

The hypothesis is specific: extending the unchanged normalized-f32, ordered-f64 squared-distance ranking from 32 to 48 leaves may recover discovery misses. No evidence currently establishes that it reaches 95% returned recall.

1. **Freeze the intervention and its cost envelope before selecting queries.**

   The authenticated nomination body is 966,714 bytes, SHA256 `407985018740bc7341d8288b766b9e4887961e27e261cff8522765891159c024`. Its footprints give the following arithmetic; the 48-column is a **1.5× planning projection**, not a measurement or bound.

   | Per query | Archived top32 mean | Top48 projection |
   |---|---:|---:|
   | Selected leaf bytes | 2,282,424 | 3,423,637 |
   | Original nominated units | 1,482.1 | 2,223.1 |
   | Physical closure pages | 230.8 | 346.2 |
   | Contiguous SOURCE runs | 38.4 | 57.5 |
   | SOURCE bytes before gap reads | 11,816,000 | 17,724,000 |
   | SQ8 bytes for the entire closure, before gaps | 46,082,400 | 69,123,600 |

   D768 SOURCE records are **200 bytes**; SQ8 records are **780 bytes**. SOURCE covers all closure pages and bridges the smallest gaps only when necessary to meet its GET budget.

   Proposed prospective allowances:

   - Exactly 48 leaves; maximum payload **4,730,880 bytes**, derived from `48 × 64 × 1540`.
   - Maximum **3,072 original units**, **3,079 seed-completed walk units**.
   - Maximum **512 closure pages**; reject excess before SOURCE reads, without truncating nomination. This rounded envelope exceeds the projected historical maximum of 433.5 pages.
   - Maximum **4,096 SOURCE-scored units**, sufficient to complete those 512 pages.
   - Retain SOURCE **64 MiB / 128 GETs**, SQ8 **16,773,120 bytes / 32 GETs**, and concurrency **16 per stage** for this first falsifier. These are this protocol’s budgets, not universal product limits.

   Cold query payload accounting would allow at most **208 logical GETs**: 48 leaf + 128 SOURCE + 32 SQ8, excluding startup. Physical attempts, latency and bills remain unknown.

   Update the existing memory model consistently: leaf buffers become `2 × 4,730,880 + 1 MiB = 10,510,336` bytes per active query; the authenticated root’s existing `32×` charge is **72,570,880 bytes**. Charge enlarged trace arrays and caller pins, then require modeled admission within the scorer’s prospective **512 MiB** envelope. That establishes admission arithmetic, not RSS. Preparation’s historical 7,932,358,656-byte peak likewise establishes no serving-memory result.

2. **Implement only the necessary production seams.**

   In [semantic_unit_router.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs:926), retain the distance computation and leaf-ID tie break; give `Fresh1m` fixed48 selection and matching `validate_selected`/`seed_walk` bounds.

   In [two_bit_generation.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1280), align local and remote leaf admission, `LEAF_BYTES`, memory accounting, SOURCE walk validation, completion capacity and `TwoBitPlanTrace::scratch_bytes`. **Changing only the 1,031 router guard is insufficient:** `rank_walked_source_with_limit` separately rejects walks over **1,272 units**, and completion currently stops at **2,544**. Apply the new bounds specifically to semantic discovery.

   Reuse the existing scorer binary and APIs:

   - `plan_two_bit_source_cover`: truth-free SOURCE range-cost falsifier.
   - `plan_two_bit_source_walks`: existing SOURCE ranking and SQ8 admission.
   - `diagnostic_search_with_store`: one complete search with nomination, SOURCE, admission and returned-result traces.
   - [check_semantic_router_scorer.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_semantic_router_scorer.rs:233): already uses `ChunkedStore<LocalFileSystem>` and freezes all 64 results before opening truth.

   Adapt the existing coverage checker, selector and preparation helper with new report/config identities. Keep historical artifacts immutable. Preserve physical formats, membership, centroids, source order and SQ8 encoding; perform no training or generation reconstruction.

3. **Close source authority before any data or compute launch.**

   Require authenticated bodies for the retained generation root, router root/membership/leaves, SOURCE metadata/records, SQ8 page manifest/digests/object and source-order permutation. Manifest hashes alone are insufficient. If required retained bodies are unavailable, **STOP at artifact admission**; rebuilding is outside this protocol.

   Resolve the supplied qualified-source label `a345399` to its full inventory and receipts—it did not resolve as a local Git revision here. Qualify the new routing/scorer revision and binaries explicitly. Keep `cb63` publication qualification separate; its pending gate cannot qualify this routing change.

   The minimal new evidence set is one preregistration/config, panel authority/freeze, nomination seal, source/binary authority, scorer transcript, resource record and final closure.

4. **Select and freeze one genuinely fresh panel.**

   Reuse [select_cohere_fresh64_coverage.py](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/select_cohere_fresh64_coverage.py:234), extending its authenticated exclusions to **both** prior fresh64 panels plus the existing consumed-query ledger and FIRST1M exclusion. From the original eligible population, excluding those distinct 128 locators leaves **8,996,872** candidates.

   Freeze seed text `borsuk-cohere-first1m-fixed48-routing-fresh64-v1`; retain SHA256-to-big-endian seed derivation and the existing sampling/order rule under a pinned Python version. Seal exactly 64 locators before vector extraction. Audit raw and normalized duplicates against indexed, historical and both consumed-panel identities; stop the entire panel on mismatch or duplicate, with no replacement.

   Declare fixed48 and all thresholds before selection. Do not evaluate alternative widths or revisit the consumed64 truth.

5. **Run the bounded falsifier once, then decide.**

   First extend existing synthetic checks to prove deterministic top48 selection, complete unit retention, seed completion, rejection of excess payload/pages, and acceptance through the shared SOURCE scorer of walks over 1,272 units. Include the maximum 3,079-unit walk and complete closure scoring within 4,096 units.

   Before handoff, compile/run affected Rust targets and record exact revision/status for:

   ```bash
   cargo test --locked -p borsuk --lib semantic_unit_router::
   cargo test --locked -p borsuk --lib source_walk_tests
   cargo test --locked -p borsuk --bin check_semantic_router_scorer
   cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
   bash scripts/check_rust_test_build.sh
   ```

   After root authorization, prepare the new queries, seal all nominations before the sole GT100 construction, and run **one scorer pass** through authenticated local files. Freeze outputs before evaluation. Record @10/@100 membership through:

   `nominated units → page closure → SOURCE-scored units → SOURCE-ranked pages → SQ8-admitted ranges → returned IDs`.

   Ordered decisions:

   - **FAIL** on authority, duplicate, resource, identity or execution failure.
   - **Discovery FAIL** below **608/640 page-closure hits@10**.
   - **Downstream FAIL** if discovery passes but returned hits@10 remain below **608/640**; identify the first loss-producing stage.
   - **GO to a separately preregistered cold gate** only with all64 completed, returned recall ≥95%, authenticated closure and resource compliance.

The likely downstream bottleneck is SQ8 admission: its current clamp permits at most **84 full pages**, versus approximately 346 projected closure pages. Exhaustive closure retrieval cannot fit that budget, although a successful shortlist could still attain 95%; the one-pass decomposition decides this. If it fails through page admission, the single alternative is to rank pages by their best existing FP16 unit-centroid distance within the fetched leaves and nominate a fixed affordable page set before SOURCE. Test that separately on another fresh panel; do not silently enlarge I/O budgets.

Preserve the [top32 FAIL and qualification limits](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage/a0002/decision.md), and the ReLAION whole-pair FAIL despite viable 97.5% candidate cells at cold p90 455.282/448.433 ms. Dated vendor numbers remain comparison context. No CoHere cold result, new QPS result or vendor win follows.

Read-only inspection only. Shared HEAD advanced to `76f84b88` during inspection; the changes concern publication qualification, and the routing files inspected above are unchanged from requested `d3872586`. Root retains design, freeze and launch authority.
