**Verdict: minimal repairs first, then GO for the bounded example-only implementation.** The diagnostic can measure source-neighborhood fragmentation. Its current interpretation overstates what one anchor per unit can establish.

I read all three pending documents fully and inspected the connected source using `git show e359f0a33661107f55861af9a17d5fe58f56d432`. Source references below refer to that revision.

1. **BLOCKER — The proposed unit-resolution inference is not supported by these measurements.**

   [The interpretation](/data/target/borsuk-post-boundary-next-arm.pending/decision.pending.md:19) says locality below 32-row units warns that unit-only partitioning lacks resolution. A coherent unit can have all 31 other members among an anchor’s nearest neighbors; regrouping whole units preserves that locality perfectly. Conversely, one sampled row cannot reveal conflicting neighborhoods among its 31 siblings. With self excluded, top32 necessarily contains at least one external-unit row.

   **Smallest fix:** Replace that inference with “unit indivisibility remains unresolved.” Retain per-anchor counts jointly classified by own-unit/own-leaf membership and target-leaf rank, plus distinct target units. Treat the 32 anchors—not their 1,024 correlated neighbor slots—as the sampling units. Source anchors also participate in the fitted partition; their ranks are not held-out routing evidence.

   **Required falsifier:** A coherent-unit fixture with 31 internal neighbors must not trigger “insufficient unit resolution.” A fixture whose cross-leaf neighbors are all inside top48 must not gain positive fragmentation evidence merely because other, same-leaf neighbors fall outside top48. Freeze an explicit *inconclusive* outcome; no larger survey is needed.

2. **BLOCKER — Normalization and residual arithmetic are still insufficiently specified.**

   [The admission wording](/data/target/borsuk-post-boundary-next-arm.pending/decision.pending.md:9) requires finite, nonzero rows, while the residual definitions remain names rather than formulas. Arbitrarily scaling a row preserves cosine neighborhoods but changes its Euclidean prototype ranks and residuals.

   Canonical construction uses approximately unit-length, unrotated coordinates. Stored unit centers average `low + code * step` without dividing reconstructed rows by their SQ8 norms; leaf prototypes are **unweighted means of unit centers**, without normalization. Zero centers are valid. See [canonical normalization](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/sq8_source.rs:22), [unit construction](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/unit_centroid_pages.rs:127), and [leaf construction](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs:712).

   **Smallest fix:** Specify:
   - Canonical norm admission consistent with the writer’s `1e-6` squared-norm tolerance, while retaining actual norms in cosine denominators.
   - Exact scalar accumulation/division/clamping order and signed-zero/tie treatment.
   - Squared Euclidean residuals in the unchanged coordinate space, using decoded stored centers and persisted leaf prototypes.
   - No center renormalization, second calibration transform, or row-weighted replacement leaf mean.

   **Required falsifier:** Near-unit FP32 rounding passes; materially rescaled canonical rows fail admission. Zero centers remain valid. Partial-unit and unequal-unit-population fixtures distinguish the persisted unweighted prototype from a row-weighted mean.

3. **BLOCKER TO ENVELOPE FREEZE — The resource table is not a coexistence budget.**

   [The proposed envelope](/data/target/borsuk-post-boundary-next-arm.pending/decision.pending.md:15) is plausible, but its phase admission is not yet concrete. At 1M/D1024, decoding can temporarily retain **64,000,032 bytes of encoded centroids alongside 128,125,000 bytes of decoded arrays**. The decoder allocates the latter while borrowing the former. [Evidence](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/unit_centroid_pages.rs:158).

   Scratch likewise depends on staging: two canonical copies plus SQ8 already exceed 8 GiB. That combination is unnecessary, but the launch contract must exclude or charge it.

   **Smallest fix:** Add one checked phase table covering authentication, decoding, anchor sealing, neighborhood scanning and publication. Include router copies, ID bitset, buffers, heap capacities, serialization/readback, retained snapshots, allocator allowance and charged file cache. Separately enumerate staged files and temporary-copy lifetimes. Do not reuse the old coverage allocation formula unchanged.

   **Required falsifier:** Overflow and one-byte-under-budget cases refuse before large-body reads. Root-owned remote admission verifies actual staging occupancy and charged memory. A correctly enforced timeout/resource failure rejects this envelope; it does not disprove partition geometry.

   I found **no source-level proof that 1 GiB is inherently insufficient**, and make no prediction that 1,200 seconds will suffice.

4. **BLOCKER TO RESULT ACCEPTANCE — The publication helper does not eliminate success-looking files after abrupt death.**

   The reusable helper writes the report before directory sync and final input rechecks. Its invalidation runs only when execution reaches the error handler. SIGKILL, OOM or Spot interruption can bypass it, leaving `"complete": true` bytes. Existing tests exercise returned errors and mutations, not that crash window. [Write/check sequence](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:5395), [cleanup](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:5352).

   **Smallest fix:** Explicitly require the existing root-owned successful process/terminal receipt, bound to the report and anchor-seal hashes, before accepting any result. A report alone is insufficient. This needs no new controller feature.

   **Required falsifier:** Terminate after report bytes are written but before final publication checks. The surviving artifact must remain unaccepted, with interruption recorded as INVALID.

One additional implementation trap needs an explicit acceptance test: **`coverage_router` is metadata admission, not canonical-body authentication.** Its canonical check verifies geometry and digest syntax, not a supplied canonical body’s identity. [Evidence](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:5098). Bind the streamed body’s SHA/length to the generation’s canonical descriptor, hash physical-order IDs against the ordinal-order authority, and retain descriptor/path checks through publication. Do not equate the canonical digest with `plane.source_sha256`; their byte layouts differ. Reject an independently valid but wrong-generation canonical body before neighborhood computation.

The historical reconciliation and stored-versus-resident corrections are sound. Preserve both KILLs. These repairs authorize only implementation and subsequent root-owned qualification—not an ANN arm, serving adoption, cold-quality claim or 100M feasibility claim.

**Not checked:** compilation, tests, native execution, data, mocks or network. No files were edited and no children were launched.
