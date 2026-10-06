# Fitter engineering review

Group b8e87f76e8fd444d; reviewer3e923eec9cc94beb, GPT-6 Astra. Read-only plan review at2d13d634; native fitter source was not reviewed or executed.

**REPAIR — scoped to the proposed source-only Rust fitter.** The selector is a suitable base, but several mathematical and execution choices need freezing before the next slice can be qualified.

Reviewed `2d13d634` without edits or native/data/network experiments. The selector and `lib.rs` match the recorded native revision. The [verification receipt](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/implementation-gates/a0001/parent-verification.json:3) records seven passing tests and four successful stages; it explicitly excludes ANN qualification and full-workspace test execution.

1. **P1 — Caching inference alone does not repair the work envelope.**

   The [current selector](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:779) additionally charges two `256²` sorts per evaluation. Across the full declared traversal, retaining those charges costs:

   `6,250 × 8 × 256 × 2 rounds × 2 states × 131,072 = 6,710,886,400,000`

   That exceeds the 512B cap even with zero forward-inference cost. This is conditional source arithmetic, not a runtime prediction.

   **Required repair:** prepare the deduplicated candidates and their complete score order once per model/query; filter that order under current occupancy without re-sorting. Reuse the accepted baseline’s coverage, and charge immutable validation once where ownership permits. Account separately for construction and evaluations. “Every receipt” parity in the [plan](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/fitter-native-plan.md:7) must mean identical selection semantics and serving charges—not identical `Work`/`Memory`, which should change.

   **Smallest falsifier:** compare cached and uncached candidates, probability bits, selections, skips, body reservations, actual bytes, visible rows, coverage, dispositions and bindings; independently check their different work ledgers.

2. **P1 — An occupancy change can affect anchors that do not teach the moved group.**

   Caching **before occupancy filtering** is correct. However, evaluating only anchors associated with the moved group would be wrong: moving an unrelated group into an empty, highly ranked label can displace an object containing another anchor’s teachers. Even without an occupancy change, changed row counts can cause underfill.

   The [existing selector](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:806) filters occupancy before selecting and subsequently computes visible rows and coverage. Preserve that complete behavior.

   **Required repair:** initially evaluate every training anchor for each proposal using the cached order. Any later affected-anchor shortcut must include selection and underfill dependencies, plus both groups in a swap.

   **Smallest falsifier:** one-body budget; highest-ranked label initially empty; second-ranked label contains query Q’s teacher. Move a group absent from Q’s teachers into the empty label. Q must lose coverage. Add the reverse transition and a row-underfill case.

3. **P1 — Teaching mass is specified; destination aggregation and traversal are not fully specified.**

   The [plan](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/fitter-native-plan.md:13) requires neighbor-count label mass but describes proposals as aggregating probabilities of anchors whose teachers “include” a group. That admits materially different algorithms.

   Freeze the loss target as:

   \[
   y_{q,\ell}=\frac{\sum_{g:\operatorname{owner}(g)=\ell}w_{q,g}}{100}.
   \]

   Teacher labels outside the serving shortlist must still contribute to training. Do not renormalize over occupied or candidate labels.

   **Required repair:** specify whether destination scores are `Σq w[q,g]·p[q,label]` or an unweighted sum over matching anchors. Also freeze the destination universe, whether the current owner consumes a top-eight slot, first-improvement versus best-improvement versus sequential acceptance, and when swapping is attempted. The existing [full-grid helper](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:902) excludes empty labels, so it cannot silently supply an all-label proposal ranking.

   **Smallest falsifier:** two anchors contributing weights 16 and 1 to one group, with opposing destination preferences; two improving destinations; and two possible swap donors. Assert the exact proposal and acceptance sequence.

4. **P1 — Freeze the complete backward arithmetic, not just its accumulator type.**

   The epsilon clarification in the [plan](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/fitter-native-plan.md:11) is correct:

   \[
   \frac{\partial L}{\partial p_\ell}
   =-\frac{y_\ell}{p_\ell+2^{-24}}.
   \]

   An ordinary unsmoothed cross-entropy shortcut is wrong here. At `p = epsilon`, its probability derivative would be twice the intended magnitude.

   **Required repair:** specify whether intermediate derivatives are f64 or merely accumulated into f64, where epsilon addition/log evaluation occurs, and how derivatives use the saved f32 forward intermediates. Define an analytic backward convention that ignores rounding; infinitesimal differentiation of the actual rounded f32 program is not the intended training rule. Preserve sequential f32 serving inference.

   **Smallest falsifier:** an independent asymmetric scalar oracle covering mixture and both heads, positive/negative/zero ReLU inputs, and a taught probability near epsilon. Supplement with finite differences using perturbations large enough to survive f32 rounding and away from activation boundaries.

5. **P1 — A fitter needs one cumulative admission ledger and explicit rollback ownership.**

   The current [limits](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:35) exclude borrowed storage, and admissions are local to individual calls. Passing the full 512B/8GiB allowance to each helper would not enforce a whole-fit ceiling.

   **Required repair:** charge simultaneous current/candidate parameters, gradients, both prepared-cache generations, queries/teachers, memberships, proposal scratch and retained receipts. Pre-admit growth and count actual capacities. Rejected proposals still consume work. Exhaustion must retain consumed-work evidence and return an incomplete/refused outcome, without exposing a partially updated model as completed.

   Strict checkpoint acceptance is already appropriate: [the evaluator](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:1308) rejects incomplete selection, nonpositive total gain and lower-tail regression. Preserve all three.

   **Smallest falsifier:** a cap sufficient for either checkpoint alone but insufficient for coexistence; exhaustion between candidate construction and acceptance; and a loss-improving checkpoint with unchanged coverage. Verify rollback of parameters, membership, caches and identities.

6. **P2 — Determinism and diagnostic identity need canonical byte definitions.**

   [Initialization](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/native-spec.md:48) leaves the exact domain separator, seed encoding, meaning of “raw input identity,” and parameter-index convention unspecified. Multiple implementations can satisfy the prose and produce different models. The centered integer must mean `i64(u32) − 2³¹`, not reinterpretation as `i32`.

   There is also an identity seam: this fitter has no corpus bodies, while `Membership` requires object digests. The [test helper](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:1392) generates label-only values that remain unchanged when an occupied label’s contents change.

   **Required repair:** publish initialization test vectors and exact minibatch/update ordering. Define membership-derived **diagnostic tokens**, bound to declared source/layout identity, separately from authenticated payload digests. Keep prepared fields immutable and bind evaluation receipts to current membership, snapshot, teachers, budget and requested rows.

   **Smallest falsifier:** repeated fits produce identical parameter and receipt bytes; changing a raw query without changing its normalized direction, one parameter bit, teacher weight, group owner or source identity changes/refuses the appropriate binding.

7. **P1 — The existing gate would not execute the new fitter tests.**

   The [gate script](/home/rb/worktrees/borsuk-prod-ready-v9/scripts/check_budget_object_selector_implementation.sh:46) runs only `budget_object_selector::tests`. Workspace test compilation does not execute `budget_object_fitter::tests`.

   **Required repair:** the new exact-source qualification contract must explicitly run both modules, preserving the seven selector tests and adding the independent backward, cache-parity and complete fitter tests.

   **Smallest falsifier:** make the tiny fitter test non-vacuous. A small grid whose occupied objects all fit under the fifteen-object allowance cannot demonstrate strict coverage improvement. Use a constrained body budget and fixtures exercising actual SGD updates, accepted moves, zero-gain rejection, lower-tail rejection and rollback through the real fitter entry point.

These are contract and qualification repairs, not evidence against learnability. Incremental coverage optimization, SIMD, parallel training and broader lifecycle work can wait. No learned-model quality, recall, resource feasibility on real inputs, or product readiness follows from the existing compilation receipt.
