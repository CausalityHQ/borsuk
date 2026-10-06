# Fitter research review

Group b8e87f76e8fd444d; reviewer42525a05fd3c425d. Requested Opus5.5 fell back to GPT-6.1 Sol after authentication failure. Read-only plan review at2d13d634; no fitter compilation, execution or learned quality evidence.

**REPAIR, scoped to the source-only prepared-inference and fitter slice.** Keep the same Rust child and the three owned paths. The mechanism is suitable for implementation, but several choices need explicit definitions and independent fixtures before qualification.

At `2d13d634`, the selector, `lib.rs`, and cosine helper match their recorded qualification hashes. The [parent receipt](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/implementation-gates/a0001/parent-verification.json:12) supports seven passing selector tests and four successful native stages. It explicitly records no ANN quality/performance qualification. There is no fitted model or recall evidence in this slice.

1. **Prepared inference needs semantic receipt parity and a separate work ledger.**

   Caching every deduplicated candidate and its score **before occupancy filtering** is correct: filtering a frozen score order preserves the uncached occupied order. Caching the existing `occupied_candidates` would lose labels that become occupied later. The current selector filters occupancy before scoring and sorting at [lines 806 onward](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:806).

   “Compare every receipt” needs qualification. Cached and uncached paths must match identities, candidates, probability bits, selected/skipped objects, reserved and actual bytes, visible rows, coverage, and disposition. Their work and allocation receipts should accurately differ; literal equality would defeat the optimization or misreport its cost.

   Prepared inference should bind model and **original query bytes**. Each evaluation must bind current membership, snapshot, teachers, budget and requested rows. Proportional queries with identical normalized inputs still have different raw-query identities. A cache for a rejected fitted model must never serve subsequent moves under the restored model.

   **Smallest falsifier:** an empty high-ranked label becomes occupied and displaces another selected label; cached and uncached semantic receipts remain identical.

2. **Freeze teaching mass and destination traversal explicitly.**

   The label target should be:

   \[
   y_{q,\ell}=\frac{1}{100}
       \sum_{g:\operatorname{owner}(g)=\ell} n_{q,g}.
   \]

   Enforce unique groups, positive integer counts, counts bounded by actual group rows, and total count 100. Rebuild targets from the **current accepted membership** before the next fitting round. Binary “contains a neighbor” labels would change the frozen method. The existing [weight validator](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:439) provides most of this validation, but does not require sum 100.

   The [fitter plan](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/fitter-native-plan.md:13) leaves procedural choices open:

   - Does destination aggregation use counts? The coherent weighted choice is \(S_g(\ell)=\sum_q n_{q,g}p_q(\ell)\).
   - Are destinations ranked across all grid labels, including empty labels? Exclude the current owner explicitly.
   - Does traversal accept the first improving destination, select the best of eight, or permit several accepted moves for one group?
   - When a move cannot fit, which destination group is the smallest eligible swap ordinal?

   Choose one procedure, record it, and test it. These alternatives produce different layouts. Full-grid probabilities for offline proposals need their own charge; they must not become a serving fallback.

   **Smallest falsifier:** two anchors give conflicting destination preferences and unequal neighbor counts. Add two improving destinations so the test also distinguishes first-improvement from best-improvement traversal.

3. **The epsilon clarification is correct; backprop precision remains underspecified.**

   For the stated loss,

   \[
   L=-\sum_\ell y_\ell\ln(p_\ell+\epsilon),
   \qquad
   \frac{\partial L}{\partial p_\ell}
       =-\frac{y_\ell}{p_\ell+\epsilon},
   \quad \epsilon=2^{-24}.
   \]

   Using `max(p, epsilon)`, omitting epsilon in the derivative, or applying the ordinary `p-y` shortcut would implement a different objective.

   With \(p_{ij}=\sum_r\pi_r u_{r,i}v_{r,j}\), differentiation must include all mixture terms. For example,

   \[
   \frac{\partial L}{\partial u_{r,i}}
     =-\sum_j
       \frac{y_{ij}\pi_r v_{r,j}}{p_{ij}+\epsilon}.
   \]

   Apply the mixture and both head softmax Jacobians, then hidden-layer backprop. Preserve ReLU derivative zero at zero.

   Sequential f32 forward execution is a rounded, discontinuous program. Define the intended **analytic backprop approximation using its cached activations**, including where probabilities/products become f64. Do not describe finite differences as exact differentiation of the f32 program. Use an independent smooth f64 reference for calculus checks and appropriately sized perturbations for f32 sanity checks.

   **Smallest falsifier:** asymmetric nonzero scalar inference/backprop covering every parameter family, active/inactive ReLU units, and a low-probability target where epsilon materially changes the gradient. Expected values must not call production backprop helpers.

4. **Inference caching alone does not establish admission under 512 billion operations.**

   A further source-derived counterexample exists. At D768 with 100 unique teacher groups, current per-selection charges include:

   - Weight validation: \(100+100^2=10{,}100\).
   - Identity work: \(8(768)+6(100)+64=6{,}808\).

   These come directly from [validation and identity accounting](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:434). Repeating them for both sides of the complete eight-destination traversal costs:

   \[
   6250\cdot8\cdot256\cdot2\cdot2
   \cdot(10100+6808)
   =865{,}689{,}600{,}000.
   \]

   That exceeds the cap before selection, coverage, gradients or membership construction. This is a permitted worst case, not a prediction of the actual teachers.

   The minimal repair is one-time admission of immutable queries/teachers and their identities, plus retaining the current accepted coverage baseline. Every proposal still needs exact current-state evaluation. Publish a complete cumulative ledger, including initialization, validation, hashing, cache construction, gradients, proposal ranking, rejected proposals and checkpoint evaluation.

   Memory admission must include simultaneous accepted/candidate models, gradients, caches, both membership states, evaluation buffers and retained receipts. Existing [`Limits`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:35) excludes borrowed storage and runtime overhead, so it cannot alone prove an 8GiB process envelope.

   **Smallest falsifier:** individually fitting buffers exceed the cap when coexisting; refusal occurs before commit and preserves the accepted state. Repeat with operation exhaustion and cancellation.

5. **Exact move coverage can be simple, provided occupancy changes receive full treatment.**

   Under unchanged occupancy and frozen maximum-body reservations, selected labels remain unchanged. A move \(g:a\rightarrow b\) then has exact coverage delta

   \[
   \Delta c_q=n_{q,g}
      \bigl[\mathbf1(b\in S_q)-\mathbf1(a\in S_q)\bigr].
   \]

   A swap adds the corresponding term for its second group. Rows, body bytes, selected digests and underfill still need updating.

   When a label becomes occupied or empty, selection can change for anchors with **no teacher weight on either moved group**. Restricting reevaluation to those two groups’ teacher anchors would be wrong.

   Preserve the current [acceptance rule](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:1308): both selections complete, strict total gain, and no lower-tail regression. It also means an incomplete baseline cannot be repaired through an accepted checkpoint under this rule; return that refusal explicitly. For 256 observations the rank is 13; smaller library fixtures require a defined rank convention.

   **Smallest falsifier:** occupancy changes displace a useful object for an unrelated anchor. Also test a second accepted move against the updated baseline, and total gain accompanied by a 13th-smallest regression.

6. **Distinguish diagnostic membership identities from authenticated body digests.**

   This child has no corpus bodies, so it cannot calculate new SQ8 payload digests after reassignment. The selector currently requires caller-provided digests and includes them in [membership identity](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/budget_object_selector.rs:305).

   Define deterministic, domain-separated **diagnostic layout identities** for this pure fitter, explicitly unauthenticated as payload hashes. The later adapter owns actual body construction/authentication. Preserve source/layout/coefficient pins and ordered training-input identity in the fitting result.

   Initialization also needs exact domain bytes, seed encoding, the meaning of “raw input identity,” and parameter indexing across bias slots. Center LEu32 by integer subtraction of \(2^{31}\), rather than an int32 reinterpretation.

   **Smallest falsifier:** fixed initialization golden bits; independent mutations of model, raw query, teacher count, ownership and source/layout/coefficient pins; complete rollback restores the prior model, membership and applicable cache.

The smallest sufficient test bundle is the three requested families: **independent scalar forward/backward**, **prepared versus uncached selection/move/checkpoint parity**, and **an actual tiny two-round fitter**. The last must execute SGD, acceptance and rejection, restore rejected model/cache state, and reproduce final parameter bits, membership and decisions on repetition. Exercising only the existing checkpoint evaluator would leave training and rollback untested.

The repository’s [recorded prior-art comparison](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/research-review.md:107) supports alternating learning/reassignment and neighbor-weighted routing as mechanisms. It supplies no convergence or quality guarantee for this combination. Each query’s probability matrix has nonnegative rank at most four; cross-entropy improvement need not improve truncated serving coverage. Strict acceptance makes accepted training coverage monotone, but establishes neither optimality nor held generalization.

Incremental coverage indexes, histograms, SIMD and parallel gradients are optional after the complete ledger identifies a need. Preserve the frozen method and qualify this slice with independent Rust fixtures and exact-source native gates before real-input admission.

No files edited; no native tests, corpus/GT access or network experiments performed.
