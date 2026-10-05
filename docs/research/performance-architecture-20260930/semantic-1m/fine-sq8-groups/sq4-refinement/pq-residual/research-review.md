# Research review

Consultation `b448c9d8b3b84794`, group `651b4ee78c01498d`; completed exit0. Opus attempt failed authentication; the audited fallback GPT-6.1 Sol completed this review. Reviewed e3569b82 with f98c03f7 documentation clarification observed. No native execution qualification.

**Retain the single PQ-residual hypothesis, but do not freeze it for real execution yet.** The main confirmed defect is the description of the native SQ8 score. The remaining work is to freeze numerical policies and demonstrate binding, budget, and closure behavior.

Reviewed `e3569b821b97813c26d6078b03039a6229a80356`. During review, documentation-only commit `f98c03f7` clarified cohort-only output and DP-only reuse; those address two ambiguities in the requested revision. The inspected Rust and evidence files remained unchanged. I made no edits or executions.

**Research findings**

- **The closed evidence supports changing refinement.** Corrected four-bit fails both panels while same-fetched SQ8 passes. This localizes the principal additional loss to compressed refinement; it does not establish radial error as the cause. CoHere’s nominee coverage of `.95609375` also rules out nominee-only refinement under the unchanged `.98` gate. Preserve eligibility of every incidental fetched row. [Closed decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/decision.md), [nomination audit](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/nomination-ceiling-audit.json)

- **Residual refinement has precedent, not transferred qualification.** Jégou et al. refine an existing reconstruction with quantized residuals. Their residual product quantizer, reconstructed-distance estimator, and nearest-neighbor rank metric differ from this per-axis four-bit, source-norm estimator and set recall@100. Their results support testing the mechanism, not predicting BORSUK’s recall. [Primary paper, §§3–4](https://arxiv.org/pdf/1102.3828)

- **The selected alternative is defensible within this fixed arm.** Exact SQ8 reranking adds a dependent fetch and requires unmeasured shortlist retention. Direct SQ8 under a larger range envelope changes the scientific protocol. Neither is a required addition to this probe. Likewise, the local stage profile establishes no residual CPU advantage and supplies no serving p95 or QPS requirement. [Stage-profile limitations](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/local-stage-profile.json)

**Engineering findings: required before execution**

1. **Correct the scoring contract while retaining the proposed score.** The method says the native SQ8 core omits query norm. It actually accumulates `qnorm`, subtracts `qnorm / 2` from `shift`, and evaluates `norm - 2*(inner + shift)`. [Actual scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:87)

   Keep the candidate’s `original_norm - 2*ordered_dot(q, xhat)`, but describe it as a deliberately different reduction. The stated ideal error `-2 q·(rhat-r)` applies against the **constant-stripped ideal target**; against the native ideal expression it also contains `-||q||²`. Neither establishes f32 equivalence.

   Freeze separate f32 decode multiplication/addition, residual subtraction, predictor-plus-center addition, ascending-coordinate dot accumulation, final subtraction, and `(score.total_cmp, ID)` ordering. Do not replace this with separate predictor/residual dot sums, subspace sums, FMA, or navigation scores. Keep the native control unchanged.

2. **Finish the residual trainer’s numerical specification.** “Freeze policies” remains an outstanding prerequisite in the method. Specify the residual-to-bin expression, endpoint treatment, rounding, constant-axis behavior, bin-center conversion to stored residual centers, and nearest-center ties.

   Reuse the existing weighted DP’s checked integer moments, f64 costs, cancellation allowance, smallest-predecessor ties, and `min(16, occupied)` behavior. Its optimization concerns binned values, not exact residual distortion or recall. [DP](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4633)

   The existing encoder maps original SQ8 bins and **recomputes the norm**; it cannot encode this arm. The `f98c03f7` clarification correctly requires actual-residual center selection and copying the original four norm bytes. [Existing encoder](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4456)

3. **Freeze anchor-query provenance and preserve original physical addresses.** Keep the specified ordinal sampling and anchor indices. Explicitly define anchor queries from the original decoded SQ8 anchor, followed by unchanged native preparation, and bind their bits. That preparation uses f64 normalization and preserves near-unit input unchanged within its `1e-6` squared-norm tolerance. [Preparation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/sq8_source.rs:22)

   Seal logical ordinals, mapped physical rows, IDs, and anchor identities. Cohort slot `j` must never become PQ physical row `j`. Raw reconstruction must use floor subspace boundaries and padded book stride; cosine navigation scores apply normalization and cannot substitute for reconstruction. [PQ representation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/pq64_nominee.rs:80)

4. **Demonstrate binding and cumulative admission, beyond matching geometry.** Validate the new root against the independently pinned original root, PQ digest, order, records/groups, coefficient bits, norm convention, trainer/bin policies, centers, and compiled source closure. Equal dimensions or 396-byte rows do not authorize another codec or predictor.

   Budget the entire two-panel run, including PQ encoded/decoded coexistence and inverse-norm construction, all source passes and closure rereads, histogram/DP scratch, center searches, scoring, sorting, serialization, and retained outputs. A fresh per-helper guard must not reset the 20B-work or 600-second allowance. Insufficient admission must fail before the corresponding body read.

   Two full residual payloads require **79,200,000 bytes**, exceeding 64 MiB. `f98c03f7` correctly limits this probe to cohort payloads totaling **3,244,032 bytes** before other outputs. Preserve that restriction only for the mechanism probe.

5. **Make closure failure dominate numerical success.** Freeze and authenticate complete results before reduction; verify 100 unique, cohort-contained results with self excluded. Preserve exclusive output creation, partial-write accounting, sync failures, and late `INVALID`. A copied success report needs the original matching exit/resource/drain/cleanup receipt. The existing pipeline provides patterns, not proof that the new path inherits them. [Closure and terminal handling](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:6289)

**Independent falsifiers**

- **Scalar:** independently decode PQ/SQ8 and packed residuals; cover `D=1,3,65`, padded-word sentinels, nonunit and near-unit queries, constant residuals, center ties, odd-nibble padding, copied norm bits, and near-tie rankings. Exact residual encoding must not assume `fl(p + fl(x-p)) == x`.
- **Pipeline:** independently recount sealed overlaps; test same-shape wrong PQ/order/root bindings, mutation between passes, truncated/grown bodies, prebody budget refusal, overwrite refusal, partial-write/sync failure, and late second-panel failure. Track opens to prove requests and GT remain unopened throughout.

Keep the gates unchanged: **6336/6400 and fourth-smallest overlap ≥98 separately per panel**, then scientific execution only after survival and qualification, with **6272/6400, fourth-smallest hits ≥95, and 32 ranges/16 MiB**. Preserve every completed REJECT.

Even survival would establish only a partial numerical mechanism. The approximately 59 GB conservative generation residency and `4N` query workspace remain unresolved at 100M; product completion and competitor advantage remain unmeasured.
