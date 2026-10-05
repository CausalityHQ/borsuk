# Corrected codec method engineering review

Consultation `b32f9378aa694a07`, group `fe7709b005174a8a`, GPT-6 Astra, completed exit 0. Specification review; the worker source was not reviewed. Required findings are not native execution evidence.

**Do not freeze this method yet.** The estimator is coherent, but the numerical contract and diagnostic integration need the fixes below. These are specification findings and reuse hazards—not findings against the unseen OWN4 implementation.

Reviewed HEAD `128a27266091945b65e93a67d683194cd977c7d9`; tracked files remained unchanged.

1. **[Research, P2 — definite error] The score-error bounds omit a factor of two.**

   [Research specification, lines 65 and 87](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/research-result.md:65) describes cosine error as score error. For the declared distance:
   \[
   \widehat d-d=-2v^\top e,\qquad
   |\Delta d_{\lambda}|=2|\lambda_{32}-\lambda|\,|v^\top z|.
   \]
   The existing expression is correct for **cosine**, not distance. Separate this serialization term from reciprocal/dot/normalization error and the final `f32` distance rounding. The real-arithmetic rotation bound likewise needs multiplication by two when expressed in distance units.

   **Smallest falsifier:** an independently decoded row whose correction is not exactly representable in `f32`; verify cosine and distance errors separately, including self-query and antipode.

2. **[Research, P1 — unresolved contract] “Process equal thresholds deterministically” does not fully specify the encoder.**

   [Line 73](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/research-result.md:73) leaves event ordering, objective ties, signed zeros and reconstruction of the winning state undecided. Reconstructing solely from the winning floating-point scale can produce different codes from those whose objective won.

   Freeze the initial-state evaluation, event ordering, zero sign, objective tie rule and reconstruction rule. Retaining and replaying the winning event prefix avoids another rounding decision. Recompute the correction from the final codes.

   For **truly equal** thresholds \(t\), a subset of transitions changes dot and squared norm by \(s/(2t)\) and \(s\). Thus the objective is
   \[
   (A+s/(2t))/\sqrt{B+s},
   \]
   whose maximum over that interval occurs at an endpoint. Whole-group processing is therefore sufficient; exponential subset enumeration is unnecessary. However, equal rounded divisions do not establish mathematically equal thresholds. Ordinary ordered `f64` arithmetic also does not establish an exact-real argmax guarantee. The method’s search argument comes from [Extended RaBitQ §3.2](https://arxiv.org/html/2409.09913v1#S3.SS2); its finite implementation needs an explicit numerical contract.

   **Smallest falsifier:** exhaustive \(16^3=4,096\) codes for several tiny integer source directions. Compare positive-dot squared alignment using exact integer cross-products. Include equal magnitudes, rational threshold coincidences, zeros, mixed signs and adjacent floating-point inputs. Compare optimal objective separately from canonical tie selection.

3. **[Research/engineering, P1] V23’s rotation validator cannot certify the proposed bound.**

   [V23 validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/v23_rabitq_quantizer.rs:57) checks individual **row** Gram entries against `1e-5`. The proposal requires a conservative **matrix infinity norm** for \(R^\top R-I\), including computation error, at `1e-10`. Replacing the constant alone is incorrect.

   Specify a certificate that sums absolute defects plus conservative arithmetic-error allowances, including the certificate’s own summation error. Distinguish the real-arithmetic matrix defect from subsequent floating-point matrix-vector multiplication and normalization errors.

   Freeze Gaussian generation, matrix orientation, orthogonalization/sign convention, singular handling and seed. V23’s positive-norm Gram–Schmidt construction is a reasonable starting point; substituting arbitrary QR requires the appropriate diagonal-sign convention. Orthogonality alone does not prove Haar sampling. See [Mezzadri’s construction](https://arxiv.org/pdf/math-ph/0609050).

   **Smallest falsifier:** \(R=I+2\cdot10^{-11}\mathbf1\mathbf1^\top\) at D4. Individual Gram defects are approximately \(4\cdot10^{-11}\), but their row sum exceeds `1e-10`; it must fail. Also test persisted-bit round trips, a nonsymmetric rotation to expose transpose mistakes, nonfinite entries and truncation.

4. **[Engineering, P1] Direct V23 reuse introduces unadmitted cubic work and large stack allocations.**

   [V23 encoding](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/v23_rabitq_quantizer.rs:175) validates the full matrix on every row. At D768, its triangular Gram check performs **226,787,328 dot-product terms per validation**. Repeating that for both 100k panels adds approximately **45.36 trillion terms**, beyond the intended **117,964,800,000 transformation MACs**.

   Validate once per admitted immutable matrix and reuse that validated state. Charge construction, numerical validation and each actual reopen explicitly. [V23’s stack arrays](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/v23_rabitq_quantizer.rs:94) must also become admitted heap allocations: one D768 `f64` matrix already occupies 4.5 MiB.

   The [existing configuration validator](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:2867) rejects operation caps above 20 billion, while existing transcoding accounting charges essentially linear row work. Give the new arm its own explicit admission and counters; preserve historical limits. Include matrix construction/validation, both panels, up to **1,075,200,000 threshold events**, sorting, query transforms, controls and authentication. Check deadlines within bounded work chunks.

   **Smallest falsifier:** a tiny counted run proving that additional rows add transformation/encoding work without repeating Gram validation; insufficient work or memory admission must fail before matrix/source bodies are consumed.

5. **[Engineering, P1] Aggregate memory and output admission must change with the new control and matrix.**

   The [current allocation model](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:2915) does not establish admission for rotation construction, serialization/reopen coexistence, retained matrix state and the extra control. Its [output model](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4900) explicitly counts two candidate-population rosters and three top-100 outputs. Adding decoded cosine changes that accounting unless the serialized format shares those rosters.

   Admit the maximum **simultaneously live** capacities across both panels and all stages, including retained results and terminal failure output. Independent successful component checks do not establish aggregate admission.

   Keep the distinct-object startup ledger, plus actual read/attempt counts. Sharing one matrix permits one retained allocation; it does not erase repeated reads. The worst payload plus a 64-byte-header matrix is already **21,344,320 bytes**, before other startup objects.

   **Smallest falsifier:** every component fits individually but their coexistence exceeds the cap. Reject before the corresponding body read/allocation. Exercise short writes and failed syncs without publishing a successful generation.

6. **[Engineering, P1] Equal row widths make codec confusion particularly dangerous.**

   Histogram SQ4 and corrected SQ4 both use 396-byte rows. A positive finite correction can pass a norm-field check and produce plausible, incorrect scores. The [existing packed scorer dispatch](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4715) chooses histogram scoring or SQ4 expansion; neither understands this estimator.

   Require explicit codec/root dispatch. Bind matrix bits and orientation, source coefficients/order, payload and group hashes, numerical policies, configuration and source closure. The [manually enumerated source closure](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:2750) must include the new reusable codec.

   Prepared queries must retain matching rotation/semantics authority; plans must retain query, generation and mutation identity. Preserve streamed per-group authentication, row-ID checks, whole-source digest and exact EOF. Keep corrected rows out of the existing SQ8 snapshot API. Production update/compaction qualification can remain deferred if the diagnostic explicitly declares it unsupported.

   **Smallest falsifier:** substitute another valid same-size rotation, swap valid roots/groups, replay a prepared query under different rotation authority, and feed corrected rows to an old-codec root. Reject before ranking. Also retain source-growth, final-group corruption and unpublished-output tests.

7. **[Engineering/research, P1] Nominee retention is weaker than the required frozen-population comparison.**

   The [existing planner](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4656) recomputes covers and checks nominee retention plus containment of the original256 cover. Those checks can pass after incidental fetched rows change.

   Authenticate and compare the complete ordered ordinal intervals against the closed histogram populations, then translate by the 396-byte stride. All three required scorers must consume exactly those rows.

   Preserve the original query preparation once, including its [near-unit shortcut](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/sq8_source.rs:22). Decode source coordinates with separate ordered `f32` multiplication/addition before widening. The SQ8-to-cosine-control difference includes arithmetic changes as well as normalization; do not attribute every changed rank solely to normalization.

   **Smallest pipeline falsifier:** extend the existing tiny two-panel/all128 fixture with the cosine control. A changed incidental interval must fail despite retaining every nominee. Assert that both payloads and rotation precede request access, and all 128 complete results precede truth access. Preserve `INVALID` for execution defects and `REJECT` for valid scientific failures.

The closed histogram **REJECT remains intact**. Required acceptance remains **6,272/6,400 hits per dataset, fourth-smallest hit count ≥95, ≤32 payload ranges and ≤16 MiB payload bytes**. Survival would establish only this consumed-panel diagnostic—not cold latency, QPS, lifecycle readiness or a vendor win.

No edits, Cargo/native execution, corpus/query/truth reads, AWS actions or children were performed.

