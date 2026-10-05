# Corrected codec method research review

Consultation `68dd3fc48e6a4deb`, group `fe7709b005174a8a`, effective GPT-6.1 Sol after Opus authentication failure, completed exit 0. Specification review, not native execution evidence.

**Verdict: the cosine estimator is mathematically coherent, but the encoder and numerical admission contract need repairs before freeze.** Proceed with the single source-only codec implementation; native qualification remains pending.

Reviewed revision: `128a27266091945b65e93a67d683194cd977c7d9`. The worktree later advanced to `69923c5f7cca94337c4e469e0ce03611e3980a82`; the inspected Rust files remained unchanged. Its documentation amendment fixes several ambiguities, but its threshold-equality rule is still insufficient. No files edited, Cargo/native runs, corpus/query/truth reads, AWS actions, or children.

1. **Research — one correction scalar is sufficient for this declared cosine target.**

   For unit \(u\), let \(A=u^\top z>0\). Then
   \[
   \lambda z=u+e,\qquad u^\top e=0,\qquad
   \widehat{\cos}-u^\top v=v^\top e.
   \]
   Cancelling the code norm is valid; neither another norm field nor renormalizing \(\lambda z\) is appropriate. This matches the denominator estimator in [Extended RaBitQ §3.3](https://arxiv.org/html/2409.09913v1#S3.SS3).

   The remaining directional error is
   \[
   \|e\|=\sqrt{\|z\|^2/A^2-1}.
   \]
   Maximizing alignment minimizes this quantity, but does not guarantee preservation of rank-100 margins.

   Preserve the exact source convention in [the proposal](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/research-result.md:24): separate `f32` multiplication/addition when decoding original SQ8, followed by ordered `f64` normalization. The cosine control must consume the same frozen query-preparation output and decoded rows. The [existing SQ8 scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:88) must remain unchanged.

   This is one combined codec intervention involving normalization, rotation, encoding and correction. Recovery would establish its combined effect; it would not establish that radial error caused the histogram failure.

2. **Engineering — rounded quotient equality cannot define exact critical-threshold ties.**

   A concrete counterexample:
   \[
   a=3/8+2^{-53},\qquad b=5/8+2^{-52}.
   \]
   Binary64 division produces
   ```
   3/a == 5/b == 7.999999999999997
   ```
   although
   \[
   3b-5a=2^{-53}>0.
   \]
   These are different mathematical transitions. The later amendment’s “exact equality of finite `f64` values” can therefore merge distinct thresholds even without epsilon grouping.

   **Required fix:** order \(k/|u_i|\) through an exact ratio comparison of the represented inputs, or another explicitly justified procedure that preserves distinct transitions. With \(k\le7\), significand/exponent comparisons provide a bounded approach. Fix coordinate ordering for genuine ties.

   Also retain the winning **event state**, rather than reconstructing it by multiplying by a rounded winning threshold. Evaluate the initial state, all relevant transitions and the saturated endpoint. Recompute the winning code’s denominator directly before serializing its correction.

3. **Research — genuine simultaneous transitions can be batched; exponential tie enumeration is unnecessary.**

   My derivation: at a common exact threshold \(t\), an increment from \(k-\tfrac12\) to \(k+\tfrac12\) contributes
   \[
   \Delta A=|u_i|=k/t,\qquad \Delta B=2k=2t\Delta A.
   \]
   Across a partial batch, alignment is
   \[
   F(s)=\frac{A+s}{\sqrt{B+2ts}},\qquad
   F'(s)=\frac{B-tA+ts}{(B+2ts)^{3/2}}.
   \]
   Its derivative can cross only from negative to positive. Thus an interior partial batch cannot beat both endpoints.

   This supports batching **true** equal thresholds. It does not justify batching the rounded collisions above.

   Freeze zero-coordinate signs, signed-zero treatment and objective ties. Distinguish exhaustive candidate coverage from a claim of an exactly computed real-arithmetic argmax: incremental `f64` objective accumulation needs a qualified error bound and independent direct checks.

4. **Engineering — the rotation certificate needs an implementable conservative definition.**

   The stated bound
   \[
   |\cos(Rx,Rq)-\cos(x,q)|\le\frac{2\delta}{1-\delta}
   \]
   is valid for ideal normalization when \(\delta\) conservatively bounds \(\|R^\top R-I\|_\infty<1\).

   **Required fix:** certify the complete row sum, including Gram-dot, diagonal-subtraction and row-sum roundoff. A possible certificate bounds each exact Gram residual by its measured residual plus a justified dot-product error allowance, then sums with conservative rounding.

   [V23 validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/v23_rabitq_quantizer.rs:57) checks individual entries of an `f32` matrix. It does not implement this certificate. Validate once when admitting immutable rotation state, then borrow that state during encoding and query preparation.

   Specify matrix orientation, Gaussian generation, singularity handling and the orthogonalization/sign convention. V23’s two-pass Gram–Schmidt is a reasonable starting mechanism; a substituted QR routine must preserve the appropriate sign convention. [Mezzadri](https://arxiv.org/html/math-ph/0609050v2) explains why arbitrary QR output is insufficient.

   Orthogonality establishes geometric fidelity, not Haar distribution. One persisted seeded matrix cannot support an unconditional finite-implementation unbiasedness claim. Freeze the seed/construction before requests and prohibit recall-based seed selection.

5. **Engineering — report distance error, not only cosine error.**

   For the declared distance, correction serialization contributes
   \[
   2|\lambda_{32}-\lambda|\,|v^\top z|.
   \]
   The later amendment fixes this factor of two. The transformed-cosine defect bound likewise becomes \(4\delta/(1-\delta)\) when expressed as distance error.

   Separately account for normalization, matrix-vector accumulation, denominator computation, packed-dot computation and final `f32` casting. Floating normalization also means the stored numerical \(u\) need not satisfy \(u^\top e=0\) exactly.

   Keep finite-score rejection, unclamped estimates, `total_cmp` and ID ordering. Do not promise exact self-distance zero after correction serialization.

6. **Engineering — freeze the authenticated population and complete binding chain.**

   [The existing planner](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4656) recomputes a cover from nominees. The corrected arm must preserve the closed histogram arm’s exact ordinal intervals, including incidental rows. Either consume their authenticated intervals or prove exact equality against them before scoring; nominee containment alone is insufficient.

   The new root must bind original records and coefficient bits, physical order, router identity, codec semantics, rotation bytes, output payload and group hashes, configuration, and compiled source closure. The [current source-identity list](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:2750) must include the new reusable codec module.

   Prepared queries need rotation and normalization-policy identities. Plans additionally bind generation, query and mutation revision. Sharing a prepared query across the two panels is legitimate when their admitted rotation and score semantics match.

   Authenticate each streamed source group before encoding it; verify the complete source digest and exact EOF before publishing the completed payload. Reauthenticate the published closure before truth opens. Source growth or corruption must produce INVALID with a durable partial-output account.

7. **Engineering — admission must cover simultaneous allocations and all construction stages.**

   Required stage models include the retained first panel while constructing the second, matrix construction and serialization coexistence, rotation decoding/validation scratch, encoder events, source groups, plans, prepared queries, controls, ranking buffers and result serialization.

   Individual allocation checks do not establish an aggregate cap. The added cosine control also changes output admission; [the existing calculation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4896) assumes 300 top-k records per result.

   The two panels require **117,964,800,000 dense transform MACs**, before encoding and authentication. Also charge:

   - \(D^3\) matrix construction and Gram validation; adapting V23’s two-pass construction entails approximately **904,790,016 projection/AXPY MAC equivalents**.
   - Up to **1,075,200,000 encoder transition events** across both panels, plus sorting comparisons.
   - Query transforms, controls, hashes, reads and serialization.

   Define counting units explicitly. Preserve host safety limits and obtain a new explicit native-build/runtime admission later; the historical 20-billion-operation allowance is not portable.

8. **Research — payload feasibility survives; total cold feasibility remains unqualified.**

   The maximum payload remains **16,625,664 B**. Adding the proposed matrix object alone yields **21,344,320 B**, before roots, group hashes and router startup.

   Keep both a distinct-object ledger and actual-read/GET counters: sharing one matrix does not erase repeated reads. The histogram book counters are subsets of generation startup counters, as the later documentation correction acknowledges.

   Prior art supports the mechanism, not the acceptance threshold. [EDEN](https://arxiv.org/html/2108.08842v3#S2.SS3) provides related denominator scaling; [TurboQuant](https://arxiv.org/html/2504.19874v1) uses a different base-plus-residual construction, consuming three base bits plus one residual bit at this budget. Neither establishes BORSUK recall@100 or a vendor win. Structured rotations remain a separate unqualified method.

9. **Engineering — keep lifecycle scope explicit.**

   The source-only diagnostic need not implement maintenance now. It must reject stale rotation/query/root combinations and incompatible codec versions.

   [FineSq8Snapshot](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:923) currently interprets replacement rows through SQ8 geometry and norm semantics. It cannot accept corrected rows unchanged. Before a later lifecycle claim, updates must use the declared original source convention, deletes must remove stale versions before ranking, and pinned generations must retain their matching rotation and payload state.

The smallest required falsifiers are:

| Check | Minimum useful coverage |
|---|---|
| **Encoder oracle** | Enumerate all \(16^D\) codes for \(D=1,2,3\), independently evaluating alignment. Include axes, zeros, mixed signs, `[1,2,0]` genuine ties, the rounded-ratio collision above, adjacent-representable thresholds, initial/saturated winners and declared objective ties. |
| **Serialized scorer oracle** | Independently unpack \(h=2c-15\); compare the ideal ratio and persisted estimator \((\lambda_{32}/2)v^\top h\). Cover nonunit sources, positive scaling, antipodes, odd padding, correction rounding and near-tied final scores. |
| **Rotation certificate** | Identity, a nonsymmetric orthogonal matrix to expose transpose mistakes, malformed matrices, and a matrix whose individually small Gram residuals exceed the aggregate row-sum gate. Exercise certificate rounding near the boundary. |
| **Authenticated pipeline** | Two small panels with at least 101 rows, \(D=3\), partial final groups and all 128 result slots. Include an incidental fetched winner, cross-rotation prepared-query rejection, corruption/growth, aggregate cap failure before bodies, partial output, and a late failure that leaves truth unopened. |
| **Later consumed-panel falsifier** | Exact frozen populations; unchanged SQ8, decoded-cosine control and corrected codec reported separately; both payloads sealed before requests and all results sealed before truth. |

Retain **6,272/6,400 hits per dataset**, fourth-smallest hit count **≥95**, **≤32 payload ranges**, **≤16 MiB payload bytes**, and complete terminal/resource receipts. The closed histogram result remains scientific REJECT. No native evidence yet qualifies the corrected candidate.

