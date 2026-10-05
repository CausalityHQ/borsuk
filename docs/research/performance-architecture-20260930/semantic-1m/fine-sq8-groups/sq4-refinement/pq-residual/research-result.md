**Recommend one fixed, one-wave refinement change: encode four-bit residuals relative to the PQ64 reconstruction already resident for each row.** Keep nomination, physical order, fetched populations and the `.98 mean / 95 p05` quality gates unchanged. Advance only through a cheap native falsifier; this is an unqualified hypothesis.

I resolved the requested revision to `f387cb2da919df5eebdfc2a88e06a30f170e9f32`. During inspection, HEAD advanced to `5b69581a5ef7714273f99514fa6816191b66ba8d`, adding two evidence audits. The inspected Rust APIs remained unchanged. I made no edits or executions.

**Established facts and their implication**

The corrected codec is a valid scientific REJECT. Passing same-population SQ8 and decoded-cosine controls localize the principal additional loss to compressed refinement, rather than insufficient fetched coverage. They do not establish that radial error caused the loss. [Closed decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/decision.md)

The new nomination audit strengthens the warning against reducing nomination or overfetch: CoHere’s 1,024 nominees contain only `.95609375` mean truth coverage, with p05 **87**. Incidental fetched rows supply **254** true hits in the returned SQ8 rankings. Restricting refinement to nominees cannot pass this panel, even with perfect subsequent scoring. [Nomination audit](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/nomination-ceiling-audit.json)

The authenticated local scoring profile also gives no CPU advantage to corrected four-bit: its scoring-stage p95 is **43.80/35.92 ms**, versus SQ8’s **38.32/31.80 ms**. These stages include reads, authentication, allocations and sorting; they do not identify a SIMD bottleneck or measure S3 serving latency. [Stage profile](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/local-stage-profile.json)

**Comparison of the three routes**

| Route | Assessment |
|---|---|
| **Compressed top-256, then exact SQ8 rerank** | Directly tests recovery, but adds a dependent fetch stage, substantial requests and another persistent representation. Top-256 retention remains unmeasured. |
| **Resident PQ64 prediction + four-bit residual refinement** | Selected. Uses information already in RAM, preserves the 396-byte row and one payload wave, and avoids another dense rotation. Accuracy is unmeasured. |
| **SQ8 with fewer nominees or less overfetch** | Unsupported by the CoHere nomination ceiling. A distinct 128-range SQ8 envelope is a credible control, but does not justify trimming candidates. |

For the rerank alternative, with shortlist size \(M=256\), SQ8 width \(w_8=780\) and authentication groups of \(g=16\):

\[
F_2\le M=256,\qquad B_2\le gMw_8=3,194,880\text{ B}.
\]

Combined with the largest compressed cover, payload could reach **19,820,544 B**, with up to **288 payload GETs**. At the current 32-request concurrency, that is as many as nine request batches. Raising second-stage concurrency to 128 permits three nominal batches, subject to separate transport admission; it does not establish three measured RTTs. S3 cannot combine disjoint byte ranges into one GET. [AWS GetObject documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

A fair future comparison must include direct SQ8 under a separately preregistered 128-range envelope. Closed arithmetic already shows every consumed plan fits 16 MiB there, with mean bytes **6,342,570/8,938,800**. That could avoid the rerank dependency entirely at sufficient admitted concurrency. Its returned recall and physical performance remain unmeasured. [Cover arithmetic](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/frozen-cover-arithmetic.json)

**The selected change**

For each original SQ8 row, decode \(x\) using the existing ordered `f32` multiplication/addition. Obtain predictor \(p\) from that physical row’s **existing PQ64 codewords and books**, without normalizing the reconstructed predictor. Encode:

\[
r=x-p,\qquad \widehat x=p+\widehat r.
\]

Use one source-trained, per-coordinate 16-center residual book. Freeze a simple trainer: authenticate residual extrema, accumulate 256-bin residual histograms, reuse the existing weighted contiguous-partition DP, then encode each actual residual against the resulting 16 centers. Constant coordinates use one center. No bit-width, seed, rotation or clustering sweep.

The proposed row is:

```text
i64 ID | original SQ8 f32 norm | 384 packed residual bytes
```

Keep the **source SQ8 norm**, and explicitly change the scoring convention:

\[
\widehat s(q,x)=n_{\mathrm{SQ8}}+\|q\|^2-2q^\top(p+\widehat r).
\]

In ideal arithmetic, against the same source-norm SQ8 target,

\[
\widehat s-s=-2q^\top(\widehat r-r).
\]

This removes reconstructed-norm error from this estimator. It leaves directional residual error and numerical rounding, both requiring measurement. It is a new estimator, not ordinary squared distance to \(\widehat x\).

**Hypothesis:** predicting from the already-paid 64-byte PQ representation makes the remaining residual easier to encode accurately than the entire vector. This is distinct from histogram whole-vector SQ4 and corrected directional SQ4. The residual-refinement mechanism has primary precedent in [Jégou et al., §3](https://arxiv.org/pdf/1102.3828); their representation, dimensions and recall metric do not establish a BORSUK numeric result.

The Rust seams are narrow:

- [Pq64Codes](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/pq64_nominee.rs:85) already exposes books and codes within the crate. Use raw codeword reconstruction; its cosine navigation scorer normalizes the predictor.
- [fine_sq8_groups](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:4161) supplies authenticated construction and the histogram DP machinery.
- [returned_sq8](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:117) remains the unchanged numerical control. Its scorer cannot interpret residual rows.
- [corrected_four_bit](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/corrected_four_bit.rs:307) remains rejected historical research.

Introduce a distinct codec/root marker binding PQ bytes, physical order, original SQ8 coefficients and norm semantics, residual centers, payload hashes, query preparation and source revision. Equal row width must never permit old-codec dispatch.

**First falsifier: one bounded, truth-free native source-neighborhood probe**

Before another scale experiment, preregister one local probe:

1. Run a tiny synthetic scorer/codec oracle covering zero and exact residuals, nonunit queries, source-norm semantics, ties, odd dimensions, wrong PQ/order bindings and corruption.
2. On authenticated FIRST100k source artifacts, train once without requests or truth. Select **4,096 logical IDs per panel by a fixed source-ID rule**, then 64 fixed source anchors. Normalize anchor queries through the existing query preparation, exclude self, and rank the cohort with unchanged native SQ8 and the candidate.
3. Freeze the selection and all numerical policies beforehand. Requests and GT remain unopened throughout. Proposed ceilings: CPU1, 1 GiB, no swap, 600 seconds, 20-billion counted operations; validate aggregate admission before execution.

**Exact probe survival condition, separately on both panels:** candidate top-100 overlap with native SQ8 top-100 totals **at least 6,336/6,400**, and the fourth-smallest overlap is **at least 98**. All authentication, numeric-oracle and resource checks must pass. This is a prospective mechanism gate, not measured ANN recall. Smaller local populations make survival insufficient to predict full-population success.

If it fails, stop this residual arm. Do not widen bits or select another residual trainer on the same diagnostic.

After survival, qualify the exact Rust revision and run the original 128-query paired test on **identical fetched populations, including incidental rows**. Scientific acceptance remains:

- Each panel: **≥6,272/6,400 truth hits**, fourth-smallest hit count **≥95**.
- Every plan: all nominees retained, **≤32 payload ranges**, **≤16,777,216 payload bytes**.
- Complete authenticated source, result, supervisor and cleanup receipts.

Retain unchanged SQ8 and decoded-cosine controls. Execution defects are INVALID; completed scientific failure preserves REJECT. Consumed-panel survival permits subsequent qualification, not a fresh-quality or vendor claim.

**Payload, RAM, startup and lifecycle arithmetic**

Let \(N\) be rows, \(D=768\), \(g=\lceil N/16\rceil\), \(E\) encoded graph bytes, \(C\) active queries and \(T\) fetched rows.

\[
B_q=396T.
\]

The existing worst cover stays **16,625,664 B**, leaving **151,552 B**. There is no additional per-row field. Six-bit rows would cost **24,686,592 B** on that population; even one extra four-byte scalar exceeds 16 MiB. Shared residual books belong in separately charged startup.

With resident metadata and one-attempt reads:

\[
F_q\le32,\qquad
W_{\mathrm{payload}}=\lceil F_q/P\rceil=1\quad(P\ge32).
\]

There is no remote graph chase. Total cold requests and bytes still include startup, metadata and attempts; one payload wave does not certify the full cold boundary.

Serving-generation storage is approximately:

\[
S=396N+64N+8N+32g+E+\text{books/headers}.
\]

At 100M: **47.0 GB + \(E\)**. Retaining original SQ8 adds **78 GB**; canonical FP32 records add approximately **308 GB**. Those source-retention costs must remain visible.

Current conservative residency remains approximately:

\[
R=G_{\mathrm{heap}}+68N+8N+32g+786,432
  +2\text{ MiB}+O(16D),
\]

with \(G_{\mathrm{heap}}\le512N\): roughly **59.003 GB per distinct generation** at 100M. Residual refinement does not solve graph residency.

A conservative direct-scoring query allowance is:

\[
Q=4N+3B+8\text{ MiB}
  +256\min(N,\lfloor B/396\rfloor)+O(D),
\]

approximately **469.57 MB per active query** at 100M. Admit:

\[
M_{\mathrm{peak}}
=R_{\mathrm{unique\ pins}}+\max(CQ,M_{\mathrm{open}})
 +M_\Delta+M_{\mathrm{maintenance}}+M_{\mathrm{runtime}}.
\]

Current opening transients include encoded graph/PQ coexistence and row-sized scratch. Keep the existing 100k guard until those stages qualify; these formulas are projections, not 100M RSS evidence.

Residual construction adds **\(O(ND)\)** work using existing PQ assignments, rather than another dense \(O(ND^2)\) transform. Three SQ8 source passes at 100M read approximately **234 GB**, then write **39.6 GB** of payload. Existing PQ/graph construction costs remain.

Updates require matching PQ assignments, residual books and visibility revision. Unchanged-state compaction can copy encoded rows; changing predictors or books requires original source. Existing mutations support replacements/deletes, while durable new-ID insertion remains incomplete.

Preserve the uniform, histogram and corrected codec REJECTs, the graph-affinity packing REJECT, and the earlier routing failures. This proposal changes refinement information while retaining their passing discovery control. Its prospective gain is high recall within one payload wave and the existing row budget; runtime, startup, update throughput and total lifecycle gains still require native evidence.
