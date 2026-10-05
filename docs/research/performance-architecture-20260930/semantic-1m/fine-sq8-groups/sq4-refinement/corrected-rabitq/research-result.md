# Corrected four-bit codec research

Consultation `3d0be5e7408f47c1`, terminal exit 0, 773 seconds. Effective provider GPT-6.1 Sol after Fable and Opus authentication failures. Prospective research and specification only; no native tests or measured candidate quality.

**Recommend one four-bit Extended RaBitQ codec with a per-vector cosine correction, followed by one matched native falsifier.** Keep nomination, physical order, fetched populations and quality gates frozen.

```text
i64 ID | f32 cosine correction | 384 packed code bytes = 396 B at D768
```

The correction **replaces** the squared-norm field. It does not accompany it.

The [closed decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/histogram-codebook/native-diagnostic/decision.md), a0001 parent verification and terminal report establish:

| Mean recall@100 / p05 hits | ReLAION | CoHere |
|---|---:|---:|
| Same-population SQ8 | .99515625 / 99 | .9903125 / 97 |
| Histogram SQ4 | .958125 / 91 | .93359375 / 89 |
| Uniform SQ4 | .93078125 / 88 | .89046875 / 84 |
| Fetched-truth coverage | .99984375 | .99625 |

All 128 payload envelopes passed. The remaining histogram-to-SQ8 losses are **3.703125 and 5.671875 percentage points**. This supports another scoring intervention; neither improved SSE nor adequate fetched coverage establishes recall preservation.

**Declare the source and target precisely.** For this increment, encode authenticated original SQ8 reconstructions, retaining the existing source adapter:

\[
x_j=\operatorname{fl}_{32}(\mathrm{low}_j+b_j\mathrm{step}_j).
\]

Decode with the existing multiplication/addition order, then normalize in `f64`. Do not encode histogram reconstructions or silently switch to canonical FP32 inputs.

Today’s [SQ8 scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:88) uses reconstructed squared-L2 distance after query preparation; it does **not** normalize decoded database vectors. Therefore retain that scorer unchanged and separately report a full-precision **decoded-SQ8 cosine control**. This distinguishes normalization effects from quantization error without introducing another candidate method.

For nonzero vectors, define \(N(a)=a/\|a\|_2\), and use one shared orthogonal transform \(R\):

\[
u=N(RN(x)),\qquad v=N(RN(q)).
\]

Here \(q\) contains the frozen query-preparation output. Additional normalization belongs only to the new cosine scorer and its control.

Store codes \(c_j\in\{0,\ldots,15\}\), representing

\[
z_j=c_j-7.5.
\]

The corrected estimator is

\[
\lambda=\frac{1}{u^\top z},\qquad
\widehat{\cos}(x,q)=\lambda\,v^\top z,\qquad
\widehat d=2-2\widehat{\cos}(x,q).
\]

Indeed, with \(\bar z=z/\|z\|\), the RaBitQ ratio
\((v^\top\bar z)/(u^\top\bar z)\) cancels the code norm. **One scalar suffices for cosine.** This is the denominator correction described in [the multibit comparison, §2.2](https://arxiv.org/html/2604.19528v2#S2.SS2).

The corrected reconstruction has length \(\lambda\|z\|\), generally exceeding one. Renormalizing it would discard the correction. In ideal arithmetic,

\[
\tilde u=\lambda z=u+e,\qquad u^\top e=0,
\]

so score error is \(v^\top e\): the scalar removes radial error, while directional error remains. The related scale mechanism is also established in [EDEN, §2.3](https://arxiv.org/html/2108.08842v3#S2.SS3). It does not guarantee preservation of close neighbors.

**Use an exact four-bit encoder, with no experimental parameter sweep.** Choose the grid direction maximizing

\[
\frac{u^\top z}{\|z\|}.
\]

Implement the critical-scale enumeration from [Extended RaBitQ, Algorithm 1](https://arxiv.org/html/2409.09913v1#S3.SS2): start with sign-matched magnitudes \(0.5\), then enumerate transitions at \(k/|u_j|\), \(k=1,\ldots,7\), for nonzero coordinates. At D768 there are at most 5,376 transitions. Use stdlib sorting or a heap, incrementally maintain dot product and squared norm, process equal thresholds deterministically, and reconstruct the best code afterward. Fix ties and zero-coordinate signs in the format. This searches the encoder’s discrete choices, rather than testing several scientific arms.

**Use a full D768 rotation for the first qualification.** Generate a seeded Gaussian matrix and orthonormalize in `f64`, adapting the existing [V23 construction](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/v23_rabitq_quantizer.rs:94). Persist the resulting matrix bits; cross-platform regeneration from a seed alone is insufficient authority.

The ideal Haar rotation supplies the symmetry behind [RaBitQ’s estimator analysis](https://arxiv.org/html/2405.12497v1). A finite seeded implementation approximates that model and needs numerical qualification.

Three independent 256-coordinate Hadamard blocks preserve norms but retain block energy and block-specific directions. They are not a full Haar rotation. Padding a full Hadamard transform to D1024 increases four-bit codes from 384 to 512 bytes. Recent [Hadamard quantization theory](https://arxiv.org/html/2605.13810v1) covers a specifically dithered construction with its own assumptions and storage—not arbitrary reuse of BORSUK’s blocks. Defer a structured substitute until this method survives.

The existing [TurboQuant implementation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/turboquant.rs:832) offers useful packing patterns, but its MSE codec and structured residual correction are different estimators. [TurboQuant](https://arxiv.org/html/2504.19874v1) allocates **3 base bits plus 1 residual bit** at a four-bit total budget. Four base bits plus residual signs would exceed this budget. Its reported ANN metric is 1@k, including D200, D1536 and D3072 experiments; these support studying the mechanism, not predicting BORSUK recall@100.

**Freeze these numerical and format rules.**

- Perform normalization, rotation, encoder objectives and packed dot products in ordered `f64`; serialize \(\lambda\) once as little-endian `f32`.
- Validate a conservative \(\|R^\top R-I\|_\infty\le10^{-10}\), including Gram-computation roundoff. With defect \(\delta<1\), transformed cosine differs from source cosine by at most \(2\delta/(1-\delta)\) in real arithmetic.
- Report correction-rounding error separately. Its score contribution is bounded by \(|\lambda_{32}-\lambda|\,|v^\top z|\).
- Cast the final distance once to `f32`, then retain `score.total_cmp` and ID ordering. Do not clamp estimated cosine or negative distances.
- Reject nonfinite inputs, zero source/query norms, nonpositive or nonfinite correction, malformed widths and invalid bindings. Pack the lower nibble first; require zero unused high nibble for odd dimensions.
- Introduce a distinct codec/root version binding source records, coefficient bits, order/router identities, normalization and tie policies, rotation digest, payload/group digests, configuration and compiled source closure.

Do not reuse V23’s fixed D96 layout, one-bit alignment thresholds, query quantization or error constants.

**The row budget works; startup and construction need separate admission.**

| Representation | Worst 41,984-row payload |
|---|---:|
| 396 B corrected row | 16,625,664 B |
| 400 B row with another scalar | 16,793,600 B |

The latter exceeds 16 MiB by **16,384 B**. The proposed format leaves **151,552 B** of payload headroom.

Translate the frozen ordinal intervals using the 396-byte stride, generate new group hashes, and independently verify identical row sets, nominee retention, range counts and byte totals. Score **every fetched row**, including incidental rows. Neither trimming nor a new cover selection is permitted.

A D768 `f64` matrix occupies **4,718,592 B**, plus its header. Share one admitted immutable matrix between the two panels. Charge its startup read, serialization coexistence, construction scratch and retained capacity. A 64-byte header makes the matrix object 4,718,656 B; worst payload plus that object alone is **21,344,320 B**. Thus this preserves the historical **payload** envelope, while it does not qualify a 16 MiB total-cold-byte limit or 32 total cold GETs.

Maintain a distinct-object startup ledger. In the existing histogram code, `startup_book_bytes` is already included in `startup_generation_bytes`; those counters must not be added together.

Dense transformation of both 100k panels requires **117,964,800,000 multiply-accumulates**, before encoder search and authentication. It cannot inherit the old 20-billion-operation construction allowance. Preregister a new, explicitly counted construction-work budget after bounded native stage checks, preserving host safety limits. Admit matrix, scratch, output and diagnostic coexistence before allocation. Separate construction time from query preparation and scoring.

At 100M, bodies alone remain **39.6 GB per generation**, with another **200 MB of group hashes**, plus rotation, router, IDs/maps and retained generations. Dense encoding costs roughly **59 trillion multiply-accumulates per dataset**. Scale feasibility remains unqualified.

**Updates and pins require matching immutable state.** Updates must encode the original admitted vector through the declared source convention and the same rotation. Never rebuild from four-bit reconstructions. Deletes must exclude stale base and replacement rows before ranking. Plans bind generation, mutation revision and query identities. Compaction can copy codes when their rotation and semantics match; changing either requires reencoding from source. Retain old payloads, hashes and rotation state while readers remain pinned, charging coexistence. The current SQ8 snapshot API cannot interpret this new four-byte field as a norm.

**Run this qualification sequence later; none was executed here.**

1. **Independent numerical oracle:** brute-force all grid directions at tiny dimensions against the encoder. Independently unpack signed integers \(2c-15\) and evaluate the ratio with source directions. Cover nonunit inputs, axes, anisotropy, scale invariance, antipodes, zero/nonfinite rejection, odd dimensions, ties, correction serialization and near-tied final rankings. Distinguish implementation drift from quantization error.
2. **Native qualification:** compile/run affected Rust library and CLI targets; pass workspace Clippy correctness/suspicious checks and `bash scripts/check_rust_test_build.sh` on the exact revision. Add small authenticated round-trip, corruption, cap-before-read, source-growth and partial-output checks. Use a minimal Rust codec and existing diagnostic machinery.
3. **Admission:** pass source-bound validation on actual pinned inputs and a separate disposable CLI canary, declaring real and mocked operations. Encoding receives no queries or truth. Freeze rotation and both payloads before requests.
4. **One consumed-panel falsifier:** use the original FIRST100k, D768, k100, 64 queries per dataset and exact frozen fetched populations. Report unchanged SQ8, decoded-cosine control and corrected four-bit results separately. Seal all 128 plans/results before truth opens. Record replacements and rank100/101 gaps.
5. **Unchanged acceptance:** each dataset requires at least **6,272/6,400 hits**, fourth-smallest hit count **≥95**, retained nominees, **≤32 payload ranges** and **≤16,777,216 payload bytes**, with complete authenticated resource and terminal receipts. Execution defects are INVALID; valid quality/envelope failures remain REJECT.

**Decision:** implement this single corrected four-bit RaBitQ falsifier next. Do not start scale work until it survives. Other-dimensional papers, the earlier D96 residual-routing failure and reconstruction-error results neither certify nor reject this D768 scoring method. There is no 98% guarantee or matched vendor result.

Read-only throughout: no edits, Cargo, experiments, corpus/query/truth reads, AWS or children. The worktree advanced concurrently to `3feea1d`; only the decision document changed from `371e0988`, and the reviewed Rust files remained unchanged.

## Root corrections before implementation freeze

The consultation's correction-rounding expression bounds cosine error. For the declared distance `2 - 2*cosine`, the correction-only bound is **twice** that expression: `2*abs(lambda32-lambda)*abs(dot(v,z))`. Final `f32` score-cast error must be accounted separately. This follows directly from the declared score algebra; no native measurement is implied.

Validate the conservative Gram certificate once when constructing or admitting the immutable rotation. Encoding and query preparation borrow that admitted state; copying V23's validation-per-row pattern would add repeated cubic work. Persist exact matrix bits and bind the admitted matrix identity to each prepared query. The certificate must account for dot-product and row-sum measurement roundoff before comparison with the declared tolerance.

Compare threshold ratios `k/abs(u[j])` exactly from the dyadic representation of each finite `f64` component and the small integer `k`; equality of rounded quotients is insufficient. For `a=3/8+2^-53` and `b=5/8+2^-52`, `3/a` and `5/b` round to the same `7.999999999999997`, while exact `3*b-5*a=2^-53`. This witness was independently reproduced with Python's exact rational arithmetic; it is a comparator falsifier, not a corpus measurement. Batch only truly equal ratios, never epsilon groups. Retain/replay the winning event prefix so final code reconstruction matches the evaluated state. Ordered `f64` objective arithmetic needs a stated numerical contract; do not claim exact-real global optimality from it. Exhaustive tiny-dimensional tests must cover exact ties, colliding rounded quotients and adjacent-representable components.
