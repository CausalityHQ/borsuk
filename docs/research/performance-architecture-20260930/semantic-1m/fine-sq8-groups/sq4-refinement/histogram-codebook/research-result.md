**Yes—implement per-dimension, histogram-optimal scalar SQ4 first, using exact weighted 1D dynamic programming.** Treat it as one falsifiable scoring intervention, with no routing change and no expectation that MSE optimality guarantees .98 recall.

The [closed a0002 decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/native-diagnostic/a0002/decision.json) establishes the reason to try it: identical fetched rows pass with SQ8 and fail with uniform SQ4. The candidate must recover approximately **76% of ReLAION’s and 90% of CoHere’s observed scoring loss**. Nothing supplied measures coordinate distributions or rank-boundary margins, so its probability of passing remains unknown.

This advances the earlier [layout reconciliation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/implementation-review/layout-research-result-20261003.md): fetched coverage is now adequate on these consumed panels, making quantization the isolated next question. Preserve the uniform arm’s valid **REJECT**.

**The exact candidate**

Keep original order, group16 authentication, nominees, deterministic covers, incidental fetched rows, original `low/step`, request preprocessing and stable score/ID ordering. Rows remain:

```text
i64 ID + f32 reconstructed squared norm + 384 packed nibble bytes = 396 B
```

Use one dataset-wide codebook per dimension—not one codebook shared across dimensions, and not cell-specific books.

1. Stream every authenticated original SQ8 record into `h[j][b]`, where `b` is its original unsigned byte. Training receives only corpus bytes and coefficients; requests, nomination-derived subsets and truth cannot influence it. Authenticate group hashes, IDs, whole-body SHA and exact EOF.
2. For each dimension, retain its occupied bins in increasing order. Solve weighted contiguous clustering with `K = min(16, occupied_bins)`.
3. Store the weighted bin-coordinate means as 16 ordered `f32` centers. For fewer than 16 occupied bins, use exact occupied-bin centers and deterministic duplicate padding.
4. Construct a 256-entry encoding map per dimension by exhaustive nearest-center search against the **stored** centers. Break equal distances by smaller center index.
5. Authenticate the original source again during the encoding pass. Seal both codebooks and both payloads before opening requests.

For occupied bins \(b_i\) with counts \(w_i\), interval cost is

\[
E(a,z)=V-\frac{M^2}{W},\qquad
W=\sum_{i=a}^{z}w_i,\quad M=\sum w_i b_i,\quad V=\sum w_i b_i^2.
\]

With prefix sums:

\[
D(t,i)=\min_{t-1\le r<i}\{D(t-1,r)+E(r+1,i)\}.
\]

Use deterministic split ties and `f64` training costs. This enumerates the complete 1D partition problem; Lloyd would introduce unnecessary initialization and convergence choices. Weighted 1D DP is an established exact optimization mechanism. [Official weighted-clustering documentation](https://search.r-project.org/CRAN/refmans/Ckmeans.1d.dp/html/Ckmeans.1d.dp.html).

Because each dimension’s positive `step` is fixed, minimizing bin-domain SSE also minimizes real-arithmetic affine reconstruction SSE:

\[
(\mathrm{low}_j+\mathrm{step}_j b-
[\mathrm{low}_j+\mathrm{step}_j a])^2
=\mathrm{step}_j^2(b-a)^2.
\]

“Exact” describes the DP search, not exact real-number arithmetic. Casting centers and the existing `f32` reconstruction introduce rounding. Report actual decoded training distortion against nearest17, using the histogram and literal reconstructions; require it not to exceed uniform distortion beyond a preregistered roundoff allowance.

**Retain the current scoring arithmetic**

The [existing codec](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:2929) recomputes squared norm. The [SQ8 scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:88) ranks squared L2 after query preparation—it does not divide the final score by reconstructed vector norm.

Store centers in **bin coordinates** to preserve its decomposition. For code \(c_j\), decode

\[
\hat x_j=\mathrm{low}_j+\mathrm{step}_j A_{j,c_j}.
\]

Compute `norm += value * value` sequentially in `f32`; never copy the SQ8 norm. Retain finite zero reconstructions.

Prepare `q` with the existing `cosine_vector`, then retain the current operation order:

```text
shift   += q[j] * low[j]
qnorm   += q[j] * q[j]
weight[j] = q[j] * step[j]
shift   -= qnorm / 2

inner   += center[j][nibble] * weight[j]
score    = reconstructed_norm - 2 * (inner + shift)
```

Reject nonfinite values and malformed geometry. Keep sequential `f32` arithmetic and the existing `score.total_cmp`, then ID tie-break.

Arbitrary centers cannot pass through the current `17*nibble` expansion into an SQ8 byte. Add a small private packed scorer in `sq4_diagnostic`, replacing that expansion for the candidate. Keep matched SQ8 scoring unchanged. No generic codec framework or SIMD work is needed.

**Allocation, startup and generation identity**

| Allocation | D768 size | Lifetime |
|---|---:|---|
| `u32` histogram, 768×256 | 786,432 B | Training |
| Encoding maps, 768×256 | 196,608 B | Transcoding |
| Centers, 768×16 `f32` | 49,152 B | Pinned generation |
| Reused 256×256 interval-cost table | 524,288 B | Training |
| DP rows, backpointers and prefixes | About 19 KiB | Training |

This is approximately **1.6 MiB of bounded training workspace**, including small streaming buffers, before ordinary metadata/runtime overhead. Train datasets sequentially; discard histograms, maps and DP scratch after encoding. Retaining both diagnostic books costs 98,304 B of center storage.

Use checked allocations before reads, checked histogram increments, widened prefix arithmetic, and per-dimension count sums equal to admitted training rows. Account for DP work and poll the existing guard during training. The loose transition bound is \(768\times16\times256^2\) per dataset. Qualification must establish actual training CPU and memory; the old 9.8413 seconds does not predict them.

Publish centers as binary little-endian `f32` bits in an authenticated object capped at 64 KiB, including its header. Keep its descriptor in the root rather than expanding centers into JSON. Bind:

- codec/version, dimensions, training row count and trainer/tie policy;
- original records, coefficient and histogram digests;
- codebook, new payload and group-authentication identities;
- source/configuration and binary identities in the terminal closure.

Bump the experimental format/schema and update fixtures. The old archive and receipts remain immutable; its 14/70 qualification does not qualify this implementation.

Explicitly admit the book before queries and charge its encoded/decoded coexistence and retained generations. The query’s 32 ranges and row bytes remain unchanged. Even conservatively adding a 64 KiB codebook allowance to the reported largest payload remains below 16 MiB.

**GET accounting still matters:** a cached, authenticated book needs no query GET; first admission needs a read. Preserve the historical 32-range payload gate and report startup separately. A future claim of **32 total cold GETs** must include startup and cannot silently add a 33rd request. Co-fetching the book with already-required metadata would require its own bounded format/admission change.

Freeze books within a generation. Retraining creates a new generation and requires re-encoding affected rows; old readers retain their matching books. Unchanged books permit ordinary copying/compaction without retraining. Updates must use the codec’s declared input mapping. This diagnostic quantizes SQ8 reconstructions; direct encoding of original `f32` vectors would be another codec revision. Never train or rebuild repeatedly from SQ4 reconstructions.

**Comparison at equal total bytes**

The paper mechanisms remain relevant at D768, including results obtained at D200 or D384. Their reported metrics and conditions do not establish this arm’s recall@100 or vendor parity.

| Alternative | Possible 396 B construction | Additional requirements |
|---|---|---|
| Histogram SQ4 | ID8 + norm4 + 384 B codes | Per-dimension book; unchanged score decomposition |
| Four-bit RaBitQ | ID8 + correction4 + 384 B codes | Unit-vector convention, rotation and corrected estimator |
| TurboQuant MSE, four bits | ID8 + scalar4 + 384 B codes | Rotation, codebook and declared reconstruction/score convention |
| TurboQuant inner-product, **3+1 bits** | ID8 + residual norm4 + 288 B base + 96 B residual signs | Unit-vector convention, rotation and QJL projection |

RaBitQ’s denominator correction can be combined with code normalization into one per-row factor under a unit-vector convention. General nonunit squared-L2 scoring also needs the source squared norm; straightforward storage becomes **400 B**, unless another explicitly qualified representation removes that field. [Multi-bit RaBitQ, §3.3](https://arxiv.org/html/2409.09913v1).

TurboQuant’s residual correction consumes bits and a residual norm. **Four base bits plus one residual bit is 492 B** even with unit input and only one scalar—not a four-bit alternative. Its 3+1 construction can fit 396 B, but changes the estimator and input convention. [TurboQuant, §3.2](https://arxiv.org/html/2504.19874v1).

Charge shared rotation/projection state, codebooks, authentication, query scratch and refinement too. A dense D768 `f32` matrix alone is 2,359,296 B. A seed reduces serialized state, not materialized memory or transform work. Additional SQ8 refinement also consumes bytes and requests; this frozen cover already reaches 32 ranges.

The existing three-block Hadamard transform offers a structured implementation starting point, but does not automatically inherit guarantees derived for a full random orthogonal rotation or independent Gaussian projection. Padding to D1024 would also exceed the 384 B four-bit code budget.

Thus histogram SQ4 is the smallest intervention worth falsifying first. If it fails, **vector-corrected four-bit RaBitQ** is the single stronger next alternative to specify, with its unit normalization and field semantics explicit. There is no evidence here guaranteeing that it passes either.

**Correctness checks before corpus scoring**

Reuse the existing native test machinery and add three focused checks:

1. **DP oracle:** tiny weighted histograms compared with exhaustive enumeration of all contiguous partitions, using independently calculated weighted deviations. Include holes, dominant tails, tied optima and ≤16 occupied bins.
2. **Codec/scoring oracle:** exhaustive mappings of all 256 input bytes against stored centers; nibble order, odd dimensions, short groups, zero reconstructions and recomputed norms. Compare scores with independently decoded brute-force distances. With centers `17*i`, require bitwise reproduction of the existing uniform scorer. Include exact ID ties and deliberately near-tied rows.
3. **Authenticated pipeline:** corrupted books, wrong coefficient/source binding, malformed centers, changed second-pass source, insufficient allocation admission, durability failures and requests/truth opened too early. Reuse existing descriptor and closure checks.

On the exact implementation revision, compile/run affected library and CLI tests, build the release binary, pass workspace Clippy correctness/suspicious checks, and run the real `scripts/check_rust_test_build.sh` with bounded concurrency and its test shim unset. Compiler cost is not a rejection criterion.

**The cheapest authentic paired gate**

Use one native consumed-panel attempt over the retained authenticated FIRST100k inputs:

1. Complete the required source-bound admission and separate disposable CLI canary first. Declare real versus mocked operations.
2. Train and seal both books/payloads without request or truth fitting.
3. Reuse the exact frozen 396 B covers. Score every fetched row with the candidate and unchanged SQ8 reference in the same bounded native pass. Retain the original256 result as historical evidence.
4. Seal all 128 plans and complete outputs before truth opens; authenticate the entire closure.
5. Apply the unchanged gates separately to both datasets: **sum of hits ≥6,272/6,400**, fourth-smallest hit count ≥95, all nominees retained, ≤32 ranges and ≤16,777,216 payload bytes per query. Retain the original supervisor/resource/durability requirements.

Record candidate–SQ8 top100 replacements and score gaps around rank100 during scoring. This directly tests whether smaller reconstruction error preserves near ties.

There is also a cheap, conservative pretruth check. Let \(A_q\) be SQ8’s top100 and \(B_q\) the candidate’s:

\[
|G_q\cap B_q|\ge |G_q\cap A_q|-|A_q\setminus B_q|.
\]

The closed SQ8 totals leave **97 replacement slots for ReLAION and 66 for CoHere** before that lower bound falls below .98. Maximum replacements of four and two per query respectively would also preserve the recorded p05 lower bound. Failure of these sufficient conditions is **not** rejection: replacements may remove SQ8 false positives. Actual sealed truth evaluation remains decisive.

MSE is not recall. Score error contains the directional term

\[
2(x-q)\cdot e+\|e\|^2,
\]

and tiny boundary gaps can turn small errors into many replacements. Report paired losses per query; do not treat 6,400 hit indicators as independent observations. With 64 consumed queries, the fourth-smallest statistic has limited tail precision, and adaptive development on this panel prevents a fresh-quality claim.

A completed quality/envelope failure is **REJECT**; setup, authentication, compilation or supervision failure is **INVALID**. Neither disposition changes historical results. Survival only warrants further qualification; it establishes no cold latency, QPS, scale or matched vendor result.

This consultation was read-only: no edits, native experiment, corpus/truth reads, cloud work, reviews or children.
