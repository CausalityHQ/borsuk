# V113 resident nominee gate (preregistered design)

Status: design only, 2026-09-23. No V113 measurement has run. V112's
99.234% returned Recall@100 on ReLAION-1M development 1,000 is an offline
ceiling that reads SQ8 nominee scores before choosing object ranges. Its
paired hard-capped V109 control returned 98.803%. The job and source
identities are in `v112-precise-nominee-closeout.md`.

## Decision and format candidate

The next candidate keeps V77 PQ64 nomination of 512 rows, SQ8 physical
pages, 100 primary and 412 secondary votes, the exact interval planner,
and final SQ8 scoring. It replaces V112's local SQ8 nominee lookup with a
generation-bound resident 16-byte row plane. The plane is an additional
representation, not the final scorer. No memory cap or collection-size
switch is imposed: benchmark the least-cost qualified point in
`M(N,R,C,G)` for vector count, target recall, concurrency, and pinned
generations. The 16-byte plane adds exactly 1.6 GB per 100M vectors in one
generation and 3.2 GB for two pinned generations before overhead. These are
arithmetic projections, not measured 100M peaks.

For any `D ≥ 64`, PQ64 and residual PQ12 partition coordinates at
`floor(sD/M)` for each of `M=64` and `M=12` blocks. Each block is padded
individually to `ceil(D/M)` coordinates for the common training kernel;
every code byte has at least one real coordinate. Persist the original
dimension and partition rule. The page body keeps the original dimension.
Corpus-specific codebooks and layouts may be trained from corpus rows;
query-driven changes
to vote factors, shortlist size, or acceptance thresholds are disallowed.

The candidate encoding targets the **SQ8-dequantized** row `t` and the norm
field actually used by the V112 scorer. Let `p` be its PQ64 reconstruction,
`e=t-p`, `nu=SQ8_norm-||p||²`, and raw `alpha=<e,p>/||p||²`. Clip `alpha`
to the fixed interval `[-1,1]` before quantizing it; the residual target
uses this quantized value, so clipping transfers the excess into the
residual without invalidating the score identity. Store quantized `nu` and
`alpha` as signed 16-bit scalars and round ties to even. Fit `nu`'s scale
from the maximum absolute value over **every** row in the generation and
reject an append outside that range until a new generation is encoded.
The alpha range and scale are fixed by the clipping rule. Use 12 residual PQ bytes,
one for each balanced block of `e-alpha_hat*p`. The per-block zero padding,
original `D` and codebook shape are generation-bound.
At `D=768`, each block has 64 real coordinates. If `||p||²=0`, encode
`alpha=0` and train the residual on `e`; the scorer uses the same rule.
The scorer estimates
`||q||²+||p||²+nu_hat-2[(1+alpha_hat)<q,p>+<q,r_hat>]` for each nominee.
Its top 100 receive 513 votes each; the other 412 receive one vote each.
Persist the plane, scalar scales and residual codebook with a new format
marker and hashes binding it to the exact PQ64 codes/codebook, SQ8 object,
and layout permutation. Reject any mismatch at generation load. Row
identity and SQ8 final scoring must use the same layout ordinal.

Record the maximum residual-to-centroid norm over **all** encoded rows for
each residual codeword, and the maximum L2 error when the exact residual is
cast to float32. Because the residual target uses quantized `alpha_hat`,
the ideal-arithmetic score error is bounded by
`nu_scale/2 + 2||q||₂(sqrt(sum_s rmax[s,code_s]²) + cast_error_max)`;
add explicit per-query allowances for the V112 float32 SQ8 score relative
to the mathematical `low + code*span_step` score and for the resident
scorer's arithmetic. Independently recompute both on all 512 nominees;
only an outward-rounded total can be called a certificate. Report the
fraction of queries whose SQ8 top-100 boundary is certified. A loose bound is
an inconclusive certificate, not evidence of poor measured recall.

This is a hypothesis, not a selected production default. In particular,
16 bytes may lack enough score fidelity. A plain residual PQ16 arm and a
four-byte scalar-only arm will isolate whether the two scalar corrections
or the residual code add useful primary-set fidelity. Query/GT rows are
never used for codebook training. A fresh, recorded corpus-only training
sample and seed fix the representation before any query evaluation.

## Cheapest decisive screen and promotion

Run one ReLAION-100k **development** 1,000-query score-fidelity cell on
Causality Spot from a pushed, frozen source revision. Source SHA-256 is
`a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`;
query SHA-256 is
`4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac`.
The sealed inputs are the `source` and `queries` identities in
`scripts/launch_native_geometric_layout_spot.py`; their S3 URIs and encoded
lengths are part of the launcher registration.
Build the SQ8 target by V70's corpus-only quantization, PQ64 by V77's
source-only recipe (seed 7301, 10 iterations, 100k sampled rows), and R16
by the source-only residual recipe (seed 113031, 10 iterations, 100k sampled
rows). Exhaustively scan the 100k PQ64 codes and choose the same top 512
nominees for every arm. Compare their SQ8 top-100 membership with the
resident R16, four-byte scalar-only, and plain residual PQ16 estimates.
No ground truth, page layout, GET cap, or returned recall is used in this
screen: those would confound a representation decision at this size.

Record mean and p05 overlap of the 100 SQ8-primary nominees, exact ties,
per-row score errors and the fraction of queries satisfying the Lean margin
premise. A representation passes the screen only if mean primary overlap is
at least 95/100 and p05 at least 90/100. Promote the smallest passing row
plane; the 16-byte plane must beat the scalar-only arm if both pass. A miss
rejects the encoding at this width. If R16 gains at least two mean primary
positions over the scalar-only arm but misses the threshold, compare one
preregistered 32-byte width; otherwise change the representation. These
thresholds are screening choices, not a claimed implication for returned
recall. The independent validator must recompute source hashes, SQ8
quantization, top-512 nomination, scores and primary sets. It must hash
persisted codebooks/codes and recompute error maxima from those codes;
bitwise re-encoding is not a validity criterion because BLAS near-ties can
change assignments across machines. Sync terminal
evidence to S3 and terminate the Spot instance.

Only a passing representation gets a paired ReLAION-1M **development** route
replay on the frozen V63/V77 layout and authenticated SQ8 object. Recompute
V109-style PQ64 and V112-style local-SQ8 controls in the same source/run;
historical aggregates are not a paired baseline after a planner change.
All new arms and the independent validator use one canonical interval tie
order: highest weight, then fewer GETs, then fewer units, with closed state
preferred to open on a remaining tie. Record returned GT100 hits, p05,
sub-90 queries, planned GETs, bytes, cap violations and physical containment.
The 1M gate requires at least
99.0% returned Recall@100, at least 75% recovery of the paired positive
V109-to-V112 hit gap, p05 at least the larger of V109's p05 and V112's p05
minus one, no more sub-90 queries than V109, and zero physical cap
violations before untouched validation or a live S3 serving benchmark.
The 99.0% ReLAION development floor was set after seeing V112's 99.234%; it
is a **development** gate. Freeze the candidate and a separate 99.0% returned
Recall@100, p05 ≥90, zero-cap-violation acceptance rule before untouched
ReLAION-1M validation or another corpus is run.

The cross-corpus gate uses deep-image-96-angular (9.99M real vectors with
shipped GT100) and new corpus-only codebooks. Normalize every corpus and
query vector to unit L2 norm **before** PQ and SQ8 encoding; store the norm
of that normalized vector in SQ8. L2 and cosine then have the same rank on
nonzero vectors. Reject any zero vector under this declared metric rule.
Use the same 100-primary/412-secondary vote rule, 32 GET and 16,777,216-byte
physical caps, and a preregistered region count of
`ceil(page_count * 1024 / 3907)`. A 96-dimensional SQ8 row occupies 108
bytes, so the byte cap covers at most 155,344 rows (1.56% of 9.99M), versus
21,509 rows (2.15%) on ReLAION-1M; report actual cap-binding rates. The
paired control is V109-style PQ64 minimum-score ranked page admission under
the same caps; the upper control is V112-style local-SQ8 nominee ranking.
The historical V82 reader used another format and 45 requests, so it is
context rather than a paired baseline. For a paired oracle-control gap of
at least 100 returned hits over 1,000 queries, require at least 75% recovery,
tail noninferiority, and zero cap violations; below 100 hits, use at least
95/100 mean SQ8-primary overlap and the frozen absolute recall/tail gates.
A failure on the other embedding family is a method failure to diagnose,
not a reason to choose special thresholds for that dataset. Live S3 latency/QPS,
charged memory `M(N,R,C,G)`, cost, and generation rollover remain measured
release gates. A later 100M run must use the frozen qualified revision.

If a candidate passes the 100k screen but fails the 1M gate, substitute the
SQ8-oracle primary set into
the same planner as a counterfactual. Test one 32-byte residual plane only
if this recovers more than half of the candidate-to-oracle lost hits and the
full 16-byte plane recovers at least 25% more of that gap than the
scalar-only arm. Otherwise change the planner or representation according
to the failing layer. A 32-byte plane is a new frozen cell, not a sweep.

On append, either publish new monotonically increased `rmax` and
`cast_error_max` values with an authenticated generation manifest, or reject
the append until a full generation rebuild. A stale bound must never be used
to claim primary-set certification.

`formal/NomineePrimaryStability.lean` checks the conditional statement that
authenticated score-error bounds plus a positive SQ8 boundary margin leave
the 100 primary rows and any deterministic page-vote calculation identical.
It also checks the two-generation 100M R16 payload arithmetic. A separate
proof of the Rust interval DP's optimum is still needed. None of these
arithmetic and conditional theorems establishes unseen-query recall, live
S3 latency, or 100M node-charged resource peaks.
