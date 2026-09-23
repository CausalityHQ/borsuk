# V113 resident nominee gate (preregistered design)

Status: design only, 2026-09-23. No V113 measurement has run. V112's
99.234% returned Recall@100 on ReLAION-1M development 1,000 is an offline
ceiling that reads SQ8 nominee scores before choosing object ranges. Its
paired hard-capped V109 control returned 98.803%. The job and source
identities are in `v112-precise-nominee-closeout.md`.

## Decision and format candidate

The next candidate keeps V77 PQ64 nomination of 512 rows, V63 SQ8 physical
pages, 100 primary and 412 secondary votes, the exact interval planner,
and final SQ8 scoring. It replaces V112's local SQ8 nominee lookup with a
generation-bound resident 16-byte row plane. The plane is an additional
representation, not the final scorer. No memory cap or collection-size
switch is imposed: benchmark the least-cost qualified point in
`M(N,R,C,G)` for vector count, target recall, concurrency, and pinned
generations. The 16-byte plane adds exactly 1.6 GB per 100M vectors in one
generation and 3.2 GB for two pinned generations before overhead. These are
arithmetic projections, not measured 100M peaks.

The candidate encoding targets the **SQ8-dequantized** row `t` and the norm
field actually used by the V112 scorer. Let `p` be its PQ64 reconstruction,
`e=t-p`, `nu=SQ8_norm-||p||²`, and `alpha=<e,p>/||p||²`. Store quantized
`nu` and `alpha` as signed 16-bit scalars. Fit their fixed scales from the
maximum absolute values over **every** row in the generation, round ties to
even, and reject an append that exceeds either range until a new generation
is encoded; silently clamped codes are forbidden. Use 12 residual PQ bytes,
one for each 64-dimensional block of
`e-alpha_hat*p`. If `||p||²=0`, encode `alpha=0` and train the residual on
`e`; the scorer uses the same rule. The scorer estimates
`||q||²+||p||²+nu_hat-2[(1+alpha_hat)<q,p>+<q,r_hat>]` for each nominee.
Its top 100 receive 513 votes each; the other 412 receive one vote each.
Persist the plane, scalar scales and residual codebook with a new format
marker and hashes binding it to the exact PQ64 codes/codebook, SQ8 object,
and layout permutation. Reject any mismatch at generation load. Row
identity and SQ8 final scoring must use the same layout ordinal.

Record the maximum residual-to-centroid norm over **all** encoded rows for
each residual codeword. Together with the two scalar quantization steps,
query norm and an explicit floating-point accumulation allowance, these
give a per-nominee SQ8 score-error bound. Report the fraction of queries
whose SQ8 top-100 boundary is certified by those bounds. A loose bound is
an inconclusive certificate, not evidence of poor measured recall.

This is a hypothesis, not a selected production default. In particular,
16 bytes may lack enough score fidelity. A plain residual PQ16 arm and a
four-byte scalar-only arm will isolate whether the two scalar corrections
or the residual code add useful primary-set fidelity. Query/GT rows are
never used for codebook training. A fresh, recorded corpus-only training
sample and seed fix the representation before any query evaluation.

## Cheapest decisive test

Run one paired ReLAION-100k **development** 1,000-query cell on Causality
Spot, from a pushed, frozen source revision. Train and hash a new PQ64 plane
on the 100k corpus with the V77 recipe, then use the **same within-cell**
top-512 nominees and 32 GET / 16,777,216-byte caps for every arm. The 100k
SQ8 object is 100,000 × 780 bytes, with 390 full 256-row pages and a
160-row final page. Use 32-row physical units: 24,960 bytes/unit, 8 units
per full page, 5 for the last, and a 672-unit byte budget. This is a
separate 100k geometry; it does not reuse V112's 1M artifact. Compare the
resident candidate and the scalar-only/plain-residual ablations with a
V112-style local-SQ8 nominee oracle and the V109-style PQ64 control. Fix
all routes before consulting GT100. Record, per query, SQ8-oracle primary
set overlap, exact page-weight/plan agreement, returned GT hits, p05,
GETs, bytes, and cap violations. Verify source/data hashes, both route
decisions and final SQ8 ranking independently; sync terminal evidence to
S3 and terminate the instance. Apply the pass/reject rule below without
tuning on the fully observed development split.

All new arms and the independent validator must use one canonical interval
tie order: highest weight, then fewer GETs, then fewer units, with closed
state preferred to open on a remaining tie. Existing V112 artifacts retain
their historical Python tie order. Recompute the V109 and SQ8-oracle arms
inside the new cell; do not use the historical aggregate as a paired result.

The 100k cell licenses a 1M development replay only if caps hold, the
candidate recovers at least 75% of the *positive* returned-hit gap between
the paired PQ64 control and local-SQ8 oracle, and candidate p05 is at most
one hit below that oracle. If that paired gap is under 100 hits over 1,000
queries, call the recall recovery criterion uninformative and require at
least 95% average overlap of the oracle's 100 primary nominees. Do not use
plan equality as a substitute: the 100k caps may leave it insensitive to
primary ordering. Report the observed cap-binding rate. Failure of the
applicable recall or primary-overlap rule rejects this encoding. This is a
screening rule, not proof of 1M quality. The 1M gate requires at least
99.0% returned Recall@100, at least 75% recovery of the paired positive
V109-to-V112 hit gap, p05 at least the larger of V109's p05 and V112's p05
minus one, no more sub-90 queries than V109, and zero physical cap
violations before untouched validation or a live S3 serving benchmark.
10M/100M quality, latency, QPS, charged
memory, cost, and rollover still require their own measured gates.

If the 16-byte candidate fails, substitute the SQ8-oracle primary set into
the same planner as a counterfactual. Test one 32-byte residual plane only
if this recovers more than half of the candidate-to-oracle lost hits and the
full 16-byte plane recovers at least 25% more of that gap than the
scalar-only arm. Otherwise change the planner or representation according
to the failing layer. A 32-byte plane is a new frozen cell, not a sweep.

`formal/NomineePrimaryStability.lean` checks the conditional statement that
authenticated score-error bounds plus a positive SQ8 boundary margin leave
the 100 primary rows and any deterministic page-vote calculation identical.
It also checks the two-generation 100M R16 payload arithmetic. A separate
proof of the Rust interval DP's optimum is still needed. None of these
arithmetic and conditional theorems establishes unseen-query recall, live
S3 latency, or 100M node-charged resource peaks.
