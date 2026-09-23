# One-million source-trained page-dispersion mass routing screen

## One decision

The closed geometric whole-group permutation at `4367ddf1` worsened the
paired 96-byte projection (98.151% to 98.018% mean GT100, 51 to 59 sub-90
queries) without changing its 89% p05. The 80-byte truth-ranked diagnostic
only shows that those physical groups can contain the tail neighbors when
ground truth chooses them; it is not a 96-byte bound or a query-blind route.
For the 51 PQ96 failing queries, the median deficit to 90 hits is six and
the median two missing groups can cover that deficit. Test exactly one new
source-trained **routing signal** that estimates nearby row mass per group.
Keep the original physical group order, the 910 eight-page groups, projected
96-byte row lengths and the unchanged 32-GET/16,777,216-byte range planner.
No real PQ codes or S3 code reads are involved.

## Source-only moments and fixed score

Authenticate the seven frozen input identities in
`scripts/native_one_million_selector_cell.py`. Rebuild the previous page
centroids/membership/page order and require their closed SHA-256 values:
`757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704`,
`55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13`,
and `bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b`.
For every page, stream source rows in fixed Parquet order to compute float64
population mean `mu[p,j]`, variance `v[p,j]`, and integer count `n[p]`.
Use two passes: sum/count in the first, then sum squared deviations from the
first-pass mean in the second. Seal float64 arrays and counts before query
or truth download. Reject nonfinite values, zero counts and negative
variances. Do not use query/truth data to train, normalize or choose a scale.

For query `q`, calculate for each page

`m[p] = sum_j((q[j]-mu[p,j])²) + sum_j(v[p,j])`

`s2[p] = 4*sum_j(v[p,j]*(q[j]-mu[p,j])²) + 2*sum_j(v[p,j]²)`.

Set `s[p]=sqrt(s2[p])`. For `s>0`, use the standard-normal CDF
`F[p](t)=ndtr((t-m[p])/s[p])`; for `s=0`, use `1[t>=m[p]]`.
Set lower `min(m)-12*max(s)-1` and upper `max(m)+12*max(s)+1`.
Run exactly 48 bisections of total expected row mass
`sum_p n[p]*F[p](t)` against 100; when mass is at least 100 retain the
upper half endpoint, otherwise retain the lower half endpoint. At the
final upper endpoint, set group mass `M[g]=sum_{p in g}n[p]*F[p](upper)`.
Rank descending `M[g]`, tying by `(role, original group ordinal)`, then
apply the unchanged planner. Use `scipy.special.ndtr` pinned in the
experiment requirements; no alternative CDF, mass target, radius, blending
weight or byte normalization is screened on this cohort.

This is a diagonal-Gaussian moment surrogate followed by a normal
approximation for squared distance. It is not a calibrated probability,
distance bound, or production index. Correlated dimensions, normalized
geometry and multimodal pages may make it worse; that is what this finite
screen tests.

## Paired replay and stop gate

The same 1,000 ordered development queries and GT100 must reproduce the
closed original-layout PQ96 control exactly: 98.151% mean GT100, 89% p05,
99.28% GT10 and 51 sub-90 queries, including every per-query plan. The
closed PQ96 canonical 1,000-sample list SHA-256 is
`ce61ad527968c5bf1bb5bc45e576b4f94eeb690c9ac7000fff6acf6f2ead713d`.
Seal moments before exposing either object. The evaluator is unprivileged and
without network. The independent validator rebuilds source moments, the
score, all baseline and challenger admissions, and per-query containment
without calling the producer router or planner. Preserve paired raw samples,
rescues and newly failing queries.

Advance only if challenger mean GT100 >=98.151%, p05 >=90%, GT10 >=99.28%,
at most 49 sub-90 queries, and every query <=32 projected GETs and
<=16,777,216 projected bytes. Use integer exact hit totals for paired
comparisons. Any valid miss kills this fixed score without retuning.
A pass authorizes only a preregistered paired 100k **actual** PQ96 code
quality/read/resource test against the successful 200-byte two-bit control,
then fresh holdout and 1M code-plane qualification. No product serving
claim follows from this source-only projection.

Run one immutable Causality Spot attempt. Preserve source/query separation,
create-only artifact and terminal publication with readback, terminal-only
monitoring while incomplete, phase process-tree RSS plus 64 MiB below 3 GiB,
zero swaps, and immediate instance termination at terminal. Discard and
restart an interrupted cell under a new attempt prefix.
