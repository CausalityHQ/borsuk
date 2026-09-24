# V166 source-surrogate unit-ranking probe

## Decision and scope

Test whether a source-derived score surrogate ranks extra physical SQ8 units
better than V165's equal-vote rule under the same per-query encoded-byte and
GET charges. This is a GT-blind, offline feasibility screen on ReLAION-1M
D768. It cannot establish Recall@100, serving latency, or a 100M operating
point. Its only promotion is permission to freeze a separate returned-quality
experiment on a fresh query panel.

The V164 source-only cosine order and V115 PQ64 router remain fixed. The
authenticated V164 32-row mapping, old SQ8 mirror, V115 router, and V36
source vectors are read as complete artifacts. No validation GT, V155/V164
returned result, or quality summary is read during this screen. Check all
source-object byte counts and SHA-256 values before processing; seal the
selected pseudoquery IDs, parameter, per-query plans, and raw scores before
reducing the decision. A second implementation replays the plan and charge
checks from the sealed inputs.
The V115 router is an immutable research artifact with a v1 manifest. The
V166 runner authenticates and reads that exact manifest and its five
sections locally for this experiment; production's v2 reader continues to
reject it. This does not establish compatibility with old index formats.

## Frozen pseudoqueries and comparison

Use SHA-256 with the fixed domain prefix given by the exact UTF-8 bytes
`borsuk-v166-pseudoquery-v1:` followed by the decimal
stable ID; take the first 256 IDs in digest order. The
first 128 are the fit partition and the next 128 the holdout. This rule is
independent of source row order and corpus name. The query vector is the
raw source vector, matching the pinned squared-L2 SQ8 scorer. Generate a
fresh PQ64 top-512 roster
through the authenticated V115 router, then score those nominees using the
old authenticated SQ8 representation and take the best 100 as exact-primary
units. Exclude the pseudoquery's own ID before setting the threshold or
counting outcomes. Reject duplicate or short rosters.

The candidate universe per query is the union of each nominee's V164
32-row physical unit and its immediately adjacent units. Deduplicate it.
Build a per-unit raw-source mean and residual second moment; for the query's
own unit, recompute both without its source row. Compare the modeled count
of non-nominee rows with SQ8 distance at most the 100th nominee SQ8
distance against actual SQ8 scores for every non-nominee row in this
universe. Let `m` be the mean, `r2` the mean squared residual norm, `q`
the raw query, `d` the dimension, and `t` the SQ8 threshold.
Set modeled distance mean `u = ||q-m||² + r2`, raw standard deviation
`s = max(1e-6, 2 sqrt(r2 ||q-m||² / d))`, and predicted per-row exceedance
probability `Phi((t-u)/(alpha s))`, where `Phi` is the standard normal CDF.
The sole fitted parameter `alpha` comes from the fixed grid
`{0.25, 0.5, 1, 2, 4, 8}`. Select the value minimizing mean squared error
between predicted and actual fractions of eligible rows across fit units;
break exact ties toward the lower value. Multiply the probability by the
number of non-nominee rows in each unit to obtain modeled mass. No mean
offset, directional correction, or threshold adjustment may be introduced
after observing the holdout. This squared-L2 isotropic approximation ignores SQ8
quantization error and norm variance, which the screen must expose rather
than repair post hoc.
Report the source/SQ8 score mismatch and per-unit calibration; a poor fit
is a method failure, not a reason to tune thresholds on the holdout.

For each holdout query, replay V165's equal-vote plan using its own planned
bytes and GETs as that query's matched cap. The surrogate arm must fetch
every exact-primary unit, then maximize predicted non-nominee exceedance
mass within the same candidate universe and matched caps. Charge every
intervening physical 32-row unit and merge adjacent units into one GET,
exactly as V165. If V165 fails to cover every exact-primary unit, or the
surrogate cannot do so within the matched cap, stop and report the
comparability failure. Both arms count captured *actual* above-threshold
non-nominee rows in the candidate universe. Also report candidate-universe
rows and units, model and baseline bytes/GETs, and uncaptured actual
exceedances. No GT-derived quality metric is calculated.

## Gate and limits

The surrogate passes only if its mean paired gain in captured actual
above-threshold non-nominee rows is positive and the lower endpoint of a
10,000-resample paired 95% bootstrap interval is strictly positive on the
128 holdout queries, with every per-query byte and GET cap respected. Use a
fixed bootstrap seed and publish the paired 128-query raw counts. Any
failure kills this base surrogate; do not tune the model on this holdout.
Even a pass is an internal query-distribution diagnostic because the V115
router and V164 order were trained on the entire source, including these
source pseudoqueries. It does not prove external generalization. A later
GT-blind external-query check may supply a transfer signal, but if used to
select the policy, that query panel is consumed for quality evaluation.

The learned mass is a score-ranking hypothesis. Lean proves only that,
for a fixed model and universe, the *minimum feasible charge* is
nondecreasing as its required mass rises, assuming the returned plans are
optimal. It neither proves the Gaussian model, the planner's optimality,
nor containment of true neighbors. Cosine-scoring transfer and the 10M/100M bounded
router and placement gates remain separate decisions.

Run one whole 256-query cell on `c7i.12xlarge` Spot in `eu-central-1` with
AWS profile `causality` and a 14,400-second hard cap. Record its instance
ID, source archive and commit, every frozen input digest, peak RSS and wall
time, all per-query cases and plans, and a terminal marker. If Spot
interrupts or the hard cap fires, discard the incomplete cell and use a
new immutable attempt; do not read an incomplete measurement file.
Immediately terminate compute on any terminal marker, then stream every
terminal-listed S3 artifact and recheck byte count and SHA-256 from the
controller. The runner uses eight BLAS threads. A nonfinite or zero source
norm, or a failed physical-cap check, is a method failure requiring
diagnosis before any replacement attempt.
