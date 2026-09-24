# V124 generic source-tier precision and certified-refinement diagnostic

Status: preregistered before any V124 scoring. This is a **development-only**
format study. Both cohorts have already been used: deep-image-96-angular
V121/V123 publication test-first-1000 at 9,990,000 rows, and ReLAION-1M V116
validation-1000 at 1,000,000 rows. The second embedding family probes whether
V123's corrected cosine behavior transfers, but neither result is fresh
validation. Keep each corpus's sealed 512 physical nominees and source-ID
layout fixed. Do not refit routing or choose a score width from GT during a
cell. The historical method/layout mismatch between V116 and V121 must be
reported; this study compares source-tier numeric behavior, not matched
end-to-end architectures.

Inputs are the authenticated source Parquet, physical layout, query/nominee
requests and GT100 Parquet tied to V121 a0003 and V116 a0001 terminals. The
worker must check every declared SHA-256 and byte count, unique source row IDs,
dimension, finite nonzero source/query norms, layout permutation, query
geometry, query ordinals, unique nominees and GT IDs before scoring. Deep-image
GT is a list column; ReLAION GT is 100 flat `feature_row_id` rows per query,
and its source IDs are original feature IDs rather than train ordinals.
Normalize both source and query vectors in float64 for the modeled cosine
kernel. Only the first 1,000 query groups of each already used GT split may
enter scoring. No remote SQ8 data wave is used: K=0
isolates the source-tier representation.

For each 512-row union, compare three fixed source representations against
float64 cosine on original float32 source coordinates:

1. FP16 round-to-nearest coordinates widened to float64 and renormalized.
2. Signed int16 per-row symmetric coordinates using
   `scale=max(abs(x))/32767`, nearest-even rounding, widened and renormalized.
3. Original float32 coordinates widened to float64 and renormalized (reference).

Stable ties use ascending source ID. Record ordered returned IDs, GT100 hits,
net and gross disagreements, score boundary margins, peak RSS and wall time.
For each quantized candidate, compute the build-time directional error
`delta=norm(unit(decoded)-unit(original))+1e-12` in float64. The fixed additive
term covers roundoff in this offline numerical check; verify every measured
score difference is within its interval. This gives the modeled
cosine score interval `[approx-delta, approx+delta]` for a unit query by
Cauchy-Schwarz. Choose `tau` as the 100th-largest lower bound among the 512
nominees. Candidates whose upper bound is below `tau` are certified prunable;
exact-source refine all remaining candidates and verify that its top100 IDs
equal the full original-coordinate reference. Record eligible/refined counts,
including p50/p95/max, and any certificate failure. This interval certificate
does **not** cover a deployed FP32/FP16 kernel until its arithmetic error is
bounded and added to `delta`.

The experiment asks whether FP16 or int16 gives a useful compact first tier
and whether sound refinement is selective on both families. It does not pick
a production width from a single dataset or permit skipping exact fallback
based on a vector-count threshold. A two-tier candidate is worth implementing
only if its measured complete I/O and compute path later beats a plain FP32
source tier under the same recall/latency/resource target. Both require a
persisted S3 generation and a measured local RAM/SSD or coalesced S3 access
path for all nominees. The next independent gate remains a matched ReLAION
build, fresh deep-image query ordinals, a sublinear router, live S3 and charged
RAM at the intended `(N,D,R,C,G,L)` operating points.

Use one interruptible Causality Spot attempt with an immutable source archive,
per-input hashes, terminal artifact receipt, interruption handling and
immediate termination after terminal. If interrupted, discard and restart the
whole incomplete diagnostic cell under a new attempt ID. Do not inspect an
incomplete measurement file while the campaign is running. Lean's
`AdaptiveRerank.lean` proves the abstract threshold implication and linear
payload arithmetic from sound intervals; it does not establish the numerical
error bound for this runner, unseen recall, hardware latency or 100M capacity.
