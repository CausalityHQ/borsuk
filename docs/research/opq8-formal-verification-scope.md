# OPQ8 formal verification boundary

The 100k OPQ8 router and paired two-bit reader have two kinds of claims.
Formal verification can establish planner and format invariants for every
input satisfying explicit preconditions. Recall, throughput and remote
latency need observed data and service measurements.

## First Lean target: the fixed 100k group planner

Model a finite ordered list of distinct group ordinals, a positive byte
length for each sealed group, a count cap of 32 and a byte cap of
16,777,216. The production 100k planner scans this list in order, accepts
a group exactly when its length fits the remaining byte budget, and stops
after 32 acceptances. The Lean model continues scanning the remaining list
without accepting more groups. Prove:

1. Every selected ordinal occurs exactly once in the input and selected
   ordinals preserve input order.
2. The selected count is at most 32 and the sum of sealed lengths is at
   most 16,777,216, including when a too-large group is skipped.
3. The planner terminates after at most the input length iterations.
4. Each planned read uses the offset and length bound to its selected
   ordinal in the authenticated group manifest. The broker admits only
   these manifest coordinates; returned bytes must match their digest.

The manifest uniqueness, positive lengths, in-bounds offsets, finite
scores and authenticated input identities are explicit preconditions.
A proof of an abstract planner does not prove the Python or future Rust
implementation follows it. Keep executable conformance tests and a small
translation or refinement check between the proved function and production
code.

## Other candidate proofs

- Source ordinals form a permutation; every physical row belongs to one
  page and one group, so a selected group's containment count is exact.
- Lexicographic ties on row scores and group ordinals yield deterministic
  ranking when scores are finite.
- A generation seal binds the source identity, model, physical order, group
  manifest and incompatible format marker. Readers reject a mismatched
  seal before serving results.

## Conditional recall theorem to be formalized

For one query, define the true row distance `d(r)` and the row routing
distance `a(r)`. Assume a certified uniform bound
`|a(r) - d(r)| <= epsilon` for every row. The mean of the four smallest
distances in a group is 1-Lipschitz under this uniform error, so the
corresponding true and routed group scores also differ by at most
`epsilon`. A gap above `2*epsilon` between every pair of distinct groups
is a sufficient condition for the full route order to match the true-score
order. It is likely too strong to certify useful 1M queries. A practical
certificate should check the smaller set of comparisons needed to preserve
the actual selected plan, including skip decisions. If that true-score
plan contains the owners of all GT100 rows, the route contains GT100.

The same argument applies to the two-bit row scorer with its own certified
distance-error bound. If its true 100th and 101st selected-row distances
have a gap above twice that bound, its top 100 rows are unchanged. If
their distinct owner pages number at most 32 and their total planned
bytes fit 16,777,216, the existing page nomination rule includes all
their owners. These conditions together imply perfect GT100 page
containment for that query. We can also prove a weaker lower bound by
counting individually certified truth positions when the full-margin
condition fails.

This proposed theorem is intentionally conditional and is not yet checked
end to end in Lean. Its data premises can be
checked against frozen source rows, queries, codebooks and page sizes,
yielding a small per-query certificate and a mechanically checked count
of certified queries or GT positions. Computing and authenticating those
premises still requires dataset work; a distributional assumption alone
cannot establish the observed cohort's recall. Float32 rounding,
non-exact stored rotation and the precise scorer expression must either
enter the error bound or be modeled at their actual finite precision.

One way to discharge the routing-error premise is to measure each row's
rotated reconstruction residual `e = ||(x-mean)R-c||` and bound the stored
rotation's Gram defect by `gamma = ||R Rᵀ-I||₂`. In exact arithmetic, with
`z=(q-x)R`, the ADC-to-true squared-distance error is at most
`2||z||e + e² + gamma||q-x||²`; a separately verified finite-precision
allowance must cover table construction, summation and final float32
rounding. Per-row intervals can feed an interval-order checker instead
of assuming one loose global `epsilon`. Constructing those certificates
still reads source data and measures residuals and margins; Lean can
verify the implication from authenticated numbers.

The current 8-byte row code plane occupies exactly `8N` bytes for `N`
rows; two full generations occupy `16N` bytes. At 100 million rows that is
1,600,000,000 bytes before the 3,148,820-byte model per generation,
liveness, metadata, query tables, runtime and allocator reserves. The
current `row_adc_scores` also allocates a float64 score array and a
float32 returned array, about 1.2 GB in aggregate at 100M rows per
in-flight query, before temporary lookup arrays. A route query evaluates
eight codebook lookups per row plus group reduction and ranking. These
are useful analytical resource bounds, but they do not
establish a wall-clock latency.

Orthogonal rotation preserves squared L2 distance in exact arithmetic.
Finite precision and 8-byte quantization can change neighbor order; a
rank-stability theorem would need bounds on quantization error and on the
distance margin of the actual queries. Neither a worst-case proof nor a
complexity bound gives the cohort's recall. Similarly, object-store GET
latency and Spot interruptions are external behaviors with no finite
worst-case bound supplied by this algorithm. Keep paired, terminal-closed
measurements for quality, latency, throughput, memory and cost claims.

The 100k paired quality gate selected this OPQ8 route for the next 1M gate.
`formal/Opq8Planner.lean` now checks with Lean 4.33.0 and proves that the
abstract **100k** admission loop stays within 32 reads and 16,777,216 bytes,
preserves input order and uniqueness, and accounts for exactly the selected
group lengths. It also proves a pairwise integer interval order lemma,
counting lemmas conditional on already certified truth-owner coverage,
and the 8N/16N code-plane arithmetic. The structural `List.foldl`
consumes one ranked group per recursive step. The 1M planner merges
adjacent selected groups into GET intervals; its GET bound and its
composition with routing/recall have not been mechanized.

The proof has no `sorry`, but the refinement from production Python to the
Lean model, the top-four score error bound, plan stability, page
nomination and per-query numerical certificates are still open. The
integer margin theorem requires outward-rounded score intervals and an
authenticated check of their premises. Model, code and object bytes beyond
the eight-byte route plane remain outside the code-plane arithmetic. Run
the empirical 1M, serving and 100M resource gates separately.
