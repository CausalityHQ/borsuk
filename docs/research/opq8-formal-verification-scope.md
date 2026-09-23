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
in-flight query. During each table lookup, NumPy advanced indexing can
hold an additional float64 array of `8N` bytes alongside the float64
score array. If both full code generations are resident, their `16N`
bytes plus these two simultaneous `8N` arrays total `32N` bytes:
3,200,000,000 bytes at 100M rows. Lean proves this exceeds the campaign's
3-GiB cap with its required 64-MiB margin even before metadata and
codebooks. This is conditional on those arrays being resident as modeled;
it rules out the current full-array scorer under that cap and motivates
chunked scoring for memory. A route query evaluates
eight codebook lookups per row plus group reduction and ranking. These
are useful analytical resource bounds, but they do not
establish a wall-clock latency.
The checked full-scan model counts 800,000,000 table lookups at 100M
rows per query, assuming the current eight-code-row scorer scans every
row. This is a work count, not a measured throughput or a lower bound on
time without a machine-rate assumption.
Chunking alone leaves this full-scan work count unchanged. V76 measured
64 million lookups at about 206 core-milliseconds per 1M-row query for
a different flat router, evidence that a region or hierarchical route
must be measured before claiming 100M throughput. That historical rate
is not a bound for the current OPQ8 implementation.

For the proposed data-range wave, Lean also proves a conditional sequential
latency ceiling: if the final plan uses at most 32 GET ranges and 16,777,216
encoded bytes, and authenticated service evidence bounds each request's fixed
overhead, each byte's transfer cost, and local compute, then total latency is
at most `localTime + 32*requestTime + 16777216*byteTime` in the common time
unit. A separate theorem bounds eight-table routing work by `8*regionCap`
when an implementation certifies it visits at most `regionCap` rows. Neither
the remote service bounds nor the visited-row cap has been established for
production; these are proof obligations for a measured implementation.

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
a cohort theorem that sums conservative per-query certificates, and the
8N/16N code-plane arithmetic. If the certificates contain at least
98,151 positions, the checked theorem proves the 1M control's aggregate
selected-group containment threshold under those premises. The actual
frozen 1M outcome was established by the terminal-closed data replay,
not by filling these certificates. The certificate theorem assumes valid
owner lists and coverage; it does not authenticate artifacts, establish
a 1,000-by-100 query/truth shape, prove p05 or GT10 tails, or show
nominated-page containment. Those checks must be separate inputs to an
end-to-end proof. The earlier 1M **group-score** source-distance arm used
a float32 matrix product before conversion to float64, so its scores
are not an exact-arithmetic witness without a rounding allowance.

The structural `List.foldl` consumes one ranked group per recursive
step. A second abstract model
counts 1M merged GETs as selected groups whose same-role physical
predecessor is absent. It proves a 32-GET/16-MiB bound and checks a
three-group bridging example. The supplied physical predecessor map and
the Python planner's incremental count still need a refinement proof.
Neither model has been composed with routing and recall.

For latency, a conditional service model can state that a single parallel
wave of at most 32 successful GETs finishes within `L` if each GET finishes
within `L` after launch, the scheduler starts all 32 together, and local
planning, scoring and decoding finish within a stated CPU bound `C`.
Then end-to-end successful-query latency is at most `C + L` plus any
explicit queueing and data-page waves. This is a proof about those
assumptions, not a measured bound for S3: network tail latency, throttling,
retries, queueing and CPU rate have no fixed values supplied by the
algorithm. A timeout can bound time to success-or-failure, but cannot
guarantee successful recall. The 1M source-only run supplied no serving
latency samples, so actual-read and serving measurements remain required.

The proof has no `sorry`, but the refinement from production Python to the
Lean model, the top-four score error bound, plan stability, page
nomination and per-query numerical certificates are still open. The
integer margin theorem requires outward-rounded score intervals and an
authenticated check of their premises. Model, code and object bytes beyond
the eight-byte route plane remain outside the code-plane arithmetic. Run
the empirical 1M, serving and 100M resource gates separately.

## Source-range fidelity budget after the closed 1M diagnostic

The terminal-closed source-vector final-range diagnostic retained
98,920/100,000 GT100 and 9,956/10,000 GT10 positions under the fixed
32-GET/16-MiB budget. It converts stored float32 source/query coordinates
to float64 before the squared-L2 norm-plus-dot calculation and records
the BLAS implementation in its sealed plan. It remains an operational
finite-precision scorer, not a real-arithmetic oracle.

`formal/SourceRangeFidelity.lean` checks with Lean 4.33.0. It models
each ordered truth position as a pair of source-score and compressed-score
hit bits. It proves that source hits cannot exceed compressed hits plus
positions lost by compression. For an authenticated 100,000-position
GT100 pairing, at most 769 lost source hits implies the fixed 98,151
aggregate gate. For the 10,000-position GT10 pairing, at most 28 lost
source hits implies the 9,928 gate. The theorem also requires separate
certificates that p05 is at least 90 and no more than 49 queries fall
below 90. Aggregate loss bounds alone cannot prove these distribution
conditions.

The same file now proves a per-query lower-tail implication: for 100
paired truth positions, if the number of source hits covers 90 plus
all source-to-compressed losses, compressed containment stays at least
90. This can support a machine-checked count of at-risk queries once
the paired hit masks are authenticated. It also checks the proposed
96-byte sign record's arithmetic: 752 sign bits occupy 94 bytes and a
binary16 scale occupies two more; 100M records occupy 9.6 billion bytes
before headers, replicas and metadata. **If a separate** 16,777,216-byte
code-payload budget is imposed, any group payload with four-byte page
counts contains at most 174,762 such rows. The data-range 16-MiB cap
does not imply this code-fetch premise. This is a conditional row-count
bound for code bytes fetched in that wave, not a bound on rows scored,
CPU time, resident memory or storage-system latency. The proposed
two-scalar variant is 98 bytes and
does not satisfy this record format.
The same conditional code-payload premise limits the 94-byte-table
scorer to 16,427,628 table lookups, after 24,064 per-query table entries.
These are operation counts, not elapsed-time bounds.

The file now also proves a score-based, non-recall premise: if every
integer-scored row of a page is within `epsilon` of its true score, that
page's minimum compressed score is within `epsilon` of its true minimum.
If true minima of two pages differ by more than `2*epsilon`, their
minimum-score order is preserved. This follows through arbitrary page row
lists by induction, without assuming the answer's hit count. To turn it
into a cohort recall certificate, the campaign must authenticate
outward-rounded score-error intervals for the actual finite-precision
scorer, separately certify the top-100-row count part of priority, and
prove that the pages retained by the **actual** merged-range
planner include enough truth owners. A uniform worst-case bound may be
too loose to certify many queries; query-specific bounds can be checked
without changing this implication.

This is a conditional finite-cohort claim. A future checker must bind
both hit-mask lists to immutable source, query, truth, selected-range
and scorer identities, and prove or validate the executable-to-model
mapping. The current Lean file does not authenticate those bytes or
establish unseen-query recall. A useful pre-truth score-error proof
would additionally have to certify per-row or per-page score intervals
and show that the compressed priority/admission plan preserves enough
source-score truth-owner pages. S3 latency, serving throughput and
100M work still require their separately stated service and index
premises.
