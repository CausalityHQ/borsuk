# OPQ8 proofs

Run `lean Opq8Planner.lean` from this directory with the pinned Lean
toolchain. The file imports only `Std` and contains no admitted theorems.

The model covers the 100k one-GET-per-group planner. It proves group
selection order, uniqueness under a unique input ranking, exact byte
accounting and the 32-read/16-MiB caps. It also proves one pairwise
fixed-point score-order lemma, truth-owner counting lemmas conditional on
the owners already being certified as selected, and 8-byte code-plane
arithmetic. It assumes valid group lengths and an authenticated ranking.
The file also proves the budget bound for an abstract 1M planner that
recounts merged GETs from sealed physical predecessors, including a
bridging example where three groups need one GET. It does not prove that
the Python incremental merge counter equals this recount, or that the
predecessor map was constructed correctly. Python refinement and
numerical certificates for actual queries remain separate work.

The 100M arithmetic theorem adds a conditional memory result: if two
complete route-code planes, a float64 score array and a float64 indexed
lookup array coexist, their `32N` bytes exceed the 3-GiB campaign cap
with its 64-MiB allowance at 100M rows. It excludes allocator and
metadata overhead; it is not a latency theorem.

For a future data-range implementation, a separate conditional theorem
combines a 32-GET/16-MiB plan with certified per-GET, per-byte and local
compute upper bounds into a sequential latency ceiling. A region-work
theorem similarly converts a certified visited-row cap into at most eight
table lookups per visited row. These results provide arithmetic implications;
the bounds and implementation correspondence must be supplied and checked
for any production claim.

`SourceRangeFidelity.lean` adds paired finite-cohort hit accounting:
authenticated source/compressed hit pairs and bounded lost hits imply
the 100k and 1M aggregate fidelity floors. It separately proves the
96-byte sign/PQ record arithmetic, a conditional 16-MiB **code** payload
row bound, 94 sign or 96 PQ table lookups per fetched row, and 9.6 billion
PQ lookups for a full 100M-row scan. It proves page-minimum score order
when every row's score error is bounded and two true page minima differ
by more than twice that bound. Actual hit pairs, score-error bounds,
Python refinement, S3 service times and unseen-query recall are explicit
external premises. In particular the checked lookup count is a work
bound, not a latency or scalability claim.

The threshold-admission theorem also proves exact equality of the
admitted page list and its truth-hit count when every competing page
has a certified absolute score-error bound and its true score lies
outside that error band around a fixed admission threshold. This is a
conditional finite-query recall certificate: the threshold, page scores,
truth-owner counts and error bounds must be authenticated, and a concrete
quota/range planner must be proved to implement the threshold model.
It cannot certify the failed sign96 or PQ96 development runs merely from
their aggregate error statistics.
