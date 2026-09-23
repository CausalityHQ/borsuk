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
