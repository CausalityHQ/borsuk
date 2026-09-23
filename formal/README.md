# OPQ8 proofs

Run `lean Opq8Planner.lean` from this directory with the pinned Lean
toolchain. The file imports only `Std` and contains no admitted theorems.

The model proves group selection order, uniqueness under a unique input
ranking, exact byte accounting, the 32-read/16-MiB caps, a conditional
fixed-point score-order lemma, certificate-based recall bounds, and 8-byte
code-plane arithmetic. It assumes valid group lengths and an authenticated
ranking. Connecting the Python planner to this model and constructing
numerical certificates for actual queries are separate work.
