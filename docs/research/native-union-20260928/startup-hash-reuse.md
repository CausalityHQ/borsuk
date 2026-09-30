# Graph decode identity reuse, 2026-09-30

One native source-line change: graph header digest compares with the immutable
scorer's stored blob digest, followed by the existing scorer-to-input-blob hash
check. Previously both checks hashed the same supplied centroid blob.

Let A=graph header digest, B=scorer digest, C=hash(input blob). Previous admission
A=C and B=C is equivalent to new admission A=B and B=C. Tampered blob, foreign
scorer and mismatched graph remain rejected. Graph geometry/adjacency decoding,
scoring, format, allocation cap and all callers remain unchanged. Both two-bit
graphs now hash once each instead of twice. This is source-level work reduction,
not a measured speedup or published-target attainment.

Original local test25093 CLOSED0:
`CARGO_TARGET_DIR=/data/target/agents/borsuk-ranges-20260930 cargo test -p borsuk --lib unit_centroid_graph::tests`
Seven tests PASS: tampered centroid/scorer rejection, allocation cap before
adjacency, round trips/order, partial final page, bounded seeded search and
reachable duplicate centroids. No full suite, ARM build or paid benchmark.

Current native source epoch differs from qualified binary3d96aa35/source0525.
All closed cold/ranges/load evidence remains attached to its exact previous
binary; no new quality/latency claim. Requalify changed native source before a
future candidate campaign. Do not launch a separate campaign for this tiny
change alone. Measured remaining startup costs (~1.08s staging/~650ms decode)
require a substantive next reduction to close the BOTH-vendor cold gap.
