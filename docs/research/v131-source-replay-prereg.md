# V131 production-Rust paired source replay preregistration

V130 stopped before a quality measurement because its validation demanded
the **ordered** SQ8 top-100 list reproduce V122's NumPy float32 matrix
scorer. The sealed V122 query 1 capped-control top-100 *set* matched the
production Rust scorer exactly, but two internal ranks swapped at Rust
scores 0.91336000 and 0.91336024. This is a mismatch of scorer arithmetic
and the cross-implementation ordering contract; it does not establish a
source-tier quality result. V130's three terminal attempts remain immutable:
`a0001` compile error, `a0002` copied source-digest error, and `a0003`
ordered-parity failure. No V130 attempt is promoted.

V131 asks a revised, generic question: with frozen V122 physical ranges and
nominees, how do production Rust SQ8 top-100 and its top-512-plus-router
exact-source union compare on both candidate and capped-control arms? The
production Rust SQ8 scorer is the **same-run baseline** for source quality.
V122's sealed Python SQ8 totals (99,106 candidate and 98,203 control hits
out of 100,000) remain authenticated historical context, not asserted as an
identical scorer. V131 records every query's Rust SQ8 top-100 IDs and hits,
plus ordered and set deltas against V122, without dropping or retuning
queries. This is the already used deep-image-96 100k subset and test ordinals
9000–9999, so it is development evidence only.

The worker authenticates all eight V122 inputs by terminal-listed SHA-256
and length, checks the 1,000 query/truth/evidence identities and sealed V122
SQ8 totals, then converts source Parquet to F32 rows. The production source
writer, source-ID map, `rank_returned_ranges`, and exact scorer run with
512 router nominees, top-512 returned expansion, 65,536-byte local
verification blocks, the original V122 physical ranges, and a 32-GET,
16,777,216-byte cap. The same settings apply to both arms and do not vary
with dataset or vector count. Arm order alternates by query ordinal.

Promotion gate: production candidate SQ8 returned Recall@100 at least
99.0%, source candidate Recall@100 at least 99.5%, source candidate at least
the same-run source control, p05 source hits at least 98, and no source
candidate query below 90 hits. Local source-rank p95 must be at most 30 ms
and maximum logical authenticated local bytes at most 32 MiB. These are
provisional local headroom screens, not live S3 latency or physical SSD
I/O claims. Any failed gate leads to root-cause analysis and a revised
implementation before scaling.

Run one immutable Causality `c7i.8xlarge` Spot cell with a 90-minute wall
deadline. Discard and restart a cell interrupted before its terminal
marker. Upload per-query evidence, source/map artifacts and hashes, resource
logs, summary and terminal to S3; terminate the instance at terminal.
