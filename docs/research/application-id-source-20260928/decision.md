# Logical application-ID construction: shipped prerequisite

## Decision

Expose signed-i64 application IDs independently of source-row ordinals through
`build_sq8_source_with_ids`, `TwoBitSource::build_with_order`, and
`TwoBitGenerationBuilder::build_with_order`. Reuse the existing encoders,
generation publication and ranker; do not introduce a resident ID map.

Source-plane schema is v2 for every new build and records the SHA256 of the
canonical physical-position-to-source-row permutation (little-endian u64).
The reader rejects v1. The generation root fields are unchanged; its authenticated
child receipt/body hashes change. Historical v1 evidence remains immutable and
is not a measurement of this revision. The caller must supply immutable matching
raw/SQ8/order/ID snapshots; hashes authenticate bytes, not their semantic relation.

## Original verification

Spark aarch64, two Cargo build jobs, 600-second command limits. Original red
exit101 proves the required API was absent. First green exit0; then expanded
application-ID integration, existing source and generation integrations exit0
(four tests), and focused SQ8 unit tests exit0 (two tests).

The expanded 512-row D2 fixture checks signed-i64 extremes, reverse physical
order, duplicate IDs/order and payload rejection, old-format rejection, local
open, publication, authenticated remote reload, and exact returned-range logical
ID ranking. Same-order ordinal control has identical source-order/mean/record
hashes, showing IDs do not change vector nomination encoding. A malformed but
hashed SQ8 duplicate-ID body rejects before successful receipt.

[Original log hashes/exits](verification.json), [source parity](source.sha256).
Logs are losslessly gzipped; hashes describe decompressed bytes. Final rustfmt
edition2024 and diff-whitespace checks pass. The only post-test source change
is a struct documentation comment. No local compiler, cloud job or full suite
was launched for this slice.

## Scope and next gate

This closes numeric-ID binding only. String-ID catalog, incremental insert/update/
delete, replay/recovery and self-contained compaction are still missing from
this object-native path. Next implementation decision is to reuse existing
mutation-version/publication machinery for a bounded authenticated overlay,
with a runnable create/mutate/reopen/search check before paid measurements.

No recall, cold latency, QPS, RSS or vendor superiority is measured here. Both
matched vendor comparisons, a winning quality candidate and 100M/lifecycle
qualification remain OPEN in the [acceptance matrix](../../release/competitive-acceptance.md).
