# V138 source-only unit-bound feasibility preregistration

**Decision question:** can sound center/radius bounds select every unit that
could beat the current 100th witness within 32 S3 GETs and 16,777,216
bytes on both development corpora? This is a fail-fast architecture screen
for a generic selective range policy. It does not measure returned recall,
live S3 latency, or a production default. V132 already rejected the
unchanged broad S3 schedule; V137 found unvoted-page misses on D96 100k
and cap-dropped voted pages on D768 1M. No GT is read in V138.

## Frozen inputs and geometry

Use the complete V122 **deep-image-96-angular random 100k train subset**,
already-used publication-test queries 9000–9999, and complete V116
**ReLAION-1M**, already-used validation-1000. These are different historical
layouts and do not qualify a matched method; the screen asks whether the
same bound construction is even feasible on both. The immutable inputs are:

| Role | Immutable S3 source | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| D96 query | V122 a0001 `artifacts/queries.jsonl` | 2,041,773 | `dd95571cc7c333f331b8c4c0b55070b366b389508b0e195e0e56d5982177da99` |
| D96 primary | V122 a0001 `artifacts/evidence.jsonl` | 6,183,526 | `deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6` |
| D96 SQ8 | V122 a0001 `artifacts/built/sq8.bin` | 10,800,000 | `c20dcb8058d2409791c6c584d9f078491d4c239acbe7c19350be7757d533e8df` |
| D96 low/step | V122 a0001 `artifacts/router/low.bin`, `step.bin` | 384 each | `3fd035c242b99b6835727faa49f0dc52c77979b8c900980c01fcf23412222c9e`, `bd6a8adb231b89d18ad1edf72cff043512d663117955a826bd8ca11224d02038` |
| D768 query | V116 a0001 `artifacts/requests.jsonl` | 18,726,909 | `c250ef3c871af55ee1ea91e61214a93903b3d575ef458926b1f8c5ad0547b2c9` |
| D768 primary | V116 a0001 `artifacts/rust-replay.jsonl` | 13,455,525 | `3bfd155ac5f9e1b7aacbc263e1732e2314c9722f235d3454e0f17d0c6bc3c960` |
| D768 SQ8 | V70 single-stage `index/sq8.bin` | 780,000,000 | `2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b` |
| D768 low/step | V115 router `low.bin`, `step.bin` | 3,072 each | `ce036f48f918312f694adbd12994646987eb0e144df20f64ffe583bed0d1f891`, `64d49c7413f163fb3446183629d743f69199156f208fa959523d1bcde27da69c` |

Build 32-row physical units directly from the source-derived SQ8 object.
For each unit, reconstruct SQ8 rows, compute their arithmetic centroid and
maximum Euclidean radius, then inflate that radius by the conservative
quantization envelope `sqrt(sum((step_i/2)^2))` plus a recorded floating
roundoff margin. Unit summaries use no query or truth input. For every
query, compute exact Euclidean distances from the query to each resident
unit centroid. Let `T` be the largest SQ8-reconstructed distance among the
100 sealed primary rows plus the same error envelope; this is a conservative
upper bound on the exact-source 100th witness distance if the SQ8 error
assumption holds. A unit is promising when
`max(0, distance(query, centroid)-radius) <= T`. All rows in that unit share
one physical 256-row page. Convert promising units to unique pages, then
calculate the minimum bytes required to cover every promising page in at
most 32 disjoint contiguous ranges by bridging the smallest page gaps.
Charge the short final page exactly. Do not use GT or returned IDs in this
selection or threshold.

Report per corpus per-query promising units/pages, fraction of all units,
minimum 32-range bytes, number of merged ranges, and fraction under the
16,777,216-byte cap; also p50/p95/p99 and maximum. Preserve 1,000 raw rows
per corpus. A source-only 32-row summary payload is `units*(2*D+4)` bytes
with f16 centroid and f32 radius, excluding allocator, graph, cache and
scratch. Measure actual worker RSS and resource use separately. The
conditional metric, top-k and byte-budget arguments are checked in
`formal/UnitBoundAdmission.lean`; SQ8 quantization and Rust/Python
refinement are not established by that theorem.

**Stop rule:** on D96 100k, reject if mean minimum bytes exceed **4,714,063
bytes/query**, half the existing V132 candidate's measured mean S3 response
bytes (9,428,126.976). The entire D96 100k object fits the 16-MiB cap,
so a cap-only screen there would trivially pass an unselective policy. On
either corpus, also reject if more than 5% of queries need over 16 MiB
even after optimal gap bridging to 32 GETs. Rejection means exact
bound-gated admission is not the primary selective serving policy; it may
still be useful as an optional certificate or score feature. A passing feasibility screen
permits a separate, preregistered returned-quality and live-S3 test; it
does not itself qualify one. Either way, the O(ND/32) flat unit scan must
be replaced by a hierarchy before 10M/100M.

Run one frozen Causality Spot attempt with a 5,400-second deadline. Install
NumPy on the worker, authenticate every input and source archive, monitor
infrastructure/terminal only while incomplete, sync terminal-listed raw
artifacts, discard an interrupted cell, and terminate compute immediately
after terminal. The first corpus is fail-fast: if the D96 stop rule is met,
record that negative result and skip D768 download and computation in the
same terminal. Do not start an overlapping attempt.

## Input-isolation amendment after a0001

a0001 parsed a V122 evidence record containing GT IDs, contrary to the
no-GT-input statement above; see `v138-a0001-gt-input-failure.md`. That
attempt is claim-ineligible. The a0002 rerun uses a frozen **primary-only**
file derived from authenticated V122 evidence, SHA-256
`04cdbea9079837806059799d9a90c2f579526de100b7743f83a3794a15c0d6e3`.
The evaluator will load this file instead of V122 `evidence.jsonl`, and
the Spot worker will not download the GT-bearing file. All algorithmic
choices, screening thresholds and the other frozen inputs remain as above.
