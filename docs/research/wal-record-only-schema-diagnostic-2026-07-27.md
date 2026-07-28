# WAL record-only schema diagnostic — 2026-07-27

Status: diagnostic evidence only. It does not promote a production format and
must not be used as a publication comparison.

## Frozen predecessor

The completed predecessor is
`wal-layout-qualification-20260727-v3`, source archive SHA-256
`4407305c9c5f5bd9b181f273847c97e320e2e7108f2255adcd689fc5f77254d9`,
dataset-identity SHA-256
`be7912fb8c69f54200b77dad1d123afd718505f25149633ba0817ceae88b0e1c`.
Its 220 paired cases measure the old v13 WAL schema and remain immutable. No
case may be reused after the schema change.

The frozen validator and analyzer independently reproduced the remote
artifacts byte-for-byte. `qualification-cases.csv` has SHA-256
`69c2398c88700488357eed88ec345ee7aff1fef6fc42182a9a21764e4788f928`;
`wal-layout-decisions.csv` has SHA-256
`22e3a105807cb6ae12d9831edc33944ba93e4af76eee3c4db21ce51799b59ebc`.
The official v13 decision is to retain the Parquet baseline.

## Root-cause evidence

The v13 WAL writer reused the normal-segment Arrow schema even though live-tail
search exact-scores the primary vector and never consumes the segment header,
routing score, or PQ/TurboQuant code. On a 500-row, 784-dimensional
Fashion-MNIST run, the `pq_code` column alone occupied 394,160 bytes when
written as compact Vortex.

Three exact captured v3 Vortex objects were decoded and re-encoded with the
ignored `captured_wal_object_reports_layout_and_column_sizes` probe:

| Corpus/object | Captured bytes | Default Vortex | Compact Vortex | Default vector | Compact vector |
|---|---:|---:|---:|---:|---:|
| Fashion, small compact case | 683,264 | 1,078,976 | 683,264 | 446,464 | 195,960 |
| Fashion, largest compact case | 1,115,672 | 1,087,424 | 1,115,712 | 446,464 | 627,072 |
| GloVe | 189,840 | 230,376 | 189,864 | 154,056 | 129,920 |

The Fashion result is bimodal: neither Vortex compressor preset dominates
per object. Across all ten captured Fashion objects, choosing the smaller
Default/Compact result per object reduced 7,813,280 bytes only to 7,785,080
bytes, still far above the 6,802,467-byte Parquet predecessor.

Encoding only the f32 vector field as per-row binary forced compact Vortex's
binary/ZSTD path and removed the pathological Fashion spikes:

| Diagnostic | Ten Fashion objects |
|---|---:|
| Captured compact Vortex | 7,813,280 bytes |
| Compact Vortex with binary vector field | 6,811,368 bytes |
| v13 Parquet predecessor | 6,802,467 bytes |

That experiment reached only Parquet parity and made the captured GloVe object
larger (247,088 versus 189,840 bytes). It therefore was not adopted.

Captured object SHA-256 values:

- Fashion small: `59d24ddaa5dc74f9eef2ac1605ddd2ee38ec824103211d25a8134c78b9aebec2`
- Fashion largest: `ffa159c7bebe17d6e3f12d275877172b7bb18124a502802d2804e51388362ca2`
- GloVe: `f77f2becc2b5968232b792cd1bb6c8b9d44ccedb300ffb7f96092df9ae17b360`

## Implemented candidate

Table format v14 introduces one record-only WAL batch shared by the Parquet
and Vortex writers. It retains IDs, metadata, optional sparse/text/generation
columns, the exact typed primary vector, named record payloads, and explicit
element-type/dimension constants. It removes the unused normal-segment header,
routing score, and PQ code. The decoder rejects missing or inconsistent
type/dimension constants and unpaired sparse/text columns.
Foreground WAL publication now passes records and dimensions directly to this
codec; it no longer builds centroid, radius, routing, or PQ derivatives merely
to discard them.

The schema test was observed failing against v13 fields before the v14 writer
was implemented. Rust library, FP8, complete feature-matrix, local-index, and
strict scoped clippy checks then passed with v14.

## Exact-slice smoke measurement

The first 500 rows of each frozen real training input were copied byte-for-byte
from the qualification host:

| Slice | Bytes | SHA-256 |
|---|---:|---|
| Fashion-MNIST 784D | 1,568,000 | `4240847a55797cf0f515e1bd2ff490a12e44c6b59a7f8fbcc0b654a7f0043b29` |
| GloVe 100D | 200,000 | `7dcc97021f43e4ecee5b6374b8d072262689b43d4bec572f1046820fbd53964a` |

One local release-mode run per arm produced:

| Corpus | Parquet WAL bytes | Vortex WAL bytes | Vortex / Parquet |
|---|---:|---:|---:|
| Fashion-MNIST | 410,949 | 284,528 | 0.692 |
| GloVe | 300,733 | 134,664 | 0.448 |

This smoke test establishes that removing dead columns materially changes the
candidate. It is not a latency result: one local repetition cannot satisfy the
preregistered median/bootstrap/resource gates.

## Exploratory v14 policy screen

A separate local end-to-end screen ran three clean paired repetitions for every
primary type with 5,000 rows, 96 dimensions, 500-row WAL objects, 100 warm
queries, reopen, and flush. This screen selects the candidate to send to AWS; it
does not promote a default.

| Primary type | WAL bytes Vortex/Parquet | Ingest | First query | Warm p95 | Warm p99 | Flush |
|---|---:|---:|---:|---:|---:|---:|
| f32 | 0.296 | 0.981 | 0.917 | 1.001 | 0.998 | 0.991 |
| f16 | 0.174 | 1.006 | 0.978 | 1.011 | 1.013 | 0.981 |
| bf16 | 0.224 | 0.993 | 0.991 | 0.999 | 1.018 | 0.962 |
| FP8 E4M3 | 0.346 | 0.975 | 0.956 | 0.836 | 0.970 | 0.963 |
| FP8 E5M2 | 0.326 | 1.016 | 1.013 | 1.001 | 1.008 | 1.010 |
| int8 | 1.000 | 0.975 | 1.019 | 1.012 | 1.000 | 1.007 |
| binary | 0.278 | 1.004 | 1.030 | 1.000 | 1.006 | 1.032 |

Because the production objective requires a latency win as well as smaller
objects, the confirmatory candidate specializes only f32 WAL objects. Even
FP8 E4M3's 0.956 first-query ratio missed the preregistered 0.95 ceiling in
this screen, so it remains a Parquet control with every other non-f32 type.
Correctness remains broader than this performance specialization: the
forced-Vortex end-to-end matrix still covers every primary type plus metadata,
dense named vectors, sparse named vectors, late interaction, and text across
add, reopen, search, and flush.

## Confirmatory decision

The v4 source was frozen at SHA-256
`e5f0b6f15f0550aa817f3322ef5b30ead4aa89e1959cc405db3ed52c5d2cede1`,
but its complete 220-case campaign was not executed before the write protocol
changed again. Table format v16 replaces per-record MVCC generation objects
with sixteen fixed generation shards and publishes all runs prepared for one
cell lane with one conditional lane-head update. Those changes alter the
foreground request and latency distribution, so v4 is
`invalidated-before-execution`; its diagnostic rows are not qualification
evidence and no case is reused.

Campaign `wal-layout-qualification-20260728-v5` freezes the same paired
five-repetition AWS matrix against the complete v16 protocol. It completed all
220 scheduled cases on `c7g.8xlarge` in `eu-central-1`. The frozen analyzer
rejected the Vortex rule globally: although some Vortex objects were smaller,
the latency, confidence, CPU, RSS, and storage promotion gates did not all
pass. Parquet therefore remains the frozen automatic WAL default.

The exact source archive has SHA-256
`3b107a31a21375aae7301562c24744e2a6412849f6ebf8a1f0b30c75354988cb`;
the regenerated qualification cases and decisions have SHA-256
`c78f8dd5feb4bd5147831eaddd6ba4c3fc6638981444a6598b1256051f728322`
and
`963ee72d803b5d4a7f6bb1b34a298ac52dabd38855ad669c0877642ef38bdfd8`.
A fresh download of the source and results regenerated both files
byte-for-byte. The compact decision registry is
[wal-layout-qualification-v5-decision.json](wal-layout-qualification-v5-decision.json).
