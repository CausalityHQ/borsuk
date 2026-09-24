# V126 ReLAION expansion-union source-score closeout

Status: sealed **development diagnostic**, not a fresh validation or a
production default. The ReLAION-1M validation-1000 split was previously used
by V116 and V124. The query-blind V116 routes, expansion ranges, SQ8 object,
512 router nominees, and 32-GET/16,777,216-byte caps were held fixed. Both
arms used the same top-512 SQ8 replay width and original-source cosine
scorer. No result here measures live S3 query latency or serving RAM.

## Identity and verification

The successful Causality Spot attempt was `a0002` under
`s3://borsuk-bench-453182569524-euc1/research/v126-relaion-expansion-source/c334ec638932f579f2ee62766d24c722c7c9b07c/runs/v126-20260924T041131Z/`.
It ran committed revision `c334ec638932f579f2ee62766d24c722c7c9b07c`
from the complete source archive SHA-256
`497b3f8cdb1110d0bbff918b0726672cf71bb9fe9218520e7a5523280ee4b9db`.
Its complete terminal SHA-256 is
`f1be459e9d5168853248cb6679a7f100f059b5356f4e62f3974f5764c8ac9dc4`.
The terminal reported `complete`, exit 0, and instance
`i-0429eb4b306930e5e`, which was independently observed **terminated**.
All 16 terminal-listed artifacts were downloaded and checked against their
recorded byte lengths and SHA-256 values. The summary is SHA-256
`885d3779fa61c9cfeed0c3a285da82832b1eec78adc6044b7372b0d8d2e9eaee`;
the 1,000-row evidence is SHA-256
`3c674ee0368d5c1592ad8b79e8157e13c66cf61e39470b36b3dab44781b96182`.
Independent recounts of the evidence matched the summary's per-arm totals,
p05 values, maxima, and sub-90 counts. Every row had 100 unique returned IDs,
a unique candidate union, and GET/byte counts within the physical caps.

The worker passed its two focused Python contracts and a complete replay
preflight before downloading GT. It reproduced V124's nominee-only capture
of 96,849 and exact-source returned total of 96,813, plus V116's original
SQ8 totals of 99,208 candidate and 98,618 control. Every widened replay's
first 100 IDs and route/plan fields matched the sealed V116 replay.

Attempt `a0001` ended at compile with exit 101 because its slim source
archive omitted the production Rust modules referenced by the V114 replay
binary. Its terminal SHA-256 is
`710f4f85e2cbe5c2212a3eb3936b8e42b1b6feb13f2bcaae78ef34f5ef0f7d65`;
instance `i-0aaf7271b3968ceb1` was observed terminated. It produced no
quality measurement. The complete archive fixed the packaging error for
`a0002`; the evaluator and preregistered method did not change.

## Paired result

All recalls below are GT hits out of 100,000 positions across 1,000
Recall@100 queries on **ReLAION-1M validation-1000, already used**.

| Same fixed route/ranges | Candidate | Capped control |
| --- | ---: | ---: |
| V116 SQ8 top-100 baseline | 99,208 (99.208%) | 98,618 (98.618%) |
| Top-512 union GT capture ceiling | 99,652 (99.652%) | 99,517 (99.517%) |
| Original float32 source, float64 cosine top-100 | **99,563 (99.563%)** | 99,432 (99.432%) |
| Float16 round-trip, float64 cosine top-100 | 99,563 (99.563%) | 99,432 (99.432%) |
| Exact-source p05 hits per query | 98/100 | 97/100 |
| Exact-source queries below 90 hits | 2/1,000 | 2/1,000 |
| Maximum planned GETs per query | 32 | 32 |
| Maximum planned range bytes per query | 16,773,120 | 16,773,120 |

The candidate beat the capped control on 85 paired queries, lost on 5, and
tied on 910, for 131 more exact-source GT hits in total. Its exact-source
result is 355 hits above its own V116 SQ8 result. FP16 changed zero returned
IDs against float64 exact source within these measured unions; that is a
measurement for this split, not an arithmetic proof or a safe error bound.
The candidate union captured 89 more GT positions than its exact top-100
returned, and the control union captured 85 more. The fixed-width expansion
route therefore contained enough neighbors for this gate, while exact ranking
within that union remained below its capture ceiling.

The candidate used 18,343 planned range GETs and 14,179,027,200 planned
range bytes across all queries; the control used 22,494 GETs and
10,700,951,040 bytes. These are plan quantities, not live S3 request or
transfer measurements. The worker's evaluator used 17.73 seconds elapsed
and a maximum 8,550,396 KiB RSS (~8.16 GiB); the Rust replay used 22.43
seconds and 1,530,872 KiB RSS. Both are offline batch stages on a
`c7i.12xlarge` Spot worker, and include neither serving startup nor
concurrent query behavior.

## Decision and next gate

V126 meets the preregistered fixed-expansion gate: at least 99,000 exact
hits, no worse than capped control, p05 at least 90, no excess sub-90
queries, and all physical caps. It establishes that the V116 expansion path
plus original-source scoring can clear 99% on ReLAION under its frozen
layout without a dataset-specific query branch or vector-count knee. It does **not**
qualify a unified production method across the V121 deep-image and V116
ReLAION layouts, which used different fitters and test histories.

The next gate is a matched 100k serving integration: one query-blind router
and generation format across corpus families; authenticated source-ID map,
expansion and local source planes; per-query exact/FP16 refinement with a
sound error bound or exact fallback; and measured p05 recall, sub-90 count,
live S3 GET/bytes, p50/p95/p99 latency, throughput, charged RAM, startup
hydrate cost, and pinned-generation overlap. Hold a fixed `(N,D,R,C,G,L)`
resource budget, permitting more memory or SSD for a higher requested recall
without a hidden row-count threshold. Promote to fresh 1M cross-corpus
quality and live-serving gates, then 10M/100M, only after that screen passes.
