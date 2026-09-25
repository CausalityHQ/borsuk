# V207 resident FP16 nominee diagnostic closeout

## Closed authority

The preregistered Deep-Image-96-angular **already-used publication
test-first-1000** diagnostic completed on 9,990,000 train vectors from
source `03e9c6d53cdcd055ad09ccec8b1d6c376245773d`, attempt `a0001`,
Causality Spot `i-0eee33f20341ffd9f` (`c7i.12xlarge`, eu-central-1c).
Terminal SHA-256 is
`9d33b68fb3f15cdcd648e146902dbfcc38d524c1b1f1605bb155366a5511849c`.
The launcher streamed and verified every closed S3 artifact by byte count
and SHA-256, including the 1,918,080,000-byte resident FP16 plane (SHA-256
`64a83af5f38d4bcd3f6a3312073e3de284ee8c5360dcbc1975ea99bf681dc3dc`),
and verified the Spot instance **terminated**. Raw returned IDs and a
pretruth seal were uploaded before GT download. The independent
`scripts/check_v207_resident_nominee_fp16.py` read the closed witness,
V121 rosters, V206 returned IDs and publication GT, then recounted every
intersection and summary field; it passed.

## Paired quality and resources

Each query used the same 512 source-only V121 nominee ordinals, mapped
through the authenticated V120 layout to train IDs. Resident FP16 and
original float32 ranked the complete roster by float64 cosine with stable
train-ID ties. The V206 control scored only its capped physical ranges.

| Deep-Image D96 test-first-1000, GT100 | Hits / 100,000 | p05 hits / 100 | Queries below 90 |
| --- | ---: | ---: | ---: |
| V121 same-range SQ8 | 98,034 | 96 | 12 |
| V206 same-range FP16 | 99,547 | 99 | 12 |
| V207 resident 512-nominee FP16 | **99,959** | **100** | **0** |
| V207 same-nominee original float32 | 99,993 | 100 | 0 |

Resident FP16 won 76 queries, tied 924 and lost none against V206
same-range FP16. The nominee roster contained 99,994 GT100 IDs, so
float32 missed one nominated GT hit and FP16 missed 35; the other six
GT hits were absent from nomination. FP16 lost 34 hits to float32.
The preregistered ≥99,900-hit, p05≥99, zero-below90 screen **passes**.

The deterministic FP16 plane payload is 1,918,080,000 bytes
(1.786 GiB). The offline Python source-load, plane-build and
1,000-query return process took 25.13 seconds wall and peaked at
9,110,728 KiB process RSS; its separate GT reduction took 0.33 seconds
wall and peaked at 148,400 KiB. Per-query **FP16 roster-scoring only**
p50/p95/p99 was 0.253972/0.290940/0.301856 ms. Those times exclude
nomination, query parsing, generation load, concurrent requests, S3
transport, mutations and service overhead. The 9.11-GB process peak
includes temporary source-build and research-arm memory; it is not the
charged steady-state resident serving footprint. The query scoring loop
did not fetch vector bodies from S3, but this cell did not measure a
live service or object-store request bill.

## Decision and next gate

This result makes a resident vector plane a credible, generic
memory-for-recall architecture. It does **not** set a fixed RAM ceiling
or a vector-count knee: the plane grows as `2 × rows × dimensions`
bytes, and admission should be measured against recall, latency and
cost targets. At 100M × D768, the FP16 payload alone would be
153,600,000,000 bytes (143.05 GiB), before router, generations and
working memory; that is a capacity calculation, not measured serving
RAM or cost.

V120 Deep-Image and V197 ReLAION source-only builders differ, and this
Deep-Image query split was previously opened. Next implement the
authenticated resident plane and full-roster scorer in the production
Rust path, then freeze method and RAM policy before untouched Deep-Image
test ordinals 1000–1999. Build the same source-only method on ReLAION
and run a matched cross-corpus gate. Measure end-to-end live latency,
concurrency, warm/cold generation load, memory during generation swap,
S3 API cost and mutation/recovery before a production default or
10M/100M scaling claim. A direct source-only locality improvement and
adaptive I/O policy remain candidates if the resident resource/cost
envelope does not qualify; V206's exact cap oracle is only a diagnostic.
