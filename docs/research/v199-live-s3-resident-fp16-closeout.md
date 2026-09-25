# V199 live S3 resident FP16 transport closeout

## Decision and authority

**Advance the frozen method to a production Rust planner and concurrent
end-to-end gate.** On the used ReLAION-1M D768 validation-1000 real-query
split, the authenticated Rust live S3 execution returned the exact V198 FP16
top-100 lists for all 1,000 queries. Its measured returned quality and
observed S3 payload charge pass the preregistered V198/V199 limits. This
qualifies a sequential transport and scoring slice; it does not establish
end-to-end request latency, concurrent throughput, external-product parity,
distinct-dataset generality, 10M/100M scale, or generation mutation.

The terminal-closed Causality Spot `c7i.12xlarge` attempt `a0004` used source
commit `5221e5c2075e59f8d068a182a406dbc33d095d89`, archive SHA-256
`38f3c0a77f797a5a70745e36654a76dbefe0fdc7c5a1d7ca21cb6d0bf4347b05`,
and instance `i-045b9ab718def47a0`. Its terminal SHA-256 is
`c072215e0c4fd604d10012b219079f758f7e8efa6392ddaf5fd530150d258097`
at `s3://borsuk-bench-453182569524-euc1/research/v199-live-s3-resident-fp16/5221e5c2075e59f8d068a182a406dbc33d095d89/runs/a0004/terminal.json`.
The launcher streamed back and hashed every terminal artifact, then
terminated the instance. The checked-in
`scripts/check_v199_live_s3_resident_fp16.py` independently replayed
artifact identities, source/plan pairing, per-query returned-ID/GT100
intersections, physical charges, parity counts and timing percentiles. It
passed on the closed artifacts. The V198 GT100 provenance was already
authenticated by V198's sealed source and independent checker.

## Measured live result

| Arm on ReLAION-1M D768 validation-1000 | Returned GT100 / 100,000 | p05 / 100 | SQ8 payload bytes | SQ8 GETs |
| --- | ---: | ---: | ---: | ---: |
| V199 Rust, live S3 + resident FP16 | **99,605** | **98** | **7,388,559,360 observed** | **10,047 submitted/successful** |
| V198 Python plan + resident FP16, same queries | 99,605 | 98 | 7,388,559,360 planned | 10,047 planned |
| V155 cached sparse + exact source, historical paired baseline | 99,567 | 98 | 11,134,007,040 planned | 22,126 planned |

V199 minimum returned GT100 was 88/100; 31/1,000 queries were below 98.
V199 observed successful range response payload bytes and submitted GETs
equaled V198's sealed plan exactly. These counts exclude request/response
headers and do not constitute an independent S3 billing trace. The Rust
reader uses zero configured hidden retries and checks a conditional ETag,
exact range, object length and every SHA-256 page digest before scoring.
The SQ8 object's full SHA-256 was authenticated when the 31,250-page sidecar
was built; the immutable object ETag was
`"fbdab9e60d3b28a988d5109d4febb4fb-93"`.

| Sequential per-query phase | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Authenticated S3 range fetch | 41.39 ms | 82.46 ms | 126.75 ms |
| SQ8 score over fetched rows | 4.11 ms | 11.39 ms | 11.48 ms |
| Resident FP16 top-100 rerank | 0.243 ms | 0.281 ms | 0.290 ms |
| Transport + SQ8 + FP16 total | **46.57 ms** | **92.23 ms** | **137.87 ms** |

The 1,000-query sequential serving loop took 51.468 s; process wall
including startup was 53.69 s. Peak Rust process RSS was **1,606,168,576
bytes**. Cold startup, including full resident-plane authentication, was
2.141 s. The page sidecar build took 1.03 s and 14,796 KiB peak RSS on
Spot after downloading the immutable 780 MB SQ8 object. These timings
exclude routing, GT-blind feature construction, physical planning,
concurrent query admission and full production generation lifecycle. The
Python exact planner still took 74.55 ms/query averaged in V198 and remains
a blocking serving-path cost.

## Numeric parity investigation

The strict original `a0001` gate failed at query 84 after 84 exact ordered
SQ8/FP16 results. `a0002` showed a two-rank SQ8 order swap with the same
128 candidates, caused by four `f32` score ULPs of separation under Rust
scalar versus NumPy matrix-product arithmetic. The set-parity `a0003` gate
then failed at query 617: one candidate crossed the 128th-rank boundary.
Both failed attempts were terminal-closed and their Spot instances
terminated; they are preserved negative evidence in the prereg amendments.

The final `a0004` measured the actual Rust returned IDs on every query
without GT in the worker. There were **28 SQ8 ordered-list mismatches** and
**one top-128 set mismatch** against V198 NumPy, but **zero FP16 returned-list
mismatches**. Therefore the Rust production scorer's quality on this used
panel is measured directly at 99,605 hits; no claim relies on transferring
V198's Python shortlist parity across numeric implementations. This does
not prove the same boundary behavior on another query distribution.

## Next gates

Implement a generic low-latency Rust physical planner with exact reference
parity or an explicit bounded-loss certificate. Include its route, feature,
plan and serving time in a concurrent end-to-end live S3 gate, with
generation-pinned resources and full observed request accounting. Then
repeat quality and performance on an authenticated *different* real-query
embedding dataset under paired equivalent conditions. Freeze production
defaults only after those gates pass; 10M and 100M profiles should set RAM
from recall target, corpus size and live generation count rather than an
arbitrary vector-count threshold. Mutation, compaction, failure recovery,
observability and packaging remain product requirements.
