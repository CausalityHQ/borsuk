# V211 PQ-screened resident neighborhood closeout

V211 passes the paired 100k **quality** screen but fails the frozen
Python stage-latency gate. On reused ReLAION-100k D768 development
queries 0–255, PQ64 screening of 32,768 relaid physical positions
before FP16 scoring recovers the quality lost by V210's direct
8,192-nearest arm. The only next viable check is a bounded Rust kernel
using the already-implemented source PQ scorer; no 1M rerun is
licensed by these Python timings.

The one complete Causality `c7i.4xlarge` Spot attempt `a0001`, instance
`i-0095766bfe8eda7ce`, used source
`6f0f0b0ee59ed5fb9b3237cafd23a74d7550487a`, archive SHA-256
`90affb49cec5b2bcafd1bba6d2796ea0ca6e8c0f71ee2048488f7713ac66b5fb`.
Terminal SHA-256 is
`8bd0c86b760fa2611b5a4094951a57c9845b182622c0931c4638e9d4da6b14a8`
at `s3://borsuk-bench-453182569524-euc1/research/v211-pq-resident-neighborhood-100k/6f0f0b0ee59ed5fb9b3237cafd23a74d7550487a/runs/a0001/terminal.json`.
All terminal artifact digests passed readback and the instance
terminated. Raw IDs and a GT-blind seal preceded truth download.

| Same 256 used ReLAION-100k queries | Returned GT100 hits / 25,600 | p05 hits / 100 | PQ screen + FP16 p95 |
| --- | ---: | ---: | ---: |
| V193 full-rank SQ8 baseline | 25,440 | not replayed here | not measured here |
| V211 top 4,096 FP16 | **25,513** | **99** | 20.682 ms |
| V211 top 8,192 FP16 | 25,516 | 99 | 23.689 ms |
| V211 top 16,384 FP16 | 25,516 | 99 | 33.646 ms |

The 4,096 arm gains 73 paired hits over V193 and misses only three
of V210's full 32,768-neighborhood FP16 hits while scoring one eighth
as many FP16 vectors. It still exceeds the preregistered 10-ms stage
limit in this single-threaded NumPy implementation, so the V211 gate
is **failed**; there is no qualifying budget. V211 does not isolate
PQ-screen versus FP16 time or include nearest-position construction.
Its offline Python prepare process took 13.55 s and peaked at
2,121,832 KiB RSS; neither is serving memory or product latency.

The 100k quality result warrants exactly one Rust kernel falsifier:
implement the same fixed 32,768→4,096→100 query path with a bounded
multi-source physical expansion, existing Rust `Pq64Router::score_rows`,
and authenticated resident FP16 ranking. Measure full in-process
latency and returned quality on this same 100k panel. If it misses
the latency/quality target, reject this family and choose a different
candidate index such as a source-built navigable graph. Do not run
another 1M comparison until the 100k Rust gate passes.
