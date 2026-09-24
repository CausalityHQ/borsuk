# V146 production Rust unit-centroid score closeout

**Decision: reject the flat scoring route at the unchanged 10 ms p95 CPU
gate; keep its exact Rust format and arithmetic as a correctness baseline.**
The frozen source is `ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0` (archive
SHA-256 `3e1819686295eb3a4f5c206a2628a447e28b281baed7872538eff82adcc649fa`).
One Causality Spot `c7i.8xlarge`, `i-0bea5f97ad393e64b`, ran 142 s
and is terminated. Its terminal is
`s3://borsuk-bench-453182569524-euc1/research/v146-centroid-score/ac9c60b3b2a8cb18bfa3c9a9417b0f8e7d831bc0/runs/v146-20260924T111500Z/a0001/terminal.json`
(SHA-256 `45d98502b274be939c9e9fd4ba4fca2033cdb3a0323cf28a6148ffd88a095086`).
The terminal is a scientifically complete `reject-cpu`, exit 0,
uninterrupted. The launcher authenticated all 22 recorded artifacts
before reporting a rejected gate. Both source and adapter builds passed.

| Frozen cohort and used split | Exact page plans vs V140 | Score p50 / p95 / p99 (ms/query) | Score + planner p50 / p95 / p99 (ms/query) | Max absolute score difference vs V145 | Logical resident centroid arrays |
|---|---:|---:|---:|---:|---:|
| deep-image-96-angular random100k train subset; publication-test ordinals 9000–9999 | 1,000/1,000 | 0.033789 / 0.038115 / 0.045380 | 0.177738 / 0.530383 / 0.659827 | 0.0000195503 | 1,212,500 bytes |
| ReLAION-1M validation-1000 | 1,000/1,000 | 9.457336 / 9.895993 / 9.985258 | 9.925824 / **11.071522** / 11.263528 | 0.0000529289 | 96,125,000 bytes |

These are authenticated V146 isolated CPU measurements on the same Spot
instance type, with all 1,000 per-query records independently recounted.
The Rust scorer uses a versioned `BORSUCP1` f16 centroid file (600,032
bytes D96; 48,000,032 bytes ReLAION) and decodes it once to f32 resident
arrays. SQ8-to-centroid build took 61.75 ms D96 and 5,961.10 ms
ReLAION. The ReLAION process peaked at 240,080 KiB RSS; its cohort
cgroup peaked at 1,130,156,032 bytes including SQ8 file cache. D96
process RSS peaked at 51,040 KiB and its cohort cgroup at 73,363,456
bytes. Logical arrays, process RSS and charged cgroup memory are different
quantities and should not be substituted for one another.

V145's pure page planner p95 was 0.474496 ms D96 and 1.235540 ms
ReLAION. V146 reproduces the same β=4 page decisions but adds a full
unit-centroid scan. The ReLAION score alone consumes almost the entire
10 ms combined budget. The flat scan performs work proportional to
`ceil(N/32) × D` per query; its CPU and memory traffic grow linearly
with N and cannot be the qualified 10M/100M route. Repeating the flat
screen with a relaxed threshold or corpus-specific SIMD branch is not a
valid next architecture decision.

Next, build a dimension-generic hierarchical candidate route over unit
centroids or page summaries with a recall/resource control that varies
with requested quality and transport cost, without a vector-count knee.
Use V146's full Rust score matrices as an authenticated flat oracle.
First preregister and measure a 100k gate against the strongest β=4
V141 returned-quality and V145 page-plan baseline. Promote only a winner
to the frozen 1M validation split, then 10M. The subsequent fresh
held-out and live-S3 gates must measure returned recall, actual GETs and
bytes, latency, concurrency, charged RAM and pinned generations. The
existing Lean proofs establish conditional budget and payload bounds;
neither these proofs nor this offline CPU test establishes empirical
recall or end-to-end latency.
