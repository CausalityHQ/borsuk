# V196 resident FP16 used-panel preflight closeout

## Decision

**Advance to a fresh 1M source holdout.** The authenticated resident
FP16 tier passes the preregistered used-panel exact-ID, local CPU and
process-RSS gates. This promotes the precision-plane primitive to fresh
testing; it does not qualify end-to-end search, S3 latency, mutation,
100M RAM, or production defaults.

The source revision is `c5fbb27ffba5fe5a78f15a4fb9e9c836a2afba04`.
The one Causality Spot `c7i.12xlarge` cell ran on
`i-057435f1b4c5b580d`, completed and was terminated. Its immutable
S3 prefix is
`s3://borsuk-bench-453182569524-euc1/research/v196-resident-fp16-preflight/c5fbb27ffba5fe5a78f15a4fb9e9c836a2afba04/runs/a0001/`.
Source archive SHA-256 is
`1cbc37ef28e570f3439b565a4c31d9053db2a47279cfc149cf6a0715ccddede7`;
terminal SHA-256 is
`a73ce19c371751d391d9c35530e6529adbaca9e8c85b525d1d6028218c47a5f3`.
The launcher streamed back and hashed every closed S3 artifact, including
the 1,544,000,064-byte plane. A separate checker replayed the 512-case
roster, all V195 expected-ID lists, closed hashes and gate arithmetic;
the pre-timing S3 case seal matched and preceded the benchmark artifact.

## Measured used-panel result

Dataset/split: **ReLAION-1M D768, reused V194 source pseudoquery SHA
ranks 2945–3456 (512 queries)**. V194's optional-risk SQ8 physical
plans and V195's generic dynamic mandatory-floor rescue are frozen.
The Rust process used one 1M-row resident plane and the fixed SQ8
top-128 shortlist. Every one of the **5,120 returned top-100 ID lists**
(512 queries × 10 timed repetitions) exactly matched V195's FP16
reference. One warm-up repetition was excluded from timing.

| Local process measure | V196 observed | Preregistered gate |
| --- | ---: | ---: |
| Worst repetition p50 rerank | 0.232328 ms | descriptive |
| Worst repetition p95 rerank | **0.267317 ms** | ≤2 ms |
| Worst repetition p99 rerank | **0.276149 ms** | ≤5 ms |
| Worst individual rerank | 0.282376 ms | descriptive |
| Cold full-file SHA authentication | 2.077398064 s | descriptive |
| Resident ID + FP16 payload charged by tier | 1,544,000,000 bytes | ≤2 GiB |
| Peak serving-process RSS (`VmHWM`) | **1,550,340,096 bytes** | ≤2 GiB |
| Plane file, including header | 1,544,000,064 bytes | exact format check |

`/usr/bin/time -v` independently reports 1,513,396 KiB peak RSS,
3.48 s total Rust-process wall time, and zero swap. The offline Python
case/plane builder took 22.16 s wall time and 9,284,080 KiB peak RSS;
its memory is **not** serving-process memory. The timing excludes
Python preparation, Rust compilation, S3 download and SQ8 first-wave
retrieval. It measures single-process sequential local rerank on this
instance, with no concurrency or network S3 latency claim.

## Quality and transport boundary

V196 did not open a new quality panel. The paired **V195 used-panel
reference** was 51,022/51,200 returned GT100 positions, p05 98,
15/512 queries below 98, with SQ8 first-wave planned
3,883,676,160 bytes and 5,179 GETs. V196 exactly reproduces its
returned IDs on the same cohort. These quality and planned transport
numbers remain V195 evidence, not fresh V196 measurements. The
separate V195 FP16 S3 sidecar failed its envelope at
5,219,725,824 combined planned bytes and 16,035 combined GETs; the
resident layout removes that second S3 wave at the cost of RAM.
The V155 ReLAION-1M validation-1000 real-query baseline is unpaired
and used: 99,567/100,000 returned GT100 positions at
11,134,007,040 planned bytes and 22,126 GETs. No equivalent live S3
or external product latency/cost comparison exists for V196.

The authenticated format's modeled payload is
`generations × (64 + N × (8 + 2D))` bytes. At 100M D768 it is
154,400,000,064 bytes per generation, or 308,800,000,128 bytes for
two complete cutover generations before router, deltas, allocator and
query workspace. `formal/ResidentFp16Resources.lean` proves that
arithmetic and conditional admission under an externally verified
overhead bound. These are projections, not measured 100M resources.
Memory should be budgeted from corpus size, dimensions, requested recall
tier and simultaneous generations; no arbitrary 100M vector knee is
part of this format.

## Next gate

Freeze the V196 primitive and generic SQ8/dynamic-floor policy for a
new identity-disjoint ReLAION-1M source pseudoquery panel. Measure
returned GT100, p05 and below-98 tail, authenticated plan bytes/GETs,
and full process RSS with the resident tier. If it passes, test a
distinct real-query dataset and paired live S3 p50/p95/p99 latency,
throughput, resource and cost against the strongest BORSUK baseline
and matched external products. Generation cutover, delta mutation and
concurrent load remain separate production integration gates.
