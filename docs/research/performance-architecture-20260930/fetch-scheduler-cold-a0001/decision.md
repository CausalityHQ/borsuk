# Adopt completion-driven fetch scheduling

Matched BORSUK-only causal comparison: 1M Cohere Wikipedia multilingual-v3, D1024 cosine k10, all1000 frozen queries, width32, original-A1/candidate-B1/candidate-B2/original-A2. Both arms use the same generation/input/scoring pins and CPU0/1GiB outer/512MiB library envelope, with payload/local-drive cache off. Resident router and within-process connections are allowed; provider cache is uncontrolled. Two repetitions per arm limit confidence and generalization.

| Cell | R10 | p90 ms | p95 ms | Sequential QPS |
|---|---:|---:|---:|---:|
| Original A1 | 97.80% | 223.427 | 238.452 | 5.0843 |
| Candidate B1 | 97.80% | 194.302 | 203.305 | 5.6996 |
| Candidate B2 | 97.80% | 193.703 | 199.814 | 5.7977 |
| Original A2 | 97.80% | 217.905 | 225.186 | 5.1722 |

Both pairs pass the preregistered >=10% p95 improvement with p90/QPS no >5% regression: p95 improves14.74%/11.27%; QPS improves12.10%/12.09%. All ordered IDs, score bits, recall, full traces, physical plans and logical GET/bytes match; underfill0. No recall relaxation. Candidate process peak RSS67,888/69,648KiB; whole-cgroup peak106,237,952/104,296,448B. Swap/OOM0.

Causal change: indexed completion-driven fetches refill slots without waiting for an earlier body; collect/drain all outcomes, then restore plan order before error/trace/result processing. Index/future memory is charged. SOURCE drops123.38/122.48s to103.15/101.46s per1000; planning stays19.3–19.8s and SQ8 remains50.2–52.0s. This supports the next bottleneck focus on SOURCE; it does not establish a compute/SIMD bottleneck.

Library source8e0b1d5ab7322574181433311e5653991d9a2ff9 passed nine native gates. Comparison reducerad0b3b180e726c03605bcace1ce3be71c9ff634b passed six combined source-bound gates, including170 regressions, Clippy and real shim-unset workspace test compilation. Prior test-first, compile-failure55bc and wrapper-INVALIDa0003 checkpoints remain preserved; the latter's independently authenticated compile/fixture successes were reused, not its failed wrapper outcome.

Actual-input527 admission and separate query0 canaries preceded the four full cells and two Rust parity reductions. All18 stages closed0. Original commandc81d74d3-e7ab-4031-8778-81be071220b1 completed0 in12m58.136s; instancei-0b01742304dad5290 terminated, root volume absent, external timer stopped. Opaque archive30,124,028B SHA260e1ef88502fb6d7e973853d29ac5e92ba85e7071cbe61aa08931e4167aebd2 at s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261012/fetch-scheduler-scale-cold-a0001/terminal/evidence.tar.gz. Raw query rows remain opaque; only native aggregate reports were extracted locally.

Existing same-byte S3 Vectors result: R10 96.10%, p90 197.550ms, p95 215.023ms, sequential QPS7.3584. Candidate recall and p95 are better in these observations, but sequential throughput is lower. Cache protocols differ/provider cache uncontrolled; normalized-f32 ranking parity and lifecycle billing remain unqualified. No overall matched vendor win. Turbopuffer has no matched reproduction. Concurrent capacity, lifecycle CRUD/recovery/compaction/GC and10M/100M feasibility remain product gates.
