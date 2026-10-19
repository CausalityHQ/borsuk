# Cold FP32 concurrency: Full1000 paired result

Same qualified Rust source and binaries; one causal intervention: maximum active queries 1 versus 2. Both use multi_thread2, fetch64, Offload2, leaf64, FP32 shortlist16 and zero application cache. Each cell has fresh scratch and a fresh process. Provider cache eviction is unproven.

| Metric | Q1 | Q2 |
|---|---:|---:|
| Mean recall@10, all repetitions | 99.02% | 99.02% |
| Aggregate cohort QPS | 5.213 | 6.332 |
| p90 ms, repetitions | 255.4 / 232.8 | 387.9 / 389.5 |
| p95 ms, repetitions | 281.8 / 257.0 | 411.0 / 415.0 |
| p99 ms, repetitions | 392.1 / 354.1 | 465.5 / 453.7 |
| Native-process peak RSS MiB | 164.1 / 150.4 | 224.4 / 221.0 |
| Conservative modeled bytes | 615,933,009 | 871,591,014 |

Q2 improves cohort throughput by 21.47% while increasing query latency and memory. Retain it as an opt-in Pareto candidate; keep Q1 as the native runner default. The generic library accepts explicit caller concurrency limits. No library scoring or layout changed in this integration.

All four measured cells returned identical IDs, f64 score bits, plans, charges and canonical shortlist, matching the authenticated historical answer reference and independent sequential-f64 oracle. Each scored 9,902/10,000 hits. Per-query quality remains uneven: 925 queries had 10 hits, 61 had 9, 9 had 8, 4 had 7 and 1 had 3. The frozen metric and truth were retained. These results do not establish performance on other distributions.

Each 1000-query cell submitted 169,024 logical object reads and verified 51,077,441,536 bytes, with zero logical failures. SDK counters include S3 and IMDS activity; they are not wire bytes or billed request counts. Cohort QPS excludes startup; performance-audit.json also records whole-process QPS including startup. Per-query durations include dispatch queue time. Overlapping per-query CPU or transport were never summed. RSS is GNU-time native-process RSS, not the whole Python-controller cgroup; phase durations are wall times, not isolated CPU.

The actual source-bound 8-query admissions and separate 2-query canaries passed before A1/B1/B2/A2. Each completed measurement passed the strict native reader, exact reference and oracle before a create-only S3 checkpoint and checksum HEAD. The controller ended exit0; no incomplete measurement was inspected. All Rust assurance is reused from exact source0cddd795:154 focused native checks, workspace Clippy, actual shim-unset test-build and release binaries. Packet8c74b307 changes docs/glue only, preserving crates treeab0730b.

The product is not yet declared ready and no matched vendor win is claimed. S3 Vectors raw-versus-normalized/cache/protocol differences and the absence of a measured Turbopuffer reproduction remain. A separate complete library runtime gate is in progress; its result must be reported separately. A fresh contextless source/primary-literature research packet will propose the next smallest falsifier. Cohere is an evaluation fixture; the Rust library remains generic.
