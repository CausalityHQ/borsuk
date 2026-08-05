# Realistic durable-write attempt ledger

This architecture qualification measures durable inserts on the official
Cohere Medium 1M corpus (1,000,000 vectors, 768 dimensions, cosine distance).
Scalar acknowledgement latency and batched throughput are separate workload
points. While an attempt or cell is incomplete, inspect only terminal markers,
process/service health, resource availability, and non-measurement progress;
never inspect its measurement CSV files.

| Attempt | Revision | Dataset descriptor SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| preparation | `3040883` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | not applicable | `s3://borsuk-bench-453182569524-euc1/research/datasets/cohere-medium-1M/preparation/3040883/` | not applicable | terminal dataset validation passed: 1,000,000 train rows, 768 float32 dimensions, 1,000 aligned test/ground-truth rows, 1,000 neighbours per query, aggregate source SHA-256 `c0c572f0265181a182ae904383f97d0e3137521eb52bd3c05d1a3935bab0273b`; no measurement campaign launched |

The preregistered v1 architecture qualification uses batches 1, 32, 128, and
1,024 over three repetitions with 100 raw durable batches per cell. It is
claim-ineligible until a committed source revision is archived, fresh prefixes
are recorded above, the root completion marker exists without a failure marker,
and the repository validator reconciles every frozen cell. Publication requires
a separate five-repetition campaign from the selected frozen revision.
