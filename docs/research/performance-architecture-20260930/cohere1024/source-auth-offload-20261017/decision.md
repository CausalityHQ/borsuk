# SOURCE authentication offload screen
Exact source: 2ef7ce0a642bca0e25f9e7bbd35740fea5d9a95c.
Disposition: retain qualified optional Pareto candidate; inline remains default.
All 10 required exact tests, affected regressions, release, workspace Clippy and actual shim-unset workspace test compilation passed. Separate real-input admission and disposable canary passed before measurement.
Frozen 8 workloads, 32 query executions, ABBA/BAAB, Q1, download64, inline versus offload2.
Exact logical parity includes returned IDs/score bits/order, physical row ordinals, complete plan/trace and logical charges. Physical query traffic also equal: 2148 attempts and 559110656 consumed bytes each; zero transport/stream failures.
Wall ratio B/A 0.9360598694; CPU 1.0356471421; SOURCE wall 0.8671003138. Seven of eight per-ordinal mean wall ratios improve; ordinal547 regresses1.59%. RSS ranges: inline63248..64944KiB, offload63116..64760KiB. Process RSS is not a bound on multi-query generation coexistence.
This is eight unique workloads repeated, not32 independent samples. No population p90/p95, concurrent QPS, recall-generalization, cost or competitor win claim.
The process-wide batch lease serializes SOURCE download/authentication batches across queries. Concurrency is unqualified. Default remains inline.
Next gate: retain this Q1 result, remove batch-wide SOURCE serialization in one causal scheduler change with shared compute/byte permits and cancellation/drain proofs, then qualify concurrency before considering default adoption. Do not repeat the current screen merely to seek a larger gain.
Historical failed compiler/fixture gates are execution INVALID, preserved separately; they do not reject the algorithm. No retrospective threshold change.
