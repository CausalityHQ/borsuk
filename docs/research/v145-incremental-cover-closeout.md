# V145 incremental-cover production planner closeout

**Decision: pass the pure Rust page-planner parity and CPU gate; proceed to
production score generation and full-route measurement.** The frozen source
is `e0e8e01f89be2c40438e09f325d9c1f72da1ca76` (archive SHA-256
`66e9cf38a2c0ebf3d3678b4a60cee54a98a689c0e1d159ddea5044201c4d2a0f`).
One Causality Spot `c7i.8xlarge`, `i-08d33a3dbd5561896`, completed in
221 s and is terminated. Its terminal is
`s3://borsuk-bench-453182569524-euc1/research/v145-page-parity/e0e8e01f89be2c40438e09f325d9c1f72da1ca76/runs/v145-20260924T110000Z/a0001/terminal.json`
(SHA-256 `6de71d7f552c096067a0c42dbef7d95c0c3e1b86b555bdc474c4f1ef021a4354`).
Terminal status is complete, exit 0, phase complete; all 27 terminal
artifacts were SHA-256 and length authenticated by the launcher. The
production crate `cargo check --lib` and release replay build passed.
Both regenerated Python raw files exactly matched V140's frozen raw hashes
before Rust comparison.

| Frozen cohort and used split | Rust/V140 exact page-plan parity | Rust planner p50 / p95 / p99 (ms/query) | Planned bytes / 1,000 queries | GETs / 1,000 queries | Gate |
|---|---:|---:|---:|---:|---|
| deep-image-96-angular random100k train subset; publication-test ordinals 9000–9999 | 1,000/1,000 | 0.131970 / 0.474496 / 0.603764 | 2,693,744,640 | 28,100 | pass |
| ReLAION-1M validation-1000 | 1,000/1,000 | 0.286306 / 1.235540 / 1.283952 | 11,801,736,960 | 23,581 | pass |

V144 is the same frozen V140 page-plan baseline with the previous Rust
cover implementation, on the same Spot instance type: D96 p95 0.442532 ms
and ReLAION p95 6.713421 ms. V145's ReLAION p95 is 5.43× lower and
all 1,000 ReLAION calls were under 5 ms (maximum 1.332760 ms). V145 D96
p95 is 0.474496 ms, 0.031964 ms higher than V144, with exact page-plan
parity. The small D96 difference is not assigned a causal speedup/slowdown
from one run; both clear the same 5 ms gate. ReLAION Rust replay RSS peaked
at 50,148 KiB; D96 replay RSS peaked at 44,164 KiB. The Python ReLAION
score-export process peaked at 991,092 KiB RSS and is not production
serving memory. The isolated Rust replay excludes score computation,
primary routing, S3 transport, source rerank, concurrency and startup.

The next gate ports V140's f16 unit-centroid score calculation to production
Rust and requires score-bit/page-plan parity on all 1,000 used queries in
both cohorts before measuring complete local route+planner CPU, charged
RAM, pinned-generation startup, and concurrent throughput on one source
revision. Then a fresh held-out quality and live-S3 transport gate is
required. Large-N scaling still requires a hierarchical source-only route;
the current flat scans are linear in N. The generic memory/resource policy
remains a function of recall target, N, D and measured transport costs, with
no vector-count knee or dataset identity switch. Existing Lean results
prove conditional budget and payload bounds only; they do not establish
empirical recall, full latency or charged RAM.
