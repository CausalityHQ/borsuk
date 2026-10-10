# Actual 1M Q32 A0001: execution INVALID

Frozen native source bc3082a8210c4370ebadae4ba093a7c211072b62; protocol 7c5813b091c57749c7480a0ca76190a54be3cf5c. Original instance i-0e842b3f25121d177 is independently confirmed terminated; both attached volumes are absent. Authenticated manager matches terminal and archive. Original root closeout API timeout is retained separately from successful read-only observation A0002; A0002 is not another experiment.

All derive/stage/generation/publish/baseline native exits were 0. Baseline reported MEASURED, 314/320 hits, zero underfill, 32 sealed queries. Overall wrapper exit 98, bootstrap exit 93, watcher exit 10: execution INVALID. The baseline resource-events closure rejected pids.events max increasing from 0 to 23, with pids.peak 128 at TasksMax 128. OOM/oom-kill/swap were zero. Memory max reclaim events rose by 589; this is permitted by the frozen event validator and does not by itself cause rejection.

These observations are not an accepted recall screen or cold comparison. Physical S3 was not measured; local filesystem cache state was uncontrolled. No thresholds are waived, no original receipt is rewritten, and no paid rerun is authorized by this record.

Source diagnosis hypothesis: production baseline current-thread Tokio runtime leaves max_blocking_threads at dependency default 512; object_store 0.14.1 LocalFileSystem dispatches filesystem operations through spawn_blocking. Query fetch parallelism is not necessarily an upper bound on the blocking pool. Exact causal trace and bounded native falsifier remain required before a Rust repair. No memory.stat evidence was retained, so page-cache attribution remains unproven.

Next gate: bounded native thread/resource correctness falsifier on exact repaired source, including workspace Clippy and real test-build, then a new frozen Q32 attempt only after admission/staging. Full-cohort truth and physical-S3 cold measurements remain downstream.
