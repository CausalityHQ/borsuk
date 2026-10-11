# Scale closure repair: native correctness qualified

Candidate: `6e78d393b4d7dadf19befeeabe3dc20a77319a26`. Only `semantic_unit_router.rs` and `two_bit_generation.rs` change production source. Independent review group `cb44cf12afe548a3` reviewed the production delta at `e94ad0d`; later changes added the codec regression and corrected accounting documentation.

| Gate | Observed result |
|---|---|
| Original limit regressions | Nomination and complete-page scoring each failed exactly once with the old closure/geometry refusal |
| Repaired exact regressions | All three passed once in their owning exact stages |
| Affected suites | Router 11/11; source walk 32/32; baseline bin 42/42 |
| Locked release baseline | Exit 0; 17,047,872 bytes, SHA256 `72ac92e78f0e12ada77f6f6dc8c48d66bc547067a8e12440b469424993fe21f0` |
| Locked workspace/all-target Clippy | Correctness and suspicious categories, exit 0 |
| Actual workspace test build | Override unset, jobs 2, exit 0; compile only, not full workspace test execution |
| Resource/source receipts | 463 source/support files and 430 Rust/Cargo files pinned; successful candidate stage source checks agree before/after; all owned cgroups drained; zero swap and OOM kills |
| Compute closure | SAME `i-01bdbd4b0e8b2dc95` terminated and waited; root `vol-09108d31384f68dd7` returns exact NotFound; external timer inactive and BG cleared |

Scale1m now admits the structural maximum of 3,072 nominated physical pages and scores all corresponding units, including a partial tail. Fresh1m and Native/graph limits remain unchanged. Retained trace allocations and the Scale planner reserve are charged; the reserve uses a conservative modeled set-entry size, so actual process RSS remains a separate runtime gate.

No source-byte, GET, fetch-concurrency, cache or process-memory budget was relaxed. The existing SOURCE 64 MiB/128 GET and SQ8 16,773,120-byte/32 GET limits remain binding. The larger structural geometry can still fail those economic bounds.

## Preserved execution defect

The first green compilation reused the red ELF (`fresh=true`, identical ELF SHA), although the candidate source bytes and map were correct. The downloaded files had S3 timestamps earlier than the red compilation. Its subsequent old-cap test failure is execution INVALID, not evidence against the repair. Both attempts and all original receipts are preserved. The frozen correction refreshed only those two source timestamps; source bytes remained unchanged. The accepted rebuild reports `fresh=false` and the accepted test ELF differs from red. No instance or scientific arm was repeated.

## Next falsifier

Run original ordinal 527 through the repaired native baseline with the same generation, requests/truth, SOURCE/SQ8 limits, CPU0/512 MiB/zero swap, fetch16 and query concurrency1. Run-time source/binary authentication and disposable staging remain required. It is a diagnostic, not population performance evidence. If SOURCE cover exceeds 64 MiB, stop this intervention and qualify a separate locality/layout change; do not increase the budget automatically.

The independent 32-query panel and unchanged original panel follow only after that gate. A truth-free all-1000 routing/cover preflight is still required before another full population run and is not yet implemented. No new population R10, latency, QPS or vendor win is claimed. Historical failed measurements and all original gate artifacts remain immutable.

## Artifact retention

Original stage bodies and manifests are retained under `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261011/scale-closure-native-qualification-a0001/`. Git receipts contain logs, commands, terminal/resource evidence and the complete manifests. Release ELF and redundant source copies remain in that authenticated S3 inventory and are intentionally omitted from Git receipt folders; fetch them before checking a whole original manifest. No real requests, truth or raw result rows were decoded locally.
