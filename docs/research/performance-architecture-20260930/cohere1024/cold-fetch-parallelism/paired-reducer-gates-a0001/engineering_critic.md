**HOLD: two admission gaps in `f0a717f37f9c68c0f45b9e5b33dd676c52dc17c6`.** Candidate file SHA matches the contract. Findings below are from source inspection; no tests or native execution were performed.

1. **[P1] Paired mode does not enforce the required fixed workload.**  
   [Paired admission](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1353) checks selectors and cross-arm equality, then inherits [generic completed-mode validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:351): any nonempty dataset/revision/profile, rows ≥10, and dimensions 1–1024. Two identically wrong populations therefore pass. The positive paired fixture already illustrates this: [its underlying inputs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1900) use `synthetic/native-fixture`, 32 rows, D16, and query offset32.

   **Minimal fix:** add paired-only checks for the specified Cohere dataset/revision, 100,000 rows, D1024, corpus/query offsets0/100000, and Native100k profile. Preserve generic completed-mode behavior and externally authenticated byte pins.

   **Smallest falsifier:** make the existing 32-row/D16 paired fixture expect `INVALID`; retain a positive fixture with the required workload metadata. No corpus access is needed.

2. **[P2] Impossible inflight observations are accepted as valid selector evidence.**  
   [Stage aggregation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:746) accepts any `u64` for `leaf_peak_inflight`; paired mode never checks its bound. Its [positive fixture explicitly sets32](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1994). At producer revision `d8e64dbc`, `two_bit_generation.rs:1703` bounds simultaneous leaf reads by `min(max_parallel_source_gets, selected_leaves)`, and `semantic_unit_router.rs:60` caps Native100k selection at16. Thus that “legitimate variation” cannot come from the selected producer.

   **Minimal fix:** validate the upper bound in the new paired path while allowing legitimate differences between arms; correct the positive fixture accordingly.

   **Smallest falsifier:** change one width16 query’s peak to17, reseal and repin the file, and require `INVALID`. Currently no check rejects that contradiction.

I found **no producer-schema mismatch** in the selector/cache fields, trace-before-transport layout, stage fields, startup metadata, or compact terminal transport record. Full-trace hashing preserves array order; pooled nearest-rank statistics and serial QPS include all observations. The report appropriately avoids timing gates and vendor/resource qualification claims.

Optional coverage: independently exercise consistent transport failures, non-success responses, and equal nonzero logical failures in both arms. Current new tests exercise stream failure, but not each remaining rejection condition.

Compilation, all12 example tests, release build, workspace Clippy, and shim-unset workspace test compilation remain **UNRUN** and required remotely. No files, jobs, infrastructure, or historical evidence were changed.
