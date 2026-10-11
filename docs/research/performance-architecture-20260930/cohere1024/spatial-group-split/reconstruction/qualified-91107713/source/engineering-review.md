**HOLD — source-only, UNVERIFIED.** Reviewed candidate `a645ca40` against parent `483b7f69`, limited to the two requested files.

The change reconstructs authenticated centroid bytes and optionally proves historical radial parity through actual current construction.

1. **F2 — P2: historical fixture has an unpinned arithmetic dependency.**  
   The [fixture’s source assertions](/home/rb/worktrees/borsuk-partition-centroid-reconstruction-native/crates/borsuk/examples/compare_native_replay/partition_reconstruction.rs:1019) pin the decoder, trainer, metric and Cargo.lock. However, [metric.rs imports SIMD operations](/home/rb/worktrees/borsuk-partition-centroid-reconstruction-native/crates/borsuk/src/metric.rs:482) from `simd_control.rs`, which remains shared and unpinned. Changing those operations could change both builders together while their parity test remains green.

   **Minimum repair:** add the historical `simd_control.rs` SHA assertion alongside the existing pins. Its SHA is `82bf82bed5167662439160cb0ea8b3db2870e8322c196e50f439ddcadc514b56`. I verified that it currently matches the historical revision; this is a missing regression guard, not demonstrated arithmetic drift.

2. **F1 — known, still pending.**  
   The [failure-injection loop](/home/rb/worktrees/borsuk-partition-centroid-reconstruction-native/crates/borsuk/examples/compare_native_replay/partition_reconstruction.rs:1547) covers `None/7` and `Some/13`. Successful sync roster/count assertions and parity-mode sync-time mutation coverage remain absent. Complete the already-requested reconciliation.

No additional production correctness or admission defect was demonstrated:

- Approval independently binds generation, epoch, plane, Discovery and construction SHA. Stage 1 leaves root/member/query/truth unopened.
- Reconstruction preserves FP16 bits and checks full leaf SHA, unique complete IDs, EOF and whole centroid SHA.
- Stage 2 authenticates complete historical bodies before extracting the allocation limit, invokes the current builder, validates/opens its output, and permits only the exact two root-marker changes.
- Cumulative admission includes external retained charges without doublecharging centroids. Reconstruction scratch is released before building.
- Publication uses create-only outputs, readback, snapshot checks, file/directory sync and receipt invalidation.
- The copied historical construction/encoder matches `2e2a789a`, except the explicitly omitted validation call. The 4097/D1024/129 fixture includes tail, signed-zero, subnormal, finite-extreme, permutation and forbidden-byte assertions.

**Compilation by inspection:** no identifiable new type, borrow or module-path error. Actual compilation, Clippy, workspace test compilation and native falsifiers still require the authorized EC2 lane.

No edits, builds, runtime/data execution, network access or consultations performed.
