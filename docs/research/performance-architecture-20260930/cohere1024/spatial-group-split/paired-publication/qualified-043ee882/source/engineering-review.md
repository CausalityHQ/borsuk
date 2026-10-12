**BLOCK — one ownership-accounting gap before native qualification.** Reviewed `556df287` against `9f482302` in the clean isolated worktree.

1. **[P2] The retained group map is charged by length, not allocated capacity.**  
   [group_map](/home/rb/worktrees/borsuk-paired-partition-source-screen-native/crates/borsuk/examples/compare_native_replay/paired_partition_coverage.rs:712) uses fallible `collect::<Result<Vec<u32>>>`, which does not guarantee exact capacity. [Import admission](/home/rb/worktrees/borsuk-paired-partition-source-screen-native/crates/borsuk/examples/compare_native_replay/paired_partition_coverage.rs:759) nevertheless reserves exactly `4 × unit_count`. The vector remains live during spatial import, where passing it as a slice hides its capacity.

   A 129-entry vector with capacity 256 retains 1,024 bytes while this ledger charges 516. The [boundary fixture](/home/rb/worktrees/borsuk-paired-partition-source-screen-native/crates/borsuk/examples/compare_native_replay/paired_partition_coverage.rs:2873) repeats the length-based formula and checks only `.len()`, so it cannot expose that discrepancy.

   **Fix:** Allocate the known-size map without growth, or conservatively admit its allocation and carry its actual capacity charge through both imports.  
   **Falsifier:** Extend the 129-unit boundary fixture to check retained capacity and admission immediately below/at the resulting cumulative charge, including spatial import. This is an accounting-contract blocker; source inspection does **not** establish an actual 1 GiB overrun.

No other concrete blocker found. Reconstruction is byte-identical to base; the router helper’s only production change is visibility. Producer provenance, replay descriptor continuity, persistent seal reservation, seal-before-truth ordering, V7 trace hashing, and the literal fixture expectations were traced through their shared helpers.

**Unverified:** Compilation, Clippy, tests, native execution and measured resources. No edits or prohibited execution occurred. Passing those gates would still establish neither population ANN quality nor a cold/default/vendor win.
