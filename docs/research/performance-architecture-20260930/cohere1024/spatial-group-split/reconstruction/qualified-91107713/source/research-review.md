**HOLD — source-only UNVERIFIED.** Reviewed `a645ca402d25f5fe0dfdfb0509d6929b1e811755` against `483b7f69b2cb59f600f9cf6a7028fb4a91d5dbad`. Later HEAD changes are excluded.

What this change does: It reconstructs authenticated BORSUCP1 bytes without opening historical root/membership or query/truth. Optional parity actually builds and validates current radial artifacts, then compares the historical publication with only the two prescribed literal exceptions.

**Must fix**

- **F2 — Output mutation can be absorbed into the post-sync stamp.** At [partition_reconstruction.rs:611](/home/rb/worktrees/borsuk-partition-centroid-reconstruction-native/crates/borsuk/examples/compare_native_replay/partition_reconstruction.rs:611), the complete file identity is captured **after** `sync`. A concrete counterexample is a callback—or concurrent writer—that successfully syncs, then truncates and rewrites the same expected bytes before returning. Exact readback passes, the new timestamps become the accepted snapshot, and subsequent checks pass. That rewrite has received no successful file sync; directory sync does not establish its durability. The receipt callback at [line 735](/home/rb/worktrees/borsuk-partition-centroid-reconstruction-native/crates/borsuk/examples/compare_native_replay/partition_reconstruction.rs:735) also lacks this guard.
  
  **Minimum repair:** Stamp each completed write immediately before file sync and require the full identity to remain unchanged afterward. Apply this to artifact files and the receipt through the module’s sync wrapper. Add a falsifier that syncs, rewrites identical bytes, and requires failure with no consumable success receipt. This is a source defect beyond F1’s missing coverage.

**Should fix before accepting the frozen-fixture evidence**

- **F3 — The shared historical dependency closure is only partially pinned.** The assertions at [line 1020](/home/rb/worktrees/borsuk-partition-centroid-reconstruction-native/crates/borsuk/examples/compare_native_replay/partition_reconstruction.rs:1020) pin the centroid decoder, trainer, metric and lockfile. But the metric calls `simd_control.rs`, and trainer resolution passes through library exports; those dependencies are shared without fixture assertions. An arithmetic change there can affect both builders together while their comparison still passes. Lockfile equality also does not freeze enabled features or target arithmetic.
  
  **Minimum repair:** Pin the remaining shared arithmetic/export sources and record the exact build features/target. Independently frozen old-output hashes would provide a stronger fixture anchor. **At these exact revisions, I found no transitive source mismatch:** the copied construction/encoder matches `2e2a789…`, apart from the declared omissions. This finding concerns the falsifier’s protection against common drift.

The remaining core argument is sound by inspection:

- Separate approval pins establish authority independently of config.
- Reconstruction scatters unchanged finite FP16 words and checks both full leaf SHA and whole centroid SHA.
- Actual radial regeneration, unchanged persisted allocation limit, full current validation/open, and exhaustive byte comparison support this specific historical structural-equivalence proof. An old decoder/exporter adds no necessary evidence here.
- Cumulative admission adds external metadata to the builder estimate, counts the centroid once, drops reconstruction scratch, and streams historical bodies. I found no additional concrete lifetime undercharge.
- The fixture includes 4097 rows, D1024, 129 units, the partial tail, signed zero, both subnormal signs, finite half extrema, and record permutation. These establish reconstruction cases, not ANN quality or performance.

**F1 remains known and pending for the requested candidate:** successful sync rosters/counts and parity-mode mutation coverage require reconciliation.

**Compile inspection:** I found no apparent type, import, module-path or committed include-file blocker under the declared Rust toolchain. Only exact-source EC2 compilation and the required native, release, Clippy and workspace test-build gates can establish success.

No edits, builds, runtime/data execution, network access or consultations were performed.
