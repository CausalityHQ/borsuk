**BLOCK** for `556df287` against `9f482302`. Two source-visible errors can classify invalid execution as a valid diagnostic FAIL.

What this change does: It separates query-free spatial preparation from paired SOURCE coverage, authenticates original producer authority independently, and seals both selections before opening truth. The reconstruction module is byte-identical to base, and the changes stay within the approved three files.

1. **P1 — Invalid root metadata becomes resource FAIL.**  
   [paired_partition_coverage.rs:1211](/home/rb/worktrees/borsuk-paired-partition-source-screen-native/crates/borsuk/examples/compare_native_replay/paired_partition_coverage.rs:1211) maps every error beginning with `modeled allocation admission:` to `ResourceFailure`. However, [semantic_unit_router.rs:1228](/home/rb/worktrees/borsuk-paired-partition-source-screen-native/crates/borsuk/src/semantic_unit_router.rs:1228) also emits that error when the root’s embedded construction limit is insufficient. With ample caller budget, a spatial root rehashed after setting bytes `80..88` to zero therefore produces FAIL rather than metadata INVALID.

   **Fix:** Distinguish the enclosing budget refusal from root-validation errors. Validate the embedded limit against the expected mode-specific peak before applying resource classification; preserve decoder failures as INVALID.

   **Falsifier:** Mutate that field in an actual public-builder spatial publication, recompute its pin, and import it with the 1 GiB envelope. Require INVALID. Pair this with the existing valid-root, insufficient-caller-budget case, which must remain FAIL. Otherwise a corrupt artifact can wrongly reject the algorithm.

2. **P1 — Early budget refusal bypasses authority validation yet reports complete FAIL.**  
   [paired_partition_coverage.rs:1059](/home/rb/worktrees/borsuk-paired-partition-source-screen-native/crates/borsuk/examples/compare_native_replay/paired_partition_coverage.rs:1059) admits metadata before `authenticate_construction`; the screen follows the same ordering at line 1984. Set `max_owned_bytes=1` and change the decoded original approval schema to an invalid value, then rehash config/authority. The ledger fails first. Preparation publishes `FAIL, complete:true`; the screen’s failure handler likewise reports complete FAIL without reaching the invalid authority.

   **Fix:** Validate decoded schema, identity and geometry before resource classification. A refusal before authority authentication completes must preserve an incomplete admission disposition; complete diagnostic FAIL requires a valid authenticated attempt.

   **Falsifier:** Extend the low-budget fixture with an independently rehashed invalid original schema or geometry. Require INVALID/incomplete, zero truth opens, and no successful spatial artifacts. The current low-budget fixture checks only the valid-authority case.

The inspected source supports several important safeguards: fresh replay root/member descriptors replace the imported descriptors only after identity continuity checks; persistent seal ownership remains charged; repeated phase labels preserve their maximum; and V7 trace hashing uses the native parser’s canonical trace hash. The strengthened full-path fixture asserts returned **9**, radial SOURCE **10**, spatial SOURCE **9**, and displaced loss **1**. These are authored assertions, not passing-test evidence.

From first principles, widest-axis quota splitting belongs to the balanced axis-aligned partition family. Its geometric compactness can improve while physical SOURCE locality worsens, so equal group assignments, displaced losses and planned closure costs remain necessary controls. A one-slot improvement on consumed32 would still establish only this SOURCE screen; downstream candidate scoring, held-out recall and matched vendor performance remain unmeasured.

Compiler/runtime status is **UNVERIFIED**. After repairing the classification paths, freeze the revised source for the single remote qualification: affected debug/release builds, authored fixtures, required workspace Clippy and bounded workspace test compilation. Real screening still needs authenticated-input admission and the separate canary.

No edits, builds, tests, data-body parsing, network/AWS calls, children or consultations were performed.
