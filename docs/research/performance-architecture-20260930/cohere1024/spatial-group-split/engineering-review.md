**BLOCK for the requested qualification gate on `7372f116e18a2ce6a597de61de623d6cfb932a53`.** I found two concrete gaps to repair first. I found no source-proven compiler failure or defect in the spatial splitting rule itself.

Verified with Git objects: parent is exactly `2e2a789ad0e11c5d79348b93e21fdc8066425eac`; only `semantic_unit_router.rs` changed. Locations below refer to that immutable candidate.

1. **Publication/import admission still charges radial costs.**  
   Public [`admit()`](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:617) always selects radial mode. Existing [publication](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/two_bit_store.rs:1292) and [repackaging](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/two_bit_build.rs:254) callers use it against their remaining budget, but now accept spatial artifacts. [`validate_publication()`](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1728) checks spatial admission against the **artifact’s recorded construction limit**, not that remaining caller budget.

   Consequently, a budget between the radial and spatial estimates passes the outer admission and proceeds with spatial validation. This is a demonstrated admission-contract mismatch, not evidence of an actual OOM.  
   **Smallest repair:** authenticate the partition and check its estimate against the caller’s remaining budget before spatial allocations; alternatively, explicitly reject spatial imports in these adapters until supported. Keep existing caps.

2. **The radial nonfinal-64 negative test fails at the wrong check.**  
   The [mode-switch mutation](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:2338) changes partition and algorithm but retains the spatial `modeled_peak_allocation_bytes`. For this fixture, that estimate exceeds radial by 384 bytes. [`decode_root()`](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1194) therefore rejects `"binary exact lengths/accounting"` before inspecting the `43/43/43` radial directory.

   The test would still pass if the radial full-leaf restriction were accidentally removed.  
   **Smallest repair:** recompute the radial estimate in the mutated manifest and assert the intended directory error. This directly addresses the required “mutation negatives reach intended authority” condition.

One additional test improvement should accompany those repairs: [`spatial_allocation_admission_accounts_for_replay`](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:2433) exercises an **emission-dominated** geometry. Its expected increase is only recursion accounting; deleting the replay allocation charge would leave this test passing. Add a validation-dominated case that detects removal of the roster charge.

The remaining source checks were consistent with the contract:

- Trainer, assignment, center ties and radial default remain unchanged. Frozen radial membership/payload hashes remain asserted.
- Spatial splitting uses f64 spans, lowest-axis ties, numeric signed-zero equality and unit-ID ties. Quota recursion preserves exactly `ceil(n/64)` balanced leaves; terminal sorting restores radius/ID order.
- BORSUSR3 codes occupy `88..92`; `92..128` remains reserved. BORSUSR2, unknown codes and inconsistent descriptive algorithms are rejected.
- Publication verifies complete unique membership, original FP16 bytes and prototype bits, then replays spatial leaf membership within each stored group.
- The limitation is explicit at [line 1792](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1792): centers are absent, so replay proves neither original trainer assignment nor terminal radius order.
- The 3,200-unit falsifier uses shared emission/publication/open and actual L2 top48 selection. Its assertions include both recovered target units/pages and displaced distractor losses.
- All seven spatial tests are present. Separate full-builder tests cover dimensions 1/2/1024, partial rows, determinism and rejection of 1025. Membership-swap and quota mutations reach their intended checks by source tracing.
- Allocation checks precede production decoding/cloning and replay allocation; recursion is bounded and library caps are unchanged. Actual runtime, capacities and RSS remain unmeasured.

**Native tests, compilation and Clippy remain UNRUN.** No files were edited, and no experiments, data parsing, network work or children were started. This review grants no adoption, recall, performance or vendor-win claim; historical `312/320` KILLs remain unchanged.
