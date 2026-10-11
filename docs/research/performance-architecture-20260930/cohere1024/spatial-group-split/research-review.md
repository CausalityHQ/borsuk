What this change does: preserves training and group assignment, adds opt-in spatial splitting, and keeps radial partitioning as the default under BORSUSR3.

**BLOCK for the requested native correctness qualification: repair finding 1 first.** I found a concrete test-authority defect, but no demonstrated partition logic defect or compiler error.

I reviewed committed candidate `7372f116e18a2ce6a597de61de623d6cfb932a53`, verified its exact parent `2e2a789ad0e11c5d79348b93e21fdc8066425eac`, and confirmed that only `semantic_unit_router.rs` changed. Locations below refer to that committed source.

**Must fix**

1. **The radial-mode negative fails before reaching the intended authority.**  
   [Mutation test](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:2338), [earlier accounting check](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1199), [radial leaf rule](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1216).

   **What this is:** The test relabels spatial `43/43/43` leaves as radial and expects opening to fail.

   **Problem:** It updates the partition and `Algorithm`, but retains the spatial allocation estimate. For this geometry, spatial’s estimate exceeds radial’s by 384 bytes. Decode therefore rejects `binary exact lengths/accounting` before checking radial’s nonfinal-64 requirement.

   **Smallest repair:** Recompute `bad.modeled_peak_allocation_bytes` with radial `admit` using the same geometry and limit, then assert the leaf-geometry rejection. Keep the earlier inconsistent-Algorithm rejection separately.

   **If skipped:** The test still passes if radial’s nonfinal-64 protection is accidentally removed. That violates the requested requirement that mutation negatives reach their intended authority.

**Should fix**

2. **Exercise the serving nomination path in the spatial falsifier.**  
   [Falsifier closure checks](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:2172), [actual serving caller](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/two_bit_generation.rs:1642).

   **What this is:** The new tests derive closure through `validate_selected`; serving and the comparison harness use `nominate_selected`.

   **Problem:** Existing radial tests establish parity between those paths, but the new spatial tests do not directly exercise nomination from the inverse directory.

   **Smallest repair:** For both falsifier arms, assert that `nominate_selected(&ids)` equals the existing payload-validated nomination. Add the same assertion to the full-builder round trips.

   **If skipped:** Spatial closure evidence covers the shared completion helper, but lacks direct coverage of the path serving actually calls. This is a coverage gap, not a demonstrated wrong result.

The partition implementation otherwise matches the stated source contract:

- [Splitting](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:791) computes spans in `f64`, selects the lowest axis on equal spans, and treats signed zeros numerically equally before unit-ID ties. Quota recursion visits left first and restores radius/ID order at terminals. Empty and ≤64 groups retain their existing behavior.
- The quota formula yields exactly `ceil(n/64)` leaves, with sizes differing by at most one and each ≤64. With at most `c` assigned groups, the unchanged total bound follows from `sum ceil(nᵢ/64) ≤ c + groups − 1 ≤ 2c − 1`. Admission bounds the arithmetic and requires 64-bit `usize`.
- [Encoding](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1058) rejects inconsistent `Algorithm` fields. Codes 1/2 occupy `88..92`; `92..128` remains reserved. Decode rejects BORSUSR2 and unknown codes. Both reader locations retain radial’s full nonfinal chunks and apply exact quota-size validation to spatial mode.
- [Publication validation](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1708) retains original FP16 byte checks, bitwise prototype checks, complete unique membership, and spatial replay against authenticated centroids. The rehashed cross-leaf swap reaches the intended replay check.

The replay limitation is explicit at [line 1792](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:1792): training centers are absent, so publication proves spatial leaf sets within stored groups. It cannot independently prove original center assignment or terminal radius order. Those remain producer responsibilities.

All seven new test bodies are present; none has native execution evidence:

| Test | Source assessment |
|---|---|
| Quota edges and terminal ordering | Covers the requested sizes, roster and ordering. |
| Axis ties, signed zeros, identical vectors, extremes | Exercises the intended comparisons. |
| Post-assignment top48 falsifier | Real BORSUCP1, Scale1m D2, 3,200 units, both 50 leaves; checks recovery **and** displaced distractors. |
| Full-builder dimensions, partial rows, determinism | Calls the real trainer at D1/D2/D1024; rejects D1025 and nonfinite values. D1/D1024 fixtures are entirely identical vectors. |
| Version, mode, Algorithm and balance refusals | Contains finding 1. The unbalanced-size mutation reaches the quota authority. |
| Rehashed membership, FP16 and prototype mutations | Intended authorities are reachable; membership explicitly asserts the replay error. |
| Allocation admission | Checks the modeled boundary, but this small geometry’s maximum is emission, so its 384-byte difference does not separately establish replay capacity. |

The old radial membership and payload hashes remain unchanged in [the regression test](/home/rb/worktrees/borsuk-spatial-leaf-partition-native/crates/borsuk/src/semantic_unit_router.rs:3164). Trainer, assignment and prototype computation remain unchanged in source.

Allocation admission precedes centroid decoding and sample cloning; replay uses one exactly sized group roster. Profile caps remain unchanged. I found no substantiated allocation-overrun defect. The accounting remains a capacity model: recursive sorting, construction time, publication replay time and actual RSS still need measurement.

From first principles, this is a quota-balanced variant of widest-axis k-d partitioning. It addresses directional cancellation that radial sorting can preserve, while retaining axis sensitivity, outlier sensitivity and cross-group fragmentation. PCA directions or additional clustering would introduce larger interventions. The current candidate is appropriately narrow for a falsifier.

The synthetic recovery also increases page closure from **384 to 392 pages**: 16 gained, eight displaced. Equal leaf counts and selected-unit counts therefore do not establish equal serving work or lower lifecycle cost.

**Verdict: fix 1 before native correctness qualification; add 2 to establish serving-path parity.** Compilation and runtime correctness remain unverified. Historical cosine/boundary `312/320` KILLs remain unchanged; this review establishes no quality, adoption or vendor-win result.

No files were edited, and no Rust execution, tests, mocks, data parsing, network access, children or experiments were used.
