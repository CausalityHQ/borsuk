What this change does: batches four authenticated source rows with independent serial f64 accumulators while retaining the public scalar scorer and callback behavior. **Verdict: HOLD—one concrete fixture defect blocks qualification.**

Reviewed immutable `6d6e679fcd4738a036ab8bfe716b86e54a916748` against `c49a2e6d`. All line references below refer to that candidate, not the stale working tree.

**Must fix:**

1. **P1 — The builder/serving fixture fails before reaching serving.**  
   [two_bit_generation.rs:2581](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:2581) creates application IDs `1000 + (rows − row) × 3`, but [line 2616](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:2616) calls `.build()`. That API supplies no source permutation, so [two_bit_source.rs:228](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_source.rs:228) treats each ID as a raw-source ordinal and rejects IDs ≥ `rows`.

   The first D1 fixture has 49 rows and first ID **1147**, yielding `Plane(Invalid("source ID"))` and panicking at the builder’s `unwrap()`. Consequently, none of its serving, ordered-result, trace, GET/byte, or scratch-boundary assertions execute.

   **Minimal repair:** retain the useful nonordinal application IDs and call `build_with_order` with the identity source permutation `0..rows`, since raw and SQ8 vectors already share physical order. Qualify the repaired, newly frozen revision.

**Missing qualification evidence, separate from implementation defects:**

- **Assembly and stack admission remain blocking before timing.** The [size assertion at rotated_two_bit.rs:655](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/rotated_two_bit.rs:655) counts selected Rust values, not emitted stack usage. The [codegen receipt check at line 852](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/rotated_two_bit.rs:852) accepts any 64 hexadecimal characters; it does not validate a receipt or its ELF binding. The root must independently verify the actual receipt, portable compiler flags, packed independent-row additions, absence of horizontal reduction/reassociation/FMA, and ≤512 bytes incremental stack across the serving caller/kernel path—including scalar fallback.

- **Native correctness and portability are unverified.** After repairing the fixture, require debug/release parity on an AVX2 host, actual non-AVX2 dispatch and non-x86 qualification, then the required workspace Clippy and unshimmed test-build checks before integration. Source inspection cannot establish compilation or emitted arithmetic.

- **The primitive has no measured disposition.** Its source implements the stated 80 cells, fixed byte targets, minimum CPU interval, counterbalanced orders and ratio thresholds. Admission still requires independently checked source/ELF/codegen identities, enforced resource limits, complete receipts and terminal exit status. An invalid fixture or execution is **INVALID**, not evidence to close the algorithm as REJECT.

The production source audit found no additional concrete arithmetic or authentication defect. Each row retains byte order, negative-zero initialization and scalar final arithmetic; the finite-score argument follows the actual constructor bounds. Public callback ordering and shared ranking/trace/admission remain intact. I found no added GET, payload layout, table allocation or source hydration.

**Optional improvement:** add a mixed `+0.0`/`-0.0` unit-maximum and page-tie witness. Existing literal zero cases use homogeneous rows and do not directly pin that folding case.

Not checked: compilation, execution, assembly or performance. No files were edited; no native/data/network/AWS work or consultations were run.
