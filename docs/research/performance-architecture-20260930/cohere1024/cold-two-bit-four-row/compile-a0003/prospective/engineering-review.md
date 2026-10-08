**GO to the exact-source remote gate for `3138bfb0`. HOLD primitive timing and production delivery.** I found no necessary source repair.

Reviewed committed blobs against parent `896d0423`, the `74dc183d` method, and closed `2073936c` evidence. The diff is exactly five insertions/five deletions in `score_four`.

- **Arithmetic and lane independence are preserved.** Each lane starts at literal `-0.0`, receives the same checked lookup once per byte in serial order, and reaches the same ordered scalar finalizer. No horizontal operation, multiplication, or reassociation enters the accumulation loop. See [score_four](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/rotated_two_bit.rs:380).
- **Error behavior and authentication remain intact.** Validation, buffered error draining, row-order maximum folding, and scalar tails are unchanged. The public stateful callback remains scalar; production batching receives authenticated records through `plan_walks`. See [private scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:705) and [production caller](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1907).
- **No new dimension, allocation, or unsafe boundary defect found.** Four lanes represent rows, with dimensions still governed by existing packed geometry. Checked scalar loads precede vector construction. The archive checksum matches `Cargo.lock`; its production backend implements lane-wise addition. The [scalar-control facade](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/simd_control.rs:155) supplies every operation used, including `Add`. Changing this to `+=` would break that facade.
- **Existing witnesses are adequate for this repair.** The unchanged twelve mandatory tests cover literal score bits, distinct lanes/all byte values, signed zeros, cancellation, dimension boundaries, unique winners, error prefixes, scratch fallback, and builder/serving parity. No additional test expansion is necessary before requalifying the repaired revision.

Two release blockers remain:

1. **Packed emission is unproved for `3138bfb0`.** The retained old disassembly contains four scalar `addsd` chains. Its disposition remains `CODEGEN_GATE_UNMET_NO_TIMING`, without a performance rejection. The proposed early screen is appropriate only for stopping cheap failures; passing it cannot establish serial lane order, absence of horizontal reduction/FMA, or serving-path qualification.
2. **The ≤512-byte incremental stack bound is unproved.** `wide::f64x4` has 32-byte alignment. Source payload sizes and the [existing layout assertion](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/rotated_two_bit.rs:655) cannot account for alignment, spills, and simultaneously live production caller/kernel frames. Retain the exact-ELF high-water comparison before any primitive.

The proposed remote sequence is sound after the root freezes the new commands, source/archive identities, and resource protocol. Preserve all stated caps and primitive thresholds. Exact-source correctness, scalar-control execution, final assembly/stack qualification, and subsequent Clippy, unshimmed test-build, portability, real-input admission, canary, and cold-comparison gates remain required.

No files changed or native commands executed. This review establishes neither emitted SIMD nor speed.
