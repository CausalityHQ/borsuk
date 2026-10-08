**HOLD — stop this scoring line and return to the measured fetch/layout bottleneck.** Explicit 256-bit AVX2 would be a distinct implementation, but the evidence does not justify another qualification cycle for widening alone.

The committed receipts establish a valid REJECT. Reaching the unchanged 0.80 ratio would require another **2.91% improvement over the current candidate for full32 and 4.68% for tail17**. That is conceivable, but the inspected assembly gives little reason to expect it:

- Each byte still requires four record-byte loads and four dependent table lookups.
- Replacing two independent 128-bit additions with one 256-bit addition also requires assembling both halves into a YMM register. Likely load-and-pack work stays similar; the serial accumulation dependency remains.
- Gather is a different mechanism already rejected. Neither gather nor reassociation should enter this proposal.
- The 10.247-second primitive understates the cost of another source qualification. Even passing the 20% scoring gate implies only 2.73% total CPU savings at the reported 13.65% share, with no demonstrated reduction in cold latency, bytes or GETs.

The minimal implementation scope is larger than the kernel body. [`borsuk` forbids unsafe code](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/lib.rs:2). Any explicit intrinsic implementation should use the existing architecture boundary in [`borsuk-fma`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk-fma/src/lib.rs:1), preserving that prohibition. The same held worker would need to cover:

- A safe, allocation-free helper around an x86/x86_64 `#[target_feature(enable = "avx2")]` kernel, with checked slice geometry and runtime feature detection before every reachable unsafe entry.
- [`score_four`](/home/rb/worktrees/borsuk-two-bit-four-row-native/crates/borsuk/src/rotated_two_bit.rs:380) and its direct test/primitive callers. Those callers bypass the serving eligibility check, so that check alone cannot guarantee ISA safety.
- Explicit exclusion under `scalar-control`, portable fallback on unsupported/non-x86 hosts, and unchanged serial byte order, `-0.0` initialization, scalar finalizers, error precedence and tails.
- Existing oracle, authenticated-serving and runner checks. No format, fixture, ranking or public serving API change is needed.

**Bounded falsifier for this HOLD, should root authorize one challenge:** preregister exactly that load-and-pack kernel, with no second variant or tuning sweep.

1. First require fresh affected-target compilation, the existing correctness matrix, workspace Clippy and `check_rust_test_build.sh`. Add coverage for unsupported-ISA/scalar-control dispatch and the new safe helper’s geometry boundary.
2. Inspect the actual primitive and serving ELFs: ordered four-row YMM accumulation; no gather, horizontal reduction, FMA or reassociation; scalar-control truly scalar and built in isolated targets. Recompute the complete nested serving stack increase: **≤512 bytes**, retaining the existing scratch admission.
3. After source-bound admission and the separate disposable canary, run **one unchanged 80-cell primitive**: D257/768/1024/1025 × full32/tail17 × five alternating-order pairs. Require exact score bits and unchanged errors; D1024 full and tail median ratios **≤0.80**, including **both order subsets**; other geometry medians **≤1.05**.
4. Keep **CPU1, 256 MiB, swap0, pids128, 30 CPU seconds, 120 wall seconds**, 256 MiB encoded work per cell and ≥50 ms cell CPU. Preserve terminal evidence and terminate compute immediately. A valid miss closes the arm; setup/interruption failures remain INVALID. Passing admits only a later root decision about cold qualification.

Skipped edits, native execution, Cargo, benchmarks, network and delegation. Instruction-cost expectations remain static inference; the new kernel’s performance and stack use are unverified.

