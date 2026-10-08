## Verdict: GO to the exact-source remote gate. No source change is needed.

The primitive and all timing stay on HOLD until your own audit of the exact compiled binary (ELF) and its serving stack passes. I did not compile or disassemble anything. Everything below comes from reading the git objects and the pinned `wide` archive, so none of it is evidence of what instructions the compiler emits or of speed.

### What I checked

- **The diff:** 3138bfb0 is the only child of 896d0423 and changes one file, `crates/borsuk/src/rotated_two_bit.rs:384-396` @3138bfb0. It matches the 74dc183d plan line for line. Tests, `two_bit_generation.rs`, `simd_control.rs`, `Cargo.toml` and `Cargo.lock` are unchanged.
- **The pinned crate:** the archive at the pinned path hashes to `0ce5da8e…16cd03`, which matches `Cargo.lock:3877`.
- **How `wide` 0.7.33 builds `f64x4` without compile-time AVX:**
  - It is two `f64x2` halves, declared `repr(C, align(32))` (`f64x4_.rs:9-11`).
  - `Add` adds each half (`f64x4_.rs:50-66`), and each half uses the SSE2 `add_m128d` (`f64x2_.rs:93-100`).
  - `splat`, `From<[f64;4]>` and `to_array` are plain bytemuck `cast`s (`lib.rs:486-510`, `f64x4_.rs:1480`). Lane i is array index i, and the round trip returns the same values in the same order.
- **Build settings:** the a0002 protocol uses rustc 1.98.0 with no RUSTFLAGS and no target-cpu, so the SSE2 path is the one that gets compiled.

### Correctness: no problems found

- **Error order is unchanged.** Nothing in the loop can fail. The four `finish_score(rN)?` calls still run in order after the loop, and the caller's buffering and drain at `two_bit_generation.rs:715-735` is untouched.
- **Each lane's arithmetic is unchanged.**
  - Each lane performs the same IEEE double additions, starting from -0.0, in byte order. A packed add behaves per lane exactly like the old scalar add.
  - Signed zero holds: -0.0 + x is exactly x. The result is therefore bit-identical to the literal oracle (which also starts at -0.0) and to `.sum()`.
  - NaN payloads cannot leak: any non-finite score becomes `Err(Record)`.
- **Lanes stay independent.** There is no reduction or horizontal operation in the source, and `to_array` runs only after the loop.
- **FMA cannot occur.** The loop has no multiply, and the build has no `+fma` target feature.
- **Dimension handling is unaffected.** The four lanes are rows, not coordinates, so there is no dimension tail. `packed_bytes` stays generic, and fewer than four rows still go through the scalar `score`.
- **Scratch use is the same.** The accumulator plus one contribution is 64 B, as before. The 512 B charge and the layout assert at `:652-659` still hold.
- **No new unsafe code.** `forbid(unsafe_code)` still holds. `wide`, `safe_arch` and bytemuck are already used in production for `f64x4`, in `lexical_simd.rs:5` and `v35_remote.rs:2112`.
- **Feature unification is not a risk.** No workspace crate enables `borsuk/scalar-control`.
- **The scalar-control facade has everything the change uses:** `splat`, `From<[f64;4]>`, `Add` and `to_array` (`simd_control.rs:159-180`).
- **The existing tests cover the change.**
  - A wrong lane order would fail the all-256-word matrix, which gives each lane a different value (`:558`).
  - The per-lane overflow test is at `:632`.
  - Signed zero and cancellation are tested at `:568`, and the planner tests mixed zeros at `two_bit_generation.rs:2463-2496`.
  - One gap, not blocking: no test puts -0.0 and +0.0 in different accumulator lanes in a single call. A packed add can't mix them up, so I'm not asking for a new test.

### Things your next job needs to get right (none of them are source changes)

1. **Limit the early packed-add screen to the loop body.** The old 896 symbol legitimately contains `addsd` in the scoring code after the loop (0x1b6a87a, 0x1b6a8bc, 0x1b6a8ee, 0x1b6a915), so a check for "no `addsd` anywhere in the symbol" would stop a good build.
   - Require at least one legacy `addpd` (encoding 66 0F 58) in the loop body, and no scalar `addsd` updating the accumulators there.
   - Do not require `vaddpd` or ymm registers: without compile-time AVX, they won't appear.
   - Reject `haddpd`, `hsubpd` and `vfmadd*`.
   - Shuffles after the loop that pull lanes out (`unpckhpd`, `movhlps`, `shufpd`) are not horizontal reductions; don't count them.
   - Find the symbol by its demangled path `PreparedTwoBit::score_four`, not the full mangled hash, and require exactly one copy.
2. **Count stack realignment in the 512 B audit.** `f64x4` is new in this kernel and is 32-byte aligned. If the release build keeps any `f64x4` on the stack, the function gets an `and $-32,%rsp` prologue. The incremental stack figure must then include the worst-case realignment padding (up to 16 B, or 24 B to be conservative), not just the offset seen once.
   - Because `score_four` is `#[inline(never)]` and its callers are unchanged, the callers' frame sizes should match 896 exactly. Any difference there needs an explanation.
   - For reference, 896's `score_four` frame was 96 B: six pushes, `sub 0x28`, and the return address.
3. **Cap the primitive at 128 processes.** The harness rejects `pids.max > 128` (`:740`). The 512 limit used for the native gates would make the primitive INVALID.
4. **Prove the audited ELF is the one that ran the tests.** Hash the retained release ELF before the release tests and again after all stages. Build scalar-control in a separate `CARGO_TARGET_DIR`.
5. **Classify a scalar-control build failure by where it occurs.** There is no recent scalar-control compile receipt on rustc 1.98; the facade dates from 784088f4. A failure outside `rotated_two_bit.rs` would be old facade problems, not a verdict on this repair: record where it fails and treat it as INVALID.
6. **Don't apply Clippy's suggested `+=` fix.** By default `wide` implements `AddAssign` (`lib.rs:332`), so `dots = dots + …` will probably raise the style warning `clippy::assign_op_pattern`. That doesn't fail the gate (`-D clippy::correctness -D clippy::suspicious`). Applying the fix would break the scalar-control build, which has no `AddAssign`. Add an allow only if a warning-free gate is required.

### What a pass would and would not show (no gate change)

- **Packing doesn't shorten the critical path.** Each lane still needs one dependent add per byte, because byte order is mandatory. On the loading side, four `addsd`-with-memory-operand instructions become 2×`movsd`, 2×`movhpd` and 2×`addpd`; that is not obviously fewer micro-ops.
- **The real gain was already in 896.** Running four independent row chains, which touch each 2 KiB table slice once per four rows instead of once per row, is what helps. 896's scalar code already did that.
- **The primitive compares against single-row scalar, not 896.** It compares the four-row path with single-row `score()` (`:675-686`). So a PASS shows "four-row kernel beats single-row scalar", not "packed adds made it faster". Packed versus 896's four scalar chains stays unmeasured, and the write-up must not credit the speedup to SIMD.
- **The ISA is fixed at compile time.** The runtime check requires AVX2 (`:64`), but the kernel compiles to SSE2. A production build with target-cpu or `+avx` would switch `wide` to 256-bit registers and invalidate the audited ELF. Binding to the exact ELF already covers this.

Your proposed order is sound: release no-run first, then the cheap screen (it can only stop the run, never pass it), then the 12 mandatory tests in debug and release, integrations in both, and the scalar-control codec test. Your exact-ELF and serving-stack audit, the primitive, workspace Clippy, `check_rust_test_build.sh`, the portability checks, and the real-input admission, canary and cold comparison all remain required before anything goes to production.
