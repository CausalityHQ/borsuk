I found no output-correctness defect in the candidate (6d6e679f), reading the source only. I ran no Cargo, rustc or native code, so the arm stays **UNVERIFIED**. The main problem is the assembly gate as it is currently written, not the kernel. Line numbers below are for the files at 6d6e679f, not the stale worktree.

## What holds up

- **Score bits:** `finish_score` (`rotated_two_bit.rs:341`) is the same expression, in the same order, as the public `score` (`:412`). Each lane adds its bytes in row order starting from `-0.0`, which matches `Iterator::sum` for f64 on Rust 1.91. Rust never fuses into FMA unless asked. There is no cross-lane or horizontal add.
- **Error order:** a missing or malformed row, or an overflow, returns the same first error as the scalar path. Preceding buffered rows are finished first, and `?` runs in lane order. The only thing that differs on an overflow is that up to 3 later rows get looked up. The private lookup closures have no side effects, so nobody can see this.
- **Finite-score bound** (`:368-378`): I re-derived it. The Hadamard is orthonormal, coordinates stay below 2^132 and scores below about 2^581. It is sound, but nothing depends on it, because the buffered-finish logic keeps the error order anyway.
- **No new reads, layout, table or hydration:** nothing new is fetched, no heap allocation, records are borrowed. The trace push still happens before scoring, and source/SQ8 statistics and charges are unchanged. Units are bounds-checked by `admit_source_walks` (rejects `unit >= rows.div_ceil(32)`) before any scoring.
- **Scratch fallback:** prepare's existing refusal still comes first. The fallback switches on at exactly `required + 512 <= cap - trace`, and nothing new is rejected.

## Defects and gate problems, ranked

1. **High – the assembly gate's "packed independent-row adds" requirement is the wrong test and will probably fail a working kernel.**
   - The speedup this arm proposes comes from four independent add chains running in parallel, not from packed SIMD adds. The table loads are scattered, and baseline SSE2 has no gather. Under `forbid(unsafe_code)` plus portable flags, AVX2 code generation is impossible: calling a `#[target_feature]` function needs `unsafe`.
   - So the compiler will most likely emit four scalar `addsd` chains, or it may happen to pair them with `movhpd`/`addpd`. Either way, load ports are the limit: about 8 loads per 4 lookups.
   - **Repair, before any assembly is produced:** require four distinct loop-carried accumulator registers with no spill or reload, per-lane adds in order, and no `vfmadd`, cross-lane add or horizontal reduction. Accept packed or scalar form. As written, a failure would be a mismatch in code form, not evidence against the hypothesis.

2. **High (missing qualification) – the "≤512 B incremental stack" check has no measurement method.**
   - `score_authenticated_rows` is generic over the lookup closure and will be inlined into the `rank_walked_source_with_limit<…>` / `plan_walks` code. Its extra stack then shows up in the caller's frame, not in a separate symbol.
   - The primitive is a test-harness build (bench profile, unwinding, different closure), so its code is not the serving code.
   - **Repair:** measure frame sizes (the `sub rsp` amounts) of the serving functions that contain the scorer, in a release build of the production code, against the same functions at c49a2e6d. Also measure `PreparedTwoBit::score_four` (`#[inline(never)]`) in that same build.

3. **High (gate enforcement) – the code-generation seal is self-attested.**
   - At `rotated_two_bit.rs:852` the primitive only checks that `BORSUK_TWO_BIT_FOUR_ROW_CODEGEN_SHA256` is 64 hex characters. Any string passes, so "assembly before timing" is not enforced.
   - **Repair:** the root validator must show that the assembly receipt names this run's `executable_sha256`, its verdict is PASS, and it was written before the first cell. Alternatively, the primitive can read the receipt file and check its ELF hash.

4. **Medium – the serving fixture never proves the four-row path actually ran.**
   - In `four_row_tiny_builder_serving_parity` (`two_bit_generation.rs:2704`), the 511 and 512 runs only check that results match. On a host without AVX2, on non-x86, or with a mis-wired scratch argument, both runs are scalar and the test still passes.
   - **Repair:** add a test-only call counter (`#[cfg(test)]` atomic) in `score_four`. Assert it stays at 0 at +511 and is greater than 0 at +512 on AVX2 hosts, and stays at 0 elsewhere.

5. **Medium – the batching path is only checked through each unit's maximum.**
   - `score_authenticated_rows` returns only the maximum. A bug that drops or duplicates a lane, such as `score_four([b0,b0,b2,b3])`, goes undetected unless the dropped row happens to be the maximum. The `seen` check proves the lookups happened, not that every row's score was folded in.
   - **Repair:** sweep the target row t over 0..32 and 32..49. Make row t the unique maximum (for example, scale 0 on every other row with a positive dot) and assert the unit score bits equal row t's literal score.

6. **Low – signed zeros in the unit maximum.** Rust's `f64::max` may return either zero when the inputs are ±0, and the ranking at `:886` uses `total_cmp`. The scalar and four-row max loops are compiled in different code, so a unit holding both +0 and −0 scores could rank differently.
   - It can actually happen: a row equal to the mean gets scale 0, and a query orthogonal to the mean gives `mean_dot = 0`.
   - **Repair:** add a fixture with mixed ±0 scores in one unit, or document why this cannot occur.

7. **Low – error-row coverage gaps.** The bad rows {0,1,3,4,16,31} never test lane 2 and never touch the tail unit.
   - **Repair:** add rows 2 or 30, 44–47 (the last full group of the tail unit) and 48 (the single scalar tail row).

8. **Low (latent) – a validated record is not tied to the `PreparedTwoBit` that checked it.**
   - Scoring it against a different instance with a smaller `packed_bytes` would quietly score only a prefix. No current caller does this.
   - **Repair:** at the top of `score_four` and `score_validated`, add `assert_eq!(r.packed.len(), self.packed_bytes)`. This also tells the compiler the lengths, so it can drop the four per-iteration packed bounds checks (see optional item 1).

## Missing qualification evidence (none present yet)

- A compile of the exact revision, workspace Clippy with `-D clippy::correctness -D clippy::suspicious`, and `scripts/check_rust_test_build.sh`.
- Native debug and release runs of the three new tests on an x86 host with AVX2, plus one run where the scalar fallback is taken (non-AVX2 or aarch64).
- The amended assembly and stack receipts from items 1–2, bound to the timed ELF and to the production build.
- The primitive run itself, with resource limits checked by an independent validator.
  - `controls()` accepts any CPU quota at or below the period, so a 0.5-CPU group would pass; that does not prove "CPU1".
  - The primitive seals scores per row only for direct `score_four` calls. The timed path is checked only through the unit maximum (which item 5 fixes).

## Optional improvements, not defects

1. **Hoist the bounds checks** in `score_four` (`:387`): slice `&r.packed[..n]` once per lane and iterate `self.table.chunks_exact(256)`. This removes about 5 compare-and-branch pairs per iteration. That matters because lookup overhead is the risk the plan itself names.
2. **The AVX2 runtime check** (`rotated_two_bit.rs:58-71`) has no effect on what code runs. It turns the path off on every non-x86 host (Graviton) without any reason in the code, and the receipt field `avx2:true` hints at a causal role it doesn't have. Once the arm is qualified, consider qualifying per architecture and dropping the ISA check. Similarly, the 512 B "scratch" is stack memory, not heap scratch; the fallback is harmless but protects no real resource.
3. **Unchanged scope assumption:** the expected cold-path gain stays about 2.7% CPU at most, with no tail-latency gain established. The primitive keeps 32 records hot in L1 and the 512 KiB table in L2. Part of any gain may come from four rows sharing each 2 KiB table slice, not from parallel add chains. Attribute it that way.
