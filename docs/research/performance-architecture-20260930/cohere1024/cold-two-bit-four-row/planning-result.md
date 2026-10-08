**Falsify four-row two-bit scoring next**, using independent f64 accumulators and compiler-emitted SIMD. This addresses the serial accumulation chain that within-row gathers left intact. SQ8 already batches rows; the rejected SHA and gather arms stay closed.

Inspection used committed source at `32a23302` and reused both existing reviews. That revision has no `score_authenticated_walks` symbol; it would be a new private boundary.

1. **Make one two-file candidate.** In [rotated_two_bit.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/rotated_two_bit.rs:301), add crate-private record validation and a four-row scorer accepting validated borrowed records. Use checked scalar table lookups and four independent accumulators. Preserve each row’s byte order, the frozen `.sum()` initialization, and final arithmetic; prohibit horizontal reductions, reassociation and FMA. Keep `forbid(unsafe_code)` and baseline compiler flags. Require release assembly to demonstrate SIMD across rows; scalar syntax alone is insufficient evidence.

   In [two_bit_generation.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1730), use it only after authenticated source ranges have arrived. Share the existing ranking, admission and trace logic through a private unit-scoring boundary. Keep public `plan_two_bit_source_walks` and its stateful `FnMut` callback scalar. Retain scalar dispatch on non-AVX2/non-x86 targets, short tails and insufficient scratch.

2. **Preserve observable ordering explicitly.**
   
   - Public callbacks remain fetch → score → next callback, including post-score errors.
   - Private authenticated lookups visit rows in original order and validate each header before accessing the next. On a missing or malformed row, finish buffered preceding rows before returning the original error.
   - Independently document the finite-score bound for valid construction: finite f32 inputs/scalars and the existing dimension ceiling permit a conservative `|score| < 2^600`. This excludes a deferred overflow error in production; authentication alone does not establish that argument.
   - Fold scores into the unit maximum in row order. Preserve unit evaluation order, trace insertion timing, tie handling and error wrapping.
   
   Source requests already finish before scoring. Keep their ranges, ETags, authentication, drain behavior and charges unchanged; a planning error must initiate no SQ8 request.

3. **Bound work and memory.** Full units become eight four-row groups; a 17-row tail becomes four groups plus one scalar row. Successful work retains exactly the same rows, header decodes and `rows × packed_bytes` table lookups. No padding rows, speculative GETs, table copies, payload changes or source hydration.

   Hold only four borrowed validated records and fixed accumulator/output arrays. Preregister **512 bytes of incremental scoring scratch**, verify the release stack frame against it, and include it within the existing query cap. Select scalar scoring when it does not fit, preserving existing admission behavior.

4. **Use a tiny falsifier before timing.** Compare every score’s `to_bits()` and error against an independent copy of the frozen scalar expression in debug and release. Include distinct lane words, all 256 byte values, signed zero, and the literal cancellation sequence `1e16, 1, -1e16, 1, 1 → 2`. Cover dimensions `1,3,5,255,256,257,768,1023,1024,1025,4097`, full groups and tails.

   Inject missing records and invalid scalars at rows `0,1,3,4,16,31`. Require identical callback prefixes, first errors, ordered scores/IDs, plans, traces and recorded request/charge fixtures. Run affected tests, required workspace Clippy and `scripts/check_rust_test_build.sh` before integration.

5. **Reuse the fixed v2 timing method as a newly identified arm.** Preserve historical receipts. Use D`257/768/1024/1025` × panels`32/17`, five counterbalanced blocks, **80 cells**, fixed repetitions targeting 256 MiB encoded bytes per cell, preparation excluded, and sealed score outputs. Time validation and batching overhead as well as accumulation.

   Caps: **CPU1, 256 MiB, swap0, pids128, 30 CPU seconds, 120 wall seconds**; every cell needs ≥50 ms CPU. Require D1024 candidate/scalar median **≤0.80** for both panels and both order subsets; other geometry medians **≤1.05**. Independently verify receipt math, identities and exits. Incomplete cells, resource or method failures are **INVALID**, not algorithm REJECT.

Only a passing primitive warrants real-input admission, separate canary and frozen cold ABBA. Require exact quality/work parity, ≥5% p90/p95 improvement in both pairs and QPS nonregression.

No edits or native/data/cloud execution occurred. Main risks are compiler scalarization, lookup overhead and limited cold impact: a 20% scorer saving represents roughly **2.73% total CPU** at the measured 13.65% share, with no established tail-latency gain.
