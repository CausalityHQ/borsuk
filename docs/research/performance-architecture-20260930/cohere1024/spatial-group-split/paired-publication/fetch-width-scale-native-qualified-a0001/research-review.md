What this change does: Adds `--paired-scale` for two sealed, full-population 1M runs ordered fetch16 then fetch32. It reuses the completed-v4 parser and retains the historical 100k pins and peak limit.

I reviewed committed source at **`ff3c8cb3f18e6f1f768d66a574ad7ddf0b2ffc81`**, against its verified parent **`04eb6a75fa643d05de53a1cf05bdebf01880f459`**. All line references below refer to that checkpoint.

**Must fix before using this as a scheduling-only admission gate:**

1. **The final SQ8 plan is excluded from parity.**
   [L1651–1656](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1651) hashes only `trace`; [L4185–4193](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:4185) compares that hash, results, recall and charges. V7 separately records the actual selected pages and ordered byte ranges in `plan`, explicitly described by the producer as full range identity at [producer L2239–2271](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_cohere_native_baseline.rs:2239).

   Concrete counterexample: change the last query’s fixture range from `[[0,100]]` to `[[100,200]]`, then reseal and repin. The current bounds and byte-total checks accept that range, while every paired comparison remains unchanged. The report explicitly limits `plan_scope` to `TwoBitPlanTrace`; that narrower claim does not establish final physical-plan invariance.

   **Fix:** Compare the complete validated `plan` separately in scale mode, and add this resealed mutation falsifier. Otherwise different physical reads can escape the gate despite equal results and logical totals.

2. **The pre-open test can pass after an input-opening failure.**
   [L11748–11757](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:11748) asserts only that the error is not `std::io::Error`. But [L1352–1359](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1352) propagates errors directly from Rustix `openat`; a boxed Rustix error is not that type. Mutating only one arm also permits the cross-arm equality check to mask a weakened pin validator.

   **Fix:** Assert the exact admission error, using consistently wrong pins in both arms where appropriate. The historical tests already use that stronger pattern at [L12361–12416](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:12361). Otherwise the test does not protect its advertised validation-before-open boundary.

**Should fix:**

3. **File lifetime checks stop at each individual read. This is inherited, but the new mode inherits it too.**
   [L4175–4183](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:4175) reads A completely, then B. [L1427–1438](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1427) checks each file’s SHA, EOF and identity only when that read finishes.

   A can therefore be replaced during B’s read without a later check. More concretely, both arms may name one pathname: replacing its A file with the valid B file between reads satisfies the stored different-SHA/different-inode check, although both reported paths finally identify B.

   **Fix:** In scale mode, require distinct artifact roles and recheck both saved identities after both reads and at publication. Reuse the existing final recheck pattern at [L3364–3365](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:3364). Otherwise “frozen paired evidence” exceeds the reducer’s actual lifetime guarantee.

4. **Two remaining negative tests need sharper boundaries.**
   [L11763–11784](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:11763) tests fetch32/peak33, but lacks fetch16/peak17. Its tail mutation removes the terminating newline, so rejection can occur at row framing before whole-file SHA verification.

   **Fix:** Add the first-arm overrun case and a terminal mutation that preserves valid JSON, newline, byte length and semantic consistency while retaining the old file SHA. Otherwise those tests leave the selector-specific and final-file authentication boundaries incompletely exercised.

The source supports several positive conclusions. The aggregate peak includes all queries, including ordinal999; the comparison permits a **declared** peak32 in the second scale arm, rejects values above its selector, and retains ≤16 for both historical arms. The synthetic fixture supplies the current v7 fields, and `authenticate()` recomputes prefix and sealed-body hashes after its changes. I found no concrete compilation error through inspection.

Actual concurrency remains a separate evidence boundary. The library documents `leaf_peak_inflight` as zero for membership discovery at [generation L551–552](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:551). The existing native HTTP falsifier observes SOURCE/SQ8 concurrency separately and explicitly expects that field to remain zero at [L3533–3570](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:3533). Reuse its fresh, exact-source receipt if available; manually inserting peak32 into synthetic rows establishes parser acceptance only.

The parser bounds are concrete: 32KiB configuration, 64MiB result, 256KiB row, 2MiB report and bounded query population. The 512MiB input setting establishes a declared library cap; RSS, host limits, cold state, provenance and cost remain external qualifications, as the report correctly states.

For causal interpretation, the selector changes **both SOURCE and SQ8 widths** at [producer L1235–1244](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_cohere_native_baseline.rs:1235). This is a joint scheduling arm. A SOURCE-specific attribution would require holding SQ8 width fixed. Likewise, pooled 2000-query statistics describe a mixture of arms, not independent repetitions or a production configuration.

**Verdict:** Fix 1–2 and close the scale-mode lifetime gap before relying on this gate. Keep the checkpoint **UNVERIFIED** until the root records affected tests, required Clippy, the shim-unset workspace test build, real-input admission and the separate canary.

**Not checked:** Compilation, tests, artifacts, measurements or cloud state. No files changed or jobs launched.
