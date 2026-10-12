What this change does: Adds `--paired-scale` for the pinned 1M/full1000 width-16/32 comparison while retaining historical `--paired-v2`. Reviewed **`ff3c8cb3` against `04eb6a75`**, using committed source only; all line references below refer to that checkpoint.

**Must fix**

1. **[P1] v7’s physical fetch plan is omitted from parity.**
   The [comparison](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:4185) checks returned hits, recall, charges and `trace_sha256`. That hash covers only [`raw["trace"]`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1651); the producer emits a separate [physical `plan`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_cohere_native_baseline.rs:2239).

   **Concrete case:** Change the second fixture’s last-query `plan.ranges` from `[[0,100]]` to `[[100,200]]`, retaining its byte total, trace, hits and charges, then reseal and repin. The source checks admit that range, but the comparison never examines its identity and reports `trace_plan_parity: true`.

   **Fix:** Compare the complete ordered v7 plan alongside the trace; add a resealed plan-only mutation regression. Otherwise different fetch work can enter the scheduling comparison undetected.

**Should fix**

2. **[P2] The admission-before-open test can pass after opening the forbidden path.**
   The [assertion](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:11754) excludes only `std::io::Error`. However, [`open_input`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1352) propagates `rustix::fs::openat`’s `rustix::io::Errno` directly into the boxed error.

   **Concrete case:** Move input opening ahead of admission. The nonexistent `must-not-open` path produces `ENOENT`, yet the downcast remains `None`, satisfying this assertion.

   **Fix:** Assert the specific admission failure for each mutation, with a valid-config/nonexistent-input control. Otherwise this test does not protect the boundary named in its title.

3. **[P2, inherited] Input lifetime checks stop before paired publication.**
   [`Rows::finish`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:1427) checks each descriptor and pathname at that file’s EOF. The [paired reducer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:4175) reads A, then B, and publishes without rechecking A, B or the config.

   **Concrete case:** Replace or modify A after its reader finishes while B is being parsed. The report can still succeed while its recorded A pathname no longer identifies the authenticated artifact.

   **Fix:** Recheck both stored file identities at publication and retain equivalent config binding. The existing [`recheck_parity_path`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/compare_native_replay.rs:3245) provides a reusable check. This predates the patch but limits the new mode’s lifetime guarantee.

Source inspection supports these narrower conclusions:

- The scale branch permits peak **32** for selector32 and rejects **33**; selector16 rejects **17**. Historical mode retains ceiling16 for both arms.
- The v7 fixture’s fields and seal construction appear consistent with the current producer. Its synthetic concurrency values do not establish native width32 execution.
- File/line/config/report caps remain bounded. The 512MiB input pin is not proof of observed resource compliance; the report correctly leaves external qualification required.
- Add the missing scale **selector16/peak17** regression: the current scale tests would miss replacing both selector-specific ceilings with32.

**Verdict: fix 1–3 before qualification.** No definite new compile error identified, but this checkpoint remains uncompiled and untested. Root still owns affected-target tests, required Clippy/workspace test-build, authenticated admission and canary gates.

**Not checked:** runtime behavior, performance, dataset bodies or cloud state. No files edited or builds/tests launched.
