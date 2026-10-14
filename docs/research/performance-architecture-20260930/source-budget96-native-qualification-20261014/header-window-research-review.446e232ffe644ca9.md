**Verdict: GO for the next gate, which is native GREEN qualification of 8910ef50. HOLD on integration until that passes.** I found no false-success, refusal-ordering, memory or path defect in the repaired reader. The commit itself still says UNVERIFIED, and nothing so far shows it has compiled or passed tests.

## What I checked (read-only, at `8910ef50:crates/borsuk/examples/compare_native_replay.rs`)

1. **The binding now runs before any result file is opened.** The run-header loop at :4774–4848 finishes before the first `read_run_observed` at :4852. The `header-window` case at :13573 deletes all four run files. Before the fix that produced `No such file` (your RED a0002); after the fix it must produce `SOURCE runtime/header binding`. So the test tells the two states apart, and you have already seen its RED outcome natively.

2. **Comparing `get()` to `get()` is sound here.** `fields(native, names)` at :250 / :4726 demands exactly the listed key set, and `shared_byte_window` is not in `names`. So `native.get("shared_byte_window")` is always `None`, and the new lines amount to "this window is absent from identity and from inputs", per run and per arm.

3. **This class of bug is now closed for identity and bound_inputs.** `validate_scale_row` (identity/bound_inputs) treats only two fields as optional: `source_get_budget` and `shared_byte_window`. Both are now tied to the authenticated runtime config, so no other optional header field is left unbound in the same way.

4. **The result rows are tied to the headers too.** `read_run_observed_with_identity` checks the raw identity and bound_inputs rows for exact equality with the pinned headers (:1596, :1643). The startup state-bytes check (:1711–1727) requires `shared_byte_window_state_bytes` to be absent when no window is expected. A window cannot reach the comparison through the result files either.

## Minor (none blocks the next gate)

- **Intent hidden in the comparison.** Lines :4786–4787 only work as "must be absent" because `names` stays exact. If someone later adds `shared_byte_window` to `names`, the check quietly becomes "matching window allowed", and nothing in this reducer requires it to stay off. Writing `identity.get(..).is_none() && inputs.get(..).is_none()` would say what is meant, and it mirrors the historical guard at :4452.
- **One untested refusal path.** If the runtime config itself carries a window, it is refused with the generic `unknown/missing v2 field` message. That fails closed, but no test covers it, and the message doesn't name SOURCE.
- **Asymmetric case not tested.** A window in identity but not in inputs is caught twice: once by the new lines and again at :1706. A separate test is worth little.

## Evidence gap (procedural)

- **The c853 13-gate GREEN is for another commit.** It does not carry over to 8910ef50. The diff only touches the example target, so reusing the 47 bin tests and the selector/cover results is defensible if you record that scoping. The following still have to run on 8910ef50 itself:
  - compile;
  - the exact refusal test;
  - the positive ABBA test;
  - the example regressions;
  - release;
  - workspace correctness/suspicious Clippy;
  - the actual `check_rust_test_build`.
- **The GREEN launch looks prepared but unrun.** `launch-header-window-green-a0001/` holds upload receipts but no terminal or independent-verification receipt. I treated it as unlaunched or not yet terminal and did not poll or launch anything.

## Research-critic notes

- The repair addresses exactly the native counterexample. The earlier research report's minor items (fixture gaps, replay hashes, noise cautions) still stand and still apply, and nothing new was found.
- The measurement design leaves A1 vs A2 as the only noise reference, so one repeat pair can't estimate p99 noise. Keep the predeclared rule that a gain no larger than the repeat difference is inconclusive for adoption, and record Pareto points only.
- This screen can't support a SOURCE96 cold result, a claim of beating a vendor, or progress on the matched-quality/tail-latency/QPS/lifecycle-cost goal against both S3 Vectors and Turbopuffer. That goal is still unachieved.
