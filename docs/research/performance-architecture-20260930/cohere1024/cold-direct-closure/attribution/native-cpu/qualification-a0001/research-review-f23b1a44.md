# Critique of f23b1a44 (`native_process_cpu` helper)

**Verdict:** I found no correctness or compile defect in the helper's code, based on reading it only. Three things block qualification. The source stays **UNVERIFIED**. I did not change the 1.02 CPU gate, the 1.02 whole-process gate or the 1 ms wall gate, and no performance or sample authority is implied. I ran nothing except `git show`, `git merge-tree` and reads of the contract.

## Blocking repairs

1. **The exact source cannot be pushed as qualified.**
   - f23b1a44 and `origin/main` (c6dbddf6) are siblings: both have parent 2f344557, so this cannot be a fast-forward. `git merge-tree` reports no conflicts.
   - Workspace Clippy and `check_rust_test_build.sh` cover the whole tree. Results on f23b1a44 alone would not cover the commit that gets integrated.
   - Rebase onto c6dbddf6 first and run every gate on the new SHA.
   - Build from a clean export of that SHA, not from this worktree. The worktree has staged deletions of `compare_native_replay.rs` and `exact_sq8_runtime_probe.rs`, which the manifest still lists. A worktree build would fail for reasons unrelated to the helper.

2. **No positive control for a descendant that finishes but is never waited for.**
   - Example: a descendant exits before the child does and the child never waits for it. It ends up reaped by init. Its CPU is missing from `wait4`.
   - The survivor check does not see it either: the cgroup shows no live processes when the helper exits.
   - Only the comparison of drained cgroup CPU against `wait4` CPU can catch it. The current roster never shows that comparison has any detection power. `survivor` sleeps, so it adds only startup CPU, and root refuses it anyway.
   - **Fix:** add an `unwaited` fixture (two processes) that:
     - spawns a child that burns about 20 ms of CPU (e.g. `--fixture late`);
     - waits for that child to exit without reaping it, using `waitid(P_PID, pid, WEXITED|WNOWAIT)`;
     - then exits without reaping.
   - Root must show it flags roughly 20 ms of cgroup CPU missing from `wait4` while the cgroup is empty.
   - Also fix `accounting_scope`. "excludes unwaited survivors" understates what is excluded. It should say: every descendant not waited for inside the child's wait hierarchy, whether still alive or reaped elsewhere.

3. **The signal-forwarding code cannot be exercised by the planned fixtures.**
   - The required fixture list says "actual signal propagation", but nothing in the roster triggers the helper's re-raise path for a signal the child could have caught.
   - `timeout` exits normally with 124.
   - Rust children ignore SIGPIPE, so an externally sent SIGPIPE does nothing to them. That means the helper's `SIG_DFL` restore line can never be tested this way.
   - Choose one:
     - add a `sigpipe` fixture that resets SIGPIPE to default, burns 2 ms, then raises SIGPIPE; or
     - delete the re-raise and `exit(128+sig)` instead. The original raw status, core flag and signal are already in both the report and the receipt.
   - If the re-raise stays, disable core dumps before raising, as coreutils `timeout` does. Otherwise a child that crashes makes the helper write its own core dump.

## Verified sound

- **`wait4` coverage.** The reaped usage covers the child's whole thread group plus every descendant it waited for. Its user + system total equals the kernel's exact scheduler runtime, minus at most about 2 µs of microsecond truncation.
- **Helper self-usage.** `RUSAGE_SELF` excludes the child. Clarification for the scope text: the *absolute* entry snapshot already includes the helper's startup since fork. Only work after the second snapshot goes unreported.
- **Status handling.**
  - Status from the collector and status from the child are kept separate.
  - A spawn failure exits 125 and leaves the empty reserved report.
  - There is no double reap: the `Child` handle is never used after a successful `wait4`.
  - Every failure leaves either no report or no matching receipt, so root refuses it.
- **Durability.** The report is created exclusively with mode 0600. Both the file and its directory are flushed to disk before the child starts and again after the write.
- **Exec through `/proc/self/fd`.** This works for ELF even though the descriptor is close-on-exec. If `/proc` is missing, the spawn fails and the helper refuses to proceed.
- **Compile and Clippy hazards: none found.**
  - The locked libc version (0.2.186) matches the new requirement.
  - `tempfile` and `serde` are normal dependencies.
  - `autoexamples = false`, so the new `[[example]]` entry is required.
  - `zombie_processes` does not fire in `run()`, because there is a `wait()` on the error branch.
  - The remaining warnings (`redundant_closure_call`, `useless_vec`, `unnecessary_cast`, `seek_to_start_instead_of_rewind`) are outside the `-D` set.
  - The build script does not use `-D warnings`.

## Recommended, not blocking

- **Fixture output should include absolute clocks.** Every fixture should print the absolute process CPU clock at the end of its body.
  - Currently only deltas are printed, so the unknown startup and exit cost sits inside the comparison.
  - `pulse` checks only `child ≥ delta`. That does not catch values rounded *up* to 10 ms ticks.
  - Using wall time as the upper bound only works on a single-CPU cpuset, not a `cpu.max` quota.
- **Reset SIGCHLD to its default at helper entry.** If SIGCHLD arrives already ignored, `wait4` fails with ECHILD and the child's status is lost (exit 125). Root's environment sanitising does not cover inherited signal settings.
- **Production setup: helper → timeout → runner.**
  - `executable_sha256` will be the hash of `timeout`. Root must pin the runner binary itself.
  - `timeout -k` sends SIGKILL to its own process group, which kills `timeout` itself. The runner is then reparented and its CPU is dropped from `wait4`, yet the receipt still reads `REPORT_SYNCED` with signal 9.
  - Root's INVALID classification of that case must hold. The case also serves as a second positive control.
- **Evidence binding.**
  - Repeated runs of the same argv produce the same config SHA. Add an `invocation` nonce field to the config.
  - Create the report relative to the opened directory (`/proc/self/fd/{dir}/{name}`) instead of by path.
  - Re-check the executable's dev, inode, size and ctime after the child is reaped, to detect in-place modification.
- **Optional first-party completeness check.** Make the helper a child subreaper (`PR_SET_CHILD_SUBREAPER`). After reaping, a non-blocking `waitid(P_ALL, WEXITED|WNOHANG|WNOWAIT)` then detects both survivors and orphans directly. The cgroup check stays as the independent cross-check.
- **Fixture details.**
  - `threads` should use the per-thread CPU clock, giving a deterministic floor of at least 10 ms.
  - `io` has no oracle that discriminates anything.
  - `--ignored` or `--include-ignored` will run `child_cpu_exit` and kill the whole test harness with exit 7. Guard it on argv.
  - The child's `comm` will show the fd number (e.g. `3`).
  - `waited` and `survivor` re-exec by path; use `/proc/self/exe` instead.
- **Placement sign.** If root moves the helper into the cgroup after fork, the cgroup-minus-`wait4` difference can go negative. Place the process before exec, or use `CLONE_INTO_CGROUP`, and record which.

## Minimum remote set on the rebased SHA

Clear `RUSTC_WRAPPER` and `RUSTC_WORKSPACE_WRAPPER` first.

1. `cargo test --locked -p borsuk --example native_process_cpu -- --test-threads=1` (this also covers `--no-run`)
2. `cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious`
3. **New:** `cargo clippy --locked -p borsuk --example native_process_cpu --profile test -- -D clippy::correctness -D clippy::suspicious`. `--all-targets` checks examples only in non-test mode, so the example's test module is otherwise never linted.
4. `cargo build --locked --release -p borsuk --example native_process_cpu`
5. `env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh`

**Native falsifier: exactly 16 fixture processes.**

| Group | Processes |
|---|---|
| Existing 8 cases | 10 |
| Real child test | 1 |
| `unwaited` | 2 |
| `sigpipe` | 1 |
| Publication failure: `pulse` under a 1 KiB file-size limit with SIGXFSZ ignored | 1 |
| `delay` under `timeout -s KILL` | 1 |
| **Total** | **16** |

- The publication-failure case expects a partial report, a `FAILED` receipt, and the helper exiting with the child's status of 0.
- The `timeout -s KILL` case tests the SIGKILL mirror and the runner-exclusion path.

**Root oracles.**
- Exactly one receipt line, with fields identical to the report.
- `waited`: `child ≥ process_cpu_ns + waited_child_cpu_ns`.
- `late`: `child ≥ fake_terminal_cpu_ns + injected_cpu_ns`, and a validator mutated to use the fake terminal value must fail.
- `unwaited`: the cgroup-minus-`wait4` difference is flagged.
- `survivor`: refused.

All original failed and INCONCLUSIVE receipts stay untouched. I saved a memory note so these findings are not re-reported as new.
