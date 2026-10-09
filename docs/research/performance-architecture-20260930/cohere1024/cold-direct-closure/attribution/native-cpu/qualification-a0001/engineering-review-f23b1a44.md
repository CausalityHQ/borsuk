What this change does: `f23b1a4` adds authenticated ELF execution with external `wait4` CPU accounting, preserves the original child status, and publishes a separately qualified accounting report. I checked the exact candidate against `2f34455`; all three file hashes match the contract.

**Verdict: repair receipt framing, then qualify remotely. Keep this candidate UNVERIFIED.**

1. **Must fix — collector receipts are not reliably separate JSON lines.**  
   [native_process_cpu.rs:472](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/native_process_cpu.rs:472), with inherited child stderr at line 414.

   A valid child can write `progress...` without a trailing newline. The collector then emits `progress...{"schema":...}`, so a JSON-line reader cannot find the required success or failure receipt. A durable report and correctly preserved child exit can consequently become unusable evidence.

   **Minimum repair:** start the receipt with a newline. Add a focused test that preloads the writer with unterminated child output and verifies that the final line independently parses as the collector receipt. Exercise this through the existing nonzero fixture remotely.

2. **Qualification blocker — the supplied fixtures do not expose a query-wall oracle.**  
   [native_process_cpu.rs:586](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/examples/native_process_cpu.rs:586) measures query CPU; line 599 sleeps without recording a query-window wall interval.

   `lifecycle_wall_bound_ns` includes spawning and lifecycle work and cannot substitute for the retained **median added query wall ≤1 ms** gate. The frozen remote falsifier needs an authenticated monotonic query-window oracle, supplied by a minimal fixture extension or the root’s native harness. Otherwise its result can qualify lifecycle accounting mechanics only, not the complete three-gate method.

I found no other definite source blocker under the contract’s stated external controls:

- Config authentication, cumulative argument bounds, streamed executable hashing, and execution through the held descriptor are coherent. In-place modification, runtime dependencies, and directory stability remain explicitly external obligations.
- Exact-PID `wait4`, EINTR retry, checked integer conversions, and ownership of the `Child` show no evident unsafe lifecycle error.
- After successful reaping, accounting/publication errors preserve the original status. Missing receipts conservatively invalidate collection.
- Successful publication syncs both the file and its parent. Existing files are protected; failed reservations remain.
- Helper snapshots explicitly exclude startup and terminal costs. They are not silently subtracted.
- I found no definite compile or Clippy failure by inspection. That is not compilation evidence.

Optional hardening: use a separate collector channel if receipt provenance must withstand child-controlled stderr. The shared stream currently provides no independent provenance; authenticated, trusted workloads make this a conditional concern.

The minimum remote verification should run on the **exact frozen revision after repair**, with real compiler identities, compiler wrappers/shims cleared, and a separately bounded compiler environment:

```bash
cargo test --locked -p borsuk --example native_process_cpu --no-run
cargo test --locked -p borsuk --example native_process_cpu -- --test-threads=1
cargo build --locked --release -p borsuk --example native_process_cpu
cargo clippy --locked --workspace --all-targets -- \
  -D clippy::correctness -D clippy::suspicious
env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 \
  bash scripts/check_rust_test_build.sh
```

Do not run the ignored child test independently. Record revision, command, compiler identity, and terminal exit status.

A sufficient frozen native roster fits the **16-fixture-process cap**:

| Execution | Fixture processes |
|---|---:|
| Eight named cases, including the two nested children | 10 |
| `real_child_accounting` child, counted once | 1 |
| Authenticated timeout wrapper around `delay` | 1 |
| Direct child terminated by a real signal | 1 |
| Post-spawn publication failure with child exit 0 | 1 |
| Post-spawn publication failure with child exit 7 | 1 |
| Valid report but unavailable collector receipt | 1 |
| **Total** | **16** |

For that roster, require:

- Fresh root-owned cgroups established before execution; CPU1/256 MiB/swap0, ext4 scratch, no network, and the aggregate 60-second cap. Compiler work has a separate envelope.
- Raw user/system fields and arithmetic consistency; native-clock corroboration for the pulse, threads, waited child, and late work.
- Original timeout/nonzero status and actual signal termination, not merely an exit code numerically equal to `128 + signal`.
- Publication failures and missing receipts rejected even when the child exits zero or complete report bytes exist.
- Survivor observation before its natural exit, followed by refusal and verified cleanup. Failure to observe the intended survivor makes that falsifier inconclusive.
- Full helper lifecycle accounting from an outer observer, compared with drained cgroup accounting. Establish the discrepancy envelope and bound omitted helper costs without subtracting them from the reported child result.
- Premature-counter mutation using the captured `late` evidence: substitute its fake terminal counter consistently and require the independent oracle to reject it. This needs no additional fixture execution.
- Explicit failing controls for both **1.02 CPU ceilings** and the **1 ms query-wall ceiling**, using the correct respective windows. These remain mechanical falsifiers, not statistical qualification or sample-size evidence.

Not checked: compilation, Clippy, runtime fixtures, signal delivery, or filesystem failure injection; local execution was prohibited. No files were edited, no jobs were launched, and earlier failed/INCONCLUSIVE receipts remain unchanged. `c6dbddf6` qualification does not transfer to this candidate.
