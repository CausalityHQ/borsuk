What this change does: The payloads qualify the frozen Rust candidate through five serial, bounded native gates and publish stage evidence. The direct SSM approach fits this narrow task; extending the old launcher is unnecessary.

**Blocker**

1. **Post-run `systemctl show` can lose the original unit and interrupt publication.** All five payloads run it under `set -e`, before creating or uploading terminal evidence; see [header payload, line 16](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/header-binding-tests-ssm.parameters.pending.json:16). Local `systemd-run` documentation explicitly says successful transient units are unloaded immediately. Consequently, the subsequent query can report a missing unit or default properties rather than the original execution; a nonzero query also aborts publication after an otherwise successful native gate.

   **Smallest correction:** Capture the query’s output, stderr and exit separately without letting it skip finalization. Preserve the original `systemd-run --wait` output alongside its captured exit. Treat an unloaded unit explicitly; retain the native exit and recorded cgroup drain as independent evidence. Adding `RemainAfterExit=yes` would conflict with `--wait` waiting for deactivation.

   **Runtime falsifier:** On the disposable causality canary, let a short bounded service succeed, then query it after unloading. Require complete published receipts despite the missing unit. Repeat with a nonzero service exit and require preserved failure evidence.

**Smaller risks**

2. **The drain check accepts a family of services rather than the exact stage.** [Line 18](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/header-binding-tests-ssm.parameters.pending.json:18) accepts `/system.slice/borsuk-header-*.service`. A sibling service path therefore satisfies the ownership check. Compare against the exact expected stage path before reading its drain state. Falsifier: a sibling path must produce `drained=false`.

3. **The targeted gate verifies seven passes, but does not itself compare the seven required names.** Its count and summary checks permit a different seven-name set. Your stated root-side roster authentication closes this gap **if implemented exactly**. The smallest payload correction is a sorted comparison against the preregistered seven names; otherwise make that root check an explicit frozen prerequisite. Falsifier: seven passing names with one substitution must be rejected.

The remaining named concerns do not establish defects from this static inspection:

- The Rust environment settings are inside the service, and `RUSTUP_HOME`, `CARGO_HOME` and `PATH` are explicitly passed. Outer AWS exports serve the outer publication commands.
- CPU and memory readbacks reject mismatches. Explicitly setting `CPUQuotaPeriodSec=100ms` would make the expected `cpu.max` representation less dependent on system configuration.
- Ordinary nonzero native exits proceed toward publication. Timeout or termination can prevent inner finalization; missing receipts must remain execution **INVALID**.
- The manifest includes the observed compile-time fixtures and embedded Rust modules. I found no concrete missing fixture.
- The pending watcher, launch claim, client token and lifecycle receipts remain prerequisites already acknowledged in the proposal.

**Verdict: Fix 1 before launch; tighten 2 and enforce 3.** All six decoded payloads and shutdown user-data passed `bash -n`. No files were edited or payloads executed. Actual SSM interpreter behavior, AMI/systemd behavior and native qualification remain unverified.
