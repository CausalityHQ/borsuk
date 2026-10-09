**HOLD — correct R5 before GO-to-bounded-source-implementation.**

Verified the [frozen R5 plan](/data/target/borsuk-cold-membership-native/scale-maintenance-qualification-a0002/next1m-derivation-draft/canary-plan-r5.review-input.md): 13,368 bytes, matching the supplied SHA-256. Findings concern this plan and its pinned dependencies.

**Concrete blockers**

1. **Expected failures abort the required three-case sequence.**
   R5 lines 42–47 require cases 2/3 to produce a failed oneshot, but line 44 rejects any failed start. A blocking `systemctl start` reports failure for that expected outcome, preventing completion of the sequence.
   **Smallest correction:** capture the start command’s status separately and distinguish the expected failed job from submission errors, signals and timeouts. Alternatively, submit with `--no-block` and classify the observed unit outcome. Preserve the expected nonzero service status.

2. **The source cut removes executable permissions needed by T2.**
   R5 line 12 retains production lines 1–158, excluding [user-data.sh:159](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/user-data.sh:159): `chmod 0500 assets/bin/*`. The transport creates ordinary downloaded files without adding execute bits; the unchanged [wrapper canary requires `-x`](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/wrapper-canary.sh:80). As specified, T2 fails before completing ELF usage checks.
   **Smallest correction:** retain line 159 and update the segment pin and cut boundary.

3. **Matching manager tuples can conceal a failed stop hook or publication.**
   R5 lines 39–47 check the main-process tuple and manager JSON. But [service-stop.sh:16](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/service-stop.sh:16) writes that JSON **before** its upload, whose timeout is 20 seconds plus 2 seconds of kill grace. R5 permits only 8 seconds for stopping. For cases 2/3, a subsequent hook failure can leave the service’s existing `exit-code` result unchanged, alongside the expected JSON. Systemd preserves the earlier failure result in these transitions. [Systemd 255 implementation](https://github.com/systemd/systemd/blob/v255/src/core/service.c#L1866)

   **Smallest correction:** require the actual `ExecStopPost` process to exit normally with status 0, preserve its status before `reset-failed`, and verify publication separately. Reconcile the upload budget within the existing 180 seconds—or invoke the hook’s existing no-upload form and publish through C5, explicitly recording that invocation difference. Also require the original bootstrap’s expected final exit and writer outcomes; C0’s “record present” is insufficient. Keep local qualification provisional until C5 and the committed root closeout requirements succeed.

4. **Copied configuration and links are insufficiently admitted before execution.**
   C0 excludes special files but leaves symlinks unrestricted. C2 preserves links with `cp -a`, then edits copied paths outside the case namespace. An outward link can redirect those edits. Separately, changing copied `/etc/cloud/cloud.cfg` does not establish the effective module list: cloud-init also merges runtime, instance, vendor and datasource configuration. An overriding module or output path can execute or write outside the intended trees before C3/C4 detects it. [Cloud-init configuration precedence](https://github.com/canonical/cloud-init/blob/26.1/cloudinit/helpers.py#L151), [runtime configuration loading](https://github.com/canonical/cloud-init/blob/26.1/cloudinit/stages.py#L1051)

   **Smallest correction:** admit link targets and copied-file edits without following unchecked links; verify cached instance identity and paths; and assert the effective module list and writable output paths under the actual bindings **before fixture execution**. Refuse unexpected configuration. Post-run comparison remains useful evidence, but cannot prevent damage.

5. **The claimed hard phase bounds contradict the retained source.**
   R5 lines 52–59 promise setup ≤900 seconds and `finish()` ≤300 seconds. The retained setup command limits alone total **915 seconds** before metadata, support downloads, formatting or kill grace. H3’s deadline check occurs after those commands. The retained `finish()` also contains unbounded waits, filesystem operations and `systemctl` calls. Transport’s 900-second runtime limit additionally has a stop phase.

   There is a provenance contradiction too: line 59 requires a pre-transport deadline guard, but the retained prefix has no such guard and the tail starts after transport. The four declared hooks do not insert it.
   **Smallest correction:** register and pin the necessary deadline hooks, enforce the phase bounds including termination overhead, and reconcile the timeline within the existing envelope. Do not describe unchanged segments as providing limits they lack.

6. **The deadline clamp can disable its own timeout.**
   R5 line 31 uses `timeout -k 1 min(per-call cap, D-now)`. At exactly `D`, that becomes `timeout … 0`, which disables the timeout. The extra kill second also lies outside the stated remainder unless reserved.
   **Smallest correction:** reject nonpositive remaining budgets before invocation, reserve kill/drain time, and clamp against both the case and batch deadlines. Preserve and drain the exact owned unit even when the timed-out command was merely its `systemctl` client.

**Useful nonblocking limitations**

- The fixed-root `BindPaths` approach is viable. Systemd recreates the configured filesystem view for each executed command; it need not preserve one identical namespace object between `ExecStart` and `ExecStopPost`. [Systemd namespace documentation](https://github.com/systemd/systemd/blob/v255/man/systemd.exec.xml#L2099)
- C4 establishes unchanged contents only for its enumerated paths. When interpreting copied logs, isolate each case’s newly appended segment so historical boot modules do not contaminate the result.
- The controlled 38-byte IAM probe and cost arithmetic are coherent. Actual role behavior, current pricing, termination, both volume deletions and independent replay remain root-owned runtime evidence.
- Wrapper fixtures, CLI usage and manager mapping cannot establish native-chain closure, recall or performance.

No files edited; no fixtures, native experiments or AWS commands executed; no children or consultations started. No runtime or performance GO is issued.
