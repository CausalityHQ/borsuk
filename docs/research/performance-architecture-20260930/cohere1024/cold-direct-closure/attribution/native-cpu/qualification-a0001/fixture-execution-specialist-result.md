**Use a retained service with a separate measurement subgroup. This can test execution mechanics; it cannot yet qualify the unwaited ≥20 ms discrepancy.**

1. Await the **original compiler run’s** verified terminal result for `d7e6c8ac…` and authenticate its retained ELF. Leave `i-09757926061839bea` / watcher `79162` untouched. Freeze the existing 12 invocations: **15 fixture processes + the reserved compiler-test child = 16**, with no retries.

2. For each invocation, create fresh root-owned ext4 scratch, immutable config, separate stdout/stderr files, two FIFOs, and an absent report path. Require target systemd support for `DelegateSubgroup` and launch this service shape:

```bash
systemd-run --unit="$unit" --service-type=exec \
  -p User=root -p Restart=no \
  -p 'Delegate=cpu cpuset memory pids' \
  -p DelegateSubgroup=payload \
  -p CPUAccounting=yes -p CPUQuota=100% -p AllowedCPUs=0 \
  -p MemoryMax=268435456 -p MemorySwapMax=0 -p TasksMax=128 \
  -p PrivateNetwork=yes -p 'SystemCallFilter=~@network-io' \
  -p LimitCORE=0 -p KillMode=process -p TimeoutStopSec=60s \
  -p "StandardOutput=append:$case_dir/stdout" \
  -p "StandardError=append:$case_dir/stderr" \
  -p "ExecStopPost=/bin/bash --noprofile --norc $hold_script $release_fifo" \
  /usr/bin/env -i PATH=/usr/bin:/bin LC_ALL=C TMPDIR="$scratch" \
  /bin/bash --noprofile --norc "$gate_script" \
  "$go_fifo" "$helper" "$config" "$config_sha" "$report"
```

The fixed gate script blocks on `go`, then **execs** `"$helper" "$config" "$config_sha" "$report"`. Only `survivor` and `unwaited` receive the context attestation. The stop-post script blocks on `release`; under delegation it runs in `.control`, outside `payload`. `KillMode=process` prevents automatic descendant killing before observation. Keep the stop-post hold active until evidence is durable; omit `--wait` and `--collect`.

3. Before releasing `go`, record the actual `ControlGroup`, main PID/start time, membership in `payload`, root ownership, and enforced parent limits: `cpu.max`, effective cpuset `0`, `memory.max=268435456`, `memory.swap.max=0`, `pids.max=128`. Check scratch filesystem, inherited signals, limits and network isolation. Save initial `payload/cpu.stat`.

   Arm **one batch-wide 60-second deadline**, outside the measured subgroups, before the first release. Its cleanup must explicitly kill the campaign’s exact cgroup subtree; merely timing out `systemd-run` would leave services running. Deadline intervention makes affected evidence incomplete, preserves any already-recorded child status, and supplies no substitute child wait status.

4. Observe from release onward. For `survivor`, capture the emitted descendant PID, start time, non-zombie state and cgroup membership **after its parent terminates and before its natural two-second exit**. Missing that observation is **INCONCLUSIVE**, without another invocation.

   While stop-post holds the unit, save:
   - `ExecMainCode`, `ExecMainStatus`, main PID and service result;
   - original raw child status from the matching native report/receipt;
   - `payload/cpu.stat`, process membership and `cgroup.events`;
   - final counters after `populated=0`, plus memory/PID failure events.

   Persist and hash these before releasing stop-post. Measure `payload`, not the parent containing the hold process. A vanished subgroup or failed readback invalidates accounting.

5. Keep the two special cases exact:

   - **Publication failure:** add `LimitFSIZE=1024`; the gate executes `trap '' XFSZ` before waiting. Verify both settings before release. Use the ordinary authenticated `pulse` config and writable, absent report path. Require pulse execution, original child exit **0**, and matching `ACCOUNTING_OR_PUBLICATION_FAILED` receipt with the file-size failure. Preserve the partial report. A reservation/spawn failure does **not** exercise this control.
   - **Timeout-KILL:** authenticate the actual timeout ELF and separately pin the helper/runtime referenced by its argv:

```text
executable = authenticated timeout ELF
args       = ["--signal=KILL", "0.2", HELPER, "--fixture", "delay"]
```

   The accounting helper’s direct child is **timeout**. Its raw signal **9** is supervisor status, not independently reaped `delay` status. Preserve publication validity separately and refuse full descendant accounting. Likewise, require actual signal **13** for `sigpipe`, not normal exit 141.

Apply missing-receipt and premature-`late`-counter mutations only to copies of closed artifacts.

**Decisive blocker:** drained cgroup CPU minus reported child CPU also contains collector/gate CPU and accounting discrepancy. The helper’s absolute snapshot includes startup, but omits its final serialization, publication, receipt and exit costs. Systemd’s cgroup CPU value supplies no independent separation of those costs. The existing controls can show an **observed separation**, but their maximum residual is not a justified error bound.

The `unwaited` witness can establish the deliberately unpropagated ≥20 ms workload and support refusal. It cannot, with these instruments alone, qualify quantitative discrepancy detection against admitted controls. Retain **UNQUALIFIED/INCONCLUSIVE** for that obligation; retain successful mechanical results separately. No 2 µs accuracy, whole-tree completeness, zero helper cost, query-wall qualification or performance GO follows.

Read-only inspection only; no files changed or native workloads, network actions, children or consultations launched.
