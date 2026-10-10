**Verdict: fix the three blockers below before freezing launch.** This reviews only the direct SSM qualification payloads, not the repaired Rust logic.

**Blockers**

1. **Successful stages can abort before publishing evidence.** All five payloads run `systemctl show` under `set -e` after `systemd-run --wait` returns. Successful transient units can already be unloaded; a failed lookup then exits before drain verification, `terminal.json`, and S3 publication. See [header-binding payload, line 16](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/header-binding-tests-ssm.parameters.pending.json:16).

   Smallest correction: capture the diagnostic query’s exit status without aborting finalization. Retain the already captured manager status and independently validate the exact cgroup drain. Do not blindly combine `RemainAfterExit=yes` with `--wait`, which waits for deactivation.

   Runtime falsifier: on the authorized EC2 canary, complete a short successful unit, allow it to unload, and require terminal publication with preserved native/manager statuses.

2. **The service deadline competes with native timeout cleanup.** `RuntimeMaxSec="$left"` expires at approximately the same absolute deadline as the inner `timeout "$left"`. It does not provide the declared 240-second supervision margin. Systemd can signal the wrapper before `timeout` returns; its signal trap exits 97 without writing the native exit, source-after check, or counters. See [header-binding payload, line 12](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/header-binding-tests-ssm.parameters.pending.json:12), repeated in every stage.

   Smallest correction: preserve the shared native deadline while giving the unit a separately bounded cleanup interval, including the 30-second kill allowance. Keep publication and the external watcher within the machine deadline.

   Runtime falsifier: a disposable command exceeding a shortened deadline must produce an explicit timeout receipt, manager result, and verified drain before publication.

3. **Setup evidence disappears on termination.** Setup writes `evidence/rustc.txt`, `cargo.txt`, and `source-setup.log`, but never uploads them. Stage upload loops cover only their individual subdirectories. The supplied path therefore loses the actual toolchain and setup-authentication records when the instance terminates. See [setup, line 27](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/setup-ssm.parameters.pending.json:27) and [stage publication, line 22](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/header-binding-tests-ssm.parameters.pending.json:22).

   Smallest correction: publish a create-only, hashed setup evidence bundle before admitting stage one, including an explicit failed-setup disposition when setup fails.

   Runtime falsifier: authenticate those records from S3 after canary termination, without reading its disk.

**Smaller risks**

4. **Drain validation accepts another stage’s path.** The wildcard `/system.slice/borsuk-header-*.service` accepts any matching unit; a nonexistent wrong-stage path yields `drained=true`. Replace it with the exact expected unit path. Falsifier: a different stage’s path must be rejected. [Drain check, line 18](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/header-binding-tests-ssm.parameters.pending.json:18).

5. **The release executable is not retained remotely.** It is copied outside `$ev`, while publication uploads only `$ev/*`. Its hash and ELF header survive; its bytes do not. If subsequent replay must use this qualified executable, publish it conditionally and authenticate the downloaded bytes before termination. [Release payload, line 12](/tmp/borsuk-native-header-binding-root-repair/qualification-inputs-a0003/release-reducer-ssm.parameters.pending.json:12).

All six decoded payloads and launch user-data passed `bash -n`; the archive, manifest, native-map, and candidate-file hashes matched the supplied pins. Required service variables are explicitly set or assigned inside the service. The inspected compile-time fixtures are packaged; I found no concrete missing-fixture blocker.

Not checked: actual AWS-RunShellScript interpreter behavior, target-system systemd behavior, compilation, or runtime. No payloads executed, files edited, network calls, or consultations.
