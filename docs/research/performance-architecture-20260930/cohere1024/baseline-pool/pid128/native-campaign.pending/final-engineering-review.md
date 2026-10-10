What this change does: sequences admission, staging, a cleanup negative, and widths using separate systemd services and external closure collectors. **Seven source defects block acceptance.** Reviewed hashes remained unchanged, including recipe `dd4033d1…`, library `9b46fccb…`, and campaign `e9a67046…`.

1. **[P1] Retained manager state is mistaken for retained cgroup state.**
   Both collectors require an existing `cgroup.events` after the observer terminates ([admission collector](/tmp/borsuk-pid128-native-campaign-next/collect-admission-outer.sh:44), [stage collector](/tmp/borsuk-pid128-native-campaign-next/collect-stage-outer.sh:44)). The repaired runner also requires the original `ControlGroup` in every terminal-state poll ([library](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:262)). Systemd prunes empty cgroups on both `exited` and `failed` transitions; `RemainAfterExit=yes` does not prevent this. Thus even the positive `/bin/true` canary can fail after correct execution. [Systemd source](https://github.com/systemd/systemd/blob/main/src/core/service.c), [cgroup removal implementation](https://github.com/systemd/systemd/blob/main/src/core/cgroup.c).
   **Smallest fix:** bind the original cgroup while live, then distinguish an empty retained cgroup from a removed original cgroup while requiring the same terminal invocation. Update the admission closure validator accordingly; never manufacture raw `populated 0` evidence.

2. **[P1] Collectors can start before their evidence directory exists.**
   The campaign launches each collector immediately after `systemd-run` and an invocation lookup. Both collectors immediately call `realpath -e` on the evidence directory ([admission](/tmp/borsuk-pid128-native-campaign-next/collect-admission-outer.sh:12), [stage](/tmp/borsuk-pid128-native-campaign-next/collect-stage-outer.sh:13)). Staging creates that directory only after authentication and validation ([entrypoint](/tmp/borsuk-pid128-native-campaign-next/staging-r2/run-staging-native.sh:47)). `Type=exec` establishes command startup, not completion of those operations. [Systemd documentation](https://github.com/systemd/systemd/blob/main/man/systemd-run.xml).
   **Smallest fix:** defer directory existence/canonicality checks until terminal-state collection, or wait within the existing deadline while fencing the invocation.

3. **[P1] Expected negative exit 98 hides closure failures.**
   In [finish](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:120), manifest/write/sync failures set `rc=98`. That is already the controlled negative’s expected return. A failed final sync can therefore leave all terminal fields and the actual outer return matching the [collector’s acceptance predicate](/tmp/borsuk-pid128-native-campaign-next/collect-stage-outer.sh:55). The external return correctly captures execution, but cannot distinguish these two outcomes.
   **Smallest fix:** reserve a different return code for closure/durability failures. Accept 98 only when the intended negative and every closure operation succeeded. Also require the complete stage closure roster, rather than merely checking whichever entries appear in its manifest.

4. **[P1] Campaign completion can return zero after losing its final receipt or sync.**
   The [campaign EXIT trap](/tmp/borsuk-pid128-native-campaign-next/run-finite-campaign.sh:44) disables `errexit`, then ignores failures writing `campaign.exit` and synchronizing the evidence filesystem. With saved `rc=0`, it still exits zero. Its stop-command failures are likewise not consistently reflected in `cleanup`.
   **Smallest fix:** explicitly propagate each required write, sync, and stop failure while continuing teardown. An external observer must receive a nonzero return when final durability fails.

5. **[P1] The negative gate skips checks needed to identify the intended failure.**
   The runner rejects the expected manager exit 98 **before** checking final limits and resource events ([library](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:282)). The campaign’s [negative-proof block](/tmp/borsuk-pid128-native-campaign-next/run-finite-campaign.sh:99) never performs those checks independently. For example, killing the timed command through OOM can produce 137, which this gate accepts as an intended timeout.
   **Smallest fix:** require validated resource limits and unchanged task/OOM counters before accepting the negative. Require evidence that the recorded descendant survived the timeout’s process-group termination and then drained within a bound shorter than its natural 60-second lifetime. Preserve automatic systemd drain as a separately evidenced valid outcome.

6. **[P1] Campaign cleanup does not cover a payload whose observer dies.**
   Payload services deliberately occupy sibling cgroups ([runner](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:235)). The campaign’s [failure cleanup](/tmp/borsuk-pid128-native-campaign-next/run-finite-campaign.sh:47) controls only the observer. If that observer is OOM-killed or otherwise cannot run its trap, an active native payload can continue until its own runtime limit—up to 3,680 seconds.
   **Smallest fix:** bind each payload’s lifetime to its exact observer service, with appropriate stop ordering, and verify the original payload’s stop/drain during reconciliation. This defect is inside the current cleanup path, independent of the unfinished external watchdog.

7. **[P2] Widths still rereads an admission receipt after its authentication expires.**
   The [widths path](/tmp/borsuk-root-pid128-repair-r4/run-native-pid128.sh:845) authenticates admission once, then rereads it for query descriptors and preservation comparisons. `authenticate_inputs` excludes admission and its closure. Changing even an otherwise ignored admission field during widths can leave successful width closure with a broken admission hash. The staging repair does not cover this path.
   **Smallest fix:** reuse staging’s authenticated private baseline and reauthenticate the original admission plus complete closure before widths succeeds.

The remaining target assumptions need small falsifications on **causality EC2**:

- Real systemd positive/negative lifecycle, including cgroup removal, delayed manager updates, and original outer returns.
- Checked-FD library sourcing on the target Bash, protected source directories, and exact deployed library/fixture locations.
- Required cgroup counters, effective ancestor limits, and cumulative scratch/free-space admission on the actual filesystem.
- Injected receipt/sync failures, delayed evidence creation, admission mutation, and observer death while a bounded disposable payload is active.

The earlier staging namespace, private-admission, and checked-filesystem repairs are present. Normal stage sequencing is serial, and scratch accounting includes retained campaign evidence.

**Verdict: fix 1–7 before acceptance.** Transport, watchdog, freeze, and infrastructure closure remain explicitly unfinished.

No recipes/helpers, native workloads, corpus, builds, AWS operations, or children were executed. No files were edited.
