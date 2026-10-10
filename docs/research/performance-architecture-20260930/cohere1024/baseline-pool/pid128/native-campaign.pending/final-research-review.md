What this change does: it adds a serial admission → staging → controlled negative → widths campaign, with separate capture of the recipe’s actual return. The current source remains **UNVERIFIED**. I found eight source blockers.

1. **Terminal checks confuse manager retention with cgroup retention.**
   [r4 final polling](/tmp/borsuk-root-pid128-repair-r4/run-native-pid128.sh:354), [library polling](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:261), and both collectors require the original `ControlGroup`; collectors also require readable `cgroup.events` after termination.

   Systemd can prune an empty cgroup while retaining the service’s manager state—including successful `active/exited` services. Its `ControlGroup` property then becomes empty. This affects the positive path as well as the failed negative. See upstream [service transitions](https://github.com/systemd/systemd/blob/v255/src/core/service.c#L1147-L1150), [cgroup pruning](https://github.com/systemd/systemd/blob/v255/src/core/cgroup.c#L2848-L2885), and [property reporting](https://github.com/systemd/systemd/blob/v255/src/core/dbus-unit.c#L1137-L1160).

   **Smallest fix:** preserve the live invocation/cgroup association before termination. Keep strict ownership checks for live controls, but permit terminal cgroup removal only with the same retained invocation, matching terminal manager fields, and absence of the previously authenticated path. Record removal explicitly; do not fabricate a `populated 0` observation.

2. **Collectors can reject a healthy observer before it creates evidence.**
   [Admission collector initialization](/tmp/borsuk-pid128-native-campaign-next/collect-admission-outer.sh:12) and [stage collector initialization](/tmp/borsuk-pid128-native-campaign-next/collect-stage-outer.sh:13) immediately require `realpath -e "$admission"`. Staging creates that directory after library loading and several authentication steps.

   The driver calls collectors immediately after `Type=exec` startup. That startup establishes execution of the service binary, not completion of script initialization. [Systemd’s documented startup semantics](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml#L158-L169) support this distinction.

   **Smallest fix:** validate the existing parent first, poll the authenticated invocation under the existing deadline, and require the evidence directory before inspecting terminal artifacts.

3. **Expected negative exit 98 also accepts some closure failures.**
   [Library finalization](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:120) maps manifest-generation and sync failures to `rc=98`. An intended negative already has original/intended/wrapper exits of 98, so these failures need not change anything the collector checks.

   A manifest producer that writes several valid checksum lines and then fails can leave a valid subset. [The collector](/tmp/borsuk-pid128-native-campaign-next/collect-stage-outer.sh:58) checks listed hashes, without requiring the complete roster. That subset can therefore pass.

   **Smallest fix:** give finalization failure a distinct actual return, such as 99, and require an exact manifest roster excluding only the two declared closure files. Preserve 98 for the successfully finalized controlled negative.

4. **Negative acceptance omits resource-event validation.**
   [The runner](/tmp/borsuk-pid128-native-campaign-next/staging-r2/staging-library.sh:282) rejects the expected failed manager result before reaching `check_limits` and `check_events`. The negative also bypasses the entrypoint’s final observer checks. [Campaign acceptance](/tmp/borsuk-pid128-native-campaign-next/run-finite-campaign.sh:99) checks timeout, descendant membership and drain, but never validates those resource snapshots.

   A timeout accompanied by an unexpected payload OOM or task-limit event can satisfy the current negative predicate.

   **Smallest fix:** require independently validated payload and observer resource evidence in negative acceptance. Merely moving checks earlier is insufficient if their failure still becomes an indistinguishable 98.

5. **Stopping the observer does not stop its sibling payload.**
   [Payload launch](/tmp/borsuk-root-pid128-repair-r4/run-native-pid128.sh:313) creates a separate `system.slice` service. [Campaign teardown](/tmp/borsuk-pid128-native-campaign-next/run-finite-campaign.sh:47) controls only the observer. If the observer is killed while a payload runs, its EXIT cleanup cannot execute, and stopping its cgroup leaves the sibling payload alive until its own deadline.

   Campaign teardown also uses the exhausted campaign deadline, which can prevent even its ownership query from running.

   **Smallest fix:** bind payload lifetime to the authenticated observer using systemd’s existing `BindsTo=` plus `After=` dependency, and retain independent drain verification. Give teardown a separate bounded grace period. [Systemd documents this dependency behavior](https://github.com/systemd/systemd/blob/v255/man/systemd.unit.xml#L640-L655).

6. **Campaign finalization can return zero after failed durability operations.**
   [Campaign `finish`](/tmp/borsuk-pid128-native-campaign-next/run-finite-campaign.sh:60) disables `errexit`, then leaves both the `campaign.exit` write and `sync -f` unchecked. With original exit zero and no owned unit, either operation can fail while the campaign still exits zero.

   **Smallest fix:** propagate those failures into the final return, as the adjacent platform-pair script already does. Propagate required cleanup-command failures into the cleanup ledger too.

7. **The widths path still trusts a mutable admission baseline.**
   [r4 widths](/tmp/borsuk-root-pid128-repair-r4/run-native-pid128.sh:845) authenticates admission once, then rereads its original pathname for query descriptors and preservation comparisons. Its later `authenticate_inputs` calls omit admission and its closure.

   Changing an otherwise ignored admission field after authentication can leave widths successful despite breaking the original admission seal. Changing the expected roster also changes the comparison baseline.

   **Smallest fix:** reuse staging’s authenticated private admission copy and final original-closure recheck, with fresh output names. Also reject evidence overlap with the admission directory before the recipe’s first `mkdir`; the standalone widths API still permits that overlap.

8. **The cleanup fixture path is disconnected from its admitted support role.**
   [The negative entrypoint](/tmp/borsuk-pid128-native-campaign-next/staging-r2/run-cleanup-negative.sh:90) executes `/timeout-descendant.sh`. The campaign authenticates `.support.timeout_fixture.path` but neither requires that root pathname nor installs it.

   A correctly authenticated fixture beside the other support files therefore passes campaign preflight and fails when the negative begins.

   **Smallest fix:** resolve the fixture beside the entrypoint, consistently with the platform canary, and require the admitted support descriptor to name that exact file.

The following assumptions need small falsifications on **causality EC2**, separately from repairing those source defects:

- Run the real-systemd positive and negative without ANN/data. Preserve live and terminal invocation fields, cgroup presence/removal, and the descendant’s PID, process group, session and cgroup. Confirm automatic drain separately from explicit `cgroup.kill`.
- Delay observer initialization; kill an observer while its bounded Bash payload runs; exercise cleanup after deadline expiry. Require terminal proof that the original payload stopped.
- Inject manifest truncation, evidence-write failures and final-sync failures. Every case must reject acceptance, including the expected-98 negative.
- Exercise the repaired namespace, failed-`stat`, admission-mutation and FD-source cases. The open FD prevents pathname substitution after opening; protected parent directories and absence of in-place mutation remain prerequisites.
- Verify target counters, ancestor limits, FD inheritance and cumulative scratch admission with all retained staging/evidence bytes. Endpoint `du` plus reserves remains an admission estimate, not a measured peak.

The existing separate return capture, FD-based library sourcing, and cumulative `ROOT + campaign evidence` accounting are useful improvements. Adapting terminal predicates to systemd’s lifecycle is smaller than adding another holder process. After successful closures, the normal stage sequence is serial.

**Verdict: fix 1–8 before runtime qualification.** Transport, watch, freeze and external instance/volume closure remain pending; this review establishes no readiness or performance result.

Not checked: runtime, helpers, native binaries, corpus, builds or AWS. No files were edited or children launched. All nine named source hashes were unchanged across the review.
