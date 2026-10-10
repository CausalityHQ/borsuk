What this change does: it repairs lifecycle evidence and adds a disposable platform bootstrap/watch. **Candidate `e85fed7c29d40b124ef766dc489fd90d1d5276f6` still has source blockers. I would hold the proposed canary.** All references below are to its `review-repair-r4/` sources.

**Must fix before the platform canary:**

1. **The negative fixture can block its own witness.** `timeout-descendant.sh:7` makes the fixture parent ignore TERM. Only the escaped child closes logger descriptors; the parent retains stdout/stderr and inherited descriptors while waiting. Meanwhile, `staging-library.sh:573` times the surrounding INNER shell, which has no TERM trap. GNU timeout returns when that monitored shell exits; it does not continue supervising surviving grandchildren. The surviving fixture parent can therefore keep `tee` waiting until its child’s natural exit, preventing `done` before the observer’s 20-second deadline. This is a **source prediction**, not an observed run. Smallest repair: remove the parent’s TERM-ignore trap while retaining the escaped child’s trap. [GNU timeout source](https://raw.githubusercontent.com/coreutils/coreutils/v9.4/src/timeout.c#L524).

2. **Volume absence is enforced after collection.** `platform-watch.sh:63` records `absent=false`, but lines 67–79 still download the terminal/archive and write `COLLECTED_PLATFORM_EVIDENCE_ONLY`; rejection occurs at line 80. Move the absence guard before collection. The final return already rejects this case—the defect is ordering and the premature receipt, rather than a zero-exit volume leak.

3. **Bootstrap cleanup can report success after failed stops.** `platform-worker.sh:30,48,69` ignores required stop-command returns under `set +e`. A failed stop followed by an already absent cgroup leaves `cleanup_exit=0` and can preserve the successful terminal status. Record each return and propagate failure while continuing teardown.

4. **The watcher does not enforce the stated cutoff precisely.** `platform-watch.sh:44–49` checks the deadline only before a HEAD call and subsequent sleep; termination can begin after 1080 seconds. Also, its age rejection at line 10 precedes the cleanup trap. Bound polling against the remaining time, reserve termination-request grace, and arm cleanup before rejecting an already launched instance. Distinguish request initiation from AWS-confirmed termination; neither is a billing guarantee. [AWS termination semantics](https://docs.aws.amazon.com/cli/latest/reference/ec2/terminate-instances.html).

5. **Exit 137 still does not prove timeout expiry.** `collect-stage-outer.sh:143,162` accepts 137 with any positive elapsed duration below 60 seconds. An early SIGKILL of the timeout process group can leave the escaped child alive, preserve resource counters, and subsequently drain—satisfying the predicate without the five-second timer firing. Require evidence of timer expiry and the admitted minimum duration. With finding 1 repaired, the controlled fixture can use the narrower expected-124 path. [GNU timeout’s exit classification](https://raw.githubusercontent.com/coreutils/coreutils/v9.4/src/timeout.c#L273).

**Additional blockers before the native campaign:**

6. **Both staging entrypoints retain the old recipe pin.** `run-staging-native.sh:31` and `run-cleanup-negative.sh:31` require `dd4033d1…`, while the finite controller and actual r6 blob require `40edcc41…`. Valid r6 staging is rejected before evidence initialization. Update both authority pins together.

7. **The controller overwrites the admission closure.** `run-finite-campaign.sh:95` stores its JSON descriptor in `closure`; line 108 replaces it with the negative closure pathname. Lines 121 and 124 then pass that pathname to `--argjson`, so widths cannot start. Preserve the admission JSON and use a separate variable for the negative pathname. [jq argument semantics](https://jqlang.org/manual/v1.7/#invoking-jq).

The repaired source does address the earlier removal-versus-empty distinction: removal binds the original invocation/path, and empty drain carries authenticated raw events. Reserved 97, complete manifest rosters, private width baselines, descendant start-time/session checks, and both resource-event validations are present. The six transported files’ sizes and hashes match the candidate; the library/source-map top-level pins also match.

Using systemd’s `BindsTo=` plus `After=` is the smallest appropriate lifetime dependency here. It still needs a target observer-death falsifier: the current standalone pair exercises positive execution and timeout only. [Systemd dependency contract](https://github.com/systemd/systemd/blob/v255/man/systemd.unit.xml#L640).

Necessary target falsifiers remain separate from these source repairs:

- Real FD sourcing, literal argv preservation, required counters, effective limits, and original terminal fields before stop.
- Observer death with a live sibling payload, plus cleanup after deadline exhaustion.
- Truncated manifests and failed required writes/syncs, including the intended-98 path.
- Premature SIGKILL rejected as an unintended negative; genuine timeout followed by same-process survival and drain before natural exit.
- Later staging namespace/stat/admission-mutation checks; the platform pair does not qualify those paths.

After the platform blockers are repaired and the root freezes launch identity, assets, configuration, resources, and watcher ownership, one disposable causality Spot attempt remains a useful bounded falsifier. The **$0.05 is a planning allowance**. No ANN, scientific, performance, or production readiness follows from that canary.

**Tested nothing.** I read immutable source, compared source hashes/sizes, and consulted primary documentation. No files were edited and no scripts, validators, builds, workloads, children, or AWS operations were executed.
