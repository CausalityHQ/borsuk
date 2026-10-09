I've finished reading the plan and the referenced blobs; here is the review.

# Canary plan R5 review — **HOLD**

R5 is a plan only. Nothing was run. I made no edits. I read the R5 file (sha 096dfec1…, matches), plus these blobs at f483ef3c: `user-data.sh` (a9ebd7a5), `service-stop.sh`, `wrapper-canary.sh` (bf2cb012; its wrapper pin c5f4c4b8 matches the f483 `run_native_scale_build_gate.sh`), `transport.py`, c81 proposal, c987 observed, 5c81 probe and the R4 root review.

The architecture (one boot, `After=cloud-final` coordinator, private BindPaths per case) is sound and within scope. Seven plan-text defects stop it as written. Each one either makes every case fail, makes the evidence spurious, or lets the stack check pass the wrong unit. All have one-line fixes, and none enlarges scope.

## Blockers (all must be in R6)

**B1. `systemctl start` is expected to return nonzero for cases 2/3, but the plan treats a failed start as abort.**
- For a oneshot, a blocking `systemctl start` returns 1 when ExecStart fails, which is exactly the expected outcome for cases 2 and 3.
- C3 says "any failed start ⇒ INVALID, no further case". Taken literally, case 2 is always INVALID and case 3 never runs, so the verdict is always UNQUALIFIED.
- A blocking start killed by the per-call `timeout` also leaves the job running.
- **Fix:** use `systemctl start --no-block`. Its return code only means "job enqueued". Then poll `ActiveState,SubState,Result,ExecMainCode,ExecMainStatus` until active/exited (case 0) or failed (cases 2/3), or the case deadline. Classify only from the `show` values and the hook record.

**B2. The 3-argument stop hook can run 22 s, but the canary allows 8 s.**
- `service-stop.sh` uploads under `timeout -k 2 20`. `TimeoutStopSec=8` also bounds ExecStopPost, and the `systemctl stop` cap is 8 s.
- If the upload is slow (a cold AWS CLI start), systemd kills the hook. For case 0 the Result then becomes `timeout`, the record says `success`, the case is UNQUALIFIED, and the remaining cases are aborted.
- Raising the timeout does not fit: 3 × (20+25+5+2) + 55 > 180.
- **Fix:** make the case ExecStopPost the supported 1-argument form, `service-stop.sh bootstrap`. That script path is unchanged and makes no network call. Upload the `root/` records create-only in C5.
- The 3-argument upload is still exercised for real by the original cloud-final's stop (the nested-99 case, `TimeoutSec=0`).

**B3. `After=cloud-final.service` does not mean cloud-final has finished.**
- A start job is marked failed as soon as the unit leaves "activating", which happens when it enters stop-post. So the coordinator can start while the original ExecStopPost is still writing and uploading live `bootstrap-manager.json` and `.put.json`.
- C0/C1 would then either preserve a partial record or take a baseline mid-write, which fails C4 for the wrong reason.
- **Fix:** the coordinator's first bounded step polls the original cloud-final until ActiveState is `failed` or `inactive` (never `deactivating`). It then requires both live manager files to exist before C0. Charge this time to the 180 s batch.

**B4. A `daemon-reload` after C1 changes live `/run/cloud-init`.**
- Every reload re-runs `cloud-init-generator`. It appends its log under `/run/cloud-init` and runs ds-identify.
- If the case units are installed, with a reload, after the C1 baseline, C4 always fails.
- **Fix:** in C0, write the three case units and the trial unit, run exactly one `daemon-reload`, and only then take C1. Forbid any reload between C1 and C4. The cloud-init log files are in the listing, so this matters.

**B5. The C0 rule "no FIFOs/sockets" probably refuses every run.**
- Ubuntu cloud images normally have the FIFO `/run/cloud-init/hook-hotplug-cmd` (from `cloud-init-hotplugd.socket`). The a0001 evidence does not list `/run/cloud-init`, so this is unverified, but likely.
- If present, C0 returns NOT_RUN every time.
- **Fix:** add a pinned allowlist of exact special-file paths. Exclude them from the copy and record each one's name and type. Refuse any other special file.

**B6. Cutting the plan at line 159 also removes the `chmod 0500 assets/bin/*`.**
- Transport runs as a `systemd-run` unit with the default service UMask of 0022. The AWS CLI therefore probably writes the ELFs as 0644, with no execute bit.
- `wrapper-canary.sh` requires `-x $bins/$name`. Without the chmod, T2 fails every time.
- **Fix:** the tail runs the exact production line 159, pinned as a segment, before T2.

**B7. The T1 unit hash can pass on the wrong unit.**
- `systemctl cat` includes drop-ins: a0001's captured unit is 1012 B including `borsuk-exit.conf`, while the plan pins a 755 B base. A vendor drop-in could change ExecStart (for example to the socket shim) while the base sha still matches. That is a false DIRECT pass, and the case copies would then differ from the original unit.
- **Fix:** hash the bytes of `FragmentPath`, and require `DropInPaths` to equal exactly `/run/systemd/system/cloud-final.service.d/borsuk-exit.conf`.

## Non-blocking limitations (state them in R6)

- **N1. The "before transport" deadline guard has no hook point.** Lines 149–158 are unchanged, so either add a pinned H5 guard line or drop the claim. Setup is not hard-bounded at 900 s: the production timeouts before the binding wait add up to roughly 1200 s worst case. The real ceilings are H1/H2 and the root watcher, and the guard before T1 still fails closed.
- **N2. The H1 guard exits 95 before `bad()` and the trap exist,** so the instance idles until the external watcher stops it at 3720 s. Make the guard `shutdown -h now; exit 95`.
- **N3. The 35 s case slice is not the systemd worst case.** On a start timeout: 20 s start, 8 s SIGTERM, 8 s stop-post, then up to 16 s final SIGTERM/SIGKILL, about 52 s. The clamp at D plus the cgroup drain keeps this fail-closed; the plan should say so.
- **N4. IAM probe hardening.**
  - Send the denied PUT with `--if-none-match '*'`, so a missing deny gives 412 instead of overwriting the probe.
  - Require the error code to be exactly `AccessDenied`, not just "an error".
  - Record that the instance cannot tell an explicit deny from an implicit one, and check bucket versioning before the DELETE.
- **N5. Pin every fetched canary script.** The tail should carry the SHAs of the coordinator, cgroup and IAM scripts, not only its own pin from the stub.
- **N6. Spot interruption handling is not preregistered,** which AGENTS.md requires: an interruption makes the attempt INVALID, it is preserved, and a new attempt is launched.
- **N7. The two kinds of evidence differ in faithfulness.** The original unit's nested-99 failure is the faithful nonzero datapoint. Cases 2/3 only show that the exit value doesn't matter, in a modified copy (Timeout\*, BindPaths, ExecStopPost). Label them that way.
- **N8. The pin "2|3 → exited/1, one failed module" is unproven on the target.** Keep it fail-closed and never adjust it. Take the module count from the copied `result.json` errors, not from parsing the log.

## Verdict

**HOLD on R5 as written.** Once R6 folds in B1–B7 exactly as stated, with no other design change, that revision is **GO-to-bounded-source-implementation**. This is never a runtime or performance GO: launch, cost and EC2 evidence remain the root's.
