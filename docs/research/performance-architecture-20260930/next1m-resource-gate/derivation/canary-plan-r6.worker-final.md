# Canary plan R6 (READ-ONLY; nothing implemented, nothing run). R5 (/tmp/native-scale-build-canary-plan.md, root copy D/canary-plan-r5.review-input.md 096dfec1..5786) is preserved unchanged.
Inputs read in full: both review0553 reports (canary-plan-r5.engineering_critic.review.md, canary-plan-r5.research_critic.review.md) + the parent R6 instruction. Envelope unchanged: external 3600 s + 120 s grace, LOCAL stop started+3000 s, manager batch 180 s total. No new framework: still ONE coordinator, ONE boot, no reboot, no fallback route.

## 0. Reconciliation (every blocker -> fix; section)
| # | blocker (eng / research) | R6 fix |
|---|---|---|
| E1/B1 | blocking `systemctl start` returns nonzero for the EXPECTED failed oneshot (cases 2/3) | `systemctl start --no-block` (rc = job enqueued only); poll `show` and CLASSIFY the exact outcome (section 4.C3) |
| E2/B6 | cutting at line 158 drops `chmod 0500 assets/bin/*`; T2 needs `-x` | production line 159 RETAINED as a pinned segment (lines 1..159 = 11,612 B); cut before line 160 `phase=prep-config`; tail runs nothing that re-implements it |
| E3/B2 | 3-arg stop hook uploads (20 s + 2 s kill) but stop allows <= 8 s; matching tuples can hide a failed hook | case ExecStopPost = the existing 1-ARGUMENT form `service-stop.sh bootstrap` (no network; script path unchanged); C5 publishes the `root/` records; the difference is recorded; the 3-arg upload is exercised only by the ORIGINAL cloud-final (nested-99) |
| E3 | systemd keeps the earlier `exit-code` Result if the stop hook later fails | the ACTUAL ExecStopPost process must be `code=exited status=0` (from `systemctl show -p ExecStopPost`, read BEFORE `reset-failed`) and is recorded separately from the (script, cloud-init, systemd-record) tuple; same requirement for the original nested-99 hook + its upload writer |
| B3 | `After=` does not mean cloud-final finished (stop-post may still be writing) | coordinator step W: poll original cloud-final until ActiveState is `failed` or `inactive` (NEVER `deactivating`/`activating`), then require the live manager record AND put receipt, `final.exit`==99 and the production finish() writer outputs |
| B4 | `daemon-reload` after the baseline rewrites live /run/cloud-init | C0 installs ALL three case units + the trial/probe units and runs EXACTLY ONE `daemon-reload` BEFORE C1; any reload between C1 and C4 is forbidden (and detected by C4) |
| B5 | "no special files" probably refuses every run (hook-hotplug FIFO) | pinned allowlist of exact special-file paths (initial: `/run/cloud-init/hook-hotplug-cmd` FIFO), excluded from the copy, name+type recorded; any other special file => NOT_RUN naming it |
| E4 | symlinks unrestricted; edits could be redirected; merged config/output paths not admitted | C0: every symlink target classified (inside its tree, or an allowlisted system path, else NOT_RUN); copied-file edits only on `-f && ! -L` files whose `realpath -e` stays inside the case backing dir; cached identity checked; ONE read-only config probe per case under the same BindPaths asserts the effective module list and every output/log path BEFORE the fixture runs |
| E5/N1 | setup <= 900 and finish <= 300 claimed as caps; retained source has no such bounds; no pre-transport guard hook | claims narrowed (section 6): HARD bounds are only H2 power-off and the external watchdog; pinned hook H5 added before transport; setup/finish are BUDGETS with guards, not caps |
| E6 | `timeout -k 1 min(cap, D-now)`: at D it becomes `timeout 0` (disabled) and the kill second is outside the remainder | helper `clamp`: remaining = min(case, batch) - now - 1 (kill reserve); remaining <= 0 => REFUSE without invoking; else `timeout -k 1 min(cap, remaining)`; the exact owned unit is preserved/drained even if only its `systemctl` client timed out |
| B7 | `systemctl cat` hash can pass with a vendor drop-in changing ExecStart | T1 hashes the FragmentPath FILE bytes and requires `DropInPaths` == exactly the production drop-in |
| N2 | H1 guard exits 95 before `bad()`/trap exist => idle until watchdog | H1 guard is `shutdown -h now; exit 95` |
| N3 | case slice 35 s is not the systemd worst case | stated: start timeout 10 + SIGTERM 5 + stop-post 5 + final 5 = 25 s + probe 5 = 30 s slice; clamp + exact-cgroup drain keep it fail-closed |
| N4 / research | IAM probe hardening | T4: PUT with `--if-none-match '*'`; only the exact error code `AccessDenied` passes (412 = deny absent, nothing overwritten); controlled probe retained; no rewrite/restore by the canary (section 3.T4) |
| N5 | only the tail was pinned | the tail carries SHAs of coordinator, cgroup, IAM scripts and wrapper-canary.sh (root's, sha prefix bf2cb012 as given; root supplies the full pin); each fetched object is verified before use |
| N6 | Spot interruption not preregistered | preregistered (section 7): interruption => attempt INVALID, preserved, a new attempt only by root decision |
| N7 | evidence differs in faithfulness | labelled (section 7) |
| N8 | module count from a log | module failure count taken from the copied `result.json` errors, never from the log |

## 1. Target facts and pins (a0001 closed evidence, tar stdout only; F untouched)
Ubuntu 24.04.5, systemd 255, bash 5.2.21, jq-1.7; cloud-init 26.1-0ubuntu1~24.04.1 (console). Unit fragment `/usr/lib/systemd/system/cloud-final.service`: Type=oneshot, `ExecStart=/usr/bin/cloud-init modules --mode=final`, RemainAfterExit=yes, TimeoutSec=0, KillMode=process, TasksMax=infinity => DIRECT CLI.
PINS (derived from the a0001 `systemctl cat` text; ROOT TO CONFIRM against the AMI file): FragmentPath file bytes = 25 lines/708 B, sha256 96b9a7b8a74af189954aa04dc589b0e4fbf9e0d725f3f0fa7ca5ab05e6204a24 (the earlier 755 B/a4ef0420 pin included the `# path` header, the blank separator
and was NOT a file hash - superseded); `DropInPaths` == `/run/systemd/system/cloud-final.service.d/borsuk-exit.conf` exactly. a0001 nested-0: manager exited/"0"/success, final_exit "0". Prospective, UNPROVEN on the target: nested 2|3 -> cloud-final `exited`, status 1, `exit-code`,
exactly one failed `scripts-user` module. `RuntimeMaxSec` is a documented no-op for oneshot (never used to bound a case; production systemd-run units are Type=simple where it works). Successful oneshot RemainAfterExit => ExecStopPost only at unit STOP.

## 2. Canary user-data = pinned segments + five individually pinned hooks + stub (NOT claimed byte-identical to production)
Production d4 `user-data.sh` (blob a9ebd7a5): unchanged segments pinned by line range + sha256 over lines 1..159 (11,612 B, includes `chmod 0500 assets/bin/*`; cut before line 160), with these hook points; the assembler pins the ORIGINAL line sha and the replacement text and refuses on any mismatch:
 H1 l.15 `local_stop_epoch=$((boot_epoch + 14400))` -> `ROOT_FROZEN_STARTED_EPOCH + 3000`; guard `boot_epoch in [started-5, started+600]` else `shutdown -h now; exit 95` (no trap exists yet). Set BEFORE any work.
 H2 l.76 `shutdown -h +240` -> `shutdown -h +$(( (local_stop_epoch - $(date +%s)) / 60 ))` (>= 1). THE HARD BOUND: instance power-off at started+3000.
 H3 l.110 `setup_stop=$((boot_epoch + 900))` -> `boot_epoch + 800` (bounds the binding wait only). H4 l.137 `timeout -k 5 300 mkfs.ext4` -> `timeout -k 5 60`.
 H5 (new, inserted before l.149 `phase=transport`): `(( $(date +%s) + 1800 <= local_stop_epoch )) || exit 95` (transport 900 + downstream reserve 900); here the production trap exists, so finish() runs and reports 95.
Stub (~450 B) fetches the pinned `canary-tail.sh` via the production `get`. user-data ~12.3 KB incl. placeholders (<= 16,384).

## 3. Tail (inside the ORIGINAL cloud-final; each step under a hard `timeout`; overrun or unknown shape/resource failure => INVALID; guard before T1: remaining >= 120+300+180+300)
 T1 STACK ASSERTION (<= 10 s): sha256 of the FragmentPath file == pin; `DropInPaths` exact; `cloud-init --version`, `dpkg-query -W -f='${Version}' cloud-init`, `systemd --version` recorded. Mismatch => REFUSE (no coordinator, UNQUALIFIED, early terminate). Version != 26.1-0ubuntu1~24.04.1 => VERSION_DIFFERS recorded (root decides).
 T2 `wrapper-canary.sh $root/run_native_scale_build_gate.sh $root/assets/bin $root/canary-wrapper` (root's, unchanged; ELF `-x` bits come from the retained production chmod) cap 70 s.
 T3 `cgroup-canary.sh` cap 25 s (production chain properties in `systemd-run`; exact effective 4 CPU 0-3/8GiB/swap0/pids128 + closure; stray process dies on stop). T4 `iam-canary.sh` cap 15 s on the controlled probe
    `research/semantic-router/20261009/actual1m-scale-chain-canary-a0001/inputs/permission-probe.txt` (38 B, sha 6111cbc9f504be3dc43c96dc450f2d30d319ab20ccdcf0acf94d65a518072fc9): (1) GET sha-pinned; (2) PUT of the identical bytes with `--if-none-match '*'`: ONLY the exact error code `AccessDenied` passes (412 PreconditionFailed
    = deny absent, nothing overwritten => FAIL); (3) DELETE of that key: ONLY `AccessDenied` passes - run by default on the controlled probe as instructed (record bucket versioning if readable); if it unexpectedly succeeds the canary FAILS and does NOT rewrite/restore (root restores);
    ROOT KNOB: set `iam_delete=off` to skip (3) and instead supply root-side policy-simulation evidence; (4) GET unchanged; (5) positive PUT under `$prefix/canary/` allowed. No guessed/nonexistent key is touched. (T1..T4 = 120 s.)
 T5 write `borsuk-canary-coordinator.service` (`After=cloud-final.service`), `systemctl start --no-block` it, exit 0. Production finish() then runs honestly (status 99, upload, final.exit 99) and the ORIGINAL cloud-final fails => its 3-arg ExecStopPost record (with upload) is the faithful nested-99 datapoint.

## 4. Coordinator (`canary-coordinator.sh`). MANAGER BATCH D = start+180 s: W 20 + C0 25 + C1 15 + 3 cases x 30 + C4 20 = 170 s (10 s slack). Every call goes through `clamp` (section 0, E6).
 W  poll the original cloud-final to `failed`/`inactive`; require live `bootstrap-manager.json` (exited / final_exit "99" / status+result per target, recorded verbatim) and `bootstrap-manager.put.json` (valid upload receipt), live `final.exit` == 99, `terminal.json` and the finish() writer outputs; the original `ExecStopPost` must show `code=exited status=0`. Else NOT_RUN.
 C0 ADMISSION (no fixture runs here): systemd >= 233 and trial BindPaths unit starts; size caps (`du -sb`: /var/lib/cloud <= 64 MiB, /run/cloud-init <= 8 MiB, /etc/cloud <= 1 MiB, each log <= 64 MiB); free space >= 3x copies + 1 GiB; special files = allowlist only; symlink classification; cached identity
    (`/var/lib/cloud/instance` -> instances/<iid> with <iid> == the IMDS instance id; data/instance-id; non-empty obj.pkl); write the three case units + probe/trial units; ONE `daemon-reload`.
 C1 PRESERVE + BASELINE: copy original manager record, `systemctl show cloud-final.service` (incl. ExecStopPost), status/result/data to `/mnt/borsuk-canary-cases/original/` (outside the bound target); content-sha256 listing (names+contents, not mtimes) of LIVE /var/lib/cloud, /run/cloud-init, /etc/cloud, the two logs and the named live files
    /mnt/borsuk-scale1m/{terminal.json,final.exit,bootstrap-manager.json,bootstrap-manager.put.json} + evidence-root/. Nothing live is moved, replaced or rewritten; no further `daemon-reload`.
 C2 per case N in 0, 2, 3 SERIAL (slice 30 s: probe 5 + start/wait 10 + stop 5 + drain 4 + record 3 + slack 3; systemd worst case 10+5+5+5 = 25 + probe 5): backing dirs OUTSIDE the bound target `/mnt/borsuk-canary-cases/N/{var-lib-cloud,run-cloud-init,etc-cloud,cloud-init.log,cloud-init-output.log,root,out}` by `cp -a`;
    `root/` = canary-owned minimal root bound onto /mnt/borsuk-scale1m: exact `service-stop.sh` copy (sha == support pin) + canary-labelled `terminal.json` {schema borsuk-canary-terminal-v1, instance_id, canary:true}. Edits only in the copies (reset status/result, replace `instances/<iid>/scripts/*` by the fixture which writes
    `/mnt/borsuk-scale1m/final.exit`=N and `out/script.exit`=N then `exit N`, copied cloud.cfg `cloud_final_modules` = `[[scripts-user, always]]`). PROBE (read-only, same BindPaths, no module executed, via the target's own cloud-init Python API): effective merged `cloud_final_modules` must be exactly that list and every output/log path
    must be inside the bound set {the two bound log files, the bound trees}; any other value or API failure => NOT_RUN naming it. Unit COPY `borsuk-canary-final-N.service` - HONEST OVERRIDES of the asserted base (ExecStart unchanged): new name; BindPaths for the five cloud copies + `/mnt/borsuk-canary-cases/N/root:/mnt/borsuk-scale1m`;
    `TimeoutSec=0` REPLACED by `TimeoutStartSec=10` and `TimeoutStopSec=5`; `ExecStopPost=/bin/bash /mnt/borsuk-scale1m/service-stop.sh bootstrap` (1-ARG form: no upload; systemd gives it the same binds). EXIT_* only from systemd; never a NATIVE_CHAIN_CLOSED claim.
 C3 `systemctl start --no-block` then poll `show -p ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,ControlGroup,ExecStopPost` (1 s loop, 3 s per-call cap, case deadline). Classify EXACTLY: EXPECTED_ACTIVE (case 0: active/exited/success/exited/0 -> then `systemctl stop` <= 5 s to fire the success-path hook);
    EXPECTED_FAILURE (cases 2/3: failed/exit-code/ExecMainCode exited/ExecMainStatus 1 - observed value recorded verbatim, compared with the pin; a different value is UNQUALIFIED, the batch continues); TIMEOUT (Result=timeout); SIGNAL (ExecMainCode killed/dumped); SUBMISSION_ERROR (`--no-block` rc != 0); NOT_ACTIVATED.
    TIMEOUT/SIGNAL/SUBMISSION_ERROR/NOT_ACTIVATED or any failed stop/drain => that case NOT_RUN/INVALID with the actual timeout/signal retained, NO further case launched, only the exact owned cgroup (`ControlGroup` == /system.slice/borsuk-canary-final-N.service) drained (cgroup.kill, then stop).
    Record SEPARATELY: (a) script exit + final.exit; (b) cloud-init status = ExecMainCode/Status/Result + copied `result.json` error list (module failure count) + the modules that actually ran; (c) the manager record `root/bootstrap-manager.json`; (d) the ACTUAL ExecStopPost `code=exited status=0` (read before `reset-failed`). Mismatch of (d) with success => UNQUALIFIED even if (a)-(c) match.
 C4 (<= 20 s) repeat the C1 listing; require identical (this also detects any reload). C5 (outside the batch, evidence-cleanup bucket): create-only upload of each `root/` record + results to `$prefix/canary/case-N/` (the upload that the 1-arg hook does not do), then `shutdown -h now`.
Verdict (fail-closed): QUALIFIED only if T1, W, C0, probes and C4 passed and every case's tuples AND (d) equal the pins; any NOT_RUN/deviation/extra module/missing record => UNQUALIFIED with the verbatim reason; pins never adjusted.

## 5. `clamp` (deadline discipline, used by every tail and coordinator call)
remaining = min(case_deadline, batch_deadline) - now - 1; if remaining <= 0 => REFUSE (record DEADLINE, do not invoke); else run `timeout -k 1 min(cap, remaining) cmd`. A timed-out `systemctl` client never ends the case silently: the owned unit's state is re-read, preserved and, if needed, drained by its exact cgroup.

## 6. Honest budget (root envelope; not launch authority). $0.61 at 0.59/h <= 0.65 compute; + 0.15 ancillary = $0.76 <= 0.80.
| item | amount | status |
|---|---|---|
| machine power-off | started+3000 (H1/H2) | HARD |
| external termination | 3600 + 120 grace | HARD (root) |
| transport unit | 900 (RuntimeMaxSec on a Type=simple transient service) | HARD for the unit; stop phase + receipt checks add seconds |
| before transport | remaining >= 1800 (H5) | GUARD |
| setup | 900 budget | NOT hard: the unchanged pre-binding timeouts sum to ~915 s before metadata, support GETs, mkfs or kill grace. H3 bounds only the binding wait (<= user-data+800); if pre-binding steps overrun, the loop is skipped and the run refuses (exit 90) |
| wrapper-class | 120 (T1 10 + wrapper-canary 70 + cgroup 25 + IAM 15) | hard `timeout`s |
| production finish() | 300 budget | NOT hard: sum of its own timeouts on the normal path; its `systemctl`/cp/tar steps are unbounded in source. If it overruns, H2 power-off truncates evidence and the attempt is INVALID/partial |
| manager batch | 180 (170 planned) | clamped to D |
| C5 upload + shutdown | <= 300 | budget |
| margin | 300 (boot <= ~90 + slop) | - |
Worst-case timeline (t after launch, assuming the bucket budgets hold): user-data ~90 -> setup <= ~990 -> transport <= ~1890 -> wrapper-class <= ~2010 -> finish <= ~2310 -> manager <= ~2490 -> C5 <= ~2790 -> 210 s unused. If any budget does not hold, a guard fails closed or H2 powers the instance off; the attempt is then INVALID/partial, never extended.
Guards: H5 before transport (>= 1800); before T1 (>= 900); before T5 (>= 300 + 180 + 300); W/C0 NOT_RUN if remaining < 480. NO ANN: nothing runs prep, derive, generation, publish, queries, truth or any native chain; the four ELFs only get missing-argument usage errors in wrapper-canary.sh.

## 7. Preregistered outcomes and evidence labels
Spot interruption/capacity loss at any time => attempt INVALID, evidence preserved, a new attempt only by root decision. FAITHFULNESS: the ORIGINAL cloud-final nested-99 stop (3-arg hook with upload, TimeoutSec=0) is the faithful nonzero datapoint; cases 0/2/3 are MODIFIED-COPY datapoints (Timeout*, BindPaths, 1-arg hook, private roots) that qualify the exit
mapping and service-stop.sh logic only, never a production closure. Native-chain closure, recall and performance stay out of scope.

## 8. Seams still open (nothing invented)
S1 fragment pin must be confirmed against the AMI file (derived from `systemctl cat` text). S2 the config probe uses the target's own cloud-init Python API (version-sensitive); failure => NOT_RUN. S3 special-file allowlist is a guess from Ubuntu cloud images (hook-hotplug FIFO); any other file => NOT_RUN.
S4 the namespace mechanics (BindPaths of dirs, single log files, private /mnt/borsuk-scale1m root, ExecStopPost seeing the same binds per command) are untested; any unsupported part => NOT_RUN with the exact admission, no fallback. S5 canary user-data is hooked production text; the assembler's per-segment/per-hook pins are its only provenance.
S6 OWN2 watcher / OWN3 verifier are not reusable unmodified here; the root terminates/collects. S7 IAM DELETE on the controlled probe is destructive if the deny is absent (root knob `iam_delete`); the instance cannot tell an explicit from an implicit deny.
S8 KillMode=process leaves strays on timeout; only the exact-cgroup drain cleans up, and its failure is INVALID. S9 decoupling the outer manager exit (bootstrap exits 0 on any validated closure) would remove the 2/3 platform coupling; needs a reviewed OWN2/OWN3 diff; not required here.
