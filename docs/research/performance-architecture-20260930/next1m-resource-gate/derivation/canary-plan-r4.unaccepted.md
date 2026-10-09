# Canary plan R4 (READ-ONLY proposal; nothing implemented, nothing run). Root reviews before any implementation.
R4 = R3 + root's 1791583867: ONE boot, ONE coordinator, 3 serial direct-CLI fixture units over systemd PRIVATE BIND MOUNTS of canary-owned copies; NO multi-boot and NO reboot fallback.
Envelope reconciliation: every parent envelope to me was read via agent_messages + cross_audit (1791583276, 348, 443, 509, 557, 867); 557 (target evidence) is delivered and incorporated since R3. Nothing else is queued.

## 0. Target facts (a0001 closed evidence, read via `tar -xzO` of two tiny members; F untouched)
Ubuntu 24.04.5, systemd 255, bash 5.2.21, jq-1.7, GNU time present; cloud-init 26.1-0ubuntu1~24.04.1 (console). cloud-final base unit (27 lines/755 B) sha256 a4ef0420239ab2bdbb10f75924bdbea4c6c4df07cb752858a4e77581ddcc8a76:
Type=oneshot, `ExecStart=/usr/bin/cloud-init modules --mode=final`, RemainAfterExit=yes, TimeoutSec=0, KillMode=process, TasksMax=infinity => DIRECT CLI. a0001 nested-0 record: manager exited/"0"/success, final_exit "0".
CLI status = len(final-stage module errors); a failing user script (2 OR 3) = one failed `scripts-user` module => 1 if exactly one module fails (local Ubuntu 26.04 source reading; target 2/3 UNPROVEN). The local socket shim is NOT the target: a non-DIRECT unit is REFUSED, never emulated.
Successful oneshot RemainAfterExit => ExecStopPost only at unit STOP.

## 1. Route: boot 1 only
user-data = production `user-data.sh` lines 1..158 BYTE-IDENTICAL (11,588 B; exact d4 staging through the transport verification; cut at the unique line `chmod 0500 assets/bin/*`, hash-checked) + ~450 B stub fetching the pinned `canary-tail.sh`.
Tail (inside the ORIGINAL cloud-final), each step under a hard `timeout`:
 T1 STACK ASSERTION: capture `systemctl cat cloud-final.service`, `cloud-init --version`, `dpkg-query -W -f='${Version}' cloud-init`, `systemd --version`; require the base-unit section sha == the pin above. Mismatch (shim/other) => REFUSE: coordinator not started,
    mapping UNQUALIFIED, other checks still recorded, early terminate. Version != 26.1-0ubuntu1~24.04.1 => recorded VERSION_DIFFERS (root decides); unit equality is the hard gate.
 T2 `wrapper-canary.sh $root/run_native_scale_build_gate.sh $root/assets/bin $root/canary-wrapper` (root's, unchanged; cap 300 s).
 T3 `cgroup-canary.sh` (cap 120 s): awk-extract the wrapper's jq programs + `resources()/resource_closure()`; run in `systemd-run` with the PRODUCTION chain properties (CPUQuota=400% AllowedCPUs=0-3 MemoryMax=8G MemorySwapMax=0 TasksMax=128
    KillMode=control-group); require exact effective 4 CPU/8GiB/swap0/pids128 + closure; a stray `setsid sleep` must die on `systemctl stop`.
 T4 `iam-canary.sh` (cap 40 s) using the ROOT-STAGED probe `research/semantic-router/20261009/actual1m-scale-chain-canary-a0001/inputs/permission-probe.txt` (38 B, sha 6111cbc9f504be3dc43c96dc450f2d30d319ab20ccdcf0acf94d65a518072fc9): (1) role GET => sha matches the pin; (2) PUT of the
    IDENTICAL bytes (so an absent deny changes nothing) => must be AccessDenied; (3) DELETE of that key => must be AccessDenied; (4) GET again => unchanged sha; (5) positive control PUT under `$prefix/canary/` succeeds.
    No guessed or nonexistent key is ever deleted. Unexpected success of (2)/(3) => FAIL, stop, root restores the probe from its staged copy. `$prefix` = research/semantic-router/20261009/actual1m-scale-chain-canary-a0001 (matches the production prefix regex).
 T5 write `borsuk-canary-coordinator.service` (`After=cloud-final.service`) and `systemctl start --no-block` it (no wait on its own parent => no recursion), re-arm `shutdown -h` at launch+3000, exit 0. The production `finish()` then runs honestly (status 99, upload,
    final.exit 99) and the original cloud-final fails => its ExecStopPost record is the natural nested-99 datapoint.

## 2. Coordinator (`canary-coordinator.sh`, ~70 lines; starts only after the original cloud-final reached inactive/failed; never concurrent with it)
C0 ADMISSION (bounded copying + namespace support; any failure => NOT_RUN with the exact reason, no fallback route):
   - original manager record present (wait <= 20 s); `systemd-analyze verify` of the unit copies; systemd >= 233 (BindPaths) and a trial unit with the same BindPaths starts;
   - size caps before any copy (`du -sb`): /var/lib/cloud <= 64 MiB, /run/cloud-init <= 8 MiB, /etc/cloud <= 1 MiB, /var/log/cloud-init.log + cloud-init-output.log <= 64 MiB each; root fs free >= 3x the copies + 1 GiB;
   - no non-regular/non-dir/non-symlink entries (sockets, FIFOs, devices) under the trees to copy (`cp -a` cannot recreate sockets); symlinks inside the copies must resolve inside the copy namespace (absolute links resolve under the bind, so none needs re-pointing).
C1 PRESERVE + PROVE-UNTOUCHED BASELINE: copy the original manager record, `systemctl show cloud-final.service`, `/run/cloud-init/{status,result}.json`, `/var/lib/cloud/data/*` to `$root/canary-evidence/original/` and upload (create-only);
   take a content-sha256 listing (names + contents, not mtimes) of the LIVE /var/lib/cloud, /run/cloud-init, /etc/cloud and the two logs. Move the production `final.exit`/`terminal.json` aside (kept) so per-case canary-labelled ones can be written.
C2 per case N in 0, 2, 3 SERIAL (budget below): fresh private copies `$root/canary-case-N/{var-lib-cloud,run-cloud-init,etc-cloud,cloud-init.log,cloud-init-output.log}` made by `cp -a` of the live trees (cached datasource obj.pkl and instance receipts preserved);
   in the COPIES only: reset status.json/result.json (data/ and run), replace `instances/<iid>/scripts/*` with the fixture (`printf N > $root/canary-evidence/case-N/script.exit; exit N`), set the copy's cloud.cfg `cloud_final_modules` to exactly `[[scripts-user, always]]`
   (private copied cloud.cfg.d keeps no other cloud_final_modules definition); unit COPY `borsuk-canary-final-N.service` = the asserted actual unit text with identical Type/ExecStart (`/usr/bin/cloud-init modules --mode=final`, NO --file)/RemainAfterExit/TimeoutSec/KillMode/TasksMax/StandardOutput
   plus ONLY: a new name, `BindPaths=` mapping the five copies onto /var/lib/cloud, /run/cloud-init, /etc/cloud and the two log files, a safety `RuntimeMaxSec=45`, and `ExecStopPost=/bin/bash $root/service-stop.sh bootstrap <bucket> <prefix>/canary/case-N/bootstrap-manager.json`
   (the UNMODIFIED production script; its previous local record is moved aside first because it uses noclobber; terminal.json{instance_id}/final.exit=N are canary-labelled, never a NATIVE_CHAIN_CLOSED claim).
C3 start the unit, wait for its main process, record three SEPARATE tuples: (a) fixture script exit (file written before exit); (b) cloud-init original process status = the unit's ExecMainStatus/ExecMainCode, the copied result.json error list and the modules that actually ran (copied log);
   (c) systemd record EXIT_CODE/EXIT_STATUS/SERVICE_RESULT from the manager JSON. Case 0: the unit stays active (RemainAfterExit), so the coordinator `systemctl stop`s it to produce the success-path ExecStopPost record; cases 2/3 fail and write the record at once; then `reset-failed`.
C4 PROOF OF NO LIVE MUTATION: after the last case repeat the live content-sha256 listing and require it identical to C1's (live /var/lib/cloud, /run/cloud-init, /etc/cloud, logs untouched); a difference => FAIL and recorded.
C5 upload `$prefix/canary/...` results (create-only), then `shutdown -h now`.
Verdict (fail-closed): QUALIFIED only if every case's tuples equal the pinned mapping (nested 0 -> exited/"0"/success; nested 2|3 -> exited/"1"/exit-code, exactly one failed module) AND C4 passes AND T1 passed; any NOT_RUN, deviation, extra failing module,
missing record or admission failure => UNQUALIFIED with the verbatim reason. The pin is never adjusted to the observation.

## 3. Envelope and fit (root numbers; not launch authority)
1x c7i.2xlarge Spot, bid <= 0.59/h, CPU 0-3 / 8 GiB / swap 0, root 80 GiB + fresh 40 GiB gp3; external 3600 s + 120 s grace, LOCAL stop launch+3000 s. 3720 s x 0.59/h = $0.61 <= compute 0.65; + 0.15 ancillary = $0.76 <= 0.80.
Manager-case allowance 180 s TOTAL: per case unit start + wait + stop + record <= 55 s (RuntimeMaxSec=45 + stop <= 10), 3 cases <= 165 s <= 180. C0+C1 <= 40 s, C4+C5 <= 40 s => coordinator <= ~250 s.
Worst case (production constants; t after launch; boot-to-user-data ~90): setup bounded to user-data+900, mkfs <= 300, transport <= 900 => <= ~2200; T2/T3/T4 capped 300/120/40 => tail ends <= ~2660; + coordinator ~250 => <= ~2910 < 3000. Typical ~800-1000 s.
Gates, never extending: G1 refuse transport if remaining < 1360 s; G2 start the coordinator only if remaining >= 250 s, else NOT_RUN and early terminate (=> UNQUALIFIED).
NO ANN: nothing runs prep, derive, generation, publish, baseline queries, truth or any native chain; the four ELFs only receive missing-argument usage errors inside wrapper-canary.sh.

## 4. Artifacts (create-only under the canary `$prefix`; root supplies prefix, support.sha256 for the 8 production names, scratch binding + exact 94-byte companion, launch/volume/role evidence, cost authority)
build-canary-user-data.sh | canary-tail.sh | canary-coordinator.sh | cgroup-canary.sh | iam-canary.sh (~220 lines) + root's unchanged wrapper-canary.sh. user-data ~12.1 KB incl. placeholders (<= 16,384).
Unchanged: production prefix text, service-stop.sh, transport.py, 17 transport pins, scratch binding/format/mount/admission, production finish() upload.

## 5. Concrete seams (nothing invented)
S1 Target DIRECT is asserted at runtime (unit sha), not assumed; a fresh AMI may change it => REFUSE.
S2 Production `finish()` can never yield nested 0/2/3 for a truncated flow (always 99): fixtures qualify the PLATFORM mapping + the unmodified service-stop.sh, never a production closure.
S3 OWN2 watch-original.sh / OWN3 verify-closed.py are not reusable unmodified on this canary (incomplete flow + coordinator evidence): root terminates/collects with its own absolute-clock calls; OWN3 stays unexercised on real evidence.
S4 The prefix cut is anchored on exact production line text (hash-bound; any production edit invalidates it).
S5 The ONE untested mechanism is the namespace admission in C0/C2 (BindPaths of directories AND single log files, private cloud.cfg edit, cp -a of the trees). If any part is unsupported on the target the case is NOT_RUN and the exact failing admission is the reported seam;
   there is deliberately no 4-boot fallback in this plan. cloud-init may write other live state I have not enumerated (e.g. /var/log beyond the two files, /var/cache); C4's before/after content listing is the evidence, and any live change fails the verdict.
S6 Effective final-module list depends on the copied cloud.cfg edit being honored; C3 records the modules that actually ran and fails the annotation if any other module ran.
S7 IAM test (T4) is valid only if the root's deny covers the probe's key; the root-staged probe may be overwritten/deleted if the deny is absent (restorable from the staged copy).
S8 Decoupling the outer manager exit (bootstrap exits 0 on any validated closure) would remove S1/S2 coupling entirely; needs a reviewed OWN2/OWN3 diff; not required for this canary.
