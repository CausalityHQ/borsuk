# Canary plan R5 (READ-ONLY proposal; nothing implemented, nothing run). Root reviews before any implementation.
R5 = R4 + root's 1791584223 (private minimal /mnt/borsuk-scale1m per case; committed caps wrapper 120 / manager 180; hooks pinned individually) + 1791584294 (RuntimeMaxSec is a no-op for oneshot -> explicit Timeout*Sec + absolute deadlines).
Envelope reconciliation: all parent envelopes to me were read (agent_messages + cross_audit: 276, 348, 443, 509, 557, 867, 223, 294). No multi-boot, no reboot fallback, no controller beyond ONE coordinator.

## 0. Target facts (a0001 closed evidence, `tar -xzO` of two tiny members; F untouched)
Ubuntu 24.04.5, systemd 255, bash 5.2.21, jq-1.7, GNU time; cloud-init 26.1-0ubuntu1~24.04.1 (console). cloud-final base unit (27 lines/755 B) sha256 a4ef0420239ab2bdbb10f75924bdbea4c6c4df07cb752858a4e77581ddcc8a76:
Type=oneshot, `ExecStart=/usr/bin/cloud-init modules --mode=final`, RemainAfterExit=yes, TimeoutSec=0, KillMode=process, TasksMax=infinity => DIRECT CLI. a0001 nested-0 record: exited/"0"/success, final_exit "0".
CLI status = len(final-stage module errors); a failing user script (2 OR 3) = one failed `scripts-user` module => 1 if exactly one fails (local 26.04 source reading; target 2/3 UNPROVEN). A non-DIRECT unit is REFUSED, never emulated.
Successful oneshot RemainAfterExit => ExecStopPost only at unit STOP. `RuntimeMaxSec` is documented as NO EFFECT for Type=oneshot (man systemd.service) -> never used to bound a case; production systemd-run units are Type=simple, where it works.

## 1. Boot 1 = canary user-data (NOT claimed byte-identical): pinned segments + individually pinned hooks + stub
Production d4 `user-data.sh` (blob a9ebd7a5) lines 1..158 (cut at the unique `chmod 0500 assets/bin/*`, line 159) = unchanged segments, each pinned by line range + sha256, EXCEPT these four hook points, each replacement text and the ORIGINAL line sha pinned (assembler refuses on any mismatch):
 H1 line 15 `local_stop_epoch=$((boot_epoch + 14400))` -> `local_stop_epoch=<ROOT_FROZEN_STARTED_EPOCH>+3000` (root placeholder frozen at launch, like prefix/support SHA; guard: boot_epoch within [started-5, started+600] else exit 95). BEFORE any work.
 H2 line 76 `shutdown -h +240` -> `shutdown -h +$(( (local_stop_epoch - $(date +%s)) / 60 ))` (>=1; machine-local stop = started+3000, set at the same early point, not re-armed at the tail).
 H3 line 110 `setup_stop=$((boot_epoch + 900))` -> `boot_epoch + 800` so binding wait + mkfs + mount + admission fit the 900 s setup bucket.
 H4 line 137 `timeout -k 5 300 mkfs.ext4` -> `timeout -k 5 60 mkfs.ext4` (blank 40 GiB lazy-init mkfs is seconds).
 (If the root rejects H3/H4 the setup bucket can overrun by <= ~85 s/~300 s and is charged to the margin; stated, not hidden.)
Stub (~450 B): fetch the pinned `canary-tail.sh` via the production `get`, require its SHA, run it. user-data ~12.2 KB incl. placeholders (<= 16,384).

## 2. Tail (inside the ORIGINAL cloud-final; every step under a hard `timeout`, overrun => INVALID, unknown shape/resource failure => INVALID)
 T1 STACK ASSERTION (<=10 s): `systemctl cat cloud-final.service`, `cloud-init --version`, `dpkg-query -W -f='${Version}' cloud-init`, `systemd --version`; require base-unit sha == the pin. Non-DIRECT => REFUSE (no coordinator, UNQUALIFIED, early terminate).
    Version != 26.1-0ubuntu1~24.04.1 => VERSION_DIFFERS recorded (root decides); unit equality is the hard gate.
 T2 `wrapper-canary.sh $root/run_native_scale_build_gate.sh $root/assets/bin $root/canary-wrapper` (root's, unchanged) cap 70 s.
 T3 `cgroup-canary.sh` cap 25 s: awk-extract the wrapper's jq programs + `resources()/resource_closure()`; run them in `systemd-run` with the PRODUCTION chain properties (CPUQuota=400% AllowedCPUs=0-3 MemoryMax=8G MemorySwapMax=0 TasksMax=128 KillMode=control-group); require exact effective 4 CPU/8GiB/swap0/pids128 + closure; stray `setsid sleep` must die on stop.
 T4 `iam-canary.sh` cap 15 s with the ROOT-STAGED probe `research/semantic-router/20261009/actual1m-scale-chain-canary-a0001/inputs/permission-probe.txt` (38 B, sha 6111cbc9f504be3dc43c96dc450f2d30d319ab20ccdcf0acf94d65a518072fc9): GET sha-pinned; PUT of the IDENTICAL bytes must be AccessDenied;
    DELETE of that key must be AccessDenied; GET unchanged; positive PUT under `$prefix/canary/` succeeds. No guessed/nonexistent key is deleted. Unexpected success => FAIL (root restores the probe).
    (T1+T2+T3+T4 = 120 s = the committed wrapper-class cap.)
 T5 write `borsuk-canary-coordinator.service` (`After=cloud-final.service`), `systemctl start --no-block` it, exit 0. The production `finish()` then runs honestly (status 99, upload, final.exit 99) and the original cloud-final fails => its ExecStopPost record is the natural nested-99 datapoint (live files untouched afterwards).

## 3. Coordinator (`canary-coordinator.sh`; starts only after the original cloud-final reached inactive/failed). MANAGER BATCH = 180 s TOTAL, absolute deadline D = start+180
Every systemctl/cp/du/poll call runs as `timeout -k 1 min(per-call cap, D-now)`; polls are 1 s loops with 3 s per-call caps and a case deadline; no unbounded wait anywhere.
 C0 ADMISSION (<=20 s): original manager record present; systemd >= 233; `systemd-analyze verify` of the unit copies; trial BindPaths unit starts; size caps (`du -sb`: /var/lib/cloud <= 64 MiB, /run/cloud-init <= 8 MiB, /etc/cloud <= 1 MiB, the two logs <= 64 MiB each);
    no sockets/FIFOs/devices under the trees; free space >= 3x copies + 1 GiB. Failure => NOT_RUN with the exact reason; no fallback.
 C1 PRESERVE + BASELINE (<=15 s): copy the original manager record/`systemctl show`/status/result/data to `/mnt/borsuk-canary-cases/original/` (outside the bound target) and upload; content-sha256 listing (names+contents, not mtimes) of LIVE /var/lib/cloud, /run/cloud-init, /etc/cloud,
    the two logs AND the named live files /mnt/borsuk-scale1m/{terminal.json,final.exit,bootstrap-manager.json,bootstrap-manager.put.json} + evidence-root/. NOTHING live is moved, replaced or rewritten.
 C2 per case N in 0, 2, 3, SERIAL, case slice <= 35 s (start <= 20, stop <= 8, drain <= 5, record <= 2): backing dirs OUTSIDE the bound target: `/mnt/borsuk-canary-cases/N/{var-lib-cloud,run-cloud-init,etc-cloud,cloud-init.log,cloud-init-output.log,root,out}` made by `cp -a` of the live trees (cached datasource + boot receipts preserved).
    `root/` = canary-owned MINIMAL root bound onto /mnt/borsuk-scale1m: exact copy of `service-stop.sh` (sha == support pin) + canary-labelled `terminal.json` {schema borsuk-canary-terminal-v1, instance_id, canary:true}. In the COPIES only: reset status.json/result.json (data/ and run), replace `instances/<iid>/scripts/*` with the fixture
    (writes `/mnt/borsuk-scale1m/final.exit` = N inside the namespace, `out/script.exit` = N outside it, then `exit N`), set the copy's cloud.cfg `cloud_final_modules` to exactly `[[scripts-user, always]]` (copied cloud.cfg.d keeps no other definition).
    Unit COPY `borsuk-canary-final-N.service` — HONEST OVERRIDES vs the asserted base unit (everything else copied verbatim; ExecStart unchanged): new name; `BindPaths=` for the five cloud copies AND `/mnt/borsuk-canary-cases/N/root:/mnt/borsuk-scale1m`; `TimeoutSec=0` REPLACED by `TimeoutStartSec=20` and `TimeoutStopSec=8`
    (RuntimeMaxSec NOT used); `ExecStopPost=/bin/bash /mnt/borsuk-scale1m/service-stop.sh bootstrap <bucket> <prefix>/canary/case-N/bootstrap-manager.json`. systemd applies the same BindPaths to ExecStopPost, so the UNMODIFIED script sees the same private root; it writes bootstrap-manager.json / bootstrap-manager.put.json into `root/`
    (retained). EXIT_CODE/EXIT_STATUS/SERVICE_RESULT come only from systemd - no synthesis; terminal.json is canary-labelled, never a NATIVE_CHAIN_CLOSED claim. Fresh `root/` per case => no noclobber collision, no moving of live records.
 C3 start, poll to a terminal state under the case deadline, record three SEPARATE tuples: (a) fixture script exit (`out/script.exit`) and `final.exit`; (b) cloud-init original process status = the unit's ExecMainCode/ExecMainStatus/Result, the copied result.json error list and the modules that actually ran (copied log);
    (c) the systemd record from `root/bootstrap-manager.json`. Case 0 stays active (RemainAfterExit) so it is explicitly `systemctl stop`ped (<= 8 s) to produce the success-path record; cases 2/3 fail and record immediately; then `reset-failed`.
    Any failed start/stop/poll/drain or timeout => that case NOT_RUN/INVALID with the ACTUAL timeout/signal retained (Result=timeout, ExecMainCode killed/TERM...), NO further case is launched, and only the exact owned case cgroup (`systemctl show -p ControlGroup` must equal /system.slice/borsuk-canary-final-N.service) is drained (cgroup.kill, then stop).
 C4 PROOF OF NO LIVE MUTATION (<=20 s): repeat the C1 listing and require it identical. C0+C1 <= 35, 3 cases <= 105, C4 <= 20 => <= 160 <= 180 (20 s slack inside the batch; never a larger cap).
 C5 (outside the manager batch, inside the evidence-cleanup bucket) upload `$prefix/canary/...` create-only, then `shutdown -h now`.
Verdict (fail-closed): QUALIFIED only if T1 passed, C4 passed and every case's tuples equal the pinned mapping (nested 0 -> exited/"0"/success; nested 2|3 -> exited/"1"/exit-code, exactly one failed module). Any NOT_RUN, deviation, extra failing module, missing record or admission failure => UNQUALIFIED with the verbatim reason. The pin is never adjusted.

## 4. Explicit budget (root c81: external 3600 s + 120 s grace, LOCAL stop = started+3000; $0.61 at 0.59/h <= 0.65 compute; +0.15 = $0.76 <= 0.80). Not launch authority.
| bucket | cap | covers (worst case, hard-bounded) |
|---|---|---|
| setup | 900 | apt/AWS CLI/support/binding window (H3: <= user-data+800) + identity + mkfs (<= 65 via H4) + mount/admission |
| transport | 900 | production unit RuntimeMaxSec=900 (Type=simple) + receipt validation |
| wrapper-class | 120 | T1 10 + wrapper-canary 70 + cgroup 25 + IAM 15 |
| manager | 180 | C0..C4 above (batch deadline D) |
| evidence-cleanup | 600 | production finish() <= 300 (unit stop checks, copies+cmp, tar, uploads 150+30+30) + C5 upload/shutdown <= 300 |
| margin | 300 | EC2 boot to user-data (<= ~90) + slop |
Worst-case timeline (t after launch): user-data ~90 -> setup <= 990 -> transport <= 1890 -> wrapper-class <= 2010 -> finish <= 2310 -> manager <= 2490 -> upload/shutdown <= 2790 -> 210 s unused margin => <= 3000. Typical ~800-1000 s.
Deadline guards, never extending a cap: before transport remaining >= 900+900; before T1 remaining >= 120+300+180+300; before the coordinator remaining >= 180+300; inside the batch every call is clamped to D. A guard failure => NOT_RUN/INVALID and early terminate. No ANN: nothing runs prep, derive, generation, publish,
queries, truth or any native chain; the four ELFs only get missing-argument usage errors inside wrapper-canary.sh.

## 5. Artifacts (create-only under the canary `$prefix`; root supplies prefix, started epoch, support.sha256 for the 8 production names, scratch binding + exact 94-byte companion, launch/volume/role evidence, cost authority)
build-canary-user-data.sh (assembler: pins the production blob, every unchanged segment, every hook original line + replacement) | canary-tail.sh | canary-coordinator.sh | cgroup-canary.sh | iam-canary.sh (~260 lines) + root's unchanged wrapper-canary.sh.

## 6. Concrete seams (nothing invented)
S1 Target DIRECT is asserted at runtime (unit sha), not assumed; a fresh AMI may change it => REFUSE.
S2 Production `finish()` can never yield nested 0/2/3 for a truncated flow (99): fixtures qualify the PLATFORM mapping + the unmodified service-stop.sh, never a production closure.
S3 OWN2 watch-original.sh / OWN3 verify-closed.py are not reusable unmodified here (incomplete flow + coordinator evidence): root terminates/collects with its own absolute-clock calls; OWN3 stays unexercised on real evidence.
S4 Cutting and hooking the production text means the canary user-data is NOT the production bytes: the assembler's per-segment/per-hook pins are the only provenance; any production edit invalidates it. H3/H4 change production timing constants for the canary only.
S5 The one untested mechanism is the namespace admission (BindPaths of dirs, single log files and the private /mnt/borsuk-scale1m root; ExecStopPost seeing the same binds; private cloud.cfg edit; cp -a). Any unsupported part => NOT_RUN with the exact failing admission; there is deliberately no fallback route.
   cloud-init may write live state I have not enumerated; C4's before/after content listing is the only evidence and any live change fails the verdict.
S6 Effective final-module list depends on the copied cloud.cfg edit being honored; C3 records the modules that ran and fails the case annotation if any other module ran.
S7 With KillMode=process (base) strays survive a timeout; the exact-cgroup drain is the only cleanup and its failure is INVALID.
S8 IAM test valid only if the root's deny covers the probe key (the probe may be overwritten/deleted if the deny is absent; restorable from root's staged copy).
S9 Decoupling the outer manager exit (bootstrap exits 0 on any validated closure) would remove S1/S2 coupling; needs a reviewed OWN2/OWN3 diff; not required here.
