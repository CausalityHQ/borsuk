# OWN2 review at 92bbe8fc: `user-data.sh` and `watch-original.sh`

**Verdict: GO to a bounded, disposable composition canary only after the one watcher fix (B1) and the start-time fix (P1).** B1 also changes no user-data bytes. This is not science or launch authorization. I made no edits and ran no tests, network calls or jobs. I read the scripts from Git blobs: user-data.sh is 15933 B and watch-original.sh is 7700 B, which match the contract.

## Concrete blockers

**B1. The watcher can never accept a 2 or 3 outcome** (`watch-original.sh:76-77`).
- The watcher requires the receipt's `exit_status` and `final_exit` to both equal `terminal.exit`.
- `exit_status` comes from cloud-final, whose main process is `cloud-init modules --mode=final`, not the user-data script. When a script fails, cloud-init exits 1. The unit then fails right away and the ExecStopPost hook records `EXIT_STATUS=1`.
- So every `BASELINE_NONZERO_EXIT` (exit 2 or 3) ends as watcher exit 13, INVALID. The only historical receipt (a0001) shows `"0"` for an exit-0 run, so the 2/3 path has never been checked.
- The script's real exit already reaches the receipt through `final_exit`, which service-stop.sh reads from `final.exit`. The fix:
  ```
  .exit_code=="exited" and .final_exit==$x and
  (if $x=="0" then .exit_status=="0" and .service_result=="success"
   else .exit_status!="0" and .service_result=="exit-code" end)
  ```
  Pin the exact nonzero value (expected `"1"`) after the canary. The canary must force one exit 2 or 3 run. The contract's S7/S8 wording should be corrected to match.

**P1. The cost bound depends on a start time the watcher simply trusts** (`watch-original.sh:11-18`).
- 15120 s × $0.59/h = $2.478, which leaves only $0.022 (about 134 s) under the $2.50 compute cap.
- If the root passes the time it started the watcher, or the time it wrote the binding, instead of the instance's LaunchTime, the bound slips by that delay. Writing the binding needs the volume id, which only appears after the instance is running.
- Fix: on the first poll, read `Reservations[0].Instances[0].LaunchTime` and exit 64 if `started` is later than it, or use the earlier of the two.

## Should fix before the canary (watcher side or root launch receipt)

1. **A late watcher never terminates the instance** (`watch-original.sh:45-55`). If the watcher starts or restarts after started+15120, the terminate loop never runs and no terminate call is issued. Fix: in the unconfirmed branch, make one best-effort 25 s `terminate-instances` call before `exit 11`. The instance's own `shutdown -h +240` is the backstop only if the instance shuts down with terminate behaviour.
2. **Root launch checks that neither script can see.** The root must check these and record the API responses:
   - the `/dev/sdf` volume has `DeleteOnTermination=true`, no `SnapshotId`, size 40, and type gp3 with IOPS and throughput declared;
   - `InstanceInitiatedShutdownBehavior=terminate`, a one-time Spot request, and `MaxPrice ≤ 0.59`.

   The volume id is usually missing from the RunInstances response while the instance is still pending. So "only from the original RunInstances mapping" can't be done literally; the root will need DescribeInstances and DescribeVolumes and should record those responses.
3. **Check that the scratch volume is actually deleted after termination.** Otherwise it can leak, keep billing, and be mistaken for a fresh volume later. The watcher can read the volume id from the binding object and call `describe-volumes` until it is gone.
4. **The instance role must not be able to write `$prefix/inputs/*`.** The SHA companion only proves the file wasn't corrupted: it comes from the same writer over the same channel, so it isn't proof of who wrote it. S3 write permission is the real trust anchor, and the instance can write to `$prefix/*` for evidence.

## Minor issues (fail closed or only mislabel; never a false 0/2/3)

| Location | Issue | Minimal fix |
|---|---|---|
| user-data `finish():20` | curl at lines 79, 88 and 89 (`phase=bootstrap`) can exit with 90–98, e.g. 92 for an HTTP/2 stream error. These are then reported as "prep unit nonzero" or "evidence failure". | `elif [[ $phase != bootstrap ]] && (( original>=90 && original<=98 ))` (about 20 B) |
| user-data:116 | `jq -er` reads multiple JSON values from one file, so `{bad}{valid}` is accepted. | `jq -ser` with `length==1` (about 20 B) |
| watcher:66-67 | `artifacts.sha256` is downloaded but never checked, yet `collection.json` says `archive_authenticated:true`. | Extract safely and verify, or record `manifest_verified:false` and leave it to the replay. |
| watcher:68-77 | `collection.json` is written before the closure check (line 71) and the manager check (line 76), so an INVALID run leaves an authentic-looking file. | Write it last, or add a `watcher_exit` field. |
| watcher | A Spot reclaim and a glue failure both end as exit 10. | Save `StateReason` and `StateTransitionReason` after termination (one call). |
| watcher:37-41 | On a successful run the manager receipt is only written when the instance shuts down, so the manager wait always times out (about 60–360 s of billed idle). | None needed. The comment claiming no race is misleading. |

## Questions only the canary can answer

- **Device naming:** the NVMe serial format on c7i, and that exactly one device matches. The `nvme*n1` glob would also match hidden multipath nodes, which fails closed with 90.
- **cloud-init exit codes:** the code for a forced exit 2 or 3 (B1). On a successful run, check cloud-init doesn't exit 2 for warnings.
- **Disk throughput:** gp3 defaults to 125 MiB/s and 3000 IOPS, and the envelope declares only type and size. The derive step's 1M-seek SQ8 pass becomes disk-bound if the 4.1 GB normalized file is evicted under the 8 GiB memory limit. Record phase wall times and the disk samples.
- **Timing margins:**
  - how long shutting-down → terminated takes against the 120 s grace (exit 11 there is conservative; billing stops at shutting-down);
  - whether background inode-table zeroing competes with I/O;
  - that a slow setup is refused with 95, as designed.
- **Binding wait:** whether the root can write the binding inside the 900 s window.

## Checked and correct

- **Binding companion:** `cmp` of the exact 94-byte companion is correct (64 + 2 + 27 + 1).
- **Binding contents:** keys, schema, instance, device, size and volume-id format are all checked.
- **Disk selection:** the device is chosen by serial only. Size, partitions, mounts, FSTYPE/PTTYPE, wipefs, swap, `blkid` rc 2, the zeroed first 4 MiB and the mount device check are all correct.
- **No forced format:** `mkfs` with stdin from /dev/null aborts rather than overwrite an existing filesystem.
- **Scratch space:** ext4 `-m 0` on 40 GiB leaves about 39 GiB free (about 0.9 GiB overhead). That clears the 28 GiB scratch floor, the wrapper's 27.58e9 total and its 23.28e9 free-space check.
- **Root-volume space:** the root floor is 2,438,351,737 + 1.5 GiB on an 80 GiB volume.
- **Support files:** the pinned SHA leads to exactly 8 files, each under 64 KiB, checked with `--strict` against an exact name set.
- **17 assets:** all four binaries' sizes and SHAs equal the gate-config template. The prep binary (`a3a828be`) and the requests file (`8460a81f`, the wrapper's `RESERVED_SHA`) also match.
- **Gate config:**
  - `scratch_root` is the mount, and all five namespaces are direct children whose paths match the evidence paths in `finish()`;
  - the wrapper's cgroup limits match the chain unit's properties;
  - finalization checks the 5 seals, the null precondition and the two-field fill. The strip-and-`cmp` step is nearly a no-op, so the replay must pin the jq 1.7.x serializer.
- **Exit separation:** a 0/2/3 exit is reported only when the chain closed and the original exit equals the chain exit. The chain exit is cross-checked between the unit receipt, `wrapper.exit` and `terminal.json`, and a signal after closure becomes 97.
- **Deadlines:** the start-up time checks (12780 s and 10290 s) and the per-call clamp of min(25 s, time left) are correct.
- **Evidence is immutable:** every upload is create-only (`--if-none-match`), and the instance is terminated before anything is collected.
- **Baseline output stays opaque:** it is copied (≤64 MiB) and never parsed.

I saved these findings as a memory note so the next OWN2 revision can be checked against B1 and P1 directly.
