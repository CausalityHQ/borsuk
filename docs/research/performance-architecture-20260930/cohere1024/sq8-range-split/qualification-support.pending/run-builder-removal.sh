#!/bin/bash
# UNVERIFIED source-only wrapper. Run ONLY on the parent's disposable instance.
# ROOT BUCKET PREFIX PROBE_SHA MANIFEST_SHA RUNNER_SHA SOURCE_COMMIT DEADLINE_EPOCH UNIT
# ROOT contains source/, source-files.sha256, prove-builder-removal.sh, .cargo/, .rustup/.
# Parent owns admission, exact unused unit, external timer, termination and spend.
# ShellCheck does not follow EXIT trap calls through the publication helpers.
# shellcheck disable=SC2329
set -euo pipefail
umask 077
export LC_ALL=C AWS_PAGER='' AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard

# Leave time for the original SSM invocation to commit its terminal receipt.
# Parent's independent hard timers remain authoritative and are never modified.
work_created=false
request_shutdown() {
 local rc out=/dev/null err=/dev/null
 if [[ $work_created == true ]]; then
  out=$work/shutdown-request.stdout err=$work/shutdown-request.stderr
 fi
 timeout -k 1 5 /sbin/shutdown -h +1 > "$out" 2> "$err" && rc=0 || rc=$?
 if [[ $work_created == true ]]; then
  printf '%s\n' "$rc" > "$work/shutdown-request.exit" || return 94
 fi
 return "$rc"
}
trap request_shutdown EXIT
[[ $# == 9 && $EUID == 0 ]] || exit 98
root=$1 bucket=$2 prefix=$3 probe_sha=$4 manifest_sha=$5 runner_sha=$6
revision=$7 deadline=$8 unit=$9
[[ $root =~ ^/[a-zA-Z0-9_./-]+$ && $root != / && -d $root &&
   $(realpath -e -- "$root") == "$root" &&
   $bucket =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ &&
   $prefix =~ ^[a-zA-Z0-9][a-zA-Z0-9/_-]{0,511}$ && $prefix != */ &&
   $probe_sha =~ ^[0-9a-f]{64}$ && $manifest_sha =~ ^[0-9a-f]{64}$ &&
   $runner_sha =~ ^[0-9a-f]{64}$ && $revision =~ ^[0-9a-f]{40}$ &&
   $deadline =~ ^[1-9][0-9]{9}$ &&
   $unit =~ ^borsuk-sq8-split-mutation-[a-z0-9-]{1,80}\.service$ ]] || exit 98
started=$(date +%s)
(( deadline - started > 320 && deadline - started <= 1800 )) || exit 98
probe_deadline=$((deadline - 180))
work=$root/builder-removal-wrapper
ev=$root/builder-removal-probe
archive=$root/builder-removal-evidence.tar.gz
[[ ! -e $ev && ! -L $ev && ! -e $archive && ! -L $archive ]] || exit 98
mkdir -- "$work"
work_created=true
cg=/system.slice/$unit
owner="$unit:$revision:$probe_sha:$(cat /proc/sys/kernel/random/uuid)"
invocation='' driver='' launched=false owned=false drained=false forced=false resources=false unit_ok=false
evidence_ok=true probe_exit=null systemd_start_exit=null
properties=(LoadState Description InvocationID ActiveState SubState Result ExecMainCode ExecMainStatus
 ControlGroup CPUUsageNSec MemoryPeak MemoryCurrent MemorySwapPeak TasksCurrent)
show_args=()
for property in "${properties[@]}"; do show_args+=(-p "$property"); done

run() {
 local name=$1 cap=$2 left rc
 shift 2
 left=$((deadline - $(date +%s) - 12))
 (( left <= cap )) || left=$cap
 printf '%s\n' "$left" > "$work/$name.timeout-seconds" || return 94
 if (( left > 0 )); then
  timeout -k 2 "$left" "$@" > "$work/$name.stdout" 2> "$work/$name.stderr" && rc=0 || rc=$?
 else
  rc=124
 fi
 printf '%s\n' "$rc" > "$work/$name.exit" || return 94
 return "$rc"
}

owned_status() {
 local seen
 [[ $systemd_start_exit == 0 ]] || return 96
 run show 5 systemctl show "$unit" "${show_args[@]}" || return
 grep -Fxq 'LoadState=loaded' "$work/show.stdout" || return 96
 grep -Fxq "Description=$owner" "$work/show.stdout" || return 96
 seen=$(sed -n 's/^InvocationID=//p' "$work/show.stdout")
 [[ $seen =~ ^[0-9a-f]{32}$ && ( -z $invocation || $seen == "$invocation" ) ]] || return 96
 if [[ -z $invocation ]]; then
  grep -Fxq "ControlGroup=$cg" "$work/show.stdout" || return 96
  cp -- "$work/show.stdout" "$work/unit.initial" || return 94
  invocation=$seen
 fi
 if ! grep -Fxq "ControlGroup=$cg" "$work/show.stdout"; then
  # RemainAfterExit may release the cgroup after this bound invocation succeeds.
  grep -Fxq 'ControlGroup=' "$work/show.stdout" &&
   grep -Fxq 'Result=success' "$work/show.stdout" &&
   grep -Fxq 'ExecMainCode=1' "$work/show.stdout" &&
   grep -Fxq 'ExecMainStatus=0' "$work/show.stdout" &&
   grep -Fxq 'ActiveState=active' "$work/show.stdout" &&
   grep -Fxq 'SubState=exited' "$work/show.stdout" || return 96
  [[ ! -e /sys/fs/cgroup$cg ]] ||
   grep -Fxq 'populated 0' "/sys/fs/cgroup$cg/cgroup.events" || return 96
 fi
}

# Missing receipts are allowed in an INVALID archive; arbitrary files are never selected.
copy_receipt() {
 local file=$1 dest=$2 size copied
 [[ -e $file || -L $file ]] || return 0
 if [[ ! -f $file || -L $file ]]; then evidence_ok=false; return 0; fi
 size=$(stat -c %s -- "$file") || return 94
 # Reserve 1 MiB of the 64 MiB archive limit for tar headers and the file index.
 if (( size > 8388608 || total + size > 66060288 )); then
  evidence_ok=false
  printf '%s\t%s\tomitted:limit\n' "$dest" "$size" >> "$bundle/files.tsv" || return 94
  return 0
 fi
 head -c 8388609 -- "$file" > "$bundle/$dest" || return 94
 copied=$(stat -c %s -- "$bundle/$dest") || return 94
 if (( copied != size )); then
  evidence_ok=false
  rm -- "$bundle/$dest" || return 94
  printf '%s\t%s\tomitted:changed\n' "$dest" "$size" >> "$bundle/files.tsv" || return 94
  return 0
 fi
 total=$((total + copied))
 printf '%s\t%s\t%s\n' "$dest" "$size" "$copied" >> "$bundle/files.tsv" || return 94
}

collect() {
 local name suffix phase total=0
 bundle=$work/archive
 mkdir -- "$bundle" "$bundle/wrapper" "$bundle/probe" || return 94
 : > "$bundle/files.tsv" || return 94
 for name in pins.json input.sha256 wrapper.sha256 owner.txt unit.initial unit.final unit.stopped \
  unit.stopped.exit probe.exit manager.original.exit cgroup.events.before cgroup.events.after; do
  copy_receipt "$work/$name" "wrapper/$name" || return
 done
 for name in load pins-before pins-after start show stop drain; do
  for suffix in stdout stderr exit timeout-seconds; do
   copy_receipt "$work/$name.$suffix" "wrapper/$name.$suffix" || return
  done
 done
 for name in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.events \
  memory.peak memory.swap.peak cpu.stat pids.peak; do
  for suffix in before after; do
   copy_receipt "$work/$name.$suffix" "wrapper/$name.$suffix" || return
  done
 done
 copy_receipt "$root/prove-builder-removal.sh" probe/prove-builder-removal.sh || return
 copy_receipt "$driver" wrapper/run-builder-removal.sh || return
 for name in driver.started driver.finished driver.sha256 driver.original.exit driver.restoration.exit \
  driver.exit verdict.json receipts.sha256 deadline.epoch source-directory.txt source-manifest-path.txt \
  source.expected.sha256 source.mutant.expected.sha256 cgroup.txt memory.events.before memory.events.after \
  oom-check.exit runner.original.sha256 runner.mutant.sha256 mutation.diff mutation-line.txt \
  expected-panic-location.txt; do
  copy_receipt "$ev/$name" "probe/$name" || return
 done
 for phase in before-positive-restore final-restore; do
  for suffix in copy.exit runner.sha256 runner-identity.exit tree-identity.exit; do
   copy_receipt "$ev/$phase.$suffix" "probe/$phase.$suffix" || return
  done
 done
 local stages=(rustc-version cargo-version rustc-cfg source.before baseline-source mutant-source
  mutant-source-after before-positive-restore.tree final-restore.tree restore-tree)
 for phase in baseline mutant restored; do
  for suffix in compile artifact elf elf-sha inventory test elf-after; do stages+=("$phase.$suffix"); done
  copy_receipt "$ev/$phase.executable.txt" "probe/$phase.executable.txt" || return
 done
 for name in "${stages[@]}"; do
  for suffix in command timeout-seconds started finished stdout stderr time timeout.exit original.exit time.exit; do
   copy_receipt "$ev/$name.$suffix" "probe/$name.$suffix" || return
  done
 done
}

publish() {
 local file=$1 key=$2 label=$3
 run "$label" 45 aws --cli-connect-timeout 5 --cli-read-timeout 20 s3api put-object \
  --bucket "$bucket" --key "$prefix/$key" --body "$file" --if-none-match '*' || return
 [[ $(stat -c %s "$work/$label.stdout") -le 65536 ]] || return 94
 jq -e 'type == "object" and (.ETag | type == "string" and length > 0)' \
  "$work/$label.stdout" > /dev/null || return 94
}

# No failed manager, resource, restoration, drain or writer result becomes a sensitivity pass.
finalize() {
 local original=$1 result=INVALID status=95 file expected value archive_sha unpacked
 printf '%s\n' "$original" > "$work/manager.original.exit" || return 94
 if [[ $launched == true ]] && owned_status; then
  owned=true
  cp -- "$work/show.stdout" "$work/unit.final" || return 94
  if grep -Fxq 'Result=success' "$work/unit.final" &&
     grep -Fxq 'ExecMainCode=1' "$work/unit.final" &&
     grep -Fxq 'ExecMainStatus=0' "$work/unit.final" &&
     grep -Fxq 'ActiveState=active' "$work/unit.final" &&
     grep -Fxq 'SubState=exited' "$work/unit.final"; then unit_ok=true; fi
  if [[ -d /sys/fs/cgroup$cg ]]; then
   cat "/sys/fs/cgroup$cg/cgroup.events" > "$work/cgroup.events.before" || return 94
   grep -Fxq 'populated 0' "$work/cgroup.events.before" || forced=true
  else
   printf '%s\n' absent > "$work/cgroup.events.before" || return 94
  fi
  # Recheck the invocation immediately before stopping this exact unit.
  if owned_status && run stop 5 systemctl stop --no-block "$unit"; then
   # cgroup.events populated includes every descendant, not just cgroup.procs.
   # shellcheck disable=SC2016
   if run drain 20 /bin/bash -c '
    cg=$1; unit=$2; work=$3; owner=$4; invocation=$5
    shift 5
    while :; do
     systemctl show "$unit" "$@" > "$work/unit.stopped"
     rc=$?
     printf "%s\n" "$rc" > "$work/unit.stopped.exit" || exit 94
     grep -Fxq "ControlGroup=$cg" "$work/unit.stopped" ||
      grep -Fxq "ControlGroup=" "$work/unit.stopped" || exit 96
     if grep -Fxq "LoadState=not-found" "$work/unit.stopped"; then
      [[ ( $rc == 0 || $rc == 1 ) && ! -e /sys/fs/cgroup$cg ]] && exit 0
      exit 96
     fi
     [[ $rc == 0 ]] &&
      grep -Fxq "LoadState=loaded" "$work/unit.stopped" &&
      grep -Fxq "Description=$owner" "$work/unit.stopped" || exit 96
     if ! grep -Fxq "InvocationID=$invocation" "$work/unit.stopped"; then
      # A stopped unit may clear its ID before unloading; this is not drain proof.
      grep -Fxq "InvocationID=" "$work/unit.stopped" &&
       grep -Fxq "ActiveState=inactive" "$work/unit.stopped" || exit 96
     elif grep -Eq "^ActiveState=(inactive|failed)$" "$work/unit.stopped"; then
      [[ ! -e /sys/fs/cgroup$cg ]] && exit 0
      grep -Fxq "populated 0" "/sys/fs/cgroup$cg/cgroup.events" && exit 0
     fi
     sleep 0.1
    done
   ' bash "$cg" "$unit" "$work" "$owner" "$invocation" "${show_args[@]}"; then drained=true; fi
  fi
  if [[ -d /sys/fs/cgroup$cg ]]; then
   cat "/sys/fs/cgroup$cg/cgroup.events" > "$work/cgroup.events.after" || return 94
  else
   printf '%s\n' absent > "$work/cgroup.events.after" || return 94
  fi
 fi
 if [[ -f $work/probe.exit && ! -L $work/probe.exit && $(stat -c %s "$work/probe.exit") -le 4 ]]; then
  value=$(cat "$work/probe.exit")
  [[ $value =~ ^[0-9]{1,3}$ ]] && (( value <= 255 )) && probe_exit=$value
 fi
 resources=true
 for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
  case $file in
   cpu.max) expected='1600000 100000' ;; cpuset.cpus.effective) expected=0-15 ;;
   memory.max) expected=60129542144 ;; memory.swap.max) expected=0 ;; pids.max) expected=512 ;;
  esac
  for value in before after; do
   [[ -f $work/$file.$value && $(cat "$work/$file.$value") == "$expected" ]] || resources=false
  done
 done
 for value in before after; do
  grep -Fxq 'oom 0' "$work/memory.events.$value" || resources=false
  grep -Fxq 'oom_kill 0' "$work/memory.events.$value" || resources=false
  awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' "$work/memory.events.$value" || resources=false
  [[ -f $work/memory.swap.peak.$value && $(cat "$work/memory.swap.peak.$value") == 0 ]] || resources=false
 done
 run pins-after 15 sha256sum --strict --check "$work/input.sha256" || resources=false
 collect || return
 if (( original == 0 )) && [[ $systemd_start_exit == 0 && $probe_exit == 0 && $owned == true &&
  $drained == true && $forced == false && $resources == true && $unit_ok == true && $evidence_ok == true ]] &&
  jq -e --arg manifest "$manifest_sha" --arg runner "$runner_sha" \
   '.verdict == "BUILDER_REMOVAL_CAUGHT" and .exit == 0 and .original_exit == 0 and
    .restoration_exit == 0 and .runner_sha256 == $runner and .source_manifest_sha256 == $manifest' \
   "$ev/verdict.json" > /dev/null &&
  [[ $(cat "$ev/driver.exit") == 0 && $(cat "$ev/driver.restoration.exit") == 0 &&
     $(cat "$ev/oom-check.exit") == 0 ]]; then
  result=BUILDER_REMOVAL_CAUGHT status=0
 fi
 run archive 25 tar -czf "$archive" -C "$bundle" -- wrapper probe files.tsv || return
 [[ $(stat -c %s -- "$archive") -le 16777216 ]] || return 95
 run archive-size 5 gzip --list -- "$archive" || return
 unpacked=$(awk 'NR == 2 {print $2}' "$work/archive-size.stdout")
 [[ $unpacked =~ ^[0-9]{1,8}$ ]] && (( unpacked <= 67108864 )) || return 95
 run digest 5 sha256sum -- "$archive" || return
 archive_sha=$(cut -d ' ' -f1 "$work/digest.stdout")
 [[ $archive_sha =~ ^[0-9a-f]{64}$ ]] || return 94
 printf '%s  evidence.tar.gz\n' "$archive_sha" > "$work/evidence.sha256" || return 94
 # No terminal is published if either earlier create-only write fails.
 publish "$archive" evidence.tar.gz put-archive || return
 publish "$work/evidence.sha256" evidence.sha256 put-digest || return
 jq -n --arg verdict "$result" --arg source "$revision" --arg probe "$probe_sha" \
  --arg manifest "$manifest_sha" --arg runner "$runner_sha" --arg archive "$archive_sha" \
  --arg unit "$unit" --arg invocation "$invocation" --arg prefix "$prefix" \
  --argjson manager "$original" --argjson start "$systemd_start_exit" --argjson probe_exit "$probe_exit" \
  --argjson deadline "$deadline" --argjson probe_deadline "$probe_deadline" --argjson exit "$status" \
  --argjson owned "$owned" --argjson drained "$drained" --argjson forced "$forced" \
  --argjson resources "$resources" --argjson unit_ok "$unit_ok" --argjson evidence "$evidence_ok" \
  '{schema:"borsuk-builder-removal-v1",verdict:$verdict,exit:$exit,source_commit:$source,
    probe_sha256:$probe,source_manifest_sha256:$manifest,runner_sha256:$runner,
    evidence_archive_sha256:$archive,prefix:$prefix,unit:$unit,invocation_id:$invocation,
    manager_original_exit:$manager,systemd_start_exit:$start,probe_exit:$probe_exit,
    unit_owned:$owned,owned_cgroup_drained:$drained,forced_cleanup:$forced,
    resources_verified:$resources,unit_status_verified:$unit_ok,evidence_complete:$evidence,
    deadline_epoch:$deadline,probe_deadline_epoch:$probe_deadline,
    archive_publication_exit:0,digest_publication_exit:0,
    interpretation:"Execution falsifier only. Parent must also verify original wrapper/publisher exit zero; missing or failed publication is INVALID, never an algorithm rejection."}' \
  > "$work/terminal.json" || return 94
 publish "$work/terminal.json" terminal.json put-terminal || return
 return "$status"
}

# Invoked by EXIT; publication failures still request delayed shutdown.
finish() {
 local original=$? rc publication_rc shutdown_rc
 trap - EXIT
 trap '' HUP INT TERM
 set +e
 finalize "$original"
 rc=$?
 publication_rc=$rc
 printf '%s\n' "$publication_rc" > "$work/publication.exit" || rc=94
 request_shutdown && shutdown_rc=0 || shutdown_rc=$?
 (( shutdown_rc == 0 || rc != 0 )) || rc=96
 printf '%s\n' "$rc" > "$work/wrapper.exit" || rc=94
 printf 'probe_exit=%s\npublication_exit=%s\nshutdown_request_exit=%s\nwrapper_exit=%s\n' \
  "$probe_exit" "$publication_rc" "$shutdown_rc" "$rc" || rc=94
 exit "$rc"
}
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
driver=$(realpath -e -- "$0")
[[ -d $root/source && $(realpath -e -- "$root/source") == "$root/source" &&
   -d $root/.cargo && -d $root/.rustup ]]
for file in "$root/prove-builder-removal.sh" "$root/source-files.sha256" \
 "$root/source/crates/borsuk/src/bin/check_cohere_native_baseline.rs"; do
 [[ -f $file && ! -L $file && $(stat -c %h "$file") == 1 ]]
done
printf '%s  %s\n' "$probe_sha" "$root/prove-builder-removal.sh" \
 "$manifest_sha" "$root/source-files.sha256" \
 "$runner_sha" "$root/source/crates/borsuk/src/bin/check_cohere_native_baseline.rs" > "$work/input.sha256"
printf '%s\n' "$owner" > "$work/owner.txt"
sha256sum -- "$driver" > "$work/wrapper.sha256"
jq -n --arg source "$revision" --arg probe "$probe_sha" --arg manifest "$manifest_sha" \
 --arg runner "$runner_sha" --argjson deadline "$deadline" \
 '{source_commit:$source,probe_sha256:$probe,source_manifest_sha256:$manifest,
   runner_sha256:$runner,deadline_epoch:$deadline}' > "$work/pins.json"
run pins-before 15 sha256sum --strict --check "$work/input.sha256"
run load 5 systemctl show "$unit" -p LoadState --value && load_rc=0 || load_rc=$?
[[ ( $load_rc == 0 || $load_rc == 1 ) && $(cat "$work/load.stdout") == not-found ]]
left=$((probe_deadline - $(date +%s) + 10))
(( left > 140 ))
launched=true
# RemainAfterExit retains the invocation and main status until our owned stop.
# The small service shell records the probe's real exit, before resource/status checks.
# shellcheck disable=SC2016
run start 20 systemd-run --expand-environment=no --unit="$unit" --service-type=exec \
 -p Description="$owner" -p WorkingDirectory="$root/source" -p RemainAfterExit=yes \
 -p CPUQuota=1600% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0-15 \
 -p MemoryMax=60129542144 -p MemorySwapMax=0 -p TasksMax=512 \
 -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec="$left" -p LimitCORE=0 \
 -p StandardOutput=null -p StandardError=null \
 --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" \
 --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
 /bin/bash -c '
  set -uo pipefail
  root=$1; work=$2; ev=$3; deadline=$4; runner=$5; manifest=$6; expected_cg=$7
  cg=$(awk -F: '\''$1 == 0 {print $3}'\'' /proc/self/cgroup)
  [[ $cg == "$expected_cg" ]] || exit 96
  snapshot() {
   local file
   for file in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max memory.events \
    memory.peak memory.swap.peak cpu.stat pids.peak; do
    cp -- "/sys/fs/cgroup$cg/$file" "$work/$file.$1" || return 96
   done
  }
  snapshot before || exit 96
  [[ $(cat "$work/cpu.max.before") == "1600000 100000" &&
     $(cat "$work/cpuset.cpus.effective.before") == 0-15 &&
     $(cat "$work/memory.max.before") == 60129542144 &&
     $(cat "$work/memory.swap.max.before") == 0 &&
     $(cat "$work/pids.max.before") == 512 ]] || exit 96
  /bin/bash "$root/prove-builder-removal.sh" "$root/source" "$runner" "$ev" "$deadline" "$manifest"
  rc=$?
  printf "%s\n" "$rc" > "$work/probe.exit" || exit 94
  snapshot after || exit 96
  exit "$rc"
 ' bash "$root" "$work" "$ev" "$probe_deadline" "$runner_sha" "$manifest_sha" "$cg" \
 && systemd_start_exit=0 || systemd_start_exit=$?
(( systemd_start_exit == 0 )) || exit "$systemd_start_exit"
while :; do
 owned_status || exit 96
 if grep -Fxq 'SubState=exited' "$work/show.stdout" ||
    grep -Eq '^ActiveState=(failed|inactive)$' "$work/show.stdout"; then break; fi
 (( $(date +%s) < probe_deadline + 15 )) || exit 124
 sleep 1
done
exit 0
