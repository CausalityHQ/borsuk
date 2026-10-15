#!/bin/bash
# UNVERIFIED source-only supervisor; infrastructure/cost authority stays with root.
set -euo pipefail
[[ $# == 5 ]] || exit 98
root=$1
revision=$2
inner_sha=$3
inventory=$4
prefix=$5
[[ $root == /mnt/borsuk-paired-qualification && $revision =~ ^[0-9a-f]{40}$ && $inner_sha =~ ^[0-9a-f]{64}$ && $inventory == 24 ]] || exit 98
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/gc-session-[a-z0-9-]+$ ]] || exit 98
[[ -f $root/QUALIFICATION_ADMITTED && ! -L $root/QUALIFICATION_ADMITTED ]] || exit 98
[[ $(cat "$root/QUALIFICATION_ADMITTED") == "$revision" ]] || exit 98
[[ -f $root/gate-deadline.epoch && ! -L $root/gate-deadline.epoch ]] || exit 98
external_deadline=$(cat "$root/gate-deadline.epoch")
[[ $external_deadline =~ ^[1-9][0-9]{9}$ ]] || exit 98
started=$(date +%s)
(( external_deadline <= started + 5520 )) || external_deadline=$((started + 5520))
machine_deadline=$((started + 5400))
(( machine_deadline <= external_deadline - 120 )) || machine_deadline=$((external_deadline - 120))
native_deadline=$((started + 4200))
(( native_deadline <= machine_deadline - 180 )) || native_deadline=$((machine_deadline - 180))
ev=$root/evidence/gc-session
manager_ev=$root/evidence/gc-session-manager
mkdir "$manager_ev"
unit=borsuk-gc-session-qualification.service
cg=/system.slice/$unit
owner="$unit:$revision:$inner_sha:$(cat /proc/sys/kernel/random/uuid)"
printf '%s\n' "$owner" > "$manager_ev/owner.txt"
printf '%s\n' "$external_deadline" > "$manager_ev/external-deadline.epoch"
printf '%s\n' "$machine_deadline" > "$manager_ev/machine-deadline.epoch"
printf '%s\n' "$native_deadline" > "$manager_ev/native-deadline.epoch"
printf '%s\n' 'native_seconds=4200 machine_seconds=5400 external_seconds=5520 root_cost_ceiling_usd=0.60' > "$manager_ev/root-envelope.txt"
manager=null
native=null
owned=false
drained=false
forced_cleanup=false
unit_status=false
resources=false
launched=false
current_stage=preflight
manager_stage() {
 local name=$1 cap=$2 left rc
 shift 2
 current_stage=$name
 printf '%q ' "$@" > "$manager_ev/$name-command.txt" || return 94
 printf '\n' >> "$manager_ev/$name-command.txt" || return 94
 date -u +%FT%TZ > "$manager_ev/$name-started.txt" || return 94
 left=$(( machine_deadline - $(date +%s) - 2 ))
 (( left <= cap )) || left=$cap
 printf '%s\n' "$left" > "$manager_ev/$name-timeout-seconds.txt" || return 94
 if (( left <= 0 )); then
  printf '%s\n' 'shared machine deadline exhausted before stage' > "$manager_ev/$name.log" || return 94
  : > "$manager_ev/$name-time.txt" || return 94
  rc=124
 else
  /usr/bin/time -v -o "$manager_ev/$name-time.txt" timeout -k 2 "$left" "$@" > "$manager_ev/$name.log" 2> "$manager_ev/$name.stderr" && rc=0 || rc=$?
 fi
 printf '%s\n' "$rc" > "$manager_ev/$name-exit.txt" || return 94
 date -u +%FT%TZ > "$manager_ev/$name-finished.txt" || return 94
 return "$rc"
}
# Invoked by the EXIT trap.
# shellcheck disable=SC2329
finish() {
 local original=$? show_rc stop_rc status f resource_ok archive archive_sha publication_deadline remaining put_rc invocation
 trap - EXIT
 set +e
 status=$original
 printf '%s\n' "$original" > "$manager_ev/supervisor-original-exit.txt"
 printf '%s\n' "$current_stage" > "$manager_ev/final-stage.txt"
 if [[ $launched == true ]]; then
  manager_stage systemd-after 10 systemctl show "$unit" -p LoadState -p Description -p InvocationID -p ActiveState -p SubState -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup
  show_rc=$?
  printf '%s\n' "$show_rc" > "$manager_ev/show-exit.txt"
  # ExecStopPost captures authoritative status before a successful unit unloads.
  invocation=$(sed -n 's/^InvocationID=//p' "$manager_ev/unit-stop-status.txt" 2>/dev/null)
  if [[ $invocation =~ ^[0-9a-f]{32}$ && -f $manager_ev/unit-stop-status.txt ]] && grep -Fxq "Description=$owner" "$manager_ev/unit-stop-status.txt" && grep -Fxq "ControlGroup=$cg" "$manager_ev/unit-stop-status.txt"; then
   if { grep -Fxq "Description=$owner" "$manager_ev/systemd-after.log" && grep -Fxq "InvocationID=$invocation" "$manager_ev/systemd-after.log"; } || { [[ $show_rc == 0 || $show_rc == 1 ]] && grep -Fxq 'LoadState=not-found' "$manager_ev/systemd-after.log"; }; then
    owned=true
   fi
   if grep -Fxq 'Result=success' "$manager_ev/unit-stop-status.txt" && grep -Fxq 'ExecMainCode=1' "$manager_ev/unit-stop-status.txt" && grep -Fxq 'ExecMainStatus=0' "$manager_ev/unit-stop-status.txt"; then unit_status=true; fi
  elif grep -Fxq "Description=$owner" "$manager_ev/systemd-after.log" && grep -Fxq "ControlGroup=$cg" "$manager_ev/systemd-after.log"; then
   owned=true
  fi
  if [[ $owned == true ]]; then
   if [[ -d /sys/fs/cgroup$cg ]]; then cat "/sys/fs/cgroup$cg/cgroup.events" > "$manager_ev/cgroup.events.before"; else printf '%s\n' absent > "$manager_ev/cgroup.events.before"; fi
   if [[ ! -d /sys/fs/cgroup$cg ]] || grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi
   if [[ $drained != true ]]; then
    forced_cleanup=true
    manager_stage stop 10 systemctl stop "$unit"
    stop_rc=$?
    printf '%s\n' "$stop_rc" > "$manager_ev/stop-exit.txt"
    if [[ -w /sys/fs/cgroup$cg/cgroup.kill ]]; then
     printf '1\n' > "/sys/fs/cgroup$cg/cgroup.kill"
     printf '%s\n' "$?" > "$manager_ev/cgroup-kill-exit.txt"
    fi
    # The child shell expands its positional argument.
    # shellcheck disable=SC2016
    if manager_stage owned-drain 5 /bin/bash -c 'while [[ -d $1 ]] && ! grep -qx "populated 0" "$1/cgroup.events"; do sleep 0.1; done' bash "/sys/fs/cgroup$cg"; then drained=true; fi
   fi
   if [[ -d /sys/fs/cgroup$cg ]]; then cat "/sys/fs/cgroup$cg/cgroup.events" > "$manager_ev/cgroup.events.after"; else printf '%s\n' absent > "$manager_ev/cgroup.events.after"; fi
  fi
 fi
 if [[ -f $ev/native-exit.txt && ! -L $ev/native-exit.txt ]]; then
  native=$(cat "$ev/native-exit.txt")
  [[ $native =~ ^[0-9]{1,3}$ ]] || native=null
 fi
 resource_ok=true
 for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
  cmp "$ev/$f.before" "$ev/$f.after" >> "$manager_ev/resource-check.log" 2>&1 || resource_ok=false
 done
 [[ $(cat "$ev/cpu.max.before" 2>/dev/null) == '800000 100000' ]] || resource_ok=false
 [[ $(cat "$ev/cpuset.cpus.effective.before" 2>/dev/null) == 0-7 ]] || resource_ok=false
 [[ $(cat "$ev/memory.max.before" 2>/dev/null) == 30064771072 ]] || resource_ok=false
 [[ $(cat "$ev/memory.swap.max.before" 2>/dev/null) == 0 ]] || resource_ok=false
 [[ $(cat "$ev/pids.max.before" 2>/dev/null) == 512 ]] || resource_ok=false
 grep -Fxq 'oom 0' "$ev/memory.events.after" || resource_ok=false
 grep -Fxq 'oom_kill 0' "$ev/memory.events.after" || resource_ok=false
 [[ $(cat "$ev/memory.swap.peak.after" 2>/dev/null) == 0 ]] || resource_ok=false
 resources=$resource_ok
 printf '%s\n' "$manager" > "$manager_ev/manager-exit.txt"
 printf '%s\n' "$native" > "$manager_ev/native-exit.txt"
 printf '%s\n' "$owned" > "$manager_ev/owned.txt"
 printf '%s\n' "$drained" > "$manager_ev/drained.txt"
 printf '%s\n' "$forced_cleanup" > "$manager_ev/forced-cleanup.txt"
 if [[ $manager != 0 || $native != 0 || $owned != true || $drained != true || $forced_cleanup != false || $unit_status != true || $resources != true ]]; then
  (( status != 0 )) || status=96
 fi
 archive=$root/qualification-evidence.tar.gz
 [[ ! -e $archive && ! -L $archive ]] || exit 98
 manager_stage archive 60 tar -czf "$archive" -C "$root" evidence source-files.sha256 native-source.json required-exact-tests.txt qualify-gc-session.sh supervise-gc-session.sh
 put_rc=$?
 if (( put_rc != 0 )); then exit "$put_rc"; fi
 manager_stage archive-sha 10 sha256sum "$archive"
 put_rc=$?
 if (( put_rc != 0 )); then exit "$put_rc"; fi
 cp "$manager_ev/archive-sha.log" "$manager_ev/archive.sha256" || exit 94
 archive_sha=$(cut -d ' ' -f 1 "$manager_ev/archive.sha256")
 jq -n --arg source "$revision" --arg inner "$inner_sha" --arg archive "$archive_sha" --arg prefix "$prefix/terminal/" \
  --argjson supervisor "$status" --argjson manager "$manager" --argjson native "$native" \
  --argjson owned "$owned" --argjson drained "$drained" --argjson forced "$forced_cleanup" \
  --argjson unit_status "$unit_status" --argjson resources "$resources" '
  {schema:"borsuk-gc-session-qualification-v1",source_commit:$source,inner_sha256:$inner,
   source_files:478,native_source_files:445,expected_gc_inventory:24,terminal_prefix:$prefix,
   evidence_archive_sha256:$archive,supervisor_exit:$supervisor,manager_exit:$manager,native_exit:$native,
   unit_owned:$owned,owned_cgroup_drained:$drained,forced_cleanup:$forced,unit_status_verified:$unit_status,
   resource_envelope_verified:$resources,native_qualified:($supervisor==0 and $manager==0 and $native==0
    and $owned and $drained and ($forced|not) and $unit_status and $resources),
   ignored_s3_smoke:"native_s3_conditional_fence_and_gc_smoke",real_s3_gc_qualified:false,
   production_benchmark_binary:false,scientific_performance_claim:false,compute_closure_pending:true}
 ' > "$manager_ev/terminal.json" || exit 94
 export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
 publication_deadline=$(( $(date +%s) + 120 ))
 (( publication_deadline <= external_deadline )) || publication_deadline=$external_deadline
 # Create-only archive and digest must succeed before publishing terminal LAST.
 for f in "$archive" "$manager_ev/archive.sha256" "$manager_ev/terminal.json"; do
  remaining=$(( publication_deadline - $(date +%s) - 2 ))
  (( remaining > 0 )) || exit 124
  (( remaining <= 90 )) || remaining=90
  printf '%q ' timeout -k 2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/terminal/${f##*/}" --body "$f" --if-none-match '*' > "$manager_ev/${f##*/}.put-command.txt"
  printf '\n' >> "$manager_ev/${f##*/}.put-command.txt"
  date -u +%FT%TZ > "$manager_ev/${f##*/}.put-started.txt"
  /usr/bin/time -v -o "$manager_ev/${f##*/}.put-time.txt" timeout -k 2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/terminal/${f##*/}" --body "$f" --if-none-match '*' > "$manager_ev/${f##*/}.put.json" 2> "$manager_ev/${f##*/}.put.stderr"
  put_rc=$?
  printf '%s\n' "$put_rc" > "$manager_ev/${f##*/}.put-exit.txt"
  date -u +%FT%TZ > "$manager_ev/${f##*/}.put-finished.txt"
  (( put_rc == 0 )) || exit "$put_rc"
 done
 cat "$manager_ev/terminal.json"
 exit "$status"
}
trap finish EXIT
(( native_deadline > $(date +%s) + 31 )) || exit 124
[[ -f $root/qualify-gc-session.sh && ! -L $root/qualify-gc-session.sh ]]
printf '%s  %s\n' "$inner_sha" "$root/qualify-gc-session.sh" > "$manager_ev/inner.sha256"
manager_stage inner-sha 10 sha256sum --strict --check "$manager_ev/inner.sha256"
manager_stage supervisor-sha 10 sha256sum "$0"
manager_stage load-before 10 systemctl show "$unit" -p LoadState --value && load_rc=0 || load_rc=$?
[[ ( $load_rc == 0 || $load_rc == 1 ) && $(cat "$manager_ev/load-before.log") == not-found ]] || exit 98
left=$(( native_deadline - $(date +%s) + 30 ))
(( left > 30 )) || exit 124
# Capture status while the service still exists; successful transient units may unload.
stop_hook="/bin/sh -c 'exec /usr/bin/systemctl show $unit -p Description -p InvocationID -p Result -p ExecMainCode -p ExecMainStatus -p ControlGroup > $manager_ev/unit-stop-status.txt'"
launched=true
manager_stage systemd-run "$((left + 15))" systemd-run --expand-environment=no --unit="$unit" --wait --pipe \
 -p Description="$owner" -p WorkingDirectory="$root/source" -p CPUQuota=800% -p CPUQuotaPeriodSec=100ms \
 -p AllowedCPUs=0-7 -p MemoryMax=30064771072 -p MemorySwapMax=0 -p TasksMax=512 \
 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$left" \
 -p ExecStopPost="$stop_hook" \
 --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" \
 --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
 /bin/bash "$root/qualify-gc-session.sh" "$root" "$revision" "$native_deadline" "$inventory" && manager=0 || manager=$?
exit "$manager"
