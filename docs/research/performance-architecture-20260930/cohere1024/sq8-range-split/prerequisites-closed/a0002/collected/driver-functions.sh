safe_path() { [[ $1 =~ ^/[A-Za-z0-9_./-]+$ && $1 != *'/../'* && $1 != *'/./'* && $1 != */.. && $1 != */. ]]; }
bounded() {
 local cap=$1 left
 shift
 left=$((machine_deadline - $(date +%s) - 2))
 (( left <= cap )) || left=$cap
 (( left > 0 )) || return 124
 timeout -k 2 "$left" "$@"
}
pin() {
 local path bytes sha actual
 IFS=$'\t' read -r path bytes sha < <(jq -er '[.path,.bytes,.sha256]|@tsv' <<< "$1")
 safe_path "$path"
 [[ $path != "$campaign"/* && -f $path && ! -L $path && $(realpath -e -- "$path") == "$path" ]]
 [[ $bytes =~ ^[1-9][0-9]*$ && $(stat -c %s -- "$path") == "$bytes" ]]
 [[ $sha =~ ^[0-9a-f]{64}$ && $sha != 0000000000000000000000000000000000000000000000000000000000000000 && $sha != 1111111111111111111111111111111111111111111111111111111111111111 ]]
 actual=$(bounded 15 sha256sum -- "$path")
 [[ ${actual%% *} == "$sha" ]]
}
verify_inputs() {
 local descriptor
 printf '%s  %s\n' "$manifest_sha" "$manifest" | bounded 10 sha256sum --strict --check
 while IFS= read -r descriptor; do pin "$descriptor"; done < "$campaign/input-pins.jsonl"
}
read_exit() {
 local value
 if [[ -f $1 && ! -L $1 ]]; then
  value=$(cat "$1")
  if [[ $value =~ ^[0-9]{1,3}$ ]] && (( value <= 255 )); then printf '%s' "$value"; return; fi
 fi
 printf null
}
exact_owner() {
 [[ $invocation =~ ^[0-9a-f]{32}$ ]] &&
 grep -Fxq "Description=$owner" "$1" &&
 grep -Fxq "InvocationID=$invocation" "$1" &&
 grep -Fxq "ControlGroup=$cg" "$1"
}
drain_empty() {
 [[ ! -d /sys/fs/cgroup$cg ]] || grep -Fxq 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"
}
settle() {
 local show_rc stop_rc=0 valid=true owned=false drained=false forced=false resources=true f live invocation='' native timer timed runner
 settled=true
 invocation=$(cat "$ev/invocation-id.txt" 2>/dev/null) || invocation=''
 bounded 3 systemctl show "$unit" -p LoadState -p Description -p InvocationID -p ControlGroup -p Result -p ExecMainCode -p ExecMainStatus > "$ev/systemd-after.txt" 2> "$ev/systemd-after.stderr" && show_rc=0 || show_rc=$?
 printf '%s\n' "$show_rc" > "$ev/show-exit.txt"
 if exact_owner "$ev/unit-stop-status.txt" && exact_owner "$ev/unit-start-status.txt"; then
  if exact_owner "$ev/systemd-after.txt" || { [[ $show_rc == 0 || $show_rc == 1 ]] && grep -Fxq 'LoadState=not-found' "$ev/systemd-after.txt"; }; then owned=true; fi
 elif exact_owner "$ev/unit-start-status.txt" && exact_owner "$ev/systemd-after.txt"; then
  owned=true
 fi
 if [[ $owned == true ]]; then
  if drain_empty; then drained=true; else
   forced=true
   # Recheck the same invocation immediately before control; never adopt a name alone.
   live=$ev/systemd-before-stop.txt
   if bounded 3 systemctl show "$unit" -p Description -p InvocationID -p ControlGroup > "$live" && exact_owner "$live"; then
    bounded 7 systemctl stop "$unit" > "$ev/stop.log" 2> "$ev/stop.stderr" && stop_rc=0 || stop_rc=$?
    printf '%s\n' "$stop_rc" > "$ev/stop-exit.txt"
    if drain_empty; then drained=true; fi
   fi
  fi
  if [[ -d /sys/fs/cgroup$cg ]]; then cat "/sys/fs/cgroup$cg/cgroup.events" > "$ev/cgroup.events.drained"; else printf 'absent\n' > "$ev/cgroup.events.drained"; fi
 fi
 for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
  cmp "$ev/$f.before" "$ev/$f.after" >> "$ev/resource-check.log" 2>&1 || resources=false
 done
 [[ $(cat "$ev/cpu.max.before" 2>/dev/null) == '100000 100000' ]] || resources=false
 [[ $(cat "$ev/cpuset.cpus.effective.before" 2>/dev/null) == 0 ]] || resources=false
 [[ $(cat "$ev/memory.max.before" 2>/dev/null) == 2147483648 ]] || resources=false
 [[ $(cat "$ev/memory.swap.max.before" 2>/dev/null) == 0 && $(cat "$ev/pids.max.before" 2>/dev/null) == 128 ]] || resources=false
 [[ $(cat "$ev/memory.swap.peak.after" 2>/dev/null) == 0 ]] || resources=false
 grep -Fxq 'oom 0' "$ev/memory.events.after" || resources=false
 grep -Fxq 'oom_kill 0' "$ev/memory.events.after" || resources=false
 exact_owner "$ev/unit-stop-status.txt" || valid=false
 grep -Fxq 'Result=success' "$ev/unit-stop-status.txt" || valid=false
 grep -Fxq 'ExecMainCode=1' "$ev/unit-stop-status.txt" || valid=false
 grep -Fxq 'ExecMainStatus=0' "$ev/unit-stop-status.txt" || valid=false
 [[ $(cat "$ev/service-result.txt" 2>/dev/null) == success ]] || valid=false
 native=$(read_exit "$ev/native-exit.txt")
 timer=$(read_exit "$ev/timeout-exit.txt")
 timed=$(read_exit "$ev/time-exit.txt")
 runner=$(read_exit "$ev/systemd-run-exit.txt")
 [[ $native == 0 && $timer == 0 && $timed == 0 && $runner == 0 && $(read_exit "$ev/manager-envelope-exit.txt") == 0 && $(read_exit "$ev/worker-exit.txt") == 0 && $(read_exit "$ev/binary-after-exit.txt") == 0 ]] || valid=false
 [[ $owned == true && $drained == true && $forced == false && $resources == true ]] || valid=false
 jq -n --arg label "$phase" --arg owner "$owner" --arg invocation "$invocation" --arg cgroup "$cg" \
  --argjson native "$native" --argjson timeout "$timer" --argjson time "$timed" --argjson manager "$runner" \
  --argjson valid "$valid" --argjson owned "$owned" --argjson drained "$drained" --argjson forced "$forced" --argjson resources "$resources" \
  '{schema:"borsuk-sq8-split-cell-execution-v1",scope:"LOCAL_ONLY",label:$label,owner:$owner,invocation_id:$invocation,cgroup:$cgroup,
    native_exit:$native,timeout_exit:$timeout,time_exit:$time,systemd_run_exit:$manager,
    valid:$valid,owned:$owned,drained:$drained,forced_cleanup:$forced,resources_verified:$resources}' > "$ev/execution.json"
 [[ $valid == true ]]
}
run_stage() {
 local label=$1 role=$2 left deadline binary binary_sha binary_bytes run_rc stop_hook load_rc now
 local -a launch
 shift 2
 phase=$label
 ev=$campaign/$label
 mkdir "$ev"
 verify_inputs > "$ev/input-sha-before.txt"
 binary=$(jq -r ".$role.path" "$manifest")
 binary_sha=$(jq -r ".$role.sha256" "$manifest")
 binary_bytes=$(jq -r ".$role.bytes" "$manifest")
 unit=borsuk-sq8-panel-$(cat /proc/sys/kernel/random/uuid)-$label.service
 cg=/system.slice/$unit
 owner=$unit:$manifest_sha
 printf '%s\n' "$owner" > "$ev/owner.txt"
 bounded 3 systemctl show "$unit" -p LoadState --value > "$ev/load-before.txt" && load_rc=0 || load_rc=$?
 [[ ( $load_rc == 0 || $load_rc == 1 ) && $(cat "$ev/load-before.txt") == not-found ]]
 now=$(date +%s)
 left=$((machine_deadline - now - 35))
 (( left <= 120 )) || left=120
 (( left > 5 )) || exit 124
 deadline=$((now + left))
 printf '%s\n' "$deadline" > "$ev/stage-deadline.epoch"
 stop_hook="/bin/bash $worker stop $ev $unit $owner"
 settled=false
 launch=(systemd-run --expand-environment=no --unit="$unit" --wait --pipe \
  -p Description="$owner" -p WorkingDirectory="$campaign" -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms \
  -p AllowedCPUs=0 -p MemoryMax=2147483648 -p MemorySwapMax=0 -p TasksMax=128 \
  -p KillMode=control-group -p TimeoutStopSec=5 -p LimitCORE=0 -p RuntimeMaxSec="$left" \
  -p ExecStopPost="$stop_hook" \
  --setenv=BORSUK_CPU_THREADS=1 --setenv=RAYON_NUM_THREADS=1 \
  /bin/bash "$worker" run "$ev" "$unit" "$owner" "$deadline" "$binary" "$binary_sha" "$binary_bytes" "$@")
 printf '%q ' "${launch[@]}" > "$ev/systemd-run-command.txt"
 printf '\n' >> "$ev/systemd-run-command.txt"
 # The child records the actual systemd-run status before the outer watchdog returns.
 # shellcheck disable=SC2016
 /usr/bin/time -v -o "$ev/systemd-run-time.txt" timeout -k 2 "$((left + 10))" \
  /bin/bash -c 'set -C; ev=$1; shift; "$@"; rc=$?; printf "%s\n" "$rc" > "$ev/systemd-run-exit.txt"; exit "$rc"' \
  bash "$ev" "${launch[@]}" > "$ev/stdout.log" 2> "$ev/stderr.log" && run_rc=0 || run_rc=$?
 printf '%s\n' "$run_rc" > "$ev/manager-envelope-exit.txt"
 settle || exit 96
 verify_inputs > "$ev/input-sha-after.txt"
}
