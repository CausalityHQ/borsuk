#!/bin/bash
# UNVERIFIED source-only supervisor; infrastructure/cost authority stays with root.
set -euo pipefail
[[ $# == 5 ]] || exit 98
root=$1
revision=$2
inner_sha=$3
input_map_sha=$4
prefix=$5
[[ $root == /mnt/borsuk-paired-qualification && $revision =~ ^[0-9a-f]{40}$ && $inner_sha =~ ^[0-9a-f]{64}$ && $input_map_sha =~ ^[0-9a-f]{64}$ ]] || exit 98
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/sq8-split-[a-z0-9-]+$ ]] || exit 98
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
ev=$root/evidence/sq8-split
manager_ev=$root/evidence/sq8-split-manager
mkdir "$manager_ev"
unit=borsuk-sq8-split-qualification.service
cg=/system.slice/$unit
owner="$unit:$revision:$inner_sha:$(cat /proc/sys/kernel/random/uuid)"
printf '%s\n' "$owner" > "$manager_ev/owner.txt"
printf '%s\n' "$external_deadline" > "$manager_ev/external-deadline.epoch"
printf '%s\n' "$machine_deadline" > "$manager_ev/machine-deadline.epoch"
printf '%s\n' "$native_deadline" > "$manager_ev/native-deadline.epoch"
printf '%s\n' 'native_seconds=4200 machine_seconds=5400 external_seconds=5520 prospective_instance=m6a.4xlarge root_launch_authority_required=true' > "$manager_ev/root-envelope.txt"
manager=null
native=null
owned=false
drained=false
forced_cleanup=false
unit_status=false
resources=false
launched=false
current_stage=preflight
required_roster=null
required_roster_sha=
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
# All objects share the finish deadline; retain upload receipts before archiving.
# shellcheck disable=SC2329
publish_artifact() {
 local file=$1 key=$2 label=${1##*/} remaining put_rc
 remaining=$(( publication_deadline - $(date +%s) - 2 ))
 (( remaining > 0 )) || return 124
 (( remaining <= 90 )) || remaining=90
 printf '%q ' timeout -k 2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/terminal/$key" --body "$file" --if-none-match '*' > "$manager_ev/$label.put-command.txt" || return 94
 printf '\n' >> "$manager_ev/$label.put-command.txt" || return 94
 date -u +%FT%TZ > "$manager_ev/$label.put-started.txt" || return 94
 /usr/bin/time -v -o "$manager_ev/$label.put-time.txt" timeout -k 2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/terminal/$key" --body "$file" --if-none-match '*' > "$manager_ev/$label.put.json" 2> "$manager_ev/$label.put.stderr" && put_rc=0 || put_rc=$?
 printf '%s\n' "$put_rc" > "$manager_ev/$label.put-exit.txt" || return 94
 date -u +%FT%TZ > "$manager_ev/$label.put-finished.txt" || return 94
 (( put_rc == 0 )) || return "$put_rc"
 [[ -f $manager_ev/$label.put-time.txt && -s $manager_ev/$label.put.json && $(stat -c %s "$manager_ev/$label.put.json") -le 65536 ]] || return 94
 jq -e 'type == "object" and (.ETag | type == "string" and length > 0)' "$manager_ev/$label.put.json" > /dev/null || return 94
}
# Invoked by the EXIT handler.
# shellcheck disable=SC2329
publish_binary() {
 local name=$1 file=$ev/binaries/$1 source_path=target/release/$1 size
 [[ $name != compare_native_replay ]] || source_path=target/release/examples/$name
 [[ -f $file && ! -L $file && -x $file && $(realpath -e -- "$file") == "$file" ]] || return 98
 size=$(stat -c %s "$file") || return 94
 binary_size=$size
 [[ $binary_size =~ ^[1-9][0-9]{0,7}$ ]] && (( binary_size <= 67108864 )) || return 98
 manager_stage "binary-$name-elf" 10 readelf -h "$file" || return $?
 manager_stage "binary-$name-sha" 10 sha256sum "$file" || return $?
 binary_sha=$(cut -d ' ' -f 1 "$manager_ev/binary-$name-sha.log")
 [[ $binary_sha =~ ^[0-9a-f]{64}$ && -f $ev/production-$name-sha.log && ! -L $ev/production-$name-sha.log && $(stat -c %s "$ev/production-$name-sha.log") -le 1024 ]] || return 98
 [[ $(cat "$ev/production-$name-sha.log") == "$binary_sha  $source_path" ]] || return 98
 publish_artifact "$file" "binaries/$name" && binary_upload_rc=0 || binary_upload_rc=$?
 (( binary_upload_rc == 0 )) || return "$binary_upload_rc"
 manager_stage "binary-$name-sha-after" 10 sha256sum --strict --check "$manager_ev/binary-$name-sha.log" || return $?
 binary_verified=true
}
# Invoked by the EXIT trap.
# shellcheck disable=SC2329
finish() {
 local original=$? show_rc stop_rc status f resource_ok archive archive_sha publication_deadline put_rc invocation
 local name binary_size binary_sha binary_upload_rc binary_verified binaries='[]' binaries_verified=false
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
 [[ $(cat "$ev/cpu.max.before" 2>/dev/null) == '1600000 100000' ]] || resource_ok=false
 [[ $(cat "$ev/cpuset.cpus.effective.before" 2>/dev/null) == 0-15 ]] || resource_ok=false
 [[ $(cat "$ev/memory.max.before" 2>/dev/null) == 60129542144 ]] || resource_ok=false
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
 manager_stage admission-input-sha-after 120 sha256sum --strict --check "$manager_ev/input-map.sha256" "$root/admission-inputs.sha256" "$manager_ev/inner.sha256" "$manager_ev/supervisor-sha.log"
 put_rc=$?
 (( put_rc == 0 || status != 0 )) || status=$put_rc
 export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
 publication_deadline=$(( $(date +%s) + 120 ))
 (( publication_deadline <= external_deadline )) || publication_deadline=$external_deadline
 for name in check_cohere_native_baseline compare_native_replay; do
  binary_size=null binary_sha='' binary_upload_rc=null binary_verified=false
  if [[ $native == 0 ]]; then
   publish_binary "$name"
   put_rc=$?
   (( put_rc == 0 || status != 0 )) || status=$put_rc
  fi
  binaries=$(jq -cn --argjson entries "$binaries" --arg name "$name" --arg key "$prefix/terminal/binaries/$name" \
   --argjson size "$binary_size" --arg sha "$binary_sha" --argjson upload "$binary_upload_rc" --argjson verified "$binary_verified" '
   $entries + [{name:$name,key:$key,size_bytes:$size,sha256:(if $sha == "" then null else $sha end),
    upload_exit:$upload,upload_receipt:("evidence/sq8-split-manager/"+$name+".put.json"),verified:$verified}]
  ') || { (( status != 0 )) || status=94; exit "$status"; }
 done
 jq -n --argjson binaries "$binaries" '{schema:"borsuk-qualification-binaries-v1",binaries:$binaries}' > "$manager_ev/binary-roster.json" || { (( status != 0 )) || status=94; exit "$status"; }
 if jq -e '.binaries | length == 2 and all(.verified == true)' "$manager_ev/binary-roster.json" > /dev/null; then binaries_verified=true; fi
 [[ $native != 0 || $binaries_verified == true || $status != 0 ]] || status=96
 archive=$root/qualification-evidence.tar.gz
 [[ ! -e $archive && ! -L $archive ]] || { (( status != 0 )) || status=98; exit "$status"; }
 manager_stage archive 60 tar -czf "$archive" \
  --anchored --no-wildcards --exclude=evidence/sq8-split/binaries/check_cohere_native_baseline \
  --exclude=evidence/sq8-split/binaries/compare_native_replay \
  -C "$root" evidence QUALIFICATION_ADMITTED admission-inputs.sha256 source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-split.sh supervise-sq8-split.sh
 put_rc=$?
 if (( put_rc != 0 )); then (( status != 0 )) || status=$put_rc; exit "$status"; fi
 manager_stage archive-sha 10 sha256sum "$archive"
 put_rc=$?
 if (( put_rc != 0 )); then (( status != 0 )) || status=$put_rc; exit "$status"; fi
 cp "$manager_ev/archive-sha.log" "$manager_ev/archive.sha256" || { (( status != 0 )) || status=94; exit "$status"; }
 archive_sha=$(cut -d ' ' -f 1 "$manager_ev/archive.sha256")
 jq -n --arg source "$revision" --arg inner "$inner_sha" --arg input_map "$input_map_sha" --arg archive "$archive_sha" --arg prefix "$prefix/terminal/" \
  --arg roster_sha "$required_roster_sha" --argjson roster "$required_roster" --slurpfile binaries "$manager_ev/binary-roster.json" --argjson binaries_verified "$binaries_verified" \
  --argjson supervisor "$status" --argjson manager "$manager" --argjson native "$native" \
  --argjson owned "$owned" --argjson drained "$drained" --argjson forced "$forced_cleanup" \
  --argjson unit_status "$unit_status" --argjson resources "$resources" '
  {schema:"borsuk-sq8-split-qualification-v2",source_commit:$source,inner_sha256:$inner,
   input_map_sha256:$input_map,required_exact_tests_sha256:$roster_sha,required_exact_tests:$roster.total,
   required_split_tests:$roster.split,required_reader_tests:$roster.reader,required_other_tests:$roster.other,
   required_test_scope:$roster.scope,profiles:["debug","release"],terminal_prefix:$prefix,
   binary_roster:$binaries[0],binaries_verified:$binaries_verified,
   evidence_archive_sha256:$archive,supervisor_exit:$supervisor,manager_exit:$manager,native_exit:$native,
   unit_owned:$owned,owned_cgroup_drained:$drained,forced_cleanup:$forced,unit_status_verified:$unit_status,
   resource_envelope_verified:$resources,native_qualified:($supervisor==0 and $manager==0 and $native==0
    and $owned and $drained and ($forced|not) and $unit_status and $resources and $binaries_verified and $roster != null),
   builder_removal_mutation_falsifier:"EXTERNAL NOT_RUN",full_qualification:false,
   scientific_performance_claim:false,compute_closure_pending:true}
 ' > "$manager_ev/terminal.json" || { (( status != 0 )) || status=94; exit "$status"; }
 # Create-only archive and digest must succeed before publishing terminal LAST.
 for f in "$archive" "$manager_ev/archive.sha256" "$manager_ev/terminal.json"; do
  publish_artifact "$f" "${f##*/}"
  put_rc=$?
  (( put_rc == 0 )) || { (( status != 0 )) || status=$put_rc; exit "$status"; }
 done
 cat "$manager_ev/terminal.json"
 exit "$status"
}
trap finish EXIT
(( native_deadline > $(date +%s) + 131 )) || exit 124
[[ $(realpath -e -- "$0") == "$root/supervise-sq8-split.sh" ]]
[[ -f $root/admission-inputs.sha256 && ! -L $root/admission-inputs.sha256 ]]
printf '%s  %s\n' "$input_map_sha" "$root/admission-inputs.sha256" > "$manager_ev/input-map.sha256"
manager_stage input-map-sha 10 sha256sum --strict --check "$manager_ev/input-map.sha256"
map_files=(QUALIFICATION_ADMITTED source.tar.gz source-files.sha256 native-source.json required-exact-tests.txt qualify-sq8-split.sh supervise-sq8-split.sh)
[[ $(wc -l < "$root/admission-inputs.sha256") == "${#map_files[@]}" ]]
for f in "${map_files[@]}"; do
 [[ -f $root/$f && ! -L $root/$f ]]
 [[ $(grep -Ec "^[0-9a-f]{64}  $root/${f//./\\.}$" "$root/admission-inputs.sha256") == 1 ]]
done
[[ $(stat -c %s "$root/required-exact-tests.txt") -le 65536 ]]
manager_stage admission-input-sha 120 sha256sum --strict --check "$root/admission-inputs.sha256"
# Hash the bounded snapshot and require its exact bytes to match the admitted map.
manager_stage required-roster-read 10 head -c 65537 -- "$root/required-exact-tests.txt"
[[ $(stat -c %s "$manager_ev/required-roster-read.log") -le 65536 ]]
manager_stage required-roster-sha 10 sha256sum "$manager_ev/required-roster-read.log"
required_roster_sha=$(cut -d ' ' -f 1 "$manager_ev/required-roster-sha.log")
grep -Fxq "$required_roster_sha  $root/required-exact-tests.txt" "$root/admission-inputs.sha256"
# The jq program expands its own variables.
# shellcheck disable=SC2016
manager_stage required-roster-validate 10 jq -Rsc '
 split("\n") | (if .[-1] == "" then .[:-1] else . end) as $names
 | if ($names | length > 0) and ($names | all(length <= 512 and test("^[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)+$")))
      and (($names | unique | length) == ($names | length)) then $names else error("invalid exact harness roster") end
 | [ .[] | select(test("^(sq8_s3_range::tests::split_[A-Za-z0-9_]+|two_bit_generation::source_walk_tests::generation_sq8_split_admission_and_serving_parity|tests::(cold_sq8_split_config_version_caps_trace_and_identity|actual_sq8_split_config_reaches_serving_and_preserves_parity|sq8_split_multi_query_reservation_and_direct_memory_boundaries))$")) ] as $split
 | [ .[] | select(startswith("sq8_split_panel::tests::")) ] as $reader
 | {total:length,split:($split|length),reader:($reader|length),other:(length-($split|length)-($reader|length)),
    scope:{split:$split,reader:$reader,other:(. - $split - $reader)}}
' "$manager_ev/required-roster-read.log"
required_roster=$(cat "$manager_ev/required-roster-validate.log")
printf '%s  %s\n' "$inner_sha" "$root/qualify-sq8-split.sh" > "$manager_ev/inner.sha256"
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
 -p Description="$owner" -p WorkingDirectory="$root/source" -p CPUQuota=1600% -p CPUQuotaPeriodSec=100ms \
 -p AllowedCPUs=0-15 -p MemoryMax=60129542144 -p MemorySwapMax=0 -p TasksMax=512 \
 -p KillMode=control-group -p TimeoutStopSec=30 -p LimitCORE=0 -p RuntimeMaxSec="$left" \
 -p ExecStopPost="$stop_hook" \
 --setenv=RUSTUP_HOME="$root/.rustup" --setenv=CARGO_HOME="$root/.cargo" \
 --setenv=PATH="$root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
 /bin/bash "$root/qualify-sq8-split.sh" "$root" "$revision" "$native_deadline" "$input_map_sha" && manager=0 || manager=$?
exit "$manager"
