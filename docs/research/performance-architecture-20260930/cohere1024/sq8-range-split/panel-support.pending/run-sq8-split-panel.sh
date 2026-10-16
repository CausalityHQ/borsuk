#!/bin/bash
# UNVERIFIED, source-only exact experiment glue. Root owns all admission and cloud work.
# API: bash run-sq8-split-panel.sh NEW_CAMPAIGN INPUT_MANIFEST EXTERNAL_MANIFEST_SHA
# Input contract: /data/orchestration/borsuk-sq8-split-panel-execution-contract.json
set -euo pipefail
set -C
umask 077
export PATH=/usr/sbin:/usr/bin:/sbin:/bin LC_ALL=C
[[ $# == 3 ]] || exit 98
campaign=$1
manifest=$2
manifest_sha=$3
safe_path() { [[ $1 =~ ^/[A-Za-z0-9_./-]+$ && $1 != *'/../'* && $1 != *'/./'* && $1 != */.. && $1 != */. ]]; }
safe_path "$campaign"
safe_path "$manifest"
[[ $manifest_sha =~ ^[0-9a-f]{64}$ && $manifest_sha != 0000000000000000000000000000000000000000000000000000000000000000 && $manifest_sha != 1111111111111111111111111111111111111111111111111111111111111111 ]]
[[ ! -e $campaign && ! -L $campaign && $(realpath -m -- "$campaign") == "$campaign" ]]
[[ -f $manifest && ! -L $manifest && $(realpath -e -- "$manifest") == "$manifest" ]]
[[ $(stat -c %s -- "$manifest") -le 65536 ]]
mkdir -- "$campaign"
phase=preflight
unit=''
settled=true
completed=0
machine_deadline=0
worker=''

# Every deadline includes kill grace. Neither zero-timeout nor renewed budgets occur.
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
# Invoked by EXIT, including admission, signal and native failures.
# shellcheck disable=SC2329
finish() {
 local original=$? status
 trap - EXIT
 set +e
 status=$original
 if [[ $settled == false ]]; then settle || { (( status != 0 )) || status=96; }; fi
 printf '%s\n' "$original" > "$campaign/original-exit.txt"
 jq -n --arg phase "$phase" --arg sha "$manifest_sha" --argjson original "$original" --argjson exit "$status" --argjson cells "$completed" '
  {schema:"borsuk-sq8-split-local-completion-v1",scope:"LOCAL_ONLY",status:(if $exit==0 then "LOCAL_EXECUTION_COMPLETE" else "INVALID" end),
   input_manifest_sha256:$sha,final_stage:$phase,original_exit:$original,exit:$exit,completed_cells:$cells,
   full_qualification:false,cloud_publication:false,compute_closed:false,scientific_performance_claim:false,
   root_external_gates_required:true,original_native_results_preserved:true}' > "$campaign/terminal.json" || status=94
 exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

printf '%s  %s\n' "$manifest_sha" "$manifest" | timeout -k 2 5 sha256sum --strict --check > "$campaign/manifest-pin.txt"
jq -se 'length==1' "$manifest" > "$campaign/manifest-single-object.txt"
# The external manifest pin is the root attestation; no observed result supplies expectations.
jq -e --arg campaign "$campaign" '
 def keys_are($s): keys == ($s|split(" ")|sort);
 def pin: type=="object" and keys_are("path bytes sha256") and (.path|type=="string") and
  (.bytes|type=="number" and .>0 and floor==.) and (.sha256|type=="string" and test("^[0-9a-f]{64}$"));
 keys_are("schema campaign_dir source_commit machine_started_epoch shared_deadline_epoch admission_marker driver worker producer reducer authority receipts cells") and
 .schema=="borsuk-sq8-split-execution-input-v1" and .campaign_dir==$campaign and
 (.source_commit|test("^[0-9a-f]{40}$") and .!="0000000000000000000000000000000000000000" and .!="1111111111111111111111111111111111111111") and
 (.machine_started_epoch|type=="number" and floor==. and .>=1000000000 and .<=9999999999) and
 (.shared_deadline_epoch|type=="number" and floor==. and .>=1000000000 and .<=9999999999) and
 ([.admission_marker,.driver,.worker,.producer,.reducer]|all(pin)) and
 (.authority|keys_are("source protocol cohort") and all(.[];pin)) and
 (.receipts|keys_are("native_qualification real_input_admission smoke disabled_historical_parity builder_removal_mutation scientific_freeze") and all(.[];pin)) and
 (.cells|length==6 and map(.label)==["B0","C0","B1","C1","C2","B2"] and
  all(.[];keys_are("label runtime_config expected_headers") and (.runtime_config|pin) and (.expected_headers|pin)) and
  ([.[].runtime_config.path]|unique|length)==6 and ([.[].expected_headers.path]|unique|length)==6) and
 ([..|strings]|all(test("(?i)(TODO|TBD|PLACEHOLDER|NOT_RUN|UNVERIFIED|<[^>]*>)")|not))
 ' "$manifest" > "$campaign/manifest-schema.txt"
start=$(jq -r .machine_started_epoch "$manifest")
shared=$(jq -r .shared_deadline_epoch "$manifest")
now=$(date +%s)
(( start <= now && shared > now && shared <= start + 3720 ))
machine_deadline=$((start + 3600))
(( machine_deadline <= shared - 120 )) || machine_deadline=$((shared - 120))
(( machine_deadline > now + 30 )) || exit 124
printf '%s\n' "$machine_deadline" > "$campaign/machine-deadline.epoch"
cat "$manifest" > "$campaign/input-manifest.json"
jq -c '.admission_marker,.driver,.worker,.producer,.reducer,.authority[],.receipts[],(.cells[]|.runtime_config,.expected_headers)' "$manifest" > "$campaign/input-pins.jsonl"
verify_inputs > "$campaign/input-sha-before.txt"
[[ $(jq -r .driver.path "$manifest") == "$(realpath -e -- "$0")" ]]
worker=$(jq -r .worker.path "$manifest")
[[ $worker == "$(dirname -- "$(realpath -e -- "$0")")/panel-stage.sh" ]]
source=$(jq -r .source_commit "$manifest")
[[ $(jq -r .admission_marker.bytes "$manifest") == 41 ]]
[[ $(cat "$(jq -r .admission_marker.path "$manifest")") == "$source" ]]
for role in producer reducer; do
 binary=$(jq -r ".$role.path" "$manifest")
 [[ -x $binary ]]
 bounded 5 readelf -h "$binary" > "$campaign/$role-elf.txt"
done

mkdir "$campaign/frozen"
for slot in 0 1 2 3 4 5; do
 label=$(jq -r ".cells[$slot].label" "$manifest")
 config=$(jq -r ".cells[$slot].runtime_config.path" "$manifest")
 headers=$(jq -r ".cells[$slot].expected_headers.path" "$manifest")
 [[ $(stat -c %s -- "$config") -le 32768 && $(stat -c %s -- "$headers") -le 131072 ]]
 cat "$config" > "$campaign/frozen/$label.config.json"
 cat "$headers" > "$campaign/frozen/$label.headers.json"
 # Metadata assertions only; all scientific validation remains native.
 jq -ne --slurpfile c "$config" --slurpfile h "$headers" --slurpfile m "$manifest" --argjson slot "$slot" '
  def keys_are($s): keys == ($s|split(" ")|sort);
  $c[0] as $r | $h[0].expected_identity as $i | $h[0].expected_bound_inputs as $b | $m[0] as $m |
  [0,32,64,96,128,161,193,225,257,290,322,354,386,418,451,483,515,547,580,612,644,676,708,741,773,805,837,870,902,934,966,999] as $ordinals |
  (if [1,3,4]|index($slot) then 2121728 else 33554432 end) as $cap |
  ($h|length==1) and ($c|length==1) and ($h[0]|keys_are("expected_identity expected_bound_inputs")) and
  ($i|keys_are("schema phase config_sha256 fetch_parallelism source_get_budget sq8_get_budget sq8_byte_budget serving binary_sha256 runner_source_sha256 generation_source_sha256 router_source_sha256 codec_source_sha256 source_plane_source_sha256 scope physical_s3_measured io_measurement s3_credential_source native_transport_includes wire_bytes unread_bytes billed_bytes billed_requests external_gate_required truth_opened sq8_range_split execution sq8_range_source_sha256 returned_source_sha256")) and
  ($b|keys_are("phase dataset revision fetch_parallelism source_get_budget source_cache serving sq8_get_budget sq8_byte_budget metric tie_rule rows dimensions count k corpus_source_first query_source_first profile backend generation_prefix credential_source generation_root_sha256 requests_bytes requests_sha256 truth_bytes truth_sha256 native_source_sha256 native_sq8_sha256 native_order_sha256 truth_opened corpus_intervals reserved_query_interval reserved_queries_sha256 cohort_receipt_sha256 derivation_receipt_sha256 producer_authority max_memory_bytes sq8_range_split execution selected_count")) and
  ($r|keys_are("schema dataset revision metric tie_rule corpus_intervals reserved_query_interval cohort_receipt derivation_receipt producer_authority corpus_source_first query_source_first rows dimensions count k profile backend generation_prefix generation_root_sha256 scratch_parent requests truth native_source max_memory_bytes sq8_get_budget sq8_byte_budget fetch_parallelism source_get_budget sq8_range_split serving execution")) and
  $r.schema=="borsuk-cohere-native-baseline-config-v10" and $i.schema=="borsuk-cohere-native-baseline-result-v10" and
  $i.phase=="identity" and $b.phase=="bound_inputs" and $i.binary_sha256==$m.producer.sha256 and $i.config_sha256==$m.cells[$slot].runtime_config.sha256 and
  $r.rows==1000000 and $r.dimensions==1024 and $r.count==1000 and $r.k==10 and $r.profile=="scale1m" and
  $r.source_get_budget==128 and $r.sq8_get_budget==30 and $r.sq8_byte_budget==33554432 and $r.fetch_parallelism==64 and
  $r.max_memory_bytes==536870912 and $r.serving=={mode:"baseline"} and
  $r.execution=={mode:"diagnostic_panel",ordinals:$ordinals,trace:($slot>=2)} and
  $r.sq8_range_split=={max_physical_range_bytes:$cap,max_physical_gets:46} and
  ($r.backend|keys_are("kind bucket region physical_prefix sq8_object_key sq8_etag")) and $r.backend.kind=="s3" and
  $b.selected_count==32 and $b.source_cache=="off" and $b.truth_opened==false and
  $r.cohort_receipt==$m.authority.cohort and
  (["dataset","revision","metric","tie_rule","corpus_intervals","reserved_query_interval","producer_authority","corpus_source_first","query_source_first","rows","dimensions","count","k","profile","backend","generation_prefix","generation_root_sha256","max_memory_bytes","sq8_get_budget","sq8_byte_budget","fetch_parallelism","source_get_budget","sq8_range_split","serving","execution"]|all(.[]; . as $k | $r[$k]==$b[$k])) and
  (["fetch_parallelism","source_get_budget","sq8_get_budget","sq8_byte_budget","sq8_range_split","serving","execution"]|all(.[]; . as $k | $r[$k]==$i[$k])) and
  (["requests","truth","cohort_receipt","derivation_receipt"]|all(.[];. as $k | ($r[$k]|keys_are("path bytes sha256")) and $r[$k].sha256==$b[$k+"_sha256"])) and
  $r.requests.bytes==$b.requests_bytes and $r.truth.bytes==$b.truth_bytes and
  ($r.native_source|keys_are("source_sha256 sq8_sha256 source_order_sha256")) and
  $r.native_source.source_sha256==$b.native_source_sha256 and $r.native_source.sq8_sha256==$b.native_sq8_sha256 and $r.native_source.source_order_sha256==$b.native_order_sha256 and
  ([$r,$h[0]]|[..|objects|to_entries[]|select(.key|endswith("sha256"))|.value]|
   all(type=="string" and test("^[0-9a-f]{64}$") and .!="0000000000000000000000000000000000000000000000000000000000000000" and .!="1111111111111111111111111111111111111111111111111111111111111111")) and
  ([$r,$h[0]]|[..|strings]|all(test("(?i)(TODO|TBD|PLACEHOLDER|NOT_RUN|UNVERIFIED|<[^>]*>)")|not))
 ' > "$campaign/frozen/$label.admitted.txt"
 cmp "$config" "$campaign/frozen/$label.config.json"
 cmp "$headers" "$campaign/frozen/$label.headers.json"
 scratch=$(jq -er .scratch_parent "$config")
 safe_path "$scratch"
 [[ -d $scratch && ! -L $scratch && $(realpath -e -- "$scratch") == "$scratch" ]]
 jq -c '.requests,.truth,.cohort_receipt,.derivation_receipt' "$config" >> "$campaign/input-pins.jsonl"
done
jq -se '
 def neutral: del(.sq8_range_split) | .execution.trace=false;
 .[0] as $b | all(.[];(neutral)==($b|neutral))
' "$campaign"/frozen/*.config.json > "$campaign/common-configs.txt"
jq -se '
 def neutral: del(.sq8_range_split,.config_sha256) | .execution.trace=false;
 .[0] as $b | all(.[];(.expected_identity|neutral)==($b.expected_identity|neutral) and
  (.expected_bound_inputs|neutral)==($b.expected_bound_inputs|neutral))
' "$campaign"/frozen/*.headers.json > "$campaign/common-headers.txt"
verify_inputs > "$campaign/all-input-sha-before.txt"

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
for slot in 0 1 2 3 4 5; do
 label=$(jq -r ".cells[$slot].label" "$manifest")
 config=$(jq -r ".cells[$slot].runtime_config.path" "$manifest")
 config_sha=$(jq -r ".cells[$slot].runtime_config.sha256" "$manifest")
 [[ ! -e $campaign/$label.jsonl && ! -L $campaign/$label.jsonl ]]
 run_stage "$label" producer "$config" "$config_sha" "$campaign/$label.jsonl"
 completed=$((completed + 1))
done

# Only after ALL six terminal successes: hash opaque original JSONL, never read headers.
phase=prepare-reduction
for slot in 0 1 2 3 4 5; do
 label=$(jq -r ".cells[$slot].label" "$manifest")
 headers=$(jq -r ".cells[$slot].expected_headers.path" "$manifest")
 cmp "$headers" "$campaign/frozen/$label.headers.json"
 result=$campaign/$label.jsonl
 [[ -f $result && ! -L $result ]]
 bytes=$(stat -c %s -- "$result")
 (( bytes > 0 && bytes <= 67108864 ))
 digest=$(bounded 10 sha256sum -- "$result")
 digest=${digest%% *}
 printf '%s  %s\n' "$digest" "$result" >> "$campaign/results.sha256"
 jq -n --arg path "$result" --arg sha "$digest" --argjson bytes "$bytes" --argjson slot "$slot" \
  --slurpfile m "$manifest" --slurpfile h "$campaign/frozen/$label.headers.json" '
  $m[0].cells[$slot] as $c | {label:$c.label,runtime_config:$c.runtime_config,
   completed:{schema:"borsuk-completed-native-reduction-config-v4",input:{path:$path,sha256:$sha,bytes:$bytes},
    expected_identity:$h[0].expected_identity,expected_bound_inputs:$h[0].expected_bound_inputs}}
 ' > "$campaign/$label.descriptor.json"
done
jq -s '{schema:"borsuk-sq8-split-panel-config-v2",runs:.}' \
 "$campaign/B0.descriptor.json" "$campaign/C0.descriptor.json" "$campaign/B1.descriptor.json" \
 "$campaign/C1.descriptor.json" "$campaign/C2.descriptor.json" "$campaign/B2.descriptor.json" > "$campaign/reducer-config.json"
[[ $(stat -c %s -- "$campaign/reducer-config.json") -le 262144 ]]
digest=$(bounded 10 sha256sum -- "$campaign/reducer-config.json")
printf '%s\n' "$digest" > "$campaign/reducer-config.sha256"
run_stage reducer reducer --sq8-split-panel "$campaign/reducer-config.json" "${digest%% *}" "$campaign/report.json"
bounded 15 sha256sum --strict --check "$campaign/results.sha256" "$campaign/reducer-config.sha256" > "$campaign/reduction-input-sha-after.txt"
jq -e '.schema=="borsuk-sq8-split-panel-v2" and .status=="VALIDATED_FIXED_PANEL" and .complete==true' "$campaign/report.json" > "$campaign/report-complete.txt"
phase=local-only-complete
