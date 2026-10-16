#!/bin/bash
# UNVERIFIED source-only glue; execute only on the prepared remote EC2 machine.
# API: bash run-split-prerequisites.sh ROOT MASTER_MANIFEST MASTER_SHA
# Contract: /data/orchestration/borsuk-split-prerequisite-runner-contract.json
# Root owns assets, launch, publication, budget and compute closure.
# Imported driver functions consume the shared stage globals below.
# shellcheck disable=SC2034
set -euo pipefail
set -C
umask 077
export PATH=/usr/sbin:/usr/bin:/sbin:/bin LC_ALL=C TZ=UTC
unset BASH_ENV ENV
[[ $# == 3 && $EUID == 0 && $(cat /sys/devices/virtual/dmi/id/sys_vendor) == 'Amazon EC2' ]] || exit 98
root=$1
manifest=$2
manifest_sha=$3
for path in "$root" "$manifest"; do
 [[ $path =~ ^/[A-Za-z0-9_./-]+$ && ! -L $path && $(realpath -e -- "$path") == "$path" ]]
done
[[ $root != / && -d $root && -f $manifest && $(stat -c %s -- "$manifest") -le 65536 && $manifest_sha =~ ^[0-9a-f]{64}$ ]]
out=$root/split-prerequisites.pending
[[ ! -e $out && ! -L $out ]]
mkdir -- "$out"
campaign=$out
phase=preflight
unit=''
ev=''
settled=true
completed=0
worker=''
machine_deadline=0
# shellcheck disable=SC2329
finish() {
 local original=$? status
 trap - EXIT
 set +e
 status=$original
 if [[ $settled == false ]]; then settle || { (( status != 0 )) || status=96; }; fi
 printf '%s\n' "$original" > "$out/original-exit.txt"
 jq -n --arg phase "$phase" --arg evidence "$ev" --arg sha "$manifest_sha" --argjson original "$original" --argjson exit "$status" --argjson completed "$completed" '
  {schema:"borsuk-split-prerequisites-local-v1",scope:"LOCAL_ONLY",
   status:(if $exit==0 then "LOCAL_EXECUTION_COMPLETE" else "INVALID" end),
   input_manifest_sha256:$sha,final_stage:$phase,evidence_dir:$evidence,original_exit:$original,exit:$exit,
   completed_unmeasured_phases:$completed,scientific_cells_executed:0,
   original_native_results_preserved:true,full_qualification:false,
   cloud_publication:false,compute_closed:false,root_external_gates_required:true,
   scientific_performance_claim:false}' > "$out/terminal.json" || status=94
 exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

# Parse only bounded metadata to recover the ORIGINAL budget before hashing inputs.
jq -se 'length==1 and (.[0]|type=="object")' "$manifest" > "$out/manifest-single.txt"
start=$(jq -er '.machine_started_epoch | select(type=="number" and floor==. and .>=1000000000 and .<=9999999999)' "$manifest")
shared=$(jq -er '.shared_deadline_epoch | select(type=="number" and floor==. and .>=1000000000 and .<=9999999999)' "$manifest")
now=$(date +%s)
(( start <= now && shared > now && shared <= start + 3720 ))
machine_deadline=$((start + 3600))
(( machine_deadline <= shared - 120 )) || machine_deadline=$((shared - 120))
(( machine_deadline > now + 30 )) || exit 124
printf '%s\n' "$machine_deadline" > "$out/machine-deadline.epoch"
printf '%s  %s\n' "$manifest_sha" "$manifest" | timeout -k 2 5 sha256sum --strict --check > "$out/manifest-pin.txt"
[[ $(jq -r .machine_started_epoch "$manifest") == "$start" && $(jq -r .shared_deadline_epoch "$manifest") == "$shared" ]]
jq -e --arg root "$root" --arg machine "$(cat /etc/machine-id)" '
 def keys_are($s): keys==($s|split(" ")|sort);
 def pin: type=="object" and keys_are("path bytes sha256") and (.path|type=="string") and
  (.bytes|type=="number" and .>0 and floor==.) and (.sha256|type=="string" and test("^[0-9a-f]{64}$"));
 keys_are("schema root source_commit remote_machine_id machine_started_epoch shared_deadline_epoch runner driver worker producer reducer shell parity fixture authority original_inputs historical_completed phases") and
 .schema=="borsuk-split-prerequisites-input-v1" and .root==$root and
 .source_commit=="63e63dfd2288fd7c8d125d1c1702c57ec47edae1" and
 (.remote_machine_id|test("^[0-9a-f]{32}$")) and .remote_machine_id==$machine and
 ([.runner,.driver,.worker,.producer,.reducer,.shell,.parity,.fixture,.historical_completed]|all(pin)) and
 .driver.sha256=="a65283d5291f660b99c8c96773054b373c0cbeb9a0dd227b3e95b13904f5f644" and .driver.bytes==21270 and
 .worker.sha256=="b046c34d6276c055ddac939ecc5dcb18cf5bc8d3ca71fff42daae5cb3ebab433" and .worker.bytes==3821 and
 .producer.sha256=="65ad16cd5aecc22731a9752ca7233226b11611d21c3a1468be30cc384c14ea33" and .producer.bytes==17629808 and
 .reducer.sha256=="4efc0af9855e1320a915053dcbbd7e242bc7f03a541460be3d73290ce66dc25f" and .reducer.bytes==13384312 and
 .parity.sha256=="f778257603cb046f9d279b5e1ce765830dbabab0e331b3a64255fdaee796a9b3" and .parity.bytes==14576 and
 .fixture.sha256=="b0da310c1d80b4fe5e7087f5c754be802374d9ac8063aed43e1739fbd35d4f52" and .fixture.bytes==5861 and
 .shell.path=="/usr/bin/bash" and
 (.authority|keys_are("source protocol cohort historical") and all(.[];pin)) and
 (.original_inputs|keys_are("requests truth cohort_receipt derivation_receipt") and all(.[];pin)) and
 .authority.cohort==.original_inputs.cohort_receipt and
 (.phases|type=="array" and length==2 and map(.label)==["C1","NULL_SMOKE"] and
  all(.[];keys_are("label runtime_config expected_headers") and (.runtime_config|pin) and (.expected_headers|pin) and
   .runtime_config.path==("/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending/"+.label+".runtime.json") and
   .expected_headers.path==("/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending/"+.label+".expected.json")))
 ' "$manifest" > "$out/manifest-schema.txt"
cat "$manifest" > "$out/input-manifest.json"
driver=$(jq -er .driver.path "$manifest")
[[ $driver =~ ^/[A-Za-z0-9_./-]+$ && -f $driver && ! -L $driver && $(realpath -e -- "$driver") == "$driver" && $(stat -c %s -- "$driver") == 21270 ]]
cat "$driver" > "$out/driver-source.txt"
printf '%s  %s\n' a65283d5291f660b99c8c96773054b373c0cbeb9a0dd227b3e95b13904f5f644 "$out/driver-source.txt" | timeout -k 2 5 sha256sum --strict --check > "$out/driver-pin.txt"
# Only exact, unique definitions from this pinned source; no top-level panel code.
awk '
 BEGIN {n=split("safe_path bounded pin verify_inputs read_exit exact_owner drain_empty settle run_stage",names," ")}
 {
  for(i=1;i<=n;i++) {
   anchor=names[i] "() {"
   if ($0==anchor || (names[i]=="safe_path" && index($0,anchor)==1)) {
    if(active || ++seen[names[i]]!=1) exit 1
    active=names[i]
   }
  }
  if(active) {
   print
   if($0=="}" || active=="safe_path") {closed[active]++; active=""}
  }
 }
 END {for(i=1;i<=n;i++) if(seen[names[i]]!=1 || closed[names[i]]!=1) exit 1; if(active) exit 1}
 ' "$out/driver-source.txt" > "$out/driver-functions.sh"
bash -n "$out/driver-functions.sh"
# shellcheck source=/dev/null
source "$out/driver-functions.sh"
jq -c '.runner,.driver,.worker,.producer,.reducer,.shell,.parity,.fixture,.authority[],.original_inputs[],.historical_completed,(.phases[]|.runtime_config,.expected_headers)' "$manifest" > "$out/input-pins.jsonl"
verify_inputs > "$out/input-sha-before.txt"
[[ $(jq -er .runner.path "$manifest") == "$(realpath -e -- "$0")" ]]
worker=$(jq -er .worker.path "$manifest")
parity=$(jq -er .parity.path "$manifest")
fixture=$(jq -er .fixture.path "$manifest")
[[ $worker == "$(dirname -- "$driver")/panel-stage.sh" && $parity == "$(dirname -- "$driver")/check-disabled-historical-parity.sh" && $fixture == "$(dirname -- "$driver")/fault-fixture.sh" ]]
for role in producer reducer shell; do
 binary=$(jq -er ".$role.path" "$manifest")
 [[ -x $binary ]]
 bounded 5 readelf -h "$binary" > "$out/$role-elf.txt"
done
historical=$(jq -er .historical_completed.path "$manifest")
[[ $(stat -c %s -- "$historical") -le 262144 ]]
jq -se 'length==1 and (.[0]|keys==["expected_bound_inputs","expected_identity","input","schema"] and .schema=="borsuk-completed-native-reduction-config-v4")' "$historical" > "$out/historical-schema.txt"
jq -c .input "$historical" >> "$out/input-pins.jsonl"
mkdir "$out/frozen"
for slot in 0 1; do
 label=$(jq -er ".phases[$slot].label" "$manifest")
 config=$(jq -er ".phases[$slot].runtime_config.path" "$manifest")
 headers=$(jq -er ".phases[$slot].expected_headers.path" "$manifest")
 [[ $(stat -c %s -- "$config") -le 32768 && $(stat -c %s -- "$headers") -le 131072 ]]
 cat "$config" > "$out/frozen/$label.runtime.json"
 cat "$headers" > "$out/frozen/$label.expected.json"
 # Metadata equalities only; the native producer/decoder owns scientific validity.
 jq -ne --slurpfile c "$config" --slurpfile h "$headers" --slurpfile m "$manifest" --slurpfile old "$historical" --argjson slot "$slot" '
  def keys_are($s): keys==($s|split(" ")|sort);
  $c[0] as $r | $h[0].expected_identity as $i | $h[0].expected_bound_inputs as $b | $m[0] as $m |
  [0,32,64,96,128,161,193,225,257,290,322,354,386,418,451,483,515,547,580,612,644,676,708,741,773,805,837,870,902,934,966,999] as $ordinals |
  ($c|length==1) and ($h|length==1) and ($h[0]|keys_are("expected_identity expected_bound_inputs")) and
  $r.schema=="borsuk-cohere-native-baseline-config-v10" and $i.schema=="borsuk-cohere-native-baseline-result-v10" and
  $i.phase=="identity" and $b.phase=="bound_inputs" and $i.binary_sha256==$m.producer.sha256 and $i.config_sha256==$m.phases[$slot].runtime_config.sha256 and
  $r.rows==1000000 and $r.dimensions==1024 and $r.count==1000 and $r.k==10 and $r.profile=="scale1m" and
  $r.source_get_budget==128 and $r.sq8_get_budget==30 and $r.sq8_byte_budget==33554432 and $r.fetch_parallelism==64 and
  $r.max_memory_bytes==536870912 and $r.serving=={mode:"baseline"} and $r.backend.kind=="s3" and
  $r.execution=={mode:"diagnostic_panel",ordinals:$ordinals,trace:($slot==0)} and ($r|has("sq8_range_split")) and
  $r.sq8_range_split==(if $slot==0 then {max_physical_range_bytes:2121728,max_physical_gets:46} else null end) and
  $b.selected_count==32 and $b.source_cache=="off" and $b.truth_opened==false and
  ($b|del(.sq8_range_split)|.execution.trace=false)==$old[0].expected_bound_inputs and
  (["dataset","revision","metric","tie_rule","corpus_intervals","reserved_query_interval","producer_authority","corpus_source_first","query_source_first","rows","dimensions","count","k","profile","backend","generation_prefix","generation_root_sha256","max_memory_bytes","sq8_get_budget","sq8_byte_budget","fetch_parallelism","source_get_budget","sq8_range_split","serving","execution"]|all(.[];. as $k|$r[$k]==$b[$k])) and
  (["fetch_parallelism","source_get_budget","sq8_get_budget","sq8_byte_budget","sq8_range_split","serving","execution"]|all(.[];. as $k|$r[$k]==$i[$k])) and
  (["requests","truth","cohort_receipt","derivation_receipt"]|all(.[];. as $k|$r[$k]==$m.original_inputs[$k] and $r[$k].sha256==$b[$k+"_sha256"])) and
  $r.requests.bytes==$b.requests_bytes and $r.truth.bytes==$b.truth_bytes and
  $r.native_source.source_sha256==$b.native_source_sha256 and $r.native_source.sq8_sha256==$b.native_sq8_sha256 and $r.native_source.source_order_sha256==$b.native_order_sha256
  ' > "$out/frozen/$label.metadata.txt"
 cmp "$config" "$out/frozen/$label.runtime.json"
 cmp "$headers" "$out/frozen/$label.expected.json"
 scratch=$(jq -er .scratch_parent "$config")
 safe_path "$scratch"
 [[ -d $scratch && ! -L $scratch && $(realpath -e -- "$scratch") == "$scratch" ]]
done
jq -se 'def neutral: del(.sq8_range_split)|.execution.trace=false; (.[0]|neutral)==(.[1]|neutral)' "$out/frozen/C1.runtime.json" "$out/frozen/NULL_SMOKE.runtime.json" > "$out/common-runtime.txt"
jq -se 'def neutral: del(.sq8_range_split,.config_sha256)|.execution.trace=false;
 (.[0].expected_identity|neutral)==(.[1].expected_identity|neutral) and
 (.[0].expected_bound_inputs|neutral)==(.[1].expected_bound_inputs|neutral)' "$out/frozen/C1.expected.json" "$out/frozen/NULL_SMOKE.expected.json" > "$out/common-headers.txt"
verify_inputs > "$out/all-input-sha-before.txt"

for slot in 0 1; do
 label=$(jq -er ".phases[$slot].label" "$manifest")
 campaign=$out/$label
 mkdir "$campaign"
 cp "$out/input-pins.jsonl" "$campaign/input-pins.jsonl"
 config=$(jq -er ".phases[$slot].runtime_config.path" "$manifest")
 config_sha=$(jq -er ".phases[$slot].runtime_config.sha256" "$manifest")
 # C0 is the unchanged workers allowed label for the isolated NULL_SMOKE stage.
 stage=C1
 [[ $slot == 0 ]] || stage=C0
 result=$campaign/result.jsonl
 run_stage "$stage" producer "$config" "$config_sha" "$result"
 phase=$label-prepare-validation
 [[ -f $result && ! -L $result ]]
 bytes=$(stat -c %s -- "$result")
 (( bytes > 0 && bytes <= 16777216 ))
 digest=$(bounded 10 sha256sum -- "$result")
 digest=${digest%% *}
 printf '%s  %s\n' "$digest" "$result" >> "$out/results.sha256"
 # Whole opaque result descriptor plus INDEPENDENT headers; never inspect JSONL.
 jq -n --arg path "$result" --arg sha "$digest" --argjson bytes "$bytes" --slurpfile h "$out/frozen/$label.expected.json" '
  {schema:"borsuk-completed-native-reduction-config-v4",input:{path:$path,bytes:$bytes,sha256:$sha},
   expected_identity:$h[0].expected_identity,expected_bound_inputs:$h[0].expected_bound_inputs}' > "$campaign/completed.json"
 digest=$(bounded 10 sha256sum -- "$campaign/completed.json")
 printf '%s\n' "$digest" >> "$out/results.sha256"
 run_stage reducer reducer --completed-scale "$campaign/completed.json" "${digest%% *}" "$campaign/native-report.json"
 jq -e --slurpfile c "$campaign/completed.json" '
  .schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .complete==true and
  .diagnostic_only==true and .diagnostic_query_count==32 and
  .result_path==$c[0].input.path and .result_bytes==$c[0].input.bytes and .result_sha256==$c[0].input.sha256 and
  .identity==$c[0].expected_identity and .inputs==$c[0].expected_bound_inputs' "$campaign/native-report.json" > "$campaign/validated.txt"
 bounded 15 sha256sum --strict --check "$out/results.sha256" > "$campaign/results-sha-after.txt"
 completed=$((completed + 1))
done

campaign=$out/MOCKED
mkdir "$campaign"
cp "$out/input-pins.jsonl" "$campaign/input-pins.jsonl"
printf '%s\n' 'MOCKED synthetic parity fault fixture; no scientific qualification' > "$campaign/MOCKED.txt"
run_stage reducer shell "$fixture" "$campaign/results"

campaign=$out/PARITY
mkdir "$campaign"
cp "$out/input-pins.jsonl" "$campaign/input-pins.jsonl"
REDUCER=$(jq -er .reducer.path "$manifest")
REDUCER_SHA=$(jq -er .reducer.sha256 "$manifest")
HIST_COMPLETED_CFG=$historical
HIST_CFG_SHA=$(jq -er .historical_completed.sha256 "$manifest")
NULL_COMPLETED_CFG=$out/NULL_SMOKE/completed.json
digest=$(bounded 10 sha256sum -- "$NULL_COMPLETED_CFG")
NULL_CFG_SHA=${digest%% *}
NEW_OUTPUT_DIR=$campaign/results
# systemd does not inherit caller variables: env sets the existing seven-env API
# INSIDE the bounded trusted-shell stage. The checker validates both natives first.
# shellcheck disable=SC2016
run_stage reducer shell -c 'exec /usr/bin/env REDUCER="$1" REDUCER_SHA="$2" HIST_COMPLETED_CFG="$3" HIST_CFG_SHA="$4" NULL_COMPLETED_CFG="$5" NULL_CFG_SHA="$6" NEW_OUTPUT_DIR="$7" /usr/bin/bash "$8"' \
 bash "$REDUCER" "$REDUCER_SHA" "$HIST_COMPLETED_CFG" "$HIST_CFG_SHA" "$NULL_COMPLETED_CFG" "$NULL_CFG_SHA" "$NEW_OUTPUT_DIR" "$parity"
jq -e '.schema=="borsuk-disabled-historical-parity-v1" and .status=="EXACT_CANONICAL_PARITY" and .complete==true and .native_validations==2 and .performance_claim==false' "$NEW_OUTPUT_DIR/parity.json" > "$campaign/validated.txt"
bounded 15 sha256sum --strict --check "$out/results.sha256" > "$out/results-sha-final.txt"
verify_inputs > "$out/input-sha-final.txt"
phase=local-only-complete
