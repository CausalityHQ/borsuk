#!/bin/bash
# Fixed SQ8 diagnostic panel; Rust owns authentication and attribution. No population claims.
set -euo pipefail
umask 077
[[ $# == 0 && $EUID == 0 ]] || exit 125
root=/mnt/borsuk-v9-panel
mkdir -m 700 "$root/chain.claim"
date -u +%FT%TZ > "$root/chain.claim/started.txt"
phase=starting
code_pin=$BORSUK_RUNTIME_MANIFEST_SHA
[[ $code_pin =~ ^[a-f0-9]{64}$ ]] || exit 125
readonly code_pin
code_checks=0
verify_code() {
 code_checks=$((code_checks+1))
 printf '%s  %s\n' "$code_pin" "$root/runtime-code.sha256" | sha256sum --strict -c - >/dev/null || return 1
 (cd "$root" && sha256sum --strict -c runtime-code.sha256) > "$root/evidence/runtime-code-$code_checks.log" || return 1
}
finish() {
 original=$?; trap - EXIT
 set +e
 if ! verify_code; then printf '%s\n' RUNTIME_CODE_PIN_FAILURE_CLOSEOUT_REFUSED; exit 126; fi
 /usr/bin/timeout -k 5 150 /bin/bash "$root/close-v9-runtime.sh" "$original" "$phase"
 publication=$?
 printf 'ORIGINAL_CHAIN_EXIT=%s PUBLICATION_EXIT=%s LAST_PHASE=%s\n' "$original" "$publication" "$phase"
 if (( publication!=0 )); then exit 126; fi
 exit "$original"
}
trap finish EXIT
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_SHARED_CREDENTIALS_FILE AWS_CONFIG_FILE
owned() {
 local name=$1 cap=$2 manager cg stop token invocation current memory=2147483648 forced=false drained=false; shift 2
 if [[ $name == cold-faults ]]; then memory=268435456; fi
 stop=$(cat "$root/machine-stop.epoch")
 [[ $stop =~ ^[0-9]+$ && $stop -gt $(( $(date +%s)+cap+360 )) ]] || return 125
 cg=/sys/fs/cgroup/system.slice/borsuk-v9-prep-$name.service
 [[ ! -d $cg && $(/usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p LoadState --value) == not-found ]] || return 125
 mkdir -m 700 "$root/evidence/prep-$name.claim"
 token=$(cat /proc/sys/kernel/random/uuid)
 [[ $token =~ ^[a-f0-9-]{36}$ ]] || return 125
 printf '%s\n' "$token" > "$root/evidence/prep-$name.claim/token.txt"
 set +e
 /usr/bin/timeout -k 20 "$((cap+40))" systemd-run --unit="borsuk-v9-prep-$name" --expand-environment=no --wait --pipe -p WorkingDirectory="$root" -p Environment="BORSUK_V9_STAGE_TOKEN=$token" -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax="$memory" -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStartSec=10 -p TimeoutStopSec=10 -p LimitCORE=0 -p RuntimeMaxSec="$cap" "$@" > "$root/evidence/prep-$name.log" 2>&1
 manager=$?
 set -e
 printf '%s\n' "$manager" > "$root/evidence/prep-$name.manager-exit.txt"
 if [[ $manager != 0 ]] || { [[ -d $cg ]] && ! grep -qx 'populated 0' "$cg/cgroup.events"; }; then
  forced=true
  invocation=$(/usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p InvocationID --value) || { touch "$root/evidence/cleanup-unverified"; return 125; }
  if [[ $invocation =~ ^[a-f0-9]{32}$ ]]; then
   [[ $(/usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p Environment --value) == "BORSUK_V9_STAGE_TOKEN=$token" ]] || { touch "$root/evidence/cleanup-unverified"; return 125; }
   printf '%s\n' "$invocation" > "$root/evidence/prep-$name.claim/invocation-id.txt"
   current=$(/usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p InvocationID --value) || { touch "$root/evidence/cleanup-unverified"; return 125; }
   [[ $current == "$invocation" ]] || { touch "$root/evidence/cleanup-unverified"; return 125; }
  elif [[ -d $cg ]]; then
   touch "$root/evidence/cleanup-unverified"; return 125
  fi
  set +e
  if [[ $invocation =~ ^[a-f0-9]{32}$ ]]; then
   if [[ -d $cg ]]; then printf '1\n' > "$cg/cgroup.kill"; printf '%s\n' "$?" > "$root/evidence/prep-$name.kill-exit.txt"; fi
   current=$(/usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p InvocationID --value)
   if [[ $current == "$invocation" ]]; then
    /usr/bin/timeout -k 2 10 systemctl stop "borsuk-v9-prep-$name.service" > "$root/evidence/prep-$name.stop.log" 2>&1
    printf '%s\n' "$?" > "$root/evidence/prep-$name.stop-exit.txt"
   fi
  fi
  set -e
 fi
 /usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p ActiveState -p LoadState > "$root/evidence/prep-$name.final-state.txt" || { touch "$root/evidence/cleanup-unverified"; return 125; }
 if { [[ ! -d $cg ]] || grep -qx 'populated 0' "$cg/cgroup.events"; } && grep -Eq '^ActiveState=(inactive|failed)$' "$root/evidence/prep-$name.final-state.txt"; then drained=true; fi
 jq -n --argjson manager "$manager" --argjson forced "$forced" --argjson drained "$drained" '{manager_exit:$manager,forced_cleanup:$forced,owned_cgroup_drained:$drained,performance_claim:false}' > "$root/evidence/prep-$name.supervision.json"
 [[ $drained == true ]] || { touch "$root/evidence/cleanup-unverified"; return 125; }
 if ((manager!=0)); then return "$manager"; fi
 [[ $forced == false ]]
}
transport() {
 local name=$1 sha
 verify_code
 sha=$(sha256sum "$root/config/$name-transport.json"); sha=${sha%% *}
 owned "transport-$name" 620 /bin/bash "$root/transport-v9-inputs.sh" "$root/config/$name-transport.json" "$sha"
}
run() {
 local name=$1 config=$2 sha before rest after
 verify_code
 (cd "$root" && sha256sum --strict -c frozen-runtime-inputs.sha256) > "$root/evidence/$name.freeze-check.log"
 sha=$(sha256sum "$config" | cut -d ' ' -f 1)
 read -r before rest < /proc/uptime
 printf '%s\tSTART\t%s\t%s\n' "$name" "$before" "$sha" >> "$root/evidence/stages.tsv"
 /bin/bash "$root/run-v9-stage.sh" "$name" "$config" "$sha"
 verify_code
 read -r after rest < /proc/uptime
 printf '%s\tEND\t%s\t%s\n' "$name" "$after" "$sha" >> "$root/evidence/stages.tsv"
}
validate_closed() {
 local name=$1 template=$2 output bytes sha
 output=$root/results/$name.jsonl
 require_closed "$name"
 [[ -f $output && ! -L $output ]] || exit 125
 bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=67108864)) || exit 125
 sha=$(sha256sum "$output" | cut -d ' ' -f 1)
 [[ ! -e $root/config/completed-$name.json && ! -L $root/config/completed-$name.json ]] || exit 125
 jq --arg path "$output" --argjson bytes "$bytes" --arg sha "$sha" '.input={path:$path,bytes:$bytes,sha256:$sha}' "$root/config/$template.completed.EXPECTED.json" > "$root/config/completed-$name.json"
 run "completed-$name" "$root/config/completed-$name.json"
 require_closed "completed-$name"
 jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .complete==true' "$root/results/completed-$name.json" >/dev/null
}
require_closed() {
 jq -e '.execution_closed==true and .manager_exit==0 and .native_exit==0 and .timeout_wrapper_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$root/evidence/$1/supervision.json" >/dev/null
}
reserve() {
 local seconds=$1 stop
 stop=$(cat "$root/machine-stop.epoch")
 [[ $stop =~ ^[0-9]+$ && $stop -gt $(( $(date +%s)+seconds )) ]] || exit 125
}
panel_report() {
 local name=$1 config=$2 sha
 sha=$(sha256sum "$config"); sha=${sha%% *}
 [[ $(stat -c %s "$root/results/$name.json") -le 524288 ]] || exit 125
 jq -e --arg sha "$sha" --slurpfile expected "$config" '
  .schema=="borsuk-sq8-panel-attribution-v1" and
  (.status=="MEASURED" or .status=="INCONCLUSIVE_FIXED_PANEL") and
  .complete==true and .diagnostic_only==true and .population_percentiles_valid==false and
  .vendor_or_scientific_win_claim==false and .default_adoption==false and
  .reduction_config_sha256==$sha and .selected_count==32 and .within_actor_trace_off_parity==true and
  ([.runs[].label]==["A0","B0","A1","B1","B2","A2"]) and
  ([.runs[].input]==[$expected[0].runs[].completed.input]) and
  ([.queries[].ordinal]==[range(0;32)|(. * 999 / 31 | floor)]) and
  (.reproduced_ordinals==([.queries[]|select(.reproduced_excess==true)]|length)) and
  ((.status=="INCONCLUSIVE_FIXED_PANEL")==(.reproduced_ordinals==0))
 ' "$root/results/$name.json" >/dev/null
}
phase=control-canary
verify_code
owned control-canary 180 /bin/bash "$root/control-v9-canary.sh"
jq -e '.status=="CONTROL_CANARY_PASS" and .ann_executed==false and .scientific_bodies_opened==false' "$root/evidence/control-canary.json" >/dev/null
phase=cold-faults
verify_code
owned cold-faults 120 /bin/bash "$root/control-cold-faults.sh"
jq -e '.status=="FAULT_CANARY_PASS" and .native_actors_executed==false and .scientific_bodies_opened==false' "$root/evidence/cold-fault-canary.json" >/dev/null
phase=stage-publication-faults
verify_code
owned stage-publication-faults 180 /bin/bash "$root/control-sq8-stage-publication.sh"
jq -e '.status=="STAGE_PUBLICATION_CANARY_PASS" and .native_actors_executed==false and .scientific_bodies_opened==false and .aws_mocked==true and .real_systemd==true' "$root/evidence/stage-publication-fault-canary.json" >/dev/null
phase=binaries
transport binary
verify_code
owned extract-binaries 60 /bin/bash "$root/extract-v9-inputs.sh" binaries
[[ ! -e $root/bin/original && ! -e $root/bin/candidate ]]
mv "$root/bin/check_cohere_native_baseline" "$root/bin/candidate"
cp "$root/archives/original.elf" "$root/bin/original"
printf '%s  %s\n%s  %s\n' 59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083 "$root/bin/original" 912f23d34dd175f365aab2797896da2c45306bf9dd4faf7d44723c8bdce50995 "$root/bin/candidate" | sha256sum --strict -c -
for binary in original candidate; do
 [[ $(od -An -tx1 -N4 "$root/bin/$binary" | tr -d ' \n') == 7f454c46 ]] || exit 125
 chmod 0500 "$root/bin/$binary"
done
phase=receipts
transport auxiliary
verify_code
owned extract-receipts 60 /bin/bash "$root/extract-v9-inputs.sh" receipts
phase=requests-truth
transport query
# Independently frozen metadata, never headers inferred from observed output.
printf '%s  %s\n' 78a4d2cc7471dde48cc89bbdfa43207e32ccf278605f0e011f555c8dccbdac24 "$root/config/attribution.EXPECTED.json" | sha256sum --strict -c -
jq -e '.schema=="borsuk-sq8-panel-attribution-config-v1" and [.runs[].label]==["A0","B0","A1","B1","B2","A2"]' "$root/config/attribution.EXPECTED.json" >/dev/null
for label in a0 b0 a1 b1 b2 a2; do
 sha=$(jq -er --arg label "${label^^}" '.runs[]|select(.label==$label)|.completed.expected_identity.config_sha256' "$root/config/attribution.EXPECTED.json")
 printf '%s  %s\n' "$sha" "$root/config/$label.runtime.json" | sha256sum --strict -c -
done
(cd "$root" && find config inputs bin -type f -print0 | sort -z | xargs -0 sha256sum) > "$root/frozen-runtime-inputs.sha256"
: > "$root/evidence/stages.tsv"
for kind in admission canary; do
 for arm in original candidate; do
  phase=$kind-$arm
  reserve 1220
  run "$phase" "$root/config/$phase.config.json"
  validate_closed "$phase" "$phase"
  jq -e '.native_terminal.selected_count==1 and .native_terminal.population_count==1000 and .diagnostic_only==true and .population_percentiles_valid==false' "$root/results/completed-$phase.json" >/dev/null
 done
done
phase=smoke-object
verify_code
owned smoke-object 150 /bin/bash "$root/smoke-v9-object.sh"
jq -e '.status=="CREATED_READBACK_DELETED" and .native_publication_claim==false' "$root/evidence/smoke-object/smoke.json" >/dev/null
# Admit the next stage and its reader/closeout, not twelve maximum-duration stages.
# Native cap120: supervised envelope360; reader cap180: envelope480; closeout+slack380.
# The machine deadline is unchanged; a slow attempt may close INVALID with partial evidence.
for prefix in canary-panel panel; do
 phase=$prefix-start
 reserve 1220
 config=$root/config/$prefix.attribution.json
 [[ ! -e $config && ! -L $config && ! -e $config.next && ! -L $config.next ]] || exit 125
 (set -o noclobber; cat "$root/config/attribution.EXPECTED.json" > "$config")
 if [[ $prefix == panel ]]; then
  date -u +%FT%TZ > "$root/evidence/measurement-freeze.timestamp"
  sync -f "$root/evidence/measurement-freeze.timestamp"
  cp "$root/frozen-runtime-inputs.sha256" "$root/evidence/measurement-freeze-inputs.sha256"
  sync -f "$root/evidence/measurement-freeze-inputs.sha256"
 fi
 for label in a0 b0 a1 b1 b2 a2; do
  phase=$prefix-$label
  reserve 1220
  run "$phase" "$root/config/$label.runtime.json"
  require_closed "$phase"
  output=$root/results/$phase.jsonl
  [[ -f $output && ! -L $output ]] || exit 125
  bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=67108864)) || exit 125
  sha=$(sha256sum "$output"); sha=${sha%% *}
  # Bind only opaque input descriptors; all independently frozen headers survive unchanged.
  (set -o noclobber; jq -c --arg label "${label^^}" --arg path "$output" --argjson bytes "$bytes" --arg sha "$sha" '(.runs[]|select(.label==$label)|.completed.input)={path:$path,bytes:$bytes,sha256:$sha}' "$config" > "$config.next")
  mv "$config.next" "$config"
 done
 phase=$prefix-attribution
 reserve 860
 [[ $(stat -c %s "$config") -le 32768 ]] || exit 125
 run "$phase" "$config"
 require_closed "$phase"
 panel_report "$phase" "$config"
done
phase=summary
verify_code
[[ $(wc -l < "$root/evidence/stages.tsv") == 44 ]] || exit 125
sync -f "$root/results/panel-attribution.json"
sync -f "$root/results"
phase=complete
