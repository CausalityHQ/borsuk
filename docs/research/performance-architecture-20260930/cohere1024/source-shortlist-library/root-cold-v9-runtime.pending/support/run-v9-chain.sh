#!/bin/bash
# Two-ELF paired cold campaign; native Rust owns all per-query validation and statistics.
set -euo pipefail
[[ $# == 0 && $EUID == 0 ]] || exit 125
root=/mnt/borsuk-v9-panel
mkdir -m 700 "$root/chain.claim"
date -u +%FT%TZ > "$root/chain.claim/started.txt"
phase=starting
finish() {
 original=$?; trap - EXIT
 set +e
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
 local name=$1 cap=$2 manager cg stop token invocation current forced=false drained=false; shift 2
 stop=$(cat "$root/machine-stop.epoch")
 [[ $stop =~ ^[0-9]+$ && $stop -gt $(( $(date +%s)+cap+360 )) ]] || return 125
 cg=/sys/fs/cgroup/system.slice/borsuk-v9-prep-$name.service
 [[ ! -d $cg && $(/usr/bin/timeout -k 2 10 systemctl show "borsuk-v9-prep-$name.service" -p LoadState --value) == not-found ]] || return 125
 mkdir -m 700 "$root/evidence/prep-$name.claim"
 token=$(cat /proc/sys/kernel/random/uuid)
 [[ $token =~ ^[a-f0-9-]{36}$ ]] || return 125
 printf '%s\n' "$token" > "$root/evidence/prep-$name.claim/token.txt"
 set +e
 /usr/bin/timeout -k 20 "$((cap+40))" systemd-run --unit="borsuk-v9-prep-$name" --expand-environment=no --wait --pipe -p WorkingDirectory="$root" -p Environment="BORSUK_V9_STAGE_TOKEN=$token" -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=2147483648 -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStartSec=10 -p TimeoutStopSec=10 -p LimitCORE=0 -p RuntimeMaxSec="$cap" "$@" > "$root/evidence/prep-$name.log" 2>&1
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
 sha=$(sha256sum "$root/config/$name-transport.json"); sha=${sha%% *}
 owned "transport-$name" 620 /bin/bash "$root/transport-v9-inputs.sh" "$root/config/$name-transport.json" "$sha"
}
run() {
 local name=$1 config=$2 sha before rest after
 sha=$(sha256sum "$config" | cut -d ' ' -f 1)
 read -r before rest < /proc/uptime
 printf '%s\tSTART\t%s\t%s\n' "$name" "$before" "$sha" >> "$root/evidence/stages.tsv"
 /bin/bash "$root/run-v9-stage.sh" "$name" "$config" "$sha"
 read -r after rest < /proc/uptime
 printf '%s\tEND\t%s\t%s\n' "$name" "$after" "$sha" >> "$root/evidence/stages.tsv"
}
validate_closed() {
 local name=$1 template=$2 output bytes sha
 output=$root/results/$name.jsonl
 jq -e '.execution_closed==true and .manager_exit==0 and .native_exit==0 and .timeout_wrapper_exit==0 and .owned_cgroup_drained==true and .forced_cleanup==false' "$root/evidence/$name/supervision.json" >/dev/null
 [[ -f $output && ! -L $output ]] || exit 125
 bytes=$(stat -c %s "$output"); ((bytes>0 && bytes<=67108864)) || exit 125
 sha=$(sha256sum "$output" | cut -d ' ' -f 1)
 [[ ! -e $root/config/completed-$name.json && ! -L $root/config/completed-$name.json ]] || exit 125
 jq --arg path "$output" --argjson bytes "$bytes" --arg sha "$sha" '.input={path:$path,bytes:$bytes,sha256:$sha}' "$root/config/$template.completed.EXPECTED.json" > "$root/config/completed-$name.json"
 run "completed-$name" "$root/config/completed-$name.json"
 jq -e '.schema=="borsuk-completed-native-reduction-v4" and .status=="MEASURED" and .complete==true' "$root/results/completed-$name.json" >/dev/null
}
phase=control-canary
owned control-canary 180 /bin/bash "$root/control-v9-canary.sh"
jq -e '.status=="CONTROL_CANARY_PASS" and .ann_executed==false and .scientific_bodies_opened==false' "$root/evidence/control-canary.json" >/dev/null
phase=binaries
transport binary
owned extract-binaries 60 /bin/bash "$root/extract-v9-inputs.sh" binaries
[[ ! -e $root/bin/original && ! -e $root/bin/candidate ]]
mv "$root/bin/check_cohere_native_baseline" "$root/bin/candidate"
cp "$root/archives/original.elf" "$root/bin/original"
printf '%s  %s\n' 59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083 "$root/bin/original" | sha256sum --strict -c -
[[ $(od -An -tx1 -N4 "$root/bin/original" | tr -d ' \n') == 7f454c46 ]] || exit 125
chmod 0500 "$root/bin/original"
phase=receipts
transport auxiliary
owned extract-receipts 60 /bin/bash "$root/extract-v9-inputs.sh" receipts
phase=requests-truth
transport query
(cd "$root" && find config inputs bin -type f -print0 | sort -z | xargs -0 sha256sum) > "$root/frozen-runtime-inputs.sha256"
: > "$root/evidence/stages.tsv"
for kind in admission canary; do
 for arm in original candidate; do
  phase=$kind-$arm
  (cd "$root" && sha256sum --strict -c frozen-runtime-inputs.sha256) > "$root/evidence/$phase.freeze-check.log"
  run "$phase" "$root/config/$phase.config.json"
  validate_closed "$phase" "$phase"
  jq -e '.native_terminal.selected_count==1 and .native_terminal.population_count==1000 and .diagnostic_only==true' "$root/results/completed-$phase.json" >/dev/null
 done
done
phase=smoke-object
owned smoke-object 80 /bin/bash "$root/smoke-v9-object.sh"
jq -e '.status=="CREATED_READBACK_DELETED" and .native_publication_claim==false' "$root/evidence/smoke-object/smoke.json" >/dev/null
date -u +%FT%TZ > "$root/evidence/measurement-freeze.timestamp"
sync -f "$root/evidence/measurement-freeze.timestamp"
cp "$root/frozen-runtime-inputs.sha256" "$root/evidence/measurement-freeze-inputs.sha256"
sync -f "$root/evidence/measurement-freeze-inputs.sha256"
for cell in original-a1 candidate-b1 candidate-b2 original-a2; do
 arm=$(printf '%s' "$cell" | cut -d - -f 1)
 phase=formal-$cell
 (cd "$root" && sha256sum --strict -c frozen-runtime-inputs.sha256) > "$root/evidence/$phase.freeze-check.log"
 run "$phase" "$root/config/formal-$arm.config.json"
 validate_closed "$phase" "formal-$arm"
 jq -e '.population_percentiles_valid==true and .diagnostic_only==false and .native_terminal.selected_count==1000' "$root/results/completed-$phase.json" >/dev/null
 owned "publish-$phase" 150 /bin/bash "$root/publish-cold-cell.sh" "$phase"
done
phase=summary
[[ $(wc -l < "$root/evidence/stages.tsv") == 32 ]] || exit 125
jq -n --slurpfile a1 "$root/results/completed-formal-original-a1.json" --slurpfile b1 "$root/results/completed-formal-candidate-b1.json" --slurpfile b2 "$root/results/completed-formal-candidate-b2.json" --slurpfile a2 "$root/results/completed-formal-original-a2.json" --slurpfile ea "$root/config/formal-original.completed.EXPECTED.json" --slurpfile eb "$root/config/formal-candidate.completed.EXPECTED.json" -f "$root/reduce-v9-abba.jq" > "$root/results/abba-summary.json"
sync -f "$root/results/abba-summary.json"
sync -f "$root/results"
phase=complete
