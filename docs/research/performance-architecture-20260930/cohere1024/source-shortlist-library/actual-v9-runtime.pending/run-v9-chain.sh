#!/bin/bash
# Exact correctness packet, not a cold performance campaign.
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
native() {
 local name=$1 config=$root/config/$1.json sha
 sha=$(sha256sum "$config"); sha=${sha%% *}
 /bin/bash "$root/run-v9-stage.sh" "$name" "$config" "$sha"
}
phase=control-canary
owned control-canary 180 /bin/bash "$root/control-v9-canary.sh"
jq -e '.status=="CONTROL_CANARY_PASS" and .ann_executed==false and .scientific_bodies_opened==false' "$root/evidence/control-canary.json" >/dev/null
phase=binaries; transport binary
owned extract-binaries 60 /bin/bash "$root/extract-v9-inputs.sh" binaries
phase=construction; transport construction
phase=fixture
fixture_sha=$(sha256sum "$root/config/fixture-config.json"); fixture_sha=${fixture_sha%% *}
/bin/bash "$root/run-v9-stage.sh" fixture "$root/config/fixture-config.json" "$fixture_sha"
phase=publication
owned publication 340 /bin/bash "$root/publish-v9-fixture.sh"
prefix=$(cat "$root/evidence/publication/generation-prefix.txt")
root_sha=$(cat "$root/evidence/publication/generation-root-sha256.txt")
phase=receipts; transport auxiliary
owned extract-receipts 60 /bin/bash "$root/extract-v9-inputs.sh" receipts
phase=runtime-freeze
/bin/bash "$root/freeze_v9_panel_configs.sh" runtime "$prefix" "$root_sha"
phase=requests-truth; transport query
mkdir -m 700 "$root/scratch/panel"
phase=panel; native panel
phase=completed-freeze
/bin/bash "$root/freeze_v9_panel_configs.sh" completed "$prefix" "$root_sha" "$root/inputs/historical-count-report.json"
phase=reader; native reader
jq -e '.complete==true and .status=="VALIDATED_PANEL"' "$root/results/reader.json" >/dev/null
phase=parity; native parity
jq -e '.complete==true and .status=="EXACT_SELECTED_RANK_PARITY"' "$root/results/parity.json" >/dev/null
phase=oracle; native oracle
jq -e '.complete==true and .status=="EXACT_RECORDED_SOURCE_COVER"' "$root/results/oracle.json" >/dev/null
# Separate disposable native smoke, with one query and independently derived headers.
phase=smoke-config
set -o noclobber
jq '.execution.ordinals=[0] | .scratch_parent="/mnt/borsuk-v9-panel/scratch/smoke"' "$root/config/panel.json" > "$root/config/smoke-panel.json"
set +o noclobber
mkdir -m 700 "$root/scratch/smoke"
phase=smoke-panel; native smoke-panel
smoke_config_sha=$(sha256sum "$root/config/smoke-panel.json"); smoke_config_sha=${smoke_config_sha%% *}
smoke_sha=$(sha256sum "$root/results/smoke-panel.jsonl"); smoke_sha=${smoke_sha%% *}
smoke_bytes=$(stat -c %s "$root/results/smoke-panel.jsonl")
set -o noclobber
jq --arg config "$smoke_config_sha" --arg sha "$smoke_sha" --argjson bytes "$smoke_bytes" '.completed | .input={path:"/mnt/borsuk-v9-panel/results/smoke-panel.jsonl",bytes:$bytes,sha256:$sha} | .expected_identity.config_sha256=$config | .expected_identity.execution.ordinals=[0] | .expected_bound_inputs.execution.ordinals=[0] | .expected_bound_inputs.selected_count=1' "$root/config/reader.json" > "$root/config/smoke-reader.json"
set +o noclobber
phase=smoke-reader; native smoke-reader
phase=smoke-object
owned smoke-object 80 /bin/bash "$root/smoke-v9-object.sh"
phase=complete
