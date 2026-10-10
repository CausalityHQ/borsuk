#!/usr/bin/env bash
# SOURCE ONLY / runtime unverified. Execute exclusively on causality EC2.
# API: ROOT_STAGING_CONFIG CONFIG_SHA NEW_CAMPAIGN_EVIDENCE
# This performs one real ordinal0 query against admitted state; no cold/performance claim.
# shellcheck disable=SC2034 # Shared globals are consumed by the SHA-pinned source library.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
unset TOKIO_WORKER_THREADS BORSUK_CPU_THREADS RAYON_NUM_THREADS
[[ $# == 3 && $EUID == 0 && $2 =~ ^[0-9a-f]{64}$ ]] || exit 98
input=$1 config_sha=$2 evidence=$3
# The library is generated from exact r3 recipe fragments; never source the whole recipe.
library=${BASH_SOURCE[0]%/*}/staging-library.sh
[[ -f $library && ! -L $library ]] || exit 98
[[ $(stat -c '%u:%a:%h' "$library") == 0:500:1 ]] || exit 98
exec {library_fd}< "$library"
printf '%s  %s\n' 9b46fccb26e90deb6e380e2f67bc47302b100bdc0399c638632e755f29e46a08 "/proc/self/fd/$library_fd" | sha256sum -c -
# shellcheck source=/dev/null
source "/proc/self/fd/$library_fd"
exec {library_fd}<&-
small_json "$input"
authenticate "$input" "$(stat -c %s "$input")" "$config_sha" >/dev/null
jq -e "$JQ_TYPES
 keys_are([\"schema\",\"recipe\",\"source_config\",\"admission\",\"admission_closure\",\"staging_root\"]) and
 .schema==\"borsuk-native-pid128-staging-config-v1\" and
 ([.recipe,.source_config,.admission]|all(.[];art)) and
 (.admission_closure|keys_are([\"terminal\",\"manifest\",\"wrapper_exit\",\"outer\"]) and all(.[];art)) and
 (.staging_root|type==\"string\" and test(\"^/mnt/borsuk-pool-pid/staging-[a-z0-9-]+$\"))" "$input" >/dev/null || die 'staging configuration shape'
for key in recipe source_config admission; do auth_art "$(jq -c ".$key" "$input")" >/dev/null; done
recipe=$(jq -er .recipe.path "$input"); recipe_sha=dd4033d1b31b5b223f74a4dfbb9d73abb5c97942d399756b9bef5c34f4a7c942
[[ $(jq -er .recipe.sha256 "$input") == "$recipe_sha" ]] || die 'staging recipe authority'
source_config=$(jq -er .source_config.path "$input"); admission=$(jq -er .admission.path "$input")
small_json "$source_config"; one_object "$admission" 4194304
[[ $(jq -er .source_config.sha256 "$input") == "$(jq -er .config_sha256 "$admission")" &&
   $(jq -er .recipe_sha256 "$admission") == "$recipe_sha" ]] || die 'admitted source/config authority'
campaign_evidence_root=$(jq -er .scratch.evidence_root "$source_config")
canonical "$ROOT"; canonical "$campaign_evidence_root"
[[ $evidence == "$campaign_evidence_root/"* && $campaign_evidence_root != "$ROOT" &&
   $campaign_evidence_root != "$ROOT/"* && $ROOT != "$campaign_evidence_root/"* ]] || die 'counted disjoint staging evidence'
[[ ${evidence%/*} == "$campaign_evidence_root" ]] || die 'fresh direct-child staging evidence required'
[[ $evidence != "$admission" && $evidence != "${admission%/*}" &&
   $evidence != "${admission%/*}/"* && ${admission%/*} != "$evidence/"* ]] || die 'staging evidence overlaps sealed admission'
payload_fs_device=$(stat -c %d -- "$ROOT") || die 'payload filesystem identity unavailable'
evidence_fs_device=$(stat -c %d -- "$campaign_evidence_root") || die 'evidence filesystem identity unavailable'
[[ $payload_fs_device =~ ^[0-9]+$ && $evidence_fs_device == "$payload_fs_device" ]] || die 'payload and evidence must share filesystem'
new_path "$evidence"; mkdir "$evidence" "$evidence/manager-calls" "$evidence/phases"
stage=staging status=INVALID failed_line=0 signal_name='' owned_unit='' owned_id='' owned_cg='' phase_dir=''
completed=(); whole_deadline=1800; manager_deadline=$((SECONDS+40))
trap finish EXIT
trap 'failed_line=$LINENO' ERR
trap 'signal_name=TERM; exit 143' TERM
trap 'signal_name=INT; exit 130' INT
trap 'signal_name=HUP; exit 129' HUP
# Closure helper reads the actual admitted descriptors via the existing cfg interface.
config_copy=$evidence/source-config-with-closure.json
jq --slurpfile stage "$input" '.admission=$stage[0].admission | .admission_closure=$stage[0].admission_closure' "$source_config" > "$config_copy"
authenticate_admission_closure "$admission"
sealed_admission=$evidence/admission.private.json
cp "$admission" "$sealed_admission"
authenticate "$sealed_admission" "$(jq -er .admission.bytes "$input")" "$(jq -er .admission.sha256 "$input")" > "$evidence/admission.private.identity"
cohort=$PARENT/cohort; derived=$PARENT/derived; generation=$PARENT/generation; store=$PARENT/store
configs=$PARENT/configs; publication=$PARENT/publication-receipt.json
verify_state "$sealed_admission" before-staging
manifest=$(cfg .input_manifest.path); auth_art "$(cfg .input_manifest)" > "$evidence/manifest.identity"
small_json "$manifest"
jq -r '[.assets[],.producer_admission,.baseline_admission]|.[]|[.path,.bytes,.sha256]|@tsv' "$manifest" > "$evidence/inputs.tsv"
check_inputs() {
    local path bytes sha key
    authenticate "$recipe" "$(stat -c %s "$recipe")" "$recipe_sha"
    authenticate "$source_config" "$(jq -er .source_config.bytes "$input")" "$(jq -er .source_config.sha256 "$input")"
    auth_art "$(jq -c .admission "$input")"
    for key in input_manifest cohort_template derivation_template root_gate; do auth_art "$(cfg .$key)"; done
    while IFS=$'\t' read -r path bytes sha; do authenticate "$path" "$bytes" "$sha"; done < "$evidence/inputs.tsv"
}
check_inputs > "$evidence/inputs.before"
observer_relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ ${INVOCATION_ID:-} =~ ^[0-9a-f]{32}$ && $observer_relative =~ ^/system.slice/borsuk-pid128-observer-[a-z0-9-]+\.service$ ]] || die 'original staging observer identity'
observer_cg=/sys/fs/cgroup$observer_relative
snapshot "$observer_cg" "$evidence/observer.initial"
check_limits "$evidence/observer.initial" 268435456 1 0
jq -n --arg id "$INVOCATION_ID" --arg cg "$observer_relative" '{schema:"borsuk-native-pid128-observer-identity-v1",invocation_id:$id,control_group:$cg}' > "$evidence/observer.identity.json"
s=$(jq -er .staging_root "$input"); new_path "$s"; mkdir "$s" "$s/native" "$s/native/scratch"
source_native=$configs/diagnostic.json; small_json "$source_native"
jq --arg scratch "$s/native/scratch" '.scratch_parent=$scratch' "$source_native" > "$s/native/config.json"
jq -S 'del(.scratch_parent)' "$source_native" > "$evidence/config.original-common.json"
jq -S 'del(.scratch_parent)' "$s/native/config.json" > "$evidence/config.staging-common.json"
cmp "$evidence/config.original-common.json" "$evidence/config.staging-common.json"
# Use the admitted diagnostic resource/scratch envelope and actual recipe runner.
run_phase diagnostic 900 536870912 1 0 "$ROOT/assets/bin/check_cohere_native_baseline" "$s/native/config.json" "$(sha_of "$s/native/config.json")" "$s/native/result.jsonl"
validate_result "$s/native/result.jsonl" "$s/native/config.json" 1 "$phase_dir"
verify_state "$sealed_admission" after-staging
check_inputs > "$evidence/inputs.after"; cmp "$evidence/inputs.before" "$evidence/inputs.after"
authenticate "$input" "$(stat -c %s "$input")" "$config_sha" > "$evidence/staging-config.after"
recheck_admission_closure() {
    local evidence=$1
    mkdir "$evidence"
    authenticate_admission_closure "$admission"
}
recheck_admission_closure "$evidence/admission-recheck"
snapshot "$observer_cg" "$evidence/observer.final"
check_limits "$evidence/observer.final" 268435456 1 0
check_events "$evidence/observer.initial" "$evidence/observer.final"
status=NATIVE_STAGING_SMOKE_ROOT_GATE_REQUIRED; stage=closure
sync -f "$s"; sync -f "$evidence"
exit 0
