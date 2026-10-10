#!/usr/bin/env bash
# SOURCE ONLY: remote systemd/FD/runner platform canary; NO BORSUK ELF or ANN data.
# API: FROZEN_CONFIG SHA NEW_EVIDENCE. Positive /bin/true; negative finite Bash descendant.
# shellcheck disable=SC2034,SC2329 # Shared globals/trap are consumed by pinned helpers.
set -Eeuo pipefail
set -o noclobber
umask 077
export LC_ALL=C
[[ $# == 3 && $EUID == 0 && $2 =~ ^[0-9a-f]{64}$ ]] || exit 98
input=$1 config_sha=$2 evidence=$3
library=${BASH_SOURCE[0]%/*}/staging-library.sh
[[ -f $library && ! -L $library && $(stat -c '%u:%a:%h' "$library") == 0:500:1 ]] || exit 98
exec {library_fd}< "$library"
printf '%s  %s\n' 9b46fccb26e90deb6e380e2f67bc47302b100bdc0399c638632e755f29e46a08 "/proc/self/fd/$library_fd" | sha256sum -c -
# shellcheck source=/dev/null
source "/proc/self/fd/$library_fd"
exec {library_fd}<&-
small_json "$input"; authenticate "$input" "$(stat -c %s "$input")" "$config_sha" >/dev/null
jq -e 'keys==["mode","schema","scratch"] and .schema=="borsuk-native-pid128-platform-canary-v1" and (.mode=="positive" or .mode=="negative")' "$input" >/dev/null
mode=$(jq -er .mode "$input"); campaign_evidence_root=$(jq -er .scratch.evidence_root "$input")
[[ $campaign_evidence_root == /mnt/borsuk-pid-evidence && ${evidence%/*} == "$campaign_evidence_root" ]] || exit 98
canonical "$ROOT"; canonical "$campaign_evidence_root"; new_path "$evidence"
mkdir "$evidence" "$evidence/manager-calls" "$evidence/phases"
stage=platform status=INVALID failed_line=0 signal_name='' owned_unit='' owned_id='' owned_cg='' phase_dir=''
completed=(); whole_deadline=180; manager_deadline=$((SECONDS+40))
trap finish EXIT
trap 'failed_line=$LINENO' ERR
trap 'signal_name=TERM; exit 143' TERM
trap 'signal_name=INT; exit 130' INT
config_copy=$evidence/config.json; cp "$input" "$config_copy"
observer_relative=$(sed -n 's/^0:://p' /proc/self/cgroup)
[[ ${INVOCATION_ID:-} =~ ^[0-9a-f]{32}$ && $observer_relative =~ ^/system.slice/borsuk-pid128-observer-[a-z0-9-]+\.service$ ]] || die 'platform observer identity'
observer_cg=/sys/fs/cgroup$observer_relative
snapshot "$observer_cg" "$evidence/observer.initial"; check_limits "$evidence/observer.initial" 268435456 1 0
jq -n --arg id "$INVOCATION_ID" --arg cg "$observer_relative" '{schema:"borsuk-native-pid128-observer-identity-v1",invocation_id:$id,control_group:$cg}' > "$evidence/observer.identity.json"
if [[ $mode == negative ]]; then
    s=$ROOT/staging-platform-negative; new_path "$s"; mkdir "$s" "$s/native"
    fixture=${BASH_SOURCE[0]%/*}/timeout-descendant.sh
    authenticate "$fixture" "$(stat -c %s "$fixture")" 772366057ca756f62e60e3b0266bc5533bf28c4b023e3f8580584425ae09263d > "$evidence/fixture.identity"
    run_phase diagnostic 5 268435456 1 0 /bin/bash "$fixture" "$s/native/descendant.identity"
    die 'platform cleanup negative unexpectedly succeeded'
fi
run_phase diagnostic 5 268435456 1 0 /bin/true
snapshot "$observer_cg" "$evidence/observer.final"
check_limits "$evidence/observer.final" 268435456 1 0
check_events "$evidence/observer.initial" "$evidence/observer.final"
authenticate "$input" "$(stat -c %s "$input")" "$config_sha" > "$evidence/config.after"
status=PLATFORM_CANARY_ROOT_GATE_REQUIRED; stage=closure
exit 0
