#!/usr/bin/env bash
# UNVERIFIED, source-only remote envelope. Root owns scientific validation and closure.
# API: bash publish-sq8-split-panel.sh ROOT MASTER MASTER_SHA DRIVER_SHA
# Output contract: /data/orchestration/borsuk-split-panel-publication-contract.json
set -euo pipefail
set -C
umask 077
root=/mnt/borsuk-sq8-split-panel
campaign=$root/measurement
driver=$root/support/run-sq8-split-panel.sh
publication=$root/publication
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261016/sq8-split-screen-a0001/terminal
original=98
deadline=0
publication_ready=false
driver_started=false
export AWS_DEFAULT_REGION=eu-central-1 AWS_REGION=eu-central-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER=''

# Called by the EXIT trap's finish function.
# shellcheck disable=SC2329
bounded() {
 local cap=$1 left
 shift
 left=$(( deadline - $(date +%s) - 2 ))
 (( left <= cap )) || left=$cap
 (( left > 0 )) || return 124
 timeout -k 2 "$left" "$@"
}
# Always request shutdown, including argument, setup, pin and publication failures.
# shellcheck disable=SC2329
finish() {
 local command_exit=$? publish=0 shutdown_rc final archive_sha=''
 trap - EXIT
 trap '' INT TERM HUP
 set +e
 [[ $command_exit == 0 ]] || original=$command_exit
 if [[ $publication_ready == true ]]; then
  printf '%s\n' "$original" > "$publication/original-exit.txt" || publish=95
 else
  publish=95
 fi
 if [[ $publication_ready == true && $driver_started == true && -d $campaign && ! -L $campaign ]]; then
  bounded 20 tar -czf "$publication/evidence.tar.gz" -C "$root" measurement || publish=95
  [[ -f $publication/evidence.tar.gz && $(stat -c %s "$publication/evidence.tar.gz") -le 67108864 ]] || publish=95
 else
  publish=95
 fi
 if [[ $publish == 0 ]]; then
  bounded 15 sha256sum "$publication/evidence.tar.gz" > "$publication/evidence.sha256.full" || publish=95
  if [[ $publish == 0 ]]; then
   read -r archive_sha _ < "$publication/evidence.sha256.full"
   [[ $archive_sha =~ ^[0-9a-f]{64}$ ]] || publish=95
   printf '%s  evidence.tar.gz\n' "$archive_sha" > "$publication/evidence.sha256" || publish=95
  fi
 fi
 if [[ $publish == 0 ]]; then
  bounded 45 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" \
   --body "$publication/evidence.tar.gz" --if-none-match '*' > "$publication/archive-put.json" || publish=95
  if [[ $publish == 0 ]]; then
   bounded 20 aws s3api put-object --bucket "$bucket" --key "$prefix/evidence.sha256" \
    --body "$publication/evidence.sha256" --if-none-match '*' > "$publication/sha-put.json" || publish=95
  fi
 fi
 if [[ $publish == 0 ]]; then
  # Publication is transport only; the native reducer decision remains opaque here.
  # shellcheck disable=SC2016
  bounded 5 jq -n --argjson original "$original" --arg sha "$archive_sha" --arg master "$master_sha" --arg driver "$driver_sha" \
   '{schema:"borsuk-sq8-split-panel-publication-v1",scope:"SCIENTIFIC_DIAGNOSTIC_PANEL",original_exit:$original,
     archive_sha256:$sha,master_sha256:$master,driver_sha256:$driver,publication_complete:true,
     compute_closed:false,root_independent_verification_required:true,scientific_result_verified:false,
     performance_claim:false,quality_claim:false,vendor_win:false}' > "$publication/terminal.json" || publish=95
  if [[ $publish == 0 ]]; then
   bounded 20 aws s3api put-object --bucket "$bucket" --key "$prefix/terminal.json" \
    --body "$publication/terminal.json" --if-none-match '*' > "$publication/terminal-put.json" || publish=95
  fi
 fi
 final=$original
 if [[ $final == 0 && $publish != 0 ]]; then final=95; fi
 timeout -k 2 5 shutdown -h +1
 shutdown_rc=$?
 if [[ $final == 0 && $shutdown_rc != 0 ]]; then final=97; fi
 printf 'original_exit=%s\npublication_exit=%s\nshutdown_request_exit=%s\nwrapper_exit=%s\n' "$original" "$publish" "$shutdown_rc" "$final"
 exit "$final"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

[[ $# == 4 ]] || exit 125
[[ $1 == "$root" ]]
master=$2 master_sha=$3 driver_sha=$4
[[ $master =~ ^/[A-Za-z0-9_./-]+$ && $master_sha =~ ^[0-9a-f]{64}$ ]]
[[ $driver_sha == a65283d5291f660b99c8c96773054b373c0cbeb9a0dd227b3e95b13904f5f644 ]]
[[ -d $root && ! -L $root && $(realpath -e -- "$root") == "$root" && ! -e $publication && ! -L $publication ]]
mkdir "$publication"
publication_ready=true
[[ -f $master && ! -L $master && $(realpath -e -- "$master") == "$master" && $(stat -c %s -- "$master") -le 65536 ]]
[[ -f $driver && ! -L $driver && $(realpath -e -- "$driver") == "$driver" && $(stat -c %s -- "$driver") == 21270 ]]
printf '%s  %s\n%s  %s\n' "$master_sha" "$master" "$driver_sha" "$driver" | timeout -k 2 5 sha256sum --strict --check -
start=$(timeout -k 2 5 jq -er '.machine_started_epoch|select(type=="number" and floor==. and .>=1000000000 and .<=9999999999)' "$master")
shared=$(timeout -k 2 5 jq -er '.shared_deadline_epoch|select(type=="number" and floor==. and .>=1000000000 and .<=9999999999)' "$master")
now=$(date +%s)
(( start <= now && shared > now && shared <= start + 3720 ))
deadline=$((start + 3600))
(( deadline <= shared - 120 )) || deadline=$((shared - 120))
# Only the driver may create the fresh measurement directory.
# shellcheck disable=SC2016
bounded 5 jq -se --arg campaign "$campaign" 'length==1 and .[0].campaign_dir==$campaign' "$master" > "$publication/campaign-binding.txt"
[[ ! -e $campaign && ! -L $campaign ]]
bounded 5 systemctl is-active --quiet borsuk-sq8-split-prerequisites-shutdown.timer
left=$(( deadline - $(date +%s) - 125 ))
(( left > 0 )) || exit 124
driver_started=true
set +e
timeout -k 5 "$left" /bin/bash "$driver" "$campaign" "$master" "$master_sha"
original=$?
exit "$original"
