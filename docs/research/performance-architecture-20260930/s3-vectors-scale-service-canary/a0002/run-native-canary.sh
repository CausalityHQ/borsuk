#!/bin/bash
set -euo pipefail
umask 077
root=/mnt/borsuk-s3v
cd "$root"
export AWS_SHARED_CREDENTIALS_FILE="$root/private/credentials" AWS_CONFIG_FILE="$root/private/config"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
[[ $(stat -c %a "$AWS_SHARED_CREDENTIALS_FILE") == 600 ]]
[[ $(sha256sum bin/check_s3_vectors_baseline | cut -d' ' -f1) == e350154fe7edc63cf1e13674ac0886e63ebdbfd20edeef6a53d2cbac6a9c51ae ]]
main_sha=$(sha256sum config.json | cut -d' ' -f1)
[[ $main_sha == $(cat config.sha256) ]]
cg=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
[[ $cg == /system.slice/borsuk-s3v-scale-canary.service ]]
for field in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do
 cat "/sys/fs/cgroup$cg/$field" > "evidence/$field.before"
done
[[ $(cat evidence/cpu.max.before) == '100000 100000' ]]
[[ $(cat evidence/cpuset.cpus.effective.before) == 0 ]]
[[ $(cat evidence/memory.max.before) == 536870912 ]]
[[ $(cat evidence/memory.swap.max.before) == 0 ]]
[[ $(cat evidence/pids.max.before) == 128 ]]
config=$root/config.json
sha=$main_sha
publish_attempted=0
cleanup_attempted=0
finish() {
 original=$?
 trap - EXIT TERM
 set +e
 cleanup_status=0
 if (( publish_attempted && ! cleanup_attempted )); then
  timeout -k 5 120 bin/check_s3_vectors_baseline cleanup "$config" "$sha" > evidence/failure-cleanup.log 2>&1
  cleanup_status=$?
  printf '%s\n' "$cleanup_status" > evidence/failure-cleanup.exit
 fi
 publication_status=0
 for field in memory.peak memory.events memory.swap.peak pids.current cpu.stat; do
  cat "/sys/fs/cgroup$cg/$field" > "evidence/$field.after" || publication_status=1
 done
 printf '%s\n' "$original" > evidence/native.exit || publication_status=1
 printf '%s\n' "$cleanup_status" > evidence/cleanup-status.exit || publication_status=1
 (( publication_status == 0 && cleanup_status == 0 )) || original=1
 exit "$original"
}
trap finish EXIT
trap 'exit 97' TERM
stage() {
 local name=$1 command=$2 status
 printf '%s\n' "check_s3_vectors_baseline $command $config $sha" > "evidence/$name.command"
 set +e
 /usr/bin/time -v -o "evidence/$name.time" timeout -k 5 300 bin/check_s3_vectors_baseline "$command" "$config" "$sha" > "evidence/$name.log" 2>&1
 status=$?
 set -e
 printf '%s\n' "$status" > "evidence/$name.exit"
 return "$status"
}
stage actual-input-admit admit
stage derive-canary prepare-scale-canary
config=$root/state/scale-canary/config.json
sha=$(sha256sum "$config" | cut -d' ' -f1)
jq -e --arg sha "$sha" --arg main "$main_sha" '.config_sha256==$main and .data.config_sha256==$sha and .data.claims_quality==false and .data.claims_performance==false' state/scale-canary-prepared.json >/dev/null
stage canary-admit admit
publish_attempted=1
stage publish publish
stage measure measure
stage reduce reduce
cleanup_attempted=1
stage cleanup cleanup
jq -e '.data.status=="CANARY_VALID" and .data.claims_quality==false and .data.claims_performance==false' state/scale-canary/state/reduction.json >/dev/null
jq -e '.data.bucket_absence_verified==true and .data.index_absence_verified==true' state/scale-canary/state/cleanup.json >/dev/null
printf '%s\n' CANARY_NATIVE_VALID_NO_SCIENTIFIC_CLAIM > evidence/canary.accepted
