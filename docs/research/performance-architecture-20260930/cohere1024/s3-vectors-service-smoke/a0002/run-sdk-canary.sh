#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-s3-vectors-canary
cd "$root"
test ! -e state
mkdir state
mkdir -p evidence
export AWS_SHARED_CREDENTIALS_FILE="$root/private/credentials"
export AWS_CONFIG_FILE="$root/private/config"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
test -f "$AWS_SHARED_CREDENTIALS_FILE"
test "$(stat -c %a "$AWS_SHARED_CREDENTIALS_FILE")" = 600
config_sha=$(sha256sum config.json | cut -d' ' -f1)
test "$config_sha" = "$(cat config.sha256)"
sha256sum --check binary.sha256
cgroup=$(awk -F: '$1==0 {print $3}' /proc/self/cgroup)
for field in cpu.max memory.max memory.swap.max pids.max; do
  cat "/sys/fs/cgroup$cgroup/$field" > "evidence/$field.before"
done
test "$(cat evidence/cpu.max.before)" = '100000 100000'
test "$(cat evidence/memory.max.before)" = 536870912
test "$(cat evidence/memory.swap.max.before)" = 0
test "$(cat evidence/pids.max.before)" = 64
publish_attempted=0
cleanup_attempted=0
finish() {
  original=$?
  trap - EXIT TERM
  set +e
  if (( publish_attempted && ! cleanup_attempted )); then
    ./check_s3_vectors_baseline cleanup config.json "$config_sha" > evidence/cleanup.log 2>&1
    printf '%s\n' "$?" > evidence/cleanup.exit
  fi
  for field in memory.peak memory.events memory.swap.peak pids.current; do
    cat "/sys/fs/cgroup$cgroup/$field" > "evidence/$field.after"
  done
  printf '%s\n' "$original" > evidence/native.exit
  exit "$original"
}
trap finish EXIT
trap 'exit 97' TERM
stage() {
  local name=$1
  set +e
  /usr/bin/time -v -o "evidence/$name.time" ./check_s3_vectors_baseline "$name" config.json "$config_sha" > "evidence/$name.log" 2>&1
  local status=$?
  set -e
  printf '%s\n' "$status" > "evidence/$name.exit"
  return "$status"
}
stage admit
publish_attempted=1
stage publish
stage measure
stage reduce
cleanup_attempted=1
stage cleanup
