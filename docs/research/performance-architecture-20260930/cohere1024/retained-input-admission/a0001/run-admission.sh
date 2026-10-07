#!/bin/bash
# One-shot platform commands; native Rust validates data. No result reduction here.
set -euo pipefail
root=/mnt/borsuk-retained-admission
cd "$root"
mkdir evidence scratch
printf '%s  inputs.json\n' cb4a475b5987c1562384f73a73692b8e6b7ad9ffb4a5d10d295b7787e7c6d829 | sha256sum -c -
test "$(jq '.objects | length' inputs.json)" = 15
jq -r '.objects[] | [.sha256,.path] | @tsv' inputs.json |
while IFS=$'\t' read -r sha path; do
  printf '%s  %s\n' "$sha" "$path" | sha256sum -c -
done > evidence/inputs-before.sha256
jq -r '.objects[] | [.bytes,.path] | @tsv' inputs.json |
while IFS=$'\t' read -r bytes path; do test "$(stat -c %s "$path")" = "$bytes"; done
printf '%s  %s\n' ce43842caeea9dbb722f3497b237265e71c7d829cbaf0243a81a2a352fcf1221 bin/A 3911839ba9ef68604e2c487d802a3b3e125bdc1121aee9af268db3ba8a9ca8cf bin/B 733cf976db05b8482ce192251ad25ec203fae9aec4cf8cf519f1dc6658bf0806 bin/publisher | sha256sum -c - > evidence/binaries.sha256
test "$(stat -c %s bin/A)" = 6665104
test "$(stat -c %s bin/B)" = 6764512
test "$(stat -c %s bin/publisher)" = 6323912
sq8=store/semantic/objects/07a14360b06add9a35f82b97f4e031690ff878ff497ee047902818823992337b
inode=$(stat -c %i "$sq8")
mtime_us=$(date -d "$(stat -c %y "$sq8")" +%s%6N)
printf -v etag '"%x-%x-%x"' "$inode" "$mtime_us" "$(stat -c %s "$sq8")"
jq -n --arg root "$root" --arg etag "$etag" '{schema:"borsuk-two-bit-retained-local-publication-config-v1",store_root:($root+"/store"),retained_prefix:"semantic/index",original_root_sha256:"4ec270e5ddaf588ab637321ba8ff807712f146817d93d3780eeb54b084b27982",original_generation:1,original_control_epoch:1,sq8_object_key:"semantic/objects/07a14360b06add9a35f82b97f4e031690ff878ff497ee047902818823992337b",sq8_etag:$etag,destination_prefix:"semantic/rebound",scratch_parent:($root+"/scratch"),max_scratch_bytes:67108864,limits:{max_memory_bytes:536870912,max_active_queries:1,max_query_bytes:16773120,max_query_gets:32,max_parallel_gets:16,max_source_bytes:67108864,max_source_gets:128,max_parallel_source_gets:16,max_query_scratch_bytes:532480,already_pinned_bytes:0}}' > publisher.json
stage() {
  name=$1 expected=$2 seconds=$3; shift 3
  dir="$root/evidence/$name"; mkdir "$dir"
  printf '%s\n' "$expected" > "$dir/expected-exit"
  printf '%q ' "$@" > "$dir/command"
  stop_receipt="/bin/sh -c 'printf \"%s\\n\" \"\$\${SERVICE_RESULT}\" \"\$\${EXIT_CODE}\" \"\$\${EXIT_STATUS}\" > $dir/systemd-terminal'"
  set +e
  systemd-run --unit="borsuk-retained-$name" --slice=borsuk-retained-admission.slice --wait --pipe -p "ExecStopPost=$stop_receipt" -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec="$seconds" --setenv=RAYON_NUM_THREADS=1 --setenv=TOKIO_WORKER_THREADS=1 --setenv=BORSUK_CPU_THREADS=1 /bin/bash -c '
    set -euo pipefail
    dir=$1; shift
    cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
    printf "%s\n" "$cg" > "$dir/cgroup-path"
    for field in cpu.max memory.max memory.swap.max pids.max cpuset.cpus.effective; do cat "/sys/fs/cgroup$cg/$field" > "$dir/$field.before"; done
    test "$(cat "$dir/cpu.max.before")" = "100000 100000"
    test "$(cat "$dir/memory.max.before")" = 536870912
    test "$(cat "$dir/memory.swap.max.before")" = 0
    test "$(cat "$dir/pids.max.before")" = 128
    test "$(cat "$dir/cpuset.cpus.effective.before")" = 0
    set +e
    /usr/bin/time -v -o "$dir/time.txt" "$@"
    status=$?
    set -e
    printf "%s\n" "$status" > "$dir/native-exit"
    for field in memory.peak memory.events memory.swap.current memory.swap.peak pids.current pids.peak cpu.stat; do if [[ -f "/sys/fs/cgroup$cg/$field" ]]; then cat "/sys/fs/cgroup$cg/$field" > "$dir/$field.after"; fi; done
    test "$(cat "$dir/memory.swap.current.after")" = 0
    test "$(cat "$dir/memory.swap.peak.after")" = 0
    test "$(cat "$dir/memory.peak.after")" -le 536870912
    awk '\''$1=="oom" || $1=="oom_kill" {seen++; if ($2!=0) bad=1} END {exit (seen!=2 || bad)}'\'' "$dir/memory.events.after"
    exit "$status"
  ' bash "$dir" "$@" > "$dir/log" 2>&1
  service_exit=$?
  set -e
  printf '%s\n' "$service_exit" > "$dir/service-exit"
  systemctl show "borsuk-retained-$name.service" -p LoadState -p MainPID -p ActiveState -p SubState -p Result -p ExecMainStatus > "$dir/systemd-after"
  test "$(cat "$dir/native-exit")" = "$expected"
  test "$service_exit" = "$expected"
  service_result=success
  if [[ "$expected" != 0 ]]; then service_result=exit-code; fi
  printf '%s\n' "$service_result" exited "$expected" > "$dir/systemd-terminal-expected"
  cmp "$dir/systemd-terminal-expected" "$dir/systemd-terminal"
  test "$(systemctl show "borsuk-retained-$name.service" --value -p MainPID)" = 0
  grep -Fx ActiveState=inactive "$dir/systemd-after"
  grep -Fx SubState=dead "$dir/systemd-after"
  cg_after="/sys/fs/cgroup$(cat "$dir/cgroup-path")"
  if [[ -d "$cg_after" ]]; then test "$(cat "$cg_after/pids.current")" = 0; fi
  printf '%s\n' NO_REMAINING_CGROUP_PROCESSES > "$dir/drained"
}
stage publisher 0 300 "$root/bin/publisher" --retained "$root/publisher.json" "$(sha256sum publisher.json | cut -d' ' -f1)" "$root/publication.json"
test "$(stat -c %s publication.json)" -le 65536
jq --slurpfile pub publication.json --arg root "$root" '.original_runner_config.config | .store_root=($root+"/store") | .scratch_parent=($root+"/scratch") | .requests.path=($root+"/prepared/queries.f32") | .truth.path=($root+"/prepared/truth.u64") | .generation_prefix=$pub[0].metadata_prefix | .generation_root_sha256=$pub[0].root_sha256' inputs.json > baseline.json
stage input-admission 0 300 "$root/bin/A" "$root/baseline.json" "$(sha256sum baseline.json | cut -d' ' -f1)" "$root/admission-result.jsonl"
# Full native admission output stays opaque and is never a timing comparison.
sha256sum admission-result.jsonl > evidence/admission-result.sha256
jq -r '.objects[] | [.sha256,.path] | @tsv' inputs.json |
while IFS=$'\t' read -r sha path; do printf '%s  %s\n' "$sha" "$path" | sha256sum -c -; done > evidence/inputs-after.sha256
cmp evidence/inputs-before.sha256 evidence/inputs-after.sha256
test "$(du -sb "$root" | cut -f1)" -le 4294967296
