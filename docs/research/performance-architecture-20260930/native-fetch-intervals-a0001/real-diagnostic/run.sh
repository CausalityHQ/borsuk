#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-paired-qualification
ev=$root/evidence/interval-real
mkdir -p "$root"
mkdir -m700 "$root/closed" "$root/evidence" "$ev"
cd "$root"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout -k30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout -k30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
[[ $(stat -c%s awscliv2.zip) == 73022935 ]]
timeout -k30 120 unzip -q awscliv2.zip
timeout -k30 120 ./aws/install
get() {
 local key=$1 file=$2 bytes=$3 hash=$4
 timeout -k2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$file" > "$ev/$(basename "$file").get.json"
 [[ $(stat -c%s "$file") == "$bytes" ]]
 printf '%s  %s\n' "$hash" "$file" | sha256sum --strict -c - > "$ev/$(basename "$file").hash.log"
}
get research/semantic-router/20261013/byte-window-interval-green-a0002/binaries/compare_native_replay "$root/comparator" 9524168 a22578c6edabed9bd57a97b7bc6969426cebfb87cf23d4ee23abaa985646a7ab
chmod 0500 "$root/comparator"
get research/semantic-router/20261013/source-width128-scale-cold-a0001/terminal/evidence.tar.gz "$root/closed/evidence.tar.gz" 30123447 4c14db39f57d00b6ae783d000779f3d513fb0356213abd027d6cbf981f70ed1b
tar -tzf "$root/closed/evidence.tar.gz" > "$ev/archive-members.txt"
[[ $(grep -Fxc results/formal-original-a1.jsonl "$ev/archive-members.txt") == 1 ]]
tar -xOzf "$root/closed/evidence.tar.gz" results/formal-original-a1.jsonl > "$root/closed/original-a1.jsonl"
[[ $(stat -c%s "$root/closed/original-a1.jsonl") == 35180200 ]]
printf '%s  %s\n' 0855a896f5142b0e84c22fb2f7b1c897909a08f5d680051427ca97de9962ee4b "$root/closed/original-a1.jsonl" | sha256sum --strict -c - > "$ev/result.hash.log"
get research/semantic-router/20261013/source-cover-cost-curve-a0001/inputs/curve-real.config.json "$root/curve-real.config.json" 4764 5cd2e59a3f480fc21ebd40c422dfe4be66482af02eeb3c0d0ae175a83d6cb400
set +e
systemd-run --expand-environment=no --unit=borsuk-interval-real --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=130 -p TimeoutStopSec=5 -p KillMode=control-group -p LimitCORE=0 -p PrivateNetwork=true /bin/bash -c 'set -euo pipefail
ev=/mnt/borsuk-paired-qualification/evidence/interval-real
cg=$(awk -F: '\''$1==0 {print $3}'\'' /proc/self/cgroup)
printf "%s\n" "$cg" > "$ev/cgroup-path.txt"
for field in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$field" > "$ev/$field.before"; done
[[ $(cat "$ev/cpu.max.before") == "100000 100000" && $(cat "$ev/cpuset.cpus.effective.before") == 0 && $(cat "$ev/memory.max.before") == 536870912 && $(cat "$ev/memory.swap.max.before") == 0 && $(cat "$ev/pids.max.before") == 128 ]]
set +e
/usr/bin/time -v -o "$ev/native-time.txt" timeout -k5 120 /mnt/borsuk-paired-qualification/comparator --source-intervals-scale /mnt/borsuk-paired-qualification/curve-real.config.json 5cd2e59a3f480fc21ebd40c422dfe4be66482af02eeb3c0d0ae175a83d6cb400 "$ev/intervals.json" > "$ev/native.log" 2>&1
rc=$?
set -e
printf "%s\n" "$rc" > "$ev/native-exit.txt"
for field in memory.peak memory.events memory.swap.peak cpu.stat pids.peak; do cat "/sys/fs/cgroup$cg/$field" > "$ev/$field.after"; done
exit "$rc"' > "$ev/manager.log" 2>&1
manager=$?
set -e
native=null
if [[ -s $ev/native-exit.txt ]]; then native=$(cat "$ev/native-exit.txt"); [[ $native =~ ^[0-9]+$ ]] || native=null; fi
drained=false
if [[ -s $ev/cgroup-path.txt ]]; then cg=$(cat "$ev/cgroup-path.txt"); if [[ $cg == /system.slice/borsuk-interval-real.service ]]; then if [[ ! -d /sys/fs/cgroup$cg ]] || grep -qx 'populated 0' "/sys/fs/cgroup$cg/cgroup.events"; then drained=true; fi; fi; fi
accepted=false
if [[ $native == 0 && $manager == 0 && $drained == true ]]; then
 if jq -e '.schema=="borsuk-native-fetch-intervals-v1" and .status=="INTERVAL_GEOMETRY_ONLY" and .complete==true and .reducer_source_sha256=="49f68351d2005d23d00a67959586dd76380ca98bd9ad630e910d3cbd86eea524" and .count==1000 and (.queries|length)==1000 and ([.queries[].ordinal]==[range(0;1000)]) and ([.queries[].source_gets]|add)==107882 and ([.queries[].source_phase_bytes]|add)==27428966400 and ([.queries[].sq8_gets]|add)==29903 and ([.queries[].sq8_phase_bytes]|add)==16708608000 and .minimum_feasible_capacity_bytes==([.queries[]|.max_source_range_bytes,.max_sq8_range_bytes]|max)' "$ev/intervals.json" > "$ev/shape-and-totals.txt" 2> "$ev/shape-and-totals.stderr"; then
  if jq '.minimum_feasible_capacity_bytes as $c | ([.queries[].sq8_phase_bytes]|max) as $m | {schema:"borsuk-root-sq8-window-admission-v1",active_queries:1,fetch_parallelism:64,source_cache:"off",ignored_selection_fields:["distinct_binding_arm","capacity_upper_exclusive_bytes","selection"],minimum:$c,maximum_sq8_phase:$m,potential_sq8_queries_at_minimum:([.queries[]|select(.sq8_phase_bytes>$c)]|length),distinct_sq8_arm:($c<$m),selection:(if $c<$m then "FEASIBLE_POTENTIAL_SQ8_WINDOW" else "NO_DISTINCT_BINDING_ARM" end),waiting_observed:false}' "$ev/intervals.json" > "$ev/selection.json" 2> "$ev/selection.stderr"; then accepted=true; fi
 fi
fi
jq -n --argjson native "$native" --argjson manager "$manager" --argjson drained "$drained" --argjson accepted "$accepted" '{schema:"borsuk-interval-real-stage-v1",source_commit:"5d85c7577da4a10b7a3907b40f03bfb8eb7181f0",native_exit:$native,manager_exit:$manager,owned_cgroup_drained:$drained,geometry_accepted:$accepted,claims_performance:false}' > "$ev/terminal.json"
(cd "$ev" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS)
sync -f "$ev/SHA256SUMS"
sync -f "$ev"
deadline=$(( $(date +%s)+120 ))
for file in "$ev"/*; do [[ $(basename "$file") != terminal.json ]] || continue; left=$(( deadline-$(date +%s)-2 )); (( left>0 )) || exit 124; (( left<=60 )) || left=60; timeout -k2 "$left" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261014/byte-window-interval-real-a0001/evidence/$(basename "$file")" --body "$file" --if-none-match '*' > /dev/null; done
timeout -k2 30 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261014/byte-window-interval-real-a0001/evidence/terminal.json --body "$ev/terminal.json" --if-none-match '*' > /dev/null
[[ $accepted == true ]]
