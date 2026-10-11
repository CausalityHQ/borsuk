#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-scale1m
mkdir "$root"; mkdir -p "$root/evidence/setup" "$root/bin" "$root/closed/historical" "$root/closed/local" "$root/prepared"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time util-linux binutils
cd "$root"
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]
set +e
systemd-run --expand-environment=no --unit=borsuk-rust-replay-transport --wait --pipe -p CPUQuota=100% -p CPUQuotaPeriodSec=100ms -p AllowedCPUs=0 -p MemoryMax=1G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=30 -p RuntimeMaxSec=600 /bin/bash -c 'set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
root=/mnt/borsuk-scale1m
cd "$root"
cg=$(sed -n "s/^0:://p" /proc/self/cgroup)
printf "%s\n" "$cg" > "$root/evidence/setup/transport-cgroup.txt"
for f in cpu.max cpuset.cpus.effective memory.max memory.swap.max pids.max; do cat "/sys/fs/cgroup$cg/$f" > "$root/evidence/setup/$f.before"; done
test "$(cat "$root/evidence/setup/cpu.max.before")" = "100000 100000"
test "$(cat "$root/evidence/setup/cpuset.cpus.effective.before")" = 0
test "$(cat "$root/evidence/setup/memory.max.before")" = 1073741824
test "$(cat "$root/evidence/setup/memory.swap.max.before")" = 0
test "$(cat "$root/evidence/setup/pids.max.before")" = 128
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261011/scale-closure-native-qualification-a0001/stages/release-baseline/check_cohere_native_baseline'\'' --if-match '\''"d51a7d5853a361fc89b8aa9e958010e6"'\'' '\''/mnt/borsuk-scale1m/bin/check_cohere_native_baseline'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/bin/check_cohere_native_baseline'\'')" = 17047872
printf '\''%s  %s\n'\'' '\''72ac92e78f0e12ada77f6f6dc8c48d66bc547067a8e12440b469424993fe21f0'\'' '\''/mnt/borsuk-scale1m/bin/check_cohere_native_baseline'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261009/scale-derivation-qualification-a0002/supplemental/publish_two_bit_generation'\'' --if-match '\''"52190475b221cc1726998401c3933b15-2"'\'' '\''/mnt/borsuk-scale1m/bin/publish_two_bit_generation'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/bin/publish_two_bit_generation'\'')" = 15964832
printf '\''%s  %s\n'\'' '\''076afd99e6dc6c814461161fd3bab0fd7bc1ab4db68c1337c8c98137fef15982'\'' '\''/mnt/borsuk-scale1m/bin/publish_two_bit_generation'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/actual1m-pid128-chain-a0002/artifacts.sha256'\'' --if-match '\''"9058167f0743697d4b178ce8186ea02c"'\'' '\''/mnt/borsuk-scale1m/closed/historical/artifacts.sha256'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/closed/historical/artifacts.sha256'\'')" = 94455
printf '\''%s  %s\n'\'' '\''09b7ce99e5293c0e1254287416d536451b77c4386f70a677fb8e9f034563319d'\'' '\''/mnt/borsuk-scale1m/closed/historical/artifacts.sha256'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/actual1m-pid128-chain-a0002/evidence.tar.gz'\'' --if-match '\''"9a84a17d237ab263df3feac794d28564"'\'' '\''/mnt/borsuk-scale1m/closed/historical/evidence.tar.gz'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/closed/historical/evidence.tar.gz'\'')" = 596854
printf '\''%s  %s\n'\'' '\''c02c364a97817ed3d5fc5c86dc2f02f145c5a3f94a3ddca1d713e49e5000862c'\'' '\''/mnt/borsuk-scale1m/closed/historical/evidence.tar.gz'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/full1000-local-admission-a0004/artifacts.sha256'\'' --if-match '\''"4a0f5d8ce788583a323487c15c677ade"'\'' '\''/mnt/borsuk-scale1m/closed/local/artifacts.sha256'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/closed/local/artifacts.sha256'\'')" = 100396
printf '\''%s  %s\n'\'' '\''5795dcd4de77ce712839be11fa41fd849cbdea53bcd553a55fe5e8c09adb88aa'\'' '\''/mnt/borsuk-scale1m/closed/local/artifacts.sha256'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/full1000-local-admission-a0004/evidence.tar.gz'\'' --if-match '\''"223d69fb273a06299a04cdf56461db99"'\'' '\''/mnt/borsuk-scale1m/closed/local/evidence.tar.gz'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/closed/local/evidence.tar.gz'\'')" = 596125
printf '\''%s  %s\n'\'' '\''84e8538cd79bc4f14afdaed449c5d2de8f3ea571a1e1ae1c1b4b8f9023ed8062'\'' '\''/mnt/borsuk-scale1m/closed/local/evidence.tar.gz'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/full1000-preparation-a0002/prepared/queries.f32'\'' --if-match '\''"1553426815b5dbf6d916ccd12f1c227a"'\'' '\''/mnt/borsuk-scale1m/prepared/queries.f32'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/prepared/queries.f32'\'')" = 4096000
printf '\''%s  %s\n'\'' '\''8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e'\'' '\''/mnt/borsuk-scale1m/prepared/queries.f32'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/full1000-preparation-a0002/prepared/truth.u64'\'' --if-match '\''"09946f4a1d247107e8efd679998954ec"'\'' '\''/mnt/borsuk-scale1m/prepared/truth.u64'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/prepared/truth.u64'\'')" = 80000
printf '\''%s  %s\n'\'' '\''0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f'\'' '\''/mnt/borsuk-scale1m/prepared/truth.u64'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261011/completed-header-population-qualification-a0001/stages/release-reducer/compare_native_replay'\'' --if-match '\''"4b825e2724c5967db36aed3489da48fe"'\'' '\''/mnt/borsuk-scale1m/bin/compare_native_replay'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/bin/compare_native_replay'\'')" = 1998456
printf '\''%s  %s\n'\'' '\''4cd7cd24d33e66f3504d42c2b2191a312f471a2a2c8a6b5f6b6d0c8e0e0938a9'\'' '\''/mnt/borsuk-scale1m/bin/compare_native_replay'\'' | sha256sum -c -
timeout --kill-after=5 90 aws s3api get-object --bucket '\''borsuk-bench-453182569524-euc1'\'' --key '\''research/semantic-router/20261010/native-replay-extractor-qualification-a0001/stages/extractor-release/extract_closed_native_replay'\'' --if-match '\''"18153f5f007cf2b4fec9960fac5942fc"'\'' '\''/mnt/borsuk-scale1m/bin/extract_closed_native_replay'\'' > /dev/null
test "$(stat -c %s '\''/mnt/borsuk-scale1m/bin/extract_closed_native_replay'\'')" = 944544
printf '\''%s  %s\n'\'' '\''2525dfcedfb8b13b553e83466d96aa04b90f0f3a5fda78e19784a282a98f40f5'\'' '\''/mnt/borsuk-scale1m/bin/extract_closed_native_replay'\'' | sha256sum -c -
printf '\''%s'\'' '\''{
  "schema": "borsuk-closed-native-replay-extraction-authority-v1",
  "arm": "historical",
  "archive": {
    "path": "/mnt/borsuk-scale1m/closed/historical/evidence.tar.gz",
    "bytes": 596854,
    "sha256": "c02c364a97817ed3d5fc5c86dc2f02f145c5a3f94a3ddca1d713e49e5000862c"
  },
  "manifest": {
    "path": "/mnt/borsuk-scale1m/closed/historical/artifacts.sha256",
    "bytes": 94455,
    "sha256": "09b7ce99e5293c0e1254287416d536451b77c4386f70a677fb8e9f034563319d"
  }
}
'\'' > "$root/closed/historical/authority.json"
printf '\''%s'\'' '\''{
  "schema": "borsuk-closed-native-replay-extraction-authority-v1",
  "arm": "local",
  "archive": {
    "path": "/mnt/borsuk-scale1m/closed/local/evidence.tar.gz",
    "bytes": 596125,
    "sha256": "84e8538cd79bc4f14afdaed449c5d2de8f3ea571a1e1ae1c1b4b8f9023ed8062"
  },
  "manifest": {
    "path": "/mnt/borsuk-scale1m/closed/local/artifacts.sha256",
    "bytes": 100396,
    "sha256": "5795dcd4de77ce712839be11fa41fd849cbdea53bcd553a55fe5e8c09adb88aa"
  }
}
'\'' > "$root/closed/local/authority.json"
mkdir "$root/closed/s3"
timeout --kill-after=2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-query527-a0001/inputs/full1000-config.json "$root/closed/s3/full1000-config.json" > /dev/null
test "$(stat -c %s "$root/closed/s3/full1000-config.json")" = 3071
echo 00400d2c93a5e725a9e45624dc969a9f562720de8a5ad8be530a88dfbd3bc1b3\ \ "$root/closed/s3/full1000-config.json" | sha256sum -c -
timeout --kill-after=2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-query527-a0001/inputs/prior-canary-config.json "$root/closed/s3/canary-one-query-config.json" > /dev/null
test "$(stat -c %s "$root/closed/s3/canary-one-query-config.json")" = 3067
echo b3bc10dd04c6fbb76ed5336b6a9fd1cd04d530741ed55ad5ac627963fd3583c8\ \ "$root/closed/s3/canary-one-query-config.json" | sha256sum -c -
timeout --kill-after=2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/scale-closure-query527-a0001/inputs/prior-completed-config.json "$root/closed/s3/completed-config.json" > /dev/null
test "$(stat -c %s "$root/closed/s3/completed-config.json")" = 4180
echo 3240651011ce27270c8dbb5d29d92a69b87c19b6cf40a65cb761c51f897907ca\ \ "$root/closed/s3/completed-config.json" | sha256sum -c -
timeout --kill-after=2 30 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261011/full1000-cold-canary-a0001/stages/canary-one-query/canary-one-query-result.jsonl "$root/closed/s3/canary-one-query-result.jsonl" > /dev/null
test "$(stat -c %s "$root/closed/s3/canary-one-query-result.jsonl")" = 50535
echo 1c1cf03a55e49a1e83b2d8d8ab3a4ffcd6aa46352356aa745e84a9f08b53b05d\ \ "$root/closed/s3/canary-one-query-result.jsonl" | sha256sum -c -
cp --no-clobber "$root/closed/s3/full1000-config.json" "$root/closed/s3/query527-config.json"
chmod 0500 "$root"/bin/*
systemctl is-active --quiet borsuk-rust-replay-shutdown.timer
read -r boot_uptime ignored < /proc/uptime
now=$(date +%s); machine_stop=$(( now - ${boot_uptime%%.*} + 7200 )); native_deadline=$(( machine_stop - 240 )); (( native_deadline > now + 30 )) || exit 124
printf '\''%s\n'\'' "$machine_stop" > machine-stop.epoch
printf '\''%s\n'\'' "$native_deadline" > gate-deadline.epoch
sha256sum bin/* closed/*/* prepared/* > evidence/setup/input-sha256.txt
printf '\''TRANSPORT_BODY_AUTHENTICATED_NO_NATIVE_RUN\n'\''' > /mnt/borsuk-scale1m/evidence/setup/transport-unit.log 2>&1
manager=$?
set -e
printf '%s\n' "$manager" > /mnt/borsuk-scale1m/evidence/setup/transport-manager.exit
cat /mnt/borsuk-scale1m/evidence/setup/transport-unit.log
test "$manager" = 0
test ! -d /sys/fs/cgroup/system.slice/borsuk-rust-replay-transport.service || grep -qx 'populated 0' /sys/fs/cgroup/system.slice/borsuk-rust-replay-transport.service/cgroup.events
cat /mnt/borsuk-scale1m/evidence/setup/input-sha256.txt
