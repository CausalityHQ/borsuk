#!/usr/bin/env bash
# Source-only template. Refuses until the independent wrapper is frozen.
set -euo pipefail
wrapper_sha=a61cb43a2d2d4cd8d4709eb46995c619688ebb4967040ab2c191c6e2cb50de0d
wrapper_bytes=17227
[[ $wrapper_sha =~ ^[0-9a-f]{64}$ && $wrapper_bytes =~ ^[1-9][0-9]*$ ]] || exit 125
root=/mnt/borsuk-paired-qualification
prefix=research/semantic-router/20261016/sq8-builder-mutation-a0001
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
mkdir -p "$root/source" "$root/setup"
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake python3-dev
cd "$root"
curl -fsSL --connect-timeout 10 --max-time 90 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum --strict --check -
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
fetch() {
 local file=$1 sha=$2 bytes=$3
 timeout -k 2 45 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/$file" "$file" > "setup/$file.get.json"
 [[ $(stat -c %s "$file") == "$bytes" ]]
 printf '%s  %s\n' "$sha" "$file" | sha256sum --strict --check -
}
fetch source.tar.gz c3e66dc397d4d694700e87699a03dc6c44d01a15984a5609bdfea3dbe9757955 4866840
fetch source-files.sha256 e8c7c66d19150c6662efcd30975186f5c4e9052f1b5137dd4d1e3a7fecf8802f 53131
fetch prove-builder-removal.sh 83df03d1fc9b4ddae33a937846b70b911ab09406e1783528ca2d6ff6589f9059 13042
fetch run-builder-removal.sh "$wrapper_sha" "$wrapper_bytes"
tar -xzf source.tar.gz -C source
(cd source && sha256sum --strict --check ../source-files.sha256) > setup/source-authenticated.log
curl -fsSL --connect-timeout 10 --max-time 90 https://sh.rustup.rs | timeout -k 5 180 sh -s -- -y --profile minimal --default-toolchain 1.98.0
rustc -vV > setup/rustc.txt
cargo -vV > setup/cargo.txt
timeout -k 5 240 cargo fetch --locked --manifest-path "$root/source/Cargo.toml" > setup/cargo-fetch.stdout 2> setup/cargo-fetch.stderr
(cd source && sha256sum --strict --check ../source-files.sha256) > setup/source-after-fetch.log
cmp setup/source-authenticated.log setup/source-after-fetch.log
property=$(timeout -k 2 10 busctl --system get-property org.freedesktop.systemd1 /org/freedesktop/systemd1/unit/borsuk_2dpaired_2dqualification_2dshutdown_2etimer org.freedesktop.systemd1.Timer NextElapseUSecMonotonic)
read -r signature next rest <<< "$property"
[[ $signature == t && $next =~ ^[0-9]{1,16}$ && -z ${rest:-} ]]
uptime_us=$(awk '{printf "%.0f\n", $1*1000000}' /proc/uptime)
(( next > uptime_us && next-uptime_us <= 1800000000 ))
machine_stop=$(( $(date +%s) + (next-uptime_us)/1000000 ))
printf '%s\n' "$machine_stop" > machine-stop.epoch
(( machine_stop - $(date +%s) >= 600 )) || exit 124
systemctl is-active --quiet borsuk-paired-qualification-shutdown.timer
printf '%s\n' SETUP_EXACT_SOURCE_AND_OFFLINE_CACHE_READY
