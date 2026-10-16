#!/usr/bin/env bash
# shellcheck disable=SC2050 # Exact frozen commit literal guard.
set -euo pipefail
# Refuse before setup/network until all final authenticated pins are frozen.
[[ 103f49c7f6384b8acd27271134a8cc86c7877247 =~ ^[0-9a-f]{40}$ ]] || exit 125
for pin in b28fff421648d05c74ff7191f6e64669453551dbea63f78f310884e551a987d2 b85943533cc63ba365451395b9e4c34af2c58741f8fe6a20b698cac6d0d04424 973eec4da523da5524143be3d6a46c94a0b6cacd77fc419788579e60454a5141 31e528998da425b24e8631e552a62a314c1f33e5617476d289e103bfa260ceeb c37b35867891659227d61f53d43d90fa16cb7994981dbf828fc074aba0308f3c 78e14c9ea9f11d190e6dd54c70c2dc5d45b9ecbb0b14869cb57f1b80adf7c22c; do [[ "$pin" =~ ^[0-9a-f]{64}$ ]] || exit 125; done
for bytes in 6317297 52875 50706 11001 11993 561; do [[ "$bytes" =~ ^[1-9][0-9]*$ ]] || exit 125; done
[[ research/semantic-router/20261016/gc-session-gates-a0001 =~ ^research/semantic-router/[0-9]{8}/gc-session-[a-z0-9-]+$ ]] || exit 125
root=/mnt/borsuk-paired-qualification
mkdir -p "$root/evidence" "$root/source"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip jq time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake python3-dev
cd "$root"
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
[[ "$(aws --version)" == aws-cli/2.36.11\ * ]]
fetch_source() {
 local file=$1 sha=$2 bytes=$3
 timeout -k 5 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261016/gc-session-gates-a0001/inputs/$file" "$file" > /dev/null
 test "$(stat -c %s "$file")" = "$bytes"
 printf '%s  %s\n' "$sha" "$file" | sha256sum -c -
}
fetch_source source.tar.gz b28fff421648d05c74ff7191f6e64669453551dbea63f78f310884e551a987d2 6317297
fetch_source source-files.sha256 b85943533cc63ba365451395b9e4c34af2c58741f8fe6a20b698cac6d0d04424 52875
fetch_source native-source.json 973eec4da523da5524143be3d6a46c94a0b6cacd77fc419788579e60454a5141 50706
fetch_source qualify-gc-session.sh 31e528998da425b24e8631e552a62a314c1f33e5617476d289e103bfa260ceeb 11001
fetch_source supervise-gc-session.sh c37b35867891659227d61f53d43d90fa16cb7994981dbf828fc074aba0308f3c 11993
fetch_source required-exact-tests.txt 78e14c9ea9f11d190e6dd54c70c2dc5d45b9ecbb0b14869cb57f1b80adf7c22c 561
sha256sum qualify-gc-session.sh supervise-gc-session.sh required-exact-tests.txt > evidence/runtime-files.sha256
tar -xzf source.tar.gz -C source
(cd source && sha256sum --strict --check ../source-files.sha256) > evidence/source-setup.log
cp evidence/source-setup.log evidence/source-overlay.log
cp source-files.sha256 base-source-files.sha256
cp native-source.json base-native-source.json
jq -e 'length == 445' native-source.json > /dev/null
jq -r 'to_entries | sort_by(.key)[] | "\(.value)  \(.key)"' native-source.json > native-source.sha256
(cd source && sha256sum --strict --check ../native-source.sha256) > evidence/native-source-overlay.log
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | timeout --kill-after=30 300 sh -s -- -y --profile minimal --default-toolchain 1.98.0
timeout --kill-after=30 180 rustup component add clippy --toolchain 1.98.0
rustc -vV > evidence/rustc.txt
cargo --version > evidence/cargo.txt
date +%s > gate-started.epoch
timer_prop=$(timeout -k 2 10 busctl --system get-property org.freedesktop.systemd1 /org/freedesktop/systemd1/unit/borsuk_2dpaired_2dqualification_2dshutdown_2etimer org.freedesktop.systemd1.Timer NextElapseUSecMonotonic)
read -r signature next rest <<< "$timer_prop"
[[ $signature == t && $next =~ ^[0-9]{1,10}$ && -z ${rest:-} ]] || exit 125
uptime_us=$(awk '{printf "%.0f\n", $1*1000000}' /proc/uptime)
(( next > uptime_us && next-uptime_us <= 5400000000 )) || exit 125
now=$(date +%s); machine_stop=$(( now + (next-uptime_us)/1000000 )); native_deadline=$(( now + 4200 )); (( native_deadline <= machine_stop - 240 )) || native_deadline=$(( machine_stop - 240 ))
printf "%s\n" "$machine_stop" > machine-stop.epoch
printf "%s\n" "$machine_stop" > gate-deadline.epoch
(( native_deadline > now + 30 )) || exit 124
systemctl is-active --quiet borsuk-paired-qualification-shutdown.timer
mkdir "$root/evidence/setup"
cp evidence/rustc.txt evidence/cargo.txt evidence/source-setup.log evidence/source-overlay.log evidence/native-source-overlay.log base-source-files.sha256 base-native-source.json native-source.sha256 source-files.sha256 native-source.json gate-started.epoch gate-deadline.epoch machine-stop.epoch "$root/evidence/setup/"
cp evidence/runtime-files.sha256 qualify-gc-session.sh supervise-gc-session.sh required-exact-tests.txt "$root/evidence/setup/"
(set -o noclobber; printf '%s\n' 103f49c7f6384b8acd27271134a8cc86c7877247 > QUALIFICATION_ADMITTED)
jq -n --arg schema borsuk-scale-closure-native-setup-v1 --arg source 103f49c7f6384b8acd27271134a8cc86c7877247 --argjson exit 0 '{schema:$schema,source_commit:$source,setup_exit:$exit,source_authenticated:true,native_qualified:false}' > "$root/evidence/setup/terminal.json"
(cd "$root/evidence/setup" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$root/evidence/setup/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$root/evidence/setup"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261016/gc-session-gates-a0001/setup/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
printf "SETUP_SOURCE_AUTHENTICATED\n"
