#!/usr/bin/env bash
set -euo pipefail
# Refuse before setup/network until all final authenticated pins are frozen.
[[ fde98a39778c5fb76277327cd2b4bb224838cdc7 =~ ^[0-9a-f]{40}$ ]] || exit 125
for pin in b35eaf9063377f986caa6663da76c917ae334977b68a944b7d172425c82bad9e 188da5a7479a82e65f58b9a3004c0065bbe088a258a9eb333c79d57828efa79f 44ebb87867fdbcc5840240244ed2d76690325870f430db0965016e0a4fc5d6c2; do [[ "$pin" =~ ^[0-9a-f]{64}$ ]] || exit 125; done
for bytes in 4804865 51976 51539; do [[ "$bytes" =~ ^[1-9][0-9]*$ ]] || exit 125; done
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
 timeout -k 5 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001/inputs/$file" "$file" > /dev/null
 test "$(stat -c %s "$file")" = "$bytes"
 printf '%s  %s\n' "$sha" "$file" | sha256sum -c -
}
fetch_source source.tar.gz b35eaf9063377f986caa6663da76c917ae334977b68a944b7d172425c82bad9e 4804865
fetch_source source-files.sha256 188da5a7479a82e65f58b9a3004c0065bbe088a258a9eb333c79d57828efa79f 51976
fetch_source native-source.json 44ebb87867fdbcc5840240244ed2d76690325870f430db0965016e0a4fc5d6c2 51539
tar -xzf source.tar.gz -C source
(cd source && sha256sum --strict --check ../source-files.sha256) > evidence/source-setup.log
cp evidence/source-setup.log evidence/source-overlay.log
cp source-files.sha256 base-source-files.sha256
cp native-source.json base-native-source.json
jq -e 'length == 438' native-source.json > /dev/null
jq -r 'to_entries | sort_by(.key)[] | "\(.value)  \(.key)"' native-source.json > native-source.sha256
(cd source && sha256sum --strict --check ../native-source.sha256) > evidence/native-source-overlay.log
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | timeout --kill-after=30 300 sh -s -- -y --profile minimal --default-toolchain 1.98.0
timeout --kill-after=30 180 rustup component add clippy --toolchain 1.98.0
rustc -vV > evidence/rustc.txt
cargo --version > evidence/cargo.txt
date +%s > gate-started.epoch
read -r boot_uptime _ < /proc/uptime
now=$(date +%s); machine_stop=$(( now - ${boot_uptime%%.*} + 3600 )); native_deadline=$(( now + 3000 )); (( native_deadline <= machine_stop - 240 )) || native_deadline=$(( machine_stop - 240 ))
printf "%s\n" "$machine_stop" > machine-stop.epoch
printf "%s\n" "$native_deadline" > gate-deadline.epoch
(( native_deadline > now + 30 )) || exit 124
systemctl is-active --quiet borsuk-paired-qualification-shutdown.timer
mkdir "$root/evidence/setup"
cp evidence/rustc.txt evidence/cargo.txt evidence/source-setup.log evidence/source-overlay.log evidence/native-source-overlay.log base-source-files.sha256 base-native-source.json native-source.sha256 source-files.sha256 native-source.json gate-started.epoch gate-deadline.epoch machine-stop.epoch "$root/evidence/setup/"
jq -n --arg schema borsuk-scale-closure-native-setup-v1 --arg source fde98a39778c5fb76277327cd2b4bb224838cdc7 --argjson exit 0 '{schema:$schema,source_commit:$source,setup_exit:$exit,source_authenticated:true,native_qualified:false}' > "$root/evidence/setup/terminal.json"
(cd "$root/evidence/setup" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum) > "$root/evidence/setup/SHA256SUMS"
publication_deadline=$(( $(date +%s) + 120 ))
for file in "$root/evidence/setup"/*; do remaining=$(( publication_deadline - $(date +%s) - 2 )); (( remaining > 0 )) || exit 124; (( remaining <= 90 )) || remaining=90; timeout --kill-after=2 "$remaining" aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key research/semantic-router/20261014/sq8-shortlist-final-build-admission-red-a0001/setup/"$(basename "$file")" --body "$file" --if-none-match "*" > /dev/null; done
printf "SETUP_SOURCE_AUTHENTICATED\n"
