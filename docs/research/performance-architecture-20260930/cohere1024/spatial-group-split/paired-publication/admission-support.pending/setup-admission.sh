#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-paired-admission
root=/mnt/borsuk-paired-source-screen
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
cd "$support"
sha256sum --check support.sha256 > package-check.log
# The root admits this executable environment as part of the support package.
# shellcheck source=/dev/null
source "$support/admission.env"
: "${CONFIG_SHA256:?}" "${AUTHORITY_SHA256:?}" "${EVIDENCE_PREFIX:?}"
[[ $CONFIG_SHA256 =~ ^[0-9a-f]{64}$ && $AUTHORITY_SHA256 =~ ^[0-9a-f]{64}$ ]]
[[ $EVIDENCE_PREFIX =~ ^research/semantic-router/20261012/paired-source-admission-a[0-9]{4}$ ]]
systemctl is-active --quiet borsuk-paired-data-shutdown.timer
awk '/MemTotal:/ {exit ($2 < 8000000 || $2 > 9000000)}' /proc/meminfo
test "$(swapon --show --noheadings | wc -l)" = 0
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time strace
curl -fsSL --connect-timeout 10 --max-time 120 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
test "$(stat -c %s awscliv2.zip)" = 73022935
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum -c -
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
aws --version > aws.version
tracer=$(realpath "$(command -v strace)")
test "$tracer" = /usr/bin/strace
[[ -f $tracer && -x $tracer && ! -L $tracer ]]
test "$(od -An -tx1 -N4 "$tracer")" = ' 7f 45 4c 46'
sha256sum "$tracer" > tracer.sha256
printf '%s\n' "$tracer" > tracer.realpath
"$tracer" --version > tracer.version
dpkg-query -W -f='${Package}\t${Version}\t${Architecture}\n' strace > tracer.package
dpkg-query -S "$tracer" > tracer.owner
set +e
timeout -k 5 30 dpkg --verify strace > tracer.verify.stdout 2> tracer.verify.stderr
verify_exit=$?
set -e
printf '%s\n' "$verify_exit" > tracer.verify.exit
test "$verify_exit" = 0
test ! -s tracer.verify.stdout
[[ ! -e $root && ! -L $root ]]
mkdir "$root"
test "$(findmnt -n -o FSTYPE --target "$root")" = ext4
mkdir "$root/bin" "$root/inputs" "$root/inputs/construction" "$root/evidence"
jq -e '.schema=="borsuk-paired-admission-transport-v1" and (.roles|length)==20 and (.root_staged_metadata|length)==7 and ([.roles[].role]|unique|length)==20' transport.json > /dev/null
jq -e --slurpfile t transport.json '.schema=="borsuk-paired-root-metadata-staged-v1" and .status=="TRANSPORT_METADATA_VERIFIED" and .roles==$t[0].root_staged_metadata' metadata-receipt.json > /dev/null
jq -e --arg c "$CONFIG_SHA256" --arg a "$AUTHORITY_SHA256" '.status=="SIZE_SERVER_SHA_VERIFIED" and .config.sha256==$c and .authority.sha256==$a and .config.head.ChecksumType=="FULL_OBJECT" and .authority.head.ChecksumType=="FULL_OBJECT"' typed-receipt.json > /dev/null
jq -r '(.roles+.root_staged_metadata+[.consumer])[]|[.key,.pin.path,(.pin.bytes|tostring),.pin.sha256]|@tsv' transport.json > transport.tsv
jq -r --arg root "$root" '[[.config, "paired.config.json"],[.authority,"paired.authority.json"]][] | [.[0].key,($root+"/"+.[1]),(.[0].head.ContentLength|tostring),.[0].sha256]|@tsv' typed-receipt.json >> transport.tsv
test "$(wc -l < transport.tsv)" = 30
deadline=$(( $(date +%s)+900 ))
while IFS=$'\t' read -r key path bytes sha; do
 [[ $key =~ ^research/semantic-router/[A-Za-z0-9_./-]+$ && $key != *..* && $key != *//* ]]
 [[ $path == "$root/inputs/"* || $path == "$root/bin/compare_native_replay" || $path == "$root/paired.config.json" || $path == "$root/paired.authority.json" ]]
 [[ $path =~ ^/[A-Za-z0-9_./-]+$ && $path != *..* && $path != *//* && $sha =~ ^[0-9a-f]{64}$ && $bytes =~ ^[1-9][0-9]*$ ]]
 [[ $path != "$root/inputs/queries.f32" && $path != "$root/inputs/truth.u64" && $path != "$root/inputs/sq8.bin" ]]
 [[ ! -e $path && ! -L $path ]]
 left=$((deadline-$(date +%s)-5)); (( left > 0 )) || exit 124
 (( left <= 180 )) || left=180
 timeout -k 5 "$left" aws --cli-connect-timeout 10 --cli-read-timeout 60 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$path.part" > "${path##*/}.get.json"
 test "$(stat -c %s "$path.part")" = "$bytes"
 printf '%s  %s\n' "$sha" "$path.part" | sha256sum -c -
 mv "$path.part" "$path"
 chmod 0400 "$path"
done < transport.tsv
printf '%s  %s\n' "$CONFIG_SHA256" "$root/paired.config.json" "$AUTHORITY_SHA256" "$root/paired.authority.json" | sha256sum -c -
jq -e --arg root "$root" '.schema=="borsuk-paired-source-coverage-config-v1" and .inputs.max_owned_bytes==1073741824 and .requests.path==($root+"/inputs/queries.f32") and .truth.path==($root+"/inputs/truth.u64") and .sq8.path==($root+"/inputs/sq8.bin")' "$root/paired.config.json" > /dev/null
# Every configured non-query descriptor must be represented by the authenticated transport.
jq -s -e '.[0] as $c | .[1] as $a | .[2] as $t | [$c.inputs.generation_manifest,$c.inputs.plane_manifest,$c.inputs.centroids,$c.radial[],$c.spatial[],$c.spatial_receipt,$a.construction.original_approval,$a.construction.constructor.config,$a.construction.constructor.archive,$a.construction.constructor.binary,$a.construction.reconstruction,$a.construction.radial_parity,$a.construction.qualification,$a.construction.corrected_closure,$a.producer.result,$a.producer.config,$a.producer.binary,$a.producer.source_bundle.base_archive,$a.producer.source_bundle.overlays[].body,$a.producer.source_bundle.final_source_map,$a.producer.source_bundle.final_native_map,$a.producer.source_bundle.qualification] | sort_by(.path) == ([$t.roles[].pin,$t.root_staged_metadata[].pin]|sort_by(.path))' "$root/paired.config.json" "$root/paired.authority.json" transport.json > /dev/null
chmod 0500 "$root/bin/compare_native_replay"
printf '%s  %s\n' fd6d36f6bfbb4e370a94707941cec396af59492f832f6d1090c24eacbc1ec748 "$root/bin/compare_native_replay" | sha256sum -c -
test "$(od -An -tx1 -N4 "$root/bin/compare_native_replay")" = ' 7f 45 4c 46'
for path in "$root/inputs/queries.f32" "$root/inputs/truth.u64" "$root/inputs/sq8.bin"; do [[ ! -e $path && ! -L $path ]]; done
sync -f "$root"
printf '%s\n' ADMISSION_INPUTS_AUTHENTICATED_RUNTIME_TRACER_FREEZE_REQUIRED
