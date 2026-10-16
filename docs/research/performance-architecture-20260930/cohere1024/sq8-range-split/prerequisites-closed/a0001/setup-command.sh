#!/usr/bin/env bash
set -euo pipefail
set -C
umask 077
bootstrap=/mnt/borsuk-sq8-split-bootstrap
[[ ! -e $bootstrap && ! -L $bootstrap ]]
mkdir "$bootstrap"
cd "$bootstrap"
cat > stage-assets.sh <<'STAGE_ASSETS_SOURCE_EOF'
#!/usr/bin/env bash
# Remote EC2 only. Downloads/extracts authenticated inputs; does not run ANN queries.
set -euo pipefail
[[ $# == 3 ]] || exit 125
root=$1 manifest=$2 manifest_sha=$3
[[ $root == /mnt/borsuk-sq8-split-panel && $manifest_sha =~ ^[0-9a-f]{64}$ ]]
[[ $manifest == /* && -f $manifest && ! -L $manifest ]]
printf '%s  %s\n' "$manifest_sha" "$manifest" | sha256sum --strict --check -
jq -e '
  .schema == "borsuk-split-panel-remote-assets-draft-v1" and
  .root == "/mnt/borsuk-sq8-split-panel" and
  .bucket == "borsuk-bench-453182569524-euc1" and
  .native_source_commit == "8e7076951936b0434b7947afd5b23e192c98fe70" and
  (.assets|length)==6 and (.members|length)==3 and
  ([.assets[].target]|unique|length)==6 and
  ([.members[].target]|unique|length)==3 and
  all(.assets[]; (.sha256|test("^[0-9a-f]{64}$")) and
    (.bytes|type=="number" and floor==. and .>0 and .<=20000000) and
    (.target|test("^(inputs|archives|bin)/[A-Za-z0-9_.-]+$")) and
    (.s3_key|test("^research/semantic-router/[A-Za-z0-9_./-]+$"))) and
  all(.members[]; (.sha256|test("^[0-9a-f]{64}$")) and
    (.bytes|type=="number" and floor==. and .>0 and .<=2000000) and
    (.target|test("^(inputs|history)/[A-Za-z0-9_.-]+$")) and
    (.archive|test("^archives/[A-Za-z0-9_.-]+$")) and
    (.member|test("^(\\./)?[A-Za-z0-9_./-]+$")))
' "$manifest" >/dev/null
[[ ! -e $root && ! -L $root ]]
mkdir "$root"
mkdir "$root/inputs" "$root/archives" "$root/bin" "$root/history" "$root/setup" "$root/scratch"
cd "$root"
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
rows=$(jq -r '.assets[]|[.role,.target,.s3_key,(.bytes|tostring),.sha256]|@tsv' "$manifest")
while IFS=$'\t' read -r role target key bytes sha; do
  [[ $role =~ ^[A-Za-z0-9_]+$ && ! -e $target && ! -L $target ]]
  timeout -k 2 90 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 \
    --key "$key" "$target" > "setup/$role.get.json"
  [[ -f $target && ! -L $target && $(stat -c %s "$target") == "$bytes" ]]
  printf '%s  %s\n' "$sha" "$target" | sha256sum --strict --check -
  chmod 0400 "$target"
done <<< "$rows"
rows=$(jq -r '.members[]|[.archive,.member,.target,(.bytes|tostring),.sha256]|@tsv' "$manifest")
while IFS=$'\t' read -r archive member target bytes sha; do
  [[ -f $archive && ! -L $archive && ! -e $target && ! -L $target ]]
  # The whole archive is authenticated above. Still refuse duplicates and links.
  list=$(timeout -k 2 20 tar -tzf "$archive" -- "$member")
  [[ $list == "$member" ]]
  detail=$(timeout -k 2 20 tar --numeric-owner --full-time -tvzf "$archive" -- "$member")
  read -r mode _owner actual_bytes _day _clock actual_name <<< "$detail"
  [[ $mode == -* && $actual_bytes == "$bytes" && $actual_name == "$member" ]]
  (set -o noclobber; timeout -k 2 20 tar -xOzf "$archive" -- "$member" > "$target")
  [[ -f $target && ! -L $target && $(stat -c %s "$target") == "$bytes" ]]
  printf '%s  %s\n' "$sha" "$target" | sha256sum --strict --check -
  chmod 0400 "$target"
done <<< "$rows"
for executable in check_cohere_native_baseline compare_native_replay; do
  readelf -h "bin/$executable" > "setup/$executable.elf.txt"
  grep -Eq 'Class: +ELF64' "setup/$executable.elf.txt"
  grep -Eq 'Machine: +Advanced Micro Devices X86-64' "setup/$executable.elf.txt"
  chmod 0500 "bin/$executable"
done
printf '%s\n' ASSETS_AUTHENTICATED_NO_NATIVE_EXECUTION

STAGE_ASSETS_SOURCE_EOF
cat > setup-runtime.sh <<'SETUP_RUNTIME_SOURCE_EOF'
#!/usr/bin/env bash
# Remote-only thin setup for retained release binaries; no compiler installation.
set -euo pipefail
[[ $# == 4 ]] || exit 125
stage=$1 stage_sha=$2 manifest=$3 manifest_sha=$4
[[ $stage == /* && $manifest == /* && -f $stage && ! -L $stage && -f $manifest && ! -L $manifest ]]
[[ $stage_sha =~ ^[0-9a-f]{64}$ && $manifest_sha =~ ^[0-9a-f]{64}$ ]]
printf '%s  %s\n%s  %s\n' "$stage_sha" "$stage" "$manifest_sha" "$manifest" | sha256sum --strict --check -
[[ ! -e /mnt/borsuk-sq8-split-panel && ! -L /mnt/borsuk-sq8-split-panel ]]
setup=/mnt/borsuk-sq8-split-setup
[[ ! -e $setup && ! -L $setup ]]
mkdir "$setup"
cd "$setup"
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER=''
timeout -k 5 120 apt-get -qq -o DPkg::Lock::Timeout=60 update
timeout -k 5 180 apt-get -qq -y -o DPkg::Lock::Timeout=60 install curl unzip jq time tar gzip util-linux binutils libssl-dev
curl -fsSL --connect-timeout 10 --max-time 90 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' 50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6 | sha256sum --strict --check -
[[ $(stat -c %s awscliv2.zip) == 73022935 ]]
timeout -k 5 60 unzip -q awscliv2.zip
timeout -k 5 60 ./aws/install
[[ $(aws --version) == aws-cli/2.36.11\ * ]]
aws --version > aws-version.txt
timeout -k 2 600 bash "$stage" /mnt/borsuk-sq8-split-panel "$manifest" "$manifest_sha"
printf '%s\n' SETUP_RETAINED_RELEASE_ASSETS_READY

SETUP_RUNTIME_SOURCE_EOF
cat > assets.json <<'ASSETS_METADATA_EOF'
{
  "schema": "borsuk-split-panel-remote-assets-draft-v1",
  "status": "PREPARED_NOT_ADMITTED",
  "root": "/mnt/borsuk-sq8-split-panel",
  "bucket": "borsuk-bench-453182569524-euc1",
  "native_source_commit": "8e7076951936b0434b7947afd5b23e192c98fe70",
  "assets": [
    {
      "role": "requests",
      "bytes": 4096000,
      "sha256": "8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e",
      "s3_key": "research/semantic-router/20261010/full1000-preparation-a0002/prepared/queries.f32",
      "target": "inputs/queries.f32"
    },
    {
      "role": "truth",
      "bytes": 80000,
      "sha256": "0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f",
      "s3_key": "research/semantic-router/20261010/full1000-preparation-a0002/prepared/truth.u64",
      "target": "inputs/truth.u64"
    },
    {
      "role": "cohort_archive",
      "bytes": 596125,
      "sha256": "84e8538cd79bc4f14afdaed449c5d2de8f3ea571a1e1ae1c1b4b8f9023ed8062",
      "s3_key": "research/semantic-router/20261010/full1000-local-admission-a0004/evidence.tar.gz",
      "target": "archives/cohort-evidence.tar.gz"
    },
    {
      "role": "historical_B0_archive",
      "bytes": 7230615,
      "sha256": "8794d773c99ccf3323cda692fead6377609e521196f355e653ce2290f2ae93fe",
      "s3_key": "research/semantic-router/20261015/sq8-panel-diagnostic-a0001/terminal/evidence.tar.gz",
      "target": "archives/historical-B0.tar.gz"
    },
    {
      "role": "check_cohere_native_baseline",
      "bytes": 17629808,
      "sha256": "65ad16cd5aecc22731a9752ca7233226b11611d21c3a1468be30cc384c14ea33",
      "s3_key": "research/semantic-router/20261016/sq8-split-correctness-a0003/terminal/binaries/check_cohere_native_baseline",
      "target": "bin/check_cohere_native_baseline"
    },
    {
      "role": "compare_native_replay",
      "bytes": 13384312,
      "sha256": "4efc0af9855e1320a915053dcbbd7e242bc7f03a541460be3d73290ce66dc25f",
      "s3_key": "research/semantic-router/20261016/sq8-split-correctness-a0003/terminal/binaries/compare_native_replay",
      "target": "bin/compare_native_replay"
    }
  ],
  "members": [
    {
      "archive": "archives/cohort-evidence.tar.gz",
      "member": "./prepared-parent_cohort_complete.json",
      "target": "inputs/prepared-parent_cohort_complete.json",
      "bytes": 6238,
      "sha256": "e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17"
    },
    {
      "archive": "archives/cohort-evidence.tar.gz",
      "member": "./prepared-parent_derived_derivation.json",
      "target": "inputs/prepared-parent_derived_derivation.json",
      "bytes": 23128,
      "sha256": "00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e"
    },
    {
      "archive": "archives/historical-B0.tar.gz",
      "member": "results/panel-b0.jsonl",
      "target": "history/panel-b0.jsonl",
      "bytes": 1152331,
      "sha256": "3f5db1a50db1ccec8ababb7468d2122ee27ce7ecc54018621136f3a46f22068f"
    }
  ],
  "parsing_policy": "All raw requests, truth and historical JSONL transport/extraction/validation on remote EC2 only; no local scientific parsing. Complete original archive/member digests mandatory. No old headers or envelopes relabeled.",
  "provenance": "Original closed launch-diagnostic-a0001 query/auxiliary transport rosters and extract-v9-inputs.sh; historical closed B0 authority; independently verified current a0003 terminal binary roster",
  "does_not_admit_performance": true
}

ASSETS_METADATA_EOF
printf "%s  %s\n" a535adf177d0fa002e9ceb780f459819d9ad183e2c1ee7c391eeb14b071907e2 "$bootstrap/setup-runtime.sh" | sha256sum --strict --check -
bash "$bootstrap/setup-runtime.sh" "$bootstrap/stage-assets.sh" 22c0c34f689eddc5cc0b107481474c63c7e4f2c9d8df4522d048b1cd6c64720d "$bootstrap/assets.json" 41f37e8ed847a4b522b2b9e246bc5d335219e7f3fef441408230c700970f7fcf
bundle_sha=ab6177bbb9dbfe4ffea3c5bff0dceeaa66cbdc69091a329760575f50b21e8d38
bundle_bytes=46879
root=/mnt/borsuk-sq8-split-panel
prefix=research/semantic-router/20261016/sq8-split-prerequisites-a0001
export AWS_DEFAULT_REGION=eu-central-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
timeout -k 2 45 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/support-bundle.tar.gz" "$bootstrap/support-bundle.tar.gz" > "$bootstrap/bundle-get.json"
[[ $(stat -c %s "$bootstrap/support-bundle.tar.gz") == "$bundle_bytes" ]]
printf '%s  %s\n' "$bundle_sha" "$bootstrap/support-bundle.tar.gz" | sha256sum --strict --check -
for name in authority runtime support; do [[ ! -e $root/$name && ! -L $root/$name ]]; done
timeout -k 2 20 tar -tzvf "$bootstrap/support-bundle.tar.gz" > "$bootstrap/bundle-members.txt"
timeout -k 2 20 tar --no-same-owner --no-same-permissions -xzf "$bootstrap/support-bundle.tar.gz" -C "$root"
metadata=/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending
[[ ! -e $metadata && ! -L $metadata ]]
mkdir -p "$metadata"
[[ $(realpath -e "$metadata") == "$metadata" ]]
for name in C1.runtime.json C1.expected.json NULL_SMOKE.runtime.json NULL_SMOKE.expected.json; do
 cp "$root/runtime/$name" "$metadata/$name"
 cmp "$root/runtime/$name" "$metadata/$name"
 chmod 0400 "$metadata/$name"
done
dpkg-query -W -f='${Package} ${Version} ${Architecture}\n' bash > "$root/setup/bash-package.txt"
timeout -k 2 15 dpkg --verify bash > "$root/setup/bash-package-verify.stdout" 2> "$root/setup/bash-package-verify.stderr"
[[ ! -s $root/setup/bash-package-verify.stdout ]]
[[ $(realpath -e /usr/bin/bash) == /usr/bin/bash ]]
bash_sha=$(sha256sum /usr/bin/bash); bash_sha=${bash_sha%% *}
jq -n --arg machine "$(cat /etc/machine-id)" --arg vendor "$(cat /sys/devices/virtual/dmi/id/sys_vendor)" --arg sha "$bash_sha" --argjson bytes "$(stat -c %s /usr/bin/bash)" \
 '{schema:"borsuk-split-prerequisites-setup-observation-v1",remote_machine_id:$machine,sys_vendor:$vendor,
   shell:{path:"/usr/bin/bash",bytes:$bytes,sha256:$sha},bash_package_verify_exit:0,native_executed:false}' > "$root/setup/observation.json"
timeout -k 2 25 aws s3api put-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/setup-observation.json" --body "$root/setup/observation.json" --if-none-match '*' > "$root/setup/observation-put.json"
printf '%s\n' SETUP_SOURCE_ASSETS_METADATA_READY_NO_NATIVE_EXECUTION
