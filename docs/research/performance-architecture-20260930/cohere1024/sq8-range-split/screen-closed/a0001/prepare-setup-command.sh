#!/usr/bin/env bash
# Local source assembly only; never invokes the assembled remote command.
set -euo pipefail
prep=/data/target/borsuk-sq8-panel-attribution.pending/split-prerequisite-preparation.pending
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
assets=/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending/remote-assets.draft.json
[[ ! -e $out/setup-command.sh.pending ]]
stage_sha=$(sha256sum "$prep/stage-assets.sh"); stage_sha=${stage_sha%% *}
setup_sha=$(sha256sum "$prep/setup-runtime.sh"); setup_sha=${setup_sha%% *}
assets_sha=$(sha256sum "$assets"); assets_sha=${assets_sha%% *}
bundle_sha=$(sha256sum "$out/support-bundle.pending.tar.gz"); bundle_sha=${bundle_sha%% *}
bundle_bytes=$(stat -c %s "$out/support-bundle.pending.tar.gz")
{
 cat <<'OPEN'
#!/usr/bin/env bash
set -euo pipefail
set -C
umask 077
bootstrap=/mnt/borsuk-sq8-split-bootstrap
[[ ! -e $bootstrap && ! -L $bootstrap ]]
mkdir "$bootstrap"
cd "$bootstrap"
cat > stage-assets.sh <<'STAGE_ASSETS_SOURCE_EOF'
OPEN
 cat "$prep/stage-assets.sh"
 printf 'STAGE_ASSETS_SOURCE_EOF\ncat > setup-runtime.sh <<\x27SETUP_RUNTIME_SOURCE_EOF\x27\n'
 cat "$prep/setup-runtime.sh"
 printf 'SETUP_RUNTIME_SOURCE_EOF\ncat > assets.json <<\x27ASSETS_METADATA_EOF\x27\n'
 cat "$assets"
 printf 'ASSETS_METADATA_EOF\n'
 # Variables belong to the generated remote source.
 # shellcheck disable=SC2016
 printf 'bash "$bootstrap/setup-runtime.sh" "$bootstrap/stage-assets.sh" %q "$bootstrap/assets.json" %q\n' "$stage_sha" "$assets_sha"
 # The setup source is also pinned independently before execution.
} > "$out/setup-command.sh.pending.body"
# Insert its own setup pin immediately before the invocation, not after it.
sed '$d' "$out/setup-command.sh.pending.body" > "$out/setup-command.sh.pending"
{
 # Variables belong to the generated remote source.
 # shellcheck disable=SC2016
 printf 'printf "%%s  %%s\\n" %q "$bootstrap/setup-runtime.sh" | sha256sum --strict --check -\n' "$setup_sha"
 tail -n 1 "$out/setup-command.sh.pending.body"
 printf 'bundle_sha=%q\nbundle_bytes=%q\n' "$bundle_sha" "$bundle_bytes"
 cat <<'CLOSE'
root=/mnt/borsuk-sq8-split-panel
prefix=research/semantic-router/20261016/sq8-split-screen-a0001
export AWS_DEFAULT_REGION=eu-central-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
timeout -k 2 45 aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$prefix/inputs/support-bundle.tar.gz" "$bootstrap/support-bundle.tar.gz" > "$bootstrap/bundle-get.json"
[[ $(stat -c %s "$bootstrap/support-bundle.tar.gz") == "$bundle_bytes" ]]
printf '%s  %s\n' "$bundle_sha" "$bootstrap/support-bundle.tar.gz" | sha256sum --strict --check -
for name in authority runtime support receipts; do [[ ! -e $root/$name && ! -L $root/$name ]]; done
timeout -k 2 20 tar -tzvf "$bootstrap/support-bundle.tar.gz" > "$bootstrap/bundle-members.txt"
timeout -k 2 20 tar --no-same-owner --no-same-permissions -xzf "$bootstrap/support-bundle.tar.gz" -C "$root"
metadata=/data/target/borsuk-sq8-panel-attribution.pending/split-runtime.pending
[[ ! -e $metadata && ! -L $metadata ]]
mkdir -p "$metadata"
[[ $(realpath -e "$metadata") == "$metadata" ]]
for name in C1.runtime.json C1.expected.json; do
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
CLOSE
} >> "$out/setup-command.sh.pending"
mkdir "$out/embedded-source-check"
check_embedded() {
 local name=$1 marker=$2 original=$3
 awk -v name="$name" -v marker="$marker" '
  $0==("cat > " name " <<" sprintf("%c",39) marker sprintf("%c",39)) {starts++; active=1; next}
  active && $0==marker {ends++; active=0; next}
  active {print}
  END {if(starts!=1 || ends!=1 || active) exit 1}
 ' "$out/setup-command.sh.pending" > "$out/embedded-source-check/$name"
 cmp "$original" "$out/embedded-source-check/$name"
}
check_embedded stage-assets.sh STAGE_ASSETS_SOURCE_EOF "$prep/stage-assets.sh"
check_embedded setup-runtime.sh SETUP_RUNTIME_SOURCE_EOF "$prep/setup-runtime.sh"
check_embedded assets.json ASSETS_METADATA_EOF "$assets"
printf '%s\n' EMBEDDED_THREE_INPUTS_BYTE_IDENTICAL > "$out/embedded-source-check/verified.txt"
