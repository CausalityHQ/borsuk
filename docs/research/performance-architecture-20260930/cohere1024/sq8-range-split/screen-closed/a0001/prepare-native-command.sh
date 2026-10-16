#!/bin/bash
# Local metadata assembly, never invokes the generated remote command.
set -euo pipefail
set -C
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-split-screen-a0001
master_sha=$(sha256sum "$out/master.json"); master_sha=${master_sha%% *}
{
 cat <<'OPEN'
#!/bin/bash
set -euo pipefail
set -C
umask 077
root=/mnt/borsuk-sq8-split-panel
cat > "$root/master.json" <<'MASTER_METADATA_EOF'
OPEN
 cat "$out/master.json"
 printf 'MASTER_METADATA_EOF\n'
 # Variables are literal generated remote source.
 # shellcheck disable=SC2016
 printf 'printf "%%s  %%s\\n%%s  %%s\\n" %q "$root/master.json" %q "$root/support/publish-sq8-split-panel.sh" | sha256sum --strict --check -\n' "$master_sha" 39a9b9f5785944d7294986d5d6b2f85715d6b1af54a0baae1b2b2e0eb9297ebd
 # shellcheck disable=SC2016
 printf 'exec /bin/bash "$root/support/publish-sq8-split-panel.sh" "$root" "$root/master.json" %q %q\n' "$master_sha" a65283d5291f660b99c8c96773054b373c0cbeb9a0dd227b3e95b13904f5f644
} > "$out/native-command.sh"
awk '/^cat > .*MASTER_METADATA_EOF/ {on=1; next} on && /^MASTER_METADATA_EOF$/ {on=0; next} on {print}' "$out/native-command.sh" > "$out/embedded-master.check.json"
cmp "$out/master.json" "$out/embedded-master.check.json"
bash -n "$out/native-command.sh"
shellcheck "$out/native-command.sh"
(cd "$out"; sha256sum master.json native-command.sh support-bundle.tar.gz setup-original.json setup-observation.json scientific-freeze.pending/receipts/scientific-freeze.json) > "$out/native-inputs.sha256"
