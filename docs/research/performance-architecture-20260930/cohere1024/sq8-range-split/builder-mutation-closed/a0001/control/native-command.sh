#!/usr/bin/env bash
set -euo pipefail
wrapper_sha=a61cb43a2d2d4cd8d4709eb46995c619688ebb4967040ab2c191c6e2cb50de0d
[[ $wrapper_sha =~ ^[0-9a-f]{64}$ ]] || exit 125
root=/mnt/borsuk-paired-qualification
export AWS_DEFAULT_REGION=eu-central-1 AWS_REGION=eu-central-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
printf '%s  %s\n' "$wrapper_sha" "$root/run-builder-removal.sh" \
 83df03d1fc9b4ddae33a937846b70b911ab09406e1783528ca2d6ff6589f9059 "$root/prove-builder-removal.sh" \
 e8c7c66d19150c6662efcd30975186f5c4e9052f1b5137dd4d1e3a7fecf8802f "$root/source-files.sha256" \
 941df2d29cc844a409f20c0dd22a0829150bf159aab7067c27b163aebe3b3b4d "$root/source/crates/borsuk/src/bin/check_cohere_native_baseline.rs" | sha256sum --strict --check -
deadline=$(cat "$root/machine-stop.epoch")
[[ $deadline =~ ^[1-9][0-9]{9}$ ]]
now=$(date +%s)
(( deadline - now > 320 && deadline - now <= 1800 )) || exit 124
systemctl is-active --quiet borsuk-paired-qualification-shutdown.timer
exec /bin/bash "$root/run-builder-removal.sh" "$root" borsuk-bench-453182569524-euc1 \
 research/semantic-router/20261016/sq8-builder-mutation-a0001/terminal \
 83df03d1fc9b4ddae33a937846b70b911ab09406e1783528ca2d6ff6589f9059 \
 e8c7c66d19150c6662efcd30975186f5c4e9052f1b5137dd4d1e3a7fecf8802f \
 941df2d29cc844a409f20c0dd22a0829150bf159aab7067c27b163aebe3b3b4d \
 8e7076951936b0434b7947afd5b23e192c98fe70 "$deadline" borsuk-sq8-split-mutation-a0001.service
