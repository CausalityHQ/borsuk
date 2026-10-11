#!/usr/bin/env bash
set -euo pipefail
root=/mnt/borsuk-sealed-loss-qualification
export AWS_MAX_ATTEMPTS=1 AWS_DEFAULT_REGION=eu-central-1
deadline=$(cat "$root/gate-deadline.epoch")
now=$(date +%s)
(( deadline - now >= 1500 )) || exit 124
transport_deadline=$(( now + 450 ))
mkdir "$root/analysis-inputs"
cd "$root/analysis-inputs"
fetch() {
  local key=$1 file=$2 bytes=$3 sha=$4 left
  left=$(( transport_deadline - $(date +%s) - 2 ))
  (( left > 0 )) || return 124
  (( left <= 300 )) || left=300
  timeout --kill-after=2 "$left" aws s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key" "$file" > /dev/null
  test "$(stat -c %s "$file")" = "$bytes"
  printf '%s  %s\n' "$sha" "$file" | sha256sum -c -
}
fetch research/semantic-router/20261011/scale-closure-panels32-a0001/stages/independent32-r2/independent32-r2-result.jsonl sealed-result.jsonl 1220980 80438eff54335de1e9db5bb66c6ef10c540435cc856a2b84a05b3ea57fbe7e39
fetch research/semantic-router/20261010/full1000-local-admission-a0004/retained-source/semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1 sq8 1036000000 cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1
fetch research/semantic-router/20261010/full1000-preparation-a0002/prepared/truth.u64 truth.u64 80000 0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f
cmp "$root/source/target/release/examples/compare_native_replay" "$root/evidence/release-reducer/compare_native_replay"
sha256sum sealed-result.jsonl sq8 truth.u64 > "$root/evidence/analysis-inputs.sha256"
printf '%s\n' AUTHENTICATED_SEALED_PANEL_SQ8_TRUTH_NO_ANN_RERUN
