#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
old=/data/target/borsuk-source-neighborhood-data-gates-a0002.pending/package
test ! -e "$out/package"
mkdir "$out/package"
pkg=$out/package
# Reuse the successfully executed, hash-pinned install/hydration mechanics.
sed -e 's@/opt/borsuk-neighborhood-gate@/opt/borsuk-reconstruction-gate@g' \
 -e 's@/mnt/borsuk-neighborhood-data@/mnt/borsuk-reconstruction-real1m@g' \
 -e 's/borsukneighborhood/borsukreconstruction/g' \
 -e 's/borsuk-neighborhood/borsuk-reconstruction/g' \
 -e 's/8589934592/2147483648/g' \
 -e 's/borsuk-source-neighborhood-transport-v1/borsuk-reconstruction-inputs-v1/g' \
 -e 's/length)==6/length)==5/g' \
 -e 's/\["canonical","generation_manifest","leaves","membership","plane_manifest","router_root"\]/["generation_manifest","leaves","membership","plane_manifest","router_root"]/g' \
 "$old/setup-data.sh" > "$pkg/setup-data.sh"
jq '.schema="borsuk-reconstruction-inputs-v1"|.authority_pending=false|del(.qualification)' \
 "$out/transport.qualified-binary.draft.json" > "$pkg/transport.json"
for phase in admission smoke parity; do
 cp "$out/$phase.config.draft.json" "$pkg/$phase.config.json"
 hash=$(sha256sum "$pkg/$phase.config.json"); printf '%s\n' "${hash%% *}" > "$pkg/$phase.config.sha256"
done
cp "$out/original-approval.draft.json" "$pkg/approval.json"
hash=$(sha256sum "$pkg/approval.json"); printf '%s\n' "${hash%% *}" > "$pkg/approval.sha256"
printf '%s\n' research/semantic-router/20261011/reconstruction-real1m-a0001 > "$pkg/campaign-prefix.txt"
printf 'EXECUTION_PACKAGE_PREPARED_NOT_LAUNCHED\n'
