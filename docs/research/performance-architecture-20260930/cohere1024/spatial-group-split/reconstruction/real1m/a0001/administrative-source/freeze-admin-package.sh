#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
old=/data/target/borsuk-source-neighborhood-data-gates-a0002.pending
pkg=$out/package
test ! -e "$out/launch-request.json"
cp "$out/native-reconstruction-inner.sh" "$out/run-reconstruction-stage.sh" "$pkg/"
jq '.BlockDeviceMappings[0].Ebs.VolumeSize=32
 | .InstanceMarketOptions.SpotOptions.MaxPrice="0.15"
 | .ClientToken="borsuk-reconstruction-real1m-a0001-91107713"
 | .TagSpecifications[0].Tags=[{Key:"Name",Value:"borsuk-reconstruction-real1m-a0001"},{Key:"borsuk-source",Value:"91107713f6f7bd700b5b6d92a83e438febac8069"},{Key:"borsuk-goal",Value:"partition-reconstruction-real1m"}]' "$old/launch-request.json" > "$out/launch-request.json"
sed -e 's@borsuk-source-neighborhood-data-gates-a0002.pending@borsuk-reconstruction-real1m.pending@g' \
 -e 's/borsuk-neighborhood-data-gates-a0002-fc60c18e/borsuk-reconstruction-real1m-a0001-91107713/g' \
 "$old/stop-original-token.sh" > "$out/stop-original-token.sh"
sed -e 's@borsuk-source-neighborhood-data-gates-a0002.pending@borsuk-reconstruction-real1m.pending@g' \
 -e 's/borsuk-neighborhood-data-a0002-external/borsuk-reconstruction-real1m-a0001-external/g' \
 -e 's/9590/7320/g' "$old/launch-once.sh" > "$out/launch-once.sh"
sed -e 's/borsuk-neighborhood/borsuk-reconstruction/g' -e 's/9570/7200/g' "$old/launch-user-data.txt" > "$out/launch-user-data.txt"
(cd "$pkg" && sha256sum *.sh *.json *.sha256 campaign-prefix.txt) > "$out/package.source.sha256"
(cd "$out" && sha256sum launch-request.json launch-user-data.txt launch-once.sh stop-original-token.sh package.source.sha256 resource-envelope.draft.md) > "$out/launch-inputs.sha256"
# Syntax/static checks only; none of these experiment scripts is executed locally.
for f in "$pkg"/*.sh "$out/launch-once.sh" "$out/stop-original-token.sh" "$out/launch-user-data.txt"; do
 bash -n "$f"
done
shellcheck -S error "$pkg"/*.sh "$out/launch-once.sh" "$out/stop-original-token.sh"
jq -e '.authority_pending==false and (.inputs|length)==5 and .binary.pin.bytes==8387384 and ([.inputs[].role]|sort)==["generation_manifest","leaves","membership","plane_manifest","router_root"]' "$pkg/transport.json" > "$out/transport-static-check.log"
test "$(wc -c < "$out/launch-user-data.txt")" -lt 16384
printf 'ADMIN_PACKAGE_STATIC_CHECKS_ONLY_NO_NATIVE_OR_LAUNCH\n'
