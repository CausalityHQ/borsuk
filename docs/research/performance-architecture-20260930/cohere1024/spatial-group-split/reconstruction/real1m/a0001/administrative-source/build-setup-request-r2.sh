#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
cd "$out"
instance=$(cat instance.id)
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
test ! -e setup.ssm-request.json
(cd package && sha256sum -c ../package.source.sha256) > pre-setup-package-check.log
timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$instance" > instance.pre-setup.json
jq -e --arg instance "$instance" '[.Reservations[].Instances[]]|length==1 and .[0].InstanceId==$instance and .[0].State.Name=="running" and .[0].ImageId=="ami-0b8a830d6339a9758" and .[0].InstanceType=="c7i.xlarge" and .[0].InstanceLifecycle=="spot" and .[0].Placement.AvailabilityZone=="eu-central-1c" and .[0].ClientToken=="borsuk-reconstruction-real1m-a0001-91107713"' instance.pre-setup.json > /dev/null
request_sha=$(sha256sum launch-request.json); request_sha=${request_sha%% *}
package_sha=$(sha256sum package.source.sha256); package_sha=${package_sha%% *}
jq -n --arg instance "$instance" --arg request "$request_sha" --arg package "$package_sha" '{schema:"borsuk-reconstruction-real1m-execution-v1",attempt:"reconstruction-real1m-a0001",instance_id:$instance,source_commit:"91107713f6f7bd700b5b6d92a83e438febac8069",published_commit:"319f8c4bbac69f522fc25a0ecd93f2e9232e9b84",native_source_identity_sha256:"bef40caf201f947718554128b544cb77feedf907b897faa59d32e8d36bf62772",request_sha256:$request,package_manifest_sha256:$package,guest_deadline_seconds:7200,external_deadline_seconds:7320,total_cost_ceiling_usd:0.50}' > package/execution-identity.json
(cd package && sha256sum *.sh *.json admission.config.sha256 smoke.config.sha256 parity.config.sha256 approval.sha256 campaign-prefix.txt) > package/support.sha256
{
 printf '%s\n' '#!/bin/bash' 'set -euo pipefail' 'support=/opt/borsuk-reconstruction-gate' 'test ! -e "$support"' 'mkdir "$support"'
 for file in package/*; do
  name=${file##*/}
  [[ $name =~ ^[a-zA-Z0-9._-]+$ ]]
  test "$(tail -c 1 "$file" | xxd -p)" = 0a
  if rg -qx BORSUK_RECONSTRUCTION_FIXED_ASSET_END "$file"; then exit 125; fi
  printf 'cat > "$support/%s" <<\x27BORSUK_RECONSTRUCTION_FIXED_ASSET_END\x27\n' "$name"
  cat "$file"
  printf 'BORSUK_RECONSTRUCTION_FIXED_ASSET_END\n'
 done
 printf '%s\n' '(cd "$support" && sha256sum -c support.sha256)' 'left=$(( $(cat /run/borsuk-reconstruction-machine-stop.epoch)-$(date +%s)-150 ))' '(( left >= 1200 ))' 'timeout -k 10 1190 systemd-run --unit=borsuk-reconstruction-setup-driver --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=512M -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=10 -p RuntimeMaxSec=1180 /bin/bash "$support/setup-reconstruction-host.sh"'
} > setup.ssm-command.sh
bash -n setup.ssm-command.sh
jq -n --arg instance "$instance" --rawfile command setup.ssm-command.sh '{InstanceIds:[$instance],DocumentName:"AWS-RunShellScript",Comment:"borsuk real1m reconstruction a0001 opaque transport only",TimeoutSeconds:1200,Parameters:{executionTimeout:["1200"],commands:[$command]}}' > setup.ssm-request.json
sha256sum setup.ssm-command.sh setup.ssm-request.json package/execution-identity.json package/support.sha256 > setup.ssm-inputs.sha256
