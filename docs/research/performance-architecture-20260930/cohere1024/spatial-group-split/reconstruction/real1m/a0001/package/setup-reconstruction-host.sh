#!/usr/bin/env bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
cd "$support"
sha256sum -c support.sha256 > package-check.log
systemctl is-active --quiet borsuk-reconstruction-data-shutdown.timer
phase=install
finish() {
 original=$?
 trap - EXIT
 printf 'SETUP_PHASE=%s ORIGINAL_EXIT=%s\n' "$phase" "$original"
 if (( original != 0 )); then
  for f in install.log hydrate.log; do test ! -f "$f" || tail -c 4000 "$f"; done
 fi
 exit "$original"
}
trap finish EXIT
systemd-run --unit=borsuk-reconstruction-install --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=2G -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=570 -p TimeoutStopSec=25 /bin/bash "$support/setup-data.sh" install > install.log 2>&1
phase=identity
token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document > instance-identity.json
jq -e --slurpfile expected execution-identity.json '.instanceId==$expected[0].instance_id and .imageId=="ami-0b8a830d6339a9758" and .region=="eu-central-1" and .instanceType=="c7i.xlarge" and .availabilityZone=="eu-central-1c"' instance-identity.json > /dev/null
phase=hydrate
systemd-run --unit=borsuk-reconstruction-hydrate --slice=borsukreconstruction.slice --wait --pipe -p CPUQuota=200% -p AllowedCPUs=0-1 -p MemoryMax=6G -p MemorySwapMax=0 -p IOAccounting=yes -p TasksMax=128 -p KillMode=control-group -p RuntimeMaxSec=870 -p TimeoutStopSec=25 /bin/bash "$support/setup-data.sh" hydrate > hydrate.log 2>&1
phase=complete
for unit in borsuk-reconstruction-install borsuk-reconstruction-hydrate; do
 cg=/sys/fs/cgroup/borsukreconstruction.slice/$unit.service
 [[ $unit != borsuk-reconstruction-install ]] || cg=/sys/fs/cgroup/system.slice/$unit.service
 if test -d "$cg"; then grep -qx 'populated 0' "$cg/cgroup.events"; fi
done
test "$(cat /sys/fs/cgroup/borsukreconstruction.slice/memory.swap.peak)" = 0
awk '$1 ~ /^oom/ && $2 != 0 {bad=1} END {exit bad}' /sys/fs/cgroup/borsukreconstruction.slice/memory.events
jq -n --slurpfile execution execution-identity.json '{schema:"borsuk-reconstruction-host-setup-v1",status:"ASSETS_READY",execution:$execution[0],native_executed:false,owned_units_drained:true}' > setup-terminal.json
cat setup-terminal.json
