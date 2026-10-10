#!/usr/bin/env bash
# Scratch launch binding v2: schema and metadata validation ONLY (SHA-pinned support asset, fetched through support.sha256).
# The unique-serial, exact-size, root-disk exclusion, signature/mount/swap, blkid and pre-mkfs recheck guards stay in user-data.sh.
# usage: validate-scratch-binding.sh INSTANCE_ID scratch-launch-binding.json   stdout: "<scratch volume id> <root volume id>"; any failure exits nonzero.
set -Eeuo pipefail
[[ $# == 2 && $1 =~ ^i-[0-9a-f]+$ && $2 == scratch-launch-binding.json && -f $2 && ! -L $2 ]]
tok=$(curl -fsS --connect-timeout 5 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 30' http://169.254.169.254/latest/api/token)
az=$(curl -fsS --connect-timeout 5 --max-time 5 -H "X-aws-ec2-metadata-token: $tok" http://169.254.169.254/latest/meta-data/placement/availability-zone)
[[ $az =~ ^[a-z]{2}-[a-z]+-[0-9][a-z]$ ]]
IFS= read -r -d '' prog <<'EOF' || true
select(length == 1) | .[0] | select(
  (keys == ["attach_time_epoch","attached_device","availability_zone","create_time_epoch","delete_on_termination","describe_instances_sha256","describe_volumes_sha256","device","encrypted","instance_id","launch_time_epoch","multi_attach","root_volume_id","schema","size_bytes","snapshot_empty","state","volume_id","volume_type"])
  and .schema == "borsuk-scratch-launch-binding-v2" and .instance_id == $i and .availability_zone == $az
  and .device == "/dev/sdf" and .attached_device == "/dev/sdf" and .size_bytes == 42949672960 and .volume_type == "gp3"
  and .encrypted == true and .multi_attach == false and .snapshot_empty == true and .delete_on_termination == true and .state == "in-use"
  and ([.volume_id, .root_volume_id] | all(test("^vol-[0-9a-f]{8,17}$")) and .[0] != .[1])
  and ([.create_time_epoch, .attach_time_epoch, .launch_time_epoch] | all(type == "number" and . > 0 and floor == .))
  and .create_time_epoch >= .launch_time_epoch - 60 and .create_time_epoch <= .launch_time_epoch + 60
  and .attach_time_epoch >= .create_time_epoch - 5 and .attach_time_epoch <= .launch_time_epoch + 120
  and ([.describe_instances_sha256, .describe_volumes_sha256] | all(test("^[0-9a-f]{64}$")))
) | "\(.volume_id) \(.root_volume_id)"
EOF
jq -ers --arg i "$1" --arg az "$az" "$prog" "$2"
