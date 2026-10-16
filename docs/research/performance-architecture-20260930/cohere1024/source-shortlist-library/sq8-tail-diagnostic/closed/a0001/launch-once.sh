#!/bin/bash
# shellcheck disable=SC2024 # Redirects intentionally write caller-owned evidence.
# Root-owned single launch; never repeat an ambiguous RunInstances response.
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-diagnostic-a0001
proof=/data/target/borsuk-sq8-panel-attribution.pending/launch-reader-gates-a0001
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
jq -e '.controller_launch_authorized==true and .guarded_full_sequence_authorized==true and .total_ceiling_usd<=0.30 and .market=="spot" and .instance_type=="m6a.large" and .instance_count==1 and .source_commit=="898b3fcf82f75085d8b01cd2e462652db34dcd41"' "$out/freeze.json" >/dev/null
[[ -s $out/input-upload.closed && ! -e $out/instance.id && ! -e $out/launch-once.claim ]]
printf '%s  %s\n' f4f8246642d7f70aa62951c29e1b34595ae1233f37d88d1c5d7bc2eef216e53e "$proof/independent-verification.json" | sha256sum --strict -c -
[[ $(cat "$proof/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
jq -e 'length==0' "$out/prior-live-instances.json" >/dev/null
jq -e '.SpotPriceHistory|length>0 and all(.[]; .AvailabilityZone=="eu-central-1a" and .InstanceType=="m6a.large" and (.SpotPrice|tonumber)<=0.10)' "$out/spot-quote.json" >/dev/null
(cd "$out" && sha256sum --strict -c launch-inputs.sha256) > "$out/prelaunch-inputs.verified.log"
timeout -k2 20 aws --profile causality --region eu-central-1 s3api list-objects-v2 --bucket borsuk-bench-453182569524-euc1 --prefix research/semantic-router/20261015/sq8-panel-diagnostic-a0001/terminal/ --max-keys 1 > "$out/prior-output-prefix.json"
jq -e '.KeyCount==0' "$out/prior-output-prefix.json" >/dev/null
set -o noclobber
date -u +%FT%TZ > "$out/launch-once.claim"
set +o noclobber
date +%s > "$out/launch-started.epoch"
timeout -k2 25 aws --profile causality --region eu-central-1 ec2 run-instances --cli-input-json "file://$out/launch-request.json" --user-data "file://$out/launch-user-data.txt" > "$out/launch.stdout" 2> "$out/launch.stderr"
jq -er '.Instances|if length==1 then .[0].InstanceId else error("one instance required") end' "$out/launch.stdout" > "$out/instance.id"
instance=$(cat "$out/instance.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
emergency() {
 timeout -k2 25 aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/emergency-terminate.json" 2> "$out/emergency-terminate.stderr"
}
left=$(( $(cat "$out/launch-started.epoch")+3720-$(date +%s) ))
((left>0)) || { emergency; exit 1; }
if ! sudo -n systemd-run --unit=borsuk-sq8-panel-diagnostic-a0001-external --on-active="${left}s" -p User=rb -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 /usr/bin/timeout -k2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/external-timer-start.log" 2>&1; then emergency; exit 1; fi
if ! sudo -n systemctl is-active borsuk-sq8-panel-diagnostic-a0001-external.timer > "$out/external-timer-active.txt"; then emergency; exit 1; fi
sudo -n systemctl cat borsuk-sq8-panel-diagnostic-a0001-external.timer borsuk-sq8-panel-diagnostic-a0001-external.service > "$out/external-timer-units.txt"
sudo -n systemctl show borsuk-sq8-panel-diagnostic-a0001-external.timer -p ActiveState -p TimersMonotonic -p NextElapseUSecMonotonic > "$out/external-timer-properties.txt"
printf '%s\n' "$instance"
