#!/usr/bin/env bash
# Refuses until root has admitted and frozen every input. One original launch.
# shellcheck disable=SC2024
set -euo pipefail
out=/data/target/borsuk-sq8-panel-attribution.pending/launch-builder-mutation-a0002
export AWS_MAX_ATTEMPTS=1 AWS_PAGER=''
jq -e '
 .admitted==true and .launch_authorized==true and .source_verified==true
 and .runtime_static_verified==true and .mutation_falsifier_only==true
 and .source_commit=="8e7076951936b0434b7947afd5b23e192c98fe70"
 and .source_files==480 and .native_files==447
 and .probe_sha256=="83df03d1fc9b4ddae33a937846b70b911ab09406e1783528ca2d6ff6589f9059"
 and (.wrapper_sha256|type=="string" and test("^[0-9a-f]{64}$"))
 and .market=="spot" and .spot_ceiling_usd_hour==0.415
 and .machine_seconds==1440 and .external_seconds==1560
 and .cargo_build_jobs==2 and .memory_bytes==60129542144 and .swap_bytes==0 and .pids==512
 and .campaign_ceiling_usd==1 and .campaign_upper_usd<=0.989418862
 and .termination_accounting_allowance_seconds==120
 and .ancillary_total_reserved_usd==0.15 and .performance_run==false
' "$out/launch-admission.json" > /dev/null
jq -e '
 .ClientToken=="borsuk-sq8-builder-mutation-a0002-8e707695"
 and .MinCount==1 and .MaxCount==1 and .InstanceType=="m6a.4xlarge"
 and .ImageId=="ami-0b8a830d6339a9758"
 and .InstanceInitiatedShutdownBehavior=="terminate"
 and .InstanceMarketOptions=={MarketType:"spot",SpotOptions:{InstanceInterruptionBehavior:"terminate",MaxPrice:"0.415",SpotInstanceType:"one-time"}}
 and .BlockDeviceMappings==[{DeviceName:"/dev/sda1",Ebs:{DeleteOnTermination:true,Encrypted:true,Iops:3000,Throughput:125,VolumeSize:96,VolumeType:"gp3"}}]
 and ([.TagSpecifications[].Tags[]|select(.Key=="borsuk-source")|.Value]==["8e7076951936b0434b7947afd5b23e192c98fe70"])
' "$out/launch-request.json" > /dev/null
pending_rc=0
grep -Fq PENDING_ "$out/launch-request.json" "$out/setup-command.sh" "$out/native-command.sh" || pending_rc=$?
[[ $pending_rc == 1 ]]
[[ $(cat "$out/input-upload.closed") == SOURCE_AND_RUNTIME_INPUTS_CREATE_ONLY_PUBLISHED ]]
(cd "$out" && sha256sum --strict --check launch-inputs.sha256) > "$out/prelaunch-inputs.verified.log"
quote_epoch=$(cat "$out/quote-observed.epoch")
now=$(date +%s)
[[ $quote_epoch =~ ^[0-9]+$ ]] && (( now >= quote_epoch && now-quote_epoch <= 300 ))
jq -e '(.NextToken // "")=="" and (.SpotPriceHistory|length>0 and all(.[];.AvailabilityZone=="eu-central-1a" and .InstanceType=="m6a.4xlarge" and (.SpotPrice|tonumber)<=0.415))' "$out/spot-quote.json" > "$out/quote-admission.txt"
for attempt in a0001 a0002 a0003; do
 [[ $(cat "/data/target/borsuk-sq8-panel-attribution.pending/launch-split-gates-$attempt/compute.closed") == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
done
[[ $(cat /data/target/borsuk-sq8-panel-attribution.pending/launch-builder-mutation-a0001/compute.closed) == INSTANCE_TERMINATED_VOLUME_ABSENT_TIMER_INACTIVE ]]
token=$(jq -er .ClientToken "$out/launch-request.json")
timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --filters "Name=client-token,Values=$token" > "$out/token-preflight.json" 2> "$out/token-preflight.stderr"
jq -e '[.Reservations[].Instances[]]|length==0' "$out/token-preflight.json" > /dev/null
[[ ! -e $out/instance.id && ! -e $out/launch-once.claim ]]
(set -o noclobber; date -u +%FT%TZ > "$out/launch-once.claim")
date +%s > "$out/launch-started.epoch"
timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 run-instances --cli-input-json "file://$out/launch-request.json" --user-data "file://$out/launch-user-data.txt" > "$out/launch.stdout" 2> "$out/launch.stderr"
jq -er '.Instances|if length==1 then .[0].InstanceId else error("one instance required") end' "$out/launch.stdout" > "$out/instance.id"
instance=$(cat "$out/instance.id")
[[ $instance =~ ^i-[0-9a-f]{17}$ ]]
emergency() {
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/emergency-terminate.json" 2> "$out/emergency-terminate.stderr"
}
left=$(( $(cat "$out/launch-started.epoch") + 1560 - $(date +%s) ))
if (( left <= 0 )); then emergency; exit 124; fi
if ! sudo -n systemd-run --unit=borsuk-sq8-builder-mutation-a0002-external --on-active="${left}s" -p User=rb -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 /usr/bin/timeout -k 2 25 /usr/local/bin/aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" > "$out/external-timer-start.log" 2>&1; then emergency; exit 98; fi
if ! sudo -n systemctl is-active borsuk-sq8-builder-mutation-a0002-external.timer > "$out/external-timer-active.txt"; then emergency; exit 98; fi
printf '%s\n' "$instance"
