#!/bin/bash
set -Eeuo pipefail
export AWS_MAX_ATTEMPTS=1
umask 077
cd /tmp/borsuk-scale-parity-qualification-a0002
set -o noclobber
printf '%s\n' "$(date -u +%FT%TZ)" > launch.claim
set +o noclobber
instance=
terminate() {
 [[ -n $instance ]] || return 1
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$instance" >terminate.json
 timeout -k 2 120 aws --profile causality --region eu-central-1 ec2 wait instance-terminated --instance-ids "$instance"
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$instance" >terminated.json
}
abort() { rc=$?; trap - EXIT; set +e
 if [[ -z $instance ]]; then
  timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --filters 'Name=client-token,Values=borsuk-parity-qualification-a0002-1791650823' >ambiguous-launch.lookup.json
  instance=$(jq -er '[.Reservations[].Instances[]] | select(length==1) | .[0].InstanceId' ambiguous-launch.lookup.json)
 fi
 [[ -z $instance ]] || terminate
 exit "$rc"
}
trap abort EXIT
set +e
timeout -k 2 40 aws --profile causality --region eu-central-1 ec2 run-instances --cli-input-json file://launch-request.json >launch-response.json 2>launch.stderr
launch_rc=$?
set -e
printf '%s\n' "$launch_rc" >launch.exit
instance=$(jq -er '.Instances | select(length==1) | .[0].InstanceId' launch-response.json)
[[ $instance =~ ^i-[0-9a-f]+$ ]]
printf '%s\n' "$instance" >instance-id
[[ $launch_rc == 0 ]]
for observation in $(seq 1 12); do
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$instance" >"instance-running-${observation}.json"
 if jq -e --arg instance "$instance" '[.Reservations[].Instances[]] | length==1 and .[0].InstanceId==$instance and (.[0].BlockDeviceMappings|length)==1 and .[0].BlockDeviceMappings[0].Ebs.DeleteOnTermination==true and (.[0].BlockDeviceMappings[0].Ebs.VolumeId|test("^vol-[0-9a-f]+$"))' "instance-running-${observation}.json" >/dev/null; then
  jq -er '.Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' "instance-running-${observation}.json" >root-volume-id
  break
 fi
 sleep 5
done
test -s root-volume-id
deadline=$(( $(date +%s) + 9000 ))
key=research/native-library-check/20261010/scale-prefix-parity-a0002/output
mkdir collected
found=0
while (( $(date +%s) < deadline )); do
 if timeout -k 2 15 aws --profile causality --region eu-central-1 s3api head-object --bucket borsuk-bench-453182569524-euc1 --key "$key/terminal.json" >terminal.head.json 2>terminal.head.stderr; then found=1; break; fi
 sleep 30
done
terminate
trap - EXIT
volume=$(cat root-volume-id)
volume_absent=0
for observation in $(seq 1 12); do
 set +e
 timeout -k 2 25 aws --profile causality --region eu-central-1 ec2 describe-volumes --volume-ids "$volume" >"volume-after-${observation}.json" 2>"volume-after-${observation}.stderr"
 volume_rc=$?
 set -e
 printf '%s\n' "$volume_rc" >"volume-after-${observation}.exit"
 if [[ $volume_rc == 254 ]] && grep -Fq 'InvalidVolume.NotFound' "volume-after-${observation}.stderr" && grep -Fq "$volume" "volume-after-${observation}.stderr"; then volume_absent=1; break; fi
 [[ $volume_rc == 0 ]] || exit 11
 sleep 5
done
[[ $volume_absent == 1 ]] || exit 11
printf '%s\n' ROOT_VOLUME_EXACT_ID_ABSENT >volume-cleanup.txt
for name in evidence.tar.gz terminal.json; do
 timeout -k 2 120 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$key/$name" "collected/$name" >"collected/$name.get.json" 2>"collected/$name.get.stderr" || exit 10
done
[[ $found == 1 ]]
sha256sum collected/evidence.tar.gz collected/terminal.json >collected/SHA256SUMS
printf '%s\n' CLOSED_NATIVE_QUALIFICATION_COLLECTED >state.txt
