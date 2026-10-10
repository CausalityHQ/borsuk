#!/usr/bin/env bash
# Root-owned observer. Run once in an independent service before yielding.
set -Eeuo pipefail
umask 077
export AWS_PROFILE=causality AWS_DEFAULT_REGION=eu-central-1 AWS_PAGER='' AWS_MAX_ATTEMPTS=1
instance=${1:?}; prefix=${2:?}; out=${3:?}; launched=${4:?}; bootstrap=${5:?}
[[ $# == 5 && $instance =~ ^i-[0-9a-f]{17}$ && $prefix == research/* && $out == /* && ! -e $out && ! -L $out && $launched =~ ^[0-9]{10}$ && $bootstrap =~ ^[0-9a-f]{64}$ ]] || exit 125
bucket=borsuk-bench-453182569524-euc1
terminated=false evidence_failed=0
terminate_owned() {
    if [[ $terminated == false ]]; then
        local attempt rc=1 stdout_fd stderr_fd
        for attempt in 1 2 3 4; do
            rc=0
            # Failed receipt storage must never prevent the exact-instance API call.
            exec {stdout_fd}> "$out/terminate.$attempt.json" || { evidence_failed=1; exec {stdout_fd}>/dev/null; }
            exec {stderr_fd}> "$out/terminate.$attempt.stderr" || { evidence_failed=1; exec {stderr_fd}>/dev/null; }
            timeout -k 1 10 aws ec2 terminate-instances --instance-ids "$instance" 1>&"$stdout_fd" 2>&"$stderr_fd" || rc=$?
            exec {stdout_fd}>&-; exec {stderr_fd}>&-
            ((rc==0)) && break
            [[ $rc == 254 ]] && grep -F 'InvalidInstanceID.NotFound' "$out/terminate.$attempt.stderr" >/dev/null || return 1
            sleep 2
        done
        ((rc==0)) || return 1
        exec {stdout_fd}> "$out/terminated.wait.stdout" || { evidence_failed=1; exec {stdout_fd}>/dev/null; }
        exec {stderr_fd}> "$out/terminated.wait.stderr" || { evidence_failed=1; exec {stderr_fd}>/dev/null; }
        rc=0
        timeout -k 1 120 aws ec2 wait instance-terminated --instance-ids "$instance" 1>&"$stdout_fd" 2>&"$stderr_fd" || rc=$?
        exec {stdout_fd}>&-; exec {stderr_fd}>&-
        ((rc==0)) || return 1
        terminated=true
    fi
}
trap 'rc=$?; trap - EXIT; terminate_owned || { devbox-tell "Validator cleanup is unproven for exact instance $instance; receipts $out"; rc=94; }; ((evidence_failed==0)) || rc=94; exit "$rc"' EXIT
# Cleanup is installed before any fallible receipt or age initialization.
mkdir "$out"
age=$(($(date +%s)-launched)); ((age>=0 && age<=90)) || exit 125
# Request termination by launch+2500; allow bounded confirmation afterward.
deadline=$((SECONDS+2500-age))
printf '%s\n' 'termination_request_by_launch_plus_2500; confirmation_wait_120s; not_a_guarantee_of_AWS_completion' > "$out/deadlines.txt"
volume=''
for attempt in 1 2 3 4 5 6; do
    rc=0
    timeout -k 1 10 aws ec2 describe-instances --instance-ids "$instance" > "$out/discovery.$attempt.json" 2> "$out/discovery.$attempt.stderr" || rc=$?
    if ((rc==0)); then
        jq -e --arg i "$instance" '[.Reservations[].Instances[]]|length==1 and .[0].InstanceId==$i' "$out/discovery.$attempt.json" >/dev/null
        volume=$(jq -r '[.Reservations[].Instances[].BlockDeviceMappings[].Ebs.VolumeId]|if length==1 then .[0] else "" end' "$out/discovery.$attempt.json")
        if [[ $volume =~ ^vol-[0-9a-f]{17}$ ]]; then cp "$out/discovery.$attempt.json" "$out/instance.before.json"; break; fi
    else
        [[ $rc == 254 ]] && grep -F 'InvalidInstanceID.NotFound' "$out/discovery.$attempt.stderr" >/dev/null || exit 94
    fi
    sleep 2
done
[[ $volume =~ ^vol-[0-9a-f]{17}$ ]] || exit 125
terminal=false
while ((SECONDS<deadline)); do
    rc=0
    left=$((deadline-SECONDS-2)); ((left>0)) || break
    cap=10; ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" aws s3api head-object --bucket "$bucket" --key "$prefix/terminal.json" > "$out/terminal.head.json" 2> "$out/terminal.head.stderr" || rc=$?
    if ((rc==0)); then terminal=true; break; fi
    if ! grep -Eq '\(404\)|\(NotFound\)|\(NoSuchKey\)' "$out/terminal.head.stderr"; then exit 94; fi
    left=$((deadline-SECONDS)); ((left>0)) || break
    cap=5; ((cap<=left)) || cap=$left
    sleep "$cap"
done
printf '%s\n' "$terminal" > "$out/terminal.observed"
terminate_owned
timeout -k 2 15 aws ec2 describe-instances --instance-ids "$instance" > "$out/instance.after.json"
jq -e --arg i "$instance" '[.Reservations[].Instances[]]|length==1 and .[0].InstanceId==$i and .[0].State.Name=="terminated"' "$out/instance.after.json" >/dev/null
absent=false
for attempt in 1 2 3 4 5; do
    rc=0
    timeout -k 1 8 aws ec2 describe-volumes --volume-ids "$volume" > "$out/volume.$attempt.json" 2> "$out/volume.$attempt.stderr" || rc=$?
    if [[ $rc == 254 ]] && grep -F 'InvalidVolume.NotFound' "$out/volume.$attempt.stderr" >/dev/null; then absent=true; break; fi
    ((rc==0)) || break
    sleep 2
done
printf '%s\n' "$absent" > "$out/volume.absent"
[[ $absent == true && $evidence_failed == 0 ]] || exit 94
[[ $terminal == true ]] || exit 94
jq -e '.ContentLength>0 and .ContentLength<=65536 and (.ETag|type)=="string"' "$out/terminal.head.json" >/dev/null
etag=$(jq -r .ETag "$out/terminal.head.json")
timeout -k 2 30 aws s3api get-object --bucket "$bucket" --key "$prefix/terminal.json" --if-match "$etag" "$out/terminal.json" > "$out/terminal.download.json"
[[ $(stat -c %s "$out/terminal.json") == "$(jq -r .ContentLength "$out/terminal.head.json")" ]] || exit 94
jq -se --arg i "$instance" --arg p "$prefix" --arg b "$bootstrap" 'length==1 and (.[0]|type)=="object" and .[0].instance_id==$i and .[0].prefix==$p and .[0].bootstrap_sha256==$b' "$out/terminal.json" > "$out/terminal.identity.validated"
jq -e 'type=="object" and .schema=="borsuk-validator-ec2-v1" and .ann_executed==false and .native_cli_usage_only==true and .native_execution_status=="CLI_USAGE_ONLY_PENDING_ROOT_REPLAY" and .performance_claim==false and .production_qualification==false and (.evidence_sha256|test("^[0-9a-f]{64}$")) and (.evidence_bytes|type)=="number" and .evidence_bytes>0 and .evidence_bytes<=268435456' "$out/terminal.json" >/dev/null
sha=$(jq -r .evidence_sha256 "$out/terminal.json")
timeout -k 1 10 aws s3api head-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" > "$out/evidence.head.json"
[[ $(jq -r .ContentLength "$out/evidence.head.json") == "$(jq -r .evidence_bytes "$out/terminal.json")" ]] || exit 94
etag=$(jq -er .ETag "$out/evidence.head.json")
timeout -k 2 30 aws s3api get-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" --if-match "$etag" "$out/evidence.tar.gz" > "$out/evidence.download.json"
printf '%s  %s\n' "$sha" "$out/evidence.tar.gz" | sha256sum -c - > "$out/evidence.authenticated"
[[ $(stat -c %s "$out/evidence.tar.gz") == "$(jq -r .evidence_bytes "$out/terminal.json")" ]] || exit 94
printf '%s\n' 'COLLECTED_CANARY_EVIDENCE_ONLY_ROOT_REPLAY_PENDING' > "$out/root.status"
sync -f "$out"
[[ $absent == true ]] || exit 94
