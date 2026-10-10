#!/usr/bin/env bash
# PENDING: root freezes the run prefix. Monitors only terminal/infra state of the exact original instance, terminates it,
# verifies termination before collecting, and authenticates the archive. No quality parsing, launch or retry.
# Billing bound: every AWS call is clamped to an absolute clock: LaunchTime+7800s (hard stop) then +7920s (120s termination grace);
# the clock origin is the EARLIER of the supplied epoch and the EC2 LaunchTime read first, after cleanup is armed.
# Exit: 0 archive+manager authenticated (chain disposition recorded verbatim in collection.json); 10 terminal missing/invalid;
# 11 termination unconfirmed (also after a glue failure); 12 archive/manifest unauthentic; 13 manager receipt missing/unauthentic;
# 14 archive authenticated but the watcher was LATE (termination after launch+7920s): modeled cost bound UNPROVEN, run INVALID;
# 64 usage/unfrozen; 19 other glue failure (the exact instance was terminated first when the cleanup below was armed).
set -euo pipefail
rc=0; armed=false; issued=false; terminated=false; warned=false; late=false; signalled=
late_report() { # never silent, never claims the cost cap
  echo "watcher LATE: termination attempted after launch+7920s; modeled cost bound UNPROVEN" >&2
  { echo "late termination attempt at $(date +%s); term_stop=$term_stop" > remote-results/LATE_COST_BOUND_UNPROVEN.txt; } 2>/dev/null || true
}
unconfirmed() { # never claims termination; the marker is best effort because the result dir may be unwritable
  { printf 'instance=%s terminate_issued=%s hard_stop=%s term_stop=%s now=%s\n' "$instance" "$issued" "$hard_stop" "$term_stop" "$(date +%s)" > remote-results/TERMINATION_UNCONFIRMED.txt; } 2>/dev/null || true
  if [[ $late == true ]]; then late_report; fi
  printf 'termination unconfirmed within its window: INVALID; root must terminate %s%s\n' "$instance" "$1" >&2
  exit 11
}
on_exit() {
  rc=$?; trap - EXIT; set +e
  case $rc in 0|10|11|12|13|14|64) exit "$rc";; esac
  echo "watcher glue failure (raw exit $rc)" >&2
  if [[ -n $signalled ]]; then echo "signal $signalled: no automatic termination; root must handle ${instance:-the original instance}" >&2
  elif [[ $armed == true && $terminated != true ]]; then
    # Bounded cleanup of the exact original instance; independent of the result dir and of any receipt write.
    terminate_instance 300
    [[ $terminated == true ]] || unconfirmed " (original raw exit $rc)"
    if [[ $late == true ]]; then late_report; fi
    echo "exact original instance $instance terminated after the glue failure; collection skipped: INVALID" >&2
  fi
  exit 19
}
trap on_exit EXIT
trap 'signalled=TERM; exit 143' TERM
trap 'signalled=INT; exit 130' INT
trap 'signalled=HUP; exit 129' HUP
cd -- "$(dirname -- "$0")"
[[ $# == 3 ]] || { echo 'usage: INSTANCE STARTED_EPOCH USER_DATA_SHA256' >&2; exit 64; }
instance=$1 started=$2 user_data_sha=$3
[[ $instance =~ ^i-[0-9a-f]+$ && $started =~ ^[0-9]+$ && $user_data_sha =~ ^[0-9a-f]{64}$ ]] || { echo 'bad arguments' >&2; exit 64; }
bucket=borsuk-bench-453182569524-euc1
prefix=research/semantic-router/20261010/full1000-local-admission-a0002
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ ]] || { echo 'PENDING prefix not frozen' >&2; exit 64; }
supplied_started=$started
hard_stop=$((started + 7800)); term_stop=$((started + 7920)); call_stop=$hard_stop
# One call never outlives call_stop (hard_stop while monitoring, a window end while terminating); no time left => rc 124.
aws_call() { local t=$((call_stop - $(date +%s))); (( t > 0 )) || return 124; (( t > 25 )) && t=25; timeout -k 1 "$t" aws --profile causality --region eu-central-1 --cli-connect-timeout 5 --cli-read-timeout 15 "$@"; }
nap() { local t=$(($1 - $(date +%s))); (( t > 0 )) || return 0; sleep $(( t < $2 ? t : $2 )); }
aws_big() { timeout -k 2 300 aws --profile causality --region eu-central-1 --cli-connect-timeout 5 --cli-read-timeout 60 "$@"; }
# Receipt append is best effort: a failed write is reported once and never blocks termination.
rec() { { printf '%s\n' "$2" >> "remote-results/$1"; } 2>/dev/null || [[ $warned == true ]] || { echo "watcher: receipt write failed ($1); termination not blocked" >&2; warned=true; }; }
state_of() { aws_call ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text > remote-results/state.txt 2>remote-results/state.stderr && cat remote-results/state.txt; }
# Terminate only this original instance (idempotent re-issue until accepted); shared by the normal path and the armed cleanup.
# Past launch+7920s it still makes one bounded 60s best-effort attempt and sets late=true. Optional $1 caps the window.
terminate_instance() {
  local out st now t_end
  now=$(date +%s); t_end=$term_stop
  if (( t_end <= now )); then late=true; t_end=$((now + 60)); fi
  if [[ -n ${1:-} ]] && (( t_end > now + $1 )); then t_end=$((now + $1)); fi
  call_stop=$t_end
  while (( $(date +%s) < t_end )); do
    if [[ $issued == false ]]; then
      out=$(aws_call ec2 terminate-instances --instance-ids "$instance" 2>&1) && issued=true
      rec terminate.json "$out"
    fi
    st=$(aws_call ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text 2>&1) || st=unknown
    rec state.txt "$st"
    if [[ $st == terminated ]]; then terminated=true; break; fi
    nap "$t_end" 5
  done
}
armed=true
# Cleanup is armed; now authenticate the clock origin from EC2 before anything else can fail or wait.
now=$(date +%s); call_stop=$((now + 25))
(( started <= now + 60 )) || { echo "supplied STARTED_EPOCH $started is in the future: stale/invalid clock model" >&2; exit 17; }
launch_raw=$(aws_call ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].LaunchTime' --output text 2>&1) || { echo "EC2 LaunchTime read failed: $launch_raw" >&2; exit 17; }
launch_epoch=$(date -u -d "$launch_raw" +%s 2>/dev/null) || launch_epoch=
[[ $launch_epoch =~ ^[0-9]+$ ]] || { echo "invalid EC2 LaunchTime '$launch_raw'" >&2; exit 17; }
(( launch_epoch <= now + 60 )) || { echo "EC2 LaunchTime '$launch_raw' is in the future: stale/invalid clock model" >&2; exit 17; }
(( launch_epoch >= started )) || started=$launch_epoch
hard_stop=$((started + 7800)); term_stop=$((started + 7920)); call_stop=$hard_stop
mkdir -m 0700 remote-results
printf 'launch_time_raw=%s launch_epoch=%s supplied_started=%s effective_started=%s supplied_minus_launch_seconds=%s\n' "$launch_raw" "$launch_epoch" "$supplied_started" "$started" "$((supplied_started - launch_epoch))" > remote-results/launch-time.txt
reason=deadline
# External hard stop 7800s after the effective start; machine-local stop is 7200s.
while (( $(date +%s) < hard_stop )); do
  if aws_call s3api head-object --bucket "$bucket" --key "$prefix/terminal.json" > remote-results/terminal.head.json 2>remote-results/terminal.head.stderr; then
    reason=terminal
    break
  fi
  state=$(state_of) || state=unknown
  case $state in terminated|stopped|stopping|shutting-down) reason=instance_$state; break;; esac
  nap "$hard_stop" 15
done
printf '%s\n' "$reason" > remote-results/watcher-reason.txt
# A provisional S3 terminal cannot race termination ahead of the manager's final exit receipt: at most 60s, never past hard_stop.
if [[ $reason == terminal ]]; then
  proof_stop=$(( $(date +%s) + 60 )); (( proof_stop <= hard_stop )) || proof_stop=$hard_stop
  call_stop=$proof_stop
  while (( $(date +%s) < proof_stop )); do
    if aws_call s3api head-object --bucket "$bucket" --key "$prefix/bootstrap-manager.json" > remote-results/manager.head.json 2>remote-results/manager.head.stderr; then break; fi
    nap "$proof_stop" 5
  done
  call_stop=$hard_stop
fi
terminate_instance
[[ $terminated == true ]] || unconfirmed ""
if [[ $late == true ]]; then late_report; fi
# Collection after confirmed termination is no longer billing-bound; each call still has its own 25s (small) / 300s (archive) cap.
call_stop=$(($(date +%s) + 3600))
aws_call ec2 get-console-output --instance-id "$instance" --latest > remote-results/console.json 2>remote-results/console.stderr || true
(ulimit -f 64; aws_call s3api get-object --bucket "$bucket" --key "$prefix/terminal.json" remote-results/terminal.json > remote-results/terminal.get.json 2>remote-results/terminal.get.stderr) \
  || { printf 'terminated; terminal missing: execution INVALID\n' >&2; exit 10; }
jq -e --arg instance "$instance" '.schema=="borsuk-native-scale-build-bootstrap-closed-v1" and .instance_id==$instance and .performance_claim==false and .evidence.bytes>0 and .evidence.bytes<=268435456 and (.evidence.sha256|test("^[0-9a-f]{64}$"))' remote-results/terminal.json > /dev/null \
  || { printf 'terminal schema/instance/evidence descriptor invalid: INVALID\n' >&2; exit 10; }
(ulimit -f 262144; aws_big s3api get-object --bucket "$bucket" --key "$prefix/evidence.tar.gz" remote-results/evidence.tar.gz > remote-results/evidence.get.json) || exit 12
[[ $(stat -c %s remote-results/evidence.tar.gz) == "$(jq -er .evidence.bytes remote-results/terminal.json)" ]] || exit 12
[[ $(sha256sum remote-results/evidence.tar.gz | cut -d' ' -f1) == "$(jq -er .evidence.sha256 remote-results/terminal.json)" ]] || exit 12
(ulimit -f 2048; aws_call s3api get-object --bucket "$bucket" --key "$prefix/artifacts.sha256" remote-results/artifacts.sha256 > remote-results/manifest.get.json) || exit 12
[[ $(stat -c %s remote-results/artifacts.sha256) -le 2097152 ]] || exit 12
# Diagnostic-panel closure only; native replay and cold-performance gates remain external.
jq -e '.chain.unit_exit as $rc | .phase=="local-panel-complete" and ($rc==0 or $rc==2 or $rc==3) and .original_exit==$rc and .exit==$rc and .chain.disposition=="LOCAL_PANEL_CLOSED_EXTERNAL_REPLAY_REQUIRED"' remote-results/terminal.json > /dev/null \
  || { printf 'Original terminal is not a closed bootstrap/chain disposition: INVALID\n' >&2; exit 10; }
(ulimit -f 4; aws_call s3api get-object --bucket "$bucket" --key "$prefix/bootstrap-manager.json" remote-results/bootstrap-manager.json > remote-results/manager.get.json) || exit 13
[[ $(stat -c %s remote-results/bootstrap-manager.json) -le 4096 ]] || exit 13
exit_value=$(jq -er .exit remote-results/terminal.json)
# PROSPECTIVE cloud-final mapping (TARGET_UNVERIFIED; the canary must exercise 0, 2 and 3 on the real target): the record's final_exit is
# the nested bootstrap exit and must equal terminal.exit; cloud-final itself reports exit_status "0"/success for 0 and exactly "1"/exit-code for 2|3.
jq -e --arg instance "$instance" --arg sha "$(sha256sum remote-results/terminal.json | cut -d' ' -f1)" --arg x "$exit_value" \
  '.schema=="borsuk-parity-bootstrap-exit-v1" and .instance_id==$instance and .terminal_sha256==$sha and .exit_code=="exited" and .final_exit==$x and (if $x=="0" then (.exit_status=="0" and .service_result=="success") else (.exit_status=="1" and .service_result=="exit-code") end)' remote-results/bootstrap-manager.json > /dev/null \
  || { printf 'Original main exit unverified or disagrees with terminal: INVALID\n' >&2; exit 13; }
# Written last: an INVALID run never leaves a collection.json. manifest_verified:false - the root replay checks the actual manifest.
jq -n --arg instance "$instance" --arg reason "$reason" --arg udsha "$user_data_sha" --argjson late "$late" --argjson supplied "$supplied_started" --argjson effective "$started" --slurpfile t remote-results/terminal.json \
  '{instance_id:$instance,terminated:true,reason:$reason,user_data_sha256:$udsha,archive_authenticated:true,manifest_verified:false,independent_replay_pending:true,late:$late,cost_bound:(if $late then "UNPROVEN_LATE" else "MODELED_7800_PLUS_120" end),supplied_started_epoch:$supplied,effective_started_epoch:$effective,chain_unit_exit:$t[0].chain.unit_exit,chain_disposition:$t[0].chain.disposition,bootstrap_exit:$t[0].exit,performance_claim:false}' > remote-results/collection.json
printf 'Original instance terminated; archive and actual diagnostic bootstrap exit authenticated; independent replay pending\n'
if [[ $late == true ]]; then exit 14; fi
