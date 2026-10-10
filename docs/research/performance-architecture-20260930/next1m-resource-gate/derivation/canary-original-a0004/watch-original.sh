#!/usr/bin/env bash
# PENDING: root freezes the run prefix. Monitors only terminal/infra state of the exact original instance, terminates it,
# verifies termination before collecting, and authenticates the archive. No quality parsing, launch or retry.
# Billing bound: every AWS call is clamped to an absolute clock: LaunchTime+3600s (hard stop) then +3720s (120s termination grace);
# the clock origin is the EARLIER of the supplied epoch and the EC2 LaunchTime read first, after cleanup is armed.
# Exit: 0 archive+manager authenticated (chain disposition recorded verbatim in collection.json); 10 terminal missing/invalid;
# 11 termination unconfirmed (also after a glue failure); 12 archive/manifest unauthentic; 13 manager receipt missing/unauthentic;
# 14 archive authenticated but the watcher was LATE (termination after launch+3720s): modeled cost bound UNPROVEN, run INVALID;
# 64 usage/unfrozen; 19 other glue failure (the exact instance was terminated first when the cleanup below was armed).
set -euo pipefail
rc=0; armed=false; issued=false; terminated=false; warned=false; late=false; signalled=
late_report() { # never silent, never claims the cost cap
  echo "watcher LATE: termination attempted after launch+3720s; modeled cost bound UNPROVEN" >&2
  { echo "late termination attempt at $(date +%s); term_stop=$term_stop" > remote-results/LATE_COST_BOUND_UNPROVEN.txt; } 2>/dev/null || true
}
unconfirmed() { # never claims termination; the marker is best effort because the result dir may be unwritable
  { printf 'instance=%s terminate_issued=%s hard_stop=%s term_stop=%s now=%s\n' "$instance" "$issued" "$hard_stop" "$term_stop" "$(date +%s)" > remote-results/TERMINATION_UNCONFIRMED.txt; } 2>/dev/null || true
  if [[ $late == true ]]; then late_report; fi
  printf 'termination unconfirmed within its window: INVALID; root must terminate %s%s\n' "$instance" "$1" >&2
  exit 11
}
# shellcheck disable=SC2329 # invoked by the EXIT trap
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
prefix=research/semantic-router/20261010/actual1m-scale-chain-canary-a0004
[[ $prefix =~ ^research/semantic-router/[0-9]{8}/[a-z0-9-]+$ ]] || { echo 'PENDING prefix not frozen' >&2; exit 64; }
supplied_started=$started
hard_stop=$((started + 3600)); term_stop=$((started + 3720)); call_stop=$hard_stop
# One call never outlives call_stop (hard_stop while monitoring, a window end while terminating); no time left => rc 124.
aws_call() { local t=$((call_stop - $(date +%s))); (( t > 0 )) || return 124; (( t > 25 )) && t=25; timeout -k 1 "$t" aws --profile causality --region eu-central-1 --cli-connect-timeout 5 --cli-read-timeout 15 "$@"; }
nap() { local t=$(($1 - $(date +%s))); (( t > 0 )) || return 0; sleep $(( t < $2 ? t : $2 )); }
# Receipt append is best effort: a failed write is reported once and never blocks termination.
rec() { { printf '%s\n' "$2" >> "remote-results/$1"; } 2>/dev/null || [[ $warned == true ]] || { echo "watcher: receipt write failed ($1); termination not blocked" >&2; warned=true; }; }
state_of() { aws_call ec2 describe-instances --instance-ids "$instance" --query 'Reservations[0].Instances[0].State.Name' --output text > remote-results/state.txt 2>remote-results/state.stderr && cat remote-results/state.txt; }
# Terminate only this original instance (idempotent re-issue until accepted); shared by the normal path and the armed cleanup.
# Past launch+3720s it still makes one bounded 60s best-effort attempt and sets late=true. Optional $1 caps the window.
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
hard_stop=$((started + 3600)); term_stop=$((started + 3720)); call_stop=$hard_stop
mkdir -m 0700 remote-results
printf 'launch_time_raw=%s launch_epoch=%s supplied_started=%s effective_started=%s supplied_minus_launch_seconds=%s\n' "$launch_raw" "$launch_epoch" "$supplied_started" "$started" "$((supplied_started - launch_epoch))" > remote-results/launch-time.txt
reason=deadline
# External hard stop 3600s after the effective start; machine-local stop is 3000s.
while (( $(date +%s) < hard_stop )); do
  if aws_call s3api head-object --bucket "$bucket" --key "$prefix/canary/verdict.json" > remote-results/terminal.head.json 2>remote-results/terminal.head.stderr; then
    reason=terminal
    break
  fi
  # A setup failure publishes no coordinator verdict; stop on its bounded immutable terminal marker.
  if aws_call s3api head-object --bucket "$bucket" --key "$prefix/terminal.json" > remote-results/bootstrap.head.json 2>remote-results/bootstrap.head.stderr; then
    if jq -e '(.ContentLength | type == "number" and floor == . and . > 0 and . <= 65536) and (.ETag | type == "string" and length > 0 and length <= 256)' remote-results/bootstrap.head.json > /dev/null; then
      bootstrap_etag=$(jq -r .ETag remote-results/bootstrap.head.json)
      if aws_call s3api get-object --bucket "$bucket" --key "$prefix/terminal.json" --if-match "$bootstrap_etag" remote-results/bootstrap-terminal.json > remote-results/bootstrap.get.json 2>remote-results/bootstrap.get.stderr; then
        if jq -e --arg i "$instance" '.schema == "borsuk-native-scale-build-bootstrap-closed-v1" and .instance_id == $i and .performance_claim == false and (.original_exit | type == "number" and floor == . and . >= 1 and . <= 255) and (.exit | type == "number" and floor == . and . >= 90 and . <= 99)' remote-results/bootstrap-terminal.json > /dev/null; then
          reason=bootstrap_failure
          break
        fi
      fi
    fi
  fi
  state=$(state_of) || state=unknown
  case $state in terminated|stopped|stopping|shutting-down) reason=instance_$state; break;; esac
  nap "$hard_stop" 15
done
printf '%s\n' "$reason" > remote-results/watcher-reason.txt
# Give the coordinator at most 20 s to finish its recorded exit and immediate shutdown after verdict publication.
if [[ $reason == terminal ]]; then
  proof_stop=$(( $(date +%s) + 20 )); (( proof_stop <= hard_stop )) || proof_stop=$hard_stop
  call_stop=$proof_stop
  while (( $(date +%s) < proof_stop )); do
    state=$(state_of) || state=unknown
    case $state in terminated|stopped|stopping|shutting-down) break;; esac
    nap "$proof_stop" 2
  done
  call_stop=$hard_stop
fi
terminate_instance
[[ $terminated == true ]] || unconfirmed ""
if [[ $late == true ]]; then late_report; fi
# Verify both exact launch volumes are absent BEFORE any remote evidence collection.
call_stop=$(( $(date +%s) + 120 ))
jq -e 'type=="array" and length==2 and (unique|length)==2 and all(.[];type=="string" and test("^vol-[0-9a-f]{8,17}$"))' volume-ids.json > /dev/null || exit 19
mapfile -t volume_ids < <(jq -r '.[]' volume-ids.json)
[[ ${#volume_ids[@]} == 2 ]] || exit 19
for vol in "${volume_ids[@]}"; do
  absent=false
  while (( $(date +%s) < call_stop )); do
    vr=0
    aws_call ec2 describe-volumes --volume-ids "$vol" > "remote-results/$vol.stdout" 2> "remote-results/$vol.stderr" || vr=$?
    printf '%s\n' "$vr" > "remote-results/$vol.exit"
    if (( vr != 0 )) && grep -qE '^(aws: \[ERROR\]: )?An error occurred \(InvalidVolume\.NotFound\) when calling the DescribeVolumes operation( \(reached max retries: [0-9]+\))?:' "remote-results/$vol.stderr"; then absent=true; break; fi
    nap "$call_stop" 3
  done
  [[ $absent == true ]] || { echo "volume deletion unconfirmed: $vol" >&2; exit 19; }
done
jq -n --arg i "$instance" --arg reason "$reason" --arg ud "$user_data_sha" --argjson late "$late" --slurpfile v volume-ids.json '{schema:"borsuk-canary-infrastructure-closeout-v1",instance_id:$i,terminated:true,volumes:$v[0],both_volumes_absent:true,reason:$reason,user_data_sha256:$ud,late:$late,canary_replay_pending:true,performance_claim:false}' > remote-results/infrastructure-closeout.json
[[ $late == false ]] || exit 14
exit 0
