# shellcheck shell=bash
# Sourced only after exact support-manifest authentication on the owned EC2 host.
# SC2016: quoted programs intentionally expand in child Bash/jq, not while sourcing.
# shellcheck disable=SC2016
: "${log_pid:?}" "${local_stop_epoch:?}" "${instance:?}" "${phase:?}" "${bucket:?}" "${prefix:?}"
close_logger() {
 close_run 30 bash -c 'while [[ -r /proc/$1/stat ]]; do state=$(awk "{print \$3}" "/proc/$1/stat") || exit 1; [[ $state != Z ]] || break; sleep 0.1; done' _ "$log_pid" || return 96
 local logger_rc=0
 wait "$log_pid" || logger_rc=$?
 printf '%s\n' "$logger_rc" > evidence-root/logger.exit || return 96
 (( logger_rc == 0 ))
}
write_closure_manifest() {
 (ulimit -f 2048; close_run 60 bash -o pipefail -c 'cd -- "$1" || exit 1; find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum' _ evidence-root) > artifacts.sha256
}
preserve_failure() {
 local original_rc=$1 failing_line=$2 now left failure_end snapshot_sha snapshot_bytes
 [[ $original_rc =~ ^[0-9]+$ && $failing_line =~ ^[0-9]+$ ]] || return 96
 now=$(date +%s); left=$((local_stop_epoch-now-5))
 (( left > 0 )) || return 96
 (( left <= 25 )) || left=25
 failure_end=$((now+left))
 failure_run() {
  local remaining=$((failure_end-$(date +%s)-1))
  (( remaining > 0 )) || return 125
  timeout -k 1 "$remaining" "$@"
 }
 # Publish the failure independently FIRST; diagnostic collection may fail or exhaust its window.
 failure_run jq -n --arg instance "$instance" --arg phase "$phase" --argjson original "$original_rc" --argjson line "$failing_line" \
  '{schema:"borsuk-bootstrap-failure-v1",status:"INVALID",instance_id:$instance,phase:$phase,original_exit:$original,failing_line:$line,archive_complete:false,native_closure_verified:false,diagnostic_snapshot:null,performance_claim:false}' > bootstrap-failure.json || return 96
 failure_run sync -f bootstrap-failure.json || return 96
 failure_run env AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER='' aws --region eu-central-1 s3api put-object --bucket "$bucket" --key "$prefix/bootstrap-failure.json" --body bootstrap-failure.json --if-none-match '*' > bootstrap-failure.put.json || return 96
 # A capped snapshot is forensic evidence only; its writer may still be live.
 snapshot_sha=; snapshot_bytes=0
 if [[ -f run.log && ! -L run.log ]]; then
  failure_run tail -c 65536 run.log > bootstrap-failure-log.snapshot || return 96
  snapshot_sha=$(failure_run sha256sum bootstrap-failure-log.snapshot) || return 96
  snapshot_sha=${snapshot_sha%% *}
  snapshot_bytes=$(failure_run stat -c %s bootstrap-failure-log.snapshot) || return 96
  [[ $snapshot_sha =~ ^[0-9a-f]{64}$ && $snapshot_bytes =~ ^[0-9]+$ ]] && (( snapshot_bytes <= 65536 )) || return 96
  failure_run env AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER='' aws --region eu-central-1 s3api put-object --bucket "$bucket" --key "$prefix/bootstrap-failure-log.snapshot" --body bootstrap-failure-log.snapshot --if-none-match '*' > bootstrap-failure-log.put.json || return 96
 fi
 failure_run jq -n --arg instance "$instance" --arg phase "$phase" --argjson original "$original_rc" --argjson line "$failing_line" --arg sha "$snapshot_sha" --argjson bytes "$snapshot_bytes" \
  '{schema:"borsuk-bootstrap-failure-snapshot-v1",status:"INVALID",instance_id:$instance,phase:$phase,original_exit:$original,failing_line:$line,archive_complete:false,native_closure_verified:false,diagnostic_snapshot:{bytes:$bytes,sha256:$sha,producer_may_be_live:true},performance_claim:false}' > bootstrap-failure-snapshot.json || return 96
 failure_run sync -f bootstrap-failure-snapshot.json || return 96
 failure_run env AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_PAGER='' aws --region eu-central-1 s3api put-object --bucket "$bucket" --key "$prefix/bootstrap-failure-snapshot.json" --body bootstrap-failure-snapshot.json --if-none-match '*' > bootstrap-failure-snapshot.put.json || return 96
}
