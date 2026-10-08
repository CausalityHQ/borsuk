#!/usr/bin/env bash
set -euo pipefail
test -f /data/target/borsuk-cold-membership-native/qualification-a0003/LAUNCHED_EXACT_INSTANCE || exit 98
cache_watch_dir=/data/target/borsuk-cold-membership-native/qualification-a0003
cache_watch_instance=$(cat "$cache_watch_dir/instance-id")
cache_watch_prefix=research/semantic-router/20261008/cold-membership-native-gates-a0003
while true; do
  if timeout --kill-after=5 30 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$cache_watch_prefix/terminal.json" "$cache_watch_dir/terminal.partial.json" > "$cache_watch_dir/poll-response.json" 2> "$cache_watch_dir/poll-error.txt"; then
    mv "$cache_watch_dir/terminal.partial.json" "$cache_watch_dir/terminal.json"
    aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$cache_watch_instance" --output json > "$cache_watch_dir/terminate-response.json"
    aws --profile causality --region eu-central-1 ec2 wait instance-terminated --instance-ids "$cache_watch_instance"
    jq -n --arg instance "$cache_watch_instance" '{instance_id:$instance,wait_exit:0,terminated:true}' > "$cache_watch_dir/wait.json"
    jq -e --arg instance "$cache_watch_instance" '.instance_id == $instance and .source_commit == "fc8a23dac44ad635fc1236736df68878690a8389" and .schema == "borsuk-cold-membership-native-gates-v1" and .candidate_commit == "966a88b59be88cf0051a510c48d348587b801ec2" and .native_source_identity_sha256 == "0097af81fbf3e61a2050a77cb046adfbf0b02a745e73a2181e924cd2cb697eae" and .source_archive_sha256 == "3927688ec1344c8c62058bcecd054251c4befcff1c6fd7dd800b1a860587faa2"' "$cache_watch_dir/terminal.json" > /dev/null
    cat "$cache_watch_dir/terminal.json"
    exit 0
  fi
  if timeout --kill-after=5 30 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$cache_watch_instance" --query 'Reservations[0].Instances[0].State.Name' --output text > "$cache_watch_dir/observed-state.txt" 2> "$cache_watch_dir/state-error.txt"; then
    if [[ "$(cat "$cache_watch_dir/observed-state.txt")" == terminated ]]; then
      printf '%s\n' '{"status":"INCOMPLETE_INSTANCE_TERMINATED_WITHOUT_TERMINAL","native_qualified":false}' > "$cache_watch_dir/incomplete.json"
      exit 97
    fi
  fi
  sleep 30
done
