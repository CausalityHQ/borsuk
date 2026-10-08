#!/usr/bin/env bash
set -euo pipefail
test -f /data/target/borsuk-cold-membership-native/qualification-a0002/LAUNCHED_EXACT_INSTANCE || exit 98
cache_watch_dir=/data/target/borsuk-cold-membership-native/qualification-a0002
cache_watch_instance=$(cat "$cache_watch_dir/instance-id")
cache_watch_prefix=research/semantic-router/20261008/cold-membership-native-gates-a0002
while true; do
  if timeout --kill-after=5 30 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$cache_watch_prefix/terminal.json" "$cache_watch_dir/terminal.partial.json" > "$cache_watch_dir/poll-response.json" 2> "$cache_watch_dir/poll-error.txt"; then
    mv "$cache_watch_dir/terminal.partial.json" "$cache_watch_dir/terminal.json"
    aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$cache_watch_instance" --output json > "$cache_watch_dir/terminate-response.json"
    aws --profile causality --region eu-central-1 ec2 wait instance-terminated --instance-ids "$cache_watch_instance"
    jq -n --arg instance "$cache_watch_instance" '{instance_id:$instance,wait_exit:0,terminated:true}' > "$cache_watch_dir/wait.json"
    jq -e --arg instance "$cache_watch_instance" '.instance_id == $instance and .source_commit == "34fa1f48246992d1ae1983f938f4cbc67faf5af9" and .schema == "borsuk-cold-membership-native-gates-v1" and .candidate_commit == "2f3c78f0d6076d339a1ded344432a8db0582f22c" and .native_source_identity_sha256 == "90ec35ef11c8f6e2d69374ef2421f331a6e6a26bead98accc27afc3f7ef39324" and .source_archive_sha256 == "da891254ee38d8936521d5a69e118a051c49151ef37863310bd113a828f50997"' "$cache_watch_dir/terminal.json" > /dev/null
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
