#!/usr/bin/env bash
set -euo pipefail
cache_watch_dir=/data/target/borsuk-cold-fetch-native-gates-a0002
cache_watch_instance=i-08ea9aa14c4076479
cache_watch_prefix=research/semantic-router/20261007/cold-fetch-native-gates-a0002
while true; do
  if timeout --kill-after=5 30 aws --profile causality --region eu-central-1 s3api get-object --bucket borsuk-bench-453182569524-euc1 --key "$cache_watch_prefix/terminal.json" "$cache_watch_dir/terminal.partial.json" > "$cache_watch_dir/poll-response.json" 2> "$cache_watch_dir/poll-error.txt"; then
    mv "$cache_watch_dir/terminal.partial.json" "$cache_watch_dir/terminal.json"
    jq -e '.instance_id == "i-08ea9aa14c4076479" and .source_commit == "31eca73431901c193aff598f4c6e44d3540594da" and .schema == "borsuk-cold-fetch-native-gates-v1"' "$cache_watch_dir/terminal.json" > /dev/null
    aws --profile causality --region eu-central-1 ec2 terminate-instances --instance-ids "$cache_watch_instance" --output json > "$cache_watch_dir/terminate-response.json"
    aws --profile causality --region eu-central-1 ec2 wait instance-terminated --instance-ids "$cache_watch_instance"
    printf '%s\n' '{"instance_id":"i-08ea9aa14c4076479","wait_exit":0,"terminated":true}' > "$cache_watch_dir/wait.json"
    cat "$cache_watch_dir/terminal.json"
    exit 0
  fi
  if timeout --kill-after=5 30 aws --profile causality --region eu-central-1 ec2 describe-instances --instance-ids "$cache_watch_instance" --query 'Reservations[0].Instances[0].State.Name' --output text > "$cache_watch_dir/observed-state.txt" 2> "$cache_watch_dir/state-error.txt"; then
    if [[ "$(cat "$cache_watch_dir/observed-state.txt")" == terminated ]]; then
      printf '%s\n' '{"status":"INCOMPLETE_INSTANCE_TERMINATED_WITHOUT_TERMINAL","instance_id":"i-08ea9aa14c4076479","native_qualified":false}' > "$cache_watch_dir/incomplete.json"
      exit 97
    fi
  fi
  sleep 30
done
