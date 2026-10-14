#!/bin/bash
set -euo pipefail
base=/data/target/borsuk-sq8-shortlist.pending/launch-falsifier-full-driver-a0001
[[ ! -e $base/infra-probe-send.claim ]]
cat > "$base/infra-probe.command.sh" <<'REMOTE'
#!/bin/bash
set -euo pipefail
printf '%s\n' INFRASTRUCTURE_ONLY_NO_TEST_LOGS_OR_INPUT_READS
unit=borsuk-loss-example-regressions.service
timeout -k1 10 systemctl show "$unit" -p ActiveState -p SubState -p Result -p MainPID -p ControlGroup
cg=/sys/fs/cgroup/system.slice/$unit
if [[ -d $cg ]]; then
 for file in cpu.stat memory.current memory.peak memory.events pids.current cgroup.events; do printf '%s\n' "$file"; cat "$cg/$file"; done
 for pid in $(cat "$cg/cgroup.procs"); do
  [[ $pid =~ ^[1-9][0-9]*$ ]] || exit 1
  if [[ -r /proc/$pid/comm ]]; then printf 'PID %s\n' "$pid"; cat "/proc/$pid/comm"; grep -E '^(State|VmRSS|VmHWM|Threads):' "/proc/$pid/status" || true; fi
 done
 sleep 1
 printf '%s\n' CPU_STAT_AFTER_ONE_SECOND
 cat "$cg/cpu.stat"
fi
REMOTE
jq -Rs '{commands:[.],executionTimeout:["30"]}' "$base/infra-probe.command.sh" > "$base/infra-probe.parameters.json"
set -o noclobber
date -u +%FT%TZ > "$base/infra-probe-send.claim"
set +o noclobber
AWS_MAX_ATTEMPTS=1 AWS_PAGER='' timeout -k2 25 aws --profile causality --region eu-central-1 ssm send-command --instance-ids i-0b3294999a8103a8e --document-name AWS-RunShellScript --parameters "file://$base/infra-probe.parameters.json" --timeout-seconds 60 --comment borsuk-original-qualification-readonly-infra > "$base/infra-probe.send.json" 2> "$base/infra-probe.send.stderr"
jq -er '.Command.CommandId' "$base/infra-probe.send.json" > "$base/infra-probe.command.id"
cat "$base/infra-probe.command.id"
