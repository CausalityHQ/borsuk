#!/usr/bin/env bash
set -euo pipefail
out=/data/target/borsuk-reconstruction-real1m.pending
phase=${1:?phase}
case $phase in admission|smoke) seconds=600;; parity) seconds=4800;; *) exit 125;; esac
test ! -e "$out/$phase.ssm-request.json"
instance=$(cat "$out/instance.id")
{
 printf '%s\n' '#!/bin/bash' 'set -euo pipefail' 'support=/opt/borsuk-reconstruction-gate' '(cd "$support" && sha256sum -c support.sha256)'
 if [[ $phase == smoke ]]; then
  test -s "$out/admission.root-accepted.json"
  printf 'test ! -e "$support/admission.root-accepted.json"\ncat > "$support/admission.root-accepted.json" <<\x27BORSUK_ROOT_ACCEPTED_END\x27\n'
  cat "$out/admission.root-accepted.json"
  printf 'BORSUK_ROOT_ACCEPTED_END\n'
 fi
 if [[ $phase == parity ]]; then
  h=$(sha256sum "$out/admission.root-accepted.json"); h=${h%% *}
  printf 'printf '\''%%s  %%s\\n'\'' %s "$support/admission.root-accepted.json" | sha256sum -c -\n' "$h"
  test -s "$out/smoke.root-accepted.json"
  printf 'test ! -e "$support/smoke.root-accepted.json"\ncat > "$support/smoke.root-accepted.json" <<\x27BORSUK_ROOT_ACCEPTED_END\x27\n'
  cat "$out/smoke.root-accepted.json"
  printf 'BORSUK_ROOT_ACCEPTED_END\n'
 fi
 printf 'timeout -k 5 %s /bin/bash "$support/run-reconstruction-stage.sh" %s\n' "$((seconds+150))" "$phase"
} > "$out/$phase.ssm-command.sh"
bash -n "$out/$phase.ssm-command.sh"
jq -n --arg instance "$instance" --arg phase "$phase" --argjson seconds "$((seconds+180))" --rawfile command "$out/$phase.ssm-command.sh" '{InstanceIds:[$instance],DocumentName:"AWS-RunShellScript",Comment:("borsuk real1m reconstruction a0001 "+$phase),TimeoutSeconds:$seconds,Parameters:{executionTimeout:[($seconds|tostring)],commands:[$command]}}' > "$out/$phase.ssm-request.json"
sha256sum "$out/$phase.ssm-command.sh" "$out/$phase.ssm-request.json" > "$out/$phase.ssm-inputs.sha256"
