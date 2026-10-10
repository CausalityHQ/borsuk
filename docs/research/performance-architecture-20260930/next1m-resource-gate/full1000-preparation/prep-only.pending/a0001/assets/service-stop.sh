#!/usr/bin/env bash
set -Eeuo pipefail
set -o noclobber
umask 077
if [[ ${1:-parity} == bootstrap ]]; then
 terminal=/mnt/borsuk-scale1m/terminal.json
 [[ -f $terminal ]]
 digest=$(sha256sum "$terminal"); digest=${digest%% *}
 instance=$(jq -er .instance_id "$terminal")
 final_exit=MISSING
 if [[ -f /mnt/borsuk-scale1m/final.exit && ! -L /mnt/borsuk-scale1m/final.exit ]]; then
  final_exit=$(cat /mnt/borsuk-scale1m/final.exit)
  [[ $final_exit =~ ^[0-9]+$ ]] || final_exit=INVALID
 fi
 record=$(jq -cn --arg instance "$instance" --arg sha "$digest" --arg code "${EXIT_CODE:-}" --arg status "${EXIT_STATUS:-}" --arg result "${SERVICE_RESULT:-}" --arg final "$final_exit" '{schema:"borsuk-parity-bootstrap-exit-v1",instance_id:$instance,terminal_sha256:$sha,exit_code:$code,exit_status:$status,service_result:$result,final_exit:$final}')
 printf '%s\n' "$record" > /mnt/borsuk-scale1m/bootstrap-manager.json
 sync -f /mnt/borsuk-scale1m/bootstrap-manager.json
 if [[ $# == 3 ]]; then
  [[ $2 == borsuk-bench-453182569524-euc1 && $3 == research/semantic-router/*/bootstrap-manager.json ]]
  timeout -k 2 20 aws s3api put-object --bucket "$2" --key "$3" --body /mnt/borsuk-scale1m/bootstrap-manager.json --if-none-match '*' > /mnt/borsuk-scale1m/bootstrap-manager.put.json
 elif [[ $# != 1 ]]; then
  exit 2
 fi
 printf 'BORSUK_BOOTSTRAP_EXIT %s\n' "$record" > /dev/console
 exit 0
fi
case ${1:-parity} in parity) receipt=service-exit.json;; transport) receipt=transport-exit.json;; chain) receipt=chain-exit.json;; *) exit 2;; esac
[[ ${EXIT_CODE:-} == exited || ${EXIT_CODE:-} == killed || ${EXIT_CODE:-} == dumped ]]
[[ -n ${EXIT_STATUS:-} && -n ${SERVICE_RESULT:-} ]]
jq -n --arg code "$EXIT_CODE" --arg status "$EXIT_STATUS" --arg result "$SERVICE_RESULT" \
 '{schema:"borsuk-parity-service-exit-v1",exit_code:$code,exit_status:$status,service_result:$result}' \
 > "/mnt/borsuk-scale1m/evidence-root/$receipt"
sync -f /mnt/borsuk-scale1m/evidence-root
