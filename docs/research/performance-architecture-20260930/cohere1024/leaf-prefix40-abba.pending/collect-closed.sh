#!/bin/bash
set -euo pipefail
prep=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-preparation-a0001
out=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-run-a0001
[[ $(cat "$out/terminal") == ABBA_LATENCY_SCREEN_CLOSED && $(cat "$out/manager.exit") == 0 ]]
(cd "$prep" && sha256sum -c SHA256SUMS > /dev/null && sha256sum -c artifacts.sha256 > /dev/null)
exec 9>"$prep/owned.lock"
flock -n 9
if pgrep -f '(^|/)(check_cohere_native_baseline|compare_native_replay)( |$)' >/dev/null; then exit 70; fi
python3 "$prep/reduce-closed.py"
closed=/mnt/borsuk-workspace/evidence/leaf-prefix40-abba-closed-a0001
sha256sum "$out"/*.jsonl "$out"/*.reader-report.json "$closed/summary.json" >"$closed/closed.sha256"
sync -f "$closed"
echo CLOSED_VALID >"$closed/terminal"
