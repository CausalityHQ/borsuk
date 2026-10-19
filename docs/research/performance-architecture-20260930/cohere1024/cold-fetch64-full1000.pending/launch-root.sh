#!/bin/bash
set -euo pipefail
test "${1:-}" = measure # Root chooses measurement explicitly; no diagnostic fallthrough.
packet=$(cd -- "$(dirname -- "$0")" && pwd)
evidence=/mnt/borsuk-workspace/evidence/cold-fetch64-full1000-a0001
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 PYTHONDONTWRITEBYTECODE=1
exec 9>/mnt/borsuk-workspace/cold-native-experiment.lock
flock -n 9
test ! -e "$evidence"
python3 -B "$packet/run-prepared.py" check-binding
trap 'status=$?; if test -d "$evidence"; then printf "%s\n" "$status" > "$evidence/controller.exit"; date -u > "$evidence/controller.finished"; if ! test -f "$evidence/terminal.json"; then printf "{\"status\":\"INVALID\",\"controller_exit\":%s,\"complete\":false}\n" "$status" > "$evidence/control-terminal.json"; fi; fi; exit "$status"' EXIT
python3 -B "$packet/run-prepared.py" prepare
exec > "$evidence/controller.log" 2>&1
python3 -B "$packet/run-prepared.py" run measure
