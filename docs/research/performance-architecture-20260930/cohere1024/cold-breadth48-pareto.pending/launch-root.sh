#!/bin/bash
set -euo pipefail
packet=$(cd -- "$(dirname -- "$0")" && pwd)
evidence=/mnt/borsuk-workspace/evidence/cold-breadth48-pareto-a0001
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 PYTHONDONTWRITEBYTECODE=1
exec 9>/mnt/borsuk-workspace/cold-native-experiment.lock
flock -n 9
test "$(date +%s)" -lt 1792410900
test ! -e "$evidence"
trap 'status=$?; if test -d "$evidence"; then printf "%s\n" "$status" > "$evidence/controller.exit"; date -u > "$evidence/controller.finished"; if ! test -f "$evidence/terminal.json"; then printf "{\"status\":\"INVALID\",\"controller_exit\":%s,\"complete\":false}\n" "$status" > "$evidence/control-terminal.json"; fi; fi; exit "$status"' EXIT
python3 -B "$packet/run-prepared.py" prepare
exec > "$evidence/controller.log" 2>&1
python3 -B "$packet/run-prepared.py" run
