#!/bin/bash
set -euo pipefail
test "${1:-}" = measure # Root chooses measurement explicitly; no diagnostic fallthrough.
packet=$(cd -- "$(dirname -- "$0")" && pwd)
evidence=/mnt/borsuk-workspace/evidence/cold-fetch64-a0001
export BORSUK_CPU_THREADS=1 RAYON_NUM_THREADS=1 PYTHONDONTWRITEBYTECODE=1
exec 9>/mnt/borsuk-workspace/cold-native-experiment.lock
flock -n 9
test "$(date +%s)" -lt 1792413600
test ! -e "$evidence"
python3 - <<'CHECK_SHUTDOWN'
import time
from pathlib import Path
p=dict(line.split("=",1) for line in Path("/run/systemd/shutdown/scheduled").read_text().splitlines())
assert p["MODE"]=="poweroff"
assert int(p["USEC"])/1e6>1792415700
assert time.time()+1800<=1792415700
CHECK_SHUTDOWN
trap 'status=$?; if test -d "$evidence"; then printf "%s\n" "$status" > "$evidence/controller.exit"; date -u > "$evidence/controller.finished"; if ! test -f "$evidence/terminal.json"; then printf "{\"status\":\"INVALID\",\"controller_exit\":%s,\"complete\":false}\n" "$status" > "$evidence/control-terminal.json"; fi; fi; exit "$status"' EXIT
python3 -B "$packet/run-prepared.py" prepare
exec > "$evidence/controller.log" 2>&1
python3 -B "$packet/run-prepared.py" run measure
