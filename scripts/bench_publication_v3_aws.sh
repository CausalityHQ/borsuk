#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

manifest="${BORSUK_PUBLICATION_V3_MANIFEST:-docs/research/publication-v3-manifest.json}"
root="${BORSUK_PUBLICATION_V3_ROOT:-/tmp/borsuk-publication-v3}"
execute="${BORSUK_PUBLICATION_V3_EXECUTE:-0}"

mkdir -p "$root"
python3 scripts/publication_v3_protocol.py validate "$manifest"
python3 scripts/publication_v3_protocol.py schedule "$manifest" --output "$root/schedule.json"
cp "$manifest" "$root/manifest.json"

python3 - "$root/schedule.json" <<'PY'
import json
import pathlib
import subprocess
import sys

schedule_path = pathlib.Path(sys.argv[1])
schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
for cell in schedule["cells"]:
    output = schedule_path.parent / "cells" / cell["cell_id"] / "protocol.json"
    subprocess.run(
        [
            "python3",
            "scripts/publication_v3_protocol.py",
            "protocol",
            "--schedule",
            str(schedule_path),
            "--cell-id",
            cell["cell_id"],
            "--output",
            str(output),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
PY

if [[ "$execute" != "1" ]]; then
  printf 'publication-v3 dry run complete\n'
  exit 0
fi

printf 'Publication V3 workload execution is unavailable until the workload runners are implemented\n' >&2
exit 2
