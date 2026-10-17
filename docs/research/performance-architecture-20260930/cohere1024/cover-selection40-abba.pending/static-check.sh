#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
for script in ./*.sh; do bash -n "$script"; done
/mnt/borsuk-workspace/tools/shellcheck -S warning -e SC1091 ./*.sh
python3 self-check.py
sha256sum -c artifacts.sha256
sha256sum -c environment.sha256
