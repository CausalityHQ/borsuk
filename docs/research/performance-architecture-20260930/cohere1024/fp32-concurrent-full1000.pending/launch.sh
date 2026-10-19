#!/bin/bash
# Root only: one outer unit, no resume and no automatic retry.
set -euo pipefail
E=/mnt/borsuk-workspace/evidence/fp32-concurrent-full1000-root-a0001
test "$(date -u +%s)" -lt 1792443900
test "$(cat /sys/devices/virtual/dmi/id/board_asset_tag)" = i-09f8571d0edf296e9
systemctl is-active --quiet borsuk-fp32-concurrent-full1000-a0001-shutdown.timer
exec systemd-run --unit=borsuk-fp32-concurrent-full1000-a0001 --collect --property=AllowedCPUs=0-7 --property=CPUQuota=800% --property=MemoryMax=8G --property=MemorySwapMax=0 --property=TasksMax=1024 --property=RuntimeMaxSec=4200 --property=TimeoutStopSec=5 --property=KillMode=control-group --property=OOMPolicy=stop /bin/bash "$E/run-controller.sh"
