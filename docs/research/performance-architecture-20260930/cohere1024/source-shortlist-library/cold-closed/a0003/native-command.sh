#!/bin/bash
set -euo pipefail
root=/mnt/borsuk-v9-panel
printf '%s  %s\n' 23671f75995056c8caeb9b5fde429830c03e5ab606e16e7b95a0bfbb569b76e1 "$root/runtime-code.sha256" | sha256sum --strict -c -
(cd "$root" && sha256sum --strict -c runtime-code.sha256)
left=$(( $(cat "$root/machine-stop.epoch")-$(date +%s)-180 ))
((left>60)) || exit 125
((left<=2800)) || left=2800
/usr/bin/timeout -k 20 "$left" systemd-run --unit=borsuk-v9-cold-controller --expand-environment=no --wait --pipe -p Environment=BORSUK_RUNTIME_MANIFEST_SHA=23671f75995056c8caeb9b5fde429830c03e5ab606e16e7b95a0bfbb569b76e1 -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=536870912 -p MemorySwapMax=0 -p TasksMax=128 -p KillMode=control-group -p TimeoutStopSec=20 -p LimitCORE=0 -p RuntimeMaxSec="$((left-20))" /bin/bash "$root/run-v9-chain.sh"
