phase=scratch-fixtures
get scratch-fixture-guest.sh 128
printf '%s  scratch-fixture-guest.sh\n' 6f624ead997e6606a3dcb3dda0de0b1c35c6f2d0105a7b2dcd1ab111d81b306e | sha256sum --strict -c -
get scratch-fixture-launcher.py 128
printf '%s  scratch-fixture-launcher.py\n' 48d4c79861285f9a8a60d03a635427c096b060415f5c8fc45c364d3fabf19238 | sha256sum --strict -c -
get scratch-fixture-verifier.py 128
printf '%s  scratch-fixture-verifier.py\n' 301c882bee892578fc351491506a01e48827eab56520bdc28fdafeaea9e6c3b5 | sha256sum --strict -c -
get scratch-admission-mock.sh 128
printf '%s  scratch-admission-mock.sh\n' e56ec9a35c5a015f72f3a1a5b8417ad810c4740e5b071003c60ca87216ab4d3f | sha256sum --strict -c -
get scratch-proof-negatives.py 128
printf '%s  scratch-proof-negatives.py\n' 22f3c36298c0cb3916b0d9193883a211584237ca79be532c0c6095a26b152c65 | sha256sum --strict -c -
get scratch-settle-fixture.py 128
printf '%s  scratch-settle-fixture.py\n' 4a7b1bd0fd91c5b56187875e7b743981d5e442ed4cd03cc8844fe5bbc9062095 | sha256sum --strict -c -
get scratch-fixture-parent-launcher.py 128
printf '%s  scratch-fixture-parent-launcher.py\n' cf5d1d8131b57993689ec897f61d7c901693a9f853eb3dfda379a84f71791640 | sha256sum --strict -c -
get scratch-instance-settle-fixture.py 128
printf '%s  scratch-instance-settle-fixture.py\n' 2170e37be98080587dece20cc3327c04cacfb78152b1b5e5f0313df674fdd19f | sha256sum --strict -c -
set +e
timeout -k 2 140 systemd-run --unit=borsuk-scratch-fixtures --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=10 -p KillMode=control-group -p PrivateNetwork=yes -p LimitCORE=0 bash -c 'set -Eeuo pipefail; cd /mnt/borsuk-scale1m; python3 -I scratch-settle-fixture.py scratch-fixture-launcher.py; python3 -I scratch-instance-settle-fixture.py scratch-fixture-launcher.py scratch-fixture-parent-launcher.py; bash scratch-admission-mock.sh scratch-fixture-guest.sh /mnt/borsuk-scale1m/scratch-fixture-work; python3 -I scratch-proof-negatives.py scratch-fixture-launcher.py validate-scratch-binding.sh scratch-fixture-verifier.py' > evidence-root/scratch-fixtures.stdout 2> evidence-root/scratch-fixtures.stderr
frc=$?; set -e
printf '%s\n' "$frc" > evidence-root/scratch-fixtures.exit
timeout -k 1 5 systemctl show borsuk-scratch-fixtures.service > evidence-root/scratch-fixtures.systemd
(( frc == 0 )) || exit 94
fstate=$(timeout -k 1 5 systemctl show borsuk-scratch-fixtures.service -p ActiveState --value)
[[ $fstate == inactive || $fstate == failed ]] || exit 94
