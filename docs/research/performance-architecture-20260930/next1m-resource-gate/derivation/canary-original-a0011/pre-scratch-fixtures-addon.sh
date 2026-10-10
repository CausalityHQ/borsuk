phase=scratch-fixtures
get scratch-fixture-guest.sh 128
echo '6f624ead997e6606a3dcb3dda0de0b1c35c6f2d0105a7b2dcd1ab111d81b306e  scratch-fixture-guest.sh' | sha256sum --strict -c -
get scratch-fixture-launcher.py 128
echo '48d4c79861285f9a8a60d03a635427c096b060415f5c8fc45c364d3fabf19238  scratch-fixture-launcher.py' | sha256sum --strict -c -
get scratch-fixture-verifier.py 128
echo '301c882bee892578fc351491506a01e48827eab56520bdc28fdafeaea9e6c3b5  scratch-fixture-verifier.py' | sha256sum --strict -c -
get scratch-admission-mock.sh 128
echo 'e56ec9a35c5a015f72f3a1a5b8417ad810c4740e5b071003c60ca87216ab4d3f  scratch-admission-mock.sh' | sha256sum --strict -c -
get scratch-proof-negatives.py 128
echo '22f3c36298c0cb3916b0d9193883a211584237ca79be532c0c6095a26b152c65  scratch-proof-negatives.py' | sha256sum --strict -c -
get scratch-settle-fixture.py 128
echo '4a7b1bd0fd91c5b56187875e7b743981d5e442ed4cd03cc8844fe5bbc9062095  scratch-settle-fixture.py' | sha256sum --strict -c -
get scratch-fixture-parent-launcher.py 128
echo 'cf5d1d8131b57993689ec897f61d7c901693a9f853eb3dfda379a84f71791640  scratch-fixture-parent-launcher.py' | sha256sum --strict -c -
get scratch-instance-settle-fixture.py 128
echo '2170e37be98080587dece20cc3327c04cacfb78152b1b5e5f0313df674fdd19f  scratch-instance-settle-fixture.py' | sha256sum --strict -c -
get pf.py 128
echo 'fc970608fd25094941a62ea3404a98ae73b0661c8ac78aa128f978053356e306  pf.py' | sha256sum --strict -c -
get bt.sh 128
echo 'ba736c3d4b6c05bd6b6c242f795ad2ac138580ab66a15df9856efe23b0fb9aae  bt.sh' | sha256sum --strict -c -
get bc.sh 128
echo 'bff35749d957c1e76635b1170c2ef390fe97dadf5b3dc72712b65451397e6f35  bc.sh' | sha256sum --strict -c -
get canary-tail.sh 64
echo 'b069a2e76805683d207988e3e9764cbefb9a92b17195b8be06e4e889b2376659  canary-tail.sh' | sha256sum --strict -c -
get canary-coordinator.sh 64
echo '983a3f16e5f3567e587729502d82d3a0c33cf90df3f7054e08559b66583f0acc  canary-coordinator.sh' | sha256sum --strict -c -
get ff.py 64
echo '78e23213f40b362baed4c0227cb0eaac8e08e26b474928460292e44aa2b7f6ee  ff.py' | sha256sum --strict -c -
get ft.sh 64
echo '046316866e35788682c013337eba8118ec126ecfcfcd8e35aff61274842251c4  ft.sh' | sha256sum --strict -c -
get fc.sh 64
echo '7c2db63dea0d935614066e44b5f67e652d2a8137a7a5d883262106a348d70392  fc.sh' | sha256sum --strict -c -
set +e
timeout -k 2 140 systemd-run --unit=borsuk-scratch-fixtures --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=10 -p KillMode=control-group -p PrivateNetwork=yes -p LimitCORE=0 bash -c 'set -Eeuo pipefail; cd /mnt/borsuk-scale1m; python3 -I ff.py canary-tail.sh canary-coordinator.sh ft.sh fc.sh; python3 -I pf.py canary-tail.sh canary-coordinator.sh bt.sh bc.sh; python3 -I scratch-settle-fixture.py scratch-fixture-launcher.py; python3 -I scratch-instance-settle-fixture.py scratch-fixture-launcher.py scratch-fixture-parent-launcher.py; bash scratch-admission-mock.sh scratch-fixture-guest.sh /mnt/borsuk-scale1m/scratch-fixture-work; python3 -I scratch-proof-negatives.py scratch-fixture-launcher.py validate-scratch-binding.sh scratch-fixture-verifier.py' > evidence-root/scratch-fixtures.stdout 2> evidence-root/scratch-fixtures.stderr
frc=$?; set -e
printf '%s\n' "$frc" > evidence-root/scratch-fixtures.exit
timeout -k 1 5 systemctl show borsuk-scratch-fixtures.service > evidence-root/scratch-fixtures.systemd
(( frc == 0 )) || exit 94
fstate=$(timeout -k 1 5 systemctl show borsuk-scratch-fixtures.service -p ActiveState --value)
[[ $fstate == inactive || $fstate == failed ]] || exit 94
