phase=scratch-fixtures
ck(){ printf '%s  %s\n' "$1" "$2" | sha256sum --strict -c -; }
get scratch-fixture-guest.sh 128
ck 6f624ead997e6606a3dcb3dda0de0b1c35c6f2d0105a7b2dcd1ab111d81b306e scratch-fixture-guest.sh
get scratch-fixture-launcher.py 128
ck 48d4c79861285f9a8a60d03a635427c096b060415f5c8fc45c364d3fabf19238 scratch-fixture-launcher.py
get scratch-fixture-verifier.py 128
ck 301c882bee892578fc351491506a01e48827eab56520bdc28fdafeaea9e6c3b5 scratch-fixture-verifier.py
get scratch-admission-mock.sh 128
ck e56ec9a35c5a015f72f3a1a5b8417ad810c4740e5b071003c60ca87216ab4d3f scratch-admission-mock.sh
get scratch-proof-negatives.py 128
ck 22f3c36298c0cb3916b0d9193883a211584237ca79be532c0c6095a26b152c65 scratch-proof-negatives.py
get scratch-settle-fixture.py 128
ck 4a7b1bd0fd91c5b56187875e7b743981d5e442ed4cd03cc8844fe5bbc9062095 scratch-settle-fixture.py
get scratch-fixture-parent-launcher.py 128
ck cf5d1d8131b57993689ec897f61d7c901693a9f853eb3dfda379a84f71791640 scratch-fixture-parent-launcher.py
get scratch-instance-settle-fixture.py 128
ck 2170e37be98080587dece20cc3327c04cacfb78152b1b5e5f0313df674fdd19f scratch-instance-settle-fixture.py
get pf.py 128
ck fc970608fd25094941a62ea3404a98ae73b0661c8ac78aa128f978053356e306 pf.py
get bt.sh 128
ck ba736c3d4b6c05bd6b6c242f795ad2ac138580ab66a15df9856efe23b0fb9aae bt.sh
get bc.sh 128
ck bff35749d957c1e76635b1170c2ef390fe97dadf5b3dc72712b65451397e6f35 bc.sh
get canary-tail.sh 64
ck e47a0fbd15f2121b210ce5d1fbe2cc2d1266bebedb9e9e00d47db273d4d0b36c canary-tail.sh
get canary-coordinator.sh 64
ck b2ec593fe50776462dcb403237f1a971361bfa2249e8c303f9d4815ed968700b canary-coordinator.sh
get ff.py 64
ck 78e23213f40b362baed4c0227cb0eaac8e08e26b474928460292e44aa2b7f6ee ff.py
get ft.sh 64
ck 046316866e35788682c013337eba8118ec126ecfcfcd8e35aff61274842251c4 ft.sh
get fc.sh 64
ck 7c2db63dea0d935614066e44b5f67e652d2a8137a7a5d883262106a348d70392 fc.sh
get af.py 64
ck 856e4e1a63a8ea2f9705e6774e418821ea40d8cbbc5883ca3f0d8f8f8b0b3818 af.py
get gt.sh 64
ck b069a2e76805683d207988e3e9764cbefb9a92b17195b8be06e4e889b2376659 gt.sh
get gc.sh 64
ck 983a3f16e5f3567e587729502d82d3a0c33cf90df3f7054e08559b66583f0acc gc.sh
get pc.sh 64
ck d4ab3ec877393421140ecd7be9e110e3a4fa1f629cc0d0dc377d9b42e2fef406 pc.sh
set +e
timeout -k 2 140 systemd-run --unit=borsuk-scratch-fixtures --wait --pipe -p CPUQuota=100% -p AllowedCPUs=0 -p MemoryMax=256M -p MemorySwapMax=0 -p TasksMax=128 -p RuntimeMaxSec=120 -p TimeoutStopSec=10 -p KillMode=control-group -p PrivateNetwork=yes -p LimitCORE=0 bash -c 'set -Eeuo pipefail; cd /mnt/borsuk-scale1m; python3 -I af.py canary-tail.sh canary-coordinator.sh gt.sh gc.sh ff.py pf.py ft.sh fc.sh bt.sh bc.sh pc.sh; python3 -I scratch-settle-fixture.py scratch-fixture-launcher.py; python3 -I scratch-instance-settle-fixture.py scratch-fixture-launcher.py scratch-fixture-parent-launcher.py; bash scratch-admission-mock.sh scratch-fixture-guest.sh /mnt/borsuk-scale1m/scratch-fixture-work; python3 -I scratch-proof-negatives.py scratch-fixture-launcher.py validate-scratch-binding.sh scratch-fixture-verifier.py' > evidence-root/scratch-fixtures.stdout 2> evidence-root/scratch-fixtures.stderr
frc=$?; set -e
printf '%s\n' "$frc" > evidence-root/scratch-fixtures.exit
timeout -k 1 5 systemctl show borsuk-scratch-fixtures.service > evidence-root/scratch-fixtures.systemd
(( frc == 0 )) || exit 94
fstate=$(timeout -k 1 5 systemctl show borsuk-scratch-fixtures.service -p ActiveState --value)
[[ $fstate == inactive || $fstate == failed ]] || exit 94
