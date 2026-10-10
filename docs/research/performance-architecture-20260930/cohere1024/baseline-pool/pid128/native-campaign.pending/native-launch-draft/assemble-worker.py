"""Source assembly only. Does not import or execute experiment entrypoints."""
from pathlib import Path
import hashlib, json
d=Path('/tmp/borsuk-pid128-native-campaign-next/review-repair-r7')
c=Path('/tmp/borsuk-pid128-native-input-draft')
out=Path('/tmp/borsuk-pid128-native-launch-draft')
s=(d/'platform-worker.sh').read_text()
def replace(a,b):
 global s
 assert s.count(a)==1, a
 s=s.replace(a,b)
replace('# SOURCE ONLY. Causality EC2 bootstrap; real platform programs, no ANN/corpus/GT.\n# Transport schema retained solely for the existing root watcher.', '# SOURCE ONLY / UNVERIFIED. Causality EC2 native PID128 bootstrap.\n# New transport schema; independent root watcher and source review required.')
replace('bucket=${1:?}; prefix=${2:?}; assets=${3:?}', 'bucket=${1:?}; prefix=${2:?}; assets=${3:?}; launched=${4:?}')
replace('[[ $# == 3 &&', '[[ $# == 4 && $launched =~ ^[1-9][0-9]*$ &&')
replace('setup_deadline=$((SECONDS+500))','setup_deadline=$((launched+900))\nmachine_deadline=$((launched+18000))')
replace('    trap - EXIT; set +e', '    trap - EXIT; set +e\n    cleanup_deadline=$(( $(date +%s)+90 ))')
replace('finish() {', '''teardown() {
    local cap=$1 left; shift
    left=$((cleanup_deadline-$(date +%s)-2)); ((left>0)) || return 124
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {''')
# Shared cleanup budget; failed teardown is INVALID, not an extended measurement.
a=s.index('finish() {'); b=s.index('trap finish EXIT',a)
s=s[:a]+s[a:b].replace('timeout -k 1 5 systemctl','teardown 5 systemctl').replace('timeout -k 1 10 systemctl','teardown 10 systemctl')+s[b:]
replace('local left=$((setup_deadline-SECONDS-2));', 'local left=$((setup_deadline-$(date +%s)-2));')
s=s.replace('borsuk-platform-pair','borsuk-native-campaign')
s=s.replace('for label in positive negative; do','for label in admission staging negative widths; do')
replace('p=/mnt/borsuk-pid-evidence/$label/phases/diagnostic', 'for phase_name in prepare derive copy generation publish diagnostic query16 query32; do\n        p=/mnt/borsuk-pid-evidence/$label/phases/$phase_name')
replace('[[ $unit =~ ^borsuk-pid128-[a-z0-9-]+-diagnostic\\.service$ ]]', '[[ $unit =~ ^borsuk-pid128-[a-z0-9-]+-${phase_name}\\.service$ ]]')
replace('    ((cleanup==0)) || { state=INVALID; original=94; }','        done\n    ((cleanup==0)) || { state=INVALID; original=94; }')
replace('schema:"borsuk-validator-ec2-v1"','schema:"borsuk-native-pid128-ec2-v1"')
replace('native_executed:false,ann_executed:false,\n        real_platform_programs:true,', 'controller_execution_attempted:$attempted,')
replace('--argjson cleanup "$cleanup"', '--argjson cleanup "$cleanup" --argjson attempted "$attempted"')
replace('shutdown -h +18', 'remaining=$((machine_deadline-$(date +%s)))\n((remaining>0 && remaining<=18000)) || exit 125\nshutdown -h +$((remaining/60))')
replace('support=/mnt/borsuk-platform-support','support=/mnt/borsuk-native-support')
files=[d/n for n in ['run-native-pid128-r7.sh','run-staging-native.sh','run-cleanup-negative.sh','staging-library.sh','timeout-descendant.sh','observer-command.sh','collect-admission-outer.sh','collect-stage-outer.sh','run-finite-campaign.sh']]
files += [c/n for n in ['cohort-template.json','derivation-template.json','producer-admission.json','baseline-admission.json','input-manifest.json','admission-root-gate.json','admission-config.json','campaign-config.json','transport-manifest.json']]
files += [out/'transport-native-inputs.sh']
roster=[dict(name=p.name,bytes=len(p.read_bytes()),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files]
start=s.index('for spec in \\\n'); stop=s.index('; do\n',start)
s=s[:start]+'for spec in \\\n'+ ' \\\n'.join("  '%s:%s:%s'"%(r['name'],r['bytes'],r['sha256']) for r in roster)+s[stop:]
replace('phase=platform\n((SECONDS+350<=840)) || exit 125', 'phase=native-input-transport\nsetup 600 /bin/bash "$support/transport-native-inputs.sh" "$support/transport-manifest.json" '+next(r['sha256'] for r in roster if r['name']=='transport-manifest.json')+' "$setup_deadline" "$root/transport.json"\nphase=native-campaign\n(( $(date +%s)+16800+300 <= machine_deadline )) || exit 125')
replace('systemd-run --expand-environment=no --quiet --unit=borsuk-native-campaign', 'timeout -k 1 10 systemd-run --expand-environment=no --quiet --unit=borsuk-native-campaign')
replace('RuntimeMaxSec=330','RuntimeMaxSec=16860')
replace('"$support/run-platform-pair.sh" "$support"', '"$support/run-finite-campaign.sh" "$support/campaign-config.json" '+next(r['sha256'] for r in roster if r['name']=='campaign-config.json'))
replace('parent_id=$(setup 5 systemctl show', 'parent_id=$(timeout -k 1 5 systemctl show')
replace('deadline=$((SECONDS+345))','deadline=$(( $(date +%s)+16900 ))')
replace('((SECONDS<deadline))','(( $(date +%s)<deadline && $(date +%s)+120<machine_deadline ))')
replace('jq -es \'length==1 and .[0].status=="PLATFORM_PAIR_ROOT_REPLAY_REQUIRED" and .[0].ann_executed==false and .[0].production_qualification==false\' /mnt/borsuk-pid-evidence/platform-pair.json >/dev/null','jq -es \'length==1 and .[0].status=="PID128_MECHANICS_ROOT_REPLAY_REQUIRED" and .[0].performance_claim==false and .[0].cold_claim==false\' /mnt/borsuk-pid-evidence/campaign-terminal.json >/dev/null')
replace('phase=closed state=PLATFORM_PAIR_ROOT_REPLAY_REQUIRED','phase=closed state=PID128_MECHANICS_ROOT_REPLAY_REQUIRED')
(out/'native-worker.sh').write_text(s)
(out/'support-roster.json').write_text(json.dumps(roster,indent=2)+'\n')
print('Source assembled; no experiment entrypoint executed:',len(s.encode()),'bytes,',len(roster),'support bodies')
w=(d/'platform-watch.sh').read_text()
assert w.count('SECONDS+900-age')==1
w=w.replace('SECONDS+900-age','SECONDS+17900-age').replace('launch+900','launch+17900').replace('launch_plus_900','launch_plus_17900')
w=w.replace('.schema=="borsuk-validator-ec2-v1" and .native_executed==false', '.schema=="borsuk-native-pid128-ec2-v1" and (.controller_execution_attempted|type)=="boolean"')
w=w.replace('COLLECTED_PLATFORM_EVIDENCE_ONLY','COLLECTED_NATIVE_GATE_EVIDENCE_ROOT_REPLAY_REQUIRED')
w=w.replace('Validator cleanup is unproven','Native campaign cleanup is unproven')
(out/'native-watch.sh').write_text(w)
