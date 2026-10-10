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
replace('root=/var/lib/borsuk-validator', '''# Early setup failures must not leave an idle instance until the root cutoff.
trap 'shutdown -h now' EXIT
root=/var/lib/borsuk-validator''')
replace('mkdir "$root"','mkdir "$root" /mnt/borsuk-pid-evidence')
replace('mkdir "$support" /mnt/borsuk-pool-pid /mnt/borsuk-pid-evidence','mkdir "$support" /mnt/borsuk-pool-pid')
replace('setup_deadline=$((SECONDS+500))','setup_deadline=$((launched+700))\nmachine_deadline=$((launched+18000))\nrequest_deadline=$((launched+17900))')
replace('    trap - EXIT; set +e', "    trap 'shutdown -h now' EXIT; set +e\n    cleanup_deadline=$(( $(date +%s)+90 ))")
replace('    shutdown -h now\n    exit "$original"', '    exit "$original"')
replace('finish() {', '''publish() {
    local cap=$1 left; shift
    left=$((request_deadline-$(date +%s)-5)); ((left>0)) || return 124
    ((cap<=left)) || cap=$left
    timeout -k 1 "$cap" "$@"
}
finish() {''')
replace('    sync -f "$root" || exit 94', '    publish 10 sync -f "$root" || exit 94')
replace('    tar -czf', '    publish 60 tar --exclude=borsuk-validator/installer --exclude=borsuk-validator/awscli.zip -czf')
replace('sha=$(sha256sum /var/lib/borsuk-validator-evidence.tar.gz)', 'sha=$(publish 10 sha256sum /var/lib/borsuk-validator-evidence.tar.gz)')
start_finish=s.index('finish() {'); stop_finish=s.index('trap finish EXIT', start_finish)
s=s[:start_finish]+s[start_finish:stop_finish].replace('timeout -k 1 30 aws','publish 30 aws')+s[stop_finish:]
replace('    printf \'%s\\n\' "$original" > "$root/bootstrap.exit" || exit 94', '''    # Retain exact native replay bytes, never corpus/query/truth/index payloads.
    mkdir "$root/replay" || exit 94
    : > "$root/replay-roster.tsv" || exit 94
    for relative in prepared-parent/configs/cohort.json prepared-parent/configs/derive.json \\
      prepared-parent/configs/generation.json prepared-parent/configs/publication.json \\
      prepared-parent/configs/diagnostic.json prepared-parent/configs/query16.json prepared-parent/configs/query32.json \\
      prepared-parent/cohort/complete.json prepared-parent/derived/derivation.json \\
      prepared-parent/generation/manifest.json prepared-parent/generation/plane/manifest.json \\
      prepared-parent/publication-receipt.json prepared-parent/store/semantic/index/head.json \\
      prepared-parent/diagnostic/result.jsonl prepared-parent/query16/result.jsonl prepared-parent/query32/result.jsonl \\
      staging-staging/native/config.json staging-staging/native/result.jsonl; do
        src=/mnt/borsuk-pool-pid/$relative
        if [[ ! -e $src && ! -L $src ]]; then
            printf 'MISSING\\t%s\\n' "$relative" >> "$root/replay-roster.tsv" || exit 94
            [[ $state != PID128_MECHANICS_ROOT_REPLAY_REQUIRED ]] || exit 94
            continue
        fi
        [[ -f $src && ! -L $src && $(realpath -e "$src") == "$src" ]] || exit 94
        size=$(stat -c %s "$src") || exit 94
        [[ $size =~ ^[0-9]+$ && $size -le 4194304 ]] || exit 94
        dst=$root/replay/$relative
        mkdir -p "${dst%/*}" || exit 94
        publish 5 cp --no-clobber "$src" "$dst" || exit 94
        h=$(publish 5 sha256sum "$src") || exit 94; h=${h%% *}
        [[ $(stat -c %s "$dst") == "$size" && $h =~ ^[0-9a-f]{64}$ ]] || exit 94
        printf '%s  %s\\n' "$h" "$dst" | sha256sum -c - || exit 94
        printf '%s\\t%s\\t%s\\n' "$relative" "$size" "$h" >> "$root/replay-roster.tsv" || exit 94
    done
    printf '%s\\n' "$original" > "$root/bootstrap.exit" || exit 94''')
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
replace('--max-time 60 --connect-timeout 10 -fsS', '--max-time 60 --connect-timeout 10 --max-filesize 73022935 -fsS')
files=[d/n for n in ['run-native-pid128-r7.sh','run-staging-native.sh','run-cleanup-negative.sh','staging-library.sh','timeout-descendant.sh','observer-command.sh','collect-admission-outer.sh','collect-stage-outer.sh','run-finite-campaign.sh']]
files += [c/n for n in ['cohort-template.json','derivation-template.json','producer-admission.json','baseline-admission.json','input-manifest.json','admission-root-gate.json','admission-config.json','campaign-config.json','transport-manifest.json']]
files += [out/'transport-native-inputs.sh']
roster=[dict(name=p.name,bytes=len(p.read_bytes()),sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in files]
start=s.index('for spec in \\\n'); stop=s.index('; do\n',start)
s=s[:start]+'for spec in \\\n'+ ' \\\n'.join("  '%s:%s:%s'"%(r['name'],r['bytes'],r['sha256']) for r in roster)+s[stop:]
replace('"$support/$name" > "$root/$name.download.json"', '--range "bytes=0-$bytes" "$support/$name" > "$root/$name.download.json"\n    jq -e --argjson bytes "$bytes" \' .ContentLength==$bytes and .ContentRange==("bytes 0-"+($bytes-1|tostring)+"/"+($bytes|tostring))\' "$root/$name.download.json" >/dev/null')
replace('phase=platform\n((SECONDS+350<=840)) || exit 125', 'phase=native-input-transport\nsetup 600 /bin/bash "$support/transport-native-inputs.sh" "$support/transport-manifest.json" '+next(r['sha256'] for r in roster if r['name']=='transport-manifest.json')+' "$setup_deadline" "$root/transport.json"\nphase=native-campaign\n(( $(date +%s)+16800+400 <= request_deadline )) || exit 125')
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
w=w.replace('terminal=false\nwhile', 'terminal=false\nnext_health=0\nwhile')
w=w.replace('    rc=0\n    left=$((deadline-SECONDS-2));', '''    if ((SECONDS>=next_health)); then
        timeout -k 1 8 aws ec2 describe-instances --instance-ids "$instance" > "$out/health.json"
        jq -e --arg i "$instance" '[.Reservations[].Instances[]]|length==1 and .[0].InstanceId==$i' "$out/health.json" >/dev/null
        observed=$(jq -er '.Reservations[0].Instances[0].State.Name' "$out/health.json")
        [[ $observed != terminated && $observed != shutting-down ]] || break
        next_health=$((SECONDS+30))
    fi
    rc=0
    left=$((deadline-SECONDS-2));''')
w=w.replace('--if-match "$etag" "$out/terminal.json"', '--if-match "$etag" --range "bytes=0-$(jq -r .ContentLength "$out/terminal.head.json")" "$out/terminal.json"')
w=w.replace('--if-match "$etag" "$out/evidence.tar.gz"', '--if-match "$etag" --range "bytes=0-$(jq -r .evidence_bytes "$out/terminal.json")" "$out/evidence.tar.gz"')
(out/'native-watch.sh').write_text(w)
