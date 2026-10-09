#!/usr/bin/env python3
"""Verify closed canary metadata only; never open vector, query or truth bodies."""
import hashlib, json, re, sys, tarfile
from pathlib import Path, PurePosixPath

def require(ok, label):
    if not ok: raise ValueError(label)
def sha(body): return hashlib.sha256(body).hexdigest()
def load(path): return json.loads(path.read_bytes())

def main():
    results, pins_path = map(Path, sys.argv[1:3])
    pins = load(pins_path)
    terminal_bytes = (results/'terminal.json').read_bytes()
    terminal = json.loads(terminal_bytes)
    collection = load(results/'collection.json')
    instance = pins['instance_id']
    require(collection['instance_id']==instance and collection['terminated'] is True, 'original termination')
    require(terminal['instance_id']==instance and terminal['schema']=='borsuk-parity-smoke-closed-v1' and type(terminal['original_unit_exit']) is int and terminal['original_unit_exit']==0 and terminal['acceptance']=='ROOT_REPLAY_REQUIRED' and terminal['performance_claim'] is False, 'original collector outcome')
    archive = results/'evidence.tar.gz'
    require(0 < archive.stat().st_size <= 67108864 and archive.stat().st_size==terminal['evidence']['bytes'], 'archive size')
    require(sha(archive.read_bytes())==terminal['evidence']['sha256'], 'archive digest')
    bodies = {}; digests = {}; metadata_bytes = 0
    fixtures = {
      'results/case.combined/prepared-parent/cohort/truth.u64':17825792,
      'results/case.combined/prepared-parent/cohort/complete.json':17825792,
      'results/case.metadata/prepared-parent/cohort/complete.json':33554433,
      'results/case.log/run.log':33554433,
      'results/case.logger-overflow/run.log':33554432,
      'results/log-output-normal/unrelated-regular-output':2097152,
      'results/log-output-stdout-overflow/unrelated-regular-output':2097152,
      'results/log-output-stderr-overflow/unrelated-regular-output':2097152,
    }
    with tarfile.open(archive, 'r:gz') as tar:
        for member in tar:
            raw = member.name
            require(not raw.startswith('/') and '..' not in PurePosixPath(raw).parts, 'archive path')
            name = str(PurePosixPath(raw))
            if member.isdir(): continue
            require(member.isfile() and name not in digests, 'archive member type/duplicate')
            if name in fixtures:
                require(member.size==fixtures[name], 'exact deliberate cap fixture size')
            else:
                require(member.size<=1048576, 'metadata member cap')
                metadata_bytes += member.size
                require(metadata_bytes<=4194304, 'metadata total cap')
            stream = tar.extractfile(member)
            require(stream is not None, 'archive body')
            digest=hashlib.sha256(); length=0; pieces=[]
            while True:
                part=stream.read(65536)
                if not part: break
                length+=len(part); digest.update(part)
                if name not in fixtures: pieces.append(part)
            require(length==member.size, 'member length')
            digests[name]=digest.hexdigest()
            if name not in fixtures: bodies[name]=b''.join(pieces)
    require(set(fixtures)<=set(digests), 'all intentional cap fixtures present')
    manifest = (results/'artifacts.sha256').read_text().splitlines()
    listed = {}
    for line in manifest:
        match = re.fullmatch(r'([0-9a-f]{64})  (.+)', line)
        require(match is not None, 'manifest syntax')
        name = str(PurePosixPath(match[2]))
        require(name not in listed, 'manifest duplicate')
        listed[name] = match[1]
    require(set(listed)==set(digests), 'exact closed inventory')
    require(all(digests[n]==h for n,h in listed.items()), 'inventory authentication')
    for name, expected in pins['source_sha256'].items():
        require(name in bodies and sha(bodies[name])==expected, 'frozen source '+name)
    require(bodies['original-unit.exit'].strip()==b'0', 'original systemd-run status')
    manager = json.loads(bodies['driver-manager-exit.json'])
    require(manager['schema']=='borsuk-parity-service-exit-v1' and manager['exit_code']=='exited' and manager['exit_status']=='0' and manager['service_result']=='success', 'driver normal-zero manager')
    report = json.loads(bodies['results/result.json'])
    require(report==dict(status='MECHANICS_VERIFIED_EXTERNAL_CONSOLE_PENDING',mock_publication=True,real_systemd=True,real_s3_publication=False,http_get_tested=False,full_wrapper_native_tested=False,cloud_final_main_is_fixture=True,poweroff_hook_tested=False,performance_claim=False), 'scope and driver outcome')
    records = [json.loads(bodies[f'results/case.{mode}/bootstrap-manager.json']) for mode in ('normal','lost','term')]
    records.append(load(results/'bootstrap-manager.json'))
    for mode, fake in [('normal','i-00000000000000001'),('lost','i-00000000000000002'),('term','i-00000000000000003')]:
        local = json.loads(bodies[f'results/case.{mode}/evidence-local/terminal.json'])
        require(local['status']=='ADMISSION_VERIFIED' and local['intended_exit']==0, mode+' surviving intent')
        accepted = bodies[f'results/case.{mode}/put/terminal.json']
        found = [r for r in records if r['instance_id']==fake]
        require(len(found)==1 and found[0]['terminal_sha256']==sha(accepted), mode+' exact local stop-hook binding')
        normal = found[0]['exit_code']=='exited' and found[0]['exit_status']=='0' and found[0]['service_result']=='success'
        require(normal==(mode=='normal'), mode+' actual manager classification')
        expected = ('exited','0','success') if mode=='normal' else ('exited','96','exit-code') if mode=='lost' else ('killed','TERM','signal')
        require(tuple(found[0][k] for k in ('exit_code','exit_status','service_result'))==expected, mode+' exact manager outcome')
        require(found[0]['final_exit']==('0' if mode=='normal' else '96' if mode=='lost' else 'MISSING'), mode+' child final status')
    require(len(bodies['results/case.logger-normal/run.log'])==256, 'normal bounded bootstrap logger')
    for mode in ('normal','stdout-overflow','stderr-overflow'):
        exits=[bodies['results/log-'+mode+'/'+name+'.exit'].strip() for name in ('native','timeout','time','tee','time-log','supervisor-stderr-log','native-stderr-log')]
        require(all(re.fullmatch(rb'[0-9]+',v) for v in exits), 'original log supervisor exits')
        require(all(v==b'0' for v in exits)==(mode=='normal'), 'log overflow never accepted')
        require(len(bodies['results/log-'+mode+'/native.stdout.json'])<=1024, 'stdout cap')
        for name in ('native.stderr.txt','supervisor.stderr.txt','native.time.txt'):
            require(len(bodies['results/log-'+mode+'/'+name])<=1048576, 'separate log cap')
    for mode in ('max-increase','max-decrease','oom-change','pids-change'):
        events=json.loads(bodies['results/resource-'+mode+'/resource-events.after.json'])
        require(events['valid'] is (mode=='max-increase'), 'resource semantic refusal '+mode)
        if mode=='max-increase': require(events['memory_max_events'][0]['delta']==1, 'record reclaim pressure')
    actual = [r for r in records if r['instance_id']==instance]
    require(len(actual)==1 and actual[0]['terminal_sha256']==sha(terminal_bytes) and actual[0]['exit_code']=='exited' and actual[0]['exit_status']=='0' and actual[0]['service_result']=='success' and actual[0]['final_exit']=='0', 'original collector manager')
    for name in ('memory.events','pids.events'):
        require(bodies[f'results/{name}.before']==bodies[f'results/{name}.after'], 'resource event delta '+name)
    print(json.dumps(dict(status='MECHANICS_VERIFIED',instance_id=instance,terminated=True,performance_claim=False,native_correctness_claim=False,full_wrapper_claim=False)))

if __name__=='__main__': main()
