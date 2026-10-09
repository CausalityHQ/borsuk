#!/usr/bin/env python3
"""Root replay of closed offline parity evidence, not ANN quality or performance."""
import hashlib,json,re,sys,tarfile
from pathlib import Path,PurePosixPath

def req(ok, why):
    if not ok: raise ValueError(why)
def digest(b): return hashlib.sha256(b).hexdigest()
def read(p): return json.loads(p.read_bytes())
def main():
    R,pins_path=map(Path,sys.argv[1:3]);pins=read(pins_path)
    terminal_body=(R/'terminal.json').read_bytes();term=json.loads(terminal_body);col=read(R/'collection.json');manager=read(R/'bootstrap-manager.json')
    instance=pins['instance_id'];req(col['terminated'] is True and col['instance_id']==instance,'terminated original')
    req(term['schema']=='borsuk-actual-cohort-parity-closed-v1' and term['instance_id']==instance and term['phase']=='complete' and type(term['original_exit']) is int and term['original_exit']==0 and type(term['exit']) is int and term['exit']==0 and term['publication_verified'] is False and term['performance_claim'] is False and term['ann_run'] is False,'outer original outcome')
    req(manager['schema']=='borsuk-parity-bootstrap-exit-v1' and manager['instance_id']==instance and manager['terminal_sha256']==digest(terminal_body) and manager['exit_code']=='exited' and manager['exit_status']=='0' and manager['service_result']=='success' and manager['final_exit']=='0','original main and final child outcome')
    archive=R/'evidence.tar.gz';req(0<archive.stat().st_size<=67108864 and archive.stat().st_size==term['evidence']['bytes'],'archive size');req(digest(archive.read_bytes())==term['evidence']['sha256'],'archive SHA')
    bodies={};total=0
    with tarfile.open(archive,'r:gz') as t:
        for m in t:
            req(not m.name.startswith('/') and '..' not in PurePosixPath(m.name).parts,'archive path');name=str(PurePosixPath(m.name))
            if m.isdir():continue
            req(m.isfile() and name not in bodies and m.size<=33554432,'archive type/duplicate/member cap');total+=m.size;req(total<=67108864,'expanded evidence cap')
            f=t.extractfile(m);req(f is not None,'archive stream');bodies[name]=f.read(m.size+1);req(len(bodies[name])==m.size,'body length')
    listed={}
    for line in (R/'artifacts.sha256').read_text().splitlines():
        match=re.fullmatch(r'([0-9a-f]{64})  (.+)',line);req(match is not None,'manifest syntax');name=str(PurePosixPath(match[2]));req(name not in listed,'manifest duplicate');listed[name]=match[1]
    req(set(listed)==set(bodies),'closed exact inventory');req(all(digest(bodies[n])==h for n,h in listed.items()),'inventory SHA')
    j=lambda n:json.loads(bodies[n]);jl=lambda n:[json.loads(l) for l in bodies[n].splitlines()]
    for n in ('transport','parity'):req(bodies[n+'-unit.exit'].strip()==b'0','original '+n+' wait exit')
    for n in ('transport-exit.json','service-exit.json'):
        m=j(n);req(m['schema']=='borsuk-parity-service-exit-v1' and m['exit_code']=='exited' and m['exit_status']=='0' and m['service_result']=='success','manager '+n)
    local=j('evidence-local/terminal.json');req(local['status']=='PARITY_VERIFIED' and local['intended_exit']==0 and local['actual_manager_and_outer_exit_required'] is True and local['performance_claim'] is False,'local intended status')
    req(local['elf_sha256']==pins['elf_sha256'],'qualified ELF binding')
    for n in ('native','timeout','time','tee','wrapper'):req(bodies['evidence-local/'+n+'.exit']==b'0\n','original '+n+' status')
    req(bodies['evidence-local/inputs.before.jsonl']==bodies['evidence-local/inputs.after.jsonl'],'whole-input closure')
    inputs=jl('evidence-local/inputs.before.jsonl');observed={(v['bytes'],v['sha256']) for v in inputs};req(set(tuple(v) for v in pins['input_size_sha'])<=observed,'all frozen input pins')
    cfg=j('finalized-config.json');cfgbody=bodies['finalized-config.json'];req(digest(cfgbody)==local['config_sha256'],'exact final config SHA');req(cfg==j('evidence-local/config.json'),'config copy')
    expected=pins['config_template'];comparable=dict(cfg);comparable['output_parent']=expected['output_parent'];comparable['shards']=[dict(v,path=expected['shards'][i]['path']) for i,v in enumerate(cfg['shards'])];req(comparable==expected,'strict config template')
    req(cfg['output_parent']['path']==expected['output_parent']['path'],'exact parent path')
    req([v['path'] for v in cfg['shards']]==['/mnt/borsuk-scale-parity/assets/input/en/0000.parquet','/mnt/borsuk-scale-parity/assets/input/en/0001.parquet'],'exact installed source paths')
    req(type(cfg['output_parent']['device']) is int and cfg['output_parent']['device']>0 and type(cfg['output_parent']['inode']) is int and cfg['output_parent']['inode']>0,'admitted parent identity')
    req(bodies['evidence-local/resources.before.effective.json']==bodies['evidence-local/resources.after.effective.json'],'effective resource closure');eff=j('evidence-local/resources.before.effective.json');req(0<eff['memory_max_bytes']<=8589934592 and eff['swap_max_bytes']==0 and 0<eff['pids_max']<=128 and 0<eff['cpu_quota_cores']<=4,'resource limits')
    receipt=j('native-complete.json');seal=j('evidence-local/native.stdout.json');req(seal['status']=='COMPLETE' and seal['receipt_sha256']==digest(bodies['native-complete.json']),'native receipt seal')
    req(receipt['schema']=='borsuk-cohere-native-cohort-receipt-v3' and receipt['status']=='COMPLETE' and receipt['config']['sha256']==local['config_sha256'] and receipt['resources']==cfg['resources'] and receipt['output_parent']==cfg['output_parent'],'native receipt configuration')
    req(receipt['geometry']['corpus_rows']==100000 and receipt['geometry']['query_rows']==32 and receipt['geometry']['dimensions']==1024 and receipt['geometry']['k']==10,'actual geometry')
    req(receipt['reserved_queries_sha256']=='8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e','all reserved query seal')
    outputs={v['name']:v for v in receipt['outputs']};req(set(outputs)=={'corpus.f32','corpus.ids.jsonl','queries.f32','queries.ids.jsonl','truth.u64'},'exact native output roster')
    req(outputs['corpus.f32']['bytes']==409600000 and outputs['corpus.f32']['sha256']=='3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c','original corpus byte parity')
    authenticated=jl('evidence-local/outputs.authenticated.jsonl')
    req(sorted((Path(v['path']).name,v['bytes'],v['sha256']) for v in authenticated)==sorted((v['name'],v['bytes'],v['sha256']) for v in receipt['outputs']),'every output independently authenticated')
    prefixes=j('evidence-local/prefixes.json')
    for name,label,size in [('queries.f32','requests',131072),('truth.u64','truth',2560)]:
        req(prefixes[label]['prefix_bytes']==size and outputs[name]['bytes']==size and outputs[name]['sha256']==prefixes[label]['prefix_sha256'],'historical prefix '+label)
    req(len(bodies['actual-truth.u64'])==2560 and digest(bodies['actual-truth.u64'])==outputs['truth.u64']['sha256'],'collected opaque truth prefix SHA')
    for line in bodies['evidence-local/closure.sha256'].decode().splitlines():
        match=re.fullmatch(r'([0-9a-f]{64})  (.+)',line);req(match is not None,'local closure syntax');req(digest(bodies['evidence-local/'+match[2]])==match[1],'local closure '+match[2])
    print(json.dumps(dict(status='PARITY_VERIFIED',instance_id=instance,terminated=True,rows=100000,queries=32,performance_claim=False,ann_run=False,truth_scope='historical arithmetic regression parity',historical_id_bytes_parity=False)))
if __name__=='__main__':main()
