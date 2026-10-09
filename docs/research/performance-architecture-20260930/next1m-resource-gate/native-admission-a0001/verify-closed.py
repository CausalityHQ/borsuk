#!/usr/bin/env python3
"""Root replay of closed offline 1M source admission evidence, not ANN quality or performance."""
import hashlib,json,re,sys,tarfile
from pathlib import Path,PurePosixPath

def req(ok, why):
    if not ok: raise ValueError(why)
def digest(b): return hashlib.sha256(b).hexdigest()
def bounded(p,cap=65536):
    req(p.is_file() and not p.is_symlink() and 0<p.stat().st_size<=cap,'metadata size/type '+str(p))
    with p.open('rb') as f: b=f.read(cap+1)
    req(len(b)<=cap,'metadata growth');return b
def unique(pairs):
    req(len({k for k,v in pairs})==len(pairs),'duplicate JSON fields')
    return dict(pairs)
def decode(b): return json.loads(b,object_pairs_hook=unique)
def same(a,b): return json.dumps(a,sort_keys=True,separators=(',',':'))==json.dumps(b,sort_keys=True,separators=(',',':'))
def read(p): return decode(bounded(p))
class EvidenceHeader(tarfile.TarInfo):
    headers=0
    @classmethod
    def frombuf(cls,buf,encoding,errors):
        header=super().frombuf(buf,encoding,errors)
        cls.headers+=1
        req(cls.headers<=512,'archive header count')
        req(header.type in (tarfile.REGTYPE,tarfile.AREGTYPE,tarfile.DIRTYPE),'archive extended/link/sparse header refused')
        req(0<=header.size<=33554432 and len(header.name)<=255,'archive header bounds')
        return header
def main():
    R,pins_path=map(Path,sys.argv[1:3]);pins=read(pins_path)
    terminal_body=bounded(R/'terminal.json');term=decode(terminal_body);col=read(R/'collection.json');manager=read(R/'bootstrap-manager.json')
    req(digest(terminal_body)==pins['terminal_sha256'],'independent collected terminal pin')
    instance=pins['instance_id'];req(col['terminated'] is True and col['instance_id']==instance,'terminated original')
    req(col['user_data_sha256']==pins['user_data_sha256'],'independent launch user-data provenance')
    req(term['schema']=='borsuk-actual-cohort-parity-closed-v1' and term['instance_id']==instance and term['phase']=='complete' and type(term['original_exit']) is int and term['original_exit']==0 and type(term['exit']) is int and term['exit']==0 and term['publication_verified'] is False and term['performance_claim'] is False and term['ann_run'] is False,'outer original outcome')
    req(manager['schema']=='borsuk-parity-bootstrap-exit-v1' and manager['instance_id']==instance and manager['terminal_sha256']==digest(terminal_body) and manager['exit_code']=='exited' and manager['exit_status']=='0' and manager['service_result']=='success' and manager['final_exit']=='0','original main and final child outcome')
    archive=R/'evidence.tar.gz';req(0<archive.stat().st_size<=67108864 and archive.stat().st_size==term['evidence']['bytes'],'archive size');req(digest(archive.read_bytes())==term['evidence']['sha256']==pins['archive_sha256'],'independent archive SHA')
    bodies={};total=0
    EvidenceHeader.headers=0
    with tarfile.open(archive,'r:gz',tarinfo=EvidenceHeader) as t:
        for m in t:
            req(not m.name.startswith('/') and '..' not in PurePosixPath(m.name).parts,'archive path');name=str(PurePosixPath(m.name))
            if m.isdir():continue
            req(m.isfile() and name not in bodies and m.size<=33554432,'archive type/duplicate/member cap');total+=m.size;req(total<=67108864,'expanded evidence cap')
            f=t.extractfile(m);req(f is not None,'archive stream');bodies[name]=f.read(m.size+1);req(len(bodies[name])==m.size,'body length')
    listed={}
    manifest_body=bounded(R/'artifacts.sha256',1048576)
    req(digest(manifest_body)==pins['artifacts_manifest_sha256'],'independent manifest pin')
    for line in manifest_body.decode().splitlines():
        match=re.fullmatch(r'([0-9a-f]{64})  (.+)',line);req(match is not None,'manifest syntax');name=str(PurePosixPath(match[2]));req(name not in listed,'manifest duplicate');listed[name]=match[1]
    req(set(listed)==set(bodies),'closed exact inventory');req(all(digest(bodies[n])==h for n,h in listed.items()),'inventory SHA')
    req(digest(bodies['support.sha256'])==pins['support_manifest_sha256'],'frozen support provenance')
    for n in ('finalized-config.json','native-complete.json','evidence-local/config.json','evidence-local/terminal.json','evidence-local/native.stdout.json'):
        req(0<len(bodies[n])<=65536,'native/control body cap '+n)
    j=lambda n:decode(bodies[n]);jl=lambda n:[decode(l) for l in bodies[n].splitlines()]
    for n in ('transport','parity'):req(bodies[n+'-unit.exit'].strip()==b'0','original '+n+' wait exit')
    for n in ('transport-exit.json','service-exit.json'):
        m=j(n);req(m['schema']=='borsuk-parity-service-exit-v1' and m['exit_code']=='exited' and m['exit_status']=='0' and m['service_result']=='success','manager '+n)
    local=j('evidence-local/terminal.json');req(local['schema']=='borsuk-actual-cohort-admission-local-v1' and local['status']=='ADMISSION_VERIFIED' and type(local['intended_exit']) is int and local['intended_exit']==0 and local['actual_manager_and_outer_exit_required'] is True and local['performance_claim'] is False,'local intended status')
    for n in ('config.validated.txt','receipt.validated.txt','resource-closure.after.txt','resource-closure.closed.txt'):
        req(bodies['evidence-local/'+n]==b'true\n','validation marker '+n)
    req(local['elf_sha256']==pins['elf_sha256'],'qualified ELF binding')
    for n in ('native','timeout','time','tee','wrapper','time-log','supervisor-stderr-log','native-stderr-log'):req(bodies['evidence-local/'+n+'.exit']==b'0\n','original '+n+' status')
    for n in ('native.stderr.txt','supervisor.stderr.txt','native.time.txt'):
        req(len(bodies['evidence-local/'+n])<=1048576,'log write cap '+n)
    req(len(bodies['evidence-local/native.stdout.json'])<=1024,'native stdout write cap')
    req(bodies['evidence-local/inputs.before.jsonl']==bodies['evidence-local/inputs.after.jsonl'],'whole-input closure')
    inputs=jl('evidence-local/inputs.before.jsonl');observed={(v['bytes'],v['sha256']) for v in inputs};req(set(tuple(v) for v in pins['input_size_sha'])<=observed,'all frozen input pins')
    cfg=j('finalized-config.json');cfgbody=bodies['finalized-config.json'];req(digest(cfgbody)==local['config_sha256'],'exact final config SHA');req(cfg==j('evidence-local/config.json'),'config copy')
    expected=pins['config_template'];comparable=dict(cfg);comparable['output_parent']=expected['output_parent'];comparable['shards']=[dict(v,path=expected['shards'][i]['path']) for i,v in enumerate(cfg['shards'])];req(comparable==expected,'strict config template')
    req(cfg['output_parent']['path']==expected['output_parent']['path'],'exact parent path')
    req(len(cfg['shards'])==11 and [v['path'] for v in cfg['shards']]==['/mnt/borsuk-scale1m/assets/input/en/%04d.parquet'%i for i in range(11)],'exact installed source paths')
    req(type(cfg['output_parent']['device']) is int and cfg['output_parent']['device']>0 and type(cfg['output_parent']['inode']) is int and cfg['output_parent']['inode']>0,'admitted parent identity')
    req(bodies['evidence-local/resources.before.effective.json']==bodies['evidence-local/resources.after.effective.json']==bodies['evidence-local/resources.closed.effective.json'],'all effective resource closure');eff=j('evidence-local/resources.before.effective.json');req(eff['memory_max_bytes']==8589934592 and eff['swap_max_bytes']==0 and eff['pids_max']==128 and eff['cpu_quota_cores']==4 and eff['cpuset']=='0-3','exact resource limits')
    def counters(value):
        if value=='absent':return {}
        pairs=[line.split() for line in value.splitlines()];req(all(len(pair)==2 and pair[1].isdigit() for pair in pairs),'counter syntax');req(len({pair[0] for pair in pairs})==len(pairs),'duplicate counters');return {k:int(v) for k,v in pairs}
    before=jl('evidence-local/resources.before.jsonl');keys=('path','cpu_max','cpuset_cpus_effective','memory_max','memory_swap_max','pids_max')
    req(len(before)>0,'resource ancestor evidence')
    for phase in ('after','closed'):
        after=jl('evidence-local/resources.'+phase+'.jsonl');req(len(before)==len(after),'ancestor count')
        for a,b in zip(before,after):
            req(all(a[k]==b[k] for k in keys),'ancestor limits');ac,bc=counters(a['memory_events']),counters(b['memory_events']);req(all(ac.get(k)==bc.get(k) for k in ('oom','oom_kill','oom_group_kill')),'memory event closure');req(bc.get('max',0)>=ac.get('max',0),'reclaim counter monotonicity');req(counters(a['pids_events'])==counters(b['pids_events']),'pids event closure')
        leaf=after[0];req(int(leaf['memory_current'])<=8589934592 and int(leaf['memory_peak'])<=8589934592 and int(leaf['memory_swap_current'])==0,'leaf memory evidence')
        previous='before' if phase=='after' else 'after'
        events=j('evidence-local/resource-events.'+phase+'.json')
        expected_events=[{'path':a['path'],'before':counters(a['memory_events']).get('max'),'after':counters(b['memory_events']).get('max'),'delta':counters(b['memory_events']).get('max',0)-counters(a['memory_events']).get('max',0)} for a,b in zip(before,after)]
        req(same(events,{'from':previous,'to':phase,'memory_max_events':expected_events,'valid':True}),'independent reclaim event delta')
        before=after
    req(bodies['evidence-local/outputs.authenticated.jsonl']==bodies['evidence-local/outputs.closed.jsonl'],'whole output closure')
    receipt=j('native-complete.json');seal=j('evidence-local/native.stdout.json');req(seal['status']=='COMPLETE' and seal['receipt_sha256']==digest(bodies['native-complete.json']),'native receipt seal')
    req(receipt['schema']=='borsuk-cohere-native-cohort-receipt-v3' and receipt['status']=='COMPLETE' and receipt['config']['sha256']==local['config_sha256'] and receipt['resources']==cfg['resources'] and receipt['output_parent']==cfg['output_parent'],'native receipt configuration')
    req(receipt['config']['bytes']==len(cfgbody) and receipt['config']['path']=='/mnt/borsuk-scale1m/finalized-config.json','native config identity')
    expected_runtime={k:v for k,v in eff.items() if k not in ('path','cpuset','pids_max')}
    expected_runtime['enforcement']='cgroup-v2'
    req(receipt['runtime_limits']==expected_runtime,'native runtime enforcement identity')
    req(receipt['dataset']==cfg['dataset'] and receipt['revision']==cfg['revision'] and receipt['columns']=={'embedding':'emb','document_id':'_id'} and receipt['metric']=='cosine','publisher identity')
    req(len(receipt['sources'])==11,'native footer roster')
    for s,c in zip(receipt['sources'],cfg['shards']):
        req(all(s[k]==c[k] for k in ('publisher_path','path','bytes','sha256','rows')),'native source pin')
        req(type(s['footer_bytes']) is int and 0<s['footer_bytes']<=1048576,'native footer bounds')
        matches=[v for v in inputs if v['path']==s['path']];req(len(matches)==1 and all(s[k]==matches[0][k] for k in ('device','inode')),'authenticated source descriptor')
    req(receipt['truth_method']=='independent single corpus block-major exhaustive scan with bounded per-query top-k heaps; no ANN inputs' and receipt['truth_ties']=='ascending corpus ordinal','fresh native truth method')
    req(receipt['geometry']['query_source_ordinals']==[100000,100032],'query source ordinals')
    for key in ('observed_peak_rss_at_receipt_bytes','modeled_peak_bytes'):
        req(type(receipt[key]) is int and 0<receipt[key]<=cfg['resources']['actual_memory_bytes'],'native memory '+key)
    req(receipt['modeled_peak_bytes']<=cfg['resources']['modeled_memory_bytes'],'modeled memory admission')
    accounting={
        'id_state_bytes':1281280000,'retained_query_vector_bytes':131072,
        'truth_vector_block_bytes':1048576,'truth_all_query_top_k_bytes':5120,
        'truth_query_norm_bytes':256,'truth_heap_header_bytes':768,
        'truth_single_sorted_output_reserve_bytes':160,'retained_shard_metadata_bytes':1476395008,
        'encoded_and_decoded_row_bytes':8192,'control_output_and_failure_buffers_bytes':67108864,
        'caller_memory_bytes':67108864,'resident_peak_bytes':2893086880,
        'admitted_decoder_peak_bytes':receipt['modeled_peak_bytes']-2893086880,
        'source_bytes':2382253857,'output_cap_bytes':11271367168,
        'caller_scratch_bytes':1610612736,'temporary_and_failure_reserve_bytes':67108864,
        'failure_outputs_retained_within_output_cap':True,'process_rss_is_separate':True}
    req(same(receipt['resource_accounting'],accounting) and accounting['admitted_decoder_peak_bytes']>0,'exact native resource accounting')
    req(type(receipt['elapsed_seconds_at_receipt']) in (int,float) and 0<receipt['elapsed_seconds_at_receipt']<2400,'native elapsed bound')
    timing=bodies['evidence-local/native.time.txt'].decode()
    def time_field(label):
        values=re.findall(r'^\s*'+re.escape(label)+r':\s*(\d+)\s*$',timing,re.M);req(len(values)==1,'GNU time '+label);return int(values[0])
    peak=time_field('Maximum resident set size (kbytes)')*1024
    req(receipt['observed_peak_rss_at_receipt_bytes']<=peak<=8589934592 and time_field('Swaps')==0 and time_field('Exit status')==0,'original GNU time evidence')
    req(receipt['geometry']['corpus_rows']==1000000 and receipt['geometry']['query_rows']==32 and receipt['geometry']['dimensions']==1024 and receipt['geometry']['k']==10,'actual geometry')
    req(receipt['reserved_queries_sha256']=='8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e','all reserved query seal')
    outputs={v['name']:v for v in receipt['outputs']};req(set(outputs)=={'corpus.f32','corpus.ids.jsonl','queries.f32','queries.ids.jsonl','truth.u64'},'exact native output roster')
    req(all(set(v)=={'name','bytes','sha256'} and type(v['bytes']) is int and 0<v['bytes']<=7175168000 and re.fullmatch('[0-9a-f]{64}',v['sha256']) for v in receipt['outputs']),'native output descriptor contract')
    req(bodies['evidence-local/receipt.authenticated.jsonl']==bodies['evidence-local/receipt.closed.jsonl'],'receipt descriptor closure')
    rs=jl('evidence-local/receipt.closed.jsonl');req(len(rs)==1 and rs[0]['bytes']==len(bodies['native-complete.json']) and rs[0]['sha256']==digest(bodies['native-complete.json']),'independent complete receipt authentication')
    req(len(receipt['outputs'])==5 and outputs['corpus.f32']['bytes']==4096000000 and outputs['queries.f32']['bytes']==131072 and outputs['truth.u64']['bytes']==2560,'exact 1M native vector/truth geometry')
    req(receipt['geometry']['corpus_intervals']==[{'start':0,'end':100000},{'start':101000,'end':1001000}] and receipt['geometry']['reserved_query_interval']=={'start':100000,'end':101000},'query exclusion')
    req(receipt['truth_arithmetic']=='sequential f64 dot and squared-norm sums over original f32; 1-dot/(sqrt(cnorm2)*sqrt(qnorm2))','frozen truth arithmetic')
    authenticated=jl('evidence-local/outputs.authenticated.jsonl')
    req(sorted((Path(v['path']).name,v['bytes'],v['sha256']) for v in authenticated)==sorted((v['name'],v['bytes'],v['sha256']) for v in receipt['outputs']),'every output independently authenticated')
    prefixes=j('evidence-local/prefixes.json')
    req(prefixes['corpus']['prefix_bytes']==409600000 and prefixes['corpus']['prefix_sha256']=='3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c','historical corpus prefix')
    for name,label,size in [('queries.f32','requests',131072)]:
        req(prefixes[label]['prefix_bytes']==size and outputs[name]['bytes']==size and outputs[name]['sha256']==prefixes[label]['prefix_sha256']=='664f5b269756a1de5a77c4ec359e56ccbe85c87603fa01fc5d87cc3f02e52667','historical prefix '+label)
    req(len(bodies['actual-truth.u64'])==2560 and digest(bodies['actual-truth.u64'])==outputs['truth.u64']['sha256'],'collected opaque truth prefix SHA')
    closed=set()
    for line in bodies['evidence-local/closure.sha256'].decode().splitlines():
        match=re.fullmatch(r'([0-9a-f]{64})  (.+)',line);req(match is not None,'local closure syntax');req(digest(bodies['evidence-local/'+match[2]])==match[1],'local closure '+match[2])
        req(match[2] not in closed,'local closure duplicate');closed.add(match[2])
    expected_closed={n[len('evidence-local/'):] for n in bodies if n.startswith('evidence-local/')} - {'closure.sha256','wrapper.exit','terminal.json'}
    req(closed==expected_closed and len(closed)>0,'complete local closure inventory')
    print(json.dumps(dict(status='ADMISSION_VERIFIED',instance_id=instance,terminated=True,rows=1000000,queries=32,performance_claim=False,ann_run=False,truth_scope='fresh 1M exhaustive native arithmetic; no historical truth equality' ,historical_id_bytes_parity=False)))
if __name__=='__main__':main()
