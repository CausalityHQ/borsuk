import hashlib,json,re,subprocess
from pathlib import Path
b=Path('/home/rb/worktrees/borsuk-corrected-fourbit-qualification-bundle/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/implementation-gates/a0001')
def load(n):return json.loads((b/n).read_bytes())
def pin(p):
 h=hashlib.sha256();size=0
 with p.open('rb') as f:
  while x:=f.read(65536):h.update(x);size+=len(x)
 return {'bytes':size,'sha256':h.hexdigest()}
t=load('aws-terminal.json');w=load('workspace-receipt.json');q=load('source-qualification.json');c=load('workspace-cgroup.json');close=load('aws-closeout.json');cfg=load('config.json')
assert t['instance_id']=='i-0792fe6e3a68fcb11' and t['exit_code']==t['original_exit_code']==w['exit_status']==w['gate_status']==0
assert close['state']=='terminated' and close['nodes']['0']['instance_id']==t['instance_id']
assert t['source_commit']=='a80d423bd2c5ba19e7beafaac6865b5c61cad42f' and t['native_source_commit']=='4ec11b94bdc15b103bc713418e9f2c0705a9d0b6'
assert t['config_sha256']==pin(b/'config.json')['sha256']==q['config_sha256']==w['config_sha256']
for n,p in t['artifacts'].items():assert pin(b/n)=={k:p[k] for k in ('bytes','sha256')},n
source=load('source-before.json');assert source==load('source-after.json')==q['source_sha256']==w['source_sha256'] and len(source)==405
identity=hashlib.sha256(json.dumps(source,sort_keys=True,separators=(',',':')).encode()).hexdigest();assert identity==t['source_identity_sha256']==q['source_identity_sha256']==w['source_identity_sha256']
for n,h in source.items():assert hashlib.sha256(subprocess.check_output(['git','show',t['source_commit']+':'+n])).hexdigest()==h,n
log=(b/'test.log').read_text();rows=[]
for line in log.splitlines():
 try:d=json.loads(line)
 except (ValueError,TypeError):continue
 if isinstance(d,dict) and 'stage' in d and 'started_at' in d:rows.append(d)
assert len(w['stages'])==15 and len(rows)==30
for i,s in enumerate(w['stages']):
 assert s['exit_status']==s['gate_status']==s['log_exit_status']==0
 assert rows[2*i]['stage']==rows[2*i+1]['stage']==s['stage'] and rows[2*i]['command']==rows[2*i+1]['command']==s['command']
 assert rows[2*i+1]['exit_status']==rows[2*i+1]['gate_status']==rows[2*i+1]['log_exit_status']==0
 segment=log.split(json.dumps(rows[2*i],sort_keys=True,separators=(',',':')))[-1] if False else None
 for name in q['mandatory_tests'].get(s['stage'],[]):assert s['required_test_passes'][name]==1 and len(re.findall(r'^test '+re.escape(name)+r' \.\.\. ok$',log,re.M))>=1,name
 assert s['tests_run'] is None or s['tests_run']>0
assert w['environment']['CARGO_BUILD_JOBS']=='1' and w['environment']['BORSUK_TEST_BUILD_COMMAND'] is None and w['environment']['RUSTC_WRAPPER']==''
a=c['after'];events=dict(x.split() for x in a['memory.events'].splitlines());assert c['closed'] and int(a['memory.peak'])<=8589934592 and a['memory.max'].strip()=='8589934592' and a['memory.swap.max'].strip()==a['memory.swap.peak'].strip()=='0' and all(events[k]=='0' for k in ('oom','oom_kill','oom_group_kill'))
assert a['process_ids']==[a['observer_pid']] and a['cpu.max'].strip()=='200000 100000' and a['pids.max'].strip()=='512'
r={'schema':'borsuk-corrected-four-bit-parent-verification-v1','qualified':True,'native_source_commit':t['native_source_commit'],'source_commit':t['source_commit'],'source_identity_sha256':identity,'source_file_count':405,'binary':pin(b/'binaries/hierarchical_semantic_cells'),'stages':w['stages'],'terminal_sha256':pin(b/'aws-terminal.json')['sha256'],'terminal_artifacts_authenticated':len(t['artifacts']),'memory_peak_bytes':int(a['memory.peak']),'swap_peak_bytes':0,'oom':0,'compiler_memory_max_events':int(events['max']),'instance_id':t['instance_id'],'terminated':True,'actual_full_workspace_execution':False,'science_qualified':False,'original_exec_session':83229}
p=b/'parent-verification.json';assert not p.exists();p.write_text(json.dumps(r,sort_keys=True,indent=2)+'\n');print(json.dumps({k:v for k,v in r.items() if k!='stages'},indent=2))
