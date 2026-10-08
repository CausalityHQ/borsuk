from pathlib import Path,PurePosixPath
import json,hashlib,tarfile
p=Path(__file__).parent;t=json.loads((p/'terminal.json').read_text());w=json.loads((p/'wait.json').read_text());b=(p/'evidence.tar.gz').read_bytes()
assert len(b)==t['evidence']['bytes'] and hashlib.sha256(b).hexdigest()==t['evidence']['sha256']
assert t['instance_id']==w['instance_id']=='i-00180615e2b84beec' and w['terminated'] and w['wait_exit']==0
assert t['phase']=='complete' and t['original_exit']==t['exit']==0 and t['native_exit']==2 and t['performance_claim'] is False
out=p/'collected';out.mkdir(exist_ok=True);roster={};total=0
with tarfile.open(p/'evidence.tar.gz','r:gz') as ar:
 members=ar.getmembers();assert len(members)<100
 for m in members:
  path=PurePosixPath(m.name);assert not path.is_absolute() and '..' not in path.parts
  if m.isdir():continue
  assert m.isfile() and m.size<=67108864
  n=str(path);assert n not in roster;total+=m.size;assert total<=134217728
  data=ar.extractfile(m).read();assert len(data)==m.size
  roster[n]=hashlib.sha256(data).hexdigest();dest=out/path;dest.parent.mkdir(exist_ok=True,parents=True)
  if dest.exists():assert dest.read_bytes()==data
  else:dest.write_bytes(data)
manifest={}
for line in (p/'artifacts.sha256').read_text().splitlines():
 digest,name=line.split('  ',1);name=str(PurePosixPath(name));assert name not in manifest;manifest[name]=digest
assert manifest==roster
pr=json.loads((p/'preregistration.json').read_text());assert hashlib.sha256((out/'stage.sh').read_bytes()).hexdigest()==pr['stage']['sha256']
assert hashlib.sha256((out/'config.json').read_bytes()).hexdigest()=='e604a1ca02644329113e6c11a935d67115f784c40008859966f7e332fae53a3a'
for field,value in {'cpu.max':'100000 100000','memory.max':'536870912','memory.swap.max':'0','pids.max':'256'}.items():assert (out/(field+'.before')).read_text().strip()==value
for f in ['pins-before.txt','pins-after.txt']:assert all(x.endswith(': OK') for x in (out/f).read_text().splitlines())
assert not (out/'scratch-files.txt').read_text().strip()
assert (out/'native-exit').read_text().strip()=='2' and (out/'original-exit').read_text().strip()=='0' and (out/'final-exit').read_text().strip()=='0'
state=(out/'systemd-after.txt').read_text();assert 'MainPID=0' in state and 'ActiveState=inactive' in state
records=[json.loads(x) for x in (out/'cli-invalid.jsonl').read_text().splitlines() if x.strip()]
assert len(records)==2 and records[0]['phase']=='identity' and records[1]['phase']=='terminal'
s=records[1]['summary'];assert s['stage']=='config' and s['error']=='artifact identity/SHA' and s['completed_queries']==0 and not s['truth_opened'] and not s['all_queries_sealed'] and s['sum']['submitted_gets']==s['binding_charge']['submitted_gets']==0
assert (out/'canary.status').read_text().strip()=='PROFILER_PLATFORM_AND_CLI_CANARY_PASS_NO_ANN'
samples=(out/'perf-samples.txt').read_text();assert 'sha256sum' in samples and 'SHA256_Update' in samples
multi=sum(sum(1 for l in block.splitlines() if l.strip() and l[0].isspace())>=2 for block in samples.split('\n\n'));assert multi>=10,multi
assert 'libcrypto' in (out/'perf-report.txt').read_text() and '# Total Lost Samples: 0' in (out/'perf-report.txt').read_text()
events=dict(x.split() for x in (out/'memory.events.after').read_text().splitlines());assert events['oom']==events['oom_kill']=='0'
assert (out/'memory.swap.peak.after').read_text().strip()=='0'
verify={'status':'DISPOSABLE_CPU_PROFILER_PLATFORM_CANARY_PASS','instance_id':t['instance_id'],'terminated':True,'wait_exit':0,'original_watch_session':2702,'evidence_bytes':len(b),'evidence_sha256':t['evidence']['sha256'],'artifact_count':len(roster),'artifact_sha_verified':True,'stage_caps_verified':True,'decoded_multiframe_samples':multi,'peak_bytes':int((out/'memory.peak.after').read_text()),'perf_version':(out/'perf-version.txt').read_text().strip(),'kernel':(out/'kernel.txt').read_text().strip(),'native_queries':0,'native_gets':0,'truth_opened':False,'ANN_claim':False,'performance_claim':False,'Rust_stack_attribution_verified':False,'verification_repair':'Initial root check wrongly required executable DSO sha256sum in flat report; all sampled CPU belongs to libcrypto. Matching decoded SHA256_Update frames establish the preregistered platform check. No cloud/native rerun.'}
(p/'independent-verification.json').write_text(json.dumps(verify,indent=2)+'\n')
job=json.loads((p/'active-job.json').read_text());job.update(status='TERMINATED_COLLECTED_VERIFIED',background=False,original_watch_session=2702);(p/'active-job.json').write_text(json.dumps(job,indent=2)+'\n');print(json.dumps(verify))
