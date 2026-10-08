from pathlib import Path
import json,hashlib,tarfile,re,datetime
r=Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0001')
p=json.loads((r/'protocol.json').read_text());t=json.loads((r/'terminal.json').read_text());j=json.loads((r/'active-job.json').read_text())
assert j['status']=='TERMINATED_COLLECTED_NOT_YET_VERIFIED' and t['instance_id']==j['instance_id']
assert json.loads((r/'wait.json').read_text())['exit']==0
assert t['candidate_commit']==t['source_commit']==p['candidate']
assert t['native_source_identity_sha256']==p['source']['native_identity']
a=r/'evidence.tar.gz';assert a.stat().st_size==t['evidence']['bytes'] and hashlib.sha256(a.read_bytes()).hexdigest()==t['evidence']['sha256']
pins={n.removeprefix('./'):h for h,n in (x.split('  ',1) for x in (r/'artifacts.sha256').read_text().splitlines())};data={};total=0
with tarfile.open(a) as tar:
 for m in tar:
  if m.isdir():continue
  n=m.name.removeprefix('./');assert m.isfile() and n in pins and n not in data and not n.startswith('/') and '..' not in Path(n).parts
  total+=m.size;assert total<128*1024*1024
  b=tar.extractfile(m).read();assert hashlib.sha256(b).hexdigest()==pins[n];data[n]=b
assert set(data)==set(pins)
for n,pin in p['inputs'].items():assert len(data[n])==pin['bytes'] and hashlib.sha256(data[n]).hexdigest()==pin['sha256']
support={n:h for h,n in (x.split('  ',1) for x in data['source-files.sha256'].decode().splitlines())}
for n in ['source-before.log','source-after.log']:assert data[n].decode().splitlines()==[x+': OK' for x in support]
log=data['runner-debug.log'].decode();passed=re.findall(r'^test ([\w:]+) \.\.\. ok$',log,re.M);failed=re.findall(r'^test ([\w:]+) \.\.\. FAILED$',log,re.M)
assert len(passed)==9 and failed==['tests::scratch_formula_keeps_codec_and_trace_separate']
assert "assertion failed: (K..=257).contains(&rows)" in log
assert int(data['runner-debug.native-exit'])==101 and int(data['runner-debug.tee-exit'])==0
assert t['original_exit']==t['exit']==t['native_exit']==1
assert not any((stage['name']+'.native-exit') in data for stage in p['serial_stages'][1:])
assert data['cpu.max.before'].strip()==b'200000 100000' and data['memory.max.before'].strip()==b'8589934592' and data['memory.swap.max.before'].strip()==b'0' and data['pids.max.before'].strip()==b'512'
for scope in ['', 'global-']:
 events=dict(x.split() for x in data[scope+'memory.events.after'].decode().splitlines());assert int(events['oom'])==int(events['oom_kill'])==0
 assert int(data[scope+'memory.swap.peak.after'])==0 and int(data[scope+'memory.peak.after'])<=8589934592
assert int(data['global-pids.current.after'])==0 and b'MainPID=0' in data['systemd-after.txt']
receipt={'status':'EXECUTION_INVALID_TEST_FIXTURE_SHAPE','candidate':p['candidate'],'instance_id':j['instance_id'],'terminated':True,'wait_exit':0,'artifact_count':len(data),'source_before_after_equal':True,'native_identity':p['source']['native_identity'],'runner_debug':{'compiled':True,'passed':passed,'failed':failed,'native_exit':101,'tee_exit':0,'log_sha256':pins['runner-debug.log']},'remaining_stage_count':12,'remaining_stages':'UNRUN','root_cause':'new scratch formula test calls Shape::tiny(100000), which explicitly permits only K..=257; use Shape::PRODUCTION for the production arithmetic assertion','repair_scope':'existing test only; preserve algorithm, tiny guard, scientific limits and names','algorithm_rejected':False,'performance_claim':False,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(r/'independent-failure-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
