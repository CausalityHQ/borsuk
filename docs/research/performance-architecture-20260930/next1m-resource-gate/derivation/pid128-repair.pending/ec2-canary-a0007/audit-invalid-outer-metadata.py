from pathlib import Path,PurePosixPath
import tarfile,hashlib,json,re
D=Path('/tmp/borsuk-next1m-supervisor-canary-a0007-70448857');C=D/'collection';A=C/'evidence.tar.gz'
T=json.loads((C/'terminal.json').read_text())
assert T['instance_id']=='i-07ecdbc6eb997bddc'
assert A.stat().st_size==T['evidence_bytes'] and hashlib.sha256(A.read_bytes()).hexdigest()==T['evidence_sha256']
assert (C/'volume.absent').read_text().strip()=='true'
I=json.loads((C/'instance.after.json').read_text())['Reservations'][0]['Instances'][0]
assert I['InstanceId']==T['instance_id'] and I['State']['Name']=='terminated'
assert T['status']=='INVALID' and [T[n] for n in ['bootstrap_exit','test_exit','cleanup_exit']]==[1,0,0]
with tarfile.open(A) as f:
 members=f.getmembers();mp={m.name:m for m in members};assert len(mp)==len(members)
 for m in members:
  p=PurePosixPath(m.name);assert not p.is_absolute() and '..' not in p.parts and (m.isdir() or m.isfile())
 def read(n):
  m=mp[n];assert m.isfile() and m.size<=16777216;return f.extractfile(m).read()
 root='borsuk-pid-evidence/collector-smoke/'
 sealed=set()
 for line in read(root+'SHA256SUMS').decode().splitlines():
  match=re.fullmatch(r'([0-9a-f]{64})  (\./.+)',line);assert match
  h,rel=match.groups();n=root+rel[2:];assert n not in sealed and hashlib.sha256(read(n)).hexdigest()==h;sealed.add(n)
 assert sealed=={m.name for m in members if m.isfile() and m.name.startswith(root) and m.name!=root+'SHA256SUMS'}
 R=json.loads(read(root+'result.json'))
 assert R['status']=='COLLECTOR_MECHANICS_VERIFIED' and R['synthetic_chain_metadata'] is True and R['ann_executed'] is False and R['performance_claim'] is False
 cases=['positive0','positive2','positive3','exit-disagreement','wrong-config','truncated-seal','replaced','populated','deadline']
 assert read(root+'completed-cases.txt').decode().splitlines()==cases
 def val(n):return read(root+n).decode().strip()
 for case in cases:
  assert val(case+'/harness-cleanup.exit')=='0'
  rc=int(val(case+'/collector.exit'))
  assert rc==0 if case.startswith('positive') else rc not in [0,124,137]
 for code in [0,2,3]:
  c='positive'+str(code)+'/'
  o=json.loads(read(root+c+'evidence-root/chain-outer/outer-closure.json'))
  assert o['status']=='CLOSED' and o['actual_outer_exit']==code and o['drained'] is True
  assert o['invocation_id'] in val(c+'launch.identity')
 for case in ['populated','deadline']:
  c=case+'/'
  assert val(c+'evidence-root/chain-outer/failure-cleanup.exit')=='0'
  assert 'MainPID=0' in val(c+'collector-cleanup.after').splitlines()
  assert 'populated 1' in val(c+'live.before.events').splitlines()
  if root+c+'collector-cleanup.after.events' in mp:
   assert 'populated 0' in val(c+'collector-cleanup.after.events').splitlines()
  else:assert root+c+'collector-cleanup.removed' in mp
 assert int(val('deadline/collector.elapsed.seconds'))<=18
 assert 'populated 1' in val('populated/evidence-root/chain-outer/drain.events').splitlines()
 assert 'ActiveState=active' in val('replaced/replacement.after').splitlines()
 assert val('replaced/evidence-root/chain-outer/failure-cleanup.exit')=='1'
 result={'status':'COLLECTOR_NINE_CASES_AUTHENTICATED_OUTER_INVALID','source_commit':'70448857f8016cb3ed68029387c60eec9c45c3c2','instance_id':T['instance_id'],'archive_bytes':A.stat().st_size,'archive_sha256':T['evidence_sha256'],'sealed_regular_files':len(sealed),'completed_cases':cases,'original_exits':[0,0,0],'collector_live_cleanup_proved':True,'terminated':True,'volume_absent':True,'all_phase_metadata_synthetic':True,'ann_executed':False,'performance_claim':False,'full_native_chain_qualified':False,'local_action':'closed metadata and opaque hash audit only; no fixture/native/runtime-validator execution','remaining':['exact-source real1M native gate and Q32 recall screen','fresh physical cold S3 measurements']}
(D/'closed-metadata-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
