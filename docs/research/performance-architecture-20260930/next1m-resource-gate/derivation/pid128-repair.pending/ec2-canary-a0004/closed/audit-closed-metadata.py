from pathlib import Path,PurePosixPath
import tarfile,json,hashlib,re
D=Path('/tmp/borsuk-next1m-supervisor-canary-a0004-3a1b3ca9');C=D/'collection';T=json.loads((C/'terminal.json').read_text());A=C/'evidence.tar.gz'
assert T['instance_id']=='i-0612ad24ad4bc9912' and T['status']=='WRAPPER_CANARY_ROOT_REPLAY_REQUIRED'
assert [T[x] for x in ['bootstrap_exit','test_exit','cleanup_exit']]==[0,0,0]
assert A.stat().st_size==T['evidence_bytes'] and hashlib.sha256(A.read_bytes()).hexdigest()==T['evidence_sha256']
assert (C/'volume.absent').read_text().strip()=='true'
I=json.loads((C/'instance.after.json').read_text());assert I['Reservations'][0]['Instances'][0]['InstanceId']==T['instance_id'] and I['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
with tarfile.open(A) as tf:
 members=tf.getmembers(); names=[m.name for m in members];assert len(names)==len(set(names));mp={m.name:m for m in members}
 for m in members:
  p=PurePosixPath(m.name);assert not p.is_absolute() and '..' not in p.parts
  assert m.isfile() or m.isdir() or (m.name=='borsuk-pid-evidence/canary/fifo' and m.isfifo() and m.size==0)
 def read(n):
  m=mp[n];assert m.isfile() and m.size<=33554432;return tf.extractfile(m).read()
 root='borsuk-pid-evidence/canary/'
 manifest=read(root+'SHA256SUMS').decode(); checked=[]
 for line in manifest.splitlines():
  match=re.fullmatch(r'([0-9a-f]{64})  (\./.+)',line);assert match,line
  h,rel=match.groups();n=root+rel[2:];assert n not in checked and hashlib.sha256(read(n)).hexdigest()==h;checked.append(n)
 assert set(checked)=={m.name for m in members if m.isfile() and m.name.startswith(root) and m.name!=root+'SHA256SUMS'}
 R=json.loads(read(root+'result.json'));assert R['status']=='WRAPPER_CHECKS_VERIFIED' and R['ann_run'] is False and R['performance_claim'] is False
 components=json.loads(read(root+'replay-component-checks.json'));checks=components['checks'];assert components['status']=='COMPONENT_CHECKS_VERIFIED' and len(checks)==48
 assert len({c['case'] for c in checks})==48 and sum(c.get('positive') is True for c in checks)==4 and sum(c.get('refused') is True for c in checks)==44
 for name,code in [('build_sq8_source',1),('build_two_bit_generation',1),('publish_two_bit_generation',2),('check_cohere_native_baseline',2)]:
  assert read(root+'usage-'+name+'.exit').decode().strip()==str(code)
 p=root+'fixture-escaped-evidence/phases/fixture/'
 drain=json.loads(read(p+'drain.json'));w=json.loads(read(p+'child.witness.json'));unit=read(p+'unit').decode().strip()
 manager=dict(line.split('=',1) for line in read(p+'manager.initial.txt').decode().splitlines() if '=' in line)
 assert manager['Id']==unit and drain['path']=='/sys/fs/cgroup'+manager['ControlGroup'] and drain['invocation_id']==manager['InvocationID']==w['invocation_id']
 assert drain['schema']=='borsuk-native-pid128-payload-drain-v1' and drain['state'] in ['removed','empty']
 assert w['control_group']==manager['ControlGroup'] and w['alive_after_timeout'] is True and w['separate_process_group_and_session'] is True
 assert read(p+'manager.stop.exit').decode().strip()=='0' and read(root+'fixture-escaped-evidence/cleanup.exit').decode().strip()=='0'
 assert read(p+'timeout.exit').decode().strip()=='124'
 result={'status':'DISPOSABLE_SUPERVISOR_CANARY_METADATA_AUTHENTICATED','instance_id':T['instance_id'],'source_commit':'3a1b3ca9f6f9872290b1169989cc286881dfbca2','archive_bytes':A.stat().st_size,'archive_sha256':T['evidence_sha256'],'archive_members':len(members),'sealed_regular_files':len(checked),'component_checks':48,'positive_components':4,'refusal_components':44,'native_cli_usage_exits':[1,1,2,2],'escaped_witness_and_original_drain_bound':True,'original_exits':[0,0,0],'terminated':True,'volume_absent':True,'full_chain_replay_exercised':False,'ann_executed':False,'performance_claim':False,'local_action':'closed metadata and opaque hashes only; no fixture/native/runtime-validator execution','remaining':['complete native chain/collector qualification on exact source and real1M inputs','Q32 quality gate','fresh physical cold S3 measurements']}
(D/'closed-metadata-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
