import hashlib,json,pathlib,tarfile
D=pathlib.Path('/tmp/borsuk-root-closure-falsifier/ec2-a0001'); C=D/'collected'
term=json.loads((C/'terminal.json').read_text()); blob=(C/'evidence.tar.gz').read_bytes()
assert len(blob)==term['evidence_bytes'] and hashlib.sha256(blob).hexdigest()==term['evidence_sha256']
assert term['bootstrap_exit']==term['test_exit']==0 and term['status']=='SYNTHETIC_CLOSURE_CHECKS_ONLY'
expected=dict(positive=0,unrelated_observer=1,wrong_invocation=1,wrong_snapshot=1,nested_wrapper=1,nested_closure=1,manager_duplicate=98,manager_failed=98,wrong_outer_exit=98,populated_one=98,multi_document_outer=98,same_device=0,different_device=98,first_stat_nonzero=98,second_stat_nonzero=98)
with tarfile.open(C/'evidence.tar.gz') as t:
 def read(n): return t.extractfile('./'+n).read()
 assert json.loads(read('cases/result.json'))['passed']==15
 for name,code in expected.items(): assert read('cases/'+name+'/check.exit')==f'{code}\n'.encode()
 assert read('harness.exit')==b'0\n'
 for file in ['memory.events.after','pids.events.after']:
  counters=dict(line.split() for line in read(file).decode().splitlines()); assert all(int(counters.get(k,0))==0 for k in ['oom','oom_kill','max'])
 assert hashlib.sha256(read('recipe.sh')).hexdigest()=='ee8327efb4f744d5acc6d8b2665867ca1a3f8a80908e45a83b0a650168baad65'
 assert hashlib.sha256(read('harness.sh')).hexdigest()=='1e641c01ee224d817f4788232d8a1ea01e054e4a08bed3fc5b9a123de625f2b5'
manager=(D/'observer.original-terminal.txt').read_text().splitlines()
for line in ['InvocationID=f5fc4ac0bad04bbbad415ca4f7e65fbd','Result=success','ExecMainCode=1','ExecMainStatus=0','MainPID=0']: assert line in manager
assert json.loads((C/'instance.after.json').read_text())['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
assert (C/'volume.absent').exists()
r=dict(status='SYNTHETIC_CLOSURE_CHECKS_ACCEPTED_ONLY',cases=expected,instance_id=term['instance_id'],evidence_sha256=term['evidence_sha256'],original_root_observer_exit=0,terminated=True,root_volume_absent=True,native_executed=False,performance_claim=False,remaining=['real-input admission','separate staging','actual native PID128 query gate'])
(C/'root-verification.json').write_text(json.dumps(r,indent=2)+'\n'); print(json.dumps(r))
