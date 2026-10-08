import pathlib,json,hashlib,tarfile,subprocess,tempfile,re,datetime
r=pathlib.Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-primitive-a0001')
p=json.loads((r/'protocol.json').read_text());j=json.loads((r/'active-job.json').read_text());t=json.loads((r/'terminal.json').read_text())
assert j['status']=='TERMINATED_COLLECTED_NOT_YET_VERIFIED' and json.loads((r/'wait.json').read_text())['exit']==0
assert t['instance_id']==j['instance_id'] and t['candidate_commit']==p['candidate_commit']
assert t['schema']=='borsuk-two-bit-four-row-primitive-v1'
assert t['native_source_identity_sha256']==p['native_identity'] and t['native_source_file_count']==415
assert t['elf_sha256']==p['same_retained_elf']['sha256'] and t['elf_bytes']==p['same_retained_elf']['bytes']
b=(r/'evidence.tar.gz').read_bytes();assert len(b)==t['evidence']['bytes'] and hashlib.sha256(b).hexdigest()==t['evidence']['sha256']
pins={line.split('  ',1)[1].removeprefix('./'):line.split('  ',1)[0] for line in (r/'artifacts.sha256').read_text().splitlines()};data={}
with tarfile.open(r/'evidence.tar.gz') as archive:
 for m in archive:
  if not m.isfile():continue
  n=m.name.removeprefix('./');assert n and not n.startswith('/') and '..' not in pathlib.PurePosixPath(n).parts and n not in data
  body=archive.extractfile(m).read();assert hashlib.sha256(body).hexdigest()==pins[n];data[n]=body
assert set(data)==set(pins)
for n,pin in p['inputs'].items():assert len(data[n])==pin['bytes'] and hashlib.sha256(data[n]).hexdigest()==pin['sha256']
assert data['elf-before.sha256'].split()[0]==data['elf-after.sha256'].split()[0]==p['same_retained_elf']['sha256'].encode()
assert int(data['staging-canary-native-exit'])==0 and b'rotated_two_bit::tests::four_row_release_primitive_v1: test' in data['staging-canary.log']
assert int(data['probe-unit-tee-exit'])==0
assert data['probe-cpu.max.before'].strip()==b'100000 100000' and data['probe-memory.max.before'].strip()==b'268435456' and data['probe-memory.swap.max.before'].strip()==b'0' and data['probe-pids.max.before'].strip()==b'128'
assert data['probe-cpuset.cpus.effective.before'].strip()==b'0' and data['probe-cpu-limit.txt'].strip()==b'30'
assert int(data['probe-memory.peak.after'])<=268435456 and int(data['probe-memory.swap.peak.after'])==0
for n in ['probe-memory.events.after','gates-memory.events.after']:
 ev=dict(line.split() for line in data[n].decode().splitlines());assert all(int(ev[k])==0 for k in ['max','oom','oom_kill'])
objects=[]
for line in data['probe.log'].decode().splitlines():
 at=line.find('{')
 if at<0:continue
 try:o=json.loads(line[at:])
 except ValueError:continue
 if isinstance(o,dict) and o.get('schema')=='borsuk.two-bit-four-row.primitive.v1':objects.append(o)
assert len(objects)==1;o=objects[0]
with tempfile.TemporaryDirectory() as tmp:
 d=pathlib.Path(tmp)
 for n in ['probe.log','expected-primitive-identity.json']:(d/n).write_bytes(data[n])
 script=data['probe-check.py'].decode().replace("'/mnt/borsuk-http/evidence'",repr(tmp));(d/'check.py').write_text(script)
 q=subprocess.run(['python3',str(d/'check.py')],capture_output=True);assert q.returncode in [0,42,43],q.stderr.decode()
 assert t['original_exit']==t['exit']==t['native_exit']==int(data['final-exit'])==q.returncode
 native=int(data['probe-native-exit']);assert int(data['probe-unit-native-exit'])==native
 if q.returncode==0:assert native==0 and o['status']=='ACCEPT'
 elif q.returncode==42:assert native==101 and o['status']=='REJECT'
 if (d/'independent-primitive-math.json').exists():(r/'independent-primitive-math.json').write_bytes((d/'independent-primitive-math.json').read_bytes())
(r/'native-report.json').write_text(json.dumps(o,indent=2)+'\n')
receipt={'status':{0:'ACCEPT',42:'REJECT',43:'INVALID'}[q.returncode],'instance_id':j['instance_id'],'artifact_count':len(data),'candidate':p['candidate_commit'],'original_exit':t['original_exit'],'native_rust_exit':native,'native_canary_exit':0,'terminated_and_waited':True,'all_artifacts_authenticated':True,'all80_math_recomputed':q.returncode in [0,42],'scope':'ordered primitive CPU only; no cold/vendor/production claim','at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(r/'independent-verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
