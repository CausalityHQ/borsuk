import hashlib,json,tarfile,subprocess,re
from pathlib import Path,PurePosixPath
p=Path(__file__).parent
sha=lambda b: hashlib.sha256(b).hexdigest()
t=json.loads((p/'terminal.json').read_text()); prereg=json.loads((p/'preregistration.json').read_text())
assert t['instance_id']=='i-066e9b43ae41cfe0d' and t['phase']=='complete'
assert all(t[k]==0 and type(t[k]) is int for k in ['original_exit','exit','native_exit'])
assert t['source_commit']==prereg['source_commit'] and t['native_source_identity_sha256']==prereg['native_identity']
archive=p/'evidence.tar.gz'; assert archive.stat().st_size==t['evidence']['bytes']
assert sha(archive.read_bytes())==t['evidence']['sha256']
roster={}
for line in (p/'artifacts.sha256').read_text().splitlines():
 digest,name=line.split('  ',1); name=name.removeprefix('./'); assert name not in roster;roster[name]=digest
root=p/'evidence';root.mkdir(exist_ok=True)
seen=set()
with tarfile.open(archive,'r:gz') as tar:
 for entry in tar:
  name=entry.name.removeprefix('./'); path=PurePosixPath(name)
  assert not path.is_absolute() and '..' not in path.parts
  if entry.isdir(): continue
  assert entry.isfile() and name not in seen and name in roster
  body=tar.extractfile(entry).read();assert sha(body)==roster[name]
  target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(body);seen.add(name)
assert seen==set(roster) and len(seen)==76
for name in ['source-files.sha256','native-source.json','gates.sh']:
 assert (root/name).read_bytes()==(p/name).read_bytes()
source_names=[line.split('  ',1)[1] for line in (p/'source-files.sha256').read_text().splitlines()]
expected=''.join(name+': OK\n' for name in source_names)
assert len(source_names)==2449
assert (root/'source-before.log').read_text()==expected==(root/'source-after.log').read_text()
native=json.loads((p/'native-source.json').read_text());assert len(native)==414
objects=''.join(t['source_commit']+':'+name+'\n' for name in native)
data=subprocess.check_output(['git','-C','/home/rb/worktrees/borsuk-prod-ready-v9','cat-file','--batch'],input=objects.encode())
pos=0
for name,digest in native.items():
 end=data.index(b'\n',pos); fields=data[pos:end].split();assert fields[1]==b'blob';size=int(fields[2]);pos=end+1
 assert sha(data[pos:pos+size])==digest,name;pos+=size+1
assert pos==len(data)
test_names={}
for stage,required in prereg['tests'].items():
 log=(root/(stage+'.log')).read_text()
 names=re.findall(r'^test ([^ ]+) \.\.\. ok$',log,re.M)
 assert sorted(names)==sorted(required) and len(names)==len(set(names)),stage
 assert f'{len(required)} passed; 0 failed; 0 ignored; 0 measured;' in log,stage
 test_names[stage]=sorted(names)
commands=[(stage['name'],stage['command']) for stage in prereg['stages']]
assert len(commands)==7
stages=[]
for name,command in commands:
 assert (root/(name+'.command')).read_text().strip()==command
 assert (root/(name+'.native-exit')).read_text().strip()=='0'==(root/(name+'.tee-exit')).read_text().strip()
 stages.append({'name':name,'command':command,'native_exit':0,'tee_exit':0})
for name,value in [('cpu.max.before','200000 100000'),('memory.max.before','8589934592'),('memory.swap.max.before','0'),('pids.max.before','512')]:assert (root/name).read_text().strip()==value
assert (root/'final-exit').read_text().strip()=='0'
events=dict(line.split() for line in (root/'memory.events.after').read_text().splitlines())
assert int(events['oom'])==int(events['oom_kill'])==0
assert (root/'memory.swap.peak.after').read_text().strip()=='0'
systemd=(root/'systemd-after.txt').read_text();assert all(s in systemd for s in ['MainPID=0','Result=success','ExecMainStatus=0','ActiveState=inactive'])
binaries={}
for filename,digestfile in [('check_cohere_native_baseline','binary.sha256'),('publish_two_bit_generation','publisher-binary.sha256')]:
 binary=(root/filename).read_bytes();assert binary.startswith(b'\x7fELF');binary_sha=sha(binary)
 assert (root/digestfile).read_text().split()[0]==binary_sha
 binaries[filename]={'bytes':len(binary),'sha256':binary_sha}
state=subprocess.check_output(['aws','--profile','causality','--region','eu-central-1','ec2','describe-instances','--instance-ids',t['instance_id'],'--query','Reservations[0].Instances[0].State.Name','--output','text'],text=True).strip()
assert state=='terminated',state
proof={'status':'NATIVE_QUALIFIED','source_commit':t['source_commit'],'instance_id':t['instance_id'],'terminated':True,'artifact_count':76,'native_source_file_count':414,'native_source_identity_sha256':prereg['native_identity'],'full_source_count':2449,'full_before_after_match':True,'native_git_bytes_match':True,'mandatory_tests_by_stage':test_names,'stages':stages,'binaries':binaries,'memory_peak_bytes':int((root/'memory.peak.after').read_text()),'memory_events':events,'swap_peak_bytes':0,'systemd_after':systemd,'performance_claim':False}
with (p/'root-verification.json').open('x') as f: f.write(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof))
