import pathlib,json,hashlib,tarfile,subprocess,tempfile
r=pathlib.Path('/data/target/borsuk-cold-membership-native/page-auth-gate-a0002')
t=json.loads((r/'terminal.json').read_text());job=json.loads((r/'active-job.json').read_text());p=json.loads((r/'protocol.json').read_text());assert t['instance_id']==job['instance_id']
s=subprocess.run(['aws','--profile','causality','--region','eu-central-1','ec2','describe-instances','--instance-ids',t['instance_id'],'--query','Reservations[0].Instances[0].State.Name','--output','text'],capture_output=True,text=True,timeout=55);assert s.returncode==0 and s.stdout.strip()=='terminated'
(r/'fresh-termination.json').write_text(json.dumps({'instance_id':t['instance_id'],'state':'terminated','freshly_verified':True})+'\n')
a=r/'evidence.tar.gz';assert a.stat().st_size==t['evidence']['bytes'] and hashlib.sha256(a.read_bytes()).hexdigest()==t['evidence']['sha256']
assert t['schema']=='borsuk-page-auth-backend-receipt-repair-v1' and t['candidate_commit']==p['candidate_commit'] and t['elf_sha256']==p['same_retained_elf']['sha256'] and t['elf_bytes']==p['same_retained_elf']['bytes']
out=r/'collected';out.mkdir(exist_ok=False)
with tarfile.open(a,'r:gz') as f:
 members=f.getmembers();assert sum(v.size for v in members)<=64*1024*1024
 for v in members:
  name=pathlib.PurePosixPath(v.name);assert not name.is_absolute() and '..' not in name.parts and (v.isfile() or v.isdir())
 f.extractall(out,filter='data')
roster={}
for line in (r/'artifacts.sha256').read_text().splitlines():
 digest,name=line.split('  ',1);name=name.removeprefix('./');assert name not in roster and len(digest)==64
 f=out/name;assert f.is_file() and hashlib.sha256(f.read_bytes()).hexdigest()==digest;roster[name]=digest
assert set(roster)=={str(f.relative_to(out)) for f in out.rglob('*') if f.is_file()}
for name,pin in p['inputs'].items():
 assert (out/name).stat().st_size==pin['bytes'] and hashlib.sha256((out/name).read_bytes()).hexdigest()==pin['sha256']
status='EXECUTION_INVALID';native=t.get('native_exit');checks={}
if native in [0,42]:
 assert t['original_exit']==t['exit']==native
 for phase,expected in [('before','200000 100000')]:assert (out/f'gates-cpu.max.{phase}').read_text().strip()==expected
 for name,val in [('memory.max','8589934592'),('memory.swap.max','0'),('pids.max','512')]:assert (out/f'gates-{name}.before').read_text().strip()==val
 for name in ['elf-before.sha256','elf-after.sha256']:assert (out/name).read_text().split()[0]==t['elf_sha256']
 flags=(out/'cpuinfo.txt').read_text();assert 'sha_ni' in flags and 'avx512f' in flags
 source=(r/'probe-check.py').read_text();assert source.count("r=pathlib.Path('/mnt/borsuk-http/evidence')")==1
 source=source.replace("r=pathlib.Path('/mnt/borsuk-http/evidence')",'r=pathlib.Path('+repr(str(out))+')')
 with tempfile.NamedTemporaryFile('w',suffix='.py',delete=False) as f:f.write(source);tmp=pathlib.Path(f.name)
 try:
  z=subprocess.run(['python3',str(tmp)],capture_output=True,text=True,timeout=120);assert z.returncode==native,z.stderr
 finally:tmp.unlink()
 result=json.loads((out/'probe-independent.json').read_text());assert result['status']==('PASS' if native==0 else 'REJECT');status=result['status'];checks=result
 events=dict(v.split() for v in (out/'gates-memory.events.after').read_text().splitlines());assert all(int(events[k])==0 for k in ['max','oom','oom_kill']);assert int((out/'gates-memory.swap.peak.after').read_text())==0
v={'status':status,'instance_id':t['instance_id'],'terminated_freshly_verified':True,'protocol_commit':job['protocol_commit'],'candidate_commit':p['candidate_commit'],'same_exact_release_elf':True,'elf_sha256':t['elf_sha256'],'artifact_count':len(roster),'all_artifacts_authenticated':True,'helper_pins_match':True,'native_exit':native,'independent_probe':checks,'remaining_release_clippy_testbuild':'UNRUN','cold_or_vendor_performance_claim':False,'goal_complete':False}
(r/'independent-verification.json').write_text(json.dumps(v,indent=2)+'\n');job.update(status='TERMINATED_COLLECTED_VERIFIED_'+status,background=False);(r/'active-job.json').write_text(json.dumps(job,indent=2)+'\n');print(json.dumps(v))
