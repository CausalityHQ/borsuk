"""Terminal-only independent qualification verification; no ANN/data execution."""
import pathlib,json,hashlib,subprocess,io,re,datetime,tarfile
r=pathlib.Path('/data/target/borsuk-cold-membership-native/sq8-clock-reuse-qualification-a0001');repo=pathlib.Path('/home/rb/worktrees/borsuk-prod-ready-v9');col=r/'collected'
p=json.loads((r/'protocol.json').read_text());t=json.loads((r/'terminal.json').read_text());i=json.loads((r/'launch.json').read_text())['Instances'][0]['InstanceId'];c=p['source']['candidate']
assert t['schema']=='borsuk-sq8-clock-reuse-native-qualification-closed-v1'
assert t['instance_id']==i and t['source_commit']==c
assert t['source_archive_sha256']==p['source']['archive_sha256'] and t['native_source_identity_sha256']==p['source']['native_identity'] and t['native_source_file_count']==425
assert t['original_exit']==t['exit']==t['qualification_exit']==0
assert t['ann_run'] is False and t['performance_claim'] is False and t['quality_claim'] is False
assert json.loads((r/'wait.json').read_text()) == {'instance_id':i,'terminated':True}
assert json.loads((r/'active-job.json').read_text())['instance_id']==i
raw=(r/'evidence.tar.gz').read_bytes();assert len(raw)==t['evidence']['bytes'] and hashlib.sha256(raw).hexdigest()==t['evidence']['sha256']
col.mkdir(exist_ok=False)
with tarfile.open(r/'evidence.tar.gz') as archive:
 for item in archive:
  assert not item.name.startswith('/') and '..' not in pathlib.PurePosixPath(item.name).parts
  assert item.isdir() or item.isfile()
 archive.extractall(col,filter='data')
for line in (r/'artifacts.sha256').read_text().splitlines():
 h,path=line.split('  ',1);assert '..' not in pathlib.PurePosixPath(path).parts
 assert hashlib.sha256((col/path.removeprefix('./')).read_bytes()).hexdigest()==h,path
for name,pin in p['inputs'].items():
 raw=(r/name).read_bytes();assert len(raw)==pin['bytes'] and hashlib.sha256(raw).hexdigest()==pin['sha256'],name
 if name!='gates.sh':assert (col/name).read_bytes()==raw,name
m=json.loads((r/'native-source.json').read_text());paths=subprocess.check_output(['git','ls-tree','-r','--name-only',c],cwd=repo,text=True).splitlines()
assert set(m)=={q for q in paths if q.endswith('.rs') or pathlib.PurePosixPath(q).name in ('Cargo.toml','Cargo.lock')}
a=subprocess.run(['git','cat-file','--batch'],cwd=repo,input=''.join(c+':'+q+'\n' for q in sorted(m)).encode(),capture_output=True,check=True);s=io.BytesIO(a.stdout)
for path in sorted(m):
 header=s.readline().split();assert header[1]==b'blob';raw=s.read(int(header[2]));assert s.read(1)==b'\n';assert hashlib.sha256(raw).hexdigest()==m[path],path
assert not s.read()
assert hashlib.sha256(json.dumps(m,sort_keys=True,separators=(',',':')).encode()).hexdigest()==p['source']['native_identity']
pins={line.split('  ',1)[1]:line.split('  ',1)[0] for line in (r/'source-files.sha256').read_text().splitlines()}
for name in ['source-staging.log','source-before.log','source-after.log']:
 lines=(col/name).read_text().splitlines();assert len(lines)==len(pins) and set(lines)=={q+': OK' for q in pins},name
def zero(name):assert (col/name).read_text().strip()=='0',name
commands={}
for line in (r/'gates.sh').read_text().splitlines():
 if line.startswith('  stage '):
  name,command=line.strip()[6:].split(' ',1);commands[name]=command
assert set(commands)==set(p['mandatory_stages']+p['inventory_stages']) and len(commands)==12
previous_end=None
for stage,command in commands.items():
 zero(stage+'.native-exit');zero(stage+'.tee-exit');assert (col/(stage+'.command')).read_text().strip()==command
 assert 0<int((col/(stage+'.timeout-seconds')).read_text())<=7200
 assert 'Exit status: 0' in (col/(stage+'.time')).read_text()
 start=datetime.datetime.fromisoformat((col/(stage+'.started')).read_text().strip().replace('Z','+00:00'));end=datetime.datetime.fromisoformat((col/(stage+'.finished')).read_text().strip().replace('Z','+00:00'));assert end>=start
 if previous_end is not None: assert start>=previous_end,stage
 previous_end=end
expected=json.loads((r/'expected-test-inventory.json').read_text())['groups'];mandatory=json.loads((r/'mandatory-tests.json').read_text())
for kind,inventory_stage,affected_stage in [('ranges','range-inventory','affected-ranges'),('ranking','rank-inventory','affected-ranking'),('generation','lib-inventory','affected-lib'),('bin','bin-inventory','affected-bin')]:
 names=expected[kind]['all_names'];listed=re.findall(r'^(\S+): test$',(col/(inventory_stage+'.log')).read_text(),re.M);log=(col/(affected_stage+'.log')).read_text();passed=re.findall(r'^test (\S+) \.\.\. ok$',log,re.M)
 if kind=='bin':
  assert all(n.startswith('tests::') for n in listed+passed)
  listed=[n.removeprefix('tests::') for n in listed];passed=[n.removeprefix('tests::') for n in passed]
 assert sorted(listed)==sorted(names) and len(listed)==len(set(listed)),kind
 assert sorted(passed)==sorted(names) and len(passed)==len(set(passed)),kind
 assert set(mandatory[kind])<=set(passed)
 assert re.search(r'test result: ok\. '+str(len(names))+r' passed; 0 failed; 0 ignored; 0 measured;',log),kind
layout=(col/'range-layout.log').read_text();layout_name='sq8_s3_range::tests::range_future_and_outcome_layout_stay_within_the_modeled_allowance'
assert re.findall(r'^test (\S+) \.\.\.',layout,re.M)==[layout_name]
# --nocapture can put the printed size witness between the test label and its ok.
assert re.search(r'(?:^ok$|^test '+re.escape(layout_name)+r' \.\.\. ok$)',layout,re.M)
assert 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;' in layout
zero('qualification-exit');zero('final-exit');zero('runner.binary-upload-exit')
for name,value in {'cpu.max':'200000 100000','memory.max':'8589934592','memory.swap.max':'0','pids.max':'512'}.items():assert (col/(name+'.before')).read_text().strip()==value
for name in ['memory.events.after','global-memory.events.after']:
 events=dict(line.split() for line in (col/name).read_text().splitlines());assert events['oom']==events['oom_kill']=='0'
for name in ['memory.swap.peak.after','global-memory.swap.peak.after','global-pids.current.after']:zero(name)
assert int((col/'global-memory.current.after').read_text()) <= 8589934592
assert 'ActiveState=inactive' in (col/'global-systemd-after.txt').read_text()
assert 'MainPID=0' in (col/'systemd-after.txt').read_text()
binary=r/'check_cohere_native_baseline';raw=binary.read_bytes();assert raw[:4]==b'\x7fELF' and len(raw)==int((col/'runner.binary.bytes').read_text());assert hashlib.sha256(raw).hexdigest()==(col/'runner.binary.sha256').read_text().split()[0]
result={'status':'NATIVE_QUALIFIED_COMPILER_AND_SYNTHETIC_CORRECTNESS_ONLY','candidate':c,'instance_id':i,'terminated':True,'source_file_count':425,'native_source_identity_sha256':p['source']['native_identity'],'full_source_before_after_root_match':True,'stages':[{'name':name,'command':cmd,'exit':0} for name,cmd in commands.items()],'test_counts':{k:len(v['all_names']) for k,v in expected.items()},'binary':{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},'ann_run':False,'performance_claim':False,'quality_claim':False}
(r/'independent-verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
