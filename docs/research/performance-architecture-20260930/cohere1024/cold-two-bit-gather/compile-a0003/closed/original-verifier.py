import pathlib,json,hashlib,tarfile,re,subprocess,datetime,shlex
r=pathlib.Path('/data/target/borsuk-cold-membership-native/two-bit-gather-gate-a0003')
p=json.loads((r/'protocol.json').read_text());term=json.loads((r/'terminal.json').read_text());job=json.loads((r/'active-job.json').read_text())
assert job['status']=='TERMINATED_COLLECTED_NOT_YET_VERIFIED'
assert json.loads((r/'wait.json').read_text())['exit']==0
termination=json.loads((r/'termination.json').read_text());assert any(x['InstanceId']==job['instance_id'] and x['CurrentState']['Name'] in ('shutting-down','terminated') for x in termination['TerminatingInstances'])
assert term['instance_id']==job['instance_id'] and term['candidate_commit']==p['candidate']
assert term['native_source_identity_sha256']==p['source']['native_identity'] and term['native_source_file_count']==415
b=(r/'evidence.tar.gz').read_bytes();assert len(b)==term['evidence']['bytes'] and hashlib.sha256(b).hexdigest()==term['evidence']['sha256']
pins={line.split('  ',1)[1].removeprefix('./'):line.split('  ',1)[0] for line in (r/'artifacts.sha256').read_text().splitlines()}
data={}
with tarfile.open(r/'evidence.tar.gz') as t:
 for m in t:
  if not m.isfile():continue
  n=m.name.removeprefix('./');assert n and not n.startswith('/') and '..' not in pathlib.PurePosixPath(n).parts and n not in data
  body=t.extractfile(m).read();assert hashlib.sha256(body).hexdigest()==pins[n];data[n]=body
assert set(data)==set(pins)
for name,pin in p['inputs'].items():assert len(data[name])==pin['bytes'] and hashlib.sha256(data[name]).hexdigest()==pin['sha256']
manifest=json.loads(data['native-source.json']);assert manifest==json.loads((r/'native-source.json').read_text())
full=(r/'source-files.sha256').read_text();assert full.encode()==data['source-files.sha256']
for stage in ['source-before','source-after']:
 log=data[stage+'.log'].decode().splitlines();roster=[line.split('  ',1)[1] for line in full.splitlines()];assert len(roster)==2450 and log==[path+': OK' for path in roster]
expected=json.loads(data['mandatory-tests.json']);stage_names=['codec-debug','forced-avx2-debug','planner-debug','codec-release','forced-avx2-release','planner-release','integration','probe-compile']
commands={}
for line in data['gates.sh'].decode().splitlines():
 if line.startswith(' stage '):
  parts=shlex.split(line);assert parts[1] not in commands;commands[parts[1]]=' '.join(parts[2:])
assert list(commands)==stage_names
assert not data['compiler-env.txt'].strip()
assert re.search(rb'^flags.*\bavx2\b',data['cpuinfo.txt'],re.M)
results=[]
for name in stage_names:
 assert data[name+'.command'].decode().strip()==commands[name]
 native=int(data[name+'.native-exit']);tee=int(data[name+'.tee-exit']);assert native==tee==0
 log=data[name+'.log'].decode();passed=re.findall(r'^test ([\w:]+) \.\.\. ok$',log,re.M)
 for required in expected.get(name,[]):assert passed.count(required)==1,(name,required)
 assert not re.search(r'^test .* \.\.\. FAILED$',log,re.M)
 if name in expected:
  summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;',log);assert len(summaries)==1 and int(summaries[0][0])>0 and summaries[0][1]=='0'
 results.append({'name':name,'command':commands[name],'native_exit':native,'tee_exit':tee,'mandatory_count':len(expected.get(name,[]))})
assert term['original_exit']==term['exit']==term['native_exit']==0
assert int(data['gates-exit'])==int(data['final-exit'])==0
assert data['cpu.max.before'].strip()==b'200000 100000' and data['memory.max.before'].strip()==b'8589934592' and data['memory.swap.max.before'].strip()==b'0' and data['pids.max.before'].strip()==b'512'
events=dict(line.split() for line in data['memory.events.after'].decode().splitlines());assert int(events.get('oom',0))==int(events.get('oom_kill',0))==0
assert int(data['memory.peak.after'])<=8589934592 and int(data['memory.swap.peak.after'])==0
artifact=json.loads(data['probe-artifact.json']);assert artifact['gather_instruction_present'] is True and artifact['ordered_sum_codegen_qualified'] is False
assert int(data['probe-libtest.binary-upload-exit'])==0
retained=json.loads(data['probe-libtest.binary.json']);assert retained['bytes']==artifact['bytes'] and retained['sha256']==artifact['sha256']
receipt={'status':'COMPILE_AND_SYNTHETIC_CORRECTNESS_VERIFIED_CODEGEN_PENDING','candidate':p['candidate'],'instance_id':job['instance_id'],'source_file_count':415,'support_file_count':2450,'full_source_before_after_equal':True,'artifact_count':len(data),'stages':results,'release_elf':retained,'codegen_qualified':False,'primitive_run':False,'workspace_clippy_and_testbuild':'NOT_RUN','production_integrated':False,'performance_claim':False,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(r/'independent-verification.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
