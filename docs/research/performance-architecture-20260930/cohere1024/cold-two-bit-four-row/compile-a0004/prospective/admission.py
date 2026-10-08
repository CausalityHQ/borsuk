import pathlib,json,hashlib,tarfile,subprocess,ast,tempfile
r=pathlib.Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-gate-a0004');p=json.loads((r/'protocol.json').read_text());c=json.loads((r/'candidate-contract.json').read_text())
assert p['candidate']==c['commit']=='3138bfb0502c986d187e4098718f8b3945fd39c9'
for n,pin in p['inputs'].items():
 data=(r/n).read_bytes();assert len(data)==pin['bytes'] and hashlib.sha256(data).hexdigest()==pin['sha256']
data=(r/'source.tar.gz').read_bytes();assert len(data)==p['source']['archive_bytes'] and hashlib.sha256(data).hexdigest()==p['source']['archive_sha256'];del data
pins={line.split('  ',1)[1]:line.split('  ',1)[0] for line in (r/'source-files.sha256').read_text().splitlines()};seen=set();owned={}
with tarfile.open(r/'source.tar.gz') as t:
 for m in t:
  assert m.isfile() and m.name in pins and m.name not in seen and not m.name.startswith('/') and '..' not in pathlib.PurePosixPath(m.name).parts
  data=t.extractfile(m).read();assert hashlib.sha256(data).hexdigest()==pins[m.name];seen.add(m.name)
  if m.name in c['owned_paths']:owned[m.name]=data.decode()
assert seen==set(pins) and len(pins)==p['source']['file_count']
native=json.loads((r/'native-source.json').read_text());assert len(native)==p['source']['native_file_count']==415
assert all(pins[n]==h for n,h in native.items()) and all(native[n]==h for n,h in c['owned_sha256'].items())
identity=hashlib.sha256(b''.join(n.encode()+b'\0'+h.encode()+b'\n' for n,h in sorted(native.items()))).hexdigest();assert identity==p['source']['native_identity']
u=(r/'user-data.sh').read_bytes();assert len(u)==p['userdata']['bytes'] and hashlib.sha256(u).hexdigest()==p['userdata']['sha256'];assert identity.encode() in u and p['candidate'].encode() in u
for file in ['user-data.sh','gates.sh']:assert subprocess.run(['bash','-n',str(r/file)]).returncode==0
ast.parse((r/'select-probe.py').read_text());commands=[line.strip() for line in (r/'gates.sh').read_text().splitlines() if line.startswith(' stage ')];assert len(commands)==7 and not any('--ignored' in line for line in commands)
roster=json.loads((r/'mandatory-tests.json').read_text());assert list(roster)==['codec-debug','planner-debug','codec-release','planner-release','scalar-control-codec']
for stage,names in roster.items():
 assert len(names)==len(set(names))>0
 for n in names:
  source=owned['crates/borsuk/src/'+('rotated_two_bit.rs' if n.startswith('rotated_two_bit::') else 'two_bit_generation.rs')]
  assert 'fn '+n.rsplit('::',1)[1]+'(' in source
selector=(r/'select-probe.py').read_text()
with tempfile.TemporaryDirectory() as d:
 root=pathlib.Path(d);binary=root/'fixture-not-native';binary.write_bytes(b'opaque fake ELF metadata fixture')
 artifact={'reason':'compiler-artifact','target':{'name':'borsuk','kind':['lib']},'profile':{'test':True},'executable':str(binary)}
 def fake(args,**kwargs):
  if args[0]=='nm':return '1000 0010 T <borsuk::rotated_two_bit::PreparedTwoBit>::score_four\n'
  assert args[0]=='objdump';return ' 1000: 66 0f 58 c1 addpd %xmm1,%xmm0\n'
 original=subprocess.check_output;subprocess.check_output=fake
 try:
  script=selector.replace("pathlib.Path('/mnt/borsuk-http/evidence')",'pathlib.Path('+repr(d)+')')
  (root/'probe-compile.log').write_text(json.dumps(artifact)+'\n');exec(compile(script,'selector-fixture','exec'),{});assert json.loads((root/'probe-artifact.json').read_text())['ordered_sum_codegen_qualified'] is False
  (root/'probe-compile.log').write_text(json.dumps(artifact)+'\n'+json.dumps(artifact)+'\n')
  try:exec(compile(script,'selector-duplicate','exec'),{});raise RuntimeError('ambiguous ELF admitted')
  except AssertionError:pass
 finally:subprocess.check_output=original
receipt={'status':'PASS_BOUNDED_SOURCE_AND_BOOTSTRAP_ADMISSION','candidate':p['candidate'],'source_identity':identity,'native_count':415,'support_count':len(pins),'stage_count':7,'selector_fixture':'synthetic metadata only; ambiguity refused; no assembly correctness claim','native_execution':False,'AWS':False,'performance_claim':False}
(r/'local-admission-canary.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
