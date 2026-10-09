import pathlib,json,hashlib,re,tarfile,sys
root=pathlib.Path(sys.argv[1]);e=root/'evidence-extracted'
pre=root.parent;protocol=json.loads((pre/'protocol.preregistration.json').read_text())
collection=json.loads((root/'collection.json').read_text());assert collection['instance_id']=='i-0e7aa614cb21c3e8a' and collection['terminated'] is True
assert (root/'state.txt').read_text().strip()=='terminated'
t=json.loads((root/'terminal.json').read_text());assert t['instance_id']=='i-0e7aa614cb21c3e8a'
assert t['schema']=='borsuk-scale-derivation-qualification-closed-v1'
assert t['source_commit']=='bc3082a8210c4370ebadae4ba093a7c211072b62'
assert t['native_source_file_count']==426 and t['native_source_identity_sha256']=='06feadd3e45389e70cd56450d00bc3d62ae874c59d1a35b58b2c3f8eb65fdb91'
assert t['source_archive_sha256']=='0131c8fdfd4a7fbdbb89fa33eba1dd5345494ecd9c5c433ebceb084a9d14736e'
archive=(root/'evidence.tar.gz').read_bytes();assert len(archive)==t['evidence']['bytes'] and hashlib.sha256(archive).hexdigest()==t['evidence']['sha256']
e.mkdir()
with tarfile.open(root/'evidence.tar.gz') as tar:
 for m in tar:
  name=pathlib.PurePosixPath(m.name);assert not name.is_absolute() and '..' not in name.parts
  dest=e/name
  if m.isdir():dest.mkdir(parents=True,exist_ok=True)
  else:
   assert m.isfile() and m.size<=67108864;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(tar.extractfile(m).read())
manifest_names=[]
for line in (root/'artifacts.sha256').read_text().splitlines():
 h,name=line.split('  ',1);part=pathlib.PurePosixPath(name);assert not part.is_absolute() and '..' not in part.parts
 manifest_names.append(str(part));assert hashlib.sha256((e/name).read_bytes()).hexdigest()==h
assert len(manifest_names)==len(set(manifest_names))
assert set(manifest_names)=={str(p.relative_to(e)) for p in e.rglob('*') if p.is_file()}
for name,pin in protocol['input_pins'].items():
 b=(e/name).read_bytes();assert len(b)==pin['bytes'] and hashlib.sha256(b).hexdigest()==pin['sha256']
native=json.loads((e/'native-source.json').read_text());assert len(native)==426 and hashlib.sha256(json.dumps(native,sort_keys=True,separators=(',',':')).encode()).hexdigest()==t['native_source_identity_sha256']
for name,value in {'cpu.max.before':'200000 100000','memory.max.before':'8589934592','memory.swap.max.before':'0','pids.max.before':'512'}.items():assert (e/name).read_text().strip()==value
c=json.loads((e/'candidate-contract.json').read_text());q=c['qualified_tests_by_target'];expected={'preparer-tests':q['prepare_cohere_native_cohort'],'derivation-tests':q['build_sq8_source'],'baseline-tests':q['check_cohere_native_baseline']+q['check_cohere_native_baseline_inherited_producer'],'reducer-tests':q['compare_native_replay']+q['compare_native_replay_inherited_runner']+q['compare_native_replay_inherited_producer']}
expected['reducer-tests'] += q['compare_native_replay_inherited_source_cover']
for stage in c['root_gate_argv_by_stage']:
 name=stage['name'];assert (e/(name+'.command')).read_text().strip()==' '.join(stage['argv'])
 assert (e/(name+'.native-exit')).read_text().strip()=='0' and (e/(name+'.tee-exit')).read_text().strip()=='0'
 if name in expected:
  log=(e/(name+'.log')).read_text();passes=re.findall(r'^test ([^ ]+) \.\.\. ok$',log,re.M)
  assert sorted(passes)==sorted(expected[name]),(name,len(passes),len(expected[name]))
  assert len(passes)==len(set(passes))
  summaries=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',log)
  assert len(summaries)==1 and tuple(map(int,summaries[0]))==(len(passes),0,0)
for phase in ['before','after']:
 lines=(e/('source-'+phase+'.log')).read_text().splitlines();assert len(lines)==2465 and all(x.endswith(': OK') for x in lines)
assert (e/'source-before.log').read_bytes()==(e/'source-after.log').read_bytes()
assert all(type(t[k]) is int and t[k]==0 for k in ['original_exit','exit','qualification_exit'])
assert (e/'final-exit').read_text().strip()=='0'
for key in ['oom','oom_kill']:
 vals=dict(line.split() for line in (e/'global-memory.events.after').read_text().splitlines());assert int(vals[key])==0
assert int((e/'global-memory.swap.peak.after').read_text())==0


for name in ['prepare_cohere_native_cohort','check_cohere_native_baseline','build_two_bit_generation','publish_two_bit_generation','build_sq8_source','compare_native_replay']:
 binary=root/'binaries'/name;data=binary.read_bytes();assert data[:4]==b'\x7fELF' and len(data)==int((e/(name+'.binary.bytes')).read_text())
 assert hashlib.sha256(data).hexdigest()==(e/(name+'.binary.sha256')).read_text().split()[0]
 assert (e/(name+'.binary-upload-exit')).read_text().strip()=='0'
print(json.dumps({'status':'NATIVE_GATE_EVIDENCE_VERIFIED','tests':sum(map(len,expected.values())),'stages':7,'release_binaries':6,'performance_claim':False}))
