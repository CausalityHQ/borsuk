from pathlib import Path
import os,subprocess,tempfile,json,hashlib
p=Path(__file__).parent
cg=Path('/sys/fs/cgroup')/next(x.split(':',2)[2] for x in Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0:')).lstrip('/')
limits={x:(cg/x).read_text().strip() for x in ['cpu.max','memory.max','memory.swap.max']}
assert limits=={'cpu.max':'100000 100000','memory.max':'268435456','memory.swap.max':'0'}
fake=r'''#!/usr/bin/python3
import sys,os,json,subprocess,hashlib
from pathlib import Path
name=Path(sys.argv[0]).name;a=sys.argv[1:];mode=os.environ['CASE'];root=Path(os.environ['MOCK_ROOT'])
if name=='cat':
 if a[-1].startswith('/sys/fs/cgroup'):
  v={'cpu.max':'100000 100000','memory.max':'536870912','memory.swap.max':'0','pids.max':'256'}
  if mode=='bad-cap':v['memory.max']='1073741824'
  print(v.get(Path(a[-1]).name,'0'));sys.exit(0)
 os.execv('/bin/cat',['cat',*a])
if name=='perf':
 with (root/'calls').open('a') as f:f.write(repr(a)+'\n')
 if a==['version']:print('perf version 7.0.14');sys.exit(0)
 if a[0]=='record':
  assert a[a.index('-e')+1]=='cpu-clock:u' and a[a.index('-F')+1]=='99'
  assert '--strict-freq' in a and a[a.index('--call-graph')+1]=='dwarf,8192'
  assert '--mmap-pages=64' in a and '-a' not in a
  assert a[a.index('--')+1]=='/bin/bash'
  if mode=='record-fail':sys.exit(17)
  Path(a[a.index('-o')+1]).write_bytes(b'fixture-perf')
  if mode=='oversize-profile':
   with Path(a[a.index('-o')+1]).open('r+b') as f:f.truncate(134217729)
  code=subprocess.run(a[a.index('--')+1:]).returncode
  sys.exit(18 if mode=='record-after-native-fail' else code)
 if a[0]=='report':
  if mode=='report-fail':sys.exit(19)
  print('# Total Lost Samples: '+('1' if mode=='lost-samples' else '0')+'\n100% runner borsuk::returned_sq8::rank_returned_ranges_excluding');sys.exit(0)
 if a[0]=='script':print('runner cpu-clock:u\n abc borsuk::ranking (runner)\n def main (runner)\n');sys.exit(0)
 sys.exit(20)
if name=='runner':
 assert len(a)==3 and a[1]=='e604a1ca02644329113e6c11a935d67115f784c40008859966f7e332fae53a3a'
 Path(a[2]).write_text(json.dumps({'phase':'terminal','summary':{'status':'MEASURED','queries':1000}})+'\n')
 if mode=='pins-change':(root/'config.json').write_text('changed')
 if mode=='scratch-leak':(root/'membership-admission-a0001/query-scratch/leak').write_text('x')
 sys.exit(2 if mode=='native-fail' else 0)
if name=='reducer':
 assert len(a)==4 and a[0]=='--completed-v2'
 config=Path(a[1]).read_bytes();assert hashlib.sha256(config).hexdigest()==a[2]
 c=json.loads(config);assert c['schema']=='borsuk-completed-native-reduction-config-v1'
 data=Path(c['input']['path']).read_bytes();assert len(data)==c['input']['bytes'] and hashlib.sha256(data).hexdigest()==c['input']['sha256']
 if mode=='reducer-fail':sys.exit(21)
 Path(a[3]).write_text(json.dumps({'status':'MEASURED','complete':True,'native_terminal':{'queries':1000,'recall_numerator':9722 if mode=='wrong-recall' else 9723,'underfilled_queries':0}}))
 sys.exit(0)
sys.exit(22)
'''
results=[]
for mode,expected in [('pass',0),('bad-cap',1),('record-fail',1),('oversize-profile',1),('native-fail',1),('record-after-native-fail',1),('report-fail',19),('lost-samples',1),('reducer-fail',1),('wrong-recall',1),('pins-change',96),('scratch-leak',96)]:
 with tempfile.TemporaryDirectory(dir=p) as temp:
  d=Path(temp);root=d/'root';root.mkdir();(root/'evidence').mkdir();(root/'membership-admission-a0001/query-scratch').mkdir(parents=True)
  b=d/'bin';b.mkdir();driver=b/'driver';driver.write_text(fake);driver.chmod(0o755)
  for n in ['cat','perf']:(b/n).symlink_to(driver)
  for n in ['runner','reducer']:(root/n).symlink_to(driver)
  (root/'config.json').write_bytes((p/'config.json').read_bytes());(root/'reduction-template.json').write_bytes((p/'reduction-template.json').read_bytes())
  (root/'pins.sha256').write_text(''.join(hashlib.sha256(f.read_bytes()).hexdigest()+'  '+str(f)+'\n' for f in [root/'runner',root/'config.json',root/'reducer',root/'reduction-template.json']))
  script=d/'stage.sh';script.write_text((p/'stage.sh').read_text().replace('/mnt/borsuk-retained-s3',str(root)))
  r=subprocess.run(['/bin/bash',str(script)],env=dict(os.environ,PATH=str(b)+':/usr/bin:/bin',CASE=mode,MOCK_ROOT=str(root)),capture_output=True,text=True,timeout=15)
  if r.returncode!=expected:(p/'stage-first-failure.json').write_text(json.dumps({'case':mode,'exit':r.returncode,'stderr':r.stderr,'evidence':{f.name:f.read_text(errors='replace') for f in (root/'evidence').iterdir() if f.is_file()}},indent=2)+'\n')
  assert r.returncode==expected,(mode,r.returncode,r.stderr)
  assert (root/'evidence/final-exit').read_text().strip()==str(expected)
  if mode in ['bad-cap','record-fail','oversize-profile','native-fail','record-after-native-fail','report-fail','lost-samples']:assert not (root/'evidence/reduction.native-exit').exists()
  if mode=='native-fail':assert (root/'evidence/native-exit').read_text().strip()=='2'
  if mode=='record-after-native-fail':assert (root/'evidence/native-exit').read_text().strip()=='0' and (root/'evidence/perf-record.exit').read_text().strip()=='18'
  assert not (root/'evidence/perf.data').exists()
  if mode=='pass':assert (root/'evidence/profile.status').exists() and (root/'evidence/perf-data.sha256').exists()
  results.append({'case':mode,'exit':r.returncode})
receipt={'status':'PASS','cases':results,'kernel_limits':limits,'stage_sha256':hashlib.sha256((p/'stage.sh').read_bytes()).hexdigest(),'real':['Bash/time/timeout control flow','separate native/profiler exit evidence','SHA pins and drift refusal','reduction config rebinding and SHA','jq terminal predicate','scratch check','file limit establishment'],'mocked':['perf recording/decoding','cgroup contents','Rust CLI and reduction'],'native_perf_execution':False}
(p/'stage-check.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
