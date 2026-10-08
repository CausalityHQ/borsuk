from pathlib import Path
import os,subprocess,tempfile,json,hashlib
p=Path(__file__).parent
cg=Path('/sys/fs/cgroup')/next(x.split(':',2)[2] for x in Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0:')).lstrip('/')
limits={x:(cg/x).read_text().strip() for x in ['cpu.max','memory.max','memory.swap.max']}
assert limits=={'cpu.max':'100000 100000','memory.max':'268435456','memory.swap.max':'0'}
fake=r'''#!/usr/bin/python3
import sys,os,json
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
 if a==['version']:print('perf mock');sys.exit(0)
 if a[0]=='record':
  assert a[a.index('-e')+1]=='cpu-clock:u' and a[a.index('-F')+1]=='99'
  assert '--strict-freq' in a and a[a.index('--call-graph')+1]=='dwarf,8192'
  assert '--mmap-pages=64' in a and '-a' not in a
  assert a[a.index('--')+1]=='/bin/bash' and a[-1]==str(root/'runner')
  if mode=='record-fail':sys.exit(17)
  Path(a[a.index('-o')+1]).write_bytes(b'fixture-perf');sys.exit(0)
 if a[0]=='report':
  if mode=='report-fail':sys.exit(18)
  print('100% sha256sum libc crypto');sys.exit(0)
 if a[0]=='script':
  print('' if mode=='empty-samples' else 'sha256sum  1 cpu-clock:u\n  abc crypto (libc)\n  def main (sha256sum)\n');sys.exit(0)
 sys.exit(19)
if name=='runner':
 assert len(a)==3 and a[1]=='0'*64
 s={'status':'INVALID','stage':'config','error':'artifact identity/SHA','completed_queries':0,'truth_opened':False,'all_queries_sealed':False,'sum':{'submitted_gets':0},'binding_charge':{'submitted_gets':0}}
 if mode=='wrong-error':s['error']='other'
 if mode=='query-opened':s['completed_queries']=1
 if mode=='truth-opened':s['truth_opened']=True
 if mode=='get-submitted':s['sum']['submitted_gets']=1
 Path(a[2]).write_text(json.dumps({'phase':'identity'})+'\n'+json.dumps({'phase':'terminal','summary':s})+'\n')
 if mode=='pins-change':(root/'config.json').write_text('changed')
 if mode=='scratch-leak':(root/'membership-admission-a0001/query-scratch/leak').write_text('x')
 sys.exit(0 if mode=='unexpected-native-success' else 2)
sys.exit(20)
'''
results=[]
for mode,expected in [('pass',0),('bad-cap',1),('record-fail',17),('report-fail',18),('empty-samples',1),('wrong-error',1),('query-opened',1),('truth-opened',1),('get-submitted',1),('unexpected-native-success',1),('pins-change',96),('scratch-leak',96)]:
 with tempfile.TemporaryDirectory(dir=p) as temp:
  d=Path(temp);root=d/'root';root.mkdir();(root/'evidence').mkdir();(root/'membership-admission-a0001/query-scratch').mkdir(parents=True)
  b=d/'bin';b.mkdir();driver=b/'driver';driver.write_text(fake);driver.chmod(0o755)
  for n in ['cat','perf']:(b/n).symlink_to(driver)
  (root/'runner').symlink_to(driver);(root/'config.json').write_text('{}')
  (root/'pins.sha256').write_text(''.join(hashlib.sha256(f.read_bytes()).hexdigest()+'  '+str(f)+'\n' for f in [root/'runner',root/'config.json']))
  script=d/'stage.sh';script.write_text((p/'stage.sh').read_text().replace('/mnt/borsuk-retained-s3',str(root)))
  r=subprocess.run(['/bin/bash','-x',str(script)],env=dict(os.environ,PATH=str(b)+':/usr/bin:/bin',CASE=mode,MOCK_ROOT=str(root)),capture_output=True,text=True,timeout=15)
  if r.returncode!=expected:
   (p/'stage-check-first-failure.json').write_text(json.dumps({'case':mode,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr,'evidence':{f.name:f.read_text(errors='replace') for f in (root/'evidence').iterdir() if f.is_file()}},indent=2)+'\n')
  assert r.returncode==expected,(mode,r.returncode,r.stderr)
  assert (root/'evidence/final-exit').read_text().strip()==str(expected)
  if mode in ['bad-cap','record-fail','report-fail','empty-samples']:assert not (root/'evidence/native-exit').exists()
  if mode=='pass':assert (root/'evidence/canary.status').exists()
  results.append({'case':mode,'exit':r.returncode})
receipt={'status':'PASS','cases':results,'kernel_limits':limits,'stage_sha256':hashlib.sha256((p/'stage.sh').read_bytes()).hexdigest(),'real':['Bash/timeout control flow','SHA pins and drift refusal','jq native refusal predicate','scratch check','file limit establishment'],'mocked':['perf recording/decoding','cgroup contents','Rust CLI'],'native_perf_execution':False}
(p/'stage-check.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
