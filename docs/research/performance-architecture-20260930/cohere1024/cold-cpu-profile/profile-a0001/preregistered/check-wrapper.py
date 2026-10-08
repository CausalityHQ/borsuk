from pathlib import Path
import os,subprocess,tempfile,json,hashlib
p=Path(__file__).parent
cg=Path('/sys/fs/cgroup')/next(x.split(':',2)[2] for x in Path('/proc/self/cgroup').read_text().splitlines() if x.startswith('0:')).lstrip('/')
limits={x:(cg/x).read_text().strip() for x in ['cpu.max','memory.max','memory.swap.max']}
assert limits=={'cpu.max':'100000 100000','memory.max':'268435456','memory.swap.max':'0'},limits
(p/'wrapper-kernel-limits.json').write_text(json.dumps(limits,indent=2)+'\n')
fake=r'''#!/usr/bin/python3
import os,sys,json,datetime,subprocess
from pathlib import Path
name=Path(sys.argv[0]).name;a=sys.argv[1:];mode=os.environ['MOCK_MODE'];root=Path(os.environ['MOCK_ROOT'])
with (root.parent/'calls.log').open('a') as f:f.write(name+' '+repr(a)+'\n')
if name=='systemd-run':
 if mode=='timer-fail' and '--on-boot=3540s' in a:sys.exit(17)
 if '--unit=borsuk-retained-s3-gates' in a:
  e=root/'evidence';(e/'perf.data').write_text('mock raw user stack');(root/'native.started').write_text('yes');code=17 if mode=='native-fail' else 0
  (e/'native-exit').write_text(str(17 if code else 0)+'\n');(e/'final-exit').write_text(str(code)+'\n')
  if not code:(e/'canary-result.jsonl').write_text('{"mock":true}')
  sys.exit(code)
 sys.exit(0)
if name=='uname':
 if a==['-r']:print('7.0.0-1013-aws');sys.exit(0)
 os.execv('/usr/bin/uname',['uname',*a])
if name=='dpkg-query':print('linux-tools mock-version');sys.exit(0)
if name=='apt-get':sys.exit(17 if mode=='apt-fail' else 0)
if name in ['mount','umount','shutdown']:sys.exit(0)
if name=='systemctl':
 if mode=='already-unloaded' and a[0]=='stop' and a[-1]=='borsuk-retained-s3-gates.service':sys.exit(5)
 if mode=='already-unloaded' and a[0]=='show':print('LoadState=not-found\nMainPID=0\nActiveState=inactive\nResult=success');sys.exit(0)
 if mode=='closeout-fail' and a[0]=='stop' and a[-1]=='borsuk-retained-s3-gates.service':sys.exit(18)
 if a[0]=='show':print('MainPID=0\nActiveState=inactive\nResult=success')
 sys.exit(0)
if name=='curl':
 if '--output' in a:Path(a[a.index('--output')+1]).write_bytes(b'fixture');sys.exit(0)
 if any('dynamic/instance-identity' in x for x in a):
  t=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=3000 if mode=='late-start' else 0)
  v={'pendingTime':t.isoformat()}
  if mode!='identity-fail':v['instanceId']='i-00000000000000000'
  print(json.dumps(v))
 else:print('mock-token')
 sys.exit(0)
if name=='unzip':
 d=Path('aws');d.mkdir();f=d/'install';f.write_text('#!/bin/bash\nexit 0\n');f.chmod(0o755);sys.exit(0)
if name=='aws':
 if a==['--version']:print('aws-cli/2.36.11 fixture');sys.exit(0)
 if a[:2]==['s3','cp']:
  if mode=='download-fail':sys.exit(19)
  Path(a[3]).write_bytes(b'fixture');sys.exit(0)
 if a[:2]==['s3api','get-object']:
  assert '--if-match' in a
  Path(a[-1]).write_bytes(b'fixture');print('{\"mock\":true}');sys.exit(0)
 if a[:2]==['s3api','put-object']:
  assert a[a.index('--if-none-match')+1]=='*'
  key=a[a.index('--key')+1]
  if mode=='terminal-fail' and key.endswith('/terminal.json'):sys.exit(20)
  print('{"mock":true}');sys.exit(0)
 sys.exit(21)
if name=='sha256sum':
 if a and a[0]=='-c':print('MOCK_PINS_OK');sys.exit(0)
 os.execv('/usr/bin/sha256sum',['sha256sum',*a])
if name=='stat':
 if a[-1]=='awscliv2.zip':print('73022935');sys.exit(0)
 if a[-1]=='runner':print('16470968');sys.exit(0)
 if a[-1]=='reducer':print('1235680');sys.exit(0)
 if a[-1]=='queries.f32':print('4096000');sys.exit(0)
 if a[-1]=='truth.u64':print('80000');sys.exit(0)
 os.execv('/usr/bin/stat',['stat',*a])
sys.exit(22)
'''
results=[]
for mode,expected in [('already-unloaded',0),('success',0),('timer-fail',17),('apt-fail',17),('identity-fail',1),('download-fail',19),('late-start',95),('native-fail',17),('terminal-fail',96),('closeout-fail',96)]:
 with tempfile.TemporaryDirectory(prefix='retained-wrapper-',dir=p) as d:
  d=Path(d);root=d/'instance';bin=d/'bin';bin.mkdir();driver=bin/'driver';driver.write_text(fake);driver.chmod(0o755)
  for name in ['uname','dpkg-query','systemd-run','systemctl','apt-get','curl','unzip','aws','sha256sum','stat','mount','umount','shutdown']:(bin/name).symlink_to('driver')
  script=(p/'user-data.sh').read_text().replace('/mnt/borsuk-retained-s3',str(root)).replace('/usr/sbin/shutdown',str(bin/'shutdown')).replace('/dev/ttyS0',str(d/'console')).replace('. /etc/os-release','ID=ubuntu; VERSION_ID=24.04')
  body=d/'wrapper.sh';body.write_text(script)
  env=dict(os.environ,PATH=str(bin)+':/usr/bin:/bin',MOCK_MODE=mode,MOCK_ROOT=str(root))
  r=subprocess.run(['/bin/bash',str(body)],env=env,capture_output=True,text=True,timeout=15)
  assert r.returncode==expected,(mode,r.returncode,r.stderr,(root/'run.log').read_text() if (root/'run.log').exists() else '')
  console=(d/'console').read_text();calls=(d/'calls.log').read_text()
  assert 'shutdown ' in calls
  if mode in ['timer-fail','apt-fail','identity-fail','download-fail','late-start']:assert not (root/'native.started').exists(),mode
  if expected:assert 'BORSUK_FALLBACK' in console,mode
  else:assert 'BORSUK_TERMINAL_DELIVERED' in console,mode
  if (root/'artifacts.sha256').exists():assert 'perf.data' not in (root/'artifacts.sha256').read_text()
  if (root/'evidence.tar.gz').exists():
   import tarfile
   with tarfile.open(root/'evidence.tar.gz') as ar:assert './perf.data' not in ar.getnames()
  if (root/'terminal.json').exists():
   terminal=json.loads((root/'terminal.json').read_text())
   assert terminal['performance_claim'] is False
   if mode=='native-fail':assert terminal['original_exit']==terminal['native_exit']==17
   if mode in ['success','already-unloaded']:assert terminal['original_exit']==terminal['exit']==0 and terminal['native_exit']==0
   if mode=='closeout-fail':assert terminal['original_exit']==0 and terminal['exit']==96
  results.append({'case':mode,'actual_exit':r.returncode,'expected_exit':expected})
v={'status':'MOCK_WRAPPER_CHECK_PASS','cases':results,'source_sha256':hashlib.sha256((p/'user-data.sh').read_bytes()).hexdigest(),'real':['bash control flow','jq terminal serialization','timeouts','tar','non-input SHA computation'],'mocked':['systemd/cgroup/native publisher','IMDS/HTTP','AWS/S3','apt','mount/shutdown','input binary sizes and pin checks'],'native_runtime_pass':False,'cloud_operations':False}
(p/'wrapper-check.json').write_text(json.dumps(v,indent=2)+'\n');print(json.dumps(v))
