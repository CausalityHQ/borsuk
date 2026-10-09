import pathlib,json,subprocess,time,datetime
r=pathlib.Path('/data/target/borsuk-cold-membership-native/sq8-clock-reuse-qualification-a0001');p=json.loads((r/'protocol.json').read_text());i=json.loads((r/'active-job.json').read_text())['instance_id'];base=['aws','--profile','causality','--region','eu-central-1'];deadline=time.monotonic()+10000
while True:
 x=subprocess.run(base+['s3api','head-object','--bucket',p['bucket'],'--key',p['prefix']+'/terminal.json'],capture_output=True,text=True)
 state=subprocess.check_output(base+['ec2','describe-instances','--instance-ids',i,'--query','Reservations[0].Instances[0].State.Name','--output','text']).decode().strip();print(datetime.datetime.now(datetime.timezone.utc).isoformat(),i,state,'terminal='+str(x.returncode==0),flush=True)
 if x.returncode==0 or state=='terminated' or time.monotonic()>deadline:break
 time.sleep(45)
x=subprocess.run(base+['ec2','terminate-instances','--instance-ids',i],capture_output=True,text=True);(r/'termination.json').write_text(x.stdout);(r/'termination.stderr').write_text(x.stderr);assert x.returncode==0
subprocess.run(base+['ec2','wait','instance-terminated','--instance-ids',i],check=True);(r/'wait.json').write_text(json.dumps({'instance_id':i,'terminated':True})+'\n')
for name in ['terminal.json','artifacts.sha256','evidence.tar.gz']:
 subprocess.run(base+['s3','cp','s3://'+p['bucket']+'/'+p['prefix']+'/'+name,str(r/name),'--only-show-errors'],check=True)
t=json.loads((r/'terminal.json').read_text());assert t['instance_id']==i;print('COLLECTED TERMINAL',json.dumps(t),flush=True)
if t['exit']==0:subprocess.run(base+['s3','cp','s3://'+p['bucket']+'/'+p['prefix']+'/supplemental/check_cohere_native_baseline',str(r/'check_cohere_native_baseline'),'--only-show-errors'],check=True)
