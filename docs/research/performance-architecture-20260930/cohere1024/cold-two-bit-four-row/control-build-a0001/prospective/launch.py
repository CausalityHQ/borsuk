import pathlib,json,subprocess,hashlib,base64,datetime,os
r=pathlib.Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-control-build-a0001');p=json.loads((r/'protocol.json').read_text());env=dict(os.environ,AWS_MAX_ATTEMPTS='1');bucket='borsuk-bench-453182569524-euc1';prefix=p['prefix'];commit=(r/'protocol-commit.txt').read_text().strip()
def aws(*a):
 q=subprocess.run(['aws','--profile','causality','--region','eu-central-1',*a],capture_output=True,text=True,timeout=55,env=env);assert q.returncode==0,q.stderr;return json.loads(q.stdout) if q.stdout.strip() else {}
assert subprocess.check_output(['/usr/bin/git','rev-parse','HEAD'],text=True).strip()==commit==subprocess.check_output(['/usr/bin/git','rev-parse','origin/main'],text=True).strip()
frozen='docs/research/performance-architecture-20260930/cohere1024/cold-two-bit-four-row/control-build-a0001/prospective/'
for name in ['protocol.json','user-data.sh','local-admission-canary.json',*p['inputs']]:
 assert subprocess.check_output(['/usr/bin/git','show',commit+':'+frozen+name])==(r/name).read_bytes(),name+' differs from committed freeze'
assert subprocess.check_output(['/usr/bin/git','show',commit+':'+frozen+'launch.py'])==pathlib.Path(__file__).read_bytes(),'launch code differs from committed freeze'
assert p['cold_run'] is False and p['build_seconds']==7200 and p['build_memory_bytes']==8589934592 and p['build_cpu']==2 and p['build_jobs']==1 and p['machine_seconds']==9000 and p['swap']==0 and p['pids']==512
assert not (r/'launch.json').exists() and not (r/'launch-attempt.json').exists()
assert json.loads((r/'local-admission-canary.json').read_text())['status']=='PASS_BOUNDED_SOURCE_AND_STAGING_CANARY'
assert p['status']=='FROZEN_REVIEWED_ADMITTED_NATIVE_QUALIFICATION_ONLY' and p['paid_launch_authorized'] is True
assert p['candidate']=='fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b' and p['primitive_run'] is False
assert aws('sts','get-caller-identity')['Account']=='453182569524'
q=aws('ec2','describe-spot-price-history','--instance-types','c7i.2xlarge','--product-descriptions','Linux/UNIX','--availability-zone','eu-central-1c','--max-items','1');quote=q['SpotPriceHistory'][0];assert float(quote['SpotPrice'])<=.50 and .50*9000/3600<=1.50;(r/'spot-quote.json').write_text(json.dumps(q,indent=2)+'\n')
prior_completed=r.parent/'two-bit-four-row-qualification-a0004'
assert json.loads((prior_completed/'independent-verification.json').read_text())['status']=='FULL_NATIVE_CORRECTNESS_VERIFIED_SERVING_STACK_PENDING'
assert json.loads((prior_completed/'control-build-provenance-invalid.json').read_text())['status']=='CONTROL_SERVING_BINARY_PROVENANCE_INVALID_BUILD_REUSE'
assert json.loads((prior_completed/'wait.json').read_text())['exit']==0
assert aws('ec2','describe-instances','--instance-ids','i-05426de087af47c5a')['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
assert len(p['serial_stages'])==1 and p['control_target']=='/mnt/borsuk-http/target-control-isolated'
name='borsuk-cold-two-bit-four-row-control-build-a0001';existing=aws('ec2','describe-instances','--filters','Name=tag:Name,Values='+name);assert not any(z.get('Instances') for z in existing['Reservations'])
for n,pin in p['inputs'].items():
 data=(r/n).read_bytes();assert len(data)==pin['bytes'] and hashlib.sha256(data).hexdigest()==pin['sha256'];aws('s3','cp',str(r/n),'s3://'+bucket+'/'+prefix+'/inputs/'+n,'--only-show-errors');assert aws('s3api','head-object','--bucket',bucket,'--key',prefix+'/inputs/'+n)['ContentLength']==len(data)
source=p['source'];assert hashlib.sha256((r/'source.tar.gz').read_bytes()).hexdigest()==source['archive_sha256'];key='research/native-library-check/sources/'+source['archive_sha256']+'.tar.gz';aws('s3','cp',str(r/'source.tar.gz'),'s3://'+bucket+'/'+key,'--only-show-errors');assert aws('s3api','head-object','--bucket',bucket,'--key',key)['ContentLength']==source['archive_bytes']
data=(r/'user-data.sh').read_bytes();assert len(data)==p['userdata']['bytes'] and hashlib.sha256(data).hexdigest()==p['userdata']['sha256']
x=json.loads((r.parent/'page-auth-gate-a0001/run-instances.json').read_text());assert json.loads((r.parent/'two-bit-four-row-gate-a0001/launch-failure.json').read_text())['instance_created'] is False;x['NetworkInterfaces'][0]['SubnetId']='subnet-0a12dbed0ca6fac25';x['ClientToken']=name;x['UserData']=base64.b64encode(data).decode();x['TagSpecifications'][0]['Tags']=[{'Key':'Name','Value':name},{'Key':'Purpose','Value':'isolated-unchanged-control-serving-build-repair'}];assert sum(m.get('Ebs',{}).get('VolumeSize',0) for m in x['BlockDeviceMappings'])==80
assert x['InstanceType']=='c7i.2xlarge' and x['InstanceMarketOptions']['MarketType']=='spot' and x['InstanceInitiatedShutdownBehavior']=='terminate';(r/'run-instances.json').write_text(json.dumps(x,indent=2)+'\n')
(r/'launch-attempt.json').write_text(json.dumps({'protocol_commit':commit,'source':source['candidate'],'quote':quote,'compute_cap_usd':1.50,'ancillary_cap_usd':.15,'client_token':name,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native_ANN':False,'micro_timing':False},indent=2)+'\n')
launch=aws('ec2','run-instances','--cli-input-json','file://'+str(r/'run-instances.json'));(r/'launch.json').write_text(json.dumps(launch,indent=2)+'\n');assert len(launch['Instances'])==1;i=launch['Instances'][0]['InstanceId'];(r/'active-job.json').write_text(json.dumps({'status':'LAUNCHED','instance_id':i,'protocol_commit':commit,'prefix':prefix,'bucket':bucket,'performance_claim':False},indent=2)+'\n');print(i,flush=True)
