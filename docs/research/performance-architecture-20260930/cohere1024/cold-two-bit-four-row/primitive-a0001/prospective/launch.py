import pathlib,json,subprocess,hashlib,base64,datetime,os
r=pathlib.Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-primitive-a0001');p=json.loads((r/'protocol.json').read_text());env=dict(os.environ,AWS_MAX_ATTEMPTS='1');bucket='borsuk-bench-453182569524-euc1';prefix=p['prefix'];commit=(r/'protocol-commit.txt').read_text().strip()
def aws(*a):
 q=subprocess.run(['aws','--profile','causality','--region','eu-central-1',*a],capture_output=True,text=True,timeout=55,env=env);assert q.returncode==0,q.stderr;return json.loads(q.stdout) if q.stdout.strip() else {}
assert subprocess.check_output(['/usr/bin/git','rev-parse','HEAD'],text=True).strip()==commit==subprocess.check_output(['/usr/bin/git','rev-parse','origin/main'],text=True).strip()
assert not (r/'launch.json').exists() and not (r/'launch-attempt.json').exists()
frozen='docs/research/performance-architecture-20260930/cohere1024/cold-two-bit-four-row/primitive-a0001/prospective/'
for n in ['protocol.json','user-data.sh','local-admission.json',*p['inputs']]:assert subprocess.check_output(['/usr/bin/git','show',commit+':'+frozen+n])==(r/n).read_bytes()
assert subprocess.check_output(['/usr/bin/git','show',commit+':'+frozen+'launch.py'])==pathlib.Path(__file__).read_bytes()
assert json.loads((r/'local-admission.json').read_text())['status']=='SOURCE_BOUND_PRIMITIVE_ADMISSION_PASS_NATIVE_CANARY_PENDING'
assert p['probe_caps']=={'cpu':1,'allowed_cpus':[0],'memory_bytes':268435456,'swap_bytes':0,'pids':128,'cpu_seconds':30,'wall_seconds':120}
assert p['native_builds']==0 and p['saved_s3_rerun'] is False

assert aws('sts','get-caller-identity')['Account']=='453182569524'
q=aws('ec2','describe-spot-price-history','--instance-types','c7i.2xlarge','--product-descriptions','Linux/UNIX','--availability-zone','eu-central-1c','--max-items','1');quote=q['SpotPriceHistory'][0];assert float(quote['SpotPrice'])<=.50 and .50*1200/3600<=.20;(r/'spot-quote.json').write_text(json.dumps(q,indent=2)+'\n')
prior=r.parent/'two-bit-four-row-control-build-a0001'
assert json.loads((prior/'wait.json').read_text())['exit']==0
assert json.loads((prior/'independent-matched-serving-stack-audit.json').read_text())['status']=='MATCHED_SERVING_SCORING_STACK_PASS'
assert aws('ec2','describe-instances','--instance-ids','i-0d12b4a605771cba6')['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
name='borsuk-cold-two-bit-four-row-primitive-a0001';existing=aws('ec2','describe-instances','--filters','Name=tag:Name,Values='+name);assert not any(z.get('Instances') for z in existing['Reservations'])
for n,pin in p['inputs'].items():
 data=(r/n).read_bytes();assert len(data)==pin['bytes'] and hashlib.sha256(data).hexdigest()==pin['sha256'];aws('s3','cp',str(r/n),'s3://'+bucket+'/'+prefix+'/inputs/'+n,'--only-show-errors');assert aws('s3api','head-object','--bucket',bucket,'--key',prefix+'/inputs/'+n)['ContentLength']==len(data)
elf=p['same_retained_elf'];assert aws('s3api','head-object','--bucket',bucket,'--key',elf['key'])['ContentLength']==elf['bytes']
data=(r/'user-data.sh').read_bytes();assert len(data)==p['userdata']['bytes'] and hashlib.sha256(data).hexdigest()==p['userdata']['sha256']
x=json.loads((r.parent/'page-auth-gate-a0002/run-instances.json').read_text());x['NetworkInterfaces'][0]['SubnetId']='subnet-0a12dbed0ca6fac25';x['ClientToken']=name;x['UserData']=base64.b64encode(data).decode();x['TagSpecifications'][0]['Tags']=[{'Key':'Name','Value':name},{'Key':'Purpose','Value':'exact-retained-four-row-primitive-no-ANN'}];assert sum(v.get('Ebs',{}).get('VolumeSize',0) for v in x['BlockDeviceMappings'])==20 and x['InstanceType']=='c7i.2xlarge' and x['InstanceMarketOptions']['MarketType']=='spot' and x['InstanceInitiatedShutdownBehavior']=='terminate';(r/'run-instances.json').write_text(json.dumps(x,indent=2)+'\n')
(r/'launch-attempt.json').write_text(json.dumps({'protocol_commit':commit,'source':p['candidate_commit'],'quote':quote,'compute_cap_usd':.20,'ancillary_cap_usd':.15,'client_token':name,'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native_ANN':False,'micro_timing':True},indent=2)+'\n')
launch=aws('ec2','run-instances','--cli-input-json','file://'+str(r/'run-instances.json'));(r/'launch.json').write_text(json.dumps(launch,indent=2)+'\n');assert len(launch['Instances'])==1;i=launch['Instances'][0]['InstanceId'];(r/'active-job.json').write_text(json.dumps({'status':'LAUNCHED','instance_id':i,'protocol_commit':commit,'prefix':prefix,'bucket':bucket,'performance_claim':False},indent=2)+'\n');print(i,flush=True)
