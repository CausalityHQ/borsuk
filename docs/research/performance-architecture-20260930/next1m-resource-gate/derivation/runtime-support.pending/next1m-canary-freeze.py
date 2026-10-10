"""Build the exact request/user-data after uploads; no launch or native call."""
import pathlib,json,hashlib,shlex,time,sys,os
if sys.flags.optimize or os.environ.get("PYTHONOPTIMIZE"):
 raise SystemExit("INVALID: optimized Python cannot execute launch admission")
assert len(sys.argv)==2
d=pathlib.Path(sys.argv[1]);assert str(d).startswith('/tmp/borsuk-next1m-supervisor-canary-')
assert not (d/'launch-freeze.json').exists()
plan=json.loads((d/'asset-plan.json').read_text()); roster=json.loads((d/'support-roster.json').read_text())
assert len(roster)==3
assert {x['name'] for x in roster}=={'wrapper-canary.sh','run_native_scale_build_gate.sh','verify-closed.py'}
# Explicit root review disposition and fresh source-bound quote, never intent alone.
auth=json.loads((d/'root-source-admission.json').read_text())
assert auth['status']=='READY_DISPOSABLE_SUPERVISOR_CANARY'
for name in ['next1m-canary-worker.sh','next1m-canary-watch.sh','next1m-canary-launch-once.sh','next1m-canary-request.pending.json','next1m-canary-freeze.py','support-roster.json','canary-execution-envelope.pending.json']:
 assert hashlib.sha256((d/name).read_bytes()).hexdigest()==auth['files'][name]
assert auth['ann_measurement'] is False and auth['aws_profile']=='causality'
assert auth['total_cost_cap_usd']==0.60
assert 0 <= time.time()-auth['quote_observed_epoch'] <= 600
assert auth['quote_instance_type']=='c7i.2xlarge'
assert 0 < float(auth['quote_usd_per_hour']) <= 0.60
for row in roster:
 assert json.loads((d/(row['name']+'.upload.json')).read_text())['ETag']
 body=(d/'assets'/row['name']).read_bytes()
 assert len(body)==row['bytes'] and hashlib.sha256(body).hexdigest()==row['sha256']
epoch=int(time.time())
worker=(d/'next1m-canary-worker.sh').read_text()
anchor='bucket=${1:?}; prefix=${2:?}; assets=${3:?}; launch_epoch=${4:?}'
assert worker.count(anchor)==1
line='set -- '+' '.join(shlex.quote(x) for x in ['borsuk-bench-453182569524-euc1',plan['prefix'],plan['assets'],str(epoch)])+'\n'
bootstrap=worker.replace(anchor,line+anchor)
assert bootstrap.replace(line,'',1)==worker and len(bootstrap.encode())<=16384
(d/'bootstrap.sh').write_text(bootstrap)
r=json.loads((d/'next1m-canary-request.pending.json').read_text());r['ClientToken']=plan['client_token']
for spec in r['TagSpecifications']:
 for tag in spec['Tags']:
  if tag['Key']=='Name':tag['Value']=plan['client_token']
assert 'PENDING' not in json.dumps(r)
assert r['InstanceType']=='c7i.2xlarge' and r['BlockDeviceMappings'][0]['Ebs']['VolumeSize']==48
assert r['InstanceMarketOptions']['MarketType']=='spot'
assert r['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.60'
assert r['InstanceInitiatedShutdownBehavior']=='terminate'
assert r['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination'] is True
expected={
 'ImageId':'ami-0b8a830d6339a9758','InstanceType':'c7i.2xlarge',
 'MinCount':1,'MaxCount':1,'ClientToken':plan['client_token'],
 'IamInstanceProfile':{'Arn':'arn:aws:iam::453182569524:instance-profile/borsuk-bench-profile'},
 'NetworkInterfaces':[{'AssociatePublicIpAddress':True,'DeviceIndex':0,'Groups':['sg-0b1fd3e4fbde4af0d'],'SubnetId':'subnet-034528fbd6977848f'}],
 'BlockDeviceMappings':[{'DeviceName':'/dev/sda1','Ebs':{'VolumeSize':48,'VolumeType':'gp3','Encrypted':True,'DeleteOnTermination':True}}],
 'InstanceInitiatedShutdownBehavior':'terminate',
 'InstanceMarketOptions':{'MarketType':'spot','SpotOptions':{'SpotInstanceType':'one-time','InstanceInterruptionBehavior':'terminate','MaxPrice':'0.60'}},
 'MetadataOptions':{'HttpTokens':'required','HttpEndpoint':'enabled'},
 'TagSpecifications':[{'ResourceType':'instance','Tags':[{'Key':'Name','Value':plan['client_token']},{'Key':'Owner','Value':'borsuk/prod-ready-v9'}]},
                      {'ResourceType':'volume','Tags':[{'Key':'Name','Value':plan['client_token']}]}]
}
# JSON serialization preserves bool vs int, unlike Python structural equality.
assert json.dumps(r,sort_keys=True)==json.dumps(expected,sort_keys=True)
(d/'request.json').write_text(json.dumps(r,indent=2)+'\n')
def sha(name):return hashlib.sha256((d/name).read_bytes()).hexdigest()
plan.update(launched_epoch=epoch,bootstrap_sha256=sha('bootstrap.sh'),watch_sha256=sha('next1m-canary-watch.sh'),request_sha256=sha('request.json'),support_roster=roster,launch_authority=True)
(d/'launch-freeze.json').write_text(json.dumps(plan,indent=2)+'\n')
print('Frozen request/user-data metadata; no RunInstances call',epoch,sha('bootstrap.sh'))
