"""Freeze root request metadata; never invoke native/data validators or AWS."""
from pathlib import Path
import json,hashlib,time,re,subprocess,sys,os
assert not sys.flags.optimize and not os.environ.get('PYTHONOPTIMIZE')
D=Path('/tmp/borsuk-next1m-native-a0002-603fd5f9');REV='603fd5f96b5880ebeaf6919ad4b77775742a0ee7'
assert not (D/'launch-admission.json').exists()
prefix=(D/'run-prefix').read_text().strip()
r=json.loads((D/'request-template.json').read_text());s=json.loads((D/'source-roster.json').read_text())
q=json.loads((D/'spot-quote.json').read_text())['SpotPriceHistory'][0]
assert q['InstanceType']=='c7i.2xlarge' and q['AvailabilityZone']=='eu-central-1c' and 0<float(q['SpotPrice'])<=.59
assert 0<=time.time()-int((D/'spot-quote-observed.epoch').read_text())<=600
for row in s['assets']+[{'name':'support.sha256','bytes':len((D/'assets/support.sha256').read_bytes()),'sha256':s['support_sha256']}]:
 name=row['name'];body=(D/'assets'/name).read_bytes();assert len(body)==row['bytes'] and hashlib.sha256(body).hexdigest()==row['sha256']
 assert (D/(name+'.readback')).read_bytes()==body
 assert json.loads((D/(name+'.head.json')).read_text())['ContentLength']==len(body)
 assert json.loads((D/(name+'.head.json')).read_text())['ETag']==json.loads((D/(name+'.upload.json')).read_text())['ETag']
assert s['collector_runtime_gate_pending'] is False
assert s['collector_disposable_gate']['archive_sha256']=='e42f8d5cd1c7810aef6812a4896365a0e6df64388f2491391f6b3f3f8223d952'
assert s['qualified_baseline_source']=='c3e52c8bbf0fcc985c0a8a06d2abeec5d7442d38'
for name in ['user-data','watch-original']:
 old=(D/(name+'.pending.sh')).read_text();assert old.count('PENDING_ROOT_FREEZE_RUN_PREFIX')==1
 body=old.replace('PENDING_ROOT_FREEZE_RUN_PREFIX',prefix)
 if name=='user-data':
  assert body.count('PENDING_ROOT_FREEZE_SUPPORT_SHA256')==1
  body=body.replace('PENDING_ROOT_FREEZE_SUPPORT_SHA256',s['support_sha256'])
  assert len(body.encode())<=16384
  assert body.replace(prefix,'PENDING_ROOT_FREEZE_RUN_PREFIX',1).replace(s['support_sha256'],'PENDING_ROOT_FREEZE_SUPPORT_SHA256',1)==old
 else:assert body.replace(prefix,'PENDING_ROOT_FREEZE_RUN_PREFIX',1)==old
 (D/(name+'.sh')).write_text(body)
launcher=(D/'launch-once.pending.py').read_text()
old="LAUNCH_DIR='/data/target/borsuk-cold-membership-native/scale-maintenance-qualification-a0002/next1m-derivation-draft/actual1m-chain-a0001';RUN_PREFIX='research/semantic-router/20261010/actual1m-scale-chain-a0001';WATCH_UNIT='borsuk-next1m-scale-a0001-watch'"
new="LAUNCH_DIR="+repr(str(D))+";RUN_PREFIX="+repr(prefix)+";WATCH_UNIT='borsuk-next1m-scale-a0002-watch'"
assert launcher.count(old)==1
(D/'launch-once.py').write_text(launcher.replace(old,new))
epoch=int(time.time());token='borsuk-next1m-chain-a0002-'+str(epoch)
r.update(ClientToken=token,UserData=(D/'user-data.sh').read_text())
for spec in r['TagSpecifications']:
 for t in spec['Tags']:
  if t['Key']=='Name':t['Value']=token
assert r['InstanceType']=='c7i.2xlarge' and r['InstanceMarketOptions']['MarketType']=='spot' and r['Placement']['AvailabilityZone']=='eu-central-1c'
assert r['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.59'
assert r['ImageId']=='ami-0b8a830d6339a9758' and r['SubnetId']=='subnet-0a12dbed0ca6fac25'
assert [(m['DeviceName'],m['Ebs']['VolumeSize'],m['Ebs']['DeleteOnTermination'],m['Ebs']['Encrypted']) for m in r['BlockDeviceMappings']]==[('/dev/sda1',80,True,True),('/dev/sdf',40,True,True)]
(D/'launch-request.json').write_text(json.dumps(r,indent=2)+'\n')
a=json.loads((D/'prior-launch-admission.json').read_text())
a.update(started_epoch=epoch,external_deadline_epoch=epoch+15000,client_token=token,controller_commit=REV,native_commit=s['qualified_baseline_source'],support_sha256=s['support_sha256'],user_data_bytes=len((D/'user-data.sh').read_bytes()),user_data_sha256=hashlib.sha256((D/'user-data.sh').read_bytes()).hexdigest(),watcher_sha256=hashlib.sha256((D/'watch-original.sh').read_bytes()).hexdigest(),assets_stage_sha256=hashlib.sha256((D/'source-roster.json').read_bytes()).hexdigest(),platform_disposition_sha256=hashlib.sha256(Path('/tmp/borsuk-next1m-supervisor-canary-a0008-73886fff/closed-metadata-audit.json').read_bytes()).hexdigest(),producer_commit='bc3082a8210c4370ebadae4ba093a7c211072b62',baseline_source_commit=s['qualified_baseline_source'],original_invalid_attempt_preserved=True,collector_disposable_staging=s['collector_disposable_gate'],source_static_unit='run-p2002516-i714878876.service',source_static_exit=0,performance_claim=False)
e=json.loads((D/'prior-execution-envelope.json').read_text());e.update(candidate_controller_commit=REV,qualified_baseline_source=s['qualified_baseline_source'],status='FROZEN_AFTER_COLLECTOR_PLATFORM_AND_PRESERVED_REAL_INPUT_ADMISSION',actual_native_runtime_pending=True,collector_disposable_staging=s['collector_disposable_gate'])
e['compute'].update(spot_quote_usd_per_hour=q['SpotPrice'],quote_observed_epoch=int((D/'spot-quote-observed.epoch').read_text()),quote_sha256=hashlib.sha256((D/'spot-quote.json').read_bytes()).hexdigest())
(D/'execution-envelope.json').write_text(json.dumps(e,indent=2)+'\n')
(D/'launch-admission.json').write_text(json.dumps(a,indent=2)+'\n')
print('ROOT_METADATA_FROZEN',token,a['user_data_bytes'],s['support_sha256'],q['SpotPrice'])
