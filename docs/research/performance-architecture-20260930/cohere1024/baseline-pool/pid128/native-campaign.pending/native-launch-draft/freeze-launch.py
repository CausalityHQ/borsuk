"""Build the exact request/user-data after uploads; no launch or native call."""
import pathlib,json,hashlib,shlex,time,sys
assert len(sys.argv)==2
d=pathlib.Path(sys.argv[1]);assert str(d).startswith('/tmp/borsuk-pid128-native-ec2-')
assert not (d/'launch-freeze.json').exists()
plan=json.loads((d/'asset-plan.json').read_text()); roster=json.loads((d/'support-roster.json').read_text())
assert len(roster)==19
for row in roster:
 assert json.loads((d/(row['name']+'.upload.json')).read_text())['ETag']
 body=(d/'assets'/row['name']).read_bytes()
 assert len(body)==row['bytes'] and hashlib.sha256(body).hexdigest()==row['sha256']
epoch=int(time.time())
worker=(d/'native-worker.sh').read_text()
anchor='bucket=${1:?}; prefix=${2:?}; assets=${3:?}; launched=${4:?}'
assert worker.count(anchor)==1
line='set -- '+' '.join(shlex.quote(x) for x in ['borsuk-bench-453182569524-euc1',plan['prefix'],plan['assets'],str(epoch)])+'\n'
bootstrap=worker.replace(anchor,line+anchor)
assert bootstrap.replace(line,'',1)==worker and len(bootstrap.encode())<=16384
(d/'bootstrap.sh').write_text(bootstrap)
r=json.loads((d/'request.pending.json').read_text());r['ClientToken']=plan['client_token']
for spec in r['TagSpecifications']:
 for tag in spec['Tags']:
  if tag['Key']=='Name':tag['Value']=plan['client_token']
assert 'PENDING' not in json.dumps(r)
assert r['InstanceType']=='c7a.2xlarge' and r['BlockDeviceMappings'][0]['Ebs']['VolumeSize']==48
(d/'request.json').write_text(json.dumps(r,indent=2)+'\n')
def sha(name):return hashlib.sha256((d/name).read_bytes()).hexdigest()
plan.update(launched_epoch=epoch,bootstrap_sha256=sha('bootstrap.sh'),watch_sha256=sha('native-watch.sh'),request_sha256=sha('request.json'),support_roster=roster,launch_authority=True)
(d/'launch-freeze.json').write_text(json.dumps(plan,indent=2)+'\n')
print('Frozen request/user-data metadata; no RunInstances call',epoch,sha('bootstrap.sh'))
