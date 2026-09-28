"""One bounded frozen dirty-source correctness check on Causality Spot."""
import fcntl, gzip, hashlib, io, json, os, subprocess, sys, tarfile, time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path.cwd()))
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, PROFILE_ARN, SUBNET, SECURITY_GROUP, missing, put_if_absent
TAG = 'borsuk-native-callable-gc-review-red'
SCHEMA = 'borsuk-native-library-spot-check-v1'
OUT = Path('docs/research/callable-gc-review-20260928/red-retry')
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += ['docs/research/callable-gc-review-20260928/plan.md', 'crates/borsuk/src/two_bit_gc.rs']
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix=f'research/native-library-check/gc-review-red/{sha}/a0002'
session=boto3.Session(profile_name='causality',region_name=REGION)
ec2,s3=session.client('ec2'),session.client('s3')
if not missing(s3,prefix+'/reservation.json') or not missing(s3,prefix+'/terminal.json'):
    raise RuntimeError('Frozen check already registered; observe original, never relaunch')
active=ec2.describe_instances(Filters=[{'Name':'tag:Name','Values':[TAG]},
    {'Name':'instance-state-name','Values':['pending','running','stopping','stopped']}])
if any(r['Instances'] for r in active['Reservations']): raise RuntimeError('Original worker already active')
if missing(s3,archive_key): put_if_absent(archive_key,archive)
else:
    previous=s3.get_object(Bucket=BUCKET,Key=archive_key)['Body'].read()
    if hashlib.sha256(previous).hexdigest()!=sha: raise RuntimeError('Immutable source archive mismatch')
reservation={'schema':SCHEMA,'source_base_commit':base,'source_archive_sha256':sha,
    'source_dirty':True,'attempt':'a0002','profile':'causality','region':REGION,
    'instance_type':'c7i.4xlarge','wall_seconds':1800,'test_timeout_seconds':1200,
    'cargo_jobs':4,'spot_price_observed_usd_per_hour':'0.3618','spot_max_usd_per_hour':'0.60',
    'cost_cap_compute_usd':0.30,'scope':'Regression red only; expected assertion failure for reviewed GC admission and validation fence bug; no green or S3 smoke',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-06121aa3085b6f918'}
put_if_absent(prefix+'/reservation.json',json.dumps(reservation,sort_keys=True).encode())
runner.WALL_SECONDS=1800; runner.SCHEMA=SCHEMA; runner.ARTIFACTS += ('red-test.log','red-exit.txt')
red_archive=Path('/tmp/borsuk-gc-review-red.tar.gz').read_bytes()
red_sha=hashlib.sha256(red_archive).hexdigest()
red_key=f'research/native-library-check/sources/{red_sha}.tar.gz'
if missing(s3,red_key): put_if_absent(red_key,red_archive)
else:
    if hashlib.sha256(s3.get_object(Bucket=BUCKET,Key=red_key)['Body'].read()).hexdigest()!=red_sha: raise RuntimeError('Red archive identity')
body=runner.user_data(base,sha,archive_key,prefix)
body=body.replace('v174-relaid-bind-compile','native-callable-gc-check')
body=body.replace("'source_commit':", "'source_base_commit':")
a=body.index('/usr/bin/time -v -o test-resources.txt')
b=body.index('phase=complete',a)
body=body[:a]+'true\n'+body[b:]
red_block=f'''phase=red-test
aws s3 cp 's3://{BUCKET}/{red_key}' source.red.tar.gz --only-show-errors
printf '%s  source.red.tar.gz\\n' '{red_sha}' | sha256sum -c -
rm -rf repo
mkdir repo && tar -xzf source.red.tar.gz -C repo
set +e
timeout --signal=TERM --kill-after=30 300 "$CARGO_HOME/bin/cargo" test --locked --manifest-path repo/Cargo.toml -p borsuk --test two_bit_application_ids --jobs 4 >red-test.log 2>&1
red_code=$?
set -e
printf '%s\\n' "$red_code" >red-exit.txt
[ "$red_code" -eq 101 ] || exit 91
grep -Eq 'invalid payload caps must cause no remote IO|validation failure before deletes must release the fence' red-test.log || exit 92
rm -rf repo
mkdir repo && tar -xzf source.tar.gz -C repo
"$CARGO_HOME/bin/cargo" clean --manifest-path repo/Cargo.toml -p borsuk
phase=test
'''
body=body.replace('phase=test\ntrue',red_block+'true',1)
assert 'red_code=$?' in body, 'Red command not inserted'
reservation['red_source_archive_sha256']=red_sha
reservation['red_timeout_seconds']=300
# Red archive receipt is immutable alongside the preregistered green reservation.
put_if_absent(prefix+'/red-reservation.json',json.dumps({'red_source_archive_sha256':red_sha,'expected_exit':101},sort_keys=True).encode())
body=body.replace('phase=test\n/usr/bin/time', f"phase=test\nexport BORSUK_NATIVE_TEST_BUCKET='{BUCKET}' BORSUK_NATIVE_TEST_REGION='{REGION}' BORSUK_NATIVE_TEST_PREFIX='{prefix}/smoke'\n/usr/bin/time",1)
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
receipt=ec2.run_instances(ClientToken='gc-review-red-a0002-'+sha[:35], ImageId=reservation['image'],
    InstanceType=reservation['instance_type'],MinCount=1,MaxCount=1,
    IamInstanceProfile={'Arn':PROFILE_ARN},
    NetworkInterfaces=[{'AssociatePublicIpAddress':True,'DeviceIndex':0,'Groups':[SECURITY_GROUP],'SubnetId':SUBNET}],
    InstanceMarketOptions={'MarketType':'spot','SpotOptions':{'InstanceInterruptionBehavior':'terminate','SpotInstanceType':'one-time','MaxPrice':'0.60'}},
    InstanceInitiatedShutdownBehavior='terminate',
    BlockDeviceMappings=[{'DeviceName':'/dev/xvda','Ebs':{'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}],
    TagSpecifications=[{'ResourceType':'instance','Tags':[{'Key':'Name','Value':TAG},{'Key':'BorsukAttempt','Value':'a0002'}]}],UserData=body)
instance=receipt['Instances'][0]['InstanceId']
started=time.monotonic()
launch={'instance_id':instance,'prefix':prefix,'bucket':BUCKET,'source_archive_sha256':sha,'source_base_commit':base}
(OUT/'aws-launch.json').write_text(json.dumps(launch,indent=2)+'\n')
print(json.dumps(launch),flush=True)
terminal=None
try:
    while time.monotonic()-started<2100:
        if not missing(s3,prefix+'/terminal.json'):
            raw=s3.get_object(Bucket=BUCKET,Key=prefix+'/terminal.json')['Body'].read()
            terminal=json.loads(raw)
            if any(terminal.get(k)!=v for k,v in {'schema':SCHEMA,'instance_id':instance,
                'source_archive_sha256':sha,'source_base_commit':base}.items()): raise RuntimeError('Terminal identity mismatch')
            (OUT/'aws-terminal.json').write_bytes(raw)
            for name,ident in terminal['artifacts'].items():
                if name not in runner.ARTIFACTS: raise RuntimeError('Unexpected artifact')
                data=s3.get_object(Bucket=BUCKET,Key=f'{prefix}/artifacts/{name}')['Body'].read()
                if len(data)!=ident['bytes'] or hashlib.sha256(data).hexdigest()!=ident['sha256']: raise RuntimeError('Artifact identity mismatch')
                (OUT/(name+'.gz')).write_bytes(gzip.compress(data,mtime=0))
            (OUT/'aws-terminal.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  aws-terminal.json\n')
            print(json.dumps(terminal),flush=True)
            break
        state=ec2.describe_instances(InstanceIds=[instance])['Reservations'][0]['Instances'][0]['State']['Name']
        print(json.dumps({'instance_id':instance,'state':state,'elapsed_s':round(time.monotonic()-started)}),flush=True)
        if state in ('terminated','shutting-down'): raise RuntimeError('Instance closed without terminal; do not duplicate')
        time.sleep(20)
    else: raise TimeoutError('Original correctness worker exceeded cap')
finally:
    ec2.terminate_instances(InstanceIds=[instance])
    ec2.get_waiter('instance_terminated').wait(InstanceIds=[instance])
    close={'instance_id':instance,'state':'terminated','observed_elapsed_s':round(time.monotonic()-started),
        'compute_cost_estimate_usd':round((time.monotonic()-started)/3600*0.3618,4),
        'cost_status':'estimate from observed Spot quote and elapsed wall; excludes EBS/S3, not invoice'}
    (OUT/'aws-closeout.json').write_text(json.dumps(close,indent=2)+'\n'); print(json.dumps(close),flush=True)
if terminal is None or terminal.get('status')!='complete' or terminal.get('exit_code')!=0:
    raise RuntimeError('Original terminal failed; inspect authenticated closed evidence only')
