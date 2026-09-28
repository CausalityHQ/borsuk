"""One bounded frozen dirty-source correctness check on Causality Spot."""
import fcntl, gzip, hashlib, io, json, os, subprocess, sys, tarfile, time
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
sys.path.insert(0, str(Path.cwd()))
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, PROFILE_ARN, SUBNET, SECURITY_GROUP, missing, put_if_absent
SUBNET = 'subnet-034528fbd6977848f'
TAG = 'borsuk-source-fit-cost-check'
SCHEMA = 'borsuk-source-fit-cost-spot-v1'
OUT = Path('docs/research/source-fit-cost-20260928')
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += [str(p) for p in OUT.rglob('*') if p.is_file() and not p.name.startswith('aws-')]
files += [str(OUT/'aws-check.py')]
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix='research/source-fit-cost/20260928/a0001'
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
az=ec2.describe_subnets(SubnetIds=[SUBNET])['Subnets'][0]['AvailabilityZone']
quote=ec2.describe_spot_price_history(InstanceTypes=['c7g.2xlarge'],ProductDescriptions=['Linux/UNIX'],AvailabilityZone=az,MaxResults=1)['SpotPriceHistory'][0]
quote_rate=float(quote['SpotPrice'])
if quote_rate>0.30: raise RuntimeError('Spot quote exceeds preregistered cap')
reservation={'schema':SCHEMA,'source_base_commit':base,'source_archive_sha256':sha,
    'source_dirty':True,'attempt':'a0001','profile':'causality','region':REGION,
    'instance_type':'c7g.2xlarge','wall_seconds':3600,'test_timeout_seconds':1800,
    'cargo_jobs':4,'spot_price_observed_usd_per_hour':quote['SpotPrice'],'spot_quote_timestamp':quote['Timestamp'].isoformat(),'spot_max_usd_per_hour':'0.30',
    'cost_cap_compute_usd':0.35,'availability_zone':az,'scope':'Existing ReLAION 100k/1M source fitting CPU/RSS falsifier; no query/truth/quality/default selection',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-03748c04dc81412c6'}
runner.WALL_SECONDS=3600; runner.SCHEMA=SCHEMA
runner.ARTIFACTS += ('cpu.txt','compile.log','compile.time','fit-cost/result.json')
for rows in (100000,1000000):
    for phase in ('normalize','fit','hier-fit'):
        runner.ARTIFACTS += tuple(f'fit-cost/{rows}/{phase}{suffix}' for suffix in ('.time','.stdout','.stderr'))
    runner.ARTIFACTS += tuple(f'fit-cost/{rows}/{phase}.u64' for phase in ('fit','hier-fit'))
body=runner.user_data(base,sha,archive_key,prefix).replace('v174-relaid-bind-compile','source-fit-cost')
body=body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3.12 python3.12-pip util-linux')
body=body.replace("'source_commit':", "'source_base_commit':")
a=body.index('phase=test\n/usr/bin/time')
b=body.index('phase=complete',a)
block="""phase=source-compile
lscpu >cpu.txt
"$CARGO_HOME/bin/cargo" --version >>cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
export MALLOC_ARENA_MAX=2 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 BORSUK_CPU_THREADS=4 ARROW_DEFAULT_MEMORY_POOL=system
/usr/bin/time -v -o compile.time timeout --signal=TERM --kill-after=30 1200 \
  \"$CARGO_HOME/bin/cargo\" build --release --locked --manifest-path repo/Cargo.toml -p borsuk --jobs 4 --example build_sq8_source >compile.log 2>&1
phase=source-cost
/usr/bin/time -v -o test-resources.txt timeout --signal=TERM --kill-after=30 1800 \
  bash -c 'ulimit -v 4194304; exec taskset -c 0-3 \"$1\" \"$2\" \"$3\" \"$4\" \"$5\"' _ \
  \"$root/.venv/bin/python\" \"$root/repo/docs/research/source-fit-cost-20260928/probe.py\" \
  \"$root/repo/docs/research/source-fit-cost-20260928/config.json\" \"$root/fit-cost\" \
  \"$CARGO_TARGET_DIR/release/examples/build_sq8_source\" >test.log 2>&1
"""
body=body[:a]+block+body[b:]
if len(body.encode()) > 16384: raise ValueError('User-data too large')
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
put_if_absent(prefix+'/reservation.json',json.dumps(reservation,sort_keys=True).encode())
receipt=ec2.run_instances(ClientToken='source-fit-cost-a0001-'+sha[:35], ImageId=reservation['image'],
    InstanceType=reservation['instance_type'],MinCount=1,MaxCount=1,
    IamInstanceProfile={'Arn':PROFILE_ARN},
    NetworkInterfaces=[{'AssociatePublicIpAddress':True,'DeviceIndex':0,'Groups':[SECURITY_GROUP],'SubnetId':SUBNET}],
    InstanceMarketOptions={'MarketType':'spot','SpotOptions':{'InstanceInterruptionBehavior':'terminate','SpotInstanceType':'one-time','MaxPrice':'0.30'}},
    InstanceInitiatedShutdownBehavior='terminate',
    BlockDeviceMappings=[{'DeviceName':'/dev/xvda','Ebs':{'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}],
    TagSpecifications=[{'ResourceType':'instance','Tags':[{'Key':'Name','Value':TAG},{'Key':'BorsukAttempt','Value':'a0001'}]}],UserData=body)
instance=receipt['Instances'][0]['InstanceId']
started=time.monotonic()
launch={'instance_id':instance,'prefix':prefix,'bucket':BUCKET,'source_archive_sha256':sha,'source_base_commit':base}
(OUT/'aws-launch.json').write_text(json.dumps(launch,indent=2)+'\n')
print(json.dumps(launch),flush=True)
terminal=None
try:
    while time.monotonic()-started<3900:
        if not missing(s3,prefix+'/terminal.json'):
            raw=s3.get_object(Bucket=BUCKET,Key=prefix+'/terminal.json')['Body'].read()
            terminal=json.loads(raw)
            if any(terminal.get(k)!=v for k,v in {'schema':SCHEMA,'instance_id':instance,
                'source_archive_sha256':sha,'source_base_commit':base}.items()): raise RuntimeError('Terminal identity mismatch')
            (OUT/'aws-terminal.json').write_bytes(raw)
            required = set(runner.ARTIFACTS) - {n for n in runner.ARTIFACTS if n.endswith('.u64')}
            if terminal.get('status') == 'complete' and not required.issubset(terminal['artifacts']): raise RuntimeError('Complete terminal missing required artifacts')
            for name,ident in terminal['artifacts'].items():
                if name not in runner.ARTIFACTS: raise RuntimeError('Unexpected artifact')
                data=s3.get_object(Bucket=BUCKET,Key=f'{prefix}/artifacts/{name}')['Body'].read()
                if len(data)!=ident['bytes'] or hashlib.sha256(data).hexdigest()!=ident['sha256']: raise RuntimeError('Artifact identity mismatch')
                destination=OUT/(name+'.gz'); destination.parent.mkdir(parents=True,exist_ok=True); destination.write_bytes(gzip.compress(data,mtime=0))
            (OUT/'aws-terminal.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  aws-terminal.json\n')
            print(json.dumps(terminal),flush=True)
            break
        try:
            state=ec2.describe_instances(InstanceIds=[instance])['Reservations'][0]['Instances'][0]['State']['Name']
        except ClientError as error:
            if error.response.get('Error',{}).get('Code')!='InvalidInstanceID.NotFound' or time.monotonic()-started>=60:
                raise
            state='not-visible-yet'
        print(json.dumps({'instance_id':instance,'state':state,'elapsed_s':round(time.monotonic()-started)}),flush=True)
        if state in ('terminated','shutting-down'): raise RuntimeError('Instance closed without terminal; do not duplicate')
        time.sleep(20)
    else: raise TimeoutError('Original source-cost worker exceeded cap')
finally:
    ec2.terminate_instances(InstanceIds=[instance])
    ec2.get_waiter('instance_terminated').wait(InstanceIds=[instance])
    close={'instance_id':instance,'state':'terminated','observed_elapsed_s':round(time.monotonic()-started),
        'compute_cost_estimate_usd':round((time.monotonic()-started)/3600*quote_rate,4),
        'cost_status':'estimate from observed Spot quote and elapsed wall; excludes EBS/S3, not invoice'}
    (OUT/'aws-closeout.json').write_text(json.dumps(close,indent=2)+'\n'); print(json.dumps(close),flush=True)
if terminal is None or terminal.get('status')!='complete' or terminal.get('exit_code')!=0:
    raise RuntimeError('Original terminal failed; inspect authenticated closed evidence only')
