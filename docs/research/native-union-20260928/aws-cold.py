"""One bounded same-binary native cold comparison on Causality Spot."""
import fcntl, gzip, hashlib, io, json, os, subprocess, sys, tarfile, time
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
sys.path.insert(0, str(Path.cwd()))
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, PROFILE_ARN, SUBNET, SECURITY_GROUP, missing, put_if_absent
ATTEMPT = sys.argv[1] if len(sys.argv) > 1 else 'a0001'
if len(ATTEMPT) != 5 or ATTEMPT[0] != 'a' or not ATTEMPT[1:].isdigit(): raise ValueError('attempt must be aNNNN')
SUBNET = sys.argv[2] if len(sys.argv) > 2 else 'subnet-034528fbd6977848f'
if SUBNET not in ('subnet-034528fbd6977848f','subnet-0a12dbed0ca6fac25','subnet-00243d923761c047c'): raise ValueError('unregistered subnet')
TAG = 'borsuk-native-union-cold'
SCHEMA = 'borsuk-native-union-cold-v1'
ROOT_OUT = Path('docs/research/native-union-20260928')
OUT = ROOT_OUT / 'cold' / ATTEMPT
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += [str(ROOT_OUT/'plan.md'),str(ROOT_OUT/'aws-cold.py'),str(ROOT_OUT/'cold-config.json'),str(ROOT_OUT/'cold-preregister.md'),'scripts/run_native_union_cold.py']
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix='research/native-union/20260928/cold-'+ATTEMPT
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
    'source_dirty':True,'attempt':ATTEMPT,'profile':'causality','region':REGION,
    'instance_type':'c7g.2xlarge','availability_zone':az,'wall_seconds':1800,'test_timeout_seconds':1500,
    'cargo_jobs':0,'spot_price_observed_usd_per_hour':quote['SpotPrice'],'spot_quote_timestamp':quote['Timestamp'].isoformat(),'spot_max_usd_per_hour':'0.30',
    'cost_cap_compute_usd':0.15,'cost_allowance_ebs_s3_usd':0.15,'scope':'Same-binary v4 control/candidate ABBA native client-cold S3, ReLAION-first CoHere-second consumed dev64; no validation/scale/vendor qualification',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-03748c04dc81412c6'}
config_path=ROOT_OUT/'cold-config.json'
config_sha=hashlib.sha256(config_path.read_bytes()).hexdigest()
config=json.loads(config_path.read_text())
for name,digest in config['scorer_hashes'].items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Frozen scorer changed')
previous=ROOT_OUT/'green/a0003'
old_launch=json.loads((previous/'aws-launch.json').read_text());old_raw=(previous/'aws-terminal.json').read_bytes();old_terminal=json.loads(old_raw)
proof=json.loads((previous/'verification.json').read_text())
if proof['state']!='terminated' or proof['full_assurance']['failed']!=0 or proof['native_files_matched']!=392 or old_terminal['status']!='complete' or old_terminal['exit_code']!=0:raise RuntimeError('Compiled authority not established')
if hashlib.sha256(old_raw).hexdigest()!=proof['terminal_sha256'] or hashlib.sha256(old_raw).hexdigest()!=(previous/'aws-terminal.sha256').read_text().split()[0]:raise RuntimeError('Compiled terminal identity')
old_archive=s3.get_object(Bucket=BUCKET,Key='research/native-library-check/sources/'+old_launch['source_archive_sha256']+'.tar.gz')['Body'].read()
if hashlib.sha256(old_archive).hexdigest()!=old_launch['source_archive_sha256']:raise RuntimeError('Native archive identity')
matched=0
with tarfile.open(fileobj=io.BytesIO(old_archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            if path.read_bytes()!=tar.extractfile(member).read():raise RuntimeError('Changed native source '+member.name)
            matched+=1
if matched!=392:raise RuntimeError('Native source roster')
reservation.update(config_sha256=config_sha,reused_native_source_archive_sha256=old_launch['source_archive_sha256'],reused_native_files_matched=matched,reused_terminal_sha256=proof['terminal_sha256'],measurement_memory_max_bytes=8589934592,process_address_space_bytes=4294967296,swap_max_bytes=0,cpu_affinity='0-3',threads=4,controller_sha256=hashlib.sha256(Path('scripts/run_native_union_cold.py').read_bytes()).hexdigest())
runner.WALL_SECONDS=1800;runner.SCHEMA=SCHEMA
small=('cpu.txt','environment.txt','reuse.json','test.log','test-resources.txt','screen/decision.json','screen/cgroup.json')
small+=tuple('screen/'+dataset+'/'+name for dataset in ['relaion','cohere'] for name in ['builder.json','binding.json','build.log','build.time','paired.log','paired.time','native-paired.jsonl','native-control/manifest.json','native-candidate/manifest.json','result.json'])
small+=tuple('screen/'+dataset+'/run'+str(rep)+'-'+arm+'/'+name for dataset in ['relaion','cohere'] for rep,arm in enumerate(config['arm_order']) for name in ['live.jsonl','live.log','live.time','result.json'])
runner.ARTIFACTS+=small
body=runner.user_data(base,sha,archive_key,prefix).replace('v174-relaid-bind-compile','union-nomination-red')
body=body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config')
body=body.replace("'source_commit':", "'source_base_commit':")
a=body.index('phase=install');b=body.index('phase=complete',a)
check=f"""phase=reuse-native
dnf install -y -q python3.12 python3.12-pip util-linux time
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
mkdir binaries
"""
for name in ['build_two_bit_generation','two_bit_plan_demo']:
    identity=old_terminal['artifacts']['binaries/'+name]
    check+=f"""aws s3 cp 's3://{BUCKET}/{old_launch['prefix']}/artifacts/binaries/{name}' binaries/{name} --only-show-errors
echo '{identity['sha256']}  binaries/{name}' | sha256sum -c -
[ \"$(stat -c %s binaries/{name})\" = '{identity['bytes']}' ]
chmod 755 binaries/{name}
"""
check+=f"""echo '{{"native_files_matched":392,"source_archive_sha256":"{old_launch['source_archive_sha256']}","terminal_sha256":"{proof['terminal_sha256']}","no_compile_or_test_rerun":true}}' >reuse.json
phase=cold
systemd-run --unit=native-union-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 \\
 --setenv=PYTHONPATH=\"$root/repo\" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 \\
 /usr/bin/time -v -o \"$root/test-resources.txt\" timeout --signal=TERM --kill-after=30 1500 \\
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 \"$1\" \"$2/repo/scripts/run_native_union_cold.py\" \"$2/repo/docs/research/native-union-20260928/cold-config.json\" \"$3\" \"$2/repo\" \"$2/screen\" \"$2/binaries\" \"$4\"' _ \\
 \"$root/.venv/bin/python\" \"$root\" '{config_sha}' '{prefix}' >test.log 2>&1
"""
body=body[:a]+check+body[b:]
subprocess.run(['bash','-n'],input=body,text=True,check=True)
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
put_if_absent(prefix+'/reservation.json',json.dumps(reservation,sort_keys=True).encode())
receipt=ec2.run_instances(ClientToken='cold-cold-'+ATTEMPT+'-'+sha[:35], ImageId=reservation['image'],
    InstanceType=reservation['instance_type'],MinCount=1,MaxCount=1,
    IamInstanceProfile={'Arn':PROFILE_ARN},
    NetworkInterfaces=[{'AssociatePublicIpAddress':True,'DeviceIndex':0,'Groups':[SECURITY_GROUP],'SubnetId':SUBNET}],
    InstanceMarketOptions={'MarketType':'spot','SpotOptions':{'InstanceInterruptionBehavior':'terminate','SpotInstanceType':'one-time','MaxPrice':'0.30'}},
    InstanceInitiatedShutdownBehavior='terminate',
    BlockDeviceMappings=[{'DeviceName':'/dev/xvda','Ebs':{'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}],
    TagSpecifications=[{'ResourceType':'instance','Tags':[{'Key':'Name','Value':TAG},{'Key':'BorsukAttempt','Value':ATTEMPT}]}],UserData=body)
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
                if not name.startswith('binaries/'):
                    path=OUT/(name+'.gz');path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(gzip.compress(data,mtime=0))
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
    else: raise TimeoutError('Original correctness worker exceeded cap')
finally:
    ec2.terminate_instances(InstanceIds=[instance])
    ec2.get_waiter('instance_terminated').wait(InstanceIds=[instance])
    close={'instance_id':instance,'state':'terminated','observed_elapsed_s':round(time.monotonic()-started),
        'compute_cost_estimate_usd':round((time.monotonic()-started)/3600*quote_rate,4),
        'cost_status':'estimate from observed Spot quote and elapsed wall; excludes EBS/S3, not invoice'}
    (OUT/'aws-closeout.json').write_text(json.dumps(close,indent=2)+'\n'); print(json.dumps(close),flush=True)
if terminal is None or terminal.get('status')!='complete' or terminal.get('exit_code')!=0:
    raise RuntimeError('Original terminal failed; inspect authenticated closed evidence only')
