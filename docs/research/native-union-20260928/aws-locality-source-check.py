"""One bounded frozen dirty-source correctness check on Causality Spot."""
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
TAG = 'borsuk-native-locality-source-check'
SCHEMA = 'borsuk-native-locality-source-check-v1'
ROOT_OUT = Path('docs/research/native-union-20260928')
OUT = ROOT_OUT / 'locality-source-check' / ATTEMPT
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += [str(ROOT_OUT/'locality-source-preregister.md'),str(ROOT_OUT/'aws-locality-source-check.py'),'crates/borsuk/src/source_order.rs','scripts/check_locality_source_layout.py']
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix='research/native-union/20260928/locality-source-check-'+ATTEMPT
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
    'instance_type':'c7g.2xlarge','availability_zone':az,'wall_seconds':4200,'test_timeout_seconds':3900,
    'cargo_jobs':4,'spot_price_observed_usd_per_hour':quote['SpotPrice'],'spot_quote_timestamp':quote['Timestamp'].isoformat(),'spot_max_usd_per_hour':'0.30',
    'cost_cap_compute_usd':0.35,'cost_allowance_ebs_s3_usd':0.10,'scope':'source-cell nearest-edge ordering assertion RED; declared adjacency-local chain andv3 recipe, same groups/reservoir/assignment/radius/querycaps; graph/source identity/budget/GREEN; one full native gate; release/synthetic publicCLI; no corpusANN/quality',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-03748c04dc81412c6'}
previous=ROOT_OUT/'fine-source-check/a0002'
old_launch=json.loads((previous/'aws-launch.json').read_text());proof=json.loads((previous/'verification.json').read_text())
assert proof['state']=='terminated' and proof['valid_check'] and proof['full_assurance']['failed']==0
old_archive=s3.get_object(Bucket=BUCKET,Key='research/native-library-check/sources/'+old_launch['source_archive_sha256']+'.tar.gz')['Body'].read()
assert hashlib.sha256(old_archive).hexdigest()==old_launch['source_archive_sha256']
matched=0
with tarfile.open(fileobj=io.BytesIO(old_archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            current=path.read_bytes();expected=tar.extractfile(member).read()
            if member.name in ['crates/borsuk/src/source_order.rs','crates/borsuk/examples/build_sq8_source.rs']:
                is_source=member.name.endswith('/source_order.rs');artifact='source_order.fixed.rs' if is_source else 'build_sq8_source.fixed.rs'
                expected=s3.get_object(Bucket=BUCKET,Key=old_launch['prefix']+'/artifacts/'+artifact)['Body'].read()
                assert hashlib.sha256(expected).hexdigest()==proof['compiled_source_sha256' if is_source else 'compiled_example_sha256']
            if member.name=='crates/borsuk/src/centroid_hnsw.rs':
                start=current.index(b'    /// Source-cell ordering; qualified separately from query routing.');end=current.index(b'    /// Order nodes by a BFS',start)
                current=current[:start]+current[end:]
                start=current.index(b'    #[test]\n    fn source_cell_order_follows_nearest_unvisited_edges()');end=current.index(b'    #[test]\n    fn sequential_diverse_builder_is_deterministic_and_degree_bounded()',start)
                current=current[:start]+current[end:]
            assert current==expected,member.name
            matched+=1
assert matched==393
reservation.update(existing_native_files_verified_except_declared_regression=matched,reused_previous_assurance_terminal_sha256=proof['terminal_sha256'],measurement_memory_max_bytes=10737418240,swap_max_bytes=0,cpu_affinity='0-3',threads=4)
runner.WALL_SECONDS=4200; runner.SCHEMA=SCHEMA; runner.ARTIFACTS += ('cpu.txt','red.log','green.log','full.log','release.log','boundary-check.json','boundary-cgroup.json','source_order.fixed.rs','build_sq8_source.fixed.rs','centroid_hnsw.fixed.rs','recipe.log','recipe-check.json','fixture-order.u64','binaries/two_bit_http','binaries/build_sq8_source','binaries/build_two_bit_generation','binaries/two_bit_plan_demo')
body=runner.user_data(base,sha,archive_key,prefix).replace('v174-relaid-bind-compile','union-nomination-red')
body=body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config')
body=body.replace("'source_commit':", "'source_base_commit':")
a=body.index('phase=test\n/usr/bin/time'); b=body.index('phase=complete',a)
check=r'''phase=boundary-check
"$CARGO_HOME/bin/rustup" component add rustfmt
lscpu >cpu.txt
"$CARGO_HOME/bin/rustc" --version >>cpu.txt
systemd-run --unit=native-locality-source-check --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=3930 \
 --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \
 --setenv=TOKIO_WORKER_THREADS=4 --setenv=MALLOC_ARENA_MAX=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3900 \
 taskset -c 0-3 python3 "$root/repo/scripts/check_locality_source_layout.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
'''
check+='\nphase=check-artifacts\nfor name in centroid_hnsw.fixed.rs source_order.fixed.rs build_sq8_source.fixed.rs boundary-check.json boundary-cgroup.json release.log recipe-check.json recipe.log fixture-order.u64 binaries/build_two_bit_generation binaries/two_bit_plan_demo binaries/two_bit_http binaries/build_sq8_source; do test -s \"$root/$name\"; done\n'
body=body[:a]+check+body[b:]
subprocess.run(['bash','-n'],input=body,text=True,check=True)
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
client_token='locality-source-check-'+ATTEMPT+'-'+sha[:35]
assert len(client_token)<=64
put_if_absent(prefix+'/reservation.json',json.dumps(reservation,sort_keys=True).encode())
receipt=ec2.run_instances(ClientToken=client_token, ImageId=reservation['image'],
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
    while time.monotonic()-started<4500:
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
