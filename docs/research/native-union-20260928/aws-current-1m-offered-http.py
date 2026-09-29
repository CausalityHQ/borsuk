"""One bounded current1M actual offered HTTP development measurement on Causality Spot."""
import fcntl, gzip, hashlib, io, json, os, subprocess, sys, tarfile, time
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
sys.path.insert(0, str(Path.cwd()))
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.launch_v157_primary_feasibility_spot import BUCKET, REGION, PROFILE_ARN, SUBNET, SECURITY_GROUP, missing, put_if_absent
ATTEMPT = sys.argv[1] if len(sys.argv) > 1 else 'a0001'
if len(ATTEMPT) != 5 or ATTEMPT[0] != 'a' or not ATTEMPT[1:].isdigit(): raise ValueError('attempt must be aNNNN')
SUBNET = sys.argv[2] if len(sys.argv) > 2 else 'subnet-0a12dbed0ca6fac25'
if SUBNET not in ('subnet-034528fbd6977848f','subnet-0a12dbed0ca6fac25','subnet-00243d923761c047c'): raise ValueError('unregistered subnet')
TAG = 'borsuk-native-current-1m-offered-http'
SCHEMA = 'borsuk-current-1m-offered-http-dev-v1'
ROOT_OUT = Path('docs/research/native-union-20260928')
OUT = ROOT_OUT / 'current-1m-offered-http' / ATTEMPT
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
for binary in ['two_bit_plan_demo','build_two_bit_generation']:
    assert Path('crates/borsuk/src/bin', binary+'.rs').is_file(), binary
assert Path('crates/borsuk/examples/two_bit_http.rs').is_file()
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += [str(ROOT_OUT/n) for n in ['current-1m-offered-http-preregister.md','current-1m-offered-http-config.json','aws-current-1m-offered-http.py','verify-current-1m-offered-http.py']]+['scripts/run_native_current_1m_offered_http.py']
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix='research/native-union/20260928/current-1m-offered-http-'+ATTEMPT
session=boto3.Session(profile_name='causality',region_name=REGION)
ec2,s3=session.client('ec2'),session.client('s3')
if not missing(s3,prefix+'/reservation.json') or not missing(s3,prefix+'/terminal.json'):
    raise RuntimeError('Frozen check already registered; observe original, never relaunch')
active=ec2.describe_instances(Filters=[{'Name':'tag:Name','Values':['borsuk-*']},
    {'Name':'instance-state-name','Values':['pending','running','stopping']}])
if any(r['Instances'] for r in active['Reservations']): raise RuntimeError('Original worker already active')
original=ec2.describe_instances(Filters=[{'Name':'tag:Name','Values':[TAG]},{'Name':'instance-state-name','Values':['stopped']}])
if any(r['Instances'] for r in original['Reservations']):raise RuntimeError('Original stopped worker exists; inspect original')
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
    'python_workers_cap':8,'spot_price_observed_usd_per_hour':quote['SpotPrice'],'spot_quote_timestamp':quote['Timestamp'].isoformat(),'spot_max_usd_per_hour':'0.30',
    'cost_cap_compute_usd':0.15,'cost_allowance_ebs_s3_usd':0.15,'scope':'Actual ReLAIONFIRST1M D768 consumed external development0-63 k10/100 native and offered8QPS HTTP, no fresh/vendor/matched control/lifecycle qualification; no Cargo/numerical recomputation',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-03748c04dc81412c6'}
config_path=ROOT_OUT/'current-1m-offered-http-config.json'
config_sha=hashlib.sha256(config_path.read_bytes()).hexdigest();config=json.loads(config_path.read_text())
proofs={}
for folder,ident in config['authorities'].items():
    for name,digest in ident.items():assert hashlib.sha256((ROOT_OUT/folder/name).read_bytes()).hexdigest()==digest
    proofs[folder]=json.loads((ROOT_OUT/folder/'verification.json').read_text())
    assert proofs[folder]['state']=='terminated' and (proofs[folder].get('valid_check') or proofs[folder].get('valid_measurement'))
proof=proofs['source-completion-integration/a0002'];http=proofs['http-topk-authority/a0002'];reference=proofs['native-reference-panel/a0002'];protocol=proofs['http-offered-protocol/a0003']
assert proof['full_assurance']['passed']==2696 and protocol['checks_passed']==5
expected=dict(proof['compiled_native_sha256']);expected['crates/borsuk/examples/two_bit_http.rs']=http['compiled_http_sha256'];expected['crates/borsuk/src/bin/two_bit_plan_demo.rs']=reference['compiled_source_sha256']
assert len(expected)==395 and all(hashlib.sha256(Path(n).read_bytes()).hexdigest()==d for n,d in expected.items())
protocol_body=json.loads(gzip.decompress((ROOT_OUT/'http-offered-protocol/a0003/protocol-check.json.gz').read_bytes()))
for n,d in protocol_body['source_sha256'].items():assert hashlib.sha256(Path(n).read_bytes()).hexdigest()==d
for n,d in config['dependencies'].items():assert hashlib.sha256(Path(n).read_bytes()).hexdigest()==d
closed_launch=json.loads((ROOT_OUT/'source-completion-1m/a0001/aws-launch.json').read_text());closed=json.loads((ROOT_OUT/'source-completion-1m/a0001/aws-terminal.json').read_text())
for role,ident in config['artifacts'].items():
    if role=='original-requests':continue
    suffix=ident['key'].removeprefix(closed_launch['prefix']+'/artifacts/')
    assert {k:ident[k] for k in ['bytes','sha256']}==closed['artifacts'][suffix]
assert config['artifacts']['original-requests']==json.loads((ROOT_OUT/'source-completion-1m-config.json').read_text())['artifacts']['requests']
for name,folder in [('two_bit_plan_demo','native-reference-panel/a0002'),('two_bit_http','http-topk-authority/a0002')]:
    l=json.loads((ROOT_OUT/folder/'aws-launch.json').read_text());t=json.loads((ROOT_OUT/folder/'aws-terminal.json').read_text())
    assert config['binaries'][name]==dict(key=l['prefix']+'/artifacts/binaries/'+name,**t['artifacts']['binaries/'+name])
reservation.update(existing_native_files_verified_unchanged=395,reused_authorities=config['authorities'],config_sha256=config_sha,measurement_memory_max_bytes=8589934592,address_space_limit_bytes=4294967296,swap_max_bytes=0,cpu_affinity='0-3',threads=4,compiled_native_sha256=expected)
artifacts=['reuse.json','cpu.txt','screen/native-quality.json','screen/decision.json','screen/cgroup.json','screen/requests64.jsonl']
for k in [10,100]:artifacts.extend('screen/reference-k'+str(k)+suffix for suffix in ['.jsonl','.log','.time'])
for rep,k in enumerate(config['setting_order']):artifacts.extend('screen/run'+str(rep)+'-k'+str(k)+'/'+n for n in ['http.jsonl','result.json','server.log','server.time','server-closeout.json'])
runner.WALL_SECONDS=1800;runner.SCHEMA=SCHEMA;runner.ARTIFACTS+=tuple(artifacts)
body=runner.user_data(base,sha,archive_key,prefix).replace('v174-relaid-bind-compile','native-current-1m-offered-http').replace("'source_commit':", "'source_base_commit':")
a=body.index('phase=install\n');b=body.index('phase=complete',a)
check='phase=install\ndnf install -y -q time\nlscpu >cpu.txt\nmkdir binaries\n'
for name,ident in config['binaries'].items():
    check+=f"aws s3 cp 's3://{BUCKET}/{ident['key']}' 'binaries/{name}' --only-show-errors\necho '{ident['sha256']}  binaries/{name}' | sha256sum -c -\ntest $(stat -c %s binaries/{name}) -eq {ident['bytes']}\nchmod +x binaries/{name}\n"
check+=f"cat >reuse.json <<'REUSE'\n{json.dumps(dict(binaries=config['binaries'],authorities=config['authorities'],full_library_tests_reused=2696,offered_protocol_checks_reused=5),sort_keys=True)}\nREUSE\n"
check+=f'''phase=measurement
systemd-run --unit=native-current-1m-offered-http --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=MALLOC_ARENA_MAX=2 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 1500 \\
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 python3 -m scripts.run_native_current_1m_offered_http "$1/repo/docs/research/native-union-20260928/current-1m-offered-http-config.json" "$2" "$1/screen" "$1/binaries" "$3"' _ "$root" '{config_sha}' '{prefix}' >test.log 2>&1
phase=check-artifacts
for name in screen/decision.json screen/native-quality.json screen/cgroup.json test-resources.txt; do test -s "$root/$name"; done
'''
body=body[:a]+check+body[b:]
subprocess.run(['bash','-n'],input=body,text=True,check=True)
assert len(body.encode())<=16384
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
client_token='current1m-http-'+ATTEMPT+'-'+sha[:32]
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
            print(json.dumps(dict(terminal_status=terminal['status'],exit_code=terminal['exit_code'],artifacts=len(terminal['artifacts']))),flush=True)
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
