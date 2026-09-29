import base64
"""One bounded existing-source-order transfer through native union on Causality Spot."""
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
TAG = 'borsuk-native-source-completion-1m'
SCHEMA = 'borsuk-native-source-completion-1m-dev-v1'
ROOT_OUT = Path('docs/research/native-union-20260928')
OUT = ROOT_OUT / 'source-completion-1m' / ATTEMPT
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += [str(ROOT_OUT/'plan.md'),str(ROOT_OUT/'aws-source-completion-1m.py'),str(ROOT_OUT/'source-completion-1m-config.json'),str(ROOT_OUT/'source-completion-1m-preregister.md'),'scripts/run_native_source_completion_1m.py','crates/borsuk/src/two_bit_generation.rs']
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix='research/native-union/20260928/source-completion-1m-'+ATTEMPT
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
    'instance_type':'c7g.2xlarge','availability_zone':az,'wall_seconds':2700,'test_timeout_seconds':2400,
    'cargo_jobs':4,'spot_price_observed_usd_per_hour':quote['SpotPrice'],'spot_quote_timestamp':quote['Timestamp'].isoformat(),'spot_max_usd_per_hour':'0.30',
    'cost_cap_compute_usd':0.225,'cost_allowance_ebs_s3_usd':0.15,'scope':'ReLAION first1M consumed development0-63; same exact v4 CURRENT integrated-core control and bounded source-completion candidate root; ALL source-v3 components unchanged; fixed quality then incoming HTTP; no fresh/vendor qualification',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-03748c04dc81412c6'}
config_path=ROOT_OUT/'source-completion-1m-config.json'
config_sha=hashlib.sha256(config_path.read_bytes()).hexdigest()
config=json.loads(config_path.read_text())
for name,digest in config['dependencies'].items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Frozen scorer changed')
previous=ROOT_OUT/'locality-source-check/a0001'
old_launch=json.loads((previous/'aws-launch.json').read_text());old_terminal=json.loads((previous/'aws-terminal.json').read_text());proof=json.loads((previous/'verification.json').read_text())
assert proof['valid_check'] and proof['state']=='terminated' and proof['full_assurance']==dict(passed=2689,failed=0,ignored=26,targets=144)
assert hashlib.sha256((previous/'aws-terminal.json').read_bytes()).hexdigest()==proof['terminal_sha256']
control=ROOT_OUT/'walk-source-integration/a0001'
control_launch=json.loads((control/'aws-launch.json').read_text());control_terminal=json.loads((control/'aws-terminal.json').read_text());control_proof=json.loads((control/'verification.json').read_text())
assert control_proof['valid_check'] and control_proof['full_assurance']['passed']==2693 and control_proof['state']=='terminated'
assert hashlib.sha256((control/'aws-terminal.json').read_bytes()).hexdigest()==control_proof['terminal_sha256']
narrow=ROOT_OUT/'source-completion-integration/a0002'
narrow_launch=json.loads((narrow/'aws-launch.json').read_text());narrow_raw=(narrow/'aws-terminal.json').read_bytes();narrow_terminal=json.loads(narrow_raw);narrow_proof=json.loads((narrow/'verification.json').read_text())
assert narrow_proof['valid_check'] and narrow_proof['state']=='terminated' and narrow_proof['full_assurance']==dict(passed=2696,failed=0,ignored=26,targets=145) and narrow_proof['cargo_executed_targets']==146
assert hashlib.sha256(narrow_raw).hexdigest()==narrow_proof['terminal_sha256']
assert hashlib.sha256(Path('crates/borsuk/src/two_bit_generation.rs').read_bytes()).hexdigest()==narrow_proof['compiled_generation_sha256']
assert hashlib.sha256(Path('crates/borsuk/src/unit_centroid_graph.rs').read_bytes()).hexdigest()==narrow_proof['compiled_native_sha256']['crates/borsuk/src/unit_centroid_graph.rs']
assert config['runner_admission_bytes']==dict(control=1073741824,candidate=1073741824)
for receipt,directory in [(old_launch,previous),(control_launch,control),(narrow_launch,narrow)]:
    close=json.loads((directory/'aws-closeout.json').read_text());assert close['instance_id']==receipt['instance_id'] and close['state']=='terminated'
    found=[i for r in ec2.describe_instances(InstanceIds=[receipt['instance_id']])['Reservations'] for i in r['Instances']];assert not found or len(found)==1 and found[0]['State']['Name']=='terminated'
new_archive=s3.get_object(Bucket=BUCKET,Key='research/native-library-check/sources/'+narrow_launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(new_archive).hexdigest()==narrow_launch['source_archive_sha256'];new_matched=0
with tarfile.open(fileobj=io.BytesIO(new_archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name)
        if path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;new_matched+=1
assert new_matched==395
assert hashlib.sha256(Path('scripts/run_native_source_completion_1m.py').read_bytes()).hexdigest()==config['controller_sha256']
reservation.update(config_sha256=config_sha,reused_native_source_archive_sha256=old_launch['source_archive_sha256'],reused_terminal_sha256=proof['terminal_sha256'],measurement_memory_max_bytes=8589934592,process_address_space_bytes=4294967296,swap_max_bytes=0,cpu_affinity='0-3',threads=4,controller_sha256=config['controller_sha256'],new_native_files_matched=new_matched,new_native_source_archive_sha256=narrow_launch['source_archive_sha256'],candidate_authority_terminal_sha256=narrow_proof['terminal_sha256'],runner_admission_bytes=config['runner_admission_bytes'],control_authority_terminal_sha256=control_proof['terminal_sha256'],candidate_compiled_generation_sha256=narrow_proof['compiled_generation_sha256'],candidate_compiled_graph_sha256=narrow_proof['compiled_native_sha256']['crates/borsuk/src/unit_centroid_graph.rs'])
runner.WALL_SECONDS=2700;runner.SCHEMA=SCHEMA
prior_http=json.loads((ROOT_OUT/'source-completion-http/a0001/verification.json').read_text())
assert prior_http['valid_measurement'] and len(prior_http['rows'])==2 and all(r['decision'].startswith('GO') for r in prior_http['rows'])
assert ec2.describe_instances(InstanceIds=[prior_http['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
reservation['predecessor_http_terminal_sha256']=prior_http['terminal_sha256']
prior_build=json.loads((ROOT_OUT/'walk-source-1m/a0002/construction-verification.json').read_text());assert prior_build['valid_native_construction_metadata'] and prior_build['instance_terminated']
assert prior_build['terminal_sha256']==config['closed_construction_terminal_sha256']==hashlib.sha256((ROOT_OUT/'walk-source-1m/a0002/aws-terminal.json').read_bytes()).hexdigest()
construction=ROOT_OUT/'walk-source-1m/a0002';construction_launch=json.loads((construction/'aws-launch.json').read_text());construction_close=json.loads((construction/'aws-closeout.json').read_text())
assert construction_close['instance_id']==construction_launch['instance_id'] and construction_close['state']=='terminated'
found=[i for r in ec2.describe_instances(InstanceIds=[construction_launch['instance_id']])['Reservations'] for i in r['Instances']]
assert not found or len(found)==1 and found[0]['State']['Name']=='terminated'
reservation['closed_construction_terminal_sha256']=prior_build['terminal_sha256']
closed_science=json.loads((ROOT_OUT/'walk-source-1m/a0003/verification.json').read_text());assert closed_science['valid_measurement'] and closed_science['state']=='terminated' and closed_science['terminal_sha256']==config['closed_current_control']['terminal_sha256']
reservation['closed_current_control_terminal_sha256']=closed_science['terminal_sha256']
small=('helper.json','binaries/build_sq8_source','cpu.txt','environment.txt','reuse.json','test.log','test-resources.txt','screen/decision.json','screen/cgroup.json','screen/self-check.json')
small+=tuple('screen/relaion/'+name for name in ['normalize.log','normalize.time','hier-fit.log','hier-fit.time','order.u64','sq8.log','sq8.time','builder.json','binding.json','build.log','build.time','plan-control.log','plan-control.time','plan-control.jsonl','plan-candidate.log','plan-candidate.time','plan-candidate.jsonl','quality.json','result.json','oracle.json','truth.u32'])
small+=tuple('screen/relaion/generation/'+name for name in ['manifest.json','page_manifest.json','page_digests.bin','centroids.bin','graph.bin','diverse_graph.bin','plane/manifest.json','plane/mean.bin','plane/records.bin','canonical.bin'])
small+=tuple('screen/relaion/native-'+arm+'/'+name for arm in ['control','candidate'] for name in ['live.log','live.time','live.jsonl'])
small+=tuple('screen/relaion/run'+str(rep)+'-'+arm+'/'+name for rep,arm in enumerate(config['arm_order']) for name in ['http.jsonl','result.json','boundary.json','server.log','server.time','server-closeout.json'])
runner.ARTIFACTS+=small
body=runner.user_data(base,sha,archive_key,prefix).replace('v174-relaid-bind-compile','union-nomination-red')
body=body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3-devel pkgconf-pkg-config')
body=body.replace("'source_commit':", "'source_base_commit':")
a=body.index('phase=install');b=body.index('phase=complete',a)
check=f"""phase=reuse-native
dnf install -y -q gcc gcc-c++ cmake perl tar gzip python3.12 python3.12-pip util-linux time pkgconf-pkg-config
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
mkdir binaries
"""
for receipt,t,names in [
    (old_launch,old_terminal,[('build_sq8_source','build_sq8_source')]),
    (control_launch,control_terminal,[('two_bit_plan_demo','old_two_bit_plan_demo'),('two_bit_http','old_two_bit_http')]),
    (narrow_launch,narrow_terminal,[('build_two_bit_generation','build_two_bit_generation')]),
    (narrow_launch,narrow_terminal,[('two_bit_plan_demo','two_bit_plan_demo'),('two_bit_http','two_bit_http')]),
]:
    for name,output in names:
        identity=t['artifacts']['binaries/'+name]
        check+=f"""aws s3 cp 's3://{BUCKET}/{receipt['prefix']}/artifacts/binaries/{name}' binaries/{output} --only-show-errors
echo '{identity['sha256']}  binaries/{output}' | sha256sum -c -
[ \"$(stat -c %s binaries/{output})\" = '{identity['bytes']}' ]
chmod 755 binaries/{output}
"""
check+=f"""python3 - <<'PYHELPER' >helper.json
import hashlib,json
from pathlib import Path
p=Path('binaries/build_sq8_source')
print(json.dumps(dict(bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),source_archive_sha256='{old_launch['source_archive_sha256']}',existing_target='borsuk example build_sq8_source',native_source_matches_compiled_transformation=True,no_full_assurance_rerun=True,reused_binary=True)))
PYHELPER
"""
check+=f"""echo '{{"native_files_matched":393,"source_archive_sha256":"{old_launch['source_archive_sha256']}","terminal_sha256":"{proof['terminal_sha256']}","native_serving_binary_and_full_assurance_reused":true}}' >reuse.json
phase=layout
systemd-run --unit=native-union-layout --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \\
 --setenv=PYTHONPATH=\"$root/repo\" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o \"$root/test-resources.txt\" timeout --signal=TERM --kill-after=30 2400 \\
 bash -c 'set -e; ulimit -v 4194304; exec taskset -c 0-3 \"$1\" \"$2/repo/scripts/run_native_source_completion_1m.py\" \"$2/repo/docs/research/native-union-20260928/source-completion-1m-config.json\" \"$3\" \"$2/repo\" \"$2/screen\" \"$2/binaries\" \"$4\"' _ \\
 \"$root/.venv/bin/python\" \"$root\" '{config_sha}' '{prefix}' >test.log 2>&1
phase=measurement-artifacts
for name in screen/decision.json screen/cgroup.json screen/relaion/result.json test-resources.txt; do
 test -s "$root/$name"
done
"""
body=body[:a]+check+body[b:]
subprocess.run(['bash','-n'],input=body,text=True,check=True)
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
delivery_body=body
if len(body.encode())>16384:
    packed=base64.b64encode(gzip.compress(body.encode(),mtime=0)).decode()
    delivery_body="#!/bin/bash\nset -euo pipefail\nprintf '%s' '"+packed+"' | base64 -d | gzip -dc | bash\n"
    assert gzip.decompress(base64.b64decode(packed)).decode()==body
assert len(delivery_body.encode())<=16384
subprocess.run(['bash','-n'],input=delivery_body,text=True,check=True)
(OUT/'aws-user-data-envelope.sh').write_text(delivery_body)
client_token='frontier-1m-'+ATTEMPT+'-'+sha[:32]
assert len(client_token)<=64
put_if_absent(prefix+'/reservation.json',json.dumps(reservation,sort_keys=True).encode())
receipt=ec2.run_instances(ClientToken=client_token, ImageId=reservation['image'],
    InstanceType=reservation['instance_type'],MinCount=1,MaxCount=1,
    IamInstanceProfile={'Arn':PROFILE_ARN},
    NetworkInterfaces=[{'AssociatePublicIpAddress':True,'DeviceIndex':0,'Groups':[SECURITY_GROUP],'SubnetId':SUBNET}],
    InstanceMarketOptions={'MarketType':'spot','SpotOptions':{'InstanceInterruptionBehavior':'terminate','SpotInstanceType':'one-time','MaxPrice':'0.30'}},
    InstanceInitiatedShutdownBehavior='terminate',
    BlockDeviceMappings=[{'DeviceName':'/dev/xvda','Ebs':{'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}],
    TagSpecifications=[{'ResourceType':'instance','Tags':[{'Key':'Name','Value':TAG},{'Key':'BorsukAttempt','Value':ATTEMPT}]}],UserData=delivery_body)
instance=receipt['Instances'][0]['InstanceId']
started=time.monotonic()
launch={'instance_id':instance,'prefix':prefix,'bucket':BUCKET,'source_archive_sha256':sha,'source_base_commit':base}
(OUT/'aws-launch.json').write_text(json.dumps(launch,indent=2)+'\n')
print(json.dumps(launch),flush=True)
terminal=None
try:
    while time.monotonic()-started<3000:
        if not missing(s3,prefix+'/terminal.json'):
            raw=s3.get_object(Bucket=BUCKET,Key=prefix+'/terminal.json')['Body'].read()
            terminal=json.loads(raw)
            if any(terminal.get(k)!=v for k,v in {'schema':SCHEMA,'instance_id':instance,
                'source_archive_sha256':sha,'source_base_commit':base}.items()): raise RuntimeError('Terminal identity mismatch')
            (OUT/'aws-terminal.json').write_bytes(raw)
            for name,ident in terminal['artifacts'].items():
                if name not in runner.ARTIFACTS: raise RuntimeError('Unexpected artifact')
                stream=s3.get_object(Bucket=BUCKET,Key=f'{prefix}/artifacts/{name}')['Body']
                if ident['bytes']>64<<20:
                    digest=hashlib.sha256();size=0
                    for chunk in iter(lambda:stream.read(4<<20),b''):digest.update(chunk);size+=len(chunk)
                    assert size==ident['bytes'] and digest.hexdigest()==ident['sha256']
                    continue
                data=stream.read()
                if len(data)!=ident['bytes'] or hashlib.sha256(data).hexdigest()!=ident['sha256']: raise RuntimeError('Artifact identity mismatch')
                if not name.startswith('binaries/') and not (name.startswith('screen/relaion/generation/') and name.endswith('.bin')):
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
