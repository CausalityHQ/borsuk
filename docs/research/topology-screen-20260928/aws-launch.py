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
TAG = 'borsuk-topology-mechanism-screen'
SCHEMA = 'borsuk-topology-mechanism-screen-v1'
ROOT_OUT = Path('docs/research/topology-screen-20260928')
OUT = ROOT_OUT / ATTEMPT
OUT.mkdir(parents=True,exist_ok=True)
lock = open('/tmp/borsuk-native-callable-gc-check.lock', 'a+')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
base = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
config=json.loads((ROOT_OUT/'config.json').read_text())
for name,digest in config['source_scorer_hashes'].items():
    if hashlib.sha256(Path(name).read_bytes()).hexdigest()!=digest: raise RuntimeError('Frozen scorer changed')
files = subprocess.check_output(['git','ls-files'],text=True).splitlines()
files += [str(ROOT_OUT/'preregister.md'), str(ROOT_OUT/'aws-launch.py'), str(ROOT_OUT/'config.json'), 'scripts/run_native_two_bit_topology.py', 'scripts/test_native_two_bit_topology_runner.py']
buf=io.BytesIO()
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
    for name in sorted(set(files)):
        path=Path(name)
        if path.is_file(): tar.add(path,arcname=name,recursive=False)
archive=buf.getvalue(); sha=hashlib.sha256(archive).hexdigest()
archive_key=f'research/native-library-check/sources/{sha}.tar.gz'
prefix='research/topology-mechanism-screen/20260928/'+ATTEMPT
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
    'instance_type':'c7g.2xlarge','availability_zone':az,'wall_seconds':3600,'test_timeout_seconds':1800,
    'cargo_jobs':4,'spot_price_observed_usd_per_hour':quote['SpotPrice'],'spot_quote_timestamp':quote['Timestamp'].isoformat(),'spot_max_usd_per_hour':'0.30',
    'cost_cap_compute_usd':0.35,'cost_allowance_ebs_s3_usd':0.20,'whole_budget_cap_usd':0.55,'scope':'Existing topology-only ReLAION-first consumed dev64 mechanism falsifier; no validation, scale, physical serving or vendor qualification',
    'interruption_policy':'Discard incomplete check; no automatic replacement',
    'image':'ami-03748c04dc81412c6'}
config_sha=hashlib.sha256((ROOT_OUT/'config.json').read_bytes()).hexdigest()
reservation.update(config_sha256=config_sha,compile_timeout_seconds=1200,process_address_space_bytes=4294967296,
    measurement_memory_max_bytes=8589934592,compile_memory_max_bytes=12884901888,swap_max_bytes=0,
    logical_query_byte_cap=16773120,logical_query_get_cap=32,cpu_affinity='0-3',threads=4,
    source_payload_bytes=268435456,two_pinned_generation_allowance_bytes=2147483648,
    query_scratch='400000 + TwoBitPlanTrace::scratch_bytes(usize::MAX)',
    controller_sha256=hashlib.sha256(Path('scripts/run_native_two_bit_topology.py').read_bytes()).hexdigest(),
    metadata_helpers_sha256=hashlib.sha256(Path('scripts/native_two_bit_topology.py').read_bytes()).hexdigest())
runner.WALL_SECONDS=3600; runner.SCHEMA=SCHEMA
small=('cpu.txt','compile.log','compile.time','environment.txt','metadata-check.log','adapter-check.log','screen/decision.json','screen/cgroup.json')
phases=('normalize','fit','encode','build','preflight','graph','paired')
metadata=('manifest.json','graph.bin','centroids.bin','page_manifest.json','page_digests.bin','plane/manifest.json','plane/mean.bin','plane/records.bin')
large=[]
large += ['binaries/build_sq8_source','binaries/build_two_bit_generation','binaries/build_two_bit_graph_variant','binaries/two_bit_plan_demo']
for dataset in ('relaion','cohere'):
    small += tuple(f'screen/{dataset}/{phase}{suffix}' for phase in phases for suffix in ('.stdout','.stderr','.time'))
    small += tuple(f'screen/{dataset}/{name}' for name in ('builder.json','authority.json','scoring.json','preflight-plans.jsonl','paired-plans.jsonl','decomposition.json','gained-lost.json','result.json','candidate/build.json'))
    small += tuple(f'screen/{dataset}/{arm}/{name}' for arm in ('control','candidate') for name in metadata)
    large += [f'screen/{dataset}/{name}' for name in ('order.u64','sq8.bin','normalized.f32','scores.npy','control/canonical.bin')]
runner.ARTIFACTS += small+tuple(large)
body=runner.user_data(base,sha,archive_key,prefix).replace('v174-relaid-bind-compile','topology-mechanism-screen')
body=body.replace('gcc gcc-c++ cmake perl tar gzip time', 'gcc gcc-c++ cmake perl tar gzip time python3.12 python3.12-pip util-linux')
body=body.replace("'source_commit':", "'source_base_commit':")
a=body.index('phase=test\n/usr/bin/time'); b=body.index('phase=complete',a)
block=f"""phase=compile
lscpu >cpu.txt
\"$CARGO_HOME/bin/cargo\" --version >>cpu.txt
\"$CARGO_HOME/bin/rustc\" --version >>cpu.txt
python3.12 -m venv .venv
.venv/bin/pip install -q numpy==2.3.3 pyarrow==24.0.0
export PYTHONPATH=\"$root/repo\" MALLOC_ARENA_MAX=2 OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 ARROW_DEFAULT_MEMORY_POOL=system TOKIO_WORKER_THREADS=4
.venv/bin/python -c 'import numpy,pyarrow,platform; print(numpy.__version__,pyarrow.__version__,platform.platform())' >environment.txt
systemd-run --unit=topology-screen-compile --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=1230 \\
 --setenv=RUSTUP_HOME=\"$RUSTUP_HOME\" --setenv=CARGO_HOME=\"$CARGO_HOME\" --setenv=PATH=\"$CARGO_HOME/bin:$PATH\" \\
 /usr/bin/time -v -o \"$root/compile.time\" timeout --signal=TERM --kill-after=30 1200 \\
 \"$CARGO_HOME/bin/cargo\" build --release --locked --manifest-path \"$root/repo/Cargo.toml\" --target-dir \"$root/target\" -p borsuk --jobs 4 \\
 --example build_sq8_source --bin build_two_bit_generation --bin build_two_bit_graph_variant --bin two_bit_plan_demo >compile.log 2>&1
phase=adapter-check
systemd-run --unit=topology-screen-adapter-check --wait --pipe -p MemoryMax=12G -p MemorySwapMax=0 -p RuntimeMaxSec=630 \\
 --setenv=RUSTUP_HOME=\"$RUSTUP_HOME\" --setenv=CARGO_HOME=\"$CARGO_HOME\" --setenv=PATH=\"$CARGO_HOME/bin:$PATH\" \\
 bash -c 'set -e; cd \"$1/repo\"; \"$CARGO_HOME/bin/cargo\" test --release --locked --target-dir \"$1/target\" -p borsuk --jobs 4 --bin build_two_bit_graph_variant; \"$CARGO_HOME/bin/cargo\" test --release --locked --target-dir \"$1/target\" -p borsuk --jobs 4 --test two_bit_generation graph_variant_adapter_preserves_components_and_rejects_untrusted_roots' _ \"$root\" >adapter-check.log 2>&1
mkdir binaries
cp "$root/target/release/examples/build_sq8_source" binaries/build_sq8_source
for name in build_two_bit_generation build_two_bit_graph_variant two_bit_plan_demo; do cp "$root/target/release/$name" "binaries/$name"; done
phase=mechanism-screen
systemd-run --unit=topology-screen-measure --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1830 \\
 --setenv=PYTHONPATH=\"$root/repo\" --setenv=MALLOC_ARENA_MAX=2 --setenv=OPENBLAS_NUM_THREADS=4 --setenv=OMP_NUM_THREADS=4 --setenv=MKL_NUM_THREADS=4 --setenv=ARROW_DEFAULT_MEMORY_POOL=system --setenv=TOKIO_WORKER_THREADS=4 \\
 /usr/bin/time -v -o \"$root/test-resources.txt\" timeout --signal=TERM --kill-after=30 1800 \\
 bash -c 'set -e; ulimit -v 4194304; \"$1\" -m scripts.test_native_two_bit_topology >\"$2/metadata-check.log\" 2>&1; \"$1\" -m scripts.test_native_two_bit_topology_runner >>\"$2/metadata-check.log\" 2>&1; exec taskset -c 0-3 \"$1\" \"$2/repo/scripts/run_native_two_bit_topology.py\" \"$2/repo/docs/research/topology-screen-20260928/config.json\" \"$3\" \"$2/repo\" \"$2/screen\" \"$2/target/release\"' _ \\
 \"$root/.venv/bin/python\" \"$root\" '{config_sha}' >test.log 2>&1
"""
body=body[:a]+block+body[b:]
if len(body.encode())>16384: raise ValueError('User-data exceeds EC2 cap')
subprocess.run(['bash','-n'],input=body,text=True,check=True)
try:
    reservation['s3_versioning_observed']=s3.get_bucket_versioning(Bucket=BUCKET).get('Status','Disabled')
except Exception:
    reservation['s3_versioning_observed']='Unknown; no physical-space claim'
(OUT/'aws-user-data.sh').write_text(body)
(OUT/'aws-reservation.json').write_text(json.dumps(reservation,indent=2)+'\n')
put_if_absent(prefix+'/reservation.json',json.dumps(reservation,sort_keys=True).encode())
receipt=ec2.run_instances(ClientToken='ts-'+ATTEMPT+'-'+sha[:35], ImageId=reservation['image'],
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
    while time.monotonic()-started<3900:
        if not missing(s3,prefix+'/terminal.json'):
            raw=s3.get_object(Bucket=BUCKET,Key=prefix+'/terminal.json')['Body'].read()
            terminal=json.loads(raw)
            if any(terminal.get(k)!=v for k,v in {'schema':SCHEMA,'instance_id':instance,
                'source_archive_sha256':sha,'source_base_commit':base}.items()): raise RuntimeError('Terminal identity mismatch')
            (OUT/'aws-terminal.json').write_bytes(raw)
            if terminal.get('status')=='complete' and not {'screen/decision.json','cpu.txt','compile.time','test-resources.txt','metadata-check.log'}.issubset(terminal['artifacts']): raise RuntimeError('Complete terminal missing authority/result')
            for name,ident in terminal['artifacts'].items():
                if name not in runner.ARTIFACTS: raise RuntimeError('Unexpected artifact')
                stream=s3.get_object(Bucket=BUCKET,Key=f'{prefix}/artifacts/{name}')['Body']
                digest=hashlib.sha256(); size=0; parts=[]
                while chunk:=stream.read(4<<20):
                    digest.update(chunk);size+=len(chunk)
                    if name not in large: parts.append(chunk)
                if size!=ident['bytes'] or digest.hexdigest()!=ident['sha256']: raise RuntimeError('Artifact identity mismatch')
                if name not in large:
                    dest=OUT/(name+'.gz');dest.parent.mkdir(parents=True,exist_ok=True)
                    dest.write_bytes(gzip.compress(b''.join(parts),mtime=0))
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
