"""Root-frozen, one-original On-Demand Cargo pilot. No launch by default.

CLI: --self-check | --preflight CONFIG SHA ARCHIVE ARCHIVE_SHA COMMIT |
     --launch aNNNN CONFIG SHA ARCHIVE ARCHIVE_SHA COMMIT.
Remote modes are emitted by user_data; no Git is needed on the worker.
The external archive pointer avoids a config/archive self-hash cycle.
"""
import contextlib
import fcntl
import gzip
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tarfile
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('pilot requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_workspace_execution_spot as workspace

shared, peer, startup = workspace.shared, workspace.peer, workspace.startup
worker = workspace.worker
sha, source_hashes, source_identity = worker.sha, worker.source_hashes, worker.source_identity
encoded, write, artifact = workspace.encoded, worker._write, worker.artifact
SCHEMA = 'borsuk-native-cargo-pilot-v1'
NATIVE_IDENTITY = 'addf62bce23ceee034f22e4d1dfc0b318564d924c6e0fcee34b9336d1532813e'
MODULE = 'scripts.launch_native_cargo_pilot'
TEST = 'two_bit_generation::source_walk_tests::fragmented_paged_source_preserves_trace_and_rank_across_get_caps'
COMMAND = ['cargo', 'test', '--locked', '-p', 'borsuk', '--lib', TEST, '--', '--exact']
ENVIRONMENT = dict(worker.ENVIRONMENT, CARGO_INCREMENTAL='0', OMP_NUM_THREADS='2',
                   OPENBLAS_NUM_THREADS='2', MKL_NUM_THREADS='2')
FIXED = dict(schema=SCHEMA, architecture='x86_64', region=peer.REGION, bucket=peer.BUCKET,
    instance_type='c7i.2xlarge', market='on-demand', runtime_os='ubuntu-24.04', toolchain='1.98.0',
    root_device_name='/dev/sda1', memory_bytes=8*1024**3, swap_bytes=0,
    cpu_quota_percent=200, tasks_max=512, command=COMMAND, environment=ENVIRONMENT,
    automatic_replacement=False, full_workspace_execution=False, scientific_measurement=False)
CODE = (*workspace.CODE, 'scripts/launch_native_cargo_pilot.py', 'scripts/check_native_cargo_pilot.py')
ARTIFACTS = ('config.json', 'native-source-manifest.json', 'source-qualification.json',
    'source-before.json', 'source-after.json', 'pilot-receipt.json', 'test.log',
    'test-resources.txt', 'workspace-cgroup.json', 'cpu.txt', 'rustc-version.txt',
    'cargo-version.txt', 'cache.json', 'volume.json', 'timings.jsonl', 'resource-samples.jsonl', 'run-closed.log')
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'campaign_schema',
    'source_identity_sha256', 'source_file_count', 'artifact_roster_sha256', 'awscli_version', 'awscli_sha256')
MOUNT = Path('/mnt/cargo-pilot-cache')


def relative(value):
    path = Path(value)
    assert not path.is_absolute() and '..' not in path.parts and str(path) == value
    assert re.fullmatch(r'[A-Za-z0-9_./-]+', value), 'safe relative path'
    return path


def finite(value):
    assert type(value) in (int, float) and math.isfinite(value) and value > 0
    return value


def validate_config(c):
    assert all(type(c[k]) is type(v) and c[k] == v for k, v in FIXED.items()), 'fixed pilot protocol'
    assert c['controller_authority_pending'] is False
    for key in ('code_freeze_ns', 'price_observed_ns'):
        assert type(c[key]) is int and c[key] > 0
    assert c['price_evidence'] and isinstance(c['price_evidence'], str)
    assert re.fullmatch(r'\d+\.\d+\.\d+', c['toolchain'])
    assert c['target'] == 'x86_64-unknown-linux-gnu'
    wall, test = c['machine_limit_seconds'], c['test_limit_seconds']
    assert type(wall) is type(test) is int and 1200 <= wall <= 3600 and 1 <= test <= wall-900
    assert finite(c['on_demand_usd_per_hour'])*wall/3600 <= finite(c['compute_cap_usd']) <= .50
    assert type(c['root_gib']) is int and 16 <= c['root_gib'] <= 80
    assert type(c['storage_ttl_seconds']) is int and wall <= c['storage_ttl_seconds'] <= 86400
    cache = c['cache']
    assert set(cache) == {'volume_id','owned_volume','size_gib','availability_zone','expires_ns'}
    assert cache['volume_id'] is None or re.fullmatch(r'vol-[0-9a-f]{8,17}', cache['volume_id'])
    assert re.fullmatch(r'[a-zA-Z0-9_-]{8,64}', cache['owned_volume']), 'explicit ownedVolume proof'
    assert type(cache['size_gib']) is int and 1 <= cache['size_gib'] <= 128
    assert type(cache['expires_ns']) is int and cache['expires_ns'] > c['code_freeze_ns']
    assert cache['expires_ns'] <= c['code_freeze_ns'] + c['storage_ttl_seconds']*10**9
    storage = finite(c['gp3_gib_month_usd'])*(cache['size_gib']*c['storage_ttl_seconds']+c['root_gib']*wall)/(30*86400)
    assert storage <= finite(c['storage_cap_usd']) <= 2, 'separate storage cap'
    assert cache['availability_zone'] == c['availability_zone']
    assert re.fullmatch(r'eu-central-1[a-z]', c['availability_zone'])
    for field, pattern in (('image_id',r'ami-[0-9a-f]{8,17}'), ('subnet_id',r'subnet-[0-9a-f]{8,17}'),
                           ('security_group',r'sg-[0-9a-f]{8,17}')):
        assert re.fullmatch(pattern, c[field])
    assert c['instance_profile_arn'] == peer.PROFILE_ARN
    relative(c['output_root']); relative(c['s3_prefix'])
    assert c['s3_prefix'].startswith('research/') and c['s3_prefix'].endswith('-')


def qualify(repo, config_path, config_sha):
    repo, config_path = Path(repo), relative(str(config_path))
    body = (repo/config_path).read_bytes()
    assert sha(body) == config_sha, 'config drift'
    c = json.loads(body)
    validate_config(c)
    code = c['controller_code_sha256']
    assert set(code) == set(CODE), 'exact controller roster'
    assert all(artifact(repo/name)['sha256'] == digest for name, digest in code.items()), 'controller drift'
    pointer = c['native_source_manifest']
    path = relative(pointer['path'])
    assert artifact(repo/path) == {k:pointer[k] for k in ('bytes','sha256')}, 'manifest drift'
    manifest = json.loads((repo/path).read_bytes())
    native = source_hashes(repo)
    assert native == manifest['source_sha256'], 'source drift'
    assert len(native) == manifest['source_file_count'] == 399
    assert source_identity(native) == manifest['source_identity_sha256'] == NATIVE_IDENTITY
    assert re.fullmatch('[0-9a-f]{40}', manifest['native_source_commit'])
    proof = dict(config_path=str(config_path), config_sha256=config_sha, campaign_schema=SCHEMA,
        source_sha256=native, source_identity_sha256=source_identity(native), source_file_count=399,
        native_source_manifest=pointer, native_source_commit=manifest['native_source_commit'],
        code_sha256=code, code_identity_sha256=sha(encoded(code)), artifact_roster_sha256=sha(encoded(ARTIFACTS)),
        awscli_version=workspace.semantic.AWSCLI_VERSION, awscli_sha256=workspace.semantic.AWSCLI_SHA256)
    return c, proof


def preflight(config_path, config_sha, archive, archive_sha, commit, repo=Path('.')):
    c, proof = qualify(repo, config_path, config_sha)
    assert re.fullmatch('[0-9a-f]{40}', commit)
    assert artifact(archive)['sha256'] == archive_sha, 'archive drift'
    expected = dict(proof['source_sha256'], **proof['code_sha256'])
    expected[str(config_path)] = config_sha
    expected[c['native_source_manifest']['path']] = c['native_source_manifest']['sha256']
    seen = set()
    with tarfile.open(archive, 'r:gz') as source:
        for member in source:
            name = str(relative(member.name.rstrip('/')))
            assert name not in seen, 'duplicate archive entry'
            seen.add(name)
            assert member.isdir() or member.isfile(), 'archive links/devices forbidden'
            if member.isfile():
                if name in expected:
                    assert sha(source.extractfile(member).read()) == expected[name], 'archive file drift: '+name
                elif name.endswith('.rs') or Path(name).name in ('Cargo.toml','Cargo.lock'):
                    raise AssertionError('unexpected native archive file: '+name)
    assert set(expected) <= seen, 'incomplete archive'
    proof.update(source_commit=commit, source_archive_sha256=archive_sha)
    return c, proof


def volume_tags(c):
    return [{'Key':'Name','Value':'borsuk-cargo-pilot-cache'},
            {'Key':'ownedVolume','Value':c['cache']['owned_volume']},
            {'Key':'expiresNs','Value':str(c['cache']['expires_ns'])}]


def validate_volume(volume, c):
    assert volume['Encrypted'] is True and volume['VolumeType'] == 'gp3'
    assert volume['Size'] == c['cache']['size_gib']
    assert volume['AvailabilityZone'] == c['availability_zone']
    assert volume['State'] == 'available' and volume['Attachments'] == [], 'cache is exclusively available'
    tags = {t['Key']:t['Value'] for t in volume['Tags']}
    assert all(tags.get(t['Key']) == t['Value'] for t in volume_tags(c)), 'ownedVolume/TTL proof'
    if c['cache']['volume_id']:
        assert volume['VolumeId'] == c['cache']['volume_id']


def prepare_volume(ec2, c, out):
    volume_id = c['cache']['volume_id']
    created = volume_id is None
    if created:
        v = ec2.create_volume(AvailabilityZone=c['availability_zone'], Encrypted=True,
            VolumeType='gp3', Size=c['cache']['size_gib'], Iops=3000, Throughput=125,
            ClientToken='cargo-cache-'+sha(encoded(c['cache']))[:48],
            TagSpecifications=[dict(ResourceType='volume', Tags=volume_tags(c))])
        volume_id = v['VolumeId']
        write(out/'aws-created-volume.json', dict(volume_id=volume_id, ownedVolume=c['cache']['owned_volume'],
              expires_ns=c['cache']['expires_ns'], deletion_owner='root', auto_delete=False))
    ec2.get_waiter('volume_available').wait(VolumeIds=[volume_id], WaiterConfig={'Delay':5,'MaxAttempts':24})
    v = ec2.describe_volumes(VolumeIds=[volume_id])['Volumes']
    assert len(v) == 1
    validate_volume(v[0], c)
    proof = dict(volume_id=volume_id, created=created, ownedVolume=c['cache']['owned_volume'],
                 size_gib=c['cache']['size_gib'], expires_ns=c['cache']['expires_ns'],
                 availability_zone=c['availability_zone'], encrypted=True, auto_delete=False)
    write(out/'aws-volume.json', proof)
    return proof


def launch_request(c, attempt, body):
    assert re.fullmatch(r'a[0-9]{4}', attempt)
    prefix = c['s3_prefix']+attempt
    return dict(ClientToken='cargo-pilot-'+sha(prefix.encode())[:48], ImageId=c['image_id'],
        InstanceType=c['instance_type'], MinCount=1, MaxCount=1,
        IamInstanceProfile={'Arn':c['instance_profile_arn']},
        NetworkInterfaces=[dict(AssociatePublicIpAddress=True, DeviceIndex=0,
            Groups=[c['security_group']], SubnetId=c['subnet_id'])],
        InstanceInitiatedShutdownBehavior='terminate', MetadataOptions={'HttpTokens':'required'},
        BlockDeviceMappings=[dict(DeviceName=c['root_device_name'],
            Ebs=dict(DeleteOnTermination=True, Encrypted=True, VolumeSize=c['root_gib'], VolumeType='gp3'))],
        TagSpecifications=[dict(ResourceType='instance',Tags=[dict(Key='Name',Value='borsuk-cargo-pilot')])],
        UserData=body)


def poll(ec2, s3, prefix, instance_id, started, wall):
    # Existing observer adds 300s; reserve that within the root's machine cap.
    with patch.object(shared, 'WALL', wall-600):
        return shared.poll(ec2, s3, prefix, instance_id, started)


@contextlib.contextmanager
def interruptible():
    old = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, old)


def execute_launch(ec2, s3, c, proof, attempt, out, prefix, body, volume):
    nodes, started = {}, time.monotonic()
    try:
        write(out/'aws-request.json', dict(launch_start_ns=time.time_ns(), request=launch_request(c, attempt, body)))
        receipt = ec2.run_instances(**launch_request(c, attempt, body))
        # All ACKed IDs enter ownership before any persistence/validation can fail.
        nodes.update({str(i):dict(instance_id=row['InstanceId']) for i,row in enumerate(receipt['Instances'])})
        write(out/'aws-launch.json', dict(nodes=nodes, launch_ack_ns=time.time_ns(), prefix=prefix))
        assert len(nodes) == 1, 'one original instance; unexpected ACK IDs still owned'
        instance_id = nodes['0']['instance_id']
        if volume:
            ec2.get_waiter('instance_running').wait(InstanceIds=[instance_id], WaiterConfig={'Delay':5,'MaxAttempts':24})
            ec2.attach_volume(VolumeId=volume['volume_id'], InstanceId=instance_id, Device='/dev/sdf')
            ec2.get_waiter('volume_in_use').wait(VolumeIds=[volume['volume_id']], WaiterConfig={'Delay':5,'MaxAttempts':24})
            ec2.modify_instance_attribute(InstanceId=instance_id, BlockDeviceMappings=[
                dict(DeviceName='/dev/sdf', Ebs={'DeleteOnTermination':False})])
        poll(ec2, s3, prefix, instance_id, started, c['machine_limit_seconds'])
    finally:
        failure = sys.exc_info()[0] is not None
        # A second ordinary signal must not skip termination/wait/receipt.
        old = {s:signal.signal(s, signal.SIG_IGN) for s in (signal.SIGINT, signal.SIGTERM)}
        try:
            startup.terminate_owned(ec2, nodes)
            if volume:
                ec2.get_waiter('volume_available').wait(VolumeIds=[volume['volume_id']], WaiterConfig={'Delay':5,'MaxAttempts':24})
                observed = ec2.describe_volumes(VolumeIds=[volume['volume_id']])['Volumes']
                assert len(observed) == 1 and observed[0]['VolumeId'] == volume['volume_id']
                validate_volume(observed[0], c)
            write(out/'aws-closeout.json', dict(nodes=nodes, state='terminated', terminated_ns=time.time_ns(),
                  observed_elapsed_s=time.monotonic()-started, cache_volume=volume, cache_retained=True))
            if len(nodes) == 1:
                try:
                    collect(s3, prefix, out, nodes['0']['instance_id'], proof)
                except Exception as error:
                    write(out/'collection-error.json', dict(error_type=type(error).__name__))
                    if not failure:
                        raise
        finally:
            for s, handler in old.items():
                signal.signal(s, handler)


def user_data(c, proof, prefix, volume):
    adapter = dict(proof, awscli_version=workspace.semantic.AWSCLI_VERSION,
                   awscli_sha256=workspace.semantic.AWSCLI_SHA256,
                   native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    archive_key = 'research/native-library-check/sources/'+proof['source_archive_sha256']+'.tar.gz'
    with patch.multiple(workspace, WALL=c['machine_limit_seconds']-60, SCHEMA=SCHEMA,
            CONFIG=Path(proof['config_path']), PREFIX=c['s3_prefix'], ARTIFACTS=ARTIFACTS,
            TERMINAL_IDENTITIES=TERMINAL_IDENTITIES):
        body = workspace.user_data(proof['source_commit'], proof['source_archive_sha256'], archive_key, prefix, adapter)
    quote = shlex.quote
    args = quote(proof['config_path'])+' '+quote(proof['config_sha256'])
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain {quote(c['toolchain'])} --default-host {quote(c['target'])}
cat >volume.json <<'VOLUME'
{encoded(volume).decode()}
VOLUME
phase=cache-preparation
PYTHONPATH="$root/repo" timeout --kill-after=5 180 python3.12 -m {MODULE} --mount "$root/volume.json"
phase=execution
systemd-run --unit=native-cargo-pilot --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec={c['test_limit_seconds']+120} -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=PATH="$PATH" \\
 python3.12 -m {MODULE} --worker "$root/repo" "$root" {args}
phase=cache-close
sync -f {MOUNT}
umount {MOUNT}
phase=receipt-qualification
PYTHONPATH="$root/repo" python3.12 -m {MODULE} --check-receipt "$root"
'''
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start]+command+body[end:]
    body = body.replace('/mnt/native-workspace-execution','/mnt/native-cargo-pilot')
    body = body.replace('native-semantic-router-cold-stop','native-cargo-pilot-stop')
    # Timestamp every bootstrap phase, including before source download/setup.
    body = body.replace('phase=bootstrap\n', '''stamp() { printf '{"phase":"%s","time_ns":%s}\\n' "$1" "$(date +%s%N)" >>timings.jsonl; }
phase=bootstrap
stamp userdata-start
printf '{"phase":"boot","time_ns":%s000000000}\\n' "$(awk '$1 == \"btime\" { print $2 }' /proc/stat)" >>timings.jsonl
''', 1)
    body = re.sub(r'^(phase=[a-z-]+)$', r'\1\nstamp "$phase"', body, flags=re.M)
    body = body.replace('phase=install\n', 'stamp download-end\nphase=install\n', 1)
    body = body.replace('phase=cache-preparation\n', 'stamp setup-end\nphase=cache-preparation\n', 1)
    body = body.replace('phase=execution\n', 'stamp cache-mount-end\nphase=execution\n', 1)
    # Cleanup cache even on a failed worker. A failed unmount cannot become PASS.
    body = body.replace('  cp run.log run-closed.log || code=96',
        f'  if mountpoint -q {MOUNT}; then sync -f {MOUNT}; umount {MOUNT} || code=96; fi\n  stamp worker-terminal\n  cp run.log run-closed.log || code=96')
    subprocess.run(['bash','-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    terminal = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
    compile(terminal, '<terminal>', 'exec')
    return body


def mount_cache(volume):
    assert re.fullmatch(r'vol-[0-9a-f]{8,17}', volume['volume_id'])
    assert volume['encrypted'] is True and volume['auto_delete'] is False
    device = Path('/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_'+volume['volume_id'].replace('-',''))
    deadline = time.monotonic()+150
    while not device.exists():
        assert time.monotonic() < deadline, 'cache device attachment deadline'
        time.sleep(1)
    assert not MOUNT.is_symlink() and not os.path.ismount(MOUNT), 'cache mount already occupied'
    MOUNT.mkdir(exist_ok=True)
    assert not any(MOUNT.iterdir()), 'mountpoint must be empty'
    probe = subprocess.run(['blkid','-s','TYPE','-o','value',str(device)], capture_output=True, text=True)
    if probe.returncode == 2 and volume['created'] is True:
        subprocess.run(['mkfs.ext4','-q',str(device)], check=True, timeout=120)
    else:
        assert probe.returncode == 0 and probe.stdout.strip() == 'ext4', 'existing cache must be ext4; never format'
    subprocess.run(['mount','-o','nodev,nosuid',str(device),str(MOUNT)], check=True)
    mounted_cache(volume)


def mounted_cache(volume):
    assert not MOUNT.is_symlink() and os.path.ismount(MOUNT), 'cache mountpoint guard'
    report = json.loads(subprocess.check_output(['findmnt','-J','-M',str(MOUNT),'-o','SOURCE,FSTYPE,TARGET'], text=True))
    rows = report['filesystems']
    assert len(rows) == 1 and rows[0]['target'] == str(MOUNT) and rows[0]['fstype'] == 'ext4'
    serial = subprocess.check_output(['lsblk','-dn','-o','SERIAL',rows[0]['source']], text=True).strip()
    assert serial == volume['volume_id'].replace('-',''), 'cache volume device identity'


@contextlib.contextmanager
def cache_lock(volume):
    mounted_cache(volume)
    fd = os.open(MOUNT/'.pilot.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def cache_identity(c, proof):
    return dict(toolchain=c['toolchain'], target=c['target'], environment=ENVIRONMENT,
                lock_sha256=proof['source_sha256']['Cargo.lock'])


def cache_state(c, proof, out, repo):
    identity = cache_identity(c, proof)
    root = MOUNT/sha(encoded(identity))
    assert not root.is_symlink()
    root.mkdir(exist_ok=True)
    for name in ('cargo','target'):
        path = root/name
        assert not path.is_symlink()
        path.mkdir(exist_ok=True)
    for path in (root/'cargo/config', root/'cargo/config.toml'):
        assert not path.exists(), 'cache must not inject Cargo configuration'
    # Git archive mtimes can precede reused Cargo fingerprints. Freshen only
    # authenticated workspace inputs under the exclusive volume lock; no byte edits.
    touched_ns = time.time_ns()
    for name, digest in proof['source_sha256'].items():
        path = repo/relative(name)
        assert not path.is_symlink() and artifact(path)['sha256'] == digest
        os.utime(path, ns=(touched_ns,touched_ns))
    state = dict(identity=identity, source_identity_sha256=proof['source_identity_sha256'],
        source_touched_ns=touched_ns, source_touched_count=len(proof['source_sha256']),
        namespace=str(root), registry_populated=(root/'cargo/registry').exists(),
        target_populated=any((root/'target').iterdir()), binary_provenance_inferred=False)
    write(out/'cache.json', state)
    return state


def test_result(log):
    assert re.findall(r'^running (\d+) tests?$', log, re.M) == ['1'], 'exactly one executed test'
    assert re.findall(r'^test (\S+) \.\.\. ok$', log, re.M) == [TEST], 'required parity test actually passed'
    rows = re.findall(r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; \d+ filtered out; finished in .+$', log, re.M)
    assert rows == [('1','0','0','0')], 'one complete successful test summary'
    return dict(executed=1, passed=1, failed=0, ignored=0)


def capture_resources():
    result = worker.capture_cgroup()
    result['time_ns'] = time.time_ns()
    result['memory.pressure'] = (Path(result['cgroup'])/'memory.pressure').read_text()
    result['host_memory_pressure'] = Path('/proc/pressure/memory').read_text()
    return result


def sample_summary(samples):
    assert samples, 'resource samples missing'
    summary = dict(sample_count=len(samples),
        kernel_memory_peak_bytes=max(int(s['memory.peak']) for s in samples),
        kernel_swap_peak_bytes=max(int(s['memory.swap.peak']) for s in samples))
    for field, prefix in (('host_memory_pressure','host'),('memory.pressure','cgroup')):
        for kind in ('some','full'):
            values = [float(re.search(r'^'+kind+r' avg10=([0-9.]+)',s[field],re.M)[1]) for s in samples]
            assert all(math.isfinite(v) and 0 <= v <= 100 for v in values)
            summary[prefix+'_'+kind+'_avg10_max'] = max(values)
    return summary


def validate_resources(report):
    worker.validate_cgroup(report)
    for part in ('before','after'):
        for field in ('memory.pressure','host_memory_pressure'):
            assert all(word in report[part][field] for word in ('some avg10=', 'full avg10=')), 'PSI evidence'


def environment(c, state, out):
    return dict(ENVIRONMENT, HOME=str(out/'home'), LANG='C.UTF-8',
        PATH=str(out/'.cargo/bin')+':/usr/bin:/bin', RUSTUP_HOME=str(out/'.rustup'),
        RUSTUP_TOOLCHAIN=c['toolchain'], CARGO_BUILD_TARGET=c['target'],
        CARGO_HOME=str(Path(state['namespace'])/'cargo'), CARGO_TARGET_DIR=str(Path(state['namespace'])/'target'))


def execute_worker(repo, out, config_path, config_sha):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    assert not out.is_relative_to(repo)
    c, proof = qualify(repo, config_path, config_sha)
    for name, body in (('config.json',(repo/config_path).read_bytes()),
        ('native-source-manifest.json',(repo/c['native_source_manifest']['path']).read_bytes()),
        ('source-qualification.json',encoded(proof))):
        with (out/name).open('xb') as handle:
            handle.write(body); handle.flush(); os.fsync(handle.fileno())
    volume = json.loads((out/'volume.json').read_text())
    assert volume['ownedVolume'] == c['cache']['owned_volume'] and volume['size_gib'] == c['cache']['size_gib']
    if c['cache']['volume_id']:
        assert volume['volume_id'] == c['cache']['volume_id'] and volume['created'] is False
    before = source_hashes(repo)
    write(out/'source-before.json', before)
    result = dict(schema=SCHEMA, qualified=False, config_sha256=config_sha, command=COMMAND,
        exit_status=None, command_started=False, command_completed=False, tests=None,
        code_freeze_ns=c['code_freeze_ns'], start_ns=time.time_ns(), full_workspace_execution=False,
        binary_provenance_inferred=False)
    resources = dict(closed=False)
    try:
        with cache_lock(volume):
            state = cache_state(c, proof, out, repo)
            env = environment(c, state, out)
            result['environment'] = env
            result['cache_prepared_ns'] = time.time_ns()
            resources['before'] = capture_resources()
            validate_resources(dict(before=resources['before'], after=resources['before'], closed=True))
            for name, cmd in (('rustc-version.txt',['rustc','-vV']), ('cargo-version.txt',['cargo','-V']), ('cpu.txt',['lscpu'])):
                body = subprocess.check_output(cmd, env=env, timeout=30)
                (out/name).write_bytes(body)
            assert f'release: {c["toolchain"]}\n' in (out/'rustc-version.txt').read_text()
            assert f'host: {c["target"]}\n' in (out/'rustc-version.txt').read_text()
            assert (out/'cargo-version.txt').read_text().startswith('cargo '+c['toolchain']+' ')
            result['build_start_ns'] = time.time_ns()
            with (out/'test.log').open('xb') as log:
                result['command_started'] = True
                proc = subprocess.Popen(['/usr/bin/time','-v','-o',str(out/'test-resources.txt'),
                    'timeout','--signal=TERM','--kill-after=30',str(c['test_limit_seconds']),*COMMAND],
                    cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                samples = []
                try:
                    deadline = time.monotonic()+c['test_limit_seconds']+40
                    with (out/'resource-samples.jsonl').open('xb') as stream:
                        for _ in range(c['test_limit_seconds']+41):
                            sample = capture_resources()
                            samples.append(sample)
                            stream.write(encoded(sample)+b'\n'); stream.flush()
                            remaining = deadline-time.monotonic()
                            assert remaining > 0, 'Cargo wrapper deadline'
                            try:
                                proc.wait(timeout=min(1,remaining))
                                break
                            except subprocess.TimeoutExpired:
                                pass
                        else:
                            raise TimeoutError('resource sample bound')
                        os.fsync(stream.fileno())
                finally:
                    # Kill the entire command group even when Cargo/the wrapper died first.
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    proc.wait()
                result['exit_status'] = proc.returncode
                result['command_completed'] = resources['closed'] = True
                log.flush(); os.fsync(log.fileno())
            result['build_end_ns'] = time.time_ns()
            resources['after'] = capture_resources()
            validate_resources(resources)
            result['observed_resources'] = sample_summary(samples+[resources['after']])
            assert proc.returncode == 0, 'Cargo did not exit successfully'
            result['tests'] = test_result((out/'test.log').read_text())
            assert source_hashes(repo) == before == proof['source_sha256']
            assert qualify(repo, config_path, config_sha) == (c, proof)
            result['qualified'] = True
    except BaseException as error:
        result['error_type'] = type(error).__name__
    finally:
        write(out/'source-after.json', source_hashes(repo))
        write(out/'workspace-cgroup.json', resources)
        result['end_ns'] = time.time_ns()
        write(out/'pilot-receipt.json', result)
    return 0 if result['qualified'] else 96


def validate_receipt(out, proof=None):
    out = Path(out)
    c = json.loads((out/'config.json').read_bytes())
    validate_config(c)
    saved = json.loads((out/'source-qualification.json').read_bytes())
    if proof:
        assert saved == {k:v for k,v in proof.items() if k not in ('source_commit','source_archive_sha256')}
    proof = saved
    assert artifact(out/'config.json')['sha256'] == proof['config_sha256']
    assert artifact(out/'native-source-manifest.json') == {k:proof['native_source_manifest'][k] for k in ('bytes','sha256')}
    assert len(proof['source_sha256']) == proof['source_file_count'] == 399
    assert source_identity(proof['source_sha256']) == proof['source_identity_sha256'] == NATIVE_IDENTITY
    for name in ('source-before.json','source-after.json'):
        assert json.loads((out/name).read_bytes()) == proof['source_sha256'], 'source map drift'
    receipt = json.loads((out/'pilot-receipt.json').read_bytes())
    assert receipt['schema'] == SCHEMA and receipt['config_sha256'] == proof['config_sha256']
    assert receipt['code_freeze_ns'] == c['code_freeze_ns']
    assert receipt['qualified'] is receipt['command_started'] is receipt['command_completed'] is True
    assert type(receipt['exit_status']) is int and receipt['exit_status'] == 0
    assert receipt['command'] == COMMAND and receipt['tests'] == test_result((out/'test.log').read_text())
    assert receipt['full_workspace_execution'] is receipt['binary_provenance_inferred'] is False
    state = json.loads((out/'cache.json').read_bytes())
    assert receipt['environment'] == environment(c, state, Path('/mnt/native-cargo-pilot'))
    assert state['identity'] == cache_identity(c, proof)
    assert state['source_identity_sha256'] == proof['source_identity_sha256']
    assert state['source_touched_count'] == 399 and receipt['start_ns'] <= state['source_touched_ns'] <= receipt['cache_prepared_ns']
    assert state['namespace'] == str(MOUNT/sha(encoded(state['identity'])))
    assert state['binary_provenance_inferred'] is False
    assert f'release: {c["toolchain"]}\n' in (out/'rustc-version.txt').read_text()
    assert f'host: {c["target"]}\n' in (out/'rustc-version.txt').read_text()
    assert (out/'cargo-version.txt').read_text().startswith('cargo '+c['toolchain']+' ')
    assert type(state['registry_populated']) is type(state['target_populated']) is bool
    resources = json.loads((out/'workspace-cgroup.json').read_bytes())
    validate_resources(resources)
    samples = [json.loads(line) for line in (out/'resource-samples.jsonl').read_bytes().splitlines()]
    assert 1 <= len(samples) <= c['test_limit_seconds']+41
    assert receipt['observed_resources'] == sample_summary(samples+[resources['after']])
    assert receipt['observed_resources']['kernel_swap_peak_bytes'] == 0
    assert receipt['observed_resources']['kernel_memory_peak_bytes'] <= c['memory_bytes']
    timestamps = [receipt[k] for k in ('code_freeze_ns','start_ns','cache_prepared_ns','build_start_ns','build_end_ns','end_ns')]
    assert all(type(t) is int for t in timestamps) and timestamps == sorted(timestamps)
    assert receipt['build_end_ns']-receipt['build_start_ns'] <= (c['test_limit_seconds']+35)*10**9
    return receipt


def collect(s3, prefix, out, instance_id, proof):
    close = json.loads((out/'aws-closeout.json').read_text())
    assert close['state'] == 'terminated' and close['nodes'] == {'0':{'instance_id':instance_id}}
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    for key in (*TERMINAL_IDENTITIES,'source_commit','source_archive_sha256'):
        assert terminal[key] == proof[key], 'terminal authority: '+key
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    (out/'aws-terminal.json').write_bytes(raw)
    for name, identity in terminal['artifacts'].items():
        body = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/artifacts/'+name)['Body'].read()
        assert type(identity['bytes']) is int and identity['bytes'] > 0
        assert identity == dict(bytes=len(body),sha256=sha(body)), 'artifact authentication: '+name
        (out/name).write_bytes(body)
        (out/(name+'.gz')).write_bytes(gzip.compress(body, mtime=0))
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == terminal['original_exit_code'] == 0
    if complete:
        assert set(terminal['artifacts']) == set(ARTIFACTS)
        validate_receipt(out, proof)
        assert terminal['source_qualification_sha256'] == artifact(out/'source-qualification.json')['sha256']
        stamps = [json.loads(line) for line in (out/'timings.jsonl').read_bytes().splitlines()]
        phases = [row['phase'] for row in stamps]
        assert all(phases.count(name) == 1 for name in ('boot','userdata-start','source-download',
            'download-end','setup-end','cache-preparation','cache-mount-end','execution','cache-close','complete','worker-terminal'))
        assert all(type(row['time_ns']) is int and row['time_ns'] > 0 for row in stamps)
        assert json.loads((out/'volume.json').read_text()) == close['cache_volume']
    write(out/'collection-replay.json', dict(qualified=complete, terminal_sha256=sha(raw),
        collection_end_ns=time.time_ns(), full_workspace_execution=False))
    assert complete, 'pilot incomplete; no test qualification'
    return terminal


def launch(attempt, config_path, config_sha, archive, archive_sha, commit):
    assert re.fullmatch(r'a[0-9]{4}', attempt)
    c, proof = preflight(config_path, config_sha, archive, archive_sha, commit)
    now = time.time_ns()
    assert 0 <= now-c['code_freeze_ns'] <= 86400*10**9, 'stale/future freeze'
    assert 0 <= now-c['price_observed_ns'] <= 86400*10**9, 'stale/future price'
    assert c['cache']['expires_ns'] >= now+c['machine_limit_seconds']*10**9, 'cache TTL covers worker'
    out = Path(c['output_root'])/attempt
    out.mkdir(parents=True, exist_ok=False)
    prefix = c['s3_prefix']+attempt
    session = shared.boto3.Session(profile_name='causality', region_name=peer.REGION)
    ec2, s3 = session.client('ec2'), session.client('s3')
    assert peer.missing(s3,prefix+'/reservation.json') and peer.missing(s3,prefix+'/terminal.json')
    assert ec2.describe_subnets(SubnetIds=[c['subnet_id']])['Subnets'][0]['AvailabilityZone'] == c['availability_zone']
    active = ec2.describe_instances(Filters=[{'Name':'tag:Name','Values':['borsuk-*']},
        {'Name':'instance-state-name','Values':['pending','running','stopping']}])
    assert not any(r['Instances'] for r in active['Reservations']), 'original pilot still active'
    write(out/'aws-reservation.json', dict(schema=SCHEMA, qualification=proof, config=c,
        launch_start_ns=now, no_replacement=True, total_cost_measured=False))
    peer.put_if_absent(prefix+'/reservation.json',(out/'aws-reservation.json').read_bytes())
    key = 'research/native-library-check/sources/'+archive_sha+'.tar.gz'
    body = Path(archive).read_bytes()
    assert sha(body) == archive_sha
    if peer.missing(s3,key):
        peer.put_if_absent(key,body)
    else:
        assert sha(s3.get_object(Bucket=peer.BUCKET,Key=key)['Body'].read()) == archive_sha
    volume = prepare_volume(ec2,c,out)
    data = user_data(c,proof,prefix,volume)
    (out/'aws-user-data.sh').write_text(data)
    with interruptible():
        execute_launch(ec2,s3,c,proof,attempt,out,prefix,data,volume)


def main(args):
    if not args:
        raise SystemExit(__doc__)
    if args == ['--self-check']:
        from scripts.check_native_cargo_pilot import main as checks
        return checks()
    if args[0] == '--preflight':
        print(json.dumps(preflight(*args[1:])[1],sort_keys=True)); return 0
    if args[0] == '--launch':
        launch(*args[1:]); return 0
    if args[0] == '--mount':
        mount_cache(json.loads(Path(args[1]).read_text())); return 0
    if args[0] == '--worker':
        return execute_worker(*args[1:])
    if args[0] == '--check-receipt':
        validate_receipt(args[1]); return 0
    raise SystemExit(__doc__)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
