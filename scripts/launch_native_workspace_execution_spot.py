"""Assurance-only Spot adapter; root freezes authority and owns paid launch.

Config contract: FIXED plus controller_authority_pending=false,
controller_code_sha256={every CODE path:SHA256}, native_source_manifest=
{path,bytes,sha256}. No completed-assurance dependency. Optional leading
--semantic-1m selects the new root and manifest-pinned native identity.
--semantic-1m-test-build selects compile-only execution of the real test-build script.
CLI aNNNN | --self-check | --stage REPO OUT | --check-receipt OUT | --replay OUT.
"""
import contextlib
import copy
import fcntl
import gzip
import io
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import tempfile
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('qualification requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import check_native_workspace_execution as worker
from scripts import launch_native_semantic_router_cold_spot as semantic

shared, peer, startup = semantic.shared, semantic.peer, semantic.startup
SOURCE_IDENTITY = '714794a10c2d886f7a53a9926a4bb63e093cec16858676268e31833aeead2681'
ROOT = semantic.ROOT / 'metadata-waves/implementation-gates/remote-full'
CONFIG = ROOT / 'config.json'
NAME = ''
SCHEMA = 'borsuk-native-workspace-execution-spot-v1'
CONFIG_SCHEMA = 'borsuk-native-workspace-execution-v1'
PREFIX = 'research/semantic-router/20261001/metadata-waves-workspace-'
TOKEN_PREFIX = 'metadata-waves-workspace-'
TAG = 'borsuk-metadata-waves-workspace'
SEMANTIC_1M = False
TEST_BUILD = False
RECEIPT_SCHEMA = 'borsuk-native-workspace-execution-receipt-v1'
WALL = 9000
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', semantic.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = semantic.ROOT_DEVICE_NAME, 'subnet-034528fbd6977848f'
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, 1.25
MODULE = 'scripts.launch_native_workspace_execution_spot'
CODE = tuple('scripts/' + name + '.py' for name in (
    'check_native_metadata_ranges_stats', 'check_native_semantic_router_stats',
    'check_native_startup_build', 'check_native_startup_stats', 'check_native_workspace_execution',
    'launch_native_metadata_ranges_cold_spot', 'launch_native_peer_1m_spot',
    'launch_native_semantic_router_cold_spot', 'launch_native_startup_profile_spot',
    'launch_native_workspace_execution_spot', 'launch_v157_primary_feasibility_spot',
    'launch_v174_relaid_bind_compile_spot', 'package_semantic_native_generation',
    'prepare_native_semantic_publication', 'rest_coexistence_load', 'run_native_cold_first_query',
    'run_native_metadata_ranges_cold', 'run_native_peer_1m_worker', 'run_native_peer_offered_http',
    'run_native_semantic_router_cold', 'run_native_union_http', 'run_native_union_offered_http'))
ARTIFACTS = ('source-qualification.json', 'config.json', 'native-source-manifest.json',
    'source-before.json', 'source-after.json', 'workspace-receipt.json', 'test.log',
    'test-resources.txt', 'workspace-cgroup.json', 'cpu.txt', 'rustc-version.txt',
    'cargo-version.txt', 'run-closed.log')
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'campaign_schema',
    'source_identity_sha256', 'source_file_count', 'native_source_manifest_sha256',
    'native_source_commit', 'artifact_roster_sha256', 'awscli_version', 'awscli_sha256')
FIXED = dict(schema=CONFIG_SCHEMA, architecture='x86_64', region=peer.REGION, bucket=peer.BUCKET,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, compute_cap_usd=COMPUTE_CAP,
    ebs_s3_allowance_usd=.15, memory_bytes=worker.MEMORY, swap_bytes=0, cpu_quota_percent=200,
    tasks_max=512, test_limit_seconds=worker.TEST_SECONDS, service_limit_seconds=worker.SERVICE_SECONDS,
    machine_limit_seconds=WALL, command=list(worker.COMMAND), environment=worker.ENVIRONMENT)
FULL_CODE, FULL_FIXED = CODE, FIXED


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def configure(semantic_1m=False, *, test_build=False):
    """Select the protocol explicitly in every controller/worker process."""
    global SEMANTIC_1M, TEST_BUILD, ROOT, CONFIG, PREFIX, TOKEN_PREFIX, TAG
    global SCHEMA, CONFIG_SCHEMA, RECEIPT_SCHEMA, CODE, FIXED
    assert type(semantic_1m) is type(test_build) is bool
    assert not test_build or semantic_1m, 'test-build requires explicit semantic-1m mode'
    SEMANTIC_1M = semantic_1m
    TEST_BUILD = test_build
    ROOT = (semantic.ROOT.parent/'semantic-1m' if semantic_1m else semantic.ROOT/'metadata-waves') / ('implementation-gates/remote-test-build' if test_build else 'implementation-gates/remote-full')
    CONFIG = ROOT/'config.json'
    TOKEN_PREFIX = ('semantic-1m-test-build-' if test_build else
                    ('semantic-1m' if semantic_1m else 'metadata-waves') + '-workspace-')
    PREFIX = 'research/semantic-router/20261001/' + TOKEN_PREFIX
    TAG = 'borsuk-' + TOKEN_PREFIX.rstrip('-')
    SCHEMA = 'borsuk-native-workspace-test-build-spot-v1' if test_build else 'borsuk-native-workspace-execution-spot-v1'
    CONFIG_SCHEMA = 'borsuk-native-workspace-test-build-v1' if test_build else FULL_FIXED['schema']
    RECEIPT_SCHEMA = 'borsuk-native-workspace-test-build-receipt-v1' if test_build else 'borsuk-native-workspace-execution-receipt-v1'
    CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh') if test_build else FULL_CODE
    FIXED = dict(FULL_FIXED, schema=CONFIG_SCHEMA)
    if test_build:
        FIXED.update(execution_kind='workspace-test-build', command=['bash', 'scripts/check_rust_test_build.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None))


@contextlib.contextmanager
def execution_mode(semantic_1m=False, *, test_build=False):
    """Restore the caller's protocol after a worker or synthetic check."""
    previous = SEMANTIC_1M, TEST_BUILD
    configure(semantic_1m, test_build=test_build)
    try:
        yield
    finally:
        configure(previous[0], test_build=previous[1])


def mode_flag():
    return ' --semantic-1m-test-build' if TEST_BUILD else ' --semantic-1m' if SEMANTIC_1M else ''


def qualify(base=Path('.')):
    """Portable source/config/code qualification, without Git or completed assurance."""
    base = Path(base).resolve()
    body = (base/CONFIG).read_bytes()
    config = json.loads(body)
    assert config['controller_authority_pending'] is False, 'root authority freeze pending'
    assert all(type(config[k]) is type(v) and config[k] == v for k,v in FIXED.items()), 'fixed execution protocol'
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive code roster'
    assert all(worker.artifact(base/name)['sha256'] == digest for name,digest in code.items()), 'controller code drift'
    pointer = config['native_source_manifest']
    path = Path(pointer['path'])
    assert not path.is_absolute() and '..' not in path.parts and str(path) == pointer['path']
    assert type(pointer['bytes']) is int and pointer['bytes'] > 0
    assert worker.artifact(base/path) == {key:pointer[key] for key in ('bytes','sha256')}, 'manifest authority'
    manifest = json.loads((base/path).read_bytes())
    inventory = worker.source_hashes(base)
    assert type(manifest['source_file_count']) is int and manifest['source_file_count'] == len(inventory) == 399
    assert manifest['source_sha256'] == inventory, 'full native source drift'
    identity = worker.source_identity(inventory)
    assert identity == manifest['source_identity_sha256']
    assert SEMANTIC_1M or identity == SOURCE_IDENTITY, 'historical native source identity'
    assert re.fullmatch('[0-9a-f]{40}', manifest['native_source_commit'])
    proof = dict(schema='borsuk-native-workspace-test-build-qualification-v1' if TEST_BUILD else 'borsuk-native-workspace-execution-qualification-v1',
        config_path=str(CONFIG), config_sha256=worker.sha(body), campaign_schema=SCHEMA,
        source_sha256=inventory, source_identity_sha256=identity, source_file_count=399,
        native_source_commit=manifest['native_source_commit'], native_source_manifest=pointer,
        native_source_manifest_sha256=pointer['sha256'], code_sha256=code,
        code_identity_sha256=worker.sha(encoded(code)), artifact_roster_sha256=worker.sha(encoded(ARTIFACTS)),
        command=list(FIXED['command']), environment=dict(FIXED['environment']),
        actual_full_workspace_execution=False, awscli_version=semantic.AWSCLI_VERSION,
        awscli_sha256=semantic.AWSCLI_SHA256)
    if TEST_BUILD:
        proof.update(execution_kind=FIXED['execution_kind'], actual_workspace_test_build=False)
    return proof


def preflight(base=Path('.')):
    assert not subprocess.check_output(['git','status','--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    assert subprocess.check_output(['git','for-each-ref','--contains=HEAD','--format=%(refname)',
        'refs/remotes/origin/'], cwd=base, text=True).strip(), 'source commit not on an origin ref'
    return qualify(base)


def stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    proof = qualify(repo)
    for name, body in (('source-qualification.json', encoded(proof)+b'\n'),
                       ('config.json', (repo/CONFIG).read_bytes()),
                       ('native-source-manifest.json', (repo/proof['native_source_manifest']['path']).read_bytes())):
        with (out/name).open('xb') as output:
            output.write(body); output.flush(); os.fsync(output.fileno())
    return proof


def validate_receipt(out, proof):
    out = Path(out)
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA, 'receipt mode'
    receipt = json.loads((out/'workspace-receipt.json').read_bytes())
    assert receipt['schema'] == RECEIPT_SCHEMA
    assert type(receipt['exit_status']) is int and receipt['exit_status'] == 0
    assert type(receipt['gate_status']) is int and receipt['gate_status'] == 0
    assert receipt['qualified'] is receipt['command_started'] is receipt['command_completed'] is True
    assert receipt['source_unchanged'] is True
    assert proof['command'] == FIXED['command'] and proof['environment'] == FIXED['environment'], 'proof protocol'
    if TEST_BUILD:
        assert proof['execution_kind'] == receipt['execution_kind'] == FIXED['execution_kind']
        assert receipt['actual_full_workspace_execution'] is False
        assert receipt['command'] == FIXED['command'], 'exact test-build script command'
    else:
        assert receipt['command'][1:] == list(worker.COMMAND[1:]), 'exact full command'
    assert isinstance(receipt['command'][0], str) and receipt['command'][0]
    assert receipt['environment'] == FIXED['environment']
    assert receipt['source_sha256'] == proof['source_sha256']
    assert receipt['source_identity_sha256'] == worker.source_identity(receipt['source_sha256']) == proof['source_identity_sha256']
    assert SEMANTIC_1M or proof['source_identity_sha256'] == SOURCE_IDENTITY
    assert type(receipt['source_file_count']) is int and receipt['source_file_count'] == 399
    for key in ('config_sha256','code_identity_sha256','campaign_schema','artifact_roster_sha256'):
        assert receipt[key] == proof[key], 'receipt ' + key
    assert receipt['qualification_sha256'] == worker.artifact(out/'source-qualification.json')['sha256']
    expected = set(ARTIFACTS) - {'workspace-receipt.json','run-closed.log'}
    assert set(receipt['artifacts']) == expected
    for name, identity in receipt['artifacts'].items():
        assert worker.artifact(out/name) == identity, 'receipt artifact: ' + name
        assert type(identity['bytes']) is int and identity['bytes'] > 0
    assert worker.artifact(out/'config.json')['sha256'] == proof['config_sha256']
    assert worker.artifact(out/'native-source-manifest.json')['sha256'] == proof['native_source_manifest_sha256']
    for name in ('source-before.json','source-after.json'):
        assert json.loads((out/name).read_bytes()) == proof['source_sha256']
    worker.validate_cgroup(json.loads((out/'workspace-cgroup.json').read_bytes()))
    return receipt


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix)
    assert qualification['campaign_schema'] == SCHEMA and qualification['config_path'] == str(CONFIG)
    # Keep only small terminal identities in the existing bootstrap. The full
    # 399-file map is regenerated from the authenticated archive on the worker.
    adapter = {key:qualification[key] for key in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    with patch.multiple(semantic, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(semantic, '_offered', return_value=False):
        body = semantic.user_data(commit, archive_sha, archive_key, prefix, adapter)
    flag = mode_flag()
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
phase=source-qualification
PYTHONPATH="$root/repo" python3.12 -m {MODULE}{flag} --stage "$root/repo" "$root"
phase=execution
systemd-run --unit=native-workspace-execution --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" \\
 python3.12 -m scripts.check_native_workspace_execution{flag} "$CARGO_HOME/bin/cargo" "$root/repo" "$root"
phase=receipt-qualification
PYTHONPATH="$root/repo" python3.12 -m {MODULE}{flag} --check-receipt "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start]+command+body[end:]
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/native-workspace-execution')
    body = body.replace('python3.12 time tar gzip util-linux binutils',
                        'python3.12 python3-dev time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake')
    marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    assert body.count(marker) == 1
    body = body.replace(marker, marker+"'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),")
    subprocess.run(['bash','-n'], input=body, text=True, check=True)
    terminal = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
    compile(terminal, '<terminal>', 'exec')
    assert len(body.encode()) < 16384, 'EC2 user data limit'
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def replay(out):
    out = Path(out)
    reservation = json.loads((out/'aws-reservation.json').read_bytes())
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    terminal = json.loads((out/'aws-terminal.json').read_bytes())
    proof = reservation['qualification']
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA, 'replay mode'
    assert closed['state'] == 'terminated'
    assert terminal['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key] == reservation[key], 'campaign source binding'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal ' + key
    assert set(proof['code_sha256']) == set(CODE)
    assert proof['code_identity_sha256'] == worker.sha(encoded(proof['code_sha256']))
    base = Path(__file__).resolve().parents[1]
    assert all(worker.artifact(base/name)['sha256'] == digest for name,digest in proof['code_sha256'].items())
    assert proof['artifact_roster_sha256'] == worker.sha(encoded(ARTIFACTS))
    assert len(proof['source_sha256']) == proof['source_file_count'] == 399
    assert worker.source_identity(proof['source_sha256']) == proof['source_identity_sha256']
    assert SEMANTIC_1M or proof['source_identity_sha256'] == SOURCE_IDENTITY
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        assert worker.artifact(out/name) == identity, 'terminal artifact: ' + name
    for name,key in (('config.json','config_sha256'),('native-source-manifest.json','native_source_manifest_sha256')):
        if name in terminal['artifacts']:
            assert worker.artifact(out/name)['sha256'] == proof[key], 'saved authority: ' + name
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out/'source-qualification.json').read_bytes()) == proof
    assert terminal['source_qualification_sha256'] == terminal['artifacts'].get('source-qualification.json',{}).get('sha256')
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['exit_code'] == 0 and terminal['phase'] == 'complete'
    assert terminal['status'] == ('complete' if complete else 'failed')
    if complete:
        assert terminal['original_exit_code'] == 0
        assert set(terminal['artifacts']) == set(ARTIFACTS), 'exact completed artifact roster'
        validate_receipt(out, proof)
    result = dict(qualified=complete, actual_full_workspace_execution=complete and not TEST_BUILD,
                  source_identity_sha256=proof['source_identity_sha256'], exit_status=terminal['original_exit_code'])
    if TEST_BUILD:
        assert proof['execution_kind'] == FIXED['execution_kind']
        result.update(execution_kind=FIXED['execution_kind'], actual_workspace_test_build=complete)
    return result


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        source = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/artifacts/'+name)['Body']
        with (out/name).open('wb') as output, gzip.GzipFile(filename=str(out/(name+'.gz')), mode='wb', mtime=0) as archived:
            for chunk in iter(lambda:source.read(1024*1024), b''):
                output.write(chunk); archived.write(chunk)
        assert worker.artifact(out/name) == identity, 'downloaded artifact: ' + name
    result = replay(out)
    (out/'collection-replay.json').write_bytes(encoded(dict(terminal_sha256=worker.sha(raw),result=result))+b'\n')
    return terminal


def main(attempt):
    return shared.main(attempt, campaign=sys.modules[__name__])


def _worker_self_check(proof, config_body, manifest_body):
    """Mock only Cargo/cgroup; test the real command, receipt and source gate."""
    from scripts import launch_native_workspace_execution_spot as authority
    inventory = proof['source_sha256']
    counters = {'memory.max': str(worker.MEMORY), 'memory.peak': '10000',
        'memory.swap.max': '0', 'memory.swap.peak': '0', 'memory.swap.events': 'max 0\nfail 0\n',
        'memory.events': 'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n',
        'cpu.max': '200000 100000', 'cpu.stat': 'usage_usec 42\n',
        'pids.max': '512', 'pids.current': '1', 'pids_peak': '4', 'pids.events': 'max 0\n',
        'observer_pid': 42, 'process_ids': [42]}
    saved = None
    for failure in ('success', 'exit17', 'timeout', 'source-drift', 'reclaim', 'oom', 'peak', 'orphan', 'log-fsync'):
        with tempfile.TemporaryDirectory() as tmp:
            repo, out = Path(tmp)/'repo', Path(tmp)/'out'
            repo.mkdir(); out.mkdir()
            (out/'source-qualification.json').write_bytes(encoded(proof))
            (out/'config.json').write_bytes(config_body)
            (out/'native-source-manifest.json').write_bytes(manifest_body)
            (repo/CONFIG).parent.mkdir(parents=True)
            (repo/CONFIG).write_bytes(config_body)
            manifest_path = repo/proof['native_source_manifest']['path']
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_bytes(manifest_body)
            changed = dict(inventory, **{'Cargo.toml': '0'*64}) if failure == 'source-drift' else inventory
            after = copy.deepcopy(counters)
            if failure == 'oom':
                after['memory.events'] = counters['memory.events'].replace('oom 0','oom 1').replace('oom_kill 0','oom_kill 1')
            if failure == 'reclaim':
                after['memory.events'] = counters['memory.events'].replace('max 0','max 174')
            if failure == 'peak':
                after['memory.peak'] = str(worker.MEMORY+1)
            if failure == 'orphan':
                after['process_ids'] = [42,43]
            calls = []
            persist_failure = [False]
            def run(args, **kw):
                if args[0] != '/usr/bin/time':
                    return subprocess.CompletedProcess(args, 0, b'actual mocked compiler version\n')
                calls.append(args)
                expected_command = FIXED['command'] if TEST_BUILD else ['fake-cargo', *worker.COMMAND[1:]]
                assert args[args.index('7200')+1:] == expected_command
                assert args[args.index('--kill-after=30')+1] == '7200'
                assert kw['cwd'] == repo.resolve()
                assert all(kw['env'].get(k) == v for k,v in FIXED['environment'].items())
                if TEST_BUILD:
                    assert 'BORSUK_TEST_BUILD_COMMAND' not in kw['env']
                assert not Path(kw['env']['CARGO_TARGET_DIR']).is_relative_to(repo)
                kw['stdout'].write(b'full mocked cargo log\n')
                Path(args[args.index('-o')+1]).write_text('GNU time mocked resources\n')
                persist_failure[0] = failure == 'log-fsync'
                return subprocess.CompletedProcess(args, {'exit17':17, 'timeout':124}.get(failure,0))
            def fsync(fd):
                if persist_failure[0]:
                    persist_failure[0] = False
                    raise OSError('test log persistence failed')
            with patch.object(authority, 'qualify', return_value=proof), \
                    patch.object(worker, 'source_hashes', side_effect=[inventory, changed]), \
                    patch.object(worker, 'capture_cgroup', side_effect=[counters, after]), \
                    patch.object(worker.subprocess, 'run', side_effect=run), patch.object(os,'fsync',side_effect=fsync):
                previous = authority.SEMANTIC_1M, authority.TEST_BUILD, authority.CONFIG, authority.CODE, authority.FIXED
                result = worker.main('cargo' if TEST_BUILD else 'fake-cargo', repo, out,
                                     semantic_1m=SEMANTIC_1M, test_build=TEST_BUILD)
                assert previous == (authority.SEMANTIC_1M, authority.TEST_BUILD, authority.CONFIG, authority.CODE, authority.FIXED)
            assert len(calls) == 1, 'full test repeated'
            assert type(result['exit_status']) is int
            assert result['exit_status'] == {'exit17':17, 'timeout':124}.get(failure,0)
            assert result['qualified'] == (failure in ('success','reclaim')), failure
            assert result['source_unchanged'] == (failure != 'source-drift')
            assert json.loads((out/'workspace-receipt.json').read_bytes()) == result
            if failure in ('success','reclaim'):
                validate_receipt(out, proof)
                if failure == 'success':
                    saved = {name:(out/name).read_bytes() for name in ARTIFACTS if (out/name).is_file()}
                    if TEST_BUILD:
                        for changes in (dict(schema='borsuk-native-workspace-execution-receipt-v1'),
                                        dict(exit_status=False), dict(gate_status=False),
                                        dict(actual_full_workspace_execution=True),
                                        dict(command=list(worker.COMMAND)),
                                        dict(environment=dict(FIXED['environment'], BORSUK_TEST_BUILD_COMMAND='true'))):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, **changes)))
                            rejected(lambda: validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                (out/'test.log').write_bytes(b'tampered')
                rejected(lambda: validate_receipt(out, proof))
            else:
                assert result['gate_status'] != 0, 'failure gate lost'
                rejected(lambda: validate_receipt(out, proof))
    return dict(saved, **{'run-closed.log': b'closed mocked worker\n'})


def rejected(call):
    try:
        call()
    except (AssertionError, ValueError):
        return
    raise AssertionError('tampered or failed authority accepted')


def _lifecycle_self_check():
    from datetime import datetime, timezone
    from unittest.mock import Mock
    module = sys.modules[__name__]
    for failure in ('success','multi-ack','multi-ack-fsync','upload','interrupt'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2,s3]
            ec2.describe_instances.return_value = {'Reservations':[]}
            ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[
                {'SpotPrice':'.1','Timestamp':datetime.now(timezone.utc)}]}
            ids = ['i-owned','i-extra'] if failure.startswith('multi-ack') else ['i-owned']
            ec2.run_instances.return_value = {'Instances':[{'InstanceId':n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw:events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw:events.append('wait')
            def collected(*args):
                assert events == ['terminate','wait'], 'collection before termination waiter'
                events.append('collect')
                return dict(status='complete',phase='complete',exit_code=0,artifacts={n:{} for n in ARTIFACTS})
            with patch.object(module,'ROOT',Path(tmp)), patch.object(module,'preflight',return_value={'config_sha256':'a'*64}), \
                    patch.object(module,'user_data',return_value='mock'), patch.object(module,'collect',side_effect=collected), \
                    patch.object(module,'poll',side_effect=KeyboardInterrupt() if failure == 'interrupt' else None), \
                    patch.object(shared.boto3,'Session',return_value=session), \
                    patch.object(subprocess,'check_output',side_effect=['','0'*40,b'archive']), \
                    patch.object(peer,'missing',return_value=True), \
                    patch.object(peer,'put_if_absent',side_effect=[None,None,OSError('upload')] if failure == 'upload' else [None]*3), \
                    patch.object(os,'fsync',side_effect=OSError('persist') if failure.endswith('fsync') else None), \
                    contextlib.redirect_stdout(io.StringIO()):
                try:
                    main('a0001')
                except (OSError,KeyboardInterrupt):
                    assert failure not in ('success','multi-ack')
                else:
                    assert failure in ('success','multi-ack'), 'failure swallowed'
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            assert events == ['terminate','wait','collect']
            assert ec2.run_instances.call_args.kwargs['InstanceType'] == INSTANCE_TYPE
            assert ec2.run_instances.call_args.kwargs['ImageId'] == IMAGE_ID
            assert ec2.run_instances.call_args.kwargs['BlockDeviceMappings'][0]['DeviceName'] == ROOT_DEVICE_NAME
            for name in ('aws-launch.json','aws-closeout.json'):
                assert json.loads((Path(tmp)/'a0001'/name).read_bytes())['nodes'] == {
                    str(i):dict(instance_id=n) for i,n in enumerate(ids)}
    with contextlib.redirect_stdout(io.StringIO()):
        shared.self_check(lifecycle_only=True)


def _collection_self_check(proof, files, body):
    from unittest.mock import Mock
    terminal_script = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        for name,data in files.items():
            (out/name).write_bytes(data)
        env = dict(os.environ, INSTANCE_ID='i-owned', EXIT_CODE='0', ORIGINAL_EXIT_CODE='0',
                   PHASE='complete', ARTIFACT_NAMES=' '.join(ARTIFACTS))
        terminal = json.loads(subprocess.check_output([sys.executable,'-c',terminal_script], cwd=out, env=env))
        assert set(terminal['artifacts']) == set(ARTIFACTS)
        reservation = dict(schema=SCHEMA, qualification=proof, source_commit='0'*40,source_archive_sha256='1'*64)
        (out/'aws-reservation.json').write_bytes(encoded(reservation))
        (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated',nodes={'0':dict(instance_id='i-owned')})))
        store = {PREFIX+'a0001/terminal.json':encoded(terminal),
                 **{PREFIX+'a0001/artifacts/'+name:data for name,data in files.items()}}
        s3 = Mock()
        s3.get_object.side_effect = lambda **kw: {'Body':io.BytesIO(store[kw['Key']])}
        collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64)
        replayed = replay(out)
        assert replayed['qualified'] is True
        assert replayed['actual_full_workspace_execution'] is (not TEST_BUILD)
        if TEST_BUILD:
            assert replayed['actual_workspace_test_build'] is True
        for name,data in files.items():
            assert gzip.decompress((out/(name+'.gz')).read_bytes()) == data
        for changed in (dict(terminal,config_sha256='0'*64), dict(terminal,instance_id='i-other'),
                        dict(terminal,source_archive_sha256='2'*64), dict(terminal,exit_code=False),
                        dict(terminal,artifacts={n:v for n,v in terminal['artifacts'].items() if n != 'test.log'})):
            (out/'aws-terminal.json').write_bytes(encoded(changed))
            rejected(lambda:replay(out))
        (out/'aws-terminal.json').write_bytes(encoded(terminal))
        (out/'test.log').write_bytes(b'tampered log')
        rejected(lambda:replay(out))
        store[PREFIX+'a0001/artifacts/test.log'] = b'tampered download'
        rejected(lambda:collect(s3,PREFIX+'a0001',out,'i-owned','0'*40,'1'*64))
        store[PREFIX+'a0001/artifacts/test.log'] = files['test.log']
        failed = dict(terminal,exit_code=17,original_exit_code=17,status='failed',phase='execution')
        store[PREFIX+'a0001/terminal.json'] = encoded(failed)
        collect(s3,PREFIX+'a0001',out,'i-owned','0'*40,'1'*64)
        expected = dict(qualified=False,actual_full_workspace_execution=False,
                        source_identity_sha256=proof['source_identity_sha256'],exit_status=17)
        if TEST_BUILD:
            expected.update(execution_kind=FIXED['execution_kind'], actual_workspace_test_build=False)
        assert replay(out) == expected
        assert (out/'test.log').read_bytes() == files['test.log'], 'failed logs lost'


def _remote_self_check():
    """Run both CLIs away from the repo, faking only Cargo and cgroup files."""
    import shutil
    base = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        repo = root/'repo'
        for name in CODE:
            (repo/name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(base/name, repo/name)
        for index in range(399):
            (repo/f'mock-{index}.rs').write_text('// mock native source\n')
        inventory = worker.source_hashes(repo)
        manifest_path = ROOT/'native-source-manifest.json'
        (repo/manifest_path).parent.mkdir(parents=True)
        (repo/manifest_path).write_bytes(encoded(dict(source_sha256=inventory, source_file_count=399,
            source_identity_sha256=worker.source_identity(inventory), native_source_commit='1'*40)))
        config = dict(FIXED, controller_authority_pending=False,
            controller_code_sha256={n:worker.artifact(repo/n)['sha256'] for n in CODE},
            native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)))
        (repo/CONFIG).write_bytes(encoded(config))
        cargo = root/'cargo'
        cargo_args = ['test', '--locked', '--workspace', '--all-targets', '--no-run'] if TEST_BUILD else list(worker.COMMAND[1:])
        cargo.write_text(f'''#!{sys.executable}
import os, sys
from pathlib import Path
if sys.argv[1:] == ['-V']:
    print('fake cargo version')
else:
    assert sys.argv[1:] == {cargo_args!r}
    assert os.environ['CARGO_BUILD_JOBS'] == os.environ['RUST_TEST_THREADS'] == '1'
    assert os.environ['RUSTC_WRAPPER'] == os.environ['RUSTC_WORKSPACE_WRAPPER'] == ''
    if {TEST_BUILD!r}:
        assert os.environ['BORSUK_TEST_BUILD_JOBS'] == '1'
        assert 'BORSUK_TEST_BUILD_COMMAND' not in os.environ
    target = Path(os.environ['CARGO_TARGET_DIR'])
    assert target.is_dir() and not list(target.iterdir())
    (target.parent/'cargo-called').open('x').close()
    if os.environ.get('MUTATE'):
        with Path(os.environ['MUTATE']).open('ab') as out: out.write(b' ')
    print('fake workspace cargo')
    sys.exit(int(os.environ.get('CARGO_EXIT', '0')))
''')
        cargo.chmod(0o755)
        if TEST_BUILD:
            # Fake Bash records the exact invocation, then executes the real script.
            (root/'bash').write_text(f'''#!{sys.executable}
import os, sys
from pathlib import Path
assert sys.argv[1:] == ['scripts/check_rust_test_build.sh']
(Path(os.environ['CARGO_TARGET_DIR']).parent/'bash-called').open('x').close()
os.execv('/bin/bash', ['/bin/bash', *sys.argv[1:]])
''')
            (root/'bash').chmod(0o755)
        (root/'rustc').write_text('#!/bin/sh\necho fake rustc version\n')
        (root/'rustc').chmod(0o755)
        # Execute the real worker CLI; only its kernel evidence is synthetic.
        runner = '''import os, runpy
from pathlib import Path
from unittest.mock import patch
original = Path.read_text
counters = {'memory.max':'8589934592', 'memory.peak':'10000', 'memory.swap.max':'0',
 'memory.swap.peak':'0', 'memory.swap.events':'max 0\\nfail 0\\n',
 'memory.events':'low 0\\nhigh 0\\nmax 0\\noom 0\\noom_kill 0\\noom_group_kill 0\\n',
 'cpu.max':'200000 100000', 'cpu.stat':'usage_usec 42\\n', 'pids.max':'512',
 'pids.current':'1', 'pids.events':'max 0\\n', 'cgroup.procs':str(os.getpid())}
def read(path, *args, **kwargs):
    if str(path) == '/proc/self/cgroup': return '0::/workspace-mock\\n'
    if str(path.parent) == '/sys/fs/cgroup/workspace-mock': return counters[path.name]
    return original(path, *args, **kwargs)
with patch.object(Path, 'read_text', read):
    runpy.run_module('scripts.check_native_workspace_execution', run_name='__main__')
'''
        env = dict(os.environ, PYTHONPATH=str(repo), MUTATE='', CARGO_EXIT='0',
                   CARGO_BUILD_JOBS='99', BORSUK_TEST_BUILD_JOBS='99',
                   RUSTC_WRAPPER='forbidden-wrapper', RUSTC_WORKSPACE_WRAPPER='forbidden-wrapper')
        env.pop('BORSUK_TEST_BUILD_COMMAND', None)
        flag = mode_flag().strip()
        cli = [sys.executable, '-m', MODULE, flag]
        failures = [('success',None), ('exit17',None), ('native','mock-0.rs'),
                    ('config',str(CONFIG)), ('code',CODE[0]), ('manifest',str(manifest_path))]
        if TEST_BUILD:
            failures.append(('script','scripts/check_rust_test_build.sh'))
        for failure, mutation in failures:
            out = root/failure
            out.mkdir()
            subprocess.run([*cli,'--stage',str(repo),str(out)], cwd=root, env=env, check=True)
            before = (repo/mutation).read_bytes() if mutation else None
            result = subprocess.run([sys.executable,'-c',runner,flag,str(cargo),str(repo),str(out)],
                cwd=root, env=dict(env, MUTATE=str(repo/mutation) if mutation else '',
                                  CARGO_EXIT='17' if failure == 'exit17' else '0'), capture_output=True, text=True)
            assert result.returncode == (0 if failure == 'success' else 17 if failure == 'exit17' else 96), result.stderr
            receipt = json.loads((out/'workspace-receipt.json').read_bytes())
            assert receipt['exit_status'] == (17 if failure == 'exit17' else 0)
            assert receipt['qualified'] is (failure == 'success')
            assert receipt['source_file_count'] == 399 and (out/'cargo-called').exists()
            log = (out/'test.log').read_text()
            assert log.startswith('fake workspace cargo\n')
            if TEST_BUILD:
                assert (out/'bash-called').exists()
                assert f'rust-test-build status={receipt["exit_status"]}' in log and 'jobs=1' in log
            checked = subprocess.run([*cli,'--check-receipt',str(out)], cwd=root, env=env, capture_output=True)
            assert (checked.returncode == 0) is (failure == 'success'), checked.stderr
            if failure == 'success':
                proof = json.loads((out/'source-qualification.json').read_bytes())
                body = user_data('0'*40, '1'*64, 'sources/mock', PREFIX+'a0001', proof)
                terminal_script = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
                (out/'run-closed.log').write_text('closed synthetic CLI worker\n')
                terminal = subprocess.check_output([sys.executable,'-c',terminal_script], cwd=out,
                    env=dict(env, INSTANCE_ID='i-owned', EXIT_CODE='0', ORIGINAL_EXIT_CODE='0',
                             PHASE='complete', ARTIFACT_NAMES=' '.join(ARTIFACTS)))
                (out/'aws-terminal.json').write_bytes(terminal)
                (out/'aws-reservation.json').write_bytes(encoded(dict(schema=SCHEMA, qualification=proof,
                    source_commit='0'*40, source_archive_sha256='1'*64)))
                (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated', nodes={'0':dict(instance_id='i-owned')})))
                replayed = json.loads(subprocess.check_output([*cli,'--replay',str(out)], cwd=root, env=env))
                assert replayed['qualified'] is True and replayed['actual_full_workspace_execution'] is (not TEST_BUILD)
                if TEST_BUILD:
                    assert replayed['actual_workspace_test_build'] is True
            if mutation:
                (repo/mutation).write_bytes(before)
        if TEST_BUILD:
            # A true/false shim could report success without invoking Cargo.
            for shim in ('true', 'false', 'arbitrary'):
                out = root/('shim-'+shim)
                out.mkdir()
                subprocess.run([*cli,'--stage',str(repo),str(out)], cwd=root, env=env, check=True)
                wrong = subprocess.run([sys.executable,'-c',runner,flag,str(cargo),str(repo),str(out)],
                    cwd=root, env=dict(env, BORSUK_TEST_BUILD_COMMAND=shim), capture_output=True)
                assert wrong.returncode != 0 and not (out/'target').exists()
                assert not (out/'cargo-called').exists()
        # Missing mode must reject the semantic authority before creating a target.
        out = root/'wrong-mode'
        out.mkdir()
        subprocess.run([*cli,'--stage',str(repo),str(out)], cwd=root, env=env, check=True)
        wrong = subprocess.run([sys.executable,'-c',runner,str(cargo),str(repo),str(out)],
                               cwd=root, env=env, capture_output=True)
        assert wrong.returncode != 0 and not (out/'target').exists()
        if TEST_BUILD:
            wrong = subprocess.run([sys.executable,'-c',runner,'--semantic-1m',str(cargo),str(repo),str(out)],
                                   cwd=root, env=env, capture_output=True)
            assert wrong.returncode != 0 and not (out/'target').exists()
            checked = subprocess.run([sys.executable,'-m',MODULE,'--semantic-1m','--check-receipt',str(root/'success')],
                                     cwd=root, env=env, capture_output=True)
            assert checked.returncode != 0, 'compile-only receipt accepted as full execution'


def _test_build_protocol_self_check():
    configure(True, test_build=True)
    assert str(ROOT).endswith('semantic-1m/implementation-gates/remote-test-build')
    assert PREFIX == 'research/semantic-router/20261001/semantic-1m-test-build-'
    assert FIXED['command'] == ['bash', 'scripts/check_rust_test_build.sh']
    assert FIXED['environment']['BORSUK_TEST_BUILD_JOBS'] == '1'
    assert FIXED['environment']['BORSUK_TEST_BUILD_COMMAND'] is None
    assert FIXED['execution_kind'] == 'workspace-test-build'
    assert 'scripts/check_rust_test_build.sh' in CODE and len(CODE) == 23
    configure(True)
    assert FIXED['command'] == list(worker.COMMAND) and len(CODE) == 22
    assert 'execution_kind' not in FIXED
    configure()
    assert TOKEN_PREFIX == 'metadata-waves-workspace-'


def self_check(semantic_1m=False, *, test_build=False):
    with execution_mode(semantic_1m, test_build=test_build):
        _self_check()


def _self_check():
    semantic_1m = SEMANTIC_1M
    module = sys.modules[__name__]
    base = Path(__file__).resolve().parents[1]
    manifest = json.loads((base/semantic.ROOT/'metadata-waves/native-source-manifest.json').read_bytes())
    inventory = manifest['source_sha256']
    if semantic_1m:
        inventory = dict(inventory, **{'Cargo.toml':'1'*64})
        manifest.update(source_sha256=inventory, source_identity_sha256=worker.source_identity(inventory))
        assert manifest['source_identity_sha256'] != SOURCE_IDENTITY
    manifest_path = ROOT/'native-source-manifest.json'
    manifest_body = encoded(manifest)
    config = dict(FIXED, controller_authority_pending=False,
        controller_code_sha256={n:worker.artifact(base/n)['sha256'] for n in CODE},
        native_source_manifest=dict(path=str(manifest_path),bytes=len(manifest_body),sha256=worker.sha(manifest_body)))
    with tempfile.TemporaryDirectory() as tmp:
        repo, out = Path(tmp)/'repo', Path(tmp)/'out'
        repo.mkdir(); out.mkdir()
        for name in CODE:
            (repo/name).parent.mkdir(parents=True,exist_ok=True)
            (repo/name).symlink_to(base/name)
        (repo/manifest_path).parent.mkdir(parents=True,exist_ok=True)
        (repo/manifest_path).write_bytes(manifest_body)
        (repo/CONFIG).parent.mkdir(parents=True,exist_ok=True)
        config_body = encoded(config)
        (repo/CONFIG).write_bytes(config_body)
        with patch.object(worker,'source_hashes',return_value=inventory):
            proof = qualify(repo)
            assert proof['source_identity_sha256'] == manifest['source_identity_sha256']
            assert proof['actual_full_workspace_execution'] is False
            stage(repo,out)
            assert json.loads((out/'source-qualification.json').read_bytes()) == proof
            for key,value in (('controller_authority_pending',True), ('memory_bytes',worker.MEMORY+1),
                              ('command',[*FIXED['command'],'--no-run']), ('environment',dict(FIXED['environment'],CARGO_BUILD_JOBS='2')),
                              ('controller_code_sha256',dict(config['controller_code_sha256'],**{CODE[0]:'0'*64})),
                              ('native_source_manifest',dict(config['native_source_manifest'],sha256='0'*64))):
                (repo/CONFIG).write_bytes(encoded(dict(config,**{key:value})))
                rejected(lambda:qualify(repo))
            for key,value in (('source_identity_sha256','0'*64), ('source_file_count',398),
                              ('native_source_commit','not-a-commit')):
                (repo/manifest_path).write_bytes(encoded(dict(manifest,**{key:value})))
                pointer = dict(path=str(manifest_path),**worker.artifact(repo/manifest_path))
                (repo/CONFIG).write_bytes(encoded(dict(config,native_source_manifest=pointer)))
                rejected(lambda:qualify(repo))
            (repo/manifest_path).write_bytes(manifest_body)
            (repo/CONFIG).write_bytes(config_body)
        if not semantic_1m:
            changed = dict(inventory,**{'Cargo.toml':'1'*64})
            (repo/manifest_path).write_bytes(encoded(dict(manifest,source_sha256=changed,
                source_identity_sha256=worker.source_identity(changed))))
            pointer = dict(path=str(manifest_path),**worker.artifact(repo/manifest_path))
            (repo/CONFIG).write_bytes(encoded(dict(config,native_source_manifest=pointer)))
            with patch.object(worker,'source_hashes',return_value=changed):
                rejected(lambda:qualify(repo))  # Default cannot repin historical native code.
            (repo/manifest_path).write_bytes(manifest_body)
            (repo/CONFIG).write_bytes(config_body)
        with patch.object(worker,'source_hashes',return_value=dict(inventory,**{'Cargo.toml':'0'*64})):
            rejected(lambda:qualify(repo))
        with patch.object(subprocess,'check_output',return_value='dirty'):
            rejected(lambda:preflight(repo))
        with patch.object(subprocess,'check_output',side_effect=['','']):
            rejected(lambda:preflight(repo))
        body = user_data('0'*40,'1'*64,'sources/mock',PREFIX+'a0001',proof)
        assert len(CODE) == len(set(CODE)) == (23 if TEST_BUILD else 22)
        assert len(ARTIFACTS) == len(set(ARTIFACTS)) == 13
        assert '--on-active=9000s' in body and 'RuntimeMaxSec=7260' in body
        assert all(k in body for k in ('MemoryMax=8G','MemorySwapMax=0','CPUQuota=200%','TasksMax=512'))
        assert 'build-essential' in body and 'python3-dev' in body
        assert semantic.AWSCLI_URL in body and semantic.AWSCLI_SHA256 in body
        assert 'phase=publication' not in body and 'native-semantic-publication' not in body
        flag = mode_flag()
        for invocation in (f'{MODULE}{flag} --stage', f'{MODULE}{flag} --check-receipt',
                           f'scripts.check_native_workspace_execution{flag} "$CARGO_HOME/bin/cargo"'):
            assert invocation in body, invocation
        files = _worker_self_check(proof,config_body,manifest_body)
        from scripts.launch_native_semantic_metadata_cold_spot import _full_receipt
        full_check = lambda: _full_receipt(files['workspace-receipt.json'],files['test.log'],inventory,proof['source_identity_sha256'])
        if TEST_BUILD:
            rejected(full_check)
        else:
            full_check()
        _collection_self_check(proof,files,body)
    _lifecycle_self_check()
    if semantic_1m:
        _remote_self_check()
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    print(f'PASS workspace {"test-build" if TEST_BUILD else "execution"} ({"semantic-1m" if semantic_1m else "metadata-waves"}): command once; exit17/timeout/drift/OOM/peak/orphan/persistence/tamper rejected; max reclaim admitted; full/compile authority checked; all-ACK/fsync/wait-before-collection; remote_cli={semantic_1m}; code={len(CODE)} artifacts=13 userdata={len(body.encode())} peak_bytes={peak}; AWS/Cargo/cgroup MOCKED')


if __name__ == '__main__':
    args = sys.argv[1:]
    test_build = args[:1] == ['--semantic-1m-test-build']
    semantic_1m = test_build or args[:1] == ['--semantic-1m']
    if semantic_1m:
        args = args[1:]
    configure(semantic_1m, test_build=test_build)
    # ponytail: shared launch archives the repository in memory; stream it in
    # the shared launcher if root's launch resource gate proves insufficient.
    if args[:1] and args[0].startswith('--'):
        resource.setrlimit(resource.RLIMIT_AS, (200*1024**2,200*1024**2))
    if args == ['--self-check']:
        with execution_mode():
            _test_build_protocol_self_check()
        self_check(semantic_1m, test_build=test_build)
    elif args[:1] == ['--stage']:
        assert len(args) == 3
        stage(*args[1:])
    elif args[:1] == ['--check-receipt']:
        assert len(args) == 2
        out = Path(args[1])
        validate_receipt(out,json.loads((out/'source-qualification.json').read_bytes()))
    elif args[:1] == ['--replay']:
        assert len(args) == 2
        print(json.dumps(replay(args[1]),sort_keys=True))
    else:
        assert len(args) == 1, 'usage: launch_native_workspace_execution_spot.py [--semantic-1m | --semantic-1m-test-build] aNNNN | --self-check | --stage REPO OUT | --check-receipt OUT | --replay OUT'
        with open('/tmp/borsuk-native-workspace-execution-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(args[0])
