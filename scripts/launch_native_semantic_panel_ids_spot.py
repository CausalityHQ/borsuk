"""Bounded metadata-only selection; the root freezes config and owns launch.

Config: FIXED plus controller_authority_pending=false, refs=REFS,
controller_code_sha256={every CODE path: SHA256}. No config is created here.
CLI: aNNNN | --self-check | --stage REPO OUTPUT | --replay OUTPUT.
--stage requires BORSUK_PANEL_{CONFIG_SHA256,SOURCE_COMMIT,ARCHIVE_SHA256}
from the authenticated bootstrap, inside the actual selection cgroup.
Runtime imports only this module and the unchanged selector/audit. The larger
controller closure is pinned but never imported by the metadata worker.
No embeddings, truth, oracle, ANN, Rust build or qualification is performed.
"""
import contextlib
import fcntl
import gzip
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/panel-tools/remote-selection')
CONFIG = ROOT / 'config.json'
NAME = ''
MODULE = 'scripts.launch_native_semantic_panel_ids_spot'
SCHEMA = 'borsuk-semantic-panel-ids-spot-v1'
CONFIG_SCHEMA = 'borsuk-semantic-panel-ids-selection-v1'
PREFIX = 'research/semantic-router/20261001/panel-ids-'
TOKEN_PREFIX = 'semantic-panel-ids-'
TAG = 'borsuk-semantic-panel-ids'
WALL, WORKER_SECONDS, SERVICE_SECONDS = 4500, 3600, 3660
MEMORY = 8 * 1024**3
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME, SUBNET = '/dev/sda1', 'subnet-034528fbd6977848f'
REGION, BUCKET = 'eu-central-1', 'borsuk-bench-453182569524-euc1'
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .60
SEED = b'borsuk-v36-rank16-fresh-v1'
FIELDS = ('query_ordinal', 'feature_row_id', 'source_rank', 'source_row_offset', 'selector_sha256')
RUNTIME_CODE = tuple('scripts/' + name + '.py' for name in (
    'audit_v36_ranked_physical_ids', 'select_v36_rank16_fresh_ids',
    'launch_native_semantic_panel_ids_spot'))
CODE = tuple(sorted((*RUNTIME_CODE, *('scripts/' + name + '.py' for name in (
    'check_native_metadata_ranges_build', 'check_native_metadata_ranges_stats',
    'check_native_semantic_concurrency', 'check_native_semantic_router_stats',
    'check_native_startup_build', 'check_native_startup_stats',
    'launch_native_metadata_ranges_cold_spot', 'launch_native_peer_1m_spot',
    'launch_native_semantic_router_cold_spot', 'launch_native_startup_profile_spot',
    'launch_v157_primary_feasibility_spot', 'launch_v174_relaid_bind_compile_spot',
    'package_semantic_native_generation', 'prepare_native_semantic_publication',
    'rest_coexistence_load', 'run_native_cold_first_query', 'run_native_cold_offered',
    'run_native_metadata_ranges_cold', 'run_native_peer_1m_worker',
    'run_native_peer_offered_http', 'run_native_semantic_router_cold',
    'run_native_union_http', 'run_native_union_offered_http')))))
REFS = {
    'registry': dict(path='docs/research/v36-prefix-source-registry.json', bytes=726170,
        sha256='b9a19e2f142fd54983ed1db9f09862f2c5623b6b2105d66e538664f8adda9180'),
    'population': dict(path='docs/research/native-union-20260928/fresh-identity-authorities/population-authority.json', bytes=9839,
        sha256='be6abb86e2930b38da572cb7daec3fc0e1a7adb93a53ce974ab038f1efa42fbe'),
    'overlap': dict(path='docs/research/native-union-20260928/fresh-relaion-physical-id-overlap.json', bytes=9422,
        sha256='888748ef1125416bfa47c6dc791280e8be1885eb7ea68f3145872985fe5801c6'),
    'old_panel': dict(path='docs/research/native-union-20260928/fresh-rank16-provisional-ids.json', bytes=227817,
        sha256='a8bd97d6e6468e715c4b26d9df8c400f649e5428b2a85f151307f20cb030230c')}
ARTIFACTS = ('source-qualification.json', 'config.json', 'panel.json',
    'selection-receipt.json', 'input-hashes.json', 'test.log', 'test-resources.txt',
    'selection-cgroup.json', 'tool-versions.json', 'run-closed.log',
    *(f'inputs/{name}.json' for name in REFS))
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256',
    'runtime_code_identity_sha256', 'refs_identity_sha256', 'artifact_roster_sha256',
    'campaign_schema', 'awscli_version', 'awscli_sha256')
AWSCLI_VERSION = '2.36.11'
AWSCLI_SHA256 = '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6'
FIXED = dict(schema=CONFIG_SCHEMA, architecture='x86_64', region=REGION, bucket=BUCKET,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, volume_gib=80, volume_type='gp3', encrypted=True,
    delete_on_termination=True, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
    compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=.15, memory_bytes=MEMORY,
    swap_bytes=0, cpu_quota_percent=200, tasks_max=512, worker_limit_seconds=WORKER_SECONDS,
    service_limit_seconds=SERVICE_SECONDS, machine_limit_seconds=WALL,
    versions={'pyarrow': '24.0.0'}, source_object_count=32, source_encoded_bytes=10968608516,
    reservoir_ordinals=[1000, 1063], queries=64, metadata_only=True,
    quality_peek_allowed=False, complete_historical_coverage=False)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def artifact(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), 'regular body required: ' + str(path)
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024*1024), b''):
            size += len(chunk)
            digest.update(chunk)
    return dict(bytes=size, sha256=digest.hexdigest())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as output:
        output.write(value if isinstance(value, bytes) else encoded(value) + b'\n')
        output.flush()
        os.fsync(output.fileno())


def qualify(base=Path('.')):
    base = Path(base).resolve()
    body = (base/CONFIG).read_bytes()  # Missing config fails before any cloud call.
    config = json.loads(body)
    assert config['controller_authority_pending'] is False, 'root authority freeze pending'
    assert all(type(config[k]) is type(v) and config[k] == v for k,v in FIXED.items()), 'fixed selection protocol'
    assert config['refs'] == REFS, 'frozen metadata authorities'
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive controller code roster'
    assert all(artifact(base/name)['sha256'] == digest for name,digest in code.items()), 'controller source drift'
    assert code[RUNTIME_CODE[0]] == '67af40bf3d505d45979f269218f4bfd426aaaa5f1e6705b6947bd32831d8bb2b'
    assert code[RUNTIME_CODE[1]] == '6388e274ba128d770bbadd8b02833af4f685214bc1f78c9796fd778471d30399'
    for name, pointer in REFS.items():
        assert artifact(base/pointer['path']) == {k:pointer[k] for k in ('bytes','sha256')}, name
    ranked_shards(base/REFS['registry']['path'], base/REFS['population']['path'], base/REFS['overlap']['path'])
    runtime = {name:code[name] for name in RUNTIME_CODE}
    return dict(config_path=str(CONFIG), config_sha256=sha(body), campaign_schema=SCHEMA,
        code_sha256=code, code_identity_sha256=sha(encoded(code)),
        runtime_code_sha256=runtime, runtime_code_identity_sha256=sha(encoded(runtime)),
        refs=REFS, refs_identity_sha256=sha(encoded(REFS)),
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), metadata_only=True,
        quality_peek_allowed=False, complete_historical_coverage=False,
        native_qualification_claim=False, awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git','status','--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    return proof


def ranked_shards(registry, population, overlap):
    registry, population, overlap = (json.loads(Path(path).read_bytes()) for path in (registry, population, overlap))
    ranked = sorted(registry, key=lambda shard: (
        hashlib.sha256(b'borsuk-v36-screen-object-v1' + shard['path'].encode()
                       + shard['encoded_bytes'].to_bytes(8, 'little')).digest(), shard['path'].encode()))[:32]
    assert len(registry) == 2298 and len(ranked) == len(overlap['objects']) == 32
    assert len({Path(s['path']).name for s in ranked}) == 32, 'scratch filename collision'
    assert sum(s['encoded_bytes'] for s in ranked) == FIXED['source_encoded_bytes']
    fields = ('path', 'sha256', 'encoded_bytes', 'uri')
    assert all({k:s[k] for k in fields} == {k:p[k] for k in fields}
               for s,p in zip(ranked[:16], population['consumed_objects']))
    assert all(row['path'] == s['path'] and row['source_sha256'] == s['sha256']
               and row['rank'] == rank for rank,(s,row) in enumerate(zip(ranked, overlap['objects'])))
    return ranked


def capture_cgroup():
    relative = Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    group = Path('/sys/fs/cgroup')/relative
    names = ('memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
             'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat',
             'pids.max', 'pids.current', 'pids.events')
    return dict(cgroup=str(group), observer_pid=os.getpid(),
        process_ids=[int(pid) for pid in (group/'cgroup.procs').read_text().split()],
        **{name:(group/name).read_text() for name in names})


def validate_cgroup(report):
    assert report['closed'] is True
    for counters in (report['before'], report['after']):
        assert int(counters['memory.max']) == MEMORY and 0 <= int(counters['memory.peak']) <= MEMORY
        assert int(counters['memory.swap.max']) == int(counters['memory.swap.peak']) == 0
        quota, period = map(int, counters['cpu.max'].split())
        assert period > 0 and quota == period * 2 and int(counters['pids.max']) == 512
        assert 0 < int(counters['pids.current']) <= 512 and counters['cpu.stat'].strip()
        assert counters['observer_pid'] in counters['process_ids']
        for name in ('memory.events','memory.swap.events','pids.events'):
            events = dict(line.split() for line in counters[name].splitlines())
            failures = ('oom','oom_kill','oom_group_kill') if name == 'memory.events' else events
            assert all(int(events.get(key, 0)) == 0 for key in failures), 'resource failure: ' + name
    assert report['before']['cgroup'] == report['after']['cgroup'], 'cgroup changed'
    assert set(report['after']['process_ids']) <= set(report['before']['process_ids']), 'selection descendants remain'


def tool_versions():
    version = importlib.metadata.version('pyarrow')
    assert version == FIXED['versions']['pyarrow'] and sys.version_info[:2] == (3,12)
    assert platform.machine() == 'x86_64'
    os_release = platform.freedesktop_os_release()
    assert os_release['ID'] == 'ubuntu' and os_release['VERSION_ID'] == '24.04'
    return dict(python=sys.version, executable=sys.executable, pyarrow=version,
        machine=platform.machine(), os_release=os_release)


def tuple_digest(rows):
    return sha(json.dumps([[r[k] for k in FIELDS] for r in rows], separators=(',', ':')).encode())


def validate_panel(out):
    out = Path(out)
    panel = json.loads((out/'panel.json').read_bytes())
    old = json.loads((out/'inputs/old_panel.json').read_bytes())['selected']
    overlap = json.loads((out/'inputs/overlap.json').read_bytes())
    assert len(old) == 1000 and [r['query_ordinal'] for r in old] == list(range(1000)), 'consumed prefix'
    expected = dict(schema='borsuk-semantic-1m-fresh-panel-ids-v1', metadata_only=True,
        query_embeddings_or_gt_opened=False, prior_query_audit_pass=False, qualification=False,
        complete_historical_coverage=False, reservoir_ordinals=[1000,1063],
        development_ordinals=[0,63], sealed_ordinals=[], selector_seed=SEED.decode(),
        consumed_panel_sha256=REFS['old_panel']['sha256'],
        source_registry_sha256=REFS['registry']['sha256'],
        population_authority_sha256=REFS['population']['sha256'],
        physical_id_report_sha256=REFS['overlap']['sha256'],
        consumed_prefix_tuple_sha256=tuple_digest(old))
    assert all(type(panel[k]) is type(v) and panel[k] == v for k,v in expected.items()), 'panel authority/prefix'
    rows = panel['selected']
    assert len(rows) == 64, 'panel count'
    for ordinal, row in enumerate(rows):
        assert set(row) == set(FIELDS) | {'reservoir_ordinal'}, 'panel fields'
        assert type(row['query_ordinal']) is int and row['query_ordinal'] == ordinal
        assert type(row['reservoir_ordinal']) is int and row['reservoir_ordinal'] == ordinal + 1000
        feature = row['feature_row_id']
        assert type(feature) is int and 0 <= feature < 2**64
        rank, offset = row['source_rank'], row['source_row_offset']
        assert type(rank) is int and 16 <= rank < 32
        assert type(offset) is int and 0 <= offset < overlap['objects'][rank]['physical_rows']
        assert row['selector_sha256'] == sha(SEED + feature.to_bytes(8, 'little'))
    ids = [r['feature_row_id'] for r in rows]
    assert len(set(ids)) == 64 and not set(ids) & {r['feature_row_id'] for r in old}, 'consumed/repeated IDs'
    reservoir = old + [{k:(r['reservoir_ordinal'] if k == 'query_ordinal' else r[k]) for k in FIELDS} for r in rows]
    keys = [(r['selector_sha256'],r['feature_row_id']) for r in reservoir]
    assert keys == sorted(keys), 'reservoir order'
    assert panel['reservoir_tuple_sha256'] == tuple_digest(reservoir), 'reservoir identity'
    return panel


def metadata():
    from scripts import audit_v36_ranked_physical_ids as audit
    return audit


def stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    assert out.is_dir() and not out.is_relative_to(repo), 'owned output outside source required'
    resources = dict(before=capture_cgroup(), closed=False)
    report = dict(schema='borsuk-semantic-panel-ids-selection-receipt-v1', selected=False,
        metadata_only=True, embeddings_truth_or_ann_opened=False, exit_status=None)
    error = None
    try:
        validate_cgroup(dict(resources, after=resources['before'], closed=True))
        proof = qualify(repo)
        assert proof['config_sha256'] == os.environ['BORSUK_PANEL_CONFIG_SHA256'], 'bootstrap config pin'
        source = dict(source_commit=os.environ['BORSUK_PANEL_SOURCE_COMMIT'],
            source_archive_sha256=os.environ['BORSUK_PANEL_ARCHIVE_SHA256'])
        assert re.fullmatch('[0-9a-f]{40}', source['source_commit'])
        assert re.fullmatch('[0-9a-f]{64}', source['source_archive_sha256'])
        write(out/'source-qualification.json', dict(proof, **source))
        write(out/'config.json', (repo/CONFIG).read_bytes())
        write(out/'tool-versions.json', tool_versions())
        for name,pointer in REFS.items():
            write(out/f'inputs/{name}.json', (repo/pointer['path']).read_bytes())
            assert artifact(out/f'inputs/{name}.json') == {k:pointer[k] for k in ('bytes','sha256')}
        scratch = out/'scratch'
        scratch.mkdir(exist_ok=False)  # This attempt owns it; no shared cache or retry.
        assert not scratch.is_symlink()
        shards = ranked_shards(*(out/f'inputs/{name}.json' for name in ('registry','population','overlap')))
        audit = metadata()
        ledger = dict(refs=REFS, objects=[])
        with (out/'test.log').open('x') as log:
            for rank,shard in enumerate(shards):
                path = audit.acquire(shard, scratch)
                assert path == scratch/Path(shard['path']).name and not path.is_symlink()
                assert audit.digest(path) == (shard['encoded_bytes'],shard['sha256']), 'downloaded body identity'
                ledger['objects'].append(dict(rank=rank, path=shard['path'],
                    uri=shard['uri'], bytes=shard['encoded_bytes'], sha256=shard['sha256']))
                log.write(f'authenticated source rank={rank} bytes={shard["encoded_bytes"]}\n')
                log.flush()
            write(out/'input-hashes.json', ledger)
            command = [sys.executable, '-m', 'scripts.select_v36_rank16_fresh_ids',
                '--scratch', str(scratch), '--output', str(out/'panel.json'), '--next64',
                '--previous', str(repo/REFS['old_panel']['path']),
                '--previous-sha256', REFS['old_panel']['sha256']]
            report['command'] = command
            result = subprocess.run(command, cwd=repo, env=dict(os.environ, PYTHONPATH=str(repo)),
                stdout=log, stderr=subprocess.STDOUT, check=False)
            log.flush(); os.fsync(log.fileno())
            report['exit_status'] = result.returncode
            assert result.returncode == 0, 'selector failed'
        validate_panel(out)
        assert qualify(repo) == proof, 'source/config changed during selection'
        report.update(selected=True, config_sha256=proof['config_sha256'],
            code_identity_sha256=proof['code_identity_sha256'], **source,
            panel=artifact(out/'panel.json'), input_hashes=artifact(out/'input-hashes.json'))
    except BaseException as failure:
        error = failure
        report.update(selected=False, error_type=type(failure).__name__, error=str(failure))
        raise
    finally:
        resources.update(after=capture_cgroup(), closed=True)
        try:
            validate_cgroup(resources)
        except AssertionError:
            report['selected'] = False
            if error is None:
                raise
        finally:
            write(out/'selection-cgroup.json', resources)
            write(out/'selection-receipt.json', report)
    return report


def lifecycle():
    from scripts import launch_native_metadata_ranges_cold_spot as shared
    from scripts import launch_native_semantic_router_cold_spot as bootstrap
    return shared, bootstrap


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix)
    assert qualification['config_path'] == str(CONFIG) and qualification['campaign_schema'] == SCHEMA
    _, bootstrap = lifecycle()
    adapter = {k:qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, '_offered', return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps pyarrow==24.0.0
phase=selection
systemd-run --unit=semantic-panel-ids --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=3660 -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=BORSUK_PANEL_CONFIG_SHA256={qualification['config_sha256']} \\
 --setenv=BORSUK_PANEL_SOURCE_COMMIT={commit} --setenv=BORSUK_PANEL_ARCHIVE_SHA256={archive_sha} \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 3600 \\
 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start,end = body.index('phase=install\n'),body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/native-semantic-panel-ids')
    body = body.replace('python3-boto3 python3.12', 'python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    assert body.count(marker) == 1
    body = body.replace(marker, marker + "'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),")
    subprocess.run(['bash','-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0], '<terminal>', 'exec')
    assert len(body.encode()) < 16384
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = lifecycle()
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def replay(out):
    out = Path(out)
    reservation = json.loads((out/'aws-reservation.json').read_bytes())
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    launch = json.loads((out/'aws-launch.json').read_bytes())
    terminal = json.loads((out/'aws-terminal.json').read_bytes())
    proof = reservation['qualification']
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'same ACKed IDs closed'
    assert terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key] == reservation[key] == launch[key], 'source binding: ' + key
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal pin: ' + key
    assert set(proof['code_sha256']) == set(CODE)
    base = Path(__file__).resolve().parents[1]
    assert all(artifact(base/name)['sha256'] == digest for name,digest in proof['code_sha256'].items()), 'replay source drift'
    assert proof['code_identity_sha256'] == sha(encoded(proof['code_sha256']))
    assert proof['runtime_code_sha256'] == {n:proof['code_sha256'][n] for n in RUNTIME_CODE}
    assert proof['runtime_code_identity_sha256'] == sha(encoded(proof['runtime_code_sha256']))
    assert proof['refs'] == REFS and proof['refs_identity_sha256'] == sha(encoded(REFS))
    assert proof['artifact_roster_sha256'] == sha(encoded(ARTIFACTS))
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name,identity in terminal['artifacts'].items():
        assert artifact(out/name) == identity, 'terminal body: ' + name
    assert terminal['source_qualification_sha256'] == terminal['artifacts'].get('source-qualification.json',{}).get('sha256')
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out/'source-qualification.json').read_bytes()) == dict(proof,
            source_commit=terminal['source_commit'], source_archive_sha256=terminal['source_archive_sha256'])
    if 'config.json' in terminal['artifacts']:
        config_body = (out/'config.json').read_bytes()
        assert sha(config_body) == proof['config_sha256']
        config = json.loads(config_body)
        assert config['controller_authority_pending'] is False and config['refs'] == REFS
        assert config['controller_code_sha256'] == proof['code_sha256']
        assert all(type(config[k]) is type(v) and config[k] == v for k,v in FIXED.items())
    for name,pointer in REFS.items():
        if f'inputs/{name}.json' in terminal['artifacts']:
            assert artifact(out/f'inputs/{name}.json') == {k:pointer[k] for k in ('bytes','sha256')}
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['exit_code'] == 0 and terminal['phase'] == 'complete'
    assert terminal['status'] == ('complete' if complete else 'failed')
    if complete:
        assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(ARTIFACTS), 'exact complete roster'
        ledger = json.loads((out/'input-hashes.json').read_bytes())
        shards = ranked_shards(*(out/f'inputs/{name}.json' for name in ('registry','population','overlap')))
        assert ledger == dict(refs=REFS, objects=[dict(rank=i,path=s['path'],uri=s['uri'],
            bytes=s['encoded_bytes'],sha256=s['sha256']) for i,s in enumerate(shards)])
        receipt = json.loads((out/'selection-receipt.json').read_bytes())
        assert receipt['schema'] == 'borsuk-semantic-panel-ids-selection-receipt-v1'
        assert receipt['selected'] is receipt['metadata_only'] is True
        assert receipt['embeddings_truth_or_ann_opened'] is False
        assert type(receipt['exit_status']) is int and receipt['exit_status'] == 0
        for key in ('config_sha256','code_identity_sha256'):
            assert receipt[key] == proof[key]
        for key in ('source_commit','source_archive_sha256'):
            assert receipt[key] == terminal[key]
        assert receipt['panel'] == artifact(out/'panel.json') and receipt['input_hashes'] == artifact(out/'input-hashes.json')
        command = receipt['command']
        assert type(command) is list and len(command) == 12
        assert command[1:4] == ['-m','scripts.select_v36_rank16_fresh_ids','--scratch']
        assert [command[i] for i in (5,7,8,10,11)] == [
            '--output','--next64','--previous','--previous-sha256',REFS['old_panel']['sha256']]
        assert Path(command[6]).is_absolute() and Path(command[6]).name == 'panel.json'
        assert command[4] == str(Path(command[6]).parent/'scratch')
        assert Path(command[9]).is_absolute() and command[9].endswith('/'+REFS['old_panel']['path'])
        versions = json.loads((out/'tool-versions.json').read_bytes())
        assert command[0] == versions['executable'] and Path(command[0]).is_absolute()
        assert versions['pyarrow'] == '24.0.0' and versions['machine'] == 'x86_64'
        assert versions['python'].startswith('3.12')
        assert versions['os_release']['ID'] == 'ubuntu' and versions['os_release']['VERSION_ID'] == '24.04'
        validate_cgroup(json.loads((out/'selection-cgroup.json').read_bytes()))
        validate_panel(out)
    return dict(selected=complete, metadata_only=True, native_qualification_claim=False,
        exit_status=terminal['original_exit_code'])


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    raw = s3.get_object(Bucket=BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    for name,identity in terminal['artifacts'].items():
        source = s3.get_object(Bucket=BUCKET, Key=prefix+'/artifacts/'+name)['Body']
        path = out/name
        path.parent.mkdir(parents=True, exist_ok=True)
        with source, path.open('wb') as output, gzip.GzipFile(filename=str(path)+'.gz', mode='wb', mtime=0) as archived:
            for chunk in iter(lambda:source.read(1024*1024), b''):
                output.write(chunk); archived.write(chunk)
            output.flush(); os.fsync(output.fileno())
        assert artifact(path) == identity, 'collected body: ' + name
    replay(out)
    return terminal


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}', attempt), 'attempt must be aNNNN'
    shared, _ = lifecycle()
    return shared.main(attempt, campaign=sys.modules[__name__])


def self_check():
    """Mock downloads/selector/AWS; never open shards, build or select real IDs."""
    import ast
    import copy
    import tempfile
    from types import SimpleNamespace, ModuleType
    from unittest.mock import Mock
    module = sys.modules[__name__]
    here = Path(__file__).resolve().parents[1]

    def rejects(action):
        try:
            action()
        except (AssertionError, ValueError, KeyError, FileNotFoundError):
            return
        raise AssertionError('invalid authority accepted')

    # Check the exact AST import closure, including imports inside functions.
    seen = set()
    def closure(name):
        if name in seen:
            return
        seen.add(name)
        for node in ast.walk(ast.parse((here/name).read_bytes())):
            targets = []
            if isinstance(node, ast.ImportFrom):
                targets = [a.name for a in node.names] if node.module == 'scripts' else (
                    [node.module[8:]] if node.module and node.module.startswith('scripts.') else [])
            if isinstance(node, ast.Import):
                targets = [a.name[8:] for a in node.names if a.name.startswith('scripts.')]
            for target in targets:
                path = 'scripts/' + target.replace('.', '/') + '.py'
                if (here/path).is_file():
                    closure(path)
    closure(RUNTIME_CODE[-1])
    assert seen == set(CODE), 'transitive code closure drift'
    rejects(lambda: qualify(here)) if not (here/CONFIG).exists() else None
    counters = {'cgroup':'/synthetic/selection', 'observer_pid':42, 'process_ids':[42],
        'memory.max':str(MEMORY), 'memory.peak':'4096', 'memory.swap.max':'0',
        'memory.swap.peak':'0', 'memory.events':'max 0\noom 0\noom_kill 0\noom_group_kill 0\n',
        'memory.swap.events':'max 0\nfail 0\n', 'cpu.max':'200000 100000',
        'cpu.stat':'usage_usec 42\n', 'pids.max':'512', 'pids.current':'1', 'pids.events':'max 0\n'}
    shared, bootstrap = lifecycle()
    with patch.object(shared.boto3, 'Session', side_effect=AssertionError('AWS forbidden')), \
            patch('urllib.request.urlopen', side_effect=AssertionError('download forbidden')):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)/'repo'
            for name in (*CODE, *(r['path'] for r in REFS.values())):
                path = repo/name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes((here/name).read_bytes())
            config = dict(FIXED, controller_authority_pending=False, refs=REFS,
                controller_code_sha256={name:artifact(repo/name)['sha256'] for name in CODE})
            path = repo/CONFIG
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(encoded(config))
            proof = qualify(repo)
            for key,value in [('controller_authority_pending', True), ('queries', 63),
                              ('memory_bytes', MEMORY+1), ('reservoir_ordinals', [0,63]),
                              ('versions', {'pyarrow':'23.0.0'})]:
                path.write_bytes(encoded(dict(config, **{key:value})))
                rejects(lambda: qualify(repo))
            path.write_bytes(encoded(config))
            for name in (CODE[0], REFS['old_panel']['path'], REFS['registry']['path']):
                original = (repo/name).read_bytes()
                (repo/name).write_bytes(original + b'\n')
                rejects(lambda: qualify(repo))
                (repo/name).write_bytes(original)
            source = dict(source_commit='0'*40, source_archive_sha256='1'*64)
            environment = dict(BORSUK_PANEL_CONFIG_SHA256=proof['config_sha256'],
                BORSUK_PANEL_SOURCE_COMMIT=source['source_commit'],
                BORSUK_PANEL_ARCHIVE_SHA256=source['source_archive_sha256'])
            shards = ranked_shards(*(repo/REFS[n]['path'] for n in ('registry','population','overlap')))
            old = json.loads((repo/REFS['old_panel']['path']).read_bytes())['selected']
            # Synthetic successor rows, never a corpus selection or claimed real panel.
            rows = [dict(query_ordinal=i, feature_row_id=2**63+i, source_rank=16,
                source_row_offset=i, selector_sha256=sha(SEED+(2**63+i).to_bytes(8,'little')))
                for i in range(64)]
            rows.sort(key=lambda r:(r['selector_sha256'],r['feature_row_id']))
            reservoir = old + [dict(r, query_ordinal=1000+i) for i,r in enumerate(rows)]
            fake_panel = dict(schema='borsuk-semantic-1m-fresh-panel-ids-v1', metadata_only=True,
                query_embeddings_or_gt_opened=False, prior_query_audit_pass=False,
                qualification=False, complete_historical_coverage=False, selector_seed=SEED.decode(),
                source_registry_sha256=REFS['registry']['sha256'],
                population_authority_sha256=REFS['population']['sha256'],
                physical_id_report_sha256=REFS['overlap']['sha256'],
                consumed_panel_sha256=REFS['old_panel']['sha256'],
                consumed_prefix_tuple_sha256=tuple_digest(old), reservoir_tuple_sha256=tuple_digest(reservoir),
                reservoir_ordinals=[1000,1063], development_ordinals=[0,63], sealed_ordinals=[],
                selected=[dict(r,query_ordinal=i,reservoir_ordinal=1000+i) for i,r in enumerate(rows)])
            versions = dict(python='3.12 synthetic', executable=sys.executable, pyarrow='24.0.0', machine='x86_64',
                os_release={'ID':'ubuntu','VERSION_ID':'24.04'})
            outputs = []
            for failure in ('success','body','selector','interrupt','config-pin','scratch'):
                out = Path(tmp)/failure
                out.mkdir()
                outputs.append(out)
                if failure == 'scratch':
                    (out/'scratch').mkdir()
                acquired = []
                def acquire(shard, scratch):
                    assert scratch == out/'scratch' and not scratch.is_relative_to(repo)
                    acquired.append(shard)
                    result = scratch/Path(shard['path']).name
                    result.write_bytes(b'synthetic shard, not parquet')
                    return result
                def digest(path):
                    shard = next(s for s in shards if Path(s['path']).name == path.name)
                    return (shard['encoded_bytes'], '0'*64 if failure == 'body' else shard['sha256'])
                def selector(command, **kw):
                    assert command == [sys.executable,'-m','scripts.select_v36_rank16_fresh_ids',
                        '--scratch',str(out/'scratch'),'--output',str(out/'panel.json'),'--next64',
                        '--previous',str(repo/REFS['old_panel']['path']),'--previous-sha256',REFS['old_panel']['sha256']]
                    assert kw['cwd'] == repo and kw['env']['PYTHONPATH'] == str(repo)
                    assert acquired == shards, 'exactly 32 ranked authenticated downloads first'
                    if failure == 'interrupt':
                        raise KeyboardInterrupt
                    (out/'panel.json').write_bytes(encoded(fake_panel))
                    kw['stdout'].write('synthetic selector log\n')
                    return subprocess.CompletedProcess(command, 17 if failure == 'selector' else 0)
                env = dict(environment, BORSUK_PANEL_CONFIG_SHA256='0'*64) if failure == 'config-pin' else environment
                with patch.object(module,'capture_cgroup', return_value=counters), \
                        patch.object(module,'tool_versions',return_value=versions), \
                        patch.object(module,'metadata',return_value=SimpleNamespace(acquire=acquire,digest=digest)), \
                        patch.object(subprocess,'run',side_effect=selector), patch.dict(os.environ, env):
                    try:
                        stage(repo,out)
                    except (AssertionError,KeyboardInterrupt,FileExistsError):
                        assert failure != 'success'
                    else:
                        assert failure == 'success'
                receipt = json.loads((out/'selection-receipt.json').read_bytes())
                assert receipt['selected'] is (failure == 'success')
                assert json.loads((out/'selection-cgroup.json').read_bytes())['closed'] is True
                assert len(acquired) == (1 if failure == 'body' else 0 if failure in ('config-pin','scratch') else 32)
            out = outputs[0]
            for name in ('test-resources.txt','run-closed.log'):
                (out/name).write_text('synthetic closed resource/log\n')
            nodes = {'0':{'instance_id':'i-original'},'1':{'instance_id':'i-extra'}}
            reservation = dict(schema=SCHEMA, qualification=proof, **source)
            launch = dict(instance_id='i-original', nodes=nodes, **source)
            terminal = dict(schema=SCHEMA, instance_id='i-original', **source,
                **{k:proof[k] for k in TERMINAL_IDENTITIES}, phase='complete', status='complete',
                exit_code=0, original_exit_code=0, artifacts={n:artifact(out/n) for n in ARTIFACTS},
                source_qualification_sha256=artifact(out/'source-qualification.json')['sha256'])
            for name,value in [('aws-reservation.json',reservation),('aws-launch.json',launch),
                               ('aws-closeout.json',dict(state='terminated',nodes=nodes)),
                               ('aws-terminal.json',terminal)]:
                (out/name).write_bytes(encoded(value))
            assert replay(out)['selected']
            for name in ARTIFACTS:
                original = (out/name).read_bytes()
                (out/name).write_bytes(original+b'tampered')
                rejects(lambda: replay(out))
                (out/name).write_bytes(original)
            for key,value in [('source_commit','f'*40),('code_identity_sha256','f'*64),
                              ('config_sha256','f'*64),('instance_id','i-unowned')]:
                (out/'aws-terminal.json').write_bytes(encoded(dict(terminal, **{key:value})))
                rejects(lambda: replay(out))
            (out/'aws-terminal.json').write_bytes(encoded(terminal))
            for key,value in [('selected', fake_panel['selected'][:-1]),
                              ('consumed_prefix_tuple_sha256','0'*64),
                              ('selected',[dict(fake_panel['selected'][0],source_rank=0),*fake_panel['selected'][1:]]),
                              ('selected',[dict(fake_panel['selected'][0],feature_row_id=True),*fake_panel['selected'][1:]])]:
                (out/'panel.json').write_bytes(encoded(dict(fake_panel, **{key:value})))
                rejects(lambda: validate_panel(out))
            (out/'panel.json').write_bytes(encoded(fake_panel))
            bad = copy.deepcopy(counters)
            bad['memory.events'] = bad['memory.events'].replace('oom 0','oom 1')
            rejects(lambda: validate_cgroup(dict(before=counters,after=bad,closed=True)))
            bad = dict(counters,process_ids=[42,43])
            rejects(lambda: validate_cgroup(dict(before=counters,after=bad,closed=True)))
            # Authenticate streaming collection and failed terminals, after closure only.
            bodies = {n:(out/n).read_bytes() for n in ARTIFACTS}
            def get_object(**kw):
                key = kw['Key']
                data = encoded(terminal) if key.endswith('/terminal.json') else bodies[key.split('/artifacts/',1)[1]]
                return {'Body':io.BytesIO(data)}
            s3 = Mock()
            s3.get_object.side_effect = get_object
            collect(s3,'synthetic',out,'i-original',source['source_commit'],source['source_archive_sha256'])
            assert s3.get_object.call_count == len(ARTIFACTS)+1
            assert all(gzip.decompress((out/(n+'.gz')).read_bytes()) == b for n,b in bodies.items())
            close_body = (out/'aws-closeout.json').read_bytes()
            (out/'aws-closeout.json').write_bytes(encoded(dict(state='running',nodes=nodes)))
            s3.reset_mock()
            rejects(lambda: collect(s3,'synthetic',out,'i-original',source['source_commit'],source['source_archive_sha256']))
            s3.get_object.assert_not_called()
            (out/'aws-closeout.json').write_bytes(close_body)
            s3.get_object.side_effect = lambda **kw: {'Body':io.BytesIO(encoded(terminal)
                if kw['Key'].endswith('/terminal.json') else b'tampered')}
            rejects(lambda: collect(s3,'synthetic',out,'i-original',source['source_commit'],source['source_archive_sha256']))
            s3.get_object.side_effect = get_object
            collect(s3,'synthetic',out,'i-original',source['source_commit'],source['source_archive_sha256'])
            incomplete = dict(terminal, artifacts={n:v for n,v in terminal['artifacts'].items() if n != 'panel.json'})
            (out/'aws-terminal.json').write_bytes(encoded(incomplete))
            rejects(lambda: replay(out))
            for code in (17,97):
                terminal.update(phase='selection',status='failed',exit_code=code,original_exit_code=code)
                (out/'aws-terminal.json').write_bytes(encoded(terminal))
                assert replay(out) == dict(selected=False,metadata_only=True,native_qualification_claim=False,exit_status=code)
            failed_partial = dict(terminal, artifacts={'run-closed.log':terminal['artifacts']['run-closed.log']},
                source_qualification_sha256=None)
            (out/'aws-terminal.json').write_bytes(encoded(failed_partial))
            assert replay(out)['exit_status'] == 97 and replay(out)['selected'] is False
            terminal.update(phase='complete',status='complete',exit_code=0,original_exit_code=0)
            body = user_data(source['source_commit'],source['source_archive_sha256'],
                'sources/synthetic.tar.gz',PREFIX+'a0001',proof)
            assert '--on-active=4500s' in body and 'RuntimeMaxSec=3660' in body
            assert 'MemoryMax=8G' in body and 'MemorySwapMax=0' in body and 'CPUQuota=200%' in body and 'TasksMax=512' in body
            assert 'timeout --signal=TERM --kill-after=30 3600' in body
            assert '--setenv=PYTHONPATH="$root/repo"' in body and '-p WorkingDirectory="$root"' in body
            assert 'pyarrow==24.0.0' in body and '--no-deps' in body and 'python3.12-venv' in body
            assert all(token not in body for token in ('rustup','cargo','--publish','unused','native-qualification'))
            assert len(body.encode()) < 16384
            # Execute only the service shell with stubbed native commands, from nonrepo CWD.
            shell = body[body.index('systemd-run --unit=semantic-panel-ids '):body.index('for name in $ARTIFACT_NAMES; do',body.index('phase=selection'))]
            stub = 'systemd-run() { printf "%s\\n" "$@"; }; root=/synthetic; ' + shell
            result = subprocess.run(['bash','-c',stub],cwd=tmp,text=True,capture_output=True,check=True)
            assert 'PYTHONPATH=/synthetic/repo' in result.stdout and '--stage\n/synthetic/repo\n/synthetic' in result.stdout
            assert result.stderr == ''
            # Invoke the portable CLI from outside the repo; missing config stops before metadata import.
            empty = Path(tmp)/'empty'
            empty.mkdir()
            env = dict(os.environ, PYTHONPATH=str(here), **environment)
            result = subprocess.run([sys.executable,'-m',MODULE,'--stage',str(empty),str(Path(tmp)/'unused')],
                cwd=tmp,env=env,capture_output=True,text=True)
            assert result.returncode != 0 and 'owned output' in result.stderr
            result = subprocess.run([sys.executable,'-c',
                'import runpy,sys; runpy.run_path(sys.argv[1]); '
                'assert not any(m in sys.modules for m in ("boto3","pyarrow","scripts.run_native_semantic_router_cold"))',
                str(here/RUNTIME_CODE[-1])],cwd=tmp,capture_output=True,text=True,check=True)
            assert result.stdout == result.stderr == ''

        # Test the existing prefix guard on synthetic tuples, with PyArrow disabled.
        arrow, parquet = ModuleType('pyarrow'), ModuleType('pyarrow.parquet')
        arrow.parquet = parquet
        with patch.dict(sys.modules, {'pyarrow':arrow,'pyarrow.parquet':parquet}):
            from scripts import select_v36_rank16_fresh_ids as selector
            assert selector.continuation(reservoir,old) == fake_panel['selected']
            rejects(lambda: selector.continuation(reservoir[:-1],old))
            changed = copy.deepcopy(reservoir)
            changed[0]['source_row_offset'] += 1
            rejects(lambda: selector.continuation(changed,old))
            changed = copy.deepcopy(reservoir)
            changed[1000]['feature_row_id'] = old[0]['feature_row_id']
            rejects(lambda: selector.continuation(changed,old))
        # The shared mock suite exercises fsync failure, multi-ACK, interruption,
        # cleanup retries, and terminate + waiter strictly before collection.
        with contextlib.redirect_stdout(io.StringIO()):
            shared.self_check(lifecycle_only=True)
        # Campaign attributes reach the shared SDK lifecycle, including all ACKs.
        from datetime import datetime, timezone
        for interrupted in (False,True):
            with tempfile.TemporaryDirectory() as tmp:
                ec2, s3, session = Mock(), Mock(), Mock()
                session.client.side_effect = [ec2,s3]
                ec2.describe_instances.return_value = {'Reservations':[]}
                ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'mock-az'}]}
                ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[
                    {'SpotPrice':'0.1','Timestamp':datetime.now(timezone.utc)}]}
                ec2.run_instances.return_value = {'Instances':[{'InstanceId':'i-original'},{'InstanceId':'i-extra'}]}
                events = []
                ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
                ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
                def collected(*args):
                    assert events == ['terminate','wait']
                    events.append('collect')
                    return dict(status='complete',phase='complete',exit_code=0,artifacts={n:{} for n in ARTIFACTS})
                with patch.object(module,'ROOT',Path(tmp)), patch.object(module,'preflight',return_value=proof), \
                        patch.object(module,'user_data',return_value='mock'), \
                        patch.object(module,'poll',side_effect=KeyboardInterrupt() if interrupted else None), \
                        patch.object(module,'collect',side_effect=collected), \
                        patch.object(shared.boto3,'Session',return_value=session), \
                        patch.object(subprocess,'check_output',side_effect=['','0'*40,b'archive']), \
                        patch.object(shared.peer,'missing',return_value=True), \
                        patch.object(shared.peer,'put_if_absent'), contextlib.redirect_stdout(io.StringIO()):
                    try:
                        main('a0001')
                    except KeyboardInterrupt:
                        assert interrupted
                    else:
                        assert not interrupted
                assert events == ['terminate','wait','collect']
                kwargs = ec2.run_instances.call_args.kwargs
                assert kwargs['InstanceType'] == INSTANCE_TYPE and kwargs['ImageId'] == IMAGE_ID
                assert kwargs['NetworkInterfaces'][0]['SubnetId'] == SUBNET
                assert kwargs['BlockDeviceMappings'] == [{'DeviceName':'/dev/sda1','Ebs':{
                    'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}]
                assert kwargs['InstanceMarketOptions']['SpotOptions']['MaxPrice'] == '0.50'
                ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-original','i-extra'])
                ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-original','i-extra'])
                assert json.loads((Path(tmp)/'a0001/aws-launch.json').read_bytes())['nodes'] == nodes
                reserved = json.loads((Path(tmp)/'a0001/aws-reservation.json').read_bytes())
                assert reserved['wall_seconds'] == WALL and reserved['compute_cap_usd'] == COMPUTE_CAP
                assert reserved['ebs_s3_allowance_usd'] == .15
        with patch.object(shared,'main') as delegated:
            main('a0001')
            delegated.assert_called_once_with('a0001', campaign=module)
        rejects(lambda: main('a０００１'))
    print(f'PASS metadata-only synthetic stage/replay/body/prefix/CLI/lifecycle checks; {len(CODE)} code pins, {len(ARTIFACTS)} artifacts; user-data {len(body.encode())} bytes')


if __name__ == '__main__':
    args = sys.argv[1:]
    if args == ['--self-check']:
        self_check()
    elif len(args) == 3 and args[0] == '--stage':
        stage(Path(args[1]), Path(args[2]))
    elif len(args) == 2 and args[0] == '--replay':
        print(json.dumps(replay(Path(args[1])), sort_keys=True))
    else:
        assert len(args) == 1, 'usage: aNNNN | --self-check | --stage REPO OUTPUT | --replay OUTPUT'
        with open('/tmp/borsuk-semantic-panel-ids-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(args[0])
