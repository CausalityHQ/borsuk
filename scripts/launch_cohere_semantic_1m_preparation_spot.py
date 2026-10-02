"""One root-authorized CoHere preparation Spot; never build or measure ANN.

CLI: aNNNN | --self-check | --stage REPO OUTPUT PREFIX | --replay OUTPUT.
Root supplies cohere-panel-tools/preparation-spot-config.json: exactly FIXED
plus authority_pending=false and controller_code_sha256={every CODE path: SHA}.
The nested helper_config is the immutable full-body pointer below. No config
is created here. --stage invokes only Python3.12's preparation helper, with
CONFIG CONFIGSHA REPO NEW_OUTPUT PREFIX, under the prospective cgroup.
failure.json is always present: status pending, failed, or complete; failed
construction is never promoted by successful upload or authenticated receipts.
"""
import fcntl
import hashlib
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import prepare_cohere_semantic_1m_panel as helper

ROOT = Path(helper.BASE)
CONFIG = ROOT / 'cohere-panel-tools/preparation-spot-config.json'
HELPER_CONFIG = dict(path=str(ROOT / 'cohere-panel-tools/preparation-config.json'),
    bytes=15208, sha256='21804f730d1044e5798e391949d62abd588efab2a09b5883d258adf4cc175dff')
NAME = 'cohere-preparation'
MODULE = 'scripts.launch_cohere_semantic_1m_preparation_spot'
SCHEMA = 'borsuk-cohere-semantic-1m-preparation-spot-v1'
PREFIX = 'research/semantic-router/20261002/cohere-fresh64-preparation-'
TOKEN_PREFIX, TAG = 'cohere-fresh64-preparation-', 'borsuk-cohere-fresh64-preparation'
WALL, WORKER_SECONDS, SERVICE_SECONDS = 5400, 4800, 4860
MEMORY, SCRATCH = 2 << 30, 8 << 30
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME, SUBNET = '/dev/sda1', 'subnet-034528fbd6977848f'
REGION, BUCKET = 'eu-central-1', 'borsuk-bench-453182569524-euc1'
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .75
AWSCLI_VERSION = '2.36.11'
AWSCLI_SHA256 = '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6'
THREAD_ENV = ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS',
              'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS')
CONTROLLER_CODE = tuple('scripts/' + name + '.py' for name in (
    'launch_cohere_semantic_1m_preparation_spot',
    'check_native_metadata_ranges_stats', 'check_native_semantic_router_stats',
    'check_native_startup_build', 'check_native_startup_stats',
    'launch_native_metadata_ranges_cold_spot', 'launch_native_peer_1m_spot',
    'launch_native_semantic_router_cold_spot', 'launch_native_startup_profile_spot',
    'launch_v157_primary_feasibility_spot', 'launch_v174_relaid_bind_compile_spot',
    'package_semantic_native_generation', 'prepare_native_semantic_publication',
    'rest_coexistence_load', 'run_native_cold_first_query',
    'run_native_metadata_ranges_cold', 'run_native_peer_1m_worker',
    'run_native_peer_offered_http', 'run_native_semantic_router_cold',
    'run_native_union_http', 'run_native_union_offered_http'))
CODE = tuple(sorted(set((*helper.CODE, *CONTROLLER_CODE))))
HELPER_OUTPUTS = ('queries.raw', 'requests.jsonl', 'truth.u32', 'truth.i64',
    'panel.json', 'duplicate-audit.json', 'oracle.json', 'resources.json', 'decision.json')
ARTIFACTS = ('config.json', 'helper-config.json', 'source-qualification.json',
    'cpu.txt', 'tool-versions.json', 'run-closed.log', 'profile.log',
    'profile-resources.txt', 'profile-cgroup.json', 'preparation-closure.json',
    'failure.json', *('screen/' + n for n in (*HELPER_OUTPUTS,
    'final-resources.json', 'seal-readback.json', 'failure.json')))
COMPLETE_ARTIFACTS = ARTIFACTS
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256',
    'helper_config_sha256', 'refs_identity_sha256', 'artifact_roster_sha256',
    'campaign_schema', 'awscli_version', 'awscli_sha256',
    'ann_quality_measured', 'native_qualification_claim', 'complete_historical_coverage')
FIXED = dict(schema=SCHEMA, architecture='x86_64', region=REGION, bucket=BUCKET,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, volume_gib=80, volume_type='gp3', encrypted=True,
    delete_on_termination=True, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
    compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=.15, memory_bytes=MEMORY,
    scratch_bytes=SCRATCH, swap_bytes=0, cpu_quota_percent=200, tasks_max=512,
    worker_limit_seconds=WORKER_SECONDS, service_limit_seconds=SERVICE_SECONDS,
    machine_limit_seconds=WALL, versions=helper.VERSIONS, threads=2,
    aws_max_attempts=1, helper_config=HELPER_CONFIG, fixed_queries=64,
    selected_shards=59, extraction_shards=61, quality_peek_allowed=False,
    ann_quality_measured=False, native_qualification_claim=False,
    complete_historical_coverage=False)


def encoded(value):
    return helper.canonical(value)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def artifact(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), 'regular body required: ' + str(path)
    ident = helper.local_identity(path)
    return {k:ident[k] for k in ('bytes', 'sha256')}


def write(path, value):
    path = Path(path)
    with path.open('wb') as stream:
        stream.write(value if isinstance(value, bytes) else encoded(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())


def read(base, pointer):
    path = helper.repo_path(Path(base), pointer['path'])
    assert artifact(path) == {k:pointer[k] for k in ('bytes', 'sha256')}, 'pinned body: ' + pointer['path']
    return path.read_bytes()


def qualify(base=Path('.')):
    base = Path(base).resolve()
    body = (base / CONFIG).read_bytes()  # Missing/pending authority fails before SDK import.
    config = json.loads(body)
    assert set(config) == set(FIXED) | {'authority_pending', 'controller_code_sha256'}, 'exact config fields'
    assert config['authority_pending'] is False, 'root authority freeze pending'
    assert all(encoded(config[k]) == encoded(v) for k,v in FIXED.items()), 'fixed preparation protocol'
    nested = json.loads(read(base, HELPER_CONFIG))
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive controller code roster'
    for name,digest in code.items():
        assert artifact(helper.repo_path(base, name))['sha256'] == digest, 'controller source drift: ' + name
    assert set(nested['code_sha256']) == set(helper.CODE)
    assert all(code[name] == digest for name,digest in nested['code_sha256'].items()), 'helper source drift'
    assert nested['quality_peek_allowed'] is nested['complete_historical_coverage'] is False
    assert nested['limits'] == helper.LIMITS and nested['versions'] == helper.VERSIONS
    refs = {name:json.loads(read(base, pointer)) for name,pointer in nested['refs'].items()}
    assert all(nested['refs'][name] == pointer for name,pointer in helper.FIXED.items()), 'original metadata pins'
    scripts = str(Path(helper.__file__).resolve().parent)
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    from scripts import select_cohere_1m_fresh64
    helper.selector = select_cohere_1m_fresh64
    helper.validate_fixed(refs['metadata_authority'], refs['panel'], refs['root_freeze'])
    selected = refs['panel']['selected']
    assert len(selected) == 64 and len({r['shard_ordinal'] for r in selected}) == 59
    assert len({r['shard_ordinal'] for r in selected} | {45,46}) == 61
    assert helper.value_sha(selected) == refs['panel']['selected_sha256'], 'immutable locators'
    return dict(config_path=str(CONFIG), config_sha256=sha(body), helper_config=HELPER_CONFIG,
        helper_config_sha256=HELPER_CONFIG['sha256'], code_sha256=code,
        code_identity_sha256=sha(encoded(code)), refs=nested['refs'],
        refs_identity_sha256=sha(encoded(nested['refs'])), campaign_schema=SCHEMA,
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), ann_quality_measured=False,
        native_qualification_claim=False, complete_historical_coverage=False,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    return proof


def lifecycle():
    from scripts import launch_native_metadata_ranges_cold_spot as shared
    from scripts import launch_native_semantic_router_cold_spot as bootstrap
    return shared, bootstrap


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    import base64
    import re
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch('[a-zA-Z0-9/._-]+', archive_key) and '..' not in archive_key.split('/')
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    assert qualification['config_path'] == str(CONFIG) and qualification['campaign_schema'] == SCHEMA
    _, bootstrap = lifecycle()
    adapter = {k:qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, '_offered', return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    proof = dict(qualification, source_commit=commit, source_archive_sha256=archive_sha)
    packed = base64.b64encode(gzip.compress(encoded(proof), mtime=0)).decode()
    env = ' '.join('--setenv=' + name + '=2' for name in THREAD_ENV)
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("{packed}")))'
cp "$root/repo/{CONFIG}" config.json
cp "$root/repo/{HELPER_CONFIG['path']}" helper-config.json
lscpu >cpu.txt
phase=preparation
systemd-run --unit=cohere-fresh64-preparation --wait --pipe -p MemoryMax=2G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=4860 -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C {env} \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 4800 \\
 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root" {prefix} >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/cohere-fresh64-preparation')
    body = body.replace('python3-boto3 python3.12', 'python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    # Failure exists even if apt, source download, or the helper never starts.
    marker = 'exec >run.log 2>&1\n'
    assert body.count(marker) == 1
    body = body.replace(marker, marker + "printf '%s\\n' '{\"schema\":\"borsuk-cohere-preparation-failure-v1\",\"status\":\"pending\",\"replacement_allowed\":false}' >failure.json\n")
    marker = '  cp run.log run-closed.log || code=96\n'
    assert body.count(marker) == 1
    body = body.replace(marker, '''  if [ "$phase" != complete ] || [ "$original_code" != 0 ]; then
    FAILURE_CODE="$original_code" FAILURE_PHASE="$phase" python3.12 - <<'FAILURE' || code=96
import json,os
from pathlib import Path
p=Path('failure.json'); value=json.loads(p.read_text()) if p.exists() else {}
value.update(schema='borsuk-cohere-preparation-failure-v1',status='failed',replacement_allowed=False,bootstrap_phase=os.environ['FAILURE_PHASE'],bootstrap_exit_code=int(os.environ['FAILURE_CODE']))
p.write_text(json.dumps(value,sort_keys=True,separators=(',',':'))+'\\n')
FAILURE
  fi
''' + marker)
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<terminal>', 'exec')
    assert len(body.encode()) < 16384, 'EC2 user-data limit'
    assert all(n not in body for n in ('rustup', 'cargo', 'unused', '--publish', '--debug', 'two_bit_http'))
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = lifecycle()
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def capture_cgroup():
    relative = Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    group = Path('/sys/fs/cgroup') / relative
    names = ('memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
             'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat',
             'pids.max', 'pids.current', 'pids.events')
    return dict(cgroup=str(group), observer_pid=os.getpid(),
        process_ids=[int(pid) for pid in (group / 'cgroup.procs').read_text().split()],
        **{name:(group / name).read_text() for name in names})


def validate_cgroup(report):
    assert report['closed'] is True
    for counters in (report['before'], report['after']):
        assert int(counters['memory.max']) == MEMORY and 0 <= int(counters['memory.peak']) <= MEMORY
        assert int(counters['memory.swap.max']) == int(counters['memory.swap.peak']) == 0
        quota, period = map(int, counters['cpu.max'].split())
        assert period > 0 and quota == 2 * period and int(counters['pids.max']) == 512
        assert 0 < int(counters['pids.current']) <= 512 and counters['cpu.stat'].strip()
        assert counters['observer_pid'] in counters['process_ids']
        for name in ('memory.events', 'memory.swap.events', 'pids.events'):
            events = dict(line.split() for line in counters[name].splitlines())
            failures = ('oom', 'oom_kill', 'oom_group_kill') if name == 'memory.events' else events
            assert all(int(events.get(k, 0)) == 0 for k in failures), 'resource failure: ' + name
    assert report['before']['cgroup'] == report['after']['cgroup']
    assert set(report['after']['process_ids']) <= set(report['before']['process_ids']), 'helper descendants remain'


def tools():
    import importlib.metadata
    import platform
    assert sys.version_info[:2] == (3,12), 'Python3.12 required'
    release = platform.freedesktop_os_release()
    assert platform.machine() == 'x86_64' and release['ID'] == 'ubuntu' and release['VERSION_ID'] == '24.04'
    versions = {n:importlib.metadata.version(n) for n in helper.VERSIONS}
    assert versions == helper.VERSIONS, 'numerical dependency versions'
    assert all(os.environ.get(n) == '2' for n in THREAD_ENV), 'thread admission'
    assert os.environ.get('AWS_MAX_ATTEMPTS') == '1', 'retry admission'
    return dict(python=sys.version, executable=sys.executable, versions=versions,
        architecture=platform.machine(), os_release=release, threads=2, aws_max_attempts=1)


def stage(repo, out, prefix):
    """Service observer; the unchanged helper owns all data and scientific work."""
    import re
    import signal
    repo, out = Path(repo).resolve(), Path(out).resolve()
    signal.signal(signal.SIGTERM, helper.terminate)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix), 'preparation prefix'
    status = dict(schema='borsuk-cohere-preparation-failure-v1', status='pending',
                  replacement_allowed=False, ann_quality_measured=False)
    write(out / 'failure.json', status)
    closure = dict(schema='borsuk-cohere-preparation-closure-v1', closed=False,
        helper_exit_code=None, helper_invocations=0, prefix=prefix,
        native_build_invocations=0, native_search_invocations=0,
        native_publication_invocations=0, gt_reuse=False, ann_quality_measured=False)
    counters = dict(closed=False, before=None, after=None)
    started = time.monotonic()
    try:
        proof = qualify(repo)
        recorded = json.loads((out / 'source-qualification.json').read_bytes())
        source = {k:recorded[k] for k in ('source_commit', 'source_archive_sha256')}
        assert re.fullmatch('[0-9a-f]{40}', source['source_commit'])
        assert re.fullmatch('[0-9a-f]{64}', source['source_archive_sha256'])
        assert recorded == dict(proof, **source), 'bootstrap source qualification'
        assert artifact(out / 'config.json')['sha256'] == proof['config_sha256']
        assert artifact(out / 'helper-config.json') == {k:HELPER_CONFIG[k] for k in ('bytes','sha256')}
        closure.update(config_sha256=proof['config_sha256'], helper_config_sha256=HELPER_CONFIG['sha256'], **source)
        write(out / 'tool-versions.json', tools())
        counters['before'] = capture_cgroup()
        validate_cgroup(dict(closed=True, before=counters['before'], after=counters['before']))
        assert not (out / 'screen').exists(), 'fresh helper output required'
        command = [sys.executable, str(repo / helper.CODE[0]), str(repo / HELPER_CONFIG['path']),
                   HELPER_CONFIG['sha256'], str(repo), str(out / 'screen'), prefix]
        closure.update(command=command, helper_invocations=1)
        completed = subprocess.run(command, cwd=repo, check=False)
        closure['helper_exit_code'] = completed.returncode
        assert type(completed.returncode) is int and completed.returncode == 0, 'preparation helper failed'
        counters.update(after=capture_cgroup(), closed=True)
        validate_cgroup(counters)
        write(out / 'profile-cgroup.json', counters)
        status.update(status='complete', helper_exit_code=0)
        write(out / 'screen/failure.json', status)
        closure.update(closed=True, wall_seconds=time.monotonic() - started)
        assert closure['wall_seconds'] <= WORKER_SECONDS
        write(out / 'preparation-closure.json', closure)
        validate_preparation(out, proof, prefix)
        write(out / 'failure.json', status)
        return closure
    except BaseException as error:
        status.update(status='failed', error_type=type(error).__name__, error=str(error),
                      helper_exit_code=closure['helper_exit_code'])
        if (out / 'screen/failure.json').exists():
            status['helper_failure'] = json.loads((out / 'screen/failure.json').read_bytes())
        if (out / 'screen').exists():
            # Never upload raw corpus, consumed inputs, parquet, or credential traces.
            status['cleanup'] = helper.cleanup_failure(out / 'screen')
        write(out / 'failure.json', status)
        raise
    finally:
        closure['wall_seconds'] = time.monotonic() - started
        if counters['before'] is not None and counters['after'] is None:
            counters['after'] = capture_cgroup()
        write(out / 'profile-cgroup.json', counters)
        write(out / 'preparation-closure.json', closure)


def validate_preparation(out, proof, prefix):
    """Authenticate the small closed panel; exhaustive work belongs to the helper."""
    import math
    import struct
    out = Path(out)
    config = json.loads((out / 'helper-config.json').read_bytes())
    assert artifact(out / 'helper-config.json') == {k:HELPER_CONFIG[k] for k in ('bytes','sha256')}
    screen = out / 'screen'
    closure = json.loads((out / 'preparation-closure.json').read_bytes())
    assert closure['schema'] == 'borsuk-cohere-preparation-closure-v1' and closure['closed'] is True
    assert type(closure['helper_exit_code']) is int and closure['helper_exit_code'] == 0
    assert closure['helper_invocations'] == 1 and closure['prefix'] == prefix
    assert all(closure[k] == 0 for k in ('native_build_invocations', 'native_search_invocations', 'native_publication_invocations'))
    assert closure['gt_reuse'] is closure['ann_quality_measured'] is False
    assert 0 <= closure['wall_seconds'] <= WORKER_SECONDS
    assert closure['config_sha256'] == proof['config_sha256'] and closure['helper_config_sha256'] == HELPER_CONFIG['sha256']
    versions = json.loads((out / 'tool-versions.json').read_bytes())
    assert versions['python'].startswith('3.12') and versions['versions'] == helper.VERSIONS
    assert versions['architecture'] == 'x86_64' and versions['os_release']['ID'] == 'ubuntu'
    assert versions['os_release']['VERSION_ID'] == '24.04' and versions['threads'] == 2 and versions['aws_max_attempts'] == 1
    assert closure['command'][0] == versions['executable']
    assert closure['command'][1:] == [str(Path(closure['command'][4]) / helper.CODE[0]),
        str(Path(closure['command'][4]) / HELPER_CONFIG['path']), HELPER_CONFIG['sha256'],
        closure['command'][4], str(Path(closure['command'][4]).parent / 'screen'), prefix], 'exact helper invocation'
    validate_cgroup(json.loads((out / 'profile-cgroup.json').read_bytes()))
    decision = json.loads((screen / 'decision.json').read_bytes())
    assert decision['schema'] == 'borsuk-cohere-semantic-1m-construction-v1'
    assert decision['config_sha256'] == HELPER_CONFIG['sha256'] and decision['prefix'] == prefix
    assert decision['decision'] == 'PASS fixed fresh64 construction only'
    for k in ('refs', 'code_sha256', 'corpus', 'consumed_queries', 'registered_test'):
        assert decision[k] == config[k], 'construction binding: ' + k
    assert decision['qualification'] is decision['ann_quality_measured'] is decision['complete_historical_coverage'] is False
    assert decision['truth_id_space'] == 'source ordinal' and decision['queries'] == 64 and decision['gt_k'] == 100
    assert decision['selected_locators_sha256'] == '379b6421d794fb2d0956e7ed787478b019abfd599e0510be157cdcaa53205abf'
    assert set(decision['artifacts']) == set(HELPER_OUTPUTS) - {'decision.json'}
    expected = dict(decision['artifacts'], **{'decision.json':dict(path='decision.json', **artifact(screen / 'decision.json'))})
    readback = json.loads((screen / 'seal-readback.json').read_bytes())
    assert readback['schema'] == 'borsuk-semantic-1m-seal-readback-v1' and set(readback['artifacts']) == set(expected)
    for name, ident in expected.items():
        assert ident == dict(path=name, **artifact(screen / name)), 'construction body: ' + name
        sealed = readback['artifacts'][name]
        assert sealed['authenticated_readback'] is True and sealed['key'] == prefix + '/sealed/' + name
        assert {k:sealed[k] for k in ('bytes','sha256')} == artifact(screen / name), 'seal body: ' + name
    assert readback['final_resources'] == dict(path='final-resources.json', **artifact(screen / 'final-resources.json'))
    assert artifact(screen / 'panel.json') == {k:config['refs']['panel'][k] for k in ('bytes','sha256')}
    for name in ('resources.json', 'final-resources.json'):
        report = json.loads((screen / name).read_bytes())
        assert report['schema'] == 'borsuk-cohere-preparation-resources-v1' and report['passed'] is True
        assert report['prospective_preparation_limits'] == helper.LIMITS and report['serving_or_build_measurement'] is False
        assert report['ann_quality_measured'] is False and report['aws_max_attempts'] == 1
        assert 0 <= report['wall_seconds'] <= WORKER_SECONDS
        assert 0 < report['process_max_rss_kib'] * 1024 <= MEMORY and report['child_max_rss_kib'] * 1024 <= MEMORY
        assert 0 <= report['actual_scratch_bytes'] <= report['peak_scratch_bytes'] <= SCRATCH
        assert report['transport_accounting_complete'] is True and report['aws_errors'] == 0
        attempts = report['http_request_dispatch_attempts']
        assert set(attempts) == {'GET','HEAD','PUT'} and all(type(v) is int and v >= 0 for v in attempts.values())
        assert sum(attempts.values()) <= 128
        assert report['confirmed_wire_requests'] == report['billed_requests'] == 'UNMEASURED'
    audit = json.loads((screen / 'duplicate-audit.json').read_bytes())
    assert audit['schema'] == 'borsuk-cohere-semantic-1m-vector-audit-v1'
    assert audit['passed'] is True and audit['replacement_allowed'] is audit['complete_historical_coverage'] is False
    assert (audit['new_rows_audited'],audit['indexed_rows_audited'],audit['consumed_rows_audited']) == (64,1000000,4000)
    assert audit['normalization'] == helper.NORMALIZATION
    assert len(audit['raw_sha256']) == len(set(audit['raw_sha256'])) == 64
    assert len(audit['unit_sha256']) == len(set(audit['unit_sha256'])) == 64
    assert audit['config_sha256'] == HELPER_CONFIG['sha256'] and audit['selected_locators_sha256'] == decision['selected_locators_sha256']
    assert audit['consumed_scopes'] == dict(old_sealed1000=1000, source_holdout_prior=2000, registered_test=1000)
    assert audit['within_index_prior_covered_by_first1m'] is True
    oracle = json.loads((screen / 'oracle.json').read_bytes())
    assert oracle['schema'] == 'borsuk-cohere-semantic-1m-oracle-v1' and oracle['passed'] is oracle['oracle_self_check'] is True
    assert (oracle['rows'],oracle['queries'],oracle['k']) == (1000000,64,100)
    assert oracle['exhaustive_block_sort_top100_merge'] is True and oracle['truth_id_space'] == 'source ordinal'
    assert oracle['source_raw'] == config['corpus']['raw']
    for name,k in (('truth.u32','truth_u32'),('truth.i64','truth_i64')):
        assert oracle[k] == dict(path=name, **artifact(screen / name))
    assert artifact(screen / 'queries.raw')['bytes'] == 64 * 768 * 4
    assert artifact(screen / 'truth.u32')['bytes'] == 64 * 100 * 4
    assert artifact(screen / 'truth.i64')['bytes'] == 64 * 100 * 8
    queries = (screen / 'queries.raw').read_bytes()
    assert audit['raw_sha256'] == [sha(queries[i*3072:(i+1)*3072]) for i in range(64)]
    requests = (screen / 'requests.jsonl').read_bytes().splitlines()
    assert len(requests) == 64
    narrow, wide = (screen / 'truth.u32').read_bytes(), (screen / 'truth.i64').read_bytes()
    for i,line in enumerate(requests):
        request = json.loads(line)
        assert set(request) == {'ordinal','query'} and type(request['ordinal']) is int and request['ordinal'] == i
        vector = request['query']
        assert len(vector) == 768 and all(type(v) in (float,int) and math.isfinite(v) for v in vector)
        assert any(vector) and struct.pack('<768f', *vector) == queries[i*3072:(i+1)*3072], 'request f32 parity'
        ids = struct.unpack_from('<100I', narrow, i*400)
        assert len(set(ids)) == 100 and max(ids) < 1000000
        assert ids == struct.unpack_from('<100q', wide, i*800), 'signed source ordinal widening'
    failure = json.loads((screen / 'failure.json').read_bytes())
    assert failure['status'] == 'complete' and failure['helper_exit_code'] == 0


def replay(out):
    out = Path(out)
    reservation, launch, closed, terminal = (json.loads((out / name).read_bytes()) for name in
        ('aws-reservation.json', 'aws-launch.json', 'aws-closeout.json', 'aws-terminal.json'))
    proof = reservation['qualification']
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'same ACKed IDs terminated'
    assert terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert launch['prefix'].startswith(PREFIX) and terminal['schema'] == reservation['schema'] == SCHEMA
    for k in ('source_commit', 'source_archive_sha256'):
        assert terminal[k] == launch[k] == reservation[k], 'source binding: ' + k
    for k in TERMINAL_IDENTITIES:
        assert terminal[k] == proof[k], 'terminal identity: ' + k
    assert proof['helper_config'] == HELPER_CONFIG and proof['helper_config_sha256'] == HELPER_CONFIG['sha256']
    assert set(proof['code_sha256']) == set(CODE) and proof['code_identity_sha256'] == sha(encoded(proof['code_sha256']))
    assert proof['artifact_roster_sha256'] == sha(encoded(ARTIFACTS))
    assert proof['refs_identity_sha256'] == sha(encoded(proof['refs']))
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA
    repo = Path(__file__).resolve().parents[1]
    for name,digest in proof['code_sha256'].items():
        assert artifact(repo / name)['sha256'] == digest, 'replay code drift: ' + name
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    for name,ident in terminal['artifacts'].items():
        assert artifact(out / name) == ident, 'closed body: ' + name
    if 'config.json' in terminal['artifacts']:
        assert artifact(out / 'config.json')['sha256'] == proof['config_sha256']
        config = json.loads((out / 'config.json').read_bytes())
        assert set(config) == set(FIXED) | {'authority_pending','controller_code_sha256'}
        assert config['authority_pending'] is False and config['controller_code_sha256'] == proof['code_sha256']
        assert all(encoded(config[k]) == encoded(v) for k,v in FIXED.items())
    if 'helper-config.json' in terminal['artifacts']:
        assert artifact(out / 'helper-config.json') == {k:HELPER_CONFIG[k] for k in ('bytes','sha256')}
        assert json.loads((out / 'helper-config.json').read_bytes())['refs'] == proof['refs']
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out / 'source-qualification.json').read_bytes()) == dict(proof,
            source_commit=terminal['source_commit'], source_archive_sha256=terminal['source_archive_sha256'])
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    assert 0 <= terminal['exit_code'] <= 255 and 0 <= terminal['original_exit_code'] <= 255
    assert terminal['phase'] in ('bootstrap','apt-update','apt-install','awscli-download',
        'awscli-install','source-download','install','preparation','complete'), 'typed bootstrap phase'
    complete = terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['status'] == ('complete' if complete else 'failed')
    if complete:
        assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(COMPLETE_ARTIFACTS)
        assert all(ident['bytes'] > 0 for ident in terminal['artifacts'].values())
        failure = json.loads((out / 'failure.json').read_bytes())
        assert failure['status'] == 'complete' and failure['helper_exit_code'] == 0
        validate_preparation(out, proof, launch['prefix'])
        closure = json.loads((out / 'preparation-closure.json').read_bytes())
        assert all(closure[k] == terminal[k] for k in ('source_commit','source_archive_sha256'))
    else:
        if 'failure.json' in terminal['artifacts']:
            assert json.loads((out / 'failure.json').read_bytes())['status'] in ('pending','failed','complete')
    return dict(prepared=complete, ann_quality_measured=False, native_qualification_claim=False,
                exit_status=terminal['original_exit_code'])


def collect(s3, prefix, out, instance_id, commit, digest):
    import re
    out = Path(out)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    launch, closed = (json.loads((out / n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'termination wait required'
    assert launch['prefix'] == prefix and launch['instance_id'] == instance_id
    stream = s3.get_object(Bucket=BUCKET, Key=prefix + '/terminal.json')['Body']
    with stream:
        raw = stream.read((4 << 20) + 1)
    assert len(raw) <= 4 << 20, 'terminal exceeds small-artifact cap'
    terminal = json.loads(raw)
    write(out / 'aws-terminal.json', raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact (raw/debug forbidden)'
    for name,ident in terminal['artifacts'].items():
        assert set(ident) == {'bytes','sha256'} and type(ident['bytes']) is int and 0 <= ident['bytes'] <= 16 << 20
        stream = s3.get_object(Bucket=BUCKET, Key=prefix + '/artifacts/' + name)['Body']
        with stream:
            body = stream.read(ident['bytes'] + 1)
        assert len(body) == ident['bytes'] and sha(body) == ident['sha256'], 'collected body: ' + name
        path = out / name
        path.parent.mkdir(parents=True, exist_ok=True)
        write(path, body)
        write(Path(str(path) + '.gz'), gzip.compress(body, mtime=0))
    replay(out)
    return terminal


def main(attempt):
    import re
    assert re.fullmatch(r'a[0-9]{4}', attempt), 'attempt must be aNNNN'
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()  # Exact config/code authentication precedes the SDK import.
        shared, _ = lifecycle()
        return shared.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """Real frozen metadata, synthetic result bodies, and mocked network only."""
    import contextlib
    import copy
    import io
    import shutil
    import struct
    import tempfile
    from datetime import datetime, timezone
    from unittest.mock import Mock
    repo = Path(__file__).resolve().parents[1]
    module = sys.modules[__name__]
    def rejects(call):
        try:
            call()
        except (AssertionError, ValueError, KeyError, FileNotFoundError):
            return
        raise AssertionError('invalid authority accepted')
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        for name in CODE:
            path = base / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((repo / name).read_bytes())
        body = json.loads((repo / HELPER_CONFIG['path']).read_bytes())
        for pointer in (HELPER_CONFIG, *body['refs'].values()):
            path = base / pointer['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((repo / pointer['path']).read_bytes())
        config = dict(FIXED, authority_pending=False,
            controller_code_sha256={n:hashlib.sha256((repo/n).read_bytes()).hexdigest() for n in CODE})
        (base / CONFIG).write_text(json.dumps(config))
        proof = qualify(base)
        assert proof['helper_config'] == HELPER_CONFIG
        assert set(proof['code_sha256']) == set(CODE) and len(CODE) == len(set(CODE))
        assert not any(n.endswith(('source.raw','source-order.u64','aws-debug.tmp','.parquet')) for n in ARTIFACTS)
        for key,value in (('authority_pending',True),('quality_peek_allowed',True),
            ('memory_bytes',8<<30),('scratch_bytes',16<<30),('threads',4),
            ('machine_limit_seconds',5401),('helper_config',dict(HELPER_CONFIG,sha256='0'*64)),
            ('aws_max_attempts',2),('ann_quality_measured',True),('native_qualification_claim',True),
            ('versions',dict(helper.VERSIONS,numpy='2.3.4')),('unknown',True)):
            (base/CONFIG).write_bytes(encoded(dict(config,**{key:value})))
            rejects(lambda: qualify(base))
        changed = copy.deepcopy(config)
        changed['controller_code_sha256'].pop(CODE[-1])
        (base/CONFIG).write_bytes(encoded(changed)); rejects(lambda:qualify(base))
        (base/CONFIG).write_bytes(encoded(config))
        proof = qualify(base)
        for name in (HELPER_CONFIG['path'], body['refs']['panel']['path'], CODE[0]):
            path=base/name; saved=path.read_bytes(); path.write_bytes(saved+b' ')
            rejects(lambda:qualify(base)); path.write_bytes(saved)
        with patch.object(subprocess,'check_output',return_value='dirty'):
            rejects(lambda:preflight(base))
        with patch.object(subprocess,'check_output',return_value=''):
            assert preflight(base) == proof
        prefix = PREFIX + 'a0001'
        generated = user_data('0'*40,'1'*64,'sources/mock.tar.gz',prefix,proof)
        assert len(generated.encode()) < 16384
        for marker in ('--on-active=5400s','RuntimeMaxSec=4860','MemoryMax=2G',
            'MemorySwapMax=0','CPUQuota=200%','TasksMax=512','30 4800',
            'numpy==2.3.3','pyarrow==24.0.0','--stage','AWS_MAX_ATTEMPTS=1',
            "status='failed'",'original_exit_code','shutdown -h now'):
            assert marker in generated, marker
        assert '--debug' not in generated and not any(n in generated for n in ('rustup','cargo','--replay'))
        assert all('--setenv='+n+'=2' in generated for n in THREAD_ENV)
        rejects(lambda:user_data('0'*40,'1'*64,'sources/mock',PREFIX+'a0001\nBAD',proof))
        rejects(lambda:user_data('0'*40,'1'*64,"bad'key",prefix,proof))
        # Exact transitive closure of the actual imports, plus the helper's
        # independently authenticated runtime closure (SDK imports are external).
        import ast
        seen=set(); pending=['launch_cohere_semantic_1m_preparation_spot','launch_native_semantic_router_cold_spot']
        while pending:
            name='scripts/'+pending.pop()+'.py'
            if name in seen: continue
            seen.add(name)
            for node in ast.parse((repo/name).read_text()).body:
                if isinstance(node,ast.ImportFrom):
                    if node.module=='scripts': pending.extend(a.name for a in node.names)
                    elif node.module and node.module.startswith('scripts.'):
                        pending.append(node.module.split('.')[1])
        assert seen | set(helper.CODE) == set(CODE), 'actual transitive source closure'
        # Replay fixture has real frozen config/panel but synthetic query/GT
        # bodies. It does not invoke the helper, datasets, oracle, or native code.
        out=base/'collected'; out.mkdir(); screen=out/'screen'; screen.mkdir()
        source=dict(source_commit='0'*40,source_archive_sha256='1'*64)
        nodes={'0':dict(instance_id='i-original')}
        launch=dict(instance_id='i-original',nodes=nodes,prefix=prefix,**source)
        write(out/'aws-launch.json',launch)
        write(out/'aws-closeout.json',dict(nodes=nodes,state='terminated'))
        write(out/'aws-reservation.json',dict(schema=SCHEMA,qualification=proof,**source))
        write(out/'config.json',(base/CONFIG).read_bytes())
        write(out/'helper-config.json',(repo/HELPER_CONFIG['path']).read_bytes())
        write(out/'source-qualification.json',dict(proof,**source))
        counters=dict(cgroup='/mock',observer_pid=1,process_ids=[1],
            **{'memory.max':str(MEMORY),'memory.peak':'1024','memory.swap.max':'0',
            'memory.swap.peak':'0','cpu.max':'200000 100000','cpu.stat':'usage_usec 1',
            'pids.max':'512','pids.current':'1','memory.events':'oom 0\noom_kill 0',
            'memory.swap.events':'max 0\nfail 0','pids.events':'max 0'})
        cg=dict(closed=True,before=counters,after=counters)
        write(out/'profile-cgroup.json',cg); validate_cgroup(cg)
        for k,value in (('memory.peak',str(MEMORY+1)),('memory.swap.max','1'),
            ('cpu.max','300000 100000'),('pids.max','513'),('memory.events','oom_kill 1')):
            rejects(lambda k=k,value=value:validate_cgroup(dict(cg,after=dict(counters,**{k:value}))))
        version=dict(python='3.12.0',executable='/mock/venv/bin/python',versions=helper.VERSIONS,
            architecture='x86_64',os_release=dict(ID='ubuntu',VERSION_ID='24.04'),threads=2,aws_max_attempts=1)
        write(out/'tool-versions.json',version)
        command=[version['executable'],'/mock/repo/'+helper.CODE[0],'/mock/repo/'+HELPER_CONFIG['path'],
            HELPER_CONFIG['sha256'],'/mock/repo','/mock/screen',prefix]
        closure=dict(schema='borsuk-cohere-preparation-closure-v1',closed=True,
            helper_exit_code=0,helper_invocations=1,prefix=prefix,wall_seconds=1,
            native_build_invocations=0,native_search_invocations=0,native_publication_invocations=0,
            gt_reuse=False,ann_quality_measured=False,command=command,
            config_sha256=proof['config_sha256'],helper_config_sha256=HELPER_CONFIG['sha256'],**source)
        write(out/'preparation-closure.json',closure)
        failure=dict(schema='borsuk-cohere-preparation-failure-v1',status='complete',helper_exit_code=0,replacement_allowed=False)
        write(out/'failure.json',failure); write(screen/'failure.json',failure)
        queries=[(float(i+1),*([0.0]*767)) for i in range(64)]
        write(screen/'queries.raw',b''.join(struct.pack('<768f',*q) for q in queries))
        write(screen/'requests.jsonl',b''.join(encoded(dict(ordinal=i,query=q))+b'\n' for i,q in enumerate(queries)))
        write(screen/'truth.u32',struct.pack('<6400I',*list(range(100))*64))
        write(screen/'truth.i64',struct.pack('<6400q',*list(range(100))*64))
        write(screen/'panel.json',(repo/body['refs']['panel']['path']).read_bytes())
        audit=dict(schema='borsuk-cohere-semantic-1m-vector-audit-v1',passed=True,
            replacement_allowed=False,complete_historical_coverage=False,new_rows_audited=64,
            indexed_rows_audited=1000000,consumed_rows_audited=4000,normalization=helper.NORMALIZATION,
            raw_sha256=[sha(struct.pack('<768f',*q)) for q in queries],unit_sha256=[sha(str(i).encode()) for i in range(64)],
            config_sha256=HELPER_CONFIG['sha256'],selected_locators_sha256='379b6421d794fb2d0956e7ed787478b019abfd599e0510be157cdcaa53205abf',
            consumed_scopes=dict(old_sealed1000=1000,source_holdout_prior=2000,registered_test=1000),
            within_index_prior_covered_by_first1m=True)
        write(screen/'duplicate-audit.json',audit)
        oracle=dict(schema='borsuk-cohere-semantic-1m-oracle-v1',passed=True,oracle_self_check=True,
            rows=1000000,queries=64,k=100,exhaustive_block_sort_top100_merge=True,
            truth_id_space='source ordinal',source_raw=body['corpus']['raw'],
            truth_u32=dict(path='truth.u32',**artifact(screen/'truth.u32')),
            truth_i64=dict(path='truth.i64',**artifact(screen/'truth.i64')))
        write(screen/'oracle.json',oracle)
        report=dict(schema='borsuk-cohere-preparation-resources-v1',passed=True,
            prospective_preparation_limits=helper.LIMITS,serving_or_build_measurement=False,
            ann_quality_measured=False,aws_max_attempts=1,wall_seconds=1,process_max_rss_kib=1,
            child_max_rss_kib=1,actual_scratch_bytes=1,peak_scratch_bytes=1,
            transport_accounting_complete=True,aws_errors=0,
            http_request_dispatch_attempts=dict(GET=84,HEAD=9,PUT=9),
            confirmed_wire_requests='UNMEASURED',billed_requests='UNMEASURED')
        write(screen/'resources.json',report); write(screen/'final-resources.json',report)
        decision=dict(schema='borsuk-cohere-semantic-1m-construction-v1',
            config_sha256=HELPER_CONFIG['sha256'],prefix=prefix,decision='PASS fixed fresh64 construction only',
            qualification=False,ann_quality_measured=False,complete_historical_coverage=False,
            truth_id_space='source ordinal',queries=64,gt_k=100,
            selected_locators_sha256=audit['selected_locators_sha256'],
            **{k:body[k] for k in ('refs','code_sha256','corpus','consumed_queries','registered_test')},
            artifacts={n:dict(path=n,**artifact(screen/n)) for n in HELPER_OUTPUTS if n!='decision.json'})
        write(screen/'decision.json',decision)
        sealed=dict(schema='borsuk-semantic-1m-seal-readback-v1',
            artifacts={n:dict(key=prefix+'/sealed/'+n,authenticated_readback=True,**artifact(screen/n)) for n in HELPER_OUTPUTS},
            final_resources=dict(path='final-resources.json',**artifact(screen/'final-resources.json')))
        write(screen/'seal-readback.json',sealed)
        for name in ('cpu.txt','run-closed.log','profile.log','profile-resources.txt'):
            write(out/name,b'synthetic closed receipt\n')
        def terminal_body():
            return dict(schema=SCHEMA,instance_id='i-original',status='complete',phase='complete',
                exit_code=0,original_exit_code=0,artifacts={n:artifact(out/n) for n in ARTIFACTS},
                **{k:proof[k] for k in TERMINAL_IDENTITIES},**source)
        terminal=terminal_body(); write(out/'aws-terminal.json',terminal)
        assert replay(out)['prepared'] is True
        # Execute the generated terminal writer against closed synthetic bytes.
        # Shell syntax alone would miss invalid Python embedded in user-data.
        terminal_code=generated.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
        failure_code=generated.split("python3.12 - <<'FAILURE' || code=96\n",1)[1].split('\nFAILURE\n',1)[0]
        compile(failure_code,'<bootstrap failure>','exec')
        before=Path.cwd(); output=io.StringIO()
        try:
            os.chdir(out)
            with patch.dict(os.environ,ARTIFACT_NAMES=' '.join(ARTIFACTS),INSTANCE_ID='i-original',
                EXIT_CODE='0',ORIGINAL_EXIT_CODE='0',PHASE='complete'),contextlib.redirect_stdout(output):
                exec(compile(terminal_code,'<bootstrap terminal>','exec'),{})
            assert json.loads(output.getvalue())==terminal
        finally: os.chdir(before)
        # Exercise the actual observer flow with exactly one mocked helper
        # process. Failure inputs/debug scratch must be removed; exit/closure
        # and the scientific failure receipt remain visible.
        for scenario in ('success','helper-fail','interrupt','dependency-fail'):
            stage_out=base/('stage-'+scenario); stage_out.mkdir()
            stage_repo=stage_out/'repo'; stage_repo.mkdir()
            for name in (*CODE,HELPER_CONFIG['path'],CONFIG,*[p['path'] for p in body['refs'].values()]):
                target=stage_repo/name; target.parent.mkdir(parents=True,exist_ok=True)
                target.write_bytes((base/name).read_bytes())
            for name in ('config.json','helper-config.json','source-qualification.json'):
                write(stage_out/name,(out/name).read_bytes())
            def prepared(args,**kwargs):
                assert args==[sys.executable,str(stage_repo/helper.CODE[0]),
                    str(stage_repo/HELPER_CONFIG['path']),HELPER_CONFIG['sha256'],
                    str(stage_repo),str(stage_out/'screen'),prefix]
                if scenario=='success':
                    shutil.copytree(screen,stage_out/'screen')
                    return subprocess.CompletedProcess(args,0)
                (stage_out/'screen').mkdir()
                write(stage_out/'screen/source.raw',b'owned synthetic source')
                write(stage_out/'screen/aws-debug.tmp',b'SECRET MUST NOT SURVIVE')
                write(stage_out/'screen/failure.json',dict(error_type='ValueError',scientific_status='FAIL',replacement_allowed=False))
                if scenario=='interrupt': raise KeyboardInterrupt()
                return subprocess.CompletedProcess(args,1)
            with patch.object(module,'tools',side_effect=ValueError('wrong dependency') if scenario=='dependency-fail' else None,
                return_value=dict(version,executable=sys.executable)),\
                patch.object(module,'capture_cgroup',return_value=counters),\
                patch.object(subprocess,'run',side_effect=prepared) as invoked:
                try: stage(stage_repo,stage_out,prefix)
                except (AssertionError,ValueError,KeyboardInterrupt): assert scenario!='success'
                else: assert scenario=='success'
            saved_failure=json.loads((stage_out/'failure.json').read_bytes())
            saved_closure=json.loads((stage_out/'preparation-closure.json').read_bytes())
            assert saved_failure['status']==('complete' if scenario=='success' else 'failed')
            assert invoked.call_count==(0 if scenario=='dependency-fail' else 1)
            assert saved_closure['closed'] is (scenario=='success')
            if scenario in ('helper-fail','interrupt'):
                assert saved_failure['helper_failure']['scientific_status']=='FAIL'
                assert not (stage_out/'screen/source.raw').exists()
                assert not (stage_out/'screen/aws-debug.tmp').exists()
                assert not (stage_out/'screen/seal-readback.json').exists()
        # Reauthenticate tampered bodies too: a forged seal/exit/semantic FAIL
        # cannot become complete even if the outer terminal hashes are updated.
        for path,key,value in ((screen/'seal-readback.json','artifacts',{}),
            (out/'preparation-closure.json','helper_exit_code',2),
            (out/'preparation-closure.json','closed',False),
            (screen/'duplicate-audit.json','passed',False),
            (out/'failure.json','status','failed')):
            saved=path.read_bytes(); mutated=json.loads(saved); mutated[key]=value; write(path,mutated)
            write(out/'aws-terminal.json',terminal_body()); rejects(lambda:replay(out))
            write(path,saved)
        write(out/'aws-terminal.json',terminal)
        saved=(screen/'truth.i64').read_bytes(); write(screen/'truth.i64',saved[:-1]+b'X')
        rejects(lambda:replay(out)); write(screen/'truth.i64',saved)
        for key,value in (('exit_code',False),('phase','invalid'),('source_commit','2'*40),('helper_config_sha256','0'*64)):
            write(out/'aws-terminal.json',dict(terminal,**{key:value})); rejects(lambda:replay(out))
        write(out/'aws-terminal.json',dict(terminal,status='failed',phase='preparation',exit_code=1,original_exit_code=1))
        assert replay(out)['prepared'] is False, 'scientific failure promoted'
        # Authenticated collection and wait gate, with no physical network.
        terminal_bytes=encoded(terminal)
        payloads={prefix+'/terminal.json':terminal_bytes,
            **{prefix+'/artifacts/'+n:(out/n).read_bytes() for n in ARTIFACTS}}
        s3=Mock()
        s3.get_object.side_effect=lambda **kw:dict(Body=io.BytesIO(payloads[kw['Key']]))
        assert collect(s3,prefix,out,'i-original','0'*40,'1'*64)['status']=='complete'
        assert s3.get_object.call_count==len(ARTIFACTS)+1
        payloads[prefix+'/artifacts/screen/queries.raw']+=b'tamper'
        rejects(lambda:collect(s3,prefix,out,'i-original','0'*40,'1'*64))
        write(out/'aws-closeout.json',dict(nodes=nodes,state='running'))
        s3.reset_mock(); rejects(lambda:collect(s3,prefix,out,'i-original','0'*40,'1'*64))
        s3.get_object.assert_not_called()
    shared,_=lifecycle()
    with contextlib.redirect_stdout(io.StringIO()):
        shared.self_check(lifecycle_only=True)  # Poll, interruptions, multi-ACK, fsync and upload failures.
    for failure in ('success','fsync','upload','poll','interrupt','multi-ack'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2,s3,session=Mock(),Mock(),Mock(); session.client.side_effect=[ec2,s3]
            ec2.describe_instances.return_value={'Reservations':[]}
            ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock-az'}]}
            ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[dict(SpotPrice='.1',Timestamp=datetime.now(timezone.utc))]}
            ids=['i-original','i-extra'] if failure=='multi-ack' else ['i-original']
            ec2.run_instances.return_value={'Instances':[dict(InstanceId=n) for n in ids]}
            events=[]
            ec2.terminate_instances.side_effect=lambda **kw:events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect=lambda **kw:events.append('wait')
            def collected(*args):
                assert events==['terminate','wait']; events.append('collect')
                return dict(status='complete',phase='complete',exit_code=0,artifacts={n:{} for n in ARTIFACTS})
            writes=[None,None,OSError('upload')] if failure=='upload' else [None,None,None]
            poll_error=KeyboardInterrupt() if failure=='interrupt' else RuntimeError('poll') if failure in ('poll','multi-ack') else None
            with patch.object(module,'ROOT',Path(tmp)),patch.object(module,'preflight',return_value=proof),\
                patch.object(module,'user_data',return_value=generated),patch.object(module,'poll',side_effect=poll_error),\
                patch.object(module,'collect',side_effect=collected),patch.object(shared.boto3,'Session',return_value=session),\
                patch.object(subprocess,'check_output',side_effect=['','0'*40,b'archive']),\
                patch.object(shared.peer,'missing',return_value=True),patch.object(shared.peer,'put_if_absent',side_effect=writes),\
                patch.object(os,'fsync',side_effect=OSError('fsync') if failure=='fsync' else None),\
                contextlib.redirect_stdout(io.StringIO()):
                try: shared.main('a0001',campaign=module)
                except (OSError,RuntimeError,KeyboardInterrupt): assert failure!='success'
                else: assert failure=='success'
            ec2.run_instances.assert_called_once()
            call=ec2.run_instances.call_args.kwargs
            assert call['InstanceType']==INSTANCE_TYPE and call['ImageId']==IMAGE_ID
            assert call['BlockDeviceMappings']==[dict(DeviceName=ROOT_DEVICE_NAME,Ebs=dict(DeleteOnTermination=True,Encrypted=True,VolumeSize=80,VolumeType='gp3'))]
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            persisted=json.loads((Path(tmp)/NAME/'a0001/aws-launch.json').read_bytes())
            assert [n['instance_id'] for n in persisted['nodes'].values()]==ids
            assert events==['terminate','wait','collect']
    print('PASS fixed parent metadata; bounded bootstrap; failed seal/identity/tamper/resources; authenticated collection; ACK/fsync/upload/poll/interrupt/multi-ACK termination wait')
    print('user-data bytes=' + str(len(generated.encode())))


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv)==5 and sys.argv[1]=='--stage':
        stage(*sys.argv[2:])
    elif len(sys.argv)==3 and sys.argv[1]=='--replay':
        print(json.dumps(replay(sys.argv[2])))
    else:
        assert len(sys.argv)==2, 'usage: aNNNN | --self-check | --stage REPO OUTPUT PREFIX | --replay OUTPUT'
        with open('/tmp/borsuk-cohere-fresh64-preparation-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
