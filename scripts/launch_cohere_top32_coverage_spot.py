#!/usr/bin/env python3
"""One frozen CoHere top32 coverage Spot; root owns config, freeze and launch.

CLI: aNNNN | --self-check | --stage REPO OUTPUT PREFIX | --replay OUTPUT.
Config is exactly FIXED plus authority_pending=false, controller_code_sha256
(every CODE member), and helper_config={path:HELPER_CONFIG,bytes,sha256}.
The helper seals both nominations before one GT100. Scientific FAIL can close
successfully; identity, duplicate, admission, or cleanup failures cannot.
"""
import fcntl
import gzip
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/cohere-top32-coverage')
CONFIG, HELPER_CONFIG = ROOT / 'config.json', str(ROOT / 'preparation-config.json')
NAME = ''
MODULE = 'scripts.launch_cohere_top32_coverage_spot'
SCHEMA = 'borsuk-cohere-top32-coverage-spot-v1'
PREFIX = 'research/semantic-router/20261002/cohere-top32-coverage-'
TOKEN_PREFIX, TAG = 'cohere-top32-coverage-', 'borsuk-cohere-top32-coverage'
WALL, WORKER_SECONDS, SERVICE_SECONDS = 3000, 1800, 1860
MEMORY, SCRATCH = 8 << 30, 8 << 30
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME, SUBNET = '/dev/sda1', 'subnet-034528fbd6977848f'
REGION, BUCKET = 'eu-central-1', 'borsuk-bench-453182569524-euc1'
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .45
AWSCLI_VERSION = '2.36.11'
AWSCLI_SHA256 = '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6'
VERSIONS = dict(numpy='2.3.3', pyarrow='24.0.0')
THREAD_ENV = ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'MKL_NUM_THREADS',
              'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS')
HELPER_CODE = (
    'crates/borsuk/src/semantic_unit_router.rs',
    'crates/borsuk/src/sq8_source.rs',
    'scripts/audit_v36_ranked_physical_ids.py',
    'scripts/build_cohere_1m_source.py',
    'scripts/check_native_metadata_ranges_build.py',
    'scripts/check_native_metadata_ranges_stats.py',
    'scripts/check_native_semantic_concurrency.py',
    'scripts/check_native_semantic_router_stats.py',
    'scripts/check_native_startup_build.py',
    'scripts/check_native_startup_stats.py',
    'scripts/check_semantic_binary_coverage.py',
    'scripts/check_semantic_router_coverage.py',
    'scripts/launch_native_cohere_semantic_1m_quality_spot.py',
    'scripts/launch_native_metadata_ranges_cold_spot.py',
    'scripts/launch_native_peer_1m_spot.py',
    'scripts/launch_native_semantic_1m_quality_spot.py',
    'scripts/launch_native_semantic_fresh_panel_spot.py',
    'scripts/launch_native_semantic_panel_ids_spot.py',
    'scripts/launch_native_semantic_router_cold_spot.py',
    'scripts/launch_native_startup_profile_spot.py',
    'scripts/launch_v157_primary_feasibility_spot.py',
    'scripts/launch_v174_relaid_bind_compile_spot.py',
    'scripts/native_geometric_layout_screen.py',
    'scripts/native_rotated_two_bit_codes.py',
    'scripts/native_row_score_code_artifacts.py',
    'scripts/native_two_bit_cosine_development.py',
    'scripts/native_two_bit_topology.py',
    'scripts/package_semantic_native_generation.py',
    'scripts/prepare_cohere_semantic_1m_panel.py',
    'scripts/prepare_cohere_top32_coverage.py',
    'scripts/prepare_native_semantic_publication.py',
    'scripts/prepare_semantic_1m_fresh_panel.py',
    'scripts/rest_coexistence_load.py',
    'scripts/run_native_cohere_semantic_1m_quality.py',
    'scripts/run_native_cold.py',
    'scripts/run_native_cold_first_query.py',
    'scripts/run_native_cold_offered.py',
    'scripts/run_native_metadata_ranges_cold.py',
    'scripts/run_native_peer_1m_worker.py',
    'scripts/run_native_peer_offered_http.py',
    'scripts/run_native_semantic_1m_quality.py',
    'scripts/run_native_semantic_router_cold.py',
    'scripts/run_native_source_frontier_1m.py',
    'scripts/run_native_union_cold.py',
    'scripts/run_native_union_http.py',
    'scripts/run_native_union_offered_http.py',
    'scripts/seal_v36_rank16_fresh_1m.py',
    'scripts/select_cohere_1m_fresh64.py',
    'scripts/select_cohere_fresh64_coverage.py',
    'scripts/select_v36_rank16_fresh_ids.py',
    'scripts/v102_two_wave_pq48_refinement.py',
    'scripts/v271_fresh_frontier.py',
    'scripts/v284_page_primary_dev64.py',
    'scripts/v285_exact_page_rank_bound.py',
    'scripts/v291_two_stage_development.py',
    'scripts/v97_row_width_screen.py',
    'scripts/v98_hierarchical_row_router.py',
    'scripts/v99_ranked_gap_range_router.py',
)
CODE = tuple(sorted((*HELPER_CODE, 'scripts/launch_cohere_top32_coverage_spot.py',
                     'scripts/launch_cohere_semantic_1m_preparation_spot.py')))
HELPER_OUTPUTS = (
    'queries.raw',
    'requests.jsonl',
    'panel.json',
    'duplicate-audit.json',
    'source-qualification.json',
    'sq8-ordinal-check.json',
    'builder-config.json',
    'build.log',
    'build-resources.txt',
    'build-resources.json',
    'generation-manifest.json',
    'nominate-config.json',
    'nomination.json',
    'nomination-seal.json',
    'truth.u32',
    'truth.i64',
    'oracle.json',
    'reduce-config.json',
    'coverage.json',
    'resources.json',
    'source-order.u64',
    'source-root.json',
    'prospective-protocol.json',
    'decision.json',
    'final-resources.json',
    'seal-readback.json',
)
ARTIFACTS = ('config.json', 'helper-config.json', 'source-qualification.json',
    'archived-builder-assurance.json', 'cpu.txt', 'tool-versions.json', 'run-closed.log',
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
    'coverage-closure.json', 'failure.json', *('screen/' + n for n in HELPER_OUTPUTS))
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'helper_config_sha256',
    'refs_identity_sha256', 'artifact_roster_sha256', 'builder_assurance_sha256',
    'original_source_identity_sha256', 'archived_quality_source_identity_sha256',
    'builder_binary_sha256', 'campaign_schema', 'awscli_version', 'awscli_sha256',
    'coverage_only', 'old_fail_preserved', 'returned_recall_measured',
    'cold_http_measured', 'physical_s3_query_gets_measured',
    'native_qualification_claim', 'current_whole_tree_full_execution', 'complete_historical_coverage')
FIXED = dict(schema=SCHEMA, architecture='x86_64', region=REGION, bucket=BUCKET,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, volume_gib=80, volume_type='gp3', encrypted=True,
    delete_on_termination=True, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
    compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=.15, memory_bytes=MEMORY,
    scratch_bytes=SCRATCH, swap_bytes=0, cpu_quota_percent=200, tasks_max=512,
    worker_limit_seconds=WORKER_SECONDS, service_limit_seconds=SERVICE_SECONDS,
    machine_limit_seconds=WALL, versions=VERSIONS, threads=2, aws_max_attempts=1,
    fixed_queries=64, selected_shards=62, quality_peek_allowed=False,
    coverage_only=True, old_fail_preserved=True, nomination_freeze_before_truth=True,
    maximum_generation_builds=1, ground_truth_constructions=1, scorer_invocations=0,
    hits10_minimum=608, denominator10=640, replacement_allowed=False,
    returned_recall_measured=False, cold_http_measured=False,
    physical_s3_query_gets_measured=False, native_qualification_claim=False,
    current_whole_tree_full_execution=False, complete_historical_coverage=False)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def artifact(path):
    path = Path(path)
    assert path.is_file() and not path.is_symlink(), 'regular body required: ' + str(path)
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            digest.update(chunk)
    return dict(bytes=path.stat().st_size, sha256=digest.hexdigest())


def write(path, value):
    with Path(path).open('wb') as stream:
        stream.write(value if isinstance(value, bytes) else encoded(value) + b'\n')
        stream.flush(); os.fsync(stream.fileno())


def repo_path(base, name):
    path = Path(name)
    assert not path.is_absolute() and '..' not in path.parts
    path = base / path
    assert path.resolve().is_relative_to(base) and not path.is_symlink(), 'escaped authority'
    return path


def helper_module(base):
    helper = importlib.import_module('scripts.prepare_cohere_top32_coverage')
    assert Path(helper.__file__).resolve() == base / 'scripts/prepare_cohere_top32_coverage.py', 'helper import origin'
    return helper


def qualify(base=Path('.'), config_path=None):
    base = Path(base).resolve()
    path = base / CONFIG if config_path is None else Path(config_path)
    assert artifact(path)['bytes'] <= 1 << 20
    body = path.read_bytes(); config = json.loads(body)
    assert set(config) == set(FIXED) | {'authority_pending', 'controller_code_sha256', 'helper_config'}, 'exact config fields'
    assert config['authority_pending'] is False, 'root authority freeze pending'
    assert all(encoded(config[k]) == encoded(v) for k,v in FIXED.items()), 'fixed coverage protocol'
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive code roster'
    for name,digest in code.items():
        assert artifact(repo_path(base, name))['sha256'] == digest, 'controller source drift: ' + name
    pin = config['helper_config']
    assert set(pin) == {'path','bytes','sha256'} and pin['path'] == HELPER_CONFIG, 'helper config pointer'
    nested_path = repo_path(base, pin['path'])
    assert artifact(nested_path) == {k:pin[k] for k in ('bytes','sha256')}, 'helper config identity'
    helper = helper_module(base)  # Only after the entire executable closure authenticates.
    nested = helper.read_config(nested_path, pin['sha256'], base)
    assert set(helper.CODE) == set(HELPER_CODE) and tuple(helper.ARTIFACTS) == HELPER_OUTPUTS, 'helper API drift'
    assert all(code[n] == digest for n,digest in nested['code_sha256'].items()), 'nested source drift'
    original,_ = helper.offline_modules(base)
    data,_ = helper.load_inputs(nested, base, original)
    _,assurance = helper.builder_authority(base, data)
    assert len(nested['refs']) == 45, 'exact metadata authority roster'
    return dict(config_path=str(CONFIG), config_sha256=sha(body), helper_config=pin,
        helper_config_sha256=pin['sha256'], code_sha256=code,
        code_identity_sha256=sha(encoded(code)), refs=nested['refs'],
        refs_identity_sha256=sha(encoded(nested['refs'])), campaign_schema=SCHEMA,
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), builder_assurance_sha256=sha(encoded(assurance)),
        original_source_identity_sha256=assurance['original_source_identity_sha256'],
        archived_quality_source_identity_sha256=assurance['archived_quality_source_identity_sha256'],
        builder_binary_sha256=assurance['builder_binary']['sha256'], coverage_only=True,
        old_fail_preserved=True, returned_recall_measured=False, cold_http_measured=False,
        physical_s3_query_gets_measured=False, native_qualification_claim=False,
        current_whole_tree_full_execution=False, complete_historical_coverage=False,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    return proof


def lifecycle():
    from scripts import launch_cohere_semantic_1m_preparation_spot as preparation
    return preparation.lifecycle()


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch('[a-zA-Z0-9/._-]+', archive_key) and '..' not in archive_key.split('/')
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    assert qualification['config_path'] == str(CONFIG) and qualification['campaign_schema'] == SCHEMA
    _,bootstrap = lifecycle()
    adapter = {k:qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, '_offered', return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    env = ' '.join('--setenv=' + name + '=2' for name in THREAD_ENV)
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
phase=coverage
systemd-run --unit=cohere-top32-coverage --wait --pipe -p MemoryMax={MEMORY} -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=1860 -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C {env} \\
 --setenv=BORSUK_COVERAGE_SOURCE_COMMIT={commit} --setenv=BORSUK_COVERAGE_ARCHIVE_SHA256={archive_sha} \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1800 \\
 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root" {prefix} >profile.log 2>&1
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start,end = body.index('phase=install\n'),body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/cohere-top32-coverage')
    body = body.replace('python3-boto3 python3.12', 'python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    subprocess.run(['bash','-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0], '<terminal>', 'exec')
    assert len(body.encode()) < 16384, 'EC2 user-data limit'
    assert all(n not in body for n in ('rustup','cargo','unused','--publish','two_bit_http'))
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared,_ = lifecycle()
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def capture_cgroup():
    from scripts import launch_cohere_semantic_1m_preparation_spot as preparation
    return preparation.capture_cgroup()


def validate_cgroup(report):
    from scripts import launch_cohere_semantic_1m_preparation_spot as preparation
    with patch.object(preparation, 'MEMORY', MEMORY):
        preparation.validate_cgroup(report)
    assert all(int(dict(line.split() for line in c['memory.events'].splitlines()).get('max', 0)) == 0
               for c in (report['before'], report['after'])), 'memory admission failure'


def tools():
    from scripts import launch_cohere_semantic_1m_preparation_spot as preparation
    assert all(os.environ.get(n) == '2' for n in THREAD_ENV), 'two-thread environment'
    return dict(preparation.tools(), thread_environment={n:os.environ[n] for n in THREAD_ENV})


def run_process(*args, **kwargs):
    from scripts import run_native_semantic_1m_quality as native
    return native.run_process(*args, **kwargs)


def stage(repo, out, prefix):
    repo,out = Path(repo).resolve(),Path(out).resolve()
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    started = time.monotonic()
    status = dict(schema='borsuk-cohere-top32-coverage-failure-v1', status='pending', replacement_allowed=False)
    closure = dict(schema='borsuk-cohere-top32-coverage-closure-v1', closed=False,
        helper_invocations=0, helper_exit_code=None, prefix=prefix, coverage_only=True,
        scorer_invocations=0, returned_recall_measured=False, cold_http_measured=False)
    counters = dict(closed=False, before=None, after=None)
    write(out / 'failure.json', status)
    helper = None
    previous = signal.getsignal(signal.SIGTERM)
    try:
        proof = qualify(repo)
        helper = helper_module(repo)
        signal.signal(signal.SIGTERM, helper.prior.terminate)
        source = {k:os.environ['BORSUK_COVERAGE_' + n] for k,n in (
            ('source_commit','SOURCE_COMMIT'), ('source_archive_sha256','ARCHIVE_SHA256'))}
        assert re.fullmatch('[0-9a-f]{40}',source['source_commit'])
        assert re.fullmatch('[0-9a-f]{64}',source['source_archive_sha256'])
        write(out / 'config.json', (repo / CONFIG).read_bytes())
        write(out / 'helper-config.json', repo_path(repo, HELPER_CONFIG).read_bytes())
        write(out / 'source-qualification.json', dict(proof, **source))
        nested = json.loads((out / 'helper-config.json').read_bytes())
        original,_ = helper.offline_modules(repo)
        data,_ = helper.load_inputs(nested, repo, original)
        _,assurance = helper.builder_authority(repo, data)
        write(out / 'archived-builder-assurance.json', assurance)
        write(out / 'tool-versions.json', tools())
        counters['before'] = capture_cgroup()
        validate_cgroup(dict(counters, after=counters['before'], closed=True))
        assert not (out / 'screen').exists(), 'fresh helper output required'
        command = [sys.executable, str(repo / 'scripts/prepare_cohere_top32_coverage.py'),
            str(repo / HELPER_CONFIG), proof['helper_config_sha256'], str(repo), str(out / 'screen'), prefix]
        closure.update(helper_invocations=1, command=command, **source,
            config_sha256=proof['config_sha256'], helper_config_sha256=proof['helper_config_sha256'])
        result = run_process(command, out / 'profile.log', WORKER_SECONDS - (time.monotonic() - started))
        closure.update(helper_exit_code=result['exit_status'], process_cleanup=result['process_cleanup'])
        assert type(result['exit_status']) is int and result['exit_status'] == 0 and result['process_cleanup'] is True, 'coverage helper failed'
        counters.update(after=capture_cgroup(), closed=True)
        validate_cgroup(counters)
        closure.update(closed=True, wall_seconds=time.monotonic() - started)
        assert closure['wall_seconds'] <= WORKER_SECONDS
        write(out / 'coverage-closure.json', closure)
        write(out / 'profile-cgroup.json', counters)
        validate_coverage(out, proof, prefix, repo)
        status.update(status='complete', helper_exit_code=0)
        write(out / 'failure.json', status)
        return closure
    except BaseException as error:
        status.update(status='failed', error_type=type(error).__name__, error=str(error), helper_exit_code=closure['helper_exit_code'])
        if helper is not None and (out / 'screen').exists():
            for name, key in (('failure.json', 'helper_failure'), ('failure-resources.json', 'helper_failure_resources')):
                path = out / 'screen' / name
                if path.exists():
                    assert artifact(path)['bytes'] <= 1 << 20, 'bounded helper failure body'
                    status[key] = json.loads(path.read_bytes())
            write(out / 'failure.json', status)
            status['cleanup'] = helper.cleanup_failure(out / 'screen')
        write(out / 'failure.json', status)
        raise
    finally:
        signal.signal(signal.SIGTERM, previous)
        closure['wall_seconds'] = time.monotonic() - started
        if counters['before'] is not None and counters['after'] is None:
            counters['after'] = capture_cgroup()
        write(out / 'profile-cgroup.json', counters)
        write(out / 'coverage-closure.json', closure)


def validate_coverage(out, proof, prefix, repo):
    out,repo = Path(out),Path(repo).resolve()
    helper = helper_module(repo)
    screen = out / 'screen'
    names = {p.name for p in screen.iterdir()}
    assert set(HELPER_OUTPUTS) <= names <= set(HELPER_OUTPUTS) | {n + '.gz' for n in HELPER_OUTPUTS}, 'exact helper output roster; no heavy scratch'
    assert (screen / 'nomination.json').stat().st_mode & 0o222 == 0, 'immutable nomination'
    assert artifact(out / 'helper-config.json') == {k:proof['helper_config'][k] for k in ('bytes','sha256')}
    assert artifact(out / 'config.json')['sha256'] == proof['config_sha256']
    assert sha(encoded(json.loads((out / 'archived-builder-assurance.json').read_bytes()))) == proof['builder_assurance_sha256']
    counters = json.loads((out / 'profile-cgroup.json').read_bytes()); validate_cgroup(counters)
    closure = json.loads((out / 'coverage-closure.json').read_bytes())
    assert closure['schema'] == 'borsuk-cohere-top32-coverage-closure-v1'
    assert closure['closed'] is closure['process_cleanup'] is True
    assert closure['prefix'] == prefix and closure['helper_invocations'] == 1 and closure['helper_exit_code'] == 0
    assert closure['scorer_invocations'] == 0 and closure['coverage_only'] is True
    assert closure['returned_recall_measured'] is closure['cold_http_measured'] is False
    assert 0 <= closure['wall_seconds'] <= WORKER_SECONDS
    assert closure['config_sha256'] == proof['config_sha256'] and closure['helper_config_sha256'] == proof['helper_config_sha256']
    versions = json.loads((out / 'tool-versions.json').read_bytes())
    assert versions['versions'] == VERSIONS and versions['architecture'] == 'x86_64'
    assert versions['os_release']['ID'] == 'ubuntu' and versions['os_release']['VERSION_ID'] == '24.04'
    assert versions['threads'] == 2 and versions['aws_max_attempts'] == 1
    assert versions['thread_environment'] == dict.fromkeys(THREAD_ENV,'2')
    assert versions['python'].startswith('3.12.') and (out / 'cpu.txt').read_text().strip()
    timing = (out / 'profile-resources.txt').read_text()
    assert 0 <= int(timing.split('Maximum resident set size (kbytes): ',1)[1].splitlines()[0]) * 1024 <= MEMORY
    assert int(timing.split('Exit status: ',1)[1].splitlines()[0]) == 0
    for name in ('resources.json','final-resources.json'):
        report = json.loads((screen / name).read_bytes())
        assert report['schema'] == 'borsuk-cohere-top32-preparation-resources-v1' and report['passed'] is True
        assert report['prospective_preparation_limits'] == dict(memory_bytes=MEMORY, scratch_bytes=SCRATCH,
            cpu=2, threads=2, swap_bytes=0, max_requests=256)
        assert report['build_invocations'] == report['oracle_invocations'] == 1 and report['scorer_invocations'] == 0
        assert report['stage_limit_seconds'] == 1800 and 0 <= report['wall_seconds'] <= WORKER_SECONDS
        assert 0 <= report['peak_scratch_bytes'] <= SCRATCH and 0 <= report['actual_scratch_bytes'] <= SCRATCH
        assert 0 <= report['aggregate_memory_peak_bytes'] == int(report['cgroup']['memory.peak']) <= int(counters['after']['memory.peak']) <= MEMORY
        assert report['memory_events'] == dict(line.split() for line in counters['after']['memory.events'].splitlines())
        assert report['coverage_only'] is True and report['ann_quality_measured'] is False
        assert report['returned_recall_measured'] is report['cold_http_measured'] is report['physical_s3_query_gets_measured'] is False
        assert report['process_max_rss_kib'] * 1024 <= MEMORY and report['child_max_rss_kib'] * 1024 <= MEMORY
        assert all(0 <= v <= 1800 for v in report['stage_seconds'].values())
        quota,period = map(int,report['cgroup']['cpu.max'].split())
        assert period > 0 and quota == 2 * period and report['cgroup']['cpu.stat'].strip()
        assert int(report['cgroup']['memory.max']) == MEMORY
        assert int(report['cgroup']['memory.swap.max']) == int(report['cgroup']['memory.swap.peak']) == 0
    final = json.loads((screen / 'final-resources.json').read_bytes())
    assert final['success_cleanup']['heavy_scratch_remaining'] is False
    assert set(final['success_cleanup']['removed']) == {'source.raw','source-sq8.bin','consumed-queries.raw',
        'prior-queries.raw','test-queries.raw','generation'}
    # Run the genuine relocated-artifact reducer; no raw/SQ8/router, GT, builder or SDK.
    replayed = helper.replay(out / 'helper-config.json', proof['helper_config_sha256'], repo, screen)
    assert replayed == dict(passed=True, coverage_only=True, remote_objects_reopened=False,
                           ground_truth_reexecuted=False, builder_reexecuted=False)
    decision = json.loads((screen / 'decision.json').read_bytes())
    assert decision['prefix'] == prefix and decision['status'] in ('GO-for-native-investigation','FAIL')
    return decision['status']


def replay(out, repo=None):
    out = Path(out)
    repo = Path(__file__).resolve().parents[1] if repo is None else Path(repo).resolve()
    launch,closed,reservation,terminal = (json.loads((out / n).read_bytes()) for n in (
        'aws-launch.json','aws-closeout.json','aws-reservation.json','aws-terminal.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'all owned nodes must terminate'
    assert terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', launch['prefix'])
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key] == reservation[key] == launch[key]
    proof = reservation['qualification']
    assert proof == qualify(repo), 'frozen authority'
    if 'config.json' in terminal['artifacts']:
        assert artifact(out / 'config.json')['sha256'] == proof['config_sha256']
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal identity: ' + key
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name,pin in terminal['artifacts'].items():
        assert set(pin) == {'bytes','sha256'} and type(pin['bytes']) is int and 0 < pin['bytes'] <= 16 << 20
        assert artifact(out / name) == pin, 'closed body: ' + name
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out / 'source-qualification.json').read_bytes()) == dict(proof,
            source_commit=terminal['source_commit'], source_archive_sha256=terminal['source_archive_sha256'])
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    assert 0 <= terminal['exit_code'] <= 255 and 0 <= terminal['original_exit_code'] <= 255
    assert terminal['phase'] in ('bootstrap','apt-update','apt-install','awscli-download',
        'awscli-install','source-download','install','coverage','complete')
    complete = terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['status'] == ('complete' if complete else 'failed')
    coverage_status = 'UNKNOWN'
    if complete:
        assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(ARTIFACTS), 'complete artifact roster'
        failure = json.loads((out / 'failure.json').read_bytes())
        assert failure['status'] == 'complete' and failure['helper_exit_code'] == 0
        coverage_status = validate_coverage(out, proof, launch['prefix'], repo)
        closure = json.loads((out / 'coverage-closure.json').read_bytes())
        assert all(closure[k] == terminal[k] for k in ('source_commit','source_archive_sha256'))
    else:
        assert terminal['exit_code'] != 0, 'failed execution cannot have exit zero'
    return dict(executed=complete, coverage_status=coverage_status, coverage_only=True,
        old_fail_preserved=True, returned_recall_measured=False, cold_http_measured=False,
        physical_s3_query_gets_measured=False, native_qualification_claim=False,
        current_whole_tree_full_execution=False, complete_historical_coverage=False,
        exit_status=terminal['exit_code'])


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    launch,closed = (json.loads((out / n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'termination wait required'
    assert launch['prefix'] == prefix and launch['instance_id'] == instance_id
    stream = s3.get_object(Bucket=BUCKET, Key=prefix + '/terminal.json')['Body']
    with stream:
        raw = stream.read((4 << 20) + 1)
    assert len(raw) <= 4 << 20, 'bounded terminal'
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    write(out / 'aws-terminal.json', raw)
    for name,pin in terminal['artifacts'].items():
        assert set(pin) == {'bytes','sha256'} and type(pin['bytes']) is int and 0 < pin['bytes'] <= 16 << 20
        stream = s3.get_object(Bucket=BUCKET, Key=prefix + '/artifacts/' + name)['Body']
        with stream:
            body = stream.read(pin['bytes'] + 1)
        assert len(body) == pin['bytes'] and sha(body) == pin['sha256'], 'collected body: ' + name
        path = out / name; path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            assert artifact(path) == pin, 'existing collected body drift: ' + name
        else:
            write(path, body)
        if name == 'screen/nomination.json':
            path.chmod(0o444)
        write(Path(str(path) + '.gz'), gzip.compress(body, mtime=0))
    replay(out)
    return terminal


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}', attempt), 'attempt must be aNNNN'
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()
        shared,_ = lifecycle()
        return shared.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """Frozen metadata, actual relocated reducer, mocked processes and cloud only."""
    import copy
    from datetime import datetime, timezone
    import io
    import resource
    import shutil
    import tempfile
    from types import ModuleType
    from unittest.mock import Mock
    from contextlib import redirect_stdout
    started = time.monotonic()
    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    signal.alarm(55)
    module = sys.modules[__name__]
    repo = Path(__file__).resolve().parents[1]
    def rejected(action):
        try:
            action()
        except (AssertionError, ValueError, KeyError, OSError, subprocess.TimeoutExpired):
            return
        raise AssertionError('invalid authority accepted')
    helper = helper_module(repo)
    draft = Path('/tmp/borsuk-cohere-top32-root-draft-config.json')
    before_draft = artifact(draft) if draft.exists() else None
    original_repo_path = repo_path
    with tempfile.TemporaryDirectory() as directory, patch.object(module, 'repo_path',
            side_effect=lambda base, name: nested_target if name == HELPER_CONFIG else original_repo_path(base, name)):
        work = Path(directory)
        # Refresh only temporary copies; committed 4GiB authority remains frozen.
        nested_target = work / 'helper.json'
        nested = json.loads((repo / HELPER_CONFIG).read_bytes())
        nested.update(limits=dict(helper.LIMITS),
            code_sha256={n:artifact(repo / n)['sha256'] for n in HELPER_CODE})
        write(nested_target, nested)
        pin = dict(path=HELPER_CONFIG, **artifact(nested_target))
        config = dict(json.loads((repo / CONFIG).read_bytes()), memory_bytes=MEMORY,
            authority_pending=False, helper_config=pin,
            controller_code_sha256={n:artifact(repo / n)['sha256'] for n in CODE})
        target = work / 'controller.json'; write(target, config)
        # Unrefreshed authority must fail before any launch or cloud import.
        rejected(lambda:qualify(repo))
        # Actual helper metadata, 45 original refs, and separately archived builder.
        proof = qualify(repo, target)
        assert len(proof['code_sha256']) == 60 and len(proof['refs']) == 45
        assert proof['helper_config_sha256'] == pin['sha256']
        assert proof['builder_binary_sha256'] == helper.BUILDER_SHA
        assert proof['original_source_identity_sha256'] == helper.ORIGINAL_SOURCE
        assert proof['archived_quality_source_identity_sha256'] == helper.QUALITY_SOURCE
        assert not proof['native_qualification_claim'] and not proof['current_whole_tree_full_execution']
        for field,value in (('authority_pending',True), ('memory_bytes',MEMORY+1),
                ('threads',4), ('hits10_minimum',600), ('replacement_allowed',True),
                ('ground_truth_constructions',2), ('maximum_generation_builds',2),
                ('cold_http_measured',True), ('compute_cap_usd',1), ('machine_limit_seconds',3001)):
            changed = dict(config, **{field:value}); write(target,changed)
            rejected(lambda:qualify(repo,target))
        write(target,dict(config,controller_code_sha256=dict(config['controller_code_sha256'],
            **{CODE[0]:'0'*64})))
        rejected(lambda:qualify(repo,target))
        write(target,dict(config,helper_config=dict(pin,sha256='0'*64)))
        rejected(lambda:qualify(repo,target))
        write(target,dict(config,controller_code_sha256={}))
        rejected(lambda:qualify(repo,target)); write(target,config)
        # SDK fakes cannot make a paid call even if a test forgets a seam.
        sdk,botocore,exceptions = (ModuleType(n) for n in ('boto3','botocore','botocore.exceptions'))
        class SDKError(Exception):
            def __init__(self, **kwargs):
                super().__init__('mock SDK')
        for name in ('ClientError','EndpointConnectionError','ReadTimeoutError'):
            setattr(exceptions,name,SDKError)
        sdk.Session = Mock(side_effect=AssertionError('cloud forbidden'))
        botocore.exceptions = exceptions
        with patch.dict(sys.modules,{'boto3':sdk,'botocore':botocore,'botocore.exceptions':exceptions}):
            shared,_ = lifecycle()
            body = user_data('0'*40,'1'*64,'source/key',PREFIX+'a0001',proof)
            assert '--on-active=3000s' in body and 'RuntimeMaxSec=1860' in body
            assert all(n in body for n in ('MemoryMax=8589934592','MemorySwapMax=0','CPUQuota=200%','TasksMax=512'))
            assert all('--setenv='+n+'=2' in body for n in THREAD_ENV)
            command = body.split('systemd-run --unit=cohere-top32-coverage',1)[1].split('\nfor name',1)[0]
            shell = 'systemd-run() { printf "%s\\n" "$@"; }; root=/synthetic; systemd-run --unit=cohere-top32-coverage'+command
            argv = subprocess.check_output(['bash','-c',shell.replace(' >profile.log 2>&1','')],text=True).splitlines()
            assert all('--setenv='+n+'=2' in argv for n in THREAD_ENV)
            assert '-m' in argv and MODULE in argv
            with patch.object(module,'CONFIG',Path('/nonexistent/coverage-authority.json')):
                rejected(lambda:main('a0001'))
            sdk.Session.assert_not_called()
            # Execute actual terminal hashing with synthetic bootstrap bodies.
            terminal_script = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
            terminal_dir = work / 'terminal'; terminal_dir.mkdir(); write(terminal_dir / 'run-closed.log',b'closed log\n')
            old = Path.cwd(); os.chdir(terminal_dir)
            try:
                for code,phase in ((0,'complete'),(99,'coverage'),(96,'complete')):
                    captured = io.StringIO()
                    with patch.dict(os.environ,INSTANCE_ID='i-synthetic',EXIT_CODE=str(code),
                            ORIGINAL_EXIT_CODE=str(0 if code==96 else code),PHASE=phase,
                            ARTIFACT_NAMES='run-closed.log'), redirect_stdout(captured):
                        exec(compile(terminal_script,'<terminal>','exec'),{})
                    terminal = json.loads(captured.getvalue())
                    assert terminal['status'] == ('complete' if code==0 else 'failed')
                    assert terminal['artifacts']['run-closed.log'] == artifact('run-closed.log')
                    assert terminal['schema'] == SCHEMA and terminal['config_sha256'] == proof['config_sha256']
                    assert terminal['exit_code'] == code
            finally:
                os.chdir(old)
            with redirect_stdout(io.StringIO()):
                shared.self_check(lifecycle_only=True)
            # Campaign-specific launch parameters plus ACK/fsync/interrupt/wait failures.
            for failure in ('success','fsync','multi-ack','interrupt','interruption','wait'):
                ec2,s3,session = Mock(),Mock(),Mock(); session.client.side_effect=[ec2,s3]
                ec2.describe_instances.return_value={'Reservations':[]}
                ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock-az'}]}
                ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[
                    {'SpotPrice':'0.1','Timestamp':datetime.now(timezone.utc)}]}
                nodes=['i-original','i-extra'] if failure=='multi-ack' else ['i-original']
                ec2.run_instances.return_value={'Instances':[{'InstanceId':n} for n in nodes]}
                events=[]
                ec2.terminate_instances.side_effect=lambda **kw:events.append('terminate')
                def wait(**kwargs):
                    events.append('wait')
                    if failure=='wait':
                        raise RuntimeError('termination wait failed')
                ec2.get_waiter.return_value.wait.side_effect=wait
                def collected(*args):
                    assert events==['terminate','wait']; events.append('collect')
                    return dict(status='complete',phase='complete',exit_code=0,artifacts=dict.fromkeys(ARTIFACTS,{}))
                error={'interrupt':KeyboardInterrupt(),'interruption':RuntimeError('Spot interruption')}.get(failure)
                destination=work/failure
                with patch.object(module,'ROOT',destination), patch.object(module,'preflight',return_value=proof), \
                     patch.object(shared.boto3,'Session',return_value=session), \
                     patch.object(shared.subprocess,'check_output',side_effect=['','0'*40,b'synthetic archive']), \
                     patch.object(shared.peer,'missing',return_value=True), patch.object(shared.peer,'put_if_absent'), \
                     patch.object(os,'fsync',side_effect=OSError('ACK fsync failure') if failure=='fsync' else None), \
                     patch.object(module,'poll',side_effect=error), patch.object(module,'collect',side_effect=collected) as collector, \
                     redirect_stdout(io.StringIO()):
                    try:
                        main('a0001')
                    except (OSError,RuntimeError,KeyboardInterrupt):
                        assert failure not in ('success','multi-ack')
                    else:
                        assert failure in ('success','multi-ack')
                ec2.run_instances.assert_called_once()
                ec2.terminate_instances.assert_called_once_with(InstanceIds=nodes)
                ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=nodes)
                if failure=='wait':
                    collector.assert_not_called(); assert not (destination/'a0001/aws-closeout.json').exists()
                else:
                    assert events==['terminate','wait','collect']
                args=ec2.run_instances.call_args.kwargs
                assert args['InstanceType']==INSTANCE_TYPE and args['ImageId']==IMAGE_ID
                assert args['BlockDeviceMappings']==[{'DeviceName':ROOT_DEVICE_NAME,'Ebs':{
                    'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}]
                assert args['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.50'
                reservation=json.loads((destination/'a0001/aws-reservation.json').read_bytes())
                assert reservation['schema']==SCHEMA and reservation['compute_cap_usd']==.45
                assert reservation['ebs_s3_allowance_usd']==.15 and reservation['wall_seconds']==3000
            # Stage observer mocks; real shared process-group cleanup gets its own falsifier.
            counters=dict(cgroup='/synthetic/cgroup',observer_pid=1234,process_ids=[1234],
                **{'memory.max':str(MEMORY),'memory.peak':'1234','memory.swap.max':'0','memory.swap.peak':'0',
                   'memory.events':'max 0\noom 0\noom_kill 0','memory.swap.events':'max 0\nfail 0',
                   'cpu.max':'200000 100000','cpu.stat':'usage_usec 10','pids.max':'512','pids.current':'1','pids.events':'max 0'})
            validate_cgroup(dict(before=counters,after=counters,closed=True))
            for field,value in (('memory.max',str(MEMORY+1)),('memory.peak',str(MEMORY+1)),
                    ('memory.swap.peak','1'),('cpu.max','300000 100000'),('pids.max','513'),('memory.events','max 1\noom 0\noom_kill 0')):
                rejected(lambda:validate_cgroup(dict(before=counters,after=dict(counters,**{field:value}),closed=True)))
            source=dict(source_commit='0'*40,source_archive_sha256='1'*64)
            with patch.dict(os.environ,BORSUK_COVERAGE_SOURCE_COMMIT=source['source_commit'],
                    BORSUK_COVERAGE_ARCHIVE_SHA256=source['source_archive_sha256']):
                real_cleanup = helper.cleanup_failure
                for mode in ('success','nonzero','cleanup','interrupt','observer'):
                    out=work/('stage-'+mode); out.mkdir()
                    result=dict(exit_status=2 if mode in ('nonzero','observer') else 0,process_cleanup=mode!='cleanup')
                    diagnostic=dict(schema='borsuk-cohere-top32-preparation-resources-v1', passed=False,
                        resource_failure=dict(stage='fixed_locator_extraction',error_type='ValueError',
                            error='cgroup memory admission failure', cgroup_path='/synthetic/cgroup',
                            cgroup={'memory.current':'1234','memory.peak':'2345','memory.max':str(MEMORY),
                                'memory.stat':'anon 100\nfile 1134','memory.events':'max 271\noom 0\noom_kill 0',
                                'memory.swap.current':'0','memory.swap.max':'0','memory.swap.peak':'0',
                                'memory.swap.events':'max 0\nfail 0'},process_pid=1234,
                            process_rss_kib=42,process_max_rss_kib=42,child_max_rss_kib=0,snapshot_errors={}))
                    def prepared(*args):
                        if mode=='interrupt':
                            raise KeyboardInterrupt()
                        if mode=='observer':
                            screen=out/'screen'; screen.mkdir(); write(screen/'source.raw',b'owned input')
                            write(screen/'failure-resources.json',diagnostic)
                            write(screen/'failure.json',dict(error_type='ValueError',error='cgroup memory admission failure'))
                        return result
                    def cleaned(screen):
                        if mode=='observer':
                            saved=json.loads((out/'failure.json').read_bytes())
                            assert saved['helper_failure_resources']==diagnostic, 'diagnostic must precede controller cleanup'
                            assert saved['helper_failure']['error']=='cgroup memory admission failure'
                        return real_cleanup(screen)
                    with patch.object(module,'CONFIG',target),patch.object(module,'qualify',return_value=proof), \
                         patch.object(module,'tools',return_value={}),patch.object(module,'capture_cgroup',return_value=counters), \
                         patch.object(module,'run_process',side_effect=prepared) as process, \
                         patch.object(helper,'cleanup_failure',side_effect=cleaned), \
                         patch.object(module,'validate_coverage',return_value='FAIL'):
                        try:
                            closed=stage(repo,out,PREFIX+'a0001')
                        except (AssertionError,KeyboardInterrupt):
                            assert mode!='success'
                        else:
                            assert mode=='success' and closed['closed'] and closed['helper_invocations']==1
                    process.assert_called_once()
                    assert process.call_args.args[2]<=1800
                    failure=json.loads((out/'failure.json').read_bytes())
                    assert failure['status']==('complete' if mode=='success' else 'failed')
                    if mode=='observer':
                        assert failure['helper_failure_resources']==diagnostic
                        assert not (out/'screen/source.raw').exists()
            from scripts import run_native_semantic_1m_quality as native
            process=Mock(pid=1234)
            with patch.object(native.subprocess,'Popen',return_value=process),patch.object(native.os,'killpg') as killed:
                process.wait.side_effect=[subprocess.TimeoutExpired('mock',1),0,0]
                rejected(lambda:run_process(['mock'],work/'timeout.log',1))
                assert killed.call_args_list[0].args==(1234,signal.SIGTERM)
                assert killed.call_args_list[-1].args==(1234,signal.SIGKILL)
                assert process.wait.call_count==3

        # Route the helper's existing small production-geometry fixture through
        # this collector while its synthetic config and corpus remain scoped.
        actual_replay,actual_run=helper.replay,helper.run
        collected_once=[]
        def fixture_run(config_path,digest,base,out,prefix):
            return actual_run(config_path,digest,base,out,PREFIX+'a0001' if prefix=='synthetic/tool' else prefix)
        def fixture_replay(config_path,digest,base,screen):
            report=actual_replay(config_path,digest,base,screen)
            if collected_once:
                return report
            collected_once.append(True)
            out=work/'collected'; out.mkdir(); shutil.copytree(screen,out/'screen')
            fixture_proof=copy.deepcopy(proof)
            fixture_proof.update(helper_config=dict(path=HELPER_CONFIG,**artifact(config_path)),helper_config_sha256=digest)
            write(out/'config.json',config); write(out/'helper-config.json',Path(config_path).read_bytes())
            fixture_proof['config_sha256']=artifact(out/'config.json')['sha256']
            write(out/'source-qualification.json',dict(fixture_proof,**source))
            assurance=json.loads((screen/'source-qualification.json').read_bytes())
            write(out/'archived-builder-assurance.json',assurance)
            fixture_proof['builder_assurance_sha256']=sha(encoded(assurance))
            write(out/'source-qualification.json',dict(fixture_proof,**source))
            write(out/'cpu.txt',b'Architecture: x86_64\n'); write(out/'profile.log',b'synthetic helper closed\n')
            write(out/'profile-resources.txt',b'Maximum resident set size (kbytes): 42\nExit status: 0\n')
            write(out/'run-closed.log',b'synthetic whole service closed\n')
            write(out/'tool-versions.json',dict(versions=VERSIONS,architecture='x86_64',
                os_release=dict(ID='ubuntu',VERSION_ID='24.04'),threads=2,aws_max_attempts=1,python='3.12.0',
                thread_environment=dict.fromkeys(THREAD_ENV,'2')))
            write(out/'profile-cgroup.json',dict(before=counters,after=counters,closed=True))
            write(out/'coverage-closure.json',dict(schema='borsuk-cohere-top32-coverage-closure-v1',
                closed=True,process_cleanup=True,prefix=PREFIX+'a0001',helper_invocations=1,helper_exit_code=0,
                scorer_invocations=0,coverage_only=True,returned_recall_measured=False,cold_http_measured=False,
                wall_seconds=.01,config_sha256=fixture_proof['config_sha256'],helper_config_sha256=digest,**source))
            write(out/'failure.json',dict(status='complete',helper_exit_code=0))
            launch=dict(**source,instance_id='i-original',nodes={'0':dict(instance_id='i-original')},prefix=PREFIX+'a0001')
            terminal=dict(**source,**{n:fixture_proof[n] for n in TERMINAL_IDENTITIES},schema=SCHEMA,
                instance_id='i-original',status='complete',phase='complete',exit_code=0,original_exit_code=0,
                artifacts={n:artifact(out/n) for n in ARTIFACTS})
            for name,value in (('aws-launch.json',launch),('aws-closeout.json',dict(state='terminated',nodes=launch['nodes'])),
                    ('aws-reservation.json',dict(schema=SCHEMA,qualification=fixture_proof,**source))):
                write(out/name,value)
            bodies={n:(out/n).read_bytes() for n in ARTIFACTS}; s3=Mock()
            def get(**kwargs):
                key=kwargs['Key']
                return {'Body':io.BytesIO(encoded(terminal) if key.endswith('/terminal.json') else bodies[key.split('/artifacts/',1)[1]])}
            s3.get_object.side_effect=get
            # Genuine helper replay runs on newly collected paths with original
            # nominal pointer bodies intact; no heavy scratch or native call.
            with patch.object(module,'qualify',return_value=fixture_proof):
                assert collect(s3,launch['prefix'],out,'i-original','0'*40,'1'*64)['exit_code']==0
                assert s3.get_object.call_count==len(ARTIFACTS)+1
                assert replay(out)['coverage_status']=='GO-for-native-investigation'
                with patch.object(module,'validate_coverage',return_value='FAIL'):
                    closed=replay(out); assert closed['executed'] and closed['coverage_status']=='FAIL' and closed['exit_status']==0
                # Full-body identity, immutable nomination, roster, terminal pins,
                # and termination must all reject before accepting execution.
                for name in ('source_commit','builder_binary_sha256','config_sha256'):
                    old=terminal[name]; terminal[name]='f'*len(old); write(out/'aws-terminal.json',terminal)
                    rejected(lambda:replay(out)); terminal[name]=old
                write(out/'aws-terminal.json',terminal)
                nominal=out/'screen/nomination.json'; nominal.chmod(0o644)
                rejected(lambda:replay(out)); nominal.chmod(0o444)
                bodies['screen/truth.i64']=b'tamper'; rejected(lambda:collect(s3,launch['prefix'],out,'i-original','0'*40,'1'*64))
                bodies['screen/truth.i64']=(screen/'truth.i64').read_bytes()
                terminal['artifacts']['screen/raw.secret']=dict(bytes=1,sha256='0'*64)
                rejected(lambda:collect(s3,launch['prefix'],out,'i-original','0'*40,'1'*64)); del terminal['artifacts']['screen/raw.secret']
                closed=dict(state='terminated',nodes={'0':dict(instance_id='i-unowned')})
                write(out/'aws-closeout.json',closed); s3.reset_mock()
                rejected(lambda:collect(s3,launch['prefix'],out,'i-original','0'*40,'1'*64)); s3.get_object.assert_not_called()
                write(out/'aws-closeout.json',dict(state='running',nodes=launch['nodes']))
                rejected(lambda:collect(s3,launch['prefix'],out,'i-original','0'*40,'1'*64)); s3.get_object.assert_not_called()
            return report
        with patch.object(helper,'run',side_effect=fixture_run),patch.object(helper,'replay',side_effect=fixture_replay),redirect_stdout(io.StringIO()):
            helper.self_check()
        assert collected_once
    if before_draft is not None:
        assert artifact(draft)==before_draft
    signal.alarm(0)
    assert time.monotonic()-started<55
    print('PASS temporary8GiB60code/45refs/archived builder; actual26-artifact relocated helper replay; observer diagnostics before cleanup; mocked ACK/fsync/multiACK/interrupt/termination wait, terminal/body/roster/resource failures. Cloud/native UNRUN.')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif sys.argv[1:2] == ['--stage']:
        assert len(sys.argv) == 5, '--stage REPO OUTPUT PREFIX'
        stage(*sys.argv[2:])
    elif sys.argv[1:2] == ['--replay']:
        assert len(sys.argv) == 3, '--replay OUTPUT'
        print(json.dumps(replay(sys.argv[2]), sort_keys=True))
    else:
        assert len(sys.argv) == 2, 'usage: aNNNN | --self-check | --stage REPO OUTPUT PREFIX | --replay OUTPUT'
        with open('/tmp/borsuk-cohere-top32-coverage-launch.lock','a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
