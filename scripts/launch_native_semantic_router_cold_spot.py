"""Launch one frozen FIRST100k semantic/graph cold HTTP Spot experiment.

Authority pointers use {path, bytes, sha256, key}; archived receipt/log pointers
may also use archived_path/archived_sha256 for gzip transport. Root supplies the
config, standalone native_proof, completed native_assurance and exact 399-file
native_source_manifest. This controller never qualifies or rebuilds native code.
publication uses the preparation helper's contract unchanged; native_publisher
is a separate {path,bytes,sha256,key} download pointer. The asset archive is the
immutable declaration in publication-assets-preparation.json, never child input.
"""

import fcntl
import gzip
import io
import importlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from shlex import quote
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('qualification requires Python assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_metadata_ranges_cold_spot as shared
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts import prepare_native_semantic_publication as publication
from scripts.check_native_startup_build import source_hashes, source_identity

ROOT = Path('docs/research/performance-architecture-20260930/semantic-cold')
CONFIG = ROOT / 'config.json'
NAME = ''
SCHEMA = 'borsuk-native-semantic-router-cold-spot-v1'
PREFIX = 'research/semantic-router/20261001/cold-'
TOKEN_PREFIX = 'semantic-router-cold-'
TAG = 'borsuk-semantic-router-cold'
WALL = 3600
INSTANCE_TYPE = 'm7i.2xlarge'
IMAGE_ID = 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME = '/dev/sda1'
RUNTIME_OS = dict(ID='ubuntu', VERSION_ID='24.04')
RUNTIME_GLIBC = '2.39'
SUBNET = peer.SUBNET
SPOT_MAX_USD_PER_HOUR = COMPUTE_CAP = .50
BINARY_SHA = 'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533'
BINARY_BYTES = 16191384
GATES = ('affected-final', 'release-final', 'clippy-final', 'test-build-final', 'full-workspace-final')
ASSET_PREPARATION = ROOT / 'publication-assets-preparation.json'
ASSET_PREPARATION_SHA = 'e13e18118b5313f4e8bf4427aab72eb00e7d1e16ae18113e91ce7fc5a0ecf6c1'
EXTRAS = ('scripts/launch_native_semantic_router_cold_spot.py',
          'scripts/launch_native_metadata_ranges_cold_spot.py',
          'scripts/launch_native_startup_profile_spot.py', 'scripts/launch_native_peer_1m_spot.py',
          'scripts/launch_v174_relaid_bind_compile_spot.py', 'scripts/launch_v157_primary_feasibility_spot.py',
          'scripts/check_native_startup_build.py', 'scripts/prepare_native_semantic_publication.py',
          'scripts/package_semantic_native_generation.py')
AUTHORITY_FILES = ('native-assurance.json', 'boundary-check.json', 'native-source-manifest.json',
                   'native-source.tar.gz', 'qualified-source.tar.gz', 'publisher-proof.json', 'asset-manifest.json',
                   *(f'assurance/{gate}.{suffix}' for gate in GATES for suffix in ('json', 'log')))
PUBLICATION_FILES = ('config.json', 'asset-manifest.json', 'publication-receipt.json',
                     *(f'{dataset}/{arm}/{name}' for dataset in ('ReLAION', 'CoHere')
                       for arm in ('control', 'candidate')
                       for name in ('native.jsonl', 'stdout.log', 'stderr.log', 'resources.txt', 'head.json')))
ARTIFACTS = ('source-qualification.json', 'runtime-abi.json', 'binaries/two_bit_http', 'binaries/two_bit_plan_demo', 'cpu.txt', 'run-closed.log',
             'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
             'screen/records.jsonl', 'screen/summary.json', 'screen/config.json', 'screen/qualification.json',
             *AUTHORITY_FILES, *('publication/' + name for name in PUBLICATION_FILES))
TERMINAL_IDENTITIES = ('config_sha256', 'qualification_sha256', 'binary_sha256', 'binary_bytes',
    'native_source_commit', 'source_identity_sha256', 'source_file_count', 'artifact_roster_sha256',
    'native_source_archive_sha256', 'native_source_manifest_sha256', 'native_assurance_sha256',
    'publisher_sha256', 'publisher_bytes', 'publisher_qualification_sha256', 'asset_manifest_sha256',
    'publication_assets', 'asset_preparation_sha256', 'runtime_os', 'runtime_glibc', 'required_glibc')


def _worker():
    # The runtime is integrated independently; a missing module is a launch blocker.
    return importlib.import_module('scripts.run_native_semantic_router_cold')


@contextmanager
def _cwd(base):
    previous = Path.cwd()
    os.chdir(base)
    try:
        yield
    finally:
        os.chdir(previous)


def _identity(body):
    return dict(bytes=len(body), sha256=peer.sha(body))


def _read(base, pointer):
    assert type(pointer['bytes']) is int and pointer['bytes'] > 0
    assert len(pointer['sha256']) == 64 and all(c in '0123456789abcdef' for c in pointer['sha256'])
    if 'key' in pointer:
        assert isinstance(pointer['key'], str) and pointer['key'] and '\n' not in pointer['key']
    if 'archived_path' in pointer:
        body = (base / pointer['archived_path']).read_bytes()
        if 'archived_sha256' in pointer:
            assert peer.sha(body) == pointer['archived_sha256']
        # Receipt gzip is transport; a qualified .tar.gz is itself the artifact.
        if pointer['archived_path'].endswith('.gz') and not pointer['path'].endswith('.gz'):
            body = gzip.decompress(body)
    else:
        body = (base / pointer['path']).read_bytes()
    assert _identity(body) == {key: pointer[key] for key in ('bytes', 'sha256')}
    return body


def _archive_sources(body):
    with tarfile.open(fileobj=io.BytesIO(body), mode='r:gz') as archive:
        members = [m for m in archive if m.isfile() and
                   (m.name.endswith('.rs') or Path(m.name).name in ('Cargo.toml', 'Cargo.lock'))]
        assert len(members) == len({m.name for m in members}), 'duplicate source archive entries'
        assert all(not Path(m.name).is_absolute() and '..' not in Path(m.name).parts for m in members)
        return {m.name: peer.sha(archive.extractfile(m).read()) for m in members}


def _version(value):
    assert re.fullmatch(r'[0-9]+(?:\.[0-9]+)+', value), 'invalid GLIBC version'
    return tuple(map(int, value.split('.')))


def _required_glibc(path):
    # Only version needs, never exported definitions or a binary invocation.
    result = subprocess.run(['readelf', '--version-info', '--wide', str(path)],
        capture_output=True, text=True, check=True, timeout=15, env=dict(os.environ, LC_ALL='C'))
    needs = result.stdout.split('Version needs section', 1)
    assert len(needs) == 2, 'missing ELF version needs'
    versions = re.findall(r'\bName: GLIBC_(\S+)', needs[1])
    assert versions, 'missing GLIBC requirements'
    return max(versions, key=_version)


def _validate_runtime_abi(qualification, report):
    assert report['schema'] == 'borsuk-native-semantic-runtime-abi-v1' and report['qualified'] is True
    assert report['os_release'] == qualification['runtime_os'] == RUNTIME_OS, 'runtime OS'
    assert report['architecture'] == 'x86_64', 'runtime architecture'
    assert report['libc'] == 'glibc' and qualification['runtime_glibc'] == RUNTIME_GLIBC, 'runtime libc'
    assert report['required_glibc'] == qualification['required_glibc'], 'runtime binary GLIBC requirements'
    assert report['binaries'] == {
        'two_bit_http': dict(bytes=qualification['binary_bytes'], sha256=qualification['binary_sha256']),
        'two_bit_plan_demo': dict(bytes=qualification['publisher_bytes'], sha256=qualification['publisher_sha256'])}, 'runtime binary identities'
    assert set(report['ldd']) == set(report['binaries']) == set(report['required_glibc'])
    for name, result in report['ldd'].items():
        assert _version(report['required_glibc'][name]) <= _version(report['glibc_version']), 'incompatible GLIBC'
        assert type(result['returncode']) is int and result['returncode'] == 0, 'ldd failed'
        output = (result['stdout'] + result['stderr']).lower()
        assert output.strip() and not any(s in output for s in ('not found', 'undefined symbol', 'unresolved')), 'unresolved runtime dependency'


def _runtime_abi(out):
    out = Path(out)
    qualification = json.loads((out/'source-qualification.json').read_bytes())
    report = dict(schema='borsuk-native-semantic-runtime-abi-v1', qualified=False)
    try:
        release = platform.freedesktop_os_release()
        libc, version = os.confstr('CS_GNU_LIBC_VERSION').split()
        report.update(os_release={k: release[k] for k in RUNTIME_OS}, architecture=platform.machine(),
                      libc=libc, glibc_version=version, binaries={}, required_glibc={}, ldd={})
        for name in ('two_bit_http', 'two_bit_plan_demo'):
            path = out/'binaries'/name
            report['binaries'][name] = _identity(path.read_bytes())
            assert report['binaries'][name] == dict(
                bytes=qualification['binary_bytes' if name == 'two_bit_http' else 'publisher_bytes'],
                sha256=qualification['binary_sha256' if name == 'two_bit_http' else 'publisher_sha256']), 'ABI binary authentication'
            report['required_glibc'][name] = _required_glibc(path)
            result = subprocess.run(['ldd', '-r', str(path)], capture_output=True, text=True,
                                    timeout=15, env=dict(os.environ, LC_ALL='C'))
            report['ldd'][name] = dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        report['qualified'] = True
        _validate_runtime_abi(qualification, report)
    except BaseException:
        report['qualified'] = False
        raise
    finally:
        (out/'runtime-abi.json').write_text(json.dumps(report, sort_keys=True, separators=(',', ':')) + '\n')
    return report


def _qualify(base, binary_override=None, publisher_override=None):
    base = Path(base).resolve()
    body = (base / CONFIG).read_bytes()
    config = json.loads(body)
    assert not config.get('authority_pending'), 'publication/cold authorities pending'
    expected = dict(schema='borsuk-native-semantic-router-cold-v1', architecture='x86_64',
        region=peer.REGION, bucket=peer.BUCKET, count=64, k=10, ann_queries=512,
        dataset_order=['ReLAION', 'CoHere'], blocks=['control0', 'candidate1', 'candidate2', 'control3'],
        client_cpu_affinity=[4, 5], native_cpu_affinity=[0, 1, 2, 3],
        worker_limit_seconds=3000, machine_limit_seconds=WALL, instance_type=INSTANCE_TYPE,
        image_id=IMAGE_ID, subnet_id=SUBNET, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
        compute_cap_usd=COMPUTE_CAP, ebs_s3_allowance_usd=.15, profile_memory_bytes=8*1024**3,
        native_rlimit_as_bytes=4*1024**3, profile_swap_bytes=0, native_memory_bytes=536870912,
        credential_protocol='instance-imdsv2')
    assert all(config[key] == value for key, value in expected.items()), 'fixed protocol/infrastructure'
    worker = _worker()
    with _cwd(base):
        worker.validate_config(config)
    assert set(config['code_sha256']) == set(worker.CODE)
    assert set(config['controller_code_sha256']) == set(EXTRAS)
    code = dict(config['code_sha256'], **config['controller_code_sha256'])
    for name, digest in code.items():
        assert peer.sha((base/name).read_bytes()) == digest, name
    binary_pointer = config['native_binary']
    assert {key: binary_pointer[key] for key in ('bytes', 'sha256')} == config['binary'] == dict(bytes=BINARY_BYTES, sha256=BINARY_SHA)
    binary = Path(binary_override).read_bytes() if binary_override is not None else _read(base, binary_pointer)
    assert _identity(binary) == config['binary']
    files = {name: _read(base, config[key]) for key, name in (
        ('native_assurance', 'native-assurance.json'), ('native_proof', 'boundary-check.json'),
        ('native_source_manifest', 'native-source-manifest.json'))}
    manifest = json.loads(files['native-source-manifest.json'])
    identities = source_hashes(base)
    identity = source_identity(identities)
    assert manifest['schema'] == 'borsuk-native-semantic-router-source-manifest-v1'
    assert identities == manifest['source_sha256']
    assert len(identities) == manifest['source_file_count'] == config['native_source_file_count'] == 399
    assert identity == manifest['source_identity_sha256'] == config['native_source_identity_sha256']
    files['native-source.tar.gz'] = _read(base, manifest['native_source_archive'])
    assert _archive_sources(files['native-source.tar.gz']) == identities, 'complete qualified source archive'
    proof = json.loads(files['boundary-check.json'])
    assert config['qualification_sha256'] == peer.sha(files['boundary-check.json'])
    assert proof['qualified'] is True and type(proof['green_status']) is type(proof['release_status']) is int
    assert proof['green_status'] == proof['release_status'] == 0
    assert proof['binary_sha256'] == BINARY_SHA
    assert proof['source_file_count'] == 399 and proof['source_identity_sha256'] == identity
    assert proof['compiled_native_sha256'] and all(identities.get(name) == digest
        for name, digest in proof['compiled_native_sha256'].items())
    assurance = json.loads(files['native-assurance.json'])
    assert assurance['full_workspace_test_execution'] is True, 'completed full workspace execution required'
    assert {k: assurance['binaries']['two_bit_http'][k] for k in ('bytes', 'sha256')} == config['binary']
    files['qualified-source.tar.gz'] = _read(base, assurance['source_archive'])
    qualified = _archive_sources(files['qualified-source.tar.gz'])
    assert qualified and all(identities.get(name) == digest for name, digest in qualified.items())
    for name in GATES:
        gate = assurance['gates'][name]
        assert type(gate['exit_status']) is int and gate['exit_status'] == 0, name
        receipt_body, log = _read(base, gate['receipt']), _read(base, gate['log'])
        receipt = json.loads(receipt_body)
        assert type(receipt['exit_status']) is int and receipt['exit_status'] == 0
        assert receipt['source_unchanged'] is True and receipt['command'] == gate['command']
        if name == 'full-workspace-final':
            assert receipt['artifacts']['test.log'] == _identity(log)
        else:
            assert receipt['log_sha256'] == peer.sha(log)
        for path, digest in receipt['source_sha256'].items():
            matches = ([identities[path]] if path in identities else
                       [d for n, d in identities.items() if Path(n).name == path])
            assert matches == [digest], 'receipt source identity: ' + path
        if name == 'full-workspace-final':
            assert receipt['source_sha256'] == identities
            assert receipt['source_identity_sha256'] == identity and receipt['source_file_count'] == 399
            assert 'test' in receipt['command'] and '--workspace' in receipt['command']
            assert '--no-run' not in receipt['command'], 'compilation is not full-suite execution'
        files[f'assurance/{name}.json'], files[f'assurance/{name}.log'] = receipt_body, log
    pub = config['publication']
    publisher_pointer = config['native_publisher']
    assert set(publisher_pointer) == {'path', 'bytes', 'sha256', 'key'}, 'publisher download pointer'
    assert {k: publisher_pointer[k] for k in ('bytes', 'sha256')} == publication.identity(pub['publisher'])
    publisher = Path(publisher_override) if publisher_override is not None else base / publisher_pointer['path']
    assert publisher_pointer['key'] and '\n' not in publisher_pointer['key']
    with _cwd(base):
        publication.validate_publisher(config, publisher)
        files['publisher-proof.json'], _ = publication.authenticated_json(pub['qualification'])
        files['asset-manifest.json'], assets = publication.authenticated_json(pub['asset_manifest'])
        preparation = publication.files.metadata(publication.PREPARATION, publication.PREPARATION_SHA,
                                                 cap=publication.CAP)[1]
        references = publication.files.metadata(publication.REFERENCES, publication.REFERENCES_SHA,
                                                cap=publication.CAP)[1]
        publication.validate_manifest(config, assets, preparation, references)
    declared_body = (base / ASSET_PREPARATION).read_bytes()
    assert peer.sha(declared_body) == ASSET_PREPARATION_SHA, 'immutable asset preparation authority'
    declared = json.loads(declared_body)
    assert declared['schema'] == 'borsuk-native-semantic-publication-assets-preparation-v1'
    publication.object_identity(pub['assets'])
    assert pub['assets'] == {k: declared['assets'][k] for k in ('key', 'bytes', 'sha256')}
    assert pub['asset_manifest'] == declared['manifest'], 'declared asset manifest identity'
    assert set(files) == set(AUTHORITY_FILES)
    required_glibc = dict(two_bit_http=_required_glibc(
        Path(binary_override) if binary_override is not None else base/binary_pointer['path']),
        two_bit_plan_demo=_required_glibc(publisher))
    assert all(_version(v) <= _version(RUNTIME_GLIBC) for v in required_glibc.values()), 'target Ubuntu GLIBC too old'
    qualification = dict(config_path=str(CONFIG), config_sha256=peer.sha(body), campaign_schema=SCHEMA,
        native_source_commit=manifest['native_source_commit'], source_identity_sha256=identity,
        source_file_count=399, code_sha256=code, binary_sha256=BINARY_SHA, binary_bytes=BINARY_BYTES,
        qualification_sha256=config['qualification_sha256'], native_rebuilt=False,
        current_full_suite_pass_claim=True, full_workspace_test_execution=True,
        native_source_archive_sha256=manifest['native_source_archive']['sha256'],
        native_source_manifest_sha256=peer.sha(files['native-source-manifest.json']),
        native_assurance_sha256=peer.sha(files['native-assurance.json']),
        native_publisher=publisher_pointer, publisher_sha256=pub['publisher']['sha256'],
        publisher_bytes=pub['publisher']['bytes'], publisher_qualification_sha256=pub['qualification']['sha256'],
        asset_manifest_sha256=pub['asset_manifest']['sha256'], publication_assets=pub['assets'],
        asset_preparation_sha256=peer.sha(declared_body),
        runtime_os=RUNTIME_OS, runtime_glibc=RUNTIME_GLIBC, required_glibc=required_glibc,
        native_binary=binary_pointer, authority_artifacts={n: _identity(b) for n, b in files.items()},
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()))
    return qualification, files


def preflight(base=Path('.')):
    return _qualify(base)[0]


def _stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    expected = json.loads((out/'source-qualification.json').read_bytes())
    (out/'binaries/two_bit_plan_demo').chmod(0o755)
    actual, files = _qualify(repo, out/'binaries/two_bit_http', out/'binaries/two_bit_plan_demo')
    assert actual == expected, 'remote qualification differs from local authority'
    for name, body in files.items():
        path = out/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    (out/'binaries/two_bit_http').chmod(0o755)
    _runtime_abi(out)


def _published(config, digest, bodies):
    """Authenticate the closed helper receipt and every small publication body."""
    assert set(bodies) == set(PUBLICATION_FILES), 'closed publication body roster'
    assert peer.sha(bodies['config.json']) == digest
    assert _identity(bodies['asset-manifest.json']) == {k: config['publication']['asset_manifest'][k]
                                                      for k in ('bytes', 'sha256')}
    manifest = json.loads(bodies['asset-manifest.json'])
    receipt = json.loads(bodies['publication-receipt.json'])
    assert receipt['schema'] == 'borsuk-native-semantic-publication-receipt-v1'
    assert receipt['outcome'] == 'published-and-validated' and receipt['config_sha256'] == digest
    assert receipt['publication'] == config['publication']
    assert receipt['native_source_identity_sha256'] == config['native_source_identity_sha256']
    assert type(receipt['native_source_file_count']) is int and receipt['native_source_file_count'] == 399
    assert receipt['asset_files'] == manifest['files']
    assert receipt['canonical_objects'] == {i['dataset']: i['canonical'] for i in manifest['items']}
    assert receipt['source_identities'] == {i['dataset']: i['source_identity'] for i in config['items']}
    assert receipt['preparation_sha256'] == publication.PREPARATION_SHA
    assert receipt['reference_authority_sha256'] == publication.REFERENCES_SHA
    assert set(receipt['adapter_code']) == {'scripts/prepare_native_semantic_publication.py',
                                           'scripts/package_semantic_native_generation.py'}
    for name, identity in receipt['adapter_code'].items():
        assert identity['sha256'] == config['controller_code_sha256'][name]
    assert receipt['cold_performance_measured'] is receipt['credential_values_recorded'] is False
    arms = [(item, name) for item in config['items'] for name in ('control', 'candidate')]
    assert len(receipt['arms']) == len(arms) == 4, 'four closed publication arms required'
    for row, (item, name) in zip(receipt['arms'], arms):
        arm = item['arms'][name]
        assert (row['dataset'], row['arm'], row['prefix']) == (item['dataset'], name, arm['indexes']['10'])
        assert row['authority'] == arm['authority']
        assert row['outcome'] == 'published-and-validated'
        assert type(row['native_invocations']) is type(row['returncode']) is int
        assert row['native_invocations'] == 1 and row['returncode'] == 0
        validation = row['validation']
        assert type(validation['validated_queries']) is int and validation['validated_queries'] == 64
        assert validation['validated_fields'] == list(publication.KNOWN_PARITY)
        assert validation['unknown'] == list(publication.UNKNOWN_NATIVE)
        assert {k: validation['startup'][k] for k in arm['authority']} == arm['authority']
        assert 0 < row['resources']['max_rss_kib'] <= 524288
        prefix = item['dataset'] + '/' + name + '/'
        assert set(row['artifacts']) == {'native.jsonl', 'stdout.log', 'stderr.log', 'resources.txt', 'head.json'}
        for path, identity in row['artifacts'].items():
            assert _identity(bodies[prefix + path]) == identity, prefix + path
        head = json.loads(bodies[prefix + 'head.json'])
        assert head == row['head'] == dict(schema='borsuk-two-bit-head-v2',
            epoch=arm['authority']['control_epoch'], generation=arm['authority']['generation'],
            root_sha256=arm['authority']['root_sha256'], mutation=None, fence=None)
        assert _identity(bodies[prefix + 'head.json']) == arm['head_file']
    return receipt


def _publish(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    qualification = json.loads((out/'source-qualification.json').read_bytes())
    _validate_runtime_abi(qualification, json.loads((out/'runtime-abi.json').read_bytes()))
    body = (repo/CONFIG).read_bytes()
    assert peer.sha(body) == qualification['config_sha256']
    config = json.loads(body)
    with _cwd(repo):
        try:
            publication.run(repo/CONFIG, qualification['config_sha256'], out/'binaries/two_bit_plan_demo',
                            out/'publication')
        except Exception as error:
            # Match the helper CLI: SDK exception text can contain credentials.
            raise RuntimeError('publication failed: ' + type(error).__name__) from None
    bodies = {n: (out/'publication'/n).read_bytes() for n in PUBLICATION_FILES}
    return _published(config, qualification['config_sha256'], bodies)


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert len(commit) == 40 and len(archive_sha) == 64
    with patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace('v174-relaid-bind-compile', 'native-semantic-router-cold')
    bootstrap = 'exec >run.log 2>&1\naws s3 cp '
    assert body.count(bootstrap) == 1, 'bootstrap early AWS hook changed'
    body = body.replace(bootstrap, '''exec >run.log 2>&1
export DEBIAN_FRONTEND=noninteractive
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install awscli python3-boto3 python3.12 time tar gzip util-linux binutils
aws s3 cp ''')
    # The bootstrap creates the trap and authenticates the campaign archive.
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':')).encode()
    import base64
    encoded = base64.b64encode(gzip.compress(proof, mtime=0)).decode()
    binary_key = quote('s3://' + peer.BUCKET + '/' + qualification['native_binary']['key'])
    publisher_key = quote('s3://' + peer.BUCKET + '/' + qualification['native_publisher']['key'])
    command = f'''phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("{encoded}")))'
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir binaries
aws s3 cp {binary_key} binaries/two_bit_http --only-show-errors
aws s3 cp {publisher_key} binaries/two_bit_plan_demo --only-show-errors
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --stage "$root/repo" "$root"
phase=publication
systemd-run --unit=native-semantic-publication --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3600 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.launch_native_semantic_router_cold_spot --publish "$1/repo" "$1"' _ "$root"
phase=profile
systemd-run --unit=native-semantic-router-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_router_cold {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    command += '\n'.join('test ' + ('-f' if name.startswith('publication/') and
                          name.endswith(('/stdout.log', '/stderr.log')) else '-s') +
                          ' "$root/' + name + '"' for name in ARTIFACTS) + '\n'
    body = body[:start] + command + body[end:]
    # Terminal identities are emitted by the bootstrap alongside its byte roster.
    marker = "'source_archive_sha256':'" + archive_sha + "',"
    terminal_fields = {key: qualification[key] for key in TERMINAL_IDENTITIES}
    assert body.count(marker) == 1, 'bootstrap terminal identity hook changed'
    body = body.replace(marker, marker + repr(terminal_fields)[1:-1] +
                        ", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),")
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    reservation = json.loads((out/'aws-reservation.json').read_bytes())
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    qualification = reservation['qualification']
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == reservation['source_commit'] == commit
    assert terminal['source_archive_sha256'] == reservation['source_archive_sha256'] == digest
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == qualification[key], key
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    files = {}
    for name, identity in terminal['artifacts'].items():
        data = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/artifacts/'+name)['Body'].read()
        assert _identity(data) == identity, name
        path = out/(name+'.gz')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(data, mtime=0))
        files[name] = data
    assert terminal['runtime_abi_sha256'] == (peer.sha(files['runtime-abi.json'])
        if 'runtime-abi.json' in files else None), 'terminal ABI report identity'
    if terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0:
        assert set(files) == set(ARTIFACTS), 'complete artifact roster'
        assert json.loads(files['source-qualification.json']) == qualification
        _validate_runtime_abi(qualification, json.loads(files['runtime-abi.json']))
        for name, identity in qualification['authority_artifacts'].items():
            assert _identity(files[name]) == identity, name
        assert _identity(files['binaries/two_bit_http']) == dict(bytes=qualification['binary_bytes'], sha256=qualification['binary_sha256'])
        assert _identity(files['binaries/two_bit_plan_demo']) == dict(bytes=qualification['publisher_bytes'],
                                                                   sha256=qualification['publisher_sha256'])
        assert peer.sha(files['screen/config.json']) == qualification['config_sha256']
        assert peer.sha(files['boundary-check.json']) == peer.sha(files['screen/qualification.json']) == qualification['qualification_sha256']
        assert peer.sha(files['publisher-proof.json']) == qualification['publisher_qualification_sha256']
        assert peer.sha(files['asset-manifest.json']) == qualification['asset_manifest_sha256']
        config = json.loads(files['screen/config.json'])
        assert config['native_source_identity_sha256'] == qualification['source_identity_sha256']
        assert config['native_source_file_count'] == qualification['source_file_count']
        assert config['publication']['publisher'] == dict(bytes=qualification['publisher_bytes'],
                                                         sha256=qualification['publisher_sha256'])
        assert config['publication']['assets'] == qualification['publication_assets']
        _published(config, qualification['config_sha256'], {n: files['publication/'+n] for n in PUBLICATION_FILES})
    return terminal


def main(attempt):
    return shared.main(attempt, campaign=sys.modules[__name__])


def lifecycle_self_check():
    """Exercise this campaign through the actual shared lifecycle, including wait."""
    from datetime import datetime, timezone
    import tempfile
    from unittest.mock import Mock
    from botocore.exceptions import ReadTimeoutError, EndpointConnectionError
    module = sys.modules[__name__]
    tokens = []
    for failure in ('success', 'fsync', 'upload', 'poll', 'interruption', 'interrupt', 'multi-ack', 'multi-ack-fsync'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'synthetic-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ids = ['i-owned', 'i-extra'] if failure.startswith('multi-ack') else ['i-owned']
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
            def collected(*args):
                assert events == ['terminate', 'wait']
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0, artifacts={n: {} for n in ARTIFACTS})
            error = {'poll': ReadTimeoutError(endpoint_url='synthetic'),
                     'interruption': RuntimeError('interrupted'), 'interrupt': KeyboardInterrupt()}.get(failure)
            writes = [None, None, OSError('upload')] if failure == 'upload' else [None]*3
            attempt = 'a' + str(len(tokens)+1).zfill(4)
            with patch.object(module, 'ROOT', Path(tmp)), patch.object(module, 'preflight', return_value={'config_sha256': 'a'*64}), \
                    patch.object(module, 'user_data', return_value='synthetic'), patch.object(module, 'poll', side_effect=error), \
                    patch.object(module, 'collect', side_effect=collected), patch.object(shared.boto3, 'Session', return_value=session), \
                    patch.object(shared.subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), \
                    patch.object(peer, 'missing', return_value=True), patch.object(peer, 'put_if_absent', side_effect=writes), \
                    patch.object(shared.os, 'fsync', side_effect=OSError('persist') if failure.endswith('fsync') else None) as fsync:
                try:
                    main(attempt)
                except (OSError, RuntimeError, ReadTimeoutError, KeyboardInterrupt):
                    assert failure not in ('success', 'multi-ack'), 'unexpected lifecycle failure'
                else:
                    assert failure in ('success', 'multi-ack'), 'failure swallowed'
                fsync.assert_called_once()
                shared.boto3.Session.assert_called_once_with(profile_name='causality', region_name=peer.REGION)
            assert events == ['terminate', 'wait', 'collect']
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            args = ec2.run_instances.call_args.kwargs
            assert args['InstanceType'] == INSTANCE_TYPE and args['ImageId'] == IMAGE_ID
            assert args['InstanceMarketOptions'] == {'MarketType': 'spot', 'SpotOptions': {
                'InstanceInterruptionBehavior': 'terminate', 'SpotInstanceType': 'one-time', 'MaxPrice': '0.50'}}
            assert args['BlockDeviceMappings'][0]['Ebs'] == dict(DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType='gp3')
            assert args['BlockDeviceMappings'][0]['DeviceName'] == ROOT_DEVICE_NAME == '/dev/sda1'
            assert args['NetworkInterfaces'][0]['AssociatePublicIpAddress'] is True
            assert args['IamInstanceProfile'] == {'Arn': peer.PROFILE_ARN}
            assert args['ClientToken'].startswith(TOKEN_PREFIX) and len(args['ClientToken']) <= 64
            tokens.append(args['ClientToken'])
            out = Path(tmp)/attempt
            for name in ('aws-launch.json', 'aws-closeout.json'):
                assert json.loads((out/name).read_bytes())['nodes'] == {str(i): dict(instance_id=n) for i, n in enumerate(ids)}
            reservation = json.loads((out/'aws-reservation.json').read_bytes())
            assert reservation['instance_type'] == INSTANCE_TYPE and reservation['image_id'] == IMAGE_ID
            assert reservation['root_device_name'] == ROOT_DEVICE_NAME == '/dev/sda1'
            assert reservation['compute_cap_usd'] == .50 and reservation['wall_seconds'] == WALL
            assert reservation['ebs_s3_allowance_usd'] == .15 and reservation['total_cost_measured'] is False
    assert len(tokens) == len(set(tokens))
    ec2, s3 = Mock(), Mock()
    ec2.describe_instances.side_effect = [ReadTimeoutError(endpoint_url='synthetic'),
        {'Reservations': [{'Instances': [{'State': {'Name': 'running'}}]}]}]
    with patch.object(peer, 'missing', side_effect=[EndpointConnectionError(endpoint_url='synthetic'), True, True, False]), \
            patch.object(startup.time, 'sleep'):
        poll(ec2, s3, 'synthetic', 'i-owned', startup.time.monotonic())
    assert ec2.describe_instances.call_count == 2
    assert all(c.kwargs == {'InstanceIds': ['i-owned']} for c in ec2.describe_instances.call_args_list)
    ec2.run_instances.assert_not_called()
    ec2.describe_instances.side_effect = None
    ec2.describe_instances.return_value = {'Reservations': [{'Instances': [{'State': {'Name': 'terminated'}}]}]}
    with patch.object(peer, 'missing', return_value=True), patch.object(startup.time, 'sleep'):
        try:
            poll(ec2, s3, 'synthetic', 'i-owned', startup.time.monotonic())
        except RuntimeError:
            pass
        else:
            raise AssertionError('interruption accepted')
    with patch.object(startup.time, 'monotonic', return_value=WALL+301):
        try:
            poll(ec2, s3, 'synthetic', 'i-owned', 0)
        except TimeoutError:
            pass
        else:
            raise AssertionError('wall cap ignored')


def _publication_fixture(config, manifest, manifest_body=None, config_body=None):
    """Small closed helper outputs; no data transfer or native invocation."""
    bodies = {'config.json': config_body or json.dumps(config).encode(),
              'asset-manifest.json': json.dumps(manifest).encode()}
    if manifest_body is not None:
        bodies['asset-manifest.json'] = manifest_body
    receipt = dict(schema='borsuk-native-semantic-publication-receipt-v1', outcome='published-and-validated',
        config_sha256=peer.sha(bodies['config.json']), publication=config['publication'],
        native_source_identity_sha256=config['native_source_identity_sha256'], native_source_file_count=399,
        asset_files=manifest['files'], canonical_objects={i['dataset']: i['canonical'] for i in manifest['items']},
        source_identities={i['dataset']: i['source_identity'] for i in config['items']},
        preparation_sha256=publication.PREPARATION_SHA, reference_authority_sha256=publication.REFERENCES_SHA,
        adapter_code={n: dict(bytes=42, sha256=config['controller_code_sha256'][n]) for n in
            ('scripts/prepare_native_semantic_publication.py', 'scripts/package_semantic_native_generation.py')},
        cold_performance_measured=False, credential_values_recorded=False, arms=[])
    for item in config['items']:
        for name in ('control', 'candidate'):
            arm = item['arms'][name]
            prefix = item['dataset']+'/'+name+'/'
            head = dict(schema='borsuk-two-bit-head-v2', epoch=arm['authority']['control_epoch'],
                        generation=arm['authority']['generation'], root_sha256=arm['authority']['root_sha256'],
                        mutation=None, fence=None)
            artifacts = {'native.jsonl': b'synthetic closed native output\n', 'stdout.log': b'',
                         'stderr.log': b'', 'resources.txt': b'synthetic resources\n',
                         'head.json': json.dumps(head, separators=(',', ':')).encode()}
            bodies.update({prefix+n: body for n, body in artifacts.items()})
            assert _identity(artifacts['head.json']) == arm['head_file']
            receipt['arms'].append(dict(dataset=item['dataset'], arm=name, prefix=arm['indexes']['10'],
                authority=arm['authority'], outcome='published-and-validated', native_invocations=1,
                returncode=0, head=head, artifacts={n: _identity(b) for n, b in artifacts.items()},
                resources={'max_rss_kib': 100}, validation=dict(validated_queries=64,
                    validated_fields=list(publication.KNOWN_PARITY), unknown=list(publication.UNKNOWN_NATIVE),
                    startup=arm['authority'])))
    bodies['publication-receipt.json'] = json.dumps(receipt).encode()
    return bodies


def collection_self_check():
    import tempfile
    from unittest.mock import Mock
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        files = {n: b'synthetic artifact\n' for n in ARTIFACTS}
        binary = files['binaries/two_bit_http']
        config = json.loads((Path(__file__).resolve().parents[1]/ROOT/'config-draft.json').read_bytes())
        manifest_body = (Path(__file__).resolve().parents[1]/ROOT/'publication-assets.json').read_bytes()
        manifest = json.loads(manifest_body)
        config['controller_code_sha256'] = {n: 'a'*64 for n in EXTRAS}
        config['publication']['publisher'] = _identity(files['binaries/two_bit_plan_demo'])
        config['publication']['qualification'] = dict(path='proof', **_identity(files['publisher-proof.json']))
        publication_bodies = _publication_fixture(config, manifest, manifest_body)
        files.update({'publication/'+n: b for n, b in publication_bodies.items()})
        config_body = files['screen/config.json'] = publication_bodies['config.json']
        files['asset-manifest.json'] = manifest_body
        proof = files['boundary-check.json']
        files['screen/qualification.json'] = proof
        qualification = dict(config_sha256=peer.sha(config_body), qualification_sha256=peer.sha(proof),
            binary_sha256=peer.sha(binary), binary_bytes=len(binary), native_source_commit='2'*40,
            source_identity_sha256=config['native_source_identity_sha256'], source_file_count=399, artifact_roster_sha256='4'*64,
            native_source_archive_sha256='5'*64, native_source_manifest_sha256='6'*64, native_assurance_sha256='7'*64,
            publisher_sha256=peer.sha(files['binaries/two_bit_plan_demo']),
            publisher_bytes=len(files['binaries/two_bit_plan_demo']),
            publisher_qualification_sha256=peer.sha(files['publisher-proof.json']),
            asset_manifest_sha256=peer.sha(manifest_body), publication_assets=config['publication']['assets'],
            asset_preparation_sha256=ASSET_PREPARATION_SHA,
            runtime_os=RUNTIME_OS, runtime_glibc=RUNTIME_GLIBC,
            required_glibc=dict(two_bit_http='2.38', two_bit_plan_demo='2.38'),
            authority_artifacts={n: _identity(files[n]) for n in AUTHORITY_FILES})
        files['source-qualification.json'] = json.dumps(qualification).encode()
        report = dict(schema='borsuk-native-semantic-runtime-abi-v1', qualified=True,
            os_release=RUNTIME_OS, architecture='x86_64', libc='glibc', glibc_version=RUNTIME_GLIBC,
            required_glibc=qualification['required_glibc'],
            binaries={n: _identity(files['binaries/'+n]) for n in qualification['required_glibc']},
            ldd={n: dict(returncode=0, stdout='libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6\n', stderr='')
                 for n in qualification['required_glibc']})
        files['runtime-abi.json'] = json.dumps(report).encode()
        terminal = dict(schema=SCHEMA, instance_id='i-owned', source_commit='0'*40,
            source_archive_sha256='1'*64, status='complete', phase='complete', exit_code=0,
            runtime_abi_sha256=peer.sha(files['runtime-abi.json']),
            artifacts={n: _identity(b) for n, b in files.items()}, **{k:v for k,v in qualification.items() if k != 'authority_artifacts'})
        (out/'aws-reservation.json').write_text(json.dumps(dict(source_commit='0'*40,
            source_archive_sha256='1'*64, qualification=qualification)))
        (out/'aws-closeout.json').write_text(json.dumps(dict(state='terminated', nodes={'0': dict(instance_id='i-owned')})))
        def fetched(**kwargs):
            key = kwargs['Key']
            return {'Body': io.BytesIO(json.dumps(current).encode() if key.endswith('/terminal.json')
                else files[key.split('/artifacts/')[1]])}
        s3 = Mock()
        s3.get_object.side_effect = fetched
        for change in (None, 'source', 'config', 'proof', 'binary', 'roster', 'sha', 'bytes', 'unknown',
                       'publisher', 'publication-roster', 'receipt', 'head', 'publication-config', 'closeout',
                       'abi-missing', 'abi-sha', 'abi-os', 'abi-libc', 'abi-binary', 'abi-required', 'abi-unresolved'):
            current = json.loads(json.dumps(terminal))
            original_files = dict(files)
            if change in ('source', 'config', 'proof', 'binary', 'publisher'):
                key = {'source': 'source_identity_sha256', 'config': 'config_sha256',
                       'proof': 'qualification_sha256', 'binary': 'binary_sha256', 'publisher': 'publisher_sha256'}[change]
                current[key] = 'f'*64
            elif change == 'closeout':
                (out/'aws-closeout.json').write_text(json.dumps(dict(state='running', nodes={'0': dict(instance_id='i-owned')})))
            elif change == 'publication-roster':
                del current['artifacts']['publication/CoHere/candidate/head.json']
            elif change == 'abi-missing':
                del current['artifacts']['runtime-abi.json']
                current['runtime_abi_sha256'] = None
            elif change == 'abi-sha':
                current['runtime_abi_sha256'] = 'f'*64
            elif change and change.startswith('abi-'):
                bad = json.loads(files['runtime-abi.json'])
                if change == 'abi-os': bad['os_release']['ID'] = 'amzn'
                elif change == 'abi-libc': bad['glibc_version'] = '2.34'
                elif change == 'abi-binary': bad['binaries']['two_bit_plan_demo']['sha256'] = 'f'*64
                elif change == 'abi-required': bad['required_glibc']['two_bit_http'] = '2.34'
                else: bad['ldd']['two_bit_plan_demo']['stdout'] = 'libgcc_s.so.1 => not found'
                files['runtime-abi.json'] = json.dumps(bad).encode()
                current['artifacts']['runtime-abi.json'] = _identity(files['runtime-abi.json'])
                current['runtime_abi_sha256'] = peer.sha(files['runtime-abi.json'])
            elif change in ('receipt', 'head', 'publication-config'):
                name = {'receipt': 'publication/publication-receipt.json',
                        'head': 'publication/ReLAION/control/head.json',
                        'publication-config': 'publication/config.json'}[change]
                if change == 'receipt':
                    receipt = json.loads(files[name])
                    receipt['arms'].pop()
                    files[name] = json.dumps(receipt).encode()
                else:
                    files[name] = b'{}'
                current['artifacts'][name] = _identity(files[name])
            elif change == 'roster':
                del current['artifacts']['screen/records.jsonl']
            elif change in ('sha', 'bytes'):
                current['artifacts']['screen/summary.json']['sha256' if change == 'sha' else 'bytes'] = 'f'*64 if change == 'sha' else 1
            elif change == 'unknown':
                current['artifacts']['../escape'] = _identity(b'bad')
            try:
                result = collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64)
            except AssertionError:
                assert change is not None
            else:
                assert change is None and result == terminal, 'changed terminal accepted'
            files = original_files
            (out/'aws-closeout.json').write_text(json.dumps(dict(state='terminated', nodes={'0': dict(instance_id='i-owned')})))
        # A failed ABI gate still closes and authenticates its partial evidence.
        files = {'runtime-abi.json': json.dumps(dict(report, qualified=False)).encode()}
        current = dict(terminal, status='failed', phase='binary-qualification', exit_code=1,
            artifacts={n: _identity(b) for n, b in files.items()}, runtime_abi_sha256=peer.sha(files['runtime-abi.json']))
        assert collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64) == current


def self_check():
    """Only synthetic bodies and mocked AWS; no native or cloud execution."""
    import tempfile
    import copy
    import shutil
    import traceback
    from contextlib import ExitStack
    from types import SimpleNamespace
    module = sys.modules[__name__]
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        # A missing authority must fail before any cloud client is constructed.
        with patch.object(shared.boto3, 'Session') as aws:
            try:
                preflight(base)
            except FileNotFoundError:
                pass
            else:
                raise AssertionError('missing authority accepted')
            aws.assert_not_called()
        binary = b'synthetic binary; never executable'
        binary_sha = peer.sha(binary)
        publisher = b'synthetic publisher; never invoked'
        def put(name, body):
            if not isinstance(body, bytes):
                body = json.dumps(body, sort_keys=True, separators=(',', ':')).encode()
            path = base / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            return dict(path=name, bytes=len(body), sha256=peer.sha(body), key='synthetic/' + name)
        for n in range(399):
            put(('Cargo.lock' if n == 0 else 'crates/nested/Cargo.lock' if n == 1
                 else f'crates/test{n}.rs'), f'// synthetic {n}\n'.encode())
        identities = source_hashes(base)
        source_sha = source_identity(identities)
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode='w:gz') as tar:
            for name in identities:
                tar.add(base / name, arcname=name)
        manifest = dict(schema='borsuk-native-semantic-router-source-manifest-v1',
            source_file_count=399, source_identity_sha256=source_sha,
            native_source_commit='0'*40, source_sha256=identities,
            native_source_archive=put('native-source.tar.gz', archive.getvalue()))
        gates = {}
        for name in GATES:
            log = put('receipts/' + name + '.log', b'test result: ok. 1 passed; 0 failed;\n')
            receipt = dict(exit_status=0, source_unchanged=True, command=['cargo', 'test', '--workspace'],
                source_sha256=identities, source_identity_sha256=source_sha, source_file_count=399,
                log_sha256=log['sha256'])
            if name == 'full-workspace-final':
                del receipt['log_sha256']
                receipt['artifacts'] = {'test.log': {k:log[k] for k in ('bytes', 'sha256')}}
            gates[name] = dict(exit_status=0, command=receipt['command'],
                receipt=put('receipts/' + name + '.json', receipt), log=log)
        assurance = dict(full_workspace_test_execution=True, gates=gates,
            source_archive=dict(manifest['native_source_archive'], path='/missing/qualified-source.tar.gz',
                                archived_path=manifest['native_source_archive']['path']),
            binaries={'two_bit_http': dict(bytes=len(binary), sha256=binary_sha)})
        proof = dict(qualified=True, green_status=0, release_status=0,
            binary_sha256=binary_sha, source_file_count=399, source_identity_sha256=source_sha,
            compiled_native_sha256=dict(list(identities.items())[:2]))
        runtime = SimpleNamespace(CODE=('scripts/synthetic-runtime.py',), validate_config=lambda _: None)
        real_run = subprocess.run
        abi_failure = None
        def abi_command(args, **kwargs):
            if args[0] not in ('readelf', 'ldd'):
                return real_run(args, **kwargs)
            assert Path(args[-1]).read_bytes() in (binary, publisher), 'only synthetic ELF inspections'
            assert kwargs['timeout'] == 15 and kwargs['env']['LC_ALL'] == 'C'
            if abi_failure == 'missing-tool': raise FileNotFoundError('synthetic ABI tool')
            if args[0] == 'readelf':
                assert kwargs['check'] is True
                if abi_failure == 'readelf-exit': raise subprocess.CalledProcessError(1, args)
                version = '2.40' if abi_failure == 'required-newer' else '2.38'
                return SimpleNamespace(stdout=('Version needs section .gnu.version_r\nName: GLIBC_'+version+
                    '\nName: GLIBC_2.9\n') if abi_failure != 'missing-needs' else '', stderr='', returncode=0)
            assert args[1] == '-r'
            output = {'notfound': 'libgcc_s.so.1 => not found', 'unresolved': 'unresolved symbol: synthetic',
                      'undefined': 'undefined symbol: synthetic'}.get(abi_failure, 'libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6\n')
            return SimpleNamespace(stdout=output, stderr='version GLIBC_2.38 not found' if abi_failure == 'stderr' else '',
                                   returncode=1 if abi_failure == 'ldd-exit' else 0)
        for name in (*runtime.CODE, *EXTRAS):
            put(name, b'# synthetic controller/runtime closure\n')
        config = dict(schema='borsuk-native-semantic-router-cold-v1', architecture='x86_64',
            region=peer.REGION, bucket=peer.BUCKET, count=64, k=10, ann_queries=512,
            dataset_order=['ReLAION', 'CoHere'], blocks=['control0', 'candidate1', 'candidate2', 'control3'],
            client_cpu_affinity=[4, 5], native_cpu_affinity=[0, 1, 2, 3],
            worker_limit_seconds=3000, machine_limit_seconds=3600,
            instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, subnet_id=SUBNET,
            spot_max_usd_per_hour=.50, compute_cap_usd=.50, ebs_s3_allowance_usd=.15,
            profile_memory_bytes=8*1024**3, native_rlimit_as_bytes=4*1024**3,
            profile_swap_bytes=0, native_memory_bytes=536870912, credential_protocol='instance-imdsv2',
            native_source_file_count=399, native_source_identity_sha256=source_sha,
            native_binary=put('binary', binary), native_assurance=put('assurance.json', assurance),
            native_source_manifest=put('manifest.json', manifest), native_proof=put('proof.json', proof),
            code_sha256={n: peer.sha((base/n).read_bytes()) for n in runtime.CODE},
            controller_code_sha256={n: peer.sha((base/n).read_bytes()) for n in EXTRAS})
        config['binary'] = dict(bytes=len(binary), sha256=binary_sha)
        config['qualification_sha256'] = config['native_proof']['sha256']
        def freeze(value):
            put(str(CONFIG), value)
        freeze(config)
        with patch.object(module, '_worker', return_value=runtime), \
                patch.multiple(module, BINARY_SHA=binary_sha, BINARY_BYTES=len(binary)), ExitStack() as overrides:
            overrides.enter_context(patch.object(subprocess, 'run', side_effect=abi_command))
            overrides.enter_context(patch.object(platform, 'freedesktop_os_release', return_value=RUNTIME_OS))
            overrides.enter_context(patch.object(platform, 'machine', return_value='x86_64'))
            overrides.enter_context(patch.object(os, 'confstr', return_value='glibc 2.39'))
            # Publication authority is mandatory even when cold assurance is green.
            try:
                preflight(base)
            except (KeyError, AssertionError, ValueError, FileNotFoundError):
                pass
            else:
                raise AssertionError('missing publication authority accepted')
            config['items'] = json.loads((Path(__file__).resolve().parents[1]/ROOT/'config-draft.json').read_bytes())['items']
            asset_body = (Path(__file__).resolve().parents[1]/ROOT/'publication-assets.json').read_bytes()
            asset_pointer = put(str(ROOT/'publication-assets.json'), asset_body)
            config['native_publisher'] = put('publisher', publisher)
            (base/'publisher').chmod(0o755)
            publisher_proof = dict(proof, binary_sha256=peer.sha(publisher), full_suite_status=0)
            publisher_proof_pointer = put('publisher-proof.json', publisher_proof)
            pub = config['publication'] = dict(publisher=_identity(publisher),
                assets=dict(key='synthetic/assets.tar.gz', **_identity(b'synthetic assets')),
                asset_manifest={k: asset_pointer[k] for k in ('path', 'bytes', 'sha256')},
                qualification={k: publisher_proof_pointer[k] for k in ('path', 'bytes', 'sha256')})
            declared = dict(schema='borsuk-native-semantic-publication-assets-preparation-v1',
                assets=dict(pub['assets'], path='/tmp/root-owned-assets.tar.gz'), manifest=pub['asset_manifest'])
            declared_pointer = put(str(ASSET_PREPARATION), declared)
            freeze(config)
            overrides.enter_context(patch.object(module, 'ASSET_PREPARATION_SHA', declared_pointer['sha256']))
            qualification = preflight(base)
            assert qualification['required_glibc'] == dict(two_bit_http='2.38', two_bit_plan_demo='2.38')
            with patch.object(platform, 'freedesktop_os_release', side_effect=AssertionError('local OS inspected')), \
                    patch.object(os, 'confstr', side_effect=AssertionError('local libc inspected')):
                assert preflight(base) == qualification
            assert qualification['source_file_count'] == 399 and qualification['native_rebuilt'] is False
            assert qualification['current_full_suite_pass_claim'] is True
            stage = base/'stage'
            (stage/'binaries').mkdir(parents=True)
            (stage/'binaries/two_bit_http').write_bytes(binary)
            (stage/'binaries/two_bit_plan_demo').write_bytes(publisher)
            (stage/'source-qualification.json').write_text(json.dumps(qualification))
            _stage(base, stage)
            assert (stage/'runtime-abi.json').is_file(), 'runtime ABI gate/report missing before publication'
            good_abi = (stage/'runtime-abi.json').read_bytes()
            assert json.loads(good_abi)['qualified'] is True
            for failure in ('libc', 'os', 'architecture', 'missing-binary', 'changed-binary', 'missing-tool',
                            'missing-needs', 'required-newer', 'readelf-exit', 'ldd-exit', 'notfound', 'stderr', 'unresolved', 'undefined'):
                abi_failure = failure
                path = stage/'binaries/two_bit_plan_demo'
                if failure == 'missing-binary': path.unlink()
                elif failure == 'changed-binary': path.write_bytes(publisher+b'changed')
                with patch.object(platform, 'freedesktop_os_release', return_value=dict(RUNTIME_OS, ID='amzn') if failure == 'os' else RUNTIME_OS), \
                        patch.object(platform, 'machine', return_value='aarch64' if failure == 'architecture' else 'x86_64'), \
                        patch.object(os, 'confstr', return_value='glibc 2.34' if failure == 'libc' else 'glibc 2.39'), \
                        patch.object(publication, 'run') as helper, patch.object(publication, 'sdk_client') as sdk:
                    try:
                        _runtime_abi(stage)
                        _publish(base, stage)
                    except (AssertionError, FileNotFoundError, subprocess.CalledProcessError):
                        assert json.loads((stage/'runtime-abi.json').read_bytes())['qualified'] is False
                    else:
                        raise AssertionError('ABI failure reached publication: '+failure)
                    helper.assert_not_called()
                    sdk.assert_not_called()
                path.write_bytes(publisher)
                path.chmod(0o755)
            abi_failure = 'required-newer'
            try:
                preflight(base)
            except AssertionError:
                pass
            else:
                raise AssertionError('local target ABI ceiling ignored')
            abi_failure = None
            _stage(base, stage)
            assert (stage/'runtime-abi.json').read_bytes() == good_abi
            with patch.object(os, 'confstr', return_value='glibc 2.43'):
                assert _runtime_abi(stage)['glibc_version'] == '2.43'
            (stage/'runtime-abi.json').write_bytes(good_abi)
            for failure in ('missing-report', 'incompatible-report'):
                if failure == 'missing-report': (stage/'runtime-abi.json').unlink()
                else: (stage/'runtime-abi.json').write_text(json.dumps(dict(json.loads(good_abi), glibc_version='2.34')))
                with patch.object(publication, 'run') as helper, patch.object(publication, 'sdk_client') as sdk:
                    try:
                        _publish(base, stage)
                    except (AssertionError, FileNotFoundError):
                        pass
                    else:
                        raise AssertionError('publication accepted '+failure)
                    helper.assert_not_called()
                    sdk.assert_not_called()
            (stage/'runtime-abi.json').write_bytes(good_abi)
            assert all(_identity((stage/n).read_bytes()) == ident
                       for n, ident in qualification['authority_artifacts'].items())
            raw = b'synthetic archived log\n'
            pointer = put('archived.log.gz', gzip.compress(raw, mtime=0))
            pointer.update(path='missing.log', archived_path=pointer['path'], archived_sha256=pointer['sha256'], **_identity(raw))
            assert _read(base, pointer) == raw
            try:
                _read(base, dict(pointer, archived_sha256='f'*64))
            except AssertionError:
                pass
            else:
                raise AssertionError('bad gzip transport accepted')
            for key, value in [('architecture', 'aarch64'), ('ann_queries', 256),
                               ('worker_limit_seconds', 3001), ('native_memory_bytes', 1073741824),
                               ('native_source_file_count', 398), ('authority_pending', ['publisher']),
                               ('native_publisher', dict(config['native_publisher'], sha256='f'*64)),
                               ('publication', dict(pub, assets=dict(pub['assets'], key='wrong'))),
                               ('publication', dict(pub, assets=dict(pub['assets'], sha256='f'*64))),
                               ('publication', dict(pub, assets=dict(pub['assets'], bytes=1))),
                               ('qualification_sha256', 'f'*64), ('controller_code_sha256', {})]:
                freeze(dict(config, **{key: value}))
                try:
                    preflight(base)
                except (AssertionError, ValueError):
                    pass
                else:
                    raise AssertionError('changed authority accepted: ' + key)
            freeze(config)
            original_proof = (base/'publisher-proof.json').read_bytes()
            for change in ('pending', 'failed', 'boolean', 'qualified', 'compiled', 'source', 'hash', 'count'):
                bad = copy.deepcopy(publisher_proof)
                if change == 'pending': del bad['full_suite_status']
                elif change == 'failed': bad['full_suite_status'] = 1
                elif change == 'boolean': bad['full_suite_status'] = False
                elif change == 'qualified': bad['qualified'] = False
                elif change == 'compiled': bad['compiled_native_sha256'] = {}
                elif change == 'source': bad['compiled_native_sha256'][next(iter(identities))] = 'f'*64
                elif change == 'hash': bad['binary_sha256'] = 'f'*64
                else: bad['source_file_count'] = 398
                pointer = put('publisher-proof.json', bad)
                freeze(dict(config, publication=dict(pub, qualification={k: pointer[k] for k in ('path', 'bytes', 'sha256')})))
                try:
                    preflight(base)
                except (ValueError, KeyError):
                    pass
                else:
                    raise AssertionError('bad publisher proof accepted: '+change)
            put('publisher-proof.json', original_proof)
            freeze(config)
            for name in ('publisher', str(ROOT/'publication-assets.json'), str(ASSET_PREPARATION)):
                path = base/name
                original = path.read_bytes()
                path.write_bytes(original+b'changed')
                try:
                    preflight(base)
                except (ValueError, AssertionError):
                    pass
                else:
                    raise AssertionError('changed publication body accepted: '+name)
                path.write_bytes(original)
            (base/'publisher').chmod(0o644)
            try:
                preflight(base)
            except ValueError:
                pass
            else:
                raise AssertionError('non-executable publisher accepted')
            (base/'publisher').chmod(0o755)
            bodies = _publication_fixture(config, json.loads(asset_body), asset_body, (base/CONFIG).read_bytes())
            for change in (None, 'helper-fatal', 'missing', 'failed', 'partial', 'arm', 'head', 'body', 'config', 'manifest', 'source'):
                changed = dict(bodies)
                receipt = json.loads(changed['publication-receipt.json'])
                if change == 'failed': receipt['outcome'] = 'failed'
                elif change == 'partial': receipt['arms'].pop()
                elif change == 'arm': receipt['arms'][0]['outcome'] = 'failed'
                elif change == 'head': receipt['arms'][0]['head']['epoch'] = 2
                elif change == 'source': receipt['native_source_identity_sha256'] = 'f'*64
                elif change == 'body': changed['ReLAION/control/native.jsonl'] += b'changed'
                elif change == 'config': changed['config.json'] += b'changed'
                elif change == 'manifest': changed['asset-manifest.json'] += b'changed'
                elif change == 'missing': del changed['CoHere/candidate/head.json']
                changed['publication-receipt.json'] = json.dumps(receipt).encode()
                def helper(config_path, digest, executable, output):
                    assert (config_path, digest, executable) == (base/CONFIG, qualification['config_sha256'], stage/'binaries/two_bit_plan_demo')
                    assert Path.cwd() == base
                    if change == 'helper-fatal': raise RuntimeError('synthetic-secret')
                    output.mkdir()
                    for name, data in changed.items():
                        path = output/name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(data)
                with patch.object(publication, 'run', side_effect=helper), patch.object(publication, 'sdk_client') as sdk:
                    try:
                        _publish(base, stage)
                    except (RuntimeError, AssertionError, FileNotFoundError):
                        assert change is not None
                        if change == 'helper-fatal': assert 'synthetic-secret' not in traceback.format_exc()
                    else:
                        assert change is None, 'bad publication accepted: '+str(change)
                    sdk.assert_not_called()
                shutil.rmtree(stage/'publication', ignore_errors=True)
            original = (base/'assurance.json').read_bytes()
            for changed in (dict(assurance, full_workspace_test_execution=False),
                            dict(assurance, gates=dict(gates, **{'full-workspace-final': dict(gates['full-workspace-final'], exit_status=1)}))):
                broken = dict(config, native_assurance=put('assurance.json', changed))
                freeze(broken)
                try:
                    preflight(base)
                except AssertionError:
                    pass
                else:
                    raise AssertionError('pending/failed full suite accepted')
            put('assurance.json', original)
            freeze(config)
            path = base / next(iter(identities))
            original = path.read_bytes()
            path.write_bytes(original + b'// changed\n')
            try:
                preflight(base)
            except AssertionError:
                pass
            else:
                raise AssertionError('changed source accepted')
            path.write_bytes(original)
            body = user_data('0'*40, '1'*64, 'sources/synthetic', PREFIX+'a0001', qualification)
            assert len(body.encode()) < 16384
            for marker in ('--on-active=3600s', 'MemoryMax=8G', 'MemorySwapMax=0',
                           'RuntimeMaxSec=3030', 'ulimit -v 4194304', 'taskset -c 4-5',
                           'PYTHONPATH="$root/repo"', '-m scripts.run_native_semantic_router_cold',
                           '--kill-after=30 3000'):
                assert marker in body, marker
            assert 'rustup' not in body and 'cargo build' not in body
            assert all(word not in body for word in ('dnf ', 'ensurepip', '-m pip '))
            early = body.split('exec >run.log 2>&1\n', 1)[1]
            assert early.index('apt-get') < early.index('aws s3 cp ')
            assert 'DEBIAN_FRONTEND=noninteractive' in early and early.count('DPkg::Lock::Timeout=120') == 2
            assert 'install awscli python3-boto3 python3.12 time tar gzip util-linux binutils' in early
            assert body.index('--stage ') < body.index('phase=publication\n')
            # Execute generated existence gates, including eight valid empty logs.
            for name in ARTIFACTS:
                path = stage/name
                path.parent.mkdir(parents=True, exist_ok=True)
                if name.startswith('publication/') and name.endswith(('/stdout.log', '/stderr.log')):
                    path.write_bytes(b'')
                elif not path.exists():
                    path.write_bytes(b'synthetic artifact\n')
            gates_script = '\n'.join(line for line in body.splitlines() if line.startswith('test -'))
            subprocess.run(['bash', '-ec', gates_script], env=dict(os.environ, root=str(stage)), check=True)
            missing = stage/'binaries/two_bit_plan_demo'
            missing.unlink()
            assert subprocess.run(['bash', '-ec', gates_script], env=dict(os.environ, root=str(stage))).returncode != 0
            missing.write_bytes(publisher)
            # Run generated sequencing with only the Python/native boundary stubbed.
            commands = base/'stubs'
            commands.mkdir()
            for name, text in [('taskset', '#!/bin/bash\nshift 2\nexec "$@"\n'),
                               ('apt-get', '#!/bin/bash\ntest "$DEBIAN_FRONTEND" = noninteractive || exit 91\nprintf "apt-get:%s\\n" "$*" >> "$EVENTS"\n'),
                               ('aws', '#!/bin/bash\nprintf "aws:%s\\n" "$*" >> "$EVENTS"\n'),
                               ('python3.12', '#!/bin/bash\nprintf "%s\\n" "$*" >> "$EVENTS"\ncase "$*" in *--stage*) exit "$ABI_STATUS";; *--publish*) exit "$PUB_STATUS";; esac\nprintf "synthetic closed summary\\n"\n')]:
                path = commands/name
                path.write_text(text)
                path.chmod(0o755)
            events = base/'bootstrap-events'
            subprocess.run(['bash', '-ec', early[:early.index("printf '%s  source.tar.gz")]], cwd=stage,
                env=dict(os.environ, PATH=str(commands)+os.pathsep+os.environ['PATH'], EVENTS=str(events)), check=True)
            calls = events.read_text().splitlines()
            assert len(calls) == 3 and calls[0].startswith('apt-get:') and calls[0].endswith(' update')
            assert calls[1].startswith('apt-get:') and ' install awscli python3-boto3 python3.12 ' in calls[1]
            assert calls[2].startswith('aws:s3 cp '), 'AWS ran before Ubuntu prerequisites'
            wrapper = 'systemd-run() { while [[ "$1" != bash && "$1" != /usr/bin/time ]]; do shift; done; "$@"; };\n'
            sequence = body[body.index('PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --stage '):body.index('phase=complete\n')]
            for abi_status, status in (('0', '0'), ('23', '0'), ('0', '23')):
                events = base/('events-'+abi_status+'-'+status)
                result = subprocess.run(['bash', '-ec', wrapper+sequence], cwd=stage,
                    env=dict(os.environ, root=str(stage), PATH=str(commands)+os.pathsep+os.environ['PATH'],
                             EVENTS=str(events), ABI_STATUS=abi_status, PUB_STATUS=status), capture_output=True)
                calls = events.read_text().splitlines()
                assert '--stage' in calls[0]
                assert any('--publish' in call for call in calls) == (abi_status == '0')
                success = abi_status == status == '0'
                assert (result.returncode == 0) == success, (abi_status, status, result.returncode, result.stderr,
                                                                 calls, (stage/'profile.log').read_bytes())
                assert any('scripts.run_native_semantic_router_cold ' in call for call in calls) == success
            terminal_script = body.split("python3 - <<'PY' >terminal.json\n")[1].split('\nPY\n')[0]
            terminal = json.loads(subprocess.check_output([sys.executable, '-c', terminal_script],
                cwd=stage, env=dict(os.environ, INSTANCE_ID='i-synthetic', EXIT_CODE='0', PHASE='complete')))
            assert terminal['schema'] == SCHEMA and terminal['binary_sha256'] == binary_sha
            assert terminal['source_identity_sha256'] == source_sha
            assert terminal['qualification_sha256'] == qualification['qualification_sha256']
            assert terminal['required_glibc'] == qualification['required_glibc']
            assert terminal['runtime_abi_sha256'] == peer.sha((stage/'runtime-abi.json').read_bytes())
    lifecycle_self_check()
    collection_self_check()
    # The closed authority has the real source/code/pointer shape, larger than the synthetic fixture.
    repo = Path(__file__).resolve().parents[1]
    real = json.loads(gzip.decompress((repo/ROOT/'a0001/source-qualification.json.gz').read_bytes()))
    real.update(runtime_os=RUNTIME_OS, runtime_glibc=RUNTIME_GLIBC,
                required_glibc=dict(two_bit_http='2.38', two_bit_plan_demo='2.38'),
                config_sha256=peer.sha(b'fresh synthetic config'),
                artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()))
    for name in ('scripts/launch_native_semantic_router_cold_spot.py', 'scripts/launch_native_metadata_ranges_cold_spot.py'):
        real['code_sha256'][name] = peer.sha((repo/name).read_bytes())
    closed = json.loads((repo/ROOT/'a0001/aws-reservation.json').read_bytes())
    real_body = user_data(closed['source_commit'], closed['source_archive_sha256'],
        'research/native-library-check/sources/'+closed['source_archive_sha256']+'.tar.gz', PREFIX+'a0002', real)
    print(f'semantic cold controller self-check PASS; bootstrap_bytes={len(body.encode())}; real_shape_bytes={len(real_body.encode())}; cloud/native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv) == 4 and sys.argv[1] == '--stage':
        _stage(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 4 and sys.argv[1] == '--publish':
        _publish(sys.argv[2], sys.argv[3])
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_semantic_router_cold_spot aNNNN'
        with open('/tmp/borsuk-native-semantic-router-cold-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
