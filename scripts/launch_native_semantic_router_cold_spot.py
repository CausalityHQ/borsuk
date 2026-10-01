"""Launch one frozen FIRST100k semantic/graph cold HTTP Spot experiment.

Authority pointers use {path, bytes, sha256, key}; archived receipt/log pointers
may also use archived_path/archived_sha256 for gzip transport. Root supplies the
config, standalone native_proof, completed native_assurance and exact 399-file
native_source_manifest. This controller never qualifies or rebuilds native code.
publication uses the preparation helper's contract unchanged; native_publisher
is a separate {path,bytes,sha256,key} download pointer. The asset archive is the
immutable declaration in publication-assets-preparation.json, never child input.

--offered aNNNN uses offered-config.json and the unchanged shared Spot lifecycle.
Runtime seam: main(five_args, on_cell_closed=callback); callback(summary, paths)
receives records/summary Paths after fsync and all owned cleanup. Cell markers
bind records={bytes,sha256}, config/binary/qualification SHA and exact
rate_index/dataset/arm, with closed/process_cleanup/identity/bounded_memory gates.
Execution success and scientific offered_gate_passed remain separate outcomes.
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
import stat
import subprocess
import sys
import tarfile
from contextlib import contextmanager
from functools import partial
from shlex import quote
from types import SimpleNamespace
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
AWSCLI_VERSION = '2.36.11'
AWSCLI_URL = 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip'
AWSCLI_SHA256 = '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6'
AWSCLI_BYTES = 73022935
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
OFFERED_CONFIG = ROOT / 'offered-config.json'
OFFERED_SCHEMA = 'borsuk-native-semantic-router-cold-offered-spot-v1'
OFFERED_RUNTIME_SCHEMA = 'borsuk-native-semantic-router-cold-offered-v1'
OFFERED_PREFIX = 'research/semantic-router/20261001/offered-'
CELL_STEMS = tuple(f'rate{i}-{dataset.lower()}-{arm}' for i in range(6)
                   for dataset in ('ReLAION', 'CoHere') for arm in ('control', 'candidate'))
OFFERED_INPUTS = tuple(f'screen/inputs/{dataset}/{name}' for dataset in ('ReLAION', 'CoHere')
                       for name in ('requests', 'truth', 'control/reference-k10', 'candidate/reference-k10'))
OFFERED_ARTIFACTS = (*(n for n in ARTIFACTS if n != 'screen/records.jsonl'),
                    *(f'screen/{stem}-{suffix}' for stem in CELL_STEMS
                      for suffix in ('records.jsonl', 'summary.json')), *OFFERED_INPUTS)
TERMINAL_IDENTITIES = ('config_sha256', 'qualification_sha256', 'binary_sha256', 'binary_bytes',
    'native_source_commit', 'source_identity_sha256', 'source_file_count', 'artifact_roster_sha256',
    'native_source_archive_sha256', 'native_source_manifest_sha256', 'native_assurance_sha256',
    'publisher_sha256', 'publisher_bytes', 'publisher_qualification_sha256', 'asset_manifest_sha256',
    'publication_assets', 'asset_preparation_sha256', 'runtime_os', 'runtime_glibc', 'required_glibc',
    'awscli_version', 'awscli_sha256')


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


def _qualify(base, binary_override=None, publisher_override=None, *, offered=False):
    base = Path(base).resolve()
    config_path = OFFERED_CONFIG if offered else CONFIG
    artifacts = OFFERED_ARTIFACTS if offered else ARTIFACTS
    body = (base / config_path).read_bytes()
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
    if offered:
        del expected['blocks']
        expected.update(schema=OFFERED_RUNTIME_SCHEMA, ann_queries=1536,
            offered_qps=[.25, .5, 1, 2, 4, 8], workers=6, base_port=18080,
            max_dispatch_lateness_ns=125000000, cleanup_reserve_seconds=90)
        assert 'blocks' not in config, 'serial blocks in offered protocol'
        assert all(set(item['arms'][arm]['inputs']) == {'reference-k10'}
                   for item in config['items'] for arm in ('control', 'candidate')), 'eight consumed offered inputs'
    assert all(config[key] == value for key, value in expected.items()), 'fixed protocol/infrastructure'
    worker = _worker()
    with _cwd(base):
        worker.validate_config(config)
    assert set(config['code_sha256']) == set(worker.OFFERED_CODE if offered else worker.CODE)
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
    qualification = dict(config_path=str(config_path), config_sha256=peer.sha(body),
        campaign_schema=OFFERED_SCHEMA if offered else SCHEMA,
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
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256,
        native_binary=binary_pointer, authority_artifacts={n: _identity(b) for n, b in files.items()},
        artifact_roster_sha256=peer.sha(json.dumps(artifacts, separators=(',', ':')).encode()))
    return qualification, files


def preflight(base=Path('.'), offered=False):
    return _qualify(base, offered=offered)[0]


def _offered(qualification):
    offered = qualification['campaign_schema'] == OFFERED_SCHEMA
    assert qualification['campaign_schema'] in (SCHEMA, OFFERED_SCHEMA), 'campaign schema'
    assert qualification['config_path'] == str(OFFERED_CONFIG if offered else CONFIG), 'qualified config path'
    return offered


def _stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    expected = json.loads((out/'source-qualification.json').read_bytes())
    (out/'binaries/two_bit_plan_demo').chmod(0o755)
    actual, files = _qualify(repo, out/'binaries/two_bit_http', out/'binaries/two_bit_plan_demo',
                             offered=_offered(expected))
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
    _offered(qualification)
    config_path = repo/qualification['config_path']
    body = config_path.read_bytes()
    assert peer.sha(body) == qualification['config_sha256']
    config = json.loads(body)
    with _cwd(repo):
        try:
            publication.run(config_path, qualification['config_sha256'], out/'binaries/two_bit_plan_demo',
                            out/'publication')
        except Exception as error:
            # Match the helper CLI: SDK exception text can contain credentials.
            raise RuntimeError('publication failed: ' + type(error).__name__) from None
    bodies = {n: (out/'publication'/n).read_bytes() for n in PUBLICATION_FILES}
    return _published(config, qualification['config_sha256'], bodies)


def _cell_bodies(summary, paths, config):
    """The runtime calls this seam only after full cleanup and immutable closure."""
    assert set(paths) == {'records', 'summary'}, 'closed cell paths'
    records, marker = (Path(paths[n]) for n in ('records', 'summary'))
    stem = records.name.removesuffix('-records.jsonl')
    assert stem in CELL_STEMS and records.name == stem+'-records.jsonl', 'closed cell roster'
    assert marker == records.with_name(stem+'-summary.json'), 'closed cell marker path'
    record_body, summary_body = records.read_bytes(), marker.read_bytes()
    assert json.loads(summary_body) == summary, 'closed summary body'
    assert (summary['closed'] is summary['process_cleanup_complete'] is
            summary['identity_gate_passed'] is summary['bounded_memory_gate_passed'] is True), 'cell not closed'
    for key, expected in (('config_sha256', config['config_sha256']),
                          ('binary_sha256', config['binary']['sha256']),
                          ('qualification_sha256', config['qualification_sha256'])):
        assert summary[key] == expected, 'closed cell '+key
    assert summary['records'] == _identity(record_body), 'closed cell record identity'
    rows = [json.loads(line) for line in record_body.splitlines()]
    rate, dataset, arm = stem.split('-')
    identity = dict(rate_index=int(rate[4:]), dataset={'relaion': 'ReLAION', 'cohere': 'CoHere'}[dataset], arm=arm)
    assert type(summary['rate_index']) is int and all(summary[k] == v for k, v in identity.items()), 'closed cell identity'
    assert (len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
            and all(type(r['query_ordinal']) is type(r['rate_index']) is int and
                    all(r[k] == v for k, v in identity.items()) for r in rows)), 'closed offer ledger'
    return stem, record_body, summary_body


def _run_offered(argv, prefix):
    assert re.fullmatch(re.escape(OFFERED_PREFIX)+r'a[0-9]{4}', prefix), 'checkpoint prefix'
    assert len(argv) == 5
    body = Path(argv[0]).read_bytes()
    assert peer.sha(body) == argv[1], 'checkpoint config identity'
    config = json.loads(body)
    assert config['schema'] == OFFERED_RUNTIME_SCHEMA and not config.get('authority_pending')
    config = dict(config, config_sha256=argv[1])
    output = Path(argv[4]).resolve()
    # Existing one-attempt IMDS/S3 client; no SDK exception text reaches logs.
    try:
        client = publication.sdk_client(config['region'])
    except Exception as error:
        raise RuntimeError('closed cell client failed: '+type(error).__name__) from None
    uploaded = set()
    def closed(summary, paths):
        assert all(Path(path).resolve().parent == output for path in paths.values()), 'checkpoint output path'
        stem, records, marker = _cell_bodies(summary, paths, config)
        assert stem not in uploaded, 'duplicate closed cell'
        try:
            client.put_object(Bucket=config['bucket'], Key=prefix+'/cells/'+stem+'-records.jsonl',
                              Body=records, IfNoneMatch='*')
            client.put_object(Bucket=config['bucket'], Key=prefix+'/cells/'+stem+'-summary.json',
                              Body=marker, IfNoneMatch='*')
        except Exception as error:
            raise RuntimeError('closed cell upload failed: '+type(error).__name__) from None
        uploaded.add(stem)
    return _worker().main(argv, on_cell_closed=closed)


def _profile_resources(report):
    """Actual shared cgroup evidence; six admissions are not an aggregate peak."""
    try:
        events = dict(line.split() for line in report['memory.events'].splitlines())
        return (int(report['memory.max']) == 8*1024**3
            and 0 < int(report['memory.peak']) <= 8*1024**3
            and int(report['memory.swap.max']) == int(report['memory.swap.peak']) == 0
            and all(int(events[name]) == 0 for name in ('oom', 'oom_kill', 'oom_group_kill'))
            and report['rlimit_as_bytes'] == [4*1024**3, 4*1024**3]
            and report['cpu_affinity'] == [0, 1, 2, 3])
    except (KeyError, TypeError, ValueError):
        return False


def _check_offered_closure(out):
    out = Path(out)
    body = (out/'screen/config.json').read_bytes()
    config = json.loads(body)
    assert config['schema'] == OFFERED_RUNTIME_SCHEMA
    config = dict(config, config_sha256=peer.sha(body))
    summary = json.loads((out/'screen/summary.json').read_bytes())
    assert summary['closed'] is summary['process_cleanup_complete'] is summary['identity_gate_passed'] is True
    for stem in CELL_STEMS:
        paths = {n: out/'screen'/(stem+'-'+suffix) for n, suffix in
                 (('records', 'records.jsonl'), ('summary', 'summary.json'))}
        _cell_bodies(json.loads(paths['summary'].read_bytes()), paths, config)
    return summary, _profile_resources(json.loads((out/'profile-cgroup.json').read_bytes()))


def _closed_artifacts(out):
    """Retain deterministic owned cell files for forensics, without checkpoint authority."""
    out = Path(out).resolve()
    cells = set()
    for name in OFFERED_ARTIFACTS:
        if not name.startswith('screen/rate'): continue
        path = out/name
        try:
            info = path.lstat()
            if stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and path.resolve().parent == out/'screen':
                cells.add(name)
        except OSError:
            continue
    return tuple(n for n in OFFERED_ARTIFACTS if not n.startswith('screen/rate') or n in cells)


def _collect_offered(out, files, qualification, terminal):
    import tempfile
    from scripts import check_native_semantic_router_stats as stats
    # Replay only the authenticated bodies, including all eight consumed inputs.
    with tempfile.TemporaryDirectory() as tmp:
        replay = Path(tmp)
        for name, body in files.items():
            path = replay/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        summary, resources_passed = _check_offered_closure(replay)
        verified = stats.check_saved(replay/'screen', qualification['config_sha256'], replay/'binaries/two_bit_http')
        assert verified['offered_gate_passed'] == summary['offered_gate_passed'], 'saved offered decision'
    passed = summary['offered_gate_passed'] is True and resources_passed
    outcome = 'PASS' if passed else 'FAIL'
    assert terminal['scientific_qualification'] == outcome, 'terminal scientific qualification'
    assert not passed or terminal['exit_code'] == 0, 'qualified execution failed'
    receipt = dict(schema='borsuk-native-semantic-router-cold-offered-collection-v1',
        config_sha256=qualification['config_sha256'], authenticated_artifacts=len(files),
        process_cleanup_complete=True, resource_gate_passed=resources_passed,
        scientific_qualification=outcome, original_exit_code=terminal['original_exit_code'],
        saved_verification=verified)
    (Path(out)/'offered-verification.json').write_text(json.dumps(receipt, indent=2, allow_nan=False)+'\n')


def user_data(commit, archive_sha, archive_key, prefix, qualification, offered=False):
    assert len(commit) == 40 and len(archive_sha) == 64
    assert _offered(qualification) == offered, 'bootstrap campaign mode'
    artifacts = OFFERED_ARTIFACTS if offered else ARTIFACTS
    schema = OFFERED_SCHEMA if offered else SCHEMA
    config_path = qualification['config_path']
    if offered:
        assert re.fullmatch(re.escape(OFFERED_PREFIX)+r'a[0-9]{4}', prefix), 'offered prefix'
    assert (qualification['awscli_version'], qualification['awscli_sha256']) == (AWSCLI_VERSION, AWSCLI_SHA256)
    with patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=schema, ARTIFACTS=artifacts):
        body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace('v174-relaid-bind-compile', 'native-semantic-router-cold')
    # Share the exact roster across existence gates, uploads and terminal hashing.
    body = body.replace('phase=bootstrap\n', 'phase=bootstrap\nexport ARTIFACT_NAMES='+
                        quote('run-closed.log' if offered else ' '.join(artifacts))+'\n', 1)
    terminal = body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0]
    terminal = terminal.replace(f'for name in {artifacts!r}:', "for name in os.environ['ARTIFACT_NAMES'].split():")
    terminal = terminal.replace("'exit_code':code,", "'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),")
    finish = f'''finish() {{
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  set +e
  cd "$root"
  {{ printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\\n'; }} >/dev/ttyS0 2>/dev/null || true
  cp run.log run-closed.log || code=96
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
  instance_id=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://{peer.BUCKET}/{prefix}/artifacts/$name" --only-show-errors || code=96
      fi
    done
  else code=96; fi
  write_terminal() {{
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" ORIGINAL_EXIT_CODE="$original_code" PHASE="$phase" python3 - <<'PY' >terminal.json
{terminal}
PY
  }}
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  {{ if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\\n'; fi; }} >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://{peer.BUCKET}/{prefix}/terminal.json" --only-show-errors || {{ code=96; write_terminal || terminal_ready=0; }}
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}}
'''
    start, end = body.index('finish() {\n'), body.index('trap finish EXIT\n')
    if offered:
        finish = finish.replace('  aws_ready=0\n', '''  if [ -d "$root/repo/scripts" ]; then
    export PYTHONPATH="$root/repo"
    ARTIFACT_NAMES=$(python3.12 -m scripts.launch_native_semantic_router_cold_spot --closed-artifacts "$root") || ARTIFACT_NAMES="run-closed.log profile.log profile-resources.txt profile-cgroup.json"
    export ARTIFACT_NAMES
  else ARTIFACT_NAMES="run-closed.log"; export ARTIFACT_NAMES; fi
  aws_ready=0
''')
    body = body[:start] + finish + body[end:]
    bootstrap = 'exec >run.log 2>&1\naws s3 cp '
    assert body.count(bootstrap) == 1, 'bootstrap early AWS hook changed'
    body = body.replace(bootstrap, f'''exec >run.log 2>&1
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1
phase=apt-update
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 time tar gzip util-linux binutils
phase=awscli-download
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip {quote(AWSCLI_URL)}
printf '%s  awscliv2.zip\\n' '{AWSCLI_SHA256}' | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = {AWSCLI_BYTES}
phase=awscli-install
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
cli_version=$(aws --version)
[[ "$cli_version" == aws-cli/{AWSCLI_VERSION}\\ * ]]
phase=source-download
aws s3 cp ''')
    # The bootstrap creates the trap and authenticates the campaign archive.
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':')).encode()
    import base64
    encoded = base64.b64encode(gzip.compress(proof, mtime=0)).decode()
    binary_key = quote('s3://' + peer.BUCKET + '/' + qualification['native_binary']['key'])
    publisher_key = quote('s3://' + peer.BUCKET + '/' + qualification['native_publisher']['key'])
    profile_command = ('scripts.launch_native_semantic_router_cold_spot --run-offered' if offered
                       else 'scripts.run_native_semantic_router_cold')
    checkpoint = ' ' + quote(prefix) if offered else ''
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
{'set +e' if offered else ''}
systemd-run --unit=native-semantic-router-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m {profile_command} {config_path} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"{checkpoint}; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    if offered:
        command = command.replace('phase=binary-qualification\n', '''export ARTIFACT_NAMES=$(PYTHONPATH="$root/repo" python3.12 -c "from scripts.launch_native_semantic_router_cold_spot import OFFERED_ARTIFACTS; print(' '.join(OFFERED_ARTIFACTS))")
phase=binary-qualification
''', 1)
        command += '''profile_code=$?
set -e
test "$profile_code" -le 1
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_semantic_router_cold_spot --check-offered-closure "$root"
'''
    command += '''for name in $ARTIFACT_NAMES; do
  case "$name" in
    publication/*/stdout.log|publication/*/stderr.log) test -f "$root/$name";;
    run-closed.log) test -s "$root/run.log";;
    *) test -s "$root/$name";;
  esac
done
'''
    body = body[:start] + command + body[end:]
    if offered:
        body = body.replace('phase=complete\n', 'phase=complete\nexit "$profile_code"\n', 1)
    # Terminal identities are emitted by the bootstrap alongside its byte roster.
    marker = "'source_archive_sha256':'" + archive_sha + "',"
    terminal_fields = {key: qualification[key] for key in TERMINAL_IDENTITIES}
    assert body.count(marker) == 1, 'bootstrap terminal identity hook changed'
    body = body.replace(marker, marker + repr(terminal_fields)[1:-1] +
                        ", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),")
    if offered:
        # Scientific failure retains the runtime exit status after a complete closeout.
        marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
        scientific = "json.loads(Path('screen/summary.json').read_text())['offered_gate_passed'] is True and __import__('scripts.launch_native_semantic_router_cold_spot',fromlist=['_profile_resources'])._profile_resources(json.loads(Path('profile-cgroup.json').read_text()))"
        body = body.replace(marker, marker + "'scientific_qualification':('PASS' if "+scientific+" else 'FAIL') if os.environ['PHASE']=='complete' else None,")
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
    offered = qualification.get('campaign_schema') == OFFERED_SCHEMA
    schema = OFFERED_SCHEMA if offered else SCHEMA
    artifacts = OFFERED_ARTIFACTS if offered else ARTIFACTS
    if offered:
        _offered(qualification)
        assert qualification['artifact_roster_sha256'] == peer.sha(json.dumps(artifacts, separators=(',', ':')).encode())
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == schema and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == reservation['source_commit'] == commit
    assert terminal['source_archive_sha256'] == reservation['source_archive_sha256'] == digest
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == qualification[key], key
    assert set(terminal['artifacts']) <= set(artifacts)
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
    complete = terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    if offered and terminal['phase'] == 'complete':
        assert terminal['exit_code'] == terminal['original_exit_code'] in (0, 1), 'offered original status'
        assert terminal['status'] == ('complete' if terminal['exit_code'] == 0 else 'failed'), 'offered terminal status'
        complete = True
    if complete:
        assert set(files) == set(artifacts), 'complete artifact roster'
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
        if offered:
            _collect_offered(out, files, qualification, terminal)
    return terminal


def main(attempt, offered=False):
    campaign = sys.modules[__name__]
    if offered:
        # Shared lifecycle owns every ACK, fsync, terminate, wait and collection.
        campaign = SimpleNamespace(ROOT=ROOT, NAME='offered', SCHEMA=OFFERED_SCHEMA,
            PREFIX=OFFERED_PREFIX, TOKEN_PREFIX='semantic-router-offered-', TAG='borsuk-semantic-router-offered',
            ARTIFACTS=OFFERED_ARTIFACTS, WALL=WALL, INSTANCE_TYPE=INSTANCE_TYPE, IMAGE_ID=IMAGE_ID,
            ROOT_DEVICE_NAME=ROOT_DEVICE_NAME, SUBNET=SUBNET, SPOT_MAX_USD_PER_HOUR=SPOT_MAX_USD_PER_HOUR,
            COMPUTE_CAP=COMPUTE_CAP, preflight=partial(preflight, offered=True),
            user_data=partial(user_data, offered=True), poll=poll, collect=collect)
    return shared.main(attempt, campaign=campaign)


def lifecycle_self_check(offered=False):
    """Exercise this campaign through the actual shared lifecycle, including wait."""
    from datetime import datetime, timezone
    import tempfile
    from unittest.mock import Mock
    from botocore.exceptions import ReadTimeoutError, EndpointConnectionError
    module = sys.modules[__name__]
    artifacts = OFFERED_ARTIFACTS if offered else ARTIFACTS
    token_prefix = 'semantic-router-offered-' if offered else TOKEN_PREFIX
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
                return dict(status='complete', phase='complete', exit_code=0, artifacts={n: {} for n in artifacts})
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
                    main(attempt, offered=offered)
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
            assert args['ClientToken'].startswith(token_prefix) and len(args['ClientToken']) <= 64
            tokens.append(args['ClientToken'])
            out = Path(tmp)/'offered'/attempt if offered else Path(tmp)/attempt
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


def _offered_fixture(config):
    config = dict(config, schema=OFFERED_RUNTIME_SCHEMA, ann_queries=1536,
                  offered_qps=[.25, .5, 1, 2, 4, 8], workers=6, base_port=18080,
                  max_dispatch_lateness_ns=125000000, cleanup_reserve_seconds=90)
    config.pop('blocks', None)
    return config


def _resource_fixture():
    return {'memory.max': str(8*1024**3), 'memory.peak': '1234', 'memory.swap.max': '0',
            'memory.swap.peak': '0', 'memory.events': 'oom 0\noom_kill 0\noom_group_kill 0\n',
            'rlimit_as_bytes': [4*1024**3, 4*1024**3], 'cpu_affinity': [0, 1, 2, 3]}


def _cell_fixture(out, stem, config):
    out.mkdir(parents=True, exist_ok=True)
    paths = {n: out/(stem+'-'+suffix) for n, suffix in
             (('records', 'records.jsonl'), ('summary', 'summary.json'))}
    rate, dataset, arm = stem.split('-')
    identity = dict(rate_index=int(rate[4:]), dataset={'relaion': 'ReLAION', 'cohere': 'CoHere'}[dataset], arm=arm)
    paths['records'].write_text(''.join(json.dumps(dict(identity, query_ordinal=q))+'\n' for q in range(64)))
    summary = dict(identity, closed=True, process_cleanup_complete=True, identity_gate_passed=True,
        bounded_memory_gate_passed=True, records=_identity(paths['records'].read_bytes()),
        config_sha256=config['config_sha256'], binary_sha256=config['binary']['sha256'],
        qualification_sha256=config['qualification_sha256'])
    paths['summary'].write_text(json.dumps(summary)+'\n')
    return paths


def _offered_self_check(base, config, stage):
    """Controller-only fixtures; the independent runtime API is mocked until integration."""
    from unittest.mock import Mock
    from scripts import check_native_semantic_router_stats as stats
    module = sys.modules[__name__]
    offered = _offered_fixture(config)
    config_path = base/OFFERED_CONFIG
    config_path.write_text(json.dumps(offered))
    qualified = preflight(base, offered=True)
    assert qualified['config_path'] == str(OFFERED_CONFIG) and qualified['campaign_schema'] == OFFERED_SCHEMA
    assert qualified['source_file_count'] == 399 and qualified['native_rebuilt'] is False
    assert len(OFFERED_ARTIFACTS) == len(set(OFFERED_ARTIFACTS)) == 108
    assert 'screen/records.jsonl' not in OFFERED_ARTIFACTS and len(OFFERED_INPUTS) == 8
    def rejected(call):
        try: call()
        except (AssertionError, ValueError, KeyError, FileNotFoundError): pass
        else: raise AssertionError('changed offered authority accepted')
    for key, value in (('authority_pending', True), ('schema', config['schema']), ('ann_queries', 512),
            ('offered_qps', [1, 2]), ('workers', 7), ('base_port', 8080), ('count', 63), ('k', 100),
            ('max_dispatch_lateness_ns', 125000001), ('cleanup_reserve_seconds', 89),
            ('blocks', ['control0']), ('worker_limit_seconds', 3001), ('profile_swap_bytes', 1),
            ('controller_code_sha256', {}), ('code_sha256', {}), ('native_source_file_count', 398)):
        config_path.write_text(json.dumps(dict(offered, **{key: value})))
        rejected(lambda: preflight(base, offered=True))
    config_path.write_text(json.dumps(offered))
    for name in (next(iter(config['code_sha256'])), next(iter(config['controller_code_sha256'])),
                 next(iter(source_hashes(base))), str(OFFERED_CONFIG)):
        path = base/name
        saved = path.read_bytes()
        path.write_bytes(saved+b'changed')
        rejected(lambda: preflight(base, offered=True))
        path.write_bytes(saved)
    (stage/'source-qualification.json').write_text(json.dumps(qualified))
    _stage(base, stage)
    bad = dict(qualified, artifact_roster_sha256='f'*64)
    (stage/'source-qualification.json').write_text(json.dumps(bad))
    rejected(lambda: _stage(base, stage))
    (stage/'source-qualification.json').write_text(json.dumps(qualified))
    bodies = _publication_fixture(offered, json.loads((base/ROOT/'publication-assets.json').read_bytes()),
                                  (base/ROOT/'publication-assets.json').read_bytes(), config_path.read_bytes())
    def publish(path, digest, binary, out):
        assert path == config_path and digest == qualified['config_sha256']
        out.mkdir(exist_ok=True)
        for name, body in bodies.items():
            target = out/name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
    with patch.object(publication, 'run', side_effect=publish):
        _publish(base, stage)
    body = user_data('0'*40, '1'*64, 'sources/synthetic', OFFERED_PREFIX+'a0001', qualified, offered=True)
    assert len(body.encode()) < 16384 and '--run-offered' in body and str(OFFERED_CONFIG) in body
    assert '--closed-artifacts' in body and 'exit "$profile_code"' in body and 'original_exit_code' in body
    rejected(lambda: user_data('0'*40, '1'*64, 'sources/synthetic', OFFERED_PREFIX+'a0001', qualified))
    checked_config = dict(offered, config_sha256=qualified['config_sha256'])
    output = base/'checkpoint'
    argv = [str(config_path), qualified['config_sha256'], str(stage/'binaries/two_bit_http'),
            str(stage/'boundary-check.json'), str(output)]
    client = Mock()
    with patch.object(publication, 'sdk_client', side_effect=RuntimeError('synthetic-sensitive-sdk-text')), \
            patch.object(module, '_worker') as worker:
        try: _run_offered(argv, OFFERED_PREFIX+'a0001')
        except RuntimeError as error: assert 'synthetic-sensitive-sdk-text' not in str(error)
        else: raise AssertionError('SDK client failure swallowed')
        worker.assert_not_called()
    for failure in ('success', 'partial', 'identity', 'resource', 'upload', 'interrupt'):
        client.reset_mock(side_effect=True)
        if failure == 'upload': client.put_object.side_effect = RuntimeError('synthetic-sensitive-sdk-text')
        def runtime(args, *, on_cell_closed):
            assert args == argv
            paths = _cell_fixture(output, CELL_STEMS[0], checked_config)
            marker = json.loads(paths['summary'].read_bytes())
            if failure == 'partial': marker['closed'] = False
            elif failure == 'identity': marker['identity_gate_passed'] = False
            elif failure == 'resource': marker['bounded_memory_gate_passed'] = False
            paths['summary'].write_text(json.dumps(marker))
            on_cell_closed(marker, paths)
            if failure == 'interrupt':
                partial = _cell_fixture(output, CELL_STEMS[1], checked_config)
                summary = json.loads(partial['summary'].read_bytes())
                summary['closed'] = False
                partial['summary'].write_text(json.dumps(summary))
                (output/'config.json').write_bytes(config_path.read_bytes())
                raise KeyboardInterrupt()
            return 0
        with patch.object(module, '_worker', return_value=SimpleNamespace(main=runtime)), \
                patch.object(publication, 'sdk_client', return_value=client), patch.object(stats, 'check_saved') as reducer:
            try: result = _run_offered(argv, OFFERED_PREFIX+'a0001')
            except (AssertionError, RuntimeError, KeyboardInterrupt) as error:
                assert failure != 'success'
                assert 'synthetic-sensitive-sdk-text' not in str(error)
            else: assert failure == 'success' and result == 0
            reducer.assert_not_called()
        assert client.put_object.call_count == (0 if failure in ('partial', 'identity', 'resource') else 1 if failure == 'upload' else 2)
        if failure in ('success', 'interrupt'):
            assert [c.kwargs['Key'] for c in client.put_object.call_args_list] == [
                OFFERED_PREFIX+'a0001/cells/'+CELL_STEMS[0]+'-'+s for s in ('records.jsonl', 'summary.json')]
            assert all(c.kwargs['IfNoneMatch'] == '*' for c in client.put_object.call_args_list)
        if failure == 'interrupt':
            closed = _closed_artifacts(output.parent)  # The actual roster is rooted at screen/.
            assert not any('rate0-' in n for n in closed)
            (stage/'screen').mkdir(exist_ok=True)
            (stage/'screen/config.json').write_bytes(config_path.read_bytes())
            for stem in CELL_STEMS[:2]:
                for suffix in ('records.jsonl', 'summary.json'):
                    name = stem+'-'+suffix
                    (stage/'screen'/name).write_bytes((output/name).read_bytes())
            closed = _closed_artifacts(stage)
            assert all('screen/'+CELL_STEMS[0]+'-'+s in closed for s in ('records.jsonl', 'summary.json'))
            assert all('screen/'+CELL_STEMS[1]+'-'+s in closed for s in ('records.jsonl', 'summary.json'))
    assert _profile_resources(_resource_fixture())
    for key, value in (('memory.max', 'max'), ('memory.peak', str(8*1024**3+1)), ('memory.swap.max', '1'),
            ('memory.swap.peak', '1'), ('memory.events', 'oom 1\noom_kill 0\noom_group_kill 0'),
            ('memory.events', 'oom 0\noom_kill 1\noom_group_kill 0'),
            ('memory.events', 'oom 0\noom_kill 0\noom_group_kill 1')):
        assert not _profile_resources(dict(_resource_fixture(), **{key: value}))
    assert not _profile_resources({})
    # Execute the generated terminal Python, preserving execution versus scientific outcome.
    for name in OFFERED_ARTIFACTS:
        path = stage/name
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists(): path.write_bytes(b'synthetic artifact\n')
    terminal_script = body.split("python3 - <<'PY' >terminal.json\n")[1].split('\nPY\n')[0]
    for attained, code, resources in ((False, 0, True), (True, 0, True), (False, 1, True), (True, 1, False)):
        (stage/'screen/summary.json').write_text(json.dumps({'offered_gate_passed': attained}))
        report = _resource_fixture()
        if not resources: report['memory.events'] = 'oom 1\noom_kill 0\noom_group_kill 0'
        (stage/'profile-cgroup.json').write_text(json.dumps(report))
        terminal = json.loads(subprocess.check_output([sys.executable, '-c', terminal_script], cwd=stage,
            env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]),
                ARTIFACT_NAMES=' '.join(OFFERED_ARTIFACTS), INSTANCE_ID='i-synthetic', EXIT_CODE=str(code),
                ORIGINAL_EXIT_CODE=str(code), PHASE='complete')))
        assert terminal['schema'] == OFFERED_SCHEMA and terminal['original_exit_code'] == code
        assert terminal['scientific_qualification'] == ('PASS' if attained and resources else 'FAIL')
        assert terminal['status'] == ('complete' if code == 0 else 'failed')
        assert set(terminal['artifacts']) == set(OFFERED_ARTIFACTS)
    print(f'offered controller fixture PASS; artifacts=108; bootstrap_bytes={len(body.encode())}; runtime/cloud/native MOCKED')


def collection_self_check(offered=False):
    import tempfile
    from unittest.mock import Mock
    artifacts = OFFERED_ARTIFACTS if offered else ARTIFACTS
    schema = OFFERED_SCHEMA if offered else SCHEMA
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        files = {n: b'synthetic artifact\n' for n in artifacts}
        binary = files['binaries/two_bit_http']
        config = json.loads((Path(__file__).resolve().parents[1]/ROOT/'config-draft.json').read_bytes())
        manifest_body = (Path(__file__).resolve().parents[1]/ROOT/'publication-assets.json').read_bytes()
        manifest = json.loads(manifest_body)
        config['controller_code_sha256'] = {n: 'a'*64 for n in EXTRAS}
        if offered:
            config = _offered_fixture(config)
        config['publication']['publisher'] = _identity(files['binaries/two_bit_plan_demo'])
        config['publication']['qualification'] = dict(path='proof', **_identity(files['publisher-proof.json']))
        publication_bodies = _publication_fixture(config, manifest, manifest_body)
        files.update({'publication/'+n: b for n, b in publication_bodies.items()})
        config_body = files['screen/config.json'] = publication_bodies['config.json']
        files['asset-manifest.json'] = manifest_body
        proof = files['boundary-check.json']
        files['screen/qualification.json'] = proof
        qualification = dict(config_sha256=peer.sha(config_body), qualification_sha256=peer.sha(proof),
            config_path=str(OFFERED_CONFIG if offered else CONFIG), campaign_schema=schema,
            binary_sha256=peer.sha(binary), binary_bytes=len(binary), native_source_commit='2'*40,
            source_identity_sha256=config['native_source_identity_sha256'], source_file_count=399,
            artifact_roster_sha256=peer.sha(json.dumps(artifacts, separators=(',', ':')).encode()),
            native_source_archive_sha256='5'*64, native_source_manifest_sha256='6'*64, native_assurance_sha256='7'*64,
            publisher_sha256=peer.sha(files['binaries/two_bit_plan_demo']),
            publisher_bytes=len(files['binaries/two_bit_plan_demo']),
            publisher_qualification_sha256=peer.sha(files['publisher-proof.json']),
            asset_manifest_sha256=peer.sha(manifest_body), publication_assets=config['publication']['assets'],
            asset_preparation_sha256=ASSET_PREPARATION_SHA,
            runtime_os=RUNTIME_OS, runtime_glibc=RUNTIME_GLIBC,
            awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256,
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
        if offered:
            files['screen/summary.json'] = json.dumps(dict(closed=True, process_cleanup_complete=True,
                identity_gate_passed=True, offered_gate_passed=False)).encode()
            files['profile-cgroup.json'] = json.dumps(_resource_fixture()).encode()
            for stem in CELL_STEMS:
                paths = _cell_fixture(out/'fixture', stem, dict(config, config_sha256=peer.sha(config_body)))
                files.update({'screen/'+path.name: path.read_bytes() for path in paths.values()})
        terminal = dict(schema=schema, instance_id='i-owned', source_commit='0'*40,
            source_archive_sha256='1'*64, status='complete', phase='complete', exit_code=0,
            original_exit_code=0, scientific_qualification='FAIL' if offered else None,
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
                       'awscli-version', 'awscli-hash',
                       'publisher', 'publication-roster', 'receipt', 'head', 'publication-config', 'closeout',
                       'abi-missing', 'abi-sha', 'abi-os', 'abi-libc', 'abi-binary', 'abi-required', 'abi-unresolved',
                       *(['cell-partial', 'cell-config', 'cell-record', 'cell-identity', 'cell-memory', 'inputs',
                          'cell-ledger', 'scientific', 'original-exit', 'saved-reducer'] if offered else [])):
            current = json.loads(json.dumps(terminal))
            original_files = dict(files)
            if change in ('source', 'config', 'proof', 'binary', 'publisher'):
                key = {'source': 'source_identity_sha256', 'config': 'config_sha256',
                       'proof': 'qualification_sha256', 'binary': 'binary_sha256', 'publisher': 'publisher_sha256'}[change]
                current[key] = 'f'*64
            elif change == 'awscli-version': current['awscli_version'] = '2.0.0'
            elif change == 'awscli-hash': current['awscli_sha256'] = 'f'*64
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
                del current['artifacts']['screen/'+CELL_STEMS[0]+'-records.jsonl' if offered else 'screen/records.jsonl']
            elif change == 'inputs': del current['artifacts'][OFFERED_INPUTS[0]]
            elif change == 'scientific': current['scientific_qualification'] = 'PASS'
            elif change == 'original-exit': current['original_exit_code'] = 1
            elif change == 'cell-ledger':
                name = 'screen/'+CELL_STEMS[0]+'-records.jsonl'
                rows = [json.loads(line) for line in files[name].splitlines()]
                rows[0]['dataset'] = 'CoHere'
                files[name] = ''.join(json.dumps(r)+'\n' for r in rows).encode()
                current['artifacts'][name] = _identity(files[name])
                marker = 'screen/'+CELL_STEMS[0]+'-summary.json'
                summary = json.loads(files[marker])
                summary['records'] = _identity(files[name])
                files[marker] = json.dumps(summary).encode()
                current['artifacts'][marker] = _identity(files[marker])
            elif change and change.startswith('cell-'):
                name = 'screen/'+CELL_STEMS[0]+'-summary.json'
                bad = json.loads(files[name])
                key = {'cell-partial': 'closed', 'cell-config': 'config_sha256', 'cell-record': 'records',
                       'cell-identity': 'identity_gate_passed', 'cell-memory': 'bounded_memory_gate_passed'}[change]
                bad[key] = 'f'*64 if key == 'config_sha256' else {} if key == 'records' else False
                files[name] = json.dumps(bad).encode()
                current['artifacts'][name] = _identity(files[name])
            elif change in ('sha', 'bytes'):
                current['artifacts']['screen/summary.json']['sha256' if change == 'sha' else 'bytes'] = 'f'*64 if change == 'sha' else 1
            elif change == 'unknown':
                current['artifacts']['../escape'] = _identity(b'bad')
            try:
                from scripts import check_native_semantic_router_stats as stats
                def saved(output, digest, binary):
                    if change == 'saved-reducer': raise ValueError('synthetic offline reducer rejection')
                    assert digest == qualification['config_sha256'] and binary.read_bytes() == files['binaries/two_bit_http']
                    assert all((output/n.removeprefix('screen/')).read_bytes() == files[n] for n in OFFERED_INPUTS)
                    return {'offered_gate_passed': False, 'records': 1536}
                with patch.object(stats, 'check_saved', side_effect=saved) as reducer:
                    result = collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64)
                    assert reducer.call_count == int(offered)
            except (AssertionError, ValueError):
                assert change is not None
            else:
                assert change is None and result == terminal, 'changed terminal accepted'
            files = original_files
            (out/'aws-closeout.json').write_text(json.dumps(dict(state='terminated', nodes={'0': dict(instance_id='i-owned')})))
        if offered:
            # Capacity/quality FAIL is collectable with execution zero; no OOM becomes a PASS.
            for code, resource_ok in ((0, True), (1, True), (0, False), (1, False)):
                report = _resource_fixture()
                if not resource_ok: report['memory.events'] = 'oom 1\noom_kill 0\noom_group_kill 0'
                files['profile-cgroup.json'] = json.dumps(report).encode()
                current = dict(terminal, exit_code=code, original_exit_code=code,
                    status='complete' if code == 0 else 'failed', scientific_qualification='FAIL',
                    artifacts={n: _identity(b) for n, b in files.items()})
                with patch.object(stats, 'check_saved', return_value={'offered_gate_passed': False, 'records': 1536}):
                    assert collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64) == current
                checked = json.loads((out/'offered-verification.json').read_bytes())
                assert checked['scientific_qualification'] == 'FAIL' and checked['resource_gate_passed'] == resource_ok
                assert checked['original_exit_code'] == code
        # A failed ABI gate still closes and authenticates its partial evidence.
        files = {'runtime-abi.json': json.dumps(dict(report, qualified=False)).encode()}
        current = dict(terminal, status='failed', phase='binary-qualification', exit_code=1,
            artifacts={n: _identity(b) for n, b in files.items()}, runtime_abi_sha256=peer.sha(files['runtime-abi.json']))
        assert collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64) == current
        if offered:
            record_name, marker_name = ('screen/'+CELL_STEMS[0]+'-'+s for s in ('records.jsonl', 'summary.json'))
            files.update({record_name: b'{"raw_response_base64":"/wBmYWlsZWQ=","cgroup_after":{"diagnostics":{"memory.peak":{"type":"FileNotFoundError","errno":2}}}}\n',
                          marker_name: b'{"closed":true,"identity_gate_passed":false,"bounded_memory_gate_passed":false}\n'})
            current = dict(current, phase='profile', original_exit_code=1,
                           artifacts={n: _identity(b) for n, b in files.items()})
            assert collect(s3, 'synthetic', out, 'i-owned', '0'*40, '1'*64) == current
            for name in (record_name, marker_name):
                assert gzip.decompress((out/(name+'.gz')).read_bytes()) == files[name], 'failed forensic body changed'


def bootstrap_self_check(qualification):
    """Run whole generated user data with cloud, installer and native danger stubs."""
    import shutil
    import tempfile
    module = sys.modules[__name__]
    payload = b'synthetic pinned installer\n'
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        commands = base/'commands'
        commands.mkdir()
        for name in ('bash', 'timeout', 'mkdir', 'cp', 'tar', 'gzip', 'tail', 'stat', 'cat', 'uname'):
            (commands/name).symlink_to(shutil.which(name))
        def stub(name, text):
            path = commands/name
            path.write_text('#!/bin/bash\nset -eu\n'+text)
            path.chmod(0o755)
            return path
        stub('shutdown', 'echo shutdown >> "$EVENTS"\n')
        stub('lscpu', 'echo synthetic-cpu\n')
        stub('taskset', 'shift 2; exec "$@"\n')
        stub('time', 'test "$1" = -v; test "$2" = -o; echo synthetic-resources > "$3"; shift 3; exec "$@"\n')
        stub('systemd-run', '''echo "systemd:$*" >> "$EVENTS"
case "$*" in *--on-active=3600s*) exit 0;; esac
while [[ "$1" != bash && "$1" != "$STUBS/time" ]]; do shift; done
exec "$@"
''')
        stub('apt-get', '''test "$DEBIAN_FRONTEND" = noninteractive
echo "apt:$*" >> "$EVENTS"
case "$*" in *' install awscli '*) echo unsupported-apt-awscli; exit 100;; esac
case "$FAIL:$*" in apt-update:*update) printf '%8192s\\n' ''; echo original-apt-update; exit 42;;
apt-install:*install*|apt-install-no-python:*install*) echo original-apt-install; exit 100;; esac
''')
        curl_template = stub('curl-template', '''echo "curl:$*" >> "$EVENTS"
[[ "$*" == *--connect-timeout* && "$*" == *--max-time* ]]
case "$*" in *169.254.169.254*)
  test -s "$SERIAL"
  case "$FAIL" in apt-install|apt-install-no-python|imds) exit 28;; esac
  case "$*" in */api/token*) echo synthetic-token;; *) echo i-synthetic;; esac;;
*) test "$FAIL" != download || exit 22
   while [[ "$1" != --output ]]; do shift; done
   cp "$INSTALLER_ZIP" "$2"
   test "$FAIL" != hash || echo corrupt >> "$2";; esac
''')
        stub('sha256sum', f'''echo hash >> "$EVENTS"
exec {quote(shutil.which('sha256sum'))} "$@"
''')
        installer = stub('installer-template', '''echo installer >> "$EVENTS"
test "$FAIL" != installer || exit 27
cp "$AWS_TEMPLATE" "$STUBS/aws"
''')
        stub('unzip', '''echo unzip >> "$EVENTS"
mkdir aws
cp "$INSTALLER" aws/install
''')
        aws_template = stub('aws-template', '''echo "aws:$*" >> "$EVENTS"
if [[ "$1" = --version ]]; then echo 'aws-cli/2.36.11 Python/synthetic'; exit; fi
test "$1 $2" = 's3 cp'
case "$4" in source.tar.gz) test "$FAIL" != source || exit 55; cp "$SOURCE_ARCHIVE" "$4";;
binaries/*) echo synthetic-binary > "$4";;
s3://*/artifacts/*) test "$FAIL" != upload || exit 55;;
s3://*/terminal.json) [[ "$(cat "$SERIAL")" == *BORSUK_TERMINAL* ]]
  test "$FAIL" != terminal-upload || exit 55;;
*) exit 91;; esac
''')
        python_stub = commands/'python3.12'
        python_stub.write_text(f'''#!{sys.executable}
import os,sys
from pathlib import Path
args=sys.argv[1:]
with open(os.environ['EVENTS'],'a') as log: log.write('python:'+str(args)+'\\n')
if args[:1]==['-c']:
    if args[1]!='import boto3': exec(args[1])
elif '--stage' in args:
    if os.environ['FAIL']=='abi': sys.exit(23)
    root=Path(os.environ['WORKER_ROOT'])
    assert not (root/'run-closed.log').exists(), 'test precreated closed log'
    for name in os.environ['ARTIFACT_NAMES'].split():
        if name in ('run-closed.log','source-qualification.json'): continue
        path=root/name
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(b'' if name.endswith(('/stdout.log','/stderr.log')) else b'synthetic proof\\n')
    if os.environ['FAIL']=='missing-log': (root/'publication/ReLAION/control/stdout.log').unlink()
    if os.environ['FAIL']=='empty-proof': (root/'screen/summary.json').write_bytes(b'')
elif '--publish' in args:
    if os.environ['FAIL']=='publication': sys.exit(23)
else: print('synthetic closed profile')
''')
        python_stub.chmod(0o755)
        source = base/'source.tar.gz'
        with tarfile.open(source, 'w:gz') as archive:
            info = tarfile.TarInfo('synthetic-source')
            info.size = 6
            archive.addfile(info, io.BytesIO(b'source'))
        zipped = base/'installer.zip'
        zipped.write_bytes(payload)
        with patch.multiple(module, AWSCLI_SHA256=peer.sha(payload), AWSCLI_BYTES=len(payload)):
            proof = dict(qualification, awscli_version='2.36.11', awscli_sha256=peer.sha(payload))
            body = user_data('0'*40, peer.sha(source.read_bytes()), 'sources/synthetic', PREFIX+'a0001', proof)
        for failure, original, phase in (
                ('apt-update', 42, 'apt-update'), ('apt-install', 100, 'apt-install'),
                ('apt-install-no-python', 100, 'apt-install'), ('download', 22, 'awscli-download'),
                ('hash', 1, 'awscli-download'), ('installer', 27, 'awscli-install'),
                ('source', 55, 'source-download'), ('abi', 23, 'binary-qualification'),
                ('publication', 23, 'publication'), ('missing-log', 1, 'profile'),
                ('empty-proof', 1, 'profile'), ('upload', 0, 'complete'),
                ('terminal-upload', 0, 'complete'), ('imds', 0, 'complete'), ('success', 0, 'complete')):
            work = base/failure
            work.mkdir()
            events, serial = work/'events', work/'serial'
            (commands/'aws').unlink(missing_ok=True)
            (commands/'curl').unlink(missing_ok=True)
            (commands/'python3').unlink(missing_ok=True)
            if failure != 'apt-install-no-python':
                (commands/'curl').symlink_to(curl_template)
                (commands/'python3').symlink_to(sys.executable)
            script = body.replace('/mnt/native-semantic-router-cold', str(work))
            script = script.replace('/dev/ttyS0', str(serial)).replace('/usr/bin/time', str(commands/'time'))
            env = dict(os.environ, PATH=str(commands), FAIL=failure, WORKER_ROOT=str(work),
                EVENTS=str(events), SERIAL=str(serial), STUBS=str(commands), INSTALLER=str(installer),
                INSTALLER_ZIP=str(zipped), AWS_TEMPLATE=str(aws_template), SOURCE_ARCHIVE=str(source))
            result = subprocess.run(['/bin/bash', '-c', script], env=env, capture_output=True, timeout=20)
            console = serial.read_text() if serial.exists() else ''
            assert f'phase={phase} original_exit_code={original}' in console, (failure, result.returncode, result.stderr, console)
            calls = events.read_text().splitlines()
            assert calls.count('shutdown') == 1, (failure, calls)
            assert len(console.split('BORSUK_TERMINAL', 1)[0].encode()) < 4300, failure
            assert (result.returncode == 0) == (failure == 'success'), (failure, result.returncode, result.stderr, console)
            assert (work/'run-closed.log').is_file(), failure
            if failure.startswith('apt-'):
                assert 'original-apt-' in console
                assert not any(c.startswith('aws:') for c in calls), (failure, calls)
                assert not any(c.startswith('curl:') and '169.254' not in c for c in calls)
            if failure in ('download', 'hash'):
                assert 'unzip' not in calls and 'installer' not in calls
            if phase in ('apt-update', 'apt-install', 'awscli-download', 'awscli-install'):
                assert not any(c.startswith('aws:s3 cp ') and ' source.tar.gz ' in c for c in calls)
            if phase in ('apt-update', 'apt-install', 'awscli-download', 'awscli-install', 'source-download'):
                assert not any(c.startswith('python:') for c in calls)
            if failure == 'abi': assert not any('--publish' in c for c in calls)
            if failure == 'publication': assert not any('scripts.run_native_semantic_router_cold' in c for c in calls)
            if failure == 'apt-update': assert not any(' install ' in c for c in calls)
            if failure == 'apt-install-no-python':
                assert not (work/'terminal.json').exists() and 'unavailable' in console
                continue
            terminal = json.loads((work/'terminal.json').read_bytes())
            assert terminal['original_exit_code'] == original and terminal['phase'] == phase, failure
            assert terminal['status'] == ('complete' if failure == 'success' else 'failed'), (failure, terminal)
            assert terminal['exit_code'] == result.returncode, (failure, terminal, result.returncode)
            assert 'BORSUK_TERMINAL' in console and terminal['artifacts']['run-closed.log'] == _identity((work/'run-closed.log').read_bytes())
            assert f'BORSUK_FINISH phase={phase} original_exit_code={original} exit_code={result.returncode}' in console
            if failure == 'success':
                assert set(terminal['artifacts']) == set(ARTIFACTS)
                assert terminal['awscli_version'] == proof['awscli_version'] and terminal['awscli_sha256'] == proof['awscli_sha256']
                assert calls.index('hash') < calls.index('unzip') < calls.index('installer')
                assert calls.index('installer') < next(i for i,c in enumerate(calls) if c.startswith('aws:s3 cp '))
                assert sum('/artifacts/' in c for c in calls) == len(ARTIFACTS)


def self_check(offered=False):
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
        runtime = SimpleNamespace(CODE=('scripts/synthetic-runtime.py',),
                                  OFFERED_CODE=('scripts/synthetic-runtime.py',), validate_config=lambda _: None)
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
            bootstrap_self_check(qualification)
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
            assert 'install curl unzip python3-boto3 python3.12 time tar gzip util-linux binutils' in early
            assert all(value in early for value in (AWSCLI_URL, AWSCLI_SHA256, str(AWSCLI_BYTES)))
            assert body.index('--stage ') < body.index('phase=publication\n')
            # Execute generated existence gates, including eight valid empty logs.
            for name in ARTIFACTS:
                path = stage/name
                path.parent.mkdir(parents=True, exist_ok=True)
                if name.startswith('publication/') and name.endswith(('/stdout.log', '/stderr.log')):
                    path.write_bytes(b'')
                elif not path.exists():
                    path.write_bytes(b'synthetic artifact\n')
            (stage/'run.log').write_bytes(b'synthetic run log\n')
            gates_script = body[body.rindex('for name in $ARTIFACT_NAMES; do\n'):body.index('phase=complete\n')]
            gates_env = dict(os.environ, root=str(stage), ARTIFACT_NAMES=' '.join(ARTIFACTS))
            subprocess.run(['bash', '-ec', gates_script], env=gates_env, check=True)
            missing = stage/'binaries/two_bit_plan_demo'
            missing.unlink()
            assert subprocess.run(['bash', '-ec', gates_script], env=gates_env).returncode != 0
            missing.write_bytes(publisher)
            terminal_script = body.split("python3 - <<'PY' >terminal.json\n")[1].split('\nPY\n')[0]
            terminal = json.loads(subprocess.check_output([sys.executable, '-c', terminal_script],
                cwd=stage, env=dict(gates_env, INSTANCE_ID='i-synthetic', EXIT_CODE='0', ORIGINAL_EXIT_CODE='0', PHASE='complete')))
            assert terminal['schema'] == SCHEMA and terminal['binary_sha256'] == binary_sha
            assert terminal['source_identity_sha256'] == source_sha
            assert terminal['qualification_sha256'] == qualification['qualification_sha256']
            assert terminal['required_glibc'] == qualification['required_glibc']
            assert terminal['runtime_abi_sha256'] == peer.sha((stage/'runtime-abi.json').read_bytes())
            if offered:
                _offered_self_check(base, config, stage)
    lifecycle_self_check()
    collection_self_check()
    if offered:
        lifecycle_self_check(offered=True)
        collection_self_check(offered=True)
    # The closed authority has the real source/code/pointer shape, larger than the synthetic fixture.
    repo = Path(__file__).resolve().parents[1]
    closed = json.loads((repo/ROOT/'a0002/aws-reservation.json').read_bytes())
    real = dict(closed['qualification'], awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
    for name in ('scripts/launch_native_semantic_router_cold_spot.py', 'scripts/launch_native_metadata_ranges_cold_spot.py'):
        real['code_sha256'][name] = peer.sha((repo/name).read_bytes())
    authority = json.loads((repo/ROOT/'bootstrap-repair/installer-authority.json').read_bytes())
    assert (authority['version'], authority['url'], authority['sha256'], authority['bytes'], authority['signature_status']) == (
        AWSCLI_VERSION, AWSCLI_URL, AWSCLI_SHA256, AWSCLI_BYTES, 0)
    real_body = user_data(closed['source_commit'], closed['source_archive_sha256'],
        'research/native-library-check/sources/'+closed['source_archive_sha256']+'.tar.gz', PREFIX+'a0002', real)
    if offered:
        real.update(config_path=str(OFFERED_CONFIG), campaign_schema=OFFERED_SCHEMA,
                    artifact_roster_sha256=peer.sha(json.dumps(OFFERED_ARTIFACTS, separators=(',', ':')).encode()))
        real_body = user_data(closed['source_commit'], closed['source_archive_sha256'],
            'research/native-library-check/sources/'+closed['source_archive_sha256']+'.tar.gz',
            OFFERED_PREFIX+'a0002', real, offered=True)
        print(f'offered real-shape bootstrap_bytes={len(real_body.encode())}; actual preflight UNRUN')
    print(f'semantic cold controller self-check PASS; bootstrap_bytes={len(body.encode())}; real_shape_bytes={len(real_body.encode())}; cloud/native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif sys.argv[1:] == ['--offered-self-check']:
        self_check(offered=True)
    elif len(sys.argv) == 4 and sys.argv[1] == '--stage':
        _stage(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 4 and sys.argv[1] == '--publish':
        _publish(sys.argv[2], sys.argv[3])
    elif len(sys.argv) == 8 and sys.argv[1] == '--run-offered':
        raise SystemExit(_run_offered(sys.argv[2:7], sys.argv[7]))
    elif len(sys.argv) == 3 and sys.argv[1] == '--closed-artifacts':
        print(' '.join(_closed_artifacts(sys.argv[2])))
    elif len(sys.argv) == 3 and sys.argv[1] == '--check-offered-closure':
        _check_offered_closure(sys.argv[2])
    else:
        offered = len(sys.argv) == 3 and sys.argv[1] == '--offered'
        assert len(sys.argv) == 2 or offered, 'usage: python3 -m scripts.launch_native_semantic_router_cold_spot [--offered] aNNNN'
        with open('/tmp/borsuk-native-semantic-router-cold-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[2] if offered else sys.argv[1], offered=offered)
