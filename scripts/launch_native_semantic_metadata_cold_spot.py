"""Frozen paired semantic metadata Spot controller; never builds or publishes.

Root supplies metadata-waves/paired-config.json with the runtime's exact contract,
native_arms binary pointers additionally carrying immutable S3 keys, and
native_assurances.{control,candidate} {path,bytes,sha256} pointers. The exact
controller_code_sha256 roster is CODE (including runtime and transitive launch
imports); code_sha256 remains the runtime's CODE. Config and authorities must be
in the pushed source archive. Native binary paths may be external on the host.

CLI: aNNNN | --self-check | --stage REPO OUT | --runtime-abi OUT | --replay OUT
OUT retains raw artifacts; collection also saves each transport body as .gz.
No paid work occurs in self-check. Root owns final config, uploads and launch.

CLI --offered uses metadata-waves/offered-config.json, runtime.OFFERED_CODE,
controller OFFERED_CODE, OFFERED_ARTIFACTS and the offered Spot schema/prefix.
Authorities and binary/proof pointers stay identical to paired-config.json;
proofs do not depend on the new config hash. --offered --run-offered PREFIX
takes the runtime's seven arguments; its shared callback conditionally uploads
records before summary after checking role identities and immutable closure.
--offered --replay OUT separates scientific PASS/FAIL from execution exit0;
identity/resource/cleanup/transport-upload failures remain fatal.
"""
import base64
import copy
import fcntl
import gzip
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
from contextlib import contextmanager, nullcontext
from types import SimpleNamespace
from shlex import quote
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('qualification requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_semantic_router_cold_spot as semantic
from scripts import run_native_semantic_metadata_cold as runtime
from scripts import check_native_semantic_metadata_cold as checker

shared, peer, startup = semantic.shared, semantic.peer, semantic.startup
ROOT = semantic.ROOT / 'metadata-waves'
CONFIG = ROOT / 'paired-config.json'
NAME = ''
SCHEMA = 'borsuk-native-semantic-metadata-waves-cold-spot-v1'
PREFIX = 'research/semantic-router/20261001/metadata-waves-'
TOKEN_PREFIX = 'semantic-metadata-waves-cold-'
TAG = 'borsuk-semantic-metadata-waves-cold'
WALL = 3600
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', semantic.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = semantic.ROOT_DEVICE_NAME, 'subnet-034528fbd6977848f'
SPOT_MAX_USD_PER_HOUR = COMPUTE_CAP = .50
MODULE = 'scripts.launch_native_semantic_metadata_cold_spot'
CODE = tuple(sorted(set((*runtime.CODE, *semantic.EXTRAS,
    'scripts/run_native_metadata_ranges_cold.py', 'scripts/check_native_metadata_ranges_stats.py',
    'scripts/launch_native_semantic_metadata_cold_spot.py'))))
BASELINE = semantic.ROOT / 'config.json'
BASELINE_SHA = '361f82d84e3a08a1efb5f31b516f20aa0d8735a264052ebfef2e921620564304'
# Candidate proof is bound by the root's final config, after actual full execution.
PROOF_SHA = dict(control='528591dd8e6d88c850b445ef17234a700638e5fae939378318fa9cc7d299abbe')
GATES = semantic.GATES
CANDIDATE_GATES = ('object-native', 'generation', 'http-release', 'clippy', 'workspace-test-build', 'full-workspace-final')
IMPLEMENTATION_SHA = '8d56760e8f9063d02bfc3421ebf4079facc473d8697433c0f5d2f7dfb9f65cb2'
BOUND_FILES = ('checker-authority.json', *(f'{r}-{n}' for r in runtime.ROLES
    for n in ('source.json', 'checker.py.gz', 'proof.json')))
AUTHORITY_FILES = ('panel-authority.json', *(f'{r}/{n}' for r, gates, verification in (
    ('control', GATES, 'worker-verification.json'),
    ('candidate', CANDIDATE_GATES, 'implementation-verification.json'))
    for n in ('native-assurance.json', 'native-source.tar.gz', verification,
              *(f'assurance/{g}.{s}' for g in gates for s in ('json', 'log')))))
ARTIFACTS = ('source-qualification.json', 'runtime-abi.json', 'cpu.txt', 'run-closed.log',
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
    *(f'binaries/{r}/two_bit_http' for r in runtime.ROLES), *BOUND_FILES, *AUTHORITY_FILES,
    'screen/config.json', 'screen/records.jsonl', 'screen/summary.json',
    *('screen/' + n for n in BOUND_FILES), *semantic.OFFERED_INPUTS)
OFFERED_CONFIG = ROOT / 'offered-config.json'
OFFERED_SCHEMA = 'borsuk-native-semantic-metadata-waves-cold-offered-spot-v1'
OFFERED_PREFIX = 'research/semantic-router/20261001/metadata-waves-offered-'
OFFERED_CODE = tuple(sorted(set((*CODE, *runtime.OFFERED_CODE))))
OFFERED_ARTIFACTS = (*(n for n in ARTIFACTS if n != 'screen/records.jsonl'),
    *(f'screen/{stem}-{suffix}' for stem in semantic.CELL_STEMS for suffix in ('records.jsonl', 'summary.json')))
_semantic_cell_bodies = semantic._cell_bodies
TERMINAL_IDENTITIES = ('config_sha256', 'role_bindings', 'artifact_roster_sha256',
    'code_identity_sha256', 'awscli_version', 'awscli_sha256')


def encoded(value):
    return runtime.worker.encoded(value).encode()


@contextmanager
def offered_mode():
    with patch.multiple(sys.modules[__name__], CONFIG=OFFERED_CONFIG, SCHEMA=OFFERED_SCHEMA,
            PREFIX=OFFERED_PREFIX, TOKEN_PREFIX=TOKEN_PREFIX+'offered-', TAG=TAG+'-offered',
            CODE=OFFERED_CODE, ARTIFACTS=OFFERED_ARTIFACTS):
        yield


def _cell_bodies(summary, paths, config):
    role = summary['arm']
    assert role in runtime.ROLES
    # Retain the shared closed-ledger checks, with this cell's actual role proof.
    cell = dict(config, binary=config['native_arms'][role]['binary'],
                qualification_sha256=config['native_arms'][role]['proof']['sha256'])
    evidence = runtime.authenticate(config, Path(paths['records']).parent)
    expected = runtime.binding(config, config['config_sha256'], role, evidence)
    assert all(summary[k] == v for k, v in expected.items()), 'cell role authority'
    assert summary['code_sha256'] == config['code_sha256'] and summary['native_source_file_count'] == 399
    result = _semantic_cell_bodies(summary, paths, cell)
    assert all(all(row.get(k) == v for k, v in expected.items())
               for row in map(json.loads, result[1].splitlines())), 'cell row role authority'
    return result


def _run_offered(argv, prefix):
    assert len(argv) == 7, 'paired offered CLI'
    adapter = SimpleNamespace(main=lambda unused, **kwargs: runtime.main(argv, **kwargs))
    with patch.multiple(semantic, OFFERED_PREFIX=OFFERED_PREFIX, OFFERED_RUNTIME_SCHEMA=runtime.OFFERED_SCHEMA), \
            patch.object(semantic, '_worker', return_value=adapter), patch.object(semantic, '_cell_bodies', _cell_bodies):
        return semantic._run_offered([*argv[:4], argv[6]], prefix)


def _zero(value):
    assert type(value) is int and value == 0, 'typed completed gate'


def _repo_path(name):
    path = Path(name)
    assert not path.is_absolute() and '..' not in path.parts and str(path) == name, 'repository authority path'
    return path


def _read(base, pointer):
    _repo_path(pointer.get('archived_path', pointer['path']))
    return semantic._read(base, pointer)


def _full_receipt(body, log, inventory, identity):
    receipt = json.loads(body)
    _zero(receipt['exit_status'])
    assert receipt['source_unchanged'] is True
    assert receipt['source_sha256'] == inventory and receipt['source_identity_sha256'] == identity
    assert type(receipt['source_file_count']) is int and receipt['source_file_count'] == 399
    assert {'test', '--workspace', '--all-targets', '--locked'} <= set(receipt['command'])
    assert '--no-run' not in receipt['command'], 'test compilation is not execution'
    assert receipt['artifacts']['test.log'] == semantic._identity(log)


def _assurance(base, role, config, evidence, proof):
    """Authenticate completed HEAD/wave receipts without manufacturing qualification."""
    body = _read(base, config['native_assurances'][role])
    assert peer.sha(body) == proof['assurance_sha256'], 'proof/assurance binding'
    assurance = json.loads(body)
    assert assurance['full_workspace_test_execution'] is True
    inventory = evidence['manifests'][role]['source_sha256']
    identity = runtime.SOURCE_IDS[role]
    files = {'native-assurance.json': body}
    archive = _read(base, evidence['manifests'][role]['native_source_archive'])
    assert semantic._archive_sources(archive) == inventory, 'full qualified native archive'
    files['native-source.tar.gz'] = archive
    assert assurance['schema'] == 'borsuk-semantic-metadata-native-assurance-v1'
    assert assurance['qualified'] is True and assurance['source_sha256'] == inventory
    assert assurance['source_identity_sha256'] == identity and assurance['source_file_count'] == 399
    _zero(assurance['full_suite_status'])
    if role == 'control':
        verification_body = _read(base, assurance['worker_verification'])
        verification = json.loads(verification_body)
        assert verification['native_source_identity_sha256'] == identity and verification['native_source_file_count'] == 399
        assert {k: verification['binaries']['two_bit_http'][k] for k in ('bytes', 'sha256')} == runtime.BINARY_IDS[role]
        assert all(verification['owned_sources'][n] == inventory[n] for n in runtime.NATIVE_DELTA)
        files['worker-verification.json'] = verification_body
        for name in GATES[:-1]:
            gate = assurance['gates'][name]
            assert gate == verification['gates'][name], 'control gate/worker binding'
            _zero(gate['exit_status'])
            log = gzip.decompress((base / _repo_path(gate['archived_log']['path'])).read_bytes())
            assert semantic._identity(log) == dict(bytes=gate['archived_log']['uncompressed_bytes'], sha256=gate['archived_log']['sha256'])
            assert peer.sha(log) == gate['log_sha256']
            files[f'assurance/{name}.json'], files[f'assurance/{name}.log'] = encoded(gate), log
    else:
        verification_body = _read(base, assurance['implementation_gate_verification'])
        assert peer.sha(verification_body) == IMPLEMENTATION_SHA, 'frozen final-slice verification'
        verification = json.loads(verification_body)
        assert verification['schema'] == 'borsuk-metadata-waves-slice-verification-v1'
        assert verification['source_identity_sha256'] == identity and verification['source_file_count'] == 399
        assert [g['name'] for g in verification['checks']] == list(CANDIDATE_GATES[:-1]), 'exact five implementation checks'
        files['implementation-verification.json'] = verification_body
        for gate in verification['checks']:
            _zero(gate['exit_status'])
            archive = (base / _repo_path(gate['archived_log'])).read_bytes()
            assert peer.sha(archive) == gate['archived_sha256'], 'implementation archived log'
            log = gzip.decompress(archive)
            assert semantic._identity(log) == dict(bytes=gate['log_bytes'], sha256=gate['log_sha256'])
            name = gate['name']
            files[f'assurance/{name}.json'], files[f'assurance/{name}.log'] = encoded(gate), log
    receipt_archive = _read(base, assurance['full_workspace_receipt'])
    assert receipt_archive == _read(base, proof['full_workspace_receipt'])
    receipt_body = gzip.decompress(receipt_archive)
    log = gzip.decompress(_read(base, assurance['full_workspace_log']))
    _full_receipt(receipt_body, log, inventory, identity)
    files['assurance/full-workspace-final.json'] = receipt_body
    files['assurance/full-workspace-final.log'] = log
    return {role + '/' + name: value for name, value in files.items()}


def _qualify(base=Path('.'), binaries=None):
    base = Path(base).resolve()
    config_body = (base / CONFIG).read_bytes()
    config = json.loads(config_body)
    assert config.get('controller_authority_pending') is False, 'controller authority pending'
    _repo_path(config['checker_authority']['path'])
    assert all(config['native_arms'][r]['proof']['sha256'] == h for r, h in PROOF_SHA.items()), 'frozen completed control proof'
    with semantic._cwd(base), patch.object(runtime, 'ROOT', base):
        runtime.validate_config(config)
        evidence = runtime.authenticate(config)
        proofs = {r: base / _repo_path(config['native_arms'][r]['proof']['path']) for r in runtime.ROLES}
        binaries = binaries or {r: base / config['native_arms'][r]['binary']['path'] for r in runtime.ROLES}
        qualified = runtime.validate_runtime(config, binaries, proofs, evidence)
    expected = dict(architecture='x86_64', bucket=peer.BUCKET, region=peer.REGION,
        instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, subnet_id=SUBNET,
        spot_max_usd_per_hour=.50, compute_cap_usd=.50, ebs_s3_allowance_usd=.15)
    assert all(type(config[k]) is type(v) and config[k] == v for k, v in expected.items()), 'fixed infrastructure/cost'
    assert set(config['controller_code_sha256']) == set(CODE), 'complete transitive controller code'
    for name, digest in config['controller_code_sha256'].items():
        assert runtime.old.sha(base/name) == runtime.stats.digest(digest), 'controller code identity: ' + name
    assert all(config['controller_code_sha256'][n] == d for n, d in config['code_sha256'].items())
    assert set(config['native_assurances']) == set(runtime.ROLES)
    files = dict(evidence['bodies'])
    panel_body = (base / BASELINE).read_bytes()
    assert peer.sha(panel_body) == BASELINE_SHA, 'original immutable semantic panel authority'
    baseline = json.loads(panel_body)
    expected_items = copy.deepcopy(baseline['items'])
    for item in expected_items:
        item['arms'] = {r: copy.deepcopy(item['arms']['candidate']) for r in runtime.ROLES}
    assert config['items'] == expected_items, 'original candidate semantic inputs for both roles'
    files['panel-authority.json'] = panel_body
    assert semantic.source_hashes(base) == evidence['manifests']['candidate']['source_sha256'], 'current candidate source identity'
    required_glibc = {}
    for role in runtime.ROLES:
        arm = config['native_arms'][role]
        for name in ('proof', 'source_manifest'):
            _repo_path(arm[name]['path'])
        binary = arm['binary']
        assert set(binary) == {'path', 'bytes', 'sha256', 'key'}
        assert type(binary['key']) is str and binary['sha256'] in binary['key']
        assert re.fullmatch(r'[A-Za-z0-9_./-]+', binary['key']) and '..' not in Path(binary['key']).parts
        files[role + '-proof.json'] = _read(base, arm['proof'])
        files.update(_assurance(base, role, config, evidence, qualified[role]))
        required_glibc[role] = semantic._required_glibc(binaries[role])
        assert semantic._version(required_glibc[role]) <= semantic._version(semantic.RUNTIME_GLIBC)
    assert set(files) == set(BOUND_FILES) | set(AUTHORITY_FILES), 'exact authority roster'
    digest = peer.sha(config_body)
    qualification = dict(campaign_schema=SCHEMA, config_path=str(CONFIG), config_sha256=digest,
        role_bindings={r: runtime.binding(config, digest, r, evidence) for r in runtime.ROLES},
        native_binaries={r: config['native_arms'][r]['binary'] for r in runtime.ROLES},
        code_sha256=config['controller_code_sha256'], code_identity_sha256=peer.sha(encoded(config['controller_code_sha256'])),
        authority_artifacts={n: semantic._identity(b) for n, b in files.items()},
        artifact_roster_sha256=peer.sha(encoded(ARTIFACTS)), native_rebuilt=False,
        current_full_suite_pass_claim=True, full_workspace_test_execution=True,
        runtime_os=semantic.RUNTIME_OS, runtime_glibc=semantic.RUNTIME_GLIBC, required_glibc=required_glibc,
        awscli_version=semantic.AWSCLI_VERSION, awscli_sha256=semantic.AWSCLI_SHA256)
    return qualification, files


def preflight(base=Path('.')):
    return _qualify(base)[0]


def _validate_runtime_abi(qualification, report):
    assert report['schema'] == 'borsuk-native-semantic-metadata-waves-runtime-abi-v1' and report['qualified'] is True
    assert report['os_release'] == qualification['runtime_os'] == semantic.RUNTIME_OS
    assert report['architecture'] == 'x86_64' and report['libc'] == 'glibc'
    assert qualification['runtime_glibc'] == semantic.RUNTIME_GLIBC
    assert report['binaries'] == runtime.BINARY_IDS
    assert report['required_glibc'] == qualification['required_glibc']
    assert set(report['ldd']) == set(report['required_glibc']) == set(runtime.ROLES)
    for role, result in report['ldd'].items():
        assert semantic._version(report['required_glibc'][role]) <= semantic._version(report['glibc_version'])
        _zero(result['returncode'])
        output = (result['stdout'] + result['stderr']).lower()
        assert output.strip() and not any(s in output for s in ('not found', 'undefined symbol', 'unresolved'))


def _runtime_abi(out):
    out = Path(out)
    qualification = json.loads((out/'source-qualification.json').read_bytes())
    report = dict(schema='borsuk-native-semantic-metadata-waves-runtime-abi-v1', qualified=False)
    try:
        release = platform.freedesktop_os_release()
        libc, version = os.confstr('CS_GNU_LIBC_VERSION').split()
        report.update(os_release={k: release[k] for k in semantic.RUNTIME_OS}, architecture=platform.machine(),
            libc=libc, glibc_version=version, binaries={}, required_glibc={}, ldd={})
        for role in runtime.ROLES:
            binary = out/'binaries'/role/'two_bit_http'
            identity = dict(bytes=binary.stat().st_size, sha256=runtime.old.sha(binary))
            assert identity == runtime.BINARY_IDS[role], 'ABI binary authentication'
            report['binaries'][role] = identity
            report['required_glibc'][role] = semantic._required_glibc(binary)
            result = subprocess.run(['ldd', '-r', str(binary)], capture_output=True, text=True,
                timeout=15, env=dict(os.environ, LC_ALL='C'))
            report['ldd'][role] = dict(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        report['qualified'] = True
        _validate_runtime_abi(qualification, report)
    except BaseException:
        report['qualified'] = False
        raise
    finally:
        (out/'runtime-abi.json').write_bytes(encoded(report) + b'\n')


def _stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    saved = json.loads((out/'source-qualification.json').read_bytes())
    binaries = {r: out/'binaries'/r/'two_bit_http' for r in runtime.ROLES}
    actual, files = _qualify(repo, binaries)
    assert actual == saved, 'remote qualification differs from frozen preflight'
    for name, body in files.items():
        path = out/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    for binary in binaries.values():
        binary.chmod(0o755)
    _runtime_abi(out)  # Before the first measurement or native invocation.


def _check_closed(out):
    out = Path(out)
    proof = json.loads((out/'source-qualification.json').read_bytes())
    result = checker.check_saved(out/'screen', proof['config_sha256'],
        out/'binaries/control/two_bit_http', out/'binaries/candidate/two_bit_http')
    offered = proof['campaign_schema'] == OFFERED_SCHEMA
    assert result['records'] == (1536 if offered else 512) and result['process_cleanup_complete'], 'fixed closed raw ledger'
    if offered:
        assert result['execution_gate_passed'] and result['identity_gate_passed'] and result['bounded_memory_gate_passed'], 'offered fatal execution'
    assert semantic._profile_resources(json.loads((out/'profile-cgroup.json').read_bytes())), 'profile resources'
    return result


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch(r'[0-9a-f]{40}', commit) and re.fullmatch(r'[0-9a-f]{64}', archive_sha)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    assert qualification['campaign_schema'] == SCHEMA and qualification['config_path'] == str(CONFIG)
    # Reuse the pinned official installer, source authentication and EXIT trap.
    # The helper's measurement/publication section is replaced before use.
    offered = qualification['campaign_schema'] == OFFERED_SCHEMA
    adapter = dict({k: qualification[k] for k in (*TERMINAL_IDENTITIES, 'config_path')},
        native_binary=qualification['native_binaries']['control'],
        native_publisher=qualification['native_binaries']['candidate'])
    with patch.multiple(semantic, SCHEMA=SCHEMA, ARTIFACTS=('run-closed.log',) if offered else ARTIFACTS,
            TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(semantic, '_offered', return_value=False):
        body = semantic.user_data(commit, archive_sha, archive_key, prefix, adapter)
    payload = base64.b64encode(gzip.compress(encoded(qualification), mtime=0)).decode()
    downloads = '\n'.join('aws s3 cp ' + quote('s3://' + peer.BUCKET + '/' + qualification['native_binaries'][r]['key']) +
        ' binaries/' + r + '/two_bit_http --only-show-errors' for r in runtime.ROLES)
    config_path = quote(qualification['config_path'])
    command = f'''phase=install
python3.12 -c 'import boto3'
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("{payload}")))'
phase=binary-qualification
lscpu >cpu.txt
test "$(uname -m)" = x86_64
mkdir -p binaries/control binaries/candidate
{downloads}
PYTHONPATH="$root/repo" python3.12 -m {MODULE} --stage "$root/repo" "$root"
phase=profile
set +e
systemd-run --unit=native-semantic-metadata-waves-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=3030 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=536870912 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3000 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_semantic_metadata_cold {config_path} {qualification['config_sha256']} "$1/binaries/control/two_bit_http" "$1/control-proof.json" "$1/binaries/candidate/two_bit_http" "$1/candidate-proof.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 -m scripts.check_native_startup_build --cgroup "$1/profile-cgroup.json" 8589934592; resources=$?; if [ "$code" = 0 ] && [ "$resources" != 0 ]; then code=96; fi; exit "$code"' _ "$root" >profile.log 2>&1
profile_code=$?
set -e
if [ "$profile_code" -gt 1 ]; then exit "$profile_code"; fi
if ! PYTHONPATH="$root/repo" python3.12 -m {MODULE} --check-closed "$root"; then
  if [ "$profile_code" = 0 ]; then profile_code=96; fi
  exit "$profile_code"
fi
for name in $ARTIFACT_NAMES; do
  if [ "$name" = run-closed.log ]; then test -s "$root/run.log"; else test -s "$root/$name"; fi
done
'''
    if offered:
        command = command.replace('phase=binary-qualification\n',
            'export ARTIFACT_NAMES=$(PYTHONPATH="$root/repo" python3.12 -c "from '+MODULE+
            ' import OFFERED_ARTIFACTS; print(\' \'.join(OFFERED_ARTIFACTS))")\nphase=binary-qualification\n', 1)
        command = command.replace('scripts.run_native_semantic_metadata_cold', MODULE+' --run-offered '+quote(prefix))
        command = command.replace('-m '+MODULE+' ', '-m '+MODULE+' --offered ')
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    body = body.replace('phase=complete\n', 'phase=complete\nexit "$profile_code"\n', 1)
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/native-semantic-metadata-waves-cold')
    marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    body = body.replace(marker, marker + "'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),")
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384, 'EC2 user data limit'
    return body


def poll(ec2, s3, prefix, instance_id, started):
    return semantic.poll(ec2, s3, prefix, instance_id, started)


def _authenticate_collected(out, terminal, qualification):
    """Authenticate every listed body before interpreting even a failed run."""
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == qualification[key], 'terminal ' + key
    assert qualification['artifact_roster_sha256'] == peer.sha(encoded(ARTIFACTS))
    assert set(qualification['code_sha256']) == set(CODE)
    assert qualification['code_identity_sha256'] == peer.sha(encoded(qualification['code_sha256']))
    for name, digest in qualification['code_sha256'].items():
        assert runtime.old.sha(runtime.ROOT/name) == digest, 'replay controller code: ' + name
    files = {}
    for name, identity in terminal['artifacts'].items():
        body = (out/name).read_bytes()
        assert semantic._identity(body) == identity, 'artifact body: ' + name
        files[name] = body
    assert terminal['source_qualification_sha256'] == terminal['artifacts'].get('source-qualification.json', {}).get('sha256')
    assert terminal['runtime_abi_sha256'] == terminal['artifacts'].get('runtime-abi.json', {}).get('sha256')
    if 'source-qualification.json' in files:
        assert json.loads(files['source-qualification.json']) == qualification
    for name, identity in qualification['authority_artifacts'].items():
        if name in files:
            assert semantic._identity(files[name]) == identity, 'authority: ' + name
        if 'screen/' + name in files:
            assert semantic._identity(files['screen/' + name]) == identity, 'saved authority: ' + name
    for role in runtime.ROLES:
        name = f'binaries/{role}/two_bit_http'
        if name in files:
            assert semantic._identity(files[name]) == runtime.BINARY_IDS[role], 'role binary'
    if 'runtime-abi.json' in files:
        _validate_runtime_abi(qualification, json.loads(files['runtime-abi.json']))
    if 'screen/config.json' in files:
        assert peer.sha(files['screen/config.json']) == qualification['config_sha256']
        config = json.loads(files['screen/config.json'])
        assert config.get('controller_authority_pending') is False
        assert config['controller_code_sha256'] == qualification['code_sha256']
        assert all(config['native_arms'][r]['proof']['sha256'] == h for r, h in PROOF_SHA.items())
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    assert terminal['status'] == ('complete' if terminal['exit_code'] == 0 and terminal['phase'] == 'complete' else 'failed')
    if terminal['phase'] == 'complete':
        assert terminal['exit_code'] == terminal['original_exit_code'] in (0, 1), 'original runtime status'
        assert set(files) == set(ARTIFACTS), 'exact terminal artifact roster'
        result = _check_closed(out)
        if qualification['campaign_schema'] == OFFERED_SCHEMA:
            assert terminal['exit_code'] == 0 and result['execution_gate_passed'], 'offered execution status'
            result = dict(result, scientific_qualification='PASS' if result['offered_gate_passed'] else 'FAIL')
        else:
            assert (terminal['exit_code'] == 0) == result['paired_gate_passed'], 'raw scientific outcome/status'
        return result
    return None


def replay(out):
    """Offline saved artifact authentication/reduction; no AWS/native calls."""
    out = Path(out)
    reservation = json.loads((out/'aws-reservation.json').read_bytes())
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    terminal = json.loads((out/'aws-terminal.json').read_bytes())
    assert closed['state'] == 'terminated'
    assert terminal['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for name in ('source_commit', 'source_archive_sha256'):
        assert terminal[name] == reservation[name]
    return _authenticate_collected(out, terminal, reservation['qualification'])


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['instance_id'] == instance_id and terminal['source_commit'] == commit
    assert terminal['source_archive_sha256'] == digest and terminal['schema'] == SCHEMA
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    for name, identity in terminal['artifacts'].items():
        body = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/artifacts/'+name)['Body'].read()
        path = out/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)  # Preserve raw failed bodies as well as valid measurements.
        (out/(name+'.gz')).write_bytes(gzip.compress(body, mtime=0))
        assert semantic._identity(body) == identity, 'download body: ' + name
    result = replay(out)
    (out/'collection-replay.json').write_bytes(encoded(dict(terminal_sha256=peer.sha(raw), result=result)) + b'\n')
    return terminal


def main(attempt):
    # Require the exact source commit already on a remote ref; never fetch/build remotely.
    subprocess.run(['git', 'merge-base', '--is-ancestor', 'HEAD', 'refs/remotes/origin/main'], check=True)
    return shared.main(attempt, campaign=sys.modules[__name__])


def _lifecycle_self_check():
    from datetime import datetime, timezone
    from unittest.mock import Mock
    module = sys.modules[__name__]
    for failure in ('success', 'fsync', 'upload', 'poll', 'interrupt', 'multi-ack', 'multi-ack-fsync'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ids = ['i-owned', 'i-extra'] if failure.startswith('multi-ack') else ['i-owned']
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
            def collected(*args):
                assert events == ['terminate', 'wait'], 'collection before termination waiter'
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0, artifacts={n: {} for n in ARTIFACTS})
            error = {'poll': RuntimeError('interrupted'), 'interrupt': KeyboardInterrupt()}.get(failure)
            with patch.object(module, 'ROOT', Path(tmp)), patch.object(module, 'preflight', return_value={'config_sha256': 'a'*64}), \
                    patch.object(module, 'user_data', return_value='mock'), patch.object(module, 'poll', side_effect=error), \
                    patch.object(module, 'collect', side_effect=collected), patch.object(shared.boto3, 'Session', return_value=session), \
                    patch.object(subprocess, 'run'), patch.object(subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), \
                    patch.object(peer, 'missing', return_value=True), patch.object(peer, 'put_if_absent',
                        side_effect=[None, None, OSError('upload')] if failure == 'upload' else [None]*3), \
                    patch.object(os, 'fsync', side_effect=OSError('persist') if failure.endswith('fsync') else None) as fsync:
                try:
                    main('a0001')
                except (OSError, RuntimeError, KeyboardInterrupt):
                    assert failure not in ('success', 'multi-ack')
                else:
                    assert failure in ('success', 'multi-ack'), 'failure swallowed'
                fsync.assert_called_once()
            assert events == ['terminate', 'wait', 'collect']
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            args = ec2.run_instances.call_args.kwargs
            assert args['InstanceType'] == INSTANCE_TYPE and args['ImageId'] == IMAGE_ID
            assert args['BlockDeviceMappings'] == [dict(DeviceName='/dev/sda1', Ebs=dict(
                DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType='gp3'))]
            assert args['ClientToken'].startswith(TOKEN_PREFIX)
            for name in ('aws-launch.json', 'aws-closeout.json'):
                assert json.loads((Path(tmp)/'a0001'/name).read_bytes())['nodes'] == {
                    str(i): dict(instance_id=n) for i, n in enumerate(ids)}
    shared.self_check(lifecycle_only=True)  # Includes transient reads and bounded termination retries.


def _collection_self_check(qualification, authorities, abi, config, rejected):
    import io
    from unittest.mock import Mock
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        files = {n: b'synthetic closed artifact\n' for n in ARTIFACTS}
        files.update(authorities)
        files.update({'screen/'+n: authorities[n] for n in BOUND_FILES})
        files['source-qualification.json'] = encoded(qualification)
        files['runtime-abi.json'] = encoded(abi)
        files['screen/config.json'] = encoded(config)
        files['profile-cgroup.json'] = encoded(semantic._resource_fixture())
        for role in runtime.ROLES:
            files[f'binaries/{role}/two_bit_http'] = Path(config['native_arms'][role]['binary']['path']).read_bytes()
        terminal = dict(schema=SCHEMA, instance_id='i-owned', source_commit='0'*40,
            source_archive_sha256='1'*64, phase='complete', status='complete', exit_code=0, original_exit_code=0,
            **{k: qualification[k] for k in TERMINAL_IDENTITIES},
            artifacts={n: semantic._identity(b) for n, b in files.items()},
            runtime_abi_sha256=peer.sha(files['runtime-abi.json']),
            source_qualification_sha256=peer.sha(files['source-qualification.json']))
        reservation = dict(schema=SCHEMA, source_commit='0'*40, source_archive_sha256='1'*64, qualification=qualification)
        (out/'aws-reservation.json').write_bytes(encoded(reservation))
        (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated', nodes={'0': dict(instance_id='i-owned')})))
        s3 = Mock()
        def get_object(**kw):
            assert kw['Bucket'] == peer.BUCKET
            name = kw['Key'].split('/artifacts/', 1)
            return dict(Body=io.BytesIO(files[name[1]] if len(name) == 2 else encoded(terminal)))
        s3.get_object.side_effect = get_object
        offered = qualification['campaign_schema'] == OFFERED_SCHEMA
        result = dict(records=1536 if offered else 512, process_cleanup_complete=True, execution_gate_passed=True,
                      paired_gate_passed=True, offered_gate_passed=True, identity_gate_passed=True, bounded_memory_gate_passed=True)
        with patch.object(checker, 'check_saved', return_value=result) as checked:
            assert collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64) == terminal
            checked.assert_called_once()
            assert replay(out) == (dict(result, scientific_qualification='PASS') if offered else result)
            # No network may be touched before the owned termination receipt.
            original_close = (out/'aws-closeout.json').read_bytes()
            (out/'aws-closeout.json').write_bytes(encoded(dict(state='running', nodes={})))
            s3.reset_mock()
            rejected(lambda: collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64))
            s3.get_object.assert_not_called()
            (out/'aws-closeout.json').write_bytes(original_close)
            for name in ARTIFACTS:
                path = out/name
                original = path.read_bytes()
                path.write_bytes(original + b'tampered')
                rejected(lambda: replay(out))
                path.write_bytes(original)
            for name in ('config_sha256', 'role_bindings', 'artifact_roster_sha256'):
                bad = dict(terminal, **{name: 'tampered'})
                (out/'aws-terminal.json').write_bytes(encoded(bad))
                rejected(lambda: replay(out))
            # Preserve a complete scientific FAIL, and replay it instead of
            # rewriting it as a successful measurement.
            terminal.update(status='complete' if offered else 'failed', exit_code=0 if offered else 1, original_exit_code=0 if offered else 1)
            result['paired_gate_passed'] = False
            result['offered_gate_passed'] = False
            assert collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64)['exit_code'] == (0 if offered else 1)
            if offered:
                assert replay(out)['scientific_qualification'] == 'FAIL'
                result['execution_gate_passed'] = False
                rejected(lambda: replay(out))
                result['execution_gate_passed'] = True
            with patch.object(checker, 'check_saved', side_effect=ValueError('raw replay failed')):
                rejected(lambda: replay(out))
            terminal.update(phase='profile', status='failed', exit_code=24, original_exit_code=24)
            del terminal['artifacts']['screen/summary.json']
            assert collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64)['exit_code'] == 24
            files['control-proof.json'] += b'tampered'
            rejected(lambda: collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64))


def _bootstrap_self_check(qualification):
    """Execute generated shell with installers, network and runtime stubbed."""
    import io
    import shutil
    import tarfile
    offered = qualification['campaign_schema'] == OFFERED_SCHEMA
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        commands = base/'commands'
        commands.mkdir()
        for name in ('bash', 'timeout', 'mkdir', 'cp', 'tar', 'gzip', 'tail', 'stat', 'cat', 'uname', 'sha256sum'):
            (commands/name).symlink_to(shutil.which(name))
        def stub(name, text):
            path = commands/name
            path.write_text('#!/bin/bash\nset -eu\n'+text)
            path.chmod(0o755)
            return path
        stub('shutdown', 'echo shutdown >> "$EVENTS"\n')
        stub('lscpu', 'echo synthetic-cpu\n')
        stub('taskset', 'shift 2; exec "$@"\n')
        stub('time', 'echo synthetic-resources > "$3"; shift 3; exec "$@"\n')
        stub('systemd-run', '''echo "systemd:$*" >> "$EVENTS"
case "$*" in *--on-active=3600s*) exit 0;; esac
while [[ "$1" != "$STUBS/time" ]]; do shift; done
exec "$@"
''')
        stub('apt-get', '''echo "apt:$*" >> "$EVENTS"
case "$FAIL:$*" in apt:*update|no-python:*update) echo original-apt-failure; exit 42;; esac
''')
        curl = stub('curl-template', '''echo "curl:$*" >> "$EVENTS"
[[ "$*" == *--connect-timeout* && "$*" == *--max-time* ]]
case "$*" in *169.254.169.254*)
  case "$*" in */api/token*) echo synthetic-token;; *) echo i-owned;; esac;;
*) while [[ "$1" != --output ]]; do shift; done
   cp "$INSTALLER_ZIP" "$2"
   test "$FAIL" != hash || echo corrupt >> "$2";; esac
''')
        installer = stub('installer-template', 'cp "$AWS_TEMPLATE" "$STUBS/aws"\n')
        stub('unzip', 'mkdir aws; cp "$INSTALLER" aws/install\n')
        aws = stub('aws-template', '''echo "aws:$*" >> "$EVENTS"
if [[ "$1" = --version ]]; then echo 'aws-cli/2.36.11 Python/synthetic'; exit; fi
test "$1 $2" = 's3 cp'
case "$4" in source.tar.gz) cp "$SOURCE_ARCHIVE" "$4";;
binaries/*) echo synthetic-binary > "$4";;
s3://*/artifacts/*) test "$FAIL" != upload || exit 55;;
s3://*/terminal.json) test "$FAIL" != terminal-upload || exit 55;;
*) exit 91;; esac
''')
        py = commands/'python3.12'
        py.write_text(f'''#!{sys.executable}
import os,sys
from pathlib import Path
args=sys.argv[1:]
root=Path(os.environ['WORKER_ROOT'])
with open(os.environ['EVENTS'],'a') as log: log.write('python:'+str(args)+'\\n')
if args[:1]==['-c']:
    if args[1].startswith('from scripts.launch_native_semantic_metadata_cold_spot import OFFERED_ARTIFACTS'):
        print(' '.join({ARTIFACTS!r}))
    elif args[1]!='import boto3': exec(args[1])
elif '--stage' in args:
    if os.environ['FAIL']=='abi': sys.exit(23)
    for name in os.environ['ARTIFACT_NAMES'].split():
        if name in ('run-closed.log','source-qualification.json'): continue
        path=root/name; path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(b'synthetic artifact\\n')
elif '--check-closed' in args:
    if os.environ['FAIL']=='malformed': sys.exit(1)
elif '--cgroup' in args:
    print('synthetic cgroup')
else:
    print('synthetic raw profile')
    sys.exit({{'scientific':{0 if offered else 1},'native':24,'malformed':1}}.get(os.environ['FAIL'],0))
''')
        py.chmod(0o755)
        source = base/'source.tar.gz'
        with tarfile.open(source, 'w:gz') as archive:
            info = tarfile.TarInfo('synthetic-source')
            info.size = 6
            archive.addfile(info, io.BytesIO(b'source'))
        zipped = base/'installer.zip'
        zipped.write_bytes(b'synthetic pinned installer\n')
        with patch.multiple(semantic, AWSCLI_SHA256=peer.sha(zipped.read_bytes()), AWSCLI_BYTES=zipped.stat().st_size):
            proof = dict(qualification, awscli_sha256=peer.sha(zipped.read_bytes()))
            body = user_data('0'*40, peer.sha(source.read_bytes()), 'sources/mock', PREFIX+'a0001', proof)
        for failure, original, phase in (('success', 0, 'complete'), ('scientific', 0 if offered else 1, 'complete'),
                ('native', 24, 'profile'), ('malformed', 1, 'profile'), ('apt', 42, 'apt-update'),
                ('no-python', 42, 'apt-update'), ('hash', 1, 'awscli-download'), ('abi', 23, 'binary-qualification'),
                ('upload', 0, 'complete'), ('terminal-upload', 0, 'complete')):
            work = base/failure
            work.mkdir()
            events, serial = work/'events', work/'serial'
            for name in ('aws', 'curl', 'python3'):
                (commands/name).unlink(missing_ok=True)
            if failure != 'no-python':
                (commands/'curl').symlink_to(curl)
                (commands/'python3').symlink_to(sys.executable)
            script = body.replace('/mnt/native-semantic-metadata-waves-cold', str(work))
            script = script.replace('/dev/ttyS0', str(serial)).replace('/usr/bin/time', str(commands/'time'))
            # The test process itself has a stricter hard limit than the worker.
            script = script.replace('ulimit -v 4194304', 'ulimit -v 204800')
            env = dict(os.environ, PATH=str(commands), FAIL=failure, WORKER_ROOT=str(work),
                EVENTS=str(events), STUBS=str(commands), INSTALLER=str(installer),
                INSTALLER_ZIP=str(zipped), AWS_TEMPLATE=str(aws), SOURCE_ARCHIVE=str(source))
            result = subprocess.run(['/bin/bash', '-c', script], env=env, capture_output=True, timeout=20)
            console, calls = serial.read_text(), events.read_text().splitlines()
            assert f'phase={phase} original_exit_code={original}' in console, (failure, console, result.stderr)
            assert calls.count('shutdown') == 1 and (work/'run-closed.log').is_file()
            successful = failure == 'success' or offered and failure == 'scientific'
            assert (result.returncode == 0) == successful, (failure, result.returncode)
            if failure == 'no-python':
                assert 'BORSUK_TERMINAL unavailable' in console and not (work/'terminal.json').exists()
                continue
            terminal = json.loads((work/'terminal.json').read_bytes())
            assert terminal['original_exit_code'] == original and terminal['exit_code'] == result.returncode
            assert terminal['status'] == ('complete' if successful else 'failed')
            if phase in ('apt-update', 'awscli-download', 'binary-qualification'):
                assert not any('scripts.run_native_semantic_metadata_cold' in c for c in calls)
            if phase == 'complete':
                assert set(terminal['artifacts']) == set(ARTIFACTS)
                assert sum(c.startswith('aws:s3 cp s3://') and ' binaries/' in c for c in calls) == 2
                assert sum(('--run-offered' if offered else 'scripts.run_native_semantic_metadata_cold') in c
                           and c.startswith('python:') for c in calls) == 1


def _offered_callback_self_check(config, qualification, authorities):
    """Run the shared conditional PUT callback with real role-bound cell bodies."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        for name in BOUND_FILES:
            (out/name).write_bytes(authorities[name])
        role = 'candidate'
        identity = dict(rate_index=0, dataset='CoHere', arm=role)
        bound = qualification['role_bindings'][role]
        rows = [dict(identity, query_ordinal=q, **bound) for q in range(64)]
        paths = dict(records=out/'rate0-cohere-candidate-records.jsonl', summary=out/'rate0-cohere-candidate-summary.json')
        paths['records'].write_bytes(b''.join(encoded(row)+b'\n' for row in rows))
        marker = dict(identity, **bound, closed=True, process_cleanup_complete=True, identity_gate_passed=True,
            bounded_memory_gate_passed=True, native_source_file_count=399, code_sha256=config['code_sha256'],
            qualification_sha256=config['native_arms'][role]['proof']['sha256'],
            records=semantic._identity(paths['records'].read_bytes()))
        paths['summary'].write_bytes(encoded(marker))
        argv = [str(CONFIG), qualification['config_sha256'],
            *(config['native_arms'][r][n]['path'] for r in runtime.ROLES for n in ('binary', 'proof')), str(out)]
        for failure in (None, 'records-upload', 'summary-upload', 'duplicate', 'identity', 'cleanup', 'hash'):
            uploads = []
            def put(command):
                uploads.append(command)
                assert command[:5] == ['aws', 's3api', 'put-object', '--if-none-match', '*']
                if failure == 'records-upload' or failure == 'summary-upload' and len(uploads) == 2:
                    raise RuntimeError('mock conditional PUT failure')
            def mocked(args, *, on_cell_closed):
                assert args == argv
                value = copy.deepcopy(marker)
                if failure == 'identity': value['proof_sha256'] = '0'*64
                if failure == 'cleanup': value['process_cleanup_complete'] = False
                if failure == 'hash': value['records']['sha256'] = '0'*64
                paths['summary'].write_bytes(encoded(value))
                on_cell_closed(value, paths)
                if failure == 'duplicate': on_cell_closed(value, paths)
                return 0
            with patch.object(runtime, 'main', side_effect=mocked), \
                    patch.object(semantic, '_check_checkpoint_cli'), patch.object(semantic, '_checkpoint_cli_call', side_effect=put):
                try:
                    status = _run_offered(argv, OFFERED_PREFIX+'a0001')
                except (AssertionError, RuntimeError):
                    assert failure is not None
                else:
                    assert failure is None and status == 0
            if failure is None:
                assert [Path(c[c.index('--body')+1]) for c in uploads] == [paths['records'], paths['summary']]
            if failure in ('identity', 'cleanup', 'hash'): assert not uploads
            if failure == 'records-upload': assert len(uploads) == 1
            if failure in ('summary-upload', 'duplicate'): assert len(uploads) == 2


def self_check(offered=False):
    """Actual pending refusal; synthetic full completion only in temporary mocks."""
    from contextlib import ExitStack, redirect_stdout
    import io
    import resource
    module = sys.modules[__name__]

    def rejected(call):
        try:
            call()
        except (AssertionError, ValueError, KeyError, TypeError, OSError):
            return
        raise AssertionError('invalid authority accepted')

    def ptr(path):
        return dict(path=str(path), bytes=Path(path).stat().st_size, sha256=runtime.old.sha(path))

    config = json.loads((ROOT/'paired-config.json').read_bytes())
    # Test actual committed pending evidence separately from synthetic positives.
    # After root freeze the same check also works against completed authority.
    pending = json.loads(runtime.read_bound(config['native_arms']['candidate']['proof']))['full_workspace_execution_pending']
    if pending:
        rejected(preflight)
        rejected(lambda: runtime.validate_config(config))
    evidence = runtime.authenticate(config)
    binaries = {r: config['native_arms'][r]['binary']['path'] for r in runtime.ROLES}
    proofs = {r: config['native_arms'][r]['proof']['path'] for r in runtime.ROLES}
    try:
        runtime.validate_runtime(config, binaries, proofs, evidence)
    except ValueError as error:
        assert pending and str(error) == 'completed qualified role proof', str(error)
    else:
        assert not pending, 'actual pending root proof accepted'
    config.update(runtime.OFFERED_FIXED if offered else runtime.FIXED)
    if offered:
        config.pop('blocks')
    config.update(schema=runtime.OFFERED_SCHEMA if offered else runtime.SCHEMA, authority_pending=False, controller_authority_pending=False,
        code_sha256={n: runtime.old.sha(n) for n in (runtime.OFFERED_CODE if offered else runtime.CODE)},
        controller_code_sha256={n: runtime.old.sha(n) for n in CODE})
    with tempfile.TemporaryDirectory() as tmp, patch.object(module, 'CONFIG', Path(tmp)/'paired-config.json'), \
            patch.object(semantic, '_required_glibc', return_value='2.38'), ExitStack() as mocks:
        def mock_body(name, body):
            path = Path(tmp)/'mock-authority'/name
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(body)
            return dict(ptr(path), path='mock-authority/'+name)

        # Mock only the missing full execution, never overwrite root authority.
        full_log = b'SYNTHETIC full execution: never a qualification claim\n'
        full_receipt = dict(exit_status=0, source_unchanged=True,
            source_sha256=evidence['manifests']['candidate']['source_sha256'],
            source_identity_sha256=runtime.SOURCE_IDS['candidate'], source_file_count=399,
            command=['cargo', 'test', '--workspace', '--all-targets', '--locked'],
            artifacts={'test.log': semantic._identity(full_log)})
        receipt_ptr = mock_body('full-receipt.json.gz', gzip.compress(encoded(full_receipt), mtime=0))
        assurance = json.loads(runtime.read_bound(config['native_assurances']['candidate']))
        assurance.update(qualified=True, full_workspace_test_execution=True, full_suite_status=0,
            full_workspace_receipt=receipt_ptr,
            full_workspace_log=mock_body('full-test.log.gz', gzip.compress(full_log, mtime=0)))
        config['native_assurances']['candidate'] = mock_body('native-assurance.json', encoded(assurance))
        proof = json.loads(runtime.read_bound(config['native_arms']['candidate']['proof']))
        proof.update(qualified=True, full_workspace_execution_pending=False, current_full_suite_pass_claim=True,
            full_suite_status=0, full_workspace_receipt=receipt_ptr,
            assurance_sha256=config['native_assurances']['candidate']['sha256'])
        config['native_arms']['candidate']['proof'] = mock_body('proof.json', encoded(proof))
        read, bound = semantic._read, runtime.read_bound

        def mock_read(base, pointer):
            return read(Path(tmp) if pointer['path'].startswith('mock-authority/') else base, pointer)

        def mock_bound(pointer, path=None):
            return bound(pointer, Path(tmp)/pointer['path'] if pointer['path'].startswith('mock-authority/') else path)

        # Temporary body routing still executes the production digest checks.
        mocks.enter_context(patch.object(semantic, '_read', side_effect=mock_read))
        mocks.enter_context(patch.object(runtime, 'read_bound', side_effect=mock_bound))
        CONFIG.write_bytes(encoded(config))
        qualification, authorities = _qualify()
        if offered:
            _offered_callback_self_check(config, qualification, authorities)
        assert set(qualification['code_sha256']) == set(CODE) and len(CODE) == (24 if offered else 23)
        assert len(ARTIFACTS) == len(set(ARTIFACTS)) == (110 if offered else 63)
        assert set(authorities) == set(BOUND_FILES) | set(AUTHORITY_FILES)
        for field, value in (('exit_status', False), ('exit_status', None), ('source_unchanged', False),
                ('source_sha256', {}), ('source_identity_sha256', '0'*64), ('source_file_count', 398),
                ('artifacts', {'test.log': semantic._identity(b'wrong log')}),
                ('command', full_receipt['command'] + ['--no-run'])):
            bad_receipt = dict(full_receipt, **{field: value})
            rejected(lambda: _full_receipt(encoded(bad_receipt), full_log,
                evidence['manifests']['candidate']['source_sha256'], runtime.SOURCE_IDS['candidate']))
        original_read = semantic._read
        def bad_verification(base, pointer):
            body = original_read(base, pointer)
            return body + b' ' if pointer['path'].endswith('final-slice/verification.json') else body
        with patch.object(semantic, '_read', side_effect=bad_verification):
            rejected(preflight)
        for role in runtime.ROLES:
            assert qualification['role_bindings'][role]['native_source_identity_sha256'] == runtime.SOURCE_IDS[role]
        for field, value in (('authority_pending', True), ('controller_authority_pending', True),
                ('controller_authority_pending', None), ('count', 63), ('count', True),
                ('ann_queries', 511), ('controller_code_sha256', {}), ('code_sha256', {}),
                ('machine_limit_seconds', 3601), ('compute_cap_usd', 1.), ('native_memory_bytes', 1073741824)):
            bad = copy.deepcopy(config)
            bad[field] = value
            CONFIG.write_bytes(encoded(bad))
            rejected(preflight)
        for role in runtime.ROLES:
            for field in ('source_manifest', 'proof', 'binary'):
                bad = copy.deepcopy(config)
                bad['native_arms'][role][field]['sha256'] = '0'*64
                CONFIG.write_bytes(encoded(bad))
                rejected(preflight)
        for field in ('checker_authority',):
            bad = copy.deepcopy(config)
            bad[field]['sha256'] = '0'*64
            CONFIG.write_bytes(encoded(bad))
            rejected(preflight)
        bad = copy.deepcopy(config)
        bad['items'][0]['arms']['control']['indexes']['10'] += '-changed'
        CONFIG.write_bytes(encoded(bad))
        rejected(preflight)
        bad['items'][0]['arms']['candidate'] = copy.deepcopy(bad['items'][0]['arms']['control'])
        CONFIG.write_bytes(encoded(bad))
        rejected(preflight)  # Identical arms still must be the original frozen semantic generation.
        CONFIG.write_bytes(encoded(config))
        with patch.object(semantic, 'source_hashes', return_value={}):
            rejected(preflight)
        original_read = semantic._read
        def tampered(base, pointer):
            body = original_read(base, pointer)
            return body + b' ' if pointer['path'].endswith('native-assurance.json') else body
        with patch.object(semantic, '_read', side_effect=tampered):
            rejected(preflight)
        # Runtime proof validator rejects pending and non-integer zero, without
        # relying solely on the controller's fixed completed-proof hashes.
        evidence = runtime.authenticate(config)
        for role in runtime.ROLES:
            for field, value in [('full_workspace_execution_pending', True), *[(n, False) for n in runtime.PROOF_GATES]]:
                proof = json.loads(runtime.read_bound(config['native_arms'][role]['proof']))
                proof[field] = value
                path = Path(tmp)/'bad-proof.json'
                path.write_bytes(encoded(proof))
                bad = copy.deepcopy(config)
                bad['native_arms'][role]['proof'] = ptr(path)
                rejected(lambda: runtime.validate_runtime(bad,
                    {r: config['native_arms'][r]['binary']['path'] for r in runtime.ROLES},
                    {r: bad['native_arms'][r]['proof']['path'] for r in runtime.ROLES}, evidence))
        body = user_data('0'*40, '1'*64, 'sources/mock', PREFIX+'a0001', qualification)
        assert body.count('aws s3 cp s3://'+peer.BUCKET+'/research/semantic-router/20261001/native/') == 2
        assert 'phase=publication' not in body and 'rustup' not in body and 'cargo ' not in body
        assert ('--run-offered' if offered else 'scripts.run_native_semantic_metadata_cold') in body and 'RuntimeMaxSec=3030' in body
        assert 'MemoryMax=8G' in body and 'MemorySwapMax=0' in body and 'ulimit -v 4194304' in body
        assert 'original_exit_code' in body and 'BORSUK_TERMINAL' in body
        assert body.index('--stage') < body.index('phase=profile')
        # Both original binaries, OS and dependency checks are independently bound.
        abi = dict(schema='borsuk-native-semantic-metadata-waves-runtime-abi-v1', qualified=True,
            os_release=semantic.RUNTIME_OS, architecture='x86_64', libc='glibc', glibc_version='2.39',
            binaries=runtime.BINARY_IDS, required_glibc=qualification['required_glibc'],
            ldd={r: dict(returncode=0, stdout='resolved', stderr='') for r in runtime.ROLES})
        _validate_runtime_abi(qualification, abi)
        for role in runtime.ROLES:
            bad = copy.deepcopy(abi)
            bad['ldd'][role]['stdout'] = 'undefined symbol: broken'
            rejected(lambda: _validate_runtime_abi(qualification, bad))
        # Exercise remote staging with the original binary bodies and authority
        # files; only loader inspection and host-OS observations are mocked.
        import shutil
        stage = Path(tmp)/'stage'
        stage.mkdir()
        (stage/'source-qualification.json').write_bytes(encoded(qualification))
        for role in runtime.ROLES:
            path = stage/'binaries'/role/'two_bit_http'
            path.parent.mkdir(parents=True)
            shutil.copyfile(config['native_arms'][role]['binary']['path'], path)
        def loader(args, **kwargs):
            assert args[:2] == ['ldd', '-r'] and kwargs['timeout'] == 15
            return subprocess.CompletedProcess(args, 0, 'resolved', '')
        with patch.object(platform, 'freedesktop_os_release', return_value=semantic.RUNTIME_OS), \
                patch.object(platform, 'machine', return_value='x86_64'), \
                patch.object(os, 'confstr', return_value='glibc 2.39'), \
                patch.object(subprocess, 'run', side_effect=loader) as ldd:
            _stage(Path('.'), stage)
            assert ldd.call_count == 2
            for name, authority_body in authorities.items():
                assert (stage/name).read_bytes() == authority_body
            bad = stage/'binaries/control/two_bit_http'
            with bad.open('ab') as stream:
                stream.write(b'tampered')
            ldd.reset_mock()
            rejected(lambda: _runtime_abi(stage))
            ldd.assert_not_called()
            assert json.loads((stage/'runtime-abi.json').read_bytes())['qualified'] is False
        _collection_self_check(qualification, authorities, abi, config, rejected)
        _bootstrap_self_check(qualification)
    with redirect_stdout(io.StringIO()):
        _lifecycle_self_check()
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    assert peak <= 200*1024**2, 'self-check RAM cap'
    print(f'paired waves controller PASS: code={len(CODE)} artifacts={len(ARTIFACTS)} peak_bytes={peak}; actual_pending_proof_rejected={pending}, full completion/AWS/native MOCKED')


if __name__ == '__main__':
    args = sys.argv[1:]
    offered = bool(args and args[0] == '--offered')
    if offered:
        args = args[1:]
    with offered_mode() if offered else nullcontext():
        if args == ['--self-check']:
            self_check(offered=offered)
        elif len(args) == 9 and args[0] == '--run-offered' and offered:
            raise SystemExit(_run_offered(args[2:], args[1]))
        elif len(args) == 3 and args[0] == '--stage':
            _stage(*args[1:])
        elif len(args) == 2 and args[0] in ('--runtime-abi', '--check-closed', '--replay'):
            result = {'--runtime-abi': _runtime_abi, '--check-closed': _check_closed, '--replay': replay}[args[0]](args[1])
            print(json.dumps(result, sort_keys=True))
        else:
            assert len(args) == 1, 'usage: [--offered] aNNNN | --self-check | --stage REPO OUT | --runtime-abi OUT | --replay OUT'
            with open('/tmp/borsuk-native-semantic-metadata-waves-cold-launch.lock', 'a+') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                main(args[0])
