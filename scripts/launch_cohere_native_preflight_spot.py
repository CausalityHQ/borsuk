"""Spot campaign adapter for the fixed Cohere D1024 100k/k10 native preflight helper.

Root freezes the config, admission and paid launch; this adapter never launches by itself.
Seam: launch_native_metadata_ranges_cold_spot.main(attempt, campaign=this module).
CLI: aNNNN | --self-check | --stage REPO OUT | --transport ROOT | --check-closed ROOT HELPER_EXIT | --retain ROOT PREFIX | --replay OUT.
Python only streams opaque bytes; it never decodes Parquet, vectors, truth, results or quality.
"""
import contextlib
import copy
import gzip
import hashlib
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
from urllib.parse import urljoin, urlsplit

if not __debug__:
    raise RuntimeError('qualification requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import check_native_workspace_execution as worker
from scripts import launch_native_semantic_router_cold_spot as semantic
from scripts import run_cohere_native_preflight as helper

shared, peer, startup = semantic.shared, semantic.peer, semantic.startup
ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/native-preflight-spot')
CONFIG = ROOT / 'config.json'
NAME = ''
SCHEMA = 'borsuk-cohere-native-preflight-spot-v1'
CONFIG_SCHEMA = 'borsuk-cohere-native-preflight-spot-config-v1'
PREFIX = 'research/semantic-router/20261006/cohere-native-preflight-'
TOKEN_PREFIX = 'cohere-native-preflight-'
TAG = 'borsuk-cohere-native-preflight'
MODULE = 'scripts.launch_cohere_native_preflight_spot'
WALL = 10800
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', semantic.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = semantic.ROOT_DEVICE_NAME, 'subnet-034528fbd6977848f'
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, 1.50
INSTANCE_ROOT = '/mnt/borsuk-cohere'  # layout of the root's pending helper config: input/en/*.parquet, bin/<role>
REVISION_URL = ('https://huggingface.co/datasets/' + helper.DATASET + '/resolve/' + helper.REVISION + '/')
HELPER_SLICE_MEMORY, TRANSFER_SECONDS = '8448M', 1800
RESOURCES = dict(helper_deadline_seconds=9600, machine_seconds=WALL, compute_cap_usd=1.5, ancillary_cap_usd=.15,
    build_cpu=4, build_memory_bytes=8 << 30, query_cpu=1, query_memory_bytes=512 << 20, swap_bytes=0, scratch_bytes=8 << 30)
CODE = tuple('scripts/' + name + '.py' for name in (
    'benchmark_with_resources', 'check_native_metadata_ranges_stats', 'check_native_semantic_router_stats',
    'check_native_startup_build', 'check_native_startup_stats', 'check_native_workspace_execution',
    'check_semantic_router_coverage', 'launch_cohere_native_preflight_spot', 'launch_native_metadata_ranges_cold_spot',
    'launch_native_peer_1m_spot', 'launch_native_semantic_router_cold_spot', 'launch_native_startup_profile_spot',
    'launch_v157_primary_feasibility_spot', 'launch_v174_relaid_bind_compile_spot', 'package_semantic_native_generation',
    'prepare_hierarchical_cells_100k', 'prepare_native_semantic_publication', 'prepare_semantic_positive_inputs',
    'rest_coexistence_load', 'run_cohere_native_preflight', 'run_hierarchical_global_leaf_probe',
    'run_native_cold_first_query', 'run_native_metadata_ranges_cold', 'run_native_peer_1m_worker',
    'run_native_peer_offered_http', 'run_native_semantic_router_cold', 'run_native_union_http',
    'run_native_union_offered_http', 'run_source_witness_paired_coverage'))
STAGE_FILES = tuple(f'preflight/{name}{suffix}' for name in helper.PHASES
                    for suffix in ('.log', '-closure.json', '-stage.json', '-stage-receipt.json', '-unit.log'))
ARTIFACTS = ('source-qualification.json', 'config.json', 'helper-config.json', 'transport-receipt.json', 'retention-receipt.json', 'cpu.txt',
    'helper.log', 'helper-resources.txt', 'run-closed.log',
    'preflight/config.json', 'preflight/admission.json', 'preflight/execution-receipt.json', 'preflight/terminal.json',
    'preflight/publish-receipt.json', 'preflight/prepared/complete.json', 'preflight/generation/manifest.json',
    'preflight/generation/plane/manifest.json', 'preflight/baseline-result.jsonl',
    *(f'preflight/configs/{name}.json' for name in helper.PHASES), *STAGE_FILES)
EMPTY_OK = ('preflight/publish.log',)  # the publisher prints nothing on success
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'campaign_schema', 'source_identity_sha256',
    'source_file_count', 'artifact_roster_sha256', 'helper_config_sha256', 'admission_sha256',
    'source_archive_paths_sha256', 'source_archive_file_count', 'awscli_version', 'awscli_sha256')
ARCHIVE_FIELDS = ('source_archive_paths', 'source_archive_paths_sha256', 'source_archive_file_count',
                  'source_archive_support_sha256')
SHARD_ORDER = tuple(name for name, _ in helper.SHARDS)
HF_HOST = 'huggingface.co'
MAX_REDIRECTS = 5
BUCKET_KEY = re.compile(r'research/[A-Za-z0-9._/-]{1,200}')
CONFIG_KEYS = ('schema controller_authority_pending controller_source_commit controller_code_sha256 '
               'source_identity_sha256 helper_config transports resources').split() + list(ARCHIVE_FIELDS)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def pin_fields(pin, names='path bytes sha256'):
    assert type(pin) is dict and set(pin) == set(names.split()), 'pin fields'
    assert type(pin['bytes']) is int and pin['bytes'] > 0 and re.fullmatch('[0-9a-f]{64}', pin['sha256']), 'pin identity'


def repo_path(base, name):
    path = Path(name)
    assert type(name) is str and not path.is_absolute() and '..' not in path.parts and str(path) == name, 'repo-relative path'
    return Path(base) / path


def instance_repo_path(base, name):
    """Map an absolute instance path under INSTANCE_ROOT/repo to the local checkout."""
    path, root = Path(name), Path(INSTANCE_ROOT) / 'repo'
    assert type(name) is str and path.is_absolute() and '..' not in path.parts and path.is_relative_to(root), 'instance repo path'
    return Path(base) / path.relative_to(root)


def in_instance(path, folder):
    path = Path(path)
    return path.is_absolute() and '..' not in path.parts and path.parent == Path(INSTANCE_ROOT) / folder


def checked_helper_config(base, config):
    pin = config['helper_config']
    pin_fields(pin)
    path = repo_path(base, pin['path'])
    assert worker.artifact(path) == {k: pin[k] for k in ('bytes', 'sha256')}, 'helper config authority'
    body = json.loads(path.read_bytes())
    assert body['schema'] == helper.SCHEMA + '-config' and body['source_identity_sha256'] == config['source_identity_sha256'], 'helper config schema/source identity'
    assert body['deadline_seconds'] <= helper.DEADLINE_MAX and all(
        body['phase_seconds'][k] <= v for k, v in helper.PHASE_MAX.items()), 'helper deadlines within the root proposal'
    assert set(body['binaries']) == set(helper.ROLES) and all(in_instance(body['binaries'][r]['path'], 'bin') for r in helper.ROLES), 'instance binary paths'
    assert [s['publisher_path'] for s in body['shards']] == list(SHARD_ORDER), 'ordered shards'
    assert all(in_instance(s['path'], 'input/en') for s in body['shards']), 'instance shard paths'
    assert [s['bytes'] for s in body['shards']] == [size for _, size in helper.SHARDS], 'pinned whole-shard lengths'
    return body


def checked_admission(base, hc):
    """Refuse unless the admission receipt is READY and consistent; helper.admit re-checks on the instance."""
    pin = hc['admission']
    local_file = instance_repo_path(base, pin['path'])
    assert worker.artifact(local_file) == {k: pin[k] for k in ('bytes', 'sha256')}, 'admission authority'
    admission = json.loads(local_file.read_bytes())
    assert admission.get('status') == 'READY_NATIVE_PREFLIGHT', 'admission not READY_NATIVE_PREFLIGHT'
    mapped = copy.deepcopy(admission)
    for gate in mapped['gates'].values():
        gate['receipt']['path'] = str(instance_repo_path(base, gate['receipt']['path']))
    with tempfile.TemporaryDirectory() as tmp:
        mapped_pin = helper.local.write_json(Path(tmp) / 'admission.json', mapped)
        helper.admit(dict(admission=mapped_pin, source_identity_sha256=hc['source_identity_sha256'], binaries=hc['binaries']))
    gate_paths = [instance_repo_path(base, g['receipt']['path']).relative_to(base).as_posix() for g in admission['gates'].values()]
    return admission, gate_paths


def checked_transports(config, hc):
    transports = config['transports']
    assert type(transports) is dict and set(transports) == {'shards', 'binaries', 'redirect_hosts'}, 'transport fields'
    hosts = transports['redirect_hosts']
    assert type(hosts) is list and len(hosts) <= 4 and hosts == sorted(set(hosts)) and all(
        type(h) is str and re.fullmatch(r'[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?', h) and '..' not in h and h != HF_HOST for h in hosts), 'exact publisher CDN host policy'
    assert set(transports['shards']) == set(SHARD_ORDER), 'two exact shard locators'
    for name, locator in transports['shards'].items():
        assert type(locator) is str and (locator == REVISION_URL + name or (
            locator.startswith('s3://' + peer.BUCKET + '/') and BUCKET_KEY.fullmatch(locator[len('s3://' + peer.BUCKET + '/'):]))), 'shard locator: ' + name
    assert set(transports['binaries']) == set(helper.ROLES), 'five binary role locators'
    assert all(type(k) is str and BUCKET_KEY.fullmatch(k) for k in transports['binaries'].values()), 'binary S3 keys'
    assert len(set(transports['binaries'].values())) == len(helper.ROLES), 'distinct binary keys'


def qualify(base=Path('.')):
    """Portable config/source/helper/admission qualification: no Git, no AWS, no native execution."""
    base = Path(base).resolve()
    body = (base / CONFIG).read_bytes()
    config = json.loads(body)
    assert type(config) is dict and set(config) == set(CONFIG_KEYS), 'exact preflight spot config'
    assert config['schema'] == CONFIG_SCHEMA, 'config schema'
    assert config['controller_authority_pending'] is False, 'root authority freeze pending'
    assert re.fullmatch('[0-9a-f]{40}', config['controller_source_commit']), 'frozen controller commit'
    assert all(type(config['resources'][k]) is type(v) and config['resources'][k] == v for k, v in RESOURCES.items()) \
        and set(config['resources']) == set(RESOURCES), 'fixed root resource proposal'
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive code roster'
    assert all(worker.artifact(base / name)['sha256'] == digest for name, digest in code.items()), 'controller code drift'
    inventory = worker.source_hashes(base)
    identity = worker.source_identity(inventory)
    assert identity == config['source_identity_sha256'] and len(inventory) > 0, 'full native source identity'
    hc = checked_helper_config(base, config)
    admission, gate_paths = checked_admission(base, hc)
    checked_transports(config, hc)
    paths = config['source_archive_paths']
    assert type(paths) is list and paths == sorted(set(paths)) and all(type(p) is str for p in paths), 'sorted archive roster'
    shared.validate_source_archive_paths(paths)
    assert config['source_archive_paths_sha256'] == peer.sha(json.dumps(paths, separators=(',', ':')).encode()), 'archive roster authentication'
    assert type(config['source_archive_file_count']) is int and config['source_archive_file_count'] == len(paths), 'archive count'
    admission_path = instance_repo_path(base, hc['admission']['path']).relative_to(base).as_posix()
    required = set(inventory) | set(CODE) | {str(CONFIG), config['helper_config']['path'], admission_path, *gate_paths}
    assert required <= set(paths), 'archive roster must contain source, code, helper config, admission and gate receipts'
    support = {name: worker.artifact(base / name)['sha256'] for name in paths if name not in inventory and name != str(CONFIG)}
    assert config['source_archive_support_sha256'] == support, 'archive support closure'
    return dict(schema='borsuk-cohere-native-preflight-spot-qualification-v1', config_path=str(CONFIG), config_sha256=sha(body),
        campaign_schema=SCHEMA, source_identity_sha256=identity, source_file_count=len(inventory), source_sha256=inventory,
        controller_source_commit=config['controller_source_commit'], code_sha256=code,
        code_identity_sha256=sha(encoded(code)), artifact_roster_sha256=sha(encoded(ARTIFACTS)),
        helper_config=config['helper_config'], helper_config_sha256=config['helper_config']['sha256'],
        admission_sha256=hc['admission']['sha256'], transports=config['transports'], resources=config['resources'],
        actual_native_preflight=False, awscli_version=semantic.AWSCLI_VERSION, awscli_sha256=semantic.AWSCLI_SHA256,
        **{key: config[key] for key in ARCHIVE_FIELDS})


def preflight(base=Path('.')):
    base = Path(base)
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    proof = qualify(base)
    parents = subprocess.check_output(['git', 'rev-list', '--parents', '-n', '1', 'HEAD'], cwd=base, text=True).split()
    assert len(parents) == 2, 'one source-bundle parent required'
    config_commit, controller = parents[1], proof['controller_source_commit']
    assert subprocess.check_output(['git', 'rev-list', '--parents', '-n', '1', config_commit], cwd=base, text=True).split() == [config_commit, controller], 'config immediately follows frozen controller'
    assert subprocess.check_output(['git', 'diff', '--no-renames', '--name-only', controller, config_commit],
        cwd=base, text=True).splitlines() == [str(CONFIG)], 'config-only authority freeze'
    assert subprocess.check_output(['git', 'for-each-ref', '--contains=' + config_commit, '--format=%(refname)',
        'refs/remotes/origin/'], cwd=base, text=True).strip(), 'controller/config commit not on an origin ref'
    for name, digest in proof['source_archive_support_sha256'].items():
        assert sha(subprocess.check_output(['git', 'show', 'HEAD:' + name], cwd=base)) == digest, 'committed support blob: ' + name
    return proof


def stage(repo, out):
    """Instance: re-qualify the authenticated archive, bind it to the deployed roster, retain authority copies."""
    repo, out = Path(repo).resolve(), Path(out).resolve()
    proof = qualify(repo)
    for key in ('source_archive_paths_sha256', 'source_archive_file_count'):
        env = 'BORSUK_COHERE_PREFLIGHT_' + key.upper()
        if env in os.environ:
            assert os.environ[env] == str(proof[key]), 'deployed archive roster binding'
    for name, body in (('source-qualification.json', encoded(proof) + b'\n'), ('config.json', (repo / CONFIG).read_bytes()),
                       ('helper-config.json', (repo / proof['helper_config']['path']).read_bytes())):
        with (out / name).open('xb') as stream:
            stream.write(body); stream.flush(); os.fsync(stream.fileno())
    return proof


def resolve(url, hosts):
    """Follow at most MAX_REDIRECTS HTTPS hops; every hop needs an exact allowed host, no userinfo, port None/443. Body-free HEAD probes."""
    allowed = {HF_HOST, *hosts}
    for _ in range(MAX_REDIRECTS + 1):
        parts = urlsplit(url)
        assert parts.scheme == 'https' and parts.hostname in allowed and '@' not in parts.netloc \
            and parts.username is None and parts.password is None and parts.port in (None, 443), 'redirect host/scheme/userinfo/port policy'
        headers = subprocess.run(['curl', '-sS', '--proto', '=https', '--connect-timeout', '10', '--max-time', '60',
            '--head', url], check=True, capture_output=True, text=True, timeout=90).stdout
        lines = headers.replace('\r', '').split('\n')
        status = int(lines[0].split()[1])
        if 300 <= status < 400:
            location = [l.split(':', 1)[1].strip() for l in lines if l.lower().startswith('location:')]
            assert len(location) == 1, 'single redirect location'
            url = urljoin(url, location[0])
            continue
        assert status == 200, 'final transport status'
        return url
    raise AssertionError('too many redirects')


def fetch(locator, destination, size, digest, hosts=()):
    """Bounded streaming transfer; exact length and SHA256 before atomic publication. Never decodes."""
    destination = Path(destination)
    part = destination.with_name(destination.name + '.part')
    assert not destination.exists() and not part.exists() and destination.parent.is_dir(), 'transport destination'
    try:
        if locator.startswith('https://'):
            command = ['curl', '-fsS', '--proto', '=https', '--max-redirs', '0', '--connect-timeout', '10',
                       '--max-time', str(TRANSFER_SECONDS), '--max-filesize', str(size), '--output', str(part), resolve(locator, hosts)]
        else:
            command = ['aws', 's3', 'cp', locator, str(part), '--only-show-errors']
        subprocess.run(command, check=True, timeout=TRANSFER_SECONDS + 60)
        metadata = os.lstat(part)
        assert os.path.isfile(part) and not os.path.islink(part) and metadata.st_size == size, 'exact transported length'
        hasher = hashlib.sha256()
        with part.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b''):
                hasher.update(chunk)
            os.fsync(stream.fileno())
        assert hasher.hexdigest() == digest, 'transported SHA256'
        os.rename(part, destination)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    helper.probe.fsync_dir(destination.parent)
    return dict(path=str(destination), bytes=size, sha256=digest)


def transport(root):
    root = Path(root)
    repo = root / 'repo'
    proof = qualify(repo)
    hc = json.loads((repo / proof['helper_config']['path']).read_bytes())
    receipt = dict(schema=SCHEMA + '-transport', transfers=[])
    for folder in ('input/en', 'bin'):
        (root / folder).mkdir(parents=True)
    place = lambda name: root / Path(name).relative_to(INSTANCE_ROOT)  # identity on the instance
    for shard in hc['shards']:
        locator = proof['transports']['shards'][shard['publisher_path']]
        receipt['transfers'].append(dict(kind='shard', locator=locator,
            **fetch(locator, place(shard['path']), shard['bytes'], shard['sha256'], proof['transports']['redirect_hosts'])))
    for role in helper.ROLES:
        pin = hc['binaries'][role]
        locator = 's3://' + peer.BUCKET + '/' + proof['transports']['binaries'][role]
        receipt['transfers'].append(dict(kind='binary', role=role, locator=locator, **fetch(locator, place(pin['path']), pin['bytes'], pin['sha256'])))
        os.chmod(place(pin['path']), 0o500)
    with (root / 'transport-receipt.json').open('xb') as stream:
        stream.write(encoded(receipt) + b'\n'); stream.flush(); os.fsync(stream.fileno())
    return receipt


RETAIN_FILES = ('normalized.f32', 'order.u64', 'sq8.bin')
RETAIN_PREFIXES = ('prepared/', 'generation/', 'store/')
RETAIN_FILE_CAP, RETAIN_TOTAL_CAP = 1 << 30, 4 << 30


def retained_names(inventory):
    """Large opaque bodies kept in S3 (cohort, queries, truth, IDs, normalized/order/SQ8, generation, store); small evidence is collected."""
    return sorted(name for name in inventory if (name in RETAIN_FILES or name.startswith(RETAIN_PREFIXES)) and 'preflight/' + name not in ARTIFACTS)


def retain(root, prefix):
    """Instance: stream bounded uploads of the helper's retained bodies, then verify S3 sizes; no decoding."""
    root = Path(root)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix), 'retention prefix'
    assert not (root / 'retention-receipt.json').exists(), 'retention receipt never overwritten'
    terminal = helper.local.read_json(helper.local.identity(root / 'preflight/terminal.json'), 8 << 20)
    names = retained_names(terminal['inventory'])
    assert sum(terminal['inventory'][n]['bytes'] for n in names) <= RETAIN_TOTAL_CAP and all(
        terminal['inventory'][n]['bytes'] <= RETAIN_FILE_CAP for n in names), 'bounded retention'
    receipt = dict(schema=SCHEMA + '-retention', bucket=peer.BUCKET, prefix=prefix, objects=[])
    for name in names:
        pin, key = terminal['inventory'][name], prefix + '/retained/' + name
        path = root / 'preflight' / name
        assert os.lstat(path).st_size == pin['bytes'], 'retained body length'
        subprocess.run(['aws', 's3', 'cp', str(path), 's3://' + peer.BUCKET + '/' + key, '--metadata', 'sha256=' + pin['sha256'],
                        '--only-show-errors'], check=True, timeout=TRANSFER_SECONDS)
        size = subprocess.run(['aws', 's3api', 'head-object', '--bucket', peer.BUCKET, '--key', key, '--query', 'ContentLength',
                               '--output', 'text'], check=True, capture_output=True, text=True, timeout=120).stdout.strip()
        assert size == str(pin['bytes']), 'uploaded object length'
        receipt['objects'].append(dict(key=key, path=name, bytes=pin['bytes'], sha256=pin['sha256']))
    with (root / 'retention-receipt.json').open('xb') as stream:
        stream.write(encoded(receipt) + b'\n'); stream.flush(); os.fsync(stream.fileno())
    return receipt


def check_closed(root, helper_exit):
    """Authenticate the helper's terminal-last inventory and map its exit status; no quality is read."""
    out = Path(root) / 'preflight'
    terminal = helper.local.read_json(helper.local.identity(out / 'terminal.json'), 8 << 20)
    assert terminal['schema'] == helper.SCHEMA + '-terminal', 'helper terminal schema'
    assert terminal['status'] in ('NATIVE_CHAIN_CLOSED', 'BASELINE_NONZERO_EXIT', 'INVALID'), 'helper terminal status'
    assert terminal['inventory'] == helper.inventory(out), 'exact terminal-last helper inventory'
    assert terminal['execution_receipt'] == terminal['inventory']['execution-receipt.json'], 'terminal receipt pin'
    closed = terminal['status'] == 'NATIVE_CHAIN_CLOSED'
    assert terminal['execution_exit_code'] == (0 if closed else 2) and type(helper_exit) is int, 'helper exit mapping'
    assert helper_exit == terminal['execution_exit_code'] and terminal['complete'] is (terminal['status'] != 'INVALID'), 'helper exit/complete'
    return dict(status=terminal['status'], complete=terminal['complete'], execution_exit_code=terminal['execution_exit_code'])


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', prefix)
    assert qualification['campaign_schema'] == SCHEMA and qualification['config_path'] == str(CONFIG)
    adapter = {key: qualification[key] for key in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key': 'unused'}, native_publisher={'key': 'unused'})
    with patch.multiple(semantic, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), \
            patch.object(semantic, '_offered', return_value=False):
        body = semantic.user_data(commit, archive_sha, archive_key, prefix, adapter)
    slice_name = 'borsuk-global-leaf-' + prefix[-5:] + '.slice'
    hconfig = qualification['helper_config']
    bindings = ' '.join('BORSUK_COHERE_PREFLIGHT_' + key.upper() + '=' + str(qualification[key])
                        for key in ('source_archive_paths_sha256', 'source_archive_file_count'))
    command = f'''phase=install
python3.12 -c 'import boto3'
lscpu >cpu.txt
test "$(uname -m)" = x86_64
phase=source-qualification
{bindings} PYTHONPATH="$root/repo" python3.12 -m {MODULE} --stage "$root/repo" "$root"
phase=transport
PYTHONPATH="$root/repo" python3.12 -m {MODULE} --transport "$root"
phase=preflight
systemctl start {slice_name}
systemctl set-property --runtime {slice_name} MemoryMax={HELPER_SLICE_MEMORY} MemorySwapMax=0 CPUQuota=400% TasksMax=512
set +e
systemd-run --slice={slice_name} --unit=cohere-native-preflight --wait --pipe -p MemoryMax={HELPER_SLICE_MEMORY} -p MemorySwapMax=0 -p CPUQuota=400% -p TasksMax=512 -p RuntimeMaxSec=9660 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=BORSUK_GLOBAL_LEAF_SLICE={slice_name} \\
 /usr/bin/time -v -o "$root/helper-resources.txt" timeout --signal=TERM --kill-after=30 9630 \\
 python3.12 scripts/run_cohere_native_preflight.py {INSTANCE_ROOT}/repo/{hconfig['path']} {hconfig['sha256']} "$root/preflight" >helper.log 2>&1
helper_code=$?
set -e
phase=check-closed
if ! PYTHONPATH="$root/repo" python3.12 -m {MODULE} --check-closed "$root" "$helper_code"; then
  if [ "$helper_code" = 0 ]; then helper_code=96; fi
  exit "$helper_code"
fi
phase=retain
PYTHONPATH="$root/repo" python3.12 -m {MODULE} --retain "$root" {prefix} || {{ if [ "$helper_code" = 0 ]; then helper_code=96; fi; exit "$helper_code"; }}
test "$helper_code" = 0 || exit "$helper_code"
for name in $ARTIFACT_NAMES; do
  case "$name" in
    run-closed.log) test -s "$root/run.log";;
    {'|'.join(EMPTY_OK)}|preflight/*-unit.log) test -f "$root/$name";;
    *) test -s "$root/$name";;
  esac
done
'''
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    body = body.replace('phase=complete\n', 'phase=complete\nexit "$helper_code"\n', 1)
    body = body.replace('  trap - EXIT TERM\n', f'  trap - EXIT TERM\n  systemctl stop {slice_name} 2>/dev/null || true\n', 1)
    body = body.replace('/mnt/native-semantic-router-cold', INSTANCE_ROOT)
    marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    assert body.count(marker) == 1
    body = body.replace(marker, marker + "'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),")
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384, 'EC2 user data limit'
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def replay(out):
    """Offline authentication of the collected evidence; reads statuses only, never quality bytes."""
    out = Path(out)
    reservation = json.loads((out / 'aws-reservation.json').read_bytes())
    closed = json.loads((out / 'aws-closeout.json').read_bytes())
    terminal = json.loads((out / 'aws-terminal.json').read_bytes())
    proof = reservation['qualification']
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA, 'replay mode'
    assert closed['state'] == 'terminated'
    assert terminal['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key in ('source_commit', 'source_archive_sha256'):
        assert terminal[key] == reservation[key], 'campaign source binding'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal ' + key
    assert type(terminal['source_archive_file_count']) is int, 'terminal archive count type'
    assert set(proof['code_sha256']) == set(CODE) and proof['code_identity_sha256'] == sha(encoded(proof['code_sha256']))
    base = Path(__file__).resolve().parents[1]
    assert all(worker.artifact(base / name)['sha256'] == digest for name, digest in proof['code_sha256'].items())
    assert proof['artifact_roster_sha256'] == sha(encoded(ARTIFACTS))
    assert type(proof['source_file_count']) is int and len(proof['source_sha256']) == proof['source_file_count'] > 0
    assert worker.source_identity(proof['source_sha256']) == proof['source_identity_sha256']
    assert all(type(proof['resources'][k]) is type(v) and proof['resources'][k] == v for k, v in RESOURCES.items())
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        assert worker.artifact(out / name) == identity, 'terminal artifact: ' + name
    for name, key in (('config.json', 'config_sha256'), ('helper-config.json', 'helper_config_sha256')):
        if name in terminal['artifacts']:
            assert worker.artifact(out / name)['sha256'] == proof[key], 'saved authority: ' + name
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out / 'source-qualification.json').read_bytes()) == proof
    assert terminal['source_qualification_sha256'] == terminal['artifacts'].get('source-qualification.json', {}).get('sha256')
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['exit_code'] == 0 and terminal['phase'] == 'complete'
    assert terminal['status'] == ('complete' if complete else 'failed')
    result = dict(complete=complete, exit_status=terminal['original_exit_code'], helper_status=None, claims_quality=False)
    if 'preflight/terminal.json' in terminal['artifacts']:
        helper_terminal = json.loads((out / 'preflight/terminal.json').read_bytes())
        assert helper_terminal['schema'] == helper.SCHEMA + '-terminal'
        retained = {name[len('preflight/'):]: identity for name, identity in terminal['artifacts'].items()
                    if name.startswith('preflight/') and name != 'preflight/terminal.json'}
        for name, identity in retained.items():
            assert {k: helper_terminal['inventory'][name][k] for k in ('bytes', 'sha256')} == identity, 'helper inventory: ' + name
        result['helper_status'] = helper_terminal['status']
        assert terminal['original_exit_code'] == helper_terminal['execution_exit_code'] or not complete
    if complete:
        assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(ARTIFACTS), 'exact completed artifact roster'
        assert result['helper_status'] == 'NATIVE_CHAIN_CLOSED'
    if 'retention-receipt.json' in terminal['artifacts']:
        retention = json.loads((out / 'retention-receipt.json').read_bytes())
        assert retention['schema'] == SCHEMA + '-retention' and retention['bucket'] == peer.BUCKET
        assert re.fullmatch(re.escape(PREFIX) + r'a[0-9]{4}', retention['prefix']), 'retention prefix'
        if (out / 'aws-launch.json').exists():
            assert json.loads((out / 'aws-launch.json').read_bytes())['prefix'] == retention['prefix'], 'retention launch prefix'
        inventory = helper_terminal['inventory']
        if complete:
            assert [o['path'] for o in retention['objects']] == retained_names(inventory), 'complete run retains every body'
        for item in retention['objects']:
            assert item['key'] == retention['prefix'] + '/retained/' + item['path'], 'retained key'
            assert {k: inventory[item['path']][k] for k in ('bytes', 'sha256')} == {k: item[k] for k in ('bytes', 'sha256')}, 'retained inventory identity'
        result['retained_objects'] = len(retention['objects'])
    elif complete:
        raise AssertionError('completed run requires the retention receipt')
    return result


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    closed = json.loads((out / 'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix + '/terminal.json')['Body'].read()
    (out / 'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        source = s3.get_object(Bucket=peer.BUCKET, Key=prefix + '/artifacts/' + name)['Body']
        (out / name).parent.mkdir(parents=True, exist_ok=True)
        with (out / name).open('wb') as output, gzip.GzipFile(filename=str(out / (name + '.gz')), mode='wb', mtime=0) as archived:
            for chunk in iter(lambda: source.read(1024 * 1024), b''):
                output.write(chunk); archived.write(chunk)
        assert worker.artifact(out / name) == identity, 'downloaded artifact: ' + name
    result = replay(out)
    if 'retention-receipt.json' in terminal['artifacts']:
        for item in json.loads((out / 'retention-receipt.json').read_bytes())['objects']:
            head = s3.head_object(Bucket=peer.BUCKET, Key=item['key'])
            assert head['ContentLength'] == item['bytes'] and head['Metadata']['sha256'] == item['sha256'], 'retained S3 object: ' + item['key']
    (out / 'collection-replay.json').write_bytes(encoded(dict(terminal_sha256=sha(raw), result=result)) + b'\n')
    return terminal


def main(attempt):
    return shared.main(attempt, campaign=sys.modules[__name__])


def rejected(call):
    try:
        call()
    except (AssertionError, ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError):
        return
    raise AssertionError('negative accepted')


def _build(root, label='base', *, config_edit=None, helper_edit=None, admission_edit=None, drift=None, shard_sizes=(6, 6)):
    """Synthetic repo at root/label/repo: real controller code, tiny native source, frozen configs."""
    real = Path(__file__).resolve().parents[1]
    base = Path(root) / label / 'repo'
    def write(name, data):
        path = base / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return dict(path=name, **worker.artifact(path))
    for name in CODE:
        write(name, (real / name).read_bytes())
    write('crates/borsuk/src/lib.rs', b'// native fixture\n'); write('Cargo.toml', b'[workspace]\n')
    inventory = worker.source_hashes(base)
    identity = worker.source_identity(inventory)
    binaries = {role: dict(path=f'{INSTANCE_ROOT}/bin/{role}', bytes=len(role.encode()), sha256=sha(role.encode())) for role in helper.ROLES}
    gates = {}
    for gate, (session, source, stages) in dict(native_publisher=(80402, identity, 6), sq8_builder=(89644, 'b' * 64, 1),
                                                 generation_builder=(12009, 'b' * 64, 6)).items():
        pin = write(str(ROOT / 'gates' / (gate + '.json')), encoded(dict(synthetic_gate=gate)))
        gates[gate] = dict(session=session, instance_id='i-' + gate.replace('_', '-'), source_identity_sha256=source, stage_count=stages,
            stage_exit_statuses=[0] * stages, terminated=True, receipt=dict(pin, path=f'{INSTANCE_ROOT}/repo/{pin["path"]}'))
    admission = dict(schema=helper.SCHEMA + '-admission', status='READY_NATIVE_PREFLIGHT', source_identity_sha256=identity,
        prior_source=dict(identity_sha256='b' * 64, relevant_source_unchanged=True), gates=gates,
        roles={role: dict(gate=helper.GATE_OF[role], binary=dict(path='/original/' + role, bytes=pin['bytes'], sha256=pin['sha256'], local_basename=role))
               for role, pin in binaries.items()})
    if admission_edit:
        admission_edit(admission)
    adm = write(str(ROOT / 'admission.json'), encoded(admission))
    hc = dict(schema=helper.SCHEMA + '-config', run_id='synthetic-a0001', source_identity_sha256=identity,
        admission=dict(adm, path=f'{INSTANCE_ROOT}/repo/{adm["path"]}'), binaries=binaries,
        shards=[dict(publisher_path=name, path=f'{INSTANCE_ROOT}/input/en/s{i}.parquet', bytes=size, sha256=sha(b'shard%d' % i))
                for i, ((name, _), size) in enumerate(zip(helper.SHARDS, shard_sizes))],
        phase_seconds=dict(helper.PHASE_MAX), deadline_seconds=helper.DEADLINE_MAX)
    if helper_edit:
        helper_edit(hc)
    hpin = write(str(ROOT / 'helper-config.json'), encoded(hc))
    config = dict(schema=CONFIG_SCHEMA, controller_authority_pending=False, controller_source_commit='1' * 40,
        controller_code_sha256={name: worker.artifact(base / name)['sha256'] for name in CODE}, source_identity_sha256=identity,
        helper_config=hpin, resources=copy.deepcopy(RESOURCES),
        transports=dict(shards={name: REVISION_URL + name for name, _ in helper.SHARDS}, binaries={role: 'research/test/' + role for role in helper.ROLES}, redirect_hosts=['cas-bridge.xethub.hf.co']))
    paths = sorted(set(inventory) | set(CODE) | {str(CONFIG), hpin['path'], adm['path'], *(g['receipt']['path'][len(INSTANCE_ROOT + '/repo/'):] for g in gates.values())})
    config.update(source_archive_paths=paths, source_archive_paths_sha256=peer.sha(json.dumps(paths, separators=(',', ':')).encode()),
        source_archive_file_count=len(paths),
        source_archive_support_sha256={n: worker.artifact(base / n)['sha256'] for n in paths if n not in inventory and n != str(CONFIG)})
    if config_edit:
        config_edit(config)
    write(str(CONFIG), json.dumps(config).encode())
    if drift:
        drift(base)
    return base


def _transport_self_check(root, base):
    from unittest.mock import patch as mock_patch
    module = sys.modules[__name__]
    proof = qualify(base)
    hc = json.loads((base / proof['helper_config']['path']).read_bytes())
    bodies = {proof['transports']['shards'][s['publisher_path']]: b'shard%d' % i for i, s in enumerate(hc['shards'])}
    for role, pin in hc['binaries'].items():
        bodies['s3://' + peer.BUCKET + '/' + proof['transports']['binaries'][role]] = role.encode()
    first = REVISION_URL + helper.SHARDS[0][0]
    cdn = 'https://cas-bridge.xethub.hf.co/object?sig=1'
    ok = 'HTTP/2 200\r\ncontent-length: 6\r\n\r\n'
    redirect = lambda target: 'HTTP/2 302\r\nLocation: ' + target + '\r\n\r\n'
    def attempt(label, *, wrong=None, short=None, long=None, occupied=False, expect_ok=True, routes=None, hops=None):
        inst = Path(root) / label
        inst.mkdir(parents=True)
        (inst / 'repo').symlink_to(base)
        calls = []
        def fake_run(command, **kwargs):
            if command[0] == 'curl' and '--head' in command:  # body-free HEAD probe; never a Range/GET probe
                calls.append(('probe', command[-1]))
                assert '=https' in command and '--proto' in command and '-r' not in command and '--range' not in command and '-o' not in command
                return subprocess.CompletedProcess(command, 0, stdout=(routes or {}).get(command[-1], ok))
            locator = command[-1] if command[0] == 'curl' else command[3]
            output = command[command.index('--output') + 1] if command[0] == 'curl' else command[4]
            calls.append((command[0], locator))
            if command[0] == 'curl':
                assert '--max-redirs' in command and command[command.index('--max-redirs') + 1] == '0' and '--max-filesize' in command
            origin = first if locator.startswith('https://cas-bridge.xethub.hf.co') else locator
            data = bodies[origin]
            data = b'tampered' + data[8:] if origin == wrong else data[:-1] if origin == short else data + b'x' if origin == long else data
            Path(output).write_bytes(data)
            return subprocess.CompletedProcess(command, 0)
        if occupied:
            (inst / 'input/en').mkdir(parents=True); (inst / 'input/en/s0.parquet').write_bytes(b'x')
        with mock_patch.object(module.subprocess, 'run', fake_run):
            if expect_ok:
                receipt = transport(inst)
                assert [t['kind'] for t in receipt['transfers']] == ['shard'] * 2 + ['binary'] * 5
                for item in receipt['transfers']:
                    assert worker.artifact(item['path']) == dict(bytes=item['bytes'], sha256=item['sha256'])
                assert all((inst / 'bin' / r).stat().st_mode & 0o777 == 0o500 for r in helper.ROLES)
                assert json.loads((inst / 'transport-receipt.json').read_bytes()) == receipt
                assert not list(inst.rglob('*.part'))
            else:
                rejected(lambda: transport(inst))
                assert not list(inst.rglob('*.part')), 'partial transport removed'
                assert not (inst / 'transport-receipt.json').exists()
        return inst, calls
    _, calls = attempt('ok')
    assert [c[0] for c in calls if c[0] != 'probe'] == ['curl'] * 2 + ['aws'] * 5
    # Pinned initial revision URL may redirect to the exact root-allowed publisher CDN host.
    _, calls = attempt('cdn', routes={first: redirect(cdn), cdn: ok})
    assert ('curl', cdn) in calls and ('probe', cdn) in calls
    # Refused before any body: other host, plain http, bad scheme, host suffix tricks, loops, 403, ambiguous Location.
    _, calls = attempt('port443', routes={first: redirect('https://cas-bridge.xethub.hf.co:443/object'), 'https://cas-bridge.xethub.hf.co:443/object': ok})
    assert ('curl', 'https://cas-bridge.xethub.hf.co:443/object') in calls
    for label, routes in (('other-host', {first: redirect('https://evil.example/x')}),
                          ('port', {first: redirect('https://cas-bridge.xethub.hf.co:8443/x')}),
                          ('port-zero', {first: redirect('https://cas-bridge.xethub.hf.co:0/x')}),
                          ('port-invalid', {first: redirect('https://cas-bridge.xethub.hf.co:abc/x')}),
                          ('userpass', {first: redirect('https://user:secret@cas-bridge.xethub.hf.co/x')}),
                          ('user-only', {first: redirect('https://user@cas-bridge.xethub.hf.co/x')}),
                          ('empty-userinfo', {first: redirect('https://@cas-bridge.xethub.hf.co/x')}),
                          ('http', {first: redirect('http://cas-bridge.xethub.hf.co/x')}),
                          ('suffix', {first: redirect('https://cas-bridge.xethub.hf.co.evil.example/x')}),
                          ('userinfo', {first: redirect('https://cas-bridge.xethub.hf.co@evil.example/x')}),
                          ('file', {first: redirect('file:///etc/passwd')}),
                          ('loop', {first: redirect(first)}),
                          ('forbidden', {first: 'HTTP/2 403\r\n\r\n'}),
                          ('two-locations', {first: 'HTTP/2 302\r\nLocation: ' + cdn + '\r\nLocation: ' + cdn + '\r\n\r\n'})):
        _, calls = attempt(label, routes=routes, expect_ok=False)
        assert all(c[0] == 'probe' for c in calls), (label, 'no body fetched after a policy refusal')
        if label in ('other-host', 'port', 'port-zero', 'port-invalid', 'userpass', 'user-only', 'empty-userinfo', 'http', 'suffix', 'userinfo', 'file'):
            assert calls == [('probe', first)], (label, 'the offending hop is never probed or fetched')
    attempt('wrong-shard', wrong=first, expect_ok=False)
    attempt('short-shard', short=first, expect_ok=False)
    attempt('long-shard', long=first, expect_ok=False)
    binary = 's3://' + peer.BUCKET + '/research/test/' + helper.ROLES[2]
    _, calls = attempt('wrong-binary', wrong=binary, expect_ok=False)
    assert [c[0] for c in calls if c[0] != 'probe'] == ['curl'] * 2 + ['aws'] * 3, 'transport stops at the first bad body'
    attempt('short-binary', short=binary, expect_ok=False)
    _, calls = attempt('occupied', occupied=True, expect_ok=False)
    assert calls == [], 'occupied destination refused before any transfer'
    # fetch() itself refuses an existing destination or stale partial before spawning any downloader.
    occupied = Path(root) / 'fetch-occupied'
    occupied.mkdir()
    (occupied / 'body').write_bytes(b'x')
    (occupied / 'other.part').write_bytes(b'x')
    with mock_patch.object(module.subprocess, 'run', side_effect=AssertionError('downloader spawned')):
        rejected(lambda: fetch(first, occupied / 'body', 6, sha(b'shard0')))
        rejected(lambda: fetch(first, occupied / 'other', 6, sha(b'shard0')))
        rejected(lambda: fetch(first, occupied / 'missing-parent' / 'body', 6, sha(b'shard0')))


def _closure_tree(directory, status, complete, *, extra=None):
    """A real helper terminal-last tree for check_closed/replay mocks."""
    out = Path(directory)
    for name in ARTIFACTS:
        if name.startswith('preflight/') and name not in ('preflight/execution-receipt.json', 'preflight/terminal.json'):
            path = out / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'' if name in EMPTY_OK else ('fixture ' + name + '\n').encode())
    for name in ('prepared/corpus.f32', 'prepared/queries.f32', 'prepared/truth.u64', 'prepared/corpus.ids.jsonl', 'prepared/queries.ids.jsonl',
                 'normalized.f32', 'order.u64', 'sq8.bin', 'store/semantic/index/head.json', 'store/semantic/objects/' + 'e' * 64,
                 'generation/canonical.bin', 'scratch/ignored.bin'):
        path = out / 'preflight' / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(('opaque ' + name + '\n').encode() * 3)
    return helper.finish(out / 'preflight', dict(status=status, complete=complete, config=None, stages=[]))


def _bootstrap_self_check(base):
    proof = qualify(base)
    prefix = PREFIX + 'a0001'
    body = user_data('0' * 40, '1' * 64, 'research/native-library-check/sources/' + '1' * 64 + '.tar.gz', prefix, proof)
    slice_name = 'borsuk-global-leaf-a0001.slice'
    for needle in (f'systemctl start {slice_name}', f'--slice={slice_name}', f'--setenv=BORSUK_GLOBAL_LEAF_SLICE={slice_name}',
            '-p MemorySwapMax=0', '-p CPUQuota=400%', '-p RuntimeMaxSec=9660', 'timeout --signal=TERM --kill-after=30 9630',
            f'python3.12 scripts/run_cohere_native_preflight.py {INSTANCE_ROOT}/repo/{proof["helper_config"]["path"]} {proof["helper_config_sha256"]} "$root/preflight"',
            f'-m {MODULE} --stage', f'-m {MODULE} --transport "$root"', f'-m {MODULE} --retain "$root" {prefix}', f'-m {MODULE} --check-closed "$root" "$helper_code"',
            f'systemctl stop {slice_name} 2>/dev/null || true', 'BORSUK_COHERE_PREFLIGHT_SOURCE_ARCHIVE_PATHS_SHA256=' + proof['source_archive_paths_sha256'],
            'exit "$helper_code"', INSTANCE_ROOT):
        assert needle in body, needle
    assert 'cargo' not in body.lower() and 'rustup' not in body and '/mnt/native-semantic-router-cold' not in body
    assert body.index('--transport') < body.index('systemd-run --slice') < body.index('--check-closed') < body.index('--retain')
    return proof, body


def _collection_self_check(root, proof, body):
    from unittest.mock import Mock
    terminal_script = body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0]
    for label, status, complete, code in (('closed', 'NATIVE_CHAIN_CLOSED', True, 0), ('nonzero', 'BASELINE_NONZERO_EXIT', True, 2), ('invalid', 'INVALID', False, 2)):
        out = Path(root) / ('collect-' + label)
        out.mkdir()
        helper_terminal = _closure_tree(out, status, complete)
        assert helper_terminal['execution_exit_code'] == code
        assert check_closed(out, code) == dict(status=status, complete=complete, execution_exit_code=code)
        rejected(lambda: check_closed(out, 1 if code == 0 else 0))
        for name in ARTIFACTS:
            if not name.startswith('preflight/'):
                (out / name).parent.mkdir(parents=True, exist_ok=True)
                (out / name).write_bytes(encoded(proof) + b'\n' if name == 'source-qualification.json' else b'artifact ' + name.encode() + b'\n')
        if status != 'INVALID':
            names = retained_names(helper_terminal['inventory'])
            assert 'scratch/ignored.bin' not in names and 'prepared/truth.u64' in names and 'sq8.bin' in names and len(names) == 11
            (out / 'retention-receipt.json').write_bytes(encoded(dict(schema=SCHEMA + '-retention', bucket=peer.BUCKET,
                prefix=PREFIX + 'a0001', objects=[dict(key=PREFIX + 'a0001/retained/' + n, path=n,
                    bytes=helper_terminal['inventory'][n]['bytes'], sha256=helper_terminal['inventory'][n]['sha256']) for n in names])) + b'\n')
        # Authority copies are exact: config.json must be the frozen spot config SHA.
        config_body = (Path(root) / 'base/repo' / CONFIG).read_bytes()
        (out / 'config.json').write_bytes(config_body)
        (out / 'helper-config.json').write_bytes((Path(root) / 'base/repo' / proof['helper_config']['path']).read_bytes())
        files = {name: (out / name).read_bytes() for name in ARTIFACTS if (out / name).exists()}
        if not complete:
            files = {n: d for n, d in files.items() if n in ('source-qualification.json', 'config.json', 'helper-config.json', 'run-closed.log')
                     or n.startswith('preflight/') and n in ('preflight/execution-receipt.json', 'preflight/terminal.json')}
        env = dict(os.environ, INSTANCE_ID='i-owned', EXIT_CODE=str(code), ORIGINAL_EXIT_CODE=str(code),
                   PHASE='complete' if code == 0 else 'check-closed', ARTIFACT_NAMES=' '.join(ARTIFACTS))
        for name in ARTIFACTS:  # the instance only keeps what was produced
            if name not in files:
                (out / name).unlink(missing_ok=True)
        terminal = json.loads(subprocess.check_output([sys.executable, '-c', terminal_script], cwd=out, env=env))
        assert set(terminal['artifacts']) == set(files)
        reservation = dict(schema=SCHEMA, qualification=proof, source_commit='0' * 40, source_archive_sha256='1' * 64)
        (out / 'aws-reservation.json').write_bytes(encoded(reservation))
        (out / 'aws-closeout.json').write_bytes(encoded(dict(state='terminated', nodes={'0': dict(instance_id='i-owned')})))
        store = {PREFIX + 'a0001/terminal.json': encoded(terminal), **{PREFIX + 'a0001/artifacts/' + n: d for n, d in files.items()}}
        s3 = Mock()
        s3.get_object.side_effect = lambda **kw: {'Body': io.BytesIO(store[kw['Key']])}
        retained = {}
        if 'retention-receipt.json' in files:
            retained = {o['key']: dict(ContentLength=o['bytes'], Metadata=dict(sha256=o['sha256']))
                        for o in json.loads(files['retention-receipt.json'])['objects']}
        s3.head_object.side_effect = lambda **kw: retained[kw['Key']]
        closeout = (out / 'aws-closeout.json').read_bytes()
        for failed in (dict(state='running', nodes={'0': dict(instance_id='i-owned')}), dict(state='terminated', nodes={'0': dict(instance_id='i-other')})):
            (out / 'aws-closeout.json').write_bytes(encoded(failed))
            rejected(lambda: collect(s3, PREFIX + 'a0001', out, 'i-owned', '0' * 40, '1' * 64))
        (out / 'aws-closeout.json').write_bytes(closeout)
        collect(s3, PREFIX + 'a0001', out, 'i-owned', '0' * 40, '1' * 64)
        result = replay(out)
        assert result['helper_status'] == status and result['claims_quality'] is False and result['exit_status'] == code
        assert result['complete'] is (code == 0 and complete)
        for name, data in files.items():
            assert gzip.decompress((out / (name + '.gz')).read_bytes()) == data
        assert result.get('retained_objects') == (11 if status != 'INVALID' else None)
        if retained:
            assert s3.head_object.call_count == 11
            key = next(iter(retained))
            for bad in (dict(retained[key], ContentLength=1), dict(retained[key], Metadata=dict(sha256='0' * 64))):
                original = retained[key]
                retained[key] = bad
                rejected(lambda: collect(s3, PREFIX + 'a0001', out, 'i-owned', '0' * 40, '1' * 64))
                retained[key] = original
        if label == 'closed':
            assert set(files) == set(ARTIFACTS), 'exact completed roster'
            assert files['preflight/publish.log'] == b'', 'empty publisher log retained and authenticated'
            for changed in (dict(terminal, config_sha256='0' * 64), dict(terminal, instance_id='i-other'), dict(terminal, source_archive_file_count=True),
                            dict(terminal, source_archive_paths_sha256='0' * 64), dict(terminal, exit_code=False),
                            dict(terminal, artifacts={n: v for n, v in terminal['artifacts'].items() if n != 'preflight/terminal.json'}),
                            dict(terminal, artifacts=dict(terminal['artifacts'], **{'unexpected.bin': dict(bytes=1, sha256='0' * 64)}))):
                (out / 'aws-terminal.json').write_bytes(encoded(changed))
                rejected(lambda: replay(out))
            (out / 'aws-terminal.json').write_bytes(encoded(terminal))
            receipt_body = (out / 'retention-receipt.json').read_bytes()
            for mutate in (lambda r: r['objects'].pop(), lambda r: r['objects'][0].update(sha256='0' * 64),
                           lambda r: r['objects'][0].update(key='research/other/retained/x'), lambda r: r.update(prefix='research/other')):
                changed = json.loads(receipt_body); mutate(changed)
                (out / 'retention-receipt.json').write_bytes(encoded(changed) + b'\n')
                rejected(lambda: replay(out))
            (out / 'retention-receipt.json').write_bytes(receipt_body)
            (out / 'preflight/baseline-result.jsonl').write_bytes(b'tampered')
            rejected(lambda: replay(out))
            store[PREFIX + 'a0001/artifacts/preflight/baseline-result.jsonl'] = b'tampered download'
            rejected(lambda: collect(s3, PREFIX + 'a0001', out, 'i-owned', '0' * 40, '1' * 64))


def _retain_self_check(root):
    from unittest.mock import patch as mock_patch
    module = sys.modules[__name__]
    inst = Path(root) / 'retain'
    inst.mkdir()
    terminal = _closure_tree(inst, 'NATIVE_CHAIN_CLOSED', True)
    names = retained_names(terminal['inventory'])
    prefix = PREFIX + 'a0001'
    uploads = []
    def runner(wrong_size=False, fail_at=None):
        def fake_run(command, **kwargs):
            if command[:3] == ['aws', 's3', 'cp']:
                assert command[3].startswith(str(inst / 'preflight')) and '--metadata' in command and '--only-show-errors' in command
                uploads.append((command[4], command[command.index('--metadata') + 1]))
                if fail_at == len(uploads):
                    raise subprocess.CalledProcessError(1, command)
                return subprocess.CompletedProcess(command, 0)
            assert command[:3] == ['aws', 's3api', 'head-object']
            name = command[command.index('--key') + 1][len(prefix + '/retained/'):]
            return subprocess.CompletedProcess(command, 0, stdout=f"{terminal['inventory'][name]['bytes'] + (1 if wrong_size else 0)}\n")
        return mock_patch.object(module.subprocess, 'run', fake_run)
    with runner(wrong_size=True):
        try:
            retain(inst, prefix)
        except AssertionError as error:
            assert 'uploaded object length' in str(error)
        else:
            raise AssertionError('wrong uploaded length accepted')
    assert len(uploads) == 1 and not (inst / 'retention-receipt.json').exists(), 'stops at the first bad object, no receipt'
    uploads.clear()
    with runner(fail_at=3):
        try:
            retain(inst, prefix)
        except subprocess.CalledProcessError:
            pass
        else:
            raise AssertionError('failed upload accepted')
    assert len(uploads) == 3 and not (inst / 'retention-receipt.json').exists()
    uploads.clear()
    with runner():
        rejected(lambda: retain(inst, PREFIX + 'a00x1'))
        assert uploads == []
        receipt = retain(inst, prefix)
        count = len(uploads)
        rejected(lambda: retain(inst, prefix))
        assert len(uploads) == count, 'second retention refused before any upload'
    assert [o['path'] for o in receipt['objects']] == names and len(names) == 11 and 'scratch/ignored.bin' not in names
    assert uploads == [('s3://' + peer.BUCKET + '/' + prefix + '/retained/' + n, 'sha256=' + terminal['inventory'][n]['sha256']) for n in names]
    assert json.loads((inst / 'retention-receipt.json').read_bytes()) == receipt


def _lifecycle_self_check(proof):
    from datetime import datetime, timezone
    from unittest.mock import Mock
    module = sys.modules[__name__]
    for failure in ('success', 'multi-ack', 'multi-ack-fsync', 'upload', 'interrupt', 'wait-failure'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [{'SpotPrice': '.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ids = ['i-owned', 'i-extra'] if failure.startswith('multi-ack') else ['i-owned']
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
            def waited(**kw):
                events.append('wait')
                if failure == 'wait-failure':
                    raise OSError('termination waiter failed')
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ['terminate', 'wait'], 'collection before termination waiter'
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0, artifacts={n: {} for n in ARTIFACTS})
            with patch.object(module, 'ROOT', Path(tmp)), patch.object(module, 'preflight', return_value=proof), \
                    patch.object(module, 'user_data', return_value='mock'), patch.object(module, 'collect', side_effect=collected), \
                    patch.object(module, 'poll', side_effect=KeyboardInterrupt() if failure == 'interrupt' else None), \
                    patch.object(shared.boto3, 'Session', return_value=session), \
                    patch.object(subprocess, 'check_output', side_effect=['', '0' * 40]), \
                    patch.object(shared, 'source_archive', return_value=b'mocked archive') as archive, \
                    patch.object(peer, 'missing', return_value=True), \
                    patch.object(peer, 'put_if_absent', side_effect=[None, None, OSError('upload')] if failure == 'upload' else [None] * 3), \
                    patch.object(os, 'fsync', side_effect=OSError('persist') if failure.endswith('fsync') else None), \
                    contextlib.redirect_stdout(io.StringIO()):
                try:
                    main('a0001')
                except (OSError, KeyboardInterrupt):
                    assert failure not in ('success', 'multi-ack')
                else:
                    assert failure in ('success', 'multi-ack'), 'failure swallowed'
            archive.assert_called_once_with('0' * 40, proof['source_archive_paths'])
            reserved = json.loads((Path(tmp) / 'a0001/aws-reservation.json').read_bytes())
            assert all(reserved['qualification'][key] == proof[key] for key in ARCHIVE_FIELDS)
            assert reserved['wall_seconds'] == WALL and reserved['compute_cap_usd'] == COMPUTE_CAP and reserved['ebs_s3_allowance_usd'] == .15
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            assert events == (['terminate', 'wait'] if failure == 'wait-failure' else ['terminate', 'wait', 'collect'])
            kwargs = ec2.run_instances.call_args.kwargs
            assert (kwargs['InstanceType'], kwargs['ImageId'], kwargs['BlockDeviceMappings'][0]['DeviceName']) == (INSTANCE_TYPE, IMAGE_ID, ROOT_DEVICE_NAME)
            assert kwargs['InstanceMarketOptions']['SpotOptions']['MaxPrice'] == '0.50'
            if failure == 'wait-failure':
                assert not (Path(tmp) / 'a0001/aws-closeout.json').exists(), 'closeout before confirmed termination'
            for name in (('aws-launch.json',) if failure == 'wait-failure' else ('aws-launch.json', 'aws-closeout.json')):
                assert json.loads((Path(tmp) / 'a0001' / name).read_bytes())['nodes'] == {str(i): dict(instance_id=n) for i, n in enumerate(ids)}


def self_check():
    """Synthetic only: no AWS, native, network, Cargo or corpus. Real shared.main with mocked EC2."""
    real = Path(__file__).resolve().parents[1]
    loaded = subprocess.check_output([sys.executable, '-B', '-c', 'import sys; import scripts.launch_cohere_native_preflight_spot; '
        'print("\\n".join(sorted(k.split(".",1)[1] for k in sys.modules if k.startswith("scripts."))))'], cwd=real, text=True).split()
    assert tuple('scripts/' + name + '.py' for name in loaded) == CODE, 'CODE is the exact transitive import closure'
    with tempfile.TemporaryDirectory(prefix='cohere-preflight-spot-') as folder, \
            patch.object(helper, 'SHARDS', tuple((name, size) for (name, _), size in zip(helper.SHARDS, (6, 6)))):
        root = Path(folder)
        base = _build(root)
        proof = qualify(base)
        assert proof['actual_native_preflight'] is False and proof['source_archive_file_count'] == len(proof['source_archive_paths'])
        # Launch refusals, each rebuilt from a fresh frozen tree; none reaches AWS or a native process.
        def breaks(label, **edits):
            rejected(lambda: qualify(_build(root, label, **edits)))
        def edit(path, value, key):
            def apply(body):
                target = body
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
            return {key: apply}
        for label, edits in (
                ('pending', edit(['controller_authority_pending'], True, 'config_edit')),
                ('pending-int', edit(['controller_authority_pending'], 0, 'config_edit')),
                ('schema', edit(['schema'], 'other', 'config_edit')),
                ('commit', edit(['controller_source_commit'], 'z', 'config_edit')),
                ('resources', edit(['resources', 'compute_cap_usd'], 2.0, 'config_edit')),
                ('resource-type', edit(['resources', 'build_cpu'], 4.0, 'config_edit')),
                ('identity', edit(['source_identity_sha256'], 'c' * 64, 'config_edit')),
                ('shard-locator-revision', edit(['transports', 'shards', helper.SHARDS[0][0]], REVISION_URL.replace(helper.REVISION, 'f' * 40) + helper.SHARDS[0][0], 'config_edit')),
                ('shard-locator-host', edit(['transports', 'shards', helper.SHARDS[1][0]], 'https://example.com/' + helper.SHARDS[1][0], 'config_edit')),
                ('shard-locator-bucket', edit(['transports', 'shards', helper.SHARDS[1][0]], 's3://other-bucket/research/x', 'config_edit')),
                ('redirect-wildcard', edit(['transports', 'redirect_hosts'], ['*.hf.co'], 'config_edit')),
                ('redirect-initial-host', edit(['transports', 'redirect_hosts'], ['huggingface.co'], 'config_edit')),
                ('redirect-unsorted', edit(['transports', 'redirect_hosts'], ['b.example.net', 'a.example.net'], 'config_edit')),
                ('redirect-scheme', edit(['transports', 'redirect_hosts'], ['https://cdn.example.net'], 'config_edit')),
                ('redirect-missing', edit(['transports', 'redirect_hosts'], None, 'config_edit')),
                ('binary-key', edit(['transports', 'binaries', 'baseline'], '../escape', 'config_edit')),
                ('binary-key-dup', edit(['transports', 'binaries', 'baseline'], 'research/test/preparer', 'config_edit')),
                ('roster-gap', edit(['source_archive_paths'], [], 'config_edit')),
                ('roster-hash', edit(['source_archive_paths_sha256'], '0' * 64, 'config_edit')),
                ('roster-count', edit(['source_archive_file_count'], 1, 'config_edit')),
                ('support', edit(['source_archive_support_sha256'], {}, 'config_edit')),
                ('unknown-key', edit(['extra'], 1, 'config_edit')),
                ('helper-deadline', edit(['deadline_seconds'], helper.DEADLINE_MAX + 1, 'helper_edit')),
                ('helper-phase', edit(['phase_seconds', 'fit'], helper.PHASE_MAX['fit'] + 1, 'helper_edit')),
                ('helper-shard-length', edit(['shards', 0, 'bytes'], 4, 'helper_edit')),
                ('helper-binary-path', edit(['binaries', 'baseline', 'path'], '/tmp/baseline', 'helper_edit')),
                ('helper-shard-path', edit(['shards', 1, 'path'], '/tmp/shard', 'helper_edit')),
                ('helper-identity', edit(['source_identity_sha256'], 'c' * 64, 'helper_edit')),
                ('adm-pending', edit(['status'], 'PENDING', 'admission_edit')),
                ('adm-failed', edit(['status'], 'FAILED', 'admission_edit')),
                ('adm-exit', edit(['gates', 'native_publisher', 'stage_exit_statuses'], [0, 0, 0, 0, 0, 1], 'admission_edit')),
                ('adm-alive', edit(['gates', 'generation_builder', 'terminated'], False, 'admission_edit')),
                ('adm-role-sha', edit(['roles', 'publisher', 'binary', 'sha256'], 'd' * 64, 'admission_edit')),
                ('adm-source', edit(['source_identity_sha256'], 'c' * 64, 'admission_edit')),
                ('code-drift', dict(drift=lambda b: (b / CODE[0]).write_bytes((b / CODE[0]).read_bytes() + b'#drift\n'))),
                ('source-drift', dict(drift=lambda b: (b / 'crates/borsuk/src/lib.rs').write_bytes(b'// drift\n'))),
                ('helper-drift', dict(drift=lambda b: (b / ROOT / 'helper-config.json').write_bytes(b'{}'))),
                ('gate-drift', dict(drift=lambda b: (b / ROOT / 'gates/sq8_builder.json').write_bytes(b'{}')))):
            breaks(label, **edits)
        proof, body = _bootstrap_self_check(base)
        (root / 'transport').mkdir()
        _transport_self_check(root / 'transport', base)
        _collection_self_check(root, proof, body)
        _retain_self_check(root)
        _lifecycle_self_check(proof)
        with contextlib.redirect_stdout(io.StringIO()):
            shared.self_check(lifecycle_only=True)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    print(f'PASS cohere native preflight spot adapter: {len(CODE)} code files exact import closure; launch refusals (pending/identity/'
          f'resources/locators/roster/admission/gates/drift); bootstrap bash/size/slice/limits; transport exact-length/SHA/occupied/partial; '
          f'helper closed/nonzero/invalid collection+replay; real shared.main ACK/same-ID terminate+wait-before-collect; SYNTHETIC ONLY '
          f'(no AWS, native, Cargo, network or corpus); peak_rss={peak}')


if __name__ == '__main__':
    args = sys.argv[1:]
    if args[:1] and args[0].startswith('--'):
        resource.setrlimit(resource.RLIMIT_AS, (400 * 1024 ** 2, 400 * 1024 ** 2))
    if args == ['--self-check']:
        self_check()
    elif args[:1] == ['--stage'] and len(args) == 3:
        stage(*args[1:])
    elif args[:1] == ['--transport'] and len(args) == 2:
        transport(args[1])
    elif args[:1] == ['--retain'] and len(args) == 3:
        retain(*args[1:])
    elif args[:1] == ['--check-closed'] and len(args) == 3:
        print(json.dumps(check_closed(args[1], int(args[2])), sort_keys=True))
    elif args[:1] == ['--replay'] and len(args) == 2:
        print(json.dumps(replay(args[1]), sort_keys=True))
    else:
        assert len(args) == 1, 'usage: aNNNN | --self-check | --stage REPO OUT | --transport ROOT | --check-closed ROOT EXIT | --replay OUT'
        main(args[0])
