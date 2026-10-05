"""One frozen native packing diagnostic; root owns config, transport and launch.

CLI: aNNNN | --self-check | --replay OUT. Remote --remote is bootstrap-only.
--sq4 explicitly selects the root-frozen native SQ4 experiment.
No compiler, query runner, packing algorithm, retries or replacement instances.
"""
import argparse
from datetime import datetime, timedelta
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import subprocess
import sys
import time

if not __debug__:
    raise RuntimeError('diagnostic requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/packing-diagnostic')
CONFIG, NAME = ROOT/'config.json', ''
SCHEMA = 'borsuk-fine-pack-diagnostic-spot-v1'
PREFIX = 'research/hierarchical-cells/20261005/fine-pack-diagnostic-'
TOKEN_PREFIX, TAG = 'fine-pack-diagnostic-', 'borsuk-fine-pack-diagnostic'
MODULE = 'scripts.launch_fine_pack_diagnostic'
WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR = 900, .15, .60
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME, SUBNET = '/dev/sda1', 'subnet-034528fbd6977848f'
REGION, BUCKET = 'eu-central-1', 'borsuk-bench-453182569524-euc1'
REMOTE_ROOT = Path('/mnt/fine-pack-diagnostic')
SUPERVISOR_UNIT = 'fine-pack-supervisor'
CONTROLLERS = {'cpu', 'memory', 'pids'}
BINARY_NAME = 'binaries/hierarchical_semantic_cells'
BINARY_PIN = dict(bytes=7860472, sha256='a4ea1d88223dbcf73b714ca2f75b9556c9d4f1389333391c1ac619a8bb9b4f45')
BINARY_KEY = 'research/semantic-router/20261005/fine-sq8-packing-binary-repair-a0001/artifacts/'+BINARY_NAME
NATIVE_COMMIT = '71f77b3a30ea7f364c7a9d7e82188151a25a6be4'
SOURCE_ID = '13286db011c6f0699f74b0e10e9d340e0ac565346a8c8640bcf5e4b64990b36d'
DIAGNOSTIC_SOURCES = {
    'fine_sq8_groups.rs': 'a66c728c9bc708e542b57a8c8dc4efa681a705c2731717ad3eda2d52546d5027',
    'resident_vector_graph.rs': 'c62459b45caeaaa798b16b24b88180ad6d4dc5bdd554cd804e6d6b354eb3b79d',
    'bin/hierarchical_semantic_cells.rs': 'f636602726e30f8f5c4d67f557546318ffb75f091da30c7a131a1247d39a6bd3'}
CAPS = dict(cpu_threads=1, memory_bytes=256*1024**2, swap_bytes=0,
            deadline_seconds=300, operations=500000000, output_bytes=2*1024**2)
FIXED = dict(region=REGION, bucket=BUCKET, instance_type=INSTANCE_TYPE,
             image_id=IMAGE_ID, subnet_id=SUBNET, root_device_name=ROOT_DEVICE_NAME,
             machine_limit_seconds=WALL, compute_cap_usd=COMPUTE_CAP,
             spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, ebs_s3_allowance_usd=.15,
             native_cpu_affinity=[0], native_caps=CAPS, tasks_max=32,
             automatic_retry=False, quality_or_performance_claim=False)
# The lifecycle's import closure is frozen too; the remote supervisor imports
# only this file and the SDK. Root supplies the minimal archive roster.
CODE = tuple(sorted('scripts/'+n+'.py' for n in (
    'launch_fine_pack_diagnostic', 'launch_native_metadata_ranges_cold_spot',
    'launch_native_startup_profile_spot', 'launch_native_peer_1m_spot',
    'launch_v174_relaid_bind_compile_spot', 'launch_v157_primary_feasibility_spot',
    'check_native_metadata_ranges_build', 'check_native_metadata_ranges_stats',
    'check_native_startup_build', 'check_native_startup_stats',
    'run_native_metadata_ranges_cold', 'run_native_cold_first_query',
    'run_native_peer_1m_worker', 'run_native_peer_offered_http',
    'run_native_union_offered_http', 'run_native_union_http', 'rest_coexistence_load')))
EVIDENCE = ROOT.parent/'packing-implementation-gates/binary-repair/a0001'
RECEIPTS = tuple(dict(path=str(EVIDENCE/name), bytes=size, sha256=digest) for name, size, digest in (
    ('parent-verification.json', 14424, 'c13efa7743a6d9e9f9d21b0e9e5ec56b3b5058484b2d4eca71ec7c817c6132bd'),
    ('aws-terminal.json', 3128, '3dca512d1b9c89e571944e792efc36d91a66e02c6e5195c78b2810098ea8855e'),
    ('aws-closeout.json', 136, '1d3cce4ac98066fe088ed543e0a34fefa34f8557d935edd63f5f4cf9b5558fd2'),
    ('workspace-receipt.json', 62788, '697db113bf61402d8a6c776fbbd7c94bb12538faec904c2f23ab7bf611a02333'),
    ('source-qualification.json', 410667, 'ffae1c458c116584539b234825a314d2fab71d90f593cd64d19c8849593c56d9')))
INPUT_PINS = (
    ('/mnt/hierarchical-100k/screen/fine/layouts/relaion/manifest.json', 22721, '0a2a2dff6a7abc48143d08eb491a150bbed03957b81b0261c2b8a0434612afec'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/relaion/graph.bin', 26712462, 'f817d7f4fbdd640f66590f58f9e07e9218753fbec4517ba95fc405a78d612325'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/cohere/manifest.json', 22815, 'ea1a792a841014ef08ed805d2872403739ba778ea8b8d3a9e5eab1b7f831c8bb'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/cohere/graph.bin', 26844914, '4afd895f2e684f833437f4e6d4270c4713bbeb54fe67b50919f796e95f896e2e'),
    ('/mnt/hierarchical-100k/packing-original-seal.json', 1109, '374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62'),
    ('/mnt/hierarchical-100k/packing-prefix.jsonl', 1665668, '7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025'))
ARTIFACTS = ('config.json', 'native-config.json', 'source-qualification.json',
    'stage-receipt.json', 'native-exit.json', 'resources.json', 'cleanup.json',
    'cpu.txt', 'runtime-abi.json', 'run-closed.log', 'native.log', BINARY_NAME,
    'screen/report.json', 'screen/report.permutations.json', 'screen/report.prefix.jsonl')
ROSTER_SHA = hashlib.sha256(json.dumps(ARTIFACTS, separators=(',', ':')).encode()).hexdigest()
CGROUP_FILES = ('memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
                'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat',
                'pids.max', 'pids.current', 'pids.events', 'cgroup.procs', 'cgroup.events')
SQ4 = False
SQ4_ROOT = ROOT.parent/'sq4-refinement'
# Prospective fixture provenance only; config.native_source owns the qualified pins.
SQ4_COMMIT = 'c2d233d6752d0a058d78c6077e51f31f0255e7c1'
SQ4_SOURCE_ID = 'e808aae7d27e570d1bf7651d70f6144a7a93e09bae46f0222151852fff0a70db'
SQ4_INPUT_ROOT = Path('/mnt/hierarchical-100k')
SQ4_SCRATCH_CAP = 4*1024**3
SQ4_CLOSURE_RESERVE = 12*1024**2  # Receipts + closed log + terminal + live log tail.
SQ4_SOURCES = {
    'crates/borsuk/src/fine_sq8_groups.rs': '250f592b8c2ad654f1b6b7281f08a0b3c420b4d4d157de29f8e2017212788f37',
    'crates/borsuk/src/bin/hierarchical_semantic_cells.rs': '4d46713e88ac6f75230327fc160734bebc991202e7904ee4994ccfd2760f7e6a'}
SQ4_RECEIPTS = ('parent-verification.json', 'source-qualification.json',
                 'workspace-receipt.json', 'aws-terminal.json', 'aws-closeout.json')
SQ4_OUTPUTS = ('screen/report.json', 'screen/report.sq4-0.bin', 'screen/report.sq4-1.bin',
    'screen/report.sq4-payloads.json', 'screen/report.sq4-prefix.jsonl', 'screen/report.sq4-freeze.json',
    *(f'screen/report.sq4-result-{i}.json' for i in range(128)))
SQ4_TESTS = ('tests::fine_sq4_strict_cli_dispatch', 'tests::fine_sq4_real_native_pipeline_128_seal_before_truth',
    *(f'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::{n}' for n in (
        'fine_sq4_all_codes_numeric_oracle_odd_tail_nonunit_ties', 'fine_sq4_exact_cover_superset_and_binding',
        'fine_sq4_auth_fifo_corruption_caps_and_durability', 'fine_sq4_full_pipeline_failure_order_and_sync')))
# Canonical encoded({stages, mandatory_tests}) from bf0667e8's existing
# FINE_SQ8_STAGES/FINE_SQ8_REQUIRED_TESTS. No reduced self-asserted roster.
SQ4_QUALIFICATION_PROTOCOL_SHA = 'd3ac6d4f45391decf7c08f483a6209f99505897c62355a52d40f0a2b42f2cc8a'


def configure_sq4():
    """Opt-in campaign only. Resource/transport authority still comes from root."""
    global SQ4, ROOT, CONFIG, SCHEMA, PREFIX, TOKEN_PREFIX, TAG, REMOTE_ROOT
    global CAPS, NATIVE_COMMIT, SOURCE_ID, INPUT_PINS, ARTIFACTS, ROSTER_SHA
    if SQ4:
        return
    SQ4 = True
    ROOT = SQ4_ROOT/'native-diagnostic'; CONFIG = ROOT/'config.json'
    SCHEMA = 'borsuk-fixed-sq4-diagnostic-spot-v1'
    PREFIX = 'research/hierarchical-cells/20261005/fixed-sq4-diagnostic-'
    TOKEN_PREFIX, TAG = 'fixed-sq4-diagnostic-', 'borsuk-fixed-sq4-diagnostic'
    REMOTE_ROOT = Path('/mnt/fixed-sq4-diagnostic')
    CAPS = dict(cpu_threads=1, memory_bytes=1024**3, swap_bytes=0,
                deadline_seconds=600, operations=20000000000, output_bytes=256*1024**2)
    NATIVE_COMMIT, SOURCE_ID = SQ4_COMMIT, SQ4_SOURCE_ID
    # Metadata only: never hydrate or parse a retained input during preflight.
    roster = decode(read(Path(__file__).resolve().parents[1]/SQ4_ROOT/'prospective-input-roster.json'))
    require(roster['schema'] == 'borsuk-sq4-prospective-input-roster-v1'
            and pin(encoded(roster['inputs'])) == dict(bytes=5175,
                sha256='1959126f0910746e2b572b305dd4e77a59310e5ee16313ce5178fd36b28c1f3b'), 'SQ4 retained roster pin')
    INPUT_PINS = tuple((d['destination'], d['bytes'], d['sha256']) for d in roster['inputs'])
    ARTIFACTS = tuple(n for n in ARTIFACTS if not n.startswith('screen/')) + (
        'scratch.json', *(f'qualification/{n}' for n in SQ4_RECEIPTS), *SQ4_OUTPUTS)
    ROSTER_SHA = sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode())


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def decode(body):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, 'duplicate JSON key: '+key)
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=pairs,
                      parse_constant=lambda n: require(False, 'invalid JSON number: '+n))


def pin(body):
    return dict(bytes=len(body), sha256=sha(body))


def regular(path):
    path = Path(path).absolute()
    require(path.resolve() == path, 'symlink or noncanonical path: '+str(path))
    fd = os.open(path, os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), 'regular file required: '+str(path))
        return os.fdopen(fd, 'rb')
    except BaseException:
        os.close(fd)
        raise


def file_pin(path):
    digest, size = hashlib.sha256(), 0
    with regular(path) as source:
        for chunk in iter(lambda: source.read(65536), b''):
            digest.update(chunk); size += len(chunk)
    return dict(bytes=size, sha256=digest.hexdigest())


def read(path, limit=4*1024**2):
    with regular(path) as source:
        body = source.read(limit+1)
    require(len(body) <= limit, 'file cap: '+str(path))
    return body


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    require(path.absolute().resolve() == path.absolute(), 'write path symlink')
    with path.open('xb') as output:
        output.write(body); output.flush(); os.fsync(output.fileno())
    sync_directory(path.parent)


def body_pin(value, max_bytes=64*1024**2):
    require(type(value) is dict and set(value) == {'bytes', 'sha256'}, 'artifact identity fields')
    require(type(value['bytes']) is int and 0 <= value['bytes'] <= max_bytes, 'artifact byte cap')
    require(type(value['sha256']) is str and re.fullmatch('[0-9a-f]{64}', value['sha256']), 'artifact digest')
    return value


def object_key(key):
    require(type(key) is str and re.fullmatch(r'[A-Za-z0-9_./-]{1,1024}', key)
            and not key.startswith('/') and all(p not in ('', '.', '..') for p in key.split('/')), 'object key')


def native_inputs(native):
    if SQ4:
        return [a for p in native['panels'] for a in (p['root'], p['requests'], p['truth'])]+[native['original_seal'], native['prefix']]
    return [a for p in native['panels'] for a in (p['root'], p['graph'])]+[native['original_seal'], native['prefix']]


def sq4_qualification(config, base, collected=False):
    receipts = {}
    identities = {}
    for r in config['native_qualification']:
        name = Path(r['path']).name
        path = Path(base)/('qualification/'+name if collected else r['path'])
        require(file_pin(path) == {k:r[k] for k in ('bytes', 'sha256')}, 'SQ4 qualification drift: '+name)
        receipts[name] = decode(read(path))
        identities[name] = {k:r[k] for k in ('bytes','sha256')}
    v, q, w, t, close = (receipts[n] for n in SQ4_RECEIPTS)
    binary = {k:config['binary'][k] for k in ('bytes', 'sha256')}
    sources = q['source_sha256']
    source_id = sha(json.dumps(sources, sort_keys=True, separators=(',', ':')).encode())
    authority = config['native_source']
    require(source_id == authority['full_source_identity_sha256'] and len(sources) == q['source_file_count'] == w['source_file_count'] == v['source_file_count'] == 404
            and all(sources.get(n) == h for n,h in authority['source_sha256'].items())
            and w['source_sha256'] == sources, 'SQ4 full404 source proof')
    for value in (v, q, t):
        require(value['native_source_commit'] == authority['commit'], 'SQ4 exact native revision')
    require(all(value['source_identity_sha256'] == source_id for value in (v,q,w,t)), 'SQ4 full404 identity')
    require(q['mandatory_test_names_pending'] is w['mandatory_test_names_pending'] is False
            and w['qualified'] is w['command_started'] is w['command_completed'] is w['source_unchanged'] is True
            and type(w['exit_status']) is int and w['exit_status'] == w['gate_status'] == 0
            and w['qualification_sha256'] == identities['source-qualification.json']['sha256'], 'SQ4 completed qualification')
    stages = w['stages']
    required = q['mandatory_tests']
    require(stages == v['stages'] and w['mandatory_tests'] == required
            and sha(encoded(dict(stages=[(s['stage'],s['command']) for s in stages],
                                 mandatory_tests=required))) == SQ4_QUALIFICATION_PROTOCOL_SHA, 'SQ4 exact all14 commands and mandatory roster')
    previous_finish = None
    for stage in stages:
        started, finished = (datetime.fromisoformat(stage[k]) for k in ('started_at','finished_at'))
        require(started.utcoffset() == finished.utcoffset() == timedelta(0)
                and finished >= started and (previous_finish is None or started >= previous_finish), 'SQ4 serial completed UTC stages')
        previous_finish = finished
        require(all(
            type(stage[k]) is int and stage[k] == 0 for k in ('exit_status','gate_status','log_exit_status')), 'SQ4 original gate exit')
        mandatory = required.get(stage['stage'], ())
        require((type(stage['tests_run']) is int and stage['tests_run'] >= len(mandatory) > 0)
                if mandatory else stage['tests_run'] is None, 'SQ4 positive counts only on test stages')
        require(stage['required_test_passes'] == {n:1 for n in mandatory}
                and all(type(n) is int for n in stage['required_test_passes'].values()), 'SQ4 exact owning-stage mandatory passes')
    require(v['source_before_equals_after'] is v['all_source_blobs_independently_matched'] is True
            and v['artifact_hashes_independently_verified'] is v['descendants_drained'] is True
            and v['original_controller_exit_status'] == v['swap_peak_bytes'] == v['oom'] == 0
            and v['binary'] == binary and v['instance_closeout'] == close, 'SQ4 parent source/resource proof')
    require(t['status'] == t['phase'] == 'complete' and t['exit_code'] == t['original_exit_code'] == 0
            and close['state'] == 'terminated' and close['nodes'] == {'0':{'instance_id':t['instance_id']}}
            and t['source_qualification_sha256'] == identities['source-qualification.json']['sha256']
            and t['artifacts']['workspace-receipt.json'] == identities['workspace-receipt.json']
            and t['native_source_manifest_sha256'] == q['native_source_manifest_sha256'] == q['native_source_manifest']['sha256']
            and t['artifacts'][BINARY_NAME] == binary, 'SQ4 qualified original terminal/binary/termination')
    return receipts


def validate_sq4_config(config, base=None):
    global FIXED, WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR, NATIVE_COMMIT, SOURCE_ID
    require(set(config) == {'schema','authority_pending','fixed','native_config','native_config_sha256',
        'binary','inputs','native_source','native_qualification','code_sha256','source_archive_paths','source_archive_paths_sha256'}
        and config['schema'] == SCHEMA and config['authority_pending'] is False, 'SQ4 frozen root authority')
    fixed = config['fixed']
    require(set(fixed) == set(FIXED)|{'scratch'} and encoded(fixed['native_caps']) == encoded(CAPS)
            and all(encoded(fixed[k]) == encoded(FIXED[k]) for k in FIXED if k not in (
                'machine_limit_seconds','compute_cap_usd','spot_max_usd_per_hour','native_caps','scratch')), 'SQ4 fixed method/host')
    wall, cost, spot = (fixed[k] for k in ('machine_limit_seconds','compute_cap_usd','spot_max_usd_per_hour'))
    require(type(wall) is int and wall == 1800 and type(spot) in (int,float) and spot == .60
            and type(cost) in (int,float) and cost == .30, 'SQ4 root wall/cost/Spot cap')
    native = config['native_config']
    authority = config['native_source']
    require(set(authority) == {'commit','full_source_identity_sha256','source_identity_sha256','source_sha256'}
            and re.fullmatch('[0-9a-f]{40}', authority['commit'])
            and set(authority['source_sha256']) == set(SQ4_SOURCES), 'SQ4 exact root source pins')
    for digest in (authority['full_source_identity_sha256'],authority['source_identity_sha256'],*authority['source_sha256'].values()):
        body_pin(dict(bytes=0,sha256=digest))
    require(set(native) == {'schema','source_identity_sha256','caps','panels','original_seal','prefix'}
            and native['schema'] == 'borsuk-fixed-sq4-config-v1' and encoded(native['caps']) == encoded(CAPS)
            and native['source_identity_sha256'] == authority['source_identity_sha256'] and len(native['panels']) == 2
            and sha(encoded(native)) == config['native_config_sha256'], 'SQ4 exact native config')
    for p,dataset in zip(native['panels'], ('relaion','cohere')):
        require(set(p) == {'dataset','root','requests','truth','truth_width'} and p['dataset'] == dataset
                and type(p['truth_width']) is int and p['truth_width'] == 100, 'SQ4 native panel')
    expected = [dict(path=p,bytes=n,sha256=h) for p,n,h in INPUT_PINS]
    require(encoded(native_inputs(native)) == encoded([expected[i] for i in (0,6,7,8,14,15,16,17)])
            and len(config['inputs']) == len(INPUT_PINS) == 18, 'SQ4 retained original descriptors')
    for d,(path,size,digest) in zip(config['inputs'], INPUT_PINS):
        require(set(d) == {'destination','key','bytes','sha256'} and d['destination'] == path
                and body_pin({k:d[k] for k in ('bytes','sha256')}, max_bytes=size) == dict(bytes=size,sha256=digest), 'SQ4 opaque input transport')
        object_key(d['key'])
    require(len({d['key'] for d in config['inputs']}) == 18, 'SQ4 distinct input objects')
    require(set(config['binary']) == {'key','bytes','sha256'} and body_pin({k:config['binary'][k] for k in ('bytes','sha256')})['bytes'] > 0, 'SQ4 root binary pin')
    object_key(config['binary']['key'])
    receipts = config['native_qualification']
    require(len(receipts) == 5 and tuple(Path(r['path']).name for r in receipts) == SQ4_RECEIPTS, 'SQ4 five completed qualification receipts')
    for r in receipts:
        require(set(r) == {'path','bytes','sha256'} and r['bytes'] > 0, 'SQ4 qualification descriptor')
        object_key(r['path']); body_pin({k:r[k] for k in ('bytes','sha256')})
    paths = sorted([str(CONFIG), str(SQ4_ROOT/'prospective-input-roster.json'), *CODE, *(r['path'] for r in receipts)])
    require(config['source_archive_paths'] == paths and config['source_archive_paths_sha256'] == sha(
        json.dumps(paths,separators=(',',':')).encode()) and set(config['code_sha256']) == set(CODE), 'SQ4 minimal source archive')
    for name,digest in config['code_sha256'].items():
        body_pin(dict(bytes=0,sha256=digest))
        if base is not None:
            require(file_pin(Path(base)/name)['sha256'] == digest, 'SQ4 code drift: '+name)
    scratch = fixed['scratch']
    require(set(scratch) == {'input_bytes','native_output_bytes','binary_bytes','source_archive_bytes',
        'bootstrap_bytes','auxiliary_bytes','cap_bytes'} and all(type(n) is int and n > 0 for n in scratch.values())
        and scratch['input_bytes'] == sum(p[1] for p in INPUT_PINS)
        and scratch['native_output_bytes'] == CAPS['output_bytes'] and scratch['binary_bytes'] == config['binary']['bytes']
        and sum(n for k,n in scratch.items() if k != 'cap_bytes') <= scratch['cap_bytes'] == SQ4_SCRATCH_CAP, 'SQ4 whole scratch coexistence charge')
    if base is not None:
        sq4_qualification(config, base)
    FIXED = fixed
    WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR = wall, cost, spot
    NATIVE_COMMIT, SOURCE_ID = authority['commit'], authority['source_identity_sha256']
    return native


def scratch_observation(root):
    """Charge apparent or allocated bytes, including temporary files and symlinks."""
    sizes = {}
    for tree in (Path(root), SQ4_INPUT_ROOT):
        total = 0
        if tree.exists():
            for directory, names, files in os.walk(tree, followlinks=False):
                for path in (Path(directory), *(Path(directory)/n for n in files),
                             *(Path(directory)/n for n in names if (Path(directory)/n).is_symlink())):
                    try:
                        s = path.lstat()
                    except FileNotFoundError:
                        continue  # A removed temporary file no longer coexists.
                    total += max(s.st_size, s.st_blocks*512)
        sizes[str(tree)] = total
    return dict(roots=sizes, whole_scratch_bytes=sum(sizes.values()))


def scratch_room(root, growth):
    observation = scratch_observation(root)
    require(observation['whole_scratch_bytes']+growth <= SQ4_SCRATCH_CAP, 'SQ4 scratch before closure write')
    return observation


def validate_config(config, base=None):
    if SQ4:
        return validate_sq4_config(config, base)
    require(set(config) == {'schema', 'authority_pending', 'fixed', 'native_config',
        'native_config_sha256', 'binary', 'inputs', 'native_qualification', 'code_sha256',
        'source_archive_paths', 'source_archive_paths_sha256'}, 'frozen config fields')
    require(config['schema'] == SCHEMA and config['authority_pending'] is False, 'root must freeze authority')
    require(encoded(config['fixed']) == encoded(FIXED), 'fixed resource/method contract')
    native = config['native_config']
    require(set(native) == {'schema', 'caps', 'panels', 'original_seal', 'prefix'}
            and native['schema'] == 'borsuk-fine-pack-config-v1'
            and encoded(native['caps']) == encoded(CAPS), 'native config contract')
    require(len(native['panels']) == 2, 'two panels')
    for p, dataset in zip(native['panels'], ('relaion', 'cohere')):
        require(set(p) == {'dataset', 'identity', 'root', 'graph'} and p['dataset'] == dataset, 'native panel')
        identity = p['identity']
        require(set(identity) == {'rows', 'dimensions', 'generation', 'source', 'layout', 'pq'}
                and type(identity['rows']) is int and identity['rows'] == 100000
                and type(identity['dimensions']) is int and identity['dimensions'] == 768
                and type(identity['generation']) is int and 0 <= identity['generation'] < 2**64, 'panel geometry')
        for name in ('source', 'layout', 'pq'):
            require(type(identity[name]) is list and len(identity[name]) == 32
                    and all(type(v) is int and 0 <= v <= 255 for v in identity[name]), 'layout identity')
    require(sha(encoded(native)) == config['native_config_sha256'], 'native config digest')
    require(len(config['inputs']) == 6, 'six input bodies only')
    for descriptor, transport, (path, size, digest) in zip(native_inputs(native), config['inputs'], INPUT_PINS):
        require(descriptor == dict(path=path, bytes=size, sha256=digest), 'historical input pin')
        require(set(transport) == {'destination', 'key', 'bytes', 'sha256'}
                and transport['destination'] == path and body_pin({k:transport[k] for k in ('bytes', 'sha256')})
                == dict(bytes=size, sha256=digest), 'exact native input transport')
        object_key(transport['key'])
    require(len({i['key'] for i in config['inputs']}) == 6, 'distinct input objects')
    require(config['binary'] == dict(BINARY_PIN, key=BINARY_KEY), 'qualified executable pin')
    require(config['native_qualification'] == list(RECEIPTS), 'qualified original receipts')
    require(set(config['code_sha256']) == set(CODE), 'code closure')
    for name, digest in config['code_sha256'].items():
        body_pin(dict(bytes=0, sha256=digest))
        if base is not None:
            require(file_pin(Path(base)/name)['sha256'] == digest, 'code drift: '+name)
    # Root fills this roster after integration. Neither config nor archive hash
    # is embedded in itself; qualification and terminal bind their actual bytes.
    paths = sorted([str(CONFIG), *CODE, *(r['path'] for r in RECEIPTS)])
    require(config['source_archive_paths'] == paths
            and config['source_archive_paths_sha256'] == sha(json.dumps(paths, separators=(',', ':')).encode()), 'minimal archive closure')
    if base is not None:
        for receipt in RECEIPTS:
            require(file_pin(Path(base)/receipt['path']) == {k:receipt[k] for k in ('bytes', 'sha256')}, 'qualification drift')
    return native


def preflight(base=Path('.')):
    raw = read(base/CONFIG)
    config = decode(raw)
    validate_config(config, base)
    proof = dict(config_path=str(CONFIG), config_sha256=sha(raw), campaign_schema=SCHEMA,
                artifact_roster_sha256=ROSTER_SHA, native_config_sha256=config['native_config_sha256'],
                binary=config['binary'], native_source_commit=NATIVE_COMMIT, source_identity_sha256=SOURCE_ID,
                code_sha256=config['code_sha256'], native_qualification=config['native_qualification'],
                source_archive_paths=config['source_archive_paths'],
                source_archive_paths_sha256=config['source_archive_paths_sha256'])
    if SQ4:
        proof['scratch'] = config['fixed']['scratch']
    return proof


def lifecycle():
    from scripts import launch_native_metadata_ranges_cold_spot as shared
    return shared


def poll(ec2, s3, prefix, instance_id, started):
    shared = lifecycle()
    original = shared.WALL
    try:
        shared.WALL = WALL
        shared.poll(ec2, s3, prefix, instance_id, started)
    finally:
        shared.WALL = original


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    require(re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha), 'source archive pins')
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'attempt prefix')
    object_key(archive_key)
    require(qualification['config_path'] == str(CONFIG), 'bootstrap config path')
    bootstrap_setup, bootstrap_stop, apt_options, supervisor_options, archive_write_guard, archive_extract_guard = '', '', '', '', '', ''
    if SQ4:
        import inspect
        reserve = qualification['scratch']
        require(reserve == FIXED['scratch'], 'SQ4 bootstrap frozen scratch admission')
        # One bounded observer uses the same scan as runtime. All owned package
        # caches, lists, logs, wheels, bytecode and process temporaries coexist here.
        bootstrap_setup = f'''mkdir -p "$root/bootstrap/tmp" "$root/bootstrap/archives/partial" "$root/bootstrap/lists/partial" "$root/bootstrap/log"
export TMPDIR="$root/bootstrap/tmp" TMP="$root/bootstrap/tmp" TEMP="$root/bootstrap/tmp" PIP_CACHE_DIR="$root/bootstrap/cache" PYTHONPYCACHEPREFIX="$root/bootstrap/pycache"
cat >"$root/bootstrap/watch.py" <<'WATCH'
import json, os, signal, sys, time
from pathlib import Path
root=Path({str(REMOTE_ROOT)!r}); SQ4_INPUT_ROOT=Path({str(SQ4_INPUT_ROOT)!r})
{inspect.getsource(scratch_observation)}
reserve={reserve!r}
remaining=sum(reserve[k] for k in ('input_bytes','native_output_bytes','binary_bytes','auxiliary_bytes'))
receipt=dict(interval_seconds=1, sample_count=0, peak_bytes=0, closed=False, cap_exceeded=False, reserve=reserve)
running=True
def stop(*args):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
try:
    available=os.statvfs(root).f_bavail*os.statvfs(root).f_frsize
    assert available >= sum(v for k,v in reserve.items() if k!='cap_bytes'), 'bootstrap free scratch admission'
    while True:
        observation=scratch_observation(root)
        receipt.update(last=observation, sample_count=receipt['sample_count']+1, peak_bytes=max(receipt['peak_bytes'],observation['whole_scratch_bytes']))
        assert receipt['peak_bytes']+remaining <= reserve['cap_bytes'], 'bootstrap/input/output overlap cap'
        assert receipt['peak_bytes'] <= reserve['bootstrap_bytes']+reserve['source_archive_bytes'], 'bootstrap/source reserve'
        if receipt['sample_count']==1:(root/'bootstrap/ready').touch(exist_ok=False)
        if not running:break
        time.sleep(1)
    receipt['closed']=True
except BaseException:
    receipt['cap_exceeded']=True
    os.kill(int(sys.argv[1]),signal.SIGTERM)
    raise
finally:
    if scratch_observation(root)['whole_scratch_bytes']+65536 <= reserve['cap_bytes']:
        with (root/'bootstrap/scratch.json').open('x') as output:
            json.dump(receipt,output);output.flush();os.fsync(output.fileno())
WATCH
python3 "$root/bootstrap/watch.py" $$ &
watcher=$!
while ! test -f "$root/bootstrap/ready"; do kill -0 "$watcher"; sleep .05; done
'''
        bootstrap_stop = 'kill "$watcher"\nwait "$watcher"\n'
        apt_options = '-o Dir::Cache::archives="$root/bootstrap/archives" -o Dir::State::lists="$root/bootstrap/lists" -o Dir::Log="$root/bootstrap/log" -o APT::Sandbox::User=root '
        supervisor_options = "-p 'ReadOnlyPaths=/tmp /var/tmp' "
        archive_write_guard = f"assert output.tell()+len(chunk) <= {reserve['source_archive_bytes']}, 'source archive scratch reserve'; "
        archive_extract_guard = f"    assert Path('source.tar.gz').stat().st_size+sum(m.size for m in archive.getmembers())+4096*len(archive.getmembers()) <= {reserve['source_archive_bytes']}, 'source extraction overlap reserve'\n"
    # SDK bootstrap, IMDSv2, one request attempt, terminal last; no toolchain.
    body = f'''#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_DEFAULT_REGION={REGION}
systemd-run --unit=fine-pack-stop --on-active={WALL}s /usr/sbin/shutdown -h now
root={REMOTE_ROOT}
mkdir "$root"
cd "$root"
trap '/usr/sbin/shutdown -h now || true' EXIT
exec >run.log 2>&1
test "$(uname -m)" = x86_64
{bootstrap_setup}apt-get {apt_options}update -qq
DEBIAN_FRONTEND=noninteractive apt-get {apt_options}install -y -qq python3.12 python3.12-venv tar gzip
python3.12 -m venv "$root/venv"
python="$root/venv/bin/python"
"$python" -m pip install --retries 0 --timeout 15 --no-cache-dir --disable-pip-version-check --only-binary=:all: --no-deps boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
"$python" - <<'PY'
import boto3, hashlib, tarfile
import botocore.session
from pathlib import Path
from botocore.config import Config
assert boto3.__version__=='1.40.72' and botocore.__version__=='1.40.72'
assert 'IfNoneMatch' in botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
s3=boto3.client('s3',region_name='{REGION}',config=Config(retries={{'total_max_attempts':1}}))
response=s3.get_object(Bucket='{BUCKET}',Key='{archive_key}')
digest=hashlib.sha256()
with response['Body'] as source, open('source.tar.gz','xb') as output:
    while chunk:=source.read(65536):
        {archive_write_guard}digest.update(chunk); output.write(chunk)
assert digest.hexdigest()=='{archive_sha}'
Path('repo').mkdir()
with tarfile.open('source.tar.gz','r:gz') as archive:
{archive_extract_guard}    archive.extractall('repo',filter='data')
PY
{bootstrap_stop}export PYTHONPATH="$root/repo"
systemd-run --unit={SUPERVISOR_UNIT} --wait --pipe {supervisor_options}-p 'Delegate=cpu memory pids' -p DelegateSubgroup=supervisor -p RuntimeMaxSec={WALL} -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=AWS_RETRY_MODE=standard \\
 {('--setenv=TMPDIR="$TMPDIR" --setenv=TMP="$TMP" --setenv=TEMP="$TEMP" --setenv=PYTHONPYCACHEPREFIX="$PYTHONPYCACHEPREFIX" '+chr(92)) if SQ4 else chr(92)}
 "$python" -m {MODULE} {'--sq4 ' if SQ4 else ''}--remote "$root/repo" "$root" '{commit}' '{archive_sha}' '{prefix}' '{qualification['config_sha256']}'
'''
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    require(len(body.encode()) < 16384, 'EC2 userdata cap')
    return body


def download(s3, descriptor, destination):
    limit = 64*1024**2
    if SQ4 and 'destination' in descriptor:
        require(descriptor['destination'] == str(destination) and
                (descriptor['destination'],descriptor['bytes'],descriptor['sha256']) in INPUT_PINS, 'SQ4 exact retained download descriptor')
        limit = descriptor['bytes']
    response = s3.get_object(Bucket=BUCKET, Key=descriptor['key'])
    expected = body_pin({k:descriptor[k] for k in ('bytes', 'sha256')}, max_bytes=limit)
    with response['Body'] as source:
        require(response['ContentLength'] == expected['bytes'], 'transport ContentLength')
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        require(destination.absolute().resolve() == destination.absolute(), 'staging symlink')
        digest, total = hashlib.sha256(), 0
        with destination.open('xb') as output:
            while chunk := source.read(min(65536, expected['bytes']-total+1)):
                total += len(chunk)
                require(total <= expected['bytes'], 'transport body exceeds descriptor')
                digest.update(chunk); output.write(chunk)
            require(dict(bytes=total, sha256=digest.hexdigest()) == expected, 'transport body pin')
            output.flush(); os.fsync(output.fileno())
    sync_directory(destination.parent)


def sdk_guard():
    import boto3
    import botocore.session
    require(boto3.__version__ == botocore.__version__ == '1.40.72', 'pinned SDK interpreter')
    members = botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
    require('IfNoneMatch' in members, 'SDK lacks immutable PutObject.IfNoneMatch')
    return dict(boto3=boto3.__version__, botocore=botocore.__version__, put_object_if_none_match=True)


def runtime_abi():
    release = dict(line.split('=',1) for line in Path('/etc/os-release').read_text().splitlines() if '=' in line)
    return dict(machine=platform.machine(), python=list(sys.version_info[:2]),
                os={k:release[k].strip('"') for k in ('ID','VERSION_ID')},
                libc=list(platform.libc_ver()), sdk=sdk_guard())


def validate_abi(abi):
    require(abi == dict(machine='x86_64', python=[3,12], os=dict(ID='ubuntu',VERSION_ID='24.04'),
        libc=['glibc','2.39'], sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True)), 'selected x86 Ubuntu/Python/native ABI and SDK')


def stage(s3, config, root):
    validate_config(config)
    scratch = None
    if SQ4:
        reserve = config['fixed']['scratch']
        available = os.statvfs(root).f_bavail*os.statvfs(root).f_frsize
        charged = sum(n for k,n in reserve.items() if k != 'cap_bytes')
        source_bytes = sum(p.stat().st_size for p in (root/'repo').rglob('*') if p.is_file())
        source_bytes += (root/'source.tar.gz').stat().st_size if (root/'source.tar.gz').exists() else 0
        bootstrap_bytes = sum(max(p.lstat().st_size,p.lstat().st_blocks*512) for tree in (root/'venv',root/'bootstrap')
                              for p in tree.rglob('*'))
        bootstrap = decode(read(root/'bootstrap/scratch.json'))
        require(bootstrap['closed'] is True and bootstrap['cap_exceeded'] is False and bootstrap['interval_seconds'] == 1
                and bootstrap['sample_count'] >= 2 and bootstrap['reserve'] == reserve
                and bootstrap['peak_bytes']+sum(reserve[k] for k in ('input_bytes','native_output_bytes','binary_bytes','auxiliary_bytes')) <= SQ4_SCRATCH_CAP,
                'SQ4 observed bootstrap overlap')
        observation = scratch_observation(root)
        remaining = reserve['input_bytes']+reserve['native_output_bytes']+reserve['binary_bytes']+reserve['auxiliary_bytes']
        require(available >= charged and source_bytes <= reserve['source_archive_bytes']
                and bootstrap_bytes <= reserve['bootstrap_bytes']
                and observation['whole_scratch_bytes']+remaining <= reserve['cap_bytes'], 'SQ4 scratch/source/input/output coexistence admission')
        require(all(not os.path.lexists(d['destination']) for d in config['inputs']), 'SQ4 retained input no overwrite')
        scratch = dict(config_sha256=config['native_config_sha256'], observation=observation,
            reserve=reserve, charged_bytes=charged, free_bytes_before=available,
            projected_peak_bytes=observation['whole_scratch_bytes']+remaining,
            source_archive_observed_bytes=source_bytes, bootstrap_observed_bytes=bootstrap_bytes, bootstrap=bootstrap)
    download(s3, config['binary'], root/BINARY_NAME)
    (root/BINARY_NAME).chmod(0o500)
    write(root/'native-config.json', encoded(config['native_config']))
    for descriptor in config['inputs']:
        if SQ4:
            require(scratch_observation(root)['whole_scratch_bytes']+descriptor['bytes'] <= SQ4_SCRATCH_CAP, 'SQ4 scratch before input write')
        download(s3, descriptor, descriptor['destination'])
    result = dict(binary=file_pin(root/BINARY_NAME), native_config=file_pin(root/'native-config.json'),
                  inputs={d['destination']:file_pin(d['destination']) for d in config['inputs']},
                  exact_six_inputs=True, compiler_used=False)
    if SQ4:
        del result['exact_six_inputs']
        result['exact_eighteen_inputs'] = True
        result['scratch'] = scratch
    write(root/'stage-receipt.json', encoded(result))
    return result


def cgroup_snapshot(group):
    return dict(path=str(group), observer_pid=os.getpid(),
                files={name:(group/name).read_text() for name in CGROUP_FILES})


def events(body):
    return {k:int(v) for k, v in (line.split() for line in body.splitlines())}


def validate_snapshot(snapshot):
    f = snapshot['files']
    require(set(f) == set(CGROUP_FILES), 'mandatory kernel resource counters')
    require(int(f['memory.max']) == CAPS['memory_bytes']
            and 0 <= int(f['memory.peak']) <= CAPS['memory_bytes'], 'native kernel memory cap/peak')
    require(int(f['memory.swap.max']) == int(f['memory.swap.peak']) == 0, 'native swap')
    quota, period = map(int, f['cpu.max'].split())
    require(quota == period and period > 0, 'CPU1 quota')
    require(int(f['pids.max']) == FIXED['tasks_max'] and int(f['pids.current']) == 0
            and not f['cgroup.procs'].strip() and events(f['cgroup.events'])['populated'] == 0, 'native descendants drained')
    require(events(f['cpu.stat'])['usage_usec'] >= 0, 'CPU evidence')
    for key in ('memory.events', 'memory.swap.events', 'pids.events'):
        counters = events(f[key])
        required = ('oom', 'oom_kill', 'oom_group_kill') if key == 'memory.events' else tuple(counters)
        require(counters and all(counters.get(k, -1) == 0 for k in required), 'resource failure: '+key)


def validate_resources(resource):
    require(resource['closed'] is True and resource['deadline_exceeded'] is False, 'resource closure/deadline')
    before, after = resource['before'], resource['after']
    require(before['path'] == after['path'], 'same original cgroup')
    for snapshot in (before, after):
        validate_snapshot(snapshot)
    d = resource['delegation']
    require(d['unit'] == SUPERVISOR_UNIT+'.service' and CONTROLLERS <= set(d['available'])
            and CONTROLLERS <= set(d['enabled']) and d['parent_process_ids'] == []
            and d['parent_type'] == 'domain' and d['observer_process_ids'] == [d['observer_pid']]
            and d['observer_pid'] == before['observer_pid'] == after['observer_pid']
            and str(Path(d['parent'])/'native') == before['path']
            and str(Path(d['parent'])/'supervisor') == d['observer'], 'dedicated delegated supervisor/native leaves')
    for key in ('memory.events', 'memory.swap.events', 'pids.events', 'cpu.stat'):
        a, b = events(before['files'][key]), events(after['files'][key])
        require(set(a) == set(b) and all(b[k] >= a[k] for k in a), 'resource counter regression')
    require(resource['cpu_affinity'] == [0] and resource['supervisor_outside_native_cgroup'] is True
            and resource['process_attached_before_exec'] is True
            and resource['pre_exec_limits_observed'] is True
            and resource['descendants_remaining_after_exit'] is False
            and 0 <= resource['elapsed_seconds'] <= CAPS['deadline_seconds'], 'original native resource authority')


def delegated_group(record, proc=Path('/proc/self/cgroup'), root=Path('/sys/fs/cgroup')):
    lines = proc.read_text().splitlines()
    require(len(lines) == 1 and lines[0].startswith('0::/'), 'unified original supervisor cgroup')
    relative = lines[0][4:]
    require(relative and all(p not in ('', '.', '..') for p in relative.split('/')), 'supervisor cgroup path')
    observer = root/relative
    parent = observer.parent
    # Only the dedicated service's delegated subtree is ours to configure.
    require(observer.name == 'supervisor' and parent.name == SUPERVISOR_UNIT+'.service', 'dedicated supervisor service required')
    record.update(unit=parent.name, parent=str(parent), observer=str(observer), observer_pid=os.getpid(),
        available=sorted((parent/'cgroup.controllers').read_text().split()),
        enabled_before=sorted((parent/'cgroup.subtree_control').read_text().split()),
        parent_process_ids=[int(p) for p in (parent/'cgroup.procs').read_text().split()],
        observer_process_ids=[int(p) for p in (observer/'cgroup.procs').read_text().split()],
        parent_type=(parent/'cgroup.type').read_text().strip())
    require(CONTROLLERS <= set(record['available']), 'required delegated cpu/memory/pids controllers unavailable')
    require(record['parent_type'] == 'domain' and record['parent_process_ids'] == []
            and record['observer_process_ids'] == [os.getpid()], 'delegated domain must keep supervisor in its leaf')
    (parent/'cgroup.subtree_control').write_text('+cpu +memory +pids')
    record['enabled'] = sorted((parent/'cgroup.subtree_control').read_text().split())
    require(CONTROLLERS <= set(record['enabled']), 'required native controllers not enabled')
    return parent/'native'


def create_group(group):
    for name, value in (('memory.max', str(CAPS['memory_bytes'])), ('memory.swap.max', '0'),
                        ('cpu.max', '100000 100000'), ('pids.max', str(FIXED['tasks_max'])),
                        ('memory.oom.group', '1')):
        require((group/name).is_file(), 'native controller interface unavailable: '+name)
        (group/name).write_text(value)


def drain_group(group):
    if (group/'cgroup.procs').read_text().strip():
        (group/'cgroup.kill').write_text('1')
    deadline = time.monotonic()+5
    while events((group/'cgroup.events').read_text())['populated']:
        require(time.monotonic() < deadline, 'cgroup drain deadline')
        time.sleep(.05)


def supervise(config, root, *, run_id):
    """Popen owns the original executable; the observer stays outside its cap."""
    command = [str(root/BINARY_NAME), 'check-fine-sq4' if SQ4 else 'check-fine-pack', str(root/'native-config.json'),
               config['native_config_sha256'], str(root/'screen/report.json')]
    (root/'screen').mkdir(exist_ok=False)
    receipt = dict(command=command, process_exit_code=None, process_started=False,
                   config_sha256=config['native_config_sha256'], binary=config['binary'], run_id=run_id)
    resource = dict(closed=False, deadline_exceeded=False, before=None, after=None,
                    cpu_affinity=[0], process_attached_before_exec=False,
                    supervisor_outside_native_cgroup=False, elapsed_seconds=0,
                    descendants_remaining_after_exit=True, delegation={}, pre_exec_limits_observed=False)
    cleanup = dict(drain_complete=False, cleanup_complete=False, output_durable=False)
    process, group, created, error = None, None, False, None
    started = time.monotonic()
    scratch = None
    if SQ4:
        temporary = root/'bootstrap/tmp'
        temporary.mkdir(parents=True, exist_ok=True)
        require(temporary.resolve() == temporary.absolute(), 'SQ4 charged temporary directory')
        scratch = dict(run_id=run_id, config_sha256=config['native_config_sha256'], cap_bytes=SQ4_SCRATCH_CAP,
            admission=decode(read(root/'stage-receipt.json'))['scratch'], sample_count=0, peak_bytes=0,
            interval_seconds=1, closed=False, cap_exceeded=False, closure_reserve_bytes=SQ4_CLOSURE_RESERVE)
    last_scratch = started-1
    def observe_scratch():
        nonlocal last_scratch
        observation = scratch_observation(root)
        scratch.update(last=observation, sample_count=scratch['sample_count']+1,
            peak_bytes=max(scratch['peak_bytes'],observation['whole_scratch_bytes']))
        last_scratch = time.monotonic()
        scratch['projected_closure_peak_bytes'] = observation['whole_scratch_bytes']+SQ4_CLOSURE_RESERVE
        if scratch['projected_closure_peak_bytes'] > SQ4_SCRATCH_CAP:
            scratch['cap_exceeded'] = True
            raise ValueError('SQ4 whole scratch 4GiB cap')
    old_term = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise InterruptedError('original supervisor interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if SQ4:
            observe_scratch()
            require(file_pin(root/'native-config.json') == pin(encoded(config['native_config']))
                    and file_pin(root/BINARY_NAME) == {k:config['binary'][k] for k in ('bytes','sha256')}, 'SQ4 pre-exec config/binary drift')
        group = delegated_group(resource['delegation'])
        group.mkdir(exist_ok=False); created = True; create_group(group)
        resource['before'] = cgroup_snapshot(group)
        validate_snapshot(resource['before'])
        resource['pre_exec_limits_observed'] = True
        observer = Path('/proc/self/cgroup').read_text()
        require(str(group).removeprefix('/sys/fs/cgroup') not in observer, 'supervisor outside native cap')
        resource['supervisor_outside_native_cgroup'] = True
        fd = os.open(group/'cgroup.procs', os.O_WRONLY)
        def attach():
            os.sched_setaffinity(0, {0})
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
        environment = dict(os.environ, BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1',
                           TOKIO_WORKER_THREADS='1', OMP_NUM_THREADS='1', AWS_MAX_ATTEMPTS='1')
        if SQ4:
            environment.update(TMPDIR=str(temporary), TMP=str(temporary), TEMP=str(temporary))
        try:
            with (root/'native.log').open('xb') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                    env=environment, preexec_fn=attach, pass_fds=(fd,), start_new_session=True)
                receipt.update(process_started=True, process_id=process.pid)
                resource['process_attached_before_exec'] = True
                while process.poll() is None:
                    resource['elapsed_seconds'] = time.monotonic()-started
                    if resource['elapsed_seconds'] >= CAPS['deadline_seconds']:
                        resource['deadline_exceeded'] = True
                        raise TimeoutError('native '+str(CAPS['deadline_seconds'])+'s deadline')
                    require(log.tell() <= 4*1024**2, 'native log cap')
                    if SQ4:
                        require(sum(p.lstat().st_size for p in (root/'screen').iterdir()) <= CAPS['output_bytes'], 'SQ4 whole native output cap')
                        if time.monotonic()-last_scratch >= 1:
                            observe_scratch()
                    time.sleep(.05)
                receipt['process_exit_code'] = process.wait()
                resource['descendants_remaining_after_exit'] = bool((group/'cgroup.procs').read_text().strip())
                log.flush(); os.fsync(log.fileno())
        finally:
            os.close(fd)
    except BaseException as failure:
        error = failure
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        resource['elapsed_seconds'] = time.monotonic()-started
        try:
            if created:
                drain_group(group)
                cleanup['drain_complete'] = True
                if process is not None:
                    receipt['process_exit_code'] = process.wait(timeout=5)
                resource['after'] = cgroup_snapshot(group); resource['closed'] = True
                group.rmdir(); cleanup['cleanup_complete'] = True
            for name in (('native.log', *SQ4_OUTPUTS) if SQ4 else ('native.log', 'screen/report.json', 'screen/report.permutations.json', 'screen/report.prefix.jsonl')):
                path = root/name
                if path.exists():
                    with regular(path) as output:
                        os.fsync(output.fileno())
            sync_directory(root/'screen'); sync_directory(root)
            cleanup['output_durable'] = True
        except BaseException as failure:
            error = error or failure
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                receipt['process_exit_code'] = process.wait(timeout=5)
        if error is not None:
            receipt['error'] = dict(type=type(error).__name__, message=str(error))
        if SQ4 and (root/'screen/report.json').exists():
            try:
                receipt['report_sha256'] = file_pin(root/'screen/report.json')['sha256']
            except BaseException as failure:
                receipt['error'] = dict(type=type(failure).__name__, message=str(failure))
        if SQ4:
            signal.signal(signal.SIGTERM, old_term)
            scratch_room(root, SQ4_CLOSURE_RESERVE)
        write(root/'native-exit.json', encoded(receipt))
        write(root/'resources.json', encoded(resource))
        write(root/'cleanup.json', encoded(cleanup))
        if SQ4:
            try:
                observe_scratch()
            except BaseException as failure:
                scratch['error'] = dict(type=type(failure).__name__, message=str(failure))
            scratch.update(closed=True, process_exit_code=receipt['process_exit_code'],
                           report_sha256=receipt.get('report_sha256'))
            require(len(encoded(scratch)) <= 65536, 'SQ4 scratch receipt closure reserve')
            scratch_room(root, SQ4_CLOSURE_RESERVE)
            write(root/'scratch.json', encoded(scratch))
        signal.signal(signal.SIGTERM, old_term)
    return receipt


def validate_sq4_result(root, config):
    validate_config(config)
    sq4_qualification(config, root, collected=True)
    validate_abi(decode(read(root/'runtime-abi.json')))
    binary = {k:config['binary'][k] for k in ('bytes','sha256')}
    require(file_pin(root/'native-config.json') == pin(encoded(config['native_config']))
            and file_pin(root/BINARY_NAME) == binary, 'SQ4 executed config/binary bytes')
    receipt, resource, cleanup = (decode(read(root/n)) for n in ('native-exit.json','resources.json','cleanup.json'))
    command = receipt['command']; original = Path(command[0]).parent.parent
    require(original.is_absolute() and original.resolve() == original and command == [str(original/BINARY_NAME),
        'check-fine-sq4', str(original/'native-config.json'), config['native_config_sha256'], str(original/'screen/report.json')]
        and receipt['config_sha256'] == config['native_config_sha256'] and receipt['binary'] == config['binary']
        and type(receipt['process_exit_code']) is int and receipt['process_exit_code'] == 0
        and receipt['process_started'] is True and 'error' not in receipt, 'SQ4 original exact native command/exit0')
    validate_resources(resource)
    require(all(cleanup.get(k) is True for k in ('drain_complete','cleanup_complete','output_durable')), 'SQ4 native cleanup/durability')
    stage_receipt = decode(read(root/'stage-receipt.json'))
    require(set(stage_receipt) == {'binary','native_config','inputs','exact_eighteen_inputs','compiler_used','scratch'}
        and stage_receipt['binary'] == binary and stage_receipt['native_config'] == pin(encoded(config['native_config']))
        and stage_receipt['inputs'] == {d['destination']:{k:d[k] for k in ('bytes','sha256')} for d in config['inputs']}
        and stage_receipt['exact_eighteen_inputs'] is True and stage_receipt['compiler_used'] is False, 'SQ4 authenticated opaque staging')
    scratch = decode(read(root/'scratch.json'))
    admission = scratch['admission']; reserve = config['fixed']['scratch']
    require(scratch['run_id'] == receipt['run_id'] and scratch['config_sha256'] == config['native_config_sha256']
        and scratch['cap_bytes'] == SQ4_SCRATCH_CAP and scratch['closed'] is True and scratch['cap_exceeded'] is False
        and scratch['process_exit_code'] == 0 and scratch['interval_seconds'] == 1 and scratch['sample_count'] >= 2
        and 'error' not in scratch and admission == stage_receipt['scratch'] and admission['reserve'] == reserve
        and admission['config_sha256'] == config['native_config_sha256']
        and admission['charged_bytes'] == sum(n for k,n in reserve.items() if k != 'cap_bytes')
        and admission['charged_bytes'] <= admission['free_bytes_before']
        and admission['projected_peak_bytes'] <= SQ4_SCRATCH_CAP
        and admission['source_archive_observed_bytes'] <= reserve['source_archive_bytes']
        and admission['bootstrap_observed_bytes'] <= reserve['bootstrap_bytes']
        and admission['bootstrap']['closed'] is True and admission['bootstrap']['cap_exceeded'] is False
        and admission['bootstrap']['reserve'] == reserve and admission['bootstrap']['sample_count'] >= 2
        and scratch['closure_reserve_bytes'] == SQ4_CLOSURE_RESERVE
        and scratch['projected_closure_peak_bytes'] == scratch['last']['whole_scratch_bytes']+SQ4_CLOSURE_RESERVE <= SQ4_SCRATCH_CAP
        and 0 <= scratch['last']['whole_scratch_bytes'] <= scratch['peak_bytes'] <= SQ4_SCRATCH_CAP
        and set(scratch['last']['roots']) == {str(original),str(SQ4_INPUT_ROOT)}
        and sum(scratch['last']['roots'].values()) == scratch['last']['whole_scratch_bytes'], 'SQ4 whole-worker scratch observations')
    require({p.name for p in (root/'screen').iterdir() if not p.name.endswith('.gz')} == {Path(n).name for n in SQ4_OUTPUTS}, 'SQ4 exact native output closure')
    require(sum(file_pin(root/n)['bytes'] for n in SQ4_OUTPUTS) <= CAPS['output_bytes'], 'SQ4 native output cap')
    body = read(root/'screen/report.json', 8192); report = decode(body)
    require(receipt['report_sha256'] == scratch['report_sha256'] == sha(body)
        and report['schema'] == 'borsuk-fixed-sq4-report-v1' and report['codec'] == 'borsuk-sq4-nearest17-original-coefficients-v1'
        and report['config_sha256'] == config['native_config_sha256'] and report['source_identity_sha256'] == SOURCE_ID
        and report['complete'] is True and type(report['queries']) is int and report['queries'] == 128
        and report['status'] in ('SURVIVED_CONSUMED_PANELS','REJECT') and report['standalone_authority'] is False
        and report['quality_or_performance_claim'] is False and report['requires_matching_supervisor_exit_receipt'] is True, 'SQ4 independently bound terminal report')
    details = report['details']; native = config['native_config']; truth = [p['truth'] for p in native['panels']]
    require(details['rows'] == 100000 and details['dimensions'] == 768 and details['truth'] == truth
        and details['frozen_original_authority'] is True and details['caps'] == CAPS
        and details['whole_process_supervisor_required'] is True and 0 <= details['operations'] <= CAPS['operations']
        and details['pair_payload_bytes'] == 79200000 and 0 <= details['modeled_peak_bytes'] <= CAPS['memory_bytes']
        and 79200000 <= details['modeled_output_bytes'] <= CAPS['output_bytes'], 'SQ4 frozen native authority/resources')
    def authenticate(descriptor, name):
        require(descriptor == dict(path=str(original/name), **file_pin(root/name)), 'SQ4 native closure pin: '+name)
    freeze_name = 'screen/report.sq4-freeze.json'
    authenticate(details['freeze'], freeze_name)
    freeze = decode(read(root/freeze_name, 8*1024**2))
    require(freeze['schema'] == 'borsuk-fixed-sq4-freeze-v1' and freeze['config_sha256'] == config['native_config_sha256']
        and freeze['source_identity_sha256'] == SOURCE_ID and freeze['truth_opened'] is False
        and freeze['original_seal'] == native['original_seal'] and freeze['truth'] == truth, 'SQ4 freeze before truth')
    authenticate(freeze['payload_seal'], 'screen/report.sq4-payloads.json')
    authenticate(freeze['nomination_prefix'], 'screen/report.sq4-prefix.jsonl')
    require(file_pin(root/'screen/report.sq4-prefix.jsonl') == {k:native['prefix'][k] for k in ('bytes','sha256')}, 'SQ4 consumed original nomination prefix')
    seal = decode(read(root/'screen/report.sq4-payloads.json', 8*1024**2))
    require(seal['schema'] == 'borsuk-fixed-sq4-payload-seal-v1' and seal['config_sha256'] == config['native_config_sha256']
        and seal['source_identity_sha256'] == SOURCE_ID and seal['codec'] == report['codec']
        and seal['original_seal'] == native['original_seal'] and seal['queries_opened'] is seal['truth_opened'] is False
        and seal['payloads'] == freeze['payloads'] and len(seal['payloads']) == 2, 'SQ4 paired payload seal')
    for i,payload in enumerate(seal['payloads']):
        authenticate(payload['payload'], f'screen/report.sq4-{i}.bin')
        require(payload['payload']['bytes'] == 39600000 and payload['original_root'] == native['panels'][i]['root']
            and payload['codec'] == report['codec'] and payload['rows'] == 100000 and payload['dimensions'] == 768
            and payload['row_bytes'] == 396 and payload['group_rows'] == 16, 'SQ4 payload identity/geometry')
    require(len(freeze['results']) == 128, 'SQ4 all128 sealed results')
    envelopes = True
    for i,descriptor in enumerate(freeze['results']):
        name = f'screen/report.sq4-result-{i}.json'; authenticate(descriptor, name)
        query = decode(read(root/name, 8*1024**2)); plan = query['plan']; sq4 = query['sq4']
        fits = sq4['verified_bytes'] <= 16*1024**2
        require(query['schema'] == 'borsuk-fixed-sq4-query-v1' and query['dataset'] == native['panels'][i//64]['dataset']
            and type(query['ordinal']) is int and query['ordinal'] == i%64 and query['nominees_retained'] is True
            and query['original_cover_contained'] is True and query['truth_opened'] is False
            and query['sq8_reference_serving_eligible'] is False and sq4['fetched_ids'] == query['sq8_reference']['fetched_ids']
            and type(sq4['range_reads']) is int and 1 <= sq4['range_reads'] <= 32
            and type(sq4['verified_bytes']) is int and 0 < sq4['verified_bytes'] == plan['candidate_bytes']
            and plan['envelope_fits'] is fits, 'SQ4 authenticated same-population result/envelope')
        envelopes &= fits
    summaries = details['summaries']
    require(len(summaries) == 2 and details['all128_envelopes_fit'] is envelopes, 'SQ4 native envelope summary')
    quality = True
    for panel,summary in zip(native['panels'], summaries):
        returned = summary['sq4_returned']; mean, p05 = returned['mean_recall'], returned['p05_hits']
        require(summary['dataset'] == panel['dataset'] and type(mean) in (int,float) and 0 <= mean <= 1
                and type(p05) is int and 0 <= p05 <= 100, 'SQ4 native quality summary')
        quality &= mean >= .98 and p05 >= 95
    require(report['status'] == ('SURVIVED_CONSUMED_PANELS' if quality and envelopes else 'REJECT'), 'SQ4 completed disposition')
    return dict(status=report['status'], valid_diagnostic=True, report_sha256=sha(body),
        native_config_sha256=config['native_config_sha256'], binary=binary, summaries=summaries,
        all128_envelopes_fit=envelopes, quality_or_performance_claim=False)


def validate_result(root, config):
    if SQ4:
        return validate_sq4_result(root, config)
    validate_config(config)
    validate_abi(decode(read(root/'runtime-abi.json')))
    require(file_pin(root/'native-config.json') == pin(encoded(config['native_config'])), 'native config bytes')
    require(file_pin(root/BINARY_NAME) == BINARY_PIN, 'executed binary bytes')
    exit_receipt = decode(read(root/'native-exit.json'))
    resource, cleanup = (decode(read(root/name)) for name in ('resources.json', 'cleanup.json'))
    require(type(exit_receipt['process_exit_code']) is int and exit_receipt['process_exit_code'] == 0
            and exit_receipt['process_started'] is True and 'error' not in exit_receipt, 'original native exit zero')
    command = exit_receipt['command']
    expected_root = Path(command[0]).parent.parent
    require(command == [str(expected_root/BINARY_NAME), 'check-fine-pack', str(expected_root/'native-config.json'),
                config['native_config_sha256'], str(expected_root/'screen/report.json')]
            and exit_receipt['config_sha256'] == config['native_config_sha256']
            and exit_receipt['binary'] == config['binary'], 'original executable/config/command binding')
    validate_resources(resource)
    require(all(cleanup.get(k) is True for k in ('drain_complete', 'cleanup_complete', 'output_durable')), 'native cleanup/durability')
    stage_receipt = decode(read(root/'stage-receipt.json'))
    require(stage_receipt == dict(binary=BINARY_PIN, native_config=pin(encoded(config['native_config'])),
            inputs={d['destination']:{k:d[k] for k in ('bytes', 'sha256')} for d in config['inputs']},
            exact_six_inputs=True, compiler_used=False), 'authenticated original staging')
    body = read(root/'screen/report.json', CAPS['output_bytes'])
    report = decode(body)
    require(report['schema'] == 'borsuk-fine-pack-diagnostic-v1' and report['complete'] is True
            and type(report['queries']) is int and report['queries'] == 128
            and report['config_sha256'] == config['native_config_sha256']
            and report['diagnostic_source_sha256'] == DIAGNOSTIC_SOURCES, 'native complete report binding')
    for key, expected in (('standalone_authority', False), ('requires_matching_supervisor_exit_receipt', True),
            ('quality_or_performance_claim', False), ('truth_opened', False), ('requests_opened', False),
            ('sq8_canonical_pq_bodies_opened', False)):
        require(report[key] is expected, 'native report scope: '+key)
    details = report['details']
    require(details['original_seal'] == config['native_config']['original_seal']
            and details['original_seal_bytes_sha256'] == INPUT_PINS[4][2]
            and details['caps'] == CAPS and 0 <= details['operations'] <= CAPS['operations']
            and 0 <= details['wall_ms'] <= 300000, 'native method/resources binding')
    for key, name in (('permutation_seal', 'screen/report.permutations.json'), ('prefix', 'screen/report.prefix.jsonl')):
        require(details[key] == dict(file_pin(root/name), path=str(expected_root/name)), 'native companion pin: '+key)
    require(file_pin(root/'screen/report.prefix.jsonl') == {k:config['native_config']['prefix'][k] for k in ('bytes', 'sha256')}, 'consumed prefix')
    permutation = decode(read(root/'screen/report.permutations.json', CAPS['output_bytes']))
    native = config['native_config']
    require(permutation['schema'] == 'borsuk-fine-pack-permutations-v1'
            and permutation['config_sha256'] == config['native_config_sha256']
            and permutation['diagnostic_source_sha256'] == DIAGNOSTIC_SOURCES
            and permutation['original_seal'] == native['original_seal']
            and permutation['nomination_prefix'] == native['prefix']
            and permutation['panels'] == native['panels'] and permutation['group_rows'] == 16
            and permutation['groups_per_pack'] == 42 and permutation['query_blind'] is True
            and permutation['original_trace_source_identity_sha256'] == details['original_trace_source_identity_sha256'], 'joint permutation binding')
    require(len(permutation['old_to_new']) == len(permutation['permutation_sha256_le_u32']) == 2, 'both permutations')
    for mapping, digest in zip(permutation['old_to_new'], permutation['permutation_sha256_le_u32']):
        require(len(mapping) == 6250 and all(type(v) is int for v in mapping)
                and sorted(mapping) == list(range(6250)), 'group bijection')
        require(sha(b''.join(v.to_bytes(4, 'little') for v in mapping)) == digest, 'permutation LE32 hash')
    replay = details['replay']
    require(len(replay['queries']) == 128, 'all 128 plans completed')
    passes, maxima = [0, 0], [0, 0]
    for index, q in enumerate(replay['queries']):
        panel = index//64
        require(q['dataset'] == native['panels'][panel]['dataset'] and q['ordinal'] == index%64
                and q['all_nominees_retained'] is True and 1 <= q['nominees'] <= 1024, '128 frozen plan identities')
        ranges = q['ranges']
        require(1 <= len(ranges) <= 32, 'explicit cover cap32')
        end, count = 0, 0
        for r in ranges:
            require(set(r) == {'start', 'end'} and all(type(r[k]) is int for k in r)
                    and end <= r['start'] < r['end'] <= 100000*780
                    and r['start']%780 == r['end']%780 == 0, 'cover range geometry')
            end = r['end']; count += r['end']-r['start']
        fits = count <= 16*1024**2
        require(type(q['planned_bytes']) is int and q['planned_bytes'] == count and q['fits'] is fits, 'cover bytes/disposition')
        passes[panel] += fits; maxima[panel] = max(maxima[panel], count)
    survived = passes == [64, 64]
    require(replay['per_panel_passes'] == passes and replay['per_panel_max_bytes'] == maxima
            and replay['survived'] is survived and report['status'] == ('SURVIVED_NECESSARY_LOCALITY' if survived else 'REJECT'), 'completed locality disposition')
    require(sum(file_pin(root/n)['bytes'] for n in ARTIFACTS if n.startswith('screen/')) <= CAPS['output_bytes'], 'native total output cap')
    return dict(status=report['status'], valid_diagnostic=True, report_sha256=sha(body),
                native_config_sha256=config['native_config_sha256'], binary=BINARY_PIN,
                per_panel_passes=passes, per_panel_max_bytes=maxima, quality_or_performance_claim=False)


def publish(s3, root, prefix, terminal):
    """Retain every present raw body, read back, then publish the terminal."""
    if SQ4:
        scratch_room(root, SQ4_CLOSURE_RESERVE)
    artifacts = {}
    for name in ARTIFACTS:
        path = root/name
        if path.exists():
            body = read(path, 64*1024**2)
            ident = pin(body)
            key = prefix+'/artifacts/'+name
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch='*')
            response = s3.get_object(Bucket=BUCKET, Key=key)
            with response['Body'] as source:
                require(pin(source.read(ident['bytes']+1)) == ident, 'upload readback: '+name)
            artifacts[name] = ident
    terminal['artifacts'] = artifacts
    if SQ4:
        observation = scratch_room(root, 4*1024**2)
        terminal['scratch_closure'] = dict(before=observation, cap_bytes=SQ4_SCRATCH_CAP,
            reserve_bytes=4*1024**2, projected_peak_bytes=observation['whole_scratch_bytes']+4*1024**2)
    raw = encoded(terminal)
    if SQ4:
        require(len(raw)+4096 <= 4*1024**2, 'SQ4 terminal closure reserve')
        scratch_room(root, 4*1024**2)
    write(root/'terminal.json', raw)
    if SQ4:
        scratch_room(root, 0)  # The successful S3 marker follows the actual final scan.
    s3.put_object(Bucket=BUCKET, Key=prefix+'/terminal.json', Body=raw, IfNoneMatch='*')
    return terminal


def remote(repo, root, commit, archive_sha, prefix, config_sha):
    import urllib.request
    import boto3
    from botocore.config import Config
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'remote prefix')
    sdk_guard()
    s3 = boto3.client('s3', region_name=REGION, config=Config(retries={'total_max_attempts':1}))
    request = urllib.request.Request('http://169.254.169.254/latest/api/token', method='PUT',
                                    headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
    with urllib.request.urlopen(request, timeout=5) as response:
        token = response.read(4096).decode()
    request = urllib.request.Request('http://169.254.169.254/latest/meta-data/instance-id',
                                    headers={'X-aws-ec2-metadata-token':token})
    with urllib.request.urlopen(request, timeout=5) as response:
        instance_id = response.read(128).decode()
    terminal = dict(schema=SCHEMA, source_commit=commit, source_archive_sha256=archive_sha,
        instance_id=instance_id, prefix=prefix, config_sha256=config_sha,
        status='failed', phase='execution', exit_code=96, original_exit_code=None,
        disposition='INVALID', artifact_roster_sha256=ROSTER_SHA)
    try:
        abi = runtime_abi(); write(root/'runtime-abi.json', encoded(abi)); validate_abi(abi)
        config_body = read(repo/CONFIG)
        require(sha(config_body) == config_sha, 'root config authentication')
        config = decode(config_body); validate_config(config, repo)
        proof = preflight(repo)
        write(root/'config.json', config_body); write(root/'source-qualification.json', encoded(proof))
        if SQ4:
            for r in config['native_qualification']:
                write(root/'qualification'/Path(r['path']).name, read(repo/r['path']))
        write(root/'cpu.txt', subprocess.check_output(['lscpu']))
        stage(s3, config, root)
        receipt = supervise(config, root, run_id=prefix+'/'+instance_id)
        terminal['original_exit_code'] = receipt['process_exit_code']
        result = validate_result(root, config)
        terminal.update(status='complete', phase='complete', exit_code=0, disposition=result['status'],
                        result=result, qualification=proof)
    except BaseException as error:
        terminal['error'] = dict(type=type(error).__name__, message=str(error))
    finally:
        if (root/'native-exit.json').exists():
            terminal['original_exit_code'] = decode(read(root/'native-exit.json'))['process_exit_code']
        closed_log = read(root/'run.log', 4*1024**2) if SQ4 else read(root/'run.log')
        if SQ4:
            scratch_room(root, SQ4_CLOSURE_RESERVE)
        write(root/'run-closed.log', closed_log)
        publish(s3, root, prefix, terminal)
    return terminal['exit_code']


def replay(out):
    terminal = decode(read(out/'aws-terminal.json'))
    reservation, launch, close = (decode(read(out/name)) for name in ('aws-reservation.json', 'aws-launch.json', 'aws-closeout.json'))
    require(close['state'] == 'terminated' and close['nodes'] == launch['nodes']
            == {'0':{'instance_id':launch['instance_id']}}, 'same acknowledged instance terminated/waited')
    require(terminal['schema'] == reservation['schema'] == SCHEMA
            and terminal['instance_id'] == launch['instance_id'] and terminal['prefix'] == launch['prefix']
            and re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', terminal['prefix']), 'terminal attempt/instance identity')
    for key in ('source_commit', 'source_archive_sha256'):
        require(terminal[key] == reservation[key] == launch[key], 'terminal source binding: '+key)
    require(terminal['config_sha256'] == reservation['config_sha256']
            and terminal['artifact_roster_sha256'] == ROSTER_SHA, 'terminal frozen config/roster')
    require(set(terminal['artifacts']) <= set(ARTIFACTS), 'terminal artifact roster')
    for name, identity in terminal['artifacts'].items():
        body_pin(identity)
        require(file_pin(out/name) == identity, 'collected full artifact hash: '+name)
        if (out/(name+'.gz')).exists():
            with regular(out/(name+'.gz')) as compressed, gzip.GzipFile(fileobj=compressed) as source:
                require(pin(source.read(identity['bytes']+1)) == identity, 'transport gzip hash: '+name)
    if terminal['status'] != 'complete':
        return dict(valid_diagnostic=False, status='INVALID', original_exit_code=terminal['original_exit_code'])
    require(terminal['phase'] == 'complete' and type(terminal['exit_code']) is int
            and type(terminal['original_exit_code']) is int
            and terminal['exit_code'] == terminal['original_exit_code'] == 0
            and set(terminal['artifacts']) == set(ARTIFACTS), 'complete original terminal/roster')
    if SQ4:
        closure = terminal['scratch_closure']
        require(closure['cap_bytes'] == SQ4_SCRATCH_CAP and closure['reserve_bytes'] == 4*1024**2
                and closure['projected_peak_bytes'] == closure['before']['whole_scratch_bytes']+closure['reserve_bytes'] <= SQ4_SCRATCH_CAP
                and sum(closure['before']['roots'].values()) == closure['before']['whole_scratch_bytes'], 'SQ4 final terminal scratch closure')
    raw = read(out/'config.json')
    require(sha(raw) == terminal['config_sha256'], 'collected config hash')
    config = decode(raw); validate_config(config)
    proof = decode(read(out/'source-qualification.json'))
    require(proof == reservation['qualification'] == terminal['qualification']
            and proof['config_sha256'] == sha(raw) and proof['code_sha256'] == config['code_sha256']
            and proof['native_qualification'] == config['native_qualification'], 'original qualification binding')
    result = validate_result(out, config)
    require(decode(read(out/'native-exit.json'))['run_id'] == terminal['prefix']+'/'+terminal['instance_id'], 'original run ID')
    require(result == terminal['result'] and terminal['disposition'] == result['status'], 'original result receipt')
    return result


def collect(s3, prefix, out, instance_id, commit, digest):
    response = s3.get_object(Bucket=BUCKET, Key=prefix+'/terminal.json')
    with response['Body'] as source:
        raw = source.read(4*1024**2+1)
    require(len(raw) <= 4*1024**2, 'terminal cap')
    write(out/'aws-terminal.json', raw)
    terminal = decode(raw)
    require(terminal['instance_id'] == instance_id and terminal['source_commit'] == commit
            and terminal['source_archive_sha256'] == digest and terminal['prefix'] == prefix
            and terminal['schema'] == SCHEMA and set(terminal['artifacts']) <= set(ARTIFACTS), 'original terminal identity')
    for name, identity in terminal['artifacts'].items():
        body_pin(identity)
        response = s3.get_object(Bucket=BUCKET, Key=prefix+'/artifacts/'+name)
        with response['Body'] as source:
            body = source.read(identity['bytes']+1)
        require(pin(body) == identity, 'remote full artifact hash: '+name)
        write(out/name, body); write(out/(name+'.gz'), gzip.compress(body, mtime=0))
    result = replay(out)
    write(out/'collection-replay.json', encoded(result))
    return terminal


def setup_self_check():
    """Filesystem controller model: refuse before exec, never touch shared roots."""
    import tempfile
    from unittest.mock import patch
    with tempfile.TemporaryDirectory(prefix='fine-pack-delegation-check-') as tmp:
        root=Path(tmp)/'cgroup';parent=root/'system.slice'/(SUPERVISOR_UNIT+'.service')
        observer=parent/'supervisor';observer.mkdir(parents=True)
        proc=Path(tmp)/'proc';proc.write_text('0::/system.slice/'+SUPERVISOR_UNIT+'.service/supervisor\n')
        initial={'cgroup.controllers':'cpu memory pids\n','cgroup.subtree_control':'',
                 'cgroup.procs':'','cgroup.type':'domain\n'}
        for name,body in initial.items():(parent/name).write_text(body)
        (observer/'cgroup.procs').write_text(str(os.getpid()))
        original_write=Path.write_text
        writes=[]
        def enable(path,body,*args,**kwargs):
            writes.append(str(path))
            require(path==parent/'cgroup.subtree_control','only dedicated parent controller write')
            require(body=='+cpu +memory +pids','exact required controllers')
            return original_write(path,'cpu memory pids\n',*args,**kwargs)
        with patch.object(Path,'write_text',enable):
            record={};group=delegated_group(record,proc,root)
        require(group==parent/'native' and writes==[str(parent/'cgroup.subtree_control')]
                and record['enabled']==['cpu','memory','pids'],'delegated controller activation')
        for name,body,label in [('cgroup.controllers','memory pids\n','CPU unavailable'),
                               ('cgroup.procs',str(os.getpid()),'internal observer'),
                               ('cgroup.type','domain threaded\n','threaded domain')]:
            old=(parent/name).read_text();(parent/name).write_text(body)
            with patch.object(Path,'write_text',side_effect=AssertionError('unsafe controller write')):
                try:delegated_group({},proc,root)
                except ValueError:pass
                else:raise AssertionError('accepted '+label)
            (parent/name).write_text(old)
        proc.write_text('0::/system.slice/cloud-final.service\n')
        try:delegated_group({},proc,root)
        except ValueError:pass
        else:raise AssertionError('undelegated shared hierarchy accepted')
        group.mkdir()
        for name in ('memory.max','memory.swap.max'):(group/name).write_text('0')
        try:create_group(group)
        except ValueError as error:require('cpu.max' in str(error),'missing controller diagnostic')
        else:raise AssertionError('missing native CPU interface accepted')
    print('PASS delegation model: explicit controllers; unavailable/internal-PID/threaded/shared-parent/missing-interface refusals before exec')


def self_check(real_cgroup=False):
    """Real fake-native original processes; cgroup, SDK, IMDS and AWS mocked.

    Run under CPU1/256MiB/swap0/120s. No corpus, graph, native ANN or network.
    """
    import copy
    import io
    import tempfile
    from datetime import datetime, timezone
    from unittest.mock import Mock, patch
    from contextlib import ExitStack
    module = sys.modules[__name__]
    shared = lifecycle()
    setup_self_check()
    # Actual installed service model, no client/credentials/network.
    require(sdk_guard() == dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True), 'real installed immutable SDK model')
    import botocore.session
    old_model=Mock()
    old_model.get_service_model.return_value.operation_model.return_value.input_shape.members={}
    with patch.object(botocore.session,'get_session',return_value=old_model):
        try:
            sdk_guard()
        except ValueError as error:
            require('IfNoneMatch' in str(error),'old SDK model rejection')
        else:
            raise AssertionError('old model without immutable PUT accepted')
    old_path=Path('/tmp/borsuk-canary-botocore-1.34.46-s3.json')
    official_old_checked=False
    if old_path.exists():
        from botocore.model import ServiceModel
        old_body=read(old_path)
        require(sha(old_body)=='0e0760c9ae10e9f8af267b0d9708fefabbaec023370f0044bbecc51565f05699','authenticated official old service model')
        service=ServiceModel(decode(old_body),service_name='s3')
        session=Mock();session.get_service_model.return_value=service
        with patch.object(botocore.session,'get_session',return_value=session):
            try:sdk_guard()
            except ValueError as error:require('IfNoneMatch' in str(error),'official old model refused')
            else:raise AssertionError('official old SDK model accepted')
        official_old_checked=True
    fake = '''#!/usr/bin/env python3
import hashlib,json,os,sys,time
from pathlib import Path
_,mode,config_path,config_sha,out=sys.argv
assert mode=='check-fine-pack'
if os.environ.get('BORSUK_FINE_PACK_REAL_CGROUP')=='1':
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 assert group.name=='native' and os.sched_getaffinity(0)=={0}
 assert int((group/'memory.max').read_text())==268435456
 assert int((group/'memory.swap.max').read_text())==0
 assert (group/'cpu.max').read_text().split()==['100000','100000']
 assert int((group/'pids.max').read_text())==32
c=json.loads(Path(config_path).read_bytes());p=Path(out)
def sha(b):return hashlib.sha256(b).hexdigest()
def emit(path,value):
 b=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
 with path.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
 return {'path':str(path),'bytes':len(b),'sha256':sha(b)}
sources=SOURCES
trace='0'*64
maps=[list(range(6250)),list(reversed(range(6250)))]
perms=emit(p.with_suffix('.permutations.json'),{'schema':'borsuk-fine-pack-permutations-v1',
 'config_sha256':config_sha,'diagnostic_source_sha256':sources,'old_to_new':maps,
 'permutation_sha256_le_u32':[sha(b''.join(v.to_bytes(4,'little') for v in m)) for m in maps],
 'original_trace_source_identity_sha256':trace,'original_seal':c['original_seal'],
 'nomination_prefix':c['prefix'],'panels':c['panels'],'group_rows':16,'groups_per_pack':42,'query_blind':True})
prefix=p.with_suffix('.prefix.jsonl');body=Path(c['prefix']['path']).read_bytes()
with prefix.open('xb') as f:f.write(body);f.flush();os.fsync(f.fileno())
prefix_pin={'path':str(prefix),'bytes':len(body),'sha256':sha(body)}
reject=os.environ.get('FINE_PACK_FAKE')=='reject'
queries=[]
for panel in c['panels']:
 for ordinal in range(64):
  size=780*22000 if reject and ordinal==0 else 780
  queries.append({'dataset':panel['dataset'],'ordinal':ordinal,'nominees':1,'all_nominees_retained':True,
   'ranges':[{'start':0,'end':size}],'planned_bytes':size,'fits':size<=16*1024**2})
emit(p,{'schema':'borsuk-fine-pack-diagnostic-v1','complete':True,'queries':128,
 'status':'REJECT' if reject else 'SURVIVED_NECESSARY_LOCALITY','config_sha256':config_sha,
 'diagnostic_source_sha256':sources,'standalone_authority':False,'requires_matching_supervisor_exit_receipt':True,
 'quality_or_performance_claim':False,'truth_opened':False,'requests_opened':False,'sq8_canonical_pq_bodies_opened':False,
 'details':{'original_seal':c['original_seal'],'original_seal_bytes_sha256':c['original_seal']['sha256'],
 'original_trace_source_identity_sha256':trace,'caps':c['caps'],'operations':1,'wall_ms':1,
 'prefix':prefix_pin,'permutation_seal':perms,'replay':{'queries':queries,'survived':not reject,
 'per_panel_passes':[63,63] if reject else [64,64],'per_panel_max_bytes':[780*22000]*2 if reject else [780,780]}}})
fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
if os.environ.get('FINE_PACK_FAKE')=='deadline':time.sleep(10)
sys.exit(17 if os.environ.get('FINE_PACK_FAKE')=='nonzero' else 0)
'''.replace('SOURCES', repr(DIAGNOSTIC_SOURCES)).encode()
    fake_pin = pin(fake)
    def failure(call, label):
        try:
            call()
        except (ValueError, FileNotFoundError, FileExistsError, OSError, KeyError, AssertionError):
            return
        raise AssertionError('accepted '+label)
    with tempfile.TemporaryDirectory(prefix='fine-pack-check-') as tmp, ExitStack() as stack:
        base = Path(tmp)
        inputs = base/'inputs'
        inputs.mkdir()
        input_pins = []
        for i in range(6):
            path, body = inputs/str(i), ('synthetic body '+str(i)+'\n').encode()
            write(path, body); input_pins.append((str(path), len(body), sha(body)))
        stack.enter_context(patch.object(module, 'INPUT_PINS', tuple(input_pins)))
        stack.enter_context(patch.object(module, 'BINARY_PIN', fake_pin))
        descriptors = [dict(path=p, bytes=n, sha256=h) for p,n,h in input_pins]
        native = dict(schema='borsuk-fine-pack-config-v1', caps=copy.deepcopy(CAPS),
            panels=[dict(dataset=dataset, root=descriptors[i*2], graph=descriptors[i*2+1],
                identity=dict(rows=100000, dimensions=768, generation=i+1, source=[0]*32, layout=[1]*32, pq=[2]*32))
                for i,dataset in enumerate(('relaion','cohere'))], original_seal=descriptors[4], prefix=descriptors[5])
        paths = sorted([str(CONFIG), *CODE, *(r['path'] for r in RECEIPTS)])
        config = dict(schema=SCHEMA, authority_pending=False, fixed=copy.deepcopy(FIXED), native_config=native,
            native_config_sha256=sha(encoded(native)), binary=dict(fake_pin, key=BINARY_KEY),
            inputs=[dict(destination=d['path'], key='synthetic/'+str(i), bytes=d['bytes'], sha256=d['sha256']) for i,d in enumerate(descriptors)],
            native_qualification=list(RECEIPTS), code_sha256={n:file_pin(n)['sha256'] for n in CODE},
            source_archive_paths=paths, source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        validate_config(config)
        failures = 0
        for key, value in [('authority_pending',True), ('native_config_sha256','0'*64), ('extra',True),
                           ('source_archive_paths_sha256','0'*64), ('native_qualification',[])]:
            bad=copy.deepcopy(config);bad[key]=value
            failure(lambda:validate_config(bad), 'config '+key); failures+=1
        bad=copy.deepcopy(config);bad['fixed']['native_caps']['memory_bytes']*=2
        failure(lambda:validate_config(bad), 'resource tuning'); failures+=1
        bad=copy.deepcopy(config);bad['inputs'][0]['destination']='/tmp/forbidden-requests'
        failure(lambda:validate_config(bad), 'foreign input'); failures+=1
        for body in (b'{"same":1,"same":2}',b'{"value":NaN}'):
            failure(lambda:decode(body), 'JSON duplicate/nonfinite'); failures+=1
        raw = encoded(config)
        proof = dict(config_path=str(CONFIG), config_sha256=sha(raw), campaign_schema=SCHEMA,
            artifact_roster_sha256=ROSTER_SHA, native_config_sha256=config['native_config_sha256'],
            binary=config['binary'], native_source_commit=NATIVE_COMMIT, source_identity_sha256=SOURCE_ID,
            code_sha256=config['code_sha256'], native_qualification=config['native_qualification'],
            source_archive_paths=paths, source_archive_paths_sha256=config['source_archive_paths_sha256'])
        userdata = user_data('a'*40,'b'*64,'synthetic/archive.tar.gz',PREFIX+'a0001',proof)
        require('total_max_attempts' in userdata and 'shutdown' in userdata and 'rustup' not in userdata
                and 'cargo' not in userdata, 'SDK/no-compiler bootstrap')
        compile(userdata.split("<<'PY'\n",1)[1].split('\nPY\n',1)[0], '<bootstrap>', 'exec')
        # Download executes the actual SDK body/auth/fsync/no-overwrite path.
        class Body(io.BytesIO):
            def read(self, n=-1):
                require(0 < n <= 64*1024**2+1, 'bounded SDK read')
                return super().read(n)
        bodies={'synthetic/'+str(i):read(inputs/str(i)) for i in range(6)}
        bodies[BINARY_KEY]=fake
        store={}
        s3=Mock()
        def get_object(**kwargs):
            require(kwargs['Bucket']==BUCKET, 'SDK bucket')
            body=(store if kwargs['Key'] in store else bodies)[kwargs['Key']]
            return dict(Body=Body(body), ContentLength=len(body))
        def put_object(**kwargs):
            require(kwargs['IfNoneMatch']=='*', 'immutable marker/artifact writes')
            require(kwargs['Key'] not in store, 'S3 no overwrite')
            store[kwargs['Key']]=kwargs['Body']
        s3.get_object.side_effect=get_object;s3.put_object.side_effect=put_object
        download(s3, config['binary'], base/'download')
        require(file_pin(base/'download')==fake_pin, 'download identity')
        failure(lambda:download(s3,config['binary'],base/'download'),'download overwrite');failures+=1
        for suffix, body in [('short',fake[:-1]),('long',fake+b'x'),('tamper',b'x'+fake[1:])]:
            bodies[BINARY_KEY]=body
            failure(lambda:download(s3,config['binary'],base/suffix),'transport '+suffix);failures+=1
        bodies[BINARY_KEY]=fake
        # Real original processes; only cgroup pseudo-files are substituted.
        files=dict(zip(CGROUP_FILES, (str(CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n',
            'high 0\nmax 0\nfail 0\n','100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',
            str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        run_id=PREFIX+'a0001/i-original'
        def fixture(name, mode='success', deadline=False, real=False):
            root=base/name;root.mkdir()
            write(root/'config.json',raw);write(root/'source-qualification.json',encoded(proof))
            write(root/'native-config.json',encoded(native));write(root/BINARY_NAME,fake);(root/BINARY_NAME).chmod(0o700)
            write(root/'stage-receipt.json',encoded(dict(binary=fake_pin,native_config=pin(encoded(native)),
                inputs={d['destination']:{k:d[k] for k in ('bytes','sha256')} for d in config['inputs']},
                exact_six_inputs=True,compiler_used=False)))
            write(root/'cpu.txt',b'synthetic cpu\n');write(root/'run-closed.log',b'')
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],
                os=dict(ID='ubuntu',VERSION_ID='24.04'),libc=['glibc','2.39'],
                sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            parent=base/(name+'-cgroup')/(SUPERVISOR_UNIT+'.service')
            parent.mkdir(parents=True)
            group=parent/'native'
            def delegate(record):
                record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),
                    observer_pid=os.getpid(),available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),
                    enabled_before=[],parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
                return group
            def create(g):
                (g/'cgroup.procs').write_text('')
            def snapshot(g):
                return dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))
            owned=[]
            def drain(g):
                if owned and owned[0].poll() is None:
                    os.killpg(owned[0].pid,signal.SIGKILL)
                (g/'cgroup.procs').unlink()
            real_popen=subprocess.Popen
            def spawn(command, **kwargs):
                p=real_popen(command,**kwargs)
                owned.append(p)
                # Synthetic cgroup cannot remove exited PIDs as a kernel does.
                (group/'cgroup.procs').write_text('')
                return p
            with ExitStack() as process_stack:
                if not real:
                    for method, implementation in (('delegated_group',delegate),('create_group',create),
                            ('cgroup_snapshot',snapshot),('drain_group',drain)):
                        process_stack.enter_context(patch.object(module,method,side_effect=implementation))
                    process_stack.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
                process_stack.enter_context(patch.dict(os.environ,FINE_PACK_FAKE=mode,
                    BORSUK_FINE_PACK_REAL_CGROUP='1' if real else '0'))
                process_stack.enter_context(patch.dict(CAPS,deadline_seconds=.2 if deadline else 300))
                receipt=supervise(config,root,run_id=run_id)
            require(not group.exists(),'synthetic cgroup cleanup')
            return root,receipt
        root, receipt=fixture('original')
        require(receipt['process_exit_code']==0 and validate_result(root,config)['status']=='SURVIVED_NECESSARY_LOCALITY', 'original fake process closure')
        require(file_pin(root/'native.log')['bytes']==0,'silent exit0 log allowed')
        previous=files['cpu.max'];files['cpu.max']='200000 100000'
        denied, denied_receipt=fixture('pre-exec-cpu-refusal')
        require(denied_receipt['process_started'] is False and denied_receipt['process_exit_code'] is None
                and not (denied/'native.log').exists(),'invalid CPU limit refuses before native exec')
        files['cpu.max']=previous
        rejected, receipt=fixture('rejected','reject')
        require(receipt['process_exit_code']==0 and validate_result(rejected,config)['status']=='REJECT','completed all128 rejection')
        nonzero, receipt=fixture('nonzero','nonzero')
        require(receipt['process_exit_code']==17 and decode(read(nonzero/'screen/report.json'))['complete'] is True,'real original exit17')
        failure(lambda:validate_result(nonzero,config),'PASS body original exit17');failures+=1
        timeout, receipt=fixture('timeout','deadline',True)
        require(receipt['process_exit_code']==-signal.SIGKILL and decode(read(timeout/'resources.json'))['deadline_exceeded'] is True,'real deadline process killed')
        failure(lambda:validate_result(timeout,config),'deadline PASS body');failures+=1
        failure(lambda:supervise(config,root,run_id=run_id),'output overwrite');failures+=1
        if real_cgroup:
            actual, receipt=fixture('actual-cgroup',real=True)
            require(receipt['process_exit_code']==0 and validate_result(actual,config)['valid_diagnostic'],'actual delegated cgroup original process/resources/drain/cleanup')
            actual_resources=decode(read(actual/'resources.json'))
            proof_dir=os.environ.get('BORSUK_FINE_PACK_SETUP_PROOF_OUT')
            if proof_dir:
                destination=Path(proof_dir);destination.mkdir(parents=True,exist_ok=False)
                for name in ('native-exit.json','resources.json','cleanup.json','native.log'):
                    write(destination/name,read(actual/name))
                write(destination/'setup-proof.json',encoded(dict(schema='borsuk-fine-pack-setup-proof-v1',
                    source_file_sha256=file_pin(__file__)['sha256'],invocation_id=os.environ.get('INVOCATION_ID'),
                    original_fake_native_exit=receipt['process_exit_code'],native_ANN_executed=False,
                    cgroups_mocked=False,SDK_transport_and_AWS_mocked=True,
                    artifacts={n:file_pin(destination/n) for n in ('native-exit.json','resources.json','cleanup.json','native.log')})))
            print('PASS actual delegated setup: '+json.dumps(dict(delegation=actual_resources['delegation'],
                memory_peak=actual_resources['after']['files']['memory.peak'],swap_peak=actual_resources['after']['files']['memory.swap.peak'],
                original_exit=receipt['process_exit_code'],cleanup=decode(read(actual/'cleanup.json'))),sort_keys=True))
        def tamper(name, change):
            path=root/name;original=read(path);path.write_bytes(change(original))
            try:failure(lambda:validate_result(root,config),'tamper '+name)
            finally:path.write_bytes(original)
        def changed(body, callback):
            value=decode(body);callback(value);return encoded(value)
        for name, change in (
            ('native-exit.json',lambda b:changed(b,lambda d:d.update(process_exit_code=17))),
            ('cleanup.json',lambda b:changed(b,lambda d:d.update(cleanup_complete=False))),
            ('cleanup.json',lambda b:changed(b,lambda d:d.update(output_durable=False))),
            ('resources.json',lambda b:changed(b,lambda d:d.update(closed=False))),
            ('resources.json',lambda b:changed(b,lambda d:d.update(descendants_remaining_after_exit=True))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.events':'oom 1\noom_kill 1\noom_group_kill 0\n'}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.peak':str(257*1024**2)}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.swap.peak':'1'}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'cgroup.procs':'999'}))),
            ('stage-receipt.json',lambda b:changed(b,lambda d:d.update(exact_six_inputs=False))),
            ('native-config.json',lambda b:b+b' '),
            (BINARY_NAME,lambda b:b+b' '),
            ('screen/report.json',lambda b:changed(b,lambda d:d.update(config_sha256='0'*64))),
            ('screen/report.json',lambda b:changed(b,lambda d:d.update(queries=127))),
            ('screen/report.permutations.json',lambda b:changed(b,lambda d:d['old_to_new'][0].__setitem__(0,1))),
            ('screen/report.prefix.jsonl',lambda b:b+b'x')):
            tamper(name,change);failures+=1
        for name in ('screen/report.json','screen/report.permutations.json','screen/report.prefix.jsonl','resources.json','cleanup.json','native-exit.json'):
            path=root/name;hidden=path.with_name(path.name+'.hidden');path.rename(hidden)
            try:failure(lambda:validate_result(root,config),'missing '+name);failures+=1
            finally:hidden.rename(path)
        # Joint seal tamper, even if its report descriptor is rehashed.
        original_perms=read(root/'screen/report.permutations.json');original_report=read(root/'screen/report.json')
        perm=decode(original_perms);perm['old_to_new'][0][0]=1
        body=encoded(perm);(root/'screen/report.permutations.json').write_bytes(body)
        report=decode(original_report);report['details']['permutation_seal'].update(pin(body))
        (root/'screen/report.json').write_bytes(encoded(report))
        failure(lambda:validate_result(root,config),'rehashed non-bijection');failures+=1
        (root/'screen/report.permutations.json').write_bytes(original_perms);(root/'screen/report.json').write_bytes(original_report)
        prefix=PREFIX+'a0001'
        result=validate_result(root,config)
        terminal=dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',
            prefix=prefix,config_sha256=sha(raw),status='complete',phase='complete',exit_code=0,original_exit_code=0,
            disposition=result['status'],artifact_roster_sha256=ROSTER_SHA,qualification=proof,result=result)
        publish(s3,root,prefix,terminal)
        require(s3.put_object.call_args.kwargs['Key']==prefix+'/terminal.json','marker published last')
        require(set(terminal['artifacts'])==set(ARTIFACTS),'complete raw artifact roster')
        def controller_receipts(out):
            out.mkdir()
            write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
                config_sha256=sha(raw),qualification=proof)))
            nodes={'0':{'instance_id':'i-original'}}
            write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes=nodes,prefix=prefix,
                source_commit='a'*40,source_archive_sha256='b'*64)))
            write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes=nodes)))
        out=base/'collected';controller_receipts(out)
        collect(s3,prefix,out,'i-original','a'*40,'b'*64)
        require(replay(out)==result,'full hash/raw/gzip original collection replay')
        failure(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64),'collection overwrite');failures+=1
        path=out/'screen/report.json';original=read(path);path.write_bytes(original+b' ')
        failure(lambda:replay(out),'collected tamper');failures+=1;path.write_bytes(original)
        key=prefix+'/artifacts/native.log';store[key]=b'tampered'
        damaged=base/'damaged';controller_receipts(damaged)
        failure(lambda:collect(s3,prefix,damaged,'i-original','a'*40,'b'*64),'remote fullhash');failures+=1
        store[key]=b''
        # SDK/upload failure leaves no misleading terminal; raw failures remain.
        failed_prefix=PREFIX+'a0002'
        failed_terminal=dict(terminal,status='failed',phase='execution',exit_code=96,original_exit_code=17,disposition='INVALID',prefix=failed_prefix)
        publish(s3,nonzero,failed_prefix,failed_terminal)
        require(failed_terminal['original_exit_code']==17 and failed_terminal['artifacts']['screen/report.json']['bytes']>0,'failure body preserved')
        reads_before=s3.get_object.call_count
        with patch.object(s3,'get_object',return_value=dict(Body=Body(b'wrong'),ContentLength=5)):
            failure(lambda:publish(s3,rejected,PREFIX+'a0003',copy.deepcopy(terminal)),'upload readback');failures+=1
        require(PREFIX+'a0003/terminal.json' not in store and s3.get_object.call_count>=reads_before,'no marker after failed upload')
        # Same shared ACK ownership, fsync, termination and waiter path used in production.
        import boto3
        campaign=Mock(ROOT=base,NAME='lifecycle',SCHEMA=SCHEMA,WALL=WALL,ARTIFACTS=ARTIFACTS,
            PREFIX=PREFIX,TOKEN_PREFIX=TOKEN_PREFIX,TAG=TAG,SUBNET=SUBNET,INSTANCE_TYPE=INSTANCE_TYPE,
            IMAGE_ID=IMAGE_ID,ROOT_DEVICE_NAME=ROOT_DEVICE_NAME,SPOT_MAX_USD_PER_HOUR=SPOT_MAX_USD_PER_HOUR,COMPUTE_CAP=COMPUTE_CAP)
        campaign.preflight.return_value=dict(config_sha256='c'*64)
        campaign.user_data.return_value='#!/bin/bash\nexit 0\n'
        for mode in ('success','poll','launch-fsync','launch-upload','collect','extra-ack'):
            campaign.ROOT=base/('lifecycle-'+mode);campaign.poll.reset_mock();campaign.collect.reset_mock()
            ec2=Mock();aws_s3=Mock();session=Mock()
            session.client.side_effect=lambda service:ec2 if service=='ec2' else aws_s3
            ec2.describe_instances.return_value={'Reservations':[]}
            ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock'}]}
            ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[{'SpotPrice':'.01','Timestamp':datetime.now(timezone.utc)}]}
            ack=['i-original','i-extra'] if mode=='extra-ack' else ['i-original']
            ec2.run_instances.return_value={'Instances':[{'InstanceId':i} for i in ack]}
            campaign.poll.side_effect=OSError('mock observation') if mode=='poll' else None
            campaign.collect.side_effect=OSError('mock collection') if mode=='collect' else None
            campaign.collect.return_value=dict(status='complete',phase='complete',exit_code=0,artifacts={n:pin(b'') for n in ARTIFACTS})
            chronology=[]
            ec2.terminate_instances.side_effect=lambda **kw:chronology.append(('terminate',kw['InstanceIds']))
            ec2.get_waiter.return_value.wait.side_effect=lambda **kw:chronology.append(('wait',kw['InstanceIds']))
            real_fsync=os.fsync
            def fsync(fd):
                if mode=='launch-fsync':raise OSError('mock launch persistence')
                return real_fsync(fd)
            def upload(key,body):
                if mode=='launch-upload' and key.endswith('/launch.json'):raise OSError('mock launch upload')
            with patch.object(boto3,'Session',return_value=session), \
                 patch.object(shared.subprocess,'check_output',side_effect=['','a'*40]), \
                 patch.object(shared,'source_archive',return_value=b'tiny synthetic archive'), \
                 patch.object(shared.peer,'missing',return_value=True), \
                 patch.object(shared.peer,'put_if_absent',side_effect=upload), \
                 patch.object(shared.os,'fsync',side_effect=fsync):
                if mode in ('success','extra-ack'):shared.main('a0001',campaign=campaign)
                else:failure(lambda:shared.main('a0001',campaign=campaign),'lifecycle '+mode);failures+=1
            require(ec2.run_instances.call_count==1,'one original launch, no retry')
            require(chronology==[('terminate',ack),('wait',ack)],'every ACK same-ID terminate/wait')
            kwargs=ec2.run_instances.call_args.kwargs
            require(kwargs['InstanceType']==INSTANCE_TYPE and kwargs['InstanceMarketOptions']['MarketType']=='spot'
                    and kwargs['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.60','bounded c7i Spot launch')
            if mode!='launch-fsync':
                require(decode(read(campaign.ROOT/'lifecycle/a0001/aws-launch.json'))['nodes']=={str(i):{'instance_id':v} for i,v in enumerate(ack)},'durable every-ACK receipt')
        print(f'PASS fine-pack: real fake-native exit0/REJECT/exit17/deadline; {failures} refusals; fullhash/readback/marker-last/no-overwrite; every-ACK same-ID terminate/wait; actual SDK model positive/official_old_negative={official_old_checked}; actual_delegated_cgroup={real_cgroup}; other cgroup/SDK transport/AWS MOCKED; no ANN/graph/corpus/network')


def sq4_self_check():
    """Mock native CLI and qualification metadata; no native/data/AWS execution."""
    import copy
    import tempfile
    from contextlib import ExitStack
    from unittest.mock import patch
    module = sys.modules[__name__]
    configure_sq4()
    retained_pins = INPUT_PINS
    require(SCHEMA == 'borsuk-fixed-sq4-diagnostic-spot-v1', 'explicit SQ4 mode')
    require(len(INPUT_PINS) == 18 and sum(p[1] for p in INPUT_PINS) == 229614200, 'opaque retained roster')
    require(len([n for n in ARTIFACTS if n.startswith('screen/')]) == 134, 'whole native closure')
    # Root-authenticated completed metadata only; the executable/transport below
    # remain mocked. No corpus, compiler or live job is opened by this fixture.
    def committed(path):
        repo=Path(__file__).resolve().parents[1]
        return subprocess.check_output(['git','-c','safe.directory='+str(repo),'show','e0d81804:'+str(path)],cwd=repo)
    deployment_body=committed(SQ4_ROOT/'qualified-deployment-pins-c2d233d6.json')
    require(pin(deployment_body)==dict(bytes=15955,sha256='97c175233d71e5e6624fa91939baf436a79ec29b3cbbaff2d609ff453464d145'), 'root completed deployment fixture pin')
    deployment=decode(deployment_body)
    actual={}
    for name,descriptor in deployment['qualification_receipts'].items():
        body=committed(descriptor['path'])
        require(pin(body)=={k:descriptor[k] for k in ('bytes','sha256')}, 'root completed receipt fixture pin')
        actual[name]=decode(body)
    old,verification,workspace=(actual[n] for n in ('source-qualification.json','parent-verification.json','workspace-receipt.json'))
    require(deployment['required_stage_protocol']==[dict(stage=s['stage'],command=s['command'],
        required_test_passes=s['required_test_passes'],require_positive_tests_run=s['tests_run'] is not None)
        for s in workspace['stages']], 'actual completed root all14 protocol')
    def refused(call):
        try: call()
        except (ValueError, OSError, KeyError): return
        raise AssertionError('unsafe SQ4 acceptance')
    # /tmp is tmpfs on Devbox: raw payload + transport + replay copies need disk.
    with tempfile.TemporaryDirectory(prefix='sq4-glue-check-', dir='/var/tmp') as tmp, ExitStack() as stack:
        base = Path(tmp)
        stack.enter_context(patch.object(module, 'SQ4_INPUT_ROOT', base/'retained'))
        pins = tuple((str(base/'retained'/f'input-{i}'), 1, sha(b'x')) for i in range(18))
        stack.enter_context(patch.object(module, 'INPUT_PINS', pins))
        descriptors = [dict(path=p, bytes=n, sha256=h) for p,n,h in pins]
        native = dict(schema='borsuk-fixed-sq4-config-v1', source_identity_sha256=SOURCE_ID,
            caps=copy.deepcopy(CAPS), original_seal=descriptors[16], prefix=descriptors[17],
            panels=[dict(dataset=d, root=descriptors[i*8], requests=descriptors[i*8+6],
                         truth=descriptors[i*8+7], truth_width=100) for i,d in enumerate(('relaion','cohere'))])
        fake = b'''#!/usr/bin/env python3
import hashlib,json,os,sys,tempfile,time
from pathlib import Path
_,mode,config_path,config_sha,out=sys.argv
assert mode=='check-fine-sq4' and hashlib.sha256(Path(config_path).read_bytes()).hexdigest()==config_sha
c=json.loads(Path(config_path).read_bytes());p=Path(out);fault=os.environ.get('SQ4_FAKE','')
def emit(path,v):
 b=json.dumps(v,separators=(',',':')).encode()
 with path.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
 return {'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def body(path,b):
 with path.open('xb') as f:f.write(b)
 return {'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
payloads=[]
for i,panel in enumerate(c['panels']):
 path=p.with_suffix('.sq4-'+str(i)+'.bin')
 with path.open('xb') as f:f.truncate(39600000)
 h=hashlib.sha256()
 with path.open('rb') as f:
  while b:=f.read(65536):h.update(b)
 payloads.append({'payload':{'path':str(path),'bytes':39600000,'sha256':h.hexdigest()},'original_root':panel['root'],
  'codec':'borsuk-sq4-nearest17-original-coefficients-v1','rows':100000,'dimensions':768,'row_bytes':396,'group_rows':16})
seal=emit(p.with_suffix('.sq4-payloads.json'),{'schema':'borsuk-fixed-sq4-payload-seal-v1','config_sha256':config_sha,
 'source_identity_sha256':c['source_identity_sha256'],'codec':'borsuk-sq4-nearest17-original-coefficients-v1',
 'original_seal':c['original_seal'],'payloads':payloads,'queries_opened':False,'truth_opened':False})
prefix=body(p.with_suffix('.sq4-prefix.jsonl'),Path(c['prefix']['path']).read_bytes())
results=[]
for i in range(128):
 fits=fault!='envelope' or i!=0
 scored={'fetched_ids':[0],'ranked':[{'id':n,'ordinal':n,'score_bits':0} for n in range(100)],
  'range_reads':1,'verified_bytes':396 if fits else 16777612}
 results.append(emit(p.with_suffix('.sq4-result-'+str(i)+'.json'),{'schema':'borsuk-fixed-sq4-query-v1',
  'dataset':c['panels'][i//64]['dataset'],'ordinal':i%64,'nominees_retained':True,'original_cover_contained':True,
  'truth_opened':False,'sq8_reference_serving_eligible':False,'plan':{'envelope_fits':fits,'candidate_bytes':scored['verified_bytes']},
  'sq4':scored,'sq8_reference':scored,'original256_baseline':scored}))
freeze=emit(p.with_suffix('.sq4-freeze.json'),{'schema':'borsuk-fixed-sq4-freeze-v1','config_sha256':config_sha,
 'source_identity_sha256':c['source_identity_sha256'],'payload_seal':seal,'payloads':payloads,'original_seal':c['original_seal'],
 'truth':[x['truth'] for x in c['panels']],'nomination_prefix':prefix,'results':results,'truth_opened':False})
reject=fault in ('reject','envelope')
emit(p,{'schema':'borsuk-fixed-sq4-report-v1','codec':'borsuk-sq4-nearest17-original-coefficients-v1',
 'config_sha256':config_sha,'source_identity_sha256':c['source_identity_sha256'],'queries':128,'complete':True,
 'status':'REJECT' if reject else 'SURVIVED_CONSUMED_PANELS','standalone_authority':False,
 'requires_matching_supervisor_exit_receipt':True,'quality_or_performance_claim':False,
 'details':{'freeze':freeze,'rows':100000,'dimensions':768,'truth':[x['truth'] for x in c['panels']],
 'frozen_original_authority':True,'caps':c['caps'],'operations':1,'whole_process_supervisor_required':True,
 'pair_payload_bytes':79200000,'modeled_peak_bytes':100000000,'modeled_output_bytes':90000000,
 'all128_envelopes_fit':fault!='envelope','summaries':[{'dataset':x['dataset'],
  'sq4_returned':{'mean_recall':.97 if fault=='reject' else .99,'p05_hits':94 if fault=='reject' else 99}} for x in c['panels']]}})
if fault=='drift':Path(config_path).write_bytes(Path(config_path).read_bytes()+b' ')
if fault in ('scratch','external-temp'):
 path=p.parent.parent/'temporary' if fault=='scratch' else Path(tempfile.gettempdir())/'external-temporary'
 if fault=='external-temp':assert path.parent==p.parent.parent/'bootstrap/tmp'
 with path.open('xb') as f:f.truncate(4294967296)
 time.sleep(2)
if fault=='deadline':time.sleep(5)
sys.exit(2 if fault=='latefailure' else 0)
'''
        source = dict(old['source_sha256'], **SQ4_SOURCES)
        full_id = sha(json.dumps(source, sort_keys=True, separators=(',', ':')).encode())
        require(full_id=='1689c53c7564f989f19da397b32b13f16f10df264d02355928849dcabf1e2849','actual prospective c2 full404 map')
        old.update(native_source_commit=NATIVE_COMMIT, source_identity_sha256=full_id, source_sha256=source)
        verification.update(native_source_commit=NATIVE_COMMIT, source_identity_sha256=full_id, binary=pin(fake))
        workspace.update(source_identity_sha256=full_id, source_sha256=source)
        terminal = actual['aws-terminal.json']
        terminal.update(native_source_commit=NATIVE_COMMIT, source_identity_sha256=full_id)
        receipt_bodies = dict(zip(SQ4_RECEIPTS, (verification, old, workspace, terminal,
                                               actual['aws-closeout.json'])))
        workspace['qualification_sha256'] = pin(encoded(old))['sha256']
        terminal['artifacts']['workspace-receipt.json'] = pin(encoded(workspace))
        terminal['artifacts'][BINARY_NAME] = pin(fake)
        terminal['source_qualification_sha256'] = pin(encoded(old))['sha256']
        receipts = []
        for name,value in receipt_bodies.items():
            path=base/'proofs'/name; write(path, encoded(value))
            receipts.append(dict(path='proofs/'+name, **file_pin(path)))
        fixed = copy.deepcopy(FIXED); fixed['native_caps']=copy.deepcopy(CAPS)
        fixed.update(machine_limit_seconds=1800,compute_cap_usd=.30)
        fixed['scratch'] = dict(input_bytes=18, native_output_bytes=CAPS['output_bytes'], binary_bytes=len(fake),
            source_archive_bytes=8*1024**2, bootstrap_bytes=256*1024**2, auxiliary_bytes=16*1024**2, cap_bytes=4*1024**3)
        paths = sorted([str(CONFIG), str(SQ4_ROOT/'prospective-input-roster.json'), *CODE, *(r['path'] for r in receipts)])
        config = dict(schema=SCHEMA, authority_pending=False, fixed=fixed, native_config=native,
            native_source=dict(commit=NATIVE_COMMIT,full_source_identity_sha256=full_id,
                               source_identity_sha256=SOURCE_ID,source_sha256=SQ4_SOURCES),
            native_config_sha256=sha(encoded(native)), binary=dict(pin(fake), key='mock/native'), inputs=[
                dict(destination=p, bytes=n, sha256=h, key='mock/input-'+str(i)) for i,(p,n,h) in enumerate(pins)],
            native_qualification=receipts, code_sha256={n:file_pin(n)['sha256'] for n in CODE},
            source_archive_paths=paths, source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        validate_config(config)
        for n in CODE:write(base/n,read(n))
        real=copy.deepcopy(config)
        descriptors=[dict(path=p,bytes=n,sha256=h) for p,n,h in retained_pins]
        real['native_config'].update(original_seal=descriptors[16],prefix=descriptors[17],panels=[
            dict(dataset=d,root=descriptors[i*8],requests=descriptors[i*8+6],truth=descriptors[i*8+7],truth_width=100)
            for i,d in enumerate(('relaion','cohere'))])
        real['native_config_sha256']=sha(encoded(real['native_config']))
        require(pin(encoded(real['native_config']).rstrip(b'\n'))==dict(bytes=1695,
            sha256='771a0c1c8ed2060318b5388e3eed4c6a8dfac0b707687de823079ed8131d10fa'),'exact 1d93036a prospective c2 native config metadata')
        real['inputs']=[dict(destination=p,bytes=n,sha256=h,key='retained/'+str(i)) for i,(p,n,h) in enumerate(retained_pins)]
        real['fixed']['scratch']['input_bytes']=229614200
        original_regular=regular
        def metadata_only(path):
            require(str(Path(path).absolute()) not in {p for p,_,_ in retained_pins},'metadata preflight opened retained data')
            return original_regular(path)
        with patch.object(module,'INPUT_PINS',retained_pins), patch.object(module,'regular',side_effect=metadata_only):
            write(base/CONFIG,encoded(real));preflight(base)
        print('PASS actual18 retained metadata, including both78000000B records; no data/GT opens',flush=True)
        sq4_qualification(config, base)
        for key,value in (('authority_pending',True), ('native_qualification',[]), ('binary',{}), ('native_config_sha256','0'*64)):
            bad=copy.deepcopy(config);bad[key]=value;refused(lambda:validate_config(bad))
        bad=copy.deepcopy(config);bad['fixed']['scratch']['cap_bytes']=1
        refused(lambda:validate_config(bad))
        bad=copy.deepcopy(config);bad['native_config']['caps']['cpu_threads']=True
        bad['native_config_sha256']=sha(encoded(bad['native_config']))
        refused(lambda:validate_config(bad))
        # Rehashing all affected receipts cannot admit failed/reordered/missing gates.
        for fault in ('failed','reordered','missing','zero-test','wrongargv','missingmandatory','reducedroster',
                      'misplacedtests','utc','overlap','reversed','non-test-count'):
            bad=copy.deepcopy(config);failed=copy.deepcopy(receipt_bodies)
            stages=failed['workspace-receipt.json']['stages']
            if fault=='failed':stages[0]['exit_status']=2
            elif fault=='reordered':stages[0],stages[1]=stages[1],stages[0]
            elif fault=='missing':stages.pop()
            elif fault=='zero-test':stages[5].update(tests_run=0,required_test_passes={})
            elif fault=='wrongargv':stages[5]['command'][6]='zero_match_filter'
            elif fault=='missingmandatory':stages[5]['required_test_passes'].pop(next(iter(stages[5]['required_test_passes'])))
            elif fault=='reducedroster':
                stages[5]['required_test_passes']={}
                for n in ('source-qualification.json','workspace-receipt.json'):failed[n]['mandatory_tests']['graph-regressions']=[]
            elif fault=='misplacedtests':stages[1]['required_test_passes'].update(stages[2]['required_test_passes']);stages[2]['required_test_passes']={}
            elif fault=='utc':stages[5]['started_at']='2026-10-05T13:05:00+01:00'
            elif fault=='overlap':stages[5]['started_at']=(datetime.fromisoformat(stages[4]['started_at'])-timedelta(seconds=1)).isoformat()
            elif fault=='reversed':stages[5]['finished_at']=(datetime.fromisoformat(stages[5]['started_at'])-timedelta(seconds=1)).isoformat()
            else:stages[0]['tests_run']=1
            failed['parent-verification.json']['stages']=stages
            qualification_sha=pin(encoded(failed['source-qualification.json']))['sha256']
            failed['workspace-receipt.json']['qualification_sha256']=qualification_sha
            failed['aws-terminal.json']['source_qualification_sha256']=qualification_sha
            failed['aws-terminal.json']['artifacts']['workspace-receipt.json']=pin(encoded(failed['workspace-receipt.json']))
            for r in bad['native_qualification']:
                name=Path(r['path']).name;p=base/fault/name;write(p,encoded(failed[name]))
                r.update(path=fault+'/'+name,**file_pin(p))
            try:refused(lambda:sq4_qualification(bad,base))
            except AssertionError as error:raise AssertionError('unsafe rebound qualification: '+fault) from error
        bad=copy.deepcopy(config);bad['native_source']['full_source_identity_sha256']='0'*64
        refused(lambda:sq4_qualification(bad,base))
        original=read(base/'proofs/workspace-receipt.json')
        (base/'proofs/workspace-receipt.json').write_bytes(original+b' ')
        refused(lambda:sq4_qualification(config,base));(base/'proofs/workspace-receipt.json').write_bytes(original)
        (base/CONFIG).unlink();write(base/CONFIG, encoded(config))
        proof=preflight(base)
        userdata=user_data('a'*40,'b'*64,'mock/archive',PREFIX+'a0001',proof)
        require('--sq4 --remote' in userdata and 'DelegateSubgroup=supervisor' in userdata, 'SQ4 SDK delegated bootstrap')
        require(all(s in userdata for s in ('ReadOnlyPaths=/tmp /var/tmp','export TMPDIR=', 'Dir::Cache::archives=',
                'Dir::State::lists=', 'Dir::Log=', '--setenv=TMPDIR=', 'bootstrap/input/output overlap cap',
                'source archive scratch reserve', 'source extraction overlap reserve', 'wait "$watcher"')), 'charged SQ4 generated Bash')
        def bootstrap_fixture(root, fault=False):
            (root/'bootstrap/tmp').mkdir(parents=True)
            with patch.object(module,'REMOTE_ROOT',root):
                generated=user_data('a'*40,'b'*64,'mock/archive',PREFIX+'a0001',proof)
            watcher=generated.split("<<'WATCH'\n",1)[1].split('\nWATCH\n',1)[0]
            compile(watcher,'generated-bootstrap-watch','exec')
            process=subprocess.Popen([sys.executable,'-c',"import os,sys;sys.argv=['watch',str(os.getpid())];"+watcher],
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            started=time.monotonic()
            while not (root/'bootstrap/ready').exists() and process.poll() is None:
                require(time.monotonic()-started < 3,'bootstrap admission ACK deadline');time.sleep(.02)
            require(process.poll() is None,'bootstrap admission before writes')
            if fault:
                with (root/'bootstrap/tmp/fault').open('xb') as f:f.truncate(SQ4_SCRATCH_CAP)
            process.terminate()
            stdout,stderr=process.communicate(timeout=3)
            require((process.returncode != 0 and not (root/'bootstrap/scratch.json').exists()) if fault
                    else (process.returncode == 0 and decode(read(root/'bootstrap/scratch.json'))['closed'] is True), 'generated bootstrap actual observer closure')
        bootstrap_denied=base/'bootstrap-denied';bootstrap_denied.mkdir();bootstrap_fixture(bootstrap_denied,True)
        import io
        from types import SimpleNamespace
        class Zeros:
            remaining=78000000
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self,n):
                require(0 < n <= 65536,'large input streaming read cap')
                count=min(n,self.remaining);self.remaining-=count;return bytes(count)
        large=base/'large.bin'
        digest='df4fabbabc07f9e687ba5ae43d2cb72c3c0f8f08e4dfdc6f9d8c91412ef9e7ff'
        descriptor=dict(destination=str(large),bytes=78000000,sha256=digest,key='mock/large')
        refused(lambda:body_pin({k:descriptor[k] for k in ('bytes','sha256')}))
        with patch.object(module,'INPUT_PINS',((str(large),78000000,digest),)):
            download(SimpleNamespace(get_object=lambda **kw:dict(Body=Zeros(),ContentLength=78000000)),descriptor,large)
        require(file_pin(large)==dict(bytes=78000000,sha256=digest),'78000000B synthetic streamed authentication')
        large.unlink()
        store={}
        bodies={'mock/native':fake, **{d['key']:b'x' for d in config['inputs']}}
        calls = [0]
        def get(**kwargs):
            calls[0] += 1
            if kwargs['Key'] in store:
                p=store[kwargs['Key']]
                return dict(Body=regular(p),ContentLength=p.stat().st_size)
            b=bodies[kwargs['Key']]
            return dict(Body=io.BytesIO(b),ContentLength=len(b))
        def put(**kwargs):
            require(kwargs['IfNoneMatch']=='*' and kwargs['Key'] not in store,'immutable put')
            p=base/'store'/sha(kwargs['Key'].encode());write(p,kwargs['Body']);store[kwargs['Key']]=p
        s3=SimpleNamespace(get_object=get,put_object=put)
        files=dict(zip(CGROUP_FILES,(str(CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n','high 0\nmax 0\nfail 0\n',
            '100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        def fixture(name,fault='',deadline=False):
            root=base/name;root.mkdir()
            write(root/'config.json',encoded(config));write(root/'source-qualification.json',encoded(proof))
            write(root/'run-closed.log',b'');write(root/'cpu.txt',b'mock')
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],os=dict(ID='ubuntu',VERSION_ID='24.04'),
                libc=['glibc','2.39'],sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            for r in receipts:write(root/'qualification'/Path(r['path']).name,read(base/r['path']))
            bootstrap_fixture(root)
            for p,_,_ in pins:
                if Path(p).exists():Path(p).unlink()
            stage(s3,config,root)
            parent=base/(name+'-cgroups')/(SUPERVISOR_UNIT+'.service');parent.mkdir(parents=True)
            group=parent/'native';owned=[]
            def delegate(record):
                record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),observer_pid=os.getpid(),
                    available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
                return group
            def create(g):(g/'cgroup.procs').write_text('')
            def snapshot(g):return dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))
            def drain(g):
                if owned and owned[0].poll() is None:os.killpg(owned[0].pid,signal.SIGKILL)
                (g/'cgroup.procs').unlink()
            original_popen=subprocess.Popen
            def spawn(command,**kwargs):
                p=original_popen(command,**kwargs);owned.append(p);(group/'cgroup.procs').write_text('');return p
            with ExitStack() as execution:
                for method,fn in (('delegated_group',delegate),('create_group',create),('cgroup_snapshot',snapshot),('drain_group',drain)):
                    execution.enter_context(patch.object(module,method,side_effect=fn))
                execution.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
                execution.enter_context(patch.dict(os.environ,SQ4_FAKE=fault))
                execution.enter_context(patch.dict(CAPS,deadline_seconds=.2 if deadline else 600))
                if fault in ('scratch','external-temp'):
                    try:supervise(config,root,run_id=PREFIX+'a0001/i-original')
                    except ValueError:pass
                    else:raise AssertionError('scratch fault accepted '+fault+': '+read(root/'native.log').decode())
                    receipt=dict(process_exit_code=owned[0].returncode)
                else:receipt=supervise(config,root,run_id=PREFIX+'a0001/i-original')
            require(not group.exists(),'drained mock cgroup')
            return root,receipt
        root,receipt=fixture('success')
        result=validate_result(root,config)
        require(receipt['process_exit_code']==0 and result['status']=='SURVIVED_CONSUMED_PANELS' and read(root/'native.log')==b'','silent original closure')
        require(receipt['report_sha256']==file_pin(root/'screen/report.json')['sha256'],'independent terminal report binding')
        refused(lambda:supervise(config,root,run_id='overwrite'))
        for fault in ('reject','envelope','latefailure','drift','deadline','scratch','external-temp'):
            out,exit_receipt=fixture(fault,fault,deadline=fault=='deadline')
            if fault in ('reject','envelope'):require(validate_result(out,config)['status']=='REJECT','completed REJECT retained')
            else:
                refused(lambda:validate_result(out,config))
                if fault=='latefailure':require(exit_receipt['process_exit_code']==2 and decode(read(out/'screen/report.json'))['complete'] is True,'late exit2 invalidates complete body')
        for name in ('screen/report.sq4-1.bin','screen/report.sq4-result-127.json','screen/report.sq4-freeze.json','cleanup.json','resources.json','scratch.json'):
            p=root/name;b=read(p,64*1024**2);p.write_bytes(b+b' ')
            if not name.startswith('screen/'):
                value=decode(b);value[{'cleanup.json':'cleanup_complete','resources.json':'closed','scratch.json':'closed'}[name]]=False
                p.write_bytes(encoded(value))
            refused(lambda:validate_result(root,config));p.write_bytes(b)
        denied=base/'scratch-denied';denied.mkdir()
        bootstrap_fixture(denied)
        with (denied/'temporary').open('xb') as f:f.truncate(4*1024**3)
        previous_calls=calls[0]
        refused(lambda:stage(s3,config,denied))
        require(calls[0]==previous_calls and not (denied/BINARY_NAME).exists(),'scratch refuses before hydration')
        prefix=PREFIX+'a0001'
        terminal=dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',prefix=prefix,
            config_sha256=sha(encoded(config)),status='complete',phase='complete',exit_code=0,original_exit_code=0,
            disposition=result['status'],artifact_roster_sha256=ROSTER_SHA,qualification=proof,result=result)
        near_cap=dict(roots={str(root):SQ4_SCRATCH_CAP-4096,str(SQ4_INPUT_ROOT):0},whole_scratch_bytes=SQ4_SCRATCH_CAP-4096)
        previous_uploads=len(store)
        with patch.object(module,'scratch_observation',return_value=near_cap):
            refused(lambda:publish(s3,root,prefix,copy.deepcopy(terminal)))
        require(len(store)==previous_uploads and not (root/'terminal.json').exists(),'near-cap closure refused before writes/uploads')
        publish(s3,root,prefix,terminal)
        out=base/'collected';out.mkdir()
        write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
            config_sha256=terminal['config_sha256'],qualification=proof)))
        write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes={'0':{'instance_id':'i-original'}},prefix=prefix,
            source_commit='a'*40,source_archive_sha256='b'*64)))
        write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes={'0':{'instance_id':'i-original'}})))
        collect(s3,prefix,out,'i-original','a'*40,'b'*64)
        require(replay(out)==result,'all raw/gzip native closure replay')
        refused(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64))
        p=out/'screen/report.sq4-result-127.json';p.write_bytes(read(p)+b' ');refused(lambda:replay(out))
        print('PASS SQ4 mock-native: exact14 argv/mandatory owning-stage passes/positive test counts/serial UTC; generated Bash/bootstrap observer; charged external-temp fault; near-cap closure; exact CLI; silent exit0; REJECT; late exit2; deadline; config/output drift; no overwrite; scratch; cleanup; full raw/gzip collection/replay. Cgroup/SDK transport/AWS/qualification metadata MOCKED; no native science.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('attempt', nargs='?')
    parser.add_argument('--self-check', action='store_true')
    parser.add_argument('--sq4', action='store_true', help='opt in to the frozen native SQ4 experiment')
    parser.add_argument('--replay', type=Path)
    parser.add_argument('--remote', nargs=6)
    args = parser.parse_args()
    if args.sq4:
        configure_sq4()
    require(sum((args.attempt is not None, args.self_check, args.replay is not None, args.remote is not None)) == 1, 'one CLI mode')
    if args.self_check:
        if SQ4:
            sq4_self_check()
        else:
            self_check(real_cgroup=os.environ.get('BORSUK_FINE_PACK_REAL_CGROUP')=='1')
        return 0
    if args.replay:
        print(json.dumps(replay(args.replay), sort_keys=True)); return 0
    if args.remote:
        repo, root, commit, digest, prefix, config_sha = args.remote
        return remote(Path(repo), Path(root), commit, digest, prefix, config_sha)
    require(re.fullmatch(r'a[0-9]{4}', args.attempt), 'attempt must be aNNNN')
    sdk_guard()
    os.environ['AWS_MAX_ATTEMPTS'] = '1'
    os.environ['AWS_RETRY_MODE'] = 'standard'
    with open('/tmp/borsuk-fixed-sq4-diagnostic.lock' if SQ4 else '/tmp/borsuk-fine-pack-diagnostic.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        lifecycle().main(args.attempt, campaign=sys.modules[__name__])
    return 0


if __name__ == '__main__':
    sys.exit(main())
