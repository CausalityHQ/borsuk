#!/usr/bin/env python3
"""Thin paired consumed64 staging adapter; root freezes CONFIG and owns launch.

CLI: aNNNN | --canary aNNNN | --stage[-canary] REPO NEW_OUTPUT WORKER_ROOT
     --replay[-canary] OUTPUT | --self-check.
Explicit --global-leaf-probe prefix selects the separate nomination campaign.
Explicit --partitioner-pair selects the fixed original401/candidate402 test.
Explicit --cell-overlap-pair stages exact inputs for the frozen six-call helper.
Missing/pending authority closes before cloud. The native proof is supplied by
the root, never inferred from a binary name or a synthetic test transcript.
Quality FAIL is a completed diagnostic; identity/execution/resource errors are
INVALID. Local reads establish neither S3 speed nor a competitor advantage.
"""
import copy
import fcntl
import hashlib
import importlib.metadata
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import patch
from types import SimpleNamespace

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_semantic_panel_ids_spot as ids
from scripts import prepare_hierarchical_cells_100k as local
from scripts import prepare_semantic_positive_inputs as positive
from scripts import prepare_native_semantic_publication as publication
from scripts.check_native_startup_build import source_hashes, source_identity

require, exact, fields = local.require, local.exact, local.fields
ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k')
CONFIG, NAME = ROOT/'config.json', ''
MODULE = 'scripts.launch_hierarchical_cells_100k_spot'
SCHEMA = 'borsuk-hierarchical-100k-spot-v2'
PREFIX = 'research/hierarchical-cells/20261003/paired100k-'
TOKEN_PREFIX, TAG = 'hierarchical-100k-', 'borsuk-hierarchical-100k'
WALL, MEMORY, SCRATCH = 1800, 2 << 30, 16 << 30
CANARY_SCHEMA = 'borsuk-hierarchical-100k-infrastructure-canary-v2'
CANARY_PREFIX = 'research/hierarchical-cells/20261003/infrastructure-canary-'
CANARY_WALL, CANARY_MEMORY, CANARY_SCRATCH = 480, 256 << 20, 4 << 30
SDK_VERSIONS = dict(boto3='1.40.72', botocore='1.40.72')
INSTANCE_TYPE, IMAGE_ID = ids.INSTANCE_TYPE, ids.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = ids.ROOT_DEVICE_NAME, ids.SUBNET
REGION, BUCKET = ids.REGION, ids.BUCKET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .25
AWSCLI_VERSION, AWSCLI_SHA256 = ids.AWSCLI_VERSION, ids.AWSCLI_SHA256
REF_PATHS = dict(zip(('preparation', 'inventory', 'sources'), (v[0] for v in local.EVIDENCE)))
GATE_ROOT = ROOT.parent/'implementation-gates/a0002'
REF_PATHS.update({name: str(GATE_ROOT/(file+'.json')) for name, file in dict(
    verification='integration-verification', receipt='workspace-receipt',
    terminal='aws-terminal', launch='aws-launch', closeout='aws-closeout',
    manifest='native-source-manifest').items()})
REF_PATHS['staging_authority'] = str(ROOT.parent/'implementation-gates/qualified-staging-authority.json')
CODE = tuple(sorted(set((*ids.CODE, *('scripts/'+name+'.py' for name in (
    'prepare_hierarchical_cells_100k', 'prepare_semantic_positive_inputs',
    'check_semantic_router_coverage', 'benchmark_with_resources',
    'launch_hierarchical_cells_100k_spot')), 'scripts/check_hierarchical_cells_implementation.sh'))))
STAGED_ROLES = dict(source='source', order='order.u64', sq8='sq8.bin',
                    mean='control/plane/mean.bin', records='control/plane/records.bin',
                    requests='requests', truth='truth')
FIXED = dict(schema='borsuk-hierarchical-100k-staging-v1', architecture='x86_64',
    region=REGION, bucket=BUCKET, rows=100000, dimensions=768, first=0, count=64,
    k=100, metric='cosine', consumed=True, held_out=False, retune_allowed=False,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, volume_gib=80, volume_type='gp3', encrypted=True,
    delete_on_termination=True, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
    compute_cap_usd=COMPUTE_CAP, memory_bytes=MEMORY, swap_bytes=0,
    cpu_quota_percent=200, tasks_max=512, scratch_bytes=SCRATCH, wall_seconds=WALL,
    versions={'numpy': '2.3.3', 'pyarrow': '24.0.0'}, source_object_count=14,
    source_encoded_bytes=683811858, policy=local.POLICY,
    mean_recall_minimum=.98, p05_hits_minimum=95,
    physical_s3_measured=False, vendor_win=False, scientific_qualification=False)
LIMITS = dict(memory_max_bytes=MEMORY, scratch_max_bytes=SCRATCH,
    writer_max_memory_bytes=64 << 20, build_max_payload_bytes=64 << 20,
    build_max_output_bytes=256 << 20, max_resident_directory_payload_bytes=384 << 20,
    max_evaluator_payload_bytes=128 << 20, max_query_payload_bytes=128 << 20,
    max_result_bytes=128 << 20, max_log_bytes=16 << 20)
OUTPUTS = ('config.json', 'source-qualification.json', 'native-proof.json',
    'tool-versions.json', 'staging.json', 'local-config.json', 'admission.json',
    'summary.json', 'resources.json', 'worker-cgroup.json', 'cleanup.json',
    'relaion-panel-binding.json', 'cohere-panel-binding.json',
    'measurement/frozen-config.json', 'measurement/receipt.json',
    *(f'measurement/{d}-{n}' for d in ('relaion', 'cohere') for n in (
        'writer.json', 'writer.log', 'build.json', 'build.log', 'diagnose.json',
        'diagnose.log', 'diagnostic.jsonl')))
ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in OUTPUTS))
CANARY_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in (
    'config.json', 'source-qualification.json', 'tool-versions.json',
    'canary.json', 'summary.json', 'resources.json', 'worker-cgroup.json', 'cleanup.json')))
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256',
    'native_identity_sha256', 'source_file_count', 'source_archive_paths_sha256',
    'artifact_roster_sha256', 'campaign_schema',
    'awscli_version', 'awscli_sha256')


def repo_path(repo, name):
    publication.relative(name)
    return positive.regular_path(Path(repo)/name)


def body_pin(value):
    return {k: value[k] for k in ('bytes', 'sha256')}


def transport_path(repo, name):
    return positive.regular_path(name) if Path(name).is_absolute() else repo_path(repo, name)


def transport_name(name):
    # Cold bodies need a safe declaration, not a local copy. Resolve only when
    # opening a body, so metadata preflight works with absent assets/ paths.
    require(type(name) is str and name and '\x00' not in name and '\n' not in name,
            'transport path declaration')
    require('..' not in Path(name).parts and str(Path(name)) == name, 'transport path traversal')
    if not Path(name).is_absolute():
        publication.relative(name)


def completed_gate_log(pin, stages):
    transcript = local.authenticate(pin, 1 << 20, read=True)
    require(transcript.endswith(b'\n'), 'completed gate transcript newline')
    events, passed, summaries, test_builds = [], {}, [], 0
    for line in transcript.splitlines():
        match = re.fullmatch(rb'test (\S+) \.\.\. ok', line)
        if match:
            name = match[1].decode(); passed[name] = passed.get(name, 0)+1
        match = re.fullmatch(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; .*', line)
        if match:
            summaries.append((int(match[1]), int(match[2])))
        if re.fullmatch(rb'rust-test-build status=0 elapsed_seconds=\d+ jobs=1', line):
            exact(len(events), 11, 'unshimmed test-build in final stage'); test_builds += 1
        if not line.startswith(b'{'):
            continue
        event = local.decode(line)
        if type(event) is not dict or event.get('schema') != 'borsuk-hierarchical-cells-implementation-stage-v1':
            continue
        position = len(events)
        require(position < 12, 'extra completed gate')
        stage = list(local.GATES)[position//2]
        exact(event['stage'], stage, 'ordered completed gates')
        exact(event['command'], local.GATES[stage], 'exact completed gate command')
        if position % 2 == 0:
            for key in ('finished_at', 'exit_status', 'gate_status', 'tests_run', 'required_test_passes'):
                exact(event[key], None, 'gate start')
            passed, summaries = {}, []
        else:
            exact(event, stages[position//2], 'authenticated completed stage receipt')
            exact(event['started_at'], events[-1]['started_at'], 'gate start/completion')
            exact(event['exit_status'], 0, 'gate exit'); exact(event['gate_status'], 0, 'gate status')
            expected = dict.fromkeys(local.TESTS.get(stage, ()), 1)
            exact(event['required_test_passes'], expected, 'named gate test roster')
            require(all(passed.get(n) == 1 for n in expected), 'actual named gate passes')
            if position < 6:
                require(summaries and sum(n for n, _ in summaries) == event['tests_run'] and
                        all(f == 0 for _, f in summaries), 'actual gate test counts')
        events.append(event)
    require(len(events) == 12 and test_builds == 1, 'six completed gates and actual test build')
    return stages


def read_ref(repo, pin, cap=local.CONFIG_CAP):
    fields(pin, 'path bytes sha256', 'reference')
    return local.read_json(dict(pin, path=str(repo_path(repo, pin['path']))), cap)


def qualification(config, repo):
    """Bind completed gates/closeout and the entire native inventory, no builds."""
    refs = config['refs']
    exact({n: p['path'] for n, p in refs.items()}, REF_PATHS, 'reference paths/roster')
    for name, (_, size, digest) in zip(('preparation', 'inventory', 'sources'), local.EVIDENCE):
        exact(body_pin(refs[name]), dict(bytes=size, sha256=digest), 'retained metadata authority')
    values = {name: read_ref(repo, pin, 8 << 20) for name, pin in refs.items()}
    terminal, launch, close = (values[n] for n in ('terminal', 'launch', 'closeout'))
    exact(close['state'], 'terminated', 'qualification host terminated')
    exact(close['nodes'], launch['nodes'], 'qualification SAME IDs')
    require(terminal['instance_id'] == launch['instance_id'] in
            {n['instance_id'] for n in close['nodes'].values()}, 'qualification owned host')
    for name in ('source_commit', 'source_archive_sha256'):
        exact(terminal[name], launch[name], 'qualification archive binding')
    exact(terminal['phase'], 'complete', 'completed qualification phase')
    exact(terminal['status'], 'complete', 'completed qualification status')
    for name in ('exit_code', 'original_exit_code'):
        exact(terminal[name], 0, 'completed qualification exit')
    receipt, verified, manifest = (values[n] for n in ('receipt', 'verification', 'manifest'))
    for name in ('qualified', 'command_started', 'command_completed', 'source_unchanged'):
        exact(receipt[name], True, 'completed native gate receipt')
    for name in ('exit_status', 'gate_status'):
        exact(receipt[name], 0, 'completed native gate status')
    for name in ('qualified', 'instance_terminated', 'all_terminal_bodies_authenticated', 'all_current_native_files_match'):
        exact(verified[name], True, 'root qualification verification')
    exact(verified['corpus_measurement_completed'], False, 'qualification is not science')
    exact(verified['swap_peak_bytes'], 0, 'qualification no swap')
    exact(verified['oom_events'], 0, 'qualification no OOM')
    current = source_hashes(repo)
    require(len(current) == 401 and current == manifest['source_sha256'] == receipt['source_sha256'],
            'exact full qualified native source inventory')
    identity = source_identity(current)
    require(all(v['source_identity_sha256'] == identity for v in (manifest, receipt, terminal, verified)),
            'qualified full native identity')
    for role, filename in (('receipt', 'workspace-receipt.json'), ('manifest', 'native-source-manifest.json')):
        exact(body_pin(refs[role]), terminal['artifacts'][filename], 'terminal authority body')
    native = config['native']
    authority = values['staging_authority']
    exact(authority['schema'], 'borsuk-hierarchical-cells-qualified-staging-authority-v1', 'staging authority schema')
    exact(authority['authority_pending'], False, 'staging qualification pending')
    exact(authority['native_source_identity_sha256'], identity, 'staging native inventory')
    for name in ('source_archive', 'gate_log'):
        exact({k: native[name][k] for k in ('key', 'bytes', 'sha256')},
              {k: authority[name][k] for k in ('key', 'bytes', 'sha256')}, 'qualified transport: '+name)
    for name in ('writer', 'cells'):
        exact({k: native['binaries'][name][k] for k in ('key', 'bytes', 'sha256')},
              authority['binaries'][name], 'qualified binary transport')
    fields(native, 'schema source_commit source_archive sources binaries gate_log', 'native template')
    exact(native['schema'], local.PROOF_SCHEMA, 'root native proof schema')
    exact(native['source_commit'], terminal['source_commit'], 'qualified archive commit')
    exact(native['source_archive']['sha256'], terminal['source_archive_sha256'], 'qualified archive SHA')
    fields(native['sources'], ' '.join(local.SOURCE_FILES), 'native source roster')
    for name, path in local.SOURCE_FILES.items():
        exact(native['sources'][name]['path'], path, 'native source path')
        local.authenticate(dict(native['sources'][name], path=str(repo_path(repo, path))), 8 << 20)
    fields(native['binaries'], 'writer cells', 'native binary roster')
    for name, filename in dict(writer='build_two_bit_generation', cells='hierarchical_semantic_cells').items():
        exact(body_pin(native['binaries'][name]), terminal['artifacts']['binaries/'+filename], 'qualified binary pin')
    exact(body_pin(native['gate_log']), terminal['artifacts']['test.log'], 'completed transcript pin')
    for pin in (native['source_archive'], native['gate_log'], *native['binaries'].values()):
        fields(pin, 'path key bytes sha256', 'native transport pin')
        publication.object_identity({k: pin[k] for k in ('key', 'bytes', 'sha256')})
        transport_name(pin['path'])
    log = dict(body_pin(native['gate_log']), path=str(transport_path(repo, native['gate_log']['path'])))
    completed_gate_log(log, receipt['stages'])
    return values, identity


def source_archive_roster(config, authorities, config_path):
    # qualification has authenticated this exact full native manifest. The
    # helper's retained_pins reads the same three EVIDENCE files in refs.
    native = authorities['manifest']['source_sha256']
    require(len(native) == 401, 'source archive requires all401 native files')
    paths = sorted({config_path, *CODE, *native, *(p['path'] for p in config['refs'].values()),
                    config['native']['gate_log']['path']})
    for path in paths:
        publication.relative(path)
    require(set(local.SOURCE_FILES.values()) <= set(paths), 'required native helper sources')
    require({p for p, _, _ in local.EVIDENCE} <= set(paths), 'required retained authority files')
    cold = [config['native']['source_archive'], *config['native']['binaries'].values()]
    require(not any(p['path'] in paths for p in cold), 'cold transport in source file roster')
    return paths


def qualify(base=Path('.'), *, native_files=False, canary=False):
    repo = Path(base).resolve()
    pin = local.identity(repo/CONFIG)
    config = local.read_json(pin, 256 << 10)
    fields(config, ' '.join(FIXED)+' authority_pending admission code_sha256 refs native '
           'scratch_admission_bytes scratch_reserve_bytes cpu_affinity', 'controller config')
    exact(config['authority_pending'], False, 'root authority freeze pending')
    for name, value in FIXED.items():
        exact(config[name], value, 'fixed protocol: '+name)
    fields(config['admission'], 'kind timeout_seconds negative_truth_hash', 'root admission choice')
    exact(config['admission']['kind'], 'one_query_per_dataset', 'root admission choice pending/unsupported')
    local.integer(config['admission']['timeout_seconds'], 1, 120, 'admission seconds')
    exact(config['admission']['negative_truth_hash'], True, 'negative truth-hash admission')
    exact(set(config['code_sha256']), set(CODE), 'exact controller code roster')
    for name, digest in config['code_sha256'].items():
        exact(local.identity(repo_path(repo, name))['sha256'], digest, 'controller source drift')
    affinity = config['cpu_affinity']
    exact(affinity, [0, 1], 'bootstrap CPU affinity')
    for cpu in affinity:
        local.integer(cpu, 0, 65535, 'CPU affinity')
    local.integer(config['scratch_reserve_bytes'], 1, SCRATCH, 'bootstrap/archive/install/log/canary reserve')
    local.integer(config['scratch_admission_bytes'], 1, SCRATCH, 'whole-worker prospective scratch')
    values, native_identity = qualification(config, repo)
    config_path = str((repo/CONFIG).relative_to(repo))
    paths = source_archive_roster(config, values, config_path)
    extra = sum(p['bytes'] for p in (config['native']['source_archive'], config['native']['gate_log'],
                                    *config['native']['binaries'].values()))
    require(config['scratch_admission_bytes'] >= 1975116322+extra+config['scratch_reserve_bytes'],
            'whole-worker scratch reserve incomplete')
    if native_files:
        # Optional body audit only; launch preflight authenticates metadata,
        # source and the small completed log. Remote science authenticates every
        # full cold body before invoking native code.
        proof = copy.deepcopy(config['native'])
        for name in ('source_archive', 'gate_log'):
            proof[name] = dict(body_pin(proof[name]), path=str(transport_path(repo, proof[name]['path'])))
        for roster in ('sources', 'binaries'):
            proof[roster] = {n: dict(body_pin(p), path=str(transport_path(repo, p['path']))) for n, p in proof[roster].items()}
        with tempfile.TemporaryDirectory(prefix='hierarchical-proof-') as tmp:
            local.validate_proof(local.write_json(Path(tmp)/'proof.json', proof))
    proof = dict(config_path=config_path, config_sha256=pin['sha256'], campaign_schema=CANARY_SCHEMA if canary else SCHEMA,
        code_identity_sha256=ids.sha(ids.encoded(config['code_sha256'])),
        refs_identity_sha256=ids.sha(ids.encoded(config['refs'])), native_identity_sha256=native_identity,
        source_file_count=401, source_archive_paths=paths, source_archive_paths_sha256=ids.sha(ids.encoded(paths)),
        artifact_roster_sha256=ids.sha(ids.encoded(CANARY_ARTIFACTS if canary else ARTIFACTS)),
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
    return config, proof, values


def preflight(base=Path('.'), *, canary=False):
    _, proof, _ = qualify(base, canary=canary)
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty source')
    if not canary:
        require_canary(base, proof)
    return proof


def scratch_snapshot(root, baseline, *, components=None):
    # Directory charge includes archives, extraction, venv, inputs and outputs.
    # Filesystem growth also charges installation/cache writes outside the root;
    # double counting inside-root growth is intentional conservative admission.
    usage = shutil.disk_usage(root)
    directory, growth = local.directory_bytes(root), max(0, usage.used-baseline)
    if components is not None:
        components.update(directory_bytes=directory, filesystem_growth_bytes=growth)
    return directory+growth


def panel_inputs(folder, original, planned, calls, client, check, geometry=local.GEOMETRY):
    rows, dimensions, count, k = geometry
    folder.mkdir()
    pins = {}
    for name, role in STAGED_ROLES.items():
        check()
        descriptor = original['artifacts'][role]
        path = folder/name
        publication.download(client, calls, BUCKET, descriptor, path)
        pins[name] = dict(path=str(path), **body_pin(descriptor))
        check()
    if original['name'] == 'relaion':
        identities = {}
        raw = folder/'raw'
        with positive.open_input(pins['source']['path']) as source, positive.output_file(raw, identities) as write:
            for block in positive.raw_blocks(source, rows, dimensions):
                check(); write(block)
        pins['raw'] = identities['raw']
    else:
        pins['raw'] = pins['source']  # Preserve authenticated f32 bytes unchanged.
    exact(body_pin(pins['raw']), dict(bytes=rows*dimensions*4, sha256=planned['decoded_raw_sha256']), 'decoded original f32 identity')
    # Full source requests were authenticated above. Isolate exactly the consumed
    # prefix before using request_bodies, which requires EOF after that prefix.
    with positive.open_input(pins['requests']['path']) as stream:
        before = positive.stamp(stream)
        lines = [stream.readline(65537) for _ in range(count)]
        require(all(0 < len(line) <= 65536 and line.endswith(b'\n') for line in lines), 'consumed request prefix')
        exact(positive.stamp(stream), before, 'source requests changed')
    bits, identities = hashlib.sha256(), {}
    with positive.output_file(folder/'requests64', identities) as write:
        for body in positive.request_bodies(io.BytesIO(b''.join(lines)), count, dimensions, bits):
            check(); write(body)
    with positive.open_input(pins['truth']['path']) as source, positive.output_file(folder/'truth64', identities) as write:
        # Opaque LE u32 bytes: Python never interprets truth IDs.
        body = source.read(count*k*4)
        require(len(body) == count*k*4, 'short consumed truth prefix')
        write(body)
    pins['requests'], pins['truth'] = identities['requests64'], identities['truth64']
    writer = {n: planned['current_writer_config_draft'][n] for n in ('generation', 'base_epoch', 'low', 'step', 'sq8_object_key', 'sq8_etag')}
    canonical = body_pin(original['artifacts']['control/canonical.bin'])
    binding = dict(schema=local.BINDING_SCHEMA, dataset=original['name'],
        source_reference='unchanged V282 retained quality reference; '+original['artifacts']['reference-paired.jsonl']['key'],
        source_requests=body_pin(original['artifacts']['requests']), source_truth=body_pin(original['artifacts']['truth']),
        **{n+'_sha256': pins[n]['sha256'] for n in ('requests', 'truth', 'raw', 'order', 'sq8')},
        canonical_sha256=canonical['sha256'], first=0, count=count, k=k, metric='cosine', consumed=True,
        **{n+'_f32_hex': positive.f32(writer[n], dimensions).tobytes().hex() for n in ('low', 'step')})
    # Recheck historical streams after reduction; never use cache names as pins.
    for role in ('requests', 'truth'):
        local.authenticate(dict(body_pin(original['artifacts'][role]), path=str(folder/role)), 64 << 20)
    return dict(dataset=original['name'], inputs={n: pins[n] for n in local.INPUTS.split()},
                writer=writer, canonical=canonical), binding, bits.hexdigest()


def admission(plans, proof, limits, deadline, receipt, *, folder, choice, check):
    """Disposable real-input admission, explicitly outside measured counters."""
    folder.mkdir()
    stages, evidence = [], []
    cap = min(deadline, time.monotonic()+choice['timeout_seconds'])
    try:
        for dataset, diagnostic, _, _ in plans:
            check()
            identities = {}
            with positive.open_input(diagnostic['requests']['path']) as source, positive.output_file(folder/(dataset+'-request'), identities) as write:
                write(source.readline(65537))
            with positive.open_input(diagnostic['truth']['path']) as source, positive.output_file(folder/(dataset+'-truth'), identities) as write:
                body = source.read(diagnostic['top_k']*4)
                require(len(body) == diagnostic['top_k']*4, 'admission truth prefix')
                write(body)
            canary = dict(diagnostic, count=1, requests=identities[dataset+'-request'], truth=identities[dataset+'-truth'])
            pin = local.write_json(folder/(dataset+'-config.json'), canary)
            target = folder/(dataset+'-diagnostic.jsonl')
            command = [proof['binaries']['cells']['path'], 'diagnose', pin['path'], pin['sha256'], str(target)]
            local.run_stage(dataset+'-admission', command, proof['binaries']['cells'], pin, folder, limits, cap, stages)
            result = local.identity(target)
            local.validate_diagnostic(result, canary, pin, proof, limits)
            evidence.append(dict(dataset=dataset, config=pin, result=result, status='DIAGNOSTIC'))
            if dataset == 'relaion' and choice['negative_truth_hash']:
                negative = copy.deepcopy(canary)
                negative['truth']['sha256'] = '0'*64
                bad = local.write_json(folder/'negative-config.json', negative)
                target = folder/'negative-diagnostic.jsonl'
                try:
                    local.run_stage('negative-admission', [command[0], 'diagnose', bad['path'], bad['sha256'], str(target)],
                                    proof['binaries']['cells'], bad, folder, limits, cap, stages)
                except ValueError:
                    require(stages[-1]['exit_status'] == 2 and stages[-1]['cleanup_complete'], 'negative admission exit/cleanup')
                else:
                    raise ValueError('corrupted truth hash accepted')
                events = [local.decode(line) for line in target.read_bytes().splitlines()]
                require([e['phase'] for e in events] == ['identity', 'query_frozen', 'all_queries_frozen', 'terminal'], 'negative admission frozen prefix')
                exact(events[-1]['status'], 'INVALID', 'negative admission terminal')
                exact(events[-1]['complete'], False, 'negative admission completeness')
                frozen = events[2]
                prefix = b''.join(target.read_bytes().splitlines(keepends=True)[:2])
                exact(frozen['trace_prefix_sha256'], local.sha(prefix), 'negative synced freeze SHA')
                exact(frozen['trace_prefix_bytes'], len(prefix), 'negative synced freeze bytes')
                evidence.append(dict(dataset=dataset, status='EXPECTED_INVALID', result=local.identity(target)))
        receipt['admission'] = dict(status='ADMITTED', excluded_from_measurement=True, quality_promotion=False,
                                    evidence=evidence, stages=stages)
    finally:
        shutil.rmtree(folder)
        receipt.setdefault('admission', dict(status='INVALID', stages=stages))['disposable_outputs_removed'] = not folder.exists()
        check()


def reduce_diagnostic(path, diagnostic, pin, proof, limits):
    local.validate_diagnostic(local.identity(path), diagnostic, pin, proof, limits)
    hits, query_counts, startup = [], [], {}
    losses = dict.fromkeys(('router_misses', 'boundary_recovered', 'boundary_misses', 'local_nomination_misses', 'final_ranking_misses'), 0)
    with positive.open_input(path) as stream:
        for line in stream:
            event = local.decode(line)
            if event['phase'] == 'identity':
                startup = {n: event[n] for n in ('startup', 'startup_directory', 'directory_admission')}
            if event['phase'] == 'query_frozen':
                a = event['trace']['accounting']
                counts = {n: sum(a[s][n] for s in ('directory', 'whole_cell', 'source', 'refinement'))
                          for n in ('submitted_gets', 'requested_bytes', 'verified_bytes', 'failed_gets')}
                for key in ('directory', 'source', 'refinement'):
                    require(all(v == 0 for v in a[key].values()), 'whole-cell inactive tier charges')
                require(counts['submitted_gets'] <= 32 and counts['requested_bytes'] <= 16 << 20
                        and counts['verified_bytes'] == counts['requested_bytes'] and counts['failed_gets'] == 0,
                        'measured query GET/byte validity')
                query_counts.append(counts)
            if event['phase'] == 'loss_attribution':
                loss = event['loss']
                for name in ('truth_count', 'primary_hits', 'boundary_hits', 'nomination_hits', 'returned_hits', *losses):
                    local.integer(loss[name], 0, diagnostic['top_k'], 'native loss count')
                exact(loss['truth_count'], diagnostic['top_k'], 'truth denominator')
                exact(sum(loss[n] for n in ('boundary_misses', 'local_nomination_misses', 'final_ranking_misses', 'returned_hits')),
                      diagnostic['top_k'], 'loss accounting identity')
                hits.append(loss['returned_hits'])
                for name in losses:
                    losses[name] += loss[name]
    exact(len(query_counts), diagnostic['count'], 'query count')
    exact(len(hits), diagnostic['count'], 'loss count')
    mean, p05 = sum(hits)/(len(hits)*diagnostic['top_k']), sorted(hits)[math.ceil(.05*len(hits))-1]
    return dict(status='PASS' if mean >= .98 and p05 >= 95 else 'FAIL',
        mean_recall_at_100=mean, p05_hits=p05, queries=len(hits), returned_hits=sum(hits),
        losses=losses, per_query_local_reads=query_counts, startup=startup,
        physical_s3_measured=False, latency_baseline=False, vendor_win=False,
        retained_reference='unchanged V282 quality only; current writer is not a V282 latency baseline')


def canary_objects(config, authorities):
    native = config['native']
    pins = [native['source_archive'], native['gate_log'], *native['binaries'].values()]
    pins += [item['artifacts'][role] for item in authorities['sources']['items'] for role in STAGED_ROLES.values()]
    pins = [{k: p[k] for k in ('key', 'bytes', 'sha256')} for p in pins]
    require(len(pins) == len({p['key'] for p in pins}) == 18, 'canary native4/dataset14 roster')
    for pin in pins:
        publication.object_identity(pin)
    return pins


def infrastructure_canary(config, authorities, client, calls, scratch, check, deadline):
    # Imports and service-model inspection are real. No native process, helper
    # preparation, panel producer, request converter or truth reader is invoked.
    for name in ('numpy', 'pyarrow', 'boto3', 'botocore'):
        importlib.import_module(name)
    versions = {n: importlib.metadata.version(n) for n in (*FIXED['versions'], *SDK_VERSIONS)}
    exact(versions, dict(FIXED['versions'], **SDK_VERSIONS), 'canary installed versions')
    model = client.meta.service_model
    require('IfNoneMatch' in model.operation_model('PutObject').input_shape.members,
            'SDK conditional PutObject service model')
    for operation in ('HeadObject', 'GetObject'):
        require({'Bucket', 'Key'} <= set(model.operation_model(operation).input_shape.members), 'SDK read service model')
    cli = subprocess.run([sys.executable, '-m', MODULE], capture_output=True, text=True,
        timeout=min(30, max(.001, deadline-time.monotonic())),
        env=dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1'))
    require(cli.returncode == 2 and 'INVALID:' in cli.stderr and 'CLI:' in cli.stderr and not cli.stdout,
            'actual controller usage/exit smoke')
    for pin in canary_objects(config, authorities):
        check()
        response = publication.sdk_call(client, calls, 'head_object', pin['key'], BUCKET)
        exact(response['ContentLength'], pin['bytes'], 'canary HEAD declared length')
    log = config['native']['gate_log']
    require(log['bytes'] <= 1 << 20, 'canary selected body cap')
    check()
    publication.download(client, calls, BUCKET, {k: log[k] for k in ('key', 'bytes', 'sha256')}, scratch/'gate.log')
    completed_gate_log(dict(body_pin(log), path=str(scratch/'gate.log')), authorities['receipt']['stages'])
    check()
    return dict(schema=CANARY_SCHEMA, status='GO', complete=True, scientific_performance_evidence=False,
        versions=versions, sdk_conditional_put_model=True, cli_exit_status=cli.returncode,
        cli_stderr=cli.stderr, sdk_calls=calls, authenticated_log=body_pin(log),
        head_proves='object presence and declared length only; no body SHA inference',
        ann_queries=0, native_processes=0, truth_or_panel_body_reads=0, dataset_payload_gets=0)


def stage(repo, output, worker_root, *, canary=False):
    repo, out, root = map(lambda p: Path(p).resolve(), (repo, output, worker_root))
    require(out.is_relative_to(root) and repo.is_relative_to(root), 'whole-worker scratch ownership')
    require(not out.exists(), 'output exists')
    config, proof, authorities = qualify(repo, canary=canary)
    exact(proof['config_sha256'], os.environ.get('BORSUK_HIERARCHICAL_CONFIG_SHA256'), 'bootstrap config binding')
    exact(proof['source_archive_paths_sha256'], os.environ.get('BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256'),
          'bootstrap source file roster binding')
    remaining = int(os.environ['BORSUK_HIERARCHICAL_DEADLINE_EPOCH'])-time.time()
    wall, memory, scratch_cap = (CANARY_WALL, CANARY_MEMORY, CANARY_SCRATCH) if canary else (WALL, MEMORY, SCRATCH)
    cpu_quota, affinity = (100, [0]) if canary else (config['cpu_quota_percent'], config['cpu_affinity'])
    require(0 < remaining <= wall, 'cumulative worker deadline')
    baseline = int(os.environ['BORSUK_HIERARCHICAL_SCRATCH_BASE_USED'])
    deadline = time.monotonic()+remaining
    limits = dict(LIMITS, memory_max_bytes=memory, scratch_max_bytes=scratch_cap,
                  timeout_seconds=max(1, int(remaining)), cpu_affinity=affinity)
    before = local.resource_snapshot(limits)
    group = Path(before['path'])
    quota, period = map(int, (group/'cpu.max').read_text().split())
    require(quota*100 == cpu_quota*period and
            (group/'pids.max').read_text().strip() == str(config['tasks_max']), 'CPU/tasks cgroup')
    before.update(cpu_max=(group/'cpu.max').read_text().strip(), tasks_max=(group/'pids.max').read_text().strip())
    versions = {n: importlib.metadata.version(n) for n in FIXED['versions']}
    exact(versions, FIXED['versions'], 'pinned decoder versions')
    out.mkdir()
    scratch = out/'scratch'
    scratch.mkdir()
    calls, peaks, errors, stopped = [], {'scratch_bytes': 0}, [], threading.Event()
    client = None
    result = dict(status='INVALID', complete=False, physical_s3_measured=False, vendor_win=False)
    original_handler = signal.getsignal(signal.SIGALRM)
    original_term = signal.getsignal(signal.SIGTERM)
    original_timer = signal.setitimer(signal.ITIMER_REAL, remaining)
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('cumulative worker deadline')))
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('worker terminated')))
    def check():
        amount = scratch_snapshot(root, baseline)
        peaks['scratch_bytes'] = max(peaks['scratch_bytes'], amount)
        require(amount <= scratch_cap, 'whole-worker scratch cap')
        require(time.monotonic() < deadline, 'cumulative worker deadline')
        require(not errors, 'whole-worker monitor: '+str(errors))
    def monitor():
        while not stopped.wait(1):
            try:
                check()
            except Exception as error:
                errors.append(str(error))
                os.kill(os.getpid(), signal.SIGALRM)
                return
    thread = threading.Thread(target=monitor, daemon=True)
    try:
        check(); thread.start()
        ids.write(out/'config.json', (repo/CONFIG).read_bytes())
        local.write_json(out/'source-qualification.json', proof)
        local.write_json(out/'tool-versions.json', dict(versions, python=sys.version, executable=sys.executable))
        client = publication.sdk_client(REGION)  # no retry, bounded read/connect timeout
        if canary:
            result = infrastructure_canary(config, authorities, client, calls, scratch, check, deadline)
            result['config_sha256'] = proof['config_sha256']
            local.write_json(out/'canary.json', result)
        else:
            native = copy.deepcopy(config['native'])
            for name, original in [('source_archive', native['source_archive']), ('gate_log', native['gate_log']),
                                   *[(n, p) for n, p in native['binaries'].items()]]:
                check()
                path = scratch/('native-'+name)
                publication.download(client, calls, BUCKET, {k: original[k] for k in ('key', 'bytes', 'sha256')}, path)
                pin = dict(body_pin(original), path=str(path))
                if name in ('writer', 'cells'):
                    path.chmod(0o700); native['binaries'][name] = pin
                else:
                    native[name] = pin
            native['sources'] = {n: dict(body_pin(p), path=str(repo_path(repo, p['path']))) for n, p in native['sources'].items()}
            native_pin = local.write_json(out/'native-proof.json', native)
            qualified = local.validate_proof(native_pin)
            items, reductions = [], {}
            for original, planned in zip(authorities['sources']['items'], authorities['preparation']['items']):
                item, binding, bits = panel_inputs(scratch/original['name'], original, planned, calls, client, check)
                item['panel_binding'] = local.write_json(out/(item['dataset']+'-panel-binding.json'), binding)
                items.append(item); reductions[item['dataset']] = dict(query_f32_sha256=bits, binding=item['panel_binding'])
            exact(sum(c.get('verified_bytes', 0) for c in calls[4:]), FIXED['source_encoded_bytes'], 'fourteen dataset bytes')
            exact(len(calls), 18, 'four qualified artifacts plus fourteen dataset GETs')
            local.write_json(out/'staging.json', dict(sdk_calls=calls, reductions=reductions, truth_decoded=False,
                                                    physical_query_s3_measured=False))
            limits['timeout_seconds'] = max(1, int(deadline-time.monotonic()))
            prepared = dict(schema=local.SCHEMA, qualification=native_pin, items=items, resources=limits)
            prepared_pin = local.write_json(out/'local-config.json', prepared)
            def hook(plans, proof, caps, inner_deadline, receipt):
                admission(plans, proof, caps, min(deadline, inner_deadline), receipt,
                          folder=out/'admission-scratch', choice=config['admission'], check=check)
                local.write_json(out/'admission.json', receipt['admission'])
            check()
            receipt = local.prepare(prepared, prepared_pin, out/'measurement', before_diagnostics=hook)
            check()
            summaries = {}
            for dataset, pin in receipt['diagnostic_configs'].items():
                diagnostic = local.read_json(pin)
                summaries[dataset] = reduce_diagnostic(receipt['results'][dataset]['path'], diagnostic, pin, qualified, limits)
            result = dict(status='PASS' if all(v['status'] == 'PASS' for v in summaries.values()) else 'FAIL',
                complete=True, items=summaries, physical_s3_measured=False, vendor_win=False,
                scientific_qualification=False, source_only_native_qualification=True)
    except BaseException as error:
        result['error'] = type(error).__name__+': '+str(error)
        raise
    finally:
        stopped.set()
        if thread.ident is not None:
            thread.join(timeout=2)
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, original_handler)
        signal.signal(signal.SIGTERM, original_term)
        if original_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, original_timer[0], original_timer[1])
        shutil.rmtree(scratch)
        for path in (out/'measurement').glob('*'):
            if path.is_dir():
                shutil.rmtree(path)
        after = local.resource_snapshot(limits)
        after.update(cpu_max=(group/'cpu.max').read_text().strip(), tasks_max=(group/'pids.max').read_text().strip())
        require(not thread.is_alive(), 'scratch monitor stopped')
        require(all(dict(line.split() for line in before['memory.events'].splitlines()).get(k) ==
                    dict(line.split() for line in after['memory.events'].splitlines()).get(k)
                    for k in ('oom', 'oom_kill', 'oom_group_kill')), 'worker OOM closure')
        local.write_json(out/'worker-cgroup.json', dict(before=before, after=after, closed=True))
        local.write_json(out/'resources.json', dict(peaks, sdk_calls=calls, monitor_errors=errors, deadline_seconds=remaining,
                                                  wall_seconds=remaining-(deadline-time.monotonic())))
        if client is not None:
            client.close()
        local.write_json(out/'cleanup.json', dict(scratch_removed=not scratch.exists(), monitor_stopped=True,
                                                sdk_client_closed=client is not None,
                                                native_processes_concurrent_max=0 if canary else 1))
        local.write_json(out/'summary.json', result)
        require(scratch_snapshot(root, baseline) <= scratch_cap, 'final whole-worker scratch cap')
        fd = os.open(out, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return result


def user_data(commit, archive_sha, archive_key, prefix, qualification, *, canary=False):
    wall, scratch_cap = (CANARY_WALL, CANARY_SCRATCH) if canary else (WALL, SCRATCH)
    schema, artifacts = (CANARY_SCHEMA, CANARY_ARTIFACTS) if canary else (SCHEMA, ARTIFACTS)
    prefix_root = CANARY_PREFIX if canary else PREFIX
    memory, cpu, cpus, threads = ('256M', 100, '0', 1) if canary else ('2G', 200, '0,1', 2)
    stage_flag, unit = ('--stage-canary', 'hierarchical-100k-canary') if canary else ('--stage', 'hierarchical-100k')
    require(re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha), 'archive identity')
    require(re.fullmatch(re.escape(prefix_root)+r'a[0-9]{4}', prefix), 'attempt prefix')
    exact(qualification['config_path'], str(CONFIG), 'bootstrap config path')
    _, bootstrap = ids.lifecycle()
    adapter = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key': 'unused'}, native_publisher={'key': 'unused'})
    with patch.multiple(bootstrap, WALL=wall, SCHEMA=schema, ARTIFACTS=artifacts,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, '_offered', return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    command = f'''phase=install
test "$(uname -m)" = x86_64
python3.12 -m venv "$root/venv"
export PIP_CACHE_DIR="$root/pip-cache" TMPDIR="$root"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0 boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
rm -rf -- "$root/pip-cache"
sync -f "$root"
phase={"infrastructure-canary" if canary else "paired-diagnostic"}
remaining=$((BORSUK_HIERARCHICAL_DEADLINE_EPOCH-$(date +%s)))
test "$remaining" -gt 0
systemd-run --unit={unit} --wait --pipe -p MemoryMax={memory} -p MemorySwapMax=0 -p CPUQuota={cpu}% -p TasksMax=512 -p RuntimeMaxSec="$remaining" -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=TMPDIR="$root" \\
 --setenv=BORSUK_HIERARCHICAL_CONFIG_SHA256={qualification['config_sha256']} \\
 --setenv=BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256={qualification['source_archive_paths_sha256']} \\
 --setenv=BORSUK_HIERARCHICAL_DEADLINE_EPOCH="$BORSUK_HIERARCHICAL_DEADLINE_EPOCH" \\
 --setenv=BORSUK_HIERARCHICAL_SCRATCH_BASE_USED="$BORSUK_HIERARCHICAL_SCRATCH_BASE_USED" \\
 --setenv=OPENBLAS_NUM_THREADS={threads} --setenv=OMP_NUM_THREADS={threads} --setenv=MKL_NUM_THREADS={threads} --setenv=RAYON_NUM_THREADS={threads} \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=5 "$remaining" \\
 taskset -c {cpus} "$root/venv/bin/python" -m {MODULE} {stage_flag} "$root/repo" "$root/screen" "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    body = body[:start]+command+body[end:]
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/'+unit)
    body = body.replace('python3-boto3 python3.12', 'python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    body = body.replace('phase=bootstrap\n', f'''phase=bootstrap
export BORSUK_HIERARCHICAL_DEADLINE_EPOCH=$(($(date +%s)+{wall}))
export BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=$(df -B1 --output=used "$root" | tail -1 | tr -d ' ')
''', 1)
    body = body.replace('exec >run.log 2>&1\n', f'''exec >run.log 2>&1
scratch_owner=$$
(while kill -0 "$scratch_owner" 2>/dev/null; do
 rooted=$(du -sb "$root" | cut -f1)
 used=$(df -B1 --output=used "$root" | tail -1 | tr -d ' ')
 growth=$((used-BORSUK_HIERARCHICAL_SCRATCH_BASE_USED))
 if [ "$growth" -lt 0 ]; then growth=0; fi
 if [ "$((rooted+growth))" -gt {scratch_cap} ]; then kill -TERM "$scratch_owner"; exit; fi
 sleep 1
done) &
scratch_watch_pid=$!
''', 1)
    body = body.replace('  trap - EXIT TERM\n', '''  trap - EXIT TERM
  if [ -n "${scratch_watch_pid:-}" ]; then
    kill "$scratch_watch_pid" 2>/dev/null || true
    wait "$scratch_watch_pid" 2>/dev/null || true
  fi
''', 1)
    body = body.replace('./aws/install', './aws/install --install-dir "$root/aws-cli" --bin-dir "$root/bin"')
    body = body.replace('cli_version=$(aws --version)', 'export PATH="$root/bin:$PATH"\ncli_version=$(aws --version)')
    # Remove only authenticated disposable transport/install files. Their peak
    # existed under the original baseline/watch; installed assets remain charged.
    body = body.replace('phase=source-download\n', 'rm -rf -- "$root/aws" "$root/awscliv2.zip"\nphase=source-download\n')
    body = body.replace('mkdir repo && tar -xzf source.tar.gz -C repo\n',
                        'mkdir repo && tar -xzf source.tar.gz -C repo\nrm -f -- "$root/source.tar.gz"\nsync -f "$root"\n')
    # The scientific bootstrap uploads terminal last. Durably sync artifacts and
    # the terminal before its marker/publication without changing owned lifecycle.
    body = body.replace('  aws_ready=0\n', '  sync -f "$root" || code=96\n  aws_ready=0\n', 1)
    body = body.replace('write_terminal && terminal_ready=1', 'write_terminal && sync -f terminal.json && terminal_ready=1')
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<terminal>', 'exec')
    require(len(body.encode()) < 16384 and all(n not in body for n in ('rustup', 'cargo', 'unused')), 'scientific bootstrap boundary')
    return body


def poll(ec2, s3, prefix, instance_id, started, *, canary=False):
    shared, _ = ids.lifecycle()
    with patch.object(shared, 'WALL', CANARY_WALL if canary else WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def validate_canary(receipt, config, authorities):
    for name, value in dict(schema=CANARY_SCHEMA, status='GO', complete=True,
            scientific_performance_evidence=False, sdk_conditional_put_model=True,
            cli_exit_status=2, ann_queries=0, native_processes=0,
            truth_or_panel_body_reads=0, dataset_payload_gets=0).items():
        exact(receipt[name], value, 'infrastructure canary: '+name)
    exact(receipt['versions'], dict(FIXED['versions'], **SDK_VERSIONS), 'canary imports')
    require('INVALID:' in receipt['cli_stderr'] and 'CLI:' in receipt['cli_stderr'], 'canary actual CLI usage')
    calls, objects = receipt['sdk_calls'], canary_objects(config, authorities)
    require(len(calls) == 19, 'canary HEAD18/GET1')
    for call, pin in zip(calls[:18], objects):
        exact(call['operation'], 'head_object', 'canary HEAD only')
        exact(call['key'], pin['key'], 'canary declared object')
        exact(call['outcome'], 'returned', 'canary HEAD returned')
        exact(call['declared_bytes'], pin['bytes'], 'canary HEAD length')
        require('verified_sha256' not in call, 'HEAD is not authenticated body evidence')
    log, call = config['native']['gate_log'], calls[-1]
    exact(call['operation'], 'get_object', 'single canary body GET')
    exact(call['key'], log['key'], 'only small gate log')
    exact(call['outcome'], 'returned', 'canary log returned')
    exact(call['declared_bytes'], log['bytes'], 'canary log declared bytes')
    exact(call['verified_bytes'], log['bytes'], 'canary log verified bytes')
    exact(call['verified_sha256'], log['sha256'], 'canary log verified SHA')
    exact(receipt['authenticated_log'], body_pin(log), 'canary log pin')
    require(log['bytes'] <= 1 << 20, 'canary body cap')


def replay(out, *, canary=False):
    out = Path(out)
    schema, artifacts = (CANARY_SCHEMA, CANARY_ARTIFACTS) if canary else (SCHEMA, ARTIFACTS)
    wall, memory, scratch_cap = (CANARY_WALL, CANARY_MEMORY, CANARY_SCRATCH) if canary else (WALL, MEMORY, SCRATCH)
    reservation, launch, close, terminal = (json.loads((out/n).read_bytes()) for n in (
        'aws-reservation.json', 'aws-launch.json', 'aws-closeout.json', 'aws-terminal.json'))
    exact(close['nodes'], launch['nodes'], 'SAME owned IDs')
    exact(close['state'], 'terminated', 'termination before collection')
    require(terminal['instance_id'] == launch['instance_id'] in {v['instance_id'] for v in close['nodes'].values()}, 'owned terminal host')
    for key in ('source_commit', 'source_archive_sha256'):
        exact(terminal[key], reservation[key], 'terminal archive binding')
        exact(terminal[key], launch[key], 'launch archive binding')
    exact(terminal['schema'], schema, 'terminal campaign schema')
    exact(reservation['schema'], schema, 'reserved campaign schema')
    if canary:
        exact(reservation['wall_seconds'], CANARY_WALL, 'canary reserved deadline')
        exact(reservation['compute_cap_usd'], .12, 'canary compute cap')
        exact(reservation['ebs_s3_allowance_usd'], .05, 'canary ancillary cap')
    for name in TERMINAL_IDENTITIES:
        exact(terminal[name], reservation['qualification'][name], 'terminal authority: '+name)
    reserved = reservation['qualification']
    paths = reserved['source_archive_paths']
    shared, _ = ids.lifecycle()
    shared.validate_source_archive_paths(paths)
    exact(paths, sorted(paths), 'reserved source file roster order')
    exact(ids.sha(ids.encoded(paths)), terminal['source_archive_paths_sha256'], 'reserved source file roster SHA')
    exact(terminal['source_file_count'], 401, 'reserved native401 count')
    require(set(terminal['artifacts']) <= set(artifacts), 'terminal artifact roster')
    for name, pin in terminal['artifacts'].items():
        exact(body_pin(local.identity(out/name)), pin, 'collected body identity')
    complete = terminal['phase'] == terminal['status'] == 'complete' and terminal['exit_code'] == 0
    if complete:
        exact(terminal['original_exit_code'], 0, 'original worker success')
        exact(set(terminal['artifacts']), set(artifacts), 'complete artifact roster')
        exact(local.decode((out/'screen/source-qualification.json').read_bytes()), reserved,
              'deployed source qualification ancestry')
        config = local.decode((out/'screen/config.json').read_bytes())
        cpu = 100 if canary else config['cpu_quota_percent']
        exact(local.sha((out/'screen/config.json').read_bytes()), terminal['config_sha256'], 'collected config')
        if canary:
            receipt = local.decode((out/'screen/canary.json').read_bytes())
            summary = local.decode((out/'screen/summary.json').read_bytes())
            exact(receipt, summary, 'canary closed summary')
            exact(receipt['config_sha256'], terminal['config_sha256'], 'canary config binding')
            authorities, identity = qualification(config, Path(__file__).resolve().parents[1])
            exact(identity, terminal['native_identity_sha256'], 'canary native identity')
            validate_canary(receipt, config, authorities)
        else:
            receipt = local.decode((out/'screen/measurement/receipt.json').read_bytes())
            exact(receipt['status'], 'DIAGNOSTIC', 'measurement status')
            exact(receipt['complete'], True, 'measurement complete')
            summaries = {}
            for dataset, pin in receipt['diagnostic_configs'].items():
                body = out/'screen/measurement'/(dataset+'-diagnose.json')
                exact(body_pin(local.identity(body)), body_pin(pin), 'collected diagnostic config')
                diagnostic = local.decode(body.read_bytes())
                summaries[dataset] = reduce_diagnostic(out/'screen/measurement'/(dataset+'-diagnostic.jsonl'), diagnostic, pin, receipt['proof'], receipt['resource_limits'])
            summary = local.decode((out/'screen/summary.json').read_bytes())
            exact(summary['complete'], True, 'complete quality reduction')
            exact(summary['items'], summaries, 'root quality reduction')
            exact(summary['status'], 'PASS' if all(v['status'] == 'PASS' for v in summaries.values()) else 'FAIL', 'quality status')
            exact(summary['physical_s3_measured'], False, 'local query counters only')
            exact(config['policy'], local.POLICY, 'collected arm')
        clean = local.decode((out/'screen/cleanup.json').read_bytes())
        exact(clean['scratch_removed'], True, 'disposable staging removed')
        exact(clean['monitor_stopped'], True, 'worker monitor cleanup')
        if canary:
            exact(clean['sdk_client_closed'], True, 'canary SDK client cleanup')
            exact(clean['native_processes_concurrent_max'], 0, 'canary no native execution')
        counters = local.decode((out/'screen/worker-cgroup.json').read_bytes())
        exact(counters['closed'], True, 'worker resource closure')
        for snapshot in (counters['before'], counters['after']):
            require(0 < int(snapshot['memory.max']) <= memory and
                    int(snapshot['memory.peak']) <= memory, 'worker memory cap')
            exact(snapshot['memory.swap.max'], '0', 'worker no swap')
            exact(int(snapshot['memory.swap.peak']), 0, 'worker swap peak')
            q, p = map(int, snapshot['cpu_max'].split())
            require(q*100 == cpu*p, 'worker CPU quota')
            exact(int(snapshot['tasks_max']), config['tasks_max'], 'worker tasks cap')
        require(counters['before']['path'] == counters['after']['path'], 'same worker cgroup')
        before_events, after_events = (dict(line.split() for line in counters[k]['memory.events'].splitlines())
                                       for k in ('before', 'after'))
        require(all(before_events.get(k) == after_events.get(k) for k in ('oom', 'oom_kill', 'oom_group_kill')), 'worker no OOM')
        resources = local.decode((out/'screen/resources.json').read_bytes())
        require(resources['scratch_bytes'] <= scratch_cap and 0 <= resources['wall_seconds'] <= wall, 'worker scratch/deadline closure')
        exact(len(resources['sdk_calls']), 19 if canary else 18, 'closed SDK call roster')
        if canary:
            exact(resources['sdk_calls'], receipt['sdk_calls'], 'canary SDK resource receipt')
        if not canary:
            admission_receipt = local.decode((out/'screen/admission.json').read_bytes())
            for name, expected in dict(status='ADMITTED', excluded_from_measurement=True, quality_promotion=False, disposable_outputs_removed=True).items():
                exact(admission_receipt[name], expected, 'separate real-input admission')
    else:
        require(terminal['status'] == 'failed' and terminal['exit_code'] != 0, 'failed terminal closure')
    return dict(executed=complete, physical_s3_measured=False, vendor_win=False)


def collect(s3, prefix, out, instance_id, commit, digest, *, canary=False):
    schema, artifacts = (CANARY_SCHEMA, CANARY_ARTIFACTS) if canary else (SCHEMA, ARTIFACTS)
    launch, close = (json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json', 'aws-closeout.json'))
    exact(close['nodes'], launch['nodes'], 'collection SAME IDs')
    exact(launch['instance_id'], instance_id, 'collection host')
    exact(launch['prefix'], prefix, 'collection prefix')
    with patch.multiple(ids, SCHEMA=schema, ARTIFACTS=artifacts, replay=lambda out: replay(out, canary=canary)):
        return ids.collect(s3, prefix, out, instance_id, commit, digest)


def require_canary(base, proof):
    pointer = local.read_json(local.identity(Path(base)/ROOT/'canary-admission.json'))
    fields(pointer, 'schema attempt config_sha256 code_identity_sha256 refs_identity_sha256 '
           'native_identity_sha256 source_archive_paths_sha256 terminal_sha256', 'root canary admission')
    exact(pointer['schema'], 'borsuk-hierarchical-100k-canary-admission-v2', 'root canary pointer')
    require(re.fullmatch(r'a[0-9]{4}', pointer['attempt']), 'canary admission attempt')
    out = Path(base)/ROOT/'canary'/pointer['attempt']
    exact(local.identity(out/'aws-terminal.json')['sha256'], pointer['terminal_sha256'], 'selected canary terminal')
    terminal = local.decode((out/'aws-terminal.json').read_bytes())
    for name in ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256',
                 'source_archive_paths_sha256'):
        exact(pointer[name], proof[name], 'canary admission current authority')
        exact(terminal[name], proof[name], 'canary terminal current authority')
    require(replay(out, canary=True)['executed'], 'terminated infrastructure canary GO required')


def canary_campaign():
    return SimpleNamespace(ROOT=ROOT, NAME='canary', SCHEMA=CANARY_SCHEMA,
        PREFIX=CANARY_PREFIX, TOKEN_PREFIX='hierarchical-100k-canary-', TAG=TAG+'-canary',
        WALL=CANARY_WALL, COMPUTE_CAP=.12, INSTANCE_TYPE=INSTANCE_TYPE, IMAGE_ID=IMAGE_ID,
        ROOT_DEVICE_NAME=ROOT_DEVICE_NAME, SUBNET=SUBNET, SPOT_MAX_USD_PER_HOUR=SPOT_MAX_USD_PER_HOUR,
        ARTIFACTS=CANARY_ARTIFACTS, preflight=lambda: preflight(canary=True),
        user_data=lambda *args: user_data(*args, canary=True),
        poll=lambda *args: poll(*args, canary=True), collect=lambda *args: collect(*args, canary=True))


def canary_reservation(*args, **kwargs):
    # Shared main owns ACK/fsync/terminate+wait unchanged. Its sole hardcoded
    # budget default is replaced only for this campaign's reservation constructor.
    if kwargs.get('schema') == CANARY_SCHEMA and 'ebs_s3_allowance_usd' in kwargs:
        exact(kwargs['ebs_s3_allowance_usd'], .15, 'shared lifecycle allowance default')
        kwargs['ebs_s3_allowance_usd'] = .05
    return dict(*args, **kwargs)


def main(attempt, *, canary=False):
    require(re.fullmatch(r'a[0-9]{4}', attempt), 'attempt must be aNNNN')
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight(canary=canary)  # No cloud before frozen source/metadata/log.
        shared, _ = ids.lifecycle()
        if canary:
            with patch.object(shared, 'dict', canary_reservation, create=True):
                return shared.main(attempt, campaign=canary_campaign())
        return shared.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def metadata_fixture(repo, config):
    """Test-only source/metadata copy; no cold archive, binary or dataset body."""
    here = Path(__file__).resolve().parents[1]
    manifest = read_ref(here, config['refs']['manifest'], 8 << 20)
    for name in source_archive_roster(config, dict(manifest=manifest), str(CONFIG)):
        if name == str(CONFIG):
            continue
        target = repo/name; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(here/name, target)
    target = repo/CONFIG; target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(local.canonical(config))
    return target


def canary_self_check():
    """Metadata/log only, actual imports/CLI; SDK/cloud operations mocked."""
    from contextlib import ExitStack
    from datetime import datetime, timezone
    from unittest.mock import Mock
    import botocore.session
    module, here = sys.modules[__name__], Path(__file__).resolve().parents[1]
    local.resource_snapshot(dict(memory_max_bytes=CANARY_MEMORY, cpu_affinity=[0]))
    exact(sorted(os.sched_getaffinity(0)), [0], 'canary check CPU1')
    def rejects(action):
        try:
            action()
        except (ValueError, OSError, AssertionError, TimeoutError):
            return
        raise AssertionError('negative canary accepted')
    with tempfile.TemporaryDirectory(prefix='hierarchical-infrastructure-check-') as tmp:
        root = Path(tmp)
        config = local.decode(Path('/tmp/borsuk-hierarchical-100k-config-draft.json').read_bytes())
        original = copy.deepcopy(config)
        # This is a temporary refresh of root's actual metadata shape, never a
        # modification to its pending draft or its immutable gate receipts.
        config['authority_pending'] = False
        config['code_sha256'] = {p: local.identity(here/p)['sha256'] for p in CODE}
        metadata_repo = root/'metadata-repo'
        controller = metadata_fixture(metadata_repo, config)
        cold_paths = {p['path'] for p in (config['native']['source_archive'], *config['native']['binaries'].values())}
        open_input = positive.open_input
        def metadata_only(path):
            require(not any(str(path).endswith('/'+p) for p in cold_paths), 'preflight opened cold native body')
            return open_input(path)
        with patch.object(positive, 'open_input', side_effect=metadata_only):
            validated, proof, authorities = qualify(metadata_repo, canary=True)
            exact(validated, config, 'actual root draft metadata refresh')
            rejects(lambda: qualify(metadata_repo, native_files=True))
            for field, value in (('authority_pending', True), ('code_sha256', original['code_sha256'])):
                damaged = dict(config, **{field: value}); controller.write_bytes(local.canonical(damaged))
                rejects(lambda: qualify(metadata_repo))
            for path in ('../escape', 'assets/../escape', 'assets//writer', 'assets/writer\n'):
                damaged = copy.deepcopy(config); damaged['native']['binaries']['writer']['path'] = path
                controller.write_bytes(local.canonical(damaged)); rejects(lambda: qualify(metadata_repo))
        exact(local.decode(Path('/tmp/borsuk-hierarchical-100k-config-draft.json').read_bytes()), original,
              'root pending draft untouched')
        repo = root/'repo'; repo.mkdir()
        (repo/CONFIG).parent.mkdir(parents=True)
        config_pin = local.write_json(repo/CONFIG, config)
        exact(config_pin['sha256'], proof['config_sha256'], 'canary fixture config identity')
        model = botocore.session.get_session().get_service_model('s3')
        descriptors = {p['key']: p for p in canary_objects(config, authorities)}
        log = (here/config['native']['gate_log']['path']).read_bytes()
        env = dict(BORSUK_HIERARCHICAL_CONFIG_SHA256=proof['config_sha256'],
            BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256=proof['source_archive_paths_sha256'],
            BORSUK_HIERARCHICAL_DEADLINE_EPOCH=str(int(time.time())+60),
            BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=str(shutil.disk_usage(root).used))
        with ExitStack() as stack:
            stack.enter_context(patch.dict(os.environ, env))
            stack.enter_context(patch.object(module, 'qualify', return_value=(config, proof, authorities)))
            forbidden = [stack.enter_context(patch.object(owner, name, side_effect=AssertionError('forbidden canary '+name)))
                for owner, name in ((local, 'prepare'), (local, 'validate_proof'), (local, 'run_stage'),
                    (positive, 'raw_blocks'), (positive, 'request_bodies'),
                    (module, 'panel_inputs'), (module, 'admission'))]
            with patch.dict(os.environ, {'BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256': '0'*64}):
                rejects(lambda: stage(repo, root/'wrong-roster', root, canary=True))
            require(not (root/'wrong-roster').exists(), 'roster refusal before staging/SDK')
            for mode in ('valid', 'short-log', 'wrong-hash', 'head-length', 'scratch', 'cli-nonzero', 'sdk-model'):
                client = Mock()
                client.meta.service_model = model
                client.head_object.side_effect = lambda **kw: {'ContentLength': descriptors[kw['Key']]['bytes']}
                def get(**kw):
                    exact(kw['Key'], config['native']['gate_log']['key'], 'no GT/panel/native payload GET')
                    body = log[:-1] if mode == 'short-log' else b'X'+log[1:] if mode == 'wrong-hash' else log
                    return dict(ContentLength=len(log), Body=io.BytesIO(body))
                client.get_object.side_effect = get
                if mode == 'head-length': client.head_object.return_value = {'ContentLength': 1}; client.head_object.side_effect = None
                if mode == 'sdk-model': client.meta.service_model = SimpleNamespace(operation_model=lambda _: SimpleNamespace(input_shape=SimpleNamespace(members={})))
                out = root/mode
                with ExitStack() as faults:
                    faults.enter_context(patch.object(publication, 'sdk_client', return_value=client))
                    if mode == 'scratch': faults.enter_context(patch.object(module, 'scratch_snapshot', return_value=CANARY_SCRATCH+1))
                    if mode == 'cli-nonzero': faults.enter_context(patch.object(subprocess, 'run', return_value=subprocess.CompletedProcess([], 7, '', 'bad CLI')))
                    if mode == 'valid':
                        result = stage(repo, out, root, canary=True)
                        validate_canary(result, config, authorities)
                        exact(len(client.head_object.call_args_list), 18, 'actual mock HEAD18')
                        exact(len(client.get_object.call_args_list), 1, 'actual mock GET1')
                        client.close.assert_called_once()
                    else:
                        rejects(lambda: stage(repo, out, root, canary=True))
                        exact(local.decode((out/'summary.json').read_bytes())['status'], 'INVALID', 'canary error INVALID')
                require(not (out/'scratch').exists(), 'canary scratch cleanup')
            for spy in forbidden: spy.assert_not_called()
        # Collect/replay exactly the closed canary roster and all resource pins.
        out, screen = root/'closed', root/'valid'
        (out/'screen').mkdir(parents=True)
        for name in CANARY_ARTIFACTS:
            destination = out/name
            if name.startswith('screen/'): shutil.copyfile(screen/name.removeprefix('screen/'), destination)
            else: destination.write_bytes(b'synthetic lifecycle evidence\n')
        source = dict(source_commit='a'*40, source_archive_sha256='b'*64)
        terminal = dict(schema=CANARY_SCHEMA, instance_id='i-canary', phase='complete', status='complete',
            exit_code=0, original_exit_code=0, **source,
            **{n: proof[n] for n in TERMINAL_IDENTITIES},
            artifacts={n: body_pin(local.identity(out/n)) for n in CANARY_ARTIFACTS})
        nodes = {'0': dict(instance_id='i-canary')}
        for name, value in (('aws-reservation.json', dict(schema=CANARY_SCHEMA, wall_seconds=480,
                compute_cap_usd=.12, ebs_s3_allowance_usd=.05, qualification=proof, **source)),
                ('aws-launch.json', dict(instance_id='i-canary', prefix=CANARY_PREFIX+'a0001', nodes=nodes, **source)),
                ('aws-closeout.json', dict(nodes=nodes, state='terminated')), ('aws-terminal.json', terminal)):
            local.write_json(out/name, value)
        require(replay(out, canary=True)['executed'], 'closed actual canary replay')
        for name, value in (('native_processes', 1), ('dataset_payload_gets', 1), ('truth_or_panel_body_reads', 1)):
            rejects(lambda n=name, v=value: validate_canary(dict(result, **{n: v}), config, authorities))
        close = out/'aws-closeout.json'; original_close = close.read_bytes()
        close.write_bytes(local.canonical(dict(nodes=nodes, state='running')))
        sdk = Mock(); rejects(lambda: collect(sdk, CANARY_PREFIX+'a0001', out, 'i-canary', source['source_commit'], source['source_archive_sha256'], canary=True))
        sdk.get_object.assert_not_called(); close.write_bytes(original_close)
        pointer = dict(schema='borsuk-hierarchical-100k-canary-admission-v2', attempt='a0001',
            **{n: proof[n] for n in ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256',
                                   'source_archive_paths_sha256')},
            terminal_sha256=local.identity(out/'aws-terminal.json')['sha256'])
        selected = root/ROOT/'canary/a0001'; selected.parent.mkdir(parents=True)
        shutil.copytree(out, selected); local.write_json(root/ROOT/'canary-admission.json', pointer)
        require_canary(root, proof)
        rejects(lambda: require_canary(root, dict(proof, config_sha256='0'*64)))
        rejects(lambda: require_canary(root, dict(proof, source_archive_paths_sha256='0'*64)))
        (selected/'aws-terminal.json').write_bytes(b'{}\n'); rejects(lambda: require_canary(root, proof))
        shell = user_data('a'*40, 'b'*64, 'sources/mock', CANARY_PREFIX+'a0001', proof, canary=True)
        require(all(s in shell for s in ('MemoryMax=256M', 'CPUQuota=100%', '--stage-canary',
            '--on-active=480s', '+480))', str(CANARY_SCRATCH), 'sync -f terminal.json',
            'rm -f -- "$root/source.tar.gz"', 'rm -rf -- "$root/aws" "$root/awscliv2.zip"',
            'rm -rf -- "$root/pip-cache"')), 'canary bootstrap envelope/disposable transport cleanup')
        require('MemoryMax=2G' not in shell and '--stage ' not in shell, 'distinct canary mode')
        for argv in (['--canary', 'a0001'], ['--stage-canary', str(repo), str(root/'cli-canary'), str(root)]):
            cli = subprocess.run([sys.executable, '-m', MODULE, *argv], capture_output=True, text=True, cwd=here)
            require(cli.returncode == 2 and 'INVALID:' in cli.stderr, 'actual canary CLI fail closed')
        # Exercise unchanged shared launch ownership with the canary campaign.
        shared, _ = ids.lifecycle(); ec2, s3, session = Mock(), Mock(), Mock()
        session.client.side_effect = [ec2, s3]
        ec2.describe_instances.return_value = {'Reservations': []}
        ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
        ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [dict(SpotPrice='0.1', Timestamp=datetime.now(timezone.utc))]}
        ec2.run_instances.return_value = {'Instances': [dict(InstanceId='i-canary')]}
        events = []
        ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
        ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
        campaign = canary_campaign(); campaign.ROOT = root/'launch'; campaign.preflight = lambda: proof
        campaign.poll = Mock()
        def collected(*args):
            exact(events, ['terminate', 'wait'], 'canary termination before collection')
            events.append('collect'); return terminal
        campaign.collect = collected
        with patch.object(shared, 'dict', canary_reservation, create=True), \
             patch.object(shared.boto3, 'Session', return_value=session), \
             patch.object(subprocess, 'check_output', side_effect=['', 'a'*40]), \
             patch.object(shared, 'source_archive', return_value=b'synthetic source archive') as archive_mock, \
             patch.object(shared.peer, 'missing', return_value=True), patch.object(shared.peer, 'put_if_absent'):
            shared.main('a0001', campaign=campaign)
        archive_mock.assert_called_once_with('a'*40, proof['source_archive_paths'])
        reserved = local.decode((campaign.ROOT/'canary/a0001/aws-reservation.json').read_bytes())
        exact(reserved['compute_cap_usd'], .12, 'actual canary reservation compute')
        exact(reserved['ebs_s3_allowance_usd'], .05, 'actual canary reservation ancillary')
        exact(events, ['terminate', 'wait', 'collect'], 'actual canary shared lifecycle')
        ec2.run_instances.assert_called_once()
        ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-canary'])
        ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-canary'])
    print('PASS actual draft metadata-only preflight, real imports/SDK model/CLI, mocked HEAD18+logGET1, no-native/no-GT spies, negatives, replay/admission/shared canary lifecycle')


def archive_roster_self_check():
    """Roundtrip the whole file closure with tiny synthetic committed bodies."""
    import gzip
    import tarfile
    shared, _ = ids.lifecycle()
    native_paths = sorted({p for p in local.SOURCE_FILES.values() if p.endswith('.rs')} |
                          {f'crates/fixture/src/f{i:03}.rs' for i in range(398)})
    require(len(native_paths) == 401, 'synthetic native401 fixture')
    config = dict(refs={n: dict(path=p) for n, p in REF_PATHS.items()}, native=dict(
        sources={n: dict(path=p) for n, p in local.SOURCE_FILES.items()},
        gate_log=dict(path='gate.log'), source_archive=dict(path='cold-source.tar.gz'),
        binaries={n: dict(path='cold-'+n) for n in ('writer', 'cells')}))
    expected = {str(CONFIG), *CODE, *native_paths, *REF_PATHS.values(), 'gate.log'}
    bodies = {p: ('synthetic '+p+'\n').encode() for p in expected}
    native_hashes = {p: local.sha(bodies[p]) for p in native_paths}
    authorities = dict(manifest=dict(source_sha256=native_hashes))
    roster = source_archive_roster(config, authorities, str(CONFIG))
    exact(roster, sorted(expected), 'required file closure')
    with tempfile.TemporaryDirectory(prefix='hierarchical-roster-check-') as tmp:
        repo, deployed = Path(tmp)/'repo', Path(tmp)/'deployed'
        repo.mkdir(); deployed.mkdir()
        for p, body in dict(bodies, **{'unselected-corpus': b'not deployed\n'}).items():
            target = repo/p; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(body)
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=repo)
        git('init', '-q'); git('add', '.')
        git('-c', 'user.name=Roman Bartusiak', '-c', 'user.email=riomus@gmail.com', 'commit', '-qm', 'Synthetic hierarchical closure')
        commit = git('rev-parse', 'HEAD').decode().strip()
        before = Path.cwd()
        try:
            os.chdir(repo)
            compressed = shared.source_archive(commit, roster)
        finally:
            os.chdir(before)
        with tarfile.open(fileobj=io.BytesIO(gzip.decompress(compressed))) as tar:
            files = {m.name: tar.extractfile(m).read() for m in tar if m.isfile()}
        exact(files, bodies, 'authenticated minimal deployed members')
        for p, body in files.items():
            target = deployed/p; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(body)
        exact(source_hashes(deployed), native_hashes, 'all401 deployed native hashes')
        exact(source_identity(source_hashes(deployed)), source_identity(native_hashes), 'deployed native identity')
        for mode in ('missing-native', 'tampered-native', 'extra-native'):
            target = deployed/(native_paths[0] if mode != 'extra-native' else 'crates/fixture/extra.rs')
            if mode == 'missing-native': target.unlink()
            else: target.write_bytes(b'changed\n')
            require(source_hashes(deployed) != native_hashes, 'native omission/tamper/extra accepted')
            if mode == 'extra-native': target.unlink()
            else: target.write_bytes(bodies[native_paths[0]])
    print('PASS tiny required-member closure/source401 roundtrip/omission/tamper/extra native checks')


def self_check():
    """Actual Arrow/helper/CLI/lifecycle, tiny fake proofs only, no real science."""
    from unittest.mock import Mock
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    module = sys.modules[__name__]
    here = Path(__file__).resolve().parents[1]
    local.resource_snapshot(dict(memory_max_bytes=256 << 20, cpu_affinity=[0]))
    exact(sorted(os.sched_getaffinity(0)), [0], 'self-check CPU1')
    archive_roster_self_check()
    def rejects(action, match=None):
        try:
            action()
        except (ValueError, OSError, AssertionError, TimeoutError) as error:
            require(match is None or match in str(error), 'unexpected refusal: '+str(error))
        else:
            raise AssertionError('accepted invalid fixture')
    with tempfile.TemporaryDirectory(prefix='hierarchical-adapter-check-') as tmp:
        base = Path(tmp)
        # Real, small committed metadata is checked separately from fake proofs.
        # No qualified binary/archive or corpus body is opened here.
        authority = local.decode((here/REF_PATHS['staging_authority']).read_bytes())
        native_metadata = {n: copy.deepcopy(authority[n]) for n in
                           ('source_commit', 'source_archive', 'sources', 'binaries', 'gate_log')}
        native_metadata['schema'] = local.PROOF_SCHEMA
        native_metadata['source_archive']['path'] = str(base/'UNSUPPLIED-real-archive')
        for name, pin in native_metadata['binaries'].items():
            pin['path'] = str(base/('UNSUPPLIED-real-'+name))
        controller = dict(FIXED, authority_pending=False,
            admission=dict(kind='one_query_per_dataset', timeout_seconds=120, negative_truth_hash=True),
            native=native_metadata, cpu_affinity=[0, 1], scratch_reserve_bytes=4 << 30,
            scratch_admission_bytes=12 << 30,
            refs={n: dict(path=p, **body_pin(local.identity(here/p))) for n, p in REF_PATHS.items()},
            code_sha256={p: local.identity(here/p)['sha256'] for p in CODE})
        metadata_repo = base/'metadata-repo'
        controller_path = metadata_fixture(metadata_repo, controller)
        for mode in ('valid-static', 'pending', 'code-tamper', 'ref-tamper', 'archive-size'):
            changed = copy.deepcopy(controller)
            if mode == 'pending': changed['authority_pending'] = True
            if mode == 'code-tamper': changed['code_sha256'][CODE[0]] = '0'*64
            if mode == 'ref-tamper': changed['refs']['receipt']['sha256'] = '0'*64
            if mode == 'archive-size': changed['native']['source_archive']['bytes'] = 1
            controller_path.write_bytes(local.canonical(changed))
            if mode == 'valid-static':
                _, authenticated, _ = qualify(metadata_repo, native_files=False)
                require(len(authenticated['source_archive_paths']) >= 401 and authenticated['source_file_count'] == 401,
                        'qualified native401 file roster')
            else:
                rejects(lambda: qualify(metadata_repo, native_files=False))
        controller_path.write_bytes(local.canonical(controller))
        qualify(metadata_repo)
        rejects(lambda: qualify(metadata_repo, native_files=True), 'UNSUPPLIED-real-archive')
        native_hashes = source_hashes(metadata_repo)
        omitted = sorted(native_hashes)[0]
        for changed in ({p: h for p, h in native_hashes.items() if p != omitted},
                        dict(native_hashes, **{omitted: '0'*64}), dict(native_hashes, **{'extra.rs': '0'*64})):
            with patch.object(module, 'source_hashes', return_value=changed):
                rejects(lambda: qualify(metadata_repo), 'exact full qualified native source inventory')
        # Exercise the real helper with its own explicitly synthetic gate proof
        # and mock CLI; assert hook timing and INVALID cleanup before measurement.
        original_prepare = local.prepare
        seen = []
        def helper_run(config, pin, output, geometry=local.GEOMETRY, before_diagnostics=None):
            def observe(plans, proof, caps, deadline, receipt):
                require(len(plans) == 2 and len(receipt['stages']) == 4, 'hook after both builds')
                require(all(Path(p['path']).is_file() and not t.exists() for _, _, p, t in plans), 'hook after sealing/before queries')
                seen.append(str(output))
            result = original_prepare(config, pin, output, geometry, observe)
            if Path(output).name == 'success':
                def fail(*args):
                    raise ValueError('synthetic admission failure')
                failed = Path(output).parent/'hook-failure'
                rejects(lambda: original_prepare(config, pin, failed, geometry, fail), 'synthetic admission failure')
                closed = local.decode((failed/'receipt.json').read_bytes())
                require(closed['status'] == 'INVALID' and len(closed['stages']) == 4 and closed['cleanup_complete'], 'hook failure closure')
                require(not any(p.is_dir() for p in failed.iterdir()), 'hook failure directories removed')
                def tamper(plans, *_):
                    Path(plans[0][2]['path']).write_bytes(b'{}')
                rejects(lambda: original_prepare(config, pin, Path(output).parent/'hook-tamper', geometry, tamper), 'authentication')
            return result
        with patch.object(local, 'prepare', side_effect=helper_run):
            local.self_check()
        require(seen, 'real helper hook invoked')

        def pin(path, body):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            return dict(path=str(path), bytes=len(body), sha256=local.sha(body))
        stats = dict(submitted_gets=1, requested_bytes=100, verified_bytes=100, failed_gets=0)
        zero = dict.fromkeys(stats, 0)
        source_pins = {n: dict(body_pin(local.identity(here/p)), path=p) for n, p in local.SOURCE_FILES.items()}
        # Synthetic executable is Python, never a copied/real native binary.
        mock = '''#!/usr/bin/env python3
import hashlib,json,os,struct,sys
from pathlib import Path
def h(b): return hashlib.sha256(b).hexdigest()
def enc(v): return (json.dumps(v,sort_keys=True,separators=(',',':'))+'\\n').encode()
def body(p): return Path(p).read_bytes()
if os.environ.get('HIERARCHICAL_MOCK_NONZERO')=='1': sys.exit(7)
if sys.argv[1] not in ('build','diagnose'):
 c=json.loads(body(sys.argv[1]));out=Path(sys.argv[4]);out.mkdir();(out/'plane').mkdir()
 ids=struct.unpack('<4Q',body(c['order']['path']));mean=struct.pack('<3f',0,0,0);records=b'R'*36
 can=b''.join(struct.pack('<q3f',i,1,0,0) for i in ids)
 for n,b in [('plane/mean.bin',mean),('plane/records.bin',records),('canonical.bin',can)]: (out/n).write_bytes(b)
 plane=dict(schema='borsuk-two-bit-plane-v3',rows=4,dimensions=3,seed=20260923,record_bytes=9,
 source_sha256=c['raw_sha256'],sq8_sha256=c['sq8_sha256'],source_order_sha256=c['order']['sha256'],
 mean_sha256=h(mean),records_sha256=h(records),query_or_truth_used=False)
 (out/'plane/manifest.json').write_bytes(enc(plane))
 root=dict(schema='borsuk-two-bit-generation-v8',generation=1,base_epoch=0,low=c['low'],step=c['step'],
 plane_manifest_sha256=h(enc(plane)),sq8_object_sha256=c['sq8_sha256'],sq8_object_key=c['sq8_object_key'],sq8_etag=c['sq8_etag'],
 discovery=dict(mode='semantic',profile='native100k'),canonical=dict(rows=4,dimensions=3,bytes=len(can),sha256=h(can)))
 (out/'manifest.json').write_bytes(enc(root));print(h(enc(root)))
else:
 mode,p,digest,output=sys.argv[1:];c=json.loads(body(p));out=Path(output)
 if mode=='build':
  out.mkdir();(out/'manifest.json').write_bytes(enc(dict(schema='borsuk-hierarchical-cells-resident-v3')))
  print(json.dumps(dict(status='BUILT_UNQUALIFIED',config_sha256=digest,module_source_sha256=MODULE_SHA)))
 else:
  identity=dict(phase='identity',schema=c['schema'],config_sha256=digest,candidate_root_sha256=c['candidate_root']['sha256'],
  requests_sha256=c['requests']['sha256'],module_source_sha256=MODULE_SHA,binary_source_sha256=BINARY_SHA,
  first=0,count=c['count'],top_k=c['top_k'],options=c['options'],startup=STATS,startup_directory=STATS,directory_admission={})
  a=dict(directory=ZERO,source=ZERO,refinement=ZERO,whole_cell=STATS,waves=[dict(stage='whole_cell',dependency=1,max_parallel_gets=1,**STATS)],modeled_query_payload_bytes=1024)
  events=[identity]+[dict(phase='query_frozen',ordinal=i,truth_opened=False,trace=dict(accounting=a)) for i in range(c['count'])]
  prefix=b''.join(enc(e) for e in events)
  events.append(dict(phase='all_queries_frozen',count=c['count'],first=0,trace_prefix_bytes=len(prefix),trace_prefix_sha256=h(prefix),truth_opened=False))
  with out.open('xb') as f:
   f.write(b''.join(enc(e) for e in events));f.flush();os.fsync(f.fileno())
   truth=body(c['truth']['path'])
   if h(truth)!=c['truth']['sha256']:
    f.write(enc(dict(phase='terminal',status='INVALID',complete=False)));f.flush();os.fsync(f.fileno());sys.exit(2)
   for i in range(c['count']):
    loss=dict(truth_count=c['top_k'],primary_hits=c['top_k'],boundary_hits=c['top_k'],nomination_hits=c['top_k'],returned_hits=c['top_k'],
    router_misses=0,boundary_recovered=0,boundary_misses=0,local_nomination_misses=0,final_ranking_misses=0)
    f.write(enc(dict(phase='loss_attribution',ordinal=i,truth_sha256=c['truth']['sha256'],loss=loss)))
   f.write(enc(dict(phase='terminal',status='DIAGNOSTIC',complete=True,queries=c['count'],truth_opened=True,scientific_qualification=False,quality_or_performance_claim=False)))
   f.flush();os.fsync(f.fileno())
  print(json.dumps(dict(status='DIAGNOSTIC')))
'''
        mock = ('#!/usr/bin/env python3\nMODULE_SHA='+repr(source_pins['module']['sha256'])+'\nBINARY_SHA='+repr(source_pins['binary']['sha256'])+
                '\nSTATS='+repr(stats)+'\nZERO='+repr(zero)+'\n'+mock.split('\n', 1)[1]).encode()
        # Build a synthetic completed gate transcript; it is never real authority.
        log = bytearray()
        for name, command in local.GATES.items():
            event = dict(schema='borsuk-hierarchical-cells-implementation-stage-v1', stage=name,
                started_at='synthetic', finished_at=None, exit_status=None, gate_status=None,
                tests_run=None, required_test_passes=None, command=command)
            log.extend(local.canonical(event))
            tests = local.TESTS.get(name, ())
            for test in tests:
                log.extend(('test '+test+' ... ok\n').encode())
            n = len(tests) or 1
            count = n if name not in ('release', 'clippy', 'test-build') else None
            if count:
                log.extend(f'test result: ok. {n} passed; 0 failed; synthetic\n'.encode())
            event.update(finished_at='synthetic', exit_status=0, gate_status=0, tests_run=count, required_test_passes=dict.fromkeys(tests, 1))
            log.extend(local.canonical(event))
        bodies = {'native-archive': b'fake archive', 'native-log': bytes(log), 'native-writer': mock, 'native-cells': mock}
        native = dict(schema=local.PROOF_SCHEMA, source_commit='a'*40, sources=source_pins,
            source_archive=dict(path='fake-archive', key='native-archive', bytes=len(bodies['native-archive']), sha256=local.sha(bodies['native-archive'])),
            gate_log=dict(path='fake-log', key='native-log', bytes=len(log), sha256=local.sha(bytes(log))),
            binaries={n: dict(path='fake-'+n, key='native-'+n, bytes=len(mock), sha256=local.sha(mock)) for n in ('writer', 'cells')})
        authorities = dict(sources={'items': []}, preparation={'items': []})
        geometry = (4, 3, 2, 2)
        raw = np.array([[1, 0, 0]]*4, dtype='<f4').tobytes()
        order = (2, 0, 3, 1)
        import struct
        for dataset in ('relaion', 'cohere'):
            source = raw
            if dataset == 'relaion':
                sink = io.BytesIO()
                table = pa.table({'embedding': pa.array([[1., 0., 0.]]*4, type=pa.list_(pa.float32(), 3))})
                pq.write_table(table, sink); source = sink.getvalue()
            corpus = dict(source=source, order=struct.pack('<4Q', *order),
                sq8=b''.join(struct.pack('<qf3B', i, 1, 7, 8, 9) for i in order),
                mean=struct.pack('<3f', 0, 0, 0), records=b'R'*36,
                requests=b''.join(local.canonical(dict(query_ordinal=i, query=[1, -0., .125], historical='kept only in full source')) for i in range(3)),
                truth=b'opaque truth bytes!!'*4)
            artifacts = {}
            for role, name in STAGED_ROLES.items():
                key = dataset+'/'+role
                bodies[key] = corpus[role]
                artifacts[name] = dict(key=key, bytes=len(corpus[role]), sha256=local.sha(corpus[role]))
            can = b''.join(struct.pack('<q3f', i, 1, 0, 0) for i in order)
            artifacts['control/canonical.bin'] = dict(bytes=len(can), sha256=local.sha(can))
            artifacts['reference-paired.jsonl'] = dict(key='synthetic V282 reference')
            authorities['sources']['items'].append(dict(name=dataset, artifacts=artifacts))
            authorities['preparation']['items'].append(dict(decoded_raw_sha256=local.sha(raw), current_writer_config_draft=dict(
                generation=1, base_epoch=0, low=[0]*3, step=[1]*3, sq8_object_key='objects/'+artifacts['sq8.bin']['sha256'], sq8_etag='synthetic')))
        config = dict(FIXED, native=native, cpu_affinity=[0], cpu_quota_percent=100, tasks_max=512,
                      admission=dict(kind='one_query_per_dataset', timeout_seconds=10, negative_truth_hash=True))
        config_file = pin(base/'fixture-config.json', local.canonical(config))
        proof = {n: 'b'*64 for n in TERMINAL_IDENTITIES}
        proof.update(config_path=str(CONFIG), config_sha256=config_file['sha256'], campaign_schema=SCHEMA,
                     source_file_count=401, source_archive_paths=[str(CONFIG)],
                     source_archive_paths_sha256=ids.sha(ids.encoded([str(CONFIG)])),
                     awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
        dataset_bytes = sum(len(v) for k, v in bodies.items() if not k.startswith('native-'))
        original_panel = panel_inputs
        def reduced(*args):
            return original_panel(*args, geometry=geometry)
        def get(**kw):
            body = bodies[kw['Key']]
            return dict(ContentLength=len(body), Body=io.BytesIO(body))
        fixture_limits = dict(LIMITS, memory_max_bytes=256 << 20, scratch_max_bytes=256 << 20,
                              max_result_bytes=1 << 20, max_log_bytes=1 << 20)
        env = dict(BORSUK_HIERARCHICAL_CONFIG_SHA256=proof['config_sha256'],
            BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256=proof['source_archive_paths_sha256'],
            BORSUK_HIERARCHICAL_DEADLINE_EPOCH=str(int(time.time())+100),
            BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=str(shutil.disk_usage(base).used))
        # Keep the original helper and adapter path; patch only authorities,
        # tiny geometry and the external SDK. Fake native proof never escapes.
        with patch.object(module, 'CONFIG', config_file['path']), patch.object(module, 'qualify', return_value=(config, proof, authorities)), \
             patch.object(module, 'FIXED', dict(FIXED, source_encoded_bytes=dataset_bytes)), \
             patch.object(module, 'LIMITS', fixture_limits), patch.object(module, 'panel_inputs', side_effect=reduced), \
             patch.object(local, 'prepare', side_effect=lambda c, p, o, **kw: original_prepare(c, p, o, geometry, **kw)), \
             patch.dict(os.environ, env), patch.object(publication, 'sdk_client', return_value=Mock(get_object=get)):
            out = base/'success'
            # repo is source-only outside the owned worker fixture. Copy only
            # the four small source pins, never corpus/native authority bodies.
            repo = base/'repo'; repo.mkdir()
            for p in source_pins.values():
                target = repo/p['path']; target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((here/p['path']).read_bytes())
            result = stage(repo, out, base)
            require(result['complete'] and result['status'] == 'FAIL', 'quality FAIL remains completed')
            require(not (out/'scratch').exists() and not (out/'admission-scratch').exists(), 'staging/canary removed')
            receipt = local.decode((out/'measurement/receipt.json').read_bytes())
            require(len(receipt['stages']) == 6 and len(receipt['admission']['stages']) == 3, 'separate admission/measurement invocations')
            require(all(local.decode((out/'measurement'/(d+'-diagnose.json')).read_bytes())['count'] == 2 for d in ('relaion', 'cohere')), 'canary did not rewrite measured configs')
            staging = local.decode((out/'staging.json').read_bytes())
            require(len(staging['sdk_calls']) == 18 and staging['truth_decoded'] is False, 'authenticated source roster')
            query_bits = local.sha(positive.f32([1, -0., .125], 3).tobytes()*2)
            require(all(v['query_f32_sha256'] == query_bits for v in staging['reductions'].values()), 'original nonunit/signed-zero query bits')
            # Exercise the actual streaming collector and replay after durable
            # same-ID termination. These artifacts are exclusively synthetic.
            collected = base/'collected'; collected.mkdir()
            uploaded = {n: (out/n.removeprefix('screen/')).read_bytes() for n in ARTIFACTS if n.startswith('screen/')}
            uploaded.update({'test-resources.txt': b'synthetic resources\n', 'run-closed.log': b'synthetic bootstrap\n'})
            host = dict(instance_id='i-synthetic', nodes={'0': dict(instance_id='i-synthetic')},
                        prefix=PREFIX+'a0001', source_commit='a'*40, source_archive_sha256='b'*64)
            terminal = dict(schema=SCHEMA, status='complete', phase='complete', exit_code=0, original_exit_code=0,
                **{n: host[n] for n in ('instance_id', 'source_commit', 'source_archive_sha256')},
                **{n: proof[n] for n in TERMINAL_IDENTITIES},
                artifacts={n: dict(bytes=len(b), sha256=local.sha(b)) for n, b in uploaded.items()})
            for n, v in [('aws-launch.json', host), ('aws-closeout.json', dict(state='terminated', nodes=host['nodes'])),
                         ('aws-reservation.json', dict(schema=SCHEMA, source_commit=host['source_commit'], source_archive_sha256=host['source_archive_sha256'], qualification=proof))]:
                ids.write(collected/n, v)
            def fetch(**kw):
                b = local.canonical(terminal) if kw['Key'].endswith('/terminal.json') else uploaded[kw['Key'].split('/artifacts/', 1)[1]]
                return dict(Body=io.BytesIO(b))
            collect(Mock(get_object=fetch), host['prefix'], collected, host['instance_id'], host['source_commit'], host['source_archive_sha256'])
            require(replay(collected)['executed'], 'actual completed collection/replay')
            reserved_path = collected/'aws-reservation.json'
            saved_reservation = reserved_path.read_bytes()
            damaged = local.decode(saved_reservation)
            damaged['qualification']['source_archive_paths'].append('omitted-native.rs')
            reserved_path.write_bytes(local.canonical(damaged))
            rejects(lambda: replay(collected), 'reserved source file roster SHA')
            reserved_path.write_bytes(saved_reservation)
            qualification_path = collected/'screen/source-qualification.json'
            saved_qualification = qualification_path.read_bytes()
            damaged = local.decode(saved_qualification); damaged['source_archive_paths'] = ['omitted-native.rs']
            qualification_path.write_bytes(local.canonical(damaged))
            changed_terminal = copy.deepcopy(terminal)
            changed_terminal['artifacts']['screen/source-qualification.json'] = body_pin(local.identity(qualification_path))
            terminal_path = collected/'aws-terminal.json'; saved_terminal = terminal_path.read_bytes()
            terminal_path.write_bytes(local.canonical(changed_terminal))
            rejects(lambda: replay(collected), 'deployed source qualification ancestry')
            qualification_path.write_bytes(saved_qualification); terminal_path.write_bytes(saved_terminal)
            diagnostic = collected/'screen/measurement/relaion-diagnostic.jsonl'
            original_body = diagnostic.read_bytes()
            diagnostic.write_bytes(original_body[:-1])
            rejects(lambda: replay(collected), 'collected body identity')
            diagnostic.write_bytes(original_body)
            (collected/'aws-closeout.json').write_bytes(local.canonical(dict(state='running', nodes=host['nodes'])))
            sdk = Mock()
            rejects(lambda: collect(sdk, host['prefix'], collected, host['instance_id'], host['source_commit'], host['source_archive_sha256']))
            sdk.get_object.assert_not_called()
            (collected/'aws-closeout.json').write_bytes(local.canonical(dict(state='terminated', nodes={'0': dict(instance_id='i-other')})))
            rejects(lambda: collect(sdk, host['prefix'], collected, host['instance_id'], host['source_commit'], host['source_archive_sha256']), 'SAME IDs')
            sdk.get_object.assert_not_called()
            for mode in ('short', 'wrong-hash', 'scratch', 'nonzero'):
                failed = base/mode
                def damaged(**kw):
                    response = get(**kw)
                    if kw['Key'] == 'native-writer':
                        body = bodies[kw['Key']]
                        response['Body'] = io.BytesIO(body[:-1] if mode == 'short' else b'X'+body[1:])
                    return response
                with patch.object(publication, 'sdk_client', return_value=Mock(get_object=damaged if mode in ('short', 'wrong-hash') else get)), \
                     patch.object(module, 'scratch_snapshot', return_value=SCRATCH+1) if mode == 'scratch' else patch.dict(os.environ, {}), \
                     patch.dict(os.environ, {'HIERARCHICAL_MOCK_NONZERO': '1'} if mode == 'nonzero' else {}):
                    rejects(lambda: stage(repo, failed, base))
                require(not (failed/'scratch').exists(), 'failed scratch removed')
                require(local.decode((failed/'summary.json').read_bytes())['status'] == 'INVALID', 'failure INVALID distinct from quality FAIL')
        # Fail closed through the actual command line before any SDK/lifecycle.
        process = subprocess.run([sys.executable, '-m', MODULE, 'a0001'], cwd=here, capture_output=True, text=True)
        require(process.returncode == 2 and 'INVALID' in process.stderr, 'missing root config CLI refusal')
        shell = user_data('a'*40, 'b'*64, 'sources/fake', PREFIX+'a0001', proof)
        require(all(s in shell for s in ('MemoryMax=2G', 'CPUQuota=200%', 'MemorySwapMax=0', 'TasksMax=512',
            'BORSUK_HIERARCHICAL_DEADLINE_EPOCH', 'sync -f terminal.json')), 'bootstrap resources/durability')
        shared, _ = ids.lifecycle()
        shared.self_check(lifecycle_only=True)
    print('PASS synthetic Arrow/authenticated staging/helper hooks/mock CLI/short-body/wrong-hash/scratch/nonzero cleanup/shared owned lifecycle; no real science')
    canary_self_check()


# This branch has its own frozen config, archives, prefixes and receipts.
# The original paired diagnostic entry points remain unchanged.
from scripts import run_hierarchical_global_leaf_probe as probe
PROBE_ROOT = ROOT/'global-leaf-probe'
PROBE_CONFIG = PROBE_ROOT/'config.json'
PROBE_SCHEMA = 'borsuk-global-leaf-probe-spot-v1'
PROBE_CANARY_SCHEMA = 'borsuk-global-leaf-probe-infrastructure-canary-v1'
PROBE_PREFIX = 'research/hierarchical-cells/20261003/global-leaf-probe-'
PROBE_CANARY_PREFIX = 'research/hierarchical-cells/20261003/global-leaf-probe-canary-'
PROBE_CODE = tuple(sorted(set((*CODE, 'scripts/run_hierarchical_global_leaf_probe.py'))))
PROBE_FIXED = {n: FIXED[n] for n in ('architecture', 'region', 'bucket', 'rows', 'dimensions', 'first', 'count',
    'k', 'metric', 'consumed', 'held_out', 'retune_allowed', 'instance_type', 'image_id', 'root_device_name',
    'subnet_id', 'volume_gib', 'volume_type', 'encrypted', 'delete_on_termination', 'spot_max_usd_per_hour',
    'compute_cap_usd', 'memory_bytes', 'swap_bytes', 'cpu_quota_percent', 'tasks_max', 'scratch_bytes',
    'wall_seconds', 'versions', 'physical_s3_measured', 'vendor_win', 'scientific_qualification')}
PROBE_FIXED.update(schema=probe.SCHEMA, source_object_count=8, native_object_count=7,
    original_root=str(probe.ORIGINAL_ROOT), no_truth_body_access=True, reduction_enabled=False)
PROBE_STAGE_NAMES = tuple(d+'-'+phase for d in probe.DATASETS for phase in ('writer', 'build', 'nominate'))
PROBE_OUTPUTS = ('config.json', 'source-qualification.json', 'tool-versions.json', 'staging.json',
    'paired-seal.json', 'native-execution-receipt.json', 'execution-receipt.json', 'summary.json', 'resources.json', 'worker-cgroup.json',
    'cleanup.json', *(f'measurement/{n}{s}' for n in PROBE_STAGE_NAMES for s in
        ('-stage.json', '-stage-receipt.json', '-closure.json', '.log', '-unit.log')),
    *(f'measurement/{d}-{n}' for d in probe.DATASETS for n in ('nominate.json', 'nominations.jsonl')),
    *(f'retained/{d}/{n}' for d in probe.DATASETS for n in
        ('manifest.json', 'directories.bin', 'cells.bin', 'requests64', 'original-build.json', 'original-writer.json')))
PROBE_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in PROBE_OUTPUTS))
PROBE_CANARY_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in
    ('config.json', 'source-qualification.json', 'tool-versions.json', 'canary.json', 'summary.json',
     'resources.json', 'worker-cgroup.json', 'cleanup.json')))


def probe_objects(config, evidence):
    objects = []
    for r in probe.ROLE_NAMES:
        native = config['roles'][r]['native']
        objects += [native['source_archive'], native['gate_log'], *native['binaries'].values()]
    objects += [i['artifacts'][n] for i in evidence['sources']['items']
                for n in ('source', 'order.u64', 'sq8.bin', 'requests')]
    exact(len(objects), 15, 'probe7 native+8 source bodies')
    pins = [{k: p[k] for k in ('key', 'bytes', 'sha256')} for p in objects]
    require(len({p['key'] for p in pins}) == len(pins), 'distinct probe objects')
    for pin in pins:
        publication.object_identity(pin)
    return pins


def probe_qualify(base=Path('.'), *, canary=False):
    repo = Path(base).resolve(); pin = local.identity(repo/PROBE_CONFIG)
    config = local.read_json(pin, 512 << 10)
    fields(config, ' '.join(PROBE_FIXED)+' authority_pending code_sha256 evidence roles resources phase_seconds '
           'scratch_admission_bytes scratch_reserve_bytes admission', 'probe config')
    exact(config['authority_pending'], False, 'probe root freeze pending')
    for n, value in PROBE_FIXED.items():
        exact(config[n], value, 'fixed probe protocol: '+n)
    fields(config['roles'], 'original nomination', 'separate binary authorities')
    fields(config['admission'], 'schema authority_pending config_sha256 code_identity_sha256 refs_identity_sha256 '
           'native_identity_sha256 receipt result_sha256 source_bound_real_no_gt', 'root real no-GT admission')
    exact(config['resources'], probe.CAPS, 'host and separate nominee caps')
    fields(config['phase_seconds'], 'writer build nominate', 'phase deadline roster')
    for n, seconds in config['phase_seconds'].items():
        local.integer(seconds, 10, 1669, 'phase deadline: '+n)
    exact(set(config['code_sha256']), set(PROBE_CODE), 'probe controller source roster')
    for name, digest in config['code_sha256'].items():
        exact(local.identity(repo_path(repo, name))['sha256'], digest, 'probe controller source drift')
    roles = {r: probe.qualify_role(r, config['roles'][r], repo) for r in probe.ROLE_NAMES}
    evidence = probe.original_evidence(repo, config)
    paths = {str(PROBE_CONFIG), *PROBE_CODE, *(p['path'] for p in config['evidence'].values())}
    for r in probe.ROLE_NAMES:
        paths.update(p['path'] for p in config['roles'][r]['refs'].values())
        qpath = roles[r]['source_qualification']['path']
        paths.add(qpath if (repo/qpath).exists() else qpath+'.gz')
        log = config['roles'][r]['native']['gate_log']; publication.relative(log['path']); paths.add(log['path'])
    original_terminal = evidence['values']['original_terminal']
    # Deploy the archived raw configurations and original trace bodies, never GT.
    paths.update(str(probe.OLD/n) for n in original_terminal['artifacts'] if n.startswith('screen/'))
    old_config = local.read_json(local.identity(repo/probe.OLD/'screen/config.json'))
    paths.update(p['path'] for p in old_config['refs'].values())
    if not canary or not config['admission']['authority_pending']:
        paths.add(config['admission']['receipt']['path'])
    paths = sorted(paths)
    for p in paths:
        publication.relative(p); repo_path(repo, p)
    objects = probe_objects(config, evidence)
    source_bytes = sum(p['bytes'] for p in objects[7:]); cold_bytes = sum(p['bytes'] for p in objects[:7])
    local.integer(config['scratch_reserve_bytes'], 1, SCRATCH, 'probe bootstrap reserve')
    local.integer(config['scratch_admission_bytes'], 1, SCRATCH, 'probe scratch admission')
    minimum = 2*(source_bytes+cold_bytes+(768 << 20)+sum(local.identity(repo/p)['bytes'] for p in paths))+config['scratch_reserve_bytes']
    require(config['scratch_admission_bytes'] >= minimum, 'probe full scratch/retained/archive reserve')
    proof = dict(config_path=str(PROBE_CONFIG), config_sha256=pin['sha256'],
        campaign_schema=PROBE_CANARY_SCHEMA if canary else PROBE_SCHEMA,
        code_identity_sha256=ids.sha(ids.encoded(config['code_sha256'])),
        refs_identity_sha256=ids.sha(ids.encoded(dict(evidence=config['evidence'],
            roles={r: config['roles'][r]['refs'] for r in probe.ROLE_NAMES}))),
        native_identity_sha256=ids.sha(ids.encoded(roles)), source_file_count=401,
        source_archive_paths=paths, source_archive_paths_sha256=ids.sha(ids.encoded(paths)),
        artifact_roster_sha256=ids.sha(ids.encoded(PROBE_CANARY_ARTIFACTS if canary else PROBE_ARTIFACTS)),
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
    if not canary:
        probe_require_admission(config, proof, repo)
    return config, proof, evidence


def probe_require_admission(config, proof, repo):
    """Cheap authenticated inputs precede canary and the one paired execution."""
    admission = config['admission']
    exact(admission['schema'], 'borsuk-global-leaf-real-no-gt-admission-v2', 'probe admission schema')
    exact(admission['authority_pending'], False, 'real fixture admission pending')
    exact(admission['source_bound_real_no_gt'], True, 'cheap real-input no-GT admission')
    # Config identity omits the admission object itself to avoid a recursive SHA.
    content = {k: v for k, v in config.items() if k != 'admission'}
    exact(admission['config_sha256'], local.sha(local.canonical(content)), 'admission final configuration')
    for n in ('code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256'):
        exact(admission[n], proof[n], 'admission unchanged final authority')
    receipt = read_ref(repo, admission['receipt'], 8 << 20)
    for n, expected in dict(schema='borsuk-global-leaf-real-no-gt-admission-receipt-v2', status='ADMITTED',
            complete=True, truth_opened=False, original_root_authority_pins_authenticated=True,
            historical_control_schema_admitted=True, exact_request_bodies_authenticated=True,
            query_f32_hashes_authenticated=True, resource_gate_passed=True, cleanup_complete=True,
            dataset_count=2, queries=128, ann_queries=0, native_processes=0, selection_receipts=0).items():
        exact(receipt[n], expected, 'real noGT receipt')
    for n in ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256'):
        exact(receipt[n], admission[n], 'real admission source/config receipt')
    exact(receipt['result_sha256'], admission['result_sha256'], 'real admission result binding')


def probe_preflight(base=Path('.'), *, canary=False):
    _, proof, _ = probe_qualify(base, canary=canary)
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty probe source')
    if not canary:
        probe_require_canary(base, proof)
    return proof


def probe_canary(config, evidence, client, calls, scratch, check, deadline):
    for n in ('numpy', 'pyarrow', 'boto3', 'botocore'):
        importlib.import_module(n)
    versions = {n: importlib.metadata.version(n) for n in (*FIXED['versions'], *SDK_VERSIONS)}
    exact(versions, dict(FIXED['versions'], **SDK_VERSIONS), 'probe real imports')
    model = client.meta.service_model
    require('IfNoneMatch' in model.operation_model('PutObject').input_shape.members, 'probe conditional SDK model')
    for operation in ('HeadObject', 'GetObject'):
        require({'Bucket', 'Key'} <= set(model.operation_model(operation).input_shape.members), 'probe SDK read model')
    selector = '--partitioner-pair' if config.get('schema') == PAIR_FIXED['schema'] else '--global-leaf-probe'
    cli = subprocess.run([sys.executable, '-m', MODULE, selector], capture_output=True, text=True,
        timeout=min(30, max(.001, deadline-time.monotonic())), env=dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1'))
    require(cli.returncode == 2 and 'INVALID:' in cli.stderr and 'CLI:' in cli.stderr and not cli.stdout, 'probe actual CLI exit2')
    for p in probe_objects(config, evidence):
        check(); response = publication.sdk_call(client, calls, 'head_object', p['key'], BUCKET)
        exact(response['ContentLength'], p['bytes'], 'probe HEAD presence/length only')
    logs = []
    for r in probe.ROLE_NAMES:
        p = config['roles'][r]['native']['gate_log']; path = scratch/(r+'-gate.log'); check()
        publication.download(client, calls, BUCKET, {k: p[k] for k in ('key', 'bytes', 'sha256')}, path)
        values = {n: read_ref(Path(__file__).resolve().parents[1], q, 8 << 20) for n, q in config['roles'][r]['refs'].items()}
        probe.gate_log(local.authenticate(dict(body_pin(p), path=str(path)), 1 << 20, read=True), values['receipt']['stages'], r)
        logs.append(body_pin(p))
    check()
    return dict(schema=PROBE_CANARY_SCHEMA, status='GO', complete=True, scientific_performance_evidence=False,
        versions=versions, sdk_conditional_put_model=True, cli_exit_status=cli.returncode, cli_stderr=cli.stderr,
        sdk_calls=calls, authenticated_logs=logs, transport='real SDK; HEAD presence/length only; two authenticated small gate-log GETs',
        ann_queries=0, native_processes=0, truth_or_panel_body_reads=0, dataset_payload_gets=0)


def probe_resource_check(root, baseline, scratch_cap, deadline, errors, peaks, *, scan=True):
    """Check deadlines immediately; inventory scratch at writes and in monitor."""
    def admitted(amount):
        require(amount <= scratch_cap and time.monotonic() < deadline and not errors,
                'probe whole-host scratch/deadline/monitor')
    admitted(peaks['scratch_bytes'])
    if scan:
        started = time.monotonic()
        worker, original = Path(root).resolve(), probe.ORIGINAL_ROOT.resolve()
        require(original == worker or not worker.is_relative_to(original), 'unexpected original root ancestor')
        components = {}
        amount = scratch_snapshot(worker, baseline, components=components)
        extra = local.directory_bytes(original) if not original.is_relative_to(worker) else 0
        amount += extra
        components.update(extra_root_bytes=extra, total_bytes=amount)
        if amount >= peaks['scratch_bytes']:
            peaks.update(scratch_bytes=amount, scratch_components=components)
        peaks['scratch_scan_calls'] = peaks.get('scratch_scan_calls', 0)+1
        peaks['scratch_scan_seconds'] = peaks.get('scratch_scan_seconds', 0)+time.monotonic()-started
        admitted(amount)


def probe_stage(repo, output, worker_root, *, canary=False):
    repo, out, root = map(lambda p: Path(p).resolve(), (repo, output, worker_root))
    require(out.is_relative_to(root) and repo == root/'probe-repo', 'probe orchestration root ownership')
    require(not out.exists() and not out.is_symlink(), 'fresh probe output')
    config, proof, evidence = probe_qualify(repo, canary=canary)
    exact(proof['config_sha256'], os.environ.get('BORSUK_HIERARCHICAL_CONFIG_SHA256'), 'probe bootstrap config')
    exact(proof['source_archive_paths_sha256'], os.environ.get('BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256'), 'probe bootstrap archive roster')
    remaining = int(os.environ['BORSUK_HIERARCHICAL_DEADLINE_EPOCH'])-time.time()
    wall, memory, scratch_cap = (CANARY_WALL, CANARY_MEMORY, CANARY_SCRATCH) if canary else (WALL, MEMORY, SCRATCH)
    require(0 < remaining <= wall, 'probe cumulative deadline')
    baseline = int(os.environ['BORSUK_HIERARCHICAL_SCRATCH_BASE_USED']); deadline = time.monotonic()+remaining
    limits = dict(LIMITS, memory_max_bytes=memory, scratch_max_bytes=scratch_cap, cpu_affinity=[0] if canary else [0, 1])
    main_before = local.resource_snapshot(limits); group = Path(main_before['path'])
    slice_name = os.environ.get('BORSUK_GLOBAL_LEAF_SLICE', '')
    require(re.fullmatch(r'borsuk-global-leaf-[a-z0-9-]+\.slice', slice_name) and group.parent.name == slice_name, 'owned aggregate host slice')
    host_before = probe.cgroup_snapshot(group.parent, memory, 100 if canary else 200)
    main_before.update(cpu_max=(group/'cpu.max').read_text().strip(), tasks_max=(group/'pids.max').read_text().strip())
    out.mkdir(); scratch = out/('infrastructure-scratch' if PROBE_SCHEMA == FINE_SCHEMA else 'scratch'); scratch.mkdir()
    client, calls, errors, peaks, stopped = None, [], [], {'scratch_bytes': 0}, threading.Event()
    result = dict(status='INVALID', complete=False, truth_opened=False, scientific_qualification=False, physical_s3_measured=False)
    old_alarm, old_term = signal.getsignal(signal.SIGALRM), signal.getsignal(signal.SIGTERM)
    old_timer = signal.setitimer(signal.ITIMER_REAL, remaining)
    def interrupted(*_):
        raise InterruptedError('probe cumulative resource/deadline interruption')
    signal.signal(signal.SIGALRM, interrupted); signal.signal(signal.SIGTERM, interrupted)
    def check(*, scan=True):
        probe_resource_check(root, baseline, scratch_cap, deadline, errors, peaks, scan=scan)
    def monitor():
        while not stopped.wait(1):
            try:
                check()
            except Exception as error:
                errors.append(str(error)); os.kill(os.getpid(), signal.SIGALRM); return
    thread = threading.Thread(target=monitor, daemon=True)
    failure = None
    try:
        check(); thread.start()
        ids.write(out/'config.json', (repo/PROBE_CONFIG).read_bytes())
        local.write_json(out/'source-qualification.json', proof)
        versions = {n: importlib.metadata.version(n) for n in (*FIXED['versions'], *SDK_VERSIONS)}
        exact(versions, dict(FIXED['versions'], **SDK_VERSIONS), 'probe installed dependencies')
        local.write_json(out/'tool-versions.json', dict(versions, python=sys.version, executable=sys.executable))
        client = publication.sdk_client(REGION)
        if canary:
            result = probe_canary(config, evidence, client, calls, scratch, check, deadline)
            result['config_sha256'] = proof['config_sha256']; local.write_json(out/'canary.json', result)
        else:
            def download(pin, path):
                check(); publication.download(client, calls, BUCKET, {k: pin[k] for k in ('key', 'bytes', 'sha256')}, path); check()
            result = probe.execute(config, local.identity(out/'config.json'), repo, out, download, check, deadline)
            expected = probe_objects(config, evidence)
            exact(len(calls), len(expected), 'closed delegated body GET roster')
            for call, pin in zip(calls, expected):
                exact(call['operation'], 'get_object', 'probe staged GET'); exact(call['key'], pin['key'], 'probe staged key')
                exact(call['verified_bytes'], pin['bytes'], 'probe staged bytes'); exact(call['verified_sha256'], pin['sha256'], 'probe staged SHA')
            paired = config.get('schema') == PAIR_FIXED['schema']
            local.write_json(out/'staging.json', dict(schema='borsuk-global-leaf-probe-staging-receipt-v1',
                sdk_calls=calls, truth_body_reads=2 if paired or config.get('schema') in (OVERLAP_SCHEMA, FINE_SCHEMA) else 0,
                old_panel_producer_called=False, diagnose_called=paired))
    except BaseException as error:
        failure = error; result.update(status='INVALID', complete=False, error=type(error).__name__+': '+str(error))
    finally:
        stopped.set()
        if thread.ident is not None:
            thread.join(timeout=2)
        signal.setitimer(signal.ITIMER_REAL, 0); signal.signal(signal.SIGALRM, old_alarm); signal.signal(signal.SIGTERM, old_term)
        if old_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, old_timer[0], old_timer[1])
        shutil.rmtree(scratch)
        if client is not None:
            client.close()
        try:
            main_after = local.resource_snapshot(limits)
            main_after.update(cpu_max=(group/'cpu.max').read_text().strip(), tasks_max=(group/'pids.max').read_text().strip())
            host_after = probe.cgroup_snapshot(group.parent, memory, 100 if canary else 200)
            probe.no_oom(host_before, host_after); probe.no_oom(main_before, main_after)
            require(not thread.is_alive(), 'probe monitor stopped')
            # Only the main owned controller may remain; every native unit was
            # stopped and waited before its resource/cleanup receipt was sealed.
            children = [p for p in group.parent.iterdir() if p.is_dir() and p != group]
            require(all(not (p/'cgroup.procs').read_text().strip() for p in children), 'aggregate native units drained')
            local.write_json(out/'worker-cgroup.json', dict(before=main_before, after=main_after,
                host_before=host_before, host_after=host_after, closed=True))
            check()
        except BaseException as error:
            failure = failure or error; result.update(status='INVALID', complete=False, error=str(error))
        local.write_json(out/'resources.json', dict(peaks, sdk_calls=calls, monitor_errors=errors,
            deadline_seconds=remaining, wall_seconds=remaining-(deadline-time.monotonic())))
        local.write_json(out/'cleanup.json', dict(scratch_removed=not scratch.exists(), monitor_stopped=not thread.is_alive(),
            sdk_client_closed=client is not None, original_root_removed=not probe.ORIGINAL_ROOT.exists(),
            native_processes_concurrent_max=0 if canary else 1, retained_layouts_preserved=(out/'retained').is_dir()))
        if not canary and (out/'native-execution-receipt.json').exists():
            result['native_execution'] = local.identity(out/'native-execution-receipt.json')
            result['worker_closure'] = {n: local.identity(out/p) for n, p in dict(
                resources='resources.json', cleanup='cleanup.json', cgroup='worker-cgroup.json').items() if (out/p).exists()}
            local.write_json(out/'execution-receipt.json', result)
        local.write_json(out/'summary.json', result); probe.fsync_dir(out)
    if failure:
        raise failure
    return result


def probe_user_data(commit, archive_sha, archive_key, prefix, proof, *, canary=False):
    """Use the shared ACK/fsync/terminal-last lifecycle without a query hook."""
    schema, artifacts = (PROBE_CANARY_SCHEMA, PROBE_CANARY_ARTIFACTS) if canary else (PROBE_SCHEMA, PROBE_ARTIFACTS)
    prefix_root = PROBE_CANARY_PREFIX if canary else PROBE_PREFIX
    module = sys.modules[__name__]
    with patch.multiple(module, CONFIG=PROBE_CONFIG, PREFIX=PROBE_PREFIX, CANARY_PREFIX=PROBE_CANARY_PREFIX,
                        SCHEMA=PROBE_SCHEMA, CANARY_SCHEMA=PROBE_CANARY_SCHEMA,
                        ARTIFACTS=PROBE_ARTIFACTS, CANARY_ARTIFACTS=PROBE_CANARY_ARTIFACTS):
        body = user_data(commit, archive_sha, archive_key, prefix, proof, canary=canary)
    require(prefix.startswith(prefix_root), 'probe bootstrap prefix')
    body = body.replace('/mnt/hierarchical-100k-canary', '/mnt/hierarchical-global-leaf-probe-canary')
    body = body.replace('/mnt/hierarchical-100k', '/mnt/hierarchical-global-leaf-probe')
    body = body.replace('/repo', '/probe-repo').replace('mkdir repo && tar -xzf source.tar.gz -C repo',
        'mkdir probe-repo && tar -xzf source.tar.gz -C probe-repo')
    flag = '--stage-canary' if canary else '--stage'
    body = body.replace('-m '+MODULE+' '+flag, '-m '+MODULE+' --global-leaf-probe '+flag)
    slice_name = 'borsuk-global-leaf-'+('canary-' if canary else '')+prefix[-5:]+'.slice'
    unit = 'hierarchical-100k-canary' if canary else 'hierarchical-100k'
    memory, cpu = ('256M', 100) if canary else ('2G', 200)
    body = body.replace('systemd-run --unit='+unit, f'''systemctl start {slice_name}
systemctl set-property --runtime {slice_name} MemoryMax={memory} MemorySwapMax=0 CPUQuota={cpu}% TasksMax=512
systemd-run --slice={slice_name} --unit=global-leaf-{'canary-' if canary else ''}{prefix[-5:]}''', 1)
    body = body.replace('--setenv=PYTHONPATH=', '--setenv=BORSUK_GLOBAL_LEAF_SLICE='+slice_name+' --setenv=PYTHONPATH=', 1)
    body = body.replace('phase=paired-diagnostic', 'phase=paired-nomination')
    # Main controller drains units normally. Bootstrap also stops the SAME slice
    # before collecting partial artifacts if the controller was killed.
    body = body.replace('  trap - EXIT TERM\n', f'  trap - EXIT TERM\n  systemctl stop {slice_name} 2>/dev/null || true\n', 1)
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<probe-terminal>', 'exec')
    require(len(body.encode()) < 16384 and schema in body and all(s in body for s in (
        'sync -f terminal.json', '--global-leaf-probe', 'MemorySwapMax=0', '--slice='+slice_name)), 'probe bootstrap contract')
    return body


def probe_replay(out, *, canary=False, repo=None):
    out = Path(out); repo = Path(repo or Path(__file__).resolve().parents[1])
    schema, artifacts = (PROBE_CANARY_SCHEMA, PROBE_CANARY_ARTIFACTS) if canary else (PROBE_SCHEMA, PROBE_ARTIFACTS)
    reservation, launch, close, terminal = (local.decode((out/n).read_bytes()) for n in (
        'aws-reservation.json', 'aws-launch.json', 'aws-closeout.json', 'aws-terminal.json'))
    exact(close['state'], 'terminated', 'probe closed instance'); exact(close['nodes'], launch['nodes'], 'probe SAME IDs')
    require(terminal['instance_id'] == launch['instance_id'] in {v['instance_id'] for v in close['nodes'].values()}, 'probe owned host')
    for n in ('source_commit', 'source_archive_sha256'):
        exact(terminal[n], launch[n], 'probe launch archive'); exact(terminal[n], reservation[n], 'probe reservation archive')
    exact(terminal['schema'], schema, 'probe terminal schema'); exact(reservation['schema'], schema, 'probe reservation schema')
    exact(reservation['wall_seconds'], CANARY_WALL if canary else WALL, 'probe reserved deadline')
    exact(reservation['compute_cap_usd'], .12 if canary else COMPUTE_CAP, 'probe reserved compute cap')
    exact(reservation['ebs_s3_allowance_usd'], .05 if canary else .15, 'probe reserved ancillary cap')
    proof = reservation['qualification']
    for n in TERMINAL_IDENTITIES:
        exact(terminal[n], proof[n], 'probe terminal authority')
    exact(proof['config_path'], str(PROBE_CONFIG), 'probe config path')
    paths = proof['source_archive_paths']; shared, _ = ids.lifecycle()
    shared.validate_source_archive_paths(paths); exact(paths, sorted(paths), 'probe archive roster order')
    exact(ids.sha(ids.encoded(paths)), proof['source_archive_paths_sha256'], 'probe archive roster SHA')
    require(set(terminal['artifacts']) <= set(artifacts), 'probe partial artifact roster')
    for n, p in terminal['artifacts'].items():
        exact(body_pin(local.identity(out/n)), p, 'probe collected body authentication')
    success = terminal['phase'] == terminal['status'] == 'complete' and terminal['exit_code'] == 0
    if not success:
        require(terminal['status'] == 'failed' and terminal['exit_code'] != 0, 'closed failed probe terminal')
        return dict(executed=False, truth_opened=False, scientific_qualification=False)
    exact(terminal['original_exit_code'], 0, 'probe unmodified worker exit')
    exact(set(terminal['artifacts']), set(artifacts), 'complete probe artifact roster')
    exact(local.decode((out/'screen/source-qualification.json').read_bytes()), proof, 'probe deployed qualification')
    config_pin = local.identity(out/'screen/config.json'); config = local.read_json(config_pin, 512 << 10)
    exact(config_pin['sha256'], proof['config_sha256'], 'probe collected frozen config')
    current, expected_proof, evidence = probe_qualify(repo, canary=canary)
    exact(current, config, 'probe replay unchanged final configuration')
    exact(expected_proof, proof, 'probe replay unchanged final authorities')
    wall, memory, scratch_cap = (CANARY_WALL, CANARY_MEMORY, CANARY_SCRATCH) if canary else (WALL, MEMORY, SCRATCH)
    counters = local.decode((out/'screen/worker-cgroup.json').read_bytes()); exact(counters['closed'], True, 'probe cgroup closed')
    for key in ('host_before', 'host_after'):
        probe.validate_cgroup(counters[key], memory, 100 if canary else 200)
    for key in ('before', 'after'):
        value = counters[key]
        require(0 < int(value['memory.max']) <= memory and int(value['memory.peak']) <= memory, 'probe worker memory')
        exact(value['memory.swap.max'], '0', 'probe worker noSwap'); exact(int(value['memory.swap.peak']), 0, 'probe worker swap peak')
        exact(value['cpu_affinity'], [0] if canary else [0, 1], 'probe worker CPUs')
        q, p = map(int, value['cpu_max'].split()); exact(q*100, (100 if canary else 200)*p, 'probe worker CPU quota')
        exact(value['tasks_max'], '512', 'probe worker tasks')
    probe.no_oom(counters['before'], counters['after']); probe.no_oom(counters['host_before'], counters['host_after'])
    resource = local.decode((out/'screen/resources.json').read_bytes())
    require(resource['scratch_bytes'] <= scratch_cap and 0 < resource['wall_seconds'] <= wall and not resource['monitor_errors'], 'probe deadline/scratch closure')
    clean = local.decode((out/'screen/cleanup.json').read_bytes())
    for n in ('scratch_removed', 'monitor_stopped', 'sdk_client_closed', 'original_root_removed'):
        exact(clean[n], not (n == 'original_root_removed' and PROBE_SCHEMA == FINE_SCHEMA and not canary), 'probe cleanup')
    calls = resource['sdk_calls']; objects = probe_objects(config, evidence)
    if canary:
        receipt = local.decode((out/'screen/canary.json').read_bytes())
        for n, expected in dict(schema=PROBE_CANARY_SCHEMA, status='GO', complete=True, scientific_performance_evidence=False,
            sdk_conditional_put_model=True, cli_exit_status=2, ann_queries=0, native_processes=0,
            truth_or_panel_body_reads=0, dataset_payload_gets=0).items():
            exact(receipt[n], expected, 'probe canary receipt')
        exact(receipt['versions'], dict(FIXED['versions'], **SDK_VERSIONS), 'probe canary real import versions')
        exact(receipt['config_sha256'], proof['config_sha256'], 'probe canary config')
        require('INVALID:' in receipt['cli_stderr'] and 'CLI:' in receipt['cli_stderr'], 'probe canary actual CLI')
        exact(receipt['sdk_calls'], calls, 'probe canary SDK receipt')
        exact(len(calls), len(objects)+len(probe.ROLE_NAMES), 'delegated HEAD roster plus authenticated log GETs')
        for call, pin in zip(calls[:len(objects)], objects):
            for n, expected in dict(operation='head_object', key=pin['key'], declared_bytes=pin['bytes'], outcome='returned').items():
                exact(call[n], expected, 'probe canary HEAD metadata')
            require('verified_sha256' not in call, 'HEAD cannot prove body SHA')
        logs = [config['canary_object']] if schema == OVERLAP_CANARY_SCHEMA else [dict(bytes=p['bytes'], sha256=p['sha256'], key=p['key']) for p in
                (objects[1], objects[4])] if schema == 'borsuk-constrained-split-falsifier-infrastructure-canary-v1' else [config['roles'][r]['native']['gate_log'] for r in probe.ROLE_NAMES]
        for call, pin in zip(calls[len(objects):], logs):
            for n, expected in dict(operation='get_object', key=pin['key'], verified_bytes=pin['bytes'], verified_sha256=pin['sha256'], outcome='returned').items():
                exact(call[n], expected, 'probe canary authenticated small log')
        exact(receipt['authenticated_logs'], [body_pin(p) for p in logs], 'probe two gate-log roles')
        exact(clean['native_processes_concurrent_max'], 0, 'probe canary no native')
        exact(local.decode((out/'screen/summary.json').read_bytes()), receipt, 'probe canary closed summary')
    else:
        seal = probe.verify_pair(out/'screen', repo=repo, config=config, config_pin=config_pin, evidence=evidence)
        probe.verify_execution(out/'screen', config, seal, evidence)
        exact(clean['native_processes_concurrent_max'], 1, 'probe serial native ownership')
        exact(clean['retained_layouts_preserved'], True, 'probe retained layouts before cleanup')
        exact(len(calls), len(objects), 'delegated body GET roster')
        for call, pin in zip(calls, objects):
            for n, expected in dict(operation='get_object', key=pin['key'], verified_bytes=pin['bytes'], verified_sha256=pin['sha256'], outcome='returned').items():
                exact(call[n], expected, 'probe staged authenticated GET')
        staging = local.decode((out/'screen/staging.json').read_bytes())
        exact(staging['sdk_calls'], calls, 'probe staging SDK closure')
        paired = config.get('schema') == PAIR_FIXED['schema']
        for n, expected in dict(truth_body_reads=2 if paired or config.get('schema') in (OVERLAP_SCHEMA, FINE_SCHEMA) else 0,
                old_panel_producer_called=False, diagnose_called=paired).items():
            exact(staging[n], expected, 'probe bypass old truth/diagnose path')
    return dict(executed=True, truth_opened=False, physical_s3_measured=False, vendor_win=False, scientific_qualification=False)


def probe_collect(s3, prefix, out, instance_id, commit, digest, *, canary=False):
    launch, close = (local.decode((Path(out)/n).read_bytes()) for n in ('aws-launch.json', 'aws-closeout.json'))
    exact(close['nodes'], launch['nodes'], 'probe collection SAME IDs'); exact(close['state'], 'terminated', 'probe collect after termination')
    exact(launch['instance_id'], instance_id, 'probe collection instance'); exact(launch['prefix'], prefix, 'probe collection prefix')
    schema, artifacts = (PROBE_CANARY_SCHEMA, PROBE_CANARY_ARTIFACTS) if canary else (PROBE_SCHEMA, PROBE_ARTIFACTS)
    with patch.multiple(ids, SCHEMA=schema, ARTIFACTS=artifacts, replay=lambda out: probe_replay(out, canary=canary)):
        return ids.collect(s3, prefix, out, instance_id, commit, digest)


def probe_require_canary(base, proof):
    pointer = local.read_json(local.identity(Path(base)/PROBE_ROOT/'canary-admission.json'))
    fields(pointer, 'schema attempt config_sha256 code_identity_sha256 refs_identity_sha256 native_identity_sha256 '
           'source_archive_paths_sha256 terminal_sha256', 'probe canary pointer')
    expected_schema = 'borsuk-fine-sq8-canary-admission-v1' if PROBE_SCHEMA == FINE_SCHEMA else 'borsuk-cell-overlap-canary-admission-v1' if PROBE_SCHEMA == OVERLAP_SCHEMA else 'borsuk-capacity-partitioner-canary-admission-v1' if PROBE_SCHEMA == PAIR_SCHEMA else 'borsuk-global-leaf-probe-canary-admission-v1'
    exact(pointer['schema'], expected_schema, 'probe canary admission')
    require(re.fullmatch(r'a[0-9]{4}', pointer['attempt']), 'probe canary attempt')
    out = Path(base)/PROBE_ROOT/'canary'/pointer['attempt']; terminal = local.decode((out/'aws-terminal.json').read_bytes())
    exact(local.identity(out/'aws-terminal.json')['sha256'], pointer['terminal_sha256'], 'probe admitted canary terminal')
    for n in ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256', 'source_archive_paths_sha256'):
        exact(pointer[n], proof[n], 'probe canary unchanged source/config'); exact(terminal[n], proof[n], 'probe canary terminal ancestry')
    require(probe_replay(out, canary=True, repo=base)['executed'], 'probe terminated canary GO required')


def probe_campaign(*, canary=False):
    return SimpleNamespace(ROOT=PROBE_ROOT, NAME='canary' if canary else '',
        SCHEMA=PROBE_CANARY_SCHEMA if canary else PROBE_SCHEMA,
        PREFIX=PROBE_CANARY_PREFIX if canary else PROBE_PREFIX,
        TOKEN_PREFIX='global-leaf-probe-canary-' if canary else 'global-leaf-probe-',
        TAG='borsuk-global-leaf-probe-canary' if canary else 'borsuk-global-leaf-probe',
        WALL=CANARY_WALL if canary else WALL, COMPUTE_CAP=.12 if canary else COMPUTE_CAP,
        INSTANCE_TYPE=INSTANCE_TYPE, IMAGE_ID=IMAGE_ID, ROOT_DEVICE_NAME=ROOT_DEVICE_NAME, SUBNET=SUBNET,
        SPOT_MAX_USD_PER_HOUR=SPOT_MAX_USD_PER_HOUR,
        ARTIFACTS=PROBE_CANARY_ARTIFACTS if canary else PROBE_ARTIFACTS,
        preflight=lambda: probe_preflight(canary=canary), user_data=lambda *a: probe_user_data(*a, canary=canary),
        poll=lambda *a: poll(*a, canary=canary), collect=lambda *a: probe_collect(*a, canary=canary))


def probe_reservation(*args, **kwargs):
    if kwargs.get('schema') == PROBE_CANARY_SCHEMA and 'ebs_s3_allowance_usd' in kwargs:
        exact(kwargs['ebs_s3_allowance_usd'], .15, 'probe shared allowance'); kwargs['ebs_s3_allowance_usd'] = .05
    return dict(*args, **kwargs)


def probe_main(attempt, *, canary=False):
    require(re.fullmatch(r'a[0-9]{4}', attempt), 'probe attempt must be aNNNN')
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1]); probe_preflight(canary=canary)
        shared, _ = ids.lifecycle()
        with patch.object(shared, 'dict', probe_reservation, create=True):
            return shared.main(attempt, campaign=probe_campaign(canary=canary))
    finally:
        os.chdir(before)


def probe_cli(args):
    require(args, 'CLI: --global-leaf-probe aNNNN | --canary aNNNN | --stage[-canary] REPO OUTPUT ROOT | --replay[-canary] OUTPUT | --self-check')
    if args == ['--self-check']:
        probe.self_check(); probe.pipeline_self_check(); probe.owned_failure_self_check(); probe.worker_closure_self_check(); probe_self_check()
    elif len(args) == 4 and args[0] in ('--stage', '--stage-canary'):
        print(json.dumps(probe_stage(*args[1:], canary=args[0] == '--stage-canary')))
    elif len(args) == 2 and args[0] in ('--replay', '--replay-canary'):
        print(json.dumps(probe_replay(args[1], canary=args[0] == '--replay-canary')))
    else:
        canary = len(args) == 2 and args[0] == '--canary'
        require(canary or len(args) == 1, 'CLI: --global-leaf-probe aNNNN | --canary aNNNN')
        with open('/tmp/borsuk-global-leaf-probe-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            probe_main(args[1] if canary else args[0], canary=canary)


def probe_admission_self_check():
    """Cheap admission precedes the single nomination run."""
    proof = {n: 'a'*64 for n in ('code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256')}
    config = dict(protocol='fixture', admission=dict(schema='borsuk-global-leaf-real-no-gt-admission-v2',
        authority_pending=False, source_bound_real_no_gt=True, result_sha256='b'*64, **proof))
    config['admission']['config_sha256'] = local.sha(local.canonical({'protocol': 'fixture'}))
    receipt = dict(schema='borsuk-global-leaf-real-no-gt-admission-receipt-v2', status='ADMITTED',
        complete=True, truth_opened=False, original_root_authority_pins_authenticated=True,
        historical_control_schema_admitted=True, exact_request_bodies_authenticated=True,
        query_f32_hashes_authenticated=True, resource_gate_passed=True, cleanup_complete=True,
        dataset_count=2, queries=128, ann_queries=0, native_processes=0, selection_receipts=0,
        **{n: config['admission'][n] for n in (*proof, 'config_sha256', 'result_sha256')})
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        def check(value):
            (root/'receipt.json').write_bytes(local.canonical(value))
            config['admission']['receipt'] = dict(local.identity(root/'receipt.json'), path='receipt.json')
            probe_require_admission(config, proof, root)
        check(receipt)
        for field, value in (('selection_receipts', 256), ('native_processes', 1), ('truth_opened', True),
                             ('queries', 127), ('query_f32_hashes_authenticated', False)):
            try:
                check(dict(receipt, **{field: value}))
            except Exception:
                pass
            else:
                raise AssertionError('admitted invalid cheap receipt: '+field)
    print('PASS cheap admission accepts exact inputs without executing the paired probe; invalid receipts rejected')


def probe_self_check(metadata_repo=None):
    """Real SDK service model/CLI, mocked transport/optional decoder imports."""
    from contextlib import ExitStack
    from types import SimpleNamespace
    import botocore.session
    probe_admission_self_check()
    metadata_repo = Path(metadata_repo or os.environ.get('BORSUK_GLOBAL_LEAF_METADATA_REPO', Path(__file__).resolve().parents[1]))
    probe.self_check_qualification(metadata_repo)
    original_config = local.read_json(local.identity(metadata_repo/probe.OLD/'screen/config.json'), 256 << 10)
    original = dict(schema='borsuk-global-leaf-binary-authority-v1', role='original', authority_pending=False,
        native=original_config['native'], refs={n: original_config['refs'][n] for n in probe.REF_NAMES})
    nominee = local.read_json(local.identity(metadata_repo/probe.BASE.parent/'implementation-gates/minimal-archive/qualified-nomination-authority.json'))
    config = dict(roles=dict(original=original, nomination=nominee), evidence={n: dict(local.identity(metadata_repo/p), path=str(p)) for n, p in dict(
        preregistration=probe.BASE/'global-leaf-routing-probe-preregistration.json',
        recovery=probe.BASE/'global-leaf-layout-recovery-admission.json', original_terminal=probe.OLD/'aws-terminal.json',
        original_launch=probe.OLD/'aws-launch.json', original_closeout=probe.OLD/'aws-closeout.json').items()})
    evidence = probe.original_evidence(metadata_repo, config)
    objects = probe_objects(config, evidence); exact(len(objects), 15, 'distinct probe roster')
    logs = {config['roles'][r]['native']['gate_log']['key']:
            (metadata_repo/config['roles'][r]['native']['gate_log']['path']).read_bytes() for r in probe.ROLE_NAMES}
    class Client:
        meta = SimpleNamespace(service_model=botocore.session.Session().get_service_model('s3'))
        closed = False
        def head_object(self, **args):
            return dict(ContentLength=next(p['bytes'] for p in objects if p['key'] == args['Key']))
        def get_object(self, **args):
            require(args['Key'] in logs, 'canary fetched dataset/truth/native body')
            return dict(ContentLength=len(logs[args['Key']]), Body=io.BytesIO(logs[args['Key']]))
        def close(self):
            self.closed = True
    client = Client(); versions = dict(FIXED['versions'], **SDK_VERSIONS)
    real_import, real_version, real_ref = importlib.import_module, importlib.metadata.version, read_ref
    def module_import(name, *args, **kwargs):
        if name in ('numpy', 'pyarrow'):
            return SimpleNamespace(__version__=versions[name])
        return real_import(name, *args, **kwargs)
    def version(name):
        return versions[name] if name in ('numpy', 'pyarrow') else real_version(name)
    def ref_read(_repo, pin, cap=local.CONFIG_CAP):
        return real_ref(metadata_repo, pin, cap)
    with tempfile.TemporaryDirectory(prefix='global-leaf-canary-') as tmp, ExitStack() as stack:
        for module, name in ((local, 'prepare'), (local, 'run_stage'), (local, 'validate_diagnostic'),
                            (sys.modules[__name__], 'panel_inputs'), (sys.modules[__name__], 'admission'),
                            (sys.modules[__name__], 'reduce_diagnostic'), (probe, 'execute'), (probe, 'native_stage')):
            stack.enter_context(patch.object(module, name, side_effect=AssertionError('forbidden canary native/panel/GT: '+name)))
        stack.enter_context(patch.object(importlib, 'import_module', side_effect=module_import))
        stack.enter_context(patch.object(importlib.metadata, 'version', side_effect=version))
        stack.enter_context(patch.object(sys.modules[__name__], 'read_ref', side_effect=ref_read))
        calls = []
        try:
            receipt = probe_canary(config, evidence, client, calls, Path(tmp), lambda: None, time.monotonic()+30)
        finally:
            client.close()
        exact(receipt['status'], 'GO', 'probe canary source-only path'); exact(len(calls), 17, 'probe mock HEAD15/GET2')
        exact(receipt['native_processes'], 0, 'canary no native'); require(client.closed, 'canary SDK client cleanup')
        require(all('truth' not in c['key'].lower() for c in calls), 'canary GT body spy')
    # Shared lifecycle tests cover reservation races, upload/CLI/collection
    # failure and exact-instance termination+wait using mocked EC2/S3 only.
    shared, _ = ids.lifecycle(); shared.self_check(lifecycle_only=True)
    proof = {n: '0'*64 for n in TERMINAL_IDENTITIES}
    proof.update(config_path=str(PROBE_CONFIG), source_file_count=401, campaign_schema=PROBE_SCHEMA,
                 awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
    scientific = probe_user_data('a'*40, 'b'*64, 'source/mock', PROBE_PREFIX+'a0001', proof)
    proof['campaign_schema'] = PROBE_CANARY_SCHEMA
    canary = probe_user_data('a'*40, 'b'*64, 'source/mock', PROBE_CANARY_PREFIX+'a0001', proof, canary=True)
    require('/mnt/hierarchical-global-leaf-probe' in scientific and 'CPUQuota=200%' in scientific
            and 'MemoryMax=2G' in scientific and '--global-leaf-probe --stage ' in scientific,
            'probe host bootstrap limits/separate mode')
    require('MemoryMax=256M' in canary and 'CPUQuota=100%' in canary and '--global-leaf-probe --stage-canary' in canary,
            'probe canary bootstrap limits/separate mode')
    require('sync -f terminal.json' in scientific and 'INSTANCE_ID="$instance_id"' in scientific,
            'shared owned-ID terminal-last bootstrap')
    print('PASS probe metadata/actual botocore model+CLI/cleanup; mocked SDK transport and NumPy/Arrow imports; shared owned lifecycle negatives; shell syntax/fsync/terminal-last/caps. No network/native/corpus/GT.')


PAIR_ROOT = probe.PARTITIONER_ROOT
PAIR_CONFIG = PAIR_ROOT/'config.json'
PAIR_SCHEMA = 'borsuk-capacity-partitioner-paired100k-spot-v1'
PAIR_CANARY_SCHEMA = 'borsuk-capacity-partitioner-infrastructure-canary-v1'
PAIR_PREFIX = 'research/hierarchical-cells/20261004/capacity-partitioner-paired100k-'
PAIR_CODE = tuple(sorted((*PROBE_CODE, 'scripts/launch_native_workspace_execution_spot.py',
                         'scripts/check_native_workspace_execution.py')))
PAIR_FIXED = {n: PROBE_FIXED[n] for n in PROBE_FIXED if n not in
    ('schema', 'source_object_count', 'native_object_count', 'no_truth_body_access', 'reduction_enabled')}
PAIR_FIXED.update(schema='borsuk-capacity-partitioner-paired100k-staging-v1', no_truth_body_access=False,
    reduction_enabled=True, policy=local.POLICY, mean_recall_minimum=.98, p05_hits_minimum=95,
    complete_query_latency='complete-query-unmeasured')
PAIR_EVIDENCE = ('method', 'inline_method', 'candidate_manifest', 'original_authority', 'retained_seal',
                 'retained_terminal', 'retained_launch', 'retained_closeout')
PAIR_OUTPUTS = ('config.json', 'source-qualification.json', 'tool-versions.json', 'staging.json',
    'paired-seal.json', 'admission-seal.json', 'inline-admission.json', 'native-execution-receipt.json', 'execution-receipt.json', 'summary.json',
    'resources.json', 'worker-cgroup.json', 'cleanup.json',
    *(f'measurement/{n}{suffix}' for n in probe.PAIR_STAGES for suffix in
        ('-stage.json', '-stage-receipt.json', '-closure.json', '.log', '-unit.log')),
    *(f'measurement/{d}-partitioner-build.json' for d in probe.DATASETS),
    *(f'measurement/{d}-{r}-{n}' for r in probe.PAIR_ROLES for d in probe.DATASETS for n in ('diagnose.json', 'diagnostic.jsonl')),
    *(f'measurement/{n}{suffix}' for n in probe.PAIR_ADMISSION_STAGES for suffix in ('-config.json', '-diagnostic.jsonl')),
    *(f'measurement/{n}{suffix}' for n in probe.PAIR_ADMISSION_STAGES[:-1] for suffix in ('-request.jsonl', '-truth.u32')),
    *(f'retained/{d}/{r}/{n}' for r in probe.PAIR_ROLES for d in probe.DATASETS for n in
        ('manifest.json', 'directories.bin', 'cells.bin', 'requests64', 'truth64', 'build.json', 'original-writer.json')))
PAIR_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in PAIR_OUTPUTS))


def pair_objects(config, evidence):
    pins = []
    for r in probe.PAIR_ROLES:
        native = config['roles'][r]['native']
        pins.extend([native['source_archive'], native['gate_log'], *native['binaries'].values()])
    pins.extend(i['artifacts'][n] for i in evidence['sources']['items']
                for n in ('source', 'order.u64', 'sq8.bin', 'requests', 'truth'))
    pins = [{k: p[k] for k in ('key', 'bytes', 'sha256')} for p in pins]
    require(len({p['key'] for p in pins}) == len(pins), 'distinct exact pair object roster')
    for pin in pins:
        publication.object_identity(pin)
    return pins


def pair_scratch_roster(config, evidence, paths, repo):
    native = [p for r in probe.PAIR_ROLES for p in
        (config['roles'][r]['native']['source_archive'], config['roles'][r]['native']['gate_log'],
         *config['roles'][r]['native']['binaries'].values())]
    bounded_refs = {str(PAIR_CONFIG): 512 << 10}
    roster = [dict(name='bootstrap-archives-and-unpacked-controller', max_bytes=2*sum(
                  bounded_refs[p] if p in bounded_refs else local.identity(repo/p)['bytes'] for p in paths)),
              dict(name='qualified-native-archives-logs-binaries', max_bytes=sum(p['bytes'] for p in native))]
    for source in evidence['sources']['items']:
        d = source['name']; item = evidence['items'][d]
        roster.extend(dict(name=d+'-input-'+n, max_bytes=source['artifacts'][n]['bytes'])
                      for n in ('source', 'order.u64', 'sq8.bin', 'requests', 'truth'))
        roster.append(dict(name=d+'-decoded-raw', max_bytes=100000*768*4 if d == 'relaion' else 0))
        roster.append(dict(name=d+'-consumed-panels', max_bytes=item['inputs']['requests']['bytes']+25600))
        unique = {p['path']: p['bytes'] for p in item['recovery']['build_inputs'].values()}
        roster.append(dict(name=d+'-generation', max_bytes=sum(unique.values())))
        for r in probe.PAIR_ROLES:
            # Native max_output_bytes includes staging. Retained copies coexist.
            roster.append(dict(name=d+'-'+r+'-layout', max_bytes=256 << 20))
            roster.append(dict(name=d+'-'+r+'-retained-layout-panels-configs', max_bytes=(256 << 20)+item['inputs']['requests']['bytes']+25600+(192 << 10)))
    for n in probe.PAIR_STAGES:
        roster.append(dict(name=n+'-logs-spec-and-receipts', max_bytes=(32 << 20)+(2 << 20)))
    roster += [dict(name='four-diagnostic-outputs', max_bytes=4*(128 << 20)),
               dict(name='inline-admission-panels-configs-seals-marker', max_bytes=(1 << 20)+2*(8 << 20)),
               dict(name='five-inline-admission-outputs', max_bytes=5*(128 << 20)),
               dict(name='cleanup-and-runtime-reserve', max_bytes=config['scratch_reserve_bytes'])]
    return roster


def pair_qualify(base=Path('.'), *, canary=False):
    repo = Path(base).resolve(); pin = local.identity(repo/PAIR_CONFIG); config = local.read_json(pin, 512 << 10)
    fields(config, ' '.join(PAIR_FIXED)+' authority_pending run_id code_sha256 evidence pair_evidence roles resources '
           'phase_seconds object_roster scratch_roster scratch_admission_bytes scratch_reserve_bytes admission', 'fixed pair config')
    exact(config['authority_pending'], False, 'root pair freeze pending')
    require(re.fullmatch(r'a[0-9]{4}', config['run_id']), 'immutable pair runID')
    for n, expected in PAIR_FIXED.items():
        exact(config[n], expected, 'fixed prospective policy: '+n)
    probe.pair_limits(config); fields(config['roles'], 'original partitioner', 'two binary layout authorities')
    exact(set(config['code_sha256']), set(PAIR_CODE), 'pair existing controller/qualification-validator roster')
    for n, digest in config['code_sha256'].items():
        exact(local.identity(repo_path(repo, n))['sha256'], digest, 'pair code drift')
    fields(config['pair_evidence'], ' '.join(PAIR_EVIDENCE), 'prospective/historical evidence roster')
    values = {n: probe.ref(repo, p, 128 << 10 if n == 'candidate_manifest' else 8 << 20)
              for n, p in config['pair_evidence'].items()}
    exact(config['pair_evidence']['method']['path'], str(PAIR_ROOT/'prospective-method.json'), 'prospective method path')
    exact(config['pair_evidence']['inline_method']['path'], str(PAIR_ROOT/'prospective-inline-admission-amendment.json'), 'inline method path')
    pair_inline_method(config, values['inline_method'])
    exact(config['pair_evidence']['candidate_manifest']['path'], str(probe.PARTITIONER_MANIFEST), 'candidate402 source path')
    method = values['method']
    for n, expected in dict(schema='borsuk-capacity-constrained-partitioner-paired100k-method-v1',
            rows=100000, dimensions=768, metric='cosine', k=100, queries_per_dataset=64,
            datasets=list(probe.DATASETS), constants=dict(cell_rows=512, sample_rows=256, max_depth=32,
            primary_beam=8, boundary_beam=24, blocks_per_cell=4, fetch_policy='whole_cell',
            max_selected_cells=32, max_total_cell_gets=32, max_cell_bytes=16 << 20),
            frozen_arm_quality=dict(mean_recall_at_100=.98, p05_hits_out_of_100=95)).items():
        exact(method[n], expected, 'prospective frozen method')
    exact(config['pair_evidence']['original_authority']['path'], str(ROOT/'qualified-original-binary-authority.json'), 'historical control authority path')
    exact(values['original_authority'], config['roles']['original'], 'historical original401 authority unchanged')
    roles = {r: probe.qualify_role(r, config['roles'][r], repo) for r in probe.PAIR_ROLES}
    evidence = probe.original_evidence(repo, config)
    retained_root = ROOT/'global-leaf-probe/a0002'
    for n, filename in dict(retained_seal='screen/paired-seal.json', retained_terminal='aws-terminal.json',
            retained_launch='aws-launch.json', retained_closeout='aws-closeout.json').items():
        exact(config['pair_evidence'][n]['path'], str(retained_root/filename), 'retained a0002 authority path')
    terminal, launch, close = (values[n] for n in ('retained_terminal', 'retained_launch', 'retained_closeout'))
    exact(close['state'], 'terminated', 'retained host closed'); exact(close['nodes'], launch['nodes'], 'retained SAME IDs')
    require(terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in close['nodes'].values()}, 'retained owned host')
    exact(terminal['phase'], 'complete', 'retained completed terminal'); exact(terminal['original_exit_code'], 0, 'retained actual exit0')
    exact(terminal['exit_code'], 0, 'retained supervisor exit0')
    seal = values['retained_seal']; exact(seal['schema'], probe.SEAL_SCHEMA, 'retained historical seal')
    exact(seal['complete'], True, 'retained seal complete'); exact(seal['truth_opened'], False, 'retained noGT')
    exact(body_pin(config['pair_evidence']['retained_seal']), terminal['artifacts']['screen/paired-seal.json'], 'terminal-bound retained seal')
    exact(seal['roles']['original'], roles['original'], 'retained qualified original native authority')
    for d in probe.DATASETS:
        exact(seal['datasets'][d]['root']['sha256'], evidence['items'][d]['recovery']['expected_cell_root_sha256'], 'retained historical control root')
        for n, filename in dict(root='manifest.json', directories='directories.bin', cells='cells.bin').items():
            exact(body_pin(seal['datasets'][d][n]), terminal['artifacts'][f'screen/retained/{d}/{filename}'], 'terminal-bound retained layout')
    objects = pair_objects(config, evidence); exact(config['object_roster'], objects, 'frozen exact transport roster')
    paths = {str(PAIR_CONFIG), *PAIR_CODE, *(p['path'] for p in config['evidence'].values()),
             *(p['path'] for p in config['pair_evidence'].values())}
    for r in probe.PAIR_ROLES:
        paths.update(p['path'] for p in config['roles'][r]['refs'].values())
        qpath = roles[r]['source_qualification']['path']; paths.add(qpath if (repo/qpath).exists() else qpath+'.gz')
        paths.add(config['roles'][r]['native']['gate_log']['path'])
    paths.update(str(probe.OLD/n) for n in evidence['values']['original_terminal']['artifacts'] if n.startswith('screen/'))
    old_config = local.read_json(local.identity(repo/probe.OLD/'screen/config.json'))
    paths.update(p['path'] for p in old_config['refs'].values())
    # Admission is produced inside this immutable job after layouts exist.
    paths = sorted(paths)
    for p in paths:
        publication.relative(p); repo_path(repo, p)
    local.integer(config['scratch_reserve_bytes'], 1, SCRATCH, 'whole pair scratch reserve')
    roster = pair_scratch_roster(config, evidence, paths, repo)
    exact(config['scratch_roster'], roster, 'static exact whole-scratch roster')
    require(sum(p['max_bytes'] for p in roster) <= config['scratch_admission_bytes'] <= SCRATCH, 'all coexisting scratch fits16GiB')
    proof = dict(config_path=str(PAIR_CONFIG), config_sha256=pin['sha256'],
        campaign_schema=PAIR_CANARY_SCHEMA if canary else PAIR_SCHEMA,
        code_identity_sha256=ids.sha(ids.encoded(config['code_sha256'])),
        refs_identity_sha256=ids.sha(ids.encoded(dict(evidence=config['evidence'], pair_evidence=config['pair_evidence'],
            roles={r: config['roles'][r]['refs'] for r in probe.PAIR_ROLES}))),
        native_identity_sha256=ids.sha(ids.encoded(roles)), source_file_count=402,
        source_archive_paths=paths, source_archive_paths_sha256=ids.sha(ids.encoded(paths)),
        artifact_roster_sha256=ids.sha(ids.encoded(PROBE_CANARY_ARTIFACTS if canary else PAIR_ARTIFACTS)),
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
    pair_require_admission(config)
    return config, proof, evidence


def pair_inline_method(config, method):
    """Authenticate the parent's preregistered protocol through pair_evidence."""
    expected = dict(schema='borsuk-capacity-partitioner-inline-admission-method-v1',
        methodology_frozen_before_run=True, native_candidate=probe.PARTITIONER_COMMIT,
        parent_method=config['pair_evidence']['method'],
        native_calls=dict(original_writers=2, layout_builds=4, one_query_positive_admissions=4,
            corrupted_truth_negative_admissions=1, measured_64_query_diagnoses=4, total=15),
        admission_query='Consumed ordinal0 unchanged f32 bytes and original100 truth IDs per dataset/role; no quality threshold/promotion/tuning from admission.',
        failure_disposition='Any admission/identity/resource/durability/cleanup error stops before measured64 as execution INVALID, never architecture quality KILL.',
        measurement_limits=dict(cache_state='samehost filesystem cache after builds and symmetric admission; report local stage timing, not object-store cold performance',
            complete_query_latency='UNMEASURED; stage sums explicitly labelled', generalization='consumed historical panels; no fresh held-out claim',
            physical_s3_query_latency_or_vendor_parity=False),
        unchanged=['two datasets100kD768cosinek100 and their64 consumed query panels',
            'original401 and qualified candidate402 separate binary/source authorities',
            'cell512/sample256/depth32/primary8/boundary24/blocks4',
            'WholeCell32GET/16MiB and frozen .98 mean recall@100/p05 hits95 arm',
            'SQ2/SQ8 arithmetic and query preparation',
            'host2GiB/noSwap/CPU2/Tasks512/scratch16GiB/deadline1800s',
            'separate metadata-only canary with zeroANN/GT reads'])
    for n, value in expected.items():
        exact(method[n], value, 'preregistered inline method: '+n)


def pair_require_admission(config):
    admission = config['admission']
    exact(admission, dict(schema=probe.PAIR_ADMISSION_SCHEMA, kind='inline_native',
        positive_count=4, negative_truth_sha256=True, measured_requires_admitted=True,
        quality_promotion=False), 'immutable inline native admission policy')


def pair_profile():
    """Reuse existing lifecycle/canary, with a fixed prospective role roster."""
    from contextlib import ExitStack
    stack = ExitStack(); module = sys.modules[__name__]
    old_user_data, old_replay = probe_user_data, probe_replay
    def user_data_pair(*args, **kwargs):
        if not kwargs.get('canary'):
            cfg = local.read_json(local.identity(PAIR_CONFIG), 512 << 10)
            exact(args[3], PAIR_PREFIX+cfg['run_id'], 'one immutable measured pair run')
        return old_user_data(*args, **kwargs).replace('--global-leaf-probe', '--partitioner-pair').replace(
            '/mnt/hierarchical-global-leaf-probe', '/mnt/hierarchical-capacity-partitioner-pair').replace('phase=paired-nomination', 'phase=partitioner-pair')
    def replay_pair(out, *, canary=False, repo=None):
        result = old_replay(out, canary=canary, repo=repo)
        if not canary and result['executed']:
            receipt = local.read_json(local.identity(Path(out)/'screen/summary.json'), 8 << 20)
            result.update(truth_opened=True, status='DIAGNOSTIC', candidate_status=receipt['candidate_status'],
                          control_status=receipt['control_status'], complete_query_latency='complete-query-unmeasured')
        return result
    helper = SimpleNamespace(**vars(probe))
    helper.execute, helper.verify_pair, helper.verify_execution = probe.pair_execute, probe.pair_verify_seal, probe.pair_verify_execution
    helper.ROLE_NAMES = probe.PAIR_ROLES
    stack.enter_context(patch.multiple(module, PROBE_ROOT=PAIR_ROOT, PROBE_CONFIG=PAIR_CONFIG, PROBE_SCHEMA=PAIR_SCHEMA,
        PROBE_CANARY_SCHEMA=PAIR_CANARY_SCHEMA, PROBE_PREFIX=PAIR_PREFIX, PROBE_CANARY_PREFIX=PAIR_PREFIX+'canary-',
        PROBE_CODE=PAIR_CODE, PROBE_ARTIFACTS=PAIR_ARTIFACTS, probe=helper, probe_qualify=pair_qualify, probe_objects=pair_objects,
        probe_user_data=user_data_pair, probe_replay=replay_pair))
    return stack



def pair_launcher_self_check():
    """No credentials/native bodies: generated shell, CLI and namespace only."""
    method_path = PAIR_ROOT/'prospective-inline-admission-amendment.json'
    fixture = method_path if method_path.exists() else Path(os.environ.get('BORSUK_INLINE_METHOD_FIXTURE', ''))
    require(fixture.is_file(), 'parent inline amendment or exact temporary fixture required')
    with tempfile.TemporaryDirectory(prefix='pair-inline-method-mock-') as tmp:
        repo = Path(tmp); path = repo/method_path; path.parent.mkdir(parents=True)
        shutil.copyfile(fixture, path)
        pin = dict(local.identity(path), path=str(method_path))
        method = probe.ref(repo, pin)
        cfg = dict(pair_evidence=dict(method=method['parent_method'], inline_method=pin))
        pair_inline_method(cfg, method)
        for n, value in (('native_candidate', '0'*40), ('native_calls', dict(method['native_calls'], total=10)),
                ('unchanged', []), ('measurement_limits', dict(method['measurement_limits'], physical_s3_query_latency_or_vendor_parity=True)),
                ('parent_method', dict(method['parent_method'], sha256='0'*64))):
            try:
                pair_inline_method(cfg, dict(method, **{n: value}))
            except ValueError:
                pass
            else:
                raise AssertionError('changed frozen inline method accepted: '+n)
    inline = dict(schema=probe.PAIR_ADMISSION_SCHEMA, kind='inline_native', positive_count=4,
        negative_truth_sha256=True, measured_requires_admitted=True, quality_promotion=False)
    with patch.object(sys.modules[__name__], 'read_ref', side_effect=AssertionError('read future admission')):
        pair_require_admission(dict(admission=inline))
    try:
        pair_require_admission(dict(admission=dict(inline, receipt='future.json')))
    except ValueError:
        pass
    else:
        raise AssertionError('immutable config accepted future external admission')
    with tempfile.TemporaryDirectory(prefix='pair-import-closure-') as tmp:
        for name in PAIR_CODE:
            path = Path(tmp)/name; path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(name, path)
        subprocess.run([sys.executable, '-B', '-c',
            'from scripts import run_hierarchical_global_leaf_probe as p; p.pair_gate_self_check()'],
            cwd=tmp, timeout=10, check=True, env=dict(os.environ, PYTHONPATH=tmp))
    with tempfile.TemporaryDirectory(prefix='pair-bootstrap-mock-') as tmp:
        cfg_path = Path(tmp)/'config.json'; local.write_json(cfg_path, dict(run_id='a0001'))
        proof = {n: '0'*64 for n in TERMINAL_IDENTITIES}
        proof.update(config_path=str(cfg_path), source_file_count=402, campaign_schema=PAIR_SCHEMA,
                     awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
        with patch.object(sys.modules[__name__], 'PAIR_CONFIG', cfg_path), pair_profile():
            for canary in (False, True):
                body = probe_user_data('a'*40, 'b'*64, 'source/mock', PAIR_PREFIX+('canary-' if canary else '')+'a0001', proof, canary=canary)
                require('--partitioner-pair --stage' in body and 'sync -f terminal.json' in body
                        and 'MemorySwapMax=0' in body and '/mnt/hierarchical-capacity-partitioner-pair' in body,
                        'shared terminal-last prospective shell/selector/resource boundary')
                require('CPUQuota='+('100' if canary else '200')+'%' in body, 'pair/bootstrap CPU envelope')
                exact(probe.ROLE_NAMES, ('original', 'partitioner'), 'scoped role roster')
    cli = subprocess.run([sys.executable, '-B', '-m', MODULE, '--partitioner-pair'], capture_output=True, text=True, timeout=10)
    require(cli.returncode == 2 and 'INVALID:' in cli.stderr and 'CLI:' in cli.stderr and not cli.stdout, 'pending pair CLI exits2 before any launch')
    exact(probe.ROLE_NAMES, ('original', 'nomination'), 'historical namespace restored')
    print('PASS authenticated parent inline amendment/candidate15calls/unchanged science/resources; exact packaged import closure, bootstrap shell syntax/terminal-last/limits, actual CLI exit2, scoped historical namespace restoration; no cloud/native bodies.')

def pair_cli(args):
    require(args, 'CLI: --partitioner-pair aNNNN | --canary aNNNN | --stage[-canary] REPO OUTPUT ROOT | --replay[-canary] OUTPUT | --self-check')
    if args == ['--self-check']:
        probe.pair_self_check(); pair_launcher_self_check(); return
    with pair_profile():
        probe_cli(args)


# Transport and lifetime only; the separately frozen helper owns all six calls.
OVERLAP_ROOT = ROOT.parent/'boundary-overlap/paired100k'
OVERLAP_CONFIG = OVERLAP_ROOT/'config.json'
OVERLAP_SCHEMA = 'borsuk-cell-overlap-paired100k-spot-v1'
OVERLAP_CANARY_SCHEMA = 'borsuk-cell-overlap-infrastructure-canary-v1'
OVERLAP_PREFIX = 'research/hierarchical-cells/20261005/cell-overlap-paired100k-'
OVERLAP_WORKER = Path('/mnt/hierarchical-cell-overlap-pair')
OVERLAP_GATE = ROOT.parent/'boundary-overlap/implementation-gates/a0001'
OVERLAP_HELPER_SHA = '66ce4aa6c896c28bea73769d1f2be68b102d6ec0e6b4ae83a609aeda7fe68608'
OVERLAP_MACHINE = dict(wall_seconds=6500, compute_cap_usd=1.25)
from scripts import launch_native_workspace_execution_spot as overlap_controller
with overlap_controller.execution_mode(cell_overlap=True):
    OVERLAP_ASSURANCE = overlap_controller.ARTIFACTS
    OVERLAP_CODE = tuple(sorted(set((*PROBE_CODE, *overlap_controller.CODE,
        'scripts/run_cell_overlap_pair.py', 'scripts/run_source_witness_paired_coverage.py'))))
OVERLAP_ORDER = tuple(d+'-'+a+'-build-overlap' for d in probe.DATASETS for a in ('control', 'candidate'))+tuple(d+'-paired-overlap' for d in probe.DATASETS)
OVERLAP_OUTPUTS = ('config.json', 'source-qualification.json', 'tool-versions.json', 'staging.json',
    'native-execution-receipt.json', 'execution-receipt.json', 'summary.json', 'resources.json', 'worker-cgroup.json', 'cleanup.json',
    'overlap-config.json', 'overlap/config.json', 'overlap/terminal.json', 'overlap/execution-receipt.json',
    'overlap/authority/current-source.json', 'overlap/authority/retained-input-authority.json',
    *(f'overlap/authority/{n}.json' for n in ('terminal', 'launch', 'closeout')),
    *('overlap/authority/native/'+n for n in OVERLAP_ASSURANCE), 'overlap/binary/hierarchical_semantic_cells',
    *(f'overlap/inputs/{d}/{category}/{n}' for d in probe.DATASETS for category, names in (
        ('layout', ('manifest.json', 'directories.bin', 'cells.bin')), ('original', 'generation plane canonical order records mean sq8'.split())) for n in names),
    *(f'overlap/requests/{d}/requests64' for d in probe.DATASETS),
    *(f'overlap/layouts/{d}-{a}/{n}' for d in probe.DATASETS for a in ('control', 'candidate') for n in ('manifest.json', 'sq8-cells.bin', 'placement.bin', 'boundaries.json')),
    *(f'overlap/layouts/{d}-{a}.build.jsonl' for d in probe.DATASETS for a in ('control', 'candidate')),
    *(f'overlap/measurement/{n}{s}' for n in OVERLAP_ORDER for s in ('-config.json', '-stage.json', '-stage-receipt.json', '-closure.json', '.log', '-unit.log')),
    *(f'overlap/measurement/{d}-paired-overlap{s}' for d in probe.DATASETS for s in ('.jsonl', '.paired-seal.json')))
OVERLAP_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in OVERLAP_OUTPUTS))


def overlap_helper():
    from scripts import run_cell_overlap_pair
    return run_cell_overlap_pair


def overlap_gzip(pin, target, raw_pin, cap):
    """Bounded opaque restoration; CR/whitespace remain original evidence."""
    import gzip
    require(raw_pin['bytes'] <= cap, 'uncompressed qualification cap'); local.authenticate(pin, cap)
    target = Path(target); target.parent.mkdir(parents=True, exist_ok=True)
    with positive.open_input(pin['path']) as source, gzip.GzipFile(fileobj=source) as stream:
        publication.transfer(stream, body_pin(raw_pin), target)
    exact(body_pin(local.identity(target)), body_pin(raw_pin), 'original decompressed qualification bytes')
    return local.identity(target)


def overlap_objects(config, evidence=None):
    pins = [{k:p[k] for k in ('key', 'bytes', 'sha256')} for p in (*config['assets'], *config['native_assets'])]
    require(len({p['key'] for p in pins}) == len(pins), 'distinct frozen object roster')
    for p in pins: publication.object_identity(p)
    return pins


def overlap_input(config, d, role, name):
    item = config['execution']['inputs'][d]
    return item[role][name] if role in ('layout', 'original') else item[name]


def overlap_qualification(config, repo, paths):
    """Local preflight authenticates metadata/logs, leaving binary bodies cold."""
    helper = overlap_helper(); cfg = config['execution']; q = cfg['qualification']
    fields(q, 'pending directory proof terminal launch closeout', 'overlap qualified authority')
    exact(q['pending'], False, 'completed qualification required')
    remote_gate = OVERLAP_WORKER/'screen/retained/native-qualification'
    exact(q['directory'], str(remote_gate), 'fixed remote qualification directory')
    values = {}
    for n, filename in dict(proof='source-qualification.json', terminal='aws-terminal.json', launch='aws-launch.json', closeout='aws-closeout.json').items():
        p = q[n]; helper.descriptor(p, 8 << 20)
        exact(p['path'], str(remote_gate/filename), 'fixed original qualification pointer')
        name = str(OVERLAP_GATE/filename); paths.add(name)
        values[n] = local.read_json(dict(p, path=str(repo/name)), 8 << 20)
    proof, terminal, launch, close = (values[n] for n in ('proof', 'terminal', 'launch', 'closeout'))
    exact(close['state'], 'terminated', 'qualified host closed'); exact(close['nodes'], launch['nodes'], 'qualified SAME IDs')
    require(terminal['instance_id'] == launch['instance_id'] in {p['instance_id'] for p in close['nodes'].values()}, 'qualified original host')
    for n in ('phase', 'status'): exact(terminal[n], 'complete', 'original complete qualification')
    for n in ('exit_code', 'original_exit_code'): exact(terminal[n], 0, 'original qualification exit0')
    for n in ('source_commit', 'source_archive_sha256'): exact(terminal[n], launch[n], 'original launched source')
    exact(terminal['source_qualification_sha256'], q['proof']['sha256'], 'original proof SHA')
    sources = source_hashes(repo); manifest = probe.ref(repo, helper.MANIFEST)
    paths.add(helper.MANIFEST['path']); paths.update(sources)
    for value in (proof, manifest):
        exact(value['source_sha256'], sources, 'all frozen403 native source bytes')
        exact(value['native_source_commit'], helper.SOURCE_COMMIT, 'qualified final Rust revision')
        exact(value['source_file_count'], 403, 'full403 source roster')
    exact(source_identity(sources), helper.FULL_SOURCE_ID, 'full403 frozen identity')
    exact(proof['source_identity_sha256'], helper.FULL_SOURCE_ID, 'qualified full403 identity')
    exact(set(config['qualification_transport']), set(OVERLAP_ASSURANCE), 'all original assurance artifacts')
    with overlap_controller.execution_mode(cell_overlap=True), tempfile.TemporaryDirectory(prefix='overlap-log-preflight-') as tmp:
        exact(set(proof['code_sha256']), set(overlap_controller.CODE), 'original qualified controller roster')
        for name, digest in proof['code_sha256'].items():
            exact(local.identity(repo/name)['sha256'], digest, 'original qualified controller source unchanged')
        for n in overlap_controller.TERMINAL_IDENTITIES: exact(terminal[n], proof[n], 'original proof/terminal binding')
        exact(terminal['schema'], overlap_controller.SCHEMA, 'completed overlap gate mode')
        for name, pin in config['qualification_transport'].items():
            fields(pin, 'path bytes sha256 encoding', 'archived assurance transport')
            exact(pin['encoding'] in ('raw', 'gzip'), True, 'explicit archive encoding')
            relative = str(OVERLAP_GATE/name)+('.gz' if pin['encoding'] == 'gzip' else '')
            exact(pin['path'], relative, 'exact archived assurance path'); paths.add(relative)
            raw = terminal['artifacts'][name]
            local.integer(raw['bytes'], 1, 32 << 20, 'bounded assurance body')
            if pin['encoding'] == 'raw': exact(body_pin(pin), raw, 'terminal original assurance pin')
            # Binary descriptors are terminal-bound; remote helper opens them.
            if not name.startswith('binaries/'):
                target = Path(tmp)/name
                if pin['encoding'] == 'gzip': overlap_gzip(dict(body_pin(pin), path=str(repo/relative)), target, raw, 32 << 20)
                else: probe.copy_bytes(target, local.authenticate(dict(body_pin(pin), path=str(repo/relative)), 32 << 20, read=True))
        receipt = local.read_json(local.identity(Path(tmp)/'workspace-receipt.json'), 8 << 20)
        exact(receipt['source_sha256'], sources, 'original receipt native source map')
        exact(receipt['stages'], overlap_controller.validate_bounded_publication_stages(Path(tmp)/'test.log', cell_overlap=True), 'all seven original completed stages')
        overlap_controller.worker.validate_cgroup(local.read_json(local.identity(Path(tmp)/'workspace-cgroup.json')))
        for n in ('qualified', 'command_started', 'command_completed', 'source_unchanged'): exact(receipt[n], True, 'original closed gate receipt')
        for n in ('exit_status', 'gate_status'): exact(receipt[n], 0, 'original gate status')
    exact(cfg['binary']['path'], str(remote_gate/'binaries/hierarchical_semantic_cells'), 'fixed qualified binary path')
    exact(body_pin(cfg['binary']), terminal['artifacts']['binaries/hierarchical_semantic_cells'], 'qualified candidate binary descriptor')
    return proof


def overlap_scratch_roster(config, evidence, paths, repo):
    helper = overlap_helper(); incoming = config['execution']['inputs']
    retained = sum(incoming[d][c][n]['bytes'] for d in probe.DATASETS for c, names in (('original', helper.ORIGINALS), ('layout', helper.LAYOUT)) for n in names)
    assurance = sum(p['bytes'] for p in config['qualification_transport'].values())
    return [dict(name='archive-and-controller', max_bytes=2*sum((512 << 10) if p == str(OVERLAP_CONFIG) else repo_path(repo,p).stat().st_size for p in paths)),
        dict(name='venv-cli-bootstrap-reserve', max_bytes=config['scratch_reserve_bytes']),
        dict(name='direct-staged-bodies', max_bytes=sum(p['bytes'] for p in overlap_objects(config))+sum(p['bytes'] for v in config['headers'].values() for p in v.values())),
        dict(name='restored-and-retained-native-assurance', max_bytes=3*assurance+config['execution']['binary']['bytes']),
        dict(name='helper-retained-originals-and-layouts', max_bytes=retained),
        dict(name='four-overlap-layouts', max_bytes=4*(256 << 20)),
        dict(name='two-native-paired-outputs', max_bytes=2*(512 << 20)),
        dict(name='six-native-logs-specs-seals-receipts', max_bytes=6*(34 << 20)),
        dict(name='retained-request-panels', max_bytes=sum(incoming[d]['requests64']['bytes'] for d in probe.DATASETS))]


def overlap_qualify(base=Path('.'), *, canary=False):
    repo = Path(base).resolve(); config_pin = local.identity(repo/OVERLAP_CONFIG); config = local.read_json(config_pin, 512 << 10)
    fields(config, 'schema authority_pending run_id code_sha256 execution qualification_transport assets native_assets headers canary_object machine scratch_reserve_bytes scratch_roster scratch_admission_bytes', 'root frozen overlap launcher config')
    exact(config['schema'], OVERLAP_SCHEMA, 'overlap launcher schema'); exact(config['authority_pending'], False, 'root freeze pending')
    require(re.fullmatch(r'a[0-9]{4}', config['run_id']), 'frozen overlap runID'); exact(config['machine'], OVERLAP_MACHINE, 'root prospective machine freeze')
    helper = overlap_helper(); cfg = config['execution']
    fields(cfg, 'schema run_id authority qualification binary inputs resources', 'unchanged helper configuration')
    exact(cfg['schema'], helper.SCHEMA, 'helper config schema'); exact(cfg['authority'], helper.AUTHORITY, 'exact relative retained authority')
    exact(cfg['resources'], helper.RESOURCES, 'two-CPU build/one-CPU query fixed resources')
    exact(cfg['run_id'], 'boundary-overlap-paired100k-'+config['run_id'], 'same immutable scientific run')
    exact(set(config['code_sha256']), set(OVERLAP_CODE), 'complete existing launcher/helper import closure')
    exact(config['code_sha256']['scripts/run_cell_overlap_pair.py'], OVERLAP_HELPER_SHA, 'final independently repaired helper source')
    for name, digest in config['code_sha256'].items(): exact(local.identity(repo/name)['sha256'], digest, 'frozen launcher/helper source')
    paths = {str(OVERLAP_CONFIG), *OVERLAP_CODE, helper.AUTHORITY['path'], str(OVERLAP_ROOT/'remote-input-roster.json'), str(OVERLAP_ROOT/'native-qualification-transport.json')}
    candidate = overlap_qualification(config, repo, paths)
    authority = probe.ref(repo, helper.AUTHORITY); retained_terminal = probe.ref(repo, authority['terminal']); paths.add(authority['terminal']['path'])
    asset_roster = local.read_json(local.identity(repo/OVERLAP_ROOT/'remote-input-roster.json'))
    exact(asset_roster['bucket'], BUCKET, 'direct asset bucket'); exact(config['assets'], asset_roster['items'], 'exact root direct sixteen-asset roster')
    exact(len(config['assets']), 16, 'sixteen opaque direct bodies')
    native_roster = local.read_json(local.identity(repo/OVERLAP_ROOT/'native-qualification-transport.json'))
    exact(native_roster['bucket'], BUCKET, 'qualified native bucket'); exact(config['native_assets'], native_roster['items'], 'exact fourteen native bodies')
    exact({p['name'] for p in config['native_assets']}, set(OVERLAP_ASSURANCE), 'fourteen actual assurance bodies')
    terminal = local.read_json(dict(cfg['qualification']['terminal'], path=str(repo/OVERLAP_GATE/'aws-terminal.json')))
    launch = local.read_json(dict(cfg['qualification']['launch'], path=str(repo/OVERLAP_GATE/'aws-launch.json')))
    for p in config['native_assets']:
        fields(p, 'name key bytes sha256', 'qualified native remote body'); exact(body_pin(p), terminal['artifacts'][p['name']], 'terminal-bound native body')
        exact(p['key'], launch['prefix']+'/artifacts/'+p['name'], 'original qualified native object')
    fields(cfg['inputs'], ' '.join(probe.DATASETS), 'both scientific datasets'); fields(config['headers'], ' '.join(probe.DATASETS), 'both source-header rosters')
    seen = set()
    for asset in config['assets']:
        fields(asset, 'dataset role name key bytes sha256', 'root direct body descriptor')
        d, role, n = (asset[k] for k in ('dataset', 'role', 'name')); require(d in probe.DATASETS and role in ('original', 'layout', 'panel'), 'direct staged body role')
        require((d,role,n) not in seen, 'unique direct body role'); seen.add((d,role,n))
        exact(body_pin(asset), body_pin(overlap_input(config,d,role,n)), 'unchanged direct body identity')
    for d in probe.DATASETS:
        item = cfg['inputs'][d]; fields(item, 'layout original requests64 truth64', 'scientific dataset')
        fields(item['original'], ' '.join(helper.ORIGINALS), 'seven exact originals'); fields(item['layout'], ' '.join(helper.LAYOUT), 'capacity-v4 layout')
        fields(config['headers'][d], 'generation plane mean manifest.json', 'six original headers plus two retained manifests')
        header_root = ROOT.parent/'boundary-overlap'
        for n, pin in config['headers'][d].items():
            fields(pin, 'path bytes sha256', 'archived input header')
            if n != 'manifest.json':
                filename = d+'-'+n+('.bin' if n == 'mean' else '.json')
                exact(pin['path'], str(header_root/('original-'+n+'-inputs')/filename), 'exact recovered source header path')
            else:
                exact(pin['path'], str(Path(authority['terminal']['path']).parent/authority['datasets'][d][n]['terminal_path']), 'original retained manifest header path')
            paths.add(pin['path']); target = item['layout'][n] if n == 'manifest.json' else item['original'][n]
            exact(body_pin(pin), body_pin(target), 'exact source header original pin'); local.authenticate(dict(pin, path=str(repo/pin['path'])), 128 << 10)
        root = read_ref(repo, config['headers'][d]['manifest.json'], 128 << 10)
        exact(root['schema'], 'borsuk-hierarchical-cells-resident-v4', 'retained capacity-v4'); exact(root['rows'], 100000, 'FIRST100k'); exact(root['dimensions'], 768, 'D768')
        fields(root['input'], ' '.join(probe.BUILD_FIELDS), 'strict original build policy')
        for n in helper.ORIGINALS: exact(body_pin(item['original'][n]), body_pin(root['input'][n]), 'retained root-bound original SHA/bytes')
        for n,value in dict(schema='borsuk-hierarchical-cells-build-v2',cell_rows=512,sample_rows=256,max_depth=32,max_build_payload_bytes=64<<20,max_output_bytes=256<<20).items(): exact(root['input'][n],value,'unchanged original build policy')
        for category,names in (('original',helper.ORIGINALS),('layout',helper.LAYOUT),('panel',('requests64','truth64'))):
            for n in names:
                p = overlap_input(config,d,category,n); helper.descriptor(p, 512 << 20)
                suffix = Path(category)/n if category != 'panel' else Path(n)
                exact(p['path'], str(OVERLAP_WORKER/'screen/retained'/d/suffix), 'root frozen staged body path')
        for n in (*helper.LAYOUT,'requests64','truth64'):
            p = item['layout'][n] if n in helper.LAYOUT else item[n]; expected = authority['datasets'][d][n]
            exact(body_pin(p),body_pin(expected),'retained layout/panel authority'); exact(body_pin(expected),retained_terminal['artifacts'][expected['terminal_path']],'original terminal-bound retained body')
    expected_seen = {(d,role,n) for d in probe.DATASETS for role,names in (('original',('canonical','order','records','sq8')),('layout',('directories.bin','cells.bin')),('panel',('requests64','truth64'))) for n in names}
    exact(seen,expected_seen,'direct body role roster')
    terminal = local.read_json(dict(cfg['qualification']['terminal'],path=str(repo/OVERLAP_GATE/'aws-terminal.json')))
    exact(body_pin(config['canary_object']),terminal['artifacts']['source-qualification.json'],'small authenticated original proof GET')
    exact(config['canary_object'], {k:p[k] for p in config['native_assets'] if p['name']=='source-qualification.json' for k in ('key','bytes','sha256')}, 'one small source-bound proof GET')
    require(config['canary_object']['bytes'] <= 1 << 20,'canary small GET cap'); overlap_objects(config)
    paths = sorted(paths)
    for name in paths: publication.relative(name); repo_path(repo,name)
    local.integer(config['scratch_reserve_bytes'],256<<20,8<<30,'root whole-runtime reserve')
    roster = overlap_scratch_roster(config,None,paths,repo); exact(config['scratch_roster'],roster,'exact coexisting physical scratch admission')
    require(sum(p['max_bytes'] for p in roster) <= config['scratch_admission_bytes'] <= 8 << 30,'all staged physical scratch fits8GiB')
    proof = dict(config_path=str(OVERLAP_CONFIG), config_sha256=config_pin['sha256'], campaign_schema=OVERLAP_CANARY_SCHEMA if canary else OVERLAP_SCHEMA,
        code_identity_sha256=ids.sha(ids.encoded(config['code_sha256'])), refs_identity_sha256=ids.sha(ids.encoded({n:config[n] for n in ('execution','qualification_transport','assets','native_assets','headers','canary_object','machine')})),
        native_identity_sha256=ids.sha(ids.encoded(candidate)), source_file_count=403, source_archive_paths=paths,
        source_archive_paths_sha256=ids.sha(ids.encoded(paths)), artifact_roster_sha256=ids.sha(ids.encoded(PROBE_CANARY_ARTIFACTS if canary else OVERLAP_ARTIFACTS)),awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
    return config,proof,{}


def overlap_canary(config,evidence,client,calls,scratch,check,deadline):
    for n in ('numpy','pyarrow','boto3','botocore'): importlib.import_module(n)
    versions = {n:importlib.metadata.version(n) for n in (*FIXED['versions'],*SDK_VERSIONS)}
    exact(versions,dict(FIXED['versions'],**SDK_VERSIONS),'actual canary imports')
    require('IfNoneMatch' in client.meta.service_model.operation_model('PutObject').input_shape.members,'conditional SDK model')
    cli = subprocess.run([sys.executable,'-m',MODULE,'--fine-sq8-pair' if PROBE_SCHEMA == FINE_SCHEMA else '--cell-overlap-pair'],capture_output=True,text=True,timeout=min(30,max(.001,deadline-time.monotonic())))
    require(cli.returncode == 2 and 'INVALID:' in cli.stderr and 'CLI:' in cli.stderr and not cli.stdout,'actual usage CLI exit2')
    for p in overlap_objects(config):
        check(); response=publication.sdk_call(client,calls,'head_object',p['key'],BUCKET); exact(response['ContentLength'],p['bytes'],'HEAD presence/length only')
    p=config['canary_object'];check();publication.download(client,calls,BUCKET,p,scratch/'qualified-gate.log');check()
    return dict(schema=OVERLAP_CANARY_SCHEMA,status='GO',complete=True,scientific_performance_evidence=False,versions=versions,
        sdk_conditional_put_model=True,cli_exit_status=cli.returncode,cli_stderr=cli.stderr,sdk_calls=calls,authenticated_logs=[body_pin(p)],
        transport='real SDK; all thirty frozen HEADs and one authenticated small proof GET',ann_queries=0,native_processes=0,truth_or_panel_body_reads=0,dataset_payload_gets=0)


def overlap_execute(config, config_pin, repo, out, download, check, deadline):
    helper = overlap_helper(); repo, out = Path(repo), Path(out); cfg = config['execution']
    (out/'retained').mkdir()
    for asset in config['assets']:
        p = overlap_input(config, asset['dataset'], asset['role'], asset['name'])
        target = Path(p['path']); target.parent.mkdir(parents=True, exist_ok=True)
        download(asset, target)
    for asset in config['native_assets']:
        target = Path(cfg['qualification']['directory'])/asset['name']; target.parent.mkdir(parents=True, exist_ok=True)
        download(asset, target)
    for n, filename in dict(terminal='aws-terminal.json', launch='aws-launch.json', closeout='aws-closeout.json').items():
        pin = cfg['qualification'][n]
        helper.retain(dict(pin, path=str(repo/OVERLAP_GATE/filename)), Path(pin['path'])); check()
    for d in probe.DATASETS:
        for n, pin in config['headers'][d].items():
            target = overlap_input(config, d, 'layout' if n == 'manifest.json' else 'original', n)
            helper.retain(dict(pin, path=str(repo/pin['path'])), Path(target['path'])); check()
    require(deadline-time.monotonic() >= 5400, 'six fixed native budgets fit cumulative machine deadline')
    helper.qualify(cfg, repo)  # Strict remote original receipt/binary/allbody authentication before science.
    execution = local.write_json(out/'overlap-config.json', cfg)
    terminal = helper.execute(execution['path'], execution['sha256'], repo, out/'overlap'); check()
    receipt = dict(schema=OVERLAP_SCHEMA+'-execution', config=config_pin, complete=terminal['complete'], status=terminal['status'],
        overlap_terminal=terminal, truth_opened=terminal['complete'], scientific_qualification=False, physical_s3_measured=False)
    local.write_json(out/'native-execution-receipt.json', receipt)
    require(terminal['execution_exit_code'] == 0, 'scientific helper execution INVALID')
    return receipt


def overlap_verify_pair(output, *, repo=None, config=None, **_):
    """Replay qualified helper with collected retained bodies, preserving raw configs."""
    helper = overlap_helper(); output = Path(output); root = output/'overlap'
    receipt = local.read_json(local.identity(root/'execution-receipt.json'), 8 << 20)
    runtime = receipt['output']; real_qualify = helper.qualify
    def qualify_relocated(cfg, repo):
        rebound = copy.deepcopy(cfg)
        q = rebound['qualification']; q['directory'] = str(root/'authority/native')
        q['proof'] = local.identity(root/'authority/native/source-qualification.json')
        for n in ('terminal', 'launch', 'closeout'): q[n] = local.identity(root/'authority'/(n+'.json'))
        rebound['binary'] = helper.witness.physical(root, runtime, receipt['binary'])
        for d in probe.DATASETS:
            for category in ('layout', 'original'):
                rebound['inputs'][d][category] = {n:helper.witness.physical(root, runtime, p) for n,p in receipt['inputs'][d][category].items()}
            rebound['inputs'][d]['requests64'] = helper.witness.physical(root, runtime, receipt['inputs'][d]['requests64'])
        return real_qualify(rebound, repo)
    pin = local.identity(output/'overlap-config.json')
    exact(local.read_json(pin), config['execution'], 'unchanged exact scientific config')
    with patch.object(helper, 'qualify', side_effect=qualify_relocated):
        return helper.replay(pin['path'], pin['sha256'], repo, root)


def overlap_verify_execution(output,config,terminal,evidence):
    out=Path(output);receipt=local.read_json(local.identity(out/'execution-receipt.json'),8<<20)
    native=local.read_json(dict(body_pin(receipt['native_execution']),path=str(out/'native-execution-receipt.json')),8<<20)
    exact({k:v for k,v in receipt.items() if k not in ('native_execution','worker_closure')},native,'final/native execution binding')
    exact(native['complete'],True,'both datasets closed');exact(native['overlap_terminal'],terminal,'original helper terminal retained')
    exact(native['status'],terminal['status'],'native PASS/FAIL preserved')
    exact(body_pin(native['config']),body_pin(local.identity(out/'config.json')),'same frozen launcher configuration')
    return native


def overlap_profile():
    """Use the existing SDK/ACK/termination/collection lifecycle and guards."""
    from contextlib import ExitStack
    module=sys.modules[__name__];stack=ExitStack();old_userdata,old_replay=probe_user_data,probe_replay
    def userdata(*args,**kwargs):
        canary=kwargs.get('canary',False)
        if not canary:
            cfg=local.read_json(local.identity(OVERLAP_CONFIG),512<<10)
            exact(args[3],OVERLAP_PREFIX+cfg['run_id'],'one unchanged scientific run')
        # The shared terminal reads ARTIFACT_NAMES. Import the frozen large
        # roster remotely so userdata stays below EC2's 16 KiB bound.
        with patch.object(module,'PROBE_ARTIFACTS',('test-resources.txt','run-closed.log')):
            body=old_userdata(*args,**kwargs)
        body=body.replace('--global-leaf-probe','--cell-overlap-pair').replace('/mnt/hierarchical-global-leaf-probe',str(OVERLAP_WORKER))
        if not canary:
            body=body.replace('MemoryMax=2G','MemoryMax=8G')
            body=body.replace('else test -s "$name"; fi','else test -f "$name"; fi')
        # Resolve only after source and the worker interpreter are installed.
        # A failed assignment retains the bootstrap fallback for the EXIT trap.
        roster='overlap_artifact_names=$(PYTHONPATH="$root/probe-repo" "$root/venv/bin/python" -c "from '+MODULE+' import OVERLAP_ARTIFACTS; print(\' \'.join(OVERLAP_ARTIFACTS))")\ntest -n "$overlap_artifact_names"\n'
        if not canary:
            roster+='export ARTIFACT_NAMES="$overlap_artifact_names"\n'
        phase='phase=infrastructure-canary' if canary else 'phase=paired-nomination'
        require(body.count(phase)==1,'one post-install overlap phase')
        body=body.replace(phase,roster+phase)
        subprocess.run(['bash','-n'],input=body,text=True,check=True)
        require(len(body.encode())<16384 and '--cell-overlap-pair' in body,'bounded shared overlap bootstrap')
        return body
    def replay_overlap(out,*,canary=False,repo=None):
        result=old_replay(out,canary=canary,repo=repo)
        if not canary and result['executed']:
            receipt=local.read_json(local.identity(Path(out)/'screen/summary.json'),8<<20)
            result.update(status=receipt['status'],truth_opened=True,complete_query_latency='complete-query-unmeasured')
        return result
    helper=SimpleNamespace(**vars(probe));helper.ROLE_NAMES=('qualified',)
    helper.execute,helper.verify_pair,helper.verify_execution=overlap_execute,overlap_verify_pair,overlap_verify_execution
    stack.enter_context(patch.multiple(module,PROBE_ROOT=OVERLAP_ROOT,PROBE_CONFIG=OVERLAP_CONFIG,PROBE_SCHEMA=OVERLAP_SCHEMA,
        PROBE_CANARY_SCHEMA=OVERLAP_CANARY_SCHEMA,PROBE_PREFIX=OVERLAP_PREFIX,PROBE_CANARY_PREFIX=OVERLAP_PREFIX+'canary-',
        PROBE_CODE=OVERLAP_CODE,PROBE_ARTIFACTS=OVERLAP_ARTIFACTS,probe=helper,probe_qualify=overlap_qualify,probe_canary=overlap_canary,
        probe_objects=overlap_objects,probe_user_data=userdata,probe_replay=replay_overlap,
        WALL=OVERLAP_MACHINE['wall_seconds'],COMPUTE_CAP=OVERLAP_MACHINE['compute_cap_usd'],MEMORY=8<<30,SCRATCH=8<<30))
    return stack


def overlap_cli(args):
    require(args,'CLI: --cell-overlap-pair aNNNN | --canary aNNNN | --stage[-canary] REPO OUTPUT ROOT | --replay[-canary] OUTPUT | --preflight | --self-check')
    if args==['--self-check']:overlap_self_check();return
    with overlap_profile():
        if args==['--preflight']:print(json.dumps(overlap_qualify()[1]));return
        probe_cli(args)


def overlap_self_check():
    """Synthetic transport/lifetime only: no network, native algorithms or GT."""
    from contextlib import ExitStack
    import gzip
    import shlex
    import botocore.session
    module = sys.modules[__name__]
    def rejects(fn):
        try: fn()
        except (ValueError, AssertionError, OSError, KeyError): return
        raise AssertionError('negative synthetic fixture admitted')
    with tempfile.TemporaryDirectory(prefix='overlap-launcher-check-') as tmp, ExitStack() as stack:
        root = Path(tmp); repo = root/'probe-repo'; repo.mkdir()
        raw = b'original evidence\r\n  spaces\t\n'; archived = root/'raw.gz'
        archived.write_bytes(gzip.compress(raw,mtime=0)); compressed = local.identity(archived)
        expected = dict(bytes=len(raw),sha256=local.sha(raw)); restored = overlap_gzip(compressed,root/'restored.log',expected,1024)
        exact(Path(restored['path']).read_bytes(),raw,'immutable CR and whitespace preserved')
        rejects(lambda:overlap_gzip(compressed,root/'wrong.log',dict(expected,sha256='0'*64),1024))
        rejects(lambda:overlap_gzip(compressed,root/'overcap.log',expected,1))
        archived.write_bytes(archived.read_bytes()+b'tamper');rejects(lambda:overlap_gzip(compressed,root/'tamper.log',expected,1024))
        # Bootstrap uses the fixed roster once, permits authenticated empty logs,
        # drains the same slice and keeps the shared terminal-last publication.
        cfg_path=root/'config.json';local.write_json(cfg_path,dict(run_id='a0001'))
        stack.enter_context(patch.object(module,'OVERLAP_CONFIG',cfg_path))
        proof={n:'0'*64 for n in TERMINAL_IDENTITIES};proof.update(config_path=str(cfg_path),source_file_count=403,
            campaign_schema=OVERLAP_SCHEMA,awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
        with overlap_profile():
            science=probe_user_data('a'*40,'b'*64,'source/mock',OVERLAP_PREFIX+'a0001',proof)
            canary=probe_user_data('a'*40,'b'*64,'source/mock',OVERLAP_PREFIX+'canary-a0001',dict(proof,campaign_schema=OVERLAP_CANARY_SCHEMA),canary=True)
        for value in ('MemoryMax=8G','CPUQuota=200%','MemorySwapMax=0','6500','--cell-overlap-pair --stage ','OVERLAP_ARTIFACTS','else test -f "$name"','sync -f terminal.json'):
            require(value in science,'scientific bootstrap contract: '+value)
        require('MemoryMax=256M' in canary and '--cell-overlap-pair --stage-canary' in canary,'separate metadata-only canary')
        exact(len(OVERLAP_ARTIFACTS),len(set(OVERLAP_ARTIFACTS)),'unique retained original artifact roster')
        require(not any('writer' in n for n in OVERLAP_ARTIFACTS),'no obsolete writer recovery artifacts')
        # A source-only archive must import both the launcher and the frozen
        # helper, without falling through to the caller's checkout.
        closure=root/'closure';closure.mkdir()
        for name in OVERLAP_CODE:
            source=Path(name)
            if name=='scripts/run_cell_overlap_pair.py' and not source.exists():
                source=Path(os.environ.get('BORSUK_OVERLAP_HELPER_FIXTURE',''))
                require(source.is_file() and local.identity(source)['sha256']==OVERLAP_HELPER_SHA,'exact helper Git-blob fixture required')
            target=closure/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,target)
        subprocess.run([sys.executable,'-B','-c','from scripts import run_cell_overlap_pair; from scripts import launch_hierarchical_cells_100k_spot'],cwd=closure,
            env=dict(os.environ,PYTHONPATH=str(closure)),timeout=10,check=True)
        # Execute the generated roster block, including its failure semantics.
        worker=root/'worker';worker.mkdir();(worker/'venv/bin').mkdir(parents=True)
        (worker/'probe-repo').symlink_to(closure.resolve(),target_is_directory=True)
        interpreter=worker/'venv/bin/python';interpreter.symlink_to(sys.executable)
        for shell in (science,canary):
            begin=shell.index('overlap_artifact_names=$(')
            end=shell.index('phase=',begin)
            require(shell.index('-m pip install')<begin,'roster follows SDK installation')
            block=shell[begin:end]
            script='set -eu\nroot='+shlex.quote(str(worker))+'\nexport ARTIFACT_NAMES=fallback\n'+block+'printf "%s" "$overlap_artifact_names"\n'
            result=subprocess.run(['bash','-c',script],capture_output=True,text=True,timeout=10,check=True)
            exact(result.stdout.split(),list(OVERLAP_ARTIFACTS),'actual generated roster import')
        interpreter.unlink();interpreter.write_text('#!/bin/sh\nexit 7\n');interpreter.chmod(0o755)
        script='set -eu\nroot='+shlex.quote(str(worker))+'\nexport ARTIFACT_NAMES=fallback\ntrap \'printf "%s" "$ARTIFACT_NAMES"\' EXIT\n'+science[science.index('overlap_artifact_names=$('):science.index('phase=paired-nomination')]
        failed=subprocess.run(['bash','-c',script],capture_output=True,text=True,timeout=10)
        exact(failed.returncode,7,'failed roster import stops bootstrap')
        exact(failed.stdout,'fallback','failure publication roster survives')
        versions=dict(FIXED['versions'],**SDK_VERSIONS);real_import=importlib.import_module;real_version=importlib.metadata.version
        def imports(name,*a,**kw):
            return SimpleNamespace(__version__=versions[name]) if name in ('numpy','pyarrow') else real_import(name,*a,**kw)
        def version(name):return versions[name] if name in ('numpy','pyarrow') else real_version(name)
        stack.enter_context(patch.object(importlib,'import_module',side_effect=imports))
        stack.enter_context(patch.object(importlib.metadata,'version',side_effect=version))
        config=dict(schema=OVERLAP_SCHEMA,assets=[dict(key='synthetic/input/'+str(i),bytes=1,sha256=local.sha(b'x')) for i in range(16)],
            native_assets=[dict(name=str(i),key='synthetic/native/'+str(i),bytes=1,sha256=local.sha(b'x')) for i in range(14)],
            canary_object=dict(key='synthetic/native/0',bytes=1,sha256=local.sha(b'x')))
        objects=overlap_objects(config);closed=[]
        class Client:
            meta=SimpleNamespace(service_model=botocore.session.Session().get_service_model('s3'))
            def head_object(self,**args):return dict(ContentLength=next(p['bytes'] for p in objects if p['key']==args['Key']))
            def get_object(self,**args):
                exact(args['Key'],config['canary_object']['key'],'canary cannot fetch dataset/panel/GT/native binary')
                return dict(ContentLength=1,Body=io.BytesIO(b'x'))
            def close(self):closed.append(True)
        for name in ('native_stage','restore_writer_inputs','execute'):
            stack.enter_context(patch.object(probe,name,side_effect=AssertionError('forbidden original scientific path: '+name)))
        calls=[];scratch=root/'canary';scratch.mkdir();client=Client()
        receipt=overlap_canary(config,{},client,calls,scratch,lambda:None,time.monotonic()+30);client.close()
        exact(len(calls),31,'HEADall30 then one proof GET');exact(receipt['authenticated_logs'],[body_pin(config['canary_object'])],'one proof body authenticated')
        require(closed and receipt['native_processes']==receipt['truth_or_panel_body_reads']==0,'canary closure/no native/no GT')
        # Exercise the real shared stage, closure, collection and canary replay
        # against mocked SDK/kernel counters; no native capability is present.
        with ExitStack() as lifecycle_stack:
            cfg_path.write_bytes(local.canonical(config))
            closed_proof=dict(proof,config_sha256=local.sha(cfg_path.read_bytes()),campaign_schema=OVERLAP_CANARY_SCHEMA,
                source_archive_paths=['scripts/launch_hierarchical_cells_100k_spot.py'])
            closed_proof['source_archive_paths_sha256']=ids.sha(ids.encoded(closed_proof['source_archive_paths']))
            lifecycle_stack.enter_context(patch.object(module,'overlap_qualify',return_value=(config,closed_proof,{})))
            lifecycle_stack.enter_context(patch.object(publication,'sdk_client',return_value=client))
            group=root/'kernel/borsuk-global-leaf-mock.slice/controller';group.mkdir(parents=True)
            (group/'cpu.max').write_text('100000 100000');(group/'pids.max').write_text('512')
            counters={'path':str(group),'memory.max':'268435456','memory.peak':'1024','memory.swap.max':'0','memory.swap.peak':'0',
                'memory.events':'oom 0\noom_kill 0\noom_group_kill 0','cpu_affinity':[0],'cpu.max':'100000 100000','pids.max':'512'}
            lifecycle_stack.enter_context(patch.object(local,'resource_snapshot',return_value=counters))
            lifecycle_stack.enter_context(patch.object(probe,'cgroup_snapshot',return_value=dict(counters,path=str(group.parent))))
            lifecycle_stack.enter_context(patch.dict(os.environ,dict(BORSUK_GLOBAL_LEAF_SLICE=group.parent.name,
                BORSUK_HIERARCHICAL_CONFIG_SHA256=closed_proof['config_sha256'],BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256=closed_proof['source_archive_paths_sha256'],
                BORSUK_HIERARCHICAL_DEADLINE_EPOCH=str(int(time.time())+30),BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=str(shutil.disk_usage(root).used))))
            closed_out=root/'closed';closed_out.mkdir()
            with overlap_profile():
                probe_stage(repo,closed_out/'screen',root,canary=True)
                rejects(lambda:probe_stage(repo,closed_out/'screen',root,canary=True))
                for n in ('test-resources.txt','run-closed.log'):probe.copy_bytes(closed_out/n,b'synthetic closure\n')
                nodes={'worker':dict(instance_id='i-synthetic')};identity=dict(source_commit='a'*40,source_archive_sha256='b'*64)
                launch=dict(identity,instance_id='i-synthetic',nodes=nodes,prefix=OVERLAP_PREFIX+'canary-a0001')
                local.write_json(closed_out/'aws-launch.json',launch);local.write_json(closed_out/'aws-closeout.json',dict(state='terminated',nodes=nodes))
                local.write_json(closed_out/'aws-reservation.json',dict(identity,schema=OVERLAP_CANARY_SCHEMA,qualification=closed_proof,
                    wall_seconds=480,compute_cap_usd=.12,ebs_s3_allowance_usd=.05))
                terminal=dict(identity,**closed_proof,instance_id='i-synthetic',schema=OVERLAP_CANARY_SCHEMA,phase='complete',status='complete',
                    exit_code=0,original_exit_code=0,artifacts={n:body_pin(local.identity(closed_out/n)) for n in PROBE_CANARY_ARTIFACTS})
                local.write_json(closed_out/'aws-terminal.json',terminal)
                require(probe_replay(closed_out,canary=True,repo=repo)['executed'],'actual collected canary replay')
                class Collected:
                    def get_object(self,**args):
                        key=args['Key'];name='aws-terminal.json' if key.endswith('/terminal.json') else key.split('/artifacts/',1)[1]
                        return dict(Body=io.BytesIO((closed_out/name).read_bytes()))
                collected=root/'collected';collected.mkdir()
                for n in ('aws-launch.json','aws-closeout.json','aws-reservation.json'):shutil.copyfile(closed_out/n,collected/n)
                probe_collect(Collected(),launch['prefix'],collected,'i-synthetic','a'*40,'b'*64,canary=True)
                require(probe_replay(collected,canary=True,repo=repo)['executed'],'actual SDK collection plus gzip sidecars replay')
                original_close=(collected/'aws-closeout.json').read_bytes()
                (collected/'aws-closeout.json').write_bytes(local.canonical(dict(state='terminated',nodes={'worker':dict(instance_id='i-wrong')})))
                rejects(lambda:probe_replay(collected,canary=True,repo=repo));(collected/'aws-closeout.json').write_bytes(original_close)
                with patch.object(client,'head_object',return_value=dict(ContentLength=2)):
                    rejects(lambda:probe_stage(repo,root/'failed-screen',root,canary=True))
                failure=local.read_json(local.identity(root/'failed-screen/summary.json'))
                exact(failure['status'],'INVALID','failed HEAD stage stays execution INVALID')
                cleanup=local.read_json(local.identity(root/'failed-screen/cleanup.json'))
                require(cleanup['sdk_client_closed'] and cleanup['scratch_removed'] and cleanup['monitor_stopped'],'actual failure cleanup')
        # Shared lifecycle fixtures exercise launch, ACK refusal, timeout,
        # collection failure, and terminate+wait of the SAME instance IDs.
        shared,_=ids.lifecycle();shared.self_check(lifecycle_only=True)
        # Exercise direct staging and handoff through a real tiny Python child.
        cfg=dict(inputs={},qualification=dict(directory=str(root/'qualified')))
        headers={};assets=[];bodies={};folder=root/'inputs'
        originals='generation plane canonical order records mean sq8'.split()
        layout=('manifest.json','directories.bin','cells.bin')
        for d in ('relaion','cohere'):
            cfg['inputs'][d]=dict(original={},layout={});headers[d]={}
            for role,names in (('original',originals),('layout',layout),('panel',('requests64','truth64'))):
                for n in names:
                    raw=b'opaque synthetic '+d.encode()+b' '+n.encode();pin=dict(bytes=len(raw),sha256=local.sha(raw),path=str(folder/d/role/n))
                    if role=='panel':cfg['inputs'][d][n]=pin
                    else:cfg['inputs'][d][role][n]=pin
                    if n in ('generation','plane','mean','manifest.json'):
                        relative=d+'-'+n;probe.copy_bytes(repo/relative,raw);headers[d][n]=dict(body_pin(pin),path=relative)
                    else:
                        key=d+'/'+role+'/'+n;assets.append(dict(dataset=d,role=role,name=n,key=key,**body_pin(pin)));bodies[key]=raw
        for n,filename in dict(terminal='aws-terminal.json',launch='aws-launch.json',closeout='aws-closeout.json').items():
            raw=b'opaque control';probe.copy_bytes(repo/'qualified'/filename,raw);cfg['qualification'][n]=dict(bytes=len(raw),sha256=local.sha(raw),path=str(root/'qualified'/filename))
        events=[];native=[dict(name='source-qualification.json',key='proof',bytes=1,sha256=local.sha(b'p'))];bodies['proof']=b'p'
        def download(pin,path):
            events.append('get:'+pin['key']);publication.transfer(io.BytesIO(bodies[pin['key']]),body_pin(pin),path)
        from scripts import run_source_witness_paired_coverage as witness
        def qualified(config,repo):
            events.append('qualified')
            for d in ('relaion','cohere'):
                for pin in (*config['inputs'][d]['original'].values(),*config['inputs'][d]['layout'].values(),config['inputs'][d]['requests64']):local.authenticate(pin,4096)
        def execute(path,sha,repo,out):
            exact(events[-1],'qualified','all staged identity checks before any native handoff')
            result=subprocess.run([sys.executable,'-c','import json; print(json.dumps({"status":"PASS","complete":True,"execution_exit_code":0}))'],capture_output=True,text=True,check=True,timeout=5)
            events.append('six-call-helper');return json.loads(result.stdout)
        fake=SimpleNamespace(retain=witness.retain,qualify=qualified,execute=execute)
        stack.enter_context(patch.object(module,'overlap_helper',return_value=fake));stack.enter_context(patch.object(module,'OVERLAP_GATE',Path('qualified')))
        config=dict(execution=cfg,assets=assets,native_assets=native,headers=headers)
        out=root/'stage';out.mkdir()
        result=overlap_execute(config,dict(bytes=1,sha256='0'*64,path='synthetic'),repo,out,download,lambda:None,time.monotonic()+5500)
        exact(result['status'],'PASS','actual direct staging handoff completed');exact(events[-1],'six-call-helper','only helper owns scientific calls')
        require(len([e for e in events if e.startswith('get:')])==17,'closed opaque GET roster, no original archive/source/writer')
        # Metadata preflight must reject pending/source/parity drift while every
        # future dataset and GT body remains absent and inaccessible.
        def fixture_json(path,value):
            path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.unlink(missing_ok=True);return local.write_json(path,value)
        with ExitStack() as metadata_stack:
            config_path=OVERLAP_ROOT/'config.json';metadata_stack.enter_context(patch.object(module,'OVERLAP_CONFIG',config_path))
            metadata_stack.enter_context(patch.object(module,'OVERLAP_CODE',('scripts/run_cell_overlap_pair.py',)))
            metadata_stack.enter_context(patch.object(module,'overlap_qualification',return_value={}))
            future=root/'never-hydrated';metadata_stack.enter_context(patch.object(module,'OVERLAP_WORKER',future))
            helper_source=Path('scripts/run_cell_overlap_pair.py')
            if not helper_source.exists():helper_source=Path(os.environ['BORSUK_OVERLAP_HELPER_FIXTURE'])
            probe.copy_bytes(repo/'scripts/run_cell_overlap_pair.py',helper_source.read_bytes())
            metadata_stack.enter_context(patch.object(module,'OVERLAP_HELPER_SHA',local.identity(helper_source)['sha256']))
            from importlib import util as import_util
            spec=import_util.spec_from_file_location('synthetic_overlap_helper',helper_source);descriptor_helper=import_util.module_from_spec(spec);spec.loader.exec_module(descriptor_helper)
            fake.SCHEMA=descriptor_helper.SCHEMA;fake.ORIGINALS=originals;fake.LAYOUT=layout;fake.RESOURCES=descriptor_helper.RESOURCES;fake.descriptor=descriptor_helper.descriptor
            cfg=copy.deepcopy(cfg);cfg.update(schema=fake.SCHEMA,run_id='boundary-overlap-paired100k-a0001',resources=copy.deepcopy(fake.RESOURCES),binary=dict(path=str(future/'binary'),bytes=1,sha256=local.sha(b'b')))
            authority=dict(datasets={});retained=dict(artifacts={});headers={};assets=[]
            for d in ('relaion','cohere'):
                headers[d]={};authority['datasets'][d]={}
                for role,names in (('original',originals),('layout',layout),('panel',('requests64','truth64'))):
                    for n in names:
                        item=cfg['inputs'][d][n] if role=='panel' else cfg['inputs'][d][role][n]
                        item['path']=str(future/'screen/retained'/d/(Path(n) if role=='panel' else Path(role)/n))
                        if role=='original' and n in ('generation','plane','mean'):
                            pin=headers[d][n]=dict(body_pin(item),path=str(ROOT.parent/'boundary-overlap'/('original-'+n+'-inputs')/(d+'-'+n+('.bin' if n=='mean' else '.json'))))
                            probe.copy_bytes(repo/pin['path'],(repo/(d+'-'+n)).read_bytes())
                        elif n!='manifest.json':assets.append(dict(dataset=d,role=role,name=n,key=d+'/'+role+'/'+n,**body_pin(item)))
                build=dict(cfg['inputs'][d]['original'],schema='borsuk-hierarchical-cells-build-v2',cell_rows=512,sample_rows=256,max_depth=32,max_build_payload_bytes=64<<20,max_output_bytes=256<<20)
                relative='retained/screen/'+d+'/manifest.json';pin=fixture_json(repo/relative,dict(schema='borsuk-hierarchical-cells-resident-v4',rows=100000,dimensions=768,input=build))
                headers[d]['manifest.json']=dict(pin,path=relative);cfg['inputs'][d]['layout']['manifest.json'].update(body_pin(pin))
                for n in (*layout,'requests64','truth64'):
                    item=cfg['inputs'][d]['layout'][n] if n in layout else cfg['inputs'][d][n];terminal_path=d+'/manifest.json' if n=='manifest.json' else d+'/'+n
                    authority['datasets'][d][n]=dict(body_pin(item),terminal_path=terminal_path);retained['artifacts'][terminal_path]=body_pin(item)
            terminal_pin=fixture_json(repo/'retained/aws-terminal.json',retained);authority['terminal']=dict(terminal_pin,path='retained/aws-terminal.json')
            # Adjust the archived manifest path to exactly parent/terminal_path.
            for d in ('relaion','cohere'):
                old=headers[d]['manifest.json'];new='retained/'+authority['datasets'][d]['manifest.json']['terminal_path']
                probe.copy_bytes(repo/new,(repo/old['path']).read_bytes());headers[d]['manifest.json']['path']=new
            auth_pin=fixture_json(repo/'authority.json',authority);fake.AUTHORITY=dict(auth_pin,path='authority.json');cfg['authority']=fake.AUTHORITY
            native=[dict(name=n,key='native/artifacts/'+n,bytes=1,sha256=local.sha(b'p')) for n in OVERLAP_ASSURANCE]
            cfg['qualification']['terminal'].update(body_pin(fixture_json(repo/'qualified/aws-terminal.json',dict(artifacts={p['name']:body_pin(p) for p in native}))))
            cfg['qualification']['launch'].update(body_pin(fixture_json(repo/'qualified/aws-launch.json',dict(prefix='native'))))
            fixture_json(repo/OVERLAP_ROOT/'remote-input-roster.json',dict(bucket=BUCKET,items=assets))
            fixture_json(repo/OVERLAP_ROOT/'native-qualification-transport.json',dict(bucket=BUCKET,items=native))
            config=dict(schema=OVERLAP_SCHEMA,authority_pending=False,run_id='a0001',code_sha256={'scripts/run_cell_overlap_pair.py':OVERLAP_HELPER_SHA},execution=cfg,
                qualification_transport={},assets=assets,native_assets=native,headers=headers,canary_object={k:p[k] for p in native if p['name']=='source-qualification.json' for k in ('key','bytes','sha256')},machine=copy.deepcopy(OVERLAP_MACHINE),scratch_reserve_bytes=256<<20,scratch_roster=[],scratch_admission_bytes=8<<30)
            source_paths=sorted({str(config_path),'scripts/run_cell_overlap_pair.py',fake.AUTHORITY['path'],str(OVERLAP_ROOT/'remote-input-roster.json'),str(OVERLAP_ROOT/'native-qualification-transport.json'),authority['terminal']['path'],*(p['path'] for v in headers.values() for p in v.values())})
            config['scratch_roster']=overlap_scratch_roster(config,None,source_paths,repo)
            before_open=positive.open_input
            def metadata_only(path):
                require(not Path(path).is_relative_to(future),'local preflight attempted dataset/GT hydration');return before_open(path)
            metadata_stack.enter_context(patch.object(positive,'open_input',side_effect=metadata_only))
            def preflight(value):
                fixture_json(repo/config_path,value)
                result=overlap_qualify(repo,canary=True)
                exact(result[1]['config_sha256'], local.sha((repo/config_path).read_bytes()), 'proof binds actual config rather than last header')
                return result
            preflight(config)
            for label,mutate in [('pending',lambda c:c.update(authority_pending=True)),('source',lambda c:c['code_sha256'].update({'scripts/run_cell_overlap_pair.py':'0'*64})),
                ('original-parity',lambda c:c['execution']['inputs']['relaion']['original']['canonical'].update(sha256='0'*64)),
                ('request-parity',lambda c:c['execution']['inputs']['cohere']['requests64'].update(sha256='0'*64)),
                ('unknown',lambda c:c.update(roles={})),('scratch',lambda c:c.update(scratch_admission_bytes=1)),('machine',lambda c:c['machine'].update(wall_seconds=6501))]:
                altered=copy.deepcopy(config);mutate(altered);rejects(lambda:preflight(altered))
            require(not future.exists(),'metadata preflight never created or hydrated future data paths')
        # Scratch/deadline checks and occupied output fail without cloud calls.
        rejects(lambda:probe_resource_check(root,0,1,time.monotonic()+10,[],dict(scratch_bytes=2),scan=False))
        rejects(lambda:probe_resource_check(root,0,1,time.monotonic()-1,[],dict(scratch_bytes=0),scan=False))
        rejects(lambda:probe_stage(repo,out,root,canary=True))
    print('PASS synthetic launcher: gzip exact CR/whitespace/tamper/cap; import closure; 30 HEAD/one proof GET and actual CLI; actual stage/cleanup/collection/gzip-sidecars/canary replay; SAME-ID ACK/terminate/wait/failures; opaque direct staging then tiny Python helper handoff; pending/source/original/request parity refusals without body hydration; scratch/deadline/nooverwrite. No network, native algorithms, real data, or GT decoding.')


# Fine mode only adapts the existing direct-body transport and owned lifecycle.
FINE_ROOT = ROOT.parent.parent/'fine-sq8-groups/paired100k'
FINE_CONFIG = FINE_ROOT/'config-scratch-v2.json'
FINE_GATE = FINE_ROOT.parent/'implementation-gates/a0001'
FINE_SCHEMA = 'borsuk-fine-sq8-paired100k-spot-v1'
FINE_CANARY_SCHEMA = 'borsuk-fine-sq8-infrastructure-canary-v1'
FINE_PREFIX = 'research/hierarchical-cells/20261005/fine-sq8-paired100k-'
FINE_WORKER = Path('/mnt/hierarchical-100k')
FINE_MACHINE = dict(wall_seconds=5000, compute_cap_usd=.75)
FINE_DIRECT_ROSTER = OVERLAP_ROOT/'remote-input-roster.json'
with overlap_controller.execution_mode(fine_sq8=True):
    FINE_ASSURANCE = overlap_controller.ARTIFACTS
    FINE_CODE = tuple(sorted(set((*OVERLAP_CODE, *overlap_controller.CODE))))
FINE_ORDER = ('relaion-build-fine', 'cohere-build-fine', 'paired-fine')
FINE_FILES = ('manifest.json', 'pq.bin', 'graph.bin', 'records.bin', 'groups.bin', 'order.bin')
FINE_OUTPUTS = (*OVERLAP_OUTPUTS[:10], 'fine-config.json', 'fine/config.json', 'fine/terminal.json', 'fine/execution-receipt.json',
    'fine/authority/current-source.json', 'fine/authority/retained-input-authority.json',
    *(f'fine/authority/{n}.json' for n in ('terminal', 'launch', 'closeout')), *('fine/authority/native/'+n for n in FINE_ASSURANCE),
    'fine/binary/hierarchical_semantic_cells',
    *(f'fine/inputs/{d}/{c}/{n}' for d in probe.DATASETS for c,names in (('layout', ('manifest.json', 'directories.bin', 'cells.bin')),
        ('original', 'generation plane canonical order records mean sq8'.split())) for n in names),
    *(f'fine/requests/{d}/requests64' for d in probe.DATASETS),
    *(f'fine/layouts/{d}/{n}' for d in probe.DATASETS for n in FINE_FILES), *(f'fine/layouts/{d}.build.jsonl' for d in probe.DATASETS),
    *(f'fine/measurement/{n}{s}' for n in FINE_ORDER for s in ('-config.json', '-stage.json', '-stage-receipt.json', '-closure.json', '.log', '-unit.log')),
    'fine/measurement/paired-fine.jsonl', 'fine/measurement/paired-fine.fine-seal.json',
    *(f'retained/{d}/layout/{n}' for d in probe.DATASETS for n in ('manifest.json', 'directories.bin', 'cells.bin')),
    *(f'retained/{d}/{n}' for d in probe.DATASETS for n in ('requests64', 'truth64')),
    *(f'measurement/{d}-generation/{n}' for d in probe.DATASETS for n in ('manifest.json', 'canonical.bin', 'plane/manifest.json', 'plane/records.bin', 'plane/mean.bin')),
    *(f'scratch/{d}/{n}' for d in probe.DATASETS for n in ('order', 'sq8')),
    *('retained/native-qualification/'+n for n in (*FINE_ASSURANCE, 'aws-terminal.json', 'aws-launch.json', 'aws-closeout.json')))
FINE_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in FINE_OUTPUTS))


def fine_scratch_roster(config, paths, repo):
    helper = overlap_helper(); incoming = config['execution']['inputs']
    originals = sum(incoming[d][c][n]['bytes'] for d in probe.DATASETS for c,names in (('original', helper.ORIGINALS), ('layout', helper.LAYOUT)) for n in names)
    terminal = local.read_json(dict(config['execution']['qualification']['terminal'], path=str(Path(repo)/FINE_GATE/'aws-terminal.json')), 8 << 20)
    assurance = sum(p['bytes'] for p in terminal['artifacts'].values())
    return [dict(name='archive-and-controller', max_bytes=2*sum((512 << 10) if p == str(FINE_CONFIG) else repo_path(repo,p).stat().st_size for p in paths)),
        dict(name='venv-cli-bootstrap-reserve', max_bytes=config['scratch_reserve_bytes']),
        dict(name='direct-staged-originals-layouts-panels', max_bytes=sum(p['bytes'] for p in overlap_objects(config))+sum(p['bytes'] for v in config['headers'].values() for p in v.values())),
        dict(name='restored-retained-native14-and-executable', max_bytes=3*assurance),
        dict(name='helper-retained-originals-and-layouts', max_bytes=originals),
        dict(name='two-fine-layouts-and-one-active-staging', max_bytes=3*helper.fine_layout_bound()),
        dict(name='native-both-panel-output', max_bytes=128 << 20), dict(name='three-native-logs-specs-seal-receipts', max_bytes=3*(34 << 20)),
        dict(name='retained-request-panels', max_bytes=sum(incoming[d]['requests64']['bytes'] for d in probe.DATASETS))]


def fine_scratch_admission(config):
    # Reserve is additional positive filesystem growth: outside-root bootstrap
    # writes, block/metadata overhead and deleted temporary files. It is charged
    # within the same cap, even when the worker's logical files are sparse.
    reserve = config['scratch_filesystem_reserve_bytes']
    local.integer(reserve, 256 << 20, 8 << 30, 'outside-root/bootstrap filesystem reserve')
    local.integer(config['scratch_admission_bytes'], 1, 8 << 30, 'fine scratch admission cap')
    directory = sum(p['max_bytes'] for p in config['scratch_roster'])
    charge = dict(directory_bytes=directory, filesystem_growth_bytes=directory+reserve,
                  extra_root_bytes=0, total_bytes=2*directory+reserve)
    require(charge['total_bytes'] <= config['scratch_admission_bytes'] <= 8 << 30,
            'whole fine scratch directory+filesystem-growth exceeds admission/8GiB')
    return charge


def fine_qualify(base=Path('.'), *, canary=False):
    repo = Path(base).resolve(); pin = local.identity(repo/FINE_CONFIG); config = local.read_json(pin, 512 << 10); helper = overlap_helper()
    fields(config, 'schema authority_pending run_id code_sha256 execution qualification_transport assets native_assets headers canary_object machine scratch_reserve_bytes scratch_filesystem_reserve_bytes scratch_roster scratch_admission_bytes', 'root frozen fine launcher')
    exact(config['schema'], FINE_SCHEMA, 'fine launcher schema'); exact(config['authority_pending'], False, 'parent freeze required')
    require(type(config['run_id']) is str and re.fullmatch(r'a[0-9]{4}', config['run_id']), 'fine attempt'); exact(config['machine'], FINE_MACHINE, 'fine prospective5000s/$0.75')
    cfg = config['execution']; exact(cfg['run_id'], 'fine-sq8-paired100k-'+config['run_id'], 'one immutable fine run')
    exact(set(config['code_sha256']), set(FINE_CODE), 'complete existing import closure')
    for n,h in config['code_sha256'].items(): exact(local.identity(repo/n)['sha256'], h, 'root frozen glue bytes')
    helper.fine_inputs(cfg, repo, headers=config['headers'])
    q = cfg['qualification']; remote = FINE_WORKER/'screen/retained/native-qualification'
    exact(q['directory'], str(remote), 'controlled qualification directory')
    for n,filename in dict(proof='source-qualification.json', terminal='aws-terminal.json', launch='aws-launch.json', closeout='aws-closeout.json').items():
        exact(q[n]['path'], str(remote/filename), 'original qualification pointer')
    exact(cfg['binary']['path'], str(remote/'binaries/hierarchical_semantic_cells'), 'qualified binary path')
    sources = helper.fine_qualification(dict(cfg, qualification_transport=config['qualification_transport']), repo, metadata=True)
    paths = {str(FINE_CONFIG), *FINE_CODE, *sources, helper.AUTHORITY['path'], str(FINE_DIRECT_ROSTER), str(FINE_ROOT/'native-qualification-transport.json'),
        str(FINE_GATE/'native-source-manifest.json'), *(p['path'] for p in config['qualification_transport'].values()),
        *(str(FINE_GATE/n) for n in ('aws-terminal.json', 'aws-launch.json', 'aws-closeout.json'))}
    authority = probe.ref(repo, helper.AUTHORITY); terminal = probe.ref(repo, authority['terminal']); paths.add(authority['terminal']['path'])
    direct = local.read_json(local.identity(repo/FINE_DIRECT_ROSTER))
    exact(direct['bucket'], BUCKET, 'direct body bucket'); exact(config['assets'], direct['items'], 'reuse unchanged direct16 bodies')
    native = local.read_json(local.identity(repo/FINE_ROOT/'native-qualification-transport.json'))
    exact(native['bucket'], BUCKET, 'native body bucket'); exact(config['native_assets'], native['items'], 'original native14 bodies')
    exact({p['name'] for p in config['native_assets']}, set(FINE_ASSURANCE), 'native14 names')
    qualified = local.read_json(dict(q['terminal'], path=str(repo/FINE_GATE/'aws-terminal.json')), 8 << 20)
    launch = local.read_json(dict(q['launch'], path=str(repo/FINE_GATE/'aws-launch.json')), 8 << 20)
    for p in config['native_assets']:
        fields(p, 'name key bytes sha256', 'qualified native object'); exact(body_pin(p), qualified['artifacts'][p['name']], 'original terminal native body')
        exact(p['key'], launch['prefix']+'/artifacts/'+p['name'], 'original native key')
    seen = set()
    for asset in config['assets']:
        fields(asset, 'dataset role name key bytes sha256', 'original direct object'); d,r,n = (asset[k] for k in ('dataset','role','name'))
        require((d,r,n) not in seen, 'unique direct body'); seen.add((d,r,n))
        exact(body_pin(asset), body_pin(overlap_input(config,d,r,n)), 'direct unchanged bytes')
    expected = {(d,r,n) for d in probe.DATASETS for r,names in (('original', ('canonical','order','records','sq8')), ('layout', ('directories.bin','cells.bin')), ('panel', ('requests64','truth64'))) for n in names}
    exact(seen, expected, 'exact sixteen direct roles'); fields(config['headers'], ' '.join(probe.DATASETS), 'both header sets')
    for d in probe.DATASETS:
        fields(config['headers'][d], 'generation plane mean manifest.json', 'original small headers')
        for n,p in config['headers'][d].items():
            fields(p, 'path bytes sha256', 'original archived header')
            expected_path = str(Path(authority['terminal']['path']).parent/authority['datasets'][d][n]['terminal_path']) if n == 'manifest.json' else str(ROOT.parent/'boundary-overlap'/('original-'+n+'-inputs')/(d+'-'+n+('.bin' if n=='mean' else '.json')))
            exact(p['path'], expected_path, 'same archived root/source header'); paths.add(p['path'])
            exact(body_pin(p), body_pin(overlap_input(config,d,'layout' if n=='manifest.json' else 'original',n)), 'same original header bytes'); local.authenticate(dict(p,path=str(repo/p['path'])), 128 << 10)
        for n in (*helper.LAYOUT, 'requests64', 'truth64'):
            original = authority['datasets'][d][n]; exact(body_pin(original), terminal['artifacts'][original['terminal_path']], 'original closed retained body')
    exact(config['canary_object'], {k:p[k] for p in config['native_assets'] if p['name']=='source-qualification.json' for k in ('key','bytes','sha256')}, 'one small original proof GET')
    require(config['canary_object']['bytes'] <= 1 << 20, 'small canary GET'); exact(len(overlap_objects(config)), 30, 'all direct16/native14 object headers')
    paths = sorted(paths)
    for n in paths: publication.relative(n); repo_path(repo,n)
    local.integer(config['scratch_reserve_bytes'], 256 << 20, 8 << 30, 'whole-runtime scratch reserve')
    exact(config['scratch_roster'], fine_scratch_roster(config,paths,repo), 'all coexisting logical scratch components')
    scratch_charge = fine_scratch_admission(config)
    proof = dict(config_path=str(FINE_CONFIG), config_sha256=pin['sha256'], campaign_schema=FINE_CANARY_SCHEMA if canary else FINE_SCHEMA,
        code_identity_sha256=ids.sha(ids.encoded(config['code_sha256'])), refs_identity_sha256=ids.sha(ids.encoded({n:config[n] for n in ('execution','qualification_transport','assets','native_assets','headers','canary_object','machine')})),
        native_identity_sha256=helper.FINE_FULL_SOURCE_ID, source_file_count=404, source_archive_paths=paths, source_archive_paths_sha256=ids.sha(ids.encoded(paths)),
        artifact_roster_sha256=ids.sha(ids.encoded(PROBE_CANARY_ARTIFACTS if canary else FINE_ARTIFACTS)), awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256,
        scratch_charge=scratch_charge)
    return config, proof, {}


def fine_execute(config, config_pin, repo, out, download, check, deadline):
    helper = overlap_helper(); repo,out = Path(repo),Path(out); cfg = config['execution']
    exact(out, FINE_WORKER/'screen', 'fine controlled output'); (out/'retained').mkdir()
    for asset in config['assets']:
        p = overlap_input(config,asset['dataset'],asset['role'],asset['name']); target = positive.regular_path(p['path'])
        require(target.is_relative_to(FINE_WORKER), 'inventoried direct destination'); target.parent.mkdir(parents=True,exist_ok=True); download(asset,target)
    for asset in config['native_assets']:
        target = positive.regular_path(Path(cfg['qualification']['directory'])/asset['name'])
        require(target.is_relative_to(FINE_WORKER), 'inventoried native destination'); target.parent.mkdir(parents=True,exist_ok=True); download(asset,target)
    for n,filename in dict(terminal='aws-terminal.json',launch='aws-launch.json',closeout='aws-closeout.json').items(): helper.retain(dict(cfg['qualification'][n],path=str(repo/FINE_GATE/filename)), Path(cfg['qualification'][n]['path'])); check()
    for d in probe.DATASETS:
        for n,p in config['headers'][d].items():
            target = overlap_input(config,d,'layout' if n=='manifest.json' else 'original',n)
            helper.retain(dict(p,path=str(repo/p['path'])), Path(target['path'])); check()
    require(deadline-time.monotonic() >= 2700, 'exact three native budgets fit cumulative deadline')
    pin = local.write_json(out/'fine-config.json', cfg); terminal = helper.fine_execute(pin['path'],pin['sha256'],repo,out/'fine'); check()
    outcome = local.read_json(local.identity(out/'fine/execution-receipt.json'), 8 << 20)
    result = dict(schema=FINE_SCHEMA+'-execution', config=config_pin, complete=terminal['complete'],status=terminal['status'],fine_terminal=terminal,
        truth_opened=outcome['results'].get('truth_opened',False), scientific_qualification=False,physical_s3_measured=False)
    local.write_json(out/'native-execution-receipt.json', result); require(terminal['execution_exit_code'] == 0, 'native fine execution INVALID'); return result


def fine_verify_pair(output, *, repo=None, config=None, **_):
    helper = overlap_helper(); output = Path(output)
    # Authenticate collected originals at their original worker-relative names.
    for d in probe.DATASETS:
        for n,p in config['execution']['inputs'][d]['original'].items():
            local.authenticate(dict(p,path=str(output/Path(p['path']).relative_to(FINE_WORKER/'screen'))), 512 << 20)
    pin = local.identity(output/'fine-config.json'); exact(local.read_json(pin), config['execution'], 'unchanged fine execution configuration')
    return helper.fine_replay(pin['path'],pin['sha256'],repo,output/'fine')


def fine_verify_execution(output, config, terminal, evidence):
    output = Path(output); result = local.read_json(local.identity(output/'execution-receipt.json'), 8 << 20)
    native = local.read_json(dict(body_pin(result['native_execution']),path=str(output/'native-execution-receipt.json')), 8 << 20)
    exact({k:v for k,v in result.items() if k not in ('native_execution','worker_closure')}, native, 'original native/worker binding')
    exact(native['complete'],True,'closed fine result'); exact(native['fine_terminal'],terminal,'raw fine terminal'); exact(native['status'],terminal['status'],'native science FAIL preserved')
    exact(body_pin(native['config']),body_pin(local.identity(output/'config.json')),'same root launcher config'); return native


def fine_profile():
    from contextlib import ExitStack
    module = sys.modules[__name__]; stack = ExitStack()
    stack.enter_context(patch.multiple(module, OVERLAP_ROOT=FINE_ROOT, OVERLAP_CONFIG=FINE_CONFIG, OVERLAP_SCHEMA=FINE_SCHEMA, OVERLAP_CANARY_SCHEMA=FINE_CANARY_SCHEMA,
        OVERLAP_PREFIX=FINE_PREFIX, OVERLAP_WORKER=FINE_WORKER, OVERLAP_MACHINE=FINE_MACHINE, OVERLAP_CODE=FINE_CODE, OVERLAP_ARTIFACTS=FINE_ARTIFACTS,
        overlap_qualify=fine_qualify, overlap_execute=fine_execute, overlap_verify_pair=fine_verify_pair, overlap_verify_execution=fine_verify_execution))
    stack.enter_context(overlap_profile()); userdata,replay = probe_user_data,probe_replay
    def fine_userdata(*args, **kwargs):
        import inspect
        result = userdata(*args, **kwargs).replace('--cell-overlap-pair','--fine-sq8-pair').replace('import OVERLAP_ARTIFACTS','import FINE_ARTIFACTS').replace('join(OVERLAP_ARTIFACTS)','join(FINE_ARTIFACTS)')
        # Bootstrap precedes source extraction: embed the same logical-name
        # scan, including both hard-link names, without importing the checkout.
        result = result.replace(' rooted=$(du -sb "$root" | cut -f1)',
            ' rooted=$(python3 - "$root" <<\'PY_SCRATCH\'\nimport os, sys\nfrom pathlib import Path\n'+
            inspect.getsource(local.directory_bytes)+'\nprint(directory_bytes(Path(sys.argv[1])))\nPY_SCRATCH\n)')
        require(len(result.encode()) < 16384, 'fine bootstrap16KiB cap')
        subprocess.run(['bash','-n'], input=result,text=True,check=True); return result
    def fine_collected(*args, **kwargs):
        result = replay(*args, **kwargs)
        if not kwargs.get('canary',False) and result['executed']:
            result['truth_opened'] = local.read_json(local.identity(Path(args[0])/'screen/summary.json'),8 << 20)['truth_opened']
        return result
    stack.enter_context(patch.multiple(module, probe_user_data=fine_userdata, probe_replay=fine_collected))
    return stack


def fine_cli(args):
    require(args, 'CLI: --fine-sq8-pair aNNNN | --canary aNNNN | --stage[-canary] REPO OUTPUT ROOT | --replay[-canary] OUTPUT | --preflight | --self-check')
    if args == ['--self-check']:
        fine_scratch_self_check(); overlap_helper().fine_self_check(); fine_launcher_self_check(); return
    with fine_profile():
        if args == ['--preflight']: print(json.dumps(fine_qualify()[1])); return
        probe_cli(args)


def fine_scratch_self_check():
    """Source accounting only: sparse committed lengths, never native/data/GT."""
    import shlex
    module = sys.modules[__name__]; repo = Path(__file__).resolve().parents[1]
    archived_config = repo/FINE_ROOT/'a0001/screen/config.json'
    config = local.read_json(local.identity(archived_config), 512 << 10)
    archived = local.read_json(local.identity(repo/FINE_ROOT/'a0001/screen/fine/execution-receipt.json'), 8 << 20)
    proof = local.read_json(local.identity(repo/FINE_ROOT/'a0001/screen/source-qualification.json'), 512 << 10)
    exact(local.identity(archived_config)['sha256'],proof['config_sha256'],'committed config authority')
    def rejects(fn, message):
        try: fn()
        except ValueError as error: require(message in str(error), str(error)); return
        raise AssertionError('scratch rejection missing: '+message)
    # Use a private TMPDIR filesystem for deterministic positive-growth tests;
    # the separate source view stays on the repository device for hard links.
    with tempfile.TemporaryDirectory(prefix='fine-scratch-check-') as tmp, tempfile.TemporaryDirectory(prefix='fine-source-check-', dir=repo.parent) as source_tmp:
        worker = Path(tmp)/'worker'; worker.mkdir(); extra = Path(tmp)/'extra'; extra.mkdir()
        # Both independently retained copies are charged even when SHA-equal.
        retained = 0
        for d in probe.DATASETS:
            for category in ('original','layout'):
                for n,p in config['execution']['inputs'][d][category].items():
                    exact(body_pin(p),body_pin(archived['inputs'][d][category][n]),'committed retained pin')
                    for path in (worker/Path(p['path']).relative_to(FINE_WORKER), worker/'screen/fine/inputs'/d/category/n):
                        path.parent.mkdir(parents=True,exist_ok=True)
                        with path.open('xb') as stream: stream.truncate(p['bytes'])
                        retained += p['bytes']
        exact(retained,2048293796,'both committed original/layout copies')
        with (worker/'allocated').open('xb') as stream:
            stream.write(b'x'*(1 << 20)); stream.flush(); os.fsync(stream.fileno())
        os.link(worker/'allocated',worker/'allocated-link')
        deadline = time.monotonic()+60; baseline = shutil.disk_usage(worker).total
        def scan(cap=8 << 30, base=baseline):
            peaks = dict(scratch_bytes=0)
            probe_resource_check(worker,base,cap,deadline,[],peaks)
            return peaks
        original = probe
        with patch.object(original,'ORIGINAL_ROOT',worker), fine_profile():
            with patch.object(module,'FINE_CONFIG',archived_config), patch.object(module,'OVERLAP_CONFIG',archived_config), patch.object(module,'PROBE_CONFIG',archived_config):
                shell = probe_user_data('a'*40,'b'*64,'source/mock',FINE_PREFIX+config['run_id'],dict(proof,config_path=str(archived_config)))
            watchdog = shell.split('(while kill -0 "$scratch_owner" 2>/dev/null; do\n',1)[1].split(' sleep 1\n',1)[0]
            def bootstrap(base):
                script = 'set -eu\nroot='+shlex.quote(str(worker))+'\nBORSUK_HIERARCHICAL_SCRATCH_BASE_USED='+str(base)+'\nscratch_owner=$$\ntrap "exit 73" TERM\n'+watchdog+'printf "%s %s" "$rooted" "$growth"\n'
                return subprocess.run(['bash','-c',script],capture_output=True,text=True,timeout=10)
            require(probe is not original,'actual fine namespace is copied')
            exact(probe.ORIGINAL_ROOT,worker,'captured whole worker root')
            with patch.object(original,'ORIGINAL_ROOT',worker/'screen/fine/measurement'):
                with patch.object(local,'directory_bytes',wraps=local.directory_bytes) as scans:
                    first = scan()
                    exact(scans.call_count,1,'equal root only one directory scan')
                exact(first['scratch_bytes'],local.directory_bytes(worker),'equal root must be scanned once')
                exact(int(bootstrap(baseline).stdout.split()[0]),first['scratch_bytes'],'watchdog counts both hard-link names')
                exact(probe.ORIGINAL_ROOT,worker,'helper patch cannot change captured root')
            for alias in (worker,worker/'screen'):
                with patch.object(probe,'ORIGINAL_ROOT',alias):
                    with patch.object(local,'directory_bytes',wraps=local.directory_bytes) as scans:
                        exact(scan()['scratch_bytes'],first['scratch_bytes'],'descendant alias charged once')
                        exact(scans.call_count,1,'descendant only one directory scan')
            with (extra/'allocated').open('xb') as stream:
                stream.write(b'x'*(1 << 20)); stream.flush(); os.fsync(stream.fileno())
            with patch.object(probe,'ORIGINAL_ROOT',extra):
                separate = scan()
                exact(separate['scratch_bytes'],first['scratch_bytes']+(1 << 20),'disjoint original charged')
                exact(separate['scratch_components']['extra_root_bytes'],1 << 20,'extra component receipt')
            with patch.object(probe,'ORIGINAL_ROOT',worker.parent): rejects(scan,'ancestor')
            os.sync(); used = shutil.disk_usage(worker).used
            with (extra/'outside-growth').open('xb') as stream:
                stream.write(b'x'*(4 << 20)); stream.flush(); os.fsync(stream.fileno())
            grown = scan(base=used)['scratch_components']
            require(grown['filesystem_growth_bytes'] >= 4 << 20,'synced outside-root allocation charged')
            exact(grown['total_bytes'],sum(grown[k] for k in ('directory_bytes','filesystem_growth_bytes','extra_root_bytes')),'sum, never max')
            watched = bootstrap(used); exact(watched.returncode,0,'generated watchdog below cap')
            require(int(watched.stdout.split()[1]) >= 4 << 20,'watchdog outside growth')
            directory = sum(p['max_bytes'] for p in config['scratch_roster'])
            with (worker/'old-envelope').open('xb') as stream: stream.truncate(directory-local.directory_bytes(worker))
            # A synthetic baseline exercises the old envelope's full growth
            # without allocating GiBs; only the outside-growth test allocates.
            envelope_baseline = shutil.disk_usage(worker).used-directory
            rejects(lambda:scan(base=envelope_baseline),'scratch/deadline/monitor')
            exact(bootstrap(envelope_baseline).returncode,73,'watchdog rejects old envelope too')
            (worker/'old-envelope').unlink()
            with (worker/'boundary').open('xb') as stream: stream.truncate((8 << 30)-local.directory_bytes(worker))
            exact(scan()['scratch_bytes'],8 << 30,'unchanged inclusive8GiB boundary')
            watched = bootstrap(baseline); exact(watched.returncode,0,'watchdog inclusive boundary')
            exact(watched.stdout,str(8 << 30)+' 0','watchdog matches logical-name runtime scan')
            with (worker/'boundary').open('ab') as stream: stream.write(b'x')
            rejects(scan,'scratch/deadline/monitor')
            exact(bootstrap(baseline).returncode,73,'generated watchdog rejects same overflow')
        # The old coexistence sum fits, but its runtime sum cannot be admitted.
        directory = sum(p['max_bytes'] for p in config['scratch_roster'])
        require(directory < 8 << 30 < 2*directory,'historical envelope mismatch reproduced')
        rejects(lambda:fine_scratch_admission(dict(config,scratch_filesystem_reserve_bytes=256 << 20)),'scratch')
        # Real metadata preflight in an isolated source view. Hard links avoid
        # duplicating the source archive; the cold binary is a sparse stub and
        # must never be authenticated/opened by metadata admission.
        shadow = Path(source_tmp); prospective = copy.deepcopy(config)
        paths = sorted((set(proof['source_archive_paths'])-{proof['config_path']})|{str(FINE_CONFIG)})
        binary = config['qualification_transport']['binaries/hierarchical_semantic_cells']['path']
        for name in paths:
            target = shadow/name; target.parent.mkdir(parents=True,exist_ok=True)
            if name == str(FINE_CONFIG): continue
            if name == binary:
                with target.open('xb') as stream: stream.truncate((repo/name).stat().st_size)
            else: os.link(repo/name,target)
        prospective['code_sha256'] = {n:local.identity(repo/n)['sha256'] for n in FINE_CODE}
        prospective['scratch_filesystem_reserve_bytes'] = 256 << 20
        prospective['scratch_roster'] = fine_scratch_roster(prospective,paths,shadow)
        local.write_json(shadow/FINE_CONFIG,prospective)
        admitted = fine_qualify(shadow)[1]['scratch_charge']
        exact(admitted,fine_scratch_admission(prospective),'actual preflight same component formula')
        prospective['scratch_filesystem_reserve_bytes'] = 8 << 30
        (shadow/FINE_CONFIG).write_bytes(local.canonical(prospective))
        rejects(lambda:fine_qualify(shadow),'scratch')
    print('PASS fine scratch: real profile copy, aliases/disjoint/ancestor, two sparse committed copies, synced outside growth,8GiB runtime/generated-watchdog boundary, real preflight components/rejection; no native/data/GT hydration.')
    return dict(retained_sparse_logical_bytes=retained, equal=first, disjoint=separate,
        outside_growth=grown, old_directory_envelope_bytes=directory,
        old_minimum_runtime_charge_bytes=2*directory, prospective_preflight=admitted,
        inclusive_cap_bytes=8 << 30, generated_watchdog_checked=True)


def fine_launcher_self_check():
    assert len(set(FINE_ARTIFACTS)) == len(FINE_ARTIFACTS), 'unique complete collected fine roster'
    assert FINE_MACHINE == dict(wall_seconds=5000, compute_cap_usd=.75), 'fixed fine machine caps'
    from contextlib import ExitStack
    import io
    module = sys.modules[__name__]
    def rejects(fn):
        try: fn()
        except (ValueError, AssertionError, OSError, KeyError): return
        raise AssertionError('negative fine launcher admitted')
    with tempfile.TemporaryDirectory(prefix='fine-launcher-check-') as tmp, ExitStack() as stack:
        root = Path(tmp); repo = root/'probe-repo'; repo.mkdir(); cfg_path = root/'config.json'
        config = dict(schema=FINE_SCHEMA,run_id='a0001',assets=[dict(key='synthetic/input/'+str(i),bytes=1,sha256=local.sha(b'x')) for i in range(16)],
            native_assets=[dict(name=str(i),key='synthetic/native/'+str(i),bytes=1,sha256=local.sha(b'x')) for i in range(14)],
            canary_object=dict(key='synthetic/native/0',bytes=1,sha256=local.sha(b'x')))
        local.write_json(cfg_path,config)
        proof = dict.fromkeys(TERMINAL_IDENTITIES,'0'*64)
        proof.update(config_path=str(cfg_path),config_sha256=local.identity(cfg_path)['sha256'],campaign_schema=FINE_CANARY_SCHEMA,source_file_count=404,
            source_archive_paths=['scripts/launch_hierarchical_cells_100k_spot.py'],awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
        proof['source_archive_paths_sha256'] = ids.sha(ids.encoded(proof['source_archive_paths']))
        stack.enter_context(patch.object(module,'FINE_CONFIG',cfg_path)); stack.enter_context(patch.object(module,'fine_qualify',return_value=(config,proof,{})))
        with fine_profile():
            exact(PROBE_ROOT,FINE_ROOT,'fine canary admission destination'); exact(PROBE_CODE,FINE_CODE,'closed packaged imports')
            science = probe_user_data('a'*40,'b'*64,'source/mock',FINE_PREFIX+'a0001',proof)
            canary = probe_user_data('a'*40,'b'*64,'source/mock',FINE_PREFIX+'canary-a0001',proof,canary=True)
        for value in ('--fine-sq8-pair --stage ', '/mnt/hierarchical-100k', 'MemoryMax=8G','CPUQuota=200%', 'MemorySwapMax=0','5000','import FINE_ARTIFACTS','else test -f "$name"','sync -f terminal.json'):
            require(value in science,'fine bootstrap: '+value)
        require('MemoryMax=256M' in canary and '--fine-sq8-pair --stage-canary' in canary,'separate bounded metadata canary')
        require(len(science.encode()) < 16384 and len(canary.encode()) < 16384,'unchanged userdata16KiB cap')
        objects = overlap_objects(config); versions = dict(FIXED['versions'],**SDK_VERSIONS)
        real_import = importlib.import_module; real_version = importlib.metadata.version
        stack.enter_context(patch.object(importlib,'import_module',side_effect=lambda n,*a,**kw:SimpleNamespace() if n in versions else real_import(n,*a,**kw)))
        stack.enter_context(patch.object(importlib.metadata,'version',side_effect=lambda n:versions[n] if n in versions else real_version(n)))
        closed = []
        class Client:
            meta = SimpleNamespace(service_model=SimpleNamespace(operation_model=lambda _:SimpleNamespace(input_shape=SimpleNamespace(members={'IfNoneMatch':None}))))
            def head_object(self,**kw): return dict(ContentLength=next(p['bytes'] for p in objects if p['key']==kw['Key']))
            def get_object(self,**kw):
                exact(kw['Key'],config['canary_object']['key'],'no canary dataset/panel/binary GET'); return dict(ContentLength=1,Body=io.BytesIO(b'x'))
            def close(self): closed.append(True)
        client = Client(); stack.enter_context(patch.object(publication,'sdk_client',return_value=client))
        for n in ('native_stage','restore_writer_inputs','execute'): stack.enter_context(patch.object(probe,n,side_effect=AssertionError('canary cannot invoke science')))
        group = root/'kernel/borsuk-global-leaf-mock.slice/controller'; group.mkdir(parents=True)
        (group/'cpu.max').write_text('100000 100000'); (group/'pids.max').write_text('512')
        counters = {'path':str(group),'memory.max':'268435456','memory.peak':'1024','memory.swap.max':'0','memory.swap.peak':'0','memory.events':'oom 0\noom_kill 0\noom_group_kill 0','cpu_affinity':[0],'cpu.max':'100000 100000','pids.max':'512'}
        stack.enter_context(patch.object(local,'resource_snapshot',return_value=counters)); stack.enter_context(patch.object(probe,'cgroup_snapshot',return_value=dict(counters,path=str(group.parent))))
        stack.enter_context(patch.dict(os.environ,dict(BORSUK_GLOBAL_LEAF_SLICE=group.parent.name,BORSUK_HIERARCHICAL_CONFIG_SHA256=proof['config_sha256'],
            BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256=proof['source_archive_paths_sha256'],BORSUK_HIERARCHICAL_DEADLINE_EPOCH=str(int(time.time())+30),BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=str(shutil.disk_usage(root).used))))
        out = root/'closed'; out.mkdir()
        with fine_profile():
            probe_stage(repo,out/'screen',root,canary=True); rejects(lambda:probe_stage(repo,out/'screen',root,canary=True))
            for n in ('test-resources.txt','run-closed.log'): probe.copy_bytes(out/n,b'synthetic closure\n')
            nodes = {'worker':dict(instance_id='i-synthetic')}; identity = dict(source_commit='a'*40,source_archive_sha256='b'*64)
            launch = dict(identity,instance_id='i-synthetic',nodes=nodes,prefix=FINE_PREFIX+'canary-a0001')
            local.write_json(out/'aws-launch.json',launch); local.write_json(out/'aws-closeout.json',dict(state='terminated',nodes=nodes))
            local.write_json(out/'aws-reservation.json',dict(identity,schema=FINE_CANARY_SCHEMA,qualification=proof,wall_seconds=480,compute_cap_usd=.12,ebs_s3_allowance_usd=.05))
            terminal = dict(identity,**proof,instance_id='i-synthetic',schema=FINE_CANARY_SCHEMA,phase='complete',status='complete',exit_code=0,original_exit_code=0,
                artifacts={n:body_pin(local.identity(out/n)) for n in PROBE_CANARY_ARTIFACTS})
            local.write_json(out/'aws-terminal.json',terminal); require(probe_replay(out,canary=True,repo=repo)['executed'],'closed fine canary replay')
            class Collected:
                def get_object(self,**kw):
                    key=kw['Key']; name='aws-terminal.json' if key.endswith('/terminal.json') else key.split('/artifacts/',1)[1]; return dict(Body=io.BytesIO((out/name).read_bytes()))
            collected = root/'collected'; collected.mkdir()
            for n in ('aws-launch.json','aws-closeout.json','aws-reservation.json'): shutil.copyfile(out/n,collected/n)
            probe_collect(Collected(),launch['prefix'],collected,'i-synthetic','a'*40,'b'*64,canary=True)
            require(probe_replay(collected,canary=True,repo=repo)['executed'],'streamed fine canary collection/gzip replay')
            selected = repo/FINE_ROOT/'canary/a0001'; selected.parent.mkdir(parents=True); shutil.copytree(collected,selected)
            pointer = {n:proof[n] for n in ('config_sha256','code_identity_sha256','refs_identity_sha256','native_identity_sha256','source_archive_paths_sha256')}
            pointer.update(schema='borsuk-fine-sq8-canary-admission-v1',attempt='a0001',terminal_sha256=local.identity(selected/'aws-terminal.json')['sha256'])
            pointer_path = repo/FINE_ROOT/'canary-admission.json'; local.write_json(pointer_path,pointer)
            probe_require_canary(repo,proof)
            before_pointer = pointer_path.read_bytes(); pointer['code_identity_sha256'] = 'f'*64; pointer_path.write_bytes(local.canonical(pointer))
            rejects(lambda:probe_require_canary(repo,proof)); pointer_path.write_bytes(before_pointer)
            before=(collected/'aws-closeout.json').read_bytes(); (collected/'aws-closeout.json').write_bytes(local.canonical(dict(state='terminated',nodes={'worker':dict(instance_id='i-wrong')})))
            rejects(lambda:probe_replay(collected,canary=True,repo=repo)); (collected/'aws-closeout.json').write_bytes(before)
            with patch.object(client,'head_object',return_value=dict(ContentLength=2)): rejects(lambda:probe_stage(repo,root/'failed-screen',root,canary=True))
            exact(local.read_json(local.identity(root/'failed-screen/summary.json'))['status'],'INVALID','failed infrastructure stays INVALID')
            clean=local.read_json(local.identity(root/'failed-screen/cleanup.json')); require(clean['sdk_client_closed'] and clean['scratch_removed'] and clean['monitor_stopped'],'same failed stage cleanup')
        require(closed,'SDK client closed')
        # Science retains the exact screen/scratch originals while removing
        # only the disposable infrastructure scratch. No native work is run.
        monitored = threading.Event(); real_check = probe_resource_check
        def observed_check(*args,**kwargs):
            real_check(*args,**kwargs)
            if threading.current_thread() is not threading.main_thread(): monitored.set()
        def staged(config,pin,repo,out,download,check,deadline):
            (out/'retained').mkdir()
            for i,p in enumerate((*config['assets'],*config['native_assets'])): download(p,out/'retained'/str(i))
            probe.copy_bytes(out/'scratch/relaion/order',b'opaque original order'); probe.copy_bytes(out/'scratch/relaion/sq8',b'opaque original SQ8')
            require(monitored.wait(3),'actual probe_stage monitor sampled fine namespace')
            native_terminal = dict(status='FAIL',complete=True,execution_exit_code=0)
            result = dict(schema=FINE_SCHEMA+'-execution',config=pin,status='FAIL',complete=True,fine_terminal=native_terminal,
                truth_opened=False,scientific_qualification=False,physical_s3_measured=False)
            local.write_json(out/'native-execution-receipt.json',result); return result
        science_counters = dict(counters,**{'memory.max':str(8 << 30),'cpu.max':'200000 100000','cpu_affinity':[0,1]})
        (group/'cpu.max').write_text('200000 100000')
        with patch.object(module,'probe_resource_check',side_effect=observed_check),patch.object(module,'fine_execute',side_effect=staged),patch.object(probe,'ORIGINAL_ROOT',root),patch.object(client,'get_object',side_effect=lambda **_:dict(ContentLength=1,Body=io.BytesIO(b'x'))):
            with patch.object(local,'resource_snapshot',return_value=science_counters),patch.object(probe,'cgroup_snapshot',return_value=dict(science_counters,path=str(group.parent))),fine_profile():
                result = probe_stage(repo,root/'science-screen',root)
                exact(result['status'],'FAIL','closed scientific FAIL remains valid')
                charged = local.read_json(local.identity(root/'science-screen/resources.json'))
                exact(charged['scratch_bytes'],charged['scratch_components']['total_bytes'],'coherent monitor peak receipt')
                exact(charged['scratch_components']['extra_root_bytes'],0,'actual fine monitor skips captured worker alias')
                clean = local.read_json(local.identity(root/'science-screen/cleanup.json'))
                require(clean['scratch_removed'] and not clean['original_root_removed'],'infrastructure scratch removed; originals retained')
                exact((root/'science-screen/scratch/relaion/order').read_bytes(),b'opaque original order','original order preserved through cleanup')
                exact((root/'science-screen/scratch/relaion/sq8').read_bytes(),b'opaque original SQ8','original SQ8 preserved through cleanup')
                fine_verify_execution(root/'science-screen',config,result['fine_terminal'],{})
        shared,_ = ids.lifecycle(); shared.self_check(lifecycle_only=True)
    exact(PROBE_SCHEMA,'borsuk-global-leaf-probe-spot-v1','historical namespace restored')
    print('PASS fine launcher synthetic: bounded bootstrap/import roster,30 HEAD/one proof GET/no ANN/no GT,actual CLI exit2,stage cleanup,stream collection/gzip replay,SAME-ID refusal and shared ACK/terminate/wait failures. SDK/kernel mocked; no cloud/native/large-body opens.')


if __name__ == '__main__':
    try:
        if sys.argv[1:2] == ['--fine-sq8-pair']:
            fine_cli(sys.argv[2:])
        elif sys.argv[1:2] == ['--cell-overlap-pair']:
            overlap_cli(sys.argv[2:])
        elif sys.argv[1:2] == ['--partitioner-pair']:
            pair_cli(sys.argv[2:])
        elif sys.argv[1:2] == ['--constrained-split-falsifier']:
            from scripts import run_constrained_split_falsifier as split
            split.cli(sys.argv[2:])
        elif sys.argv[1:2] == ['--global-leaf-probe']:
            probe_cli(sys.argv[2:])
        elif sys.argv[1:] == ['--self-check']:
            self_check()
        elif sys.argv[1:] == ['--self-check-canary']:
            canary_self_check()
        elif len(sys.argv) == 5 and sys.argv[1] == '--stage':
            print(json.dumps(stage(*sys.argv[2:])))
        elif len(sys.argv) == 5 and sys.argv[1] == '--stage-canary':
            print(json.dumps(stage(*sys.argv[2:], canary=True)))
        elif len(sys.argv) == 3 and sys.argv[1] == '--replay':
            print(json.dumps(replay(sys.argv[2])))
        elif len(sys.argv) == 3 and sys.argv[1] == '--replay-canary':
            print(json.dumps(replay(sys.argv[2], canary=True)))
        else:
            canary = len(sys.argv) == 3 and sys.argv[1] == '--canary'
            require(canary or len(sys.argv) == 2, __doc__)
            with open('/tmp/borsuk-hierarchical-100k-launch.lock', 'a+') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
                main(sys.argv[2] if canary else sys.argv[1], canary=canary)
    except Exception as error:
        print('INVALID: '+str(error), file=sys.stderr)
        sys.exit(2)
