#!/usr/bin/env python3
"""Thin paired consumed64 staging adapter; root freezes CONFIG and owns launch.

CLI: aNNNN | --stage REPO NEW_OUTPUT WORKER_ROOT | --replay OUTPUT | --self-check.
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
SCHEMA = 'borsuk-hierarchical-100k-spot-v1'
PREFIX = 'research/hierarchical-cells/20261003/paired100k-'
TOKEN_PREFIX, TAG = 'hierarchical-100k-', 'borsuk-hierarchical-100k'
WALL, MEMORY, SCRATCH = 1800, 2 << 30, 16 << 30
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
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256',
    'native_identity_sha256', 'artifact_roster_sha256', 'campaign_schema',
    'awscli_version', 'awscli_sha256')


def repo_path(repo, name):
    publication.relative(name)
    return positive.regular_path(Path(repo)/name)


def body_pin(value):
    return {k: value[k] for k in ('bytes', 'sha256')}


def transport_path(repo, name):
    return positive.regular_path(name) if Path(name).is_absolute() else repo_path(repo, name)


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
        transport_path(repo, pin['path'])
    return values, identity


def qualify(base=Path('.'), *, native_files=True):
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
    extra = sum(p['bytes'] for p in (config['native']['source_archive'], config['native']['gate_log'],
                                    *config['native']['binaries'].values()))
    require(config['scratch_admission_bytes'] >= 1975116322+extra+config['scratch_reserve_bytes'],
            'whole-worker scratch reserve incomplete')
    if native_files:
        # Preflight requires actual qualified bodies locally. Runtime instead
        # downloads those exact pins and performs this same proof validation.
        proof = copy.deepcopy(config['native'])
        for name in ('source_archive', 'gate_log'):
            proof[name] = dict(body_pin(proof[name]), path=str(transport_path(repo, proof[name]['path'])))
        for roster in ('sources', 'binaries'):
            proof[roster] = {n: dict(body_pin(p), path=str(transport_path(repo, p['path']))) for n, p in proof[roster].items()}
        with tempfile.TemporaryDirectory(prefix='hierarchical-proof-') as tmp:
            local.validate_proof(local.write_json(Path(tmp)/'proof.json', proof))
    proof = dict(config_path=str(CONFIG), config_sha256=pin['sha256'], campaign_schema=SCHEMA,
        code_identity_sha256=ids.sha(ids.encoded(config['code_sha256'])),
        refs_identity_sha256=ids.sha(ids.encoded(config['refs'])), native_identity_sha256=native_identity,
        artifact_roster_sha256=ids.sha(ids.encoded(ARTIFACTS)),
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)
    return config, proof, values


def preflight(base=Path('.')):
    _, proof, _ = qualify(base)
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty source')
    return proof


def scratch_snapshot(root, baseline):
    # Directory charge includes archives, extraction, venv, inputs and outputs.
    # Filesystem growth also charges installation/cache writes outside the root;
    # double counting inside-root growth is intentional conservative admission.
    usage = shutil.disk_usage(root)
    return local.directory_bytes(root)+max(0, usage.used-baseline)


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


def stage(repo, output, worker_root):
    repo, out, root = map(lambda p: Path(p).resolve(), (repo, output, worker_root))
    require(out.is_relative_to(root) and repo.is_relative_to(root), 'whole-worker scratch ownership')
    require(not out.exists(), 'output exists')
    config, proof, authorities = qualify(repo, native_files=False)
    exact(proof['config_sha256'], os.environ.get('BORSUK_HIERARCHICAL_CONFIG_SHA256'), 'bootstrap config binding')
    remaining = int(os.environ['BORSUK_HIERARCHICAL_DEADLINE_EPOCH'])-time.time()
    require(0 < remaining <= WALL, 'cumulative worker deadline')
    baseline = int(os.environ['BORSUK_HIERARCHICAL_SCRATCH_BASE_USED'])
    deadline = time.monotonic()+remaining
    limits = dict(LIMITS, timeout_seconds=max(1, int(remaining)), cpu_affinity=config['cpu_affinity'])
    before = local.resource_snapshot(limits)
    group = Path(before['path'])
    quota, period = map(int, (group/'cpu.max').read_text().split())
    require(quota*100 == config['cpu_quota_percent']*period and
            (group/'pids.max').read_text().strip() == str(config['tasks_max']), 'CPU/tasks cgroup')
    before.update(cpu_max=(group/'cpu.max').read_text().strip(), tasks_max=(group/'pids.max').read_text().strip())
    versions = {n: importlib.metadata.version(n) for n in FIXED['versions']}
    exact(versions, FIXED['versions'], 'pinned decoder versions')
    out.mkdir()
    scratch = out/'scratch'
    scratch.mkdir()
    calls, peaks, errors, stopped = [], {'scratch_bytes': 0}, [], threading.Event()
    result = dict(status='INVALID', complete=False, physical_s3_measured=False, vendor_win=False)
    original_handler = signal.getsignal(signal.SIGALRM)
    original_term = signal.getsignal(signal.SIGTERM)
    original_timer = signal.setitimer(signal.ITIMER_REAL, remaining)
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('cumulative worker deadline')))
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('worker terminated')))
    def check():
        amount = scratch_snapshot(root, baseline)
        peaks['scratch_bytes'] = max(peaks['scratch_bytes'], amount)
        require(amount <= SCRATCH, 'whole-worker scratch cap')
        require(time.monotonic() < deadline, 'cumulative worker deadline')
        require(not errors, 'whole-worker monitor: '+str(errors))
    def monitor():
        while not stopped.wait(.05):
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
        local.write_json(out/'cleanup.json', dict(scratch_removed=not scratch.exists(), monitor_stopped=True,
                                                native_processes_concurrent_max=1))
        local.write_json(out/'summary.json', result)
        require(scratch_snapshot(root, baseline) <= SCRATCH, 'final whole-worker scratch cap')
        fd = os.open(out, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return result


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    require(re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha), 'archive identity')
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'attempt prefix')
    exact(qualification['config_path'], str(CONFIG), 'bootstrap config path')
    _, bootstrap = ids.lifecycle()
    adapter = {k: qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key': 'unused'}, native_publisher={'key': 'unused'})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap, '_offered', return_value=False):
        body = bootstrap.user_data(commit, archive_sha, archive_key, prefix, adapter)
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    command = f'''phase=install
test "$(uname -m)" = x86_64
python3.12 -m venv "$root/venv"
export PIP_CACHE_DIR="$root/pip-cache" TMPDIR="$root"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0 boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
phase=paired-diagnostic
remaining=$((BORSUK_HIERARCHICAL_DEADLINE_EPOCH-$(date +%s)))
test "$remaining" -gt 0
systemd-run --unit=hierarchical-100k --wait --pipe -p MemoryMax=2G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec="$remaining" -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=TMPDIR="$root" \\
 --setenv=BORSUK_HIERARCHICAL_CONFIG_SHA256={qualification['config_sha256']} \\
 --setenv=BORSUK_HIERARCHICAL_DEADLINE_EPOCH="$BORSUK_HIERARCHICAL_DEADLINE_EPOCH" \\
 --setenv=BORSUK_HIERARCHICAL_SCRATCH_BASE_USED="$BORSUK_HIERARCHICAL_SCRATCH_BASE_USED" \\
 --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=5 "$remaining" \\
 taskset -c 0,1 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root/screen" "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    body = body[:start]+command+body[end:]
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/hierarchical-100k')
    body = body.replace('python3-boto3 python3.12', 'python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    body = body.replace('phase=bootstrap\n', '''phase=bootstrap
export BORSUK_HIERARCHICAL_DEADLINE_EPOCH=$(($(date +%s)+1800))
export BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=$(df -B1 --output=used "$root" | tail -1 | tr -d ' ')
''', 1)
    body = body.replace('exec >run.log 2>&1\n', '''exec >run.log 2>&1
scratch_owner=$$
(while kill -0 "$scratch_owner" 2>/dev/null; do
 rooted=$(du -sb "$root" | cut -f1)
 used=$(df -B1 --output=used "$root" | tail -1 | tr -d ' ')
 growth=$((used-BORSUK_HIERARCHICAL_SCRATCH_BASE_USED))
 if [ "$growth" -lt 0 ]; then growth=0; fi
 if [ "$((rooted+growth))" -gt 17179869184 ]; then kill -TERM "$scratch_owner"; exit; fi
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
    # The scientific bootstrap uploads terminal last. Durably sync artifacts and
    # the terminal before its marker/publication without changing owned lifecycle.
    body = body.replace('  aws_ready=0\n', '  sync -f "$root" || code=96\n  aws_ready=0\n', 1)
    body = body.replace('write_terminal && terminal_ready=1', 'write_terminal && sync -f terminal.json && terminal_ready=1')
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<terminal>', 'exec')
    require(len(body.encode()) < 16384 and all(n not in body for n in ('rustup', 'cargo', 'unused')), 'scientific bootstrap boundary')
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = ids.lifecycle()
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def replay(out):
    out = Path(out)
    reservation, launch, close, terminal = (json.loads((out/n).read_bytes()) for n in (
        'aws-reservation.json', 'aws-launch.json', 'aws-closeout.json', 'aws-terminal.json'))
    exact(close['nodes'], launch['nodes'], 'SAME owned IDs')
    exact(close['state'], 'terminated', 'termination before collection')
    require(terminal['instance_id'] == launch['instance_id'] in {v['instance_id'] for v in close['nodes'].values()}, 'owned terminal host')
    for key in ('source_commit', 'source_archive_sha256'):
        exact(terminal[key], reservation[key], 'terminal archive binding')
        exact(terminal[key], launch[key], 'launch archive binding')
    exact(terminal['schema'], SCHEMA, 'terminal campaign schema')
    for name in TERMINAL_IDENTITIES:
        exact(terminal[name], reservation['qualification'][name], 'terminal authority: '+name)
    require(set(terminal['artifacts']) <= set(ARTIFACTS), 'terminal artifact roster')
    for name, pin in terminal['artifacts'].items():
        exact(body_pin(local.identity(out/name)), pin, 'collected body identity')
    complete = terminal['phase'] == terminal['status'] == 'complete' and terminal['exit_code'] == 0
    if complete:
        exact(terminal['original_exit_code'], 0, 'original worker success')
        exact(set(terminal['artifacts']), set(ARTIFACTS), 'complete artifact roster')
        config = local.decode((out/'screen/config.json').read_bytes())
        exact(local.sha((out/'screen/config.json').read_bytes()), terminal['config_sha256'], 'collected config')
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
        counters = local.decode((out/'screen/worker-cgroup.json').read_bytes())
        exact(counters['closed'], True, 'worker resource closure')
        for snapshot in (counters['before'], counters['after']):
            require(0 < int(snapshot['memory.max']) <= config['memory_bytes'] and
                    int(snapshot['memory.peak']) <= config['memory_bytes'], 'worker memory cap')
            exact(snapshot['memory.swap.max'], '0', 'worker no swap')
            exact(int(snapshot['memory.swap.peak']), 0, 'worker swap peak')
            q, p = map(int, snapshot['cpu_max'].split())
            require(q*100 == config['cpu_quota_percent']*p, 'worker CPU quota')
            exact(int(snapshot['tasks_max']), config['tasks_max'], 'worker tasks cap')
        require(counters['before']['path'] == counters['after']['path'], 'same worker cgroup')
        before_events, after_events = (dict(line.split() for line in counters[k]['memory.events'].splitlines())
                                       for k in ('before', 'after'))
        require(all(before_events.get(k) == after_events.get(k) for k in ('oom', 'oom_kill', 'oom_group_kill')), 'worker no OOM')
        resources = local.decode((out/'screen/resources.json').read_bytes())
        require(resources['scratch_bytes'] <= SCRATCH and 0 <= resources['wall_seconds'] <= WALL, 'worker scratch/deadline closure')
        exact(len(resources['sdk_calls']), 18, 'qualified artifact and dataset call roster')
        admission_receipt = local.decode((out/'screen/admission.json').read_bytes())
        for name, expected in dict(status='ADMITTED', excluded_from_measurement=True, quality_promotion=False, disposable_outputs_removed=True).items():
            exact(admission_receipt[name], expected, 'separate real-input admission')
    else:
        require(terminal['status'] == 'failed' and terminal['exit_code'] != 0, 'failed terminal closure')
    return dict(executed=complete, physical_s3_measured=False, vendor_win=False)


def collect(s3, prefix, out, instance_id, commit, digest):
    launch, close = (json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json', 'aws-closeout.json'))
    exact(close['nodes'], launch['nodes'], 'collection SAME IDs')
    exact(launch['instance_id'], instance_id, 'collection host')
    exact(launch['prefix'], prefix, 'collection prefix')
    with patch.multiple(ids, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, replay=replay):
        return ids.collect(s3, prefix, out, instance_id, commit, digest)


def main(attempt):
    require(re.fullmatch(r'a[0-9]{4}', attempt), 'attempt must be aNNNN')
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()  # No cloud before frozen authorities and qualified bodies.
        shared, _ = ids.lifecycle()
        return shared.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


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
        controller_path = base/'metadata-only-controller.json'
        with patch.object(module, 'CONFIG', controller_path):
            for mode in ('valid-static', 'pending', 'code-tamper', 'ref-tamper', 'archive-size'):
                changed = copy.deepcopy(controller)
                if mode == 'pending': changed['authority_pending'] = True
                if mode == 'code-tamper': changed['code_sha256'][CODE[0]] = '0'*64
                if mode == 'ref-tamper': changed['refs']['receipt']['sha256'] = '0'*64
                if mode == 'archive-size': changed['native']['source_archive']['bytes'] = 1
                controller_path.write_bytes(local.canonical(changed))
                if mode == 'valid-static':
                    qualify(here, native_files=False)
                else:
                    rejects(lambda: qualify(here, native_files=False))
            controller_path.write_bytes(local.canonical(controller))
            rejects(lambda: qualify(here), 'UNSUPPLIED-real-archive')
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
                         ('aws-reservation.json', dict(source_commit=host['source_commit'], source_archive_sha256=host['source_archive_sha256'], qualification=proof))]:
                ids.write(collected/n, v)
            def fetch(**kw):
                b = local.canonical(terminal) if kw['Key'].endswith('/terminal.json') else uploaded[kw['Key'].split('/artifacts/', 1)[1]]
                return dict(Body=io.BytesIO(b))
            collect(Mock(get_object=fetch), host['prefix'], collected, host['instance_id'], host['source_commit'], host['source_archive_sha256'])
            require(replay(collected)['executed'], 'actual completed collection/replay')
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


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--self-check']:
            self_check()
        elif len(sys.argv) == 5 and sys.argv[1] == '--stage':
            print(json.dumps(stage(*sys.argv[2:])))
        elif len(sys.argv) == 3 and sys.argv[1] == '--replay':
            print(json.dumps(replay(sys.argv[2])))
        else:
            require(len(sys.argv) == 2, __doc__)
            with open('/tmp/borsuk-hierarchical-100k-launch.lock', 'a+') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
                main(sys.argv[1])
    except Exception as error:
        print('INVALID: '+str(error), file=sys.stderr)
        sys.exit(2)
