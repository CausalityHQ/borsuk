#!/usr/bin/env python3
"""Run the fixed Cohere D1024 100k/k10 native correctness preflight, serially.

CLI: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check
     --owned-stage SPEC SPEC_SHA256 RECEIPT   (internal re-exec of probe.native_stage)

Thin local glue only: every native call runs through the existing bounded
systemd supervisor (probe.native_stage / local.run_stage). Python never opens
the network, decodes Parquet, vectors, ground truth or results; it reads native
receipts/stdout metadata and stream-authenticates opaque bytes. Quality is never
interpreted: terminal status only says whether the native chain closed.
"""
import copy
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_hierarchical_global_leaf_probe as probe
from scripts import run_source_witness_paired_coverage as witness

local, positive, publication = probe.local, probe.positive, probe.publication
require, exact, fields = local.require, local.exact, local.fields
SCHEMA = 'borsuk-cohere-native-preflight-v1'
DATASET = 'CohereLabs/wikipedia-2023-11-embed-multilingual-v3'
REVISION = 'ade45fb52bd549f5e8c065636fe4160a43c2af36'
ROWS, DIMS, QUERIES, K = 100000, 1024, 1000, 10
SHARDS = (('en/0000.parquet', 216_612_385), ('en/0001.parquet', 216_746_705))
GIB = 1 << 30
ROLES = ('preparer', 'sq8', 'generation', 'publisher', 'baseline')
PHASES = ('prepare', 'normalize', 'fit', 'sq8', 'generation', 'publish', 'baseline')
OUTPUT_NAMES = ('corpus.f32', 'queries.f32', 'corpus.ids.jsonl', 'queries.ids.jsonl', 'truth.u64')
PREPARER_LIMITS = ('modeled_memory_bytes max_footer_bytes max_row_group_rows max_row_group_compressed_bytes '
                   'max_row_group_uncompressed_bytes batch_rows max_batch_bytes max_id_bytes').split()
PUBLISHER_LIMITS = ('max_memory_bytes max_active_queries max_query_bytes max_query_gets max_parallel_gets '
                    'max_source_bytes max_source_gets max_parallel_source_gets max_query_scratch_bytes '
                    'already_pinned_bytes').split()
BUILD = dict(cpu_affinity=[0, 1, 2, 3], memory_max_bytes=8 * GIB, scratch_max_bytes=8 * GIB, max_log_bytes=16 << 20)
QUERY = dict(cpu_affinity=[0], memory_max_bytes=512 << 20, scratch_max_bytes=8 * GIB, max_log_bytes=16 << 20)
PHASE_MAX = dict(prepare=2400, normalize=600, fit=1800, sq8=600, generation=1200, publish=600, baseline=1800)
DEADLINE_MAX = 9600
# Admitted gate per binary role: preparer/publisher/baseline come from the current-source six-stage
# gate; the SQ8 builder and generation builder reuse their closed prior-source gates.
GATE_OF = dict(preparer='native_publisher', publisher='native_publisher', baseline='native_publisher',
               sq8='sq8_builder', generation='generation_builder')
GATE_STAGES = dict(native_publisher=6)
STORE_PREFIX = 'semantic/index'
NUMBER = re.compile(r'-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?')
body = witness.body


def sizes():
    return {'corpus.f32': ROWS * DIMS * 4, 'queries.f32': QUERIES * DIMS * 4, 'truth.u64': QUERIES * K * 8}


def admit(config):
    """Pinned root admission receipt: closed/terminated gates and exact binary roles, before ANY native call."""
    admission = local.read_json(config['admission'], 64 << 10)
    fields(admission, 'schema status source_identity_sha256 prior_source gates roles', 'root admission receipt')
    exact(admission['schema'], SCHEMA + '-admission', 'admission schema')
    exact(admission['status'], 'READY_NATIVE_PREFLIGHT', 'admission ready status')
    current = config['source_identity_sha256']
    exact(admission['source_identity_sha256'], current, 'admission binds the current full source identity')
    prior = admission['prior_source']
    fields(prior, 'identity_sha256 relevant_source_unchanged', 'prior source reuse record')
    local.digest(prior['identity_sha256'])
    exact(prior['relevant_source_unchanged'], True, 'prior-source reuse explicitly unchanged')
    fields(admission['gates'], ' '.join(sorted(set(GATE_OF.values()))), 'admitted gates')
    for name, gate in admission['gates'].items():
        fields(gate, 'session instance_id source_identity_sha256 stage_count stage_exit_statuses terminated receipt', 'admitted gate ' + name)
        local.integer(gate['session'], 1, 1 << 40, 'gate session')
        require(type(gate['instance_id']) is str and re.fullmatch(r'[A-Za-z0-9-]{1,64}', gate['instance_id']), 'gate instance identity')
        exact(gate['source_identity_sha256'], current if name == 'native_publisher' else prior['identity_sha256'], 'gate source identity: ' + name)
        count = local.integer(gate['stage_count'], 1, 64, 'gate stage count')
        if name in GATE_STAGES:
            exact(count, GATE_STAGES[name], 'complete gate stage roster: ' + name)
        require(type(gate['stage_exit_statuses']) is list and len(gate['stage_exit_statuses']) == count
                and all(type(code) is int and code == 0 for code in gate['stage_exit_statuses']), 'all admitted gate stages exit 0: ' + name)
        exact(gate['terminated'], True, 'original gate instance terminated: ' + name)
        local.authenticate(gate['receipt'], 1 << 20)
    fields(admission['roles'], ' '.join(ROLES), 'admitted binary roles')
    digests = set()
    for role in ROLES:
        entry = admission['roles'][role]
        fields(entry, 'gate binary', 'admitted role ' + role)
        exact(entry['gate'], GATE_OF[role], 'role/gate binding: ' + role)
        fields(entry['binary'], 'path bytes sha256 local_basename', 'admitted binary Artifact ' + role)
        require(type(entry['binary']['path']) is str and Path(entry['binary']['path']).is_absolute(), 'admitted original path')
        local.integer(entry['binary']['bytes'], 1, 256 << 20, 'admitted binary bytes'); local.digest(entry['binary']['sha256'])
        configured = config['binaries'][role]
        exact(body(configured), body(entry['binary']), 'configured binary bytes/SHA equal admitted role: ' + role)
        exact(Path(configured['path']).name, entry['binary']['local_basename'], 'root-chosen local basename for role: ' + role)
        digests.add(entry['binary']['sha256'])
    require(len(digests) == len(ROLES), 'distinct admitted binary roles')
    return admission


def qualify(config):
    fields(config, 'schema run_id source_identity_sha256 admission binaries shards '
           'document_id_column preparer_limits payload_cap_bytes publisher_limits phase_seconds deadline_seconds', 'preflight config')
    exact(config['schema'], SCHEMA + '-config', 'preflight config schema')
    require(type(config['run_id']) is str and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', config['run_id']), 'run ID')
    local.digest(config['source_identity_sha256'])
    fields(config['binaries'], ' '.join(ROLES), 'five qualified binary roles')
    for role in ROLES:
        local.pointer(config['binaries'][role], 256 << 20)
    admission = admit(config)  # pending/failed/unqualified/role-swapped bodies stop here, before any native call
    for role in ROLES:
        local.authenticate(config['binaries'][role], 256 << 20)
        require(os.access(config['binaries'][role]['path'], os.X_OK), 'qualified binary executable: ' + role)
    require(len({config['binaries'][r]['path'] for r in ROLES}) == len(ROLES), 'distinct binary paths')
    require(type(config['shards']) is list and len(config['shards']) == len(SHARDS), 'two whole local shards')
    for shard, (name, size) in zip(config['shards'], SHARDS):
        fields(shard, 'publisher_path path bytes sha256', 'whole shard descriptor')
        exact(shard['publisher_path'], name, 'ordered publisher shard path')
        local.pointer(dict(path=shard['path'], bytes=shard['bytes'], sha256=shard['sha256']), 256 << 20)
        exact(shard['bytes'], size, 'pinned whole-shard byte count')
    require(len({s['path'] for s in config['shards']}) == len(SHARDS), 'distinct shard paths')
    column = config['document_id_column']
    require(type(column) is str and 0 < len(column) <= 256 and column != 'emb', 'document ID column')
    fields(config['preparer_limits'], ' '.join(PREPARER_LIMITS), 'explicit preparer limits')
    fields(config['publisher_limits'], ' '.join(PUBLISHER_LIMITS), 'explicit publisher limits')
    for limits in (config['preparer_limits'], config['publisher_limits']):
        for value in limits.values():
            local.integer(value, 0, 1 << 62, 'explicit native limit')
    local.integer(config['payload_cap_bytes'], 1 << 20, 8 * GIB, 'native payload cap')
    fields(config['phase_seconds'], ' '.join(PHASES), 'per-phase deadlines')
    for name, value in config['phase_seconds'].items():
        local.integer(value, 6, PHASE_MAX[name], 'phase seconds within the root proposal maximum')
    local.integer(config['deadline_seconds'], 6, DEADLINE_MAX, 'whole-run deadline within the root proposal maximum')
    return admission


def local_etag(path):
    """object_store 0.14.1 local.rs get_etag: quoted lowercase hex inode-mtime_us-size."""
    metadata = os.stat(path, follow_symlinks=False)
    require(stat.S_ISREG(metadata.st_mode), 'staged SQ8 must be a regular file')
    return '"%x-%x-%x"' % (metadata.st_ino, metadata.st_mtime_ns // 1000, metadata.st_size)


def stdout_text(record, cap=1 << 20):
    return local.authenticate(record['native_log'], cap, read=True).decode()


def stdout_json(record, cap=1 << 20):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate native JSON field')
            result[key] = value
        return result
    text = stdout_text(record, cap)
    require(text.endswith('\n') and text.count('\n') == 1, 'one native JSON line')
    # Numbers stay raw tokens: low/step are forwarded without float reserialization.
    value = json.loads(text, object_pairs_hook=pairs, parse_float=str, parse_int=str)
    require(type(value) is dict, 'native JSON object')
    return value


def tokens(value):
    require(type(value) is list and len(value) == DIMS and all(type(t) is str and NUMBER.fullmatch(t) for t in value),
            'exact D1024 raw numeric coefficient tokens')
    return value


def closure(record, name, command, resources, budget):
    """Original exit, exact argv, drained inactive unit, actual cgroup/no-swap/no-OOM closure."""
    for key, expected in dict(name=name, exit_status=0, closed=True, unit_drained=True,
                              cgroup_drained=True, resource_gate_passed=True).items():
        exact(record[key], expected, 'owned native closure: ' + key)
    exact(record['command'], command, 'exact native invocation')
    exact(record['unit_closeout']['MainPID'], '0', 'unit has no live PID')
    require(record['unit_closeout']['ActiveState'] in ('inactive', 'failed'), 'unit inactive')
    inner = local.read_json(record['native_receipt'], 1 << 20)
    exact(inner['status'], 'CLOSED', 'native supervisor status'); exact(inner['complete'], True, 'native supervisor complete')
    exact(len(inner['stages']), 1, 'one serial native process')
    stage = inner['stages'][0]
    for key, expected in dict(name=name, command=command, exit_status=0, cleanup_complete=True, resource_gate_passed=True).items():
        exact(stage[key], expected, 'inner native stage: ' + key)
    memory = resources['memory_max_bytes']
    probe.validate_cgroup(inner['cgroup'], memory, 100 * len(resources['cpu_affinity']))
    for group in (stage['cgroup_before'], stage['cgroup_after']):
        require(0 < int(group['memory.max']) <= memory and int(group['memory.peak']) <= memory, 'actual native memory bound')
        exact(group['memory.swap.max'], '0', 'actual native swap cap'); exact(int(group['memory.swap.peak']), 0, 'actual native noSwap peak')
        exact(group['cpu_affinity'], resources['cpu_affinity'], 'actual native CPU affinity')
    probe.no_oom(stage['cgroup_before'], stage['cgroup_after'])
    require(0 <= stage['wall_seconds'] <= record['wall_seconds'] <= budget, 'native phase deadline')


def nonzero_native_exit(stages):
    """Native exit of a cleanly closed unit, else None (resource/env failures stay INVALID)."""
    try:
        record = stages[-1]
        require(record['closed'] and record['unit_drained'] and record['cgroup_drained'], 'unit drained')
        stage = local.read_json(record['native_receipt'], 1 << 20)['stages'][0]
        require(stage['cleanup_complete'] is True and type(stage['exit_status']) is int and 0 < stage['exit_status'] < 128, 'plain native exit')
        probe.no_oom(stage['cgroup_before'], stage['cgroup_after'])
        exact(stage['cgroup_after']['memory.swap.peak'], '0', 'noSwap')
        return stage['exit_status']
    except (KeyError, IndexError, TypeError, ValueError, OSError):
        return None


def inventory(output):
    result = {}
    for path in sorted(output.rglob('*')):
        positive.regular_path(path)
        if path.is_dir() or path == output / 'terminal.json':
            continue
        result[str(path.relative_to(output))] = body(local.identity(path))
    return result


def finish(output, receipt):
    local.write_json(output / 'execution-receipt.json', receipt)
    roster = inventory(output)
    for path in sorted(output.rglob('*'), reverse=True):
        if path.is_dir():
            probe.fsync_dir(path)
    probe.fsync_dir(output)
    closed = receipt['status'] == 'NATIVE_CHAIN_CLOSED'
    terminal = dict(schema=SCHEMA + '-terminal', status=receipt['status'], complete=receipt['complete'],
        execution_exit_code=0 if closed else 2, config=receipt.get('config'),
        inventory=roster, execution_receipt=roster['execution-receipt.json'])
    local.write_json(output / 'terminal.json', terminal)
    probe.fsync_dir(output)  # Terminal is the final created artifact.
    return terminal


def execute(config_path, config_sha, output):
    output = positive.regular_path(output)
    require(not output.is_relative_to(probe.ORIGINAL_ROOT), 'new output outside shared legacy helper scratch')
    output.mkdir(parents=False, exist_ok=False)  # Never mutate or resume an old attempt.
    receipt = dict(schema=SCHEMA + '-receipt', output=str(output), status='INVALID', complete=False,
                   stages=[], calls=[], sampled_peak_scratch_bytes=0)
    started = time.monotonic()
    previous_term = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('runner terminated')))
    try:
        config, pin = local.load_config(config_path, config_sha)
        qualify(config)
        deadline = started + config['deadline_seconds']
        receipt['config'] = witness.retain(pin, output / 'config.json')
        receipt['source_identity_sha256'] = config['source_identity_sha256']
        receipt['admission'] = witness.retain(config['admission'], output / 'admission.json')
        receipt['durability'] = ('only the publish receipt file and its parent directory are fsynced by the publisher; a LocalFileSystem '
                                 'receipt readback does not guarantee that published object bodies were fsynced')
        binaries = receipt['binaries'] = config['binaries']
        cap = str(config['payload_cap_bytes'])
        for folder in ('configs', 'scratch', 'store'):
            (output / folder).mkdir()

        def check():
            scratch = local.directory_bytes(output)
            receipt['sampled_peak_scratch_bytes'] = max(receipt['sampled_peak_scratch_bytes'], scratch)
            require(scratch <= 8 * GIB, 'outer 8Gi scratch bound')
            require(time.monotonic() < deadline, 'outer run deadline')

        def call(name, role, argv, config_body, resources):
            cfg_pin = probe.copy_bytes(output / 'configs' / (name + '.json'), config_body)
            binary = binaries[role]
            command = [binary['path'], *argv]
            budget = config['phase_seconds'][name]
            limits = dict(resources, timeout_seconds=budget)
            with patch.object(probe, '__file__', str(Path(__file__).resolve())):
                record = probe.native_stage(name, command, binary, cfg_pin, output, limits, budget, deadline, receipt['stages'], check)
            closure(record, name, command, resources, budget)
            receipt['calls'].append(dict(name=name, role=role, command=command, config=cfg_pin,
                closure=local.identity(output / (name + '-closure.json')), log=record['native_log']))
            check()
            return record

        def spec(name, role, argv):
            return local.canonical(dict(schema=SCHEMA + '-invocation', name=name, role=role, argv=argv))

        # 1. Native preparation. The preparer receives its pinned actual output parent.
        parent = os.stat(output)
        prepared = output / 'prepared'
        prepare_config = local.canonical(dict(schema='borsuk-cohere-native-cohort-config-v1', dataset=DATASET,
            revision=REVISION, embedding_column='emb', document_id_column=config['document_id_column'],
            shards=[dict(publisher_path=s['publisher_path'], path=s['path'], bytes=s['bytes'], sha256=s['sha256']) for s in config['shards']],
            output_parent=dict(path=str(output), device=parent.st_dev, inode=parent.st_ino),
            resources=dict(cpu_limit=4, actual_memory_bytes=8 * GIB, modeled_memory_bytes=config['preparer_limits']['modeled_memory_bytes'],
                swap_bytes=0, scratch_bytes=8 * GIB, timeout_seconds=2400,
                **{k: v for k, v in config['preparer_limits'].items() if k != 'modeled_memory_bytes'})))
        cfg_sha = local.sha(prepare_config)
        call('prepare', 'preparer', [str(output / 'configs/prepare.json'), cfg_sha, str(prepared)], prepare_config, BUILD)
        complete = local.read_json(local.identity(prepared / 'complete.json'), 128 << 10)
        exact(complete['schema'], 'borsuk-cohere-native-cohort-receipt-v1', 'native cohort receipt schema')
        exact(complete['status'], 'COMPLETE', 'native cohort receipt complete')
        exact(complete['geometry'], dict(corpus_rows=ROWS, query_rows=QUERIES, dimensions=DIMS, k=K,
            corpus_source_ordinals=[0, ROWS], query_source_ordinals=[ROWS, ROWS + QUERIES]), 'fixed cohort geometry')
        exact(complete['config']['sha256'], cfg_sha, 'native receipt binds the generated config')
        outputs = complete['outputs']
        require(type(outputs) is list and [o.get('name') for o in outputs] == list(OUTPUT_NAMES), 'exact native output roster')
        pins = {}
        for item in outputs:
            fields(item, 'name bytes sha256', 'native output seal')
            pins[item['name']] = dict(path=str(prepared / item['name']), bytes=item['bytes'], sha256=item['sha256'])
            if item['name'] in sizes():
                exact(item['bytes'], sizes()[item['name']], 'exact fixed byte count: ' + item['name'])
            local.authenticate(pins[item['name']], 1 << 30)  # stream SHA/EOF; contents never decoded
        receipt['prepared'] = pins

        # 2. Existing Rust normalization, ordinary fit and SQ8 builder (no hier-fit).
        corpus = pins['corpus.f32']
        normalized = output / 'normalized.f32'
        argv = ['normalize', corpus['path'], corpus['sha256'], str(ROWS), str(DIMS), cap, str(normalized)]
        record = call('normalize', 'sq8', argv, spec('normalize', 'sq8', argv), BUILD)
        normalized_sha = stdout_json(record)['normalized_sha256']
        local.digest(normalized_sha)
        normalized_pin = local.identity(normalized)
        exact(normalized_pin['sha256'], normalized_sha, 'normalized source identity')
        exact(normalized_pin['bytes'], ROWS * DIMS * 4, 'normalized source exact byte count')
        order = output / 'order.u64'
        argv = ['fit', normalized_pin['path'], normalized_sha, str(ROWS), str(DIMS), cap, str(order)]
        record = call('fit', 'sq8', argv, spec('fit', 'sq8', argv), BUILD)
        fit = stdout_json(record)
        exact(fit['rows'], str(ROWS), 'fit row count')
        order_pin = local.identity(order)
        exact(order_pin['bytes'], ROWS * 8, 'order byte count'); exact(order_pin['sha256'], fit['order_sha256'], 'order identity')
        sq8 = output / 'sq8.bin'
        argv = [normalized_pin['path'], normalized_sha, str(DIMS), order_pin['path'], order_pin['sha256'], cap, str(sq8)]
        record = call('sq8', 'sq8', argv, spec('sq8', 'sq8', argv), BUILD)
        built = stdout_json(record)
        low, step = tokens(built['low']), tokens(built['step'])
        exact(built['rows'], str(ROWS), 'SQ8 row count')
        sq8_pin = local.identity(sq8)
        exact(sq8_pin['bytes'], ROWS * (DIMS + 12), 'SQ8 byte count'); exact(sq8_pin['sha256'], built['sq8_sha256'], 'SQ8 identity')

        # 3. Stage SQ8 at its FINAL LocalFileSystem key, then derive the ETag from that file.
        key = 'semantic/objects/' + sq8_pin['sha256']
        staged = output / 'store' / key
        staged.parent.mkdir(parents=True)
        with positive.open_input(sq8_pin['path']) as stream:
            before = positive.stamp(stream)
            publication.transfer(stream, body(sq8_pin), staged)
            exact(positive.stamp(stream), before, 'stable SQ8 transport')
        for folder in (staged.parent, staged.parent.parent, output / 'store'):
            probe.fsync_dir(folder)
        etag = local_etag(staged)
        receipt['staged_sq8'] = dict(local.identity(staged), object_key=key, etag=etag)

        # 4. Generation build. low/step keep their native raw number tokens.
        generation = output / 'generation'
        generation_config = json.dumps(dict(discovery='semantic', semantic_profile='native100k',
            order=dict(path=order_pin['path'], sha256=order_pin['sha256']), raw=normalized_pin['path'], raw_sha256=normalized_sha,
            sq8=sq8_pin['path'], sq8_sha256=sq8_pin['sha256'], rows=ROWS, dimensions=DIMS, generation=1, base_epoch=0,
            low='@LOW@', step='@STEP@', sq8_object_key=key, sq8_etag=etag), sort_keys=True, separators=(',', ':'))
        generation_config = generation_config.replace('"@LOW@"', '[' + ','.join(low) + ']').replace('"@STEP@"', '[' + ','.join(step) + ']')
        generation_config = (generation_config + '\n').encode()
        require(len(generation_config) <= 65536, 'native generation builder config cap')
        cfg_sha = local.sha(generation_config)
        argv = [str(output / 'configs/generation.json'), cfg_sha, cap, str(generation)]
        record = call('generation', 'generation', argv, generation_config, BUILD)
        root_sha = stdout_text(record).strip()
        local.digest(root_sha)
        exact(local.identity(generation / 'manifest.json')['sha256'], root_sha, 'native generation root identity')

        # 5. Publication through the existing library API into the LocalFileSystem store.
        publish_config = local.canonical(dict(schema='borsuk-two-bit-local-publication-config-v1',
            root=dict(path=str(generation), sha256=root_sha), store_root=str(output / 'store'),
            prefix=STORE_PREFIX, limits=config['publisher_limits']))
        publish_sha = local.sha(publish_config)
        published = output / 'publish-receipt.json'
        argv = [str(output / 'configs/publish.json'), publish_sha, str(published)]
        call('publish', 'publisher', argv, publish_config, BUILD)  # success log is legitimately empty
        native = local.read_json(local.identity(published), 64 << 10)
        fields(native, 'schema config_sha256 prefix metadata_prefix root_sha256 generation control_epoch', 'native publication receipt')
        for key_, expected in dict(schema='borsuk-two-bit-local-publication-receipt-v1', config_sha256=publish_sha,
                prefix=STORE_PREFIX, root_sha256=root_sha, generation=1).items():
            exact(native[key_], expected, 'native publication receipt: ' + key_)
        require(type(native['metadata_prefix']) is str and re.fullmatch(r'[A-Za-z0-9_./-]{1,512}', native['metadata_prefix'])
                and type(native['control_epoch']) is int, 'native publication prefix/epoch')
        plane = local.read_json(local.identity(generation / 'plane/manifest.json'), 1 << 20)
        exact(plane['source_order_sha256'], order_pin['sha256'], 'generation binds the fitted order')

        # 6. All-query baseline on the original unnormalized requests and independent truth.
        baseline_config = local.canonical(dict(schema='borsuk-cohere-native-baseline-config-v1', dataset=DATASET,
            revision=REVISION, metric='cosine', tie_rule='corpus_ordinal_ascending', corpus_source_first=0,
            query_source_first=ROWS, rows=ROWS, dimensions=DIMS, count=QUERIES, k=K, profile='native100k',
            store_root=str(output / 'store'), generation_prefix=native['metadata_prefix'], generation_root_sha256=root_sha,
            scratch_parent=str(output / 'scratch'),
            requests=body_path(pins['queries.f32']), truth=body_path(pins['truth.u64']),
            native_source=dict(source_sha256=normalized_sha, sq8_sha256=sq8_pin['sha256'], source_order_sha256=plane['source_order_sha256']),
            max_memory_bytes=QUERY['memory_max_bytes']))
        baseline_sha = local.sha(baseline_config)
        result = output / 'baseline-result.jsonl'
        argv = [str(output / 'configs/baseline.json'), baseline_sha, str(result)]
        try:
            call('baseline', 'baseline', argv, baseline_config, QUERY)
        except BaseException as error:
            code = nonzero_native_exit(receipt['stages'])
            if code is None:
                raise
            # Clean closed unit, plain native exit: record it verbatim, never interpret quality.
            receipt.update(status='BASELINE_NONZERO_EXIT', complete=True, native_exit_status=code,
                error=str(error), error_type=type(error).__name__)
            if result.exists():
                receipt['baseline_result'] = local.identity(result)
        else:
            receipt['baseline_result'] = local.identity(result)  # opaque bytes, never decoded
            receipt['status'] = 'NATIVE_CHAIN_CLOSED'
        require(all(s['closed'] and s['unit_drained'] and s['cgroup_drained'] for s in receipt['stages']), 'all native units drained')
        for role in ROLES:
            local.authenticate(binaries[role], 256 << 20)
        local.authenticate(receipt['config'], local.CONFIG_CAP)
        check()
        receipt['complete'] = True
    except BaseException as error:
        if receipt['status'] != 'BASELINE_NONZERO_EXIT':
            receipt.update(status='INVALID', complete=False)
        receipt.update(error=str(error), error_type=type(error).__name__)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
    receipt.update(wall_seconds=time.monotonic() - started,
        cleanup=dict(native_units_drained=bool(receipt['stages']) and all(
            s.get('unit_drained', False) and s.get('cgroup_drained', False) for s in receipt['stages']),
            native_processes_concurrent_max=1 if receipt['stages'] else 0, partial_artifacts_preserved=True))
    return finish(output, receipt)


def body_path(pin):
    return dict(path=pin['path'], bytes=pin['bytes'], sha256=pin['sha256'])


FIXTURE = r'''import hashlib, json, os, re, struct, sys
from pathlib import Path
S = os.environ.get('PREFLIGHT_FIXTURE', '')
G = json.loads(os.environ['PREFLIGHT_GEOMETRY'])
ROWS, DIMS, QUERIES, K = G
role = Path(sys.argv[0]).name
a = sys.argv[1:]
h = lambda b: hashlib.sha256(b).hexdigest()
def emit(text): sys.stdout.write(text); sys.stdout.flush()
def data(n, salt): return bytes((i * 7 + salt) % 251 for i in range(n))
def fail(code): sys.exit(code)
if S == role + '-exit3': fail(3)
if role == 'preparer':
    cfg_bytes = Path(a[0]).read_bytes(); assert h(cfg_bytes) == a[1]
    cfg = json.loads(cfg_bytes); out = Path(a[2]); parent = os.stat(out.parent)
    assert out.parent == Path(cfg['output_parent']['path']) and (parent.st_dev, parent.st_ino) == (cfg['output_parent']['device'], cfg['output_parent']['inode'])
    assert cfg['resources']['cpu_limit'] == 4 and cfg['resources']['actual_memory_bytes'] == 8 << 30 and len(cfg['shards']) == 2
    out.mkdir()
    files = {'corpus.f32': data(ROWS * DIMS * 4, 1), 'queries.f32': data(QUERIES * DIMS * 4, 2),
             'corpus.ids.jsonl': b'{"id":0}\n', 'queries.ids.jsonl': b'{"id":1}\n', 'truth.u64': data(QUERIES * K * 8, 3)}
    if S == 'prep-short': files['corpus.f32'] = files['corpus.f32'][:-1]
    seals = []
    for name, payload in files.items():
        (out / name).write_bytes(payload)
        seals.append(dict(name=name, bytes=len(payload), sha256=h(payload) if not (S == 'prep-sha' and name == 'queries.f32') else '0' * 64))
    receipt = dict(schema='borsuk-cohere-native-cohort-receipt-v1', status='COMPLETE', outputs=seals,
        config=dict(path=a[0], bytes=len(cfg_bytes), sha256=a[1]),
        geometry=dict(corpus_rows=ROWS, query_rows=QUERIES, dimensions=DIMS, k=K, corpus_source_ordinals=[0, ROWS], query_source_ordinals=[ROWS, ROWS + QUERIES]))
    (out / 'complete.json').write_text(json.dumps(receipt))
    emit(json.dumps(dict(status='COMPLETE', receipt_sha256=h(json.dumps(receipt).encode()))) + '\n')
elif role == 'sq8':
    if a[0] == 'normalize':
        src = Path(a[1]).read_bytes(); assert h(src) == a[2] and a[3:6] == [str(ROWS), str(DIMS), a[5]]
        out = src[::-1][:-1] if S == 'norm-short' else src[::-1]
        Path(a[6]).write_bytes(out)
        emit(json.dumps(dict(normalized_sha256='0' * 64 if S == 'norm-lie' else h(out), query_or_truth_used=False)) + '\n')
    elif a[0] == 'fit':
        assert h(Path(a[1]).read_bytes()) == a[2] and a[3:5] == [str(ROWS), str(DIMS)]
        order = b''.join(struct.pack('<Q', i) for i in reversed(range(ROWS)))
        Path(a[6]).write_bytes(order)
        emit(json.dumps(dict(order_sha256=h(order), rows=ROWS, recipe='borsuk-semantic-order-chacha8-f32-v1', query_or_truth_used=False)) + '\n')
    else:
        assert h(Path(a[0]).read_bytes()) == a[1] and a[2] == str(DIMS) and h(Path(a[3]).read_bytes()) == a[4]
        payload = data(ROWS * (DIMS + 12) - (1 if S == 'sq8-short' else 0), 5); Path(a[6]).write_bytes(payload)
        low = ','.join(['-1.5e-05', '0.30000001', '1.0000001', '-0.1'][i % 4] for i in range(DIMS))
        step = ','.join(['0.0039215689', '2e-3', '1', '0.12345679'][i % 4] for i in range(DIMS))
        emit('{"low":[' + low + '],"step":[' + step + '],"sq8_sha256":"' + h(payload) + '","rows":' + str(ROWS) + ',"query_or_truth_used":false}\n')
elif role == 'generation':
    raw = Path(a[0]).read_bytes(); assert h(raw) == a[1]
    cfg = json.loads(raw.decode())
    assert set(cfg) == set('discovery semantic_profile order raw raw_sha256 sq8 sq8_sha256 rows dimensions generation base_epoch low step sq8_object_key sq8_etag'.split())
    assert cfg['discovery'] == 'semantic' and cfg['semantic_profile'] == 'native100k' and (cfg['rows'], cfg['dimensions'], cfg['generation'], cfg['base_epoch']) == (ROWS, DIMS, 1, 0)
    assert len(cfg['low']) == len(cfg['step']) == DIMS and re.fullmatch(r'"[0-9a-f]+-[0-9a-f]+-[0-9a-f]+"', cfg['sq8_etag'])
    assert b'-1.5e-05,0.30000001,1.0000001,-0.1' in raw and b'0.0039215689,0.002' not in raw and b'0.0039215689,2e-3' in raw
    assert Path(cfg['sq8']).stat().st_size == ROWS * (DIMS + 12) and cfg['sq8_object_key'].endswith(cfg['sq8_sha256'])
    out = Path(a[3]); (out / 'plane').mkdir(parents=True)
    manifest = json.dumps(dict(fixture='generation', rows=ROWS)).encode()
    (out / 'manifest.json').write_bytes(manifest)
    (out / 'plane/manifest.json').write_text(json.dumps(dict(source_order_sha256=cfg['order']['sha256'])))
    emit(('0' * 64 if S == 'gen-lie' else h(manifest)) + '\n')
elif role == 'publisher':
    raw = Path(a[0]).read_bytes(); assert h(raw) == a[1]
    cfg = json.loads(raw)
    assert cfg['schema'] == 'borsuk-two-bit-local-publication-config-v1' and cfg['prefix'] == 'semantic/index'
    assert set(cfg['limits']) == set('max_memory_bytes max_active_queries max_query_bytes max_query_gets max_parallel_gets max_source_bytes max_source_gets max_parallel_source_gets max_query_scratch_bytes already_pinned_bytes'.split())
    assert h((Path(cfg['root']['path']) / 'manifest.json').read_bytes()) == cfg['root']['sha256']
    assert any((Path(cfg['store_root']) / 'semantic/objects').iterdir())
    with open(a[2], 'x') as f:
        json.dump(dict(schema='borsuk-two-bit-local-publication-receipt-v1', config_sha256=a[1], prefix=cfg['prefix'],
            metadata_prefix='semantic/index/generations/g1', root_sha256='1' * 64 if S == 'pub-mismatch' else cfg['root']['sha256'],
            generation=1, control_epoch=1), f)
else:
    raw = Path(a[0]).read_bytes(); assert h(raw) == a[1]
    cfg = json.loads(raw)
    assert cfg['generation_prefix'] == 'semantic/index/generations/g1' and cfg['max_memory_bytes'] == 512 << 20
    assert (cfg['rows'], cfg['dimensions'], cfg['count'], cfg['k'], cfg['query_source_first']) == (ROWS, DIMS, QUERIES, K, ROWS)
    assert cfg['requests']['path'].endswith('prepared/queries.f32') and cfg['truth']['path'].endswith('prepared/truth.u64')
    assert os.path.isdir(cfg['scratch_parent']) and os.path.isdir(cfg['store_root'])
    Path(a[2]).write_text('{"q":0}\n')
    status = 'INVALID' if S == 'baseline-exit2' else 'MEASURED'
    emit(json.dumps(dict(status=status)) + '\n')
    if S == 'baseline-exit2': fail(2)
'''


def self_check():
    """Real orchestration and shared run_stage; only systemd and cgroup files are mocked."""
    import hashlib
    from contextlib import ExitStack
    module = sys.modules[__name__]
    geometry = (6, 4, 2, 3)
    with tempfile.TemporaryDirectory(prefix='cohere-preflight-glue-') as folder, ExitStack() as stack:
        root = Path(folder)
        stack.enter_context(patch.object(module, 'ROWS', geometry[0]))
        stack.enter_context(patch.object(module, 'DIMS', geometry[1]))
        stack.enter_context(patch.object(module, 'QUERIES', geometry[2]))
        stack.enter_context(patch.object(module, 'K', geometry[3]))
        stack.enter_context(patch.object(module, 'SHARDS', (('en/0000.parquet', 11), ('en/0001.parquet', 13))))
        stack.enter_context(patch.dict(os.environ, dict(BORSUK_GLOBAL_LEAF_SLICE='borsuk-global-leaf-synthetic.slice',
            PREFLIGHT_GEOMETRY=json.dumps(geometry), PREFLIGHT_FIXTURE='')))
        binaries = {}
        for role in ROLES:
            path = root / 'bin' / role
            probe.copy_bytes(path, ('#!' + sys.executable + '\n' + FIXTURE + '\n# role ' + role + '\n').encode()); path.chmod(0o700)
            binaries[role] = local.identity(path)
        shards = []
        for (name, size), index in zip(module.SHARDS, range(2)):
            shards.append(dict(publisher_path=name, **probe.copy_bytes(root / 'shards' / ('s%d.parquet' % index), bytes([index]) * size)))
        gates = {}; (root / 'gates').mkdir()
        for gate_name, (session, source, stages) in dict(native_publisher=(80402, 'a' * 64, 6),
                sq8_builder=(89644, 'b' * 64, 1), generation_builder=(12009, 'b' * 64, 6)).items():
            receipt_pin = local.write_json(root / 'gates' / (gate_name + '.json'), dict(synthetic_gate=gate_name))
            gates[gate_name] = dict(session=session, instance_id='i-' + gate_name.replace('_', '-'), source_identity_sha256=source,
                stage_count=stages, stage_exit_statuses=[0] * stages, terminated=True, receipt=receipt_pin)
        admission = dict(schema=SCHEMA + '-admission', status='READY_NATIVE_PREFLIGHT', source_identity_sha256='a' * 64,
            prior_source=dict(identity_sha256='b' * 64, relevant_source_unchanged=True), gates=gates,
            roles={role: dict(gate=GATE_OF[role], binary=dict(path='/original/' + role + '-built', bytes=binaries[role]['bytes'],
                sha256=binaries[role]['sha256'], local_basename=role)) for role in ROLES})
        config = dict(schema=SCHEMA + '-config', run_id='synthetic-a0001',
            source_identity_sha256='a' * 64, admission=local.write_json(root / 'admission.json', admission), binaries=binaries, shards=shards,
            document_id_column='_id', payload_cap_bytes=1 << 28,
            preparer_limits=dict(modeled_memory_bytes=1 << 30, max_footer_bytes=1 << 20, max_row_group_rows=100000,
                max_row_group_compressed_bytes=1 << 28, max_row_group_uncompressed_bytes=1 << 30, batch_rows=1024,
                max_batch_bytes=1 << 30, max_id_bytes=1024),
            publisher_limits=dict(max_memory_bytes=1 << 29, max_active_queries=1, max_query_bytes=16773120, max_query_gets=32,
                max_parallel_gets=16, max_source_bytes=64 << 20, max_source_gets=128, max_parallel_source_gets=16,
                max_query_scratch_bytes=400000, already_pinned_bytes=0),
            phase_seconds={p: 60 for p in PHASES}, deadline_seconds=600)
        group = {'path': '/synthetic', 'memory.peak': '1048576', 'memory.swap.max': '0', 'memory.swap.peak': '0',
                 'memory.events': 'oom 0\noom_kill 0\noom_group_kill 0', 'pids.max': '512', 'cgroup.procs': str(os.getpid()),
                 'cpu.stat': 'usage_usec 0'}
        def snapshot(limits):
            return dict(group, **{'memory.max': str(limits['memory_max_bytes']), 'cpu_affinity': limits['cpu_affinity']})
        def inner_snapshot(_path, memory, cpu, tasks=512):
            return dict(group, **{'memory.max': str(memory), 'cpu.max': '%d 100000' % (cpu * 1000), 'cpu_affinity': []})
        stack.enter_context(patch.object(local, 'resource_snapshot', snapshot))
        stack.enter_context(patch.object(probe, 'cgroup_snapshot', inner_snapshot))
        supervisors, drain = [], {'bad': False}
        def fake_popen(command, **kwargs):
            assert command[0] == 'systemd-run'
            supervisors.append(command)
            pos = command.index('--owned-stage')
            _, pin = local.load_config(command[pos + 1], command[pos + 2])
            try:
                witness.owned_stage(pin, command[pos + 3]); code = 0
            except BaseException:
                code = 1
            return SimpleNamespace(poll=lambda: code, wait=lambda timeout=None: code, pid=os.getpid())
        def fake_run(command, **kwargs):
            if command[1] in ('stop', 'kill'):
                return subprocess.CompletedProcess(command, 0)
            bad = drain['bad']
            return subprocess.CompletedProcess(command, 0, stdout='ActiveState=' + ('active' if bad else 'inactive') +
                '\nSubState=dead\nMainPID=' + ('1' if bad else '0') + '\nControlGroup=\nLoadState=loaded\n')
        stack.enter_context(patch.object(probe, 'subprocess', SimpleNamespace(Popen=fake_popen, run=fake_run,
            DEVNULL=subprocess.DEVNULL, STDOUT=subprocess.STDOUT, TimeoutExpired=subprocess.TimeoutExpired)))
        names = [p for p in PHASES]
        spy = []
        real_native_stage = probe.native_stage
        def native_stage_spy(*args, **kwargs):
            spy.append(args[0]); return real_native_stage(*args, **kwargs)
        stack.enter_context(patch.object(probe, 'native_stage', native_stage_spy))

        def attempt(label, scenario='', changed=None, expected='INVALID', count=None, bad_drain=False, admitted=None):
            os.environ['PREFLIGHT_FIXTURE'] = scenario; drain['bad'] = bad_drain; supervisors.clear(); spy.clear()
            cfg = copy.deepcopy(config)
            if admitted:
                body = copy.deepcopy(admission); admitted(body)
                cfg['admission'] = local.write_json(root / (label + '-admission.json'), body)
            if changed:
                changed(cfg)
            pin = local.write_json(root / (label + '-config.json'), cfg)
            out = root / label
            terminal = execute(pin['path'], pin['sha256'], out)
            receipt = local.read_json(local.identity(out / 'execution-receipt.json'), 8 << 20)
            assert terminal['status'] == expected, (label, receipt.get('error'), terminal['status'])
            assert terminal['complete'] == (expected != 'INVALID') and terminal['execution_exit_code'] == (0 if expected == 'NATIVE_CHAIN_CLOSED' else 2)
            assert terminal['inventory'] == inventory(out) and 'terminal.json' not in terminal['inventory']
            if count is not None:
                assert [s['name'] for s in receipt['stages']] == names[:count], (label, [s['name'] for s in receipt['stages']])
                assert spy == names[:count] and len(supervisors) == count, 'native opener spy'
            return out, receipt

        good, receipt = attempt('success', expected='NATIVE_CHAIN_CLOSED', count=7)
        assert [c['name'] for c in receipt['calls']] == names and receipt['cleanup']['native_units_drained']
        # Roles, resources and affinity are the root-supplied seams, never widened for the query phase.
        for command, name in zip(supervisors, names):
            shell = ' '.join(command)
            cpus, memory = ('0', 536870912) if name == 'baseline' else ('0,1,2,3', 8 * GIB)
            assert '--property=MemoryMax=%d' % memory in shell and '--property=MemorySwapMax=0' in shell
            assert '--property=CPUQuota=%d%%' % (100 * len(cpus.split(','))) in shell and ' taskset -c %s ' % cpus in shell, name
            assert str(Path(__file__).resolve()) in command
        etag = receipt['staged_sq8']['etag']
        assert re.fullmatch(r'"[0-9a-f]+-[0-9a-f]+-[0-9a-f]+"', etag) and etag == local_etag(good / 'store' / receipt['staged_sq8']['object_key'])
        stat_ = os.stat(good / 'store' / receipt['staged_sq8']['object_key'])
        assert etag == '"%x-%x-%x"' % (stat_.st_ino, stat_.st_mtime_ns // 1000, stat_.st_size)
        for role_ in (name for name in names if name != 'baseline'):
            assert receipt['calls'][names.index(role_)]['closure']['bytes'] > 0
        assert receipt['calls'][names.index('publish')]['log']['bytes'] == 0, 'silent success log authenticated, not skipped'
        assert receipt['baseline_result']['bytes'] > 0 and receipt['status'] == 'NATIVE_CHAIN_CLOSED'
        # Partial evidence, native/exit/resource/identity failures: INVALID and no later native stage.
        for label, scenario, count in (('prep-exit', 'preparer-exit3', 1), ('prep-short', 'prep-short', 1), ('prep-sha', 'prep-sha', 1),
                ('norm-lie', 'norm-lie', 2), ('norm-short', 'norm-short', 2), ('sq8-short', 'sq8-short', 4), ('gen-lie', 'gen-lie', 5),
                ('gen-exit', 'generation-exit3', 5), ('pub-mismatch', 'pub-mismatch', 6), ('pub-exit', 'publisher-exit3', 6)):
            out, partial = attempt(label, scenario, count=count)
            assert partial['error'] and (out / 'terminal.json').exists() and not (out / 'baseline-result.jsonl').exists()
        # fit shares the sq8 binary: exit3 hits the first sq8-role call (normalize).
        attempt('norm-exit', 'sq8-exit3', count=2)
        attempt('drain', changed=None, bad_drain=True, count=1)
        out, partial = attempt('baseline-exit2', 'baseline-exit2', expected='BASELINE_NONZERO_EXIT', count=7)
        assert partial['native_exit_status'] == 2 and partial['error'] and partial['baseline_result']['bytes'] > 0
        # Pending/pin/limit negatives fail before any native call.
        def mutate(path, value):
            def edit(cfg):
                target = cfg
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
            return edit
        for label, edit in (('pending-shortcut', mutate(['authority_pending'], False)), ('binary', mutate(['binaries', 'baseline', 'sha256'], '0' * 64)),
                ('shard-size', mutate(['shards', 0, 'bytes'], 12)), ('shard-order', mutate(['shards', 0, 'publisher_path'], 'en/0001.parquet')),
                ('column', mutate(['document_id_column'], 'emb')), ('unknown', mutate(['extra'], 1)),
                ('limit-type', mutate(['publisher_limits', 'max_query_gets'], True)), ('phase', mutate(['phase_seconds', 'fit'], 1)),
                ('admission-pin', mutate(['admission', 'sha256'], '0' * 64)), ('cap', mutate(['payload_cap_bytes'], 1)),
                ('phase-over', mutate(['phase_seconds', 'fit'], 1801)), ('deadline-over', mutate(['deadline_seconds'], 9601)),
                ('identity', mutate(['source_identity_sha256'], 'c' * 64))):
            attempt(label, changed=edit, count=0)
        def admission_edit(path, value):
            def edit(body):
                target = body
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
            return edit
        def swap(body):
            roles = body['roles']
            roles['preparer']['binary'], roles['baseline']['binary'] = roles['baseline']['binary'], roles['preparer']['binary']
        for label, edit in (('adm-pending', admission_edit(['status'], 'PENDING')), ('adm-failed', admission_edit(['status'], 'FAILED')),
                ('adm-source', admission_edit(['source_identity_sha256'], 'c' * 64)),
                ('adm-prior', admission_edit(['prior_source', 'relevant_source_unchanged'], False)),
                ('adm-prior-type', admission_edit(['prior_source', 'relevant_source_unchanged'], 1)),
                ('adm-exit', admission_edit(['gates', 'native_publisher', 'stage_exit_statuses'], [0, 0, 0, 0, 0, 1])),
                ('adm-exit-bool', admission_edit(['gates', 'native_publisher', 'stage_exit_statuses'], [False] * 6)),
                ('adm-stages', admission_edit(['gates', 'native_publisher', 'stage_count'], 5)),
                ('adm-alive', admission_edit(['gates', 'sq8_builder', 'terminated'], False)),
                ('adm-gate-source', admission_edit(['gates', 'sq8_builder', 'source_identity_sha256'], 'a' * 64)),
                ('adm-gate-pointer', admission_edit(['gates', 'generation_builder', 'receipt', 'sha256'], '0' * 64)),
                ('adm-role-gate', admission_edit(['roles', 'sq8', 'gate'], 'native_publisher')),
                ('adm-role-sha', admission_edit(['roles', 'generation', 'binary', 'sha256'], 'd' * 64)),
                ('adm-basename', admission_edit(['roles', 'publisher', 'binary', 'local_basename'], 'other')),
                ('adm-role-swap', swap), ('adm-unknown', admission_edit(['extra'], 1)),
                ('adm-missing-role', lambda body: body['roles'].pop('baseline'))):
            attempt(label, admitted=edit, count=0)
        before = inventory(good)
        pin = local.identity(root / 'success-config.json')
        try:
            execute(pin['path'], pin['sha256'], good)
        except FileExistsError:
            pass
        else:
            raise AssertionError('existing output overwritten')
        assert inventory(good) == before, 'nooverwrite original attempt'
        assert len(tokens(['-1.5e-05'] * geometry[1])) == geometry[1]
        for bad in (['1'] * (geometry[1] - 1), ['nan'] * geometry[1], [1.0] * geometry[1], ['0x1'] * geometry[1]):
            assert _raises(lambda: tokens(bad)), bad
    print('PASS native preflight glue: seven serial native calls through the real supervisor/run_stage; roles/CPU/memory seams; '
          'silent publisher log authenticated; exact ETag formula; raw coefficient tokens; fail-closed pending/pin/size/SHA/exit/drain/'
          'baseline-nonzero/nooverwrite negatives; terminal-last inventory. systemd/cgroup and native programs are MOCKED; no corpus/GT/network/Rust.')


def _raises(fn):
    try:
        fn()
    except ValueError:
        return True
    return False


def main(args):
    try:
        if len(args) == 4 and args[0] == '--owned-stage':
            _, pin = local.load_config(args[1], args[2])
            print(json.dumps(witness.owned_stage(pin, args[3]), sort_keys=True)); return 0
        if args == ['--self-check']:
            self_check(); return 0
        require(len(args) == 3, 'usage: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check')
        terminal = execute(*args)
        print(json.dumps(dict(status=terminal['status'], complete=terminal['complete'], output=str(args[2])), sort_keys=True))
        return terminal['execution_exit_code']
    except Exception as error:
        print(type(error).__name__ + ': ' + str(error), file=sys.stderr); return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
