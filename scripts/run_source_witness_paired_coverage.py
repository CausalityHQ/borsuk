#!/usr/bin/env python3
"""Fixed native retained100k consumed64 source-witness representation test.

CLI: CONFIG CONFIG_SHA256 REPO NEW_OUTPUT | --self-check
     --replay CONFIG CONFIG_SHA256 REPO OUTPUT
No transport, launcher, layout rebuild, tuning, or Python truth evaluation.
"""
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_hierarchical_global_leaf_probe as probe
from scripts.check_native_startup_build import source_hashes, source_identity

local, positive, publication = probe.local, probe.positive, probe.publication
require, exact, fields = local.require, local.exact, local.fields
BASE = 'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/'
AUTHORITY = dict(path=BASE+'retained-input-authority.json', bytes=3711,
    sha256='93a6f221e895d400ff217e57cc5188d0aa475146ba94cd68876e41325b02a743')
WORKSPACE = dict(path=BASE+'implementation-gates/a0001/workspace-receipt.json', bytes=52056,
    sha256='064f19872eb2239b04e859487a17c3685a169728ab84b536fb34429fc2c18966')
SOURCE_ID = '2e8145e3639b3c4bf93f363dca110ff16f374bee4057e8426738fc02dc51941f'
SOURCE_COUNT = 402
SOURCE_PATHS = dict(module='crates/borsuk/src/hierarchical_semantic_cells.rs',
                    binary='crates/borsuk/src/bin/hierarchical_semantic_cells.rs')
SCHEMA = 'borsuk-source-witness-paired-coverage-v1'
DATASETS = ('relaion', 'cohere')
LAYOUT_FILES = ('manifest.json', 'directories.bin', 'cells.bin')
PANEL_FILES = ('requests64', 'truth64')
RESOURCES = dict(memory_max_bytes=512 << 20, scratch_max_bytes=16 << 30,
    swap_bytes=0, cpu_affinity=[0], per_dataset_seconds=600, max_log_bytes=16 << 20, tasks_max=512)
LIMITS = dict(max_cells=24, max_cell_gets=24, max_cell_bytes=16 << 20, max_query_payload_bytes=128 << 20)
OPTIONS = dict(LIMITS, fetch_policy='whole_cell', primary_beam=8, boundary_beam=24, blocks_per_cell=16,
    max_source_gets=24, max_source_bytes=16 << 20, max_refinement_gets=384, max_refinement_bytes=16 << 20)
NATIVE_SCHEMAS = {m: 'borsuk-source-witness-'+s+'-v1' for m, s in (
    ('build-probes', 'build'), ('nominate-probes', 'nomination'), ('diagnose-probes', 'diagnostic'))}
ORDER = [d+'-'+m for m in ('build-probes', 'nominate-probes', 'diagnose-probes') for d in DATASETS]
GATE_SCHEMA = 'borsuk-source-witness-paired-coverage-admission-v1'
SUPERVISOR_SCHEMA = 'borsuk-source-witness-coverage-supervisor-v1'


def body(pin):
    return probe.body_pin(pin)


def retain(pin, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with positive.open_input(pin['path']) as stream:
        before = positive.stamp(stream)
        publication.transfer(stream, body(pin), destination)
        exact(positive.stamp(stream), before, 'stable opaque input transport')
    probe.fsync_dir(destination.parent)
    return dict(pin, path=str(destination))


def qualify(config, repo):
    fields(config, 'schema run_id authority binary inputs diagnostics resources', 'fixed experiment config')
    exact(config['schema'], SCHEMA, 'experiment schema')
    require(type(config['run_id']) is str and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', config['run_id']), 'run ID')
    exact(config['authority'], AUTHORITY, 'frozen retained authority')
    exact(config['resources'], RESOURCES, 'fixed native CPU1/512MiB/noSwap/600s and outer scratch')
    for name, value in RESOURCES.items():
        exact(type(config['resources'][name]), type(value), 'resource field type')
    require(all(type(cpu) is int for cpu in config['resources']['cpu_affinity']), 'integer native CPU affinity')
    require(type(config['diagnostics']) is bool, 'prospective diagnostic decision')
    authority = probe.ref(repo, AUTHORITY)
    exact(authority['schema'], 'borsuk-source-witness-retained-input-authority-v1', 'retained authority schema')
    exact(authority['source_qualification_pending'], False, 'completed native qualification')
    workspace = probe.ref(repo, WORKSPACE)
    qualification = probe.ref(repo, authority['native_qualification'])
    for receipt in (qualification, workspace):
        exact(receipt['qualified'], True, 'qualified original native source')
        exact(receipt['source_file_count'], SOURCE_COUNT, 'all native source files')
        exact(receipt['source_identity_sha256'], SOURCE_ID, 'qualified native source identity')
        exact([s['stage'] for s in receipt['stages']], list(local.GATES), 'completed native assurance roster')
        for stage in receipt['stages']:
            exact(stage['exit_status'], 0, 'qualified native gate exit0')
            exact(stage['gate_status'], 0, 'qualified native gate passed')
    exact(qualification['integrated_native_bytes_match'], True, 'integrated owned2 match')
    exact(qualification['original_exit_status'], 0, 'original qualification exit0')
    exact(qualification['instance_state'], 'terminated', 'original qualification terminated')
    exact(workspace['source_unchanged'], True, 'qualification source unchanged')
    exact(workspace['exit_status'], 0, 'original workspace exit0')
    sources = source_hashes(Path(repo))
    exact(sources, workspace['source_sha256'], 'all qualified source bytes unchanged')
    exact(source_identity(sources), SOURCE_ID, 'current all-source identity')
    pins = dict(authority=AUTHORITY, workspace=WORKSPACE, qualification=authority['native_qualification'],
                method=authority['method'], historical_terminal=authority['terminal'])
    for pin in pins.values():
        probe.ref(repo, pin, read=True)
    method = probe.ref(repo, authority['method'])
    for key, expected in dict(rows=100000, dimensions=768, first=0, count=64, k=100, metric='cosine').items():
        exact(method[key], expected, 'frozen consumed64 geometry')
    fields(config['binary'], 'path bytes sha256', 'native binary')
    exact(body(config['binary']), authority['native_binary']['uncompressed'], 'qualified binary pin')
    require(Path(config['binary']['path']).is_absolute(), 'absolute qualified binary path')
    fields(config['inputs'], ' '.join(DATASETS), 'paired input datasets')
    for dataset in DATASETS:
        fields(config['inputs'][dataset], ' '.join(LAYOUT_FILES+PANEL_FILES), 'exact retained input roster')
        for name, pin in config['inputs'][dataset].items():
            fields(pin, 'path bytes sha256', 'retained input artifact')
            require(type(pin['path']) is str and Path(pin['path']).is_absolute(), 'absolute input path')
            exact(body(pin), body(authority['datasets'][dataset][name]), 'exact retained bytes/SHA')
    return authority, sources, pins


def native_config(mode, dataset, inputs, sidecars, output, gate=None):
    config = dict(schema=NATIVE_SCHEMAS[mode], candidate_root=inputs[dataset]['manifest.json'],
                  max_resident_directory_payload_bytes=64 << 20, max_result_bytes=16 << 20)
    if mode == 'build-probes':
        config['probe_output'] = str(output/'probes'/(dataset+'.bin'))
    else:
        config.update(dataset=dataset, probes=sidecars[dataset], requests=inputs[dataset]['requests64'],
            truth=inputs[dataset]['truth64'], truth_width=100, first=0, count=64,
            max_resident_probe_payload_bytes=64 << 20, max_evaluator_payload_bytes=64 << 20)
        if mode == 'nominate-probes':
            config['limits'] = LIMITS
        else:
            config.update(top_k=100, options=OPTIONS, coverage_gate=gate)
    return config


def report(path, config, config_pin, sources):
    """Replay raw native identity, all64 durable prefix and post-freeze summaries."""
    local.authenticate(local.identity(path), config['max_result_bytes'])
    mode = next(m for m, s in NATIVE_SCHEMAS.items() if s == config['schema'])
    expected = dict(schema=config['schema'], config_sha256=config_pin['sha256'],
        candidate_root_sha256=config['candidate_root']['sha256'],
        module_source_sha256=sources[SOURCE_PATHS['module']], binary_source_sha256=sources[SOURCE_PATHS['binary']])
    if mode != 'build-probes':
        expected.update(dataset=config['dataset'], probes_sha256=config['probes']['sha256'],
            requests_sha256=config['requests']['sha256'], truth=config['truth'], truth_width=100)
    prefix, prefix_bytes, selections, hits = hashlib.sha256(), 0, [], []
    marker = terminal = sealed = identity = None
    with positive.open_input(path) as stream:
        while line := stream.readline((8 << 20)+1):
            require(len(line) <= 8 << 20 and line.endswith(b'\n'), 'bounded complete native event')
            event = local.decode(line); phase = event['phase']
            require(terminal is None, 'native terminal last')
            if identity is None:
                exact(phase, 'identity', 'native identity first')
                for name, value in expected.items():
                    exact(event[name], value, 'native source/config/input identity: '+name)
                exact(event['truth_opened'], False, 'truth unopened at identity')
                identity = event
                if mode == 'build-probes':
                    exact(event['requests_opened'], False, 'source-only builder requests unopened')
                else:
                    for name, value in dict(first=0, count=64, witness_policy=16, selected_cells=24,
                            hierarchy_prefilter=False, fallback_widening=False, scientific_qualification=False).items():
                        exact(event[name], value, 'fixed representation policy')
                    exact(event['limits'] if mode == 'nominate-probes' else event['options'],
                          LIMITS if mode == 'nominate-probes' else OPTIONS, 'fixed native policy caps')
                    if mode == 'diagnose-probes':
                        exact(event['coverage_gate_sha256'], config['coverage_gate']['sha256'], 'native gate binding')
            elif mode == 'build-probes':
                if phase == 'source_probes_sealed':
                    require(sealed is None, 'one source-only sidecar seal')
                    sealed = event['receipt']['artifact']
                else:
                    exact(phase, 'terminal', 'builder event order'); terminal = event
                exact(event['truth_opened'], False, 'builder truth unopened')
                exact(event['requests_opened'], False, 'builder requests unopened')
            elif phase == 'selection_frozen':
                require(marker is None and len(selections) < 64, '64 selections before freeze/GT')
                exact(event['ordinal'], len(selections), 'complete ordered native selection panel')
                exact(event['truth_opened'], False, 'selection truth unopened')
                nomination = event['receipt'] if mode == 'nominate-probes' else event['trace']['nomination']
                exact(len(nomination['selected']), 24, 'native top24')
                ids = nomination['covered_ids']
                require(ids == sorted(set(ids)) and all(type(n) is int and 0 <= n < 100000 for n in ids), 'sorted unique selected source IDs')
                for kind in ('directory', 'whole_cell', 'source', 'refinement'):
                    stats = nomination['accounting'][kind]
                    for key in ('submitted_gets', 'requested_bytes', 'verified_bytes', 'failed_gets'):
                        exact(stats[key], 0, 'coverage selection no payload query IO')
                selections.append(local.sha(local.canonical(dict(selected=nomination['selected'], covered_ids=ids))))
            elif phase == 'all_selections_frozen':
                require(marker is None and len(selections) == 64, 'complete all64 durable freeze')
                for name, value in dict(expected, first=0, count=64, selection_receipts=64, truth_opened=False,
                        prefix_bytes=prefix_bytes, prefix_sha256=prefix.hexdigest()).items():
                    exact(event[name], value, 'native frozen prefix binding: '+name)
                marker = event
            elif phase in ('coverage_attribution', 'loss_attribution'):
                require(marker is not None and len(hits) < 64, 'GT attribution after all64 freeze')
                exact(phase, 'coverage_attribution' if mode == 'nominate-probes' else 'loss_attribution', 'correct attribution')
                exact(event['ordinal'], len(hits), 'all64 attribution ordinals')
                exact(event['truth_sha256'], config['truth']['sha256'], 'exact truth attribution')
                hit = event['selected_hits'] if mode == 'nominate-probes' else event['loss']['returned_hits']
                local.integer(hit, 0, 100, 'k100 attribution hit')
                if mode == 'nominate-probes':
                    exact(event['selected_misses'], 100-hit, 'k100 misses'); exact(event['truth_width'], 100, 'k100 width')
                else:
                    exact(event['loss']['local_nomination_misses'], 0, 'all16 nomination loss zero')
                    exact(event['final_ranking_loss_remeasured'], True, 'native final ranking remeasured')
                hits.append(hit)
            else:
                exact(phase, 'terminal', 'native event order')
                require(marker is not None and len(hits) == 64, 'complete frozen and attributed terminal')
                terminal = event
            if mode != 'build-probes' and marker is None:
                prefix.update(line); prefix_bytes += len(line)
    require(terminal is not None, 'original complete native terminal')
    exact(terminal['schema'], config['schema'], 'native terminal schema'); exact(terminal['complete'], True, 'native complete')
    if mode == 'build-probes':
        exact(terminal['status'], 'SOURCE_PROBES_SEALED', 'source-only builder complete')
        exact(terminal['artifact'], sealed, 'builder sealed terminal artifact')
        exact(sealed['path'], config['probe_output'], 'actual sidecar output path')
        return dict(status=terminal['status'], sidecar=sealed)
    for name, value in dict(config_sha256=config_pin['sha256'], queries=64, truth_opened=True,
                           requires_matching_supervisor_exit_receipt=True, scientific_qualification=False).items():
        exact(terminal[name], value, 'native terminal closure')
    mean, p05 = sum(hits)/6400, sorted(hits)[3]
    passed = mean >= .98 and p05 >= 95
    if mode == 'nominate-probes':
        for name, value in dict(expected, prefix_bytes=marker['prefix_bytes'], prefix_sha256=marker['prefix_sha256'],
                cell_payload_query_gets=0, cell_payload_query_bytes=0, quality_or_performance_claim=False).items():
            if name not in ('module_source_sha256', 'binary_source_sha256'):
                exact(terminal[name], value, 'native coverage terminal binding')
        exact(terminal['quality'], dict(mean_selected_cell_coverage=mean, p05_hits=p05, passed=passed), 'coverage arithmetic/gate')
    else:
        exact(terminal['mean_recall_at_100'], mean, 'returned mean arithmetic')
        exact(terminal['p05_hits'], p05, 'returned p05 arithmetic')
        exact(terminal['all16_blocks_admitted'], True, 'fixed full-ranking diagnostic')
        exact(terminal['final_ranking_loss_remeasured'], True, 'ranking loss remeasured')
    expected_status = ('COVERAGE_PASS' if mode == 'nominate-probes' else 'RETURNED_DIAGNOSTIC_PASS') if passed else 'FAIL'
    exact(terminal['status'], expected_status, 'scientific gate from all64 hits')
    return dict(status=expected_status, mean=mean, p05_hits=p05, selections=selections)


def physical(output, runtime_root, pin):
    origin = Path(pin['path'])
    require(origin.is_relative_to(runtime_root), 'retained runtime artifact is inside original output')
    return dict(pin, path=str(output/origin.relative_to(runtime_root)))


def authenticate_log(pin, cap):
    # Successful new native modes emit only their JSONL file, so raw stdout can
    # be empty. The shared artifact parser deliberately admits positive sizes.
    require(type(pin['bytes']) is int and 0 <= pin['bytes'] <= cap, 'raw log byte cap')
    with positive.open_input(pin['path']) as stream:
        exact(positive.digest(stream), body(pin), 'exact retained raw log including empty bytes')


def owned_stage(spec_pin, receipt_path):
    run_stage = local.run_stage
    def silent_stage(*args):
        try:
            return run_stage(*args)
        except ValueError as error:
            stages = args[-1]
            if str(error) != 'invalid artifact bytes' or not stages:
                raise
            stage = stages[-1]
            if not (stage['exit_status'] == 0 and stage['cleanup_complete'] and stage['resource_gate_passed']
                    and stage['log'] is not None and stage['log']['bytes'] == 0):
                raise
            authenticate_log(stage['log'], args[5]['max_log_bytes'])
            return b''
    # Only the shared helper's final empty-stdout read differs. Its native
    # spawn, deadlines, cgroups, RSS, kill/wait and original exit remain intact.
    with patch.object(local, 'run_stage', silent_stage):
        return probe.owned_stage(spec_pin, receipt_path)


def stage_closure(output, runtime_root, item, binary):
    """Require original exit, logs, actual resources and inactive drained unit."""
    read = lambda p: local.read_json(physical(output, runtime_root, p), 1 << 20)
    record = read(item['closure']); name = item['name']
    for key, expected in dict(name=name, exit_status=0, closed=True, unit_drained=True,
                             cgroup_drained=True, resource_gate_passed=True).items():
        exact(record[key], expected, 'original owned native process closure')
    command = [binary['path'], item['mode'], item['config']['path'], item['config']['sha256'], item['report']['path']]
    exact(record['command'], command, 'original exact native invocation')
    exact(record['unit_closeout']['MainPID'], '0', 'original unit no live PID')
    require(record['unit_closeout']['ActiveState'] in ('inactive', 'failed'), 'original unit inactive')
    inner = read(record['native_receipt'])
    exact(inner['status'], 'CLOSED', 'original native supervisor status'); exact(inner['complete'], True, 'original native supervisor complete')
    exact(len(inner['stages']), 1, 'one serial native process')
    stage = inner['stages'][0]
    for key, value in dict(name=name, command=command, binary=binary, config=item['config'],
            exit_status=0, cleanup_complete=True, resource_gate_passed=True).items():
        exact(stage[key], value, 'original inner stage/config/binary/exit')
    probe.validate_cgroup(inner['cgroup'], 512 << 20, 100)
    require(len(inner['cgroup']['cgroup.procs'].split()) == 1, 'only supervisor remains after native drain')
    for group in (stage['cgroup_before'], stage['cgroup_after']):
        require(0 < int(group['memory.max']) <= 512 << 20 and int(group['memory.peak']) <= 512 << 20, 'actual native memory bound')
        exact(group['memory.swap.max'], '0', 'actual native swap cap'); exact(int(group['memory.swap.peak']), 0, 'actual native noSwap peak')
        exact(group['cpu_affinity'], [0], 'actual native one-core affinity')
    probe.no_oom(stage['cgroup_before'], stage['cgroup_after'])
    require(0 <= stage['wall_seconds'] <= record['wall_seconds'] <= item['budget_before_seconds'] <= 600, 'cumulative original phase deadline')
    require(math.isfinite(record['wall_seconds']), 'finite native wall time')
    for key, cap in (('sampled_peak_rss_bytes', 512 << 20), ('sampled_peak_scratch_bytes', 16 << 30)):
        local.integer(stage[key], 0, cap, 'observed native RSS/scratch')
    folder = output/'measurement'
    spec = local.read_json(local.identity(folder/(name+'-stage.json')), 65536)
    for key, value in dict(schema='borsuk-global-leaf-owned-stage-v1', name=name, command=command, binary=binary,
            config=item['config'], output=str(Path(runtime_root)/'measurement'), resources=dict(RESOURCES, timeout_seconds=item['budget_before_seconds'])).items():
        exact(spec[key], value, 'original owned-stage spec')
    require(0 < spec['timeout_seconds'] <= item['budget_before_seconds'], 'bounded owned-stage timeout')
    shell = record['supervisor_command']
    for flag in ('--property=MemoryMax=536870912', '--property=MemorySwapMax=0', '--property=CPUQuota=100%',
            '--property=TasksMax=512', '--property=KillMode=control-group', '--setenv=RAYON_NUM_THREADS=1',
            '--setenv=OMP_NUM_THREADS=1', '--setenv=OPENBLAS_NUM_THREADS=1', '--setenv=MKL_NUM_THREADS=1'):
        require(flag in shell, 'original supervisor limit/thread flag')
    local.authenticate(physical(output, runtime_root, record['native_receipt']), 1 << 20)
    for pin in (record['native_log'], record['log'], stage['log']):
        authenticate_log(physical(output, runtime_root, pin), 16 << 20)
    exact(record['native_log'], stage['log'], 'retained native log identity')
    return record


def supervisor(config, item, record):
    return dict(schema=SUPERVISOR_SCHEMA,
        closure=dict(run_id=config['run_id']+'-'+item['name'], config_sha256=item['config']['sha256'],
            report_sha256=item['report']['sha256'], process_exit_code=record['exit_status'], resources_closed=record['closed']),
        resources_passed=record['resource_gate_passed'], drain_closed=record['unit_drained'] and record['cgroup_drained'],
        cleanup_complete=record['closed'] and record['unit_drained'])


def inventory(output):
    result = {}
    for path in sorted(output.rglob('*')):
        positive.regular_path(path)
        if path.is_dir() or path == output/'terminal.json':
            continue
        result[str(path.relative_to(output))] = body(local.identity(path))
    return result


def finish(output, receipt):
    local.write_json(output/'execution-receipt.json', receipt)
    roster = inventory(output)
    for path in sorted(output.rglob('*'), reverse=True):
        if path.is_dir():
            probe.fsync_dir(path)
    probe.fsync_dir(output)
    terminal = dict(schema=SCHEMA+'-terminal', status=receipt['status'], complete=receipt['complete'],
        execution_exit_code=0 if receipt['complete'] else 2, config=receipt.get('config'),
        inventory=roster, execution_receipt=roster['execution-receipt.json'])
    local.write_json(output/'terminal.json', terminal)
    probe.fsync_dir(output)  # Terminal is the final created artifact.
    return terminal


def execute(config_path, config_sha, repo, output):
    output = positive.regular_path(output)
    require(not output.is_relative_to(probe.ORIGINAL_ROOT), 'new experiment output outside shared legacy helper scratch')
    output.mkdir(parents=False, exist_ok=False)  # Never mutate or resume an old attempt.
    receipt = dict(schema=SCHEMA+'-receipt', output=str(output), status='INVALID', complete=False,
                   stages=[], results={}, native_seconds={d: 0. for d in DATASETS}, sampled_peak_scratch_bytes=0)
    started = time.monotonic()
    previous_term = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('runner terminated')))
    try:
        config, pin = local.load_config(config_path, config_sha)
        receipt['config'] = retain(pin, output/'config.json')
        authority, sources, refs = qualify(config, repo)
        receipt['source_identity_sha256'] = SOURCE_ID
        for name, ref in refs.items():
            probe.copy_bytes(output/'authority'/(name+'.json'), probe.ref(repo, ref, read=True))
        local.write_json(output/'authority/current-source.json', sources)
        binary = retain(config['binary'], output/'binary/hierarchical_semantic_cells')
        os.chmod(binary['path'], 0o500); probe.fsync_file(binary['path'])
        receipt['binary'] = binary
        for folder in ('measurement', 'probes', 'coverage'):
            (output/folder).mkdir()
        inputs = {d: {} for d in DATASETS}; sidecars = {}; configs = {}
        receipt.update(inputs=inputs, sidecars=sidecars)
        for dataset in DATASETS:
            for name in LAYOUT_FILES:
                inputs[dataset][name] = retain(config['inputs'][dataset][name], output/'inputs'/dataset/name)
            root = local.read_json(inputs[dataset]['manifest.json'])
            for key, expected in dict(schema='borsuk-hierarchical-cells-resident-v4', rows=100000, dimensions=768).items():
                exact(root[key], expected, 'unchanged retained balanced layout')
        def check():
            scratch = local.directory_bytes(output)
            receipt['sampled_peak_scratch_bytes'] = max(receipt['sampled_peak_scratch_bytes'], scratch)
            require(scratch <= 16 << 30, 'outer 16Gi scratch bound')
            require(time.monotonic()-started < 1800, 'outer experiment deadline')
        def invoke(mode, dataset, cfg_pin):
            name = dataset+'-'+mode; budget = 600-receipt['native_seconds'][dataset]
            require(budget > 5, 'remaining cumulative native dataset budget')
            report_path = output/'measurement'/(name+'-report.jsonl')
            before = len(receipt['stages'])
            try:
                with patch.object(probe, '__file__', str(Path(__file__).resolve())):
                    probe.native_stage(name, [binary['path'], mode, cfg_pin['path'], cfg_pin['sha256'], str(report_path)],
                        binary, cfg_pin, output/'measurement', dict(RESOURCES, timeout_seconds=budget), budget,
                        time.monotonic()+budget, receipt['stages'], check)
            finally:
                if len(receipt['stages']) > before:
                    receipt['native_seconds'][dataset] += receipt['stages'][-1]['wall_seconds']
            item = dict(name=name, dataset=dataset, mode=mode, budget_before_seconds=budget, config=cfg_pin,
                        report=local.identity(report_path), closure=local.identity(output/'measurement'/(name+'-closure.json')))
            # Keep the immutable original closure separate from the replay roster.
            record = stage_closure(output, output, item, binary)
            receipt.setdefault('calls', []).append(item)
            result = report(report_path, configs[name], cfg_pin, sources)
            receipt['results'][name] = result
            check()
            return item, record, result
        for dataset in DATASETS:
            name = dataset+'-build-probes'; cfg = native_config('build-probes', dataset, inputs, sidecars, output)
            configs[name] = cfg
            cfg_pin = local.write_json(output/'measurement'/(name+'-config.json'), cfg)
            _, _, result = invoke('build-probes', dataset, cfg_pin)
            local.authenticate(result['sidecar'], 128 << 20)
            sidecars[dataset] = result['sidecar']
        # Raw panels are opaque transports, kept out of both builder layouts/configs.
        nomination_pins = {}
        for dataset in DATASETS:
            for name in PANEL_FILES:
                inputs[dataset][name] = retain(config['inputs'][dataset][name], output/'panels'/dataset/name)
            name = dataset+'-nominate-probes'; cfg = native_config('nominate-probes', dataset, inputs, sidecars, output)
            configs[name] = cfg
            nomination_pins[dataset] = local.write_json(output/'measurement'/(name+'-config.json'), cfg)
        receipt['nomination_seal'] = local.write_json(output/'nomination-seal.json', dict(schema=SCHEMA+'-nomination-seal',
            config=receipt['config'], source_identity_sha256=SOURCE_ID, sidecars=sidecars, configs=nomination_pins, order=ORDER[:4]))
        probe.fsync_dir(output)
        gate = dict(schema=GATE_SCHEMA)
        for dataset in DATASETS:
            item, record, _ = invoke('nominate-probes', dataset, nomination_pins[dataset])
            close_pin = local.write_json(output/'coverage'/(dataset+'-supervisor.json'), supervisor(config, item, record))
            gate[dataset] = dict(run_id=config['run_id']+'-'+item['name'], result=item['report'], supervisor=close_pin)
        passed = all(receipt['results'][d+'-nominate-probes']['status'] == 'COVERAGE_PASS' for d in DATASETS)
        receipt['status'] = 'COVERAGE_PASS' if passed else 'FAIL'
        if passed:
            gate_pin = local.write_json(output/'coverage/gate.json', gate)
            receipt['coverage_gate'] = gate_pin
            if config['diagnostics']:
                diagnostic_pins = {}
                for dataset in DATASETS:
                    name = dataset+'-diagnose-probes'; cfg = native_config('diagnose-probes', dataset, inputs, sidecars, output, gate_pin)
                    configs[name] = cfg
                    diagnostic_pins[dataset] = local.write_json(output/'measurement'/(name+'-config.json'), cfg)
                receipt['diagnostic_seal'] = local.write_json(output/'diagnostic-seal.json', dict(schema=SCHEMA+'-diagnostic-seal',
                    nomination_seal=receipt['nomination_seal'], coverage_gate=gate_pin, configs=diagnostic_pins, order=ORDER[4:]))
                probe.fsync_dir(output)
                for dataset in DATASETS:
                    _, _, result = invoke('diagnose-probes', dataset, diagnostic_pins[dataset])
                    exact(result['selections'], receipt['results'][dataset+'-nominate-probes']['selections'], 'unchanged source-witness selection at full ranking')
                receipt['status'] = 'RETURNED_DIAGNOSTIC_PASS' if all(receipt['results'][d+'-diagnose-probes']['status'] == 'RETURNED_DIAGNOSTIC_PASS' for d in DATASETS) else 'FAIL'
        exact(source_hashes(Path(repo)), sources, 'qualified source unchanged through closure')
        local.authenticate(receipt['config'], local.CONFIG_CAP)
        for dataset in DATASETS:
            for pin in inputs[dataset].values():
                local.authenticate(pin, 128 << 20)
            local.authenticate(sidecars[dataset], 128 << 20)
        check()
        require(all(s['closed'] and s['unit_drained'] and s['cgroup_drained'] for s in receipt['stages']), 'all original native units drained')
        receipt['complete'] = True
    except BaseException as error:
        receipt.update(status='INVALID', complete=False, error=str(error), error_type=type(error).__name__)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
    receipt.update(wall_seconds=time.monotonic()-started,
        cleanup=dict(native_units_drained=bool(receipt['stages']) and all(s.get('unit_drained', False) and s.get('cgroup_drained', False) for s in receipt['stages']),
                     native_processes_concurrent_max=1 if receipt['stages'] else 0, partial_artifacts_preserved=True))
    return finish(output, receipt)


def replay(config_path, config_sha, repo, output):
    output = positive.regular_path(output)
    config, config_pin = local.load_config(config_path, config_sha)
    authority, sources, refs = qualify(config, repo)
    terminal = local.read_json(local.identity(output/'terminal.json'), 8 << 20)
    exact(terminal['schema'], SCHEMA+'-terminal', 'terminal schema')
    exact(terminal['inventory'], inventory(output), 'exact original terminal-last artifact inventory')
    exact(terminal['execution_receipt'], terminal['inventory']['execution-receipt.json'], 'terminal receipt pin')
    receipt = local.read_json(local.identity(output/'execution-receipt.json'), 8 << 20)
    exact(receipt['schema'], SCHEMA+'-receipt', 'execution receipt schema')
    runtime = positive.regular_path(receipt['output'])
    exact(terminal['config'], receipt['config'], 'terminal original config pin')
    exact(body(receipt['config']), body(config_pin), 'original frozen config SHA')
    exact(local.authenticate(local.identity(output/'config.json'), local.CONFIG_CAP, read=True),
          local.authenticate(config_pin, local.CONFIG_CAP, read=True), 'exact frozen config bytes')
    for name, ref in refs.items():
        exact(body(local.identity(output/'authority'/(name+'.json'))), body(ref), 'retained exact authority bytes')
    exact(local.read_json(local.identity(output/'authority/current-source.json'), 128 << 10), sources, 'retained qualified source identity')
    exact(receipt['source_identity_sha256'], SOURCE_ID, 'original native source identity')
    for key in ('status', 'complete'):
        exact(terminal[key], receipt[key], 'terminal/execution outcome')
    if not receipt['complete']:
        exact(receipt['status'], 'INVALID', 'partial execution invalid')
        exact(terminal['execution_exit_code'], 2, 'partial execution nonzero')
        return terminal  # Inventory authenticates partial artifacts; no gate admitted.
    exact(terminal['execution_exit_code'], 0, 'scientific completion exit0')
    binary = receipt['binary']
    exact(binary['path'], str(runtime/'binary/hierarchical_semantic_cells'), 'retained invoked binary path')
    exact(body(binary), body(config['binary']), 'qualified actual native binary')
    local.authenticate(physical(output, runtime, binary), 256 << 20)
    inputs, sidecars = receipt['inputs'], receipt['sidecars']
    fields(inputs, ' '.join(DATASETS), 'retained paired inputs'); fields(sidecars, ' '.join(DATASETS), 'paired sidecars')
    for dataset in DATASETS:
        fields(inputs[dataset], ' '.join(LAYOUT_FILES+PANEL_FILES), 'unchanged five-file input roster')
        for name, pin in inputs[dataset].items():
            folder = 'inputs' if name in LAYOUT_FILES else 'panels'
            exact(pin['path'], str(runtime/folder/dataset/name), 'retained layout/panel location')
            exact(body(pin), body(authority['datasets'][dataset][name]), 'retained body pin')
            local.authenticate(physical(output, runtime, pin), 128 << 20)
        exact(sidecars[dataset]['path'], str(runtime/'probes'/(dataset+'.bin')), 'sealed sidecar location')
        local.authenticate(physical(output, runtime, sidecars[dataset]), 128 << 20)
    seal = local.read_json(physical(output, runtime, receipt['nomination_seal']))
    expected_seal = dict(schema=SCHEMA+'-nomination-seal', config=receipt['config'], source_identity_sha256=SOURCE_ID,
        sidecars=sidecars, configs={d: next(i['config'] for i in receipt['calls'] if i['name'] == d+'-nominate-probes') for d in DATASETS}, order=ORDER[:4])
    exact(seal, expected_seal, 'both nomination configs and sidecars sealed before queries')
    require(0 <= receipt['sampled_peak_scratch_bytes'] <= 16 << 30 and 0 < receipt['wall_seconds'] < 1800, 'whole scratch/deadline closure')
    exact(receipt['cleanup'], dict(native_units_drained=True, native_processes_concurrent_max=1, partial_artifacts_preserved=True), 'serial native cleanup/retention')
    calls = receipt['calls']; require(len(calls) in (4, 6), 'four calls or six admitted calls')
    exact([i['name'] for i in calls], ORDER[:len(calls)], 'original serial native call roster')
    exact([s['name'] for s in receipt['stages']], ORDER[:len(calls)], 'original supervisor stage roster')
    totals = {d: 0. for d in DATASETS}; results = {}; closures = {}
    for item, observed in zip(calls, receipt['stages']):
        name, dataset, mode = item['name'], item['dataset'], item['mode']
        exact(name, dataset+'-'+mode, 'dataset/mode original call identity')
        exact(item['budget_before_seconds'], 600-totals[dataset], 'cumulative per-dataset budget')
        expected_cfg = native_config(mode, dataset, inputs, sidecars, runtime, receipt.get('coverage_gate'))
        exact(item['config']['path'], str(runtime/'measurement'/(name+'-config.json')), 'original native config path')
        exact(item['report']['path'], str(runtime/'measurement'/(name+'-report.jsonl')), 'original native result path')
        cfg_body = local.authenticate(physical(output, runtime, item['config']), 65536, read=True)
        exact(cfg_body, local.canonical(expected_cfg), 'exact canonical native config bytes')
        record = stage_closure(output, runtime, item, binary)
        exact(record, observed, 'original supervisor closure retained unchanged')
        totals[dataset] += record['wall_seconds']; require(totals[dataset] <= 600, 'cumulative native budget passed')
        local.authenticate(physical(output, runtime, item['report']), 16 << 20)
        result = report(physical(output, runtime, item['report'])['path'], expected_cfg, item['config'], sources)
        exact(result, receipt['results'][name], 'original native result replay')
        if mode == 'build-probes':
            exact(result['sidecar'], sidecars[dataset], 'source-only builder produced sealed actual sidecar')
        elif mode == 'nominate-probes':
            close = supervisor(config, item, record)
            close_path = output/'coverage'/(dataset+'-supervisor.json')
            exact(local.read_json(local.identity(close_path)), close, 'independent original supervisor gate closure')
            closures[dataset] = dict(run_id=close['closure']['run_id'], result=item['report'],
                supervisor=dict(local.identity(close_path), path=str(runtime/'coverage'/close_path.name)))
        else:
            exact(result['selections'], results[dataset+'-nominate-probes']['selections'], 'routing unchanged for full16 ranking')
        results[name] = result
    exact(totals, receipt['native_seconds'], 'all cumulative native seconds')
    exact(set(results), set(receipt['results']), 'no extra results')
    coverage_pass = all(results[d+'-nominate-probes']['status'] == 'COVERAGE_PASS' for d in DATASETS)
    gate_path = output/'coverage/gate.json'
    exact(gate_path.exists(), coverage_pass, 'gate exists only after BOTH coverage panels pass')
    diagnostic = coverage_pass and config['diagnostics']
    exact(len(calls), 6 if diagnostic else 4, 'only prospectively admitted optional diagnostics')
    if coverage_pass:
        exact(receipt['coverage_gate']['path'], str(runtime/'coverage/gate.json'), 'original native gate path')
        exact(local.read_json(physical(output, runtime, receipt['coverage_gate'])), dict(schema=GATE_SCHEMA, **closures), 'exact gate consumed by native')
    else:
        require('coverage_gate' not in receipt, 'failed coverage never admitted')
    exact((output/'diagnostic-seal.json').exists(), diagnostic, 'diagnostic seal only after both PASS')
    if diagnostic:
        diagnostic_seal = local.read_json(physical(output, runtime, receipt['diagnostic_seal']))
        exact(diagnostic_seal, dict(schema=SCHEMA+'-diagnostic-seal', nomination_seal=receipt['nomination_seal'],
            coverage_gate=receipt['coverage_gate'], configs={d: calls[4+i]['config'] for i, d in enumerate(DATASETS)}, order=ORDER[4:]), 'both diagnostic configs sealed with native gate')
    else:
        require('diagnostic_seal' not in receipt, 'no skipped diagnostic seal')
    status = 'FAIL' if not coverage_pass else 'COVERAGE_PASS'
    if diagnostic:
        status = 'RETURNED_DIAGNOSTIC_PASS' if all(results[d+'-diagnose-probes']['status'] == 'RETURNED_DIAGNOSTIC_PASS' for d in DATASETS) else 'FAIL'
    exact(receipt['status'], status, 'scientific outcome independently replayed')
    return terminal


# Self-check subprocesses produce tiny synthetic receipts. Kernel resource
# evidence inside these receipts is deliberately fake, never qualification.
FIXTURE = r'''import hashlib,json,os,subprocess,sys,time,resource
from pathlib import Path
def enc(v): return (json.dumps(v,sort_keys=True,separators=(',',':'))+'\n').encode()
def sha(b): return hashlib.sha256(b).hexdigest()
def pin(p):
 b=Path(p).read_bytes(); return dict(path=str(p),bytes=len(b),sha256=sha(b))
def write(p,v):
 with Path(p).open('xb') as f: f.write(enc(v)); f.flush(); os.fsync(f.fileno())
scenario=os.environ.get('SOURCE_WITNESS_FIXTURE','')
sources=json.loads(os.environ['SOURCE_WITNESS_FIXTURE_SOURCES'])
if sys.argv[1]=='supervise':
 spec=json.loads(Path(sys.argv[2]).read_bytes()); started=time.monotonic()
 log=Path(spec['output'])/(spec['name']+'.log')
 with log.open('xb') as f:
  p=subprocess.Popen(spec['command'],stdout=f,stderr=subprocess.STDOUT)
  status=p.wait();f.flush();os.fsync(f.fileno())
 before={'path':'/synthetic-native','memory.max':'536870912','memory.peak':'1048576',
  'memory.swap.max':'0','memory.swap.peak':'0','memory.events':'oom 0\noom_kill 0\noom_group_kill 0','cpu_affinity':[0]}
 after=dict(before)
 if 'cohere-nominate' in spec['name']:
  if scenario=='memory': after['memory.peak']='536870913'
  if scenario=='swap': after['memory.swap.peak']='1'
  if scenario=='oom': after['memory.events']='oom 1\noom_kill 0\noom_group_kill 0'
 stage=dict(name=spec['name'],command=spec['command'],binary=spec['binary'],config=spec['config'],
  exit_status=status,cleanup_complete=scenario!='cleanup' or 'cohere-nominate' not in spec['name'],
  resource_gate_passed=status==0,cgroup_before=before,cgroup_after=after,wall_seconds=time.monotonic()-started,
  sampled_peak_rss_bytes=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss*1024,
  sampled_peak_scratch_bytes=1024,log=pin(log))
 if scenario=='config' and 'cohere-nominate' in spec['name']: stage['config']=dict(stage['config'],sha256='0'*64)
 group=dict(after,**{'cpu.max':'100000 100000','pids.max':'512','cgroup.procs':'12345'})
 if scenario=='cpu' and 'cohere-nominate' in spec['name']: group['cpu.max']='200000 100000'
 write(sys.argv[3],dict(status='CLOSED' if status==0 else 'INVALID',complete=status==0,stages=[stage],cgroup=group))
 sys.exit(status)
mode,cfg_path,expected,result=sys.argv[1:]
raw=Path(cfg_path).read_bytes(); assert sha(raw)==expected;cfg=json.loads(raw)
out=Path(result).parents[1];assert not Path(result).exists()
identity=dict(phase='identity',schema=cfg['schema'],config_sha256=expected,
 candidate_root_sha256=cfg['candidate_root']['sha256'],module_source_sha256=sources['module'],
 binary_source_sha256=sources['binary'],truth_opened=False)
events=[]
if mode=='build-probes':
 assert not (out/'panels').exists()
 assert not any(k in cfg for k in ('requests','truth'))
 assert set(p.name for p in Path(cfg['candidate_root']['path']).parent.iterdir())=={'manifest.json','directories.bin','cells.bin'}
 identity['requests_opened']=False;events.append(identity)
 with Path(cfg['probe_output']).open('xb') as f: f.write(b'synthetic sidecar:'+cfg['candidate_root']['sha256'].encode());f.flush();os.fsync(f.fileno())
 artifact=pin(cfg['probe_output'])
 events.extend([dict(phase='source_probes_sealed',receipt=dict(artifact=artifact),truth_opened=False,requests_opened=False),
  dict(phase='terminal',schema=cfg['schema'],status='SOURCE_PROBES_SEALED',complete=True,artifact=artifact,truth_opened=False,requests_opened=False)])
else:
 assert (out/'nomination-seal.json').exists()
 seal=json.loads((out/'nomination-seal.json').read_bytes());assert len(seal['configs'])==2
 for p in seal['configs'].values(): assert pin(p['path'])==p
 if mode=='diagnose-probes':
  assert (out/'diagnostic-seal.json').exists();gate=json.loads(Path(cfg['coverage_gate']['path']).read_bytes())
  assert pin(cfg['coverage_gate']['path'])==cfg['coverage_gate']
  for d in ('relaion','cohere'):
   entry=gate[d];s=json.loads(Path(entry['supervisor']['path']).read_bytes())
   assert pin(entry['result']['path'])==entry['result'] and pin(entry['supervisor']['path'])==entry['supervisor']
   t=json.loads(Path(entry['result']['path']).read_bytes().splitlines()[-1]);assert t['status']=='COVERAGE_PASS'
   assert s['closure']['run_id']==entry['run_id'] and s['closure']['report_sha256']==entry['result']['sha256']
   assert s['closure']['config_sha256']==t['config_sha256'] and s['closure']['process_exit_code']==0
   assert all(s[k] for k in ['resources_passed','drain_closed','cleanup_complete']) and s['closure']['resources_closed']
 identity.update(dataset=cfg['dataset'],probes_sha256=cfg['probes']['sha256'],requests_sha256=cfg['requests']['sha256'],
  truth=cfg['truth'],truth_width=100,first=0,count=64,witness_policy=16,selected_cells=24,
  hierarchy_prefilter=False,fallback_widening=False,scientific_qualification=False)
 identity['limits' if mode=='nominate-probes' else 'options']=cfg['limits' if mode=='nominate-probes' else 'options']
 if mode=='diagnose-probes': identity['coverage_gate_sha256']=cfg['coverage_gate']['sha256']
 events.append(identity)
 if scenario=='timeout' and cfg['dataset']=='cohere' and mode=='nominate-probes': time.sleep(30)
 if scenario=='partial' and cfg['dataset']=='cohere' and mode=='nominate-probes':
  write(result,identity);sys.exit(2)
 stats=dict(submitted_gets=0,requested_bytes=0,verified_bytes=0,failed_gets=0)
 nomination=dict(selected=[dict(cell_id=i,score_bits=0,source_ids=[i]) for i in range(24)],covered_ids=list(range(24)),
  accounting={k:stats for k in ['directory','whole_cell','source','refinement']})
 for i in range(64):
  event=dict(phase='selection_frozen',ordinal=i,truth_opened=False)
  if mode=='nominate-probes': event['receipt']=nomination
  else: event['trace']=dict(nomination=nomination)
  events.append(event)
 prefix=b''.join(map(enc,events));binding={k:identity[k] for k in ['schema','config_sha256','dataset','candidate_root_sha256',
  'probes_sha256','requests_sha256','truth','truth_width','module_source_sha256','binary_source_sha256']}
 marker=dict(binding,phase='all_selections_frozen',first=0,count=64,selection_receipts=64,truth_opened=False,
  prefix_bytes=len(prefix),prefix_sha256=sha(prefix))
 if scenario=='freeze' and cfg['dataset']=='cohere' and mode=='nominate-probes': marker['prefix_sha256']='0'*64
 events.append(marker);hits=[]
 for i in range(64):
  hit=100
  if cfg['dataset']=='cohere':
   if scenario=='fail-mean' and mode=='nominate-probes': hit=97
   if scenario=='fail-p05' and mode=='nominate-probes': hit=94 if i<4 else 99
   if scenario=='fail-returned' and mode=='diagnose-probes': hit=90
  hits.append(hit)
  event=dict(ordinal=i,truth_sha256=cfg['truth']['sha256'])
  if mode=='nominate-probes': event.update(phase='coverage_attribution',selected_hits=hit,selected_misses=100-hit,truth_width=100)
  else: event.update(phase='loss_attribution',loss=dict(returned_hits=hit,local_nomination_misses=0),final_ranking_loss_remeasured=True)
  events.append(event)
 mean=sum(hits)/6400;p05=sorted(hits)[3];passed=mean>=.98 and p05>=95
 terminal=dict(phase='terminal',schema=cfg['schema'],complete=True,config_sha256=expected,queries=64,truth_opened=True,
  requires_matching_supervisor_exit_receipt=True,scientific_qualification=False)
 if mode=='nominate-probes': terminal.update(binding,status='COVERAGE_PASS' if passed else 'FAIL',
  quality=dict(mean_selected_cell_coverage=mean,p05_hits=p05,passed=passed),prefix_bytes=len(prefix),prefix_sha256=sha(prefix),
  cell_payload_query_gets=0,cell_payload_query_bytes=0,quality_or_performance_claim=False)
 else: terminal.update(status='RETURNED_DIAGNOSTIC_PASS' if passed else 'FAIL',mean_recall_at_100=mean,p05_hits=p05,
  all16_blocks_admitted=True,final_ranking_loss_remeasured=True)
 events.append(terminal)
with Path(result).open('xb') as f:
 for event in events: f.write(enc(event))
 f.flush();os.fsync(f.fileno())
print('synthetic native',mode,'closed')
'''


def self_check():
    """Actual execute/replay and native_stage; only native/systemd boundaries fake."""
    from contextlib import ExitStack
    with tempfile.TemporaryDirectory(prefix='source-witness-glue-') as folder, ExitStack() as stack:
        root = Path(folder); repo = root/'repo'; repo.mkdir()
        for role, name in SOURCE_PATHS.items():
            probe.copy_bytes(repo/name, ('synthetic '+role).encode())
        sources = source_hashes(repo)
        stages = [dict(stage=n, exit_status=0, gate_status=0) for n in local.GATES]
        qualification = dict(qualified=True, source_file_count=2, source_identity_sha256=source_identity(sources),
            stages=stages, integrated_native_bytes_match=True, original_exit_status=0, instance_state='terminated')
        workspace = dict(qualification, source_unchanged=True, exit_status=0, source_sha256=sources)
        def evidence(name, value):
            p = local.write_json(repo/name, value)
            return dict(p, path=name)
        workspace_pin = evidence('workspace.json', workspace)
        native = root/'fixture-native'; probe.copy_bytes(native, ('#!'+sys.executable+'\n'+FIXTURE).encode()); native.chmod(0o700)
        config = dict(schema=SCHEMA, run_id='synthetic-a0001', binary=local.identity(native), diagnostics=True,
                      resources=copy.deepcopy(RESOURCES), inputs={d: {} for d in DATASETS})
        authority = dict(schema='borsuk-source-witness-retained-input-authority-v1', source_qualification_pending=False,
            native_qualification=evidence('qualification.json', qualification),
            method=evidence('method.json', dict(rows=100000, dimensions=768, count=64, first=0, k=100, metric='cosine')),
            terminal=evidence('historical.json', dict(complete=True)), native_binary=dict(uncompressed=body(config['binary'])), datasets={d: {} for d in DATASETS})
        for dataset in DATASETS:
            for name in LAYOUT_FILES+PANEL_FILES:
                p = root/'transport'/dataset/name
                data = local.canonical(dict(schema='borsuk-hierarchical-cells-resident-v4', rows=100000, dimensions=768)) if name == 'manifest.json' else ('synthetic '+dataset+' '+name).encode()
                pin = probe.copy_bytes(p, data); config['inputs'][dataset][name] = pin; authority['datasets'][dataset][name] = body(pin)
        config['authority'] = evidence('authority.json', authority)
        module = sys.modules[__name__]
        for name, value in dict(AUTHORITY=config['authority'], WORKSPACE=workspace_pin, SOURCE_COUNT=2, SOURCE_ID=source_identity(sources)).items():
            stack.enter_context(patch.object(module, name, value))
        stack.enter_context(patch.dict(os.environ, dict(BORSUK_GLOBAL_LEAF_SLICE='borsuk-global-leaf-synthetic.slice',
            SOURCE_WITNESS_FIXTURE_SOURCES=json.dumps({k: sources[p] for k, p in SOURCE_PATHS.items()}))))
        real_popen = subprocess.Popen
        scenario = ''; processes = {}; started_native = []
        accelerated = False
        def fake_popen(command, **kwargs):
            nonlocal accelerated
            assert command[0] == 'systemd-run'
            pos = command.index('--owned-stage'); spec_path, spec_sha, receipt_path = command[pos+1:pos+4]
            spec = local.read_json(dict(local.identity(spec_path), sha256=spec_sha))
            name = spec['name']; started_native.append(name)
            env = dict(os.environ, SOURCE_WITNESS_FIXTURE=scenario)
            process = real_popen([sys.executable, str(native), 'supervise', spec_path, receipt_path], env=env, **kwargs)
            unit = next(a.removeprefix('--unit=') for a in command if a.startswith('--unit='))+'.service'
            processes[unit] = process
            accelerated = scenario == 'timeout' and name == 'cohere-nominate-probes'
            return process
        def fake_run(command, **kwargs):
            assert command[0] == 'systemctl'
            unit = next(a for a in command if a.endswith('.service')); process = processes[unit]
            if command[1] in ('stop', 'kill'):
                if process.poll() is None:
                    try: os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
                return subprocess.CompletedProcess(command, 0)
            assert command[1] == 'show'
            bad_drain = scenario == 'drain' and 'cohere-nominate' in unit
            return subprocess.CompletedProcess(command, 0, stdout='ActiveState='+('active' if bad_drain else 'inactive')+'\nSubState=dead\nMainPID='+('1' if bad_drain else '0')+'\nControlGroup=\nLoadState=loaded\n')
        fake_subprocess = SimpleNamespace(Popen=fake_popen, run=fake_run, DEVNULL=subprocess.DEVNULL,
            STDOUT=subprocess.STDOUT, TimeoutExpired=subprocess.TimeoutExpired)
        stack.enter_context(patch.object(probe, 'subprocess', fake_subprocess))
        # Time acceleration is limited to the existing native supervisor loop.
        stack.enter_context(patch.object(probe, 'time', SimpleNamespace(sleep=time.sleep,
            monotonic=lambda: time.monotonic()+(700 if accelerated else 0))))
        def attempt(label, failure='', changed=None, expected='INVALID', count=None):
            nonlocal scenario, accelerated
            scenario = failure; accelerated = False; started_native.clear()
            cfg = copy.deepcopy(config)
            if changed: changed(cfg)
            pin = local.write_json(root/(label+'-config.json'), cfg)
            out = root/label
            terminal = execute(pin['path'], pin['sha256'], repo, out)
            assert terminal['status'] == expected, (label, terminal, (out/'execution-receipt.json').read_text())
            assert terminal['complete'] == (expected != 'INVALID')
            assert terminal['execution_exit_code'] == (2 if expected == 'INVALID' else 0)
            if count is not None: assert len(started_native) == count, (label, started_native)
            assert all(p.poll() is not None for p in processes.values()), 'original subprocesses must drain'
            if expected != 'INVALID':
                assert replay(pin['path'], pin['sha256'], repo, out)['status'] == expected
            return out, pin
        good, good_pin = attempt('success', expected='RETURNED_DIAGNOSTIC_PASS', count=6)
        attempt('coverage-only', changed=lambda c: c.update(diagnostics=False), expected='COVERAGE_PASS', count=4)
        for failure in ('fail-mean', 'fail-p05'):
            out, _ = attempt(failure, failure, expected='FAIL', count=4)
            assert not (out/'coverage/gate.json').exists() and not (out/'diagnostic-seal.json').exists()
        attempt('returned-fail', 'fail-returned', expected='FAIL', count=6)
        for failure in ('freeze', 'partial', 'memory', 'swap', 'oom', 'cpu', 'drain', 'cleanup', 'config', 'timeout'):
            out, _ = attempt(failure, failure, count=4)
            assert not (out/'coverage/gate.json').exists()
            assert (out/'measurement/cohere-nominate-probes-closure.json').exists()
        attempt('binary-identity', changed=lambda c: c['binary'].update(sha256='0'*64), count=0)
        attempt('input-identity', changed=lambda c: c['inputs']['relaion']['requests64'].update(sha256='0'*64), count=0)
        attempt('resources', changed=lambda c: c['resources'].update(per_dataset_seconds=601), count=0)
        attempt('resource-type', changed=lambda c: c['resources'].update(cpu_affinity=[False]), count=0)
        before = inventory(good)
        try: execute(good_pin['path'], good_pin['sha256'], repo, good)
        except FileExistsError: pass
        else: raise AssertionError('existing output overwritten')
        exact(inventory(good), before, 'nooverwrite original attempt')
        def rejects(fn):
            try: fn()
            except (ValueError, OSError): return
            raise AssertionError('negative accepted')
        rejects(lambda: replay(good_pin['path'], '0'*64, repo, good))
        # Recompute the inventory after tampering: replay must independently
        # reject altered resource/gate/config/freeze provenance, beyond SHA drift.
        terminal_bytes = (good/'terminal.json').read_bytes()
        for name, mutate in (
            ('coverage/cohere-supervisor.json', lambda v: v['closure'].update(report_sha256='0'*64)),
            ('measurement/cohere-nominate-probes-stage-receipt.json', lambda v: v['stages'][0]['cgroup_after'].update({'memory.swap.peak':'1'})),
            ('nomination-seal.json', lambda v: v['configs']['cohere'].update(sha256='0'*64)),
            ('coverage/gate.json', lambda v: v['cohere'].update(run_id='another-run'))):
            path = good/name; original = path.read_bytes(); value = local.decode(original); mutate(value)
            path.write_bytes(local.canonical(value))
            terminal = local.decode(terminal_bytes); terminal['inventory'] = inventory(good)
            (good/'terminal.json').write_bytes(local.canonical(terminal))
            rejects(lambda: replay(good_pin['path'], good_pin['sha256'], repo, good))
            path.write_bytes(original); (good/'terminal.json').write_bytes(terminal_bytes)
        sidecar = good/'probes/cohere.bin'; original = sidecar.read_bytes(); sidecar.write_bytes(original+b'tamper')
        rejects(lambda: replay(good_pin['path'], good_pin['sha256'], repo, good)); sidecar.write_bytes(original)
        source_path = repo/SOURCE_PATHS['module']; original = source_path.read_bytes(); source_path.write_bytes(original+b'tamper')
        rejects(lambda: replay(good_pin['path'], good_pin['sha256'], repo, good)); source_path.write_bytes(original)
        # Collection can relocate bytes without reserializing original native paths.
        moved = root/'collected'; good.rename(moved)
        assert replay(good_pin['path'], good_pin['sha256'], repo, moved)['status'] == 'RETURNED_DIAGNOSTIC_PASS'
        # Exercise the production empty-log adapter against a real, silent
        # subprocess and the shared run_stage/owned_stage control flow.
        silent = root/'silent-native'; probe.copy_bytes(silent, ('#!'+sys.executable+'\npass\n').encode()); silent.chmod(0o700)
        silent_out = root/'silent'; silent_out.mkdir()
        silent_cfg = local.write_json(silent_out/'config.json', dict(synthetic=True))
        silent_binary = local.identity(silent)
        spec = local.write_json(silent_out/'spec.json', dict(schema='borsuk-global-leaf-owned-stage-v1', name='silent',
            command=[str(silent)], binary=silent_binary, config=silent_cfg, output=str(silent_out),
            resources=dict(RESOURCES, timeout_seconds=30), timeout_seconds=30))
        group = {'path':'/synthetic-silent', 'memory.max':'536870912', 'memory.peak':'1048576', 'memory.swap.max':'0',
                 'memory.swap.peak':'0', 'memory.events':'oom 0\noom_kill 0\noom_group_kill 0', 'cpu_affinity':[0],
                 'cpu.max':'100000 100000', 'pids.max':'512', 'cgroup.procs':str(os.getpid())}
        # local.run_stage shares the stdlib subprocess module, unaffected by
        # the fake systemd proxy installed only on the probe module.
        with patch.object(local, 'resource_snapshot', lambda _: copy.deepcopy(group)), patch.object(probe, 'cgroup_snapshot', lambda *_: copy.deepcopy(group)):
            closed = owned_stage(spec, silent_out/'receipt.json')
        assert closed['complete'] and closed['status'] == 'CLOSED'
        assert closed['stages'][0]['exit_status'] == 0 and closed['stages'][0]['log']['bytes'] == 0
    print('PASS actual orchestration/replay with synthetic native subprocesses: six serial calls, both builders before panels, canonical paired seals, original supervisor gate, mean/p05/returned FAIL exit0; identity/config/resources/freeze/timeout/partial/drain/cleanup/tamper/nooverwrite negatives. Native algorithms/systemd resources are mocked; no real corpus/GT/native binary used.')


def main(args):
    try:
        if len(args) == 4 and args[0] == '--owned-stage':
            _, pin = local.load_config(args[1], args[2])
            print(json.dumps(owned_stage(pin, args[3]), sort_keys=True)); return 0
        if args == ['--self-check']:
            self_check(); return 0
        replaying = bool(args and args[0] == '--replay')
        if replaying:
            args = args[1:]
        require(len(args) == 4, 'usage: CONFIG CONFIG_SHA256 REPO NEW_OUTPUT | --replay CONFIG SHA REPO OUTPUT | --self-check')
        terminal = (replay if replaying else execute)(*args)
        print(json.dumps(dict(status=terminal['status'], complete=terminal['complete'], output=str(args[3])), sort_keys=True))
        return terminal['execution_exit_code']
    except Exception as error:
        print(str(error), file=sys.stderr); return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
