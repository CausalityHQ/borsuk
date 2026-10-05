#!/usr/bin/env python3
"""Thin, fixed native overlap pair; no Python routing, scoring or truth reader.

CLI: CONFIG SHA REPO NEW_OUTPUT | --replay CONFIG SHA REPO OUTPUT | --self-check
Root stages originals/GT, supplies completed qualification and owns host lifetime.
All six native calls are serial; every artifact and original closure is retained.
"""
import copy
import hashlib
import math
import os
from pathlib import Path
import re
import signal
import struct
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_source_witness_paired_coverage as witness
from scripts import launch_native_workspace_execution_spot as controller

probe, local, positive = witness.probe, witness.local, witness.positive
require, exact, fields = local.require, local.exact, local.fields
body, retain, owned_stage = witness.body, witness.retain, witness.owned_stage
SCHEMA = 'borsuk-cell-overlap-pair-execution-v1'
BASE = 'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/'
AUTHORITY = witness.AUTHORITY
MANIFEST = dict(path=BASE+'boundary-overlap/implementation-gates/native-source-manifest.json', bytes=44771,
    sha256='95b30ea3abc56c017991c9420af403e117eba9aecc12d6d7a29b801e994afedc')
SOURCE_COMMIT = 'afd58b557d01cafc307e650d9c29f381dbf24c41'
SOURCE_ID = 'b587110515542f46107156b6b0c34b318132fdb01c96380118f58edab344a3f7'
FULL_SOURCE_ID = 'f71f6923bf5ebdd131fb1ae7528cd9434d70c8d1d51c184cdb7900b46b0d7c59'
DATASETS = ('relaion', 'cohere')
ROWS, DIMENSIONS = 100000, 768  # Fixed runtime geometry; self-check patches only tiny synthetic fixtures.
ORIGINALS = 'generation plane canonical order records mean sq8'.split()
LAYOUT = ('manifest.json', 'directories.bin', 'cells.bin')
OVERLAP = ('manifest.json', 'sq8-cells.bin', 'placement.bin', 'boundaries.json')
RESOURCES = {role: dict(memory_max_bytes=memory, scratch_max_bytes=8 << 30, swap_bytes=0,
    cpu_affinity=cpus, timeout_seconds=seconds, max_log_bytes=16 << 20, tasks_max=512)
    for role, memory, cpus, seconds in [('build', 8 << 30, [0, 1], 1200), ('query', 512 << 20, [0], 300)]}
NATIVE_RESOURCES = dict(build_workers=2, build_memory_bytes=8 << 30, build_swap_bytes=0,
    build_scratch_bytes=8 << 30, build_timeout_seconds=1200, query_workers=1,
    query_memory_bytes=512 << 20, query_swap_bytes=0, query_timeout_seconds=300)
ORDER = [d+'-'+arm+'-build-overlap' for d in DATASETS for arm in ('control', 'candidate')]+[d+'-paired-overlap' for d in DATASETS]


def descriptor(pin, cap):
    """Validate a descriptor without opening its path (including GT metadata)."""
    fields(pin, 'path bytes sha256', 'artifact descriptor')
    require(type(pin['path']) is str and Path(pin['path']).is_absolute()
        and '..' not in Path(pin['path']).parts and '\x00' not in pin['path'] and '\n' not in pin['path'], 'absolute artifact path')
    local.integer(pin['bytes'], 1, cap, 'artifact size'); local.digest(pin['sha256'])


def qualification(config, repo):
    q = config['qualification']
    fields(q, 'pending directory proof terminal launch closeout', 'completed qualification authority')
    exact(q['pending'], False, 'native qualification pending')
    require(type(q['directory']) is str and Path(q['directory']).is_absolute(), 'explicit qualification directory')
    folder = positive.regular_path(q['directory'])
    values = {}
    for name in ('proof', 'terminal', 'launch', 'closeout'):
        descriptor(q[name], 8 << 20); values[name] = local.read_json(q[name], 8 << 20)
    exact(q['proof']['path'], str(folder/'source-qualification.json'), 'supplied original qualification')
    proof, terminal, launch, close = (values[n] for n in ('proof', 'terminal', 'launch', 'closeout'))
    with controller.execution_mode(cell_overlap=True):
        receipt = controller.validate_receipt(folder, proof)
        exact([s['stage'] for s in receipt['stages']], [n for n, _ in controller.CELL_OVERLAP_STAGES], 'all seven native stages')
        for key in controller.TERMINAL_IDENTITIES:
            exact(terminal[key], proof[key], 'original terminal qualification binding: '+key)
        exact(terminal['schema'], controller.SCHEMA, 'cell-overlap campaign')
        for name in controller.ARTIFACTS:
            exact(terminal['artifacts'][name], body(local.identity(folder/name)), 'original terminal artifact: '+name)
        exact(set(proof['code_sha256']), set(controller.CODE), 'qualified controller roster')
        for name, digest in proof['code_sha256'].items():
            exact(local.identity(Path(repo)/name)['sha256'], digest, 'same qualified controller bytes')
    for key in ('phase', 'status'):
        exact(terminal[key], 'complete', 'completed original qualification')
    for key in ('exit_code', 'original_exit_code'):
        exact(terminal[key], 0, 'qualification original exit0')
    exact(close['state'], 'terminated', 'qualification actual host closure')
    exact(close['nodes'], launch['nodes'], 'same closed qualification nodes')
    require(terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in close['nodes'].values()}, 'same original qualification instance')
    for key in ('source_commit', 'source_archive_sha256'):
        exact(terminal[key], launch[key], 'same launched archive')
    exact(terminal['source_qualification_sha256'], q['proof']['sha256'], 'terminal original proof SHA')
    exact(proof['native_source_commit'], SOURCE_COMMIT, 'qualified final Rust revision')
    manifest = probe.ref(repo, MANIFEST)
    exact(manifest['native_source_commit'], SOURCE_COMMIT, 'prospective native revision')
    exact(manifest['source_file_count'], 403, 'frozen full403 roster')
    sources = witness.source_hashes(Path(repo))
    exact(sources, manifest['source_sha256'], 'current frozen full403 bytes')
    exact(proof['source_sha256'], sources, 'actual qualified full403 bytes')
    exact(receipt['source_sha256'], sources, 'original native receipt full403 bytes')
    exact(proof['source_file_count'], 403, 'qualified full403 count')
    exact(proof['source_identity_sha256'], FULL_SOURCE_ID, 'qualified full403 identity')
    exact(witness.source_identity(sources), FULL_SOURCE_ID, 'current full403 identity')
    descriptor(config['binary'], 256 << 20)
    exact(body(config['binary']), terminal['artifacts']['binaries/hierarchical_semantic_cells'], 'actually qualified release binary')
    return sources


def qualify(config, repo):
    fields(config, 'schema run_id authority qualification binary inputs resources', 'fixed pair config')
    exact(config['schema'], SCHEMA, 'pair schema')
    require(type(config['run_id']) is str and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', config['run_id']), 'run ID')
    exact(config['authority'], AUTHORITY, 'fixed retained capacity-v4 authority')
    exact(config['resources'], RESOURCES, 'fixed build/query resource policy')
    for role, limits in RESOURCES.items():
        for name, value in limits.items():
            exact(type(config['resources'][role][name]), type(value), 'resource type')
        require(all(type(v) is int for v in config['resources'][role]['cpu_affinity']), 'CPU integer affinity')
    sources = qualification(config, repo)
    authority = probe.ref(repo, AUTHORITY)
    exact(authority['schema'], 'borsuk-source-witness-retained-input-authority-v1', 'retained authority schema')
    fields(config['inputs'], ' '.join(DATASETS), 'both datasets')
    builds = {}
    for dataset in DATASETS:
        inputs = config['inputs'][dataset]
        fields(inputs, 'layout original requests64 truth64', 'staged dataset')
        fields(inputs['layout'], ' '.join(LAYOUT), 'exact retained layout roster')
        fields(inputs['original'], ' '.join(ORIGINALS), 'all seven original inputs')
        for name, pin in dict(inputs['layout'], requests64=inputs['requests64'], truth64=inputs['truth64']).items():
            descriptor(pin, 128 << 20)
            exact(body(pin), body(authority['datasets'][dataset][name]), 'exact retained layout/panel: '+name)
        local.authenticate(inputs['requests64'], 128 << 20)  # Both request bodies authenticated before any native build; no GT open.
        root = local.read_json(inputs['layout']['manifest.json'])
        exact(root['schema'], 'borsuk-hierarchical-cells-resident-v4', 'capacity-v4 only')
        exact(root['rows'], ROWS, 'FIRST100k'); exact(root['dimensions'], DIMENSIONS, 'D768')
        build = copy.deepcopy(root['input'])
        fields(build, ' '.join(probe.BUILD_FIELDS), 'strict original BuildConfig')
        exact(build['schema'], 'borsuk-hierarchical-cells-build-v2', 'exact original build policy')
        for name in ORIGINALS:
            pin = inputs['original'][name]; descriptor(pin, 512 << 20)
            exact(body(pin), body(build[name]), 'root-bound original '+name)
            local.authenticate(pin, 512 << 20)
            build[name] = pin
        for name, value in dict(cell_rows=512, sample_rows=256, max_depth=32,
                max_build_payload_bytes=64 << 20, max_output_bytes=256 << 20).items():
            exact(build[name], value, 'unchanged original policy')
        probe.layout(inputs['layout']['manifest.json'], root['input'], dimensions=DIMENSIONS, rows=ROWS, partitioner=True)
        builds[dataset] = build
    return sources, builds


def build_config(retained, original, candidate):
    return dict(schema='borsuk-cell-overlap-build-v1', retained_root=retained, original=original, overlap=candidate,
        max_resident_payload_bytes=64 << 20, max_build_payload_bytes=8 << 30, max_output_bytes=256 << 20)


def query_config(dataset, roots, requests, truth):
    return dict(schema='borsuk-cell-overlap-paired-v1', dataset=dataset, control_root=roots['control']['manifest.json'],
        candidate_root=roots['candidate']['manifest.json'], requests=requests, truth=truth, first=0, count=64, truth_width=100,
        source_identity_sha256=SOURCE_ID, max_resident_payload_bytes=512 << 20,
        max_evaluator_payload_bytes=256 << 20, max_result_bytes=512 << 20, resources=NATIVE_RESOURCES)


def closure(output, runtime, item, binary):
    read = lambda pin: local.read_json(witness.physical(output, runtime, pin), 1 << 20)
    record = read(item['closure']); limits = RESOURCES[item['role']]
    command = [binary['path'], item['mode'], item['config']['path'], item['config']['sha256'], item['destination']]
    for name, value in dict(name=item['name'], command=command, exit_status=0, closed=True,
            unit_drained=True, cgroup_drained=True, resource_gate_passed=True).items():
        exact(record[name], value, 'actual native closure: '+name)
    exact(record['unit_closeout']['MainPID'], '0', 'no live native PID')
    require(record['unit_closeout']['ActiveState'] in ('inactive', 'failed'), 'native unit inactive')
    inner = read(record['native_receipt'])
    exact(inner['status'], 'CLOSED', 'original inner supervisor closed'); exact(inner['complete'], True, 'original inner complete')
    exact(len(inner['stages']), 1, 'one serial native process')
    stage = inner['stages'][0]
    for name, value in dict(name=item['name'], command=command, binary=binary, config=item['config'],
            exit_status=0, cleanup_complete=True, resource_gate_passed=True).items():
        exact(stage[name], value, 'actual original native stage: '+name)
    probe.validate_cgroup(inner['cgroup'], limits['memory_max_bytes'], 100*len(limits['cpu_affinity']))
    require(len(inner['cgroup']['cgroup.procs'].split()) == 1, 'only supervisor after native drain')
    for group in (stage['cgroup_before'], stage['cgroup_after']):
        require(0 < int(group['memory.max']) <= limits['memory_max_bytes']
            and 0 <= int(group['memory.peak']) <= limits['memory_max_bytes'], 'actual native memory')
        exact(group['memory.swap.max'], '0', 'actual native noSwap'); exact(int(group['memory.swap.peak']), 0, 'zero swap peak')
        exact(group['cpu_affinity'], limits['cpu_affinity'], 'actual CPU affinity')
    probe.no_oom(stage['cgroup_before'], stage['cgroup_after'])
    require(0 <= stage['wall_seconds'] <= record['wall_seconds'] <= limits['timeout_seconds'] and math.isfinite(record['wall_seconds']), 'actual native deadline')
    for name, cap in [('sampled_peak_rss_bytes', limits['memory_max_bytes']), ('sampled_peak_scratch_bytes', limits['scratch_max_bytes'])]:
        local.integer(stage[name], 0, cap, 'actual native '+name)
    spec = local.read_json(local.identity(output/'measurement'/(item['name']+'-stage.json')))
    for name, value in dict(schema='borsuk-global-leaf-owned-stage-v1', name=item['name'], command=command,
            binary=binary, config=item['config'], output=str(Path(runtime)/'measurement'), resources=limits).items():
        exact(spec[name], value, 'saved actual supervisor spec')
    require(0 < spec['timeout_seconds'] <= limits['timeout_seconds'], 'supervisor deadline')
    for flag in ('--property=MemoryMax='+str(limits['memory_max_bytes']), '--property=MemorySwapMax=0',
            '--property=CPUQuota='+str(100*len(limits['cpu_affinity']))+'%', '--property=TasksMax=512',
            '--property=KillMode=control-group', '--property=RuntimeMaxSec='+str(limits['timeout_seconds']),
            '--setenv=RAYON_NUM_THREADS='+str(len(limits['cpu_affinity']))):
        require(flag in record['supervisor_command'], 'actual supervisor resource flag')
    for pin in (record['native_log'], record['log'], stage['log']):
        witness.authenticate_log(witness.physical(output, runtime, pin), limits['max_log_bytes'])
    exact(record['native_log'], stage['log'], 'preserved native log identity including silence')
    return record


def events(path, cap):
    with positive.open_input(path) as stream:
        before = positive.stamp(stream); used = 0
        while line := stream.readline((16 << 20)+1):
            used += len(line)
            require(used <= cap and len(line) <= 16 << 20 and line.endswith(b'\n'), 'bounded complete native event')
            yield line, local.decode(line)
        exact(positive.stamp(stream), before, 'stable native JSONL')


def build_report(path, config, pin, roots):
    rows = list(events(path, 65536)); exact(len(rows), 1, 'one native build terminal')
    event = rows[0][1]
    for name, value in dict(phase='terminal', status='BUILT_UNVERIFIED', complete=True,
            config_sha256=pin['sha256'], source_identity_sha256=SOURCE_ID,
            required_build_cpu_max=4, required_memory_bytes=8 << 30, required_swap_bytes=0,
            required_scratch_bytes=8 << 30, required_timeout_seconds=1200,
            scientific_qualification=False, quality_or_performance_claim=False).items():
        exact(event[name], value, 'native build identity/resource contract')
    manifest = local.read_json(roots['manifest.json'], 1 << 20)
    fields(manifest, 'schema primary_root logical_rows physical_rows dimensions overlap extent_sha256 mapping_sha256 boundary_bytes boundary_sha256 cells build', 'overlap root')
    for name, value in dict(schema='borsuk-cell-overlap-v1', primary_root=config['retained_root'],
            logical_rows=ROWS, dimensions=DIMENSIONS, overlap=config['overlap']).items():
        exact(manifest[name], value, 'matched native build root')
    receipt = event['receipt']; exact(receipt['root_sha256'], roots['manifest.json']['sha256'], 'native built root SHA')
    exact(dict(receipt, root_sha256=''), manifest['build'], 'exact native persisted build receipt')
    for name, digest in [('sq8-cells.bin', 'extent_sha256'), ('placement.bin', 'mapping_sha256'), ('boundaries.json', 'boundary_sha256')]:
        exact(roots[name]['sha256'], manifest[digest], 'authenticated native layout body')
    exact(roots['placement.bin']['bytes'], 16*ROWS, 'placement size')
    exact(roots['boundaries.json']['bytes'], manifest['boundary_bytes'], 'boundary size')
    local.integer(receipt['admitted'], 0, ROWS//4, 'fixed replica quota')
    exact(manifest['physical_rows'], ROWS+receipt['admitted'], 'physical/logical distinction')
    exact(sum(c['primary_rows'] for c in manifest['cells']), ROWS, 'complete primary partition')
    exact(sum(c['replica_rows'] for c in manifest['cells']), receipt['admitted'], 'complete admitted replicas')
    offset = ordinal = 0
    for cell_id, extent in enumerate(manifest['cells']):
        fields(extent, 'cell_id primary_rows replica_rows first_physical_ordinal offset bytes sha256', 'native extent')
        for name, value in dict(cell_id=cell_id, offset=offset, first_physical_ordinal=ordinal,
                bytes=64+(extent['primary_rows']+extent['replica_rows'])*(DIMENSIONS+12)).items():
            exact(extent[name], value, 'complete framed native extent partition')
        local.integer(extent['primary_rows'], 1, 512, 'primary capacity'); local.integer(extent['replica_rows'], 0, 128, 'replica capacity')
        local.digest(extent['sha256']); offset += extent['bytes']; ordinal += extent['primary_rows']+extent['replica_rows']
    exact(offset, roots['sq8-cells.bin']['bytes'], 'complete framed file length')
    if not config['overlap']:
        exact(receipt['admitted'], 0, 'matched control no replicas')
    exact(receipt['output_bytes'], sum(p['bytes'] for p in roots.values()), 'actual native output bytes')
    require(receipt['output_bytes'] <= config['max_output_bytes'] and receipt['modeled_build_payload_bytes'] <= config['max_build_payload_bytes'], 'native build cap')
    return manifest


def paired_report(path, config, pin, seal_pin, manifests):
    prefix, size = hashlib.sha256(), 0
    selections, rosters, metrics = 0, 0, 0
    plans = []; underfills = []; seal = terminal = None
    for line, event in events(path, config['max_result_bytes']):
        phase = event['phase']; require(terminal is None, 'native terminal last')
        if size == 0:
            exact(phase, 'paired_metadata_admission', 'metadata before heavy opens')
            exact(event['heavy_indexes_opened'], 0, 'metadata-only admission')
            for arm in ('control', 'candidate'):
                exact(event[arm+'_primary_root'], manifests[arm]['primary_root'], 'same retained primary identity')
            require(0 < event['pair_peak_payload_bytes'] <= 512 << 20, 'native coexistence admission')
        elif phase == 'paired_selection':
            require(rosters == 0 and seal is None and selections < 64, 'all selections before payload rosters')
            exact(event['ordinal'], selections, 'ordered all64 selection')
            a, b = event['control'], event['candidate']
            exact(a['selected'], b['selected'], 'paired IDs/primary/distance bits')
            require(0 < len(a['selected']) <= 32 and len({s['cell_id'] for s in a['selected']}) == len(a['selected']), 'primary8/wider24 union<=32')
            for selected in a['selected']:
                fields(selected, 'cell_id primary distance_bits', 'native selected cell')
                require(type(selected['primary']) is bool, 'native primary flag')
                local.integer(selected['distance_bits'], 0, (1 << 32)-1, 'native route distance bits')
            require(sum(s['primary'] for s in a['selected']) <= 8, 'fixed primary8 beam')
            for arm in ('control', 'candidate'):
                plan = event[arm]; fields(plan, 'root_sha256 query_sha256 selected extents bytes', 'strict native fetch plan')
                exact(plan['root_sha256'], config[arm+'_root']['sha256'], 'plan arm identity')
                local.digest(plan['query_sha256'])
                lookup = {c['cell_id']: c for c in manifests[arm]['cells']}
                exact(plan['extents'], [lookup[s['cell_id']] for s in plan['selected']], 'authenticated full native extents')
                exact(plan['bytes'], sum(c['bytes'] for c in plan['extents']), 'framed native payload bytes')
                require(0 < plan['bytes'] <= 16 << 20, 'full framed16MiB bound')
            exact(a['query_sha256'], b['query_sha256'], 'same native query')
            plans.append((a, b)); selections += 1
        elif phase == 'paired_scored_roster':
            require(selections == 64 and seal is None and rosters < 64, 'full rosters before seal/GT')
            exact(event['ordinal'], rosters, 'ordered all64 roster')
            for index, arm in enumerate(('control', 'candidate')):
                trace = event[arm]; plan = plans[rosters][index]
                fields(trace, 'root_sha256 revision delta_sha256 selected base_ids replica_ids ranked underfill accounting', 'complete native roster')
                exact(trace['root_sha256'], config[arm+'_root']['sha256'], 'roster root identity')
                exact(trace['selected'], plan['selected'], 'roster exact selected cells'); exact(trace['revision'], 0, 'empty-delta science revision')
                local.digest(trace['delta_sha256'])
                ids = trace['base_ids']; replicas = trace['replica_ids']; ranked = trace['ranked']
                require(ids == sorted(set(ids)) and all(type(i) is int and 0 <= i < ROWS for i in ids), 'unique sorted native base IDs')
                require(replicas == sorted(set(replicas)) and set(replicas) <= set(ids), 'admitted replica roster')
                exact(len(ranked), len(ids), 'complete unique scored roster')
                exact(sorted(row['id'] for row in ranked), ids, 'all scored rows exactly once')
                for row in ranked:
                    fields(row, 'id score_bits physical_ordinal framed_file_offset', 'native scored row')
                    local.integer(row['score_bits'], 0, (1 << 32)-1, 'native SQ8 score bits')
                    require(math.isfinite(struct.unpack('<f', struct.pack('<I', row['score_bits']))[0]), 'finite native SQ8 score')
                    local.integer(row['physical_ordinal'], 0, manifests[arm]['physical_rows']-1, 'physical ordinal')
                    local.integer(row['framed_file_offset'], 64, sum(c['bytes'] for c in manifests[arm]['cells'])-(DIMENSIONS+12), 'framed file offset')
                account = trace['accounting']; payload = account['payload']
                for name, value in dict(submitted_gets=len(plan['extents']), requested_bytes=plan['bytes'],
                        verified_bytes=plan['bytes'], failed_gets=0).items():
                    exact(payload[name], value, 'complete native payload account')
                for name, value in dict(dependency_waves=1, max_parallel_gets=1, unique_base_rows=len(ids),
                        visible_unique_rows=len(ids), delta_scan_rows=0, delta_body_bytes=0,
                        physical_records=sum(c['primary_rows']+c['replica_rows'] for c in plan['extents'])).items():
                    exact(account[name], value, 'actual full native roster account')
                require(account['modeled_query_payload_bytes'] <= 512 << 20, 'native modeled query cap')
                exact(trace['underfill'], len(ids) < 100, 'native k100 underfill')
            exact(event['control']['replica_ids'], [], 'control no replicas')
            exact(event['candidate']['base_ids'], sorted(set(event['control']['base_ids']) | set(event['candidate']['replica_ids'])), 'precisely admitted fetched replica union')
            underfills.append((event['control']['underfill'], event['candidate']['underfill']))
            rosters += 1
        elif phase == 'paired_seal':
            require(selections == rosters == 64 and seal is None and metrics == 0, 'both64 full rosters before truth')
            exact(event['truth_opened'], False, 'native truth unopened before seal')
            exact(event['seal'], seal_pin, 'durable native seal descriptor')
            seal = local.read_json(dict(seal_pin, path=str(Path(path).with_suffix('.paired-seal.json'))))
            expected = dict(schema='borsuk-cell-overlap-paired-seal-v1', config_sha256=pin['sha256'],
                source_identity_sha256=SOURCE_ID, control_root=config['control_root'], candidate_root=config['candidate_root'],
                requests=config['requests'], truth_descriptor=config['truth'], selections_per_arm=64,
                complete_unique_scored_rosters_per_arm=64, empty_delta=True, prefix_bytes=size, prefix_sha256=prefix.hexdigest())
            exact(seal, expected, 'authenticated durable native prefix/config/source/roots/panel seal')
        elif phase == 'paired_metrics':
            require(seal is not None and metrics < 64, 'native truth metrics only after authenticated complete seal')
            exact(event['ordinal'], metrics, 'ordered all64 native metrics'); exact(event['fixed_denominator'], 100, 'fixed k100 denominator')
            for index, arm in enumerate(('control', 'candidate')):
                exact(event[arm+'_underfill'], underfills[metrics][index], 'native underfill denominator unchanged')
            for arm in ('control', 'candidate'):
                hits = event[arm+'_hits']; exact(len(hits), 2, 'native coverage/recall metric')
                for hit in hits: local.integer(hit, 0, 100, 'native k100 hit count')
            require(event['candidate_hits'][0] >= event['control_hits'][0], 'native selected coverage monotone')
            metrics += 1
        else:
            exact(phase, 'terminal', 'strict native phase roster')
            require(seal is not None and metrics == 64, 'native complete pair before scientific outcome')
            for name, value in dict(schema=config['schema'], complete=True, dataset=config['dataset'],
                    source_identity_sha256=SOURCE_ID, config_sha256=pin['sha256'], resources=NATIVE_RESOURCES,
                    paired_seal=seal_pin, payload_dependency_waves=1, actual_max_parallel_gets=1,
                    scientific_qualification=False, quality_or_performance_claim=False).items():
                exact(event[name], value, 'native terminal exact identity/resources')
            require(event['status'] in ('PANEL_PASS', 'FAIL'), 'scientific outcome distinct from INVALID')
            for arm in ('control', 'candidate'):
                summary = event[arm]
                fields(summary, 'queries denominator_per_query p05_sorted_index coverage_mean coverage_p05_hits recall_mean recall_p05_hits pass', 'native quality summary')
                for name, value in dict(queries=64, denominator_per_query=100, p05_sorted_index=3).items():
                    exact(summary[name], value, 'native frozen reduction policy')
                require(type(summary['pass']) is bool, 'native quality gate boolean')
                for name in ('coverage_mean', 'recall_mean'):
                    require(type(summary[name]) in (int, float) and math.isfinite(summary[name]) and 0 <= summary[name] <= 1, 'finite native quality mean')
                for name in ('coverage_p05_hits', 'recall_p05_hits'): local.integer(summary[name], 0, 100, 'native p05 hit count')
            exact(event['status'], 'PANEL_PASS' if event['candidate']['pass'] else 'FAIL', 'native scientific gate')
            terminal = event
        if seal is None:
            prefix.update(line); size += len(line)
    require(terminal is not None, 'complete native terminal required')
    return terminal


def finish(output, receipt):
    local.write_json(output/'execution-receipt.json', receipt)
    roster = witness.inventory(output)
    for folder in sorted(output.rglob('*'), reverse=True):
        if folder.is_dir(): probe.fsync_dir(folder)
    terminal = dict(schema=SCHEMA+'-terminal', status=receipt['status'], complete=receipt['complete'],
        execution_exit_code=0 if receipt['complete'] else 2, inventory=roster)
    local.write_json(output/'terminal.json', terminal); probe.fsync_dir(output)
    return terminal


def execute(config_path, config_sha, repo, output):
    output = positive.regular_path(output)
    require(not output.is_relative_to(probe.ORIGINAL_ROOT), 'output outside shared original scratch')
    output.mkdir(exist_ok=False)
    receipt = dict(schema=SCHEMA+'-receipt', output=str(output), status='INVALID', complete=False, stages=[], calls=[], inputs={}, roots={}, results={}, sampled_peak_output_bytes=0)
    def check():
        # This observation covers output only. Parent counts physical staged
        # inputs, retained copies, layouts, archives, binaries and logs together.
        size = local.directory_bytes(output)
        receipt['sampled_peak_output_bytes'] = max(receipt['sampled_peak_output_bytes'], size)
        require(size <= 8 << 30, 'retained pair scratch cap')
    old_term = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('pair runner terminated')))
    try:
        config, pin = local.load_config(config_path, config_sha)
        receipt['config'] = retain(pin, output/'config.json')
        sources, builds = qualify(config, repo)
        (output/'authority').mkdir()
        local.write_json(output/'authority/current-source.json', sources)
        probe.copy_bytes(output/'authority/retained-input-authority.json', probe.ref(repo, AUTHORITY, read=True))
        # Retain every original assurance artifact before any scientific child.
        with controller.execution_mode(cell_overlap=True):
            assurance_names = controller.ARTIFACTS
        for name in assurance_names:
            path = Path(config['qualification']['directory'])/name
            retain(local.identity(path), output/'authority/native'/name); check()
        for name in ('terminal', 'launch', 'closeout'):
            retain(config['qualification'][name], output/'authority'/(name+'.json'))
        binary = retain(config['binary'], output/'binary/hierarchical_semantic_cells')
        os.chmod(binary['path'], 0o500); probe.fsync_file(binary['path']); receipt['binary'] = binary
        (output/'measurement').mkdir(); (output/'layouts').mkdir()
        for dataset in DATASETS:
            incoming = config['inputs'][dataset]
            kept = dict(layout={}, original={}); receipt['inputs'][dataset] = kept
            for category, names in [('layout', LAYOUT), ('original', ORIGINALS)]:
                for name in names:
                    kept[category][name] = retain(incoming[category][name], output/'inputs'/dataset/category/name)
                    check()
            builds[dataset].update(kept['original'])
        def invoke(name, mode, role, cfg, destination):
            cfg_pin = local.write_json(output/'measurement'/(name+'-config.json'), cfg)
            command = [binary['path'], mode, cfg_pin['path'], cfg_pin['sha256'], str(destination)]
            limits = RESOURCES[role]; seconds = limits['timeout_seconds']
            with patch.object(probe, '__file__', str(Path(__file__).resolve())):
                probe.native_stage(name, command, binary, cfg_pin, output/'measurement', limits, seconds,
                    time.monotonic()+seconds+1, receipt['stages'], check)
            report_path = destination.with_suffix('.build.jsonl') if role == 'build' else destination
            item = dict(name=name, mode=mode, role=role, config=cfg_pin, destination=str(destination),
                report=local.identity(report_path), closure=local.identity(output/'measurement'/(name+'-closure.json')))
            receipt['calls'].append(item); closure(output, output, item, binary)
            return item
        for dataset in DATASETS:
            receipt['roots'][dataset] = {}
            for arm in ('control', 'candidate'):
                name = dataset+'-'+arm+'-build-overlap'; destination = output/'layouts'/(dataset+'-'+arm)
                cfg = build_config(receipt['inputs'][dataset]['layout']['manifest.json'], builds[dataset], arm == 'candidate')
                item = invoke(name, 'build-overlap', 'build', cfg, destination)
                roots = {n: local.identity(destination/n) for n in OVERLAP}
                receipt['roots'][dataset][arm] = roots
                build_report(item['report']['path'], cfg, item['config'], roots); check()
        # No truth reader/transport here: only Rust opens the root-staged GT.
        for dataset in DATASETS:
            requests = retain(config['inputs'][dataset]['requests64'], output/'requests'/dataset/'requests64')
            receipt['inputs'][dataset]['requests64'] = requests
            receipt['inputs'][dataset]['truth64'] = config['inputs'][dataset]['truth64']
            cfg = query_config(dataset, receipt['roots'][dataset], requests, config['inputs'][dataset]['truth64'])
            name = dataset+'-paired-overlap'; destination = output/'measurement'/(name+'.jsonl')
            item = invoke(name, 'paired-overlap', 'query', cfg, destination)
            item['seal'] = local.identity(destination.with_suffix('.paired-seal.json'))
            manifests = {arm: local.read_json(pins['manifest.json'], 1 << 20) for arm, pins in receipt['roots'][dataset].items()}
            receipt['results'][dataset] = paired_report(destination, cfg, item['config'], item['seal'], manifests)
        exact([c['name'] for c in receipt['calls']], ORDER, 'four builds then both serial pairs')
        receipt.update(complete=True, status='PASS' if all(r['status'] == 'PANEL_PASS' for r in receipt['results'].values()) else 'FAIL')
        check()
    except BaseException as error:
        receipt.update(status='INVALID', complete=False, error=str(error), error_type=type(error).__name__)
    finally:
        signal.signal(signal.SIGTERM, old_term)
        receipt['cleanup'] = dict(native_units_drained=all(s.get('unit_drained') and s.get('cgroup_drained') for s in receipt['stages']),
            native_processes_concurrent_max=1 if receipt['stages'] else 0, partial_artifacts_preserved=True)
    return finish(output, receipt)


def replay(config_path, config_sha, repo, output):
    output = positive.regular_path(output); config, config_pin = local.load_config(config_path, config_sha)
    sources, _ = qualify(config, repo)
    terminal = local.read_json(local.identity(output/'terminal.json'), 8 << 20)
    exact(terminal['schema'], SCHEMA+'-terminal', 'pair terminal schema')
    exact(terminal['inventory'], witness.inventory(output), 'terminal-last exact retained inventory')
    receipt = local.read_json(local.identity(output/'execution-receipt.json'), 8 << 20)
    exact(receipt['schema'], SCHEMA+'-receipt', 'pair execution schema')
    exact(body(receipt['config']), body(config_pin), 'same frozen run config')
    exact(body(local.identity(output/'config.json')), body(config_pin), 'retained config bytes')
    for name in ('complete', 'status'): exact(terminal[name], receipt[name], 'terminal/execution outcome')
    exact(terminal['execution_exit_code'], 0 if receipt['complete'] else 2, 'scientific/execution exit distinction')
    if not receipt['complete']:
        exact(receipt['status'], 'INVALID', 'partial attempt invalid'); return terminal
    exact(local.read_json(local.identity(output/'authority/current-source.json'), 128 << 10), sources, 'retained current full native source map')
    runtime = receipt['output']; physical = lambda pin: witness.physical(output, runtime, pin)
    binary = receipt['binary']; exact(body(binary), body(config['binary']), 'actual qualified binary')
    local.authenticate(physical(binary), 256 << 20)
    exact([c['name'] for c in receipt['calls']], ORDER, 'complete six serial native calls')
    exact(receipt['cleanup'], dict(native_units_drained=True, native_processes_concurrent_max=1, partial_artifacts_preserved=True), 'serial execution cleanup')
    local.integer(receipt['sampled_peak_output_bytes'], 0, 8 << 30, 'actual whole output scratch observation')
    manifests = {d: {} for d in DATASETS}; results = {}
    for dataset in DATASETS:
        incoming = config['inputs'][dataset]; kept = receipt['inputs'][dataset]
        for category, names in [('layout', LAYOUT), ('original', ORIGINALS)]:
            for name in names:
                exact(body(kept[category][name]), body(incoming[category][name]), 'exact preserved staged bytes')
                local.authenticate(physical(kept[category][name]), 512 << 20)
        exact(body(kept['requests64']), body(incoming['requests64']), 'retained requests')
        local.authenticate(physical(kept['requests64']), 16 << 20)
        exact(kept['truth64'], incoming['truth64'], 'native-only unchanged GT descriptor')
        for arm in ('control', 'candidate'):
            for pin in receipt['roots'][dataset][arm].values(): local.authenticate(physical(pin), 256 << 20)
    for item in receipt['calls']:
        closure(output, runtime, item, binary)
        cfg = local.read_json(physical(item['config']))
        dataset = item['name'].split('-')[0]
        if item['role'] == 'build':
            arm = item['name'].split('-')[1]
            original = local.read_json(physical(receipt['inputs'][dataset]['layout']['manifest.json']))['input']
            original.update(receipt['inputs'][dataset]['original'])
            exact(cfg, build_config(receipt['inputs'][dataset]['layout']['manifest.json'], original, arm == 'candidate'), 'strict frozen native build config')
            manifests[dataset][arm] = build_report(physical(item['report'])['path'], cfg, item['config'],
                {n: physical(p) for n, p in receipt['roots'][dataset][arm].items()})
        else:
            exact(cfg, query_config(dataset, receipt['roots'][dataset], receipt['inputs'][dataset]['requests64'],
                config['inputs'][dataset]['truth64']), 'strict frozen native paired config')
            result = paired_report(physical(item['report'])['path'], cfg, item['config'], item['seal'], manifests[dataset])
            exact(result, receipt['results'][dataset], 'raw native scientific result'); results[dataset] = result
    exact(receipt['status'], 'PASS' if all(r['status'] == 'PANEL_PASS' for r in results.values()) else 'FAIL', 'both native datasets required')
    return terminal


# Only --self-check launches this tiny Python fixture. It contains no ANN or
# truth reader; fabricated resource/quality receipts are never qualification.
FIXTURE = r'''import hashlib,json,os,subprocess,sys,time
from pathlib import Path
def enc(v): return (json.dumps(v,sort_keys=True,separators=(',',':'))+'\n').encode()
def sha(b): return hashlib.sha256(b).hexdigest()
def pin(p):
 b=Path(p).read_bytes(); return dict(path=str(p),bytes=len(b),sha256=sha(b))
def write(p,v):
 with Path(p).open('xb') as f: f.write(enc(v)); f.flush(); os.fsync(f.fileno())
scenario=os.environ.get('OVERLAP_PAIR_FIXTURE',''); source=os.environ['OVERLAP_PAIR_FIXTURE_SOURCE']
if sys.argv[1]=='supervise':
 spec=json.loads(Path(sys.argv[2]).read_bytes()); start=time.monotonic(); limits=spec['resources']
 log=Path(spec['output'])/(spec['name']+'.log')
 with log.open('xb') as f:
  process=subprocess.Popen(spec['command'],stdout=f,stderr=subprocess.STDOUT); status=process.wait(); f.flush(); os.fsync(f.fileno())
 bad='cohere-paired' in spec['name']
 before={'path':'/synthetic-native','memory.max':str(limits['memory_max_bytes']),'memory.peak':'1048576',
  'memory.swap.max':'0','memory.swap.peak':'0','memory.events':'oom 0\noom_kill 0\noom_group_kill 0','cpu_affinity':limits['cpu_affinity']}
 after=dict(before)
 if bad and scenario=='memory': after['memory.peak']=str(limits['memory_max_bytes']+1)
 if bad and scenario=='swap': after['memory.swap.peak']='1'
 if bad and scenario=='oom': after['memory.events']='oom 1\noom_kill 0\noom_group_kill 0'
 stage=dict(name=spec['name'],command=spec['command'],binary=spec['binary'],config=spec['config'],exit_status=status,
  cleanup_complete=not(bad and scenario=='cleanup'),resource_gate_passed=status==0,cgroup_before=before,cgroup_after=after,
  wall_seconds=time.monotonic()-start,sampled_peak_rss_bytes=1048576,
  sampled_peak_scratch_bytes=limits['scratch_max_bytes']+1 if bad and scenario=='scratch' else 1024,log=pin(log))
 if bad and scenario=='config': stage['config']=dict(stage['config'],sha256='0'*64)
 group=dict(after,**{'cpu.max':str(len(limits['cpu_affinity'])*100000)+' 100000','pids.max':'512','cgroup.procs':'12345'})
 if bad and scenario=='cpu': group['cpu.max']='200000 100000'
 write(sys.argv[3],dict(status='CLOSED' if status==0 else 'INVALID',complete=status==0,stages=[stage],cgroup=group))
 sys.exit(status)
mode,cfg_path,expected,destination=sys.argv[1:]; raw=Path(cfg_path).read_bytes(); assert sha(raw)==expected; cfg=json.loads(raw); out=Path(destination)
if mode=='build-overlap':
 assert not out.exists(); out.mkdir(); candidate=cfg['overlap']; cells=[]; offset=ordinal=0; data=b''
 for i in range(2):
  replicas=int(candidate and i==1); framed=b'x'*(64+(2+replicas)*14)
  cells.append(dict(cell_id=i,primary_rows=2,replica_rows=replicas,first_physical_ordinal=ordinal,offset=offset,bytes=len(framed),sha256=sha(framed)))
  data+=framed; offset+=len(framed); ordinal+=2+replicas
 (out/'sq8-cells.bin').write_bytes(data); (out/'placement.bin').write_bytes(b'p'*64); (out/'boundaries.json').write_bytes(b'{}')
 receipt=dict(root_sha256='',logical_rows=4,physical_rows=4+candidate,proposed=4,admitted=int(candidate),rejected=4-int(candidate),
  cells=2,extent_bytes=len(data),mapping_bytes=64,boundary_bytes=2,output_bytes=0,modeled_build_payload_bytes=1024)
 manifest=dict(schema='borsuk-cell-overlap-v1',primary_root=cfg['retained_root'],logical_rows=4,physical_rows=4+candidate,dimensions=2,
  overlap=candidate,extent_sha256=sha(data),mapping_sha256=sha(b'p'*64),boundary_bytes=2,boundary_sha256=sha(b'{}'),cells=cells,build=receipt)
 for _ in range(8):
  body=enc(manifest); total=len(data)+64+2+len(body)
  if total==receipt['output_bytes']: break
  receipt['output_bytes']=total
 (out/'manifest.json').write_bytes(body); receipt=dict(receipt,root_sha256=sha(body))
 event=dict(phase='terminal',status='BUILT_UNVERIFIED',complete=True,receipt=receipt,config_sha256=expected,source_identity_sha256=source,
  required_build_cpu_max=4,required_memory_bytes=8589934592,required_swap_bytes=0,required_scratch_bytes=8589934592,
  required_timeout_seconds=1200,scientific_qualification=False,quality_or_performance_claim=False)
 write(out.with_suffix('.build.jsonl'),event)
 sys.exit(9 if scenario=='build-exit' and 'cohere-candidate' in destination else 0)
assert mode=='paired-overlap' and not out.exists()
manifests={a:json.loads(Path(cfg[a+'_root']['path']).read_bytes()) for a in ('control','candidate')}
events=[dict(phase='paired_metadata_admission',heavy_indexes_opened=0,pair_peak_payload_bytes=1024,
 control_primary_root=manifests['control']['primary_root'],candidate_primary_root=manifests['candidate']['primary_root'])]
plans={}
for ordinal in range(64):
 pair={}
 for arm in ('control','candidate'):
  extent=manifests[arm]['cells'][1]
  plan=dict(root_sha256=cfg[arm+'_root']['sha256'],query_sha256=sha(str(ordinal).encode()),
   selected=[dict(cell_id=1,primary=True,distance_bits=0)],extents=[extent],bytes=extent['bytes'])
  pair[arm]=plan
 plans[ordinal]=pair; events.append(dict(phase='paired_selection',ordinal=ordinal,**pair))
for ordinal in range(64):
 pair={}
 for arm in ('control','candidate'):
  plan=plans[ordinal][arm]; ids=[2,3] if arm=='control' else [0,2,3]
  ranked=[dict(id=i,score_bits=0,physical_ordinal=i,framed_file_offset=64+i*14) for i in ids]
  account=dict(payload=dict(submitted_gets=1,requested_bytes=plan['bytes'],verified_bytes=plan['bytes'],failed_gets=0),
   dependency_waves=1,max_parallel_gets=1,physical_records=len(ids),unique_base_rows=len(ids),visible_unique_rows=len(ids),
   delta_scan_rows=0,delta_body_bytes=0,modeled_query_payload_bytes=1024)
  pair[arm]=dict(root_sha256=plan['root_sha256'],revision=0,delta_sha256=sha(b'empty'),selected=plan['selected'],base_ids=ids,
   replica_ids=[] if arm=='control' else [0],ranked=ranked,underfill=True,accounting=account)
 events.append(dict(phase='paired_scored_roster',ordinal=ordinal,**pair))
bad=cfg['dataset']=='cohere'
if bad and scenario=='roster': events[-1]['candidate']['ranked'].pop()
if bad and scenario=='selection': events[1]['candidate']['selected'][0]['distance_bits']=1
if bad and scenario=='incomplete': events.pop()
prefix=b''.join(map(enc,events)); sealed=dict(schema='borsuk-cell-overlap-paired-seal-v1',config_sha256=expected,source_identity_sha256=source,
 control_root=cfg['control_root'],candidate_root=cfg['candidate_root'],requests=cfg['requests'],truth_descriptor=cfg['truth'],
 selections_per_arm=64,complete_unique_scored_rosters_per_arm=64,empty_delta=True,prefix_bytes=len(prefix),prefix_sha256=sha(prefix))
seal_path=out.with_suffix('.paired-seal.json'); write(seal_path,sealed); seal=pin(seal_path)
events.append(dict(phase='paired_seal',seal=seal,truth_opened=False))
for ordinal in range(64): events.append(dict(phase='paired_metrics',ordinal=ordinal,control_hits=[100,100],candidate_hits=[100,100],fixed_denominator=100,control_underfill=True,candidate_underfill=True))
summary=dict(queries=64,denominator_per_query=100,p05_sorted_index=3,coverage_mean=1.0,coverage_p05_hits=100,recall_mean=1.0,recall_p05_hits=100,**{'pass':True})
candidate=dict(summary)
if bad and scenario=='fail': candidate.update(recall_mean=.97,recall_p05_hits=94,**{'pass':False})
terminal=dict(phase='terminal',schema=cfg['schema'],complete=True,dataset=cfg['dataset'],source_identity_sha256=source,
 config_sha256=expected,resources=cfg['resources'],paired_seal=seal,payload_dependency_waves=1,actual_max_parallel_gets=1,
 scientific_qualification=False,quality_or_performance_claim=False,status='PANEL_PASS' if candidate['pass'] else 'FAIL',control=summary,candidate=candidate)
events.append(terminal)
if bad and scenario=='source': terminal['source_identity_sha256']='0'*64
if bad and scenario=='prefix': events[0]['pair_peak_payload_bytes']=2048
if bad and scenario=='seal': sealed['empty_delta']=False; seal_path.write_bytes(enc(sealed))
if bad and scenario=='early-truth': events.insert(1,events.pop(130))
with out.open('xb') as f: f.write(b''.join(map(enc,events))); f.flush(); os.fsync(f.fileno())
sys.exit(7 if bad and scenario=='exit' else 0)
'''


def self_check(*, repair_only=False):
    from contextlib import ExitStack
    compile(FIXTURE, '<tiny-fake-native>', 'exec')
    module = sys.modules[__name__]
    def rejects(fn):
        try: fn()
        except (ValueError, AssertionError, OSError, KeyError): return
        raise AssertionError('negative fixture admitted')
    with tempfile.TemporaryDirectory(prefix='overlap-pair-mock-') as temp, ExitStack() as stack:
        root = Path(temp); repo = root/'repo'; repo.mkdir()
        for i in range(403): probe.copy_bytes(repo/('native/f'+str(i)+'.rs'), b'synthetic source')
        sources = witness.source_hashes(repo)
        with controller.execution_mode(cell_overlap=True):
            code = {n: probe.copy_bytes(repo/n, b'synthetic controller')['sha256'] for n in controller.CODE}
            names = controller.ARTIFACTS; keys = controller.TERMINAL_IDENTITIES
            stages = [dict(stage=n, exit_status=0, gate_status=0) for n, _ in controller.CELL_OVERLAP_STAGES]
        qual = root/'qualified'; qual.mkdir()
        proof = dict.fromkeys(keys, 'synthetic')
        proof.update(native_source_commit=SOURCE_COMMIT, source_sha256=sources, source_file_count=403,
            source_identity_sha256=witness.source_identity(sources), code_sha256=code)
        proof_pin = local.write_json(qual/'source-qualification.json', proof)
        receipt = dict(source_sha256=sources, stages=stages)
        local.write_json(qual/'workspace-receipt.json', receipt)
        native = root/'tiny-fake-native'; probe.copy_bytes(native, ('#!'+sys.executable+'\n'+FIXTURE).encode()); native.chmod(0o700)
        for name in names:
            if not (qual/name).exists(): retain(local.identity(native), qual/name) if name.startswith('binaries/') else probe.copy_bytes(qual/name, b'synthetic evidence')
        terminal = dict(proof, schema='borsuk-cell-overlap-implementation-gates-spot-v1', phase='complete',status='complete',
            exit_code=0,original_exit_code=0,instance_id='mock-instance',source_qualification_sha256=proof_pin['sha256'],
            source_commit='a'*40,source_archive_sha256='b'*64,artifacts={n:body(local.identity(qual/n)) for n in names})
        launch = dict(instance_id='mock-instance',nodes={'worker':dict(instance_id='mock-instance')},source_commit='a'*40,source_archive_sha256='b'*64)
        q = dict(pending=False,directory=str(qual),proof=proof_pin,terminal=local.write_json(root/'qualification-terminal.json',terminal),
            launch=local.write_json(root/'launch.json',launch),closeout=local.write_json(root/'closeout.json',dict(state='terminated',nodes=launch['nodes'])))
        manifest = dict(native_source_commit=SOURCE_COMMIT, source_file_count=403, source_sha256=sources)
        def evidence(name, value):
            pin = local.write_json(repo/name, value); return dict(pin, path=name)
        manifest_pin = evidence('manifest.json', manifest)
        authority = dict(schema='borsuk-source-witness-retained-input-authority-v1', datasets={})
        inputs = {}
        for dataset in DATASETS:
            folder = root/'incoming'/dataset
            original = {n: probe.copy_bytes(folder/n, ('synthetic '+dataset+' '+n).encode()) for n in ORIGINALS}
            build = dict(original, schema='borsuk-hierarchical-cells-build-v2', cell_rows=512, sample_rows=256,
                max_depth=32, max_build_payload_bytes=64 << 20, max_output_bytes=256 << 20)
            pins = {n:probe.copy_bytes(folder/'layout'/n, local.canonical(dict(schema='borsuk-hierarchical-cells-resident-v4',
                rows=4,dimensions=2,input=build)) if n=='manifest.json' else b'synthetic layout') for n in LAYOUT}
            requests = probe.copy_bytes(folder/'requests64', b'synthetic requests64')
            truth = dict(path=str(folder/'NEVER-OPEN-GT'),bytes=25600,sha256=local.sha(b'synthetic GT descriptor'))
            inputs[dataset] = dict(layout=pins, original=original, requests64=requests, truth64=truth)
            authority['datasets'][dataset] = {n:body(p) for n,p in dict(pins,requests64=requests,truth64=truth).items()}
        authority_pin = evidence('authority.json', authority)
        config = dict(schema=SCHEMA,run_id='synthetic-a0001',authority=authority_pin,qualification=q,
            binary=local.identity(native),inputs=inputs,resources=copy.deepcopy(RESOURCES))
        for name, value in dict(AUTHORITY=authority_pin,MANIFEST=manifest_pin,FULL_SOURCE_ID=witness.source_identity(sources),ROWS=4,DIMENSIONS=2).items():
            stack.enter_context(patch.object(module, name, value))
        stack.enter_context(patch.object(controller, 'validate_receipt', return_value=receipt))
        stack.enter_context(patch.object(probe, 'layout', return_value={}))
        stack.enter_context(patch.dict(os.environ, dict(BORSUK_GLOBAL_LEAF_SLICE='borsuk-global-leaf-synthetic.slice', OVERLAP_PAIR_FIXTURE_SOURCE=SOURCE_ID)))
        original_open = positive.open_input
        def no_gt(path):
            require('NEVER-OPEN-GT' not in str(path), 'Python GT access forbidden'); return original_open(path)
        stack.enter_context(patch.object(positive, 'open_input', side_effect=no_gt))
        real_popen = subprocess.Popen; processes = {}; calls = []; scenario = ''
        def popen(command, **kwargs):
            exact(command[0], 'systemd-run', 'reuse native supervisor')
            pos = command.index('--owned-stage'); spec_path, _, receipt_path = command[pos+1:pos+4]
            spec = local.read_json(local.identity(spec_path)); calls.append(spec['name'])
            process = real_popen([sys.executable,str(native),'supervise',spec_path,receipt_path],
                env=dict(os.environ,OVERLAP_PAIR_FIXTURE=scenario), **kwargs)
            unit = next(c.removeprefix('--unit=') for c in command if c.startswith('--unit='))+'.service'
            processes[unit] = process; return process
        def run(command, **kwargs):
            unit = next(c for c in command if c.endswith('.service')); process = processes[unit]
            if command[1] in ('stop','kill'):
                if process.poll() is None: os.killpg(process.pid, signal.SIGKILL)
                return subprocess.CompletedProcess(command,0)
            bad = scenario=='drain' and 'cohere-paired' in unit
            return subprocess.CompletedProcess(command,0,stdout='ActiveState='+('active' if bad else 'inactive')+'\nMainPID='+('1' if bad else '0')+'\nControlGroup=\n')
        stack.enter_context(patch.object(probe,'subprocess',SimpleNamespace(Popen=popen,run=run,
            DEVNULL=subprocess.DEVNULL,STDOUT=subprocess.STDOUT,TimeoutExpired=subprocess.TimeoutExpired)))
        def attempt(label, fault='', change=None, expected='INVALID', count=6):
            nonlocal scenario
            scenario=fault; calls.clear(); cfg=copy.deepcopy(config)
            if change: change(cfg)
            pin=local.write_json(root/(label+'-config.json'),cfg); out=root/label
            terminal=execute(pin['path'],pin['sha256'],repo,out)
            if terminal['status'] != expected:
                error=local.read_json(local.identity(out/'execution-receipt.json'))['error']
                logs='\n'.join(p.read_text() for p in (out/'measurement').glob('*.log'))
                raise AssertionError('synthetic '+label+': '+error+'\n'+logs)
            exact(terminal['complete'],expected!='INVALID','synthetic completion'); exact(len(calls),count,'synthetic serial call count')
            require(all(p.poll() is not None for p in processes.values()),'all original fake-native children drained')
            if expected!='INVALID': exact(replay(pin['path'],pin['sha256'],repo,out)['status'],expected,'actual replay')
            return out,pin
        if repair_only:
            good,_=attempt('cpu2',expected='PASS')
            receipt=local.read_json(local.identity(good/'execution-receipt.json'),8 << 20)
            for item in receipt['calls'][:4]:
                closed=local.read_json(item['closure'])
                require('--property=CPUQuota=200%' in closed['supervisor_command'], 'actual build CPU2 unit')
                require('--setenv=RAYON_NUM_THREADS=2' in closed['supervisor_command'], 'actual two build workers')
                inner=local.read_json(closed['native_receipt'])
                exact(inner['stages'][0]['cgroup_after']['cpu_affinity'],[0,1],'actual build CPU2 affinity')
            for item in receipt['calls'][4:]:
                exact(local.read_json(item['config'])['resources']['build_workers'],2,'native config buildworkers2')
            attempt('cpu4-refused',change=lambda c:c['resources']['build'].update(cpu_affinity=[0,1,2,3]),count=0)
            for dataset in DATASETS:
                requests=Path(inputs[dataset]['requests64']['path']); raw=requests.read_bytes()
                requests.write_bytes(raw+b'tamper')
                attempt(dataset+'-requests-tamper-before-build',count=0)
                requests.write_bytes(raw)
            print('PASS focused correction checks: four builds CPU[0,1]/workers2 then two pairs; original exit/resource/drain replay; CPU4 and each tampered requests64 body refused before any native call. Synthetic only, no GT/body normalization/scoring/native/data/cloud.')
            return
        good,pin=attempt('pass',expected='PASS'); attempt('scientific-fail','fail',expected='FAIL')
        require(controller.validate_receipt.call_count >= 4, 'existing receipt validator used by execute/replay')
        for fault in ('exit','config','memory','swap','oom','cpu','scratch','cleanup','drain','roster','incomplete','early-truth','selection','source','prefix','seal'):
            attempt('invalid-'+fault,fault)
        failed,_=attempt('partial-build','build-exit',count=4)
        require(len(list((failed/'layouts').glob('*/manifest.json')))==4,'completed native layouts retained on nonzero build exit')
        for label, change in [
            ('unknown',lambda c:c.update(unexpected=True)),
            ('pending',lambda c:c['qualification'].update(pending=True)),
            ('resource',lambda c:c['resources']['query'].update(memory_max_bytes=1)),
            ('identity',lambda c:c['inputs']['relaion']['original']['canonical'].update(sha256='0'*64)),
            ('missing-input',lambda c:c['inputs']['cohere']['original'].pop('mean'))]:
            attempt(label,change=change,count=0)
        with patch.object(controller,'validate_receipt',side_effect=ValueError('synthetic original receipt refused')):
            attempt('unqualified',count=0)
        original=(good/'terminal.json').read_bytes(); rejects(lambda:execute(pin['path'],pin['sha256'],repo,good))
        exact((good/'terminal.json').read_bytes(),original,'nooverwrite preserves prior output')
        seal=good/'measurement/relaion-paired-overlap.paired-seal.json'; saved=seal.read_bytes(); seal.write_bytes(saved+b'tamper')
        rejects(lambda:replay(pin['path'],pin['sha256'],repo,good)); seal.write_bytes(saved)
        moved=root/'collected'; good.rename(moved); exact(replay(pin['path'],pin['sha256'],repo,moved)['status'],'PASS','relocated raw evidence')
        # Real silent native process through the reused inner supervisor adapter.
        silent=root/'silent-native'; probe.copy_bytes(silent, ('#!'+sys.executable+'\npass\n').encode()); silent.chmod(0o700)
        folder=root/'silent'; folder.mkdir(); cfg_pin=local.write_json(folder/'config.json',dict(synthetic=True))
        limits=dict(RESOURCES['query'],timeout_seconds=30)
        spec=local.write_json(folder/'spec.json',dict(schema='borsuk-global-leaf-owned-stage-v1',name='silent',
            command=[str(silent)],binary=local.identity(silent),config=cfg_pin,output=str(folder),resources=limits,timeout_seconds=30))
        group={'path':'/synthetic','memory.max':'536870912','memory.peak':'1048576','memory.swap.max':'0','memory.swap.peak':'0',
            'memory.events':'oom 0\noom_kill 0\noom_group_kill 0','cpu_affinity':[0],'cpu.max':'100000 100000','pids.max':'512','cgroup.procs':str(os.getpid())}
        with patch.object(local,'resource_snapshot',lambda _:copy.deepcopy(group)),patch.object(probe,'cgroup_snapshot',lambda *_:copy.deepcopy(group)):
            closed=owned_stage(spec,folder/'receipt.json')
        exact(closed['complete'],True,'real silent supervisor complete'); exact(closed['stages'][0]['exit_status'],0,'real silent child exit0')
        exact(closed['stages'][0]['log']['bytes'],0,'real empty log retained')
    print('PASS mock execution/replay: six serial calls, both-panel PASS/scientific FAIL, exit/config/source/seal/roster/tamper/resource/drain/closure/nooverwrite negatives; real silent tiny Python child through reused supervisor. No native algorithms, real data, GT, cloud or qualification executed.')


def main(args):
    try:
        if len(args) == 4 and args[0] == '--owned-stage':
            _, pin = local.load_config(args[1], args[2]); owned_stage(pin, args[3]); return 0
        if args == ['--self-check']:
            self_check(); return 0
        if len(args) == 5 and args[0] == '--replay':
            terminal = replay(*args[1:]); print(local.canonical(terminal).decode(), end=''); return terminal['execution_exit_code']
        if len(args) == 4:
            terminal = execute(*args); print(local.canonical(terminal).decode(), end=''); return terminal['execution_exit_code']
        raise ValueError('usage: CONFIG SHA REPO NEW_OUTPUT | --replay CONFIG SHA REPO OUTPUT | --self-check')
    except (Exception, KeyboardInterrupt) as error:
        print(str(error), file=sys.stderr); return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
