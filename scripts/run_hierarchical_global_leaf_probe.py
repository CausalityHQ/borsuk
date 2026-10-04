#!/usr/bin/env python3
"""Exact original-layout reconstruction and paired truth-free nomination.

CLI: CONFIG CONFIG_SHA256 NEW_OUTPUT | --verify OUTPUT | --self-check
There is deliberately no reducer. A paired seal binds complete files; the
execution receipt additionally requires actual exits, resource closure and
retained layout bytes before original scratch is removed. Root supplies the
final frozen launcher config and separately completed binary authorities.
"""
import copy
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import struct
import subprocess
import sys
import tarfile
import tempfile
import time
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import prepare_hierarchical_cells_100k as local
from scripts import prepare_semantic_positive_inputs as positive
from scripts import prepare_native_semantic_publication as publication
from scripts.check_native_startup_build import source_identity

require, exact, fields = local.require, local.exact, local.fields
BASE = Path('docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k')
OLD = BASE/'a0001'
ORIGINAL_ROOT = Path('/mnt/hierarchical-100k')
SCHEMA = 'borsuk-global-leaf-probe-staging-v1'
SEAL_SCHEMA = 'borsuk-global-leaf-paired-nomination-seal-v1'
RECEIPT_SCHEMA = 'borsuk-global-leaf-execution-receipt-v1'
NATIVE_SCHEMA = 'borsuk-hierarchical-cells-nomination-v1'
POLICIES = ['hierarchical8_and24', 'global_top24']
DATASETS = ('relaion', 'cohere')
BUILD_FIELDS = 'schema generation plane canonical order records mean sq8 cell_rows sample_rows max_depth max_build_payload_bytes max_output_bytes'.split()
NEW_TESTS = {k: tuple(v) for k, v in local.TESTS.items()}
NEW_TESTS['hierarchical-cell-tests'] += tuple('hierarchical_semantic_cells::tests::'+n for n in (
    'nomination_global_finds_leaf_hidden_by_hierarchy_pruning',
    'nomination_unpruned_parity_caps_source_binding_and_failure_charges'))
NEW_TESTS['hierarchical-cell-bin-tests'] += tuple('tests::'+n for n in (
    'nomination_cli_full64_freezes_prefix_and_both_policies_without_truth',
    'nomination_cli_rejects_truth_fields_incomplete_panels_and_tampering',
    'nomination_prefix_rejects_fifo_and_symlink_without_blocking',
    'nomination_freeze_reserve_and_synced_prefix_tamper_close_invalid'))
REF_NAMES = ('manifest', 'receipt', 'terminal', 'launch', 'closeout', 'verification')
ROLE_NAMES = ('original', 'nomination')
CAPS = dict(memory_max_bytes=2 << 30, scratch_max_bytes=16 << 30,
    writer_max_memory_bytes=64 << 20, build_max_payload_bytes=64 << 20,
    build_max_output_bytes=256 << 20, max_resident_directory_payload_bytes=384 << 20,
    max_evaluator_payload_bytes=128 << 20, max_query_payload_bytes=128 << 20,
    max_result_bytes=128 << 20, max_log_bytes=16 << 20,
    nomination_memory_bytes=512 << 20, nomination_cpu_affinity=[0],
    cpu_affinity=[0, 1], swap_bytes=0)


def body_pin(pin):
    return {k: pin[k] for k in ('bytes', 'sha256')}


def ref(repo, pin, cap=8 << 20, read=False):
    fields(pin, 'path bytes sha256', 'reference')
    publication.relative(pin['path'])
    bound = dict(pin, path=str(Path(repo)/pin['path']))
    if Path(bound['path']).exists():
        return local.authenticate(bound, cap, read=read) if read else local.read_json(bound, cap)
    # Closed evidence can be stored gzip-compressed. Identity always binds the
    # decompressed original bytes, never JSON reserialization.
    with positive.open_input(bound['path']+'.gz') as stream:
        before = positive.stamp(stream)
        with gzip.GzipFile(fileobj=stream) as compressed:
            body = compressed.read(cap+1)
        exact(positive.stamp(stream), before, 'compressed authority stable')
    require(len(body) <= cap, 'compressed authority cap')
    exact(dict(bytes=len(body), sha256=local.sha(body)), body_pin(pin), 'original decompressed authority')
    return body if read else local.decode(body)


def copy_bytes(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    identities = {}
    with positive.output_file(path, identities) as write:
        write(body)
    return identities[path.name]


def fsync_file(path):
    with positive.open_input(path) as stream:
        os.fsync(stream.fileno())


def fsync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def gate_log(body, stages, role):
    """Require actual named passes, counts and the unshimmed test-build footer."""
    require(body.endswith(b'\n'), 'completed gate log newline')
    events, passed, summaries, builds = [], {}, [], 0
    tests = NEW_TESTS if role == 'nomination' else local.TESTS
    for line in body.splitlines():
        match = re.fullmatch(rb'test (\S+) \.\.\. ok', line)
        if match:
            n = match[1].decode(); passed[n] = passed.get(n, 0)+1
        match = re.fullmatch(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; .*', line)
        if match:
            summaries.append(tuple(map(int, match.groups())))
        if re.fullmatch(rb'rust-test-build status=0 elapsed_seconds=\d+ jobs=1', line):
            exact(len(events), 11, 'actual test-build final stage'); builds += 1
        if not line.startswith(b'{'):
            continue
        event = local.decode(line)
        if type(event) is not dict or event.get('schema') != 'borsuk-hierarchical-cells-implementation-stage-v1':
            continue
        position = len(events)
        require(position < 12, 'six completed gates only')
        stage = list(local.GATES)[position//2]
        exact(event['stage'], stage, 'gate order')
        exact(event['command'], local.GATES[stage], 'qualified gate command')
        if position % 2 == 0:
            for key in ('finished_at', 'exit_status', 'gate_status', 'tests_run', 'required_test_passes'):
                exact(event[key], None, 'gate start')
            passed, summaries = {}, []
        else:
            exact(event, stages[position//2], 'receipt/log completion binding')
            exact(event['started_at'], events[-1]['started_at'], 'paired gate times')
            require(bool(event['finished_at']), 'completed time required')
            exact(event['exit_status'], 0, 'gate exit0'); exact(event['gate_status'], 0, 'gate status0')
            expected = dict.fromkeys(tests.get(stage, ()), 1)
            exact(event['required_test_passes'], expected, 'qualified named tests')
            require(all(passed.get(n) == 1 for n in expected), 'actual named test lines')
            if position < 6:
                require(summaries and all(f == 0 for _, f in summaries)
                        and sum(p for p, _ in summaries) == event['tests_run'], 'actual test counts')
                if stage in tests:
                    exact(event['tests_run'], len(tests[stage]), 'module/bin exact test count')
            else:
                exact(event['tests_run'], None, 'compile gate')
        events.append(event)
    require(len(events) == 12 and builds == 1 and len(stages) == 6, 'all native gates completed')


def qualify_role(role, authority, repo):
    fields(authority, 'schema role authority_pending native refs', 'binary authority')
    exact(authority['schema'], 'borsuk-global-leaf-binary-authority-v1', 'binary authority schema')
    exact(authority['role'], role, 'binary role'); exact(authority['authority_pending'], False, 'binary authority pending')
    fields(authority['refs'], ' '.join(REF_NAMES), 'binary authority refs')
    values = {n: ref(repo, p) for n, p in authority['refs'].items()}
    terminal, launch, close = (values[n] for n in ('terminal', 'launch', 'closeout'))
    exact(close['state'], 'terminated', 'qualification host closed')
    exact(close['nodes'], launch['nodes'], 'qualification SAME IDs')
    require(terminal['instance_id'] == launch['instance_id'] in
            {n['instance_id'] for n in close['nodes'].values()}, 'qualification owned host')
    for k in ('phase', 'status'):
        exact(terminal[k], 'complete', 'completed qualification')
    for k in ('exit_code', 'original_exit_code'):
        exact(terminal[k], 0, 'qualification exit0')
    native = authority['native']
    fields(native, 'schema source_commit source_archive sources binaries gate_log', 'native role')
    exact(native['schema'], local.PROOF_SCHEMA, 'native role proof schema')
    require(re.fullmatch('[0-9a-f]{40}', native['source_commit']), 'native archive commit')
    exact(native['source_commit'], terminal['source_commit'], 'qualified archive revision')
    exact(native['source_archive']['sha256'], terminal['source_archive_sha256'], 'qualified source archive')
    exact(launch['source_commit'], terminal['source_commit'], 'launch source revision')
    exact(launch['source_archive_sha256'], terminal['source_archive_sha256'], 'launch archive SHA')
    manifest, receipt, verified = (values[n] for n in ('manifest', 'receipt', 'verification'))
    inventory = manifest['source_sha256']
    require(len(inventory) == 401 and inventory == receipt['source_sha256'], 'full401 inventory')
    identity = source_identity(inventory)
    for value in (manifest, receipt, terminal):
        exact(value['source_identity_sha256'], identity, 'role native source identity')
        exact(value['source_file_count'], 401, 'role source count')
    if role == 'nomination':
        exact(verified['schema'], 'borsuk-global-leaf-native-root-verification-v1', 'new root verification schema')
        exact(verified['native_source_identity_sha256'], identity, 'new root native identity')
    else:
        exact(verified['source_identity_sha256'], identity, 'original root native identity')
    exact(verified['source_file_count'], 401, 'verified native401')
    for k in ('qualified', 'command_started', 'command_completed', 'source_unchanged'):
        exact(receipt[k], True, 'completed role receipt')
    for k in ('exit_status', 'gate_status'):
        exact(receipt[k], 0, 'completed role status')
    for k in ('qualified', 'instance_terminated', 'all_terminal_bodies_authenticated', 'all_current_native_files_match'):
        exact(verified[k], True, 'verified role qualification')
    exact(verified['swap_peak_bytes'], 0, 'qualification no swap'); exact(verified['oom_events'], 0, 'qualification no OOM')
    exact(verified['terminal_sha256'], authority['refs']['terminal']['sha256'], 'root verification terminal pin')
    for n, filename in (('manifest', 'native-source-manifest.json'), ('receipt', 'workspace-receipt.json')):
        exact(body_pin(authority['refs'][n]), terminal['artifacts'][filename], 'terminal-bound authority')
    exact(body_pin(native['gate_log']), terminal['artifacts']['test.log'], 'terminal-bound actual gate log')
    qualification_pin = dict(path=str(Path(authority['refs']['terminal']['path']).parent/'source-qualification.json'),
                             **terminal['artifacts']['source-qualification.json'])
    qualified_source = ref(repo, qualification_pin)
    exact(qualification_pin['sha256'], terminal['source_qualification_sha256'], 'terminal-bound qualified code inventory')
    exact(qualified_source['source_sha256'], inventory, 'qualified native inventory')
    qualified_code = qualified_source['code_sha256']
    exact(source_identity(qualified_code), terminal['code_identity_sha256'], 'qualified controller code identity')
    exact(receipt['code_identity_sha256'], terminal['code_identity_sha256'], 'receipt controller identity')
    support = qualified_source.get('source_archive_support_sha256', {})
    require(all(qualified_code[n] == h for n, h in support.items() if n in qualified_code), 'qualified support/code consistency')
    support = dict(support, **qualified_code)
    fields(native['sources'], ' '.join(local.SOURCE_FILES), 'role source subset')
    for n, p in native['sources'].items():
        exact(p['path'], local.SOURCE_FILES[n], 'qualified source filename')
        exact(p['sha256'], (inventory if n != 'gates' else support)[p['path']], 'qualified native/support source pin')
    fields(native['binaries'], 'writer cells' if role == 'original' else 'cells', 'binary roles')
    for n, p in native['binaries'].items():
        filename = 'build_two_bit_generation' if n == 'writer' else 'hierarchical_semantic_cells'
        exact(body_pin(p), terminal['artifacts']['binaries/'+filename], 'terminal-bound binary')
    for p in (native['source_archive'], native['gate_log'], *native['binaries'].values()):
        fields(p, 'path key bytes sha256', 'qualified transport')
        publication.object_identity({k: p[k] for k in ('key', 'bytes', 'sha256')})
        require(type(p['path']) is str and '\n' not in p['path'] and '\x00' not in p['path']
                and '..' not in Path(p['path']).parts and str(Path(p['path'])) == p['path'], 'safe transport path')
    log = native['gate_log']
    log_path = Path(log['path']) if Path(log['path']).is_absolute() else Path(repo)/log['path']
    gate_log(local.authenticate(dict(body_pin(log), path=str(log_path)), 1 << 20, read=True), receipt['stages'], role)
    return dict(source_identity_sha256=identity, source_sha256=inventory,
                source_archive_support_sha256=support, source_qualification=qualification_pin,
                source_commit=native['source_commit'], native_source_commit=terminal['native_source_commit'])


def original_evidence(repo, config):
    """Authenticate old bodies, but parse only the truth-free diagnostic prefix."""
    fields(config['evidence'], 'preregistration recovery original_terminal original_launch original_closeout', 'evidence roster')
    expected = dict(preregistration=BASE/'global-leaf-routing-probe-preregistration.json',
        recovery=BASE/'global-leaf-layout-recovery-admission.json', original_terminal=OLD/'aws-terminal.json',
        original_launch=OLD/'aws-launch.json', original_closeout=OLD/'aws-closeout.json')
    for n, p in expected.items():
        exact(config['evidence'][n]['path'], str(p), 'frozen evidence path')
    values = {n: ref(repo, p) for n, p in config['evidence'].items()}
    terminal, launch, close = (values[n] for n in ('original_terminal', 'original_launch', 'original_closeout'))
    exact(terminal['phase'], 'complete', 'original completed diagnostic'); exact(terminal['exit_code'], 0, 'original exit')
    exact(terminal['original_exit_code'], 0, 'original unmodified exit'); exact(close['state'], 'terminated', 'original closed host')
    exact(close['nodes'], launch['nodes'], 'original SAME IDs')
    require(terminal['instance_id'] == launch['instance_id'] in {v['instance_id'] for v in close['nodes'].values()}, 'original owned host')
    recovery, protocol = values['recovery'], values['preregistration']
    exact(recovery['schema'], 'borsuk-global-leaf-exact-layout-recovery-admission-v1', 'exact recovery schema')
    exact(protocol['schema'], 'borsuk-global-leaf-routing-probe-preregistration-v1', 'protocol schema')
    def original(name, cap=8 << 20, parse=True):
        pin = dict(terminal['artifacts'][name], path=str(Path(repo)/OLD/name))
        body = local.authenticate(pin, cap, read=True)
        return local.decode(body) if parse else body
    proof = original('screen/native-proof.json')
    old_config = original('screen/config.json')
    local_config = original('screen/local-config.json')
    staging = original('screen/staging.json')
    exact(original('screen/summary.json')['status'], 'FAIL', 'preserve original scientific disposition')
    exact(proof['source_commit'], '2638caaec6e7ebd9b39620e497afa14bd64d402d', 'original archive source')
    exact(config['roles']['original']['native'], old_config['native'], 'unchanged original binary authority')
    for n in REF_NAMES:
        exact(config['roles']['original']['refs'][n], old_config['refs'][n], 'original authority ref')
    exact(proof['binaries'], recovery['original_binaries'], 'original binary geometry')
    items = {}
    for item in local_config['items']:
        d = item['dataset']; require(d in DATASETS and d not in items, 'original dataset roster')
        old = recovery['items'][dict(relaion='ReLAION', cohere='CoHere')[d]]
        bind = original('screen/'+d+'-panel-binding.json')
        diagnostic = original('screen/measurement/'+d+'-diagnose.json')
        build_bytes = original('screen/measurement/'+d+'-build.json', parse=False)
        writer_bytes = original('screen/measurement/'+d+'-writer.json', parse=False)
        for b, n in ((build_bytes, 'build'), (writer_bytes, 'writer')):
            exact(dict(bytes=len(b), sha256=local.sha(b)), body_pin(old['original_'+n+'_config']), 'original raw config bytes')
        build, writer = local.decode(build_bytes), local.decode(writer_bytes)
        exact({n: build[n] for n in old['build_inputs']}, old['build_inputs'], 'original generated descriptor roster')
        exact(item['inputs']['requests'], diagnostic['requests'], 'original consumed request descriptor')
        exact(bind['requests_sha256'], diagnostic['requests']['sha256'], 'original request-panel binding')
        exact(diagnostic['candidate_root']['sha256'], old['expected_cell_root_sha256'], 'original cell root')
        exact(protocol['frozen_layouts'][dict(relaion='ReLAION', cohere='CoHere')[d]], old['expected_cell_root_sha256'], 'protocol root')
        lines = original('screen/measurement/'+d+'-diagnostic.jsonl', cap=32 << 20, parse=False).splitlines(keepends=True)
        require(len(lines) >= 67, 'original closed trace count')
        identity = local.decode(lines[0]); marker = local.decode(lines[65]); end = local.decode(lines[-1])
        exact(identity['phase'], 'identity', 'original trace identity')
        exact(identity['candidate_root_sha256'], diagnostic['candidate_root']['sha256'], 'original trace root')
        exact(identity['requests_sha256'], diagnostic['requests']['sha256'], 'original trace request-panel identity')
        exact(identity['config_sha256'], terminal['artifacts']['screen/measurement/'+d+'-diagnose.json']['sha256'], 'original trace config')
        exact(marker['phase'], 'all_queries_frozen', 'original truth-free trace marker')
        prefix = b''.join(lines[:65])
        exact(marker['trace_prefix_sha256'], local.sha(prefix), 'original synced prefix')
        exact(marker['trace_prefix_bytes'], len(prefix), 'original prefix bytes')
        exact(end['phase'], 'terminal', 'original terminal last'); exact(end['complete'], True, 'original complete body')
        traces = []
        for i, line in enumerate(lines[1:65]):
            e = local.decode(line); exact(e['phase'], 'query_frozen', 'original query phase')
            exact(e['ordinal'], diagnostic['first']+i, 'original ordinal')
            traces.append({n: e['trace'][n] for n in ('primary_ids', 'covered_ids')})
        items[d] = dict(inputs=item['inputs'], binding=bind, build=build, build_bytes=build_bytes,
            writer=writer, writer_bytes=writer_bytes, recovery=old, diagnostic=diagnostic,
            trace_pin=dict(path=str(OLD/'screen/measurement'/f'{d}-diagnostic.jsonl'),
                           **terminal['artifacts'][f'screen/measurement/{d}-diagnostic.jsonl']),
            traces=traces, query_f32_sha256=staging['reductions'][d]['query_f32_sha256'])
    exact(set(items), set(DATASETS), 'both original datasets')
    return dict(values=values, proof=proof, items=items, sources=ref(repo, old_config['refs']['sources']))


def request_hashes(pin, expected, first=0, dimensions=768):
    body = local.authenticate(pin, 32 << 20, read=True)
    exact(body_pin(pin), body_pin(expected), 'exact consumed request body')
    lines = body.splitlines(); exact(len(lines), 64, 'exact request64')
    hashes, aggregate = [], hashlib.sha256()
    for i, line in enumerate(lines):
        e = local.decode(line); fields(e, 'ordinal query', 'truth-free request')
        exact(e['ordinal'], first+i, 'request ordinal')
        require(type(e['query']) is list and len(e['query']) == dimensions, 'query dimensions')
        require(all(type(v) in (int, float) and math.isfinite(v) for v in e['query']), 'finite f32 query')
        bits = struct.pack('<'+str(dimensions)+'f', *e['query'])
        require(all(math.isfinite(v[0]) for v in struct.iter_unpack('<f', bits)), 'finite f32 conversion')
        hashes.append(local.sha(bits)); aggregate.update(bits)
    return hashes, aggregate.hexdigest()


def rust_build_hash(build):
    # BuildConfig and Artifact serialize in Rust struct declaration order.
    value = {n: ({k: build[n][k] for k in ('path', 'bytes', 'sha256')}
                 if n in ('generation', 'plane', 'canonical', 'order', 'records', 'mean', 'sq8') else build[n])
             for n in BUILD_FIELDS}
    return local.sha(json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode())


def read_range(stream, span, total):
    fields(span, 'offset bytes sha256', 'source/directory span')
    local.integer(span['offset'], 0, total, 'span offset'); local.integer(span['bytes'], 1, total, 'span length')
    require(span['offset']+span['bytes'] <= total, 'span bounds')
    stream.seek(span['offset']); body = stream.read(span['bytes'])
    exact(local.sha(body), span['sha256'], 'span digest'); exact(len(body), span['bytes'], 'span bytes')
    return body


def layout(root_pin, build, dimensions=768, rows=100000):
    manifest = local.read_json(root_pin, 65536)
    exact(manifest['schema'], 'borsuk-hierarchical-cells-resident-v3', 'original resident format')
    exact(manifest['input'], build, 'original BuildConfig in recovered root')
    exact(manifest['rows'], rows, 'root rows'); exact(manifest['dimensions'], dimensions, 'root dimensions')
    folder = Path(root_pin['path']).parent
    directory_pin = dict(path=str(folder/'directories.bin'), bytes=manifest['directory_bytes'], sha256=manifest['directory_sha256'])
    local.authenticate(directory_pin, 128 << 20)
    cells_pin = local.identity(folder/'cells.bin'); exact(cells_pin['bytes'], manifest['cell_bytes'], 'complete cell-file length')
    leaves, spans, stack = {}, [], [manifest['root_directory']]
    with positive.open_input(directory_pin['path']) as stream:
        before = positive.stamp(stream)
        while stack:
            span = stack.pop(); require(len(spans) < manifest['build']['directories'], 'directory cycle/count')
            page = local.decode(read_range(stream, span, manifest['directory_bytes'])); fields(page, 'children', 'directory page')
            spans.append((span['offset'], span['offset']+span['bytes']))
            for node in page['children']:
                fields(node, 'rows prototype target', 'directory node')
                require(len(node['prototype']) == dimensions and all(type(v) in (int, float) and math.isfinite(v) for v in node['prototype']), 'stored finite prototype dimension')
                target = node['target']
                if target['kind'] == 'directory':
                    fields(target, 'kind span', 'directory target'); stack.append(target['span'])
                else:
                    fields(target, 'kind cell', 'cell target'); exact(target['kind'], 'cell', 'leaf kind')
                    c = target['cell']; fields(c, 'id first_row whole source refinement', 'cell descriptor')
                    require(c['id'] not in leaves, 'unique layout cell ID')
                    leaves[c['id']] = dict(cell=c, rows=node['rows'], prototype_bits=list(struct.unpack(
                        '<'+str(dimensions)+'I', struct.pack('<'+str(dimensions)+'f', *node['prototype']))))
        exact(positive.stamp(stream), before, 'directory stable')
    spans.sort(); exact(len(spans), manifest['build']['directories'], 'directory count')
    exact(spans[0][0], 0, 'directory start'); exact(spans[-1][1], manifest['directory_bytes'], 'directory end')
    require(all(a[1] == b[0] for a, b in zip(spans, spans[1:])), 'directory partition')
    exact(len(leaves), manifest['build']['cells'], 'leaf count')
    exact(sum(n['rows'] for n in leaves.values()), rows, 'leaf row partition')
    cell_spans, source_ids = [], set()
    record_bytes = build['records']['bytes']//rows
    with positive.open_input(cells_pin['path']) as stream:
        before = positive.stamp(stream)
        for leaf in sorted(leaves.values(), key=lambda n: n['cell']['first_row']):
            c = leaf['cell']; local.integer(leaf['rows'], 1, build['cell_rows'], 'cell occupancy')
            exact(c['first_row'], len(source_ids), 'concatenation partition')
            exact(c['source']['bytes'], leaf['rows']*(8+record_bytes), 'source span geometry')
            body = read_range(stream, c['source'], manifest['cell_bytes'])
            ids = [struct.unpack_from('<q', body, i*(8+record_bytes))[0] for i in range(leaf['rows'])]
            require(len(set(ids)) == len(ids) and not source_ids.intersection(ids)
                    and all(0 <= n < rows for n in ids), 'unique source membership')
            source_ids.update(ids); leaf['source_ids'] = ids
            exact(c['whole']['offset'], c['source']['offset'], 'whole/source start')
            exact(c['whole']['bytes'], leaf['rows']*(8+record_bytes+12+dimensions), 'whole-cell geometry')
            read_range(stream, c['whole'], manifest['cell_bytes'])
            for span in c['refinement']:
                read_range(stream, span, manifest['cell_bytes'])
            ranges = [c['source'], *c['refinement']]
            exact(sum(p['bytes'] for p in ranges), c['whole']['bytes'], 'complete cell partition')
            exact(ranges[-1]['offset']+ranges[-1]['bytes'], c['whole']['offset']+c['whole']['bytes'], 'whole-cell end')
            require(all(a['offset']+a['bytes'] == b['offset'] for a, b in zip(ranges, ranges[1:])), 'cell span contiguity')
            cell_spans.append((c['whole']['offset'], c['whole']['offset']+c['whole']['bytes']))
        exact(positive.stamp(stream), before, 'cell body stable')
    cell_spans.sort(); exact(cell_spans[0][0], 0, 'cell file start'); exact(cell_spans[-1][1], manifest['cell_bytes'], 'cell file end')
    require(all(a[1] == b[0] for a, b in zip(cell_spans, cell_spans[1:])), 'cell file partition')
    return dict(manifest=manifest, root=root_pin, directories=directory_pin, cells=cells_pin, leaves=leaves)


def nomination_config(root, requests, first):
    return dict(schema=NATIVE_SCHEMA, candidate_root=root, requests=requests, first=first, count=64,
        limits=dict(max_source_gets=24, max_source_bytes=16 << 20, max_query_payload_bytes=128 << 20),
        max_resident_directory_payload_bytes=384 << 20, max_evaluator_payload_bytes=128 << 20, max_result_bytes=128 << 20)


def validate_nomination(path, config, config_pin, evidence, native, recovered, dimensions=768):
    pin = local.identity(path); require(pin['bytes'] <= config['max_result_bytes'], 'nomination output cap')
    query_hash, aggregate = request_hashes(config['requests'], evidence['inputs']['requests'], config['first'], dimensions)
    exact(aggregate, evidence['query_f32_sha256'], 'original aggregate query-f32 identity')
    with positive.open_input(path) as stream:
        before = positive.stamp(stream)
        lines = stream.read(pin['bytes']+1).splitlines(keepends=True)
        exact(positive.stamp(stream), before, 'nomination stable')
    require(all(l.endswith(b'\n') and len(l) <= 8 << 20 for l in lines), 'complete bounded event lines')
    exact(len(lines), 131, 'identity+128 selections+marker+terminal')
    identity = local.decode(lines[0]); marker = local.decode(lines[129]); terminal = local.decode(lines[130])
    binding = dict(config_sha256=config_pin['sha256'], candidate_root_sha256=config['candidate_root']['sha256'],
        requests_sha256=config['requests']['sha256'], module_source_sha256=native['sources']['module']['sha256'],
        binary_source_sha256=native['sources']['binary']['sha256'], source_identity_sha256=rust_build_hash(evidence['build']))
    for n, v in binding.items():
        exact(identity[n], v, 'nomination identity: '+n); exact(marker[n], v, 'freeze identity: '+n)
    exact(identity['source_identity'], evidence['build'], 'authenticated original source descriptors')
    exact(identity['phase'], 'identity', 'nomination identity phase'); exact(identity['schema'], NATIVE_SCHEMA, 'nomination schema')
    exact(identity['limits'], config['limits'], 'nomination admitted limits')
    for e in (identity, marker):
        exact(e['count'], 64, 'full64'); exact(e['first'], config['first'], 'first ordinal'); exact(e['policies'], POLICIES, 'fixed policy order')
        exact(e['truth_opened'], False, 'no truth')
    for n in ('record_scoring_performed', 'scientific_qualification', 'quality_or_performance_claim', 'physical_s3_measured'):
        exact(identity[n], False, 'routing scope')
    exact(identity['candidate_descriptor_authentication_bytes'], config['candidate_root']['bytes'], 'separate candidate descriptor authentication charge')
    for n, byte_count in (('startup', config['candidate_root']['bytes']), ('startup_directory', recovered['directories']['bytes'])):
        exact(identity[n], dict(failed_gets=0, requested_bytes=byte_count, submitted_gets=1, verified_bytes=byte_count), 'startup charged')
    admission = identity['directory_admission']; encoded = recovered['directories']['bytes']
    parsed = encoded*4+(recovered['manifest']['build']['directories']+len(recovered['leaves']))*256+8*65536
    exact(admission['encoded_bytes'], encoded, 'resident encoded bytes')
    exact(admission['modeled_parsed_payload_bytes'], parsed, 'resident parsed payload model')
    exact(admission['modeled_preload_peak_bytes'], encoded+parsed, 'resident coexistence model')
    require(0 < admission['parsed_owned_capacity_bytes'] <= parsed and encoded+parsed <= config['max_resident_directory_payload_bytes'], 'resident payload admission')
    exact(identity['modeled_evaluator_payload_bytes'], config['requests']['bytes']*4+(32 << 20), 'evaluator model')
    require(identity['modeled_evaluator_payload_bytes'] <= config['max_evaluator_payload_bytes'], 'evaluator admission')
    parity, summaries = [], []
    for position, line in enumerate(lines[1:129]):
        e = local.decode(line); index, arm = divmod(position, 2)
        exact(e['phase'], 'selection_frozen', 'selection phase'); exact(e['ordinal'], config['first']+index, 'ordered selection ordinal')
        exact(e['query_sha256'], query_hash[index], 'original ordinal/query bits'); exact(e['truth_opened'], False, 'truth-free selection')
        r = e['receipt']; exact(r['policy'], POLICIES[arm], 'ordered arms'); exact(len(r['selected']), 24, 'fixed fetch24')
        exact(r['resident_leaf_cells'], len(recovered['leaves']), 'all authenticated leaves')
        require(r['routing_distance_evaluations_bound'] > 0, 'routing comparisons')
        exact(r['routing_coordinate_evaluations_bound'], r['routing_distance_evaluations_bound']*dimensions, 'coordinate count')
        if arm:
            exact(r['routing_distance_evaluations_bound'], len(recovered['leaves']), 'global leaf evaluations')
        else:
            exact(r['routing_distance_evaluations_bound'], 2*32*(evidence['build']['max_depth']+1)+24, 'hierarchical routing bound')
        seen, primary, covered, bytes_count, order = set(), set(), set(), 0, []
        for j, selected in enumerate(r['selected']):
            cell_id = selected['cell_id']; local.integer(cell_id, 0, len(recovered['leaves'])-1, 'selected cell ID'); require(cell_id in recovered['leaves'] and cell_id not in seen, 'selected unique authenticated cells'); seen.add(cell_id)
            leaf = recovered['leaves'][cell_id]; c = leaf['cell']
            for n, v in dict(prototype_bits=leaf['prototype_bits'], first_row=c['first_row'], source_ids=leaf['source_ids'],
                    source_offset=c['source']['offset'], source_bytes=c['source']['bytes'], source_sha256=c['source']['sha256'],
                    whole_cell_bytes=c['whole']['bytes']).items():
                exact(selected[n], v, 'selected source/prototype: '+n)
            bits = selected['distance_bits']; local.integer(bits, 0, (1 << 32)-1, 'distance bits')
            distance = struct.unpack('<f', struct.pack('<I', bits))[0]
            require(math.isfinite(distance) and distance >= 0 and struct.pack('<f', selected['distance']) == struct.pack('<I', bits), 'lossless finite distance')
            # f32::total_cmp key, including negative zero.
            signed = bits if bits < 1 << 31 else bits-(1 << 32)
            order.append((signed ^ ((signed >> 31) & 0x7fffffff), cell_id))
            exact(type(selected['primary']), bool, 'primary flag')
            if arm:
                exact(selected['primary'], j < 8, 'global primary first8')
            require(all(type(n) is int for n in selected['source_ids']), 'selected source ordinal types')
            covered.update(leaf['source_ids']); bytes_count += c['source']['bytes']
            if selected['primary']:
                primary.update(leaf['source_ids'])
        exact(order, sorted(order), 'distance.total_cmp/cell-ID order')
        exact(sum(c['primary'] for c in r['selected']), 8, 'primary8')
        exact(r['primary_ids'], sorted(primary), 'primary source membership'); exact(r['covered_ids'], sorted(covered), 'covered source membership')
        a = r['accounting']
        for tier in ('directory', 'whole_cell', 'refinement'):
            exact(a[tier], dict(failed_gets=0, requested_bytes=0, submitted_gets=0, verified_bytes=0), 'inactive query tier')
        exact(a['source'], dict(failed_gets=0, requested_bytes=bytes_count, submitted_gets=24, verified_bytes=bytes_count), 'authenticated source reads')
        cell_rows = evidence['build']['cell_rows']
        modeled = 8*32*(dimensions*4+((cell_rows+31)//32)*128+512)+dimensions*16+24*cell_rows*128+2*config['limits']['max_source_bytes']+65536
        exact(a['modeled_query_payload_bytes'], modeled, 'unchanged nomination payload model')
        require(bytes_count <= config['limits']['max_source_bytes'] and modeled <= config['limits']['max_query_payload_bytes'], 'query admission')
        exact(len(a['waves']), 1, 'source-only wave'); w = a['waves'][0]
        for n, v in dict(stage='source', dependency=1, max_parallel_gets=1, submitted_gets=24, requested_bytes=bytes_count, verified_bytes=bytes_count).items():
            exact(w[n], v, 'source wave')
        if not arm:
            for n in ('primary_ids', 'covered_ids'):
                exact(r[n], evidence['traces'][index][n], 'original hierarchy per-query parity')
            parity.append(dict(ordinal=e['ordinal'], query_sha256=query_hash[index],
                primary_ids_sha256=local.sha(local.canonical(r['primary_ids'])), covered_ids_sha256=local.sha(local.canonical(r['covered_ids']))))
        summaries.append(dict(ordinal=e['ordinal'], policy=r['policy'], source_bytes=bytes_count, source_ids=len(covered)))
    exact(marker['phase'], 'all_selections_frozen', 'freeze phase'); exact(marker['schema'], NATIVE_SCHEMA, 'freeze schema')
    prefix = b''.join(lines[:129]); exact(marker['prefix_bytes'], len(prefix), 'verified prefix length'); exact(marker['prefix_sha256'], local.sha(prefix), 'reread prefix hash')
    exact(marker['selection_receipts'], 128, 'all selections frozen')
    for n, v in dict(phase='terminal', status='NOMINATIONS_FROZEN', complete=True, queries=64,
            selection_receipts=128, truth_opened=False, scientific_qualification=False, quality_or_performance_claim=False).items():
        exact(terminal[n], v, 'complete nomination terminal')
    fsync_file(path)
    exact(body_pin(local.identity(path)), body_pin(pin), 'whole file stable after validation')
    return dict(nomination=pin, config=config_pin, candidate_root_sha256=config['candidate_root']['sha256'],
        requests=config['requests'], source_identity_sha256=binding['source_identity_sha256'],
        control_parity=dict(queries=64, matched=True, original_trace=evidence['trace_pin'],
            query_f32_sha256=aggregate, per_query=parity), selected_resources=summaries)


def seal_pair(output, config_pin, roles, datasets):
    exact(set(datasets), set(DATASETS), 'both complete datasets before seal')
    out = Path(output)
    for d, value in datasets.items():
        for n in ('nomination', 'config', 'root', 'directories', 'cells', 'requests'):
            p = value[n]; fsync_file(p['path']); exact(body_pin(local.identity(p['path'])), body_pin(p), 'pair stable before seal')
            value[n] = dict(p, path=str(Path(p['path']).relative_to(out)))
    value = dict(schema=SEAL_SCHEMA, status='NOMINATIONS_FROZEN', complete=True, truth_opened=False,
        config=config_pin, roles=roles, datasets=datasets, scientific_qualification=False,
        quality_or_performance_claim=False, physical_s3_measured=False)
    pin = local.write_json(out/'paired-seal.json', value); fsync_dir(out)
    return pin


def verify_pair(output, *, repo=None, config=None, config_pin=None, evidence=None):
    out = Path(output).resolve(); repo = Path(repo or Path(__file__).resolve().parents[1])
    if config is None:
        config_pin = local.identity(out/'config.json'); config = local.read_json(config_pin, 512 << 10)
    seal = local.read_json(local.identity(out/'paired-seal.json'), 8 << 20)
    exact(seal['schema'], SEAL_SCHEMA, 'paired seal schema'); exact(seal['complete'], True, 'paired complete')
    exact(seal['truth_opened'], False, 'paired no truth'); exact(seal['status'], 'NOMINATIONS_FROZEN', 'paired status')
    exact(body_pin(seal['config']), body_pin(config_pin), 'paired frozen config')
    roles = {r: qualify_role(r, config['roles'][r], repo) for r in ROLE_NAMES}
    exact(seal['roles'], roles, 'paired separate authorities')
    evidence = evidence or original_evidence(repo, config)
    exact(set(seal['datasets']), set(DATASETS), 'paired dataset roster')
    for d in DATASETS:
        value = seal['datasets'][d]
        pins = {}
        for n in ('nomination', 'config', 'root', 'directories', 'cells', 'requests'):
            publication.relative(value[n]['path']); pins[n] = dict(value[n], path=str(out/value[n]['path']))
            local.authenticate(pins[n], 256 << 20)
        old = evidence['items'][d]
        exact(pins['root']['sha256'], old['recovery']['expected_cell_root_sha256'], 'paired original root')
        recovered = layout(pins['root'], old['build'])
        exact(body_pin(recovered['directories']), body_pin(pins['directories']), 'retained directory pin')
        exact(body_pin(recovered['cells']), body_pin(pins['cells']), 'retained cells pin')
        native_config = local.read_json(pins['config'])
        fields(native_config, 'schema candidate_root requests first count limits max_resident_directory_payload_bytes max_evaluator_payload_bytes max_result_bytes', 'strict nomination config')
        exact(native_config, nomination_config(old['diagnostic']['candidate_root'], old['inputs']['requests'], old['diagnostic']['first']), 'exact original root/requests/fixed nomination configuration')
        # Archived config keeps exact runtime paths; validation uses retained
        # bytes with only the open descriptor rebased, and the original config SHA.
        original_requests = native_config['requests']
        exact(body_pin(original_requests), body_pin(pins['requests']), 'retained request identity')
        rebound = dict(native_config, requests=pins['requests'])
        result = validate_nomination(pins['nomination']['path'], rebound, pins['config'], old,
                                    config['roles']['nomination']['native'], recovered)
        for n in ('candidate_root_sha256', 'source_identity_sha256', 'control_parity', 'selected_resources'):
            exact(result[n], value[n], 'replayed paired receipt')
    return seal


def archive_sources(archive, inventory, subset, destinations, check):
    """Verify all qualified native entries in the role's own archive, no exec."""
    check()
    local.authenticate(archive, 1 << 30)
    seen = set()
    with positive.open_input(archive['path']) as stream, tarfile.open(fileobj=stream, mode='r|*') as tar:
        for entry in tar:
            # Tar traversal only reads. The monitor still inventories scratch
            # every second; synchronous inventories bracket every file write.
            check(scan=False)
            if entry.name not in inventory:
                continue
            require(entry.isfile() and not entry.issparse() and entry.name not in seen, 'native archive regular unique entry')
            publication.relative(entry.name)
            digest, size = hashlib.sha256(), 0
            source = tar.extractfile(entry)
            require(source is not None, 'native archive body')
            chunks = [] if entry.name in subset else None
            with source:
                while True:
                    check(scan=False)
                    chunk = source.read(65536)
                    if not chunk:
                        break
                    size += len(chunk); require(size <= 8 << 20, 'native source bound')
                    digest.update(chunk)
                    if chunks is not None:
                        chunks.append(chunk)
            exact(digest.hexdigest(), inventory[entry.name], 'role archive native source SHA')
            if chunks is not None:
                exact(dict(bytes=size, sha256=digest.hexdigest()), body_pin(subset[entry.name]), 'role source size')
                check()
                copy_bytes(destinations[entry.name], b''.join(chunks))
                check()
            seen.add(entry.name)
    exact(seen, set(inventory), 'all qualified archive-native/support files authenticated')
    local.authenticate(archive, 1 << 30)
    check()


def cgroup_snapshot(path, memory, cpu, tasks=512):
    group = Path(path)
    value = {n: (group/n).read_text().strip() for n in (
        'memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak', 'memory.events', 'cpu.max', 'pids.max', 'cgroup.procs')}
    require(value['memory.max'] != 'max' and 0 < int(value['memory.max']) <= memory
            and int(value['memory.peak']) <= memory, 'actual cgroup memory bound')
    exact(value['memory.swap.max'], '0', 'actual no-swap limit'); exact(int(value['memory.swap.peak']), 0, 'actual zero swap peak')
    q, p = map(int, value['cpu.max'].split()); exact(q*100, cpu*p, 'actual CPU quota')
    exact(value['pids.max'], str(tasks), 'actual tasks bound')
    value['path'] = str(group)
    return value


def no_oom(before, after):
    exact(before['path'], after['path'], 'same cgroup resource closure')
    counts = [dict(line.split() for line in x['memory.events'].splitlines()) for x in (before, after)]
    require(all(counts[0].get(k) == counts[1].get(k) for k in ('oom', 'oom_kill', 'oom_group_kill')), 'no new cgroup OOM')


def owned_stage(spec_pin, receipt_path):
    """Runs only within the root-created bounded unit, recording native closure."""
    spec = local.read_json(spec_pin, 65536)
    fields(spec, 'schema name command binary config output resources timeout_seconds', 'owned native stage')
    exact(spec['schema'], 'borsuk-global-leaf-owned-stage-v1', 'owned stage schema')
    limits = spec['resources']; stages = []; started = time.monotonic()
    old_term = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('owned unit terminated')))
    result = dict(schema='borsuk-global-leaf-owned-stage-receipt-v1', status='INVALID', complete=False)
    try:
        native_output = ORIGINAL_ROOT/'screen/measurement' if Path(spec['binary']['path']).is_relative_to(ORIGINAL_ROOT) else Path(spec['output'])
        local.run_stage(spec['name'], spec['command'], spec['binary'], spec['config'], native_output, limits,
                        time.monotonic()+spec['timeout_seconds'], stages)
        result.update(status='CLOSED', complete=True)
    finally:
        signal.signal(signal.SIGTERM, old_term)
        result.update(stages=stages, wall_seconds=time.monotonic()-started)
        if stages and 'path' in stages[-1].get('cgroup_after', {}):
            group = Path(stages[-1]['cgroup_after']['path'])
            result['cgroup'] = cgroup_snapshot(group, limits['memory_max_bytes'], 100*len(limits['cpu_affinity']))
            require({int(p) for p in result['cgroup']['cgroup.procs'].split()} == {os.getpid()}, 'native descendants drained')
        local.write_json(Path(receipt_path), result)
    return result


def native_stage(name, command, binary, config_pin, out, resources, phase_seconds, deadline, stages, check):
    """One serial unit, separately bounded nomination, exact-unit kill/wait."""
    check(); local.authenticate(binary, 256 << 20); local.authenticate(config_pin, 65536)
    exact(command[0], binary['path'], 'exact binary role before spawn')
    require(os.access(binary['path'], os.X_OK), 'qualified binary executable')
    remaining = min(phase_seconds, deadline-time.monotonic())
    require(remaining > 5, 'owned stage remaining deadline')
    slice_name = os.environ.get('BORSUK_GLOBAL_LEAF_SLICE', '')
    require(re.fullmatch(r'borsuk-global-leaf-[a-z0-9-]+\.slice', slice_name), 'root-owned host slice required')
    unit = 'borsuk-global-leaf-'+str(os.getpid())+'-'+name
    spec = dict(schema='borsuk-global-leaf-owned-stage-v1', name=name, command=command, binary=binary,
        config=config_pin, output=str(out), resources=resources, timeout_seconds=max(1, int(remaining)-5))
    spec_pin = local.write_json(out/(name+'-stage.json'), spec)
    receipt = out/(name+'-stage-receipt.json')
    shell = ['systemd-run', '--unit='+unit, '--slice='+slice_name, '--wait', '--pipe',
        '--property=MemoryMax='+str(resources['memory_max_bytes']), '--property=MemorySwapMax=0',
        '--property=CPUQuota='+str(100*len(resources['cpu_affinity']))+'%', '--property=TasksMax=512',
        '--property=RuntimeMaxSec='+str(max(1, int(remaining))), '--property=KillMode=control-group',
        '--property=TimeoutStopSec=5', '--property=WorkingDirectory='+str(out),
        '--setenv=PYTHONPATH='+str(Path(__file__).resolve().parents[1]),
        '--setenv=RAYON_NUM_THREADS='+str(len(resources['cpu_affinity'])),
        '--setenv=OMP_NUM_THREADS='+str(len(resources['cpu_affinity'])),
        '--setenv=OPENBLAS_NUM_THREADS='+str(len(resources['cpu_affinity'])),
        '--setenv=MKL_NUM_THREADS='+str(len(resources['cpu_affinity'])),
        'taskset', '-c', ','.join(map(str, resources['cpu_affinity'])), sys.executable,
        str(Path(__file__).resolve()), '--owned-stage', spec_pin['path'], spec_pin['sha256'], str(receipt)]
    process, failure = None, None
    record = dict(name=name, unit=unit+'.service', command=command, supervisor_command=shell,
                  exit_status=None, closed=False, unit_drained=False, resource_gate_passed=False)
    stages.append(record); started = time.monotonic()
    log = out/(name+'-unit.log')
    try:
        with log.open('xb') as stream:
            process = subprocess.Popen(shell, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            while process.poll() is None:
                check(); require(log.stat().st_size <= resources['max_log_bytes'], 'unit log cap')
                if time.monotonic()-started >= remaining:
                    raise TimeoutError('owned native phase deadline')
                time.sleep(.05)
            record['exit_status'] = process.wait()
            stream.flush(); os.fsync(stream.fileno())
        exact(record['exit_status'], 0, 'owned native unit exit0')
        inner = local.read_json(local.identity(receipt), 1 << 20)
        exact(inner['status'], 'CLOSED', 'native process closed'); exact(inner['complete'], True, 'native complete')
        exact(len(inner['stages']), 1, 'one serial native stage')
        stage = inner['stages'][0]
        exact(stage['command'], command, 'actual native command')
        exact(stage['binary'], binary, 'actual binary role'); exact(stage['config'], config_pin, 'actual config bytes')
        for key, expected in dict(exit_status=0, cleanup_complete=True, resource_gate_passed=True).items():
            exact(stage[key], expected, 'actual native exit/resource cleanup')
        # The inner cgroup is a real kernel limit, distinct from payload models.
        require(int(inner['cgroup']['memory.max']) <= resources['memory_max_bytes'], 'inner actual memory cap')
        record['native_receipt'] = local.identity(receipt)
        native_log = stage['log']
        if Path(native_log['path']).parent != out:
            with positive.open_input(native_log['path']) as stream:
                publication.transfer(stream, body_pin(native_log), out/(name+'.log'))
        record['native_log'] = local.identity(out/(name+'.log'))
        exact(body_pin(record['native_log']), body_pin(native_log), 'retained native log')
        record['resource_gate_passed'] = True
    except BaseException as error:
        failure = error
    finally:
        if process is not None:
            if process.poll() is None:
                subprocess.run(['systemctl', 'kill', '--kill-whom=all', '--signal=KILL', unit+'.service'],
                               timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            subprocess.run(['systemctl', 'stop', unit+'.service'], timeout=10,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            try:
                record['exit_status'] = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL); record['exit_status'] = process.wait(timeout=5)
                if failure is None:
                    failure = ValueError('owned supervisor did not drain normally')
            state = subprocess.run(['systemctl', 'show', unit+'.service', '--property=ActiveState',
                '--property=SubState', '--property=MainPID', '--property=ControlGroup', '--property=LoadState'],
                timeout=10, capture_output=True, text=True, check=False)
            value = dict(line.split('=', 1) for line in state.stdout.splitlines() if '=' in line)
            group = Path('/sys/fs/cgroup'+value.get('ControlGroup', ''))
            empty = not value.get('ControlGroup') or not group.exists() or not (group/'cgroup.procs').read_text().strip()
            record['cgroup_drained'] = empty
            record['unit_drained'] = value.get('MainPID') == '0' and value.get('ActiveState') in ('inactive', 'failed') and empty
            record['unit_closeout'] = value
            record['closed'] = True
        else:
            record['closed'] = True  # no native unit was spawned
        record['wall_seconds'] = time.monotonic()-started
        if receipt.exists():
            record['native_receipt'] = local.identity(receipt)
        native_log = (ORIGINAL_ROOT/'screen/measurement' if Path(binary['path']).is_relative_to(ORIGINAL_ROOT) else out)/(name+'.log')
        if native_log.exists():
            if native_log.parent != out and not (out/(name+'.log')).exists():
                with positive.open_input(native_log) as stream:
                    publication.transfer(stream, body_pin(local.identity(native_log)), out/(name+'.log'))
            record['native_log'] = local.identity(out/(name+'.log'))
        record['log'] = local.identity(log) if log.exists() else None
        local.write_json(out/(name+'-closure.json'), record)
    require(record['closed'] and record['unit_drained'], 'owned native unit closure')
    if failure is not None:
        raise failure
    check()
    return record


def reconstruct_writer_command(item, role):
    """Authenticate exact original writer bytes without touching build/query inputs."""
    old = item['recovery']; proof = role
    exact(proof['binaries']['writer']['path'], str(ORIGINAL_ROOT/'screen/scratch/native-writer'), 'original writer path')
    stem = Path(old['output_cell_path']).name.removesuffix('-cells')
    folder = ORIGINAL_ROOT/'screen/measurement'
    writer = dict(path=str(folder/(stem+'-writer.json')), **body_pin(old['original_writer_config']))
    exact(local.decode(local.authenticate(writer, 65536, read=True)), item['writer'], 'original writer bytes/config')
    return (stem+'-writer', [proof['binaries']['writer']['path'], writer['path'], writer['sha256'], '67108864',
        str(folder/(stem+'-generation'))], proof['binaries']['writer'], writer)


def reconstruct_command(item, role):
    """Frozen role/path/flags admission happens before any invocation."""
    writer_command = reconstruct_writer_command(item, role)
    old = item['recovery']; proof = role
    exact(proof['binaries']['cells']['path'], str(ORIGINAL_ROOT/'screen/scratch/native-cells'), 'original cells path')
    stem = Path(old['output_cell_path']).name.removesuffix('-cells')
    build = dict(path=str(ORIGINAL_ROOT/'screen/measurement'/(stem+'-build.json')), **body_pin(old['original_build_config']))
    exact(local.decode(local.authenticate(build, 65536, read=True)), item['build'], 'original build bytes/config')
    return [writer_command,
        (stem+'-build', [proof['binaries']['cells']['path'], 'build', build['path'], build['sha256'], old['output_cell_path']],
         proof['binaries']['cells'], build)]


def restore_writer_inputs(original, consumed, folder, download, check):
    """Restore exact source/order/SQ8 only, including original Parquet conversion."""
    folder.mkdir(parents=True)
    pins = {}
    for name in ('source', 'order.u64', 'sq8.bin'):
        check(); path = folder/dict(source='source', **{'order.u64':'order', 'sq8.bin':'sq8'})[name]
        download(original['artifacts'][name], path); pins[name] = local.identity(path)
        exact(body_pin(pins[name]), body_pin(original['artifacts'][name]), 'original source asset')
    if original['name'] == 'relaion':
        identities = {}
        with positive.open_input(pins['source']['path']) as source, positive.output_file(folder/'raw', identities) as write:
            for block in positive.raw_blocks(source, 100000, 768):
                check(); write(block)
        pins['raw'] = identities['raw']
    else:
        pins['raw'] = pins['source']
    for n in ('raw', 'order', 'sq8'):
        staged = pins[n] if n == 'raw' else pins[dict(order='order.u64', sq8='sq8.bin')[n]]
        exact(body_pin(staged), body_pin(consumed['inputs'][n]), 'exact writer input bytes')
        # CoHere originally consumed source directly; ReLAION consumed raw.
        exact(staged['path'], consumed['inputs'][n]['path'], 'original consumed input path')
    return pins


def restore_panel(original, consumed, folder, download, check):
    """Only raw/order/SQ8 and requests; historical truth stays opaque metadata."""
    pins = restore_writer_inputs(original, consumed, folder, download, check)
    check(); path = folder/'full-requests'
    download(original['artifacts']['requests'], path); pins['requests'] = local.identity(path)
    exact(body_pin(pins['requests']), body_pin(original['artifacts']['requests']), 'original source asset')
    with positive.open_input(pins['requests']['path']) as stream:
        before = positive.stamp(stream); lines = [stream.readline(65537) for _ in range(64)]
        require(all(0 < len(l) <= 65536 and l.endswith(b'\n') for l in lines), 'original consumed prefix64')
        exact(positive.stamp(stream), before, 'original request body stable')
    bits, identities = hashlib.sha256(), {}
    with positive.output_file(folder/'requests64', identities) as write:
        for line in positive.request_bodies(io.BytesIO(b''.join(lines)), 64, 768, bits):
            check(); write(line)
    exact(body_pin(identities['requests64']), body_pin(consumed['inputs']['requests']), 'exact original consumed64 bytes')
    exact(bits.hexdigest(), consumed['query_f32_sha256'], 'exact original f32 query panel')


def execute(config, config_pin, repo, out, download, check, deadline):
    """No GT capability enters this execution; output survives failed dataset2."""
    out, repo = Path(out), Path(repo)
    role_ids = {r: qualify_role(r, config['roles'][r], repo) for r in ROLE_NAMES}
    evidence = original_evidence(repo, config)
    exact(config['resources'], CAPS, 'frozen host/nomination resources')
    fields(config['phase_seconds'], 'writer build nominate', 'phase deadlines')
    for n, seconds in config['phase_seconds'].items():
        local.integer(seconds, 10, 1669, 'phase seconds: '+n)
    require(not ORIGINAL_ROOT.exists() and not ORIGINAL_ROOT.is_symlink(), 'original geometry already occupied')
    require(out.is_absolute() and out.exists() and not out.is_symlink(), 'owned output root')
    receipt = dict(schema=RECEIPT_SCHEMA, status='INVALID', complete=False, truth_opened=False,
        config=config_pin, roles=role_ids, stages=[], datasets={}, cleanup=dict(original_root_removed=False),
        scientific_qualification=False, quality_or_performance_claim=False)
    started = time.monotonic(); owned = False; failure = None
    measurement = out/'measurement'; measurement.mkdir()
    retained = out/'retained'; retained.mkdir()
    scratch = out/'scratch'; require(scratch.is_dir(), 'stage scratch owner')
    try:
        ORIGINAL_ROOT.mkdir(); owned = True
        proof = copy.deepcopy(evidence['proof'])
        for role in ROLE_NAMES:
            authority = config['roles'][role]['native']; target = ORIGINAL_ROOT/'screen/scratch' if role == 'original' else scratch/'nomination'
            target.mkdir(parents=True, exist_ok=True)
            for n, p in [('source_archive', authority['source_archive']), ('gate_log', authority['gate_log']), *authority['binaries'].items()]:
                check(); path = target/('native-'+n)
                download(p, path); pin = local.identity(path); exact(body_pin(pin), body_pin(p), 'qualified cold role body')
                if n in authority['binaries']:
                    path.chmod(0o700)
                if role == 'original':
                    exact(pin, proof['binaries'][n] if n in proof['binaries'] else proof[n], 'original cold geometry')
            subset = {p['path']: p for p in authority['sources'].values()}
            destinations = {n: ORIGINAL_ROOT/'repo'/n if role == 'original' else target/'sources'/n for n in subset}
            archive_sources(dict(body_pin(authority['source_archive']), path=str(target/'native-source_archive')),
                            dict(role_ids[role]['source_sha256'], **role_ids[role]['source_archive_support_sha256']), subset, destinations, check)
        nominee = copy.deepcopy(config['roles']['nomination']['native'])
        nominee['binaries']['cells'] = dict(body_pin(nominee['binaries']['cells']), path=str(scratch/'nomination/native-cells'))
        for original in evidence['sources']['items']:
            d = original['name']; require(d in DATASETS, 'original source dataset')
            item = evidence['items'][d]
            restore_panel(original, item, ORIGINAL_ROOT/'screen/scratch'/d, download, check)
            copy_bytes(ORIGINAL_ROOT/'screen/measurement'/f'{d}-writer.json', item['writer_bytes'])
            copy_bytes(ORIGINAL_ROOT/'screen/measurement'/f'{d}-build.json', item['build_bytes'])
        # Validate BOTH datasets' roles, input identities and raw configs before
        # starting the first original writer; a drift cannot leave partial work.
        commands = {d: reconstruct_command(evidence['items'][d], proof) for d in DATASETS}
        limits = {k: v for k, v in CAPS.items() if k not in ('nomination_memory_bytes', 'nomination_cpu_affinity', 'swap_bytes')}
        recovered, retained_pins = {}, {}
        for d in DATASETS:
            item = evidence['items'][d]
            for name, command, binary, pin in commands[d]:
                if name.endswith('-build'):
                    for p in item['recovery']['build_inputs'].values():
                        local.authenticate(p, 1 << 30)
                native_stage(name, command, binary, pin, measurement, dict(limits, timeout_seconds=config['phase_seconds']['writer' if name.endswith('-writer') else 'build']),
                    config['phase_seconds']['writer' if name.endswith('-writer') else 'build'], deadline, receipt['stages'], check)
                if name.endswith('-writer'):
                    for p in item['recovery']['build_inputs'].values():
                        local.authenticate(p, 1 << 30)
            root = local.identity(Path(item['recovery']['output_cell_path'])/'manifest.json')
            exact(root['sha256'], item['recovery']['expected_cell_root_sha256'], 'exact original recovered root')
            exact(body_pin(root), body_pin(item['diagnostic']['candidate_root']), 'exact original root bytes/length')
            recovered[d] = layout(root, item['build'])
            # Every authenticated recovered layout survives a later native
            # failure. Copy it immediately, before any query or cleanup.
            target = retained/d; target.mkdir()
            retained_pins[d] = {}
            for n in ('root', 'directories', 'cells'):
                original_pin = recovered[d][n]; destination = target/Path(original_pin['path']).name
                with positive.open_input(original_pin['path']) as stream:
                    publication.transfer(stream, body_pin(original_pin), destination)
                retained_pins[d][n] = local.identity(destination)
                exact(body_pin(retained_pins[d][n]), body_pin(original_pin), 'retained original layout')
            with positive.open_input(item['inputs']['requests']['path']) as stream:
                publication.transfer(stream, body_pin(item['inputs']['requests']), target/'requests64')
            retained_pins[d]['requests'] = local.identity(target/'requests64')
            copy_bytes(target/'original-build.json', item['build_bytes'])
            copy_bytes(target/'original-writer.json', item['writer_bytes'])
            fsync_dir(target)
            receipt['datasets'][d] = dict(root_sha256=root['sha256'], retained=True)
        exact(set(recovered), set(DATASETS), 'BOTH roots authenticated before first query')
        datasets = {}
        for d in DATASETS:
            item, recovered_item = evidence['items'][d], recovered[d]
            native_config = nomination_config(recovered_item['root'], item['inputs']['requests'], item['diagnostic']['first'])
            pin = local.write_json(measurement/(d+'-nominate.json'), native_config)
            path = measurement/(d+'-nominations.jsonl'); binary = nominee['binaries']['cells']
            resources = dict(limits, memory_max_bytes=512 << 20, cpu_affinity=[0], timeout_seconds=config['phase_seconds']['nominate'])
            native_stage(d+'-nominate', [binary['path'], 'nominate', pin['path'], pin['sha256'], str(path)], binary, pin,
                measurement, resources, config['phase_seconds']['nominate'], deadline, receipt['stages'], check)
            datasets[d] = validate_nomination(path, native_config, pin, item, nominee, recovered_item)
            datasets[d].update(retained_pins[d])
        receipt['paired_seal'] = seal_pair(out, config_pin, role_ids, datasets)
        receipt['datasets'] = {d: dict(root_sha256=datasets[d]['candidate_root_sha256'], retained=True) for d in DATASETS}
        receipt.update(status='NOMINATIONS_FROZEN', complete=True)
    except BaseException as error:
        failure = error; receipt['error'] = type(error).__name__+': '+str(error)
    finally:
        if owned:
            shutil.rmtree(ORIGINAL_ROOT)
        receipt['cleanup']['original_root_removed'] = owned and not ORIGINAL_ROOT.exists()
        receipt['cleanup']['native_units_drained'] = bool(receipt['stages']) and all(s['closed'] and s['unit_drained'] for s in receipt['stages'])
        receipt['cleanup']['native_processes_concurrent_max'] = 1 if receipt['stages'] else 0
        receipt['wall_seconds'] = time.monotonic()-started
        receipt['remaining_deadline_seconds'] = deadline-time.monotonic()
        if not receipt['cleanup']['native_units_drained'] or receipt['remaining_deadline_seconds'] <= 0:
            receipt.update(status='INVALID', complete=False)
        local.write_json(out/'native-execution-receipt.json', receipt); fsync_dir(out)
    if failure:
        raise failure
    require(receipt['complete'] and receipt['cleanup']['original_root_removed'] and receipt['cleanup']['native_units_drained'], 'complete execution/resource/cleanup receipt')
    return receipt


def validate_cgroup(value, memory, cpu, tasks=512):
    require(0 < int(value['memory.max']) <= memory and int(value['memory.peak']) <= memory, 'closed actual memory cap')
    exact(value['memory.swap.max'], '0', 'closed noSwap cap'); exact(int(value['memory.swap.peak']), 0, 'closed swap peak')
    q, p = map(int, value['cpu.max'].split()); exact(q*100, cpu*p, 'closed actual CPU cap')
    exact(value['pids.max'], str(tasks), 'closed tasks cap')


def verify_worker_closure(output, pins, config):
    out = Path(output); fields(pins, 'resources cleanup cgroup', 'closed host/resource/cleanup roster')
    names = dict(resources='resources.json', cleanup='cleanup.json', cgroup='worker-cgroup.json')
    values = {n: local.read_json(dict(body_pin(pin), path=str(out/names[n])), 8 << 20) for n, pin in pins.items()}
    resource, clean, group = (values[n] for n in ('resources', 'cleanup', 'cgroup'))
    exact(group['closed'], True, 'aggregate host cgroup closed')
    for n in ('host_before', 'host_after'):
        validate_cgroup(group[n], 2 << 30, 200)
    no_oom(group['host_before'], group['host_after']); no_oom(group['before'], group['after'])
    for n in ('before', 'after'):
        value = group[n]
        require(0 < int(value['memory.max']) <= 2 << 30 and int(value['memory.peak']) <= 2 << 30, 'host controller memory cap')
        exact(value['memory.swap.max'], '0', 'host controller noSwap'); exact(int(value['memory.swap.peak']), 0, 'host controller swap peak')
        exact(value['cpu_affinity'], [0, 1], 'host controller CPU2')
        q, p = map(int, value['cpu_max'].split()); exact(q*100, 200*p, 'host controller CPU quota')
        exact(value['tasks_max'], '512', 'host controller tasks')
    require(0 <= resource['scratch_bytes'] <= 16 << 30 and 0 < resource['wall_seconds'] <= config['wall_seconds']
            and 0 < resource['deadline_seconds'] <= config['wall_seconds'] and not resource['monitor_errors'], 'actual host scratch/deadline/monitor closure')
    for n in ('scratch_removed', 'monitor_stopped', 'sdk_client_closed', 'original_root_removed', 'retained_layouts_preserved'):
        exact(clean[n], True, 'actual host cleanup')
    exact(clean['native_processes_concurrent_max'], 1, 'actual serial process ownership')
    return values


def verify_execution(output, config, seal, evidence):
    """Pair-file integrity alone never substitutes for owned execution closure."""
    out = Path(output).resolve(); receipt = local.read_json(local.identity(out/'execution-receipt.json'), 8 << 20)
    exact(receipt['schema'], RECEIPT_SCHEMA, 'execution schema'); exact(receipt['status'], 'NOMINATIONS_FROZEN', 'execution complete')
    exact(receipt['complete'], True, 'execution closed'); exact(receipt['truth_opened'], False, 'execution no truth')
    native_pin = dict(body_pin(receipt['native_execution']), path=str(out/'native-execution-receipt.json'))
    native = local.read_json(native_pin, 8 << 20)
    exact({k: v for k, v in receipt.items() if k not in ('native_execution', 'worker_closure')}, native, 'final/native execution binding')
    verify_worker_closure(out, receipt['worker_closure'], config)
    exact(body_pin(receipt['paired_seal']), body_pin(local.identity(out/'paired-seal.json')), 'execution whole paired seal')
    exact(receipt['roles'], seal['roles'], 'execution two qualified roles')
    exact(body_pin(receipt['config']), body_pin(seal['config']), 'execution frozen configuration')
    require(0 < receipt['wall_seconds'] <= config['wall_seconds'] and receipt['remaining_deadline_seconds'] > 0, 'execution deadline closure')
    for n in ('original_root_removed', 'native_units_drained'):
        exact(receipt['cleanup'][n], True, 'execution cleanup')
    exact(receipt['cleanup']['native_processes_concurrent_max'], 1, 'serial owned native processes')
    exact(set(receipt['datasets']), set(DATASETS), 'execution paired datasets')
    for d in DATASETS:
        exact(receipt['datasets'][d], dict(root_sha256=seal['datasets'][d]['candidate_root_sha256'], retained=True), 'execution retained roots')
    exact(len(receipt['stages']), 6, 'four original reconstruction+two new nominee stages')
    names = [d+'-'+p for d in DATASETS for p in ('writer', 'build')]+[d+'-nominate' for d in DATASETS]
    for n, record in zip(names, receipt['stages']):
        exact(record['name'], n, 'serial native phase order')
        for k, v in dict(exit_status=0, closed=True, unit_drained=True, resource_gate_passed=True).items():
            exact(record[k], v, 'closed owned native stage')
        close = record['unit_closeout']; exact(close['MainPID'], '0', 'unit no live main PID')
        require(close['ActiveState'] in ('inactive', 'failed') and record['cgroup_drained'], 'owned unit inactive/drained')
        stage_pin = record['native_receipt']
        body = local.read_json(dict(body_pin(stage_pin), path=str(out/'measurement'/(n+'-stage-receipt.json'))), 1 << 20)
        exact(body['status'], 'CLOSED', 'actual native closure'); exact(body['complete'], True, 'actual native complete')
        exact(len(body['stages']), 1, 'actual serial process count')
        stage = body['stages'][0]; exact(stage['command'], record['command'], 'actual native invocation')
        exact(stage['exit_status'], 0, 'actual native exit'); exact(stage['cleanup_complete'], True, 'native wait/kill-group'); exact(stage['resource_gate_passed'], True, 'actual native resources')
        no_oom(stage['cgroup_before'], stage['cgroup_after'])
        nominate = n.endswith('-nominate'); memory, cpu = ((512 << 20), 100) if nominate else ((2 << 30), 200)
        validate_cgroup(body['cgroup'], memory, cpu)
        for snap in (stage['cgroup_before'], stage['cgroup_after']):
            require(0 < int(snap['memory.max']) <= memory and int(snap['memory.peak']) <= memory, 'native before/after memory closure')
            exact(snap['memory.swap.max'], '0', 'native noSwap'); exact(int(snap['memory.swap.peak']), 0, 'native swap peak')
            exact(snap['cpu_affinity'], [0] if nominate else [0, 1], 'native CPU affinity')
        require(stage['wall_seconds'] <= config['phase_seconds']['nominate' if nominate else n.split('-')[-1]], 'actual phase deadline')
        d = n.split('-')[0]
        if nominate:
            binary = config['roles']['nomination']['native']['binaries']['cells']
            native_config = seal['datasets'][d]['config']
            exact(record['command'][1], 'nominate', 'new binary only nominate')
            exact(record['command'][3], native_config['sha256'], 'nomination config invocation')
            exact(Path(record['command'][2]).name, d+'-nominate.json', 'nomination config path')
            exact(Path(record['command'][4]).name, d+'-nominations.jsonl', 'nomination output path')
        else:
            phase = n.split('-')[-1]; binary = config['roles']['original']['native']['binaries']['writer' if phase == 'writer' else 'cells']
            old = evidence['items'][d]['recovery']; original_binary = evidence['proof']['binaries']['writer' if phase == 'writer' else 'cells']
            cfg_path = str(ORIGINAL_ROOT/'screen/measurement'/f'{d}-{phase}.json')
            if phase == 'writer':
                expected = [original_binary['path'], cfg_path, old['original_writer_config']['sha256'], '67108864',
                            str(ORIGINAL_ROOT/'screen/measurement'/(d+'-generation'))]
            else:
                expected = [original_binary['path'], 'build', cfg_path, old['original_build_config']['sha256'], old['output_cell_path']]
            exact(record['command'], expected, 'ORIGINAL reconstruction path/config/flags')
            exact(body_pin(stage['config']), body_pin(old['original_'+phase+'_config']), 'original raw config invoked')
        exact(body_pin(stage['binary']), body_pin(binary), 'separate original/nomination binary role')
        exact(stage['binary']['path'], record['command'][0], 'actual binary invocation')
        log = local.identity(out/'measurement'/(n+'.log'))
        exact(body_pin(log), body_pin(stage['log']), 'retained native stage log')
        exact(body_pin(log), body_pin(record['native_log']), 'parent-bound native log')
        exact(body_pin(local.identity(out/'measurement'/(n+'-unit.log'))), body_pin(record['log']), 'retained supervisor log')
    return receipt


def synthetic_dataset(folder):
    """Tiny authenticated format fixture; never a substitute for original roots."""
    folder.mkdir(); rows, dimensions = 48, 2
    build = {n: dict(path='/original/'+n, bytes=rows if n == 'records' else 1, sha256='a'*64)
             for n in ('generation', 'plane', 'canonical', 'order', 'records', 'mean', 'sq8')}
    build.update(schema='borsuk-hierarchical-cells-build-v1', cell_rows=2, sample_rows=2, max_depth=1,
                 max_build_payload_bytes=64 << 20, max_output_bytes=256 << 20)
    children, cells, selected = [], bytearray(), []
    for i in range(24):
        ids = [rows-1-2*i, rows-2-2*i]
        body = b''.join(struct.pack('<q', n)+b'x' for n in ids)
        span = dict(offset=len(cells), bytes=len(body), sha256=local.sha(body)); cells.extend(body)
        refinement = bytes(28); r = dict(offset=len(cells), bytes=28, sha256=local.sha(refinement)); cells.extend(refinement)
        whole = dict(offset=span['offset'], bytes=46, sha256=local.sha(body+refinement))
        c = dict(id=i, first_row=2*i, whole=whole, source=span, refinement=[r])
        children.append(dict(rows=2, prototype=[float(i), 0.0], target=dict(kind='cell', cell=c)))
        selected.append(dict(cell_id=i, distance=float(i), distance_bits=struct.unpack('<I', struct.pack('<f', i))[0],
            primary=i < 8, prototype_bits=[struct.unpack('<I', struct.pack('<f', i))[0], 0], first_row=2*i,
            source_ids=ids, source_offset=span['offset'], source_bytes=span['bytes'], source_sha256=span['sha256'], whole_cell_bytes=46))
    directory = local.canonical(dict(children=children)); copy_bytes(folder/'directories.bin', directory); copy_bytes(folder/'cells.bin', cells)
    manifest = dict(schema='borsuk-hierarchical-cells-resident-v3', input=build, rows=rows, dimensions=dimensions,
        root_directory=dict(offset=0, bytes=len(directory), sha256=local.sha(directory)), directory_bytes=len(directory),
        directory_sha256=local.sha(directory), cell_bytes=len(cells), build=dict(directories=1, cells=24))
    root = local.write_json(folder/'manifest.json', manifest)
    requests = copy_bytes(folder/'requests64', b''.join(local.canonical(dict(ordinal=i, query=[1.0, 0.0])) for i in range(64)))
    qh, aggregate = request_hashes(requests, requests, dimensions=dimensions)
    primary = sorted(n for c in selected[:8] for n in c['source_ids']); covered = list(range(rows))
    evidence = dict(inputs=dict(requests=requests), build=build, query_f32_sha256=aggregate,
        traces=[dict(primary_ids=primary, covered_ids=covered) for _ in range(64)], trace_pin=dict(path='original.jsonl', bytes=1, sha256='b'*64))
    config = dict(schema=NATIVE_SCHEMA, candidate_root=root, requests=requests, first=0, count=64,
        limits=dict(max_source_gets=24, max_source_bytes=16 << 20, max_query_payload_bytes=128 << 20),
        max_resident_directory_payload_bytes=384 << 20, max_evaluator_payload_bytes=128 << 20, max_result_bytes=128 << 20)
    config_pin = local.write_json(folder/'config.json', config)
    native = dict(sources=dict(module=dict(sha256='c'*64), binary=dict(sha256='d'*64)))
    binding = dict(config_sha256=config_pin['sha256'], candidate_root_sha256=root['sha256'], requests_sha256=requests['sha256'],
        module_source_sha256='c'*64, binary_source_sha256='d'*64, source_identity_sha256=rust_build_hash(build))
    common = dict(count=64, first=0, policies=POLICIES, truth_opened=False)
    identity = dict(binding, **common, phase='identity', schema=NATIVE_SCHEMA, source_identity=build, limits=config['limits'],
        record_scoring_performed=False, scientific_qualification=False, quality_or_performance_claim=False, physical_s3_measured=False,
        startup=dict(failed_gets=0, requested_bytes=root['bytes'], submitted_gets=1, verified_bytes=root['bytes']),
        startup_directory=dict(failed_gets=0, requested_bytes=len(directory), submitted_gets=1, verified_bytes=len(directory)),
        candidate_descriptor_authentication_bytes=root['bytes'],
        directory_admission=dict(encoded_bytes=len(directory), modeled_parsed_payload_bytes=len(directory)*4+25*256+8*65536,
            modeled_preload_peak_bytes=len(directory)*5+25*256+8*65536, parsed_owned_capacity_bytes=1000), modeled_evaluator_payload_bytes=requests['bytes']*4+(32 << 20))
    empty = dict(failed_gets=0, requested_bytes=0, submitted_gets=0, verified_bytes=0)
    account = dict(directory=empty, whole_cell=empty, refinement=empty,
        source=dict(failed_gets=0, requested_bytes=432, submitted_gets=24, verified_bytes=432), modeled_query_payload_bytes=8*32*(2*4+128+512)+2*16+24*2*128+2*(16 << 20)+65536,
        waves=[dict(stage='source', dependency=1, max_parallel_gets=1, submitted_gets=24, requested_bytes=432, verified_bytes=432)])
    events = [identity]
    for i in range(64):
        for arm in POLICIES:
            bound = 24 if arm == 'global_top24' else 152
            receipt = dict(policy=arm, resident_leaf_cells=24, routing_distance_evaluations_bound=bound,
                routing_coordinate_evaluations_bound=bound*2, selected=selected, primary_ids=primary, covered_ids=covered, accounting=account)
            events.append(dict(phase='selection_frozen', ordinal=i, query_sha256=qh[i], truth_opened=False, receipt=receipt))
    prefix = b''.join(local.canonical(e) for e in events)
    events.append(dict(binding, **common, schema=NATIVE_SCHEMA, phase='all_selections_frozen', prefix_bytes=len(prefix), prefix_sha256=local.sha(prefix), selection_receipts=128))
    events.append(dict(phase='terminal', status='NOMINATIONS_FROZEN', complete=True, queries=64, selection_receipts=128, truth_opened=False,
                       scientific_qualification=False, quality_or_performance_claim=False))
    path = folder/'nominations.jsonl'; copy_bytes(path, b''.join(local.canonical(e) for e in events))
    return dict(path=path, config=config, pin=config_pin, evidence=evidence, native=native,
                recovered=layout(root, build, dimensions, rows), events=events)


def archive_staging_self_check():
    """Read-only tar members must not each inventory the whole scratch tree."""
    from scripts import launch_hierarchical_cells_100k_spot as launcher
    def rejects(action):
        try:
            action()
        except ValueError:
            return
        raise AssertionError('archive/resource violation accepted')
    with tempfile.TemporaryDirectory(prefix='global-leaf-archive-') as tmp:
        root = Path(tmp)/'worker'; scratch = root/'scratch'; scratch.mkdir(parents=True)
        original = Path(tmp)/'original'; original.mkdir(); (original/'body').write_bytes(b'x'*65536)
        for n in range(128):
            (scratch/str(n)).write_bytes(b'scratch')
        archive = root/'source.tar.gz'; inventory = {}
        with tarfile.open(archive, 'w:gz', format=tarfile.USTAR_FORMAT) as tar:
            for n in range(256):
                name, body = 'support/'+str(n), b'qualified source'
                entry = tarfile.TarInfo(name); entry.size = len(body)
                tar.addfile(entry, io.BytesIO(body)); inventory[name] = local.sha(body)
        scans, polls = 0, 0
        peaks, errors = dict(scratch_bytes=0), []
        baseline, deadline = shutil.disk_usage(root).used, time.monotonic()+30
        def check(*, scan=True):
            nonlocal scans, polls
            scans += int(scan); polls += int(not scan)
            launcher.probe_resource_check(root, baseline, 128 << 20, deadline, errors, peaks, scan=scan)
        pin = local.identity(archive)
        subset = {'support/0': dict(bytes=16, sha256=inventory['support/0'])}
        destination = root/'extracted'
        with patch.object(launcher.probe, 'ORIGINAL_ROOT', original):
            archive_sources(pin, inventory, subset, {'support/0': destination}, check)
            exact(destination.read_bytes(), b'qualified source', 'authenticated archive subset retained')
            require(0 < scans <= 6 and polls >= 256, 'scratch scans bounded by archive/write boundaries, not tar member count')
            require(peaks['scratch_bytes'] >= 65536 and peaks['scratch_scan_calls'] == scans,
                    'separate original scratch charged and scan accounting retained')
            # Check every support body, including entries outside the subset,
            # and retain whole-archive, completeness and duplicate validation.
            rejects(lambda: archive_sources(dict(pin, sha256='0'*64), inventory, {}, {}, check))
            rejects(lambda: archive_sources(pin, dict(inventory, **{'support/255': '0'*64}), {}, {}, check))
            rejects(lambda: archive_sources(pin, dict(inventory, missing='0'*64), {}, {}, check))
            duplicate = root/'duplicate.tar.gz'
            with tarfile.open(duplicate, 'w:gz', format=tarfile.USTAR_FORMAT) as tar:
                for _ in range(2):
                    entry = tarfile.TarInfo('support/0'); entry.size = 16
                    tar.addfile(entry, io.BytesIO(b'qualified source'))
            rejects(lambda: archive_sources(local.identity(duplicate), {'support/0': inventory['support/0']}, {}, {}, check))
            for scan in (True, False):
                rejects(lambda: launcher.probe_resource_check(root, baseline, 128 << 20, time.monotonic()-1,
                                                               [], dict(scratch_bytes=0), scan=scan))
                rejects(lambda: launcher.probe_resource_check(root, baseline, 128 << 20, deadline,
                                                               ['monitor failure'], dict(scratch_bytes=0), scan=scan))
            over = dict(scratch_bytes=0)
            rejects(lambda: launcher.probe_resource_check(root, baseline, 1, deadline, [], over))
            rejects(lambda: launcher.probe_resource_check(root, baseline, 1, deadline, [], over, scan=False))
    print('PASS archive staging traversal falsifier; real gzip/tar/scratch, whole/support/subset/missing/duplicate authentication and scratch/deadline/monitor guards; no native/corpus/GT')


def self_check():
    """Stdlib, mocked process boundaries, tiny format fixture; no native/corpus."""
    def rejects(action):
        try:
            action()
        except (ValueError, OSError, AssertionError, TimeoutError):
            return
        raise AssertionError('negative probe accepted')
    archive_staging_self_check()
    with tempfile.TemporaryDirectory(prefix='global-leaf-synthetic-') as tmp:
        root = Path(tmp); fixture = synthetic_dataset(root/'dataset')
        def validate():
            return validate_nomination(fixture['path'], fixture['config'], fixture['pin'], fixture['evidence'], fixture['native'], fixture['recovered'], 2)
        validate()
        for mutate in (
            lambda v: v[1]['receipt']['selected'][0].update(source_ids=[0, 1]),
            lambda v: v[1]['receipt']['selected'][0].update(source_offset=1),
            lambda v: v[1]['receipt']['selected'][0].update(source_sha256='0'*64),
            lambda v: v[1].update(query_sha256='0'*64),
            lambda v: v[1]['receipt']['accounting']['directory'].update(submitted_gets=1),
            lambda v: v[1]['receipt'].update(covered_ids=[0]),
            lambda v: v[1]['receipt']['selected'][0].update(prototype_bits=[1, 2]),
            lambda v: v[-1].update(complete=False)):
            events = copy.deepcopy(fixture['events']); mutate(events)
            fixture['path'].write_bytes(b''.join(local.canonical(e) for e in events)); rejects(validate)
        fixture['path'].write_bytes(b''.join(local.canonical(e) for e in fixture['events']))
        fixture['path'].write_bytes(fixture['path'].read_bytes().rsplit(b'\n', 2)[0]+b'\n'); rejects(validate)
        fixture['path'].write_bytes(b''.join(local.canonical(e) for e in fixture['events']))
        wrong = copy.deepcopy(fixture['evidence']); wrong['traces'][0]['primary_ids'] = [0]
        rejects(lambda: validate_nomination(fixture['path'], fixture['config'], fixture['pin'], wrong, fixture['native'], fixture['recovered'], 2))
        binary = copy_bytes(root/'binary', b'not executable native'); (root/'binary').chmod(0o700)
        stages = []
        with patch.object(subprocess, 'Popen', side_effect=AssertionError('spawn before admission')):
            rejects(lambda: native_stage('role-drift', ['/wrong-role'], binary, fixture['pin'], root, {}, 30, time.monotonic()+30, stages, lambda: None))
            fixture['pin']['sha256'] = '0'*64
            rejects(lambda: native_stage('config-drift', [binary['path']], binary, fixture['pin'], root, {}, 30, time.monotonic()+30, stages, lambda: None))
        exact(stages, [], 'drift rejects before spawn')
        rejects(lambda: qualify_role('nomination', dict(schema='borsuk-global-leaf-binary-authority-v1', role='original', authority_pending=False, native={}, refs={}), root))
        rejects(lambda: qualify_role('nomination', dict(schema='borsuk-global-leaf-binary-authority-v1', role='nomination', authority_pending=True, native={}, refs={}), root))
        rejects(lambda: seal_pair(root, {}, {}, {'relaion': {}}))
        # Whole-file post-seal tampering is caught by the same descriptor checker
        # used before replay; partial markers/terminals cannot authenticate.
        pin = local.identity(fixture['path']); fixture['path'].write_bytes(fixture['path'].read_bytes()+b' ')
        rejects(lambda: local.authenticate(pin, 8 << 20))
    print('PASS tiny shuffled-source-ID/prototype/span/hash/query/control-parity/zero-read/full131/pending-role/config-drift/incomplete-pair/post-seal-tamper checks; no native/corpus/GT')


def self_check_qualification(repo):
    """Actual small closed authorities, no archive/binary/corpus body reads."""
    repo = Path(repo).resolve()
    old = local.read_json(local.identity(repo/OLD/'screen/config.json'), 256 << 10)
    original = dict(schema='borsuk-global-leaf-binary-authority-v1', role='original', authority_pending=False,
        native=old['native'], refs={n: old['refs'][n] for n in REF_NAMES})
    nominee = local.read_json(local.identity(repo/BASE.parent/'implementation-gates/minimal-archive/qualified-nomination-authority.json'))
    original_id = qualify_role('original', original, repo); nomination_id = qualify_role('nomination', nominee, repo)
    require(original_id['source_identity_sha256'] != nomination_id['source_identity_sha256'], 'two independent role identities')
    for role, authority in (('original', original), ('nomination', nominee)):
        for mutate in (lambda v: v['native']['sources']['gates'].update(sha256='0'*64),
                       lambda v: v['native']['source_archive'].update(sha256='0'*64),
                       lambda v: v.update(authority_pending=True)):
            damaged = copy.deepcopy(authority); mutate(damaged)
            try:
                qualify_role(role, damaged, repo)
            except (ValueError, OSError):
                pass
            else:
                raise AssertionError('actual role metadata drift accepted')
    print('PASS actual closed original9lib/2bin and nomination11lib/6bin, release/Clippy/real-test-build; gate support separate from401; no cold bodies')


def pipeline_self_check():
    """Mock native boundary only; real reconstruction/seal/cleanup control flow."""
    from contextlib import ExitStack
    from scripts import launch_hierarchical_cells_100k_spot as launcher
    with tempfile.TemporaryDirectory(prefix='global-leaf-pipeline-') as tmp:
        root = Path(tmp); fixtures = {d: synthetic_dataset(root/d) for d in DATASETS}
        original_root = root/'original'; cold = b'fake-native-source-only'
        def pin(path, key):
            return dict(path=str(path), key=key, bytes=len(cold), sha256=local.sha(cold))
        proof = dict(source_archive=pin(original_root/'screen/scratch/native-source_archive', 'archive-original'),
            gate_log=pin(original_root/'screen/scratch/native-gate_log', 'log-original'),
            binaries={n: pin(original_root/'screen/scratch'/('native-'+n), 'original-'+n) for n in ('cells', 'writer')})
        proof = dict(proof, **{n: ({k: v for k, v in p.items() if k != 'key'} if n != 'binaries' else
                                 {r: {k: v for k, v in q.items() if k != 'key'} for r, q in p.items()}) for n, p in proof.items()})
        sources = {n: dict(path=path, bytes=1, sha256='a'*64) for n, path in local.SOURCE_FILES.items()}
        sources['module']['sha256'], sources['binary']['sha256'] = 'c'*64, 'd'*64
        original = dict(proof, sources=sources)
        for n in ('source_archive', 'gate_log'):
            original[n] = pin(proof[n]['path'], 'original-'+n)
        original['binaries'] = {n: pin(p['path'], 'original-'+n) for n, p in proof['binaries'].items()}
        nominee = dict(source_archive=pin('new-archive', 'new-archive'), gate_log=pin('new-log', 'new-log'),
            sources=sources, binaries=dict(cells=pin('new-cells', 'new-cells')))
        items = {}
        for d, f in fixtures.items():
            build_bytes = local.canonical(f['evidence']['build']); writer_bytes = local.canonical(dict(schema='synthetic-writer'))
            item = dict(f['evidence'], build_bytes=build_bytes, writer_bytes=writer_bytes, writer=local.decode(writer_bytes),
                recovery=dict(original_build_config=dict(bytes=len(build_bytes), sha256=local.sha(build_bytes)),
                    original_writer_config=dict(bytes=len(writer_bytes), sha256=local.sha(writer_bytes)),
                    output_cell_path=str(original_root/'screen/measurement'/(d+'-cells')), build_inputs={},
                    expected_cell_root_sha256=f['config']['candidate_root']['sha256']),
                diagnostic=dict(first=0, candidate_root=dict(f['config']['candidate_root'],
                    path=str(original_root/'screen/measurement'/(d+'-cells')/'manifest.json'))))
            items[d] = item
        evidence = dict(proof=proof, items=items, sources=dict(items=[dict(name=d) for d in DATASETS]))
        config = dict(resources=copy.deepcopy(CAPS), phase_seconds=dict(writer=30, build=30, nominate=30), wall_seconds=1800,
                      roles=dict(original=dict(native=original), nomination=dict(native=nominee)))
        role_ids = {r: dict(source_sha256={}, source_archive_support_sha256={}) for r in ROLE_NAMES}
        calls, fail_second, wrong_root = [], [True], [False]
        real_layout, real_validate = layout, validate_nomination
        def simulated_stage(name, command, binary, cfg, out, resources, phase, deadline, stages, check):
            calls.append(name); stages.append(dict(name=name, closed=True, unit_drained=True, resource_gate_passed=True))
            if name.endswith('-build'):
                d = name.split('-')[0]; target = Path(items[d]['recovery']['output_cell_path']); target.mkdir()
                for n in ('manifest.json', 'directories.bin', 'cells.bin'):
                    shutil.copyfile(root/d/n, target/n)
                if wrong_root[0] and d == 'cohere':
                    with (target/'manifest.json').open('ab') as stream:
                        stream.write(b' ')
            if name.endswith('-nominate'):
                d = name.split('-')[0]
                if d == 'cohere' and fail_second[0]:
                    raise ValueError('synthetic dataset2 native failure')
                events = copy.deepcopy(fixtures[d]['events'])
                events[0]['config_sha256'] = events[-2]['config_sha256'] = cfg['sha256']
                prefix = b''.join(local.canonical(e) for e in events[:129])
                events[-2].update(prefix_sha256=local.sha(prefix), prefix_bytes=len(prefix))
                copy_bytes(command[-1], b''.join(local.canonical(e) for e in events))
        def no_truth(path):
            require('truth' not in str(path).lower(), 'truth-body spy')
            return original_open(path)
        original_open = positive.open_input
        with ExitStack() as stack:
            stack.enter_context(patch.object(sys.modules[__name__], 'ORIGINAL_ROOT', original_root))
            stack.enter_context(patch.object(sys.modules[__name__], 'qualify_role', side_effect=lambda r, *_: role_ids[r]))
            stack.enter_context(patch.object(sys.modules[__name__], 'original_evidence', return_value=evidence))
            stack.enter_context(patch.object(sys.modules[__name__], 'archive_sources'))
            stack.enter_context(patch.object(sys.modules[__name__], 'restore_panel'))
            stack.enter_context(patch.object(sys.modules[__name__], 'native_stage', side_effect=simulated_stage))
            stack.enter_context(patch.object(sys.modules[__name__], 'layout', side_effect=lambda p, b: real_layout(p, b, 2, 48)))
            stack.enter_context(patch.object(sys.modules[__name__], 'validate_nomination', side_effect=lambda *a: real_validate(*a, dimensions=2)))
            stack.enter_context(patch.object(positive, 'open_input', side_effect=no_truth))
            for module, n in ((launcher, 'panel_inputs'), (launcher, 'admission'), (launcher, 'reduce_diagnostic'),
                              (local, 'prepare'), (local, 'validate_diagnostic'), (publication, 'sdk_client')):
                stack.enter_context(patch.object(module, n, side_effect=AssertionError('old panel/diagnose/GT/SDK spy: '+n)))
            def run(target):
                target.mkdir(); (target/'scratch').mkdir()
                cfg = local.write_json(target/'config.json', config)
                return execute(config, cfg, root, target, lambda p, path: copy_bytes(path, cold), lambda: None, time.monotonic()+90)
            out = root/'failed-pair'
            try:
                run(out)
            except ValueError as error:
                require('dataset2' in str(error), 'expected dataset2 failure')
            else:
                raise AssertionError('dataset2 failure accepted')
            receipt = local.read_json(local.identity(out/'native-execution-receipt.json'))
            exact(receipt['complete'], False, 'failed pair not complete')
            exact(receipt['cleanup']['native_units_drained'], True, 'failed pair units drained')
            require(not original_root.exists() and not (out/'paired-seal.json').exists(), 'failed pair cleanup/no seal')
            require(all((out/'retained'/d/'cells.bin').exists() for d in DATASETS), 'both recovered layouts retained after dataset2 failure')
            fail_second[0], wrong_root[0] = False, True; calls.clear()
            try:
                run(root/'wrong-root')
            except ValueError:
                pass
            else:
                raise AssertionError('second root drift accepted')
            require(not any(n.endswith('-nominate') for n in calls), 'both exact roots before first query')
            require(not original_root.exists(), 'root mismatch cleanup')
            wrong_root[0] = False; calls.clear(); out = root/'complete-pair'; run(out)
            seal = verify_pair(out, repo=root)
            exact(seal['complete'], True, 'full pair replay')
            require(not original_root.exists() and all((out/'retained'/d/'cells.bin').exists() for d in DATASETS), 'retained before cleanup')
            pin = seal['datasets']['cohere']['nomination']; path = out/pin['path']; path.write_bytes(path.read_bytes()+b' ')
            try:
                verify_pair(out, repo=root)
            except ValueError:
                pass
            else:
                raise AssertionError('paired whole-file tamper accepted')
    print('PASS real Python pipeline with mocked native boundary: dataset2 failure drains; both exact roots before queries; GT/old-panel/diagnose spies; both retained layouts/full seal replay and tamper')


def owned_failure_self_check():
    """Failure cleanup targets only the serial unit actually started, then waits."""
    from types import SimpleNamespace
    with tempfile.TemporaryDirectory(prefix='global-leaf-owned-failure-') as tmp:
        out = Path(tmp); binary = copy_bytes(out/'binary', b'fake source-only'); Path(binary['path']).chmod(0o700)
        config = local.write_json(out/'config.json', dict(schema='synthetic'))
        class Process:
            pid = 9999999
            waited = False
            def poll(self):
                return None
            def wait(self, timeout=None):
                self.waited = True; return 2
        process, actions, records = Process(), [], []
        def systemctl(command, **_):
            actions.append(command)
            return SimpleNamespace(returncode=0, stdout='MainPID=0\nActiveState=inactive\nSubState=dead\nControlGroup=\nLoadState=loaded\n')
        with patch.dict(os.environ, BORSUK_GLOBAL_LEAF_SLICE='borsuk-global-leaf-synthetic.slice'), \
             patch.object(subprocess, 'Popen', return_value=process), patch.object(subprocess, 'run', side_effect=systemctl):
            checks = [0]
            def failure():
                checks[0] += 1
                if checks[0] > 1:
                    raise TimeoutError('synthetic deadline failure')
            try:
                native_stage('synthetic-owned', [binary['path']], binary, config, out,
                    dict(memory_max_bytes=512 << 20, cpu_affinity=[0], max_log_bytes=16 << 20),
                    30, time.monotonic()+30, records, failure)
            except TimeoutError:
                pass
            else:
                raise AssertionError('unit deadline failure accepted')
        require(process.waited and records[0]['closed'] and records[0]['unit_drained'], 'SAME unit terminate and wait')
        unit = records[0]['unit']
        exact([c[-1] for c in actions], [unit, unit, '--property=LoadState'], 'owned unit cleanup call roster')
        require(all(unit in c for c in actions), 'never terminate another unit')
    print('PASS mocked native unit deadline/failure targets SAME unit, kill/stop/wait and verified drain; no process spawned')


def worker_closure_self_check():
    with tempfile.TemporaryDirectory(prefix='global-leaf-host-close-') as tmp:
        out = Path(tmp)
        events = 'oom 0\noom_kill 0\noom_group_kill 0'
        host = {'path':'/synthetic-host', 'memory.max':str(2 << 30), 'memory.peak':'1000',
                'memory.swap.max':'0', 'memory.swap.peak':'0', 'memory.events':events,
                'cpu.max':'200000 100000', 'pids.max':'512', 'cgroup.procs':'1'}
        main = dict(host, path='/synthetic-host/main', cpu_affinity=[0,1], cpu_max='200000 100000', tasks_max='512')
        data = dict(cgroup=dict(before=main, after=copy.deepcopy(main), host_before=host, host_after=copy.deepcopy(host), closed=True),
            resources=dict(scratch_bytes=1000, wall_seconds=1.0, deadline_seconds=10.0, monitor_errors=[]),
            cleanup=dict(scratch_removed=True, monitor_stopped=True, sdk_client_closed=True, original_root_removed=True,
                retained_layouts_preserved=True, native_processes_concurrent_max=1))
        def write(values):
            pins = {}
            for n, v in values.items():
                path = out/('worker-cgroup.json' if n == 'cgroup' else n+'.json')
                if path.exists():
                    path.unlink()
                pins[n] = local.write_json(path, v)
            return pins
        verify_worker_closure(out, write(data), dict(wall_seconds=1800))
        for mutate in (
            lambda v: v['cgroup']['host_after'].update({'memory.peak':str((2 << 30)+1)}),
            lambda v: v['cgroup']['host_after'].update({'memory.swap.peak':'1'}),
            lambda v: v['cgroup']['host_after'].update({'memory.events':'oom 1\noom_kill 0\noom_group_kill 0'}),
            lambda v: v['resources'].update(scratch_bytes=(16 << 30)+1),
            lambda v: v['resources'].update(wall_seconds=1801),
            lambda v: v['resources'].update(monitor_errors=['monitor failure']),
            lambda v: v['cleanup'].update(original_root_removed=False),
            lambda v: v['cleanup'].update(retained_layouts_preserved=False)):
            bad = copy.deepcopy(data); mutate(bad)
            try:
                verify_worker_closure(out, write(bad), dict(wall_seconds=1800))
            except ValueError:
                pass
            else:
                raise AssertionError('host resource/cleanup violation accepted')
    print('PASS final receipt independently rejects host memory/swap/OOM/scratch/deadline/monitor/cleanup/retention violations')


if __name__ == '__main__':
    try:
        args = sys.argv[1:]
        if args == ['--self-check']:
            self_check(); pipeline_self_check(); owned_failure_self_check(); worker_closure_self_check()
        elif len(args) == 2 and args[0] == '--self-check-qualification':
            self_check_qualification(args[1])
        elif len(args) == 4 and args[0] == '--owned-stage':
            pin = local.identity(args[1]); exact(pin['sha256'], args[2], 'owned stage config binding')
            print(json.dumps(owned_stage(pin, args[3])))
        elif len(args) == 2 and args[0] == '--verify':
            out = Path(args[1]).resolve(); config = local.read_json(local.identity(out/'config.json'), 512 << 10)
            evidence = original_evidence(Path(__file__).resolve().parents[1], config)
            seal = verify_pair(out, config=config, config_pin=local.identity(out/'config.json'), evidence=evidence)
            print(json.dumps(verify_execution(out, config, seal, evidence)))
        elif len(args) == 3:
            from scripts import launch_hierarchical_cells_100k_spot as launcher
            config, pin = local.load_config(args[0], args[1])
            exact(Path(args[0]).resolve(), Path(__file__).resolve().parents[1]/launcher.PROBE_CONFIG, 'frozen probe config path')
            # The launcher performs transport, aggregate resource supervision and
            # closed cleanup. This entry still uses exactly that staging path.
            print(json.dumps(launcher.probe_stage(Path(__file__).resolve().parents[1], args[2],
                                                 Path(__file__).resolve().parents[2])))
        else:
            raise ValueError(__doc__)
    except Exception as error:
        print('INVALID: '+str(error), file=sys.stderr); sys.exit(2)
