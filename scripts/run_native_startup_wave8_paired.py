#!/usr/bin/env python3
"""Bounded startup wave4/wave8 ABBA; no builds, publication, or cloud launch.

CLI: CONFIG SHA REPO NEW_OUTPUT; --replay CONFIG SHA REPO OUTPUT; --self-check.
Root freezes paired-config.json with FIXED, exact CODE hashes, original closed
cold a0005 authority, prices, and control/candidate role bindings. Each binding
contains wave_objects, native_source_commit, source_manifest, proof, binary.
Control reuses its unchanged scoped proof and binary; candidate proof uses schema
borsuk-startup-wave8-role-proof-v1 and actual combined workspace evidence.
main(..., on_cell_closed=None) emits marker/paths after drain, scratch removal,
and fsync; paths contains records, summary, seal. The controller authenticates
AWS terminal/source/archive closure and all OUTPUTS before worker replay.
Synthetic checks establish no native qualification, measured speedup, or 444 pass.
"""
import base64
from datetime import datetime
import io
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import sys
import threading
import time
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_native_semantic_1m_cold as native
from scripts import run_native_semantic_1m_offered as previous
from scripts import run_native_cold_offered as offered
from scripts import check_native_workspace_execution as workspace

cold, panel, ids, stats = previous.cold, previous.panel, previous.ids, previous.stats
encoded, sha, artifact, write, read, identity = (previous.encoded, previous.sha,
    previous.artifact, previous.write, previous.read, previous.identity)


ROOT = native.ROOT.parent / 'startup-wave8'
CONFIG = ROOT / 'paired-config.json'
MANIFEST = ROOT / 'candidate-native-source-manifest.json'
PREFIX = 'research/semantic-router/20261002/fresh1m-startup-wave8-paired-'
CELLS = ('control', 'candidate', 'candidate', 'control')
WIDTHS = dict(control=4, candidate=8)
SOURCE_COMMITS = dict(control='f4d76fc040aa89c44b3526e37f148e78d21241fa',
    candidate='660fd425a8a45dda17cbf6f44915bd36615ce87a')
SOURCE_IDS = dict(control='295a79de9a499cc388db14b4b78ac9fcd4f1eb8673ceb5c1222dfc7f119e9ae4',
    candidate='58535ffeb5bb74a09fba9da489d64b6c9b21495c6be53f320a3f8efc2d47756b')
CONTROL_DELTA = {
    'crates/borsuk/src/object_native_generation.rs': '06e37eff99905c6073fba411e451bcfceeb7d20e2e3d9b50ea8d604deb27fb56',
    'crates/borsuk/src/two_bit_generation.rs': '6b37d3c3b3c570719f2f9879b3c5889625d470f52f9b5581afb762e46d30afe9'}
GATE_COMMANDS = (
    ['cargo','test','--locked','-p','borsuk','--lib','object_native_generation::','--','--test-threads=1'],
    *(['cargo','test','--locked','-p','borsuk','--lib', 'two_bit_generation::source_walk_tests::'+name,
        '--','--exact','--test-threads=1'] for name in ('semantic_object_store_parity',
        'paged_source_matches_reference_and_preserves_failure_charges',
        'fragmented_paged_source_preserves_trace_and_rank_across_get_caps')),
    ['cargo','build','--release','--locked','-p','borsuk','--example','two_bit_http'],
    ['cargo','clippy','--locked','--workspace','--all-targets','--','-D','clippy::correctness','-D','clippy::suspicious'],
    ['env','-u','BORSUK_TEST_BUILD_COMMAND','bash','scripts/check_rust_test_build.sh'])
GATE_NAMES = ('object-native-generation-tests','semantic-object-store-parity','paged-source-parity',
    'fragmented-paged-source-parity','release','clippy','test-build')
MEMORY, NATIVE, THREAD_ENV = previous.MEMORY, previous.NATIVE, previous.THREAD_ENV
FIXED = dict(previous.FIXED, schema='borsuk-startup-wave8-paired-v1', cold_invocations=256,
    offered_qps=8, cells=list(CELLS), metadata_wave_objects=WIDTHS)
CODE = tuple(sorted(set((*previous.CODE, 'scripts/check_native_workspace_execution.py', 'scripts/run_native_startup_wave8_paired.py'))))
COLD_DIRECTORY = str(native.ROOT / 'a0005')
AUTHORITY_FIELDS = ('cold_config','cold_run','cold_source_authority','cold_fail_disposition','prices','roles')
CELL_FILES = tuple(f'cell{i}-{suffix}' for i in range(4) for suffix in ('records.jsonl','summary.json','seal.json'))
OUTPUTS = ('source-qualification.json','config.json','tool-versions.json','input-hashes.json',
    'records.jsonl','failures.jsonl','summary.json','resources.json','paired-cgroup.json','cleanup.json', *CELL_FILES)
validate_cgroup = previous.validate_cgroup
transport_failure = previous.transport_failure


def validate_role(role, binding, manifest, proof, candidate_sources):
    assert role in WIDTHS and set(binding) == {'wave_objects','native_source_commit','source_manifest','proof','binary'}
    assert type(binding['wave_objects']) is int and binding['wave_objects'] == WIDTHS[role], 'role wave width'
    assert binding['native_source_commit'] == SOURCE_COMMITS[role], 'role source commit'
    expected_sources = dict(candidate_sources)
    if role == 'control': expected_sources.update(CONTROL_DELTA)
    assert type(manifest['source_file_count']) is type(proof['source_file_count']) is int
    assert len(expected_sources) == manifest['source_file_count'] == 399
    assert manifest['native_source_commit'] == SOURCE_COMMITS[role]
    assert manifest['source_sha256'] == expected_sources, 'exact role native source inventory'
    assert manifest['source_identity_sha256'] == sha(encoded(expected_sources)) == SOURCE_IDS[role]
    assert proof['qualified'] is True and proof['authority_pending'] is False
    assert proof['source_file_count'] == 399 and proof['native_source_sha256'] == expected_sources
    assert proof['source_identity_sha256'] == SOURCE_IDS[role]
    assert proof['current_whole_tree_full_execution'] is False, 'scoped gates are not whole workspace execution'
    assert type(proof['binary_bytes']) is int and proof['binary_bytes'] > 0
    assert dict(bytes=proof['binary_bytes'],sha256=proof['binary_sha256']) == identity(binding['binary'])
    for name in ('release_status','clippy_status','workspace_test_compilation_status','oom_kills','swap_peak_bytes'):
        assert type(proof[name]) is int and proof[name] == 0, 'completed source-qualified gate: '+name
    if role == 'control':
        assert proof['schema'] == 'borsuk-semantic-1m-scoped-native-proof-v1'
        assert proof['production_library_unchanged'] is True
    else:
        assert proof['schema'] == 'borsuk-startup-wave8-role-proof-v1'
        assert proof['role'] == role and type(proof['wave_objects']) is int and proof['wave_objects'] == 8
        assert proof['native_source_commit'] == SOURCE_COMMITS[role]
        assert proof['production_library_unchanged'] is False
        assert set(proof['evidence']) == {'workspace_receipt','test_log','workspace_cgroup','source_before','source_after','source_qualification'}
    return proof


def validate_candidate(repo, binding, proof):
    evidence = proof['evidence']
    bodies = {n:read(repo,p) for n,p in evidence.items()}
    receipt = json.loads(bodies['workspace_receipt'])
    assert receipt['schema'] == 'borsuk-startup-wave8-implementation-gates-receipt-v1'
    assert receipt['qualified'] is receipt['command_started'] is receipt['command_completed'] is receipt['source_unchanged'] is True
    assert receipt['execution_kind'] == 'implementation-gates' and receipt['actual_full_workspace_execution'] is False
    assert receipt['command'] == ['bash','scripts/check_startup_wave8_implementation.sh']
    assert type(receipt['exit_status']) is type(receipt['gate_status']) is int and receipt['exit_status'] == receipt['gate_status'] == 0
    assert type(receipt['source_file_count']) is int and receipt['source_file_count'] == 399
    assert receipt['source_identity_sha256'] == SOURCE_IDS['candidate']
    assert receipt['source_sha256'] == proof['native_source_sha256']
    for name in ('source_before','source_after'):
        assert json.loads(bodies[name]) == receipt['source_sha256'], 'candidate source before/after'
    qualification = json.loads(bodies['source_qualification'])
    assert qualification['schema'] == 'borsuk-startup-wave8-implementation-gates-qualification-v1'
    assert qualification['source_sha256'] == receipt['source_sha256']
    assert qualification['native_source_commit'] == SOURCE_COMMITS['candidate']
    assert qualification['actual_full_workspace_execution'] is False
    assert qualification['command'] == receipt['command'] and qualification['environment'] == receipt['environment']
    assert receipt['qualification_sha256'] == identity(evidence['source_qualification'])['sha256']
    for name in ('config_sha256','code_identity_sha256','campaign_schema','artifact_roster_sha256','controller_source_commit','candidate_delta_paths','source_identity_sha256','source_file_count'):
        assert receipt[name] == qualification[name], 'candidate receipt/source authority: '+name
    assert qualification['candidate_delta_paths'] == list(CONTROL_DELTA)
    assert panel.re.fullmatch('[0-9a-f]{40}',qualification['controller_source_commit'])
    for name,path in (('test_log','test.log'),('workspace_cgroup','workspace-cgroup.json'),
        ('source_before','source-before.json'),('source_after','source-after.json'),('source_qualification','source-qualification.json')):
        assert receipt['artifacts'][path] == identity(evidence[name]), 'candidate receipt body: '+path
    assert receipt['artifacts']['binaries/two_bit_http'] == identity(binding['binary'])
    assert receipt['artifacts']['native-source-manifest.json'] == identity(binding['source_manifest'])
    assert qualification['native_source_manifest_sha256'] == identity(binding['source_manifest'])['sha256']
    workspace.validate_cgroup(json.loads(bodies['workspace_cgroup']))
    stages = [json.loads(line) for line in bodies['test_log'].decode().splitlines() if 'borsuk-startup-wave8-implementation-stage-v1' in line]
    assert len(stages) == 14, 'seven stage start/end pairs'
    last = ''
    for index,(name,command) in enumerate(zip(GATE_NAMES,GATE_COMMANDS)):
        start,end = stages[index*2:index*2+2]
        assert start['schema'] == end['schema'] == 'borsuk-startup-wave8-implementation-stage-v1'
        assert start['stage'] == end['stage'] == name and start['command'] == end['command'] == command
        assert start['started_at'] == end['started_at'] and start['finished_at'] is start['exit_status'] is None
        assert type(end['exit_status']) is int and end['exit_status'] == 0
        assert all(type(t) is str and panel.re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',t) for t in (end['started_at'],end['finished_at']))
        for stamp in (end['started_at'],end['finished_at']): datetime.strptime(stamp,'%Y-%m-%dT%H:%M:%SZ')
        assert last <= end['started_at'] <= end['finished_at']; last = end['finished_at']
    return receipt


def qualify(config_path, expected_sha, repo):
    repo, config_path = Path(repo).resolve(), Path(config_path).absolute()
    assert config_path == config_path.resolve() == repo/CONFIG and not config_path.is_symlink()
    assert config_path.stat().st_size <= 65536
    body = config_path.read_bytes(); assert sha(body) == expected_sha, 'config identity'
    config = json.loads(body)
    assert config['authority_pending'] is False, 'root freeze pending'
    assert set(config) == set(FIXED) | {'authority_pending','code_sha256','measurement_prefix',*AUTHORITY_FIELDS}
    assert all(type(config[n]) is type(v) and config[n] == v for n,v in FIXED.items()), 'fixed paired protocol'
    assert set(config['code_sha256']) == set(CODE), 'exact transitive code closure'
    assert all(artifact(panel.repo_path(repo,n))['sha256'] == d for n,d in config['code_sha256'].items()), 'code identity'
    assert panel.re.fullmatch(panel.re.escape(PREFIX)+r'a[0-9]{4}',config['measurement_prefix'])
    run = config['cold_run']
    assert set(run) == {'directory','files'} and run['directory'] == COLD_DIRECTORY, 'retained a0005 only'
    assert set(run['files']) == set(previous.COLD_ROSTER)
    pointer = config['cold_config']; assert pointer == run['files']['screen/config.json']
    for name,p in run['files'].items():
        assert p['path'] == str(Path(run['directory'])/name)
        if p['bytes'] == 0:
            assert name in ('screen/publication.log','screen/failures.jsonl')
            assert artifact(panel.repo_path(repo,p['path'])) == dict(bytes=0,sha256=p['sha256'])
        else: read(repo,p)
    assert json.loads(read(repo,run['files']['aws-closeout.json']))['state'] == 'terminated'
    assert type(config['cold_source_authority']) is dict
    checked = previous.cold_spot.qualify_measurement(repo/run['directory'],repo,config['cold_source_authority'])
    assert all(checked[n] is True for n in ('measurement_gate_passed','quality_gate','identity_gate','resource_gate','cleanup_gate'))
    assert checked['valid_calls'] == checked['actual_http_attempts'] == 64
    previous.cold_spot.fail_disposition(repo,run,config['cold_fail_disposition'],checked)
    assert json.loads(read(repo,pointer)) == checked['cold_config']
    assert json.loads(read(repo,config['prices'])), 'root frozen price provenance'
    pinned = json.loads((repo/MANIFEST).read_bytes())
    assert pinned['native_source_commit'] == SOURCE_COMMITS['candidate'] and pinned['source_identity_sha256'] == SOURCE_IDS['candidate']
    assert sha(encoded(pinned['source_sha256'])) == SOURCE_IDS['candidate']
    assert set(config['roles']) == set(WIDTHS)
    qualified_roles = {}
    for role,binding in config['roles'].items():
        manifest = json.loads(read(repo,binding['source_manifest']))
        proof = json.loads(read(repo,binding['proof']))
        validate_role(role,binding,manifest,proof,pinned['source_sha256'])
        read(repo,binding['binary'])
        if role == 'control':
            assert binding['proof'] == checked['cold_config']['native_proofs']['http'], 'unchanged archived control proof'
            assert binding['binary'] == checked['cold_config']['binaries']['http'], 'qualified archived control binary'
            native.native_proof(repo,binding['proof'],binding['binary'],checked['qualification'])
            for pointer in proof['evidence'].values(): read(repo,pointer)
        else: validate_candidate(repo,binding,proof)
        qualified_roles[role] = dict(wave_objects=WIDTHS[role],native_source_commit=SOURCE_COMMITS[role],
            source_identity_sha256=SOURCE_IDS[role],binary=identity(binding['binary']),proof=identity(binding['proof']),
            source_manifest=identity(binding['source_manifest']),current_whole_tree_full_execution=False)
    assert qualified_roles['control']['binary'] != qualified_roles['candidate']['binary'], 'distinct role binaries'
    proof = dict(config_path=str(CONFIG),config_sha256=expected_sha,
        code_identity_sha256=sha(encoded(config['code_sha256'])),
        refs_identity_sha256=sha(encoded({n:config[n] for n in AUTHORITY_FIELDS})),
        roles=qualified_roles,measurement_prefix=config['measurement_prefix'],prices=identity(config['prices']),
        cold_terminal_sha256=run['files']['aws-terminal.json']['sha256'],
        native_rebuilt=False,publication_invocations=0,current_whole_tree_full_execution=False)
    return config,proof


def inputs(repo, config):
    evidence = previous.inputs(repo,config)
    evidence['roles'] = config['roles']
    evidence['identities'].pop('binary'); evidence['identities'].pop('native_proof')
    evidence['identities']['roles'] = {role:{n:identity(binding[n]) for n in ('binary','proof','source_manifest')}
        for role,binding in config['roles'].items()}
    return evidence

def measured_call(binary, config, evidence, q, port, *, wave_objects):
    """cold.stop is installed once by the campaign, before any worker starts."""
    assert type(wave_objects) is int and wave_objects in (4,8)
    arm, body, expected, truth = (evidence['arm'], evidence['requests'][q],
        evidence['references'][q], evidence['truth'][q])
    failures, observed = io.StringIO(), dict(native_process_started=False, namespace_start_attempted=False, http_attempts=0)
    stage = 'spawn'
    def spawn(args, **kwargs):
        nonlocal stage
        observed['namespace_start_attempted'] = True
        command = list(args); index = command.index('taskset')
        command[index:index] = ['prlimit', '--as=4294967296:4294967296']
        process = subprocess.Popen(command, **kwargs)
        observed.update(native_process_started=True, wrapper_pid=process.pid)
        stage = 'connect'
        return process
    def post(client, payload):
        nonlocal stage
        stage = 'cpu_before'
        before = native.native_cpu(observed['wrapper_pid'], binary)
        stage = 'transport'; observed['http_attempts'] = 1
        status, raw = cold.post(client, payload)
        wire = time.monotonic_ns()  # Before CPU attribution, JSON, GT or cleanup.
        observed.update(first_wire_completed_ns=wire, http_status=status,
                        raw_response_base64=base64.b64encode(raw).decode())
        stage = 'cpu_after'; after = native.native_cpu(observed['wrapper_pid'], binary)
        assert (before['pid'], before['start_ticks']) == (after['pid'], after['start_ticks'])
        ticks = sum(after[n]-before[n] for n in ('user_ticks','system_ticks'))
        assert ticks >= 0
        observed['query_cpu'] = dict(before=before, after=after, ticks=ticks,
            ticks_per_second=os.sysconf('SC_CLK_TCK'), scope='native process CPU sampled around one client POST; tick resolution')
        stage = 'response'
        return status, raw
    row = {}
    try:
        row = cold.cold_call(str(binary), config, dict(arm, dataset='ReLAION'), body, expected, truth, failures,
            port=port, response_check=lambda r,e,t,a:native.validate_query(r,arm,e,t),
            startup_check=lambda v,f,w:native.validate_startup(v,arm,w,wave_objects=wave_objects), post_call=post, spawn=spawn,
            env=dict(os.environ, BORSUK_NATIVE_MEMORY_BYTES=str(NATIVE), AWS_MAX_ATTEMPTS='1'))
        row.update(observed)
        row['completed_ns'] = observed['first_wire_completed_ns']
        for key, start in (('cold_start_to_first_http_response_ns','started_ns'),
            ('first_post_to_response_ns','connected_ns'), ('incoming_http_wall_ns','successful_connect_attempt_ns')):
            row[key] = row['completed_ns']-row[start]
        stage = 'accounting'; row['accounting'] = native.transport(row['native_header'], row['response'], arm, wave_objects=wave_objects)
        stage = 'resources'; row['resources'] = native.telemetry.resources(row['native_time_log'], NATIVE)
        stage = 'cleanup'
        assert row['native_close']['intentional_stop'] is True and row['native_close']['process_group_closed'] is True
        row.update(outcome='success', failure_kind=None, abort_admissions=False)
    except Exception as error:
        raw = failures.getvalue()
        failed = [json.loads(line) for line in raw.splitlines()]
        if not row and failed: row = failed[0]
        row.update(observed, failure_stream_raw=raw, failure_stage=stage,
                   error_type=type(error).__name__, error=str(error) or type(error).__name__)
        transport_error = transport_failure(row)
        row.update(outcome='failed', failure_kind='transport' if transport_error else 'fatal',
                   abort_admissions=not transport_error)
        # Failed payloads and any final counters survive; absent totals stay unknown.
        row['failed_final_transport'] = 'UNMEASURED'
        try:
            response = json.loads(base64.b64decode(row['raw_response_base64'], validate=True))
            row['failed_final_transport'] = stats.validate_transport(response['transport'], False)
        except (KeyError, ValueError, AssertionError, TypeError):
            pass
        if row['native_process_started']:
            try: row['resources'] = native.telemetry.resources(row['native_time_log'], NATIVE)
            except Exception as resource_error:
                row.update(failure_kind='fatal', abort_admissions=True, resource_error=str(resource_error))
    if 'first_wire_completed_ns' in observed:
        row['completed_ns'] = observed['first_wire_completed_ns']
        if row.get('started_ns') is not None:
            row['cold_start_to_first_http_response_ns'] = row['completed_ns']-row['started_ns']
    row['cleanup_confirmed'] = (not row['native_process_started'] or
        row.get('native_close', {}).get('process_group_closed') is True)
    if not row['cleanup_confirmed']: row.update(failure_kind='fatal', abort_admissions=True)
    row.update(query_ordinal=q, expected_authority=arm['authority'], request_sha256=sha(body),
               request_bytes=len(body), http_retry=False)
    return row


def validate_success(row, evidence, q, *, wave_objects):
    arm, response = evidence['arm'], row['response']
    assert row['http_status'] == 200 and row['http_attempts'] == row['valid_ann_requests'] == 1
    assert row['native_process_started'] is row['namespace_start_attempted'] is True
    assert row['response'] == json.loads(base64.b64decode(row['raw_response_base64'], validate=True))
    assert len(base64.b64decode(row['raw_response_base64'], validate=True)) == row['response_bytes']
    headers = [json.loads(line) for line in row['native_server_log'].splitlines() if line.startswith('{')]
    assert headers == [row['native_header']] and headers[0]['listen'] == f"127.0.0.1:{row['port']}"
    assert row['accounting'] == native.transport(headers[0], response, arm, wave_objects=wave_objects)
    assert row['returned_hits'] == native.validate_query(response, arm, evidence['references'][q], evidence['truth'][q])
    assert row['resources'] == native.telemetry.resources(row['native_time_log'], NATIVE)
    assert row['native_close']['intentional_stop'] is row['native_close']['process_group_closed'] is True
    assert type(row['native_close']['returncode']) is int
    cpu = row['query_cpu']; before, after = cpu['before'], cpu['after']
    assert (before['pid'], before['start_ticks']) == (after['pid'], after['start_ticks'])
    assert all(c['address_space_limit_bytes'] == 4*1024**3 and c['cpu_affinity'] == [0,1,2,3] for c in (before,after))
    assert cpu['scope'] == 'native process CPU sampled around one client POST; tick resolution'
    stats.integer(cpu['ticks_per_second'], 'tick resolution', 1)
    assert cpu['ticks'] == sum(after[n]-before[n] for n in ('user_ticks','system_ticks')) >= 0
    assert row['started_ns'] <= row['successful_connect_attempt_ns'] <= row['connected_ns'] <= row['completed_ns'] <= row['terminal_ns']
    assert row['completed_ns'] == row['first_wire_completed_ns']
    for key, start, end in (('cold_start_to_first_http_response_ns','started_ns','completed_ns'),
        ('before_successful_connect_attempt_ns','started_ns','successful_connect_attempt_ns'),
        ('successful_tcp_connect_ns','successful_connect_attempt_ns','connected_ns'),
        ('first_post_to_response_ns','connected_ns','completed_ns'),
        ('incoming_http_wall_ns','successful_connect_attempt_ns','completed_ns')):
        assert row[key] == row[end]-row[start]
    assert row['cold_start_to_first_http_response_ns'] >= headers[0]['remote_open_wall_ns']+headers[0]['head_read_wall_ns']


def reduce_cell(records, evidence, config):
    assert len(records) == 64 and [r['query_ordinal'] for r in records] == list(range(64))
    assert len(evidence['truth']) == 64 and all(len(t) == len(set(t)) == 100 and all(type(n) is int and 0 <= n < 1000000 for n in t) for t in evidence['truth']), 'sealed GT shape'
    receipt = records[0]['cell_receipt']
    epoch, terminal = receipt['epoch_ns'], receipt['terminal_ns']
    assert type(receipt['worker_started_ns']) is int and receipt['worker_started_ns'] <= epoch
    assert type(epoch) is type(terminal) is int and terminal > epoch
    assert receipt['admission_deadline_ns'] == receipt['worker_started_ns']+(3000-90)*10**9
    index = records[0]['cell_index']; stats.integer(index, 'cell', 0, 3)
    role = CELLS[index]; width = WIDTHS[role]
    rate = 8; active, intervals, delays, good = [], [], [], []
    fatal = False
    # This is deliberately after drain, never a per-call sibling PID check.
    validate_cgroup(dict(before=receipt['campaign_cgroup_before'], after=receipt['cgroup_after'], closed=True))
    validate_cgroup(dict(before=receipt['cgroup_before'], after=receipt['cgroup_after'], closed=True))
    assert receipt['resource_errors'] == [], 'shared resource observation failure'
    assert receipt['cell_scratch_removed'] is True, 'cell scratch cleanup'
    assert receipt['binary_after'] == (identity(config['roles'][role]['binary']) if receipt['cell_started'] else None), 'cell role binary drift'
    for q, row in enumerate(records):
        assert type(row['query_ordinal']) is int and type(row['cell_index']) is int
        assert row['cell_index'] == index and row['offered_qps'] == rate and row['dataset'] == 'ReLAION'
        assert row['role'] == role and type(row['wave_objects']) is int and row['wave_objects'] == width
        assert row['role_authority'] == role_authority(config, role), 'row role/source/binary authority'
        assert row['scheduled_ns'] == epoch+round(q*1e9/rate)
        assert epoch <= row['terminal_ns'] <= terminal
        outcome, port = row['outcome'], row['port']
        assert outcome in ('success','failed','capacity_drop','aborted')
        if row['dispatched_ns'] is not None:
            assert receipt['cell_started'] is True
            assert row['scheduled_ns'] <= row['dispatched_ns'] <= row['terminal_ns']
            assert row['dispatched_ns'] < receipt['admission_deadline_ns']
            delays.append(row['dispatched_ns']-row['scheduled_ns'])
        if port is None:
            assert outcome in ('capacity_drop','aborted') and row['http_attempts'] == row['valid_ann_requests'] == 0
            assert row['native_process_started'] is row['namespace_start_attempted'] is False
            assert row['started_ns'] is row['completed_ns'] is None
            assert not any(n in row for n in ('response','native_close','raw_response_base64','native_header'))
            if outcome == 'capacity_drop': assert row['terminal_ns'] == row['dispatched_ns']
            else:
                assert row['dispatched_ns'] is None and row['abort_after'] == receipt['abort_after']
                assert isinstance(row['abort_after'], dict)
            continue
        stats.integer(port, 'port', 18080, 18085)
        assert outcome in ('success','failed') and row['dispatched_ns'] is not None
        assert row['request_sha256'] == sha(evidence['requests'][q]) and row['request_bytes'] == len(evidence['requests'][q])
        assert row['expected_authority'] == evidence['arm']['authority'] and row['http_retry'] is False
        assert row['cleanup_confirmed'] is (not row['native_process_started'] or row.get('native_close',{}).get('process_group_closed') is True)
        assert row['cleanup_confirmed'] is True, 'unclosed native process group'
        if row['started_ns'] is not None: assert row['dispatched_ns'] <= row['started_ns'] <= row['terminal_ns']
        if row['completed_ns'] is not None:
            assert row['started_ns'] <= row['completed_ns'] <= row['terminal_ns']
            assert row['completed_ns'] == row['first_wire_completed_ns']
        if receipt['abort_after'] is not None: assert row['dispatched_ns'] <= receipt['abort_after']['observed_ns']
        intervals.append((row['dispatched_ns'], row['terminal_ns'], port))
        if outcome == 'success':
            assert row['failure_kind'] is None and row['abort_admissions'] is False
            validate_success(row, evidence, q, wave_objects=width); good.append(row)
        else:
            assert row['error_type'] and row['error'] and row['failure_kind'] in ('transport','fatal')
            expected_kind = 'transport' if transport_failure(row) and 'resource_error' not in row else 'fatal'
            assert row['failure_kind'] == expected_kind, 'failure classification'
            assert row['abort_admissions'] is (row['failure_kind'] == 'fatal')
            assert type(row['failure_stream_raw']) is str
            assert row['http_attempts'] in (0,1) and type(row['http_attempts']) is int
            if row['failure_stream_raw']:
                failures = [json.loads(line) for line in row['failure_stream_raw'].splitlines()]
                assert len(failures) == 1
                assert row['error_type'] == failures[0]['error_type']
                assert row['error'] == (failures[0]['error'] or failures[0]['error_type'])
                for key in ('started_ns','native_close','native_time_log','native_server_log'):
                    assert row[key] == failures[0][key]
            if row['failure_kind'] == 'transport' and row['failure_stage'] == 'response':
                assert row['http_attempts'] == 1 and row['http_status'] != 200
            if 'raw_response_base64' in row:
                raw = base64.b64decode(row['raw_response_base64'],validate=True)
                assert type(row['http_status']) is int and row['first_wire_completed_ns'] <= row['terminal_ns']
                if row['failed_final_transport'] != 'UNMEASURED':
                    assert row['failed_final_transport'] == stats.validate_transport(json.loads(raw)['transport'],False)
            if row['native_process_started']:
                assert row['resources'] == native.telemetry.resources(row['native_time_log'], NATIVE)
            fatal |= row['failure_kind'] == 'fatal'
    peak = 0
    for start, end, port in sorted(intervals):
        active = [(s,e,p) for s,e,p in active if e > start]
        assert all(p != port for _,_,p in active), 'early port reuse'
        active.append((start,end,port)); peak = max(peak,len(active))
        assert peak <= 6
    for row in records:
        if row['outcome'] == 'capacity_drop':
            assert sum(s <= row['dispatched_ns'] < e for s,e,_ in intervals) == 6, 'drop without six owners'
    abort = receipt['abort_after']
    if receipt['cell_started'] and abort is not None:
        assert epoch <= abort['observed_ns'] <= terminal
        if abort['reason'] == 'admission deadline': assert abort['observed_ns'] >= receipt['admission_deadline_ns']
        else:
            origin = records[abort['query_ordinal']]
            assert origin['abort_admissions'] is True and origin['terminal_ns'] == abort['observed_ns']
            fatal = True
    counts = dict(planned=64, dispatched=sum(r['dispatched_ns'] is not None for r in records),
        admitted=len(intervals), terminal_completed=len(intervals), successful=len(good))
    hits = sum(r['returned_hits'] for r in good)
    timing = all(d <= 125000000 for d in delays)
    complete = len(good) == 64
    attained = complete and hits >= 608 and timing
    span = terminal-epoch
    started = receipt['cell_started']; assert type(started) is bool
    tails = dict(cold=offered.tails([r['cold_start_to_first_http_response_ns'] for r in good]),
        scheduled_response=offered.tails([r['completed_ns']-r['scheduled_ns'] for r in good]),
        dispatch=offered.tails(delays), all_offers=native.telemetry.all_offer_tails(records))
    return dict(schema='borsuk-startup-wave8-paired-cell-v1', cell_index=index, role=role, wave_objects=width,
        role_authority=role_authority(config,role), offered_qps=rate,
        dataset='ReLAION', cell_started=started, epoch_ns=epoch, terminal_ns=terminal,
        full_span_ns=span if started else 'UNMEASURED', counts=counts,
        full_span_qps={n:v*1e9/span if started else 'UNMEASURED' for n,v in counts.items()},
        capacity_drops=sum(r['outcome']=='capacity_drop' for r in records),
        errors=sum(r['outcome']=='failed' for r in records), aborted=sum(r['outcome']=='aborted' for r in records),
        returned_hits=hits, all_offer_recall_at_10=hits/640, quality_gate_passed=complete and hits>=608,
        dispatch_timing_gate_passed=timing, attainment=attained, execution_gate_passed=not fatal, valid=attained and not fatal,
        published_context_gate_passed=attained and not fatal and tails['cold']['p90'] < 444,
        native_peak_rss_bytes=max((r['resources']['rss_peak_bytes'] for r in good),default=0),
        metadata_waves=[sorted({r['metadata_wave'] for r in row['native_header']['remote_open_stats']['metadata']}) for row in good],
        metadata_logical_get_requests=sum(r['accounting']['metadata']['logical_metadata_get_requests'] for r in good),
        metadata_verified_bytes=sum(r['accounting']['metadata']['metadata_bytes'] for r in good),
        peak_port_ownership=peak, closed=True, cleanup_confirmed=True,
        latency_ms=tails if started else 'UNMEASURED',
        latency_population='successful responses only; all-offer unsuccessful positions UNBOUNDED',
        successful_process_transport_totals={n:sum(r['accounting']['final_process_transport'][n] for r in good)
            for n in ('attempts','consumed_payload_bytes','transport_failures','stream_failures')},
        successful_process_method_counts=[sum(r['accounting']['final_process_transport']['method_counts'][i] for r in good) for i in range(10)],
        successful_process_status_counts={str(status):sum(dict(r['accounting']['final_process_transport']['status_counts']).get(status,0) for r in good)
            for status in sorted({s for r in good for s,_ in r['accounting']['final_process_transport']['status_counts']})},
        transport_scope='submitted process HttpService calls and consumed frames; startup plus query; includes IMDS below, not confirmed S3 requests',
        logical_query_totals={p+n:sum(r['response'][p+n] for r in good) for p in ('','source_','router_') for n in stats.COUNTERS},
        imds_token_PUTs=len(good), imds_credential_GETs=2*len(good),
        failed_transport_population='raw records; missing totals UNMEASURED',
        total_failed_call_transport='UNMEASURED' if any(r['outcome']=='failed' for r in records) else 0,
        wire_bytes='UNMEASURED', sustainable_qps='UNMEASURED', matched_vendor_comparison=False)


def role_authority(config, role):
    binding = config['roles'][role]
    return dict(role=role,wave_objects=WIDTHS[role],native_source_commit=SOURCE_COMMITS[role],
        source_identity_sha256=SOURCE_IDS[role],binary=identity(binding['binary']),
        proof=identity(binding['proof']),source_manifest=identity(binding['source_manifest']))


def reduce_records(records, evidence, config):
    assert len(records) == 256 and [(r['cell_index'],r['query_ordinal']) for r in records] == [(i,q) for i in range(4) for q in range(64)], 'exact ABBA ledger'
    cells = [reduce_cell(records[i:i+64],evidence,config) for i in range(0,256,64)]
    first = records[0]['cell_receipt']; stop,previous_end = None,0
    for index,cell in enumerate(cells):
        receipt = records[index*64]['cell_receipt']
        assert receipt['worker_started_ns'] == first['worker_started_ns']
        assert receipt['campaign_cgroup_before'] == first['campaign_cgroup_before']
        assert cell['epoch_ns'] >= previous_end; previous_end = cell['terminal_ns']
        assert receipt['cell_scratch_removed'] is True, 'cell scratch cleanup'
        if stop is None:
            assert cell['cell_started'] is True
            if not cell['valid']: stop = dict(cell_index=index,reason='cell validation failed',observed_ns=cell['terminal_ns'])
        else:
            assert cell['cell_started'] is False and receipt['abort_after'] == stop and cell['aborted'] == 64
    valid = all(c['valid'] for c in cells)
    comparison = 'UNMEASURED'
    if valid:
        comparison = {str(i):{str(j):{
            n:{p:cells[i]['latency_ms'][n][p]/cells[j]['latency_ms'][n][p] for p in ('p50','p90','p95','p99')}
            for n in ('cold','scheduled_response')} for j in (0,3)} for i in (1,2)}
    counts = {n:sum(c['counts'][n] for c in cells) for n in cells[0]['counts']}
    span = cells[-1]['terminal_ns']-cells[0]['epoch_ns']
    return dict(schema='borsuk-startup-wave8-paired-result-v1',closed=True,cells=cells,counts=counts,
        full_span_ns=span,full_span_qps={n:v*1e9/span for n,v in counts.items()},
        execution_gate_passed=all(c['execution_gate_passed'] for c in cells),
        paired_gate_passed=valid,candidate_over_actual_bracketing_control_latency_ratio=comparison,
        comparison_population='candidate cells 1 and 2 versus EACH actual control cell 0 and 3; successful responses only; all four cells must pass',
        historical_a0002_matched_control=False,sustainable_qps='UNMEASURED',matched_vendor_comparison=False,
        escalation_stop=stop)


def aborted_rows(index, epoch, stop, config):
    return [dict(query_ordinal=q,cell_index=index,role=CELLS[index],wave_objects=WIDTHS[CELLS[index]],
        role_authority=role_authority(config,CELLS[index]),dataset='ReLAION',offered_qps=8,
        scheduled_ns=epoch+q*125000000,dispatched_ns=None,started_ns=None,completed_ns=None,
        port=None,outcome='aborted',abort_after=stop,terminal_ns=epoch,
        namespace_start_attempted=False,native_process_started=False,http_attempts=0,valid_ann_requests=0) for q in range(64)]


def close_cell(output, records, result, proof, on_cell_closed=None):
    index = result['cell_index']; role = CELLS[index]
    assert result['closed'] is result['cleanup_confirmed'] is True
    assert records[0]['cell_receipt']['cell_scratch_removed'] is True
    paths = {n:output/f'cell{index}-{suffix}' for n,suffix in (
        ('records','records.jsonl'),('summary','summary.json'),('seal','seal.json'))}
    write(paths['records'],b''.join(encoded(r)+b'\n' for r in records))
    marker = dict(result,records=artifact(paths['records']),config_sha256=proof['config_sha256'],
        code_identity_sha256=proof['code_identity_sha256'],refs_identity_sha256=proof['refs_identity_sha256'],
        binary_sha256=proof['roles'][role]['binary']['sha256'])
    write(paths['summary'],marker)
    seal = dict(schema='borsuk-startup-wave8-cell-seal-v1',cell_index=index,role=role,
        records=artifact(paths['records']),summary=artifact(paths['summary']))
    write(paths['seal'],seal)
    descriptor = os.open(output,os.O_RDONLY|os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)
    if on_cell_closed is not None: on_cell_closed(dict(marker,summary=seal['summary'],seal=artifact(paths['seal'])),paths)
    return marker


def main(config_path, expected_sha, repo, output, *, on_cell_closed=None):
    started_ns = time.monotonic_ns()
    repo,output = Path(repo).resolve(),Path(output).absolute()
    config,proof = qualify(config_path,expected_sha,repo)
    evidence = inputs(repo,config)
    assert not output.exists() and not output.is_symlink() and not output.resolve().is_relative_to(repo)
    assert all(os.environ.get(n) == '2' for n in THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS') == '1'
    assert os.sched_getaffinity(0) == {4,5}
    baseline = native.capture(); validate_cgroup(dict(before=baseline,after=baseline,closed=True))
    source = {k:os.environ['BORSUK_OFFERED_'+n] for k,n in (('source_commit','SOURCE_COMMIT'),('source_archive_sha256','ARCHIVE_SHA256'))}
    assert panel.re.fullmatch('[0-9a-f]{40}',source['source_commit']) and panel.re.fullmatch('[0-9a-f]{64}',source['source_archive_sha256'])
    output.mkdir(parents=True); scratch = output/'scratch'; scratch.mkdir()
    records,cells,resource_errors = [],[],[]
    sample_report = dict(samples=0,memory_current_peak_bytes=0,anonymous_peak_bytes=0,page_cache_peak_bytes=0)
    failure,stop_after = None,None
    counters = dict(before=baseline,closed=False)
    stop,termination = threading.Event(),threading.Event()
    def sample():
        while not stop.is_set():
            try:
                snap = native.capture(); validate_cgroup(dict(before=snap,after=snap,closed=True))
                mem = dict(line.split() for line in snap['memory.stat'].splitlines())
                sample_report.update(samples=sample_report['samples']+1,
                    memory_current_peak_bytes=max(sample_report['memory_current_peak_bytes'],int(snap['memory.current'])),
                    anonymous_peak_bytes=max(sample_report['anonymous_peak_bytes'],int(mem['anon'])),
                    page_cache_peak_bytes=max(sample_report['page_cache_peak_bytes'],int(mem['file'])))
            except Exception as error:
                resource_errors.append(dict(type=type(error).__name__,message=str(error))); return
            stop.wait(.25)
    monitor = threading.Thread(target=sample,daemon=True)
    def terminated(signum,frame): termination.set()
    handlers = {s:signal.signal(s,terminated) for s in (signal.SIGTERM,signal.SIGINT)}
    try:
        write(output/'config.json',Path(config_path).read_bytes())
        write(output/'source-qualification.json',dict(proof,**source))
        write(output/'input-hashes.json',evidence['identities'])
        write(output/'tool-versions.json',dict(ids.tool_versions(),thread_environment={n:os.environ[n] for n in THREAD_ENV}))
        monitor.start(); deadline = started_ns+(3000-90)*10**9
        # Install process cleanup once; widths and authorities stay local to calls.
        with patch.object(cold,'stop',native.close_native):
            for index,role in enumerate(CELLS):
                before = native.capture(); cell_scratch = scratch/f'cell{index}'
                if stop_after is None:
                    cell_scratch.mkdir(); binary = cell_scratch/'http'
                    pointer = config['roles'][role]['binary']
                    write(binary,read(repo,pointer)); binary.chmod(0o500)
                    assert artifact(binary) == identity(pointer), 'role binary drift'
                    def call(q,port):
                        assert not resource_errors and not termination.is_set(), 'shared resource or termination failure'
                        return measured_call(binary,config,evidence,q,port,wave_objects=WIDTHS[role])
                    rows,epoch,terminal,abort = offered.schedule_offers(call,8,workers=6,base_port=18080,deadline_ns=deadline)
                else:
                    epoch = time.monotonic_ns(); rows = aborted_rows(index,epoch,stop_after,config)
                    terminal,abort = time.monotonic_ns(),stop_after
                # Preserve every scheduler row before cleanup or observation can fail.
                records.extend(rows)
                for row in rows:
                    row.update(cell_index=index,role=role,wave_objects=WIDTHS[role],role_authority=role_authority(config,role),dataset='ReLAION')
                    if row.get('failure_stage') in ('thread_start','callback'):
                        q = row['query_ordinal']
                        row.update(expected_authority=evidence['arm']['authority'],http_retry=False,
                            request_sha256=sha(evidence['requests'][q]),request_bytes=len(evidence['requests'][q]),
                            failure_kind='fatal',failure_stream_raw='')
                binary_after = None
                if stop_after is None:
                    binary_after = artifact(binary)
                    shutil.rmtree(cell_scratch)
                rows[0]['cell_receipt'] = dict(epoch_ns=epoch,terminal_ns=terminal,abort_after=abort,
                    cell_started=stop_after is None,worker_started_ns=started_ns,admission_deadline_ns=deadline,
                    campaign_cgroup_before=baseline,cgroup_before=before,cgroup_after=native.capture(),
                    resource_errors=list(resource_errors),cell_scratch_removed=not cell_scratch.exists(),binary_after=binary_after)
                result = reduce_cell(rows,evidence,config)
                cells.append(close_cell(output,rows,result,proof,on_cell_closed))
                if not result['valid'] and stop_after is None:
                    stop_after = dict(cell_index=index,reason='cell validation failed',observed_ns=terminal)
        summary = reduce_records(records,evidence,config)
        assert summary['execution_gate_passed'] and not termination.is_set(), 'fatal identity/resource/cleanup failure'
    except BaseException as error:
        failure = error
        # Preserve partial raw cells; fill only positions not already admitted.
        for index in range(len(records)//64,4):
            epoch = time.monotonic_ns()
            records.extend(aborted_rows(index,epoch,dict(reason='fatal '+type(error).__name__,observed_ns=epoch),config))
        summary = dict(schema='borsuk-startup-wave8-paired-result-v1',closed=True,execution_gate_passed=False,
            paired_gate_passed=False,candidate_over_actual_bracketing_control_latency_ratio='UNMEASURED',
            error_type=type(error).__name__,error=str(error),planned_positions=256,closed_cells=cells)
    finally:
        stop.set()
        if monitor.ident is not None: monitor.join(timeout=5)
        cleanup_error = None
        try:
            assert not monitor.is_alive()
            shutil.rmtree(scratch); assert not scratch.exists()
            counters.update(after=native.capture(),closed=True); validate_cgroup(counters)
            assert not resource_errors and not termination.is_set() and time.monotonic_ns()-started_ns <= 3000*10**9
        except BaseException as error:
            cleanup_error = error
            summary.update(execution_gate_passed=False,paired_gate_passed=False,
                candidate_over_actual_bracketing_control_latency_ratio='UNMEASURED',cleanup_error=str(error))
        for name,value in (
            ('records.jsonl',b''.join(encoded(r)+b'\n' for r in records)),
            ('failures.jsonl',b''.join(encoded(r)+b'\n' for r in records if r['outcome']=='failed')),
            ('summary.json',summary),('paired-cgroup.json',counters),
            ('cleanup.json',dict(valid=cleanup_error is None,scratch_removed=not scratch.exists(),observer_stopped=not monitor.is_alive(),
                build_invocations=0,publication_invocations=0)),
            ('resources.json',dict(wall_seconds=(time.monotonic_ns()-started_ns)/1e9,
                process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,observations=sample_report,
                resource_errors=resource_errors,native_invocations=sum(r['native_process_started'] for r in records),
                http_attempts=sum(r['http_attempts'] for r in records),memory_bytes=MEMORY,swap_bytes=0,native_memory_bytes=NATIVE,
                native_rlimit_as_bytes=4*1024**3,server_query_slots=native.SERVER_QUERY_SLOTS,
                native_budget_model=native.native_budget_model(evidence['arm']['metadata_files']),
                build_invocations=0,publication_invocations=0,retained_publication=config['cold_run']['files']['screen/publication.json'],
                prices=config['prices'],cost_measured=False,
                billing='root reduction of actual instance lifetime and request/storage populations; caps are not bills'))):
            write(output/name,value)
        for signum,handler in handlers.items(): signal.signal(signum,handler)
        if cleanup_error is not None: raise cleanup_error
    if failure is not None: raise failure
    return summary


def replay(config_path, expected_sha, repo, output):
    """Read-only replay. The controller authenticates terminal/cloud closure separately."""
    repo,output = Path(repo).resolve(),Path(output)
    config,proof = qualify(config_path,expected_sha,repo); evidence = inputs(repo,config)
    assert (output/'config.json').read_bytes() == Path(config_path).read_bytes()
    qualification = json.loads((output/'source-qualification.json').read_bytes())
    source = {n:qualification[n] for n in ('source_commit','source_archive_sha256')}
    assert panel.re.fullmatch('[0-9a-f]{40}',source['source_commit']) and panel.re.fullmatch('[0-9a-f]{64}',source['source_archive_sha256'])
    assert qualification == dict(proof,**source)
    assert json.loads((output/'input-hashes.json').read_bytes()) == evidence['identities']
    body = (output/'records.jsonl').read_bytes(); records = [json.loads(line) for line in body.splitlines()]
    assert body == b''.join(encoded(r)+b'\n' for r in records), 'canonical raw ledger'
    summary = reduce_records(records,evidence,config)
    assert json.loads((output/'summary.json').read_bytes()) == summary
    for index,cell in enumerate(summary['cells']):
        rows = records[index*64:(index+1)*64]; path = output/f'cell{index}-records.jsonl'
        assert path.read_bytes() == b''.join(encoded(r)+b'\n' for r in rows)
        marker = dict(cell,records=artifact(path),config_sha256=proof['config_sha256'],code_identity_sha256=proof['code_identity_sha256'],
            refs_identity_sha256=proof['refs_identity_sha256'],binary_sha256=proof['roles'][CELLS[index]]['binary']['sha256'])
        target = output/f'cell{index}-summary.json'
        assert json.loads(target.read_bytes()) == marker
        assert json.loads((output/f'cell{index}-seal.json').read_bytes()) == dict(
            schema='borsuk-startup-wave8-cell-seal-v1',cell_index=index,role=CELLS[index],records=artifact(path),summary=artifact(target))
    assert (output/'failures.jsonl').read_bytes() == b''.join(encoded(r)+b'\n' for r in records if r['outcome']=='failed')
    counters = json.loads((output/'paired-cgroup.json').read_bytes()); validate_cgroup(counters)
    assert counters['before'] == records[0]['cell_receipt']['campaign_cgroup_before']
    clean,report = (json.loads((output/n).read_bytes()) for n in ('cleanup.json','resources.json'))
    assert clean['valid'] is clean['scratch_removed'] is clean['observer_stopped'] is True
    assert clean['build_invocations'] == clean['publication_invocations'] == report['build_invocations'] == report['publication_invocations'] == 0
    assert type(report['wall_seconds']) in (int,float) and 0 <= report['wall_seconds'] <= 3000 and report['resource_errors'] == []
    assert report['native_invocations'] == sum(r['native_process_started'] for r in records)
    assert report['http_attempts'] == sum(r['http_attempts'] for r in records)
    assert report['prices'] == config['prices'] and report['cost_measured'] is False
    assert report['retained_publication'] == config['cold_run']['files']['screen/publication.json']
    assert report['native_memory_bytes'] == NATIVE and report['server_query_slots'] == 4
    assert report['memory_bytes'] == MEMORY and report['swap_bytes'] == 0 and report['native_rlimit_as_bytes'] == 4*1024**3
    assert report['native_budget_model'] == native.native_budget_model(evidence['arm']['metadata_files'])
    assert not (output/'scratch').exists()
    return summary


def self_check():
    sizes = [2000,1000,3907*32,500,3072,1000000,1024,125000]
    arm = dict(discovery='semantic', authority=dict(root_sha256='a'*64,generation=1,control_epoch=1),
        indexes={'10':'retained/native'},metadata_files=dict(zip(native.STARTUP,sizes)),
        metadata_sha256={n:'a'*64 for n in native.STARTUP},head_file=dict(bytes=200,sha256='a'*64),
        leaf_object=dict(bytes=31250*1540,sha256='a'*64))
    metadata = [dict(name=n,bytes=size,chunks=1,metadata_wave=0 if i==0 else (i-1)//4+1,
        metadata_wave_wall_ns=10,logical_head_requests=int(n not in native.EXACT),logical_get_requests=1,
        head_wall_ns=int(n not in native.EXACT),get_wall_ns=2,stream_wall_ns=3,write_wall_ns=1,
        payload_buffer_bound_bytes=size) for i,(n,size) in enumerate(zip(native.STARTUP,sizes))]
    startup = dict(metadata=metadata,staging_wall_ns=30,decode_wall_ns=2,source_head_requests=1,
        router_head_requests=1,source_head_wall_ns=2,router_head_wall_ns=2)
    def transport(methods, payload):
        return dict(schema='borsuk-native-transport-v1',scope='process_all_native_s3_readers',per_query_delta=False,
            attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests',method_order=native.stats.METHODS,
            status_counts_format='[http_status,count] nonzero entries',
            payload_measurement='consumed response data frames, including unauthenticated payload',
            unknown=native.stats.UNKNOWN,dropped_error_body_consumed_bytes=0,
            totals=dict(attempts=sum(methods),method_counts=methods,status_counts=[[200,sum(methods)]],
                transport_failures=0,stream_failures=0,dropped_error_bodies=0,consumed_payload_bytes=payload))
    header = dict(phase='ready',authority=arm['authority'],listen='127.0.0.1:18080',remote_open_stats=startup,
        remote_open_wall_ns=40,head_read_wall_ns=10,transport=transport([12,5,1]+[0]*7,sum(sizes)+2500))
    expected = dict(query_ordinal=0,ids=list(range(10)),ranges=[[0,199680]],planned_bytes=199680,
        submitted_gets=1,verified_bytes=199680,failed_gets=0,source_submitted_gets=1,source_verified_bytes=100,
        source_failed_gets=0,router_submitted_gets=8,router_verified_bytes=500,router_failed_gets=0)
    stages = {n:dict(start_ns=i*10+1,end_ns=i*10+8) for i,n in enumerate(native.stats.STAGES)}
    stages['leaf_peak_inflight'] = 8
    response = dict(expected,authority=arm['authority'],query_stages=stages,native_wall_ns=40,
        transport=transport([22,5,1]+[0]*7,header['transport']['totals']['consumed_payload_bytes']+200280))
    request = native.http_request([1]*768,arm['authority'])
    evidence = dict(arm=arm,requests=[request]*64,references=[dict(expected,query_ordinal=q) for q in range(64)],
        truth=[list(range(100)) for q in range(64)])
    import copy
    candidate = copy.deepcopy(header)
    for row in candidate['remote_open_stats']['metadata'][1:]: row['metadata_wave'] = 1
    try:
        actual = native.transport(candidate, response, arm, wave_objects=8)
    except TypeError as error:
        raise AssertionError('explicit wave8 transport propagation is missing') from error
    assert actual['metadata']['metadata_objects'] == 8
    assert native.transport(header, response, arm) == native.transport(header, response, arm, wave_objects=4)
    import copy
    import tempfile
    from unittest.mock import Mock
    started = time.monotonic()
    def rejects(action):
        try: action()
        except (AssertionError,ValueError,KeyError,TypeError,FileNotFoundError,FileExistsError,RuntimeError): return
        raise AssertionError('invalid evidence accepted')
    # Wrong width must fail, and explicitly widened telemetry must succeed.
    rejects(lambda:native.transport(candidate,response,arm))
    rejects(lambda:native.transport(header,response,arm,wave_objects=8))
    rejects(lambda:native.transport(header,response,arm,wave_objects=True))
    templates,failed_templates = {},{}
    log = 'User time (seconds): 0.01\nSystem time (seconds): 0.01\nMaximum resident set size (kbytes): 42\n'
    process = Mock(pid=1234,returncode=143); process.poll.return_value = None
    cpu = dict(pid=1235,start_ticks=100,user_ticks=1,system_ticks=1,address_space_limit_bytes=4*1024**3,cpu_affinity=[0,1,2,3])
    def killpg(pid,sig):
        assert pid == 1234
        if sig == 0: raise ProcessLookupError()
    for width in (4,8):
        h = header if width == 4 else candidate
        def spawn(args,**kwargs):
            assert '--as=4294967296:4294967296' in args and kwargs['start_new_session'] is True
            assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES'] == str(NATIVE)
            kwargs['stdout'].write(json.dumps(h)+'\n'); kwargs['stdout'].flush()
            Path(args[3]).write_text(log)
            return process
        for status,payload in ((200,encoded(response)),(503,b'{"error":"unavailable"}'),(200,encoded(dict(response,authority={})))):
            original_stop = cold.stop
            with patch.object(subprocess,'Popen',side_effect=spawn), patch.object(cold.http.client,'HTTPConnection'), \
                patch.object(cold,'post',return_value=(status,payload)), patch.object(native,'native_cpu',side_effect=[cpu,dict(cpu,user_ticks=2)]), \
                patch.object(native,'cold_stop',return_value=dict(intentional_stop=True,returncode=143)), \
                patch.object(native.os,'killpg',side_effect=killpg),patch.object(cold,'stop',native.close_native):
                row = measured_call('mock',FIXED,evidence,0,18080,wave_objects=width)
            assert cold.stop is original_stop and row['cleanup_confirmed'] is True
            assert base64.b64decode(row['raw_response_base64']) == payload
            if status == 200 and payload == encoded(response):
                assert row['outcome'] == 'success',row; templates[width] = row
            else:
                assert row['outcome'] == 'failed' and row['failure_stream_raw']
                assert row['failure_kind'] == ('transport' if status == 503 else 'fatal')
                failed_templates[width,row['failure_kind']]=row
    # Overlapping calls carry different telemetry widths without module mutation.
    from concurrent.futures import ThreadPoolExecutor
    barrier=threading.Barrier(2,timeout=2);cpu_calls={};cpu_lock=threading.Lock()
    def parallel_spawn(args,**kwargs):
        port=int(args[-1].rsplit(':',1)[1]);h=copy.deepcopy(header if port==18080 else candidate)
        h['listen']=args[-1];kwargs['stdout'].write(json.dumps(h)+'\n');kwargs['stdout'].flush()
        Path(args[3]).write_text(log)
        process=Mock(pid=port+100,returncode=143);process.poll.return_value=None;return process
    def parallel_cpu(pid,binary):
        with cpu_lock: cpu_calls[pid]=cpu_calls.get(pid,0)+1;tick=cpu_calls[pid]
        return dict(cpu,pid=pid+1,user_ticks=tick)
    def parallel_post(client,payload):
        barrier.wait();return 200,encoded(response)
    def parallel_kill(pid,sig):
        if sig==0: raise ProcessLookupError()
    with patch.object(subprocess,'Popen',side_effect=parallel_spawn),patch.object(cold.http.client,'HTTPConnection'), \
        patch.object(cold,'post',side_effect=parallel_post),patch.object(native,'native_cpu',side_effect=parallel_cpu), \
        patch.object(native,'cold_stop',return_value=dict(intentional_stop=True,returncode=143)), \
        patch.object(native.os,'killpg',side_effect=parallel_kill),patch.object(cold,'stop',native.close_native):
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures=[executor.submit(measured_call,'mock',FIXED,evidence,0,port,wave_objects=width) for port,width in ((18080,4),(18085,8))]
            overlap=[future.result(timeout=3) for future in futures]
    assert all(row['outcome']=='success' and row['cleanup_confirmed'] for row in overlap),overlap
    assert overlap[0]['native_header']['remote_open_stats']['metadata'][-1]['metadata_wave']==2
    assert overlap[1]['native_header']['remote_open_stats']['metadata'][-1]['metadata_wave']==1
    snapshot = dict(cgroup='/mock',observer_pid=1,process_ids=[1],**{
        'memory.max':str(MEMORY),'memory.peak':'100000','memory.swap.max':'0','memory.swap.peak':'0',
        'memory.events':'oom 0\noom_kill 0\noom_group_kill 0\nmax 0\n','memory.swap.events':'max 0\n',
        'cpu.max':'200000 100000','cpu.stat':'usage_usec 1\n','pids.max':'512','pids.current':'1','pids.events':'max 0\n',
        'memory.current':'100000','memory.stat':'anon 1000\nfile 99000\n','io.stat':'UNMEASURED',
        'io.stat_unavailable':dict(type='FileNotFoundError',errno=2)})
    config = dict(FIXED,roles={r:dict(wave_objects=w,native_source_commit=SOURCE_COMMITS[r],
        binary=dict(bytes=10,sha256=('b' if r=='control' else 'c')*64),
        proof=dict(bytes=10,sha256='d'*64),source_manifest=dict(bytes=10,sha256='e'*64)) for r,w in WIDTHS.items()})
    def ledger():
        rows=[]; epoch=10**12
        for index,role in enumerate(CELLS):
            for q in range(64):
                row=copy.deepcopy(templates[WIDTHS[role]]); at=epoch+q*125000000
                row.update(query_ordinal=q,cell_index=index,role=role,wave_objects=WIDTHS[role],role_authority=role_authority(config,role),
                    dataset='ReLAION',offered_qps=8,scheduled_ns=at,dispatched_ns=at,started_ns=at+1,
                    successful_connect_attempt_ns=at+100,connected_ns=at+200,completed_ns=at+1000,
                    first_wire_completed_ns=at+1000,terminal_ns=at+2000,port=18080,cold_start_to_first_http_response_ns=999,
                    before_successful_connect_attempt_ns=99,successful_tcp_connect_ns=100,
                    first_post_to_response_ns=800,incoming_http_wall_ns=900)
                rows.append(row)
            terminal=rows[-1]['terminal_ns']+1
            rows[-64]['cell_receipt']=dict(epoch_ns=epoch,terminal_ns=terminal,abort_after=None,cell_started=True,
                worker_started_ns=10**12,admission_deadline_ns=10**12+(3000-90)*10**9,campaign_cgroup_before=snapshot,
                cgroup_before=snapshot,cgroup_after=copy.deepcopy(snapshot),resource_errors=[],cell_scratch_removed=True,binary_after=identity(config['roles'][role]['binary']))
            epoch=terminal+1
        return rows
    rows = ledger(); result = reduce_records(rows,evidence,config)
    assert result['paired_gate_passed'] and result['counts']['successful'] == 256
    assert result['candidate_over_actual_bracketing_control_latency_ratio']['1']['0']['cold']['p90'] == 1
    assert result == reduce_records(copy.deepcopy(rows),copy.deepcopy(evidence),copy.deepcopy(config))
    rejects(lambda:reduce_records(rows[:-1],evidence,config))
    for kind in ('role','source','width','truth_hits','request','raw','wave','transport','resource','cleanup','scratch','binary_after','port','order'):
        bad=copy.deepcopy(rows)
        if kind=='role': bad[64]['role']='control'
        elif kind=='source': bad[64]['role_authority']['source_identity_sha256']='0'*64
        elif kind=='width': bad[64]['wave_objects']=4
        elif kind=='truth_hits': bad[64]['returned_hits']=9
        elif kind=='request': bad[64]['request_sha256']='0'*64
        elif kind=='raw': bad[64]['response']['ids'].reverse()
        elif kind=='wave': bad[64]['native_header']['remote_open_stats']['metadata'][5]['metadata_wave']=2
        elif kind=='transport': bad[64]['accounting']['imds_token_PUTs']=2
        elif kind=='resource': bad[64]['cell_receipt']['cgroup_after']['memory.max']='1024'
        elif kind=='cleanup': bad[64]['native_close']['process_group_closed']=False
        elif kind=='scratch': bad[64]['cell_receipt']['cell_scratch_removed']=False
        elif kind=='binary_after': bad[64]['cell_receipt']['binary_after']['sha256']='0'*64
        elif kind=='port': bad[64]['terminal_ns']=bad[65]['terminal_ns']
        else: bad[64:128]=bad[64:128][::-1]
        rejects(lambda:reduce_records(bad,evidence,config))
    bad_truth=copy.deepcopy(evidence);bad_truth['truth'][0][0]=bad_truth['truth'][0][1]
    rejects(lambda:reduce_records(rows,bad_truth,config))
    boundary=copy.deepcopy(evidence)
    cell=copy.deepcopy(rows[:64])
    for q in range(32): boundary['truth'][q]=list(range(9))+list(range(100,191));cell[q]['returned_hits']=9
    assert reduce_cell(cell,boundary,config)['valid']
    boundary['truth'][32]=list(range(9))+list(range(100,191));cell[32]['returned_hits']=9
    assert not reduce_cell(cell,boundary,config)['valid']
    # Historical reducers still accept the original default4 ledger.
    historical=copy.deepcopy(rows[:64])
    for row in historical: row['rate_index']=5
    assert previous.reduce_cell(historical,evidence,previous.FIXED)['attainment']
    for row in historical:
        row['native_header']['listen']='127.0.0.1:8080'
        row['native_server_log']=json.dumps(row['native_header'])+'\n'
    old_args=(historical,arm,evidence['requests'],evidence['references'],evidence['truth'],historical[0]['scheduled_ns'],historical[0]['cell_receipt']['terminal_ns'])
    assert native.reduce_records(*old_args)==native.reduce_records(*old_args,wave_objects=4)
    assert native.reduce_records(*old_args)['execution_gate_passed']
    # Six owners retain their slots through cleanup: position 6 must drop.
    owners,held,release= set(),threading.Event(),threading.Event(); lock=threading.Lock()
    def call(q,port):
        with lock:
            assert port not in owners;owners.add(port)
            if len(owners)==6: held.set()
        if q<6: assert release.wait(2)
        with lock: owners.remove(port)
        return dict(outcome='success',cleanup_confirmed=True,abort_admissions=False)
    def unlock():
        assert held.wait(2);time.sleep(.025);release.set()
    unlocker=threading.Thread(target=unlock);unlocker.start()
    with patch.object(offered,'scheduled_offsets_ns',return_value=[q*4000000 for q in range(64)]):
        held_rows,_,_,_=offered.schedule_offers(call,250)
    unlocker.join(2)
    assert held_rows[6]['outcome']=='capacity_drop' and len(held_rows)==64 and not owners
    assert any(r['outcome']=='success' for r in held_rows[12:])
    dropped=copy.deepcopy(rows);until=dropped[6]['scheduled_ns']+1000000
    for q in range(6):
        dropped[q]['terminal_ns']=until;dropped[q]['port']=18080+q
        dropped[q]['native_header']['listen']=f'127.0.0.1:{18080+q}'
        dropped[q]['native_server_log']=json.dumps(dropped[q]['native_header'])+'\n'
    at=dropped[6]['scheduled_ns']
    dropped[6]=dict(aborted_rows(0,rows[0]['scheduled_ns'],{},config)[6],outcome='capacity_drop',dispatched_ns=at,terminal_ns=at)
    stopped=dict(cell_index=0,reason='cell validation failed',observed_ns=rows[0]['cell_receipt']['terminal_ns'])
    for index in range(1,4):
        receipt=copy.deepcopy(rows[index*64]['cell_receipt']);receipt.update(cell_started=False,abort_after=stopped,binary_after=None)
        dropped[index*64:(index+1)*64]=aborted_rows(index,receipt['epoch_ns'],stopped,config)
        dropped[index*64]['cell_receipt']=receipt
    failed=copy.deepcopy(dropped)
    failed[:64]=copy.deepcopy(rows[:64])
    at=failed[17]['scheduled_ns'];failure=copy.deepcopy(failed_templates[4,'transport'])
    failure.update(query_ordinal=17,cell_index=0,role='control',wave_objects=4,role_authority=role_authority(config,'control'),
        dataset='ReLAION',offered_qps=8,scheduled_ns=at,dispatched_ns=at,started_ns=at+1,completed_ns=at+1000,
        first_wire_completed_ns=at+1000,terminal_ns=at+2000,port=18080,valid_ann_requests=0)
    stream=json.loads(failure['failure_stream_raw']);stream['started_ns']=failure['started_ns'];stream['completed_ns']=failure['completed_ns']
    failure['failure_stream_raw']=json.dumps(stream)+'\n';failed[17]=failure
    failed_result=reduce_records(failed,evidence,config)
    assert failed_result['execution_gate_passed'] and failed_result['cells'][0]['errors']==1 and failed_result['counts']['planned']==256
    assert failed_result['candidate_over_actual_bracketing_control_latency_ratio']=='UNMEASURED'
    rejected=reduce_records(dropped,evidence,config)
    assert not rejected['paired_gate_passed'] and rejected['candidate_over_actual_bracketing_control_latency_ratio']=='UNMEASURED'
    assert rejected['cells'][0]['capacity_drops']==1 and rejected['cells'][0]['latency_ms']['all_offers']['p99']=='UNBOUNDED'
    native_manifest=json.loads((Path(__file__).resolve().parents[1]/MANIFEST).read_bytes())
    role_fixtures = {}
    for role in WIDTHS:
        binding=config['roles'][role];source=dict(native_manifest['source_sha256'])
        if role=='control': source.update(CONTROL_DELTA)
        manifest=dict(native_source_commit=SOURCE_COMMITS[role],source_file_count=399,source_sha256=source,source_identity_sha256=SOURCE_IDS[role])
        proof=dict(schema='borsuk-startup-wave8-role-proof-v1' if role=='candidate' else 'borsuk-semantic-1m-scoped-native-proof-v1',
            qualified=True,authority_pending=False,role=role,wave_objects=WIDTHS[role],native_source_commit=SOURCE_COMMITS[role],source_file_count=399,
            native_source_sha256=source,source_identity_sha256=SOURCE_IDS[role],current_whole_tree_full_execution=False,
            binary_bytes=10,binary_sha256=binding['binary']['sha256'],production_library_unchanged=role=='control',
            original_full_source_identity_sha256='a'*64,release_status=0,clippy_status=0,workspace_test_compilation_status=0,oom_kills=0,swap_peak_bytes=0,
            evidence={n:dict(path=n,bytes=10,sha256='f'*64) for n in ('workspace_receipt','test_log','workspace_cgroup','source_before','source_after','source_qualification')})
        assert validate_role(role,binding,manifest,proof,native_manifest['source_sha256'])==proof
        role_fixtures[role] = (manifest,proof)
        for field,value in (('qualified',False),('authority_pending',True),('source_identity_sha256','0'*64),('source_file_count',398),
            ('current_whole_tree_full_execution',True),('binary_sha256','0'*64),('release_status',1),('clippy_status',None),
            ('workspace_test_compilation_status',1),('oom_kills',1),('swap_peak_bytes',1),('production_library_unchanged',role=='candidate')):
            bad=dict(proof,**{field:value});rejects(lambda:validate_role(role,binding,manifest,bad,native_manifest['source_sha256']))
        if role=='candidate':
            for field,value in (('role','wrong'),('wave_objects',4),('native_source_commit','0'*40)):
                rejects(lambda:validate_role(role,binding,manifest,dict(proof,**{field:value}),native_manifest['source_sha256']))
        bad=copy.deepcopy(manifest);bad['source_sha256'][next(iter(source))]='0'*64
        rejects(lambda:validate_role(role,binding,bad,proof,native_manifest['source_sha256']))
    with tempfile.TemporaryDirectory() as directory:
        output=Path(directory)/'screen';output.mkdir()
        proof=dict(config_sha256='b'*64,code_identity_sha256='c'*64,refs_identity_sha256='d'*64,
            roles={r:dict(binary=identity(b['binary'])) for r,b in config['roles'].items()})
        callbacks=[]
        for index,cell in enumerate(result['cells']):
            close_cell(output,rows[index*64:(index+1)*64],cell,proof,lambda m,p:callbacks.append((m,p)))
        assert len(callbacks)==4
        for marker,paths in callbacks:
            assert marker['summary']==artifact(paths['summary']) and marker['records']==artifact(paths['records'])
            assert marker['seal']==artifact(paths['seal'])
        rejects(lambda:close_cell(output,rows[:64],result['cells'][0],proof))
        write(output/'config.json',b'{}');write(output/'source-qualification.json',dict(proof,source_commit='a'*40,source_archive_sha256='a'*64))
        write(output/'input-hashes.json',evidence.get('identities',{}));evidence['identities']=evidence.get('identities',{})
        write(output/'records.jsonl',b''.join(encoded(r)+b'\n' for r in rows));write(output/'summary.json',result)
        write(output/'failures.jsonl',b'');write(output/'paired-cgroup.json',dict(before=snapshot,after=snapshot,closed=True))
        write(output/'cleanup.json',dict(valid=True,scratch_removed=True,observer_stopped=True,build_invocations=0,publication_invocations=0))
        config.update(prices=dict(path='price',bytes=1,sha256='e'*64),cold_run=dict(files={'screen/publication.json':dict(path='pub',bytes=1,sha256='e'*64)}))
        report=dict(wall_seconds=1,resource_errors=[],native_invocations=256,http_attempts=256,prices=config['prices'],cost_measured=False,
            native_memory_bytes=NATIVE,server_query_slots=4,memory_bytes=MEMORY,swap_bytes=0,native_rlimit_as_bytes=4*1024**3,
            native_budget_model=native.native_budget_model(evidence['arm']['metadata_files']),retained_publication=config['cold_run']['files']['screen/publication.json'],
            build_invocations=0,publication_invocations=0)
        write(output/'resources.json',report)
        with patch(__name__+'.qualify',return_value=(config,proof)),patch(__name__+'.inputs',return_value=evidence):
            assert replay(output/'config.json','mock',Path(directory),output)==result
            assert replay(output/'config.json','mock',Path(directory),output)==result
            target=output/'cell1-seal.json';saved=target.read_bytes();target.write_bytes(b'{}')
            rejects(lambda:replay(output/'config.json','mock',Path(directory),output));target.write_bytes(saved)
            target=output/'records.jsonl';saved=target.read_bytes();target.write_bytes(saved+b'\n')
            rejects(lambda:replay(output/'config.json','mock',Path(directory),output));target.write_bytes(saved)
            target=output/'cleanup.json';saved=target.read_bytes();target.write_bytes(encoded(dict(valid=False)))
            rejects(lambda:replay(output/'config.json','mock',Path(directory),output));target.write_bytes(saved)
        # Pending authority must fail before creating outputs or starting a native process.
        repo=Path(directory)/'repo';target=repo/CONFIG;target.parent.mkdir(parents=True)
        target.write_bytes(encoded(dict(authority_pending=True)))
        with patch.object(subprocess,'Popen',side_effect=AssertionError('unexpected process')):
            rejects(lambda:main(target,artifact(target)['sha256'],repo,Path(directory)/'must-not-exist'))
        assert not (Path(directory)/'must-not-exist').exists()
    with tempfile.TemporaryDirectory() as directory:
        root=Path(directory);repo=root/'repo';repo.mkdir()
        for name in CODE:
            target=repo/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(b'synthetic code pin')
        target=repo/MANIFEST;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(encoded(native_manifest))
        def put(name,value):
            target=repo/name;write(target,value);return dict(path=name,**artifact(target))
        run=dict(directory=COLD_DIRECTORY,files={})
        frozen_cold=dict(namespace_prefix='retained/native')
        for name in previous.COLD_ROSTER:
            content = dict(state='terminated') if name=='aws-closeout.json' else frozen_cold if name=='screen/config.json' else dict(synthetic=True)
            run['files'][name]=put(str(Path(COLD_DIRECTORY)/name),content)
        cfg=dict(FIXED,authority_pending=False,code_sha256={n:artifact(repo/n)['sha256'] for n in CODE},
            cold_config=run['files']['screen/config.json'],cold_run=run,
            cold_source_authority=put('source-authority.json',dict(synthetic=True)),cold_fail_disposition=None,
            measurement_prefix=PREFIX+'a0001',prices=put('price.json',dict(usd=1)),roles={})
        for role,(manifest,proof) in role_fixtures.items():
            proof=copy.deepcopy(proof);binary=put(role+'-http',role.encode())
            proof.update(binary_bytes=binary['bytes'],binary_sha256=binary['sha256'])
            manifest_pointer=put(role+'-manifest.json',manifest)
            if role=='control': proof['evidence']={'original':put('control-original.json',dict(synthetic=True))}
            else:
                qualification=dict(schema='borsuk-startup-wave8-implementation-gates-qualification-v1',
                    source_sha256=manifest['source_sha256'],source_identity_sha256=SOURCE_IDS[role],source_file_count=399,
                    native_source_commit=SOURCE_COMMITS[role],actual_full_workspace_execution=False,
                    command=['bash','scripts/check_startup_wave8_implementation.sh'],environment=workspace.ENVIRONMENT,
                    config_sha256='a'*64,code_identity_sha256='b'*64,campaign_schema='borsuk-startup-wave8-implementation-gates-spot-v1',
                    artifact_roster_sha256='c'*64,controller_source_commit='d'*40,candidate_delta_paths=list(CONTROL_DELTA),
                    native_source_manifest_sha256=manifest_pointer['sha256'])
                stages=[]
                for index,(name,command) in enumerate(zip(GATE_NAMES,GATE_COMMANDS)):
                    start=dict(schema='borsuk-startup-wave8-implementation-stage-v1',stage=name,command=command,
                        started_at=f'2026-10-02T00:00:{index*2:02d}Z',finished_at=None,exit_status=None)
                    stages.extend((start,dict(start,finished_at=f'2026-10-02T00:00:{index*2+1:02d}Z',exit_status=0)))
                cg=dict(before=dict(snapshot,pids_peak='1'),after=dict(snapshot,pids_peak='1'),closed=True)
                evidence_pointers=dict(source_qualification=put('candidate-source-qualification.json',qualification),
                    source_before=put('candidate-source-before.json',manifest['source_sha256']),
                    source_after=put('candidate-source-after.json',manifest['source_sha256']),workspace_cgroup=put('candidate-cgroup.json',cg),
                    test_log=put('candidate-test.log',b'compiler output\n'+b''.join(encoded(stage)+b'\n' for stage in stages)))
                receipt=dict(qualification,schema='borsuk-startup-wave8-implementation-gates-receipt-v1',qualified=True,
                    exit_status=0,gate_status=0,command_started=True,command_completed=True,source_unchanged=True,
                    execution_kind='implementation-gates',qualification_sha256=evidence_pointers['source_qualification']['sha256'],
                    artifacts={path:identity(evidence_pointers[name]) for name,path in (
                        ('test_log','test.log'),('workspace_cgroup','workspace-cgroup.json'),('source_before','source-before.json'),
                        ('source_after','source-after.json'),('source_qualification','source-qualification.json'))})
                receipt['artifacts'].update({'binaries/two_bit_http':identity(binary),'native-source-manifest.json':identity(manifest_pointer)})
                evidence_pointers['workspace_receipt']=put('candidate-workspace-receipt.json',receipt)
                proof['evidence']=evidence_pointers
            cfg['roles'][role]=dict(wave_objects=WIDTHS[role],native_source_commit=SOURCE_COMMITS[role],binary=binary,
                source_manifest=manifest_pointer,proof=put(role+'-proof.json',proof))
        frozen_cold.update(native_proofs={'http':cfg['roles']['control']['proof']},binaries={'http':cfg['roles']['control']['binary']})
        cold_path=repo/cfg['cold_config']['path'];cold_path.write_bytes(encoded(frozen_cold)+b'\n')
        cold_pointer=dict(path=cfg['cold_config']['path'],**artifact(cold_path))
        cfg['cold_config']=run['files']['screen/config.json']=cold_pointer
        checked=dict(cold_config=frozen_cold,qualification=dict(native_source_sha256=role_fixtures['control'][0]['source_sha256'],
            source_identity_sha256=SOURCE_IDS['control'],original_source_identity_sha256='a'*64),
            measurement_gate_passed=True,quality_gate=True,identity_gate=True,resource_gate=True,cleanup_gate=True,valid_calls=64,actual_http_attempts=64)
        candidate_binding=cfg['roles']['candidate']
        candidate_proof=json.loads(read(repo,candidate_binding['proof']))
        for kind in ('receipt_schema','incomplete','exit','full_execution','source','binary','gate_failed','missing_gate','gate_command','gate_time','cgroup_oom','cgroup_swap','cgroup_owner','source_after'):
            bad=copy.deepcopy(candidate_proof);pointers=bad['evidence']
            receipt=json.loads(read(repo,pointers['workspace_receipt']))
            if kind.startswith('gate_') or kind=='missing_gate':
                pairs=[json.loads(line) for line in read(repo,pointers['test_log']).splitlines() if line.startswith(b'{')]
                if kind=='gate_failed': pairs[1]['exit_status']=1
                elif kind=='missing_gate': pairs.pop()
                elif kind=='gate_command': pairs[3]['command']=['cargo','test','wrong']
                else: pairs[3]['finished_at']='2026-10-01T00:00:00Z'
                pointers['test_log']=put('bad-'+kind+'.log',b''.join(encoded(row)+b'\n' for row in pairs))
                receipt['artifacts']['test.log']=identity(pointers['test_log'])
            elif kind.startswith('cgroup_'):
                cg=json.loads(read(repo,pointers['workspace_cgroup']))
                if kind=='cgroup_oom': cg['after']['memory.events']='oom 1\noom_kill 1\noom_group_kill 0\nmax 0\n'
                elif kind=='cgroup_swap': cg['after']['memory.swap.peak']='1'
                else: cg['after']['process_ids'].append(2)
                pointers['workspace_cgroup']=put('bad-'+kind+'.json',cg)
                receipt['artifacts']['workspace-cgroup.json']=identity(pointers['workspace_cgroup'])
            elif kind=='source_after':
                body=json.loads(read(repo,pointers['source_after']));body['Cargo.lock']='0'*64
                pointers['source_after']=put('bad-source-after.json',body)
                receipt['artifacts']['source-after.json']=identity(pointers['source_after'])
            elif kind=='receipt_schema': receipt['schema']='old'
            elif kind=='incomplete': receipt['command_completed']=False
            elif kind=='exit': receipt['gate_status']=1
            elif kind=='full_execution': receipt['actual_full_workspace_execution']=True
            elif kind=='source': receipt['source_identity_sha256']='0'*64
            else: receipt['artifacts']['binaries/two_bit_http']['sha256']='0'*64
            pointers['workspace_receipt']=put('bad-'+kind+'-receipt.json',receipt)
            rejects(lambda:validate_candidate(repo,candidate_binding,bad))
        target=repo/CONFIG;target.write_bytes(encoded(cfg))
        def qualify_cfg(value):
            target.write_bytes(encoded(value));return qualify(target,artifact(target)['sha256'],repo)
        with patch.object(previous.cold_spot,'qualify_measurement',return_value=checked),patch.object(previous.cold_spot,'fail_disposition'):
            _,qualified=qualify_cfg(cfg)
            for field,value in (('authority_pending',True),('schema','old'),('offered_qps',4),('workers',8),
                ('cold_invocations',64),('publication_invocations',1),('code_sha256',{}),('measurement_prefix','historical-a2')):
                rejects(lambda:qualify_cfg(dict(cfg,**{field:value})))
            wrong=copy.deepcopy(cfg);wrong['cold_run']['directory']=str(native.ROOT/'a0004')
            rejects(lambda:qualify_cfg(wrong))
            wrong=copy.deepcopy(cfg);wrong['roles']['candidate']['wave_objects']=4
            rejects(lambda:qualify_cfg(wrong))
            wrong=copy.deepcopy(cfg);wrong['roles']['candidate']['proof']=wrong['roles']['control']['proof']
            rejects(lambda:qualify_cfg(wrong))
            wrong=copy.deepcopy(cfg);wrong['roles']['candidate']['source_manifest']=wrong['roles']['control']['source_manifest']
            rejects(lambda:qualify_cfg(wrong))
            wrong=copy.deepcopy(cfg);wrong['roles']['candidate']['binary']=wrong['roles']['control']['binary']
            rejects(lambda:qualify_cfg(wrong))
            # Tamper actual bodies while retaining frozen pointers.
            for pointer in (cfg['roles']['candidate']['binary'],cfg['roles']['candidate']['proof'],cfg['prices'],run['files']['screen/config.json']):
                path=repo/pointer['path'];saved=path.read_bytes();path.write_bytes(b'tampered')
                rejects(lambda:qualify_cfg(cfg));path.write_bytes(saved)
            _,qualified=qualify_cfg(cfg)
            # Exercise real main files, role binary staging/removal, markers and replay.
            fake_rows=ledger();cursor=[0];calls=[];callbacks=[]
            def fake_measured(binary,config,evidence,q,port,*,wave_objects):
                index=cursor[0];role=CELLS[index]
                assert wave_objects==WIDTHS[role] and Path(binary).read_bytes()==role.encode()
                calls.append((index,q,role));return copy.deepcopy(fake_rows[index*64+q])
            def fake_schedule(callback,rate,**kwargs):
                index=cursor[0];assert rate==8 and kwargs['workers']==6 and kwargs['base_port']==18080
                selected=[callback(q,18080) for q in range(64)]
                receipt=selected[0]['cell_receipt'];cursor[0]+=1
                return selected,receipt['epoch_ns'],receipt['terminal_ns'],None
            def cell_closed(marker,paths):
                assert not (paths['records'].parent/'scratch'/f"cell{marker['cell_index']}").exists()
                assert marker['summary']==artifact(paths['summary']) and marker['seal']==artifact(paths['seal'])
                callbacks.append(marker['role'])
            env=dict(os.environ,**{n:'2' for n in THREAD_ENV},AWS_MAX_ATTEMPTS='1',BORSUK_OFFERED_SOURCE_COMMIT='a'*40,BORSUK_OFFERED_ARCHIVE_SHA256='b'*64)
            output=root/'runtime'
            with patch.dict(os.environ,env),patch.object(os,'sched_getaffinity',return_value={4,5}), \
                patch.object(native,'capture',return_value=snapshot),patch.object(ids,'tool_versions',return_value={'synthetic':True}), \
                patch(__name__+'.inputs',return_value=evidence),patch(__name__+'.measured_call',side_effect=fake_measured), \
                patch.object(offered,'schedule_offers',side_effect=fake_schedule),patch.object(time,'monotonic_ns',return_value=10**12):
                actual=main(target,artifact(target)['sha256'],repo,output,on_cell_closed=cell_closed)
                assert actual['paired_gate_passed'] and len(calls)==256 and callbacks==list(CELLS)
                assert replay(target,artifact(target)['sha256'],repo,output)==actual
                rejects(lambda:main(target,artifact(target)['sha256'],repo,output))
                # Cleanup and binary identity failures must preserve the original 64 rows.
                remove=shutil.rmtree
                for kind in ('cell_cleanup','binary_drift'):
                    cursor[0]=0;failed_output=root/kind
                    def cleanup(path,*args,**kwargs):
                        if kind=='cell_cleanup' and Path(path).name=='cell0': raise RuntimeError('synthetic cell cleanup failed')
                        return remove(path,*args,**kwargs)
                    def scheduled(callback,rate,**kwargs):
                        answer=fake_schedule(callback,rate,**kwargs)
                        if kind=='binary_drift':
                            binary=failed_output/'scratch/cell0/http';binary.chmod(0o600);binary.write_bytes(b'drift')
                        return answer
                    with patch.object(shutil,'rmtree',side_effect=cleanup),patch.object(offered,'schedule_offers',side_effect=scheduled):
                        rejects(lambda:main(target,artifact(target)['sha256'],repo,failed_output))
                    retained=[json.loads(line) for line in (failed_output/'records.jsonl').read_bytes().splitlines()]
                    assert len(retained)==256 and all(row['outcome']=='success' and 'raw_response_base64' in row for row in retained[:64])
                    assert all(row['outcome']=='aborted' for row in retained[64:])
                    assert json.loads((failed_output/'summary.json').read_bytes())['paired_gate_passed'] is False
                    assert not (failed_output/'scratch').exists()
            assert not (output/'scratch').exists() and len((output/'records.jsonl').read_bytes().splitlines())==256

    assert time.monotonic()-started < 55 and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024 <= 200*1024**2
    print('PASS startup-wave8 paired synthetic: default4/explicit8, role/source/seven gates, raw wire/GT/parity, ABBA ledger, resources/cleanup, six-owner drop, deterministic replay/seals; no SDK/network/native/Cargo')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    elif len(sys.argv) == 6 and sys.argv[1] == '--replay':
        print(encoded(replay(*sys.argv[2:])).decode())
    else:
        assert len(sys.argv) == 5, 'CONFIG SHA REPO NEW_OUTPUT | --replay CONFIG SHA REPO OUTPUT | --self-check'
        main(*sys.argv[1:])
