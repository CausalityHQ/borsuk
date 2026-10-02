#!/usr/bin/env python3
"""Retained fixed48 a5 finite offers; parent freezes config and owns cloud.

CONFIG SHA REPO NEW_EXTERNAL_OUTPUT; --replay CONFIG SHA REPO CLOSED_OUTPUT.
--self-check uses only bounded local Python and authenticated archived bodies.
Historical Python maps bind the original archive, separately from executor CODE.
"""
import base64
import builtins
import copy
import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import resource
import signal
import shutil
import stat
import sys
import tempfile
import threading
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_cohere_fixed48_cold_http as fixed
from scripts import launch_cohere_fixed48_cold_http_spot as cold_spot
from scripts import run_native_cold_offered as offered

library, driver, science, stats = fixed.library, fixed.driver, fixed.science, fixed.stats
encoded, sha, artifact, write = fixed.encoded, fixed.sha, fixed.artifact, fixed.write
OWN = 'scripts/run_cohere_fixed48_offered_http.py'
SCHEMA = 'borsuk-cohere-fixed48-retained-offered-http-v1'
RATES = offered.RATES
ROOT = fixed.BASE / 'offered-http'
COLD = cold_spot.ROOT / 'a0005'
CODE = tuple(sorted(set((*cold_spot.CODE, *offered.CODE, OWN))))
FIXED = dict(fixed.FIXED, schema=SCHEMA, concurrency=5, workers=5, base_port=18080,
    offered_qps=RATES, max_dispatch_lateness_ns=125000000, cleanup_reserve_seconds=90,
    worker_limit_seconds=1440, service_limit_seconds=1500, machine_limit_seconds=1800,
    compute_cap_usd=.30, ebs_s3_allowance_usd=.15, build_invocations=0,
    publication_invocations=0, scientific_scorer_invocations=0, oracle_invocations=0, cold_invocations=384)
LIMITS = {k:v for k,v in fixed.HOST.items() if k != 'publisher_memory_bytes'}
LIMITS.update(driver_reserve_bytes=2 << 30, output_reserve_bytes=64 << 20,
              static_repository_reserve_bytes=2 << 30)
COLD_ROSTER = (*cold_spot.ARTIFACTS, 'aws-terminal.json', 'aws-closeout.json',
               'collection-receipt.json', 'root-audit.json')
# Root's full56 body attestation avoids transferring unused executable bodies.
UNUSED = ('native/binaries/check_semantic_router_scorer', 'native/binaries/two_bit_plan_demo')
CELL_FILES = tuple(f'rate{i}-{s}' for i in range(6) for s in ('records.jsonl', 'summary.json'))
OUTPUTS = ('config.json', 'source-qualification.json', 'input-hashes.json', 'records.jsonl',
    'failures.jsonl', 'summary.json', 'resources.json', 'offered-cgroup.json', 'cleanup.json',
    'failure.json', 'terminal.json', *CELL_FILES)
BRIDGE_KEYS = {'schema', 'execution_source', 'runtime_code_sha256', 'controller_code_sha256',
    'terminal', 'authenticated_artifacts', 'root_audit', 'native_source_identity_sha256',
    'source_file_count', 'whole_body_verification', 'original_controller_exit_status'}


def read(repo, pointer, *, materialize=True):
    """Authenticate original bytes; gzip is transport. Bounded streaming for logs/binaries."""
    assert set(pointer) in ({'path','bytes','sha256'}, {'path','bytes','sha256','archived_path'})
    name = pointer.get('archived_path', pointer['path'])
    if 'archived_path' in pointer:
        assert name == pointer['path']+'.gz'
    path = library.panel.repo_path(repo, name)
    assert type(pointer['bytes']) is int and 0 <= pointer['bytes'] <= 64 << 20
    stats.digest(pointer['sha256'])
    assert path.is_file() and not path.is_symlink()
    digest, size, chunks = hashlib.sha256(), 0, []
    opener = gzip.open if 'archived_path' in pointer else open
    with opener(path, 'rb') as stream:
        while chunk := stream.read(1 << 20):
            size += len(chunk)
            assert size <= pointer['bytes'], 'body exceeds pinned length: '+pointer['path']
            digest.update(chunk)
            if materialize:
                chunks.append(chunk)
    assert (size, digest.hexdigest()) == (pointer['bytes'], pointer['sha256']), 'body identity: '+pointer['path']
    return b''.join(chunks) if materialize else None


def identity(pointer):
    return {k:pointer[k] for k in ('bytes','sha256')}


def native_authority(repo, files, bridge):
    get = lambda n: json.loads(read(repo, files['native/'+n]))
    proof, root, terminal, receipt, manifest = map(get, ('source-qualification.json',
        'root-verification.json','aws-terminal.json','workspace-receipt.json','native-source-manifest.json'))
    assert proof['source_file_count'] == bridge['source_file_count'] == 399
    source = proof['source_sha256']
    assert len(source) == 399 and driver.qualification.worker.source_identity(source) == driver.SOURCE_ID
    assert bridge['native_source_identity_sha256'] == proof['source_identity_sha256'] == driver.SOURCE_ID
    # Native bodies are unchanged. This enumeration needs no Git and runs outside dispatch.
    assert driver.qualification.worker.source_hashes(repo) == source, 'qualified native source changed'
    assert manifest['source_sha256'] == source and manifest['source_file_count'] == 399
    assert manifest['source_identity_sha256'] == driver.SOURCE_ID
    for name in ('source-before.json','source-after.json'):
        assert get(name) == source
    assert root['qualified'] is True and root['source_file_count'] == 399
    assert root['source_identity_sha256'] == driver.SOURCE_ID and root['instance_state_verified'] == 'terminated'
    assert terminal['phase'] == terminal['status'] == 'complete' and terminal['exit_code'] == terminal['original_exit_code'] == 0
    closeout, reservation = get('aws-closeout.json'), get('aws-reservation.json')
    assert closeout['state'] == 'terminated' and terminal['instance_id'] in {n['instance_id'] for n in closeout['nodes'].values()}
    assert reservation['qualification'] == proof
    for name in driver.qualification.TERMINAL_IDENTITIES:
        assert terminal[name] == proof[name]
    assert proof['code_identity_sha256'] == sha(encoded(proof['code_sha256']))
    assert receipt['qualified'] is receipt['command_started'] is receipt['command_completed'] is receipt['source_unchanged'] is True
    assert receipt['exit_status'] == receipt['gate_status'] == 0
    assert receipt['source_sha256'] == source and receipt['qualification_sha256'] == files['native/source-qualification.json']['sha256']
    for key in ('config_sha256','code_identity_sha256','campaign_schema','artifact_roster_sha256'):
        assert receipt[key] == proof[key]
    with driver.qualification.execution_mode(fixed48=True):
        names = set(driver.qualification.ARTIFACTS)
        assert set(terminal['artifacts']) == names
        assert receipt['command'] == proof['command'] == driver.qualification.FIXED['command']
        assert receipt['environment'] == proof['environment'] == driver.qualification.FIXED['environment']
        assert set(receipt['artifacts']) == names-{'workspace-receipt.json','run-closed.log'}
        for n, pin in terminal['artifacts'].items():
            assert pin == identity(files['native/'+n])
        for n, pin in receipt['artifacts'].items():
            assert pin == identity(files['native/'+n])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'test.log'; path.write_bytes(read(repo, files['native/test.log']))
            assert receipt['stages'] == driver.qualification.validate_bounded_publication_stages(path, fixed48=True)
        driver.qualification.worker.validate_cgroup(get('workspace-cgroup.json'))
    assert root['focused_tests_passed'] == sum(s['tests_run'] for s in receipt['stages'][:4])
    assert get('collection-replay.json')['result']['qualified'] is True
    return proof


def historical_inputs(repo, config):
    run = config['cold_run']
    assert set(run) == {'directory','files'} and run['directory'] == str(COLD)
    files = run['files']; assert set(files) == set(COLD_ROSTER), 'full historical roster'
    for n, p in files.items():
        assert p['path'] == str(COLD/n), 'historical path binding'
    bridge = json.loads(read(repo, config['cold_source_authority']))
    assert set(bridge) == BRIDGE_KEYS and bridge['schema'] == 'borsuk-fixed48-offered-cold-source-authority-v1'
    assert bridge['whole_body_verification'] is True and type(bridge['original_controller_exit_status']) is int and bridge['original_controller_exit_status'] == 0
    assert type(bridge['source_file_count']) is int and bridge['source_file_count'] == 399
    assert bridge['terminal'] == {k:files['aws-terminal.json'][k] for k in ('path','bytes','sha256')}
    assert bridge['root_audit'] == {k:files['root-audit.json'][k] for k in ('path','bytes','sha256')}
    terminal, audit, closeout, collection = (json.loads(read(repo, files[n])) for n in
        ('aws-terminal.json','root-audit.json','aws-closeout.json','collection-receipt.json'))
    assert terminal['phase'] == terminal['status'] == 'complete'
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    assert terminal['exit_code'] == terminal['original_exit_code'] == 0
    assert closeout['state'] == collection['state'] == audit['independently_observed_instance_state'] == 'terminated'
    assert collection['whole_body_verification'] is collection['complete'] is True
    assert audit['authenticated_artifact_count'] == 56 and audit['all_declared_artifacts_authenticated'] is True
    assert audit['closed_validator_passed'] is audit['execution_gate_passed'] is audit['quality_gate_passed'] is audit['cleanup_proven'] is True
    assert audit['original_controller_exit_status'] == 0
    assert terminal['instance_id'] == collection['instance_id'] == audit['instance_id']
    assert terminal['instance_id'] in {n['instance_id'] for n in closeout['nodes'].values()}
    assert set(terminal['artifacts']) == set(cold_spot.ARTIFACTS)
    assert bridge['authenticated_artifacts'] == terminal['artifacts'] == {n:identity(files[n]) for n in cold_spot.ARTIFACTS}
    for n, p in files.items():
        if n not in UNUSED or (repo/p.get('archived_path',p['path'])).exists():
            read(repo, p, materialize=False)
    old, control, proof = (json.loads(read(repo, files[n])) for n in
        ('config.json','controller-config.json','source-qualification.json'))
    source = dict(commit=terminal['source_archive_commit'], archive_sha256=terminal['source_archive_sha256'])
    assert bridge['execution_source'] == old['execution_source'] == control['execution_source'] == source
    assert audit['source_commit'] == collection['source_commit'] == source['commit'] == terminal['source_commit']
    assert audit['source_archive_sha256'] == collection['source_archive_sha256'] == source['archive_sha256']
    assert bridge['runtime_code_sha256'] == old['code_sha256']
    assert bridge['controller_code_sha256'] == control['code_sha256']
    assert terminal['runtime_code_identity_sha256'] == proof['runtime_code_identity_sha256'] == sha(encoded(old['code_sha256']))
    assert terminal['code_identity_sha256'] == proof['code_identity_sha256'] == sha(encoded(control['code_sha256']))
    for key in cold_spot.TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'original proof/terminal identity'
    assert terminal['config_sha256'] == files['config.json']['sha256'] == files['screen/config.json']['sha256']
    assert audit['runtime_config'] == identity(files['config.json'])
    assert audit['controller_config'] == identity(files['controller-config.json'])
    assert terminal['controller_config_sha256'] == files['controller-config.json']['sha256']
    assert terminal['asset_manifest_sha256'] == files['asset-manifest.json']['sha256']
    assert json.loads(read(repo, files['asset-manifest.json']))['assets'] == proof['assets'] == terminal['assets']
    assert proof['proofs_identity_sha256'] == sha(encoded(old['proofs']))
    assert proof['assets_identity_sha256'] == sha(encoded(proof['assets']))
    assert proof['artifact_roster_sha256'] == sha(encoded(cold_spot.ARTIFACTS))
    assert all(encoded(old[k]) == encoded(v) for k,v in fixed.FIXED.items())
    assert old['namespace_prefix'] == config['namespace_prefix'] and old['bucket'] == config['bucket'] == science.BUCKET
    assert json.loads(read(repo, files['screen/source-qualification.json']))['current_executor_sha256'] == old['code_sha256']
    fixed.historical_assets(repo, fixed.read_proofs(repo, old['proofs']))
    native = native_authority(repo, files, bridge)
    binary = files['native/binaries/two_bit_http']
    assert binary['sha256'] == proof['binary_sha256']
    assert identity(binary) == library.identity(proof['assets']['qualification/binaries/two_bit_http'])
    panel = repo/fixed.SCIENTIFIC/'screen'
    for name in ('requests.jsonl','records.jsonl','truth.i64'):
        assert artifact(panel/name) == library.identity(proof['assets']['panel/'+name]), 'sealed panel body'
    derived, refs, truths = fixed.panel_inputs(panel)
    assert read(repo, files['screen/publisher-requests.jsonl']) == derived
    expected = [dict(r, ids=r['ids'][:10]) for r in refs]
    assert read(repo, files['screen/sealed-reference-k10.jsonl']) == b''.join(encoded(r)+b'\n' for r in expected)
    publication = json.loads(read(repo, files['screen/publication.json']))
    assert publication['retained_for_offered_gate'] is publication['publication_via_production_library'] is True
    assert publication['process']['exit_status'] == 0 and publication['process']['process_cleanup'] is True
    root = json.loads(read(repo, files['screen/original-generation-root.json']))
    assert identity(files['screen/original-generation-root.json']) == library.identity(proof['assets']['generation/manifest.json'])
    sq8 = publication['sq8']; assert sq8['etag'] and sq8['bytes'] == 780000000
    assert sq8['sha256'] == proof['assets']['sq8.bin']['sha256'] and sq8['key'] == config['namespace_prefix']+'/objects/'+sq8['sha256']
    serving = fixed.transport_manifest(root, config['namespace_prefix'], sq8['etag'])
    assert publication['manifest'] == serving
    arm = publication['arm']; opts = dict(native_memory_bytes=LIMITS['native_memory_bytes'],
        budget_model=fixed.native_budget_model(arm['metadata_files'], retained_root=True))
    library.validate_roster(arm, **opts)
    assert arm['indexes'] == {'10':config['namespace_prefix']}
    assert arm['authority'] == dict(root_sha256=sha(encoded(serving)+b'\n'), generation=1, control_epoch=1)
    body = base64.b64decode(publication['head_body_base64'], validate=True)
    assert json.loads(body) == dict(schema='borsuk-two-bit-head-v2',generation=1,epoch=1,
        root_sha256=arm['authority']['root_sha256'],mutation=None,fence=None)
    assert arm['head_file'] == dict(bytes=len(body),sha256=sha(body))
    for n in library.STARTUP:
        pin = dict(bytes=len(encoded(serving)+b'\n'),sha256=sha(encoded(serving)+b'\n')) if n == 'manifest.json' else library.identity(proof['assets']['generation/'+n])
        assert arm['metadata_files'][n] == pin['bytes'] and arm['metadata_sha256'][n] == pin['sha256']
    assert arm['leaf_object'] == library.identity(proof['assets']['generation/router/leaves.bin'])
    assert publication['validation'] == library.publication_reference(repo/files['screen/publication-reference.jsonl']['path'], arm, refs)
    marker = json.loads(read(repo, files['screen/COMPLETE.json']))
    assert marker['files'] == {n:identity(files['screen/'+n]) for n in cold_spot.RUNTIME_FILES}
    assert marker['schema'] == fixed.SCHEMA+'-complete' and marker['config_sha256'] == files['config.json']['sha256']
    assert marker['passed'] is True and marker['status'] == 'PASS'
    evidence = dict(arm=arm, requests=[library.http_request(json.loads(row)['query'],arm['authority']) for row in derived.splitlines()],
        references=expected, truth=truths, binary=binary, publication=publication,
        identities=dict(panel={n:library.identity(proof['assets']['panel/'+n]) for n in ('requests.jsonl','records.jsonl','truth.i64')},
            publication=identity(files['screen/publication.json']), binary=identity(binary),
            head=arm['head_file'], authority=arm['authority'], sq8=sq8, bridge=config['cold_source_authority']))
    rows = [json.loads(line) for line in read(repo, files['screen/records.jsonl']).splitlines()]
    assert len(rows) == 64
    for q, row in enumerate(rows):
        validate_success(row,evidence,q,old)
        fixed.check_cgroup(row['cgroup_before'],row['cgroup_after'],old['resources'],drained=True)
    summary = json.loads(read(repo,files['screen/summary.json']))
    replayed = fixed.reduce_records(rows,0,summary['serial_full_span_ns'])
    assert replayed == summary and summary['returned_hits10'] == 619 and summary['quality_gate_passed'] is True
    for n in ('screen/cleanup.json','cold-closure.json'):
        value = json.loads(read(repo,files[n])); assert value['process_cleanup'] is True
    assert json.loads(read(repo,files['screen/resources.json']))['cgroup']['closed'] is True
    return evidence


def qualify(config_path, expected_sha, repo):
    repo = Path(repo).resolve(); path = fixed.retained.regular_path(config_path)
    assert path.is_file() and 0 < path.stat().st_size <= 1 << 20
    body = path.read_bytes(); assert sha(body) == expected_sha, 'config identity'
    config = json.loads(body)
    assert set(config) == set(FIXED)|{'bucket','namespace_prefix','code_sha256','execution_source',
        'cold_run','cold_source_authority','prices','resources'}
    assert all(encoded(config[k]) == encoded(v) for k,v in FIXED.items()), 'root freeze pending or protocol changed'
    assert config['resources'] == LIMITS and all(type(v) is int for v in config['resources'].values())
    assert set(config['code_sha256']) == set(CODE), 'exact executor CODE closure'
    assert all(artifact(library.panel.repo_path(repo,n))['sha256'] == d for n,d in config['code_sha256'].items()), 'executor code drift'
    for module in (fixed, cold_spot, offered, library):
        assert Path(module.__file__).resolve().is_relative_to(repo), 'import origin outside source archive'
    assert Path(__file__).resolve() == repo/OWN, 'executor origin'
    source = config['execution_source']; assert set(source) == {'commit','archive_sha256'}
    assert library.panel.re.fullmatch('[0-9a-f]{40}',source['commit']); stats.digest(source['archive_sha256'])
    prices = json.loads(read(repo,config['prices'])); assert type(prices) is dict and prices, 'root price provenance'
    evidence = historical_inputs(repo,config)
    proof = dict(schema=SCHEMA+'-qualification',config_sha256=expected_sha,execution_source=source,
        code_identity_sha256=sha(encoded(config['code_sha256'])), code_sha256=config['code_sha256'],
        refs_identity_sha256=sha(encoded({k:config[k] for k in ('cold_run','cold_source_authority','prices')})),
        binary_sha256=evidence['binary']['sha256'], original_source_authority=config['cold_source_authority'],
        native_source_identity_sha256=driver.SOURCE_ID, native_source_file_count=399,
        native_rebuilt=False, build_invocations=0, publication_invocations=0, scientific_scorer_invocations=0, oracle_invocations=0)
    return config, proof, evidence


def validate_success(row, evidence, q, config):
    arm = evidence['arm']; opts = dict(native_memory_bytes=config['resources']['native_memory_bytes'],
        budget_model=fixed.native_budget_model(arm['metadata_files'],retained_root=True))
    assert row['query_ordinal'] == q and type(row['query_ordinal']) is int
    assert row['outcome'] == 'success' and row['http_attempts'] == row['valid_ann_requests'] == 1 and row['http_status'] == 200
    assert row['http_retry'] is False and row['native_process_started'] is True
    body = evidence['requests'][q]
    assert row['request_sha256'] == sha(body) and row['request_bytes'] == len(body) and row['expected_authority'] == arm['authority']
    assert row['response'] == json.loads(base64.b64decode(row['raw_response_base64'],validate=True))
    assert row['returned_hits'] == fixed.validate_query(row['response'],arm,evidence['references'][q],evidence['truth'][q])
    headers = [json.loads(line) for line in row['native_server_log'].splitlines() if line.startswith('{')]
    assert headers == [row['native_header']]
    if 'port' in row:
        assert row['native_header']['listen'] == f"127.0.0.1:{row['port']}"
    assert row['accounting'] == library.transport(row['native_header'],row['response'],arm,wave_objects=8,root_reuse=True,**opts)
    assert row['resources'] == fixed.telemetry.resources(row['native_time_log'],config['resources']['native_memory_bytes'])
    assert row['native_close']['intentional_stop'] is row['native_close']['process_group_closed'] is True
    assert type(row['native_close']['returncode']) is int and row['temporary_directory_cleanup'] is True
    assert row['started_ns'] <= row['successful_connect_attempt_ns'] <= row['connected_ns'] <= row['completed_ns'] <= row['terminal_ns']
    assert row['first_wire_completed_ns'] == row['completed_ns']
    assert row['cold_start_to_first_http_response_ns'] >= row['native_header']['remote_open_wall_ns']+row['native_header']['head_read_wall_ns']
    for key, start, end in (('cold_start_to_first_http_response_ns','started_ns','completed_ns'),
        ('before_successful_connect_attempt_ns','started_ns','successful_connect_attempt_ns'),
        ('successful_tcp_connect_ns','successful_connect_attempt_ns','connected_ns'),
        ('first_post_to_response_ns','connected_ns','completed_ns'),
        ('incoming_http_wall_ns','successful_connect_attempt_ns','completed_ns')):
        assert row[key] == row[end]-row[start]
    cpu = row['query_cpu']; before, after = cpu['before'],cpu['after']
    assert (before['pid'],before['start_ticks']) == (after['pid'],after['start_ticks'])
    assert all(c['address_space_limit_bytes'] == 4 << 30 and c['cpu_affinity'] == [0,1,2,3] for c in (before,after))
    assert cpu['ticks'] == sum(after[n]-before[n] for n in ('user_ticks','system_ticks')) >= 0
    stats.integer(cpu['ticks_per_second'],'CPU resolution',1)
    fixed.check_cgroup(row['cgroup_before'],row['cgroup_after'],config['resources'],drained=False)


def transport_failure(row):
    kind = getattr(http.client,row.get('error_type',''),getattr(builtins,row.get('error_type',''),None))
    return ('resource_error' not in row and (not row.get('native_process_started') or row.get('native_close',{}).get('intentional_stop') is True) and (
        isinstance(kind,type) and issubclass(kind,(ConnectionError,TimeoutError,http.client.HTTPException)) or
        row.get('error') == 'first and only ANN request failed; no HTTP retry'))


def call_one(binary, config, evidence, q, port, scratch):
    started = time.monotonic_ns()
    row = fixed.measured_call(binary,config,evidence['arm'],evidence['requests'][q],
        evidence['references'][q],evidence['truth'][q],scratch,port=port,require_cgroup_drained=False)
    row['namespace_start_attempted'] = True
    row['cleanup_confirmed'] = (row.get('temporary_directory_cleanup') is True and
        (not row.get('native_process_started') or row.get('native_close',{}).get('process_group_closed') is True))
    if row['outcome'] == 'success':
        row.update(failure_kind=None,abort_admissions=False)
    else:
        row.setdefault('error',row.get('error_type','failed call'))
        row['failure_kind'] = 'transport' if transport_failure(row) and row['cleanup_confirmed'] else 'fatal'
        row['abort_admissions'] = row['failure_kind'] == 'fatal'
    row['driver_call_span_ns'] = time.monotonic_ns()-started
    row['driver_before_native_start_ns'] = max(0,row.get('started_ns',started)-started)
    row['driver_after_wire_ns'] = max(0,row['terminal_ns']-row.get('first_wire_completed_ns',row['terminal_ns']))
    return row


def aborted_rows(index, epoch, reason):
    return [dict(query_ordinal=q,rate_index=index,dataset='CoHere',offered_qps=RATES[index],
        scheduled_ns=epoch+round(q*1e9/RATES[index]),dispatched_ns=None,started_ns=None,completed_ns=None,
        port=None,outcome='aborted',abort_after=reason,terminal_ns=epoch,
        native_process_started=False,namespace_start_attempted=False,http_attempts=0,valid_ann_requests=0) for q in range(64)]


def reduce_cell(rows, evidence, config):
    assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64)), 'all64 ledger'
    receipt = rows[0]['cell_receipt']; epoch, terminal = receipt['epoch_ns'],receipt['terminal_ns']
    assert type(epoch) is type(terminal) is int and terminal > epoch
    assert receipt['admission_deadline_ns'] == receipt['worker_started_ns']+(config['worker_limit_seconds']-config['cleanup_reserve_seconds'])*10**9
    index = rows[0]['rate_index']; stats.integer(index,'cell',0,5)
    assert type(receipt['cell_started']) is bool
    for before in ('campaign_cgroup_before','cgroup_before'):
        fixed.check_cgroup(receipt[before],receipt['cgroup_after'],config['resources'],drained=True)
    assert receipt['resource_errors'] == [], 'observer/resource failure'
    intervals, delays, fatal = [], [], False
    normalized = []
    for q, row in enumerate(rows):
        assert row['rate_index'] == index and row['offered_qps'] == RATES[index] and row['dataset'] == 'CoHere'
        assert row['scheduled_ns'] == epoch+round(q*1e9/RATES[index])
        assert epoch <= row['terminal_ns'] <= terminal
        outcome, port = row['outcome'],row['port']
        assert outcome in ('success','failed','capacity_drop','aborted')
        if row['dispatched_ns'] is not None:
            assert receipt['cell_started'] and row['scheduled_ns'] <= row['dispatched_ns'] <= row['terminal_ns']
            assert row['dispatched_ns'] < receipt['admission_deadline_ns']
            delays.append(row['dispatched_ns']-row['scheduled_ns'])
        if port is None:
            assert outcome in ('capacity_drop','aborted')
            assert row['native_process_started'] is row['namespace_start_attempted'] is False
            assert row['http_attempts'] == row['valid_ann_requests'] == 0
            assert row['started_ns'] is row['completed_ns'] is None
            assert not any(n in row for n in ('response','native_close','raw_response_base64'))
            if outcome == 'capacity_drop':
                assert row['terminal_ns'] == row['dispatched_ns']
            else:
                assert row['dispatched_ns'] is None and row['abort_after'] == receipt['abort_after']
        else:
            stats.integer(port,'owner port',18080,18084)
            assert outcome in ('success','failed') and row['dispatched_ns'] is not None
            assert row['cleanup_confirmed'] is True, 'owned process/scratch closure missing'
            assert row['cleanup_confirmed'] == (row.get('temporary_directory_cleanup') is True and
                (not row['native_process_started'] or row.get('native_close',{}).get('process_group_closed') is True))
            assert row['request_sha256'] == sha(evidence['requests'][q]) and row['request_bytes'] == len(evidence['requests'][q])
            assert row['expected_authority'] == evidence['arm']['authority'] and row['http_retry'] is False
            if row['started_ns'] is not None:
                assert row['dispatched_ns'] <= row['started_ns'] <= row['terminal_ns']
            if receipt['abort_after'] is not None:
                assert row['dispatched_ns'] <= receipt['abort_after']['observed_ns'], 'admission after fatal abort'
            intervals.append((row['dispatched_ns'],row['terminal_ns'],port))
            if outcome == 'success':
                validate_success(row,evidence,q,config)
                assert row['failure_kind'] is None and row['abort_admissions'] is False
            else:
                assert row['error_type'] and row['error']
                kind = 'transport' if transport_failure(row) else 'fatal'
                assert row['failure_kind'] == kind and row['abort_admissions'] is (kind == 'fatal')
                fatal |= kind == 'fatal'
                assert type(row['http_attempts']) is int and row['http_attempts'] in (0,1)
                if row['native_process_started']:
                    assert row['resources'] == fixed.telemetry.resources(row['native_time_log'],config['resources']['native_memory_bytes'])
                    assert row['native_close']['process_group_closed'] is True
                    assert row['native_close']['intentional_stop'] is True or kind == 'fatal'
                    headers = [json.loads(line) for line in row.get('native_server_log','').splitlines() if line.startswith('{')]
                    if headers:
                        assert len(headers) == 1
                        header = headers[0]
                        assert header['phase'] == 'ready' and header['authority'] == evidence['arm']['authority'] and header['listen'] == f'127.0.0.1:{port}'
                        library.validate_startup(header['remote_open_stats'],evidence['arm'],header['remote_open_wall_ns'],
                            wave_objects=8,root_reuse=True,native_memory_bytes=LIMITS['native_memory_bytes'],
                            budget_model=fixed.native_budget_model(evidence['arm']['metadata_files'],retained_root=True))
                if 'cgroup_before' in row:
                    fixed.check_cgroup(row['cgroup_before'],row['cgroup_after'],config['resources'],drained=False)
        cloned = dict(row)
        if outcome == 'success': cloned['transfer_accounting'] = row['accounting']['metadata']
        if outcome == 'aborted': cloned['outcome'] = 'arm_aborted'
        normalized.append(cloned)
    active, peak = [], 0
    for start,end,port in sorted(intervals):
        active = [(s,e,p) for s,e,p in active if e > start]
        assert all(p != port for _,_,p in active), 'owner reused before cleanup'
        active.append((start,end,port)); peak = max(peak,len(active)); assert peak <= 5
    for r in rows:
        if r['outcome'] == 'capacity_drop':
            assert sum(s <= r['dispatched_ns'] < e for s,e,_ in intervals) == 5, 'drop without five owners'
    abort = receipt['abort_after']
    if receipt['cell_started'] and abort is not None:
        assert epoch <= abort['observed_ns'] <= terminal
        if abort['reason'] == 'admission deadline':
            assert abort['observed_ns'] >= receipt['admission_deadline_ns']
        else:
            origin = rows[abort['query_ordinal']]
            assert origin['abort_admissions'] is True and origin['terminal_ns'] == abort['observed_ns']; fatal = True
    result = offered.reduce_cell(normalized,RATES[index],epoch,terminal,abort is not None)
    timing = all(d <= config['max_dispatch_lateness_ns'] for d in delays)
    attained = receipt['cell_started'] and result['all_offers_successful'] and result['quality_gate_passed'] and timing
    result.update(schema=SCHEMA+'-cell',rate_index=index,dataset='CoHere',cell_started=receipt['cell_started'],
        attainment=attained,dispatch_timing_gate_passed=timing,execution_gate_passed=not fatal,
        status='INVALID' if fatal else 'PASS' if attained else 'FAIL',closed=True,cleanup_confirmed=True,
        nominal_offered_qps=RATES[index],all_offer_recall_at_10=result['returned_hits']/640,
        peak_port_ownership=peak,all_offer_scheduled_to_response_ms=fixed.telemetry.all_offer_tails(rows),
        success_conditioned_process_transport_totals={k:sum(r['accounting']['final_process_transport'][k] for r in rows if r['outcome']=='success')
            for k in ('attempts','consumed_payload_bytes','transport_failures','stream_failures')},
        physical_wire_bytes='UNMEASURED',sustainable_qps='UNMEASURED',matched_vendor_comparison=False)
    if not receipt['cell_started']:
        for k in ('full_span_ns','successful_full_span_qps','accepted_completed_full_span_qps'):
            result[k] = 'UNMEASURED'
    return result


def sync_directory(path):
    fd = os.open(path,os.O_RDONLY|os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def close_cell(output, rows, result, proof, on_cell_closed=None):
    index = rows[0]['rate_index']; path = output/f'rate{index}-records.jsonl'
    write(path,b''.join(encoded(r)+b'\n' for r in rows))
    marker = dict(result,records=artifact(path),config_sha256=proof['config_sha256'],
        code_identity_sha256=proof['code_identity_sha256'],refs_identity_sha256=proof['refs_identity_sha256'],
        binary_sha256=proof['binary_sha256'])
    target = output/f'rate{index}-summary.json'; write(target,marker); sync_directory(output)
    if on_cell_closed is not None and marker['closed'] and marker['cleanup_confirmed'] and marker['execution_gate_passed']:
        on_cell_closed(marker,dict(records=path,summary=target))
    return marker


def scratch_usage(root, *, readonly=False):
    """Scan only owned mutable output during dispatch; reject links and special files."""
    seen, used, allocated = set(),0,0
    for base,dirs,names in os.walk(root,followlinks=False):
        for name in [None,*dirs,*names]:
            path = Path(base) if name is None else Path(base)/name
            try: value = path.lstat()
            except FileNotFoundError: continue  # An owner removed its own temporary file.
            assert stat.S_ISDIR(value.st_mode) or stat.S_ISREG(value.st_mode), 'unsafe scratch entry: '+str(path)
            if readonly: assert value.st_mode & 0o222 == 0, 'source archive must be readonly'
            if stat.S_ISREG(value.st_mode) and (value.st_dev,value.st_ino) not in seen:
                seen.add((value.st_dev,value.st_ino)); used += value.st_size; allocated += value.st_blocks*512
    return dict(unique_inode_bytes=used,physical_allocated_bytes=allocated)


def observe_once(output, config, baseline, report, deadline):
    work = time.monotonic_ns(); limits = config['resources']; now = fixed.snapshot()
    fixed.check_cgroup(baseline,now,limits)
    assert time.monotonic_ns() < deadline, 'worker deadline'
    usage = scratch_usage(output)
    assert max(usage.values())+limits['static_repository_reserve_bytes']+limits['output_reserve_bytes'] <= limits['scratch_bytes'], 'whole scratch bound'
    assert shutil.disk_usage(output).free >= limits['output_reserve_bytes'], 'output headroom'
    memory = dict(line.split() for line in now['memory.stat'].splitlines())
    rss = {}
    for pid in now['process_ids']:
        try:
            fields = Path(f'/proc/{pid}/status').read_text().splitlines()
            value = next((int(l.split()[1])*1024 for l in fields if l.startswith('VmRSS:')),None)
            if value is not None: rss[str(pid)] = value
        except FileNotFoundError: pass
    sample = dict(at_ns=time.monotonic_ns(),shared_current_bytes=int(now['memory.current']),
        anon_bytes=int(memory['anon']),file_cache_bytes=int(memory['file']),process_rss_bytes=rss,
        scratch_usage=usage,cgroup_cpu_stat=now['cpu.stat'],cgroup_io_stat=now['io.stat'])
    sample['observer_work_ns'] = time.monotonic_ns()-work
    report['samples'].append(sample)
    report['observer_work_ns'] += sample['observer_work_ns']
    report['shared_current_peak_bytes'] = max(report['shared_current_peak_bytes'],sample['shared_current_bytes'])
    assert 0 <= sample['shared_current_bytes'] <= limits['shared_memory_bytes']
    assert sample['anon_bytes'] >= 0 and sample['file_cache_bytes'] >= 0


def campaign_summary(cells):
    execution = len(cells) == 6 and all(c['execution_gate_passed'] for c in cells)
    attained = execution and all(c['attainment'] for c in cells)
    return dict(schema=SCHEMA+'-result',closed=True,planned_positions=384,status='INVALID' if not execution else 'PASS' if attained else 'FAIL',
        execution_gate_passed=execution,offered_gate_passed=attained,closed_cells=cells,
        highest_attained_nominal_qps=max((c['offered_qps'] for c in cells if c['attainment']),default=None),
        sustainable_qps='UNMEASURED',physical_wire_bytes='UNMEASURED',total_lifecycle_billing_usd='UNMEASURED',matched_vendor_comparison=False)


def main(config_path, expected_sha, repo, output, *, on_cell_closed=None):
    started = time.monotonic_ns(); repo = Path(repo).resolve(); output = Path(output).absolute()
    config, proof, evidence = qualify(config_path,expected_sha,repo)
    assert not output.exists() and output == output.resolve() and not output.is_relative_to(repo), 'fresh external output required'
    for key,env in (('commit','BORSUK_OFFERED_SOURCE_COMMIT'),('archive_sha256','BORSUK_OFFERED_ARCHIVE_SHA256')):
        assert os.environ.get(env) == config['execution_source'][key], 'new execution archive binding'
    assert os.sched_getaffinity(0) == {4,5}, 'client CPU affinity'
    assert all(os.environ.get(n) == '2' for n in fixed.retained.THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS') == '1'
    static = scratch_usage(repo,readonly=True)
    assert max(static.values()) <= LIMITS['static_repository_reserve_bytes'], 'static archive bound'
    baseline = fixed.snapshot(); fixed.check_cgroup(baseline,baseline,LIMITS,drained=True)
    output.mkdir(parents=True); scratch = output/'scratch'; (scratch/'native').mkdir(parents=True)
    records,cells,errors = [],[],[]
    report = dict(samples=[],observer_work_ns=0,shared_current_peak_bytes=0,static_repository=static,
        static_exclusion='readonly authenticated source archive; fixed fail-closed reservation; final reauthentication',
        independent_process_maxima_are_not_simultaneous_rss=True)
    stopped,terminated = threading.Event(),threading.Event()
    deadline = started+(config['worker_limit_seconds']-config['cleanup_reserve_seconds'])*10**9
    final_deadline = started+config['worker_limit_seconds']*10**9
    counters = dict(before=baseline,closed=False); failure = None; stop_after = None; summary = None
    def signal_handler(signum,frame): terminated.set()  # Do not unwind a live owner thread.
    previous = {s:signal.signal(s,signal_handler) for s in (signal.SIGTERM,signal.SIGINT)}
    def observer():
        while not stopped.wait(.25):
            try: observe_once(output,config,baseline,report,final_deadline)
            except BaseException as e:
                errors.append(dict(type=type(e).__name__,message=str(e))); return
    monitor = threading.Thread(target=observer,daemon=True)
    def receipt(rows,index,epoch,terminal,abort,before,cell_started):
        for r in rows: r.update(rate_index=index,dataset='CoHere')
        rows[0]['cell_receipt'] = dict(epoch_ns=epoch,terminal_ns=terminal,abort_after=abort,cell_started=cell_started,
            worker_started_ns=started,admission_deadline_ns=deadline,campaign_cgroup_before=baseline,
            cgroup_before=before,cgroup_after=fixed.snapshot(),resource_errors=list(errors))
    def invalid(error,drained):
        return dict(schema=SCHEMA+'-cell',rate_index=len(cells),offered_qps=RATES[len(cells)],
            attainment=False,execution_gate_passed=False,status='INVALID',closed=True,cleanup_confirmed=drained,
            error_type=type(error).__name__,error=str(error))
    try:
        write(output/'config.json',Path(config_path).read_bytes()); write(output/'source-qualification.json',proof)
        write(output/'input-hashes.json',evidence['identities'])
        binary = scratch/'http'; write(binary,read(repo,evidence['binary'])); binary.chmod(0o500)
        assert artifact(binary) == identity(evidence['binary'])
        observe_once(output,config,baseline,report,final_deadline); monitor.start()
        for index,rate in enumerate(RATES):
            before = fixed.snapshot(); rows = None; cell_started = stop_after is None
            try:
                if cell_started:
                    assert artifact(binary) == identity(evidence['binary']), 'binary drift'
                    def call(q,port):
                        if errors or terminated.is_set():
                            return dict(outcome='failed',failure_kind='fatal',abort_admissions=True,
                                cleanup_confirmed=True,temporary_directory_cleanup=True,native_process_started=False,
                                error_type='AssertionError',error='observer or termination failure',namespace_start_attempted=False,
                                http_retry=False,expected_authority=evidence['arm']['authority'],
                                request_sha256=sha(evidence['requests'][q]),request_bytes=len(evidence['requests'][q]))
                        return call_one(binary,config,evidence,q,port,scratch)
                    rows,epoch,terminal,abort = offered.schedule_offers(call,rate,workers=5,base_port=18080,deadline_ns=deadline)
                else:
                    epoch = time.monotonic_ns(); rows = aborted_rows(index,epoch,stop_after)
                    terminal,abort = max(time.monotonic_ns(),epoch+1),stop_after
                receipt(rows,index,epoch,terminal,abort,before,cell_started)
                records.extend(rows)
                result = reduce_cell(rows,evidence,config)
            except BaseException as error:
                failure = failure or error
                if rows is None:
                    epoch = time.monotonic_ns(); abort = dict(reason='fatal '+type(error).__name__,observed_ns=epoch)
                    rows = aborted_rows(index,epoch,abort); receipt(rows,index,epoch,max(time.monotonic_ns(),epoch+1),abort,before,False)
                    records.extend(rows)
                drained = True
                try: fixed.check_cgroup(baseline,fixed.snapshot(),LIMITS,drained=True)
                except BaseException: drained = False
                result = invalid(error,drained)
            # Scheduler rows already survived even if the reducer or callback fails.
            marker = close_cell(output,rows,result,proof,on_cell_closed); cells.append(marker)
            if not result['attainment'] and stop_after is None:
                stop_after = dict(reason='rate attainment failed',rate_index=index,observed_ns=time.monotonic_ns())
        summary = campaign_summary(cells)
    except BaseException as error:
        failure = failure or error
    finally:
        stopped.set()
        if monitor.ident is not None: monitor.join(timeout=5)
        cleanup_error = None
        try:
            assert not monitor.is_alive(), 'observer remains'
            assert all(not r.get('native_process_started') or r.get('native_close',{}).get('process_group_closed') is True for r in records), 'owned PGID remains'
            shutil.rmtree(scratch); assert not scratch.exists()
            counters.update(after=fixed.snapshot(),closed=True)
            fixed.check_cgroup(baseline,counters['after'],LIMITS,drained=True)
            assert not errors and not terminated.is_set(), 'observer/termination failure'
            qualify(config_path,expected_sha,repo)  # Full checks outside every dispatch interval.
            assert scratch_usage(repo,readonly=True) == static, 'readonly archive changed'
            assert time.monotonic_ns() <= final_deadline, 'worker final deadline'
        except BaseException as error:
            cleanup_error = error; failure = failure or error
        # Fill every declared position on all exception paths without replacing completed rows.
        for index in range(len(records)//64,6):
            epoch = time.monotonic_ns(); reason = dict(reason='fatal campaign exception',observed_ns=epoch)
            rows = aborted_rows(index,epoch,reason); receipt(rows,index,epoch,max(time.monotonic_ns(),epoch+1),reason,baseline,False)
            records.extend(rows)
        for index in range(6):
            target = output/f'rate{index}-summary.json'
            if not target.exists():
                rows = records[index*64:(index+1)*64]
                result = dict(schema=SCHEMA+'-cell',rate_index=index,offered_qps=RATES[index],attainment=False,
                    execution_gate_passed=False,status='INVALID',closed=True,cleanup_confirmed=cleanup_error is None)
                record_path = output/f'rate{index}-records.jsonl'
                if record_path.exists():
                    assert record_path.read_bytes() == b''.join(encoded(r)+b'\n' for r in rows)
                    marker = dict(result,records=artifact(record_path),config_sha256=expected_sha,
                        code_identity_sha256=proof['code_identity_sha256'],refs_identity_sha256=proof['refs_identity_sha256'],binary_sha256=proof['binary_sha256'])
                    write(target,marker); sync_directory(output)
                else:
                    close_cell(output,rows,result,proof)
        cells = [json.loads((output/f'rate{i}-summary.json').read_bytes()) for i in range(6)]
        summary = campaign_summary(cells)
        if failure or cleanup_error:
            summary.update(status='INVALID',execution_gate_passed=False,offered_gate_passed=False,
                error_type=type(failure).__name__,error=str(failure))
        assert len(records) == 384
        write(output/'records.jsonl',b''.join(encoded(r)+b'\n' for r in records))
        write(output/'failures.jsonl',b''.join(encoded(r)+b'\n' for r in records if r['outcome']=='failed'))
        write(output/'offered-cgroup.json',counters)
        report.update(wall_seconds=(time.monotonic_ns()-started)/1e9,process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
            driver_call_span_ns=sum(r.get('driver_call_span_ns',0) for r in records),
            driver_before_native_start_ns=sum(r.get('driver_before_native_start_ns',0) for r in records),
            driver_after_wire_ns=sum(r.get('driver_after_wire_ns',0) for r in records),
            driver_cpu_user_seconds=resource.getrusage(resource.RUSAGE_SELF).ru_utime,
            driver_cpu_system_seconds=resource.getrusage(resource.RUSAGE_SELF).ru_stime,
            resource_errors=errors,native_invocations=sum(r['native_process_started'] for r in records),
            http_attempts=sum(r['http_attempts'] for r in records),native_budget_model=fixed.native_budget_model(evidence['arm']['metadata_files'],retained_root=True),
            native_memory_bytes=2<<30,physical_wire_bytes='UNMEASURED',billing='UNMEASURED',prices=config['prices'],
            build_invocations=0,publication_invocations=0,scientific_scorer_invocations=0,oracle_invocations=0)
        write(output/'resources.json',report)
        cleanup = dict(valid=cleanup_error is None,scratch_removed=not scratch.exists(),observer_stopped=not monitor.is_alive(),
            process_cleanup=cleanup_error is None,remote_namespace_retained=config['namespace_prefix'],publication_invocations=0)
        write(output/'cleanup.json',cleanup)
        write(output/'failure.json',dict(status='failed' if failure else 'complete',error_type=type(failure).__name__ if failure else None,error=str(failure) if failure else None))
        usage = scratch_usage(output)
        if max(usage.values())+LIMITS['static_repository_reserve_bytes']+LIMITS['output_reserve_bytes'] > LIMITS['scratch_bytes']:
            failure = failure or AssertionError('final scratch admission'); summary.update(status='INVALID',execution_gate_passed=False,offered_gate_passed=False)
        write(output/'summary.json',summary); sync_directory(output)
        exit_code = 0 if summary['execution_gate_passed'] else 1
        write(output/'terminal.json',dict(schema=SCHEMA+'-terminal',phase='complete' if exit_code == 0 else 'failed',
            status=summary['status'],exit_code=exit_code,config_sha256=expected_sha,execution_source=config['execution_source'],
            files={n:artifact(output/n) for n in OUTPUTS if n != 'terminal.json'})); sync_directory(output)
        for s,handler in previous.items(): signal.signal(s,handler)
    if failure is not None: raise failure
    if not summary['execution_gate_passed']: raise AssertionError('execution INVALID; closed ledger preserved')
    return summary


def replay(config_path, expected_sha, repo, output):
    config,proof,evidence = qualify(config_path,expected_sha,Path(repo).resolve())
    output = fixed.retained.regular_path(output)
    terminal = json.loads((output/'terminal.json').read_bytes())
    assert terminal['schema'] == SCHEMA+'-terminal' and terminal['config_sha256'] == expected_sha
    assert terminal['execution_source'] == config['execution_source']
    assert set(terminal['files']) == set(OUTPUTS)-{'terminal.json'}
    assert all(artifact(output/n) == p for n,p in terminal['files'].items()), 'terminal body drift'
    assert (output/'config.json').read_bytes() == Path(config_path).read_bytes()
    assert json.loads((output/'source-qualification.json').read_bytes()) == proof
    assert json.loads((output/'input-hashes.json').read_bytes()) == evidence['identities']
    rows = [json.loads(line) for line in (output/'records.jsonl').read_bytes().splitlines()]
    assert len(rows) == 384 and [(r['rate_index'],r['query_ordinal']) for r in rows] == [(i,q) for i in range(6) for q in range(64)]
    cells,failed = [],False
    for index in range(6):
        part = rows[index*64:(index+1)*64]
        path = output/f'rate{index}-records.jsonl'
        assert path.read_bytes() == b''.join(encoded(r)+b'\n' for r in part)
        marker = json.loads((output/f'rate{index}-summary.json').read_bytes())
        assert marker['records'] == artifact(path) and marker['config_sha256'] == expected_sha
        assert marker['code_identity_sha256'] == proof['code_identity_sha256'] and marker['refs_identity_sha256'] == proof['refs_identity_sha256']
        assert marker['binary_sha256'] == proof['binary_sha256']
        if marker['status'] != 'INVALID':
            result = reduce_cell(part,evidence,config)
            assert marker == dict(result,records=artifact(path),config_sha256=expected_sha,code_identity_sha256=proof['code_identity_sha256'],
                refs_identity_sha256=proof['refs_identity_sha256'],binary_sha256=proof['binary_sha256'])
        else:
            assert marker['attainment'] is marker['execution_gate_passed'] is False
            assert terminal['exit_code'] != 0
        if failed:
            assert all(r['outcome']=='aborted' and r['port'] is None for r in part), 'escalation after failed cell'
        failed |= not marker['attainment']; cells.append(marker)
    summary = json.loads((output/'summary.json').read_bytes())
    expected = campaign_summary(cells)
    if terminal['exit_code'] == 0:
        assert summary == expected and summary['execution_gate_passed'] is True
        cleanup = json.loads((output/'cleanup.json').read_bytes())
        assert cleanup['valid'] is cleanup['process_cleanup'] is cleanup['scratch_removed'] is cleanup['observer_stopped'] is True
        counters = json.loads((output/'offered-cgroup.json').read_bytes()); assert counters['closed'] is True
        fixed.check_cgroup(counters['before'],counters['after'],config['resources'],drained=True)
        assert not (output/'scratch').exists()
    else:
        assert summary['status'] == 'INVALID' and summary['execution_gate_passed'] is summary['offered_gate_passed'] is False
    assert terminal['status'] == summary['status'] and terminal['exit_code'] == (0 if summary['execution_gate_passed'] else 1)
    return summary


def overlap_check(rows, evidence, config, baseline):
    """Run the actual measured_call seam through our wrapper with overlapping threads."""
    from types import SimpleNamespace
    reports = []
    for concurrent in (True,False):
        with tempfile.TemporaryDirectory() as tmp:
            scratch = Path(tmp); (scratch/'native').mkdir()
            live,owners,results,stops = set(),{}, {},[]
            lock = threading.Lock(); barrier = threading.Barrier(2)
            a_before,a_done = threading.Event(),threading.Event()
            def snapshot():
                with lock:
                    value = copy.deepcopy(baseline); value['process_ids'] = baseline['process_ids']+sorted(live)
                if threading.current_thread().name == 'owner18080' and not a_before.is_set(): a_before.set()
                return value
            def spawn(command,**kwargs):
                port = int(command[-1].rsplit(':',1)[1]); q = port-18080
                owner = Path(kwargs['env']['TMPDIR']); owners[port] = owner
                assert owner.parent == scratch/'native'
                (owner/'payload').write_text(str(port))
                header = dict(rows[q]['native_header'],listen=f'127.0.0.1:{port}')
                kwargs['stdout'].write(json.dumps(header)+'\n'); kwargs['stdout'].flush()
                Path(command[command.index('-o')+1]).write_text(rows[q]['native_time_log'])
                with lock: live.add(port)
                return SimpleNamespace(pid=port,returncode=-15,poll=lambda:None,wait=lambda **k:-15)
            class Client:
                def __init__(self,host,port,**kwargs): self.port = port
                def connect(self): pass
                def close(self): pass
            def post(client,body):
                q = client.port-18080; assert body == evidence['requests'][q]
                barrier.wait(timeout=3)
                if q == 0: time.sleep(.75)
                else:
                    assert a_done.wait(3)
                    assert not owners[18080].exists() and owners[18081].exists()
                    assert (owners[18081]/'payload').read_text() == '18081'
                return 200,base64.b64decode(rows[q]['raw_response_base64'])
            def stop(process):
                assert (owners[process.pid]/'payload').read_text() == str(process.pid)
                with lock: live.remove(process.pid); stops.append(process.pid)
                return dict(returncode=-15,intentional_stop=True)
            def killpg(pid,sig):
                assert pid in stops
                if sig == 0: raise ProcessLookupError()
            def call(port):
                if port == 18081: assert a_before.wait(3)
                q = port-18080
                try:
                    if concurrent:
                        results[port] = call_one('fixture',config,evidence,q,port,scratch)
                    else:
                        results[port] = fixed.measured_call('fixture',config,evidence['arm'],evidence['requests'][q],
                            evidence['references'][q],evidence['truth'][q],scratch,port=port)
                finally:
                    if q == 0: a_done.set()
            with patch.object(fixed,'snapshot',side_effect=snapshot), patch.object(fixed.subprocess,'Popen',side_effect=spawn), \
                patch.object(fixed.cold.http.client,'HTTPConnection',Client), patch.object(fixed.cold,'post',side_effect=post), \
                patch.object(library,'native_cpu',return_value=rows[0]['query_cpu']['before']), \
                patch.object(library,'cold_stop',side_effect=stop), patch.object(os,'killpg',side_effect=killpg):
                threads = [threading.Thread(target=call,args=(port,),name=f'owner{port}') for port in (18080,18081)]
                for t in threads: t.start()
                for t in threads: t.join(4)
                assert all(not t.is_alive() for t in threads)
            assert stops == [18080,18081] and not live
            assert results[18080]['cgroup_after']['process_ids'] == baseline['process_ids']+[18081]
            assert results[18080]['outcome'] == ('success' if concurrent else 'failed')
            assert results[18081]['outcome'] == 'success'
            if concurrent:
                for q,port in enumerate((18080,18081)): validate_success(results[port],evidence,q,config)
            else: assert results[18080]['error'] == 'remaining descendants'
            assert all(r['temporary_directory_cleanup'] and r['native_close']['process_group_closed'] for r in results.values())
            fixed.check_cgroup(baseline,snapshot(),LIMITS,drained=True)
            reports.append(dict(explicit_concurrent_policy=concurrent,default_rejects_live_peer=not concurrent,actual_threads=2,owned_cleanup=True))
    return reports


def campaign_check(rows, evidence, config, baseline):
    """Exercise records/markers/exception closure through main and offline replay."""
    repo = Path(__file__).resolve().parents[1]
    cfg = dict(config); cfg['prices'] = dict(path='test-price',bytes=1,sha256='a'*64)
    proof = dict(config_sha256='c'*64,code_identity_sha256='d'*64,refs_identity_sha256='e'*64,binary_sha256='f'*64)
    e = dict(evidence,binary=dict(path=str(COLD/'native/binaries/two_bit_http'),**artifact(repo/COLD/'native/binaries/two_bit_http')),
        identities=dict(test_only=True))
    proof['binary_sha256'] = e['binary']['sha256']
    original_usage, original_read = scratch_usage, read
    env = dict(BORSUK_OFFERED_SOURCE_COMMIT='a'*40,BORSUK_OFFERED_ARCHIVE_SHA256='b'*64,AWS_MAX_ATTEMPTS='1',
        **dict.fromkeys(fixed.retained.THREAD_ENV,'2'))
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); cfgpath = root/'config.json'; cfgpath.write_bytes(encoded(cfg)+b'\n')
        for mode in ('pass','scientific-fail','identity','callback','stage'):
            clock = [10**18]; admitted = []; callback_calls = []
            def now(): clock[0] += 1000; return clock[0]
            def usage(path,**kwargs):
                return dict(unique_inode_bytes=1,physical_allocated_bytes=1) if Path(path) == repo else original_usage(path,**kwargs)
            def schedule(call,rate,**kwargs):
                index = RATES.index(rate); admitted.append(index); epoch = now()
                part = cellify_for_campaign(rows,index,epoch,baseline)
                if mode in ('scientific-fail','identity') and index == 0:
                    if mode == 'scientific-fail':
                        part[0].update(outcome='failed',failure_kind='transport',error_type='ConnectionResetError',error='reset')
                    else: part[0]['returned_hits'] = -1
                terminal = part[0]['cell_receipt']['terminal_ns']; clock[0] = terminal
                return part,epoch,terminal,None
            def callback(marker,paths):
                assert list((root/mode/'scratch/native').iterdir()) == [], 'callback before owned drain'
                assert marker['execution_gate_passed'] is True
                assert paths['summary'].is_file() and paths['records'].is_file(); callback_calls.append(marker['rate_index'])
                if mode == 'callback': raise RuntimeError('callback fixture')
            def staged_read(base,pointer,**kwargs):
                if mode == 'stage': raise AssertionError('staging fixture')
                return original_read(base,pointer,**kwargs)
            with patch.dict(os.environ,env), patch.object(sys.modules[__name__],'qualify',return_value=(cfg,proof,e)), \
                patch.object(fixed,'snapshot',return_value=baseline), patch.object(os,'sched_getaffinity',return_value={4,5}), \
                patch.object(sys.modules[__name__],'scratch_usage',side_effect=usage), patch.object(time,'monotonic_ns',side_effect=now), \
                patch.object(offered,'schedule_offers',side_effect=schedule), \
                patch.object(sys.modules[__name__],'read',side_effect=staged_read):
                failure = None
                try: result = main(cfgpath,'c'*64,repo,root/mode,on_cell_closed=callback)
                except (AssertionError,RuntimeError) as error: failure = error
                if mode in ('identity','callback','stage'): assert failure is not None
                else:
                    assert failure is None, (mode,type(failure).__name__,str(failure))
                    assert result['status'] == ('PASS' if mode == 'pass' else 'FAIL')
                replayed = replay(cfgpath,'c'*64,repo,root/mode)
                assert replayed['status'] == ('PASS' if mode == 'pass' else 'FAIL' if mode == 'scientific-fail' else 'INVALID')
            final_rows = [json.loads(line) for line in (root/mode/'records.jsonl').read_bytes().splitlines()]
            assert len(final_rows) == 384 and not (root/mode/'scratch').exists()
            if mode != 'pass': assert admitted == ([] if mode == 'stage' else [0])
            if mode == 'identity':
                assert final_rows[0]['returned_hits'] == -1 and 0 not in callback_calls
            if mode == 'scientific-fail':
                assert sum(r['outcome']=='aborted' for r in final_rows) == 320
                assert json.loads((root/mode/'terminal.json').read_bytes())['exit_code'] == 0
            results.append(dict(mode=mode,status=replayed['status'],positions=384))
    return results


def cellify_for_campaign(rows,index,epoch,baseline):
    """Test-only time translation; reserve any free owner through its recorded cleanup."""
    part = copy.deepcopy(rows); free = [epoch]*5
    for q,row in enumerate(part):
        planned = epoch+round(q*1e9/RATES[index]); slot = min(range(5),key=lambda p:free[p])
        dispatched = max(planned,free[slot]); start = dispatched+1000; delta = start-row['started_ns']; port = 18080+slot
        for n in ('started_ns','successful_connect_attempt_ns','connected_ns','completed_ns','first_wire_completed_ns','terminal_ns'): row[n] += delta
        free[slot] = row['terminal_ns']
        row['native_header']['listen'] = f'127.0.0.1:{port}'
        row['native_server_log'] = row['native_server_log'].replace('127.0.0.1:8080',f'127.0.0.1:{port}')
        row.update(rate_index=index,dataset='CoHere',offered_qps=RATES[index],port=port,scheduled_ns=planned,
            dispatched_ns=dispatched,cleanup_confirmed=True,namespace_start_attempted=True,failure_kind=None,abort_admissions=False)
    part[0]['cell_receipt'] = dict(epoch_ns=epoch,terminal_ns=max(max(free),epoch+round(64*1e9/RATES[index]))+10**9,
        abort_after=None,cell_started=True,worker_started_ns=epoch,admission_deadline_ns=epoch+1350*10**9,
        campaign_cgroup_before=baseline,cgroup_before=baseline,cgroup_after=baseline,resource_errors=[])
    return part


def authority_check():
    """Archived a5 authority through qualify, with Git unavailable and targeted tampering."""
    repo = Path(__file__).resolve().parents[1]
    fixture_repo = Path(os.environ.get('BORSUK_OFFERED_AUTHORITY_TEST_REPO',str(repo)))
    files = {}
    for n in COLD_ROSTER:
        path = repo/COLD/n; pointer = dict(path=str(COLD/n))
        if path.exists(): pointer.update(artifact(path))
        else:
            body = gzip.decompress(Path(str(path)+'.gz').read_bytes())
            pointer.update(bytes=len(body),sha256=sha(body),archived_path=pointer['path']+'.gz')
        files[n] = pointer
    terminal = json.loads(read(repo,files['aws-terminal.json']))
    old,control = (json.loads(read(repo,files[n])) for n in ('config.json','controller-config.json'))
    bridge_path = cold_spot.ROOT/'offered-cold-source-authority.json'
    actual = fixture_repo/bridge_path
    if actual.exists():
        bridge_body = actual.read_bytes(); bridge = json.loads(bridge_body)
    else:
        bridge = dict(schema='borsuk-fixed48-offered-cold-source-authority-v1',execution_source=old['execution_source'],
            runtime_code_sha256=old['code_sha256'],controller_code_sha256=control['code_sha256'],
            terminal=files['aws-terminal.json'],root_audit=files['root-audit.json'],authenticated_artifacts=terminal['artifacts'],
            native_source_identity_sha256=driver.SOURCE_ID,source_file_count=399,whole_body_verification=True,original_controller_exit_status=0)
        bridge_body = encoded(bridge)+b'\n'
    pointer = dict(path=str(bridge_path),bytes=len(bridge_body),sha256=sha(bridge_body))
    prices_pointer = dict(path='test-price-provenance.json',bytes=14,sha256=sha(b'{"test":true}\n'))
    prices_pointer['bytes'] = len(b'{"test":true}\n')
    cfg = dict(FIXED,bucket=science.BUCKET,namespace_prefix=old['namespace_prefix'],resources=LIMITS,
        code_sha256={n:artifact(repo/n)['sha256'] for n in CODE},execution_source=dict(commit='a'*40,archive_sha256='b'*64),
        cold_run=dict(directory=str(COLD),files=files),cold_source_authority=pointer,prices=prices_pointer)
    original_read,original_open,original_exists = read,Path.open,Path.exists
    changed = [bridge_body]; opened = []
    def checked_read(base,pin,**kwargs):
        if pin['path'] == str(bridge_path):
            assert identity(pin) == dict(bytes=len(changed[0]),sha256=sha(changed[0]))
            return changed[0]
        if pin['path'] == prices_pointer['path']: return b'{"test":true}\n'
        return original_read(base,pin,**kwargs)
    def no_git(path,*args,**kwargs):
        assert '.git' not in path.parts, 'Git file dependency'
        opened.append(str(path))
        return original_open(path,*args,**kwargs)
    def no_unused(path):
        if any(str(path).endswith('/'+n) for n in UNUSED): return False
        return original_exists(path)
    def reject(fn):
        try: fn()
        except (AssertionError,ValueError,FileNotFoundError): return
        raise AssertionError('authority tamper admitted')
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)/'config.json'; body = encoded(cfg)+b'\n'; path.write_bytes(body)
        with patch.object(sys.modules[__name__],'read',side_effect=checked_read), patch.object(Path,'open',no_git), \
            patch.object(Path,'exists',no_unused), patch.object(fixed.subprocess,'run',side_effect=AssertionError('no subprocess/Git')), \
            patch.object(fixed.subprocess,'check_output',side_effect=AssertionError('no subprocess/Git')):
            qualified,proof,evidence = qualify(path,sha(body),repo)
            assert proof['native_source_file_count'] == 399 and len(evidence['requests']) == 64
            reject(lambda: qualify(path,'0'*64,repo))
            altered = dict(cfg,authority_pending=True); path.write_bytes(encoded(altered))
            reject(lambda: qualify(path,sha(encoded(altered)),repo))
            altered = copy.deepcopy(cfg); altered['code_sha256'][OWN] = '0'*64; path.write_bytes(encoded(altered))
            reject(lambda: qualify(path,sha(encoded(altered)),repo))
            path.write_bytes(body)
            for key,value in (('whole_body_verification',False),('source_file_count',398),
                ('runtime_code_sha256',dict(bridge['runtime_code_sha256'],**{fixed.OWN:'0'*64})),
                ('authenticated_artifacts',dict(bridge['authenticated_artifacts'],**{'screen/records.jsonl':dict(bytes=1,sha256='0'*64)}))):
                tampered = dict(bridge,**{key:value}); changed[0] = encoded(tampered)
                bad = copy.deepcopy(cfg); bad['cold_source_authority'].update(bytes=len(changed[0]),sha256=sha(changed[0]))
                reject(lambda: historical_inputs(repo,bad))
            changed[0] = bridge_body
            bad = copy.deepcopy(cfg); bad['cold_run']['files']['aws-terminal.json']['sha256'] = '0'*64
            reject(lambda: historical_inputs(repo,bad))
        assert not any(str(repo/COLD/n) in opened for n in UNUSED), 'unused binary transfer'
        # Real CLI negative checks stop at authority validation, before any runtime resource/network work.
        import subprocess
        path.write_bytes(body)
        for args in ([str(path),'0'*64,str(repo),str(Path(tmp)/'new')],
                     ['--replay',str(path),'0'*64,str(repo),str(Path(tmp)/'closed')],[]):
            process = subprocess.run([sys.executable,str(repo/OWN),*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
            assert process.returncode != 0
            if args: assert b'config identity' in process.stderr
            else: assert b'CONFIG SHA REPO OUTPUT' in process.stderr
    return dict(authenticated_artifacts=56,native_source_files=399,sealed_panel=64,git_unavailable=True,
        unused_binaries_omitted=2,actual_root_bridge=actual.exists(),bridge=pointer,negative_cli=3,tamper_checks=8)


def self_check():
    # A changed ID, physical plan, response body or request must fail this validator.
    from scripts import run_cohere_fixed48_cold_http as fixed
    directory = Path(__file__).resolve().parents[1] / fixed.BASE / 'cold-http/a0005'
    publication = json.loads((directory / 'screen/publication.json').read_bytes())
    arm = publication['arm']
    panel = Path(__file__).resolve().parents[1] / fixed.SCIENTIFIC / 'screen'
    derived, refs, truths = fixed.panel_inputs(panel)
    evidence = dict(arm=arm, requests=[fixed.library.http_request(json.loads(b)['query'], arm['authority'])
        for b in derived.splitlines()], references=[dict(r, ids=r['ids'][:10]) for r in refs], truth=truths)
    rows = [json.loads(line) for line in (directory / 'screen/records.jsonl').read_bytes().splitlines()]
    assert callable(globals().get('validate_success')), 'offered authenticated response validator is missing'
    for q, row in enumerate(rows):
        validate_success(row, evidence, q, {'resources': fixed.HOST})
    for field, value in [('request_sha256', '0'*64), ('temporary_directory_cleanup', False),
                         ('returned_hits', -1), ('raw_response_base64', '')]:
        bad = copy.deepcopy(rows[0]); bad[field] = value
        try:
            validate_success(bad, evidence, 0, {'resources': fixed.HOST})
        except (AssertionError, ValueError, KeyError):
            pass
        else:
            raise AssertionError('tamper accepted: '+field)
    config = dict(FIXED, resources=LIMITS, bucket=science.BUCKET,
        namespace_prefix=arm['indexes']['10'], execution_source=dict(commit='a'*40,archive_sha256='b'*64))
    baseline = copy.deepcopy(rows[0]['cgroup_before'])
    def reject(callback):
        try: callback()
        except (AssertionError, ValueError, KeyError, FileNotFoundError, FileExistsError): return
        raise AssertionError('negative admitted')
    cellify = lambda source,index=0,epoch=10**18: cellify_for_campaign(source,index,epoch,baseline)
    normal = cellify(rows)
    result = reduce_cell(normal,evidence,config)
    assert result['attainment'] and result['execution_gate_passed']
    assert result['successful_full_span_qps'] < result['nominal_offered_qps'], 'finite edge was made an acceptance floor'
    assert result['published_context_gate_passed'] is False, '444 context cannot erase capacity'
    late = copy.deepcopy(normal); late[0]['dispatched_ns'] += 125000001; late[0]['started_ns'] = late[0]['dispatched_ns']
    # Keep response duration identities coherent after moving the start.
    late[0]['cold_start_to_first_http_response_ns'] = late[0]['completed_ns']-late[0]['started_ns']
    late[0]['before_successful_connect_attempt_ns'] = late[0]['successful_connect_attempt_ns']-late[0]['started_ns']
    late_result = reduce_cell(late,evidence,config)
    assert not late_result['attainment'] and late_result['execution_gate_passed'] and not late_result['dispatch_timing_gate_passed']
    def truth_for_hits(target):
        value = copy.deepcopy(evidence)
        excess = 619-target
        for q,row in enumerate(rows):
            for n in list(set(row['response']['ids']) & set(value['truth'][q][:10])):
                if not excess: return value
                replacement = next(i for i in range(999999,900000,-1) if i not in value['truth'][q] and i not in row['response']['ids'])
                value['truth'][q][value['truth'][q].index(n)] = replacement; excess -= 1
        assert excess == 0
        return value
    for hits in (607,608):
        altered = truth_for_hits(hits); positions = copy.deepcopy(normal)
        for q,row in enumerate(positions):
            row['returned_hits'] = fixed.validate_query(row['response'],arm,altered['references'][q],altered['truth'][q])
        reduced = reduce_cell(positions,altered,config)
        assert reduced['returned_hits'] == hits and reduced['attainment'] is (hits == 608)
    transport = copy.deepcopy(normal)
    transport[0].update(outcome='failed',failure_kind='transport',error_type='ConnectionResetError',error='reset')
    transport[0]['cgroup_after']['process_ids'] = transport[0]['cgroup_before']['process_ids']+[18081]
    transport_result = reduce_cell(transport,evidence,config)
    assert transport_result['execution_gate_passed'] and not transport_result['attainment'] and transport_result['errors'] == 1
    reject(lambda: fixed.check_cgroup(transport[0]['cgroup_before'],transport[0]['cgroup_after'],LIMITS,drained=True))
    fatal = copy.deepcopy(normal); origin = fatal[0]
    origin.update(outcome='failed',failure_kind='fatal',abort_admissions=True,error_type='AssertionError',error='identity')
    abort = dict(query_ordinal=0,reason='fatal call failure',observed_ns=origin['terminal_ns'])
    fatal = [origin,*aborted_rows(0,normal[0]['cell_receipt']['epoch_ns'],abort)[1:]]
    # Aborted rows are observed at abort time, not their unstarted future offer times.
    for row in fatal[1:]: row['terminal_ns'] = abort['observed_ns']
    fatal[0]['cell_receipt']['abort_after'] = abort
    fatal_result = reduce_cell(fatal,evidence,config)
    assert fatal_result['status'] == 'INVALID' and not fatal_result['execution_gate_passed']
    for changed in ('ledger','cleanup','drain','port'):
        bad = copy.deepcopy(normal)
        if changed == 'ledger': bad[-1]['query_ordinal'] = 0
        elif changed == 'cleanup': bad[0]['native_close']['process_group_closed'] = False
        elif changed == 'drain': bad[0]['cell_receipt']['cgroup_after'] = dict(baseline,process_ids=baseline['process_ids']+[99999])
        else: bad[0]['port'] = 18085
        reject(lambda: reduce_cell(bad,evidence,config))
    # Real threads own five distinct slots; the sixth offer drops before any release.
    live, peak, lock, release = set(),[0],threading.Lock(),threading.Event()
    five = threading.Event(); original_sleep = time.sleep
    def threaded(q,port):
        with lock:
            assert port not in live; live.add(port); peak[0] = max(peak[0],len(live))
            if len(live) == 5: five.set()
        assert release.wait(3)
        with lock: live.remove(port)
        return dict(outcome='success',cleanup_confirmed=True,abort_admissions=False)
    def release_sleep(delay):
        assert five.wait(3); release.set(); original_sleep(delay)
    with patch.object(offered,'scheduled_offsets_ns',return_value=[0]*6+[20000000+q*1000000 for q in range(58)]), patch.object(time,'sleep',side_effect=release_sleep):
        scheduled,_,_,_ = offered.schedule_offers(threaded,8,workers=5,base_port=18080)
    assert peak[0] == 5 and scheduled[5]['outcome'] == 'capacity_drop'
    assert {r['port'] for r in scheduled[:5]} == set(range(18080,18085)) and not live
    proof = dict(config_sha256='c'*64,code_identity_sha256='d'*64,refs_identity_sha256='e'*64,binary_sha256='f'*64)
    with tempfile.TemporaryDirectory() as tmp:
        dest = Path(tmp); calls = []
        def callback(marker,paths):
            assert paths['records'].is_file() and paths['summary'].is_file()
            assert artifact(paths['records']) == marker['records']; calls.append(marker)
        marker = close_cell(dest,normal,result,proof,callback)
        assert len(calls) == 1 and marker['status'] == 'PASS'
        bad_dir = dest/'invalid'; bad_dir.mkdir()
        close_cell(bad_dir,fatal,fatal_result,proof,callback)
        assert len(calls) == 1, 'INVALID callback escaped'
        reject(lambda: close_cell(dest,normal,result,proof,callback))
        (dest/'unsafe').symlink_to(dest/'rate0-records.jsonl')
        reject(lambda: scratch_usage(dest))
    overlap = overlap_check(rows,evidence,config,baseline)
    campaign = campaign_check(rows,evidence,config,baseline)
    authority = authority_check()
    return dict(self_check=True,actual_authenticated_panel=64,owners=5,sixth_drop=True,
        quality_boundary=[607,608],dispatch_limit_ns=125000000,invalid_callback_suppressed=True,
        finite_rate_edge_accepted=True,overlap=overlap,campaign=campaign,authority=authority)


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        print(json.dumps(self_check(), sort_keys=True))
    else:
        args = sys.argv[1:]
        reducer = replay if args and args[0] == '--replay' else main
        if reducer is replay: args = args[1:]
        assert len(args) == 4, 'CONFIG SHA REPO OUTPUT or --replay CONFIG SHA REPO CLOSED_OUTPUT'
        result = reducer(*args)
        print(json.dumps(dict(status=result['status'],execution_gate_passed=result['execution_gate_passed']),sort_keys=True))
        if not result['execution_gate_passed']: raise SystemExit(1)
