#!/usr/bin/env python3
"""Root-frozen fixed48 retained-generation cold HTTP measurement.

CONFIG SHA256 REPO NEW_EXTERNAL_OUTPUT; --self-check never uses native/cloud.
"""
import base64
from contextlib import contextmanager
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_cohere_fixed48_fresh_scientific_spot as science
from scripts import run_native_semantic_1m_cold as library

driver, retained, cold, telemetry, stats = (science.driver, science.driver.retained,
    library.cold, library.telemetry, library.stats)
encoded, sha, artifact, write = library.encoded, library.sha, library.artifact, library.write
OWN = 'scripts/run_cohere_fixed48_cold_http.py'
SCHEMA = 'borsuk-cohere-fixed48-retained-cold-http-v1'
CODE = tuple(sorted(set((*science.CODE, *library.CODE, OWN))))
BASE = Path(driver.BASE)
SCIENTIFIC = science.ROOT / 'a0002'
PANEL_FILES = ('requests.jsonl', 'truth.i64', 'records.jsonl', 'source-order.u64')
PROOF_PATHS = dict(
    preregistration=BASE / 'cold-http-preregister.md',
    admission_model=BASE / 'cold-admission-model.json',
    historical_validation=science.ROOT / 'closed-thread-verification/root-historical-validation.json',
    scientific_terminal=SCIENTIFIC / 'aws-terminal.json',
    scientific_launch=SCIENTIFIC / 'aws-launch.json',
    scientific_receipt=SCIENTIFIC / 'screen/measurement-receipt.json',
    scientific_marker=SCIENTIFIC / 'screen/COMPLETE.json',
    scientific_decision=SCIENTIFIC / 'screen/decision.json',
    scientific_resources=SCIENTIFIC / 'screen/score-resources.json',
    scientific_original_root=SCIENTIFIC / 'screen/original-generation-root.json',
    panel=BASE / 'panel-tools/panel.json',
    retained_terminal=science.RETAINED / 'aws-terminal.json',
    retained_collection=science.RETAINED / 'collection-receipt.json',
    retained_root=science.RETAINED / 'root-verification.json',
    retained_config=science.RETAINED / 'screen/config.json',
    retained_marker=science.RETAINED / 'screen/COMPLETE.json',
    ordinal_check=science.RETAINED / 'screen/sq8-ordinal-check.json',
    native_terminal=science.QUALIFICATION / 'aws-terminal.json',
    native_launch=science.QUALIFICATION / 'aws-launch.json',
    native_root=science.QUALIFICATION / 'root-verification.json')
FIXED = dict(schema=SCHEMA, authority_pending=False, rows=1_000_000, dimensions=768,
    queries=64, k=10, publication_top_k=100, concurrency=1, server_query_slots=4,
    region='eu-central-1', native_cpu_affinity=[0, 1, 2, 3], client_cpu_affinity=[4, 5],
    credential_protocol=stats.CREDENTIAL_PROTOCOL,
    selected_sha256='dfe569e0198f35d690e5aa6a248aa8eee73b6d59a569b194729368e84aa2f61c')
# Parent's prospective remote-host admission, separate from ANN payload bounds.
HOST = dict(shared_memory_bytes=12 << 30, swap_bytes=0, native_memory_bytes=2 << 30,
    publisher_memory_bytes=2 << 30, native_rlimit_as_bytes=4 << 30,
    scratch_bytes=16 << 30, cpu_quota_percent=200, tasks_max=512, threads=2)
DEADLINES = ('publication_limit_seconds', 'cold_limit_seconds', 'service_limit_seconds', 'output_reserve_bytes')
DELTA = ('sq8_object_key', 'sq8_etag', 'canonical.object_key')
LEAF_BYTES = 48 * 64 * 1540  # Unchanged native Fresh1m admission.
SDK_VERSION = '1.40.72'  # Same conditional-publication SDK as the disposable canary.


def sdk_guard(model=None):
    """Admit conditional publication without credentials or network calls."""
    import boto3, botocore, botocore.session, sys
    model = botocore.session.get_session().get_service_model('s3') if model is None else model
    capability = dict(boto3=boto3.__version__, botocore=botocore.__version__,
        conditional_put='IfNoneMatch' in model.operation_model('PutObject').input_shape.members,
        network_calls=0, python_executable=sys.executable)
    assert capability['conditional_put'], f'SDK lacks PutObject.IfNoneMatch: {capability}'
    assert boto3.__version__ == botocore.__version__ == SDK_VERSION, f'pinned cold SDK: {capability}'
    return capability


def panel_inputs(directory):
    original = (directory / 'requests.jsonl').read_bytes()
    derived = library.publisher_requests(original)
    references = library.source_reference((directory / 'records.jsonl').read_bytes())
    raw_truth = (directory / 'truth.i64').read_bytes()
    assert len(raw_truth) == 64 * 100 * 8, 'sealed truth geometry'
    truth = [list(struct.unpack_from('<100q', raw_truth, q * 800)) for q in range(64)]
    for row, reference, targets in zip(derived.splitlines(), references, truth):
        query = json.loads(row)['query']
        # Publisher validation preserves raw values; Rust owns cosine normalization.
        assert any(struct.unpack('<768f', struct.pack('<768f', *query))), 'nonzero f32 query required'
        assert len(set(targets)) == 100 and all(type(n) is int and 0 <= n < 1_000_000 for n in targets)
        assert len(reference['ids']) == len(set(reference['ids'])) == 100
        assert all(type(n) is int and 0 <= n < 1_000_000 for n in reference['ids'])
    return derived, references, truth


def native_budget_model(files, *, slots=4, eager=False, retained_root=False):
    """Current two_bit_generation.rs/returned_sq8.rs admission, not RSS."""
    disk = sum(files.values()) + (200_000_000 if eager else 0)
    query = 3 * 16_773_120 + 400_000 + (1 << 20) + (16_773_120 // 780) * 256 + 768 * 4
    total = (3 * disk + 3072 * 64 + 131072 + 32 * files['router/root.bin'] +
        slots * (2 * LEAF_BYTES + (1 << 20) + query + (0 if eager else 2 * (64 << 20))) +
        (32 * 200 if eager else 0) + (files['manifest.json'] if retained_root else 0))
    return dict(server_query_slots=slots, metadata_bytes=sum(files.values()),
        root_bytes=files['router/root.bin'], eager_local_plane=eager, retained_head_root=retained_root,
        modeled_remote_payload_bytes=total, interpretation='native payload admission, not RSS')


def read_proofs(repo, pins):
    assert set(pins) == set(PROOF_PATHS), 'exact proof roster'
    return {name: science.read_repo(repo, pin, PROOF_PATHS[name]) for name, pin in pins.items()}


def historical_assets(repo, proofs):
    data = {n: json.loads(b) for n, b in proofs.items() if n != 'preregistration'}
    terminal, verified = data['scientific_terminal'], data['historical_validation']
    assert verified['closed_validation_passed'] is True and verified['scientific_status'] == 'GO'
    assert verified['authenticated_terminal_bodies'] == 70 and verified['state'] == 'terminated'
    assert verified['measurement_rerun'] is verified['launch_authority'] is False
    assert verified['original_controller_exit'] == 2 and verified['original_collection_execution_status'] == 'FAIL'
    assert verified['terminal'] == dict(bytes=len(proofs['scientific_terminal']), sha256=sha(proofs['scientific_terminal']))
    assert terminal['instance_id'] == verified['instance_id'] == data['scientific_launch']['instance_id']
    assert terminal['phase'] == terminal['status'] == 'complete' and terminal['exit_code'] == terminal['original_exit_code'] == 0
    assert terminal['config_sha256'] == verified['config_sha256']
    assert verified['execution_source'] == dict(commit=terminal['source_archive_commit'], archive_sha256=terminal['source_archive_sha256'])
    marker, config = data['retained_marker'], data['retained_config']
    assert set(marker['files']) == set(retained.RETAINED_FILES) and len(marker['files']) == 30
    assert marker['passed'] is marker['process_cleanup'] is marker['retained'] is True
    assert marker['config_sha256'] == sha(proofs['retained_config'])
    assert marker['roster_sha256'] == sha(encoded(marker['files']))
    assert config == retained.historical_config(repo)[0], 'immutable retained config'
    original = science.original_assets(repo)
    wanted = ['retained/generation/' + name for name in retained.GENERATION_FILES] + ['retained/source-sq8.bin']
    assets = {('generation/' + name.split('retained/generation/', 1)[1] if name.startswith('retained/generation/') else 'sq8.bin'):
        {k: original[name][k] for k in ('bytes', 'sha256', 'source')} for name in wanted}
    assets.update({n: {k: p[k] for k in ('bytes', 'sha256', 'source')}
        for n, p in original.items() if n.startswith('qualification/')})
    for name in ('aws-reservation.json', 'aws-closeout.json', 'aws-terminal.json', 'collection-replay.json', 'root-verification.json'):
        assets['qualification/' + name]['source'] = dict(repo_path=str(science.QUALIFICATION / name))
    prefix = data['scientific_launch']['prefix']
    for name in PANEL_FILES:
        pin = terminal['artifacts']['screen/' + name]
        assets['panel/' + name] = dict(pin, source=dict(bucket=science.BUCKET, key=prefix + '/artifacts/screen/' + name))
        assert data['scientific_marker']['files'][name] == pin, 'scientific seal body pin'
    for name, proof_name in (('measurement-receipt.json', 'scientific_receipt'), ('COMPLETE.json', 'scientific_marker'),
            ('decision.json', 'scientific_decision'), ('score-resources.json', 'scientific_resources'),
            ('original-generation-root.json', 'scientific_original_root')):
        assert terminal['artifacts']['screen/' + name] == dict(bytes=len(proofs[proof_name]), sha256=sha(proofs[proof_name]))
    receipt = data['scientific_receipt']
    assert receipt['measurement_sealed_before_truth'] is receipt['resource_gate_passed'] is receipt['cleanup_complete'] is True
    assert receipt['exit_status'] == 0 and data['scientific_decision']['scientific_status'] == 'GO'
    assert data['scientific_marker']['execution_status'] == 'SUCCESS' and data['scientific_marker']['scientific_status'] == 'GO'
    for name, field in (('requests.jsonl', 'requests'), ('records.jsonl', 'measurements'), ('source-order.u64', 'order')):
        assert library.identity(receipt[field]) == library.identity(assets['panel/' + name])
    assert assets['panel/source-order.u64']['bytes'] == 8_000_000
    assert library.identity(assets['panel/source-order.u64']) == marker['files']['source-order.u64']
    assert library.identity(assets['sq8.bin']) == library.identity(config['corpus']['sq8'])
    assert marker['files']['generation/manifest.json'] == dict(bytes=len(proofs['scientific_original_root']), sha256=sha(proofs['scientific_original_root']))
    ordinal = data['ordinal_check']
    assert data['retained_terminal']['artifacts']['screen/sq8-ordinal-check.json'] == dict(bytes=len(proofs['ordinal_check']), sha256=sha(proofs['ordinal_check']))
    assert ordinal['complete_bijection'] is ordinal['id_matches_order'] is True
    assert ordinal['rows_checked'] == 1_000_000 and ordinal['record_bytes'] == 780
    assert ordinal['order']['sha256'] == assets['panel/source-order.u64']['sha256']
    assert ordinal['sq8']['sha256'] == assets['sq8.bin']['sha256']
    assert data['panel']['selected_sha256'] == FIXED['selected_sha256']
    assert len(assets) == 38, 'exact materialized body roster'
    return assets, data


def qualify(config_path, expected_sha, repo):
    assert Path(__file__).resolve() == repo / OWN, 'executor origin'
    config_path = retained.regular_path(config_path)
    assert 0 < config_path.stat().st_size <= 1 << 20 and artifact(config_path)['sha256'] == expected_sha
    config = json.loads(config_path.read_bytes())
    assert set(config) == set(FIXED) | {'bucket', 'namespace_prefix', 'code_sha256', 'execution_source', 'asset_manifest', 'proofs', 'resources'}
    assert all(encoded(config.get(k)) == encoded(v) for k, v in FIXED.items()), 'pending or changed protocol'
    assert config['bucket'] == science.BUCKET
    driver.safe_key(config['namespace_prefix'])
    assert config['namespace_prefix'].startswith('research/semantic-router/')
    assert set(config['code_sha256']) == set(CODE), 'exact current executor closure'
    for name, digest in config['code_sha256'].items():
        assert artifact(retained.regular_path(repo / name))['sha256'] == digest, 'executor code drift: ' + name
    source = config['execution_source']
    assert set(source) == {'commit', 'archive_sha256'}
    assert driver.re.fullmatch('[0-9a-f]{40}', source['commit']) and stats.digest(source['archive_sha256'])
    for key, env in (('commit', 'BORSUK_COLD_SOURCE_COMMIT'), ('archive_sha256', 'BORSUK_COLD_ARCHIVE_SHA256')):
        assert os.environ.get(env) == source[key], 'execution archive binding: ' + env
    proofs = read_proofs(repo, config['proofs'])
    assets, historical = historical_assets(repo, proofs)
    manifest = json.loads(science.read_repo(repo, config['asset_manifest']))
    assert set(manifest) == {'schema', 'authority_pending', 'assets'}
    assert manifest['schema'] == 'borsuk-fixed48-retained-cold-assets-v1' and manifest['authority_pending'] is False
    assert manifest['assets'] == assets, 'original retained/native/sealed asset pointers'
    limits = config['resources']
    assert set(limits) == set(HOST) | set(DEADLINES)
    assert all(type(limits[n]) is int and limits[n] == v for n, v in HOST.items()), 'prospective host limits'
    assert all(type(limits[n]) is int and limits[n] > 0 for n in DEADLINES)
    assert limits['publication_limit_seconds'] + limits['cold_limit_seconds'] < limits['service_limit_seconds']
    assert sum(p['bytes'] for p in assets.values()) + limits['output_reserve_bytes'] < limits['scratch_bytes']
    source_tree = driver.qualification.worker.source_hashes(repo)
    assert len(source_tree) == 399 and driver.qualification.worker.source_identity(source_tree) == driver.SOURCE_ID
    model = historical['admission_model']
    assert model['schema'] == 'borsuk-fixed48-prospective-cold-admission-model-v1'
    assert model['native_source_identity_sha256'] == driver.SOURCE_ID and model['server_slots'] == 4
    assert model['transport_delta'] == list(DELTA) and model['unchanged_payloads'] is True
    assert set(model['prospective_remote_limits']) == set(HOST) - {'threads'}
    assert all(model['prospective_remote_limits'][n] == limits[n] for n in model['prospective_remote_limits'])
    files = {n: assets['generation/' + n]['bytes'] for n in library.STARTUP}
    assert model['modeled_remote_payload_bytes'] == native_budget_model(files)['modeled_remote_payload_bytes']
    assert model['metadata_bytes'] == sum(files.values()) and model['router_root_bytes'] == files['router/root.bin']
    assert model['observed_scientific_scorer_rss_bytes'] == historical['scientific_resources']['process_peak_rss_kib'] * 1024
    return config, assets, historical


def validate_query(response, arm, expected, truth=None):
    assert response['authority'] == arm['authority'], 'query authority'
    assert len(response['ids']) == len(set(response['ids'])) == len(expected['ids'])
    assert all(type(n) is int and 0 <= n < 1_000_000 for n in response['ids'])
    assert all(response[n] == expected[n] for n in stats.PARITY), 'sealed ordered-ID/physical-plan parity'
    for prefix, gets, size in (('', 32, 16_773_120), ('source_', 128, 64 << 20), ('router_', 48, LEAF_BYTES)):
        stats.integer(response[prefix + 'submitted_gets'], prefix + 'GETs', 1, gets)
        stats.integer(response[prefix + 'verified_bytes'], prefix + 'bytes', 1, size)
        stats.integer(response[prefix + 'failed_gets'], prefix + 'failures', 0, 0)
    previous, size = 0, 0
    for start, end in response['ranges']:
        assert type(start) is type(end) is int and previous <= start < end <= 780_000_000
        assert start % 199680 == 0 and (end % 199680 == 0 or end == 780_000_000)
        previous, size = end, size + end - start
    assert len(response['ranges']) == response['submitted_gets'] and size == response['planned_bytes'] == response['verified_bytes']
    stats.validate_stages(response['query_stages'], response['native_wall_ns'], 'semantic', response['router_submitted_gets'], True)
    return None if truth is None else len(set(response['ids']) & set(truth[:10]))


def snapshot():
    return library.capture()


def check_cgroup(before, after, limits, *, drained=False):
    assert before['cgroup'] == after['cgroup'], 'resource cgroup changed'
    for value in (before, after):
        assert int(value['memory.max']) == limits['shared_memory_bytes']
        assert 0 <= int(value['memory.peak']) <= limits['shared_memory_bytes']
        assert int(value['memory.swap.max']) == int(value['memory.swap.peak']) == limits['swap_bytes'] == 0
        quota, period = map(int, value['cpu.max'].split())
        assert quota * 100 == period * limits['cpu_quota_percent'] and period > 0
        assert int(value['pids.max']) == limits['tasks_max'] and 0 < int(value['pids.current']) <= limits['tasks_max']
        for name in ('memory.events', 'memory.swap.events', 'pids.events'):
            events = dict(line.split() for line in value[name].splitlines())
            keys = ('oom', 'oom_kill', 'oom_group_kill', 'max') if name == 'memory.events' else events
            old = dict(line.split() for line in before[name].splitlines())
            assert all(int(events.get(k, 0)) == int(old.get(k, 0)) for k in keys), 'resource failure: ' + name
    if drained:
        assert set(after['process_ids']) <= set(before['process_ids']), 'remaining descendants'


@contextmanager
def observe(output, limits, report, deadline):
    before = snapshot()
    stopped, errors = threading.Event(), []
    def check(reserve=0):
        assert time.monotonic() < deadline, 'whole service deadline'
        usage = science.scratch_usage(output)
        report['scratch_usage'] = usage
        used = max(usage['unique_inode_bytes'], usage['physical_allocated_bytes'])
        report['peak_scratch_bytes'] = max(report.get('peak_scratch_bytes', 0), used)
        assert used + reserve <= limits['scratch_bytes'], 'whole output/scratch admission'
        assert shutil.disk_usage(output).free >= reserve, 'disk headroom'
        check_cgroup(before, snapshot(), limits)
    def watch():
        while not stopped.wait(.25):
            try:
                check()
            except BaseException as error:
                errors.append(error)
                os.kill(os.getpid(), signal.SIGTERM)
                return
    check()
    thread = threading.Thread(target=watch, daemon=True)
    thread.start()
    try:
        yield check
        if errors:
            raise errors[0]
    finally:
        stopped.set()
        thread.join(timeout=5)
        assert not thread.is_alive(), 'resource observer remains'
        report['cgroup'] = dict(before=before, after=snapshot(), closed=True)
        check_cgroup(before, report['cgroup']['after'], limits, drained=True)


@contextmanager
def sdk_operation(s3, operation, bucket, key, ledger, **kwargs):
    row = dict(operation=operation, bucket=bucket, key=key, started_ns=time.monotonic_ns(),
        sdk_http_dispatch_attempts=0, http_statuses=[], retry_attempts=0, consumed_response_bytes=0,
        error=None, scope='Python staging/publication-control SDK; excludes native ANN',
        byte_scope='consumed object stream bytes; SDK-parsed control/error bodies and wire bytes unmeasured')
    token = 'fixed48-' + str(row['started_ns'])
    def sent(**unused):
        row['sdk_http_dispatch_attempts'] += 1
    def retry(response=None, **unused):
        if response is not None:
            row['http_statuses'].append(response[0].status_code)
    s3.meta.events.register('before-send.s3', sent, unique_id=token + '-send')
    s3.meta.events.register('needs-retry.s3', retry, unique_id=token + '-retry')
    response = None
    try:
        response = getattr(s3, operation)(Bucket=bucket, Key=key, **kwargs)
        metadata = response['ResponseMetadata']
        row.update(http_status=metadata['HTTPStatusCode'],
            retry_attempts=max(metadata.get('RetryAttempts', 0), row['sdk_http_dispatch_attempts'] - 1))
        assert row['retry_attempts'] == 0 and row['sdk_http_dispatch_attempts'] == 1, 'SDK retries/dispatch ledger'
        yield response, row
    except BaseException as error:
        row['error'] = dict(type=type(error).__name__, message=str(error))
        error_response = getattr(error, 'response', {})
        if error_response:
            row['sdk_error_status'] = error_response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            row['sdk_error_code'] = error_response.get('Error', {}).get('Code')
            row['retry_attempts'] = max(error_response.get('ResponseMetadata', {}).get('RetryAttempts', 0), row['sdk_http_dispatch_attempts'] - 1)
        raise
    finally:
        if response is not None and 'Body' in response:
            response['Body'].close()
        row['completed_ns'] = time.monotonic_ns()
        s3.meta.events.unregister('before-send.s3', unique_id=token + '-send')
        s3.meta.events.unregister('needs-retry.s3', unique_id=token + '-retry')
        ledger.write(encoded(row) + b'\n')
        ledger.flush()
        os.fsync(ledger.fileno())


def fetch(s3, source, pin, ledger, checkpoint, deadline, target=None, *, etag=None):
    """Full bounded SHA stream; final rename only after authentication."""
    science.pin(library.identity(pin))
    part = None if target is None else target.with_name(target.name + '.part')
    if target is not None:
        checkpoint(pin['bytes'])
        target.parent.mkdir(parents=True, exist_ok=True)
        assert not target.exists() and not part.exists()
    hashed, count = hashlib.sha256(), 0
    try:
        with sdk_operation(s3, 'get_object', source['bucket'], source['key'], ledger,
                **({'IfMatch': etag} if etag else {})) as (response, row):
            with response['Body'] as stream:
                assert response['ContentLength'] == pin['bytes'], 'SDK object length'
                if etag:
                    assert response['ETag'] == etag, 'conditional transport ETag'
                output = part.open('xb') if part is not None else None
                try:
                    while True:
                        assert time.monotonic() < deadline, 'SDK stream deadline'
                        chunk = stream.read(1 << 20)
                        if not chunk:
                            break
                        count += len(chunk)
                        row['consumed_response_bytes'] = count
                        assert count <= pin['bytes'], 'SDK length overflow'
                        hashed.update(chunk)
                        if part is not None:
                            output.write(chunk)
                    if part is not None:
                        output.flush()
                        os.fsync(output.fileno())
                finally:
                    if output is not None:
                        output.close()
            row['stream_sha256'] = hashed.hexdigest()
            assert dict(bytes=count, sha256=hashed.hexdigest()) == library.identity(pin), 'SDK body SHA/length'
            if part is not None:
                os.rename(part, target)
                driver.sync_directory(target.parent)
    finally:
        if part is not None:
            part.unlink(missing_ok=True)
    checkpoint()
    return dict(bytes=count, sha256=hashed.hexdigest())


def stage_asset(s3, pin, repo, target, ledger, checkpoint, deadline):
    if set(pin['source']) == {'repo_path'}:
        driver.safe_key(pin['source']['repo_path'])
        source = retained.regular_path(repo / pin['source']['repo_path'])
        assert source.is_relative_to(repo) and pin['bytes'] <= 1 << 20, 'bounded repository bridge'
        assert artifact(source) == library.identity(pin), 'original native bridge SHA'
        checkpoint(pin['bytes'])
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, target)
        checkpoint()
    else:
        fetch(s3, pin['source'], pin, ledger, checkpoint, deadline, target)


def remote_head(s3, bucket, key, ledger, pin):
    with sdk_operation(s3, 'head_object', bucket, key, ledger) as (head, unused):
        assert head['ContentLength'] == pin['bytes'], 'remote HEAD length'
        assert type(head['ETag']) is str and head['ETag'] and not head['ETag'].startswith('W/')
        return head


def transport_manifest(original, prefix, etag):
    value = copy.deepcopy(original)
    value['sq8_object_key'] = prefix + '/objects/' + original['sq8_object_sha256']
    value['sq8_etag'] = etag
    value['canonical']['object_key'] = prefix + '/objects/' + original['canonical']['sha256']
    restored = copy.deepcopy(value)
    for name in DELTA[:2]:
        restored[name] = original[name]
    restored['canonical']['object_key'] = original['canonical']['object_key']
    assert restored == original and all(value[n] != original[n] for n in DELTA[:2])
    assert value['canonical']['object_key'] != original['canonical']['object_key']
    return value


def publish_generation(s3, config, scratch, output, references, ledger, checkpoint, deadline):
    assets = scratch / 'assets'
    original_path = assets / 'generation/manifest.json'
    original = json.loads(original_path.read_bytes())
    assert original['sq8_object_sha256'] == artifact(assets / 'sq8.bin')['sha256']
    prefix, bucket = config['namespace_prefix'], config['bucket']
    key = prefix + '/objects/' + original['sq8_object_sha256']
    assert key != original['sq8_object_key'], 'fresh SQ8 namespace required before PUT'
    assert prefix + '/objects/' + original['canonical']['sha256'] != original['canonical']['object_key'], 'fresh canonical namespace required before publication'
    sq8_pin = artifact(assets / 'sq8.bin')
    with (assets / 'sq8.bin').open('rb') as body:
        with sdk_operation(s3, 'put_object', bucket, key, ledger, Body=body, IfNoneMatch='*') as (response, row):
            assert response['ResponseMetadata']['HTTPStatusCode'] == 200
            row['request_body_bytes'] = sq8_pin['bytes']
    head = remote_head(s3, bucket, key, ledger, sq8_pin)
    assert head['ETag'] != original['sq8_etag'], 'LocalFileSystem ETag forbidden in S3'
    fetch(s3, dict(bucket=bucket, key=key), sq8_pin, ledger, checkpoint, deadline, etag=head['ETag'])
    manifest = transport_manifest(original, prefix, head['ETag'])
    root = scratch / 'generation'
    for name in retained.GENERATION_FILES:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if name == 'manifest.json':
            write(target, manifest)
        else:
            os.link(assets / 'generation' / name, target)
    checkpoint()
    write(output / 'original-generation-root.json', original_path.read_bytes())
    root_sha = artifact(root / 'manifest.json')['sha256']
    write(output / 'transport-delta.json', dict(allowed_fields=list(DELTA),
        original=artifact(original_path), serving=artifact(root / 'manifest.json'),
        original_descriptors={n: original[n] for n in DELTA[:2]},
        serving_descriptors={n: manifest[n] for n in DELTA[:2]},
        original_canonical_key=original['canonical']['object_key'], serving_canonical_key=manifest['canonical']['object_key'],
        all_other_fields_equal=True, unchanged_payloads=True, original_physical_order=True))
    files = {n: artifact(root / n)['bytes'] for n in library.STARTUP}
    models = dict(http=native_budget_model(files, retained_root=True),
        publisher_local=native_budget_model(files, slots=1, eager=True),
        publisher_remote=native_budget_model(files, slots=1))
    assert models['http']['modeled_remote_payload_bytes'] <= config['resources']['native_memory_bytes']
    assert max(models[n]['modeled_remote_payload_bytes'] for n in ('publisher_local', 'publisher_remote')) <= config['resources']['publisher_memory_bytes']
    publisher = assets / 'qualification/binaries/two_bit_plan_demo'
    publisher.chmod(0o500)
    command = ['taskset', '-c', '0-3', 'prlimit', '--as=4294967296:4294967296', str(publisher),
        str(root), root_sha, str(output / 'publisher-requests.jsonl'), artifact(output / 'publisher-requests.jsonl')['sha256'],
        str(output / 'publication-reference.jsonl'), '0', '64', '--live-s3', bucket, config['region'], prefix,
        '--panel-count', '64', '--top-k', '100']
    with patch.dict(os.environ, BORSUK_NATIVE_MEMORY_BYTES=str(config['resources']['publisher_memory_bytes']), TMPDIR=str(scratch / 'native')):
        result = library.quality.run_process(command, output / 'publication.log',
            min(config['resources']['publication_limit_seconds'], deadline - time.monotonic()), output / 'publication-resources.txt')
    assert result['exit_status'] == 0 and result['process_cleanup'] is True, 'production publisher failed'
    publisher_resources = telemetry.resources((output / 'publication-resources.txt').read_text(), config['resources']['publisher_memory_bytes'])
    head_path = scratch / 'head.json'
    with sdk_operation(s3, 'get_object', bucket, prefix + '/head.json', ledger) as (response, row):
        with response['Body'] as stream:
            raw = stream.read(65537)
            row['consumed_response_bytes'] = len(raw)
        assert 0 < len(raw) <= 65536 and len(raw) == response['ContentLength'], 'bounded production head'
        write(head_path, raw)
    arm, published = library.publication_arm(root, prefix, raw,
        native_memory_bytes=config['resources']['native_memory_bytes'], budget_model=models['http'])
    validation = library.publication_reference(output / 'publication-reference.jsonl', arm, references)
    assert published == manifest and artifact(original_path) == artifact(output / 'original-generation-root.json')
    for name in library.STARTUP:
        fetch(s3, dict(bucket=bucket, key=prefix + '/generations/' + root_sha + '/' + name),
            artifact(root / name), ledger, checkpoint, deadline)
    canonical = artifact(root / 'canonical.bin')
    canonical_head = remote_head(s3, bucket, manifest['canonical']['object_key'], ledger, canonical)
    fetch(s3, dict(bucket=bucket, key=manifest['canonical']['object_key']), canonical, ledger,
        checkpoint, deadline, etag=canonical_head['ETag'])
    publication = dict(arm=arm, manifest=manifest, validation=validation,
        native_budget_models=models, process=result, resources=publisher_resources,
        head_body_base64=base64.b64encode(raw).decode(), sq8=dict(sq8_pin, key=key, etag=head['ETag']),
        canonical=dict(canonical, key=manifest['canonical']['object_key'], etag=canonical_head['ETag']),
        publication_via_production_library=True, retained_for_offered_gate=True)
    write(output / 'publication.json', publication)
    return arm


def measured_call(binary, config, arm, body, expected, truth, scratch, *, port=8080, require_cgroup_drained=True):
    """Concurrent callers opting out of per-call cgroup drain must prove final cell drain."""
    assert type(require_cgroup_drained) is bool, 'require_cgroup_drained must be bool'
    failures = io.StringIO()
    observed = dict(native_process_started=False, http_attempts=0)
    before = snapshot()
    def temp_dir():
        owned = tempfile.TemporaryDirectory(dir=scratch / 'native')
        observed['temporary_directory'] = owned.name
        return owned
    def spawn(command, **kwargs):
        command = list(command)
        index = command.index('taskset')
        command[index:index] = ['prlimit', '--as=4294967296:4294967296']
        observed['child_tmpdir'] = kwargs['env']['TMPDIR']
        process = subprocess.Popen(command, **kwargs)
        observed.update(native_process_started=True, wrapper_pid=process.pid)
        return process
    def post(client, payload):
        cpu_before = library.native_cpu(observed['wrapper_pid'], binary)
        observed['http_attempts'] = 1
        status, raw = cold.post(client, payload)
        observed.update(http_status=status, first_wire_completed_ns=time.monotonic_ns(),
            raw_response_base64=base64.b64encode(raw).decode())
        cpu_after = library.native_cpu(observed['wrapper_pid'], binary)
        assert (cpu_before['pid'], cpu_before['start_ticks']) == (cpu_after['pid'], cpu_after['start_ticks'])
        observed['query_cpu'] = dict(before=cpu_before, after=cpu_after,
            ticks=sum(cpu_after[n] - cpu_before[n] for n in ('user_ticks', 'system_ticks')),
            ticks_per_second=os.sysconf('SC_CLK_TCK'), scope='native process ticks sampled around one POST')
        assert observed['query_cpu']['ticks'] >= 0, 'nonmonotonic native CPU'
        return status, raw
    row = {}
    environment = dict(os.environ,
        BORSUK_NATIVE_MEMORY_BYTES=str(config['resources']['native_memory_bytes']), AWS_MAX_ATTEMPTS='1')
    try:
        admission = dict(native_memory_bytes=config['resources']['native_memory_bytes'],
            budget_model=native_budget_model(arm['metadata_files'], retained_root=True))
        library.validate_roster(arm, **admission)
        row = cold.cold_call(str(binary), config, dict(arm, dataset='CoHere'), body, expected, truth, failures, port=port,
            response_check=lambda r, e, t, a: validate_query(r, arm, e, t),
            startup_check=lambda v, f, w: library.validate_startup(v, arm, w, wave_objects=8, root_reuse=True, **admission),
            post_call=post, spawn=spawn, env=environment, stop_call=library.close_native, temp_dir=temp_dir)
        row['accounting'] = library.transport(row['native_header'], row['response'], arm, wave_objects=8, root_reuse=True, **admission)
        row.update(outcome='success', **observed)
        row['completed_ns'] = observed['first_wire_completed_ns']
        for key, start in (('cold_start_to_first_http_response_ns', 'started_ns'), ('first_post_to_response_ns', 'connected_ns'),
                ('incoming_http_wall_ns', 'successful_connect_attempt_ns')):
            row[key] = row['completed_ns'] - row[start]
        assert row['native_close']['intentional_stop'] is row['native_close']['process_group_closed'] is True
        row['resources'] = telemetry.resources(row['native_time_log'], config['resources']['native_memory_bytes'])
        check_cgroup(before, snapshot(), config['resources'], drained=require_cgroup_drained)
    except Exception as error:
        failed = [json.loads(line) for line in failures.getvalue().splitlines()]
        if not row and failed:
            assert len(failed) == 1, 'duplicate failure receipt'
            row = failed[0]
        row.update(outcome='failed', error_type=type(error).__name__, error=str(error), **observed)
        for label, header in (('startup_transport', 'native_header'), ('final_process_transport', 'response')):
            try:
                if header == 'native_header' and header not in row:
                    headers = [json.loads(line) for line in row.get('native_server_log', '').splitlines() if line.startswith('{')]
                    assert len(headers) == 1
                    row[header] = headers[0]
                if header == 'response' and header not in row:
                    row[header] = json.loads(base64.b64decode(observed['raw_response_base64']))
                row.setdefault('failure_transport', {})[label] = stats.validate_transport(row[header]['transport'], False)
            except (KeyError, ValueError, AssertionError) as telemetry_error:
                row.setdefault('telemetry_errors', {})[label] = str(telemetry_error)
        try:
            row['resources'] = telemetry.resources(row.get('native_time_log', ''), config['resources']['native_memory_bytes'])
        except ValueError as resource_error:
            row['resource_error'] = str(resource_error)
    row.update(query_ordinal=expected['query_ordinal'], terminal_ns=time.monotonic_ns(),
        expected_authority=arm['authority'], request_sha256=sha(body), request_bytes=len(body), http_retry=False,
        cgroup_before=before, cgroup_after=snapshot(), temporary_directory_cleanup=
            'temporary_directory' in observed and not Path(observed['temporary_directory']).exists())
    return row


def reduce_records(records, start, end):
    assert len(records) == 64 and [r['query_ordinal'] for r in records] == list(range(64)), 'all64 offer ledger'
    successful = [r for r in records if r['outcome'] == 'success']
    hits = sum(r['returned_hits'] for r in successful)
    assert all(r['outcome'] in ('success', 'failed', 'aborted') and r['http_attempts'] in (0, 1) for r in records)
    for row in successful:
        assert row['http_attempts'] == row['valid_ann_requests'] == 1 and row['http_status'] == 200
        assert row['http_retry'] is False and row['response'] == json.loads(base64.b64decode(row['raw_response_base64'], validate=True))
        assert row['native_close']['intentional_stop'] is row['native_close']['process_group_closed'] is True
        assert row['temporary_directory_cleanup'] is True, 'owned call temporary directory cleanup not proven'
        assert row['started_ns'] <= row['completed_ns'] <= row['terminal_ns']
        assert row['cold_start_to_first_http_response_ns'] == row['completed_ns'] - row['started_ns']
    for previous, row in zip(records, records[1:]):
        if 'started_ns' in row and 'terminal_ns' in previous:
            assert previous['terminal_ns'] <= row['started_ns'], 'concurrency1 drained order'
    metrics = {n: telemetry.tails([r[n] / 1e6 for r in successful]) for n in (
        'cold_start_to_first_http_response_ns', 'before_successful_connect_attempt_ns',
        'successful_tcp_connect_ns', 'first_post_to_response_ns')}
    for n in ('remote_open_wall_ns', 'head_read_wall_ns'):
        metrics[n] = telemetry.tails([r['native_header'][n] / 1e6 for r in successful])
    metrics['native_query_wall_ns'] = telemetry.tails([r['response']['native_wall_ns'] / 1e6 for r in successful])
    for n in stats.STAGES:
        metrics[n] = telemetry.tails([(r['response']['query_stages'][n]['end_ns'] - r['response']['query_stages'][n]['start_ns']) / 1e6 for r in successful])
    execution = len(successful) == 64
    return dict(schema=SCHEMA + '-result', dataset='CoHere', closed=True, offered_queries=64, denominator10=640,
        successful_calls=len(successful), failed_calls=sum(r['outcome'] == 'failed' for r in records),
        aborted_calls=sum(r['outcome'] == 'aborted' for r in records), returned_hits10=hits, recall_at_10=hits / 640,
        execution_gate_passed=execution, quality_gate_passed=execution and hits >= 608,
        status='PASS' if execution and hits >= 608 else 'FAIL', latency_ms=metrics,
        latency_population='successful calls; acceptance requires all64 offers succeed',
        context_p90_attained=execution and metrics['cold_start_to_first_http_response_ns']['p90'] < 444,
        serial_full_span_ns=end - start, serial_full_span_completions_per_second=len(successful) * 1e9 / (end - start) if end > start else 0,
        sustainable_qps='UNMEASURED', matched_vendor_comparison=False, physical_wire_bytes='UNMEASURED',
        actual_process_starts=sum(r.get('native_process_started', False) for r in records),
        actual_http_attempts=sum(r['http_attempts'] for r in records),
        process_transport_totals={k: sum(r['accounting']['final_process_transport'][k] for r in successful)
            for k in ('attempts', 'consumed_payload_bytes', 'transport_failures', 'stream_failures')},
        process_transport_totals_scope='successful process startup plus first query; failed raw ledgers retained separately',
        native_peak_rss_bytes=max((r['resources']['rss_peak_bytes'] for r in successful), default=0),
        scientific_recall_at_100='separate sealed scientific a0002; no cold top100 measurement',
        process_cache='empty for each HTTP process', s3_service_cache='uncontrolled',
        host_page_cache='uncontrolled; no cache drop; native executables may be resident after acquisition',
        credential_protocol=stats.CREDENTIAL_PROTOCOL,
        cold_call_boundary='preencoded body; before launch through first wire response, including first TCP connect',
        imds_cold_scope='native process S3 credentials; Python staging client has separate reused credentials',
        native_cpu_affinity=[0, 1, 2, 3], client_cpu_affinity=[4, 5], concurrency=1)


def run(config_path, expected_sha, repo, output):
    repo, output = retained.regular_path(repo), retained.regular_path(output)
    assert not output.exists() and not output.is_relative_to(repo), 'fresh external output required'
    config, assets, historical = qualify(config_path, expected_sha, repo)
    output.mkdir(parents=True)
    scratch = output / 'scratch'
    (scratch / 'native').mkdir(parents=True)
    write(output / 'config.json', Path(config_path).read_bytes())
    qualification = dict(execution_source=config['execution_source'],
        current_executor_sha256=config['code_sha256'], original_qualification_source_identity=driver.SOURCE_ID,
        actual_full_workspace_execution=False, historical_science=config['proofs']['historical_validation'],
        native_rebuilt=False, build_invocations=0, scientific_scorer_invocations=0, oracle_invocations=0)
    report = dict(build_invocations=0, scientific_scorer_invocations=0, oracle_invocations=0,
        publication_invocations=0, cold_invocations=0, shared_hardlinks_accounted=True)
    started = time.monotonic()
    deadline = started + config['resources']['service_limit_seconds']
    records, failure, summary, s3 = [], None, None, None
    reducer_error = None
    def terminated(signum, frame):
        raise TimeoutError('runtime termination signal')
    previous = signal.signal(signal.SIGTERM, terminated)
    try:
        assert os.sched_getaffinity(0) == {4, 5}, 'client CPU affinity'
        assert all(os.environ.get(n) == '2' for n in retained.THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS') == '1'
        import boto3
        from botocore.config import Config
        qualification['sdk'] = sdk_guard()
        s3 = boto3.client('s3', region_name=config['region'], config=Config(retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=5))
        with observe(output, config['resources'], report, deadline) as checkpoint, (output / 'sdk-ledger.jsonl').open('xb') as ledger:
            checkpoint(sum(p['bytes'] for p in assets.values()) + config['resources']['output_reserve_bytes'])
            for name, pin in assets.items():
                stage_asset(s3, pin, repo, scratch / 'assets' / name, ledger, checkpoint, deadline)
            q = dict(directory=str(scratch / 'assets/qualification'), files={n.split('qualification/', 1)[1]: library.identity(p)
                for n, p in assets.items() if n.startswith('qualification/')})
            native = driver.native_authority(dict(qualification=q), repo)
            report['native_qualification'] = native
            original_root = scratch / 'assets/generation/manifest.json'
            assert json.loads(original_root.read_bytes()) == historical['scientific_original_root']
            panel = scratch / 'assets/panel'
            derived, references, truth = panel_inputs(panel)
            scientific_hits = {k: sum(len(set(r['ids'][:k]) & set(t[:k])) for r, t in zip(references, truth)) for k in (10, 100)}
            write(output / 'scientific-reference.json', dict(scope='authenticated closed scientific a0002, separate from cold top10',
                requests=artifact(panel / 'requests.jsonl'), records=artifact(panel / 'records.jsonl'), truth=artifact(panel / 'truth.i64'),
                returned_hits10=scientific_hits[10], denominator10=640, recall_at_10=scientific_hits[10] / 640,
                returned_hits100=scientific_hits[100], denominator100=6400, recall_at_100=scientific_hits[100] / 6400,
                measurement_rerun=False))
            assert artifact(panel / 'source-order.u64') == library.identity(assets['panel/source-order.u64'])
            write(output / 'publisher-requests.jsonl', derived)
            write(output / 'request-derivative.json', dict(original=artifact(panel / 'requests.jsonl'),
                derivative=artifact(output / 'publisher-requests.jsonl'), only_ordinal_key_changed=True, f32_bits_equal=True))
            write(output / 'input-hashes.json', assets)
            for reference in references:
                assert 0 < reference['router_submitted_gets'] <= 48 and 0 < reference['router_verified_bytes'] <= LEAF_BYTES
            report['publication_invocations'] = 1
            publication_deadline = min(deadline, time.monotonic() + config['resources']['publication_limit_seconds'])
            arm = publish_generation(s3, config, scratch, output, references, ledger, checkpoint, publication_deadline)
            write(output / 'sealed-reference-k10.jsonl', b''.join(encoded(dict(r, ids=r['ids'][:10])) + b'\n' for r in references))
            binary = scratch / 'assets/qualification/binaries/two_bit_http'
            binary.chmod(0o500)
            bodies = [library.http_request(json.loads(row)['query'], arm['authority']) for row in derived.splitlines()]
            expected = [dict(r, ids=r['ids'][:10]) for r in references]
            cold_deadline = min(deadline, time.monotonic() + config['resources']['cold_limit_seconds'])
            start = time.monotonic_ns()
            with (output / 'records.jsonl').open('xb') as stream:
                for q in range(64):
                    checkpoint()
                    assert time.monotonic() < cold_deadline, 'cold panel deadline'
                    assert artifact(binary) == library.identity(assets['qualification/binaries/two_bit_http']), 'binary bytes drift'
                    report['cold_invocations'] += 1
                    row = measured_call(binary, config, arm, bodies[q], expected[q], truth[q], scratch)
                    records.append(row)
                    stream.write(encoded(row) + b'\n')
                    stream.flush()
                    os.fsync(stream.fileno())
                    if row['outcome'] != 'success':
                        records.extend(library.aborted_rows('fatal cold call; no retry', q + 1))
                        break
                    checkpoint()
            end = time.monotonic_ns()
            try:
                summary = reduce_records(records, start, end)
            except BaseException as error:
                reducer_error = error
                raise
            assert summary['execution_gate_passed'], 'cold execution failed'
    except BaseException as error:
        failure = error
        records.extend(library.aborted_rows('fatal ' + type(error).__name__, len(records)))
        if summary is None and reducer_error is None:
            try:
                summary = reduce_records(records, 0, 0)
            except BaseException as reduction_error:
                reducer_error = reduction_error
        # Closure metadata only when reduction fails; never manufacture metrics.
        summary = dict(summary or dict(schema=SCHEMA + '-result', dataset='CoHere', closed=True,
            offered_queries=64, denominator10=640), status='EXECUTION_FAILED', scientific_disposition='INVALID',
            execution_gate_passed=False, quality_gate_passed=False, error_type=type(error).__name__, error=str(error))
        if reducer_error is not None:
            summary['reducer_error'] = dict(error_type=type(reducer_error).__name__, error=str(reducer_error))
    finally:
        cleanup_error = None
        try:
            if s3 is not None:
                s3.close()
            shutil.rmtree(scratch)
            assert not scratch.exists(), 'owned scratch remains'
        except BaseException as error:
            cleanup_error = error
            summary = dict(summary or {}, status='EXECUTION_FAILED', scientific_disposition='INVALID', execution_gate_passed=False,
                quality_gate_passed=False, cleanup_error=str(error))
        process_cleanup = cleanup_error is None and all(r.get('temporary_directory_cleanup', True) and
            (not r.get('native_process_started') or r.get('native_close', {}).get('process_group_closed') is True) for r in records)
        if not process_cleanup:
            summary.update(status='EXECUTION_FAILED', scientific_disposition='INVALID', execution_gate_passed=False,
                quality_gate_passed=False, process_cleanup_error='native process closure not proven')
        write(output / 'source-qualification.json', qualification)
        report.update(wall_seconds=time.monotonic() - started, process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024)
        write(output / 'resources.json', report)
        write(output / 'cleanup.json', dict(valid=cleanup_error is None, scratch_removed=not scratch.exists(),
            process_cleanup=process_cleanup,
            remote_namespace_retained=config['namespace_prefix'], build_invocations=0, scientific_scorer_invocations=0,
            oracle_invocations=0, publication_invocations=report['publication_invocations'], cold_invocations=report['cold_invocations']))
        with (output / 'records.jsonl').open('ab') as stream:
            recorded = len((output / 'records.jsonl').read_bytes().splitlines())
            stream.write(b''.join(encoded(r) + b'\n' for r in records[recorded:]))
            stream.flush()
            os.fsync(stream.fileno())
        files = {str(p.relative_to(output)): artifact(p) for p in output.rglob('*') if p.is_file()}
        usage = science.scratch_usage(output)
        reserve = len(encoded(summary)) + len(encoded(files)) + 4 * 4096
        if max(usage['unique_inode_bytes'], usage['physical_allocated_bytes']) + reserve > config['resources']['scratch_bytes']:
            summary.update(status='EXECUTION_FAILED', scientific_disposition='INVALID', execution_gate_passed=False, quality_gate_passed=False,
                resource_error='final artifact scratch admission')
            failure = failure or ValueError('final artifact scratch admission')
        write(output / 'summary.json', summary)
        files['summary.json'] = artifact(output / 'summary.json')
        write(output / 'COMPLETE.json', dict(schema=SCHEMA + '-complete', files=files,
            config_sha256=expected_sha, status=summary['status'], passed=summary.get('quality_gate_passed', False) and process_cleanup))
        signal.signal(signal.SIGTERM, previous)
        usage = science.scratch_usage(output)
        if max(usage['unique_inode_bytes'], usage['physical_allocated_bytes']) > config['resources']['scratch_bytes']:
            failure = failure or AssertionError('final whole-output scratch')
        failure = failure or cleanup_error
    if failure is not None:
        raise failure
    return summary


def closed_panel_admission_check():
    """Authenticate and parse the existing panel; never execute or rescore it."""
    repo = Path(__file__).resolve().parents[1]
    config = json.loads((repo / BASE / 'cold-http/a0002/config.json').read_bytes())
    assets, historical = historical_assets(repo, read_proofs(repo, config['proofs']))
    directory = repo / SCIENTIFIC / 'screen'
    before = {name: artifact(directory / name) for name in ('requests.jsonl', 'records.jsonl', 'truth.i64')}
    assert all(pin == library.identity(assets['panel/' + name]) for name, pin in before.items())
    requests = [json.loads(row) for row in (directory / 'requests.jsonl').read_bytes().splitlines()]
    norms = [sum(v * v for v in row['query']) for row in requests]
    assert all(norm > 1 for norm in norms), 'regression requires original nonunit queries'
    derived, references, truth = panel_inputs(directory)
    assert len(derived.splitlines()) == len(references) == len(truth) == 64
    for original, row in zip(requests, derived.splitlines()):
        value = json.loads(row)
        assert value == dict(query_ordinal=original['ordinal'], query=original['query'])
        assert struct.pack('<768f', *value['query']) == struct.pack('<768f', *original['query'])
    assert before == {name: artifact(directory / name) for name in before}
    return dict(panel=before, execution_source=historical['historical_validation']['execution_source'],
        queries=64, f32_bits_equal=True, json_numeric_values_equal=True,
        squared_norm_min=min(norms), squared_norm_max=max(norms), query_execution=False, quality_recomputed=False)


def sdk_admission_check():
    """Exercise real botocore shapes and reject an old model before cloud entry."""
    import boto3
    from botocore.exceptions import ParamValidationError
    from botocore.model import ServiceModel
    from botocore.session import Session, get_session
    from botocore.validate import validate_parameters
    model = get_session().get_component('data_loader').load_service_model('s3', 'service-2')
    old = copy.deepcopy(model)
    shape = old['operations']['PutObject']['input']['shape']
    old['shapes'][shape]['members'].pop('IfNoneMatch')
    old = ServiceModel(old)
    params = dict(Bucket='offline', Key='object', Body=b'body', IfNoneMatch='*')
    try:
        validate_parameters(params, old.operation_model('PutObject').input_shape)
    except ParamValidationError as error:
        assert 'IfNoneMatch' in str(error)
    else:
        raise AssertionError('old service model accepted conditional PUT')
    validate_parameters(params, ServiceModel(model).operation_model('PutObject').input_shape)
    capability = sdk_guard(ServiceModel(model))
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        config = dict(FIXED, execution_source={}, code_sha256={},
            proofs=dict(historical_validation={}), resources=dict(HOST, service_limit_seconds=10),
            namespace_prefix='offline', region='eu-central-1')
        path, output = root / 'config.json', root / 'output'
        write(path, config)
        with (patch.object(Session, 'get_service_model', return_value=old),
                patch.object(boto3, 'client', side_effect=AssertionError('cloud forbidden')) as client,
                patch.object(sys.modules[__name__], 'qualify', return_value=(config, {}, {})),
                patch.object(sys.modules[__name__], 'stage_asset', side_effect=AssertionError('download forbidden')) as download,
                patch.object(sys.modules[__name__], 'publish_generation', side_effect=AssertionError('publication forbidden')) as publish,
                patch.object(os, 'sched_getaffinity', return_value={4, 5}),
                patch.dict(os.environ, dict.fromkeys(retained.THREAD_ENV, '2') | {'AWS_MAX_ATTEMPTS': '1'})):
            try:
                run(path, artifact(path)['sha256'], Path(__file__).resolve().parents[1], output)
            except AssertionError as error:
                assert str(error).startswith('SDK lacks PutObject.IfNoneMatch:')
                assert boto3.__version__ in str(error) and sys.executable in str(error)
            else:
                raise AssertionError('old SDK entered runtime')
            client.assert_not_called(); download.assert_not_called(); publish.assert_not_called()
        summary = json.loads((output / 'summary.json').read_bytes())
        assert summary['status'] == 'EXECUTION_FAILED' and summary['error'].startswith('SDK lacks PutObject.IfNoneMatch:')
        assert not (output / 'scratch').exists() and not json.loads((output / 'COMPLETE.json').read_bytes())['passed']
        cleanup = json.loads((output / 'cleanup.json').read_bytes())
        assert cleanup['process_cleanup'] and cleanup['publication_invocations'] == cleanup['cold_invocations'] == 0
        assert len((output / 'records.jsonl').read_bytes().splitlines()) == 64
    return dict(old_model_rejected=True, current_model_admitted=True, runtime_cloud_calls=0, sdk=capability)


def failure_reducer_check():
    """A failing error-path reducer must not mask the original runtime failure."""
    original = RuntimeError('original admission failure')
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        config = dict(FIXED, execution_source={}, code_sha256={}, proofs=dict(historical_validation={}),
            resources=dict(HOST, service_limit_seconds=10), namespace_prefix='offline')
        path, output = root / 'config.json', root / 'output'
        write(path, config)
        with (patch.object(sys.modules[__name__], 'qualify', return_value=(config, {}, {})),
                patch.object(sys.modules[__name__], 'sdk_guard', side_effect=original),
                patch.object(sys.modules[__name__], 'reduce_records', side_effect=ValueError('reducer rejected')) as reducer,
                patch.object(os, 'sched_getaffinity', return_value={4, 5}),
                patch.dict(os.environ, dict.fromkeys(retained.THREAD_ENV, '2') | {'AWS_MAX_ATTEMPTS': '1'})):
            try:
                run(path, artifact(path)['sha256'], Path(__file__).resolve().parents[1], output)
            except BaseException as error:
                assert error is original, f'original error masked by {type(error).__name__}: {error}'
            else:
                raise AssertionError('runtime failure accepted')
            reducer.assert_called_once()
        summary = json.loads((output / 'summary.json').read_bytes())
        assert summary['error'] == str(original) and summary['error_type'] == 'RuntimeError'
        assert summary['reducer_error'] == dict(error_type='ValueError', error='reducer rejected')
        assert summary['status'] == 'EXECUTION_FAILED' and summary['scientific_disposition'] == 'INVALID'
        assert not summary['execution_gate_passed'] and not summary['quality_gate_passed']
        assert len((output / 'records.jsonl').read_bytes().splitlines()) == 64
        assert json.loads((output / 'cleanup.json').read_bytes())['process_cleanup']
        marker = json.loads((output / 'COMPLETE.json').read_bytes())
        assert not marker['passed'] and not (output / 'scratch').exists()
        for name in ('records.jsonl', 'resources.json', 'cleanup.json', 'summary.json'):
            assert marker['files'][name] == artifact(output / name)
    return dict(original_failure_retained=True, reducer_failure_retained=True, ledger_rows=64, closure_artifacts=True)


def self_check():
    import boto3
    def write(path, value):
        Path(path).write_bytes(value if isinstance(value, bytes) else encoded(value) + b'\n')
    sdk_admission = sdk_admission_check()
    closed_panel = closed_panel_admission_check()
    scratch_ownership = cold.scratch_ownership_check()
    failure_reducer = failure_reducer_check()
    # The check must catch query rewriting, ordinal, ID and physical-plan drift.
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        requests, frozen, truth = [], [], []
        for q in range(64):
            query = [0.0] * 768
            query[q] = 2.5 + q / 64
            query[(q + 1) % 768] = -0.125
            query[(q + 2) % 768] = -0.0
            requests.append(dict(ordinal=q, query=query))
            returned = list(range(100))
            truth.append(list(range(100)))
            frozen.append(dict(phase='frozen_query', ordinal=q, returned_ids=returned,
                ranges=[dict(start=0, end=199680)], sq8_bytes=199680, sq8_gets=1,
                source_gets=1, source_bytes=51200, leaf_gets=48, leaf_bytes=48 * 1540,
                truth_opened=False))
        request_body = b''.join(json.dumps(r).encode() + b'\n' for r in requests)
        (directory / 'requests.jsonl').write_bytes(request_body)
        (directory / 'truth.i64').write_bytes(b''.join(struct.pack('<100q', *t) for t in truth))
        (directory / 'records.jsonl').write_bytes(b''.join(json.dumps(r).encode() + b'\n' for r in frozen))
        derived, references, truths = panel_inputs(directory)
        assert len(derived.splitlines()) == len(references) == len(truths) == 64
        for original, row in zip(requests, derived.splitlines()):
            value = json.loads(row)
            assert value['query_ordinal'] == original['ordinal']
            assert value['query'] == original['query']
            assert struct.pack('<768f', *value['query']) == struct.pack('<768f', *original['query'])
        assert references[0]['ids'] == list(range(100)) and truths == truth
        original_records = (directory / 'records.jsonl').read_bytes()
        # Unit vectors are also valid; neither large nor subnormal f32 values
        # may be normalized, rounded in JSON, or rejected because of their norm.
        for value in (1.0, 2 ** -149, 3.4028234663852886e38):
            changed = copy.deepcopy(requests)
            changed[0]['query'] = [value] + [0.0] * 767
            (directory / 'requests.jsonl').write_bytes(b''.join(encoded(r) + b'\n' for r in changed))
            admitted = json.loads(panel_inputs(directory)[0].splitlines()[0])['query']
            assert admitted == changed[0]['query']
            assert struct.pack('<768f', *admitted) == struct.pack('<768f', *changed[0]['query'])
        for field, value in (
                ('query', [0.0] * 767), ('query', [0.0] * 768),
                ('query', [1e-50] * 768), ('query', [1e39] + [0.0] * 767),
                ('query', [float('nan')] + [0.0] * 767),
                ('query', [float('inf')] + [0.0] * 767),
                ('query', [-float('inf')] + [0.0] * 767),
                ('ordinal', 1), ('ordinal', 0.0), ('ordinal', False)):
            changed = copy.deepcopy(requests)
            changed[0][field] = value
            (directory / 'requests.jsonl').write_bytes(b''.join(json.dumps(r).encode() + b'\n' for r in changed))
            try:
                panel_inputs(directory)
            except (AssertionError, OverflowError):
                pass
            else:
                raise AssertionError('invalid query/ordinal accepted: ' + field)
        (directory / 'requests.jsonl').write_bytes(request_body)

        files = {'manifest.json': b'', 'page_manifest.json': b'{}', 'plane/manifest.json': b'{}',
            'page_digests.bin': 3907 * 32, 'plane/mean.bin': 3072, 'plane/page_digests.bin': 1_000_000,
            'router/root.bin': 512, 'router/membership.bin': 125000, 'router/leaves.bin': 31250 * 1540,
            'canonical.bin': b'canonical fixture', 'centroids.bin': b'centroids fixture', 'plane/records.bin': b'plane fixture'}
        fixture = directory / 'fixture'
        for name, value in files.items():
            path = fixture / 'generation' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if type(value) is int:
                with path.open('wb') as stream:
                    stream.truncate(value)
            else:
                path.write_bytes(value)
        physical_ids = list(reversed(range(100)))
        sq8 = b''.join(struct.pack('<qf', n, 1.0) + bytes(768) for n in physical_ids)
        (fixture / 'sq8.bin').write_bytes(sq8)
        panel = fixture / 'panel'
        panel.mkdir()
        (panel / 'requests.jsonl').write_bytes(request_body)
        (panel / 'records.jsonl').write_bytes(original_records)
        (panel / 'truth.i64').write_bytes((directory / 'truth.i64').read_bytes())
        (panel / 'source-order.u64').write_bytes(struct.pack('<100Q', *physical_ids))
        check = library.quality.ordinal_check(fixture / 'sq8.bin', panel / 'source-order.u64', rows=100, dimensions=768)
        assert check['sq8'] == artifact(fixture / 'sq8.bin') and check['order'] == artifact(panel / 'source-order.u64')
        (fixture / 'sq8.bin').write_bytes(struct.pack('<q', 0) + sq8[8:])
        try:
            library.quality.ordinal_check(fixture / 'sq8.bin', panel / 'source-order.u64', rows=100, dimensions=768)
        except AssertionError:
            pass
        else:
            raise AssertionError('physical ID tamper accepted')
        (fixture / 'sq8.bin').write_bytes(sq8)
        discovery = dict(mode='semantic', profile='fresh1m')
        for label in ('root', 'membership', 'leaves'):
            pin = artifact(fixture / 'generation/router' / (label + '.bin'))
            discovery[label + '_bytes'], discovery[label + '_sha256'] = pin['bytes'], pin['sha256']
        original = dict(schema='borsuk-two-bit-generation-v8', generation=1, base_epoch=0,
            discovery=discovery, canonical=dict(artifact(fixture / 'generation/canonical.bin'), object_key='old/canonical'),
            sq8_object_key='old/sq8', sq8_etag='"old-file-etag"', sq8_object_sha256=sha(sq8), low=[0] * 768, step=[1] * 768)
        write(fixture / 'generation/manifest.json', original)
        changed = transport_manifest(original, 'new/owned', '"s3-etag"')
        assert original['canonical']['object_key'] == 'old/canonical'
        assert changed['canonical']['object_key'].startswith('new/owned/objects/') and changed['low'] == original['low']
        # ARTIFACTS is selected by execution_mode in production; the closed fixed48 roster is explicit here.
        names = ('source-qualification.json', 'config.json', 'native-source-manifest.json', 'source-before.json',
            'source-after.json', 'workspace-receipt.json', 'test.log', 'test-resources.txt', 'workspace-cgroup.json',
            'cpu.txt', 'rustc-version.txt', 'cargo-version.txt', 'run-closed.log', 'binaries/two_bit_http',
            'binaries/check_semantic_router_scorer', 'binaries/two_bit_plan_demo', 'aws-reservation.json',
            'aws-closeout.json', 'aws-terminal.json', 'collection-replay.json', 'root-verification.json')
        for name in names:
            path = fixture / 'qualification' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(('qualified fixture ' + name).encode())
        assets = {str(p.relative_to(fixture)): dict(artifact(p), source=dict(bucket='fixture', key=str(p.relative_to(fixture))))
            for p in fixture.rglob('*') if p.is_file()}
        assert len(assets) == 38
        config = dict(FIXED, bucket='fixture', namespace_prefix='new/owned', resources=dict(HOST,
            publication_limit_seconds=20, cold_limit_seconds=20, service_limit_seconds=50, output_reserve_bytes=16 << 20))
        config_path = directory / 'config.json'
        write(config_path, config)
        cgroup = dict(cgroup='/fixture', observer_pid=123, process_ids=[123],
            **{'memory.max': str(HOST['shared_memory_bytes']), 'memory.peak': '1024', 'memory.swap.max': '0',
                'memory.swap.peak': '0', 'cpu.max': '200000 100000', 'pids.max': '512', 'pids.current': '1',
                'memory.events': 'oom 0\noom_kill 0\nmax 0', 'memory.swap.events': 'max 0', 'pids.events': 'max 0'})
        native_time = 'Maximum resident set size (kbytes): 100\nUser time (seconds): 0.01\nSystem time (seconds): 0.01\n'

        class Events:
            def __init__(self):
                self.handlers = {}
            def register(self, event, callback, unique_id):
                self.handlers[event, unique_id] = callback
            def unregister(self, event, unique_id):
                self.handlers.pop((event, unique_id))
            def emit(self, event, **kwargs):
                for (name, unused), handler in list(self.handlers.items()):
                    if event == name:
                        handler(**kwargs)

        class SDK:
            def __init__(self, mode):
                self.meta = SimpleNamespace(events=Events())
                self.objects = {key: fixture / key for key in assets}
                self.calls, self.mode, self.streams = [], mode, []
                self.closed = False
            def close(self):
                self.closed = True
            def response(self, method, key, **values):
                self.calls.append((method, key))
                self.meta.events.emit('before-send.s3')
                self.meta.events.emit('needs-retry.s3', response=(SimpleNamespace(status_code=200), {}))
                return dict(ResponseMetadata=dict(HTTPStatusCode=200, RetryAttempts=int(self.mode == 'sdk-retry')), **values)
            def get_object(self, Bucket, Key, IfMatch=None):
                value = self.objects[Key]
                stream = value.open('rb') if isinstance(value, Path) else io.BytesIO(value)
                size = value.stat().st_size if isinstance(value, Path) else len(value)
                if self.mode == 'body-tamper' and Key == 'generation/manifest.json':
                    stream.close()
                    stream = io.BytesIO(b'x' * size)
                self.streams.append(stream)
                return self.response('GET', Key, Body=stream, ContentLength=size, ETag='"s3-etag"')
            def head_object(self, Bucket, Key):
                value = self.objects[Key]
                return self.response('HEAD', Key, ContentLength=value.stat().st_size if isinstance(value, Path) else len(value), ETag='"s3-etag"')
            def put_object(self, Bucket, Key, Body, IfNoneMatch):
                assert IfNoneMatch == '*' and Key not in self.objects and Key.startswith('new/owned/objects/')
                self.objects[Key] = Body.read()
                return self.response('PUT', Key)

        class Client:
            def __init__(self, *args, **kwargs):
                pass
            def connect(self):
                pass
            def close(self):
                pass

        def transport_report(gets, heads, puts, consumed, query_gets=0):
            attempts = gets + heads + puts
            return dict(schema='borsuk-native-transport-v1', scope='process_all_native_s3_readers', per_query_delta=False,
                attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests', method_order=stats.METHODS,
                status_counts_format='[http_status,count] nonzero entries',
                payload_measurement='consumed response data frames, including unauthenticated payload', unknown=stats.UNKNOWN,
                dropped_error_body_consumed_bytes=0,
                totals=dict(attempts=attempts, method_counts=[gets, heads, puts] + [0] * 7,
                    status_counts=[[200, attempts - query_gets]] + ([[206, query_gets]] if query_gets else []),
                    transport_failures=0, stream_failures=0, consumed_payload_bytes=consumed, dropped_error_bodies=0))

        def header_for(arm):
            rows = []
            for index, name in enumerate(library.STARTUP):
                size = arm['metadata_files'][name]
                reused = index == 0
                rows.append(dict(name=name, bytes=0 if reused else size, chunks=0 if reused else 1,
                    logical_get_requests=0 if reused else 1, logical_head_requests=int(not reused and name not in library.EXACT),
                    payload_buffer_bound_bytes=0 if reused else size, metadata_wave=0 if reused else 1,
                    metadata_wave_wall_ns=10, head_wall_ns=0, get_wall_ns=0, stream_wall_ns=0, write_wall_ns=0,
                    local_auth_wall_ns=int(reused), local_copy_wall_ns=int(reused),
                    reused_root_bytes=size if reused else 0, retained_root_bytes=size if reused else 0))
            consumed = sum(arm['metadata_files'].values()) + arm['head_file']['bytes'] + 64
            return dict(phase='ready', authority=arm['authority'], listen='127.0.0.1:8080', remote_open_wall_ns=100,
                head_read_wall_ns=100, transport=transport_report(11, 4, 1, consumed),
                remote_open_stats=dict(metadata=rows, staging_wall_ns=20, decode_wall_ns=1, source_head_wall_ns=1,
                    router_head_wall_ns=1, source_head_requests=1, router_head_requests=1))

        real_measured_call = measured_call
        def concurrent_calls_check(arm, response, *, concurrent=False):
            # Two real Python threads must overlap without patching call globals.
            scratch = directory / ('concurrent' if concurrent else 'serial-drain-guard')
            (scratch / 'native').mkdir(parents=True)
            barrier, a_done = threading.Barrier(2, timeout=5), threading.Event()
            a_before, lock, live = threading.Event(), threading.Lock(), set()
            owners, results, errors, posts, stops = {}, {}, {}, [], []
            ports = (18080, 18081)
            def globals_now():
                return (library.NATIVE, library.native_budget_model, cold.stop, library.close_native,
                    tempfile.tempdir, tempfile.TemporaryDirectory, dict(os.environ))
            original_globals = globals_now()
            def snapshot():
                value = copy.deepcopy(cgroup)
                with lock:
                    value['process_ids'] = [123, *sorted(live)]
                value['pids.current'] = str(len(value['process_ids']))
                a_before.set()
                return value
            class ConcurrentClient(Client):
                def __init__(self, host, port, **kwargs):
                    assert host == '127.0.0.1' and port in ports
                    self.port = port
            def spawn(command, **kwargs):
                port = int(command[-1].split(':')[-1])
                assert port in ports and port not in owners
                owned = Path(kwargs['stdout'].name).parent
                assert kwargs['env']['TMPDIR'] == str(owned) and owned.parent == scratch / 'native'
                assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES'] == str(HOST['native_memory_bytes'])
                owners[port] = owned
                (owned / 'payload').write_text(str(port))
                header = dict(header_for(arm), listen=f'127.0.0.1:{port}')
                kwargs['stdout'].write(json.dumps(header) + '\n')
                kwargs['stdout'].flush()
                write(Path(command[command.index('-o') + 1]), native_time.encode())
                with lock:
                    live.add(port)
                return SimpleNamespace(pid=port, poll=lambda: None, returncode=-15, wait=lambda **kw: -15)
            def post(client, body):
                port = client.port
                assert body == library.http_request(requests[port - ports[0]]['query'], arm['authority'])
                posts.append(port)
                barrier.wait()
                assert len(owners) == 2 and owners[ports[0]] != owners[ports[1]]
                assert globals_now() == original_globals, 'measured_call mutated shared globals'
                if port == ports[1]:
                    assert a_done.wait(5), 'A did not close while B was alive'
                    assert not owners[ports[0]].exists() and stops == [ports[0]]
                    assert (owners[port] / 'payload').read_text() == str(port), 'A deleted B scratch'
                    assert globals_now() == original_globals
                return 200, encoded(dict(response, query_ordinal=port - ports[0]))
            def stop(process):
                assert (owners[process.pid] / 'payload').read_text() == str(process.pid)
                if process.pid == ports[0]:
                    assert owners[ports[1]].exists() and not a_done.is_set()
                stops.append(process.pid)
                with lock:
                    live.remove(process.pid)
                return dict(returncode=-15, intentional_stop=True)
            def killpg(pid, sig):
                assert pid in stops
                if sig == 0:
                    raise ProcessLookupError()
            def call(port):
                q = port - ports[0]
                try:
                    if port == ports[1]:
                        assert a_before.wait(5), 'A did not take its before snapshot'
                    results[port] = real_measured_call('fixture', config, arm,
                        library.http_request(requests[q]['query'], arm['authority']),
                        dict(references[q], ids=references[q]['ids'][:10]), truths[q], scratch, port=port,
                        **({'require_cgroup_drained': False} if concurrent else {}))
                except Exception as error:
                    errors[port] = repr(error)
                finally:
                    if port == ports[0]:
                        a_done.set()
            with (patch.object(sys.modules[__name__], 'snapshot', side_effect=snapshot),
                    patch.object(subprocess, 'Popen', side_effect=spawn),
                    patch.object(cold.http.client, 'HTTPConnection', ConcurrentClient),
                    patch.object(cold, 'post', side_effect=post),
                    patch.object(library, 'native_cpu', side_effect=lambda *a: dict(pid=42, start_ticks=1, user_ticks=1, system_ticks=1)),
                    patch.object(library, 'cold_stop', side_effect=stop), patch.object(os, 'killpg', side_effect=killpg)):
                threads = [threading.Thread(target=call, args=(port,)) for port in ports]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(6)
                assert all(not thread.is_alive() for thread in threads), 'concurrent calls hung'
            assert not errors, errors
            assert globals_now() == original_globals
            assert sorted(posts) == list(ports) and stops == list(ports), (posts, stops)
            assert results[ports[0]]['cgroup_before']['process_ids'] == [123]
            assert results[ports[0]]['cgroup_after']['process_ids'] == [123, ports[1]], 'missing live peer snapshot'
            for port, row in results.items():
                if port == ports[0] and not concurrent:
                    assert row['outcome'] == 'failed' and row['error'] == 'remaining descendants', row
                else:
                    assert row['outcome'] == 'success', row
                assert row['http_attempts'] == row['valid_ann_requests'] == 1
                assert row['returned_hits'] == 10 and row['response']['ids'] == list(range(10))
                assert row['accounting']['query_transport_submissions'] == 50
                assert row['native_close']['process_group_closed'] and row['temporary_directory_cleanup']
                assert Path(row['child_tmpdir']) == owners[port] and not owners[port].exists()
            # The offered harness must close the entire cell after all owners finish.
            check_cgroup(cgroup, snapshot(), config['resources'], drained=True)
            for invalid in (None, 0, 1, 'false'):
                try:
                    real_measured_call('fixture', config, arm, b'', references[0], truths[0], scratch,
                        require_cgroup_drained=invalid)
                except (AssertionError, ValueError):
                    pass
                else:
                    raise AssertionError('non-bool drain policy admitted')
            # Injected budgets reject invalid types, geometry, slot counts and insufficient admission.
            model = native_budget_model(arm['metadata_files'], retained_root=True)
            for memory, injected in [
                    (value, model) for value in (0, -1, True, 2.0, '2147483648', model['modeled_remote_payload_bytes'] - 1)
                    ] + [(HOST['native_memory_bytes'], dict(model, **{key: value})) for key, value in (
                        ('modeled_remote_payload_bytes', True), ('modeled_remote_payload_bytes', -1),
                        ('modeled_remote_payload_bytes', 1.5), ('server_query_slots', 1),
                        ('metadata_bytes', model['metadata_bytes'] + 1), ('root_bytes', model['root_bytes'] + 1))]:
                try:
                    library.validate_roster(arm, native_memory_bytes=memory, budget_model=injected)
                except (AssertionError, ValueError):
                    pass
                else:
                    raise AssertionError('invalid injected memory/model admitted')
            library.validate_roster(arm, native_memory_bytes=model['modeled_remote_payload_bytes'], budget_model=model)
            return dict(real_python_threads=2, distinct_ports=list(ports), posts=2, stops=2,
                a_closed_while_b_alive=True, shared_globals_unchanged=True, injected_budget_negatives=True,
                explicit_concurrent_policy=concurrent, default_rejects_live_peer=not concurrent, final_cell_drained=True)

        for mode in ('success', 'http-failure', 'id-tamper', 'publication-tamper', 'body-tamper', 'sdk-retry', 'undrained', 'scratch', 'cleanup-proof'):
            sdk, calls, state = SDK(mode), [], {}
            dest = directory / ('out-' + mode)
            def publisher(command, log, seconds, timing):
                assert command[-4:] == ['--panel-count', '64', '--top-k', '100']
                assert command[0:3] == ['taskset', '-c', '0-3'] and '--live-s3' in command
                index = command.index(str(dest / 'scratch/assets/qualification/binaries/two_bit_plan_demo'))
                root = Path(command[index + 1])
                assert artifact(root / 'manifest.json')['sha256'] == command[index + 2]
                assert artifact(command[index + 3])['sha256'] == command[index + 4]
                assert Path(command[index + 3]).read_bytes() == derived, 'lossless publisher panel'
                manifest = json.loads((root / 'manifest.json').read_bytes())
                assert manifest == transport_manifest(original, config['namespace_prefix'], '"s3-etag"')
                head = encoded(dict(schema='borsuk-two-bit-head-v2', generation=1, epoch=1,
                    root_sha256=command[index + 2], mutation=None, fence=None))
                sdk.objects['new/owned/head.json'] = head
                for name in library.STARTUP:
                    sdk.objects['new/owned/generations/' + command[index + 2] + '/' + name] = root / name
                sdk.objects[manifest['canonical']['object_key']] = root / 'canonical.bin'
                arm, unused = library.publication_arm(root, 'new/owned', head,
                    native_memory_bytes=HOST['native_memory_bytes'],
                    budget_model=native_budget_model({n: artifact(root / n)['bytes'] for n in library.STARTUP}, retained_root=True))
                state['arm'] = arm
                rows = [dict(phase='startup', top_k=100, declared_panel_count=64, publish_wall_ns=10, remote_open_wall_ns=10, **arm['authority'])]
                rows += [dict(r, phase='query') for r in references]
                if mode == 'publication-tamper':
                    rows[1] = dict(rows[1], ids=list(reversed(rows[1]['ids'])))
                rows.append(dict(phase='summary', count=64, top_k=100, measurement_wall_ns=10))
                write(Path(command[index + 5]), b''.join(encoded(r) + b'\n' for r in rows))
                write(timing, native_time.encode())
                write(log, b'production library boundary fixture\n')
                calls.append('publication')
                return dict(exit_status=0, process_cleanup=True)
            def spawn(command, **kwargs):
                q = len([v for v in calls if type(v) is int])
                calls.append(q)
                state['q'] = q
                assert command[-1] == '127.0.0.1:8080' and 'prlimit' in command
                assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES'] == str(HOST['native_memory_bytes'])
                owned = Path(kwargs['stdout'].name).parent
                assert Path(kwargs['env']['TMPDIR']) == owned and owned.parent == dest / 'scratch/native'
                (owned / 'child-scratch').mkdir()
                (owned / 'child-scratch/payload').write_bytes(b'terminated child leftovers')
                # An unrelated live owner must not make this call's cleanup fail.
                (owned.parent / 'other-owner').mkdir(exist_ok=True)
                header = header_for(state['arm'])
                kwargs['stdout'].write(json.dumps(header) + '\n')
                kwargs['stdout'].flush()
                write(Path(command[command.index('-o') + 1]), native_time.encode())
                return SimpleNamespace(pid=999999999, poll=lambda: None)
            def post(client, body):
                q, arm = state['q'], state['arm']
                payload = json.loads(body)
                assert payload == dict(query=requests[q]['query'], k=10, **arm['authority'])
                response = dict(references[q], ids=references[q]['ids'][:10], authority=arm['authority'], native_wall_ns=100,
                    query_stages=dict(discovery=dict(start_ns=1, end_ns=10), source=dict(start_ns=11, end_ns=20),
                        planning=dict(start_ns=21, end_ns=30), sq8=dict(start_ns=31, end_ns=40), leaf_peak_inflight=16))
                gets = sum(response[p + 'submitted_gets'] for p in ('', 'source_', 'router_'))
                size = sum(response[p + 'verified_bytes'] for p in ('', 'source_', 'router_'))
                response['transport'] = transport_report(11 + gets, 4, 1,
                    sum(arm['metadata_files'].values()) + arm['head_file']['bytes'] + 64 + size, gets)
                if mode == 'id-tamper' and q == 5:
                    response['ids'] = list(reversed(response['ids']))
                return (500 if mode == 'http-failure' and q == 5 else 200), encoded(response)
            def cpu(*unused):
                return dict(pid=42, start_ticks=1, user_ticks=1, system_ticks=1,
                    address_space_limit_bytes=4 << 30, cpu_affinity=[0, 1, 2, 3])
            def measured(*args):
                row = real_measured_call(*args)
                assert row['temporary_directory_cleanup'] and not Path(row['temporary_directory']).exists()
                assert Path(row['child_tmpdir']) == Path(row['temporary_directory'])
                assert (dest / 'scratch/native/other-owner').is_dir()
                if mode == 'cleanup-proof':
                    row['temporary_directory_cleanup'] = False
                return row
            selected = copy.deepcopy(config)
            if mode == 'scratch':
                selected['resources']['scratch_bytes'] = 1
            selected.update(execution_source=dict(commit='0' * 40, archive_sha256='0' * 64), code_sha256={},
                proofs=dict(historical_validation=dict(path='fixture', bytes=1, sha256='0' * 64)))
            write(config_path, selected)
            with (patch.object(boto3, 'client', return_value=sdk),
                    patch.object(sys.modules[__name__], 'qualify', return_value=(selected, assets, dict(scientific_original_root=original))),
                    patch.object(driver, 'native_authority', return_value=dict(source_identity_sha256=driver.SOURCE_ID)),
                    patch.object(sys.modules[__name__], 'snapshot', side_effect=lambda: copy.deepcopy(cgroup)),
                    patch.object(os, 'sched_getaffinity', return_value={4, 5}),
                    patch.dict(os.environ, dict.fromkeys(retained.THREAD_ENV, '2') | {'AWS_MAX_ATTEMPTS': '1'}),
                    patch.object(library.quality, 'run_process', side_effect=publisher),
                    patch.object(sys.modules[__name__], 'measured_call', side_effect=measured),
                    patch.object(subprocess, 'Popen', side_effect=spawn),
                    patch.object(cold.http.client, 'HTTPConnection', Client), patch.object(cold, 'post', side_effect=post),
                    patch.object(library, 'native_cpu', side_effect=cpu),
                    patch.object(library, 'close_native', return_value=dict(returncode=-15, intentional_stop=True, process_group_closed=mode != 'undrained'))):
                try:
                    result = run(config_path, artifact(config_path)['sha256'], Path(__file__).resolve().parents[1], dest)
                except (AssertionError, ValueError) as error:
                    assert mode != 'success', str(error)
                else:
                    assert mode == 'success' and result['returned_hits10'] == 640 and result['actual_http_attempts'] == 64
            assert not (dest / 'scratch').exists()
            assert json.loads((dest / 'cleanup.json').read_bytes())['process_cleanup'] is (mode not in ('undrained', 'cleanup-proof'))
            rows = [json.loads(line) for line in (dest / 'records.jsonl').read_bytes().splitlines()]
            assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
            summary = json.loads((dest / 'summary.json').read_bytes())
            assert summary['denominator10'] == 640 and summary['offered_queries'] == 64
            if mode == 'success':
                assert calls == ['publication', *range(64)]
                assert summary['quality_gate_passed'] and summary['execution_gate_passed']
                assert all(r['accounting']['query_transport_submissions'] == 50 and r['accounting']['final_process_transport']['attempts'] == 66 for r in rows)
                assert (dest / 'original-generation-root.json').read_bytes() == (fixture / 'generation/manifest.json').read_bytes()
                assert json.loads((dest / 'resources.json').read_bytes())['scratch_usage']['hardlink_aliases'] >= 11
                serial_population_guard = concurrent_calls_check(state['arm'], rows[0]['response'])
                concurrent_calls = concurrent_calls_check(state['arm'], rows[0]['response'], concurrent=True)
            elif mode in ('http-failure', 'id-tamper'):
                assert calls == ['publication', *range(6)] and rows[5]['outcome'] == 'failed'
                assert rows[5]['http_attempts'] == 1 and rows[5]['failure_transport']['final_process_transport']['attempts'] == 66
                assert summary['returned_hits10'] == 50 and summary['recall_at_10'] == 50 / 640
                assert all(r['outcome'] == 'aborted' for r in rows[6:])
            elif mode == 'undrained':
                assert calls == ['publication', 0] and rows[0]['outcome'] == 'failed'
                assert not json.loads((dest / 'COMPLETE.json').read_bytes())['passed']
            elif mode == 'cleanup-proof':
                assert calls == ['publication', *range(64)] and all(r['outcome'] == 'success' for r in rows)
                assert summary['error'] == summary['reducer_error']['error'] == 'owned call temporary directory cleanup not proven'
                assert summary['scientific_disposition'] == 'INVALID' and not summary['execution_gate_passed']
                assert 'latency_ms' not in summary and 'returned_hits10' not in summary
                marker = json.loads((dest / 'COMPLETE.json').read_bytes())
                assert not marker['passed']
                for name in ('records.jsonl', 'resources.json', 'cleanup.json', 'summary.json'):
                    assert marker['files'][name] == artifact(dest / name)
            else:
                assert all(r['outcome'] == 'aborted' for r in rows) and not any(type(v) is int for v in calls)
            assert not sdk.meta.events.handlers and sdk.closed and all(s.closed for s in sdk.streams)
            if (dest / 'sdk-ledger.jsonl').exists():
                sdk_rows = [json.loads(line) for line in (dest / 'sdk-ledger.jsonl').read_bytes().splitlines()]
                assert all(r['sdk_http_dispatch_attempts'] == 1 and r['retry_attempts'] == int(mode == 'sdk-retry') for r in sdk_rows)
                assert all(r['completed_ns'] >= r['started_ns'] and r['http_statuses'] == [200] for r in sdk_rows)
                if mode in ('body-tamper', 'sdk-retry'):
                    assert sdk_rows[-1]['error'] is not None
        # Qualifier rejects pending and code drift before it can open any asset.
        real_repo = Path(__file__).resolve().parents[1]
        candidate = dict(FIXED, bucket=science.BUCKET, namespace_prefix='research/semantic-router/new-fixture',
            code_sha256={n: artifact(real_repo / n)['sha256'] for n in CODE}, execution_source={}, asset_manifest={}, proofs={}, resources={})
        for name in ('pending', 'code'):
            mutated = copy.deepcopy(candidate)
            if name == 'pending':
                mutated['authority_pending'] = True
            else:
                mutated['code_sha256'][OWN] = '0' * 64
            write(config_path, mutated)
            try:
                qualify(config_path, artifact(config_path)['sha256'], real_repo)
            except AssertionError as error:
                assert ('pending' if name == 'pending' else 'executor code drift') in str(error)
            else:
                raise AssertionError('pending/code tamper accepted')
        proof_path = directory / 'proof.json'
        write(proof_path, dict(frozen=True))
        local_asset = dict(artifact(proof_path), source=dict(repo_path='proof.json'))
        linked = directory / 'linked/bridge.json'
        stage_asset(None, local_asset, directory, linked, None, lambda *a: None, time.monotonic() + 1)
        assert linked.stat().st_ino == proof_path.stat().st_ino and artifact(linked) == artifact(proof_path)
        local_asset['sha256'] = '0' * 64
        try:
            stage_asset(None, local_asset, directory, directory / 'bad-bridge.json', None, lambda *a: None, time.monotonic() + 1)
        except AssertionError:
            assert not (directory / 'bad-bridge.json').exists()
        else:
            raise AssertionError('local bridge SHA tamper accepted')
        with patch.dict(PROOF_PATHS, {'fixture': Path('proof.json')}, clear=True):
            pointer = dict(path='proof.json', **artifact(proof_path))
            assert read_proofs(directory, dict(fixture=pointer))['fixture'] == proof_path.read_bytes()
            pointer['sha256'] = '0' * 64
            try:
                read_proofs(directory, dict(fixture=pointer))
            except AssertionError:
                pass
            else:
                raise AssertionError('proof tamper accepted')
        print(json.dumps(dict(self_check=True, queries=64, scenarios=9, native_or_network_execution=False,
            closed_panel_admission=closed_panel, sdk_admission=sdk_admission,
            scratch_ownership=scratch_ownership, failure_reducer=failure_reducer, concurrent_calls=concurrent_calls,
            serial_population_guard=serial_population_guard)))


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 5, 'usage: CONFIG SHA256 REPO NEW_EXTERNAL_OUTPUT | --self-check'
        run(*sys.argv[1:])
