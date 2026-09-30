"""No native/AWS: paged gates, complete reduction, identity guards and old cleanup."""
from contextlib import ExitStack
import copy
import hashlib
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
from unittest.mock import Mock, patch

from scripts import run_native_paged_cold_first_query as worker

old = worker.old
AUTHORITY = dict(root_sha256='a' * 64, generation=1, control_epoch=1)
FILES = {f'metadata-{index}': 1 for index in range(9)}
GLOBALS = ('CODE', 'checked_response', 'validate', 'cold_call', 'reduce_panel')


def rejected(call):
    try:
        call()
    except (AssertionError, KeyError):
        return
    raise AssertionError('invalid receipt accepted')


def response():
    return dict(authority=AUTHORITY, ids=list(range(10)),
        ranges=[[i * 524160, (i + 1) * 524160] for i in range(32)],
        planned_bytes=16773120, submitted_gets=32, verified_bytes=16773120, failed_gets=0,
        source_submitted_gets=128, source_verified_bytes=67108864, source_failed_gets=0)


def header():
    return dict(phase='ready', listen='127.0.0.1:8080', authority=AUTHORITY,
        remote_open_wall_ns=35, head_read_wall_ns=7, remote_open_stats=dict(
            metadata=[dict(name=name, bytes=size, chunks=1, get_wall_ns=0,
                           stream_wall_ns=0, write_wall_ns=0) for name, size in FILES.items()],
            staging_wall_ns=10, decode_wall_ns=20, source_head_wall_ns=5, source_head_requests=1))


def record(q):
    raw, ready = response(), header()
    start = q * 4_000_000_000
    return dict(query_ordinal=q, dataset='synthetic', started_ns=start, completed_ns=start+3_000_000_000,
        cold_start_to_first_http_response_ns=3_000_000_000, incoming_http_wall_ns=50_000_000,
        http_status=200, http_attempts=1, valid_ann_requests=1, returned_hits=10, response=raw,
        reference_response={key: raw[key] for key in old.FIELDS}, truth_at_10=list(range(10)),
        expected_authority=AUTHORITY, metadata_files=FILES, native_header=ready,
        native_close=dict(intentional_stop=True, returncode=143),
        metadata=worker.validate_startup(ready['remote_open_stats'], FILES, ready['remote_open_wall_ns']))


def gates_check():
    raw = response()
    assert worker.checked_response(raw, raw, range(100), AUTHORITY) == 10
    assert worker.validate_response(raw) == dict(query_gets=160, query_verified_bytes=83881984)
    limits = dict(source_submitted_gets=128, source_verified_bytes=67108864, source_failed_gets=0,
                  submitted_gets=32, verified_bytes=16773120, failed_gets=0)
    for name, maximum in limits.items():
        for value in (-1, True, 1.0, maximum+1):
            bad = dict(raw, **{name: value})
            rejected(lambda: worker.checked_response(bad, bad, range(100), AUTHORITY))
        bad = dict(raw)
        del bad[name]
        rejected(lambda: worker.checked_response(bad, raw, range(100), AUTHORITY))
    for name in ('source_submitted_gets', 'source_verified_bytes'):
        bad = dict(raw, **{name: 0})
        rejected(lambda: worker.checked_response(bad, raw, range(100), AUTHORITY))
    bad = dict(raw, ids=list(reversed(raw['ids'])))
    rejected(lambda: worker.checked_response(bad, raw, range(100), AUTHORITY))
    ready = header()
    stats = ready['remote_open_stats']
    for name, value in [('source_head_requests', 0), ('source_head_requests', True),
                        ('source_head_wall_ns', -1), ('source_head_wall_ns', True),
                        ('source_head_wall_ns', 6)]:
        bad = dict(stats, **{name: value})
        rejected(lambda: worker.validate_startup(bad, FILES, 35))
    for name in ('source_head_requests', 'source_head_wall_ns'):
        bad = dict(stats)
        del bad[name]
        rejected(lambda: worker.validate_startup(bad, FILES, 35))
    rejected(lambda: worker.validate_startup(stats, dict(FILES, **{'plane/records.bin': 1}), 35))
    print('PASS cap/type/missing source fields; unchanged ordered IDs; exclusive source HEAD startup')


def reducer_check():
    records = [record(q) for q in range(64)]
    result = worker.reduce_panel(records)
    assert result['count'] == 64 and result['returned_hits'] == 640
    assert result['recall_at_10'] == 1 and result['quality_gate_passed']
    assert not result['published_context_gate_passed']  # 3000ms stays above the old 444ms context.
    for prefix, gets, size in [('source', 128, 67108864), ('sq8', 32, 16773120), ('combined', 160, 83881984)]:
        assert result[prefix + '_submitted_gets'] == 64 * gets
        assert result[prefix + '_verified_bytes'] == 64 * size
        assert result[prefix + '_failed_gets'] == 0
    assert result['query_submitted_gets'] == result['sq8_submitted_gets']
    assert result['query_verified_bytes'] == result['sq8_verified_bytes']
    assert result['source_head_requests'] == 64
    assert result['source_head_wall_ns'] == 320 and result['head_read_wall_ns'] == 448
    assert abs(result['source_head_ms'] - .000320) < 1e-15
    assert result['metadata_objects'] == result['metadata_bytes'] == 576
    rejected(lambda: worker.reduce_panel(records[:-1]))
    mutations = [lambda row: row.update(query_ordinal=62),
        lambda row: row.update(http_status=500), lambda row: row.update(http_attempts=2),
        lambda row: row.update(returned_hits=9),
        lambda row: row['response'].update(source_submitted_gets=129),
        lambda row: row['response'].pop('source_verified_bytes'),
        lambda row: row['native_header']['remote_open_stats'].update(source_head_wall_ns=6),
        lambda row: row['metadata'].update(source_head_requests=0),
        lambda row: row['native_close'].update(intentional_stop=False),
        lambda row: row.update(cold_start_to_first_http_response_ns=42)]
    for mutate in mutations:
        bad = copy.deepcopy(records)
        mutate(bad[-1])
        rejected(lambda: worker.reduce_panel(bad))
    boundary = copy.deepcopy(records)
    for row in boundary[:3]:
        row.update(truth_at_10=list(range(10, 20)), returned_hits=0)
    boundary[3].update(truth_at_10=list(range(8)) + [10, 11], returned_hits=8)
    assert worker.reduce_panel(boundary)['quality_gate_passed']  # Exactly 608/640 = 95%.
    boundary[3].update(truth_at_10=list(range(7)) + [10, 11, 12], returned_hits=7)
    assert not worker.reduce_panel(boundary)['quality_gate_passed']
    print('PASS all 64 reducer: separate/combined totals, HEAD accounting, failed/missing offers rejected')


def fixture(base):
    binary = base / 'binary'
    binary.write_bytes(b'not a native binary')
    repo = Path('.')
    paths = set(repo.rglob('*.rs')) | set(repo.rglob('Cargo.toml')) | set(repo.rglob('Cargo.lock'))
    identities = {str(path): old.sha(path) for path in sorted(paths)
                  if not {'.git', 'target'}.intersection(path.parts)}
    identity = hashlib.sha256(json.dumps(identities, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert len(identities) == 395
    config = dict(schema=worker.SCHEMA, count=64, k=10, dataset_order=['ReLAION', 'CoHere'],
        bucket='synthetic', region='eu-central-1', code_sha256={name: old.sha(name) for name in worker.CODE},
        binary=dict(bytes=binary.stat().st_size, sha256=old.sha(binary)),
        native_source_file_count=395, native_source_identity_sha256=identity,
        items=[dict(dataset=dataset, rows=1000000, dimensions=768, authority=AUTHORITY,
                    query_split='synthetic0-63', indexes={'10': 'fixed/index'}, metadata_files=FILES,
                    inputs={name: {} for name in ('requests', 'reference-k10', 'truth')})
               for dataset in ('ReLAION', 'CoHere')])
    proof = dict(qualified=True, green_status=0, release_status=0, current_full_suite_pass_claim=False,
        binary_sha256=old.sha(binary), source_identity_sha256=identity, source_file_count=395,
        compiled_native_sha256={'crates/borsuk/examples/two_bit_http.rs': identities['crates/borsuk/examples/two_bit_http.rs']})
    cfg, qualification = base / 'config.json', base / 'qualification.json'
    cfg.write_text(json.dumps(config))
    qualification.write_text(json.dumps(proof))
    return binary, cfg, qualification, config, proof


def identity_check():
    with tempfile.TemporaryDirectory() as tmp:
        binary, cfg, qualification, config, proof = fixture(Path(tmp))
        worker.validate_config(config)
        worker.validate_runtime(config, binary, qualification)
        for name, value in [('qualified', False), ('green_status', 1), ('release_status', 1),
                            ('current_full_suite_pass_claim', True), ('current_full_suite_pass_claim', 0),
                            ('binary_sha256', 'f' * 64), ('source_file_count', 394),
                            ('source_identity_sha256', 'f' * 64),
                            ('compiled_native_sha256', {'crates/borsuk/examples/two_bit_http.rs': 'f' * 64})]:
            qualification.write_text(json.dumps(dict(proof, **{name: value})))
            rejected(lambda: worker.validate_runtime(config, binary, qualification))
        qualification.write_text(json.dumps(proof))
        for name in ('current_full_suite_pass_claim', 'source_file_count', 'source_identity_sha256', 'compiled_native_sha256'):
            bad = dict(proof)
            del bad[name]
            qualification.write_text(json.dumps(bad))
            rejected(lambda: worker.validate_runtime(config, binary, qualification))
        qualification.write_text(json.dumps(proof))
        real_sha = old.sha
        with patch.object(old, 'sha', side_effect=lambda name: 'f' * 64 if str(name) == 'crates/borsuk/src/lib.rs' else real_sha(name)):
            rejected(lambda: worker.validate_runtime(config, binary, qualification))
        for name, value in [('native_source_file_count', 394), ('native_source_identity_sha256', 'f' * 64),
                            ('binary', dict(config['binary'], bytes=config['binary']['bytes']+1))]:
            rejected(lambda: worker.validate_runtime(dict(config, **{name: value}), binary, qualification))
        binary.write_bytes(b'current binary differs')
        rejected(lambda: worker.validate_runtime(config, binary, qualification))
        for name, value in [('schema', 'borsuk-native-cold-first-query-v1'), ('count', 63), ('k', 100),
                            ('dataset_order', ['CoHere', 'ReLAION']), ('native_source_file_count', True),
                            ('code_sha256', {key: value for key, value in config['code_sha256'].items() if key != worker.CODE[-1]})]:
            rejected(lambda: worker.validate_config(dict(config, **{name: value})))
        bad = copy.deepcopy(config)
        bad['code_sha256'][worker.CODE[-1]] = 'f' * 64
        rejected(lambda: worker.validate_config(bad))
        with patch.object(sys, 'argv', ['worker', str(cfg), 'f' * 64, str(binary), str(qualification), str(Path(tmp) / 'out')]):
            rejected(worker.main)
    print('PASS original config/runtime9/current binary/source395/qualification guards')


def restoration_check():
    saved = {name: getattr(old, name) for name in GLOBALS}
    argv = sys.argv
    for failure in (False, True):
        try:
            with worker.scoped_runner(['internal']):
                assert old.CODE == worker.CODE and old.validate is worker.validate_startup
                assert old.checked_response is worker.checked_response
                assert old.cold_call is worker.cold_call and old.reduce_panel is worker.reduce_panel
                assert sys.argv == ['internal']
                if failure:
                    raise RuntimeError('synthetic failure')
        except RuntimeError:
            assert failure
        assert sys.argv is argv
        assert all(getattr(old, name) is value for name, value in saved.items())
    print('PASS scoped globals and sys.argv restoration on success/failure')


def mock_runtime(stack, bad_response=False, bad_startup=False):
    server = Mock()
    server.poll.return_value = None
    def spawn(command, **kwargs):
        assert command[4:11] == ['timeout', '--signal=TERM', '--kill-after=5', '60', 'taskset', '-c', '0-3']
        ready = header()
        if bad_startup:
            ready['remote_open_stats']['source_head_requests'] = 0
        kwargs['stdout'].write(json.dumps(ready) + '\n')
        kwargs['stdout'].flush()
        Path(command[3]).write_text('Maximum resident set size (kbytes): 1\n')
        return server
    raw = response()
    if bad_response:
        raw.pop('source_verified_bytes')
    clock = iter(range(100_000_000, 20_000_000_000, 10_000_000))
    stack.enter_context(patch.object(old.subprocess, 'Popen', side_effect=spawn))
    stack.enter_context(patch.object(old.http.client, 'HTTPConnection', side_effect=lambda *a, **k: Mock()))
    stack.enter_context(patch.object(old.time, 'monotonic_ns', side_effect=lambda: next(clock)))
    posted = stack.enter_context(patch.object(old, 'post', return_value=(200, json.dumps(raw).encode())))
    stopped = stack.enter_context(patch.object(old, 'stop', return_value=dict(intentional_stop=True, returncode=143)))
    return server, posted, stopped


def cleanup_check():
    item = dict(dataset='synthetic', authority=AUTHORITY, indexes={'10': 'fixed/index'}, metadata_files=FILES)
    expected = dict(response(), query_ordinal=0)
    for bad_response, bad_startup in ((False, False), (True, False), (False, True)):
        saved = {name: getattr(old, name) for name in GLOBALS}
        argv = sys.argv
        failed = io.StringIO()
        with ExitStack() as stack:
            server, posted, stopped = mock_runtime(stack, bad_response, bad_startup)
            with worker.scoped_runner(['internal']):
                call = lambda: old.cold_call('fixed-binary', dict(bucket='synthetic', region='synthetic'),
                                            item, b'{}', expected, range(100), failed)
                if bad_response or bad_startup:
                    rejected(call)
                    receipt = json.loads(failed.getvalue())
                    assert receipt['outcome'] == 'failed' and receipt['http_attempts'] == 1
                    assert receipt['raw_response'] and receipt['native_server_log']
                    assert receipt['native_close']['intentional_stop'] is True
                else:
                    receipt = call()
                    assert receipt['returned_hits'] == 10 and receipt['metadata']['source_head_requests'] == 1
            posted.assert_called_once()
            stopped.assert_called_once_with(server)
        assert sys.argv is argv and all(getattr(old, name) is value for name, value in saved.items())
    print('PASS old cold-call cleanup once on success/query failure/startup failure; raw failures retained')


def main_check():
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        binary, cfg, qualification, config, proof = fixture(base)
        digest = old.sha(cfg)
        def fetch(bucket, identity, path):
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.name == 'requests':
                rows = [dict(query_ordinal=q, query=[1.] + [0.] * 767) for q in range(64)]
            elif path.name == 'reference-k10':
                rows = [dict(top_k=10, declared_panel_count=64, **AUTHORITY),
                        *[dict(query_ordinal=q, **{key: response()[key] for key in old.FIELDS}) for q in range(64)], dict(count=64)]
            else:
                rows = None
            data = struct.pack('<100I', *range(100)) * 64 if rows is None else ''.join(json.dumps(row) + '\n' for row in rows).encode()
            path.write_bytes(data)
            return dict(path=str(path), bytes=len(data), sha256=old.sha(path))
        saved = {name: getattr(old, name) for name in GLOBALS}
        with ExitStack() as stack:
            server, posted, stopped = mock_runtime(stack)
            stack.enter_context(patch.object(old.os, 'sched_getaffinity', return_value={4, 5}))
            fetched = stack.enter_context(patch.object(old, 'fetch', side_effect=fetch))
            invoked = stack.enter_context(patch.object(old, 'main', wraps=old.main))
            old_calls = stack.enter_context(patch.object(worker, '_cold_call', wraps=worker._cold_call))
            argv = ['worker', str(cfg), digest, str(binary), str(qualification), str(base / 'out')]
            stack.enter_context(patch.object(sys, 'argv', argv))
            worker.main()
            invoked.assert_called_once()
            assert sys.argv is argv and all(getattr(old, name) is value for name, value in saved.items())
            assert old_calls.call_count == posted.call_count == stopped.call_count == 128
            assert fetched.call_count == 6 and all(call.args == (server,) for call in stopped.call_args_list)
        assert old.sha(cfg) == digest and json.loads(cfg.read_text()) == config
        summary = json.loads((base / 'out' / 'summary.json').read_text())
        assert summary['schema'] == 'borsuk-native-paged-cold-first-query-result-v1'
        assert summary['original_config_sha256'] == digest and summary['source_caps'] == worker.SOURCE_CAPS
        assert summary['closed_resident_reference_quality_only'] is True
        assert summary['matched_control_latency_measured'] is summary['matched_vendor_measured'] is False
        assert summary['serial_cold_qps_is_offered_or_saturation_qps'] is False
        assert summary['namespace_starts'] == summary['ann_queries'] == 128
        for dataset in ('ReLAION', 'CoHere'):
            rows = [json.loads(line) for line in (base / 'out' / (dataset.lower() + '-records.jsonl')).read_text().splitlines()]
            assert len(rows) == 64
            assert all(summary['panels'][dataset][key] == value for key, value in worker.reduce_panel(rows).items())
        # A main-level query rejection must escape through the same cleanup/finally path.
        failed_out = base / 'failed-out'
        with ExitStack() as stack:
            server, posted, stopped = mock_runtime(stack, bad_response=True)
            stack.enter_context(patch.object(old.os, 'sched_getaffinity', return_value={4, 5}))
            stack.enter_context(patch.object(old, 'fetch', side_effect=fetch))
            invoked = stack.enter_context(patch.object(old, 'main', wraps=old.main))
            argv = ['worker', str(cfg), digest, str(binary), str(qualification), str(failed_out)]
            stack.enter_context(patch.object(sys, 'argv', argv))
            rejected(worker.main)
            invoked.assert_called_once()
            posted.assert_called_once()
            stopped.assert_called_once_with(server)
            assert sys.argv is argv and all(getattr(old, name) is value for name, value in saved.items())
        failed = json.loads((failed_out / 'relaion-records.jsonl').read_text())
        assert failed['outcome'] == 'failed' and failed['raw_response']
        assert not (failed_out / 'summary.json').exists()
    print('PASS one original main invocation: 128 original cold calls/POSTs/stops, 6 inputs, original config preserved')


if __name__ == '__main__':
    gates_check()
    reducer_check()
    identity_check()
    restoration_check()
    cleanup_check()
    main_check()
