"""No native/AWS work: overlapping graph/semantic calls retain local authority and bytes."""
import base64
from concurrent.futures import ThreadPoolExecutor
import copy
import os
from pathlib import Path
import sys
from threading import Barrier, BrokenBarrierError
from unittest.mock import Mock, patch

from scripts import run_native_semantic_router_cold as worker


def fixture(mode, port):
    files = {'manifest.json': 5000, 'page_manifest.json': 1000, 'page_digests.bin': 12512,
             'plane/manifest.json': 500, 'plane/mean.bin': 3072, 'plane/page_digests.bin': 100000}
    files.update({'centroids.bin': 4800032, 'graph.bin': 1100, 'diverse_graph.bin': 1100}
                 if mode == 'graph' else {'router/manifest.json': 80000, 'router/membership.bin': 12500})
    root = ('a' if mode == 'graph' else 'b') * 64
    arm = dict(dataset=mode, discovery=mode, authority=dict(root_sha256=root, generation=1, control_epoch=1),
               indexes={'10': mode}, metadata_files=files, metadata_sha256={k: root for k in files},
               head_file=dict(bytes=200, sha256=root), leaf_object=dict(bytes=4812500, sha256=root))
    known_lengths = {'page_digests.bin', 'plane/mean.bin', 'plane/page_digests.bin',
                     'router/manifest.json', 'router/membership.bin'}
    order = ['manifest.json', 'page_manifest.json', 'page_digests.bin']
    if arm['discovery'] == 'graph':
        order += ['centroids.bin', 'graph.bin', 'diverse_graph.bin']
    order += ['plane/manifest.json', 'plane/mean.bin', 'plane/page_digests.bin']
    if arm['discovery'] == 'semantic':
        order += ['router/manifest.json', 'router/membership.bin']
    rows = []
    for i, name in enumerate(order):
        size = arm['metadata_files'][name]
        wave = 0 if i == 0 else (i - 1) // 4 + 1
        width = 1 if wave == 0 else min(4, len(order) - 1 - (wave - 1) * 4)
        heads = int(name not in known_lengths)
        rows.append(dict(name=name, bytes=size, chunks=1, head_wall_ns=heads,
                         get_wall_ns=int(size <= 4194304), stream_wall_ns=2, write_wall_ns=1,
                         logical_head_requests=heads, logical_get_requests=(size + 4194303) // 4194304,
                         payload_buffer_bound_bytes=min(size, (8 // width) * 4194304),
                         metadata_wave=wave, metadata_wave_wall_ns=10))
    semantic = int(mode == 'semantic')
    gets, heads = sum(r['logical_get_requests'] for r in rows) + 4, sum(r['logical_head_requests'] for r in rows) + 1 + semantic
    payload = sum(files.values()) + 200 + 5000 + 37

    def transport(gets, payload):
        return dict(schema='borsuk-native-transport-v1', scope='process_all_native_s3_readers', per_query_delta=False,
                    attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests',
                    method_order=worker.stats.METHODS, status_counts_format='[http_status,count] nonzero entries',
                    payload_measurement='consumed response data frames, including unauthenticated payload',
                    dropped_error_body_consumed_bytes=0, unknown=worker.stats.UNKNOWN,
                    totals=dict(attempts=gets + heads + 1, method_counts=[gets, heads, 1] + [0] * 7,
                                status_counts=[[200, gets + heads + 1]], transport_failures=0, stream_failures=0,
                                consumed_payload_bytes=payload, dropped_error_bodies=0))

    header = dict(phase='ready', listen=f'127.0.0.1:{port}', authority=arm['authority'], head_read_wall_ns=2,
                  remote_open_wall_ns=130, transport=transport(gets, payload),
                  remote_open_stats=dict(metadata=rows, staging_wall_ns=100, decode_wall_ns=20,
                                         source_head_requests=1, source_head_wall_ns=5,
                                         router_head_requests=semantic, router_head_wall_ns=5 * semantic))
    stages = {k: dict(start_ns=i * 10 + 1, end_ns=i * 10 + 9) for i, k in enumerate(worker.stats.STAGES)}
    ids = list(range(10)) if not semantic else list(range(10, 20))
    response = dict(authority=arm['authority'], ids=ids, ranges=[[0, 199680]], planned_bytes=199680,
                    submitted_gets=1, verified_bytes=199680, failed_gets=0,
                    source_submitted_gets=1, source_verified_bytes=6400, source_failed_gets=0,
                    router_submitted_gets=8 * semantic, router_verified_bytes=6400 * semantic, router_failed_gets=0,
                    query_stages=dict(stages, leaf_peak_inflight=8 * semantic), native_wall_ns=40,
                    transport=transport(gets + 2 + 8 * semantic, payload + 206080 + 6400 * semantic))
    expected = dict(query_ordinal=port, **{k: copy.deepcopy(response[k]) for k in worker.stats.PARITY})
    body = worker.encoded(dict(query=[1.] + [0.] * 767, k=10, **arm['authority'])).encode()
    return dict(arm=arm, header=header, response=response, expected=expected, body=body, truth=ids)


def check():
    old = worker.old
    config = dict(bucket='synthetic', region='synthetic', native_memory_bytes=512 * 1024 * 1024)
    for fault in (None, 'parity', 'port', 'raw'):
        calls = {port: fixture(mode, port) for mode, port in (('graph', 18080), ('semantic', 18085))}
        if fault == 'parity': calls[18085]['response']['ids'].reverse()
        if fault == 'port': calls[18085]['header']['listen'] = '127.0.0.1:18080'
        for port, call in calls.items():
            call.update(status=502 if port == 18085 and fault == 'raw' else 200,
                        raw=b'\xff\x00failed' if port == 18085 and fault == 'raw'
                        else worker.encoded(call['response']).encode(), paths=[], stopped=0, posts=0, closed=0)
        entered, release = Barrier(3, timeout=5), Barrier(3, timeout=5)
        environment, argv = dict(os.environ), sys.argv

        def spawn(command, **kwargs):
            port = int(command[-1].rsplit(':', 1)[1])
            call = calls[port]
            assert command[14] == call['arm']['indexes']['10']
            assert command[15] == call['arm']['authority']['root_sha256']
            child_environment = dict(environment, BORSUK_NATIVE_MEMORY_BYTES='536870912', AWS_MAX_ATTEMPTS='1',
                                     TOKIO_WORKER_THREADS='4')
            assert kwargs['env'] == child_environment, 'per-call child environment'
            kwargs['stdout'].write(worker.encoded(call['header']) + '\n')
            kwargs['stdout'].flush()
            path = Path(command[3])
            path.write_text('Maximum resident set size (kbytes): 1\nUser time (seconds): 0.01\nSystem time (seconds): 0.00\n')
            call['paths'].append(path.parent)
            server = Mock(port=port)
            server.poll.return_value = None
            return server

        def stop(server):
            calls[server.port]['stopped'] += 1
            return dict(intentional_stop=True, returncode=143)

        class Connection:
            def __init__(self, host, port, timeout):
                assert host == '127.0.0.1' and timeout == 5
                self.call = calls[port]

            def connect(self):
                entered.wait()
                release.wait()

            def request(self, method, path, body, headers):
                assert (method, path, body) == ('POST', '/search', self.call['body'])
                self.call['posts'] += 1

            def getresponse(self):
                return Mock(status=self.call['status'], read=lambda: self.call['raw'])

            def close(self):
                self.call['closed'] += 1

        with patch.object(old.subprocess, 'Popen', side_effect=spawn), \
             patch.object(old.http.client, 'HTTPConnection', Connection), patch.object(old, 'stop', side_effect=stop), \
             patch.object(worker, 'cgroup_snapshot', return_value='UNMEASURED'):
            saved = {k: getattr(old, k) for k in ('checked_response', 'validate', 'post', 'subprocess')}
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = {port: pool.submit(worker.measured_call, 'unused-binary', config, call['arm'],
                                            call['body'], call['expected'], call['truth'], port=port)
                           for port, call in calls.items()}
                try:
                    entered.wait()  # Both real measured_call executions must reach connect together.
                except BrokenBarrierError:
                    failures = [future.result(timeout=5) for future in futures.values()]
                    raise AssertionError('calls failed before overlap: ' + repr(
                        [(r.get('outcome'), r.get('error_type'), r.get('error')) for r in failures])) from None
                try:
                    assert all(not future.done() for future in futures.values()), 'calls serialized'
                    assert all(getattr(old, k) is v for k, v in saved.items()), 'shared hooks changed during overlap'
                    assert dict(os.environ) == environment and sys.argv is argv, 'parent state changed during overlap'
                finally:
                    release.wait()
                records = {port: future.result(timeout=5) for port, future in futures.items()}
            assert records[18080]['outcome'] == 'success', records[18080]
            assert records[18085]['outcome'] == ('failed' if fault else 'success'), records[18085]
            assert max(r['started_ns'] for r in records.values()) < min(r['completed_ns'] for r in records.values())
            for port, record in records.items():
                call = calls[port]
                assert record['raw_response_complete'] and base64.b64decode(record['raw_response_base64']) == call['raw']
                assert record['raw_response'] == call['raw'].decode(errors='replace')
                assert record['expected_authority'] == call['arm']['authority']
                assert record['namespace_start_attempted'] and record['native_process_started']
                assert record['http_attempts'] == call['posts'] == call['stopped'] == call['closed'] == 1
                assert record['native_close']['intentional_stop'] and record['temporary_directory_cleanup']
                assert len(call['paths']) == 1 and not call['paths'][0].exists()
                assert record['startup_accounting']['declared_credential_submissions'] == 3
                assert record['startup_accounting']['inferred_credential_consumed_bytes'] == 37
                assert record['resources']['native_memory_admission_bytes'] == 536870912
                if record['outcome'] == 'success':
                    worker.validate_record(record, config, call['arm'], call['body'], call['expected'], call['truth'], port=port)
                    assert record['returned_hits'] == 10 and record['response']['ids'] == call['truth']
                    try:
                        worker.validate_record(record, config, call['arm'], call['body'], call['expected'], call['truth'],
                                               port=18085 if port == 18080 else 18080)
                    except ValueError: pass
                    else: raise AssertionError('raw header port mismatch accepted')
            assert all(getattr(old, k) is v for k, v in saved.items())
            assert dict(os.environ) == environment and sys.argv is argv
    for port in (1023, 65536, True, 18080.0):
        call = fixture('graph', 18080)
        with patch.object(old.subprocess, 'Popen') as spawn:
            record = worker.measured_call('unused-binary', config, call['arm'], call['body'], call['expected'], call['truth'], port=port)
            assert record['outcome'] == 'failed' and record['http_attempts'] == 0
            assert not record['namespace_start_attempted'] and not record['native_process_started']
            spawn.assert_not_called()
        try:
            worker.validate_record({}, config, call['arm'], call['body'], call['expected'], call['truth'], port=port)
        except ValueError: pass
        else: raise AssertionError('invalid validation port accepted')
    print('PASS overlapping graph/semantic calls; explicit hooks/environment/ports; parity/raw-error/cleanup; native UNRUN')


if __name__ == '__main__':
    check()
