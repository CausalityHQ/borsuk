"""Fresh process/namespace through first HTTP response; fixed native/GT parity."""
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import time

from scripts.check_native_startup_stats import validate
from scripts.run_native_peer_1m_worker import fetch, sha, stop
from scripts.run_native_union_http import post, quantile

CODE = ('scripts/run_native_cold_first_query.py', 'scripts/check_native_startup_stats.py',
        'scripts/run_native_peer_1m_worker.py', 'scripts/run_native_peer_offered_http.py',
        'scripts/run_native_union_offered_http.py', 'scripts/run_native_union_http.py',
        'scripts/rest_coexistence_load.py')
FIELDS = ('ids', 'ranges', 'planned_bytes', 'submitted_gets', 'verified_bytes', 'failed_gets')


def checked_response(response, expected, truth, authority):
    assert response['authority'] == authority
    assert all(response[key] == expected[key] for key in FIELDS), 'ordered-ID/source/scorer/physical parity'
    assert len(response['ids']) == len(set(response['ids'])) == 10
    assert len(response['ranges']) == response['submitted_gets'] <= 32
    assert response['planned_bytes'] == response['verified_bytes'] <= 16773120
    assert response['failed_gets'] == 0
    return len(set(response['ids']) & set(truth[:10]))


def cold_call(binary, config, item, body, expected, truth, failure_stream=None, *, port=8080):
    if type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError('port must be an integer in 1024..65535')
    authority = item['authority']
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        with (directory/'server.log').open('x') as log:
            started = time.monotonic_ns()
            server = subprocess.Popen(['/usr/bin/time', '-v', '-o', str(directory/'server.time'),
                'timeout', '--signal=TERM', '--kill-after=5', '60', 'taskset', '-c', '0-3', binary,
                config['bucket'], config['region'], item['indexes']['10'], authority['root_sha256'],
                str(authority['generation']), str(authority['control_epoch']), f'127.0.0.1:{port}'],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            client = None
            refused = 0
            failure = None
            status, raw = None, b''
            completed = None
            http_attempts = 0
            try:
                deadline = time.monotonic() + 45
                while True:
                    if server.poll() is not None:
                        raise RuntimeError('namespace process closed before first connection')
                    client = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
                    attempted = time.monotonic_ns()
                    try:
                        client.connect()
                        connected = time.monotonic_ns()
                        break
                    except ConnectionRefusedError:
                        client.close()
                        refused += 1
                        if time.monotonic() >= deadline:
                            raise TimeoutError('namespace first connection')
                        time.sleep(.01)
                http_attempts = 1
                status, raw = post(client, body)
                completed = time.monotonic_ns()  # Wire completion precedes JSON/parity work.
                assert status == 200, 'first and only ANN request failed; no HTTP retry'
                response = json.loads(raw)
                hits = checked_response(response, expected, truth, authority)
                assert server.poll() is None, 'namespace ended before completed measurement'
            except Exception as error:
                failure = error
                raise
            finally:
                if client is not None: client.close()
                close = stop(server)
                if failure is not None and failure_stream is not None:
                    failure_stream.write(json.dumps(dict(query_ordinal=expected['query_ordinal'],
                        dataset=item['dataset'],outcome='failed',error_type=type(failure).__name__,
                        error=str(failure),started_ns=started,completed_ns=completed,
                        http_status=status,http_attempts=http_attempts,
                        raw_response=raw.decode(errors='replace'),connection_refused_attempts=refused,
                        native_server_log=(directory/'server.log').read_text(),
                        native_time_log=(directory/'server.time').read_text(),native_close=close),
                        sort_keys=True,allow_nan=False)+'\n')
                    failure_stream.flush()
        raw_log = (directory/'server.log').read_text()
        try:
            headers = [json.loads(line) for line in raw_log.splitlines() if line.startswith('{')]
            assert len(headers) == 1
            header = headers[0]
            assert header['phase'] == 'ready' and header['authority'] == authority and header['listen'] == f'127.0.0.1:{port}'
            assert close['intentional_stop'] is True
            metadata = validate(header['remote_open_stats'], item['metadata_files'], header['remote_open_wall_ns'])
            assert completed-started >= header['remote_open_wall_ns']+header['head_read_wall_ns']
        except Exception as error:
            if failure_stream is not None:
                failure_stream.write(json.dumps(dict(query_ordinal=expected['query_ordinal'],
                    dataset=item['dataset'], outcome='failed', error_type=type(error).__name__,
                    error=str(error), started_ns=started, completed_ns=completed, http_status=status,
                    http_attempts=http_attempts,
                    raw_response=raw.decode(errors='replace'), connection_refused_attempts=refused,
                    native_server_log=raw_log, native_time_log=(directory/'server.time').read_text(),
                    native_close=close), sort_keys=True, allow_nan=False)+'\n')
                failure_stream.flush()
            raise
        return dict(started_ns=started, successful_connect_attempt_ns=attempted,
            connected_ns=connected, completed_ns=completed,
            cold_start_to_first_http_response_ns=completed-started,
            before_successful_connect_attempt_ns=attempted-started,
            successful_tcp_connect_ns=connected-attempted, first_post_to_response_ns=completed-connected,
            incoming_http_wall_ns=completed-attempted, connection_refused_attempts=refused,
            http_status=status, http_attempts=1, valid_ann_requests=1,
            request_sha256=hashlib.sha256(body).hexdigest(), request_bytes=len(body),
            response_bytes=len(raw), response=response, returned_hits=hits,
            native_header=header, native_server_log=raw_log,
            native_time_log=(directory/'server.time').read_text(), native_close=close, metadata=metadata)


def reduce_panel(records):
    assert len(records) == 64 and [r['query_ordinal'] for r in records] == list(range(64))
    assert all(r['http_status']==200 and r['http_attempts']==r['valid_ann_requests']==1 for r in records)
    hits = sum(r['returned_hits'] for r in records)
    assert 0 <= hits <= 640
    tails = {key: {label: quantile([r[key]/1e6 for r in records], p)
        for label, p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}
        for key in ('cold_start_to_first_http_response_ns', 'incoming_http_wall_ns')}
    elapsed = records[-1]['completed_ns']-records[0]['started_ns']
    assert elapsed > 0
    return dict(count=64, returned_hits=hits, recall_at_10=hits/640,
        quality_gate_passed=hits>=608, published_context_gate_passed=hits>=608 and
        tails['cold_start_to_first_http_response_ns']['p90']<444,
        cold_start_to_first_http_response_ms=tails['cold_start_to_first_http_response_ns'],
        incoming_http_ms=tails['incoming_http_wall_ns'], serial_cold_calls_per_second=64e9/elapsed,
        serial_span_ns=elapsed, query_submitted_gets=sum(r['response']['submitted_gets'] for r in records),
        query_verified_bytes=sum(r['response']['verified_bytes'] for r in records),
        query_failed_gets=sum(r['response']['failed_gets'] for r in records),
        metadata_objects=sum(r['metadata']['metadata_objects'] for r in records),
        metadata_bytes=sum(r['metadata']['metadata_bytes'] for r in records))


def main():
    config_path, expected_sha, binary, qualification_path, output = sys.argv[1:]
    assert sha(config_path) == expected_sha
    config = json.loads(Path(config_path).read_text())
    assert config['schema'] == 'borsuk-native-cold-first-query-v1'
    assert (config['count'],config['k']) == (64,10)
    assert config['dataset_order'] == ['ReLAION','CoHere']
    assert set(config['code_sha256']) == set(CODE)
    for name,digest in config['code_sha256'].items(): assert sha(name)==digest
    assert sorted(os.sched_getaffinity(0)) == [4,5]
    qualified = json.loads(Path(qualification_path).read_text())
    assert qualified['qualified'] and qualified['green_status']==qualified['release_status']==0
    assert qualified['binary_sha256']==sha(binary)==config['binary']['sha256']
    assert Path(binary).stat().st_size==config['binary']['bytes']
    for name,digest in qualified['compiled_native_sha256'].items(): assert sha(name)==digest
    out = Path(output)
    out.mkdir(exist_ok=False)
    panels = {}
    for dataset in config['dataset_order']:
        item = next(item for item in config['items'] if item['dataset']==dataset)
        assert (item['rows'],item['dimensions'])==(1000000,768)
        sources = {name: fetch(config['bucket'],item['inputs'][name],out/'inputs'/dataset/name)
                   for name in ('requests','reference-k10','truth')}
        requests = [json.loads(line) for line in Path(sources['requests']['path']).read_text().splitlines()]
        reference = [json.loads(line) for line in Path(sources['reference-k10']['path']).read_text().splitlines()]
        truth_bytes = Path(sources['truth']['path']).read_bytes()
        assert len(requests)==64 and len(reference)==66 and len(truth_bytes)==25600
        assert reference[0]['top_k']==10 and reference[0]['declared_panel_count']==reference[-1]['count']==64
        for key,value in item['authority'].items(): assert reference[0][key]==value
        assert [r['query_ordinal'] for r in requests]==[r['query_ordinal'] for r in reference[1:-1]]==list(range(64))
        records = []
        with (out/(dataset.lower()+'-records.jsonl')).open('x') as stream:
            for q,request in enumerate(requests):
                query = request['query']
                assert len(query)==768 and all(math.isfinite(v) for v in query) and any(v!=0 for v in query)
                truth = struct.unpack_from('<100I',truth_bytes,q*400)
                assert len(set(truth))==100 and max(truth)<1000000
                body = json.dumps(dict(query=query,k=10,**item['authority']),separators=(',',':'),allow_nan=False).encode()
                record = dict(query_ordinal=q,dataset=dataset,
                    **cold_call(binary,config,item,body,reference[q+1],truth,failure_stream=stream))
                stream.write(json.dumps(record,sort_keys=True,allow_nan=False)+'\n')
                stream.flush()
                records.append(record)
        panels[dataset] = dict(**reduce_panel(records),split=item['query_split'],inputs=sources)
    summary = dict(panels=panels,ann_queries=128,namespace_starts=128,k=10,
        source_scorer_ordered_id_physical_parity=True,namespace_cold_start_included=True,
        application_sq8_cache=False,s3_service_cache='uncontrolled',transport='loopback plain HTTP',
        cold_call_boundary='preencoded request; process launch through first HTTP response; refused TCP connects included',
        client_cpu_affinity=sorted(os.sched_getaffinity(0)),native_cpu_affinity=[0,1,2,3],
        serial_cold_qps_is_offered_or_saturation_qps=False,matched_vendor_measured=False,
        quality_gate_passed=all(p['quality_gate_passed'] for p in panels.values()),
        published_context_gate_passed=all(p['published_context_gate_passed'] for p in panels.values()))
    (out/'summary.json').write_text(json.dumps(summary,indent=2,allow_nan=False)+'\n')
    print(json.dumps(dict(closed=True,namespace_starts=128,ann_queries=128,
        quality_gate_passed=summary['quality_gate_passed'],published_context_gate_passed=summary['published_context_gate_passed'])))


if __name__ == '__main__':
    assert len(sys.argv)==6
    main()
