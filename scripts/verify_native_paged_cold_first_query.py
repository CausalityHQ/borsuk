"""Independent paged cold record reduction; terminal authentication precedes use."""
import copy
import json
from unittest.mock import patch

from scripts import verify_native_cold_first_query as base
from scripts.check_native_paged_source_stats import validate_response, validate_startup


def reduce_records(records, requests, references, truth, item):
    # Reuse the existing timing, request-byte, ordered-hit, RSS and cleanup guards.
    with patch.object(base, 'validate', validate_startup):
        result, peak = base.reduce_records(records, requests, references, truth, item)
    for q, row in enumerate(records):
        validate_response(row['response'])
        assert row['reference_response'] == {key: references[q][key] for key in
            ('ids', 'ranges', 'planned_bytes', 'submitted_gets', 'verified_bytes', 'failed_gets')}
        assert row['truth_at_10'] == list(truth[q][:10])
        assert row['expected_authority'] == item['authority']
        assert row['metadata_files'] == item['metadata_files']
    for suffix in ('submitted_gets', 'verified_bytes', 'failed_gets'):
        source = sum(r['response']['source_' + suffix] for r in records)
        sq8 = sum(r['response'][suffix] for r in records)
        result.update({'source_' + suffix: source, 'sq8_' + suffix: sq8,
                       'combined_' + suffix: source + sq8})
    result.update(source_head_requests=sum(r['metadata']['source_head_requests'] for r in records),
        logical_metadata_head_requests=sum(r['metadata']['logical_metadata_head_requests'] for r in records),
        logical_metadata_get_requests=sum(r['metadata']['logical_metadata_get_requests'] for r in records),
        metadata_payload_buffer_bound_bytes=max(r['metadata']['payload_buffer_bound_bytes'] for r in records),
        source_head_wall_ns=sum(r['native_header']['remote_open_stats']['source_head_wall_ns'] for r in records),
        source_head_ms=sum(r['metadata']['source_head_ms'] for r in records),
        head_read_wall_ns=sum(r['native_header']['head_read_wall_ns'] for r in records))
    return result, peak


def self_check():
    from scripts import run_native_paged_cold_first_query_selfcheck as fixture
    records, requests, references, truth = [], [], [], []
    item = dict(dataset='synthetic', authority=fixture.AUTHORITY, metadata_files=fixture.FILES)
    for q in range(64):
        row = fixture.record(q)
        request = dict(query=[1.] + [0.] * 767)
        body = json.dumps(dict(query=request['query'], k=10, **item['authority']),
                          separators=(',', ':')).encode()
        end = row['completed_ns']; connect = end - 50_000_000
        row.update(successful_connect_attempt_ns=connect, connected_ns=connect,
            before_successful_connect_attempt_ns=connect-row['started_ns'],
            successful_tcp_connect_ns=0, first_post_to_response_ns=50_000_000,
            connection_refused_attempts=0, request_sha256=base.sha(body), request_bytes=len(body),
            native_server_log=json.dumps(row['native_header'])+'\n',
            native_time_log='Maximum resident set size (kbytes): 1024\nSwaps: 0\n')
        row['response']['native_wall_ns'] = 1
        records.append(row); requests.append(request)
        references.append(copy.deepcopy(row['response'])); truth.append(list(range(100)))
    result, peak = reduce_records(records, requests, references, truth, item)
    assert result['combined_submitted_gets'] == 10240 and peak == 1048576
    assert result['quality_gate_passed'] and not result['published_context_gate_passed']
    for mutate in (
        lambda r: r['response'].update(source_submitted_gets=129),
        lambda r: r.update(truth_at_10=list(reversed(r['truth_at_10']))),
        lambda r: r.update(incoming_http_wall_ns=1),
        lambda r: r['native_close'].update(intentional_stop=False),
        lambda r: r['metadata'].update(logical_metadata_get_requests=0)):
        bad = copy.deepcopy(records); mutate(bad[0])
        try: reduce_records(bad, requests, references, truth, item)
        except AssertionError: pass
        else: raise AssertionError('tampered receipt accepted')
    print('PASS independent paged reduction: caps, truth, timing, cleanup, metadata tamper guards')


if __name__ == '__main__':
    self_check()
