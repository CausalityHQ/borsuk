"""Validate logical staged-transfer counters, distinct from SDK wire accounting."""
from scripts.check_native_startup_stats import validate as validate_original

RANGE_BYTES = 8*1024**2
CONCURRENCY = 4


def validate(header, expected_files, arm):
    stats = header['remote_open_stats']
    result = validate_original(stats, expected_files, header['remote_open_wall_ns'])
    if arm == 'control':
        # Bound by the authenticated frozen serial staging implementation.
        return dict(result, logical_metadata_head_requests=0,
            logical_metadata_get_requests=len(expected_files), payload_buffer_bound_bytes=None)
    assert arm == 'candidate'
    rows = stats['metadata']
    for row in rows:
        for key in ('head_wall_ns', 'logical_head_requests', 'logical_get_requests', 'payload_buffer_bound_bytes'):
            assert type(row[key]) is int and row[key] >= 0
        assert row['logical_head_requests'] == 1
        expected_gets = (row['bytes']+RANGE_BYTES-1)//RANGE_BYTES
        assert row['logical_get_requests'] == expected_gets
        assert row['payload_buffer_bound_bytes'] == min(row['bytes'], CONCURRENCY*RANGE_BYTES)
        if row['bytes'] > RANGE_BYTES: assert row['get_wall_ns'] == 0
    assert sum(r['head_wall_ns']+r['get_wall_ns']+r['stream_wall_ns'] for r in rows) <= stats['staging_wall_ns']
    return dict(result, logical_metadata_head_requests=sum(r['logical_head_requests'] for r in rows),
        logical_metadata_get_requests=sum(r['logical_get_requests'] for r in rows),
        payload_buffer_bound_bytes=max(r['payload_buffer_bound_bytes'] for r in rows))


def main():
    import copy
    files = {'manifest.json': 10, 'plane/records.bin': 200000000}
    rows = [dict(name=name, bytes=size, chunks=1, get_wall_ns=0,
        stream_wall_ns=10, write_wall_ns=1, head_wall_ns=1, logical_head_requests=1,
        logical_get_requests=(size+RANGE_BYTES-1)//RANGE_BYTES,
        payload_buffer_bound_bytes=min(size, CONCURRENCY*RANGE_BYTES)) for name, size in files.items()]
    header = dict(remote_open_wall_ns=40, remote_open_stats=dict(metadata=rows, staging_wall_ns=30, decode_wall_ns=10))
    actual = validate(header, files, 'candidate')
    assert actual['logical_metadata_get_requests'] == 25 and actual['payload_buffer_bound_bytes'] == 33554432
    for field in ('logical_head_requests', 'logical_get_requests', 'payload_buffer_bound_bytes', 'head_wall_ns'):
        bad = copy.deepcopy(header)
        bad['remote_open_stats']['metadata'][1][field] += 100
        try: validate(bad, files, 'candidate')
        except AssertionError: pass
        else: raise AssertionError('invalid '+field+' accepted')
    print('ranged metadata accounting PASS (synthetic HEAD/GET/buffer/timing negative guards)')


if __name__ == '__main__': main()
