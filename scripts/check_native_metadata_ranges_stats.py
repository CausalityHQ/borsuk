"""Validate logical staged-transfer counters, distinct from SDK wire accounting."""
from scripts.check_native_startup_stats import validate as validate_original

RANGE_BYTES = 8*1024**2
CONCURRENCY = 4


def validate(header, expected_files, arm, geometry=None):
    stats = header['remote_open_stats']
    result = validate_original(stats, expected_files, header['remote_open_wall_ns'])
    if arm == 'control' and geometry is None:
        # Bound by the authenticated frozen serial staging implementation.
        return dict(result, logical_metadata_head_requests=0,
            logical_metadata_get_requests=len(expected_files), payload_buffer_bound_bytes=None)
    assert arm in ('control', 'candidate')
    if geometry is None:
        range_bytes, concurrency = RANGE_BYTES, CONCURRENCY
    else:
        assert set(geometry) == {'range_bytes', 'parallel_gets'}
        range_bytes, concurrency = geometry['range_bytes'], geometry['parallel_gets']
        assert (range_bytes, concurrency) in ((8388608, 4), (4194304, 8))
    rows = stats['metadata']
    for row in rows:
        for key in ('head_wall_ns', 'logical_head_requests', 'logical_get_requests', 'payload_buffer_bound_bytes'):
            assert type(row[key]) is int and row[key] >= 0
        assert row['logical_head_requests'] == 1
        expected_gets = (row['bytes']+range_bytes-1)//range_bytes
        assert row['logical_get_requests'] == expected_gets
        assert row['payload_buffer_bound_bytes'] == min(row['bytes'], concurrency*range_bytes)
        if row['bytes'] > range_bytes: assert row['get_wall_ns'] == 0
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
    for arm, geometry in [('control', dict(range_bytes=8388608, parallel_gets=4)),
                          ('candidate', dict(range_bytes=4194304, parallel_gets=8))]:
        shaped = copy.deepcopy(header)
        for row in shaped['remote_open_stats']['metadata']:
            row['logical_get_requests'] = (row['bytes']+geometry['range_bytes']-1)//geometry['range_bytes']
        assert validate(shaped, files, arm, geometry)['logical_metadata_get_requests'] == (25 if arm == 'control' else 49)
        wrong = dict(geometry, parallel_gets=16)
        try: validate(shaped, files, arm, wrong)
        except AssertionError: pass
        else: raise AssertionError('unbounded geometry accepted')
    for field in ('logical_head_requests', 'logical_get_requests', 'payload_buffer_bound_bytes', 'head_wall_ns'):
        bad = copy.deepcopy(header)
        bad['remote_open_stats']['metadata'][1][field] += 100
        try: validate(bad, files, 'candidate')
        except AssertionError: pass
        else: raise AssertionError('invalid '+field+' accepted')
    print('ranged metadata accounting PASS (synthetic HEAD/GET/buffer/timing negative guards)')


if __name__ == '__main__': main()
