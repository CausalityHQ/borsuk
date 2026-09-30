"""Additional gates for new paged-source arms; historical protocols stay frozen."""
from scripts.check_native_metadata_ranges_stats import validate as validate_transfer


def validate_response(response):
    limits = {'source_submitted_gets': 128, 'source_verified_bytes': 67108864,
              'submitted_gets': 32, 'verified_bytes': 16773120,
              'source_failed_gets': 0, 'failed_gets': 0}
    for name, limit in limits.items():
        assert type(response[name]) is int and 0 <= response[name] <= limit, name
    assert response['source_submitted_gets'] > 0 and response['source_verified_bytes'] > 0
    assert len(response['ranges']) == response['submitted_gets']
    assert response['planned_bytes'] == response['verified_bytes']
    return dict(query_gets=response['source_submitted_gets'] + response['submitted_gets'],
                query_verified_bytes=response['source_verified_bytes'] + response['verified_bytes'])


def validate_startup(stats, expected_files, remote_open_wall_ns):
    assert len(expected_files) == 9 and 'plane/records.bin' not in expected_files
    assert type(stats['source_head_requests']) is int and stats['source_head_requests'] == 1
    assert type(stats['source_head_wall_ns']) is int and stats['source_head_wall_ns'] >= 0
    assert stats['staging_wall_ns'] + stats['decode_wall_ns'] + stats['source_head_wall_ns'] <= remote_open_wall_ns
    result = validate_transfer(dict(remote_open_stats=stats, remote_open_wall_ns=remote_open_wall_ns),
                               expected_files, 'candidate', dict(range_bytes=4194304, parallel_gets=8))
    result.update(source_head_requests=1, source_head_ms=stats['source_head_wall_ns'] / 1e6)
    return result


def main():
    import copy
    response = dict(source_submitted_gets=128, source_verified_bytes=67108864,
                    source_failed_gets=0, submitted_gets=32, verified_bytes=16773120,
                    failed_gets=0, planned_bytes=16773120, ranges=[None] * 32)
    assert validate_response(response) == dict(query_gets=160, query_verified_bytes=83881984)
    expected = {str(index): 1 for index in range(9)}
    startup = dict(metadata=[dict(name=name, bytes=1, chunks=1, get_wall_ns=0,
                   stream_wall_ns=0, write_wall_ns=0, head_wall_ns=0,
                   logical_head_requests=1, logical_get_requests=1,
                   payload_buffer_bound_bytes=1) for name in expected],
                   staging_wall_ns=10, decode_wall_ns=20, source_head_wall_ns=5, source_head_requests=1)
    assert validate_startup(startup, expected, 35)['source_head_requests'] == 1
    for name in ('source_submitted_gets', 'source_verified_bytes', 'source_failed_gets',
                 'submitted_gets', 'verified_bytes', 'failed_gets'):
        for value in (-1, True, response[name] + 1):
            bad = copy.deepcopy(response)
            bad[name] = value
            try:
                validate_response(bad)
            except AssertionError:
                pass
            else:
                raise AssertionError((name, value))
        bad = copy.deepcopy(response)
        del bad[name]
        try:
            validate_response(bad)
        except KeyError:
            pass
        else:
            raise AssertionError('missing source accounting accepted')
    for name, value in [('source_head_requests', 0), ('source_head_requests', True),
                        ('source_head_wall_ns', -1), ('source_head_wall_ns', 6)]:
        bad = copy.deepcopy(startup)
        bad[name] = value
        try:
            validate_startup(bad, expected, 35)
        except AssertionError:
            pass
        else:
            raise AssertionError((name, value))
    print('PASS paged source: separate/combined caps, required counters, startup HEAD timing')


if __name__ == '__main__':
    main()
