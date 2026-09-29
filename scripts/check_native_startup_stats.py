"""Validate bounded startup accounting against the frozen metadata roster."""

def validate(stats, expected_files, remote_open_wall_ns):
    assert type(remote_open_wall_ns) is int and remote_open_wall_ns > 0
    rows = stats['metadata']
    assert len(rows) == len(expected_files)
    assert len({row['name'] for row in rows}) == len(rows)
    assert {row['name']: row['bytes'] for row in rows} == expected_files
    for row in rows:
        for key in ('bytes', 'chunks', 'get_wall_ns', 'stream_wall_ns', 'write_wall_ns'):
            assert type(row[key]) is int and row[key] >= 0
        assert row['bytes'] > 0 and row['chunks'] > 0
        assert row['write_wall_ns'] <= row['stream_wall_ns']
    for key in ('staging_wall_ns', 'decode_wall_ns'):
        assert type(stats[key]) is int and stats[key] >= 0
    assert sum(row['get_wall_ns'] + row['stream_wall_ns'] for row in rows) <= stats['staging_wall_ns']
    assert stats['staging_wall_ns'] + stats['decode_wall_ns'] <= remote_open_wall_ns
    return dict(metadata_objects=len(rows), metadata_bytes=sum(row['bytes'] for row in rows),
        transport_chunks=sum(row['chunks'] for row in rows),
        get_header_ms=sum(row['get_wall_ns'] for row in rows)/1e6,
        stream_and_output_ms=sum(row['stream_wall_ns'] for row in rows)/1e6,
        awaited_writes_ms=sum(row['write_wall_ns'] for row in rows)/1e6,
        staging_ms=stats['staging_wall_ns']/1e6, decode_ms=stats['decode_wall_ns']/1e6)


def main():
    import copy
    expected = {'manifest.json': 2, 'plane/records.bin': 32768}
    stats = dict(metadata=[dict(name=name, bytes=size, chunks=1,
        get_wall_ns=10, stream_wall_ns=30, write_wall_ns=20) for name,size in expected.items()],
        staging_wall_ns=100, decode_wall_ns=20)
    assert validate(stats, expected, 130)['metadata_bytes'] == 32770
    for mutation in ('bytes', 'duplicate', 'negative', 'write', 'stage', 'total'):
        bad = copy.deepcopy(stats)
        if mutation == 'bytes': bad['metadata'][1]['bytes'] -= 1
        elif mutation == 'duplicate': bad['metadata'][1]['name'] = 'manifest.json'
        elif mutation == 'negative': bad['metadata'][0]['get_wall_ns'] = -1
        elif mutation == 'write': bad['metadata'][0]['write_wall_ns'] = 31
        elif mutation == 'stage': bad['staging_wall_ns'] = 79
        else: bad['decode_wall_ns'] = 31
        try: validate(bad, expected, 130)
        except AssertionError: pass
        else: raise AssertionError('invalid startup counters accepted: ' + mutation)
    print('PASS startup accounting: exact roster/bytes, distinct names, nonnegative and nested timing bounds')


if __name__ == '__main__':
    main()
