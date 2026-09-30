"""Query-blind development conversion; old Rust readers remain unsupported."""
import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from scripts.v282_seal_generation import digest

FILES = ('manifest.json', 'page_manifest.json', 'page_digests.bin', 'centroids.bin',
         'graph.bin', 'diverse_graph.bin', 'plane/manifest.json', 'plane/mean.bin',
         'plane/records.bin')
ROOT_FIELDS = set('schema generation base_epoch plane_manifest_sha256 page_manifest_sha256 centroids_sha256 graph_sha256 graph_resident_bytes diverse_graph_sha256 diverse_graph_resident_bytes sq8_object_sha256 sq8_object_key sq8_etag canonical low step'.split())
PLANE_FIELDS = set('schema rows dimensions seed record_bytes source_sha256 sq8_sha256 source_order_sha256 mean_sha256 records_sha256 query_or_truth_used'.split())


def convert(source, trusted_root_sha256, sizes, output):
    source, output = Path(source), Path(output)
    assert not output.exists() and not output.resolve().is_relative_to(source.resolve())
    assert set(sizes) == set(FILES)
    assert all(type(size) is int and 0 < size <= 300_000_000 for size in sizes.values())

    def authenticated_json(name, expected):
        assert sizes[name] <= 65536
        body = (source / name).read_bytes()
        assert len(body) == sizes[name] and hashlib.sha256(body).hexdigest() == expected
        def unique(pairs):
            assert len({key for key, _ in pairs}) == len(pairs)
            return dict(pairs)
        return json.loads(body, object_pairs_hook=unique)

    root = authenticated_json('manifest.json', trusted_root_sha256)
    assert set(root) == ROOT_FIELDS and root['schema'] == 'borsuk-two-bit-generation-v4'
    plane = authenticated_json('plane/manifest.json', root['plane_manifest_sha256'])
    page = authenticated_json('page_manifest.json', root['page_manifest_sha256'])
    assert set(plane) == PLANE_FIELDS and plane['schema'] == 'borsuk-two-bit-plane-v2' and plane['query_or_truth_used'] is False
    assert 'page_rows' not in plane and 'page_digest_sha256' not in plane
    assert plane['seed'] == 20260923
    assert all(type(plane[key]) is int and plane[key] > 0 for key in ('rows', 'dimensions', 'record_bytes'))
    assert plane['rows'] <= 1_000_000 and plane['dimensions'] <= 8192
    complete, tail = divmod(plane['dimensions'], 256)
    padded = complete * 256 + (1 << (tail - 1).bit_length() if tail else 0)
    assert plane['record_bytes'] == (padded + 3) // 4 + 8
    assert sizes['plane/mean.bin'] == plane['dimensions'] * 4
    assert sizes['page_digests.bin'] == (plane['rows'] + 255) // 256 * 32
    assert page['schema'] == 'borsuk-v115-sq8-page-authority-v2' and page['page_rows'] == 256
    assert (page['generation'], page['rows'], page['dimensions'], page['object_sha256']) == (
        root['generation'], plane['rows'], plane['dimensions'], plane['sq8_sha256'])
    assert plane['sq8_sha256'] == root['sq8_object_sha256']
    assert sizes['plane/records.bin'] == plane['rows'] * plane['record_bytes']
    identities = {'page_manifest.json': root['page_manifest_sha256'],
                  'page_digests.bin': page['page_digest_sha256'],
                  'centroids.bin': root['centroids_sha256'], 'graph.bin': root['graph_sha256'],
                  'diverse_graph.bin': root['diverse_graph_sha256'],
                  'plane/mean.bin': plane['mean_sha256'], 'plane/records.bin': plane['records_sha256']}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent) as temporary:
        target = Path(temporary) / 'generation'
        (target / 'plane').mkdir(parents=True)
        for name, expected in identities.items():
            assert (source / name).stat().st_size == sizes[name]
            shutil.copyfile(source / name, target / name)
            assert digest(target / name) == expected
            with (target / name).open('rb') as copied:
                os.fsync(copied.fileno())
        table_digest = hashlib.sha256()
        with (target / 'plane/records.bin').open('rb') as records, (target / 'plane/page_digests.bin').open('xb', buffering=16384) as table:
            remaining = sizes['plane/records.bin']
            while remaining:
                block = records.read(min(remaining, plane['record_bytes'] * 32))
                assert len(block) == min(remaining, plane['record_bytes'] * 32)
                entry = hashlib.sha256(block).digest()
                table.write(entry)
                table_digest.update(entry)
                remaining -= len(block)
            table.flush()
            os.fsync(table.fileno())
        plane.update(schema='borsuk-two-bit-plane-v3', page_rows=32,
                     page_digest_sha256=table_digest.hexdigest())
        def write_json(name, value):
            body = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            assert len(body) <= 65536
            with (target / name).open('xb') as destination:
                destination.write(body)
                destination.flush()
                os.fsync(destination.fileno())
            return hashlib.sha256(body).hexdigest()
        root.update(schema='borsuk-two-bit-generation-v6',
                    plane_manifest_sha256=write_json('plane/manifest.json', plane))
        root_sha256 = write_json('manifest.json', root)
        proof = dict(schema='borsuk-native-paged-source-conversion-v1',
                     original_root_sha256=trusted_root_sha256, root_sha256=root_sha256,
                     rows=plane['rows'], dimensions=plane['dimensions'],
                     query_or_truth_used=False, unchanged_files=identities,
                     source_page_digest_sha256=plane['page_digest_sha256'],
                     artifacts={name: dict(bytes=(target / name).stat().st_size, sha256=digest(target / name))
                                for name in (*FILES, 'plane/page_digests.bin')})
        for directory in (target / 'plane', target):
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        assert not output.exists()
        target.rename(output)
    return proof


def self_check():
    with tempfile.TemporaryDirectory() as temporary:
        base = Path(temporary)
        source = base / 'original'
        (source / 'plane').mkdir(parents=True)
        bodies = {name: name.encode() for name in FILES}
        bodies['plane/mean.bin'] = bytes(8)
        bodies['page_digests.bin'] = bytes(32)
        bodies['plane/records.bin'] = bytes(range(33)) * 9
        sha = lambda body: hashlib.sha256(body).hexdigest()
        plane = dict(schema='borsuk-two-bit-plane-v2', rows=33, dimensions=2,
                     seed=20260923, record_bytes=9, query_or_truth_used=False,
                     source_sha256='a' * 64, sq8_sha256='b' * 64,
                     source_order_sha256='c' * 64,
                     mean_sha256=sha(bodies['plane/mean.bin']),
                     records_sha256=sha(bodies['plane/records.bin']))
        page = dict(schema='borsuk-v115-sq8-page-authority-v2', generation=1,
                    rows=33, dimensions=2, page_rows=256, object_sha256='b' * 64,
                    page_digest_sha256=sha(bodies['page_digests.bin']))
        bodies['plane/manifest.json'] = json.dumps(plane).encode()
        bodies['page_manifest.json'] = json.dumps(page).encode()
        root = dict(schema='borsuk-two-bit-generation-v4', generation=1, base_epoch=0,
                    plane_manifest_sha256=sha(bodies['plane/manifest.json']),
                    page_manifest_sha256=sha(bodies['page_manifest.json']),
                    centroids_sha256=sha(bodies['centroids.bin']),
                    graph_sha256=sha(bodies['graph.bin']), graph_resident_bytes=1,
                    diverse_graph_sha256=sha(bodies['diverse_graph.bin']), diverse_graph_resident_bytes=1,
                    sq8_object_sha256='b' * 64, sq8_object_key='objects/' + 'b' * 64,
                    sq8_etag='"frozen"', canonical={}, low=[0, 0], step=[1, 1])
        bodies['manifest.json'] = json.dumps(root).encode()
        for name, body in bodies.items():
            (source / name).write_bytes(body)
        sizes = {name: len(body) for name, body in bodies.items()}
        proof = convert(source, sha(bodies['manifest.json']), sizes, base / 'converted')
        assert proof['query_or_truth_used'] is False
        assert len(proof['unchanged_files']) == 7
        table = (base / 'converted/plane/page_digests.bin').read_bytes()
        assert table == b''.join(hashlib.sha256(bodies['plane/records.bin'][offset:offset + 32 * 9]).digest()
                                 for offset in range(0, 33 * 9, 32 * 9))
        for name in proof['unchanged_files']:
            assert (base / 'converted' / name).read_bytes() == bodies[name]
        for index, name in enumerate(FILES):
            original = bodies[name]
            (source / name).write_bytes(bytes([original[0] ^ 1]) + original[1:])
            try:
                convert(source, sha(bodies['manifest.json']), sizes, base / f'bad{index}')
            except (AssertionError, ValueError):
                pass
            else:
                raise AssertionError('tampered input accepted: ' + name)
            assert not (base / f'bad{index}/manifest.json').exists()
            (source / name).write_bytes(original)
        print('PASS conversion: authenticated inputs, unchanged payloads, 32-row/tail authority, tamper rejection')


if __name__ == '__main__':
    self_check()
