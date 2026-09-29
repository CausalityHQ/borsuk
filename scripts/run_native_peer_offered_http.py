"""Peer-client offered HTTP measurement from frozen native references and exact GT."""
import hashlib
import http.client
import json
from pathlib import Path
import struct
import sys
from urllib.parse import urlsplit

from scripts.run_native_union_offered_http import measure


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    config_path, expected_sha, endpoint, output = sys.argv[1:]
    assert sha(config_path) == expected_sha, 'frozen peer config identity'
    config = json.loads(Path(config_path).read_text())
    assert config['schema'] == 'borsuk-native-peer-offered-http-v1'
    assert config['count'] in (64, 936) and config['k'] in (10, 100)
    assert (config['rows'], config['dimensions']) == (1000000, 768)
    assert set(config['inputs']) == {'requests', 'reference', 'truth'}
    assert set(config['code_sha256']) == {'scripts/run_native_peer_offered_http.py',
        'scripts/run_native_union_offered_http.py', 'scripts/run_native_union_http.py',
        'scripts/rest_coexistence_load.py'}
    assert config['offered_qps'] == 8 and config['workers'] == 8
    for name, digest in config['code_sha256'].items():
        assert sha(name) == digest, 'peer protocol source identity'
    for item in config['inputs'].values():
        path = Path(item['path'])
        assert path.stat().st_size == item['bytes'] and sha(path) == item['sha256']
    url = urlsplit(endpoint)
    if (url.scheme != 'http' or not url.hostname or not 1 <= (url.port or 80) <= 65535
            or url.username or url.password or url.path not in ('', '/') or url.query or url.fragment):
        raise ValueError('explicit plain HTTP peer endpoint required')
    requests = [json.loads(line) for line in Path(config['inputs']['requests']['path']).read_text().splitlines()]
    records = [json.loads(line) for line in Path(config['inputs']['reference']['path']).read_text().splitlines()]
    count, k, authority = config['count'], config['k'], config['authority']
    assert len(requests) == count and len(records) == count + 2
    assert all(len(request['query']) == 768 for request in requests)
    assert [row['query_ordinal'] for row in records[1:-1]] == list(range(count))
    assert records[0]['root_sha256'] == authority['root_sha256']
    assert records[0]['generation'] == authority['generation']
    assert records[0]['control_epoch'] == authority['control_epoch']
    assert records[0]['top_k'] == k and records[0]['declared_panel_count'] == count
    assert records[-1]['count'] == count
    raw = Path(config['inputs']['truth']['path']).read_bytes()
    assert len(raw) == count * 400
    truth = [struct.unpack_from('<100I', raw, q * 400) for q in range(count)]
    assert all(len(set(row)) == 100 and max(row) < config['rows'] for row in truth)
    out = Path(output)
    out.mkdir(parents=True, exist_ok=False)
    samples, result = measure(requests, records[1:-1], truth, authority, k=k,
                              offered_qps=8, workers=8, timeout_seconds=5,
                              connection_factory=lambda: http.client.HTTPConnection(url.hostname, url.port or 80, timeout=5))
    result.update(endpoint=endpoint, transport='plain HTTP peer; connection setup included',
                  config_sha256=expected_sha, dataset=config['dataset'], split=config['query_split'],
                  rows=config['rows'], dimensions=config['dimensions'], authority=authority,
                  metadata_resident=config['metadata_resident'],
                  application_sq8_cache=config['application_sq8_cache'],
                  namespace_cold_start_included=False, s3_service_cache='uncontrolled')
    with (out / 'http.jsonl').open('x') as stream:
        for sample in samples:
            stream.write(json.dumps(sample, sort_keys=True, allow_nan=False) + '\n')
    (out / 'result.json').write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    print(json.dumps(dict(complete=True, offered=result['offered_count'], successful=result['successful_count'],
                         matched_vendor_measured=False)))


if __name__ == '__main__':
    assert len(sys.argv) == 5
    main()
