"""Bounded metadata-only namespace startup decomposition; no ANN queries."""
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

from scripts.check_native_startup_stats import validate
from scripts.run_native_peer_1m_worker import stop
from scripts.run_native_peer_offered_http import sha


def main():
    config_path, config_sha, binary, qualification_path, output = sys.argv[1:]
    assert sha(config_path) == config_sha
    config = json.loads(Path(config_path).read_text())
    assert config['schema'] == 'borsuk-native-startup-profile-v1'
    assert config['dataset_order'] == ['ReLAION', 'CoHere'] * 3
    for name, digest in config['code_sha256'].items():
        assert sha(name) == digest
    qualified = json.loads(Path(qualification_path).read_text())
    assert qualified['qualified'] and qualified['green_status'] == qualified['release_status'] == 0
    assert sha(binary) == qualified['binary_sha256']
    assert sha('crates/borsuk/examples/two_bit_http.rs') == qualified['compiled_http_sha256']
    out = Path(output)
    out.mkdir(exist_ok=False)
    records = []
    for cell, dataset in enumerate(config['dataset_order']):
        item = next(item for item in config['items'] if item['dataset'] == dataset)
        authority = item['authority']
        started = time.monotonic_ns()
        with (out / f'cell{cell}-server.log').open('x') as log:
            server = subprocess.Popen(['/usr/bin/time', '-v', '-o', str(out / f'cell{cell}-server.time'),
                'timeout', '--signal=TERM', '--kill-after=5', '90', binary, config['bucket'], config['region'],
                item['index'], authority['root_sha256'], str(authority['generation']),
                str(authority['control_epoch']), '127.0.0.1:8080'],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                deadline = time.monotonic() + 45
                while True:
                    if server.poll() is not None:
                        raise RuntimeError('namespace process closed before health')
                    try:
                        with urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2) as response:
                            health = json.loads(response.read())
                        assert health['authority'] == authority and health['dimensions'] == 768
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError('namespace startup')
                        time.sleep(.1)
                ready_ns = time.monotonic_ns() - started
            finally:
                close = stop(server)
                (out / f'cell{cell}-close.json').write_text(json.dumps(close)+'\n')
        headers = [json.loads(line) for line in (out / f'cell{cell}-server.log').read_text().splitlines()
                   if line.startswith('{')]
        assert len(headers) == 1 and headers[0]['phase'] == 'ready'
        header = headers[0]
        assert header['authority'] == authority and header['listen'] == '127.0.0.1:8080'
        reduced = validate(header['remote_open_stats'], item['metadata_files'], header['remote_open_wall_ns'])
        assert close['intentional_stop'] is True
        record = dict(cell=cell, dataset=dataset, namespace_ready_ms=ready_ns/1e6,
            head_read_ms=header['head_read_wall_ns']/1e6, remote_open_ms=header['remote_open_wall_ns']/1e6,
            **reduced)
        (out / f'cell{cell}-profile.json').write_text(json.dumps(record, indent=2)+'\n')
        records.append(record)
    (out / 'summary.json').write_text(json.dumps(dict(records=records, ann_queries=0,
        s3_service_cache='uncontrolled', first_query_measured=False, matched_vendor_measured=False),indent=2)+'\n')
    print(json.dumps(dict(closed=True, startup_samples=len(records), ann_queries=0)))


if __name__ == '__main__':
    assert len(sys.argv) == 6
    main()
