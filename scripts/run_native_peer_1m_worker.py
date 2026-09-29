"""One bounded server/client role; closed-cell S3 markers coordinate peer HTTP."""
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request

from scripts.run_native_peer_offered_http import sha


def aws(*args):
    return subprocess.check_output(['aws', *map(str, args)], stderr=subprocess.PIPE)


def get_json(bucket, key):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'object'
        try:
            aws('s3api', 'get-object', '--bucket', bucket, '--key', key, path)
        except subprocess.CalledProcessError as error:
            if b'(NoSuchKey)' in error.stderr or b'(404)' in error.stderr:
                return None
            raise
        return json.loads(path.read_text())


def wait_json(bucket, key, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = get_json(bucket, key)
        if value is not None:
            return value
        time.sleep(1)
    raise TimeoutError('coordination marker: ' + key)


def put_json(bucket, key, path, value):
    path.write_text(json.dumps(value, sort_keys=True) + '\n')
    aws('s3api', 'put-object', '--bucket', bucket, '--key', key, '--body', path,
        '--if-none-match', '*', '--metadata', 'sha256=' + sha(path))


def fetch(bucket, ident, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    args = ['s3api', 'get-object', '--bucket', bucket, '--key', ident['key']]
    if 'range_bytes' in ident:
        head = json.loads(aws('s3api', 'head-object', '--bucket', bucket, '--key', ident['key']))
        assert head['ContentLength'] == ident['bytes'] and head['Metadata']['sha256'] == ident['sha256']
        start, length = ident['range_start'], ident['range_bytes']
        args += ['--range', f'bytes={start}-{start + length - 1}', '--if-match', head['ETag']]
        expected_size, expected_sha = length, ident['range_sha256']
    else:
        expected_size, expected_sha = ident['bytes'], ident['sha256']
    aws(*args, path)
    assert path.stat().st_size == expected_size and sha(path) == expected_sha
    return dict(path=str(path), bytes=expected_size, sha256=expected_sha)


def identity():
    base = 'http://169.254.169.254/latest/'
    token = urllib.request.urlopen(urllib.request.Request(base + 'api/token', method='PUT',
        headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'}), timeout=5).read().decode()
    def field(name):
        return urllib.request.urlopen(urllib.request.Request(base + 'meta-data/' + name,
            headers={'X-aws-ec2-metadata-token': token}), timeout=5).read().decode()
    return dict(instance_id=field('instance-id'), private_ip=field('local-ipv4'))


def stop(server):
    intentional = server.poll() is None
    if intentional:
        children = (Path('/proc') / str(server.pid) / 'task' / str(server.pid) / 'children').read_text().split()
        assert len(children) == 1
        timeout_pid = int(children[0])
        os.kill(timeout_pid, signal.SIGTERM)
        try:
            server.wait(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(server.pid, signal.SIGKILL)
            server.wait(timeout=3)
    return dict(returncode=server.returncode, intentional_stop=intentional)


def main():
    role, config_path, expected_sha, prefix, out_arg, binary = sys.argv[1:]
    assert role in ('server', 'client') and sha(config_path) == expected_sha
    config = json.loads(Path(config_path).read_text())
    assert config['schema'] == 'borsuk-native-peer-1m-v1'
    assert config['setting_order'] == [10, 100, 100, 10] and config['count'] == 64
    assert config['offered_qps'] == config['workers'] == 8
    assert sha(Path(__file__)) == config['worker_sha256']
    for name, digest in config['code_sha256'].items():
        assert sha(name) == digest
    out = Path(out_arg)
    out.mkdir(exist_ok=False)
    bucket = config['bucket']
    who = identity()
    launch = wait_json(bucket, prefix + '/launch.json')
    assert launch[role] == who and launch['config_sha256'] == expected_sha
    endpoint = 'http://' + launch['server']['private_ip'] + ':8080'
    if role == 'server':
        assert Path(binary).stat().st_size == config['binary']['bytes']
        assert sha(binary) == config['binary']['sha256']
    inputs = {}
    decisions = []
    ended = False
    for item_index, item in enumerate(config['items']):
        if role == 'client':
            inputs[item_index] = {name: fetch(bucket, ident, Path('peer-inputs') / item['dataset'] / name)
                                 for name, ident in item['inputs'].items()}
        for rep, k in enumerate(config['setting_order']):
            cell = item_index * 4 + rep
            authority = item['authority']
            ready_key, done_key, closed_key = [prefix + f'/{name}/{cell}.json' for name in ('ready', 'done', 'closed')]
            if role == 'server':
                command = [binary, bucket, config['region'], item['indexes'][str(k)],
                           authority['root_sha256'], str(authority['generation']),
                           str(authority['control_epoch']), launch['server']['private_ip'] + ':8080']
                start = time.monotonic_ns()
                with (out / f'cell{cell}-server.log').open('x') as log:
                    server = subprocess.Popen(['/usr/bin/time', '-v', '-o', str(out / f'cell{cell}-server.time'),
                        'timeout', '--signal=TERM', '--kill-after=5', '180', *command],
                        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                    try:
                        deadline = time.monotonic() + 45
                        while True:
                            if server.poll() is not None:
                                raise ValueError('server closed before readiness')
                            try:
                                with urllib.request.urlopen(endpoint + '/health', timeout=2) as response:
                                    health = json.loads(response.read())
                                assert health['authority'] == authority and health['dimensions'] == 768
                                break
                            except (OSError, ConnectionError):
                                if time.monotonic() >= deadline:
                                    raise TimeoutError('server readiness')
                                time.sleep(.1)
                        ready = dict(**who, endpoint=endpoint, cell=cell, k=k, dataset=item['dataset'],
                                     authority=authority, namespace_ready_ms=(time.monotonic_ns() - start) / 1e6)
                        put_json(bucket, ready_key, out / f'ready{cell}.json', ready)
                        done = wait_json(bucket, done_key)
                        assert all(done[key] == value for key, value in launch['client'].items()) and done['cell'] == cell
                        assert type(done['gate_passed']) is bool
                        assert server.poll() is None, 'server ended during client measurement'
                    finally:
                        close = stop(server)
                        (out / f'close{cell}.json').write_text(json.dumps(close) + '\n')
                    put_json(bucket, closed_key, out / f'closed{cell}.json', dict(cell=cell, **who, **close))
                passed = done['gate_passed']
            else:
                ready = wait_json(bucket, ready_key)
                assert all(ready[key] == value for key, value in launch['server'].items()) and ready['endpoint'] == endpoint
                assert ready['dataset'] == item['dataset']
                assert ready['authority'] == authority and ready['cell'] == cell and ready['k'] == k
                (out / f'ready{cell}.json').write_text(json.dumps(ready) + '\n')
                source = inputs[item_index]
                peer = dict(schema='borsuk-native-peer-offered-http-v1', count=64, k=k, offered_qps=8,
                    workers=8, code_sha256=config['code_sha256'], authority=authority,
                    rows=1000000, dimensions=768, dataset=item['dataset'], query_split=item['query_split'],
                    metadata_resident=True, application_sq8_cache=False,
                    inputs={name: source[source_name] for name, source_name in
                            [('requests', 'requests'), ('reference', 'reference-k' + str(k)), ('truth', 'truth')]})
                peer_path = out / f'config{cell}.json'
                peer_path.write_text(json.dumps(peer, indent=2) + '\n')
                subprocess.run([sys.executable, 'scripts/run_native_peer_offered_http.py', str(peer_path),
                    sha(peer_path), endpoint, str(out / f'cell{cell}')], check=True)
                result = json.loads((out / f'cell{cell}/result.json').read_text())
                assert result['identity_parity_valid'], 'source/scorer/physical parity'
                passed = (k != 10 or (result['successful_count'] == 64 and result['mean_offered_recall'] >= .95
                    and result['successful_incoming_http_ms']['p90'] < 444 and result['achieved_successful_qps'] >= 8))
                put_json(bucket, done_key, out / f'done{cell}.json', dict(cell=cell, **who,
                         gate_passed=passed, result_sha256=sha(out / f'cell{cell}/result.json')))
                closed = wait_json(bucket, closed_key)
                assert all(closed[key] == value for key, value in launch['server'].items()) and closed['cell'] == cell
                assert closed['intentional_stop'] is True
            decisions.append(dict(cell=cell, dataset=item['dataset'], k=k, gate_passed=passed))
            if not passed:
                ended = True
                break
        if ended:
            break
    cgroup = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    report = dict(role=role, identity=who, decisions=decisions, all_cells_completed=len(decisions) == 8,
        namespace_cold_start_included=False, matched_vendor_measured=False,
        cgroup={key: (cgroup / key).read_text() for key in ['memory.max', 'memory.peak', 'memory.swap.max',
            'memory.swap.peak', 'memory.events', 'cpu.stat'] if (cgroup / key).exists()},
        address_space_limit=list(resource.getrlimit(resource.RLIMIT_AS)), cpu_affinity=sorted(os.sched_getaffinity(0)))
    (out / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(role=role, closed_cells=len(decisions))))


if __name__ == '__main__':
    assert len(sys.argv) == 7
    main()
