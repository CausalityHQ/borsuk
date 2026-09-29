"""Synthetic worker-main coordination check; stdlib only, no external access.

Run from the repository root: python3 -m scripts.check_native_peer_1m_worker
Requires the worker, its imports, and peer-1m-config.json beside this checkout.
Socket protocol coverage belongs to check_native_union_offered_http.py.
"""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import Mock, patch

from scripts import run_native_peer_1m_worker as worker

CONFIG = Path('docs/research/native-union-20260928/peer-1m-config.json')
NODES = dict(server=dict(instance_id='i-synthetic-server', private_ip='10.0.0.1'),
             client=dict(instance_id='i-synthetic-client', private_ip='10.0.0.2'))
ENDPOINT = 'http://10.0.0.1:8080'
PREFIX = 'synthetic/peer'


def exercise(config, role, *, fail_cell=None, metric=None, corrupt=None):
    """Run actual main with local JSON markers and fake process/HTTP boundaries."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        config = copy.deepcopy(config)
        binary = root / 'binary'
        binary.write_bytes(b'synthetic executable; never executed')
        config['binary'] = dict(bytes=binary.stat().st_size, sha256=worker.sha(binary))
        config_path = root / 'config.json'
        config_path.write_text(json.dumps(config))
        digest = worker.sha(config_path)
        launch = dict(**copy.deepcopy(NODES), config_sha256=digest)
        markers = root / 'markers'
        markers.mkdir()
        events, commands, measurements, fetched = [], [], [], []
        stopped = []

        def marker_path(key):
            assert key.startswith(PREFIX + '/')
            return markers / key.removeprefix(PREFIX + '/').replace('/', '-')

        marker_path(PREFIX + '/launch.json').write_text(json.dumps(launch))
        for cell in range(8):
            item, k = config['items'][cell // 4], config['setting_order'][cell % 4]
            values = dict(
                ready=dict(**NODES['server'], endpoint=ENDPOINT, cell=cell, k=k,
                           dataset=item['dataset'], authority=item['authority']),
                done=dict(**NODES['client'], cell=cell, gate_passed=cell != fail_cell),
                closed=dict(**NODES['server'], cell=cell, intentional_stop=True, returncode=-15))
            for kind, value in values.items():
                if (role == 'server' and kind == 'done') or (role == 'client' and kind != 'done'):
                    marker_path(f'{PREFIX}/{kind}/{cell}.json').write_text(json.dumps(value))
        if corrupt:
            kind, field, value = corrupt
            path = marker_path(PREFIX + ('/launch.json' if kind == 'launch' else f'/{kind}/0.json'))
            if field is None:
                path.write_text('{malformed')
            else:
                body = json.loads(path.read_text())
                if kind == 'launch':
                    body[role][field] = value
                else:
                    body[field] = value
                path.write_text(json.dumps(body))

        def aws(*args):
            assert args[0] == 's3api', args
            operation = args[1]
            assert args[args.index('--bucket') + 1] == config['bucket']
            key = args[args.index('--key') + 1]
            events.append((operation, key))
            path = marker_path(key)
            if operation == 'get-object':
                assert path.exists(), ('unexpected wait', key)
                Path(args[-1]).write_bytes(path.read_bytes())
            else:
                assert operation == 'put-object' and not path.exists(), args
                assert args[args.index('--if-none-match') + 1] == '*'
                source = Path(args[args.index('--body') + 1])
                assert args[args.index('--metadata') + 1] == 'sha256=' + worker.sha(source)
                path.write_bytes(source.read_bytes())
            return b'{}'

        def fetch(bucket, ident, path):
            assert bucket == config['bucket']
            assert any(ident == candidate for item in config['items'] for candidate in item['inputs'].values())
            fetched.append(ident['key'])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'synthetic input')
            return dict(path=str(path), bytes=path.stat().st_size, sha256=worker.sha(path))

        def popen(command, **kwargs):
            cell = len(commands)
            item, k = config['items'][cell // 4], config['setting_order'][cell % 4]
            authority = item['authority']
            assert command[-8:-1] == [str(binary), config['bucket'], config['region'], item['indexes'][str(k)],
                authority['root_sha256'], str(authority['generation']), str(authority['control_epoch'])]
            assert command[-1].rsplit(':', 1)[-1] == '8080'
            assert kwargs['start_new_session'] is True
            commands.append(command)
            return Mock(poll=Mock(return_value=None))

        def health(url, **kwargs):
            assert url == 'http://127.0.0.1:8080/health'
            item = config['items'][(len(commands) - 1) // 4]
            return contextlib.closing(io.BytesIO(json.dumps(dict(authority=item['authority'], dimensions=768)).encode()))

        def stop(server):
            stopped.append(server)
            return dict(returncode=-15, intentional_stop=True)

        def measure(args, **kwargs):
            cell = len(measurements)
            item, k = config['items'][cell // 4], config['setting_order'][cell % 4]
            path, output = Path(args[2]), Path(args[5])
            assert args[:2] == [sys.executable, 'scripts/run_native_peer_offered_http.py']
            assert args[3] == worker.sha(path) and args[4] == ENDPOINT and kwargs == dict(check=True)
            peer = json.loads(path.read_text())
            assert peer['dataset'] == item['dataset'] and peer['query_split'] == item['query_split']
            assert peer['authority'] == item['authority'] and peer['k'] == k
            assert (peer['count'], peer['offered_qps'], peer['workers'], peer['rows'], peer['dimensions']) == (64, 8, 8, 1000000, 768)
            assert peer['metadata_resident'] is True and peer['application_sq8_cache'] is False
            assert peer['code_sha256'] == config['code_sha256']
            for name, source in [('requests', 'requests'), ('reference', f'reference-k{k}'), ('truth', 'truth')]:
                assert peer['inputs'][name]['path'] == str(Path('peer-inputs') / item['dataset'] / source)
            measurements.append(peer)
            output.mkdir()
            result = dict(identity_parity_valid=True, successful_count=64, mean_offered_recall=.95,
                          successful_incoming_http_ms=dict(p90=443.99), achieved_successful_qps=8)
            # Bad k100 metrics must not trigger the k10 admission gate.
            if k == 100 or cell == fail_cell:
                result.update(metric or dict(mean_offered_recall=.94))
            (output / 'result.json').write_text(json.dumps(result))

        out = root / 'output'
        previous = Path.cwd()
        # Keep the real source-hash checks while synthetic input paths stay local.
        real_sha = worker.sha
        source_root = previous
        def sha(path):
            path = Path(path)
            return real_sha(source_root / path if str(path).startswith('scripts/') else path)
        try:
            os.chdir(root)
            with patch.object(worker, 'sha', side_effect=sha), \
                 patch.object(worker, 'aws', side_effect=aws), \
                 patch.object(worker, 'identity', return_value=NODES[role]), \
                 patch.object(worker, 'fetch', side_effect=fetch), \
                 patch.object(worker.subprocess, 'Popen', side_effect=popen), \
                 patch.object(worker.subprocess, 'run', side_effect=measure), \
                 patch.object(worker.subprocess, 'check_output', side_effect=AssertionError('external process')), \
                 patch.object(worker.urllib.request, 'urlopen', side_effect=health), \
                 patch.object(worker, 'stop', side_effect=stop), \
                 patch.object(worker.time, 'sleep', side_effect=AssertionError('unexpected polling')), \
                 patch.object(sys, 'argv', ['worker', role, str(config_path), digest, PREFIX, str(out), str(binary)]), \
                 contextlib.redirect_stdout(io.StringIO()):
                try:
                    worker.main()
                except (AssertionError, KeyError, json.JSONDecodeError):
                    if not corrupt:
                        raise
                    assert not (out / 'summary.json').exists()
                    assert len(commands if role == 'server' else measurements) <= 1
                    assert len(stopped) == len(commands)
                    return
            assert not corrupt, ('malformed marker accepted', role, corrupt)
            summary = json.loads((out / 'summary.json').read_text())
            count = 8 if fail_cell is None else fail_cell + 1
            expected = [dict(cell=cell, dataset=config['items'][cell // 4]['dataset'],
                             k=config['setting_order'][cell % 4], gate_passed=cell != fail_cell)
                        for cell in range(count)]
            assert summary['role'] == role and summary['identity'] == NODES[role]
            assert summary['decisions'] == expected
            assert summary['all_cells_completed'] == (count == 8)
            assert summary['namespace_cold_start_included'] is False and summary['matched_vendor_measured'] is False
            assert len(commands if role == 'server' else measurements) == count
            assert len(stopped) == len(commands)
            coordination = [('get-object', PREFIX + '/launch.json')]
            for cell in range(count):
                if role == 'server':
                    coordination += [('put-object', f'{PREFIX}/ready/{cell}.json'),
                                     ('get-object', f'{PREFIX}/done/{cell}.json'),
                                     ('put-object', f'{PREFIX}/closed/{cell}.json')]
                else:
                    coordination += [('get-object', f'{PREFIX}/ready/{cell}.json'),
                                     ('put-object', f'{PREFIX}/done/{cell}.json'),
                                     ('get-object', f'{PREFIX}/closed/{cell}.json')]
                done = json.loads(marker_path(f'{PREFIX}/done/{cell}.json').read_text())
                assert done['gate_passed'] is (cell != fail_cell)
                assert done['cell'] == cell
                assert all(done[key] == value for key, value in NODES['client'].items())
                if role == 'client':
                    assert done['result_sha256'] == real_sha(out / f'cell{cell}/result.json')
                else:
                    closed = json.loads(marker_path(f'{PREFIX}/closed/{cell}.json').read_text())
                    assert closed == dict(**NODES['server'], cell=cell, intentional_stop=True, returncode=-15)
                    ready = json.loads(marker_path(f'{PREFIX}/ready/{cell}.json').read_text())
                    item = config['items'][cell // 4]
                    assert ready == dict(**NODES['server'], endpoint=ENDPOINT, cell=cell,
                        k=config['setting_order'][cell % 4], dataset=item['dataset'], authority=item['authority'],
                        namespace_ready_ms=ready['namespace_ready_ms'])
                    assert ready['namespace_ready_ms'] >= 0
            assert events == coordination
            assert len(fetched) == (0 if role == 'server' else 4 * ((count - 1) // 4 + 1))
        finally:
            os.chdir(previous)


def main():
    config = json.loads(CONFIG.read_text())
    digest = worker.sha(worker.__file__)
    assert config['worker_sha256'] == digest, 'recopy matching worker/config snapshot'
    assert [item['dataset'] for item in config['items']] == ['ReLAION', 'CoHere']
    for name, expected in config['code_sha256'].items():
        assert worker.sha(name) == expected, name
    for role in ('server', 'client'):
        exercise(config, role)
        for cell in (0, 3, 4, 7):
            exercise(config, role, fail_cell=cell)
        for field in ('instance_id', 'private_ip'):
            exercise(config, role, corrupt=('launch', field, 'wrong'))
        for kind in (('done',) if role == 'server' else ('ready', 'closed')):
            for field, value in [('instance_id', 'wrong'), ('private_ip', 'wrong'), ('cell', 99)]:
                exercise(config, role, corrupt=(kind, field, value))
            exercise(config, role, corrupt=(kind, None, None))
    for field, value in [('endpoint', 'http://wrong:8080'), ('dataset', 'wrong'),
                         ('authority', {}), ('k', 100)]:
        exercise(config, 'client', corrupt=('ready', field, value))
    exercise(config, 'server', corrupt=('done', 'gate_passed', 'false'))
    exercise(config, 'client', corrupt=('closed', 'intentional_stop', False))
    for metric in [dict(successful_count=63), dict(mean_offered_recall=.94999),
                   dict(successful_incoming_http_ms=dict(p90=444)), dict(achieved_successful_qps=7.999)]:
        exercise(config, 'client', fail_cell=0, metric=metric)
    print('PASS worker main: two roles, two datasets/four cells each, marker identities/order, k10 stop, malformed markers')
    print('worker_sha256=' + digest)


if __name__ == '__main__':
    main()
