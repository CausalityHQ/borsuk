#!/usr/bin/env python3
"""Root-owned startup wave4/wave8 ABBA: aNNNN | --replay DIR | --self-check.

--worker CONFIG SHA REPO NEW_OUTPUT PREFIX runs the existing paired runtime.
Only local synthetic checks are authorized during controller development.
"""
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from unittest.mock import Mock, patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_native_startup_wave8_paired as worker
from scripts import launch_native_semantic_1m_offered_spot as offered

native, panel, ids = worker.native, worker.panel, worker.ids
ROOT, CONFIG, PREFIX = worker.ROOT / 'paired', worker.CONFIG, worker.PREFIX
MODULE = 'scripts.launch_native_startup_wave8_paired_spot'
# Keep the runtime config's roster; bind the full controller closure separately.
CODE = tuple(sorted((*worker.CODE, MODULE.replace('.', '/')+'.py',
    'scripts/check_native_semantic_metadata_cold.py', 'scripts/launch_native_semantic_metadata_cold_spot.py',
    'scripts/launch_native_workspace_execution_spot.py', 'scripts/run_native_semantic_metadata_cold.py')))
NAME = ''
SCHEMA = 'borsuk-startup-wave8-paired-spot-v2'
TOKEN_PREFIX, TAG = 'startup-wave8-paired-', 'borsuk-startup-wave8-paired'
WALL = worker.FIXED['machine_limit_seconds']
INSTANCE_TYPE, IMAGE_ID = panel.INSTANCE_TYPE, panel.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = panel.ROOT_DEVICE_NAME, panel.SUBNET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .50
AWSCLI_VERSION, AWSCLI_SHA256 = offered.AWSCLI_VERSION, offered.AWSCLI_SHA256
SDK_PACKAGES = offered.SDK_PACKAGES
ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/' + n for n in worker.OUTPUTS))
CONTROLLER_FIELDS = ('namespace_prefix', 'controller_code_sha256', 'controller_code_identity_sha256',
    'controller_sha256', 'campaign_schema', 'artifact_roster_sha256', 'awscli_version', 'awscli_sha256')
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256',
    'roles', 'measurement_prefix', 'prices', 'cold_terminal_sha256', 'native_rebuilt',
    'publication_invocations', 'current_whole_tree_full_execution',
    *(n for n in CONTROLLER_FIELDS if n != 'controller_code_sha256'))


def qualify(base=Path('.')):
    base = Path(base).resolve()
    config, proof = worker.qualify(base/CONFIG, worker.artifact(base/CONFIG)['sha256'], base)
    namespace = json.loads(worker.read(base, config['cold_config']))['namespace_prefix']
    assert not namespace.startswith(proof['measurement_prefix']), 'measurement/publication separation'
    code = {n: worker.artifact(panel.repo_path(base, n))['sha256'] for n in CODE}
    assert {n: code[n] for n in worker.CODE} == config['code_sha256']
    return dict(proof, namespace_prefix=namespace, controller_code_sha256=code,
        controller_code_identity_sha256=worker.sha(worker.encoded(code)),
        controller_sha256=code[MODULE.replace('.', '/')+'.py'], campaign_schema=SCHEMA,
        artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    return proof


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    with patch.multiple(offered, CONFIG=CONFIG, SCHEMA=SCHEMA, PREFIX=PREFIX,
        ARTIFACTS=ARTIFACTS, TERMINAL_IDENTITIES=TERMINAL_IDENTITIES, WALL=WALL):
        body = offered.user_data(commit, archive_sha, archive_key, prefix, qualification)
    body = body.replace('scripts.launch_native_semantic_1m_offered_spot', MODULE)
    body = body.replace('launch_native_semantic_1m_offered_spot as launch', MODULE.split('.')[-1]+' as launch')
    body = body.replace('semantic-1m-offered', 'startup-wave8-paired')
    smoke = ('PYTHONPATH="$root/repo" "$root/venv/bin/python" -c \'import numpy, pyarrow; '
        'from scripts import '+MODULE.split('.')[-1]+' as launch; launch.ids.lifecycle()\'')
    assert body.count(smoke) == 1, 'bootstrap import hook changed'
    body = body.replace(smoke, 'test "$(getconf GNU_LIBC_VERSION)" = \'glibc 2.39\'\n'
        'PYTHONPATH="$root/repo" "$root/venv/bin/python" -c \'import numpy, pyarrow\'\n'
        f'PYTHONPATH="$root/repo" "$root/venv/bin/python" -m {MODULE} --import-smoke')
    hook = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    assert body.count(hook) == 1, 'terminal hook changed'
    body = body.replace(hook, hook+"'scientific_qualification':('PASS' if json.loads(Path('screen/summary.json').read_bytes())['paired_gate_passed'] is True else 'FAIL') if code==0 and os.environ['PHASE']=='complete' else None,")
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<terminal>', 'exec')
    assert len(body.encode()) < 16384 and '--import-smoke' in body
    assert MODULE+' --worker' in body and 'IOAccounting=yes' in body
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(offered, 'WALL', WALL):
        return offered.poll(ec2, s3, prefix, instance_id, started)


def checkpoint(marker, paths, config, proof, output, uploaded, put):
    """Authenticate a drained/fsynced cell, then publish its seal last."""
    assert set(paths) == {'records', 'summary', 'seal'}
    index = marker['cell_index']
    assert type(index) is int and 0 <= index < 4 and index == len(uploaded) and index not in uploaded
    role = worker.CELLS[index]; authority = worker.role_authority(config, role)
    assert marker['schema'] == 'borsuk-startup-wave8-paired-cell-v2'
    assert type(marker['workers']) is int and marker['workers'] == config['workers']
    assert marker['closed'] is marker['cleanup_confirmed'] is True
    assert marker['role'] == role and type(marker['wave_objects']) is int and marker['wave_objects'] == worker.WIDTHS[role]
    assert marker['role_authority'] == authority and marker['counts']['planned'] == 64
    for name, suffix in (('records', 'records.jsonl'), ('summary', 'summary.json'), ('seal', 'seal.json')):
        path = Path(paths[name])
        assert not path.is_symlink() and path.is_file()
        assert path.absolute() == path.resolve() == output.resolve()/f'cell{index}-{suffix}'
        assert worker.artifact(path) == marker[name], 'closed body identity: '+name
    assert json.loads(paths['summary'].read_bytes()) == {n: v for n, v in marker.items() if n not in ('summary', 'seal')}
    assert json.loads(paths['seal'].read_bytes()) == dict(schema='borsuk-startup-wave8-cell-seal-v2',
        cell_index=index, role=role, records=marker['records'], summary=marker['summary'])
    for name in ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256'):
        assert marker[name] == proof[name]
    assert marker['binary_sha256'] == authority['binary']['sha256'] == proof['roles'][role]['binary']['sha256']
    body = paths['records'].read_bytes(); rows = [json.loads(line) for line in body.splitlines()]
    assert body == b''.join(worker.encoded(row)+b'\n' for row in rows)
    assert len(rows) == 64 and all(type(r['query_ordinal']) is int for r in rows)
    assert [r['query_ordinal'] for r in rows] == list(range(64))
    assert all(type(r['cell_index']) is int and r['cell_index'] == index and r['role'] == role
        and type(r['wave_objects']) is int and r['wave_objects'] == worker.WIDTHS[role]
        and r['role_authority'] == authority for r in rows)
    receipt = rows[0]['cell_receipt']
    assert receipt['cell_scratch_removed'] is True and not (output/'scratch'/f'cell{index}').exists()
    assert receipt['binary_after'] == (authority['binary'] if receipt['cell_started'] else None)
    assert all(not r['native_process_started'] or r['cleanup_confirmed'] is True
        and r['native_close']['process_group_closed'] is True for r in rows), 'native drain'
    for name in ('records', 'summary', 'seal'):
        path = paths[name]
        put(['aws', 's3api', 'put-object', '--if-none-match', '*', '--bucket', config['bucket'],
            '--key', config['measurement_prefix']+'/cells/'+path.name, '--body', str(path),
            '--region', config['region'], '--cli-connect-timeout', '5', '--cli-read-timeout', '15', '--no-cli-pager'])
    uploaded.add(index)


def remote_worker(args):
    assert len(args) == 5
    config_path, digest, repo, output, prefix = args
    config, proof = worker.qualify(config_path, digest, repo)
    assert prefix == config['measurement_prefix']
    _, bootstrap = ids.lifecycle()
    bootstrap._check_checkpoint_cli()
    uploaded = set()
    return worker.main(config_path, digest, repo, output, on_cell_closed=lambda m, p:
        checkpoint(m, p, config, proof, Path(output), uploaded, bootstrap._checkpoint_cli_call))


def replay(out, repo=None):
    """Authenticate the entire terminal roster before offline worker replay."""
    out = Path(out); repo = Path(repo) if repo is not None else Path(__file__).resolve().parents[1]
    launch, closed, reservation, terminal = (json.loads((out/n).read_bytes()) for n in (
        'aws-launch.json', 'aws-closeout.json', 'aws-reservation.json', 'aws-terminal.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes']
    nodes = [n['instance_id'] for n in closed['nodes'].values()]
    assert nodes and len(nodes) == len(set(nodes)) and all(type(n) is str and n for n in nodes)
    assert terminal['instance_id'] == launch['instance_id'] in nodes
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', launch['prefix'])
    for name, length in (('source_commit', 40), ('source_archive_sha256', 64)):
        assert re.fullmatch('[0-9a-f]{'+str(length)+'}', launch[name])
        assert terminal[name] == reservation[name] == launch[name]
    proof = reservation['qualification']; assert proof == qualify(repo)
    assert proof['measurement_prefix'] == launch['prefix']
    for name in TERMINAL_IDENTITIES:
        assert terminal[name] == proof[name], 'terminal identity: '+name
    assert reservation['config_sha256'] == proof['config_sha256']
    assert reservation['wall_seconds'] == WALL and reservation['instance_type'] == INSTANCE_TYPE
    assert reservation['image_id'] == IMAGE_ID and reservation['root_device_name'] == ROOT_DEVICE_NAME
    assert reservation['subnet_id'] == SUBNET
    assert reservation['spot_max_usd_per_hour'] == SPOT_MAX_USD_PER_HOUR and reservation['compute_cap_usd'] == COMPUTE_CAP
    assert reservation['ebs_s3_allowance_usd'] == .15 and reservation['total_cost_measured'] is False
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    # This loop must finish before opening ledger/summary bodies for reduction.
    for name, pointer in terminal['artifacts'].items():
        assert worker.artifact(out/name) == pointer, 'terminal body: '+name
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['status'] == ('complete' if complete else 'failed')
    if not complete:
        assert terminal['scientific_qualification'] in (None, 'FAIL')
        return dict(executed=False, execution_gate_passed=False, paired_gate_passed=False,
            scientific_qualification='UNMEASURED')
    assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(ARTIFACTS)
    source = {n: terminal[n] for n in ('source_commit', 'source_archive_sha256')}
    expected = dict({n: v for n, v in proof.items() if n not in CONTROLLER_FIELDS}, **source)
    assert json.loads((out/'screen/source-qualification.json').read_bytes()) == expected
    result = worker.replay(repo/CONFIG, proof['config_sha256'], repo, out/'screen')
    assert result['execution_gate_passed'] is True and type(result['paired_gate_passed']) is bool
    scientific = 'PASS' if result['paired_gate_passed'] else 'FAIL'
    assert terminal['scientific_qualification'] == scientific, 'scientific FAIL cannot become PASS'
    return dict(executed=True, execution_gate_passed=True, paired_gate_passed=result['paired_gate_passed'],
        scientific_qualification=scientific,
        candidate_over_actual_bracketing_control_latency_ratio=result['candidate_over_actual_bracketing_control_latency_ratio'])


def collect(s3, prefix, out, instance_id, commit, digest):
    launch, closed = (json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json', 'aws-closeout.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes']
    assert launch['prefix'] == prefix and launch['instance_id'] == instance_id
    assert launch['source_commit'] == commit and launch['source_archive_sha256'] == digest
    with patch.multiple(ids, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, replay=replay):
        return ids.collect(s3, prefix, out, instance_id, commit, digest)


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}', attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()
        lifecycle, _ = ids.lifecycle()
        return lifecycle.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """Local stdlib checks; real producers/replay, mocked process and SDK effects."""
    import ast
    import contextlib
    import io
    import resource
    import shutil
    started = time.monotonic()
    here = Path(__file__).resolve().parents[1]
    module = sys.modules[__name__]
    def rejects(action):
        try: action()
        except (AssertionError, ValueError, KeyError, TypeError, FileNotFoundError, RuntimeError): return
        raise AssertionError('invalid evidence accepted')
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / 'screen'; output.mkdir()
        config = dict(worker.FIXED, measurement_prefix=PREFIX + 'a0001', roles={
            r: dict(wave_objects=w, native_source_commit=worker.SOURCE_COMMITS[r],
                binary=dict(bytes=10, sha256=('b' if r == 'control' else 'c')*64),
                proof=dict(bytes=10, sha256='d'*64), source_manifest=dict(bytes=10, sha256='e'*64))
            for r, w in worker.WIDTHS.items()})
        proof = dict(config_sha256='b'*64, code_identity_sha256='c'*64, refs_identity_sha256='d'*64,
            roles={r: dict(binary=worker.identity(b['binary'])) for r, b in config['roles'].items()})
        rows = worker.aborted_rows(0, 1, dict(reason='synthetic'), config)
        rows[0]['cell_receipt'] = dict(cell_scratch_removed=True, cell_started=False, binary_after=None)
        result = dict(schema='borsuk-startup-wave8-paired-cell-v2',workers=config['workers'],cell_index=0, role='control', wave_objects=4, role_authority=worker.role_authority(config, 'control'),
            closed=True, cleanup_confirmed=True, counts=dict(planned=64))
        closed = []
        worker.close_cell(output, rows, result, proof, lambda m, p: closed.append((m, p)))
        marker, paths = closed[0]
        uploaded, calls = set(), []
        for field, value in (('schema','borsuk-startup-wave8-paired-cell-v1'),('workers',6),('workers',True),('closed', False), ('cell_index', True), ('role', 'candidate'),
            ('wave_objects', 8), ('config_sha256', '0'*64), ('binary_sha256', '0'*64)):
            rejects(lambda: checkpoint(dict(marker, **{field: value}), paths, config, proof, output, set(), calls.append))
        rejects(lambda: checkpoint(marker, {'records': paths['records'], 'summary': paths['summary']},
            config, proof, output, set(), calls.append))
        for name in paths:
            saved = paths[name].read_bytes(); paths[name].write_bytes(b'tampered')
            rejects(lambda: checkpoint(marker, paths, config, proof, output, set(), calls.append))
            paths[name].write_bytes(saved)
        scratch = output/'scratch/cell0'; scratch.mkdir(parents=True)
        rejects(lambda: checkpoint(marker, paths, config, proof, output, set(), calls.append))
        shutil.rmtree(output/'scratch')
        assert calls == [], 'invalid callback published a body'
        checkpoint(marker, paths, config, proof, output, uploaded, lambda cmd: calls.append(cmd))
        assert uploaded == {0} and [cmd[cmd.index('--body')+1] for cmd in calls] == [
            str(paths[n]) for n in ('records', 'summary', 'seal')], 'seal must be uploaded last'
        rejects(lambda: checkpoint(marker, paths, config, proof, output, uploaded, calls.append))
        partial = []; pending = set()
        def interrupted(cmd):
            partial.append(cmd)
            if len(partial) == 2: raise RuntimeError('synthetic upload failure')
        rejects(lambda: checkpoint(marker, paths, config, proof, output, pending, interrupted))
        assert not pending and len(partial) == 2 and all('seal.json' not in cmd[cmd.index('--key')+1] for cmd in partial)
        # A rehashed body still cannot claim drained native processes.
        bad = copy.deepcopy(rows); bad[0].update(native_process_started=True, cleanup_confirmed=False)
        second = output.parent/'undrained'; second.mkdir(); callbacks = []
        worker.close_cell(second, bad, result, proof, lambda m, p: callbacks.append((m, p)))
        rejects(lambda: checkpoint(*callbacks[0], config, proof, second, set(), calls.append))
        # Pending authority must fail before SDK construction or any output.
        repo = output.parent/'repo'; target = repo/CONFIG; target.parent.mkdir(parents=True)
        target.write_bytes(worker.encoded(dict(authority_pending=True)))
        with patch.object(worker.subprocess, 'Popen', side_effect=AssertionError('process forbidden')):
            rejects(lambda: preflight(repo))
            rejects(lambda: remote_worker([str(target), worker.artifact(target)['sha256'], str(repo),
                str(output.parent/'pending-output'), PREFIX+'a0001']))
        assert not (output.parent/'pending-output').exists()
        # Original candidate evidence is immutable and explicitly failed.
        failed_receipt = here/worker.ROOT/'implementation-gates/a0001/workspace-receipt.json'
        receipt = json.loads(failed_receipt.read_bytes())
        assert receipt['qualified'] is False and receipt['exit_status'] == receipt['gate_status'] == 101
        pointer = dict(path=str(failed_receipt.relative_to(here)), **worker.artifact(failed_receipt))
        rejects(lambda: worker.validate_candidate(here, {}, dict(evidence={'workspace_receipt': pointer})))

    # Exercise the real paired producer, proofs and replay using its existing
    # synthetic fixture. The controller adds closure/authentication at that seam.
    lifecycle, bootstrap = ids.lifecycle()
    original_main, original_replay = worker.main, worker.replay
    checks = []
    def exercise(config_path, digest, repo, output, *, on_cell_closed=None):
        if Path(output).name != 'runtime':
            return original_main(config_path, digest, repo, output, on_cell_closed=on_cell_closed)
        repo, output = Path(repo), Path(output)
        controller_path = repo/(MODULE.replace('.', '/')+'.py')
        for name in set(CODE)-set(worker.CODE):
            target = repo/name; target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((here/name).read_bytes())
        frozen = qualify(repo)
        assert set(frozen['controller_code_sha256']) == set(CODE)
        assert frozen['controller_sha256'] == worker.artifact(controller_path)['sha256']
        config = json.loads(Path(config_path).read_bytes())
        keys = []
        def put(cmd):
            assert cmd[:3] == ['aws', 's3api', 'put-object'] and cmd[cmd.index('--if-none-match')+1] == '*'
            keys.append(cmd[cmd.index('--key')+1])
        def run(*args, on_cell_closed):
            def both(marker, paths):
                on_cell_closed(marker, paths)
                if external is not None: external(marker, paths)
            return original_main(*args, on_cell_closed=both)
        external = on_cell_closed
        with patch.object(worker, 'main', side_effect=run), patch.object(bootstrap, '_check_checkpoint_cli') as cli, \
            patch.object(bootstrap, '_checkpoint_cli_call', side_effect=put):
            actual = remote_worker([str(config_path), digest, str(repo), str(output), config['measurement_prefix']])
        cli.assert_called_once()
        assert keys == [PREFIX+'a0001/cells/'+f'cell{i}-{suffix}'
            for i in range(4) for suffix in ('records.jsonl', 'summary.json', 'seal.json')]

        body = user_data('a'*40, 'b'*64, 'source/key', PREFIX+'a0001', frozen)
        install = next(line for line in body.splitlines() if ' -m pip install ' in line)
        assert install.split('--no-deps ', 1)[1].split() == ['numpy==2.3.3', 'pyarrow==24.0.0', *SDK_PACKAGES]
        assert '--only-binary=:all:' in install
        assert body.index(MODULE+' --import-smoke') < body.index('systemd-run --unit=startup-wave8-paired --wait')
        assert all(n in body for n in ('MemoryMax=8G', 'MemorySwapMax=0', 'CPUQuota=200%', 'TasksMax=512',
            'RuntimeMaxSec=3000', '--on-active=3600s', 'taskset -c 4-5', 'IOAccounting=yes', 'glibc 2.39'))
        command = body.split('systemd-run --unit=startup-wave8-paired --wait', 1)[1].split('\nfor name', 1)[0]
        argv = subprocess.check_output(['bash', '-c', 'systemd-run() { printf "%s\\n" "$@"; }; '
            'root=/mock; systemd-run --unit=startup-wave8-paired --wait'+command], text=True).splitlines()
        assert argv[-8:] == ['-m', MODULE, '--worker', '/mock/repo/'+str(CONFIG), digest,
            '/mock/repo', '/mock/screen', PREFIX+'a0001']
        terminal_code = body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0]
        source = dict(source_commit='a'*40, source_archive_sha256='b'*64)
        nodes = {'0': dict(instance_id='i-original'), '1': dict(instance_id='i-extra')}
        def closed_campaign(screen, destination):
            shutil.copytree(screen, destination/'screen')
            for name in ('test-resources.txt', 'run-closed.log'): worker.write(destination/name, b'synthetic log\n')
            environment = dict(ARTIFACT_NAMES=' '.join(ARTIFACTS), INSTANCE_ID='i-original', EXIT_CODE='0',
                ORIGINAL_EXIT_CODE='0', PHASE='complete')
            before = Path.cwd(); stdout = io.StringIO()
            try:
                os.chdir(destination)
                with patch.dict(os.environ, environment), contextlib.redirect_stdout(stdout):
                    exec(compile(terminal_code, '<generated-terminal>', 'exec'), {})
            finally: os.chdir(before)
            terminal = json.loads(stdout.getvalue())
            reservation = dict(source, schema=SCHEMA, qualification=frozen, config_sha256=digest,
                wall_seconds=WALL, instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
                subnet_id=SUBNET, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, compute_cap_usd=COMPUTE_CAP,
                ebs_s3_allowance_usd=.15, total_cost_measured=False)
            for name, value in (('aws-launch.json', dict(source, instance_id='i-original', nodes=nodes, prefix=PREFIX+'a0001')),
                ('aws-closeout.json', dict(state='terminated', nodes=nodes)), ('aws-reservation.json', reservation),
                ('aws-terminal.json', terminal)):
                worker.write(destination/name, value)
            return terminal
        collected = output.parent/'controller-collected'
        terminal = closed_campaign(output, collected)
        assert replay(collected, repo)['paired_gate_passed'] is True
        terminal_path = collected/'aws-terminal.json'
        for field, value in (('source_commit', '0'*40), ('source_archive_sha256', '0'*64),
            ('instance_id', 'i-unowned'), ('controller_sha256', '0'*64), ('roles', {}), ('artifacts', {})):
            terminal_path.write_bytes(worker.encoded(dict(terminal, **{field: value})))
            with patch.object(worker, 'replay', wraps=original_replay) as checked:
                rejects(lambda: replay(collected, repo)); checked.assert_not_called()
        terminal_path.write_bytes(worker.encoded(terminal))
        for name in ARTIFACTS:
            target = collected/name; saved = target.read_bytes(); target.write_bytes(saved+b'tampered')
            with patch.object(worker, 'replay', wraps=original_replay) as checked:
                rejects(lambda: replay(collected, repo)); checked.assert_not_called()
            target.write_bytes(saved)
        target = collected/'aws-closeout.json'; saved = target.read_bytes()
        for value in (dict(state='running', nodes=nodes), dict(state='terminated', nodes={'0': nodes['0']})):
            target.write_bytes(worker.encoded(value)); rejects(lambda: replay(collected, repo))
        target.write_bytes(saved)
        incomplete = dict(terminal, phase='quality', status='failed', exit_code=1, original_exit_code=1,
            scientific_qualification=None, artifacts={'run-closed.log': terminal['artifacts']['run-closed.log']})
        terminal_path.write_bytes(worker.encoded(incomplete))
        with patch.object(worker, 'replay', side_effect=AssertionError('incomplete execution replayed')):
            assert replay(collected, repo)['executed'] is False
        terminal_path.write_bytes(worker.encoded(terminal))

        # Collect the exact authenticated byte roster only after all ACKs closed.
        sdk_bodies = {'terminal.json': terminal_path.read_bytes(),
            **{'artifacts/'+name: (collected/name).read_bytes() for name in ARTIFACTS}}
        s3 = Mock(); s3.get_object.side_effect = lambda **kw: {'Body': io.BytesIO(sdk_bodies[kw['Key'].removeprefix(PREFIX+'a0001/')])}
        target = output.parent/'sdk-collected'; target.mkdir()
        for name in ('aws-launch.json', 'aws-closeout.json', 'aws-reservation.json'):
            (target/name).write_bytes((collected/name).read_bytes())
        root_replay = replay
        with patch.object(module, 'replay', side_effect=lambda out: root_replay(out, repo)):
            assert collect(s3, PREFIX+'a0001', target, 'i-original', 'a'*40, 'b'*64) == terminal
        assert s3.get_object.call_count == len(ARTIFACTS)+1
        assert all(worker.artifact(target/n) == terminal['artifacts'][n] and (target/(n+'.gz')).is_file() for n in ARTIFACTS)

        # A closed quality failure is a scientific FAIL, with no speedup claim.
        evidence = copy.deepcopy(worker.inputs(repo, config)); evidence['truth'] = [list(range(100, 200)) for _ in range(64)]
        ledger = [json.loads(line) for line in (output/'records.jsonl').read_bytes().splitlines()]
        measured_cells = []
        for row in ledger: row['returned_hits'] = 0
        clock = [10**12-10]
        def now(): clock[0] += 1; return clock[0]
        def schedule(*args, **kwargs):
            index = len(measured_cells); measured_cells.append(index)
            assert kwargs['workers'] == config['workers'] == 8
            selected = copy.deepcopy(ledger[index*64:(index+1)*64])
            receipt = selected[0]['cell_receipt']; clock[0] = receipt['terminal_ns']+1
            return selected, receipt['epoch_ns'], receipt['terminal_ns'], None
        failed_output = output.parent/'scientific-runtime'
        with patch.object(worker, 'main', original_main), patch.object(worker, 'inputs', return_value=evidence), \
            patch.object(worker.offered, 'schedule_offers', side_effect=schedule), patch.object(worker.time, 'monotonic_ns', side_effect=now), \
            patch.object(bootstrap, '_check_checkpoint_cli'), patch.object(bootstrap, '_checkpoint_cli_call', side_effect=put):
            failure = remote_worker([str(config_path), digest, str(repo), str(failed_output), PREFIX+'a0001'])
            failed_collection = output.parent/'scientific-collected'; failed_terminal = closed_campaign(failed_output, failed_collection)
            failed_replay = replay(failed_collection, repo)
            assert failure['execution_gate_passed'] and failed_replay['executed'] and not failed_replay['paired_gate_passed']
            assert failed_replay['scientific_qualification'] == failed_terminal['scientific_qualification'] == 'FAIL'
            assert failed_replay['candidate_over_actual_bracketing_control_latency_ratio'] == 'UNMEASURED'
            assert measured_cells == list(range(4)) and failure['counts']['successful'] == 256
            assert all(c['cell_started'] and not c['quality_gate_passed'] for c in failure['cells'])
            assert failure['campaign_abort'] is None and failure['collection_policy'] == config['collection_policy']
            (failed_collection/'aws-terminal.json').write_bytes(worker.encoded(dict(failed_terminal, scientific_qualification='PASS')))
            rejects(lambda: replay(failed_collection, repo))
        checks.append(dict(user_data_bytes=len(body.encode()), terminal_python_bytes=len(terminal_code.encode())))
        return actual

    with patch.object(lifecycle.boto3, 'Session', side_effect=AssertionError('cloud forbidden')) as sessions, \
        patch.object(worker, 'main', side_effect=exercise), contextlib.redirect_stdout(io.StringIO()):
        worker.self_check()
        with patch.object(module, 'preflight', side_effect=AssertionError('pending')):
            rejects(lambda: main('a0001'))
        lifecycle.self_check(lifecycle_only=True)
        sessions.assert_not_called()
        assert len(checks) == 1
    # The closure includes the unchanged native proof's explicitly pinned helpers.
    seen = set()
    def closure(name):
        if name in seen: return
        seen.add(name)
        for node in ast.walk(ast.parse((here/name).read_bytes())):
            targets = []
            if isinstance(node, ast.ImportFrom):
                targets = [a.name for a in node.names] if node.module == 'scripts' else (
                    [node.module[8:]] if node.module and node.module.startswith('scripts.') else [])
            elif isinstance(node, ast.Import):
                targets = [a.name[8:] for a in node.names if a.name.startswith('scripts.')]
            for target in targets:
                path = 'scripts/'+target.replace('.', '/')+'.py'
                if (here/path).is_file(): closure(path)
    closure(MODULE.replace('.', '/')+'.py')
    assert seen | set(native.CODE) == set(CODE), 'controller import/evidence closure drift'
    assert time.monotonic()-started < 55 and resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024 <= 200*1024**2
    print('PASS paired controller: source/binary/proof/width negatives, drained callback/seal ordering, '
        'generated shell/terminal, actual producer and closed replay, full-roster tamper rejection, '
        'four-cell nonfatal scientific FAIL preserved, all-ACK fsync/interrupt/terminate-wait/collection; '+json.dumps(checks[0])+
        '; synthetic Python/process/SDK only, no native/Cargo/network/corpus/cloud')


if __name__ == '__main__':
    args = sys.argv[1:]
    if args == ['--self-check']:
        self_check()
    elif args == ['--import-smoke']:
        ids.lifecycle()
        print('PASS paired controller/runtime/lifecycle imports; no cloud client')
    elif len(args) == 2 and args[0] == '--replay':
        print(worker.encoded(replay(args[1])).decode())
    elif args and args[0] == '--worker':
        remote_worker(args[1:])
    else:
        assert len(args) == 1, 'aNNNN | --self-check | --replay DIR'
        with open('/tmp/borsuk-startup-wave8-paired-launch.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(args[0])
