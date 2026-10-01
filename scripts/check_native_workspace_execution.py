"""One actual full-workspace execution; no Git, retry, filter, or cached target."""
import json
import hashlib
import os
from pathlib import Path
import resource
import subprocess
import sys

if not __debug__:
    raise RuntimeError('qualification requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_native_startup_build import sha, source_hashes, source_identity

COMMAND = ('cargo', 'test', '--release', '--locked', '--workspace', '--all-targets')
ENVIRONMENT = dict(CARGO_BUILD_JOBS='1', RUST_TEST_THREADS='1', RAYON_NUM_THREADS='2',
                   TOKIO_WORKER_THREADS='2', RUSTC_WRAPPER='', RUSTC_WORKSPACE_WRAPPER='')
MEMORY = 8 * 1024**3
TEST_SECONDS, SERVICE_SECONDS = 7200, 7260


def capture_cgroup():
    relative = Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    group = Path('/sys/fs/cgroup') / relative
    names = ('memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
             'memory.swap.events', 'memory.events', 'cpu.max', 'cpu.stat',
             'pids.max', 'pids.current', 'pids.events')
    return dict(cgroup=str(group), observer_pid=os.getpid(),
        process_ids=[int(pid) for pid in (group/'cgroup.procs').read_text().split()],
        pids_peak=(group/'pids.peak').read_text() if (group/'pids.peak').is_file() else None,
        **{name: (group/name).read_text() for name in names})


def artifact(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024*1024), b''):
            digest.update(chunk)
    return dict(bytes=Path(path).stat().st_size, sha256=digest.hexdigest())


def validate_cgroup(report):
    assert report['closed'] is True, 'test process not closed'
    before, after = report['before'], report['after']
    for counters in (before, after):
        assert int(counters['memory.max']) == MEMORY
        assert 0 <= int(counters['memory.peak']) <= MEMORY
        assert int(counters['memory.swap.max']) == int(counters['memory.swap.peak']) == 0
        quota, period = map(int, counters['cpu.max'].split())
        assert quota == 2 * period and period > 0, 'CPU quota'
        assert int(counters['pids.max']) == 512 and 0 < int(counters['pids.current']) <= 512
        if counters['pids_peak'] is not None:
            assert 0 < int(counters['pids_peak']) <= 512
        assert counters['process_ids'] == [counters['observer_pid']], 'test descendants still running'
        assert counters['cpu.stat'].strip(), 'missing actual CPU evidence'
    for name in ('memory.events', 'memory.swap.events', 'pids.events'):
        def events(counters):
            return {key:int(value) for key,value in (line.split() for line in counters[name].splitlines())}
        first, last = events(before), events(after)
        assert set(first) == set(last) and all(last[k] >= first[k] for k in first), 'counter regression'
        # memory.max events include successful reclaim at the enforced cap.
        failures = ('oom', 'oom_kill', 'oom_group_kill') if name == 'memory.events' else tuple(first)
        assert all(first.get(k,0) == last.get(k,0) == 0 for k in failures), 'resource failure: ' + name


def _write(path, value):
    with Path(path).open('x') as output:
        json.dump(value, output, sort_keys=True, separators=(',', ':'))
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())


def main(cargo, repo, out):
    from scripts import launch_native_workspace_execution_spot as controller
    repo, out = Path(repo).resolve(), Path(out).resolve()
    assert not out.is_relative_to(repo), 'output/target must be outside source'
    proof = json.loads((out/'source-qualification.json').read_bytes())
    assert controller.qualify(repo) == proof, 'worker authority drift'
    assert artifact(out/'config.json') == artifact(repo/controller.CONFIG)
    assert artifact(out/'native-source-manifest.json') == artifact(repo/proof['native_source_manifest']['path'])
    target = out/'target'
    target.mkdir(exist_ok=False)  # Never reuse even an empty previous target.
    command = [str(cargo), *COMMAND[1:]]
    env = dict(os.environ, **ENVIRONMENT, CARGO_TARGET_DIR=str(target))
    before = source_hashes(repo)
    _write(out/'source-before.json', before)
    assert before == proof['source_sha256'], 'source before execution'
    report = dict(schema='borsuk-native-workspace-execution-receipt-v1', qualified=False,
        exit_status=None, gate_status=96, command=command, command_started=False,
        command_completed=False, source_unchanged=False, source_sha256=before,
        source_file_count=len(before), source_identity_sha256=source_identity(before),
        config_sha256=proof['config_sha256'], code_identity_sha256=proof['code_identity_sha256'],
        campaign_schema=proof['campaign_schema'], artifact_roster_sha256=proof['artifact_roster_sha256'],
        qualification_sha256=artifact(out/'source-qualification.json')['sha256'],
        environment=ENVIRONMENT, fresh_target=str(target), artifacts={})
    # Bound the Python orchestrator, while restoring Cargo's original address space limit.
    original_limit = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (min(200*1024**2, original_limit[0])
        if original_limit[0] != resource.RLIM_INFINITY else 200*1024**2, original_limit[1]))
    def child_limit():
        resource.setrlimit(resource.RLIMIT_AS, original_limit)
    resources = dict(closed=False)
    try:
        resources['before'] = capture_cgroup()
        validate_cgroup(dict(before=resources['before'], after=resources['before'], closed=True))
        for name, args in (('rustc-version.txt', [str(Path(cargo).with_name('rustc')), '-vV']),
                           ('cargo-version.txt', [str(cargo), '-V']), ('cpu.txt', ['lscpu'])):
            result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    check=True, timeout=30, env=env, preexec_fn=child_limit)
            (out/name).write_bytes(result.stdout)
        with (out/'test.log').open('xb') as log:
            report['command_started'] = True
            result = subprocess.run(['/usr/bin/time', '-v', '-o', str(out/'test-resources.txt'),
                'timeout', '--signal=TERM', '--kill-after=30', str(TEST_SECONDS), *command],
                cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, check=False,
                preexec_fn=child_limit)
            report['exit_status'] = result.returncode
            report['command_completed'] = resources['closed'] = True
            log.flush(); os.fsync(log.fileno())
        report['gate_status'] = report['exit_status']
    except BaseException as error:
        report['error'] = dict(type=type(error).__name__, message=str(error))
        report['gate_status'] = 97 if isinstance(error, KeyboardInterrupt) else 96
    finally:
        try:
            after = source_hashes(repo)
            _write(out/'source-after.json', after)
            report['source_unchanged'] = before == after == proof['source_sha256']
            resources['after'] = capture_cgroup()
            _write(out/'workspace-cgroup.json', resources)
            validate_cgroup(resources)
            assert report['source_unchanged'], 'source changed during execution'
            assert controller.qualify(repo) == proof, 'config/code authority changed during execution'
            assert report['command_completed'] and type(report['exit_status']) is int
            report['qualified'] = report['exit_status'] == report['gate_status'] == 0
        except BaseException as error:
            report['validation_error'] = dict(type=type(error).__name__, message=str(error))
            if report['gate_status'] == 0:
                report['gate_status'] = 96
        report['artifacts'] = {name: artifact(out/name) for name in controller.ARTIFACTS
            if name not in ('workspace-receipt.json', 'run-closed.log') and (out/name).is_file()}
        _write(out/'workspace-receipt.json', report)
        resource.setrlimit(resource.RLIMIT_AS, original_limit)
    return report


if __name__ == '__main__':
    assert len(sys.argv) == 4, 'usage: check_native_workspace_execution.py CARGO REPO OUTPUT'
    result = main(*sys.argv[1:])
    print(json.dumps(result, sort_keys=True))
    sys.exit(result['gate_status'] if result['gate_status'] >= 0 else 128-result['gate_status'])
