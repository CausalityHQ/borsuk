"""One explicit workspace execution, test build or gate pipeline; no retry or cached target."""
import json
import hashlib
import os
from pathlib import Path
import resource
import shutil
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


def main(cargo, repo, out, *, semantic_1m=False, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False, budget_object_selector=False, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, cohere_sq8_builder=False, exact_sq8_runtime=False, retained_generation_rebind=False, baseline_pool=False):
    from scripts import launch_native_workspace_execution_spot as controller
    with controller.execution_mode(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection, budget_object_selector=budget_object_selector, budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort, cohere_sq8_builder=cohere_sq8_builder, exact_sq8_runtime=exact_sq8_runtime, retained_generation_rebind=retained_generation_rebind, baseline_pool=baseline_pool):
        return _execute(cargo, repo, out, controller)


def _execute(cargo, repo, out, controller):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    assert not out.is_relative_to(repo), 'output/target must be outside source'
    proof = json.loads((out/'source-qualification.json').read_bytes())
    assert controller.qualify(repo) == proof, 'worker authority drift'
    assert artifact(out/'config.json') == artifact(repo/controller.CONFIG)
    assert artifact(out/'native-source-manifest.json') == artifact(repo/proof['native_source_manifest']['path'])
    if controller.COHERE_SQ8_BUILDER:
        controller.completed_native_qualification(out, proof, retained=True)
    environment = controller.FIXED['environment']
    if controller.TEST_BUILD or controller.IMPLEMENTATION:
        assert not os.environ.get('BORSUK_TEST_BUILD_COMMAND'), 'test-only build shim forbidden'
        assert Path(cargo).name == 'cargo', 'gate Cargo executable'
    target = out/'target'
    target.mkdir(exist_ok=False)  # Never reuse even an empty previous target.
    command = list(controller.FIXED['command']) if controller.TEST_BUILD or controller.IMPLEMENTATION else [str(cargo), *COMMAND[1:]]
    env = dict(os.environ, CARGO_TARGET_DIR=str(target))
    for key, value in environment.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    if controller.TEST_BUILD or controller.IMPLEMENTATION:
        env['PATH'] = str(Path(cargo).absolute().parent) + os.pathsep + env.get('PATH', '')
    before = source_hashes(repo)
    _write(out/'source-before.json', before)
    assert before == proof['source_sha256'], 'source before execution'
    report = dict(schema=controller.RECEIPT_SCHEMA, qualified=False,
        exit_status=None, gate_status=96, command=command, command_started=False,
        command_completed=False, source_unchanged=False, source_sha256=before,
        source_file_count=len(before), source_identity_sha256=source_identity(before),
        config_sha256=proof['config_sha256'], code_identity_sha256=proof['code_identity_sha256'],
        campaign_schema=proof['campaign_schema'], artifact_roster_sha256=proof['artifact_roster_sha256'],
        qualification_sha256=artifact(out/'source-qualification.json')['sha256'],
        environment=environment, fresh_target=str(target), artifacts={})
    if controller.TEST_BUILD or controller.IMPLEMENTATION:
        report.update(execution_kind=controller.FIXED['execution_kind'], actual_full_workspace_execution=False)
    if controller.STARTUP_WAVE8 or controller.ROOT_REUSE or controller.BOUNDED_PUBLICATION or controller.FIXED48 or controller.MINIMAL_ARCHIVE:
        report.update(controller_source_commit=proof['controller_source_commit'],
                      candidate_delta_paths=proof['candidate_delta_paths'])
    if controller.CONSTRAINED_SPLIT:
        report.update({key:proof[key] for key in ('mandatory_test_names_pending', 'mandatory_tests',
            'control_native_source_commit', 'control_module_prefix')})
    if controller.CELL_OVERLAP or controller.FINE_SQ8 or controller.PQ_RESIDUAL or controller.CO_SELECTION or controller.BUDGET_OBJECT_SELECTOR or controller.BUDGET_OBJECT_FITTER or controller.COHERE1024 or controller.COHERE_COHORT or controller.COHERE_SQ8_BUILDER:
        report.update({key:proof[key] for key in ('mandatory_test_names_pending', 'mandatory_tests')})
    if controller.COHERE_SQ8_BUILDER:
        report['completed_native_qualification'] = proof['completed_native_qualification']
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
        if controller.IMPLEMENTATION and report['exit_status'] == 0:
            if controller.EXACT_SQ8_RUNTIME:
                # Verbatim probe JSON lines from the raw stage log; shapes/parity validated, timing never interpreted.
                with (out/controller.EXACT_SQ8_PROBE_ARTIFACT).open('xb') as probe:
                    probe.write(controller.probe_lines((out/'test.log').read_bytes()))
                    probe.flush(); os.fsync(probe.fileno())
            (out/'binaries').mkdir(exist_ok=False)
            for name in controller.RELEASE_ARTIFACTS:
                source = target/'release'/('examples' if name.endswith(controller.EXAMPLE_BINARIES) else '')/Path(name).name
                assert source.is_file() and not source.is_symlink(), 'regular release binary: '+name
                identity = artifact(source)
                assert identity['bytes'] > 4, 'empty release binary: '+name
                with source.open('rb') as binary, (out/name).open('xb') as output:
                    assert binary.read(4) == b'\x7fELF', 'ELF release binary: '+name
                    binary.seek(0)
                    shutil.copyfileobj(binary, output, length=1024*1024)
                    os.fchmod(output.fileno(), 0o755)
                    output.flush(); os.fsync(output.fileno())
                assert artifact(out/name) == identity == artifact(source), 'release copy identity: '+name
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
            if controller.COHERE_SQ8_BUILDER:
                controller.completed_native_qualification(out, proof, retained=True)
            assert report['command_completed'] and type(report['exit_status']) is int
            if (controller.BOUNDED_PUBLICATION or controller.FIXED48 or controller.MINIMAL_ARCHIVE) and report['exit_status'] == report['gate_status'] == 0:
                report['stages'] = controller.validate_bounded_publication_stages(out/'test.log', fixed48=controller.FIXED48, hierarchical_cells=controller.HIERARCHICAL_CELLS, constrained_split=controller.CONSTRAINED_SPLIT, cell_overlap=controller.CELL_OVERLAP, fine_sq8=controller.FINE_SQ8, pq_residual=controller.PQ_RESIDUAL, co_selection=controller.CO_SELECTION, budget_object_selector=controller.BUDGET_OBJECT_SELECTOR, budget_object_fitter=controller.BUDGET_OBJECT_FITTER, cohere1024=controller.COHERE1024, cohere_cohort=controller.COHERE_COHORT, cohere_sq8_builder=controller.COHERE_SQ8_BUILDER)
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
    args = sys.argv[1:]
    cohere_sq8_builder = args[:1] == ['--cohere-sq8-builder']
    exact_sq8_runtime = args[:1] == ['--exact-sq8-runtime']
    retained_generation_rebind = args[:1] == ['--retained-generation-rebind-implementation']
    baseline_pool = args[:1] == ['--baseline-pool-implementation']
    cohere_cohort = args[:1] == ['--cohere-cohort-implementation']
    cohere1024 = args[:1] == ['--cohere1024-implementation']
    budget_object_fitter = args[:1] == ['--budget-object-fitter-implementation']
    budget_object_selector = args[:1] == ['--budget-object-selector-implementation']
    co_selection = args[:1] == ['--co-selection-implementation']
    pq_residual = args[:1] == ['--pq-residual-implementation']
    fine_sq8 = args[:1] == ['--fine-sq8-implementation']
    cell_overlap = args[:1] == ['--cell-overlap-implementation']
    constrained_split = args[:1] == ['--constrained-split-implementation']
    hierarchical_cells = args[:1] == ['--hierarchical-cells-implementation']
    fixed48 = args[:1] == ['--fixed48-implementation']
    bounded_publication = args[:1] == ['--bounded-publication-implementation']
    root_reuse = args[:1] == ['--root-reuse-implementation']
    startup_wave8 = args[:1] == ['--startup-wave8-implementation']
    implementation = baseline_pool or cohere_sq8_builder or exact_sq8_runtime or retained_generation_rebind or cohere_cohort or cohere1024 or budget_object_fitter or budget_object_selector or co_selection or pq_residual or fine_sq8 or cell_overlap or constrained_split or hierarchical_cells or fixed48 or bounded_publication or root_reuse or startup_wave8 or args[:1] == ['--semantic-1m-implementation']
    test_build = args[:1] == ['--semantic-1m-test-build']
    semantic_1m = implementation or test_build or args[:1] == ['--semantic-1m']
    if semantic_1m:
        args = args[1:]
    assert len(args) == 3, 'usage: check_native_workspace_execution.py [--baseline-pool-implementation | --cohere-sq8-builder | --exact-sq8-runtime | --retained-generation-rebind-implementation | --semantic-1m | --semantic-1m-test-build | --semantic-1m-implementation | --startup-wave8-implementation | --root-reuse-implementation | --bounded-publication-implementation | --fixed48-implementation | --hierarchical-cells-implementation | --constrained-split-implementation | --cell-overlap-implementation | --fine-sq8-implementation | --pq-residual-implementation | --co-selection-implementation | --budget-object-selector-implementation | --budget-object-fitter-implementation | --cohere1024-implementation | --cohere-cohort-implementation] CARGO REPO OUTPUT'
    result = main(*args, semantic_1m=semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection, budget_object_selector=budget_object_selector, budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort, cohere_sq8_builder=cohere_sq8_builder, exact_sq8_runtime=exact_sq8_runtime, retained_generation_rebind=retained_generation_rebind, baseline_pool=baseline_pool)
    print(json.dumps(result, sort_keys=True))
    sys.exit(result['gate_status'] if result['gate_status'] >= 0 else 128-result['gate_status'])
