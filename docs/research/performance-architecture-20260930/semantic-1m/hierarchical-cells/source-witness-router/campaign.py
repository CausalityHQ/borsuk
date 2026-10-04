#!/usr/bin/env python3
"""Fixed source-witness transport bridge; root freezes configuration and launches.

CLI: --preflight | --self-check | --canary aNNNN | aNNNN
Private bootstrap/collection: --stage[-canary] REPO NEW_OUTPUT WORKER_ROOT;
--replay[-canary] OUTPUT. No native qualification, compiler, retry or tuning.
"""
import ast
import copy
import gzip
import hashlib
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

REPO = next(p for p in Path(__file__).resolve().parents if (p/'scripts').is_dir())
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from scripts import run_source_witness_paired_coverage as runner
from scripts import launch_hierarchical_cells_100k_spot as bridge

local, probe, publication, ids = runner.local, runner.probe, runner.publication, bridge.ids
require, exact, fields = local.require, local.exact, local.fields
ROOT = Path(runner.BASE.rstrip('/'))
FILE = str(ROOT/'campaign.py')
CONFIG = ROOT/'campaign-config.json'
SCHEMA = 'borsuk-source-witness-fixed-campaign-v1'
CANARY_SCHEMA = 'borsuk-source-witness-fixed-canary-v1'
CONFIG_SCHEMA = 'borsuk-source-witness-fixed-campaign-config-v1'
ADMISSION_SCHEMA = 'borsuk-source-witness-fixed-canary-admission-v1'
PREFIX = 'research/hierarchical-cells/20261004/source-witness-paired100k-'
CANARY_PREFIX = 'research/hierarchical-cells/20261004/source-witness-canary-'
WALL, CANARY_WALL = 2400, 480
CLOSEOUT_SECONDS, CLOSEOUT_RESERVE = 60, 75
MEMORY, CANARY_MEMORY = 1 << 30, 256 << 20
SCRATCH, CANARY_SCRATCH = 16 << 30, 4 << 30
WORKER_ROOT = Path('/mnt/source-witness')
VERSIONS = dict(bridge.FIXED['versions'], **bridge.SDK_VERSIONS)
OUTPUTS = ('config.json', 'source-qualification.json', 'tool-versions.json',
           'resources.json', 'worker-cgroup.json', 'native-drain.json', 'cleanup.json', 'summary.json')
ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in
             (*OUTPUTS, 'runner-config.json', 'runner-return.json', 'runner.stdout',
              'runner.stderr', 'native-output.tar.gz')))
CANARY_ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in
                    (*OUTPUTS, 'canary.json', 'gate.log')))
IDENTITIES = bridge.TERMINAL_IDENTITIES
BINDINGS = ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256',
            'native_identity_sha256', 'source_archive_paths_sha256')
# Small historical metadata only; no binary, archive, log, panel or layout bodies.
REFS = {n: dict(path=str(ROOT/f), bytes=size, sha256=digest) for n, f, size, digest in (
    ('cloud', 'readonly-cloud-admission.json', 5336, '535469a47c07f045f5414a58d0625a61b5281e9f5ae8df0202d57cb9ebfdb726'),
    ('envelope', 'prospective-infrastructure-envelope.json', 1792, '316be5dacb2e2d8a6b973f0e3dd3e6a9546ec7dc48075bccb76a7b35eec9a39a'),
    ('runner_verification', 'runner-verification/root-verification.json', 1508, 'dcdd467c2db1f37a4ddc0f9e31e11c8cbc5e9d7346c2609df99b1ffaa52d999c'),
    ('native_terminal', 'implementation-gates/a0001/aws-terminal.json', 3497, '1e7f7f2bcf4683b6517e7c717fb2e4a77c172a418c956ddfe04b95deaabd4bf5'),
    ('native_launch', 'implementation-gates/a0001/aws-launch.json', 371, 'fa226b7503b4a19c9cf64b0e7e96ab321d0fb38871de36da61b701deb715e02e'))}


def code_roster(repo):
    """Only this bridge and the transitive local Python imports it consumes."""
    pending, seen = [FILE], set()
    while pending:
        name = pending.pop()
        if name in seen:
            continue
        seen.add(name)
        tree = ast.parse((Path(repo)/name).read_text())
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.ImportFrom):
                if node.module == 'scripts':
                    modules = [n.name for n in node.names]
                elif node.module and node.module.startswith('scripts.'):
                    modules = [node.module[8:]]
            elif isinstance(node, ast.Import):
                modules = [n.name[8:] for n in node.names if n.name.startswith('scripts.')]
            for module in modules:
                child = 'scripts/'+module.replace('.', '/')+'.py'
                require((Path(repo)/child).is_file(), 'missing transitive script: '+child)
                pending.append(child)
    return sorted(seen)


def qualify(repo=REPO, *, canary=False):
    repo = Path(repo)
    config_pin = local.identity(repo/CONFIG)
    config = local.read_json(config_pin, 256 << 10)
    fields(config, 'schema authority_pending runner code_sha256', 'fixed bridge configuration')
    exact(config['schema'], CONFIG_SCHEMA, 'fixed configuration schema')
    exact(config['authority_pending'], False, 'root configuration freeze pending')
    authority, native, refs = runner.qualify(config['runner'], repo)
    binary_path = WORKER_ROOT/'assets/hierarchical_semantic_cells'
    exact(config['runner']['binary']['path'], str(binary_path), 'fixed binary staging path')
    for dataset in runner.DATASETS:
        for name in runner.LAYOUT_FILES+runner.PANEL_FILES:
            exact(config['runner']['inputs'][dataset][name]['path'], str(WORKER_ROOT/'assets'/dataset/name), 'fixed input staging path')
    code = {name: local.identity(repo/name)['sha256'] for name in code_roster(repo)}
    exact(config['code_sha256'], code, 'entire consumed script closure')
    values = {name: probe.ref(repo, pin) for name, pin in REFS.items()}
    refs = dict(refs, **REFS)
    checked_runner = values['runner_verification']['source']
    exact(runner.body(checked_runner), runner.body(local.identity(repo/checked_runner['path'])), 'qualified unchanged native runner')
    terminal, launch = values['native_terminal'], values['native_launch']
    exact(terminal['source_file_count'], 402, 'qualified native402')
    exact(terminal['source_identity_sha256'], runner.SOURCE_ID, 'qualified native source identity')
    exact(terminal['phase'], 'complete', 'completed native qualification')
    exact(terminal['status'], 'complete', 'native qualification status')
    exact(terminal['exit_code'], 0, 'native qualification original exit')
    for name in ('source_commit', 'source_archive_sha256', 'instance_id'):
        exact(terminal[name], launch[name], 'native original launch ancestry')
    cloud = values['cloud']
    exact(cloud['bucket'] if 'bucket' in cloud else authority['bucket'], ids.BUCKET, 'asset bucket')
    exact(cloud['instance_type'], 'c7i.large', 'fixed machine')
    exact(cloud['image_id'], ids.IMAGE_ID, 'reviewed Ubuntu AMI')
    exact(cloud['subnet_id'], ids.SUBNET, 'fixed subnet')
    objects = []
    for head in cloud['heads']:
        pin = dict(key=head['key'], bytes=head['bytes'], sha256=head['expected_sha256'])
        publication.object_identity(pin)
        if 'dataset' in head:
            d, n = head['dataset'], head['name']
            exact(runner.body(pin), runner.body(authority['datasets'][d][n]), 'retained object body authority')
            exact(pin['key'], 'research/hierarchical-cells/20261004/capacity-partitioner-paired100k-a0001/artifacts/'+authority['datasets'][d][n]['terminal_path'], 'retained immutable object key')
            pin['destination'] = config['runner']['inputs'][d][n]['path']
        else:
            exact(head['name'], 'binaries/hierarchical_semantic_cells', 'only qualified binary asset')
            exact(runner.body(pin), runner.body(config['runner']['binary']), 'native binary asset')
            exact(runner.body(pin), terminal['artifacts'][head['name']], 'original qualified binary body')
            exact(pin['key'], launch['prefix']+'/artifacts/'+head['name'], 'original binary S3 key')
            pin['destination'] = str(binary_path)
        objects.append(pin)
    exact(len(objects), 11, 'one binary plus ten input bodies')
    exact({p['destination'] for p in objects}, {str(binary_path), *(p['path'] for d in config['runner']['inputs'].values() for p in d.values())}, 'exact asset roster')
    exact(len({p['key'] for p in objects}), 11, 'unique asset keys')
    workspace = probe.ref(repo, runner.WORKSPACE)
    log = dict(terminal['artifacts']['test.log'], key=launch['prefix']+'/artifacts/test.log')
    exact(runner.body(log), workspace['artifacts']['test.log'], 'completed gate-log body')
    require(log['bytes'] <= 1 << 20, 'only small gate-log canary GET')
    paths = sorted({str(CONFIG), *code, *native, *(p['path'] for p in refs.values())})
    shared, _ = ids.lifecycle()
    shared.validate_source_archive_paths(paths)
    require(len(native) == 402 and set(native) <= set(paths), 'full native archive closure')
    for name in paths:
        require((repo/name).is_file() and not (repo/name).is_symlink(), 'regular archive file')
    proof = dict(config_path=str(CONFIG), config_sha256=config_pin['sha256'],
        code_identity_sha256=ids.sha(ids.encoded(code)), refs_identity_sha256=ids.sha(ids.encoded(refs)),
        native_identity_sha256=runner.SOURCE_ID, source_file_count=402,
        source_archive_paths=paths, source_archive_paths_sha256=ids.sha(ids.encoded(paths)),
        artifact_roster_sha256=ids.sha(ids.encoded(CANARY_ARTIFACTS if canary else ARTIFACTS)),
        campaign_schema=CANARY_SCHEMA if canary else SCHEMA,
        awscli_version=ids.AWSCLI_VERSION, awscli_sha256=ids.AWSCLI_SHA256)
    return config, proof, dict(objects=objects, gate_log=log, stages=workspace['stages'])


def preflight(repo=REPO, *, canary=False, launch=False):
    _, proof, _ = qualify(repo, canary=canary)
    if launch:
        require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo, text=True).strip(), 'clean frozen checkout required')
        if not canary:
            admitted = require_canary(repo, proof)
            # Admission is outside the archive roster; science reuses the exact
            # canary archive commit/bytes even after root commits its admission.
            proof = dict(proof, source_archive_commit=admitted['source_commit'],
                         expected_source_archive_sha256=admitted['source_archive_sha256'])
    return proof


def gate_log(pin, stages):
    # The completed402 qualification extends the old helper's named-test roster.
    with patch.multiple(local, GATES={s['stage']: s['command'] for s in stages},
                        TESTS={s['stage']: tuple(s['required_test_passes']) for s in stages}):
        bridge.completed_gate_log(pin, stages)


def canary(evidence, client, calls, out, check, deadline, repo):
    for name in VERSIONS:
        importlib.import_module(name)
    versions = {n: importlib.metadata.version(n) for n in VERSIONS}
    exact(versions, VERSIONS, 'actual installed SDK and numerical imports')
    model = client.meta.service_model
    require('IfNoneMatch' in model.operation_model('PutObject').input_shape.members, 'SDK conditional PUT model')
    for operation in ('HeadObject', 'GetObject'):
        require({'Bucket', 'Key'} <= set(model.operation_model(operation).input_shape.members), 'SDK asset read model')
    check()
    cli = subprocess.run([sys.executable, str(Path(repo)/'scripts/run_source_witness_paired_coverage.py')],
        capture_output=True, text=True, timeout=min(30, max(.001, deadline-time.monotonic())))
    require(cli.returncode == 2 and 'usage: CONFIG' in cli.stderr and not cli.stdout, 'actual helper CLI usage refusal/exit')
    for pin in evidence['objects']:
        check()
        response = publication.sdk_call(client, calls, 'head_object', pin['key'], ids.BUCKET)
        exact(response['ContentLength'], pin['bytes'], 'asset HEAD declared length')
    log = evidence['gate_log']
    require(log['bytes'] <= 1 << 20, 'canary authenticated GET <=1MiB')
    check(); publication.download(client, calls, ids.BUCKET, log, out/'gate.log')
    probe.fsync_dir(out); check()
    gate_log(dict(runner.body(log), path=str(out/'gate.log')), evidence['stages'])
    return dict(schema=CANARY_SCHEMA, status='GO', complete=True, versions=versions,
        sdk_conditional_put_model=True, cli_exit_status=cli.returncode, cli_stderr=cli.stderr,
        authenticated_log=runner.body(log), sdk_calls=calls,
        native_processes=0, ann_queries=0, truth_or_panel_body_reads=0, dataset_payload_gets=0)


def validate_calls(calls, pins, operation):
    exact(len(calls), len(pins), 'closed SDK call roster')
    for call, pin in zip(calls, pins):
        for name, expected in dict(operation=operation, key=pin['key'], outcome='returned', declared_bytes=pin['bytes']).items():
            exact(call[name], expected, 'original SDK asset call')
        if operation == 'get_object':
            exact(call['verified_bytes'], pin['bytes'], 'authenticated streamed asset bytes')
            exact(call['verified_sha256'], pin['sha256'], 'authenticated streamed asset SHA')
        else:
            require('verified_sha256' not in call, 'HEAD does not authenticate bodies')


def validate_canary(receipt, evidence):
    for name, expected in dict(schema=CANARY_SCHEMA, status='GO', complete=True,
            versions=VERSIONS, sdk_conditional_put_model=True, cli_exit_status=2,
            native_processes=0, ann_queries=0, truth_or_panel_body_reads=0, dataset_payload_gets=0).items():
        exact(receipt[name], expected, 'canary NO-GT/NO-ANN contract')
    require('usage: CONFIG' in receipt['cli_stderr'], 'original helper usage')
    calls = receipt['sdk_calls']
    exact(len(calls), 12, 'HEAD11 plus one small gate-log GET')
    validate_calls(calls[:11], evidence['objects'], 'head_object')
    validate_calls(calls[11:], [evidence['gate_log']], 'get_object')
    exact(receipt['authenticated_log'], runner.body(evidence['gate_log']), 'canary authenticated completed log')
    require(evidence['gate_log']['bytes'] <= 1 << 20, 'canary log cap')


def bundle(source, destination, check):
    check()
    with Path(destination).open('xb') as stream, gzip.GzipFile(fileobj=stream, mode='wb', mtime=0) as zipped, tarfile.open(fileobj=zipped, mode='w|') as archive:
        for path in sorted(Path(source).rglob('*')):
            runner.positive.regular_path(path)
            if path.is_dir():
                continue
            require(path.is_file(), 'only regular native outputs')
            archive.add(path, arcname=str(path.relative_to(source)), recursive=False)
        zipped.flush()
    probe.fsync_file(destination); probe.fsync_dir(Path(destination).parent); check()
    expected = {str(p.relative_to(source)): ids.artifact(p) for p in sorted(Path(source).rglob('*')) if p.is_file()}
    seen = []
    with tarfile.open(destination, 'r|gz') as archived:
        for member in archived:
            check(); require(member.isfile() and member.name in expected and member.name not in seen, 'closed bundle original roster')
            digest, count = hashlib.sha256(), 0
            with archived.extractfile(member) as body:
                while chunk := body.read(65536):
                    digest.update(chunk); count += len(chunk)
            exact(dict(bytes=count, sha256=digest.hexdigest()), expected[member.name], 'closed bundle every original raw byte')
            seen.append(member.name)
    exact(seen, sorted(expected), 'closed lossless native bundle')
    check()
    return ids.artifact(destination)


def drain_owned(group, check):
    """Stop only this fixed runner's sibling units, then observe empty cgroups."""
    group = Path(group); record = dict(owned_units=[], stop_calls=[], closed=False)
    error = None
    try:
        for child in sorted(group.parent.iterdir()):
            if not child.is_dir() or child == group:
                continue
            require(re.fullmatch(r'borsuk-global-leaf-[0-9]+-(?:relaion|cohere)-(?:build-probes|nominate-probes|diagnose-probes)\.service',child.name), 'only original owned native sibling units')
            record['owned_units'].append(child.name)
            if not (child/'cgroup.procs').exists() or not (child/'cgroup.procs').read_text().strip():
                continue
            check()
            command = ['systemctl','stop',child.name]
            call = subprocess.run(command,capture_output=True,text=True,timeout=10)
            record['stop_calls'].append(dict(command=command,original_exit_code=call.returncode,stdout=call.stdout,stderr=call.stderr))
            require(call.returncode == 0,'original native sibling stop succeeded')
        while True:
            check()
            live = [p for p in group.parent.iterdir() if p.is_dir() and p != group and
                    (p/'cgroup.procs').exists() and (p/'cgroup.procs').read_text().strip()]
            if not live:
                record['closed'] = True; break
            time.sleep(.1)
    except BaseException as caught:
        error = caught; record['error'] = type(caught).__name__+': '+str(caught)
    return record, error


def unpack(archive, destination):
    destination = runner.positive.regular_path(destination)
    destination.mkdir(exist_ok=False)
    names, total = [], 0
    with tarfile.open(archive, mode='r|gz') as transport:
        for member in transport:
            publication.relative(member.name)
            require(member.isfile() and member.name not in names, 'regular unique bundle member')
            require(type(member.size) is int and member.size >= 0, 'native bundle member size')
            total += member.size
            require(total <= SCRATCH and len(names) < 10000, 'bounded native bundle expansion')
            target = runner.positive.regular_path(destination/member.name)
            require(target.is_relative_to(destination), 'contained native bundle output')
            target.parent.mkdir(parents=True, exist_ok=True)
            with transport.extractfile(member) as source, target.open('xb') as output:
                shutil.copyfileobj(source, output, 65536); output.flush(); os.fsync(output.fileno())
            exact(target.stat().st_size, member.size, 'lossless native member bytes')
            names.append(member.name)
    exact(names, sorted(names), 'sorted original native-output roster')
    require(names, 'nonempty original runner output')
    return destination


def snapshots(memory):
    before = local.resource_snapshot(dict(memory_max_bytes=CANARY_MEMORY, cpu_affinity=[0]))
    group = Path(before['path'])
    before.update(cpu_max=(group/'cpu.max').read_text().strip(), tasks_max=(group/'pids.max').read_text().strip())
    quota, period = map(int, before['cpu_max'].split()); exact(quota, period, 'outer controller CPU1')
    exact(before['tasks_max'], '512', 'outer controller task cap')
    require(re.fullmatch(r'borsuk-global-leaf-(?:canary-)?a[0-9]{4}\.slice', group.parent.name), 'owned original aggregate slice')
    exact(group.parent.name, os.environ['BORSUK_GLOBAL_LEAF_SLICE'], 'original owned slice')
    return before, probe.cgroup_snapshot(group.parent, memory, 100)


def invoke(config_pin, repo, output, native_out, deadline):
    command = [sys.executable, str(Path(repo)/'scripts/run_source_witness_paired_coverage.py'),
               config_pin['path'], config_pin['sha256'], str(repo), str(native_out)]
    process, failure = None, None
    with (output/'runner.stdout').open('xb', buffering=0) as stdout, (output/'runner.stderr').open('xb', buffering=0) as stderr:
        try:
            process = subprocess.Popen(command, stdout=stdout, stderr=stderr)
            process.wait(timeout=max(.001, deadline-time.monotonic()-15))
        except BaseException as error:
            failure = error
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=5)
            os.fsync(stdout.fileno()); os.fsync(stderr.fileno())
    record = dict(command=command, original_exit_code=process.returncode if process else None,
        pid=process.pid if process else None, waited=process is not None and process.poll() is not None,
        stdout=ids.artifact(output/'runner.stdout'), stderr=ids.artifact(output/'runner.stderr'),
        interruption=None if failure is None else type(failure).__name__+': '+str(failure))
    local.write_json(output/'runner-return.json', record)
    if failure is not None:
        raise failure
    return record


def stage(repo, output, worker_root, *, is_canary=False):
    repo, out, root = (runner.positive.regular_path(p) for p in (repo, output, worker_root))
    exact(root, WORKER_ROOT, 'fixed worker root')
    exact(repo, root/'probe-repo', 'owned authenticated source root')
    exact(out, root/'screen', 'fixed fresh bridge output')
    require(not out.exists(), 'new attempt only')
    config, proof, evidence = qualify(repo, canary=is_canary)
    for field, env in (('config_sha256', 'BORSUK_HIERARCHICAL_CONFIG_SHA256'),
                       ('source_archive_paths_sha256', 'BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256')):
        exact(proof[field], os.environ[env], 'bootstrap frozen identity')
    remaining = int(os.environ['BORSUK_HIERARCHICAL_DEADLINE_EPOCH'])-time.time()
    wall, memory, cap = (CANARY_WALL, CANARY_MEMORY, CANARY_SCRATCH) if is_canary else (WALL, MEMORY, SCRATCH)
    require(CLOSEOUT_RESERVE < remaining <= wall, 'cumulative worker deadline includes fixed closeout reserve')
    baseline = int(os.environ['BORSUK_HIERARCHICAL_SCRATCH_BASE_USED'])
    deadline = time.monotonic()+remaining
    work_deadline = deadline-CLOSEOUT_RESERVE
    before, host_before = snapshots(memory)
    out.mkdir(); assets, native_out = root/'assets', root/'native-output'
    require(not assets.exists() and not native_out.exists(), 'fresh staged/native paths')
    calls, errors, peaks, stopped = [], [], dict(scratch_bytes=0), threading.Event()
    client, failure, thread, sdk_closed = None, None, None, False
    result = dict(status='INVALID', complete=False, execution_exit_code=2)
    def check():
        bridge.probe_resource_check(root, baseline, cap, work_deadline, errors, peaks)
    def interrupted(*_):
        raise InterruptedError('fixed bridge deadline/resource interruption')
    def monitor():
        while not stopped.wait(1):
            try:
                check()
            except Exception as error:
                errors.append(str(error)); os.kill(os.getpid(), signal.SIGALRM); return
    old_alarm, old_term = signal.getsignal(signal.SIGALRM), signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGALRM, interrupted); signal.signal(signal.SIGTERM, interrupted)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, remaining-CLOSEOUT_RESERVE)
    try:
        check(); thread = threading.Thread(target=monitor, daemon=True); thread.start()
        ids.write(out/'config.json', (repo/CONFIG).read_bytes())
        local.write_json(out/'source-qualification.json', proof)
        versions = {n: importlib.metadata.version(n) for n in VERSIONS}
        exact(versions, VERSIONS, 'actual installed worker dependencies')
        local.write_json(out/'tool-versions.json', dict(versions, python=sys.version, executable=sys.executable))
        check(); client = publication.sdk_client(ids.REGION)
        if is_canary:
            result = canary(evidence, client, calls, out, check, work_deadline, repo)
            validate_canary(result, evidence)
            local.write_json(out/'canary.json', result)
        else:
            for pin in evidence['objects']:
                path = Path(pin['destination']); path.parent.mkdir(parents=True, exist_ok=True)
                check(); publication.download(client, calls, ids.BUCKET, {k: pin[k] for k in ('key', 'bytes', 'sha256')}, path)
                probe.fsync_dir(path.parent); check()
            validate_calls(calls, evidence['objects'], 'get_object')
            config_pin = local.write_json(out/'runner-config.json', config['runner']); check()
            record = invoke(config_pin, repo, out, native_out, work_deadline)
            check()
            native_terminal = runner.replay(config_pin['path'], config_pin['sha256'], repo, native_out)
            exact(record['original_exit_code'], native_terminal['execution_exit_code'], 'original runner terminal/exit unchanged')
            require(native_terminal['complete'] and native_terminal['execution_exit_code'] == 0, 'native runner INVALID execution')
            result = dict(status=native_terminal['status'], complete=True, execution_exit_code=0,
                          original_native_terminal=ids.artifact(native_out/'terminal.json'))
    except BaseException as error:
        failure = error
        result = dict(status='INVALID', complete=False, execution_exit_code=2, error=type(error).__name__+': '+str(error))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        stopped.set()
        if thread is not None:
            thread.join(timeout=2)
        closeout_deadline = min(deadline, time.monotonic()+CLOSEOUT_SECONDS)
        def closeout_check():
            # Active-work errors stay INVALID, but cannot prohibit their receipts.
            require(time.monotonic() < closeout_deadline, 'fixed closeout deadline')
            amount = bridge.scratch_snapshot(root, baseline)
            peaks['scratch_bytes'] = max(peaks['scratch_bytes'], amount)
            peaks['scratch_scan_calls'] = peaks.get('scratch_scan_calls',0)+1
            require(amount <= cap, 'closeout whole-worker scratch cap')
        closed_bundle = None
        drain = dict(owned_units=[],stop_calls=[],closed=False)
        try:
            signal.setitimer(signal.ITIMER_REAL, max(.001, closeout_deadline-time.monotonic()))
            drain, drain_error = drain_owned(before['path'], closeout_check)
            local.write_json(out/'native-drain.json',drain); closeout_check()
            if drain_error is not None:
                raise drain_error
            if drain['stop_calls']:
                failure = failure or ValueError('runner left live owned native units; stopped during closeout')
            if assets.exists():
                shutil.rmtree(assets)
            if not is_canary and native_out.exists():
                closed_bundle = bundle(native_out, out/'native-output.tar.gz', closeout_check)
                shutil.rmtree(native_out)  # Only a closed, byte-authenticated bundle permits deletion.
            if client is not None:
                client.close(); sdk_closed = True
            after, host_after = snapshots(memory)
            probe.no_oom(before, after); probe.no_oom(host_before, host_after)
            local.write_json(out/'worker-cgroup.json', dict(before=before, after=after, host_before=host_before, host_after=host_after, closed=True, native_units_drained=drain['closed']))
            closeout_check()
        except BaseException as error:
            failure = failure or error
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
        clean = dict(assets_removed=not assets.exists(), native_output_bundled_and_removed=is_canary or (closed_bundle is not None and not native_out.exists()),
            closed_native_bundle=closed_bundle, closeout_limit_seconds=CLOSEOUT_SECONDS,
            sdk_client_closed=sdk_closed, monitor_stopped=thread is None or not thread.is_alive(),
            native_processes_concurrent_max=0 if is_canary else 1)
        if failure is not None or not all(clean[n] for n in ('assets_removed', 'native_output_bundled_and_removed', 'sdk_client_closed', 'monitor_stopped')):
            result.update(status='INVALID', complete=False, execution_exit_code=2, error=str(failure or 'incomplete original cleanup'))
        local.write_json(out/'resources.json', dict(peaks, sdk_calls=calls, monitor_errors=errors,
            deadline_seconds=remaining, closeout_reserve_seconds=CLOSEOUT_RESERVE, closeout_limit_seconds=CLOSEOUT_SECONDS,
            wall_seconds=remaining-(deadline-time.monotonic())))
        local.write_json(out/'cleanup.json', clean)
        local.write_json(out/'summary.json', result); probe.fsync_dir(out)
        signal.signal(signal.SIGALRM, old_alarm); signal.signal(signal.SIGTERM, old_term)
        if previous_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)
    if not result['complete']:
        raise ValueError('INVALID fixed bridge: '+result.get('error', 'incomplete'))
    return result


def user_data(commit, archive_sha, archive_key, prefix, proof, *, is_canary=False):
    if not is_canary and 'source_archive_commit' in proof:
        exact(commit,proof['source_archive_commit'],'same admitted canary source archive commit')
        exact(archive_sha,proof['expected_source_archive_sha256'],'same admitted canary source archive bytes')
    module = sys.modules[bridge.__name__]
    with patch.multiple(module, PROBE_CONFIG=CONFIG, PROBE_PREFIX=PREFIX, PROBE_CANARY_PREFIX=CANARY_PREFIX,
            PROBE_SCHEMA=SCHEMA, PROBE_CANARY_SCHEMA=CANARY_SCHEMA,
            PROBE_ARTIFACTS=ARTIFACTS, PROBE_CANARY_ARTIFACTS=CANARY_ARTIFACTS,
            WALL=WALL, CANARY_WALL=CANARY_WALL, SCRATCH=SCRATCH, CANARY_SCRATCH=CANARY_SCRATCH):
        body = bridge.probe_user_data(commit, archive_sha, archive_key, prefix, proof, canary=is_canary)
    body = body.replace('/mnt/hierarchical-global-leaf-probe-canary', str(WORKER_ROOT)).replace('/mnt/hierarchical-global-leaf-probe', str(WORKER_ROOT))
    flag = '--stage-canary' if is_canary else '--stage'
    needle = '-m '+bridge.MODULE+' --global-leaf-probe '+flag
    require(body.count(needle) == 1, 'reviewed bootstrap worker hook')
    body = body.replace(needle, '"$root/probe-repo/'+FILE+'" '+flag)
    if not is_canary:
        body = body.replace('MemoryMax=2G', 'MemoryMax=1G', 1)
        body = body.replace(' -p MemoryMax=2G', ' -p MemoryMax=256M', 1)
        body = body.replace('CPUQuota=200%', 'CPUQuota=100%').replace('taskset -c 0,1', 'taskset -c 0')
        body = body.replace('NUM_THREADS=2', 'NUM_THREADS=1')
    # Empty original native stdout/stderr are valid evidence, never fabrication.
    body = body.replace('if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi',
        'case "$name" in run-closed.log) test -s run.log;; screen/runner.stdout|screen/runner.stderr) test -f "$name";; *) test -s "$name";; esac')
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0], '<fixed-terminal>', 'exec')
    require(len(body.encode()) < 16384 and all(s not in body for s in ('rustup', 'cargo', 'unused', 'NUM_THREADS=2', 'CPUQuota=200%')), 'fixed bootstrap <16KiB/no compiler/CPU1')
    return body


def poll(*args, is_canary=False):
    shared, _ = ids.lifecycle()
    with patch.object(shared, 'WALL', CANARY_WALL if is_canary else WALL):
        return shared.poll(*args)


def replay(out, *, is_canary=False, repo=REPO):
    out = Path(out)
    reservation, launch, close, terminal = (local.decode((out/name).read_bytes()) for name in
        ('aws-reservation.json', 'aws-launch.json', 'aws-closeout.json', 'aws-terminal.json'))
    exact(close['state'], 'terminated', 'collected original instance terminated')
    exact(close['nodes'], launch['nodes'], 'same original instance IDs')
    require(terminal['instance_id'] == launch['instance_id'] in {p['instance_id'] for p in close['nodes'].values()}, 'owned original host')
    schema, artifacts = (CANARY_SCHEMA, CANARY_ARTIFACTS) if is_canary else (SCHEMA, ARTIFACTS)
    exact(terminal['schema'], schema, 'original terminal schema'); exact(reservation['schema'], schema, 'reservation schema')
    for field in ('source_commit', 'source_archive_sha256'):
        exact(terminal[field], launch[field], 'original source/archive ancestry')
        exact(terminal[field], reservation[field], 'original reservation ancestry')
    for field, expected in dict(wall_seconds=CANARY_WALL if is_canary else WALL,
            compute_cap_usd=.02 if is_canary else .10, ebs_s3_allowance_usd=.15,
            instance_type='c7i.large', image_id=ids.IMAGE_ID, root_device_name='/dev/sda1', subnet_id=ids.SUBNET,
            spot_max_usd_per_hour=.15).items():
        exact(reservation[field], expected, 'fixed admission/cost bound')
    require(0 < float(reservation['spot_price_observed_usd_per_hour']) <= .15, 'original bounded Spot quote')
    proof = reservation['qualification']
    for name in IDENTITIES:
        exact(terminal[name], proof[name], 'original frozen terminal authority')
    require(set(terminal['artifacts']) <= set(artifacts), 'original partial transport roster')
    for name, pin in terminal['artifacts'].items():
        exact(ids.artifact(out/name), pin, 'every original collected raw pin')
    success = terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    if not success:
        require(terminal['status'] == 'failed' and terminal['exit_code'] != 0, 'closed failed terminal INVALID')
        return dict(executed=False, status='INVALID')
    exact(terminal['original_exit_code'], 0, 'original worker process exit')
    exact(set(terminal['artifacts']), set(artifacts), 'complete original transport roster')
    config, expected, evidence = qualify(repo, canary=is_canary)
    deployed = dict(proof)
    if not is_canary:
        exact(deployed.pop('source_archive_commit'),launch['source_commit'],'exact admitted source archive commit')
        exact(deployed.pop('expected_source_archive_sha256'),launch['source_archive_sha256'],'exact admitted source archive bytes')
    exact(deployed, expected, 'unchanged frozen code/native/config/source roster')
    screen = out/'screen'
    exact(local.decode((screen/'config.json').read_bytes()), config, 'collected exact frozen config')
    exact(local.identity(screen/'config.json')['sha256'], proof['config_sha256'], 'collected frozen config bytes')
    exact(local.decode((screen/'source-qualification.json').read_bytes()), deployed, 'original deployed proof')
    counters = local.decode((screen/'worker-cgroup.json').read_bytes())
    exact(counters['closed'], True, 'original outer resources closed')
    exact(counters['native_units_drained'], True, 'original native-unit drain')
    drain = local.decode((screen/'native-drain.json').read_bytes())
    exact(drain['closed'],True,'original explicit native sibling drain')
    exact(drain['stop_calls'],[],'complete runner drained its own units normally')
    for name in ('host_before', 'host_after'):
        probe.validate_cgroup(counters[name], CANARY_MEMORY if is_canary else MEMORY, 100)
    for name in ('before', 'after'):
        value = counters[name]
        require(0 < int(value['memory.max']) <= CANARY_MEMORY and int(value['memory.peak']) <= CANARY_MEMORY, 'original outer controller memory')
        exact(value['memory.swap.max'], '0', 'outer noSwap'); exact(int(value['memory.swap.peak']), 0, 'outer swap peak')
        exact(value['cpu_affinity'], [0], 'original outer CPU1 affinity')
        quota, period = map(int, value['cpu_max'].split()); exact(quota, period, 'original outer CPU1 quota')
        exact(value['tasks_max'], '512', 'original outer task cap')
    probe.no_oom(counters['before'], counters['after']); probe.no_oom(counters['host_before'], counters['host_after'])
    resources = local.decode((screen/'resources.json').read_bytes())
    cap, wall = (CANARY_SCRATCH, CANARY_WALL) if is_canary else (SCRATCH, WALL)
    require(0 < resources['wall_seconds'] <= resources['deadline_seconds'] <= wall and
        resources['scratch_bytes'] <= cap and not resources['monitor_errors'] and resources['scratch_scan_calls'] >= 2, 'actual whole-worker resource/deadline closure')
    exact(resources['closeout_reserve_seconds'],CLOSEOUT_RESERVE,'original machine-deadline closeout reserve')
    exact(resources['closeout_limit_seconds'],CLOSEOUT_SECONDS,'original bounded closeout allowance')
    clean = local.decode((screen/'cleanup.json').read_bytes())
    exact(clean['closeout_limit_seconds'],CLOSEOUT_SECONDS,'original cleanup allowance')
    exact(clean['closed_native_bundle'],None if is_canary else ids.artifact(screen/'native-output.tar.gz'),'closed authenticated original native bundle')
    for name in ('assets_removed', 'native_output_bundled_and_removed', 'sdk_client_closed', 'monitor_stopped'):
        exact(clean[name], True, 'original bridge cleanup')
    exact(clean['native_processes_concurrent_max'], 0 if is_canary else 1, 'serial native runner ownership')
    versions = local.decode((screen/'tool-versions.json').read_bytes())
    exact({n: versions[n] for n in VERSIONS}, VERSIONS, 'original actual dependency versions')
    summary = local.decode((screen/'summary.json').read_bytes())
    if is_canary:
        receipt = local.decode((screen/'canary.json').read_bytes())
        validate_canary(receipt, evidence); exact(receipt['sdk_calls'], resources['sdk_calls'], 'single original SDK ledger')
        exact(summary, receipt, 'original closed canary summary')
        gate_log(dict(runner.body(evidence['gate_log']), path=str(screen/'gate.log')), evidence['stages'])
    else:
        validate_calls(resources['sdk_calls'], evidence['objects'], 'get_object')
        cfg = local.identity(screen/'runner-config.json')
        exact(local.read_json(cfg), config['runner'], 'original exact native runner config')
        record = local.decode((screen/'runner-return.json').read_bytes())
        exact(record['original_exit_code'], 0, 'original native runner exit'); exact(record['waited'], True, 'original runner wait')
        for name in ('stdout', 'stderr'):
            exact(record[name], ids.artifact(screen/('runner.'+name)), 'unmodified possibly-silent runner raw log')
        expected_command = [versions['executable'], str(WORKER_ROOT/'probe-repo/scripts/run_source_witness_paired_coverage.py'),
            str(WORKER_ROOT/'screen/runner-config.json'), cfg['sha256'], str(WORKER_ROOT/'probe-repo'), str(WORKER_ROOT/'native-output')]
        exact(record['command'], expected_command, 'exact ONE original native runner invocation')
        with tempfile.TemporaryDirectory(prefix='source-witness-collected-') as tmp:
            native = unpack(screen/'native-output.tar.gz', Path(tmp)/'native')
            exact(ids.artifact(native/'terminal.json'), summary['original_native_terminal'], 'original runner terminal bytes')
            original = runner.replay(cfg['path'], cfg['sha256'], repo, native)
            exact(original['execution_exit_code'], 0, 'original replayed execution exit0')
            exact(original['complete'], True, 'complete original native execution')
            exact(summary, dict(status=original['status'], complete=True, execution_exit_code=0,
                original_native_terminal=ids.artifact(native/'terminal.json')), 'scientific FAIL preserved as execution0')
    return dict(executed=True, status=summary['status'])


def collect(s3, prefix, out, instance_id, commit, digest, *, is_canary=False):
    launch, close = (local.decode((Path(out)/n).read_bytes()) for n in ('aws-launch.json', 'aws-closeout.json'))
    exact(close['nodes'], launch['nodes'], 'collection original same owned IDs'); exact(close['state'], 'terminated', 'collection after original termination')
    exact(launch['instance_id'], instance_id, 'original collection instance'); exact(launch['prefix'], prefix, 'original collection prefix')
    with patch.multiple(ids, SCHEMA=CANARY_SCHEMA if is_canary else SCHEMA,
            ARTIFACTS=CANARY_ARTIFACTS if is_canary else ARTIFACTS,
            replay=lambda out: replay(out, is_canary=is_canary)):
        return ids.collect(s3, prefix, out, instance_id, commit, digest)


def require_canary(repo, proof):
    pointer = local.read_json(local.identity(Path(repo)/ROOT/'canary-admission.json'))
    fields(pointer, 'schema attempt terminal_sha256 '+' '.join(BINDINGS), 'root canary admission')
    exact(pointer['schema'], ADMISSION_SCHEMA, 'root admitted canary schema')
    require(re.fullmatch(r'a[0-9]{4}', pointer['attempt']), 'admitted canary attempt')
    out = Path(repo)/ROOT/'canary'/pointer['attempt']
    exact(local.identity(out/'aws-terminal.json')['sha256'], pointer['terminal_sha256'], 'root collected original canary terminal')
    terminal = local.decode((out/'aws-terminal.json').read_bytes())
    for name in BINDINGS:
        exact(pointer[name], proof[name], 'root admission unchanged frozen identities')
        exact(terminal[name], proof[name], 'canary/science same frozen identities')
    require(replay(out, is_canary=True, repo=repo)['executed'], 'terminated collected GO canary required')
    return terminal


def campaign(*, is_canary=False):
    return SimpleNamespace(ROOT=ROOT, NAME='canary' if is_canary else '',
        SCHEMA=CANARY_SCHEMA if is_canary else SCHEMA, PREFIX=CANARY_PREFIX if is_canary else PREFIX,
        TOKEN_PREFIX='source-witness-canary-' if is_canary else 'source-witness-',
        TAG='borsuk-source-witness-canary' if is_canary else 'borsuk-source-witness',
        WALL=CANARY_WALL if is_canary else WALL, COMPUTE_CAP=.02 if is_canary else .10,
        INSTANCE_TYPE='c7i.large', IMAGE_ID=ids.IMAGE_ID, ROOT_DEVICE_NAME='/dev/sda1', SUBNET=ids.SUBNET,
        SPOT_MAX_USD_PER_HOUR=.15, ARTIFACTS=CANARY_ARTIFACTS if is_canary else ARTIFACTS,
        preflight=lambda: preflight(canary=is_canary, launch=True),
        user_data=lambda *a: user_data(*a, is_canary=is_canary), poll=lambda *a: poll(*a, is_canary=is_canary),
        collect=lambda *a: collect(*a, is_canary=is_canary))


def main(args):
    try:
        if args == ['--self-check']:
            self_check(); return 0
        if args == ['--preflight']:
            print(json.dumps(preflight(), sort_keys=True)); return 0
        if len(args) == 4 and args[0] in ('--stage', '--stage-canary'):
            stage(*args[1:], is_canary=args[0] == '--stage-canary'); return 0
        if len(args) == 2 and args[0] in ('--replay', '--replay-canary'):
            result = replay(args[1], is_canary=args[0] == '--replay-canary')
            print(json.dumps(result, sort_keys=True)); return 0 if result['executed'] else 2
        is_canary = bool(args and args[0] == '--canary')
        if is_canary:
            args = args[1:]
        require(len(args) == 1 and re.fullmatch(r'a[0-9]{4}', args[0]), 'CLI: --preflight | --self-check | --canary aNNNN | aNNNN')
        os.chdir(REPO)
        shared, _ = ids.lifecycle()
        shared.main(args[0], campaign=campaign(is_canary=is_canary)); return 0
    except Exception as error:
        print('INVALID: '+str(error), file=sys.stderr); return 2


def self_check():
    """Source-bound tiny fixtures; native algorithms, cloud and cgroups mocked."""
    from contextlib import ExitStack
    from botocore.session import Session
    from unittest.mock import Mock
    module = sys.modules[__name__]
    rejected = []
    def put_json(path, value):
        Path(path).unlink(missing_ok=True)
        return local.write_json(path, value)
    def reject(name, call):
        try:
            call()
        except (ValueError, OSError, KeyError, subprocess.TimeoutExpired, ImportError, tarfile.TarError, EOFError):
            rejected.append(name)
        else:
            raise AssertionError('accepted negative: '+name)
    with tempfile.TemporaryDirectory(prefix='source-witness-bridge-') as tmp:
        base = Path(tmp); repo = base/'source'; repo.mkdir()
        # Real402 source and the actual imported script/authority closure, no assets.
        sources = probe.ref(REPO, runner.WORKSPACE)['source_sha256']
        authority = probe.ref(REPO, runner.AUTHORITY)
        refs = dict(REFS, authority=runner.AUTHORITY, workspace=runner.WORKSPACE,
            qualification=authority['native_qualification'], method=authority['method'], historical_terminal=authority['terminal'])
        names = sorted({*sources, *code_roster(REPO), *(p['path'] for p in refs.values())})
        for name in names:
            target = repo/name; target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(REPO/name, target)
        config = local.decode(subprocess.check_output(['git','-c','safe.directory='+str(REPO),'show','782a7a3:'+str(ROOT/'campaign-config.draft.json')],cwd=REPO))
        exact(config['authority_pending'],True,'actual root pending draft refuses authority')
        config['authority_pending']=False
        config['code_sha256']={n: local.identity(repo/n)['sha256'] for n in code_roster(repo)}
        put_json(repo/CONFIG, config)
        config, proof, evidence = qualify(repo)
        exact(proof['source_file_count'], 402, 'real source402 self-check')
        exact(len(evidence['objects']), 11, 'real qualified object roster')
        require(set(sources) <= set(proof['source_archive_paths']), 'full native self-check closure')
        require(not any(n.endswith(('.gz', '.bin')) for n in proof['source_archive_paths']), 'no historical payload source archive')
        for is_canary in (False, True):
            _, q, _ = qualify(repo, canary=is_canary)
            body = user_data('0'*40, '1'*64, 'research/sources/mock',
                (CANARY_PREFIX if is_canary else PREFIX)+'a0001', q, is_canary=is_canary)
            require('boto3==1.40.72 botocore==1.40.72' in body and
                'X-aws-ec2-metadata-token' in body and 'sync -f terminal.json' in body and
                'screen/runner.stdout|screen/runner.stderr' in body and
                'CPUQuota=100%' in body and 'taskset -c 0 ' in body, 'reviewed fixed bootstrap')
        broken = copy.deepcopy(config); broken['authority_pending'] = True
        put_json(repo/CONFIG, broken); reject('pending root freeze', lambda: qualify(repo))
        broken = copy.deepcopy(config); broken['code_sha256'][FILE] = 'f'*64
        put_json(repo/CONFIG, broken); reject('code drift', lambda: qualify(repo))
        put_json(repo/CONFIG, config)
        altered = repo/runner.SOURCE_PATHS['module']; original = altered.read_bytes()
        altered.write_bytes(original+b'\n'); reject('native source drift', lambda: qualify(repo)); altered.write_bytes(original)
        source = base/'original'; source.mkdir(); (source/'silent.log').write_bytes(b''); (source/'terminal.json').write_bytes(b'original terminal\n')
        bundle(source, base/'native-output.tar.gz', lambda: None)
        restored = unpack(base/'native-output.tar.gz', base/'restored')
        exact((restored/'silent.log').read_bytes(), b'', 'silent raw log lossless')
        exact((restored/'terminal.json').read_bytes(), b'original terminal\n', 'original terminal lossless')
        for name in ('../escape', '/absolute', 'duplicate'):
            path = base/(name.rsplit('/',1)[-1]+'.tar.gz')
            with tarfile.open(path, 'w:gz') as tar:
                member = tarfile.TarInfo(name); tar.addfile(member)
                if name == 'duplicate':
                    tar.addfile(member)
            reject('unsafe bundle '+name, lambda p=path: unpack(p, base/(p.stem+'-out')))
        # Tiny source-bound service fixture, real botocore service model and
        # the authenticated completed native gate log; never a corpus body.
        raw_log = (REPO/ROOT/'implementation-gates/a0001/test.log').read_bytes()
        bodies = {p['key']: bytes([i+1])*19 for i,p in enumerate(evidence['objects'])}
        small = copy.deepcopy(evidence)
        for p in small['objects']:
            b = bodies[p['key']]; p.update(bytes=len(b), sha256=ids.sha(b))
        bodies[small['gate_log']['key']] = raw_log
        model = Session().get_service_model('s3')
        class Client:
            meta = SimpleNamespace(service_model=model)
            def head_object(self, *, Bucket, Key):
                return dict(ContentLength=len(bodies[Key]))
            def get_object(self, *, Bucket, Key):
                import io
                return dict(ContentLength=len(bodies[Key]), Body=io.BytesIO(bodies[Key]))
            def close(self):
                pass
        numerical_import = importlib.import_module
        version = importlib.metadata.version
        def imported(name):
            return SimpleNamespace() if name in ('numpy', 'pyarrow') else numerical_import(name)
        def installed(name):
            return VERSIONS[name] if name in ('numpy', 'pyarrow') else version(name)
        smoke = base/'canary'; smoke.mkdir(); ledger = []
        with patch.object(importlib, 'import_module', side_effect=imported), patch.object(importlib.metadata, 'version', side_effect=installed):
            receipt = canary(small, Client(), ledger, smoke, lambda: None, time.monotonic()+30, repo)
        validate_canary(receipt, small)
        exact(len(ledger), 12, 'real SDK helper canary HEAD11/GET1')
        for name in ('native_processes', 'ann_queries', 'truth_or_panel_body_reads', 'dataset_payload_gets'):
            bad = copy.deepcopy(receipt); bad[name] = 1
            reject('canary NO-GT '+name, lambda b=bad: validate_canary(b, small))
        bad = copy.deepcopy(receipt); bad['sdk_calls'][-1]['key'] = small['objects'][0]['key']
        reject('canary full-input GET', lambda: validate_canary(bad, small))
        for kind in ('missing', 'wrong-length', 'wrong-body', 'import', 'sdk-model', 'cli-exit', 'cli-stdout'):
            destination = base/('bad-canary-'+kind); destination.mkdir()
            c = Client()
            if kind == 'missing':
                c.head_object = Mock(side_effect=KeyError('missing asset'))
            if kind == 'wrong-length':
                c.head_object = Mock(return_value=dict(ContentLength=1))
            if kind == 'wrong-body':
                old = bodies[small['gate_log']['key']]; bodies[small['gate_log']['key']] = b'x'*len(old)
            if kind == 'sdk-model':
                c.meta = SimpleNamespace(service_model=SimpleNamespace(operation_model=lambda _: SimpleNamespace(input_shape=SimpleNamespace(members={}))))
            with ExitStack() as stack:
                stack.enter_context(patch.object(importlib, 'import_module', side_effect=ImportError('SDK import') if kind == 'import' else imported))
                stack.enter_context(patch.object(importlib.metadata, 'version', side_effect=installed))
                if kind.startswith('cli-'):
                    stack.enter_context(patch.object(subprocess, 'run', return_value=SimpleNamespace(returncode=0 if kind=='cli-exit' else 2, stderr='usage: CONFIG', stdout='unexpected' if kind=='cli-stdout' else '')))
                reject(kind, lambda: canary(small, c, [], destination, lambda: None, time.monotonic()+30, repo))
            if kind == 'wrong-body':
                bodies[small['gate_log']['key']] = old
        # Actual single Python subprocess, silent logs and explicit failing exit;
        # it stands in for the already separately qualified native runner.
        native_body = b'{"status":"FAIL","complete":true,"execution_exit_code":0}\n'
        script = '''import sys\nfrom pathlib import Path\nif len(sys.argv)==1:\n print("usage: CONFIG CONFIG_SHA256 REPO NEW_OUTPUT",file=sys.stderr);sys.exit(2)\nout=Path(sys.argv[4]);out.mkdir();(out/"terminal.json").write_bytes(%r);(out/"silent.log").write_bytes(b"")\nsys.exit(%s)\n'''
        roots = {}
        def fixture(is_canary=False, *, fail_exit=False, resource_failure=False, drain_failure=False, cleanup_failure=False, expired_work=False, timed_native=False, bundle_failure=False):
            root = base/('worker-'+str(len(roots))); root.mkdir(); roots[str(root)] = True
            work = root/'probe-repo'; work.mkdir()
            (work/'scripts').mkdir()
            stub = script % (native_body, 2 if fail_exit else 0)
            if timed_native:
                stub = stub.replace('sys.exit(0)','import time;time.sleep(2);sys.exit(0)')
            (work/'scripts/run_source_witness_paired_coverage.py').write_text(stub)
            cfg = copy.deepcopy(config)
            e = copy.deepcopy(small)
            for i,p in enumerate(e['objects']):
                p['destination'] = str(root/'assets'/str(i))
            cfg_path = work/CONFIG; cfg_path.parent.mkdir(parents=True)
            put_json(cfg_path, cfg)
            q = dict(proof, config_sha256=local.identity(cfg_path)['sha256'], campaign_schema=CANARY_SCHEMA if is_canary else SCHEMA,
                artifact_roster_sha256=ids.sha(ids.encoded(CANARY_ARTIFACTS if is_canary else ARTIFACTS)))
            group = root/'cgroup/main'; group.mkdir(parents=True)
            if drain_failure or timed_native:
                child = group.parent/'borsuk-global-leaf-123-relaion-build-probes.service'; child.mkdir(); (child/'cgroup.procs').write_text('123')
            counters = dict(path=str(group), cpu_affinity=[0], cpu_max='100000 100000', tasks_max='512',
                **{'memory.max':str(CANARY_MEMORY), 'memory.peak':'1048576', 'memory.swap.max':'0', 'memory.swap.peak':'0', 'memory.events':'oom 0\noom_kill 0\noom_group_kill 0'})
            host = dict(counters, path=str(group.parent), **{'cpu.max':'100000 100000', 'pids.max':'512'})
            client = Client()
            if cleanup_failure:
                client.close = Mock(side_effect=ValueError('client cleanup failed'))
            def original_replay(*args):
                output = Path(args[-1])
                exact({p.name for p in output.iterdir()}, {'terminal.json','silent.log'}, 'all original runner outputs')
                exact((output/'terminal.json').read_bytes(), native_body, 'original native terminal raw body')
                exact((output/'silent.log').read_bytes(), b'', 'original silent raw log')
                return dict(status='INVALID' if fail_exit else 'FAIL', complete=not fail_exit, execution_exit_code=2 if fail_exit else 0)
            count = 0
            def observed(_):
                nonlocal count
                count += 1
                if resource_failure and count > 1:
                    raise ValueError('resource closure failed')
                return copy.deepcopy(counters), copy.deepcopy(host)
            with ExitStack() as stack:
                stack.enter_context(patch.object(module, 'WORKER_ROOT', root))
                stack.enter_context(patch.object(module, 'qualify', return_value=(cfg, q, e)))
                stack.enter_context(patch.object(module, 'snapshots', side_effect=observed))
                stack.enter_context(patch.object(publication, 'sdk_client', return_value=client))
                replay_mock = stack.enter_context(patch.object(runner, 'replay', side_effect=original_replay))
                actual_run = subprocess.run
                def stop_original(command,**kwargs):
                    if command[:2] == ['systemctl','stop']:
                        (group.parent/command[2]/'cgroup.procs').write_text('')
                        return SimpleNamespace(returncode=0,stdout='',stderr='')
                    return actual_run(command,**kwargs)
                stack.enter_context(patch.object(subprocess,'run',side_effect=stop_original))
                stack.enter_context(patch.object(importlib, 'import_module', side_effect=imported))
                stack.enter_context(patch.object(importlib.metadata, 'version', side_effect=installed))
                stack.enter_context(patch.dict(os.environ, dict(BORSUK_HIERARCHICAL_CONFIG_SHA256=q['config_sha256'],
                    BORSUK_HIERARCHICAL_SOURCE_ARCHIVE_PATHS_SHA256=q['source_archive_paths_sha256'],
                    BORSUK_HIERARCHICAL_DEADLINE_EPOCH=str(int(time.time())+110),
                    BORSUK_HIERARCHICAL_SCRATCH_BASE_USED=str(shutil.disk_usage(root).used))))
                if bundle_failure:
                    stack.enter_context(patch.object(module,'bundle',side_effect=ValueError('bundle authentication failed')))
                if timed_native:
                    original_invoke = invoke
                    stack.enter_context(patch.object(module,'invoke',side_effect=lambda a,b,c,d,_: original_invoke(a,b,c,d,time.monotonic()+15.1)))
                if expired_work:
                    resource_check = bridge.probe_resource_check
                    def expired(*a,**k):
                        if (root/'native-output').exists():
                            raise ValueError('active work deadline/errors expired')
                        return resource_check(*a,**k)
                    stack.enter_context(patch.object(bridge,'probe_resource_check',side_effect=expired))
                run = lambda: stage(work, root/'screen', root, is_canary=is_canary)
                if bundle_failure or timed_native or expired_work or fail_exit or resource_failure or drain_failure or cleanup_failure:
                    reject('stage closure '+str((fail_exit,resource_failure,drain_failure,cleanup_failure)), run)
                    exact(local.decode((root/'screen/summary.json').read_bytes())['status'], 'INVALID', 'failed outer closure invalid')
                    if bundle_failure:
                        require((root/'native-output/terminal.json').exists() and (root/'native-output/silent.log').exists(),'failed bundle preserves every original native file')
                        require(not local.decode((root/'screen/cleanup.json').read_bytes())['native_output_bundled_and_removed'],'failed bundle never claims cleanup')
                        return
                    require((root/'screen/native-output.tar.gz').exists(), 'failed original runner retained')
                    with tempfile.TemporaryDirectory(prefix='bridge-failure-collected-') as retained:
                        original = unpack(root/'screen/native-output.tar.gz',Path(retained)/'native')
                        exact((original/'terminal.json').read_bytes(),native_body,'expired/error partial native terminal collected')
                        exact((original/'silent.log').read_bytes(),b'','expired/error partial native raw log collected')
                    require(not (root/'native-output').exists(),'delete original only after closed lossless bundle')
                    if drain_failure or timed_native:
                        drained = local.decode((root/'screen/native-drain.json').read_bytes())
                        require(drained['closed'] and len(drained['stop_calls'])==1 and not (child/'cgroup.procs').read_text().strip(),'original live sibling explicitly stopped and drained')
                    if timed_native:
                        timed = local.decode((root/'screen/runner-return.json').read_bytes())
                        require(timed['waited'] and timed['original_exit_code'] != 0 and 'TimeoutExpired' in timed['interruption'],'timed-out runner original exit/drain retained')
                    return
                result = run()
                exact(result['status'], 'GO' if is_canary else 'FAIL', 'scientific FAIL/0 separate from INVALID')
                if not is_canary:
                    exact(replay_mock.call_count, 1, 'native replay delegated once before bundle')
                    exact((root/'screen/runner.stdout').read_bytes(), b'', 'original silent runner stdout')
                    exact((root/'screen/runner.stderr').read_bytes(), b'', 'original silent runner stderr')
                artifacts = CANARY_ARTIFACTS if is_canary else ARTIFACTS
                (root/'test-resources.txt').write_bytes(b'MOCK resource transcript\n')
                (root/'run-closed.log').write_bytes(b'MOCK closed bootstrap log\n')
                launch = dict(instance_id='i-fixture', nodes={'0':dict(instance_id='i-fixture')},
                    prefix=(CANARY_PREFIX if is_canary else PREFIX)+'a0001', source_commit='0'*40, source_archive_sha256='1'*64)
                reserved = dict(launch, schema=CANARY_SCHEMA if is_canary else SCHEMA, qualification=q if is_canary else dict(q,source_archive_commit='0'*40,expected_source_archive_sha256='1'*64),
                    wall_seconds=CANARY_WALL if is_canary else WALL, compute_cap_usd=.02 if is_canary else .10,
                    ebs_s3_allowance_usd=.15, instance_type='c7i.large', image_id=ids.IMAGE_ID,
                    root_device_name='/dev/sda1', subnet_id=ids.SUBNET, spot_max_usd_per_hour=.15,
                    spot_price_observed_usd_per_hour='.0459')
                terminal = dict(launch, **{n:q[n] for n in IDENTITIES}, schema=reserved['schema'], status='complete', phase='complete',
                    exit_code=0, original_exit_code=0, artifacts={n:ids.artifact(root/n) for n in artifacts})
                for name,value in [('aws-reservation.json',reserved),('aws-launch.json',launch),
                        ('aws-closeout.json',dict(state='terminated',nodes=launch['nodes'])),('aws-terminal.json',terminal)]:
                    put_json(root/name, value)
                exact(replay(root, is_canary=is_canary, repo=work)['status'], result['status'], 'collected raw replay')
                # Re-pin corrupted outer evidence to test semantic validation,
                # rather than merely the common outer raw-byte hash guard.
                mutations = [('worker-cgroup.json', 'native_units_drained', False),
                    ('resources.json','scratch_bytes',(CANARY_SCRATCH if is_canary else SCRATCH)+1),
                    ('resources.json','wall_seconds',WALL+1), ('resources.json','monitor_errors',['failure']),
                    ('cleanup.json','monitor_stopped',False), ('cleanup.json','assets_removed',False)]
                for filename,key,value in mutations:
                    path = root/'screen'/filename; raw = path.read_bytes(); changed = local.decode(raw); changed[key] = value
                    put_json(path,changed); terminal['artifacts']['screen/'+filename]=ids.artifact(path)
                    put_json(root/'aws-terminal.json',terminal)
                    reject('replay '+filename+':'+key, lambda: replay(root,is_canary=is_canary,repo=work))
                    path.write_bytes(raw);terminal['artifacts']['screen/'+filename]=ids.artifact(path)
                    put_json(root/'aws-terminal.json',terminal)
                if not is_canary:
                    transport = root/'screen/native-output.tar.gz'; raw=transport.read_bytes()
                    transport.write_bytes(b'not gzip'); terminal['artifacts']['screen/native-output.tar.gz']=ids.artifact(transport)
                    put_json(root/'aws-terminal.json',terminal)
                    reject('bundle tamper', lambda: replay(root,repo=work))
                    transport.write_bytes(raw); terminal['artifacts']['screen/native-output.tar.gz']=ids.artifact(transport)
                    put_json(root/'aws-terminal.json',terminal)
                    cfg_pin=local.identity(root/'screen/runner-config.json')
                    timed = root/'timeout'; timed.mkdir()
                    reject('timeout original exit preserved', lambda: invoke(cfg_pin,work,timed,root/'never',time.monotonic()-1))
                    timed_return = local.decode((timed/'runner-return.json').read_bytes())
                    require(timed_return['waited'] and timed_return['original_exit_code'] != 0 and 'TimeoutExpired' in timed_return['interruption'], 'original timeout kill/wait/exit retained')
                    log = root/'screen/runner.stdout'; log.write_bytes(b'tamper')
                    reject('silent raw log tamper',lambda: replay(root,repo=work)); log.write_bytes(b'')
                    transport = root/'screen/native-output.tar.gz'; raw = transport.read_bytes()
                    with tarfile.open(transport,'w:gz') as archive:
                        member = tarfile.TarInfo('terminal.json');member.size=len(native_body)
                        import io
                        archive.addfile(member,io.BytesIO(native_body))
                    terminal['artifacts']['screen/native-output.tar.gz']=ids.artifact(transport);put_json(root/'aws-terminal.json',terminal)
                    reject('silent raw log omitted from bundle',lambda: replay(root,repo=work))
                    transport.write_bytes(raw);terminal['artifacts']['screen/native-output.tar.gz']=ids.artifact(transport);put_json(root/'aws-terminal.json',terminal)
                    original_terminal = copy.deepcopy(terminal)
                    terminal.update(status='failed',phase='stage',exit_code=2,original_exit_code=2)
                    put_json(root/'aws-terminal.json',terminal)
                    exact(replay(root,repo=work),dict(executed=False,status='INVALID'),'original partial/failed outer terminal nonzero')
                    put_json(root/'aws-terminal.json',original_terminal)
                else:
                    admitted = work/ROOT/'canary/a0001'; shutil.copytree(root/'screen',admitted/'screen')
                    for n in ('aws-reservation.json','aws-launch.json','aws-closeout.json','aws-terminal.json','test-resources.txt','run-closed.log'):
                        shutil.copyfile(root/n, admitted/n)
                    pointer = dict(schema=ADMISSION_SCHEMA, attempt='a0001', terminal_sha256=local.identity(admitted/'aws-terminal.json')['sha256'], **{n:q[n] for n in BINDINGS})
                    put_json(work/ROOT/'canary-admission.json',pointer)
                    admitted_terminal = require_canary(work,q)
                    exact(admitted_terminal['source_commit'],'0'*40,'science archive from admitted canary')
                    with patch.object(subprocess,'check_output',return_value=''):
                        admitted_proof = preflight(work,launch=True)
                    exact(admitted_proof['source_archive_commit'],'0'*40,'external archive commit proof avoids configuration hash cycle')
                    exact(admitted_proof['expected_source_archive_sha256'],'1'*64,'exact unchanged admitted archive bytes')
                    reject('science source archive mismatch',lambda: user_data('0'*40,'2'*64,'research/sources/mock',PREFIX+'a0001',admitted_proof))
                    pointer['config_sha256']='e'*64; put_json(work/ROOT/'canary-admission.json',pointer)
                    reject('canary science admission mismatch',lambda: require_canary(work,q))
                    close=local.decode((admitted/'aws-closeout.json').read_bytes());close['state']='running';put_json(admitted/'aws-closeout.json',close)
                    reject('original instance not terminated',lambda: replay(admitted,is_canary=True,repo=work))
        fixture()
        fixture(is_canary=True)
        fixture(fail_exit=True)
        fixture(resource_failure=True)
        fixture(expired_work=True)
        fixture(timed_native=True)
        fixture(drain_failure=True)
        fixture(cleanup_failure=True)
        fixture(bundle_failure=True)
    print('PASS fixed bridge source-bound self-check; negatives='+str(len(rejected))+'; native/cloud/cgroups MOCKED; no corpus bodies/GT/ANN')


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
