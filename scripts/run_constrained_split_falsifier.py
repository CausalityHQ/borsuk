#!/usr/bin/env python3
"""Writer-only recovery and one qualified Rust source-neighborhood diagnostic.

Root owns frozen config/deployment/admission and paid execution. Local self-check
uses tiny bytes and mocked SDK/process boundaries; it is no scientific result.
"""
import copy
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_hierarchical_global_leaf_probe as probe
from scripts import launch_hierarchical_cells_100k_spot as launcher

local, positive, publication = probe.local, probe.positive, probe.publication
require, exact, fields = local.require, local.exact, local.fields

ROOT = probe.BASE.parent/'constrained-split/falsifier'
CONFIG = ROOT/'config.json'
SCHEMA = 'borsuk-constrained-split-falsifier-staging-v1'
CAMPAIGN_SCHEMA = 'borsuk-constrained-split-falsifier-spot-v1'
CANARY_SCHEMA = 'borsuk-constrained-split-falsifier-infrastructure-canary-v1'
PREFIX = 'research/hierarchical-cells/20261004/constrained-split-falsifier-'
CANARY_PREFIX = PREFIX+'canary-'
CODE = tuple(sorted(set((*launcher.PROBE_CODE, 'scripts/run_constrained_split_falsifier.py'))))
FIXED = {n: launcher.FIXED[n] for n in (
    'architecture', 'region', 'bucket', 'rows', 'dimensions', 'instance_type', 'image_id', 'root_device_name',
    'subnet_id', 'volume_gib', 'volume_type', 'encrypted', 'delete_on_termination', 'spot_max_usd_per_hour',
    'compute_cap_usd', 'memory_bytes', 'swap_bytes', 'cpu_quota_percent', 'tasks_max', 'wall_seconds',
    'versions', 'physical_s3_measured', 'vendor_win', 'scientific_qualification')}
FIXED.update(schema=SCHEMA, scratch_bytes=8 << 30, source_object_count=6, native_object_count=6,
             original_root=str(probe.ORIGINAL_ROOT), no_truth_body_access=True, reduction_enabled=False)
RESOURCES = dict(launcher.LIMITS, scratch_max_bytes=8 << 30, cpu_affinity=[0, 1], swap_bytes=0,
                 diagnostic_memory_bytes=512 << 20, diagnostic_cpu_affinity=[0],
                 diagnostic_authentication_bytes=1 << 30, diagnostic_output_bytes=2 << 20)
STAGES = ('relaion-writer', 'cohere-writer', 'split-diagnostic')
OUTPUTS = ('config.json', 'source-qualification.json', 'tool-versions.json', 'staging.json',
    'recovery.json', 'supervisor-receipt.json', 'native-execution-receipt.json', 'execution-receipt.json',
    'summary.json', 'resources.json', 'worker-cgroup.json', 'cleanup.json',
    *(f'measurement/{n}{suffix}' for n in STAGES for suffix in
      ('-stage.json', '-stage-receipt.json', '-closure.json', '.log', '-unit.log')),
    'measurement/split-config.json', 'measurement/split-report.json',
    *(f'retained/{d}/{n}' for d in probe.DATASETS for n in
      ('canonical.bin', 'order', 'generation.json', 'original-writer.json')))
ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in OUTPUTS))
CANARY_ARTIFACTS = launcher.PROBE_CANARY_ARTIFACTS
EXIT_CODES = dict(PASS=0, REJECT=1, INVALID=2, INCONCLUSIVE=3, INPUT_UNAVAILABLE=3)
EVIDENCE = 'recovery source_inputs original_authority decision terminal receipt termination qualification contract preregistration retained_terminal'.split()
NATIVE_IDENTITY = '62297f047f170fba8475c2f89db4f7308d28ef0370140880bc1ecd72d90fd098'


def evidence_inputs(config, repo):
    """Read only source/config/qualification metadata; no old diagnostic traces."""
    fields(config['evidence'], ' '.join(EVIDENCE), 'source-only evidence roster')
    evidence = {n: probe.ref(repo, p, 8 << 20) for n, p in config['evidence'].items()}
    old = evidence['recovery']; exact(old['schema'], 'borsuk-global-leaf-exact-layout-recovery-admission-v1', 'original recovery authority')
    items = {}
    for d, label in (('relaion', 'ReLAION'), ('cohere', 'CoHere')):
        recovery = old['items'][label]
        writer_bytes = probe.ref(repo, recovery['original_writer_config'], 65536, read=True)
        writer = local.decode(writer_bytes)
        inputs = {n: recovery['build_inputs'][n] for n in ('order', 'sq8')}
        inputs['raw'] = dict(path=writer['raw'], bytes=100000*768*4, sha256=writer['raw_sha256'])
        exact(writer['order'], recovery['writer_inputs']['order'], 'original writer order')
        exact(writer['raw'], recovery['writer_inputs']['raw'], 'original raw path')
        exact(writer['sq8'], recovery['writer_inputs']['sq8'], 'original SQ8 path')
        exact(writer['sq8_sha256'], inputs['sq8']['sha256'], 'original SQ8 identity')
        items[d] = dict(recovery=recovery, writer=writer, writer_bytes=writer_bytes, inputs=inputs)
    sources = evidence['source_inputs']
    exact(sources['rows'], 100000, 'original source rows'); exact(sources['dimensions'], 768, 'original source dimensions')
    evidence['sources'] = dict(items=[dict(name=i['name'], artifacts={n: i['artifacts'][n]
        for n in ('source', 'order.u64', 'sq8.bin')}) for i in sources['items']])
    exact([i['name'] for i in evidence['sources']['items']], list(probe.DATASETS), 'source restoration order')
    evidence['items'] = items
    return evidence


def objects(config, evidence):
    native = evidence['original_authority']['native']
    pins = [native['source_archive'], native['gate_log'], native['binaries']['writer'],
            config['diagnostic']['source_archive'], config['diagnostic']['gate_log'], config['diagnostic']['binary']]
    pins += [i['artifacts'][n] for i in evidence['sources']['items'] for n in ('source', 'order.u64', 'sq8.bin')]
    pins = [{k: p[k] for k in ('key', 'bytes', 'sha256')} for p in pins]
    exact(len(pins), 12, 'six native plus six original inputs')
    require(len({p['key'] for p in pins}) == 12, 'distinct source-only cold objects')
    exact(sum(p['bytes'] for p in pins[6:]), 609921661, 'six original source bytes')
    for p in pins:
        publication.object_identity(p)
    return pins


def qualify(base=Path('.'), *, canary=False):
    repo = Path(base).resolve(); pin = local.identity(repo/CONFIG); config = local.read_json(pin, 512 << 10)
    fields(config, ' '.join(FIXED)+' authority_pending run_id code_sha256 evidence diagnostic layouts resources phase_seconds admission', 'falsifier config')
    exact(config['authority_pending'], False, 'root freeze pending')
    require(re.fullmatch(r'a[0-9]{4}', config['run_id']), 'immutable scientific runID')
    for n, value in FIXED.items():
        exact(config[n], value, 'frozen source-only protocol: '+n)
    exact(config['resources'], RESOURCES, 'prospective worker/native caps require root freeze')
    exact(config['phase_seconds'], dict(writer=180, diagnostic=180), 'serial frozen phase deadlines')
    exact(set(config['code_sha256']), set(CODE), 'exact controller roster')
    for name, digest in config['code_sha256'].items():
        exact(local.identity(launcher.repo_path(repo, name))['sha256'], digest, 'controller drift')
    evidence = evidence_inputs(config, repo)
    original = probe.qualify_role('original', evidence['original_authority'], repo)
    decision, terminal, receipt = (evidence[n] for n in ('decision', 'terminal', 'receipt'))
    for value in (decision, terminal, receipt):
        exact(value['source_file_count'], 402, 'qualified native402')
        exact(value['source_identity_sha256'], NATIVE_IDENTITY, 'qualified exact native identity')
    for value in (decision, receipt):
        exact(value['qualified'], True, 'completed qualification'); exact(value['source_unchanged'], True, 'unchanged native sources')
    exact(decision['affected_tests_passed'], 26, 'all affected tests')
    exact(decision['native_exit_status'], 0, 'native gate exit0')
    termination = evidence['termination']
    exact(termination['schema'], 'borsuk-native-gate-termination-recheck-v1', 'observed gate termination receipt')
    exact(termination['exit_status'], 0, 'termination check completed')
    exact(termination['instances'], [dict(instance_id=terminal['instance_id'], state='terminated')], 'SAME qualified gate instance terminated')
    exact(decision['instance_id'], terminal['instance_id'], 'decision/terminal SAME gate instance')
    exact(terminal['phase'], 'complete', 'gate terminal'); exact(terminal['original_exit_code'], 0, 'gate original exit')
    exact(receipt['exit_status'], 0, 'gate supervisor exit'); exact(receipt['gate_status'], 0, 'gate status')
    exact(len(receipt['stages']), 6, 'six qualification layers')
    exact([stage['stage'] for stage in receipt['stages']], ['hierarchical-cell-tests', 'split-balance-bin-tests',
          'generation-integration', 'release', 'clippy', 'test-build'], 'qualified exact six gates')
    for stage in receipt['stages']:
        exact(stage['exit_status'], 0, 'every qualified native layer'); exact(stage['gate_status'], 0, 'every gate')
    inventory = receipt['source_sha256']
    exact(launcher.source_identity(inventory), NATIVE_IDENTITY, 'native inventory identity')
    qualification = evidence['qualification']
    exact(qualification['source_sha256'], inventory, 'qualified archive inventory')
    exact(launcher.source_identity(qualification['code_sha256']), terminal['code_identity_sha256'], 'qualified controller inventory')
    for name, filename in (('receipt', 'workspace-receipt.json'), ('qualification', 'source-qualification.json')):
        exact(probe.body_pin(config['evidence'][name]), terminal['artifacts'][filename], 'terminal-bound native evidence')
    fields(config['diagnostic'], 'source_archive gate_log binary', 'one qualified diagnostic')
    native = config['diagnostic']
    exact(native['source_archive']['sha256'], terminal['source_archive_sha256'], 'qualified diagnostic archive')
    exact(probe.body_pin(native['binary']), terminal['artifacts']['binaries/check_hierarchical_split_balance'], 'qualified diagnostic binary')
    exact(probe.body_pin(native['gate_log']), terminal['artifacts']['test.log'], 'qualified diagnostic log')
    contract = evidence['contract']['api']
    exact(contract['cli'], 'check_hierarchical_split_balance CONFIG SHA256 NEW_OUTPUT', 'qualified Rust CLI')
    exact(contract['config_schema'], 'borsuk-constrained-split-config-v2', 'qualified Rust API')
    exact(evidence['preregistration']['schema'], 'borsuk-constrained-semantic-split-falsifier-preregistration-v2', 'frozen v2 scientific protocol')
    old_terminal = evidence['retained_terminal']
    exact(old_terminal['phase'], 'complete', 'retained layout completed terminal'); exact(old_terminal['original_exit_code'], 0, 'retained actual exit')
    fields(config['layouts'], 'relaion cohere', 'two retained layouts')
    paths = {str(CONFIG), *CODE, *(p['path'] for p in config['evidence'].values())}
    for d, pins in config['layouts'].items():
        fields(pins, 'root directories cells', 'exact retained layout roster')
        for n, filename in (('root', 'manifest.json'), ('directories', 'directories.bin'), ('cells', 'cells.bin')):
            expected_path = probe.BASE/'global-leaf-probe/a0002/screen/retained'/d/filename
            exact(pins[n]['path'], str(expected_path), 'retained rooted layout path')
            exact(probe.body_pin(pins[n]), old_terminal['artifacts'][f'screen/retained/{d}/{filename}'], 'terminal-bound retained body')
            # Metadata only here; authenticate layout bodies on disposable worker.
            paths.add(pins[n]['path'])
        exact(pins['root']['sha256'], evidence['items'][d]['recovery']['expected_cell_root_sha256'], 'unchanged original root')
        paths.add(evidence['items'][d]['recovery']['original_writer_config']['path'])
    authority = evidence['original_authority']
    paths.update(p['path'] for p in authority['refs'].values())
    paths.add(original['source_qualification']['path']); paths.add(authority['native']['gate_log']['path'])
    paths.add(native['gate_log']['path'])
    for p in tuple(paths):
        publication.relative(p)
        if not (repo/p).exists() and (repo/(p+'.gz')).exists():
            paths.remove(p); paths.add(p+'.gz')
    paths = sorted(paths)
    transport = objects(config, evidence)
    roles = dict(original=original, diagnostic=dict(source_identity_sha256=NATIVE_IDENTITY, source_file_count=402,
        source_commit=terminal['source_commit'], native_source_commit=terminal['native_source_commit'],
        binary=probe.body_pin(native['binary']), source_archive_sha256=native['source_archive']['sha256']))
    evidence['roles'] = roles
    proof = dict(config_path=str(CONFIG), config_sha256=pin['sha256'], campaign_schema=CANARY_SCHEMA if canary else CAMPAIGN_SCHEMA,
        code_identity_sha256=launcher.ids.sha(launcher.ids.encoded(config['code_sha256'])),
        refs_identity_sha256=launcher.ids.sha(launcher.ids.encoded(dict(evidence=config['evidence'], layouts=config['layouts']))),
        native_identity_sha256=launcher.ids.sha(launcher.ids.encoded(roles)), source_file_count=402,
        source_archive_paths=paths, source_archive_paths_sha256=launcher.ids.sha(launcher.ids.encoded(paths)),
        artifact_roster_sha256=launcher.ids.sha(launcher.ids.encoded(CANARY_ARTIFACTS if canary else ARTIFACTS)),
        awscli_version=launcher.AWSCLI_VERSION, awscli_sha256=launcher.AWSCLI_SHA256)
    if not canary:
        require_admission(config, proof, repo)
    if not canary or not config['admission']['authority_pending']:
        admission_path = config['admission']['receipt']['path']
        publication.relative(admission_path)
        proof['source_archive_paths'] = sorted(set(paths) | {admission_path})
        proof['source_archive_paths_sha256'] = launcher.ids.sha(launcher.ids.encoded(proof['source_archive_paths']))
    return config, proof, evidence


def require_admission(config, proof, repo):
    admission = config['admission']
    fields(admission, 'schema authority_pending receipt config_sha256 code_identity_sha256 refs_identity_sha256 native_identity_sha256', 'root source-only admission')
    exact(admission['schema'], 'borsuk-constrained-split-source-only-admission-v1', 'distinct admission')
    exact(admission['authority_pending'], False, 'source-bound admission pending')
    exact(admission['config_sha256'], local.sha(local.canonical({k: v for k, v in config.items() if k != 'admission'})), 'admission config without recursion')
    receipt = probe.ref(repo, admission['receipt'])
    for n in ('code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256'):
        exact(admission[n], proof[n], 'root admitted source identity'); exact(receipt[n], proof[n], 'receipt admitted source identity')
    for n, value in dict(schema='borsuk-constrained-split-source-only-admission-receipt-v1', status='ADMITTED', complete=True,
                        truth_opened=False, ann_queries=0, native_processes=0, resource_gate_passed=True, cleanup_complete=True,
                        config_sha256=admission['config_sha256']).items():
        exact(receipt[n], value, 'root source-bound admission receipt')


def admit_report(body, expected_run_id, expected_config_sha256, supervisor):
    """Mirror the qualified Rust supervisor contract, without rescreening science."""
    require(0 < len(body) <= 2 << 20, 'native report cap')
    require(type(expected_run_id) is str and expected_run_id and re.fullmatch('[0-9a-f]{64}', expected_config_sha256), 'expected supervisor identity')
    fields(supervisor, 'run_id config_sha256 report_sha256 process_exit_code resources_closed', 'original supervisor receipt')
    exact(supervisor['run_id'], expected_run_id, 'original immutable runID')
    exact(supervisor['config_sha256'], expected_config_sha256, 'original configSHA')
    exact(supervisor['report_sha256'], local.sha(body), 'original reportSHA')
    exact(supervisor['resources_closed'], True, 'independent native resource closure')
    report = local.decode(body); exact(report['schema'], 'borsuk-constrained-split-diagnostic-v2', 'qualified report schema')
    exact(report['config_sha256'], expected_config_sha256, 'report original config')
    exact(report['requires_matching_supervisor_exit_receipt'], True, 'report requires supervisor')
    exact(report['durability']['standalone_authority'], False, 'body never authority')
    exact(report['durability']['status'], 'SUPERVISOR_EXIT_RECEIPT_REQUIRED', 'durable terminal status')
    require(report['status'] in EXIT_CODES and type(supervisor['process_exit_code']) is int, 'native disposition')
    exact(supervisor['process_exit_code'], EXIT_CODES[report['status']], 'actual native disposition exit')
    return report['status']


def execute(config, config_pin, repo, out, download, check, deadline):
    """Recover originals, validate both layouts, retain canonicals, invoke Rust once."""
    repo, out = Path(repo), Path(out); evidence = evidence_inputs(config, repo)
    require(not probe.ORIGINAL_ROOT.exists() and not probe.ORIGINAL_ROOT.is_symlink(), 'original geometry occupied')
    stages = []; receipt = dict(schema='borsuk-constrained-split-execution-receipt-v1', status='INVALID', complete=False,
        truth_opened=False, config=config_pin, run_id=config['run_id'], stages=stages, scientific_qualification=False)
    owned = False; started = time.monotonic(); measurement = out/'measurement'; measurement.mkdir()
    retained = out/'retained'; retained.mkdir(); recovery = {}; failure = None
    try:
        check(); probe.ORIGINAL_ROOT.mkdir(); owned = True
        original = evidence['original_authority']['native']; roles = probe.qualify_role('original', evidence['original_authority'], repo)
        native_inputs = [(original['source_archive'], 'native-source_archive'), (original['gate_log'], 'native-gate_log'),
                         (original['binaries']['writer'], 'native-writer')]
        target = probe.ORIGINAL_ROOT/'screen/scratch'; target.mkdir(parents=True)
        for pin, name in native_inputs:
            check(); download(pin, target/name); exact(probe.body_pin(local.identity(target/name)), probe.body_pin(pin), 'original qualified body')
        writer_binary = dict(probe.body_pin(original['binaries']['writer']), path=str(target/'native-writer'))
        (target/'native-writer').chmod(0o700)
        subset = {original['sources']['writer']['path']: original['sources']['writer']}
        probe.archive_sources(dict(probe.body_pin(original['source_archive']), path=str(target/'native-source_archive')),
            dict(roles['source_sha256'], **roles['source_archive_support_sha256']), subset,
            {n: probe.ORIGINAL_ROOT/'repo'/n for n in subset}, check)
        diagnostic = config['diagnostic']; diag_target = out/'scratch/diagnostic'; diag_target.mkdir()
        for n in ('source_archive', 'gate_log', 'binary'):
            check(); download(diagnostic[n], diag_target/n); exact(probe.body_pin(local.identity(diag_target/n)), probe.body_pin(diagnostic[n]), 'qualified diagnostic body')
        (diag_target/'binary').chmod(0o700)
        qualification = evidence['qualification']
        inventory = dict(qualification['source_sha256'], **qualification.get('source_archive_support_sha256', {}), **qualification['code_sha256'])
        probe.archive_sources(dict(probe.body_pin(diagnostic['source_archive']), path=str(diag_target/'source_archive')), inventory, {}, {}, check)
        for source in evidence['sources']['items']:
            d = source['name']; item = evidence['items'][d]
            probe.restore_writer_inputs(source, item, target/d, download, check)
            probe.copy_bytes(probe.ORIGINAL_ROOT/'screen/measurement'/f'{d}-writer.json', item['writer_bytes'])
        commands = {d: probe.reconstruct_writer_command(evidence['items'][d], dict(binaries=dict(writer=writer_binary))) for d in probe.DATASETS}
        limits = {k: v for k, v in RESOURCES.items() if not k.startswith('diagnostic_') and k != 'swap_bytes'}
        datasets = {}
        for d in probe.DATASETS:
            item = evidence['items'][d]; command = commands[d]
            probe.native_stage(*command, measurement, dict(limits, timeout_seconds=180), 180, deadline, stages, check)
            for p in item['recovery']['build_inputs'].values():
                check(); local.authenticate(p, 1 << 30)
            # Root supplied the six retained bodies. No cell builder is called.
            layout_pins = {n: dict(probe.body_pin(p), path=str(repo/p['path'])) for n, p in config['layouts'][d].items()}
            for p in layout_pins.values():
                check(); local.authenticate(p, 256 << 20)
            root_manifest = local.read_json(layout_pins['root'], 65536)
            exact(layout_pins['root']['sha256'], item['recovery']['expected_cell_root_sha256'], 'exact original rooted layout')
            exact({n: root_manifest['input'][n] for n in item['recovery']['build_inputs']}, item['recovery']['build_inputs'], 'layout bound recovered descriptors')
            recovered = probe.layout(layout_pins['root'], root_manifest['input'])
            for n in ('root', 'directories', 'cells'):
                exact(recovered[n], layout_pins[n], 'validated complete old layout')
            datasets[d] = dict(layout_pins, canonical=item['recovery']['build_inputs']['canonical'], order=item['recovery']['build_inputs']['order'])
            destination = retained/d; destination.mkdir(); recovery[d] = {}
            for n, filename in (('canonical', 'canonical.bin'), ('order', 'order'), ('generation', 'generation.json')):
                source_pin = item['recovery']['build_inputs'][n]
                check()
                with positive.open_input(source_pin['path']) as stream:
                    publication.transfer(stream, probe.body_pin(source_pin), destination/filename)
                recovery[d][n] = dict(local.identity(destination/filename), key=PREFIX+config['run_id']+f'/artifacts/screen/retained/{d}/{filename}')
            probe.copy_bytes(destination/'original-writer.json', item['writer_bytes']); probe.fsync_dir(destination)
        local.write_json(out/'recovery.json', dict(schema='borsuk-constrained-split-recovered-originals-v1', datasets=recovery,
            preparatory_reads_excluded_from_native_budget=True, canonical_remote_publication='terminal artifact upload; authenticate terminal before reuse'))
        pin = local.write_json(measurement/'split-config.json', dict(schema='borsuk-constrained-split-config-v2', datasets=datasets))
        binary = dict(probe.body_pin(diagnostic['binary']), path=str(diag_target/'binary'))
        report_path = measurement/'split-report.json'
        diagnostic_stage([binary['path'], pin['path'], pin['sha256'], str(report_path)], binary, pin,
                         measurement, deadline, stages, check)
        native = local.read_json(stages[-1]['native_receipt'], 1 << 20)
        actual = native['stages'][0]
        require(0 < report_path.stat().st_size <= 2 << 20, 'native report pre-read byte cap')
        body = local.authenticate(local.identity(report_path), 2 << 20, read=True)
        supervisor = dict(run_id=config['run_id'], config_sha256=pin['sha256'], report_sha256=local.sha(body),
                          process_exit_code=actual['exit_status'], resources_closed=True)
        receipt['supervisor'] = local.write_json(out/'supervisor-receipt.json', supervisor)
        receipt.update(report=local.identity(report_path), recovered=recovery)
        status = admit_report(body, config['run_id'], pin['sha256'], supervisor)
        receipt.update(status=status, complete=status != 'INVALID')
        require(status != 'INVALID', 'native diagnostic INVALID with independently observed exit2')
    except BaseException as error:
        failure = error; receipt['error'] = type(error).__name__+': '+str(error)
    finally:
        if owned:
            shutil.rmtree(probe.ORIGINAL_ROOT)
        receipt['cleanup'] = dict(original_root_removed=owned and not probe.ORIGINAL_ROOT.exists(),
            native_units_drained=all(s.get('closed') and s.get('unit_drained') for s in stages), native_processes_concurrent_max=1 if stages else 0)
        receipt['wall_seconds'] = time.monotonic()-started
        if not all(receipt['cleanup'][n] for n in ('original_root_removed', 'native_units_drained')) or time.monotonic() >= deadline:
            receipt.update(status='INVALID', complete=False)
        local.write_json(out/'native-execution-receipt.json', receipt); probe.fsync_dir(out)
    if failure:
        raise failure
    return receipt


def owned_diagnostic(spec_pin, receipt_path):
    """Reuse native process monitoring, observing nonzero scientific exits too."""
    spec = local.read_json(spec_pin, 65536); limits = spec['resources']; stages = []
    result = dict(schema='borsuk-constrained-split-owned-diagnostic-v1', status='INVALID', complete=False)
    started = time.monotonic(); failure = None
    try:
        try:
            local.run_stage('split-diagnostic', spec['command'], spec['binary'], spec['config'],
                            Path(spec['output']), limits, started+spec['timeout_seconds'], stages)
        except ValueError as error:
            # run_stage deliberately treats every nonzero exit as an error.
            # Only its exact observed-exit error is admissible here. Recheck
            # resource closure independently, never infer it from report status.
            require(len(stages) == 1 and str(error) == 'split-diagnostic exit '+str(stages[0]['exit_status'])
                    and stages[0]['exit_status'] in (1, 2, 3), 'diagnostic process failure beyond terminal exit')
        stage = stages[0]
        for p in (spec['binary'], spec['config']):
            local.authenticate(p, 256 << 20 if p == spec['binary'] else 65536)
        exact(stage['cleanup_complete'], True, 'actual native descendants waited')
        require(stage['wall_seconds'] <= spec['timeout_seconds'] and local.directory_bytes(spec['output']) <= limits['scratch_max_bytes'], 'actual diagnostic deadline/scratch')
        for snapshot in (stage['cgroup_before'], stage['cgroup_after']):
            require(0 < int(snapshot['memory.max']) <= 512 << 20 and int(snapshot['memory.peak']) <= 512 << 20, 'actual diagnostic memory')
            exact(snapshot['memory.swap.max'], '0', 'actual diagnostic noSwap'); exact(int(snapshot['memory.swap.peak']), 0, 'actual diagnostic swap')
            exact(snapshot['cpu_affinity'], [0], 'actual diagnostic CPU1')
        probe.no_oom(stage['cgroup_before'], stage['cgroup_after'])
        group = Path(stage['cgroup_after']['path']); result['cgroup'] = probe.cgroup_snapshot(group, 512 << 20, 100)
        require({int(p) for p in result['cgroup']['cgroup.procs'].split()} == {os.getpid()}, 'diagnostic descendants drained')
        stage['resource_gate_passed'] = True
        result.update(status='CLOSED', complete=True)
    except BaseException as error:
        failure = error; result['error'] = str(error)
    finally:
        result.update(stages=stages, wall_seconds=time.monotonic()-started)
        local.write_json(Path(receipt_path), result)
    if failure:
        raise failure
    return result


def diagnostic_stage(command, binary, pin, out, deadline, stages, check):
    """One child unit with the frozen CPU1/512Mi/noSwap/180s envelope."""
    check(); remaining = min(180, deadline-time.monotonic()); require(remaining > 5, 'diagnostic deadline remaining')
    slice_name = os.environ.get('BORSUK_GLOBAL_LEAF_SLICE', '')
    require(re.fullmatch(r'borsuk-global-leaf-[a-z0-9-]+\.slice', slice_name), 'owned aggregate slice')
    unit = f'borsuk-global-leaf-{os.getpid()}-split-diagnostic.service'
    limits = dict(launcher.LIMITS, memory_max_bytes=512 << 20, cpu_affinity=[0], scratch_max_bytes=8 << 30,
                  max_log_bytes=2 << 20, timeout_seconds=180)
    spec = local.write_json(out/'split-diagnostic-stage.json', dict(command=command, binary=binary, config=pin,
        output=str(out), resources=limits, timeout_seconds=max(1, int(remaining)-5)))
    receipt_path = out/'split-diagnostic-stage-receipt.json'
    shell = ['systemd-run', '--unit='+unit, '--slice='+slice_name, '--wait', '--pipe',
        '--property=MemoryMax=536870912', '--property=MemorySwapMax=0', '--property=CPUQuota=100%',
        '--property=TasksMax=512', '--property=RuntimeMaxSec='+str(int(remaining)), '--property=KillMode=control-group',
        '--property=TimeoutStopSec=5', '--property=WorkingDirectory='+str(out),
        '--setenv=PYTHONPATH='+str(Path(__file__).resolve().parents[1]), '--setenv=RAYON_NUM_THREADS=1',
        '--setenv=OPENBLAS_NUM_THREADS=1', '--setenv=OMP_NUM_THREADS=1', '--setenv=MKL_NUM_THREADS=1',
        'taskset', '-c', '0', sys.executable, str(Path(__file__).resolve()), '--owned-diagnostic',
        spec['path'], spec['sha256'], str(receipt_path)]
    record = dict(name='split-diagnostic', command=command, unit=unit, supervisor_command=shell,
                  exit_status=None, closed=False, unit_drained=False, resource_gate_passed=False)
    stages.append(record); process = None; failure = None; started = time.monotonic(); log = out/'split-diagnostic-unit.log'
    try:
        with log.open('xb') as stream:
            process = subprocess.Popen(shell, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            while process.poll() is None:
                check(); require(log.stat().st_size <= limits['max_log_bytes'], 'diagnostic supervisor log bound')
                require(time.monotonic()-started < remaining, 'diagnostic hard deadline')
                time.sleep(.05)
            exact(process.wait(), 0, 'independent diagnostic supervisor exit0')
            stream.flush(); os.fsync(stream.fileno())
        native = local.read_json(local.identity(receipt_path), 1 << 20)
        exact(native['complete'], True, 'diagnostic resource closure'); exact(native['status'], 'CLOSED', 'closed diagnostic')
        exact(len(native['stages']), 1, 'exactly one native invocation')
        actual = native['stages'][0]
        exact(actual['command'], command, 'original observed diagnostic argv')
        exact(actual['binary'], binary, 'original diagnostic binary'); exact(actual['config'], pin, 'original diagnostic config')
        exact(actual['cleanup_complete'], True, 'native wait/kill closure'); exact(actual['resource_gate_passed'], True, 'observed resource closure')
        record['resource_gate_passed'] = True
    except BaseException as error:
        failure = error
    finally:
        if process is not None:
            if process.poll() is None:
                subprocess.run(['systemctl', 'kill', '--kill-whom=all', '--signal=KILL', unit], timeout=10, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(['systemctl', 'stop', unit], timeout=10, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                record['exit_status'] = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL); record['exit_status'] = process.wait(timeout=5)
                failure = failure or ValueError('diagnostic supervisor failed to drain')
            state = subprocess.run(['systemctl', 'show', unit, '--property=MainPID', '--property=ActiveState', '--property=ControlGroup'],
                                   timeout=10, capture_output=True, text=True, check=False)
            value = dict(line.split('=', 1) for line in state.stdout.splitlines() if '=' in line)
            group = Path('/sys/fs/cgroup'+value.get('ControlGroup', ''))
            empty = not value.get('ControlGroup') or not group.exists() or not (group/'cgroup.procs').read_text().strip()
            record.update(unit_closeout=value, cgroup_drained=empty,
                          unit_drained=value.get('MainPID') == '0' and value.get('ActiveState') in ('inactive', 'failed') and empty)
        record['closed'] = True; record['wall_seconds'] = time.monotonic()-started
        for n, path in (('native_receipt', receipt_path), ('native_log', out/'split-diagnostic.log'), ('log', log)):
            if path.exists():
                record[n] = local.identity(path)
        local.write_json(out/'split-diagnostic-closure.json', record)
    require(record['unit_drained'], 'diagnostic owned unit drained')
    if failure:
        raise failure
    return record


def verify_recovery(out, *, repo=None, config=None, config_pin=None, evidence=None):
    out = Path(out); receipt = local.read_json(local.identity(out/'native-execution-receipt.json'))
    exact(receipt['config']['sha256'], config_pin['sha256'], 'native execution frozen config')
    exact(receipt['run_id'], config['run_id'], 'native immutable runID')
    recovery = local.read_json(local.identity(out/'recovery.json'))
    exact(recovery['datasets'], receipt['recovered'], 'retained recovery receipt')
    for d in probe.DATASETS:
        for n, filename in (('canonical', 'canonical.bin'), ('order', 'order'), ('generation', 'generation.json')):
            pin = recovery['datasets'][d][n]
            exact(probe.body_pin(local.identity(out/'retained'/d/filename)), probe.body_pin(pin), 'retained original body')
            exact(probe.body_pin(pin), probe.body_pin(evidence['items'][d]['recovery']['build_inputs'][n]), 'exact original recovered identity')
            exact(pin['key'], PREFIX+config['run_id']+f'/artifacts/screen/retained/{d}/{filename}', 'immutable retained remote key')
    return receipt


def verify_execution(out, config, receipt, evidence):
    out = Path(out); final = local.read_json(local.identity(out/'execution-receipt.json'))
    exact({k: v for k, v in final.items() if k not in ('native_execution', 'worker_closure')}, receipt, 'final/native receipt binding')
    exact(final['complete'], True, 'completed execution')
    exact(final['truth_opened'], False, 'source-only execution')
    for n in ('original_root_removed', 'native_units_drained'):
        exact(final['cleanup'][n], True, 'native execution cleanup')
    exact([s['name'] for s in final['stages']], list(STAGES), 'two writers then one diagnostic')
    for record in final['stages']:
        for key in ('closed', 'unit_drained', 'cgroup_drained', 'resource_gate_passed'):
            exact(record[key], True, 'independent unit closure')
        exact(record['unit_closeout']['MainPID'], '0', 'unit original main PID drained')
        require(record['unit_closeout']['ActiveState'] in ('inactive', 'failed'), 'unit observed inactive')
        exact(record['exit_status'], 0, 'owned supervisor exit0')
        name = record['name']; inner = local.read_json(dict(probe.body_pin(record['native_receipt']), path=str(out/'measurement'/(name+'-stage-receipt.json'))))
        exact(inner['complete'], True, 'actual native closure'); exact(inner['status'], 'CLOSED', 'actual native complete')
        exact(len(inner['stages']), 1, 'serial child count'); actual = inner['stages'][0]
        exact(actual['command'], record['command'], 'observed native command')
        exact(actual['cleanup_complete'], True, 'actual process waited'); exact(actual['resource_gate_passed'], True, 'actual child resources')
        memory, cpu = (512 << 20, 100) if name == 'split-diagnostic' else (2 << 30, 200)
        probe.validate_cgroup(inner['cgroup'], memory, cpu); probe.no_oom(actual['cgroup_before'], actual['cgroup_after'])
        for key, filename in (('native_log', name+'.log'), ('log', name+'-unit.log')):
            exact(probe.body_pin(local.identity(out/'measurement'/filename)), probe.body_pin(record[key]), 'retained actual log')
        if name == 'split-diagnostic':
            native_pin = local.identity(out/'measurement/split-config.json')
            # Collection changes the absolute output prefix, not immutable argv.
            exact(record['command'][1:3], [actual['config']['path'], native_pin['sha256']], 'native config argv binding')
            exact(probe.body_pin(actual['binary']), probe.body_pin(config['diagnostic']['binary']), 'qualified diagnostic role')
            supervisor = local.read_json(dict(probe.body_pin(final['supervisor']), path=str(out/'supervisor-receipt.json')))
            exact(supervisor['process_exit_code'], actual['exit_status'], 'supervisor original process exit')
            body = local.authenticate(local.identity(out/'measurement/split-report.json'), 2 << 20, read=True)
            exact(admit_report(body, config['run_id'], native_pin['sha256'], supervisor), final['status'], 'preserve native result with supervisor')
        else:
            d = name.split('-')[0]; old = evidence['items'][d]['recovery']
            exact(actual['exit_status'], 0, 'original writer exit0')
            exact(record['command'], [str(probe.ORIGINAL_ROOT/'screen/scratch/native-writer'),
                str(probe.ORIGINAL_ROOT/'screen/measurement'/f'{d}-writer.json'), old['original_writer_config']['sha256'],
                '67108864', str(probe.ORIGINAL_ROOT/'screen/measurement'/f'{d}-generation')], 'original writer command only')
            exact(probe.body_pin(actual['binary']), probe.body_pin(evidence['original_authority']['native']['binaries']['writer']), 'qualified original writer')
    return final


def canary(config, evidence, client, calls, scratch, check, deadline):
    """Existing SDK canary shape, distinct source-only CLI and object roster."""
    for n in ('numpy', 'pyarrow', 'boto3', 'botocore'):
        launcher.importlib.import_module(n)
    versions = {n: launcher.importlib.metadata.version(n) for n in (*FIXED['versions'], *launcher.SDK_VERSIONS)}
    exact(versions, dict(FIXED['versions'], **launcher.SDK_VERSIONS), 'canary real installed versions')
    model = client.meta.service_model
    require('IfNoneMatch' in model.operation_model('PutObject').input_shape.members, 'conditional SDK upload model')
    for operation in ('HeadObject', 'GetObject'):
        require({'Bucket', 'Key'} <= set(model.operation_model(operation).input_shape.members), 'SDK source transport model')
    process = subprocess.run([sys.executable, '-m', launcher.MODULE, '--constrained-split-falsifier'], capture_output=True, text=True,
        timeout=min(30, max(.001, deadline-time.monotonic())), env=dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1'))
    require(process.returncode == 2 and 'CLI:' in process.stderr and 'INVALID:' in process.stderr and not process.stdout, 'distinct actual source-only CLI')
    for pin in objects(config, evidence):
        check(); response = publication.sdk_call(client, calls, 'head_object', pin['key'], launcher.BUCKET)
        exact(response['ContentLength'], pin['bytes'], 'HEAD presence/length only')
    logs = [evidence['original_authority']['native']['gate_log'], config['diagnostic']['gate_log']]
    for i, pin in enumerate(logs):
        check(); publication.download(client, calls, launcher.BUCKET, {k: pin[k] for k in ('key', 'bytes', 'sha256')}, scratch/f'gate-{i}.log')
    return dict(schema=CANARY_SCHEMA, status='GO', complete=True, scientific_performance_evidence=False,
        versions=versions, sdk_conditional_put_model=True, cli_exit_status=process.returncode, cli_stderr=process.stderr,
        sdk_calls=calls, authenticated_logs=[probe.body_pin(p) for p in logs], ann_queries=0, native_processes=0,
        truth_or_panel_body_reads=0, dataset_payload_gets=0)


def require_canary(base, proof):
    pointer = local.read_json(local.identity(Path(base)/ROOT/'canary-admission.json'))
    exact(pointer['schema'], 'borsuk-constrained-split-falsifier-canary-admission-v1', 'distinct source-only canary')
    require(re.fullmatch(r'a[0-9]{4}', pointer['attempt']), 'canary attempt')
    out = Path(base)/ROOT/'canary'/pointer['attempt']
    exact(local.identity(out/'aws-terminal.json')['sha256'], pointer['terminal_sha256'], 'admitted immutable canary terminal')
    for n in ('config_sha256', 'code_identity_sha256', 'refs_identity_sha256', 'native_identity_sha256', 'source_archive_paths_sha256'):
        exact(pointer[n], proof[n], 'unchanged canary authority')
    require(launcher.probe_replay(out, canary=True, repo=base)['executed'], 'distinct terminated canary GO')


def profile():
    """Scoped delegation to the existing lifecycle, SDK, resources and cleanup."""
    stack = ExitStack(); old_user_data = launcher.probe_user_data; old_replay = launcher.probe_replay
    def user_data(*args, **kwargs):
        if not kwargs.get('canary'):
            cfg = local.read_json(local.identity(CONFIG), 512 << 10)
            exact(args[3], PREFIX+cfg['run_id'], 'immutable scientific run identity')
        body = old_user_data(*args, **kwargs)
        return body.replace('--global-leaf-probe', '--constrained-split-falsifier').replace('/mnt/hierarchical-global-leaf-probe', '/mnt/hierarchical-constrained-split-falsifier')
    def replay(out, *, canary=False, repo=None):
        result = old_replay(out, canary=canary, repo=repo)
        if not canary and (Path(out)/'screen/summary.json').exists():
            result['status'] = local.read_json(local.identity(Path(out)/'screen/summary.json'))['status']
        return result
    helper = SimpleNamespace(**{n: getattr(probe, n) for n in
        ('ORIGINAL_ROOT', 'cgroup_snapshot', 'no_oom', 'fsync_dir', 'validate_cgroup')},
        execute=execute, verify_pair=verify_recovery, verify_execution=verify_execution)
    stack.enter_context(patch.multiple(launcher, PROBE_ROOT=ROOT, PROBE_CONFIG=CONFIG, PROBE_SCHEMA=CAMPAIGN_SCHEMA,
        PROBE_CANARY_SCHEMA=CANARY_SCHEMA, PROBE_PREFIX=PREFIX, PROBE_CANARY_PREFIX=CANARY_PREFIX,
        PROBE_CODE=CODE, PROBE_ARTIFACTS=ARTIFACTS, PROBE_CANARY_ARTIFACTS=CANARY_ARTIFACTS,
        SCRATCH=8 << 30, probe=helper, probe_qualify=qualify, probe_objects=objects, probe_canary=canary,
        probe_require_canary=require_canary, probe_user_data=user_data, probe_replay=replay))
    return stack


def cli(args):
    require(args, 'CLI: --constrained-split-falsifier aNNNN | --canary aNNNN | --stage[-canary] REPO OUTPUT ROOT | --replay[-canary] OUTPUT | --self-check')
    if args == ['--self-check']:
        self_check(); return
    if len(args) == 4 and args[0] == '--owned-diagnostic':
        owned_diagnostic(dict(path=args[1], bytes=Path(args[1]).stat().st_size, sha256=args[2]), args[3]); return
    with profile():
        if len(args) == 4 and args[0] in ('--stage', '--stage-canary'):
            print(json.dumps(launcher.probe_stage(*args[1:], canary=args[0] == '--stage-canary')))
        elif len(args) == 2 and args[0] in ('--replay', '--replay-canary'):
            print(json.dumps(launcher.probe_replay(args[1], canary=args[0] == '--replay-canary')))
        else:
            canary_run = len(args) == 2 and args[0] == '--canary'
            require(canary_run or len(args) == 1, 'CLI: --constrained-split-falsifier aNNNN | --canary aNNNN')
            with open('/tmp/borsuk-constrained-split-falsifier-launch.lock', 'a+') as lock:
                launcher.fcntl.flock(lock, launcher.fcntl.LOCK_EX | launcher.fcntl.LOCK_NB)
                launcher.probe_main(args[1] if canary_run else args[0], canary=canary_run)


def pipeline_self_check():
    """Tiny original writer products and rooted layouts; fake processes only."""
    import struct
    with tempfile.TemporaryDirectory(prefix='split-pipeline-') as tmp, ExitStack() as stack:
        root = Path(tmp); original = root/'original'; fixtures = root/'fixtures'; fixtures.mkdir()
        stack.enter_context(patch.object(probe, 'ORIGINAL_ROOT', original))
        bodies, items, layouts, source_items = {}, {}, {}, []
        payload = b'original'; cold = b'mocked qualified transport'
        def transport(key):
            bodies[key] = cold
            return dict(path='assets/'+key, key=key, bytes=len(cold), sha256=local.sha(cold))
        original_native = dict(source_archive=transport('old-archive'), gate_log=transport('old-log'),
                              binaries=dict(writer=transport('old-writer')),
                              sources=dict(writer=dict(path='writer.rs', bytes=1, sha256='a'*64)))
        diagnostic = {n: transport('new-'+n) for n in ('source_archive', 'gate_log', 'binary')}
        for d in probe.DATASETS:
            folder = original/'screen/scratch'/d; measurement = original/'screen/measurement'
            inputs = {n: dict(path=str(folder/f), bytes=len(payload), sha256=local.sha(payload))
                      for n, f in (('raw', 'raw' if d == 'relaion' else 'source'), ('order', 'order'), ('sq8', 'sq8'))}
            artifacts = {}
            for n in ('source', 'order.u64', 'sq8.bin'):
                key = d+'-'+n; bodies[key] = payload
                artifacts[n] = dict(key=key, bytes=len(payload), sha256=local.sha(payload))
            source_items.append(dict(name=d, artifacts=artifacts))
            build_inputs = {n: dict(path=str(measurement/(d+'-generation')/filename), bytes=len(payload), sha256=local.sha(payload))
                for n, filename in (('generation', 'manifest.json'), ('plane', 'plane/manifest.json'),
                    ('canonical', 'canonical.bin'), ('mean', 'plane/mean.bin'), ('records', 'plane/records.bin'))}
            build_inputs.update(order=inputs['order'], sq8=inputs['sq8'])
            # Four rows, one-byte records; rooted cell checks use their real parser.
            build_inputs['records'].update(bytes=4, sha256=local.sha(b'abcd'))
            build = dict(build_inputs, schema='borsuk-hierarchical-cells-build-v1', cell_rows=2,
                         sample_rows=2, max_depth=1, max_build_payload_bytes=64 << 20, max_output_bytes=256 << 20)
            cell_body = bytearray(); children = []
            for i in range(2):
                source = b''.join(struct.pack('<q', n)+b'x' for n in (2*i, 2*i+1)); refinement = bytes(28)
                start = len(cell_body); cell_body.extend(source+refinement)
                span = dict(offset=start, bytes=len(source), sha256=local.sha(source))
                refine = dict(offset=start+len(source), bytes=len(refinement), sha256=local.sha(refinement))
                whole = dict(offset=start, bytes=46, sha256=local.sha(source+refinement))
                children.append(dict(rows=2, prototype=[float(i), 0.0], target=dict(kind='cell',
                    cell=dict(id=i, first_row=2*i, source=span, refinement=[refine], whole=whole))))
            target = fixtures/d; target.mkdir()
            directory = local.canonical(dict(children=children))
            probe.copy_bytes(target/'directories.bin', directory); probe.copy_bytes(target/'cells.bin', cell_body)
            manifest = dict(schema='borsuk-hierarchical-cells-resident-v3', input=build, rows=4, dimensions=2,
                directory_bytes=len(directory), directory_sha256=local.sha(directory), cell_bytes=len(cell_body),
                root_directory=dict(offset=0, bytes=len(directory), sha256=local.sha(directory)), build=dict(directories=1, cells=2))
            root_pin = local.write_json(target/'manifest.json', manifest)
            layouts[d] = {n: dict(local.identity(target/f), path=str((target/f).relative_to(root)))
                          for n, f in (('root', 'manifest.json'), ('directories', 'directories.bin'), ('cells', 'cells.bin'))}
            writer_bytes = b'{"fixture":"exact original writer bytes"}\n'
            recovery = dict(build_inputs=build_inputs, original_writer_config=dict(bytes=len(writer_bytes), sha256=local.sha(writer_bytes)),
                            expected_cell_root_sha256=root_pin['sha256'], output_cell_path=str(measurement/(d+'-cells')))
            items[d] = dict(inputs=inputs, recovery=recovery, writer=local.decode(writer_bytes), writer_bytes=writer_bytes)
        evidence = dict(items=items, sources=dict(items=source_items), original_authority=dict(native=original_native),
                        qualification=dict(source_sha256={}, code_sha256={}))
        config = dict(run_id='a0001', diagnostic=diagnostic, layouts=layouts)
        calls, writer_failure, changed_product, pass_exit2 = [], [False], [False], [False]
        def download(pin, destination):
            publication.transfer(__import__('io').BytesIO(bodies[pin['key']]), probe.body_pin(pin), destination)
        def fake_writer(name, command, binary, pin, output, resources, seconds, deadline, stages, check):
            calls.append(name); stages.append(dict(name=name, closed=True, unit_drained=True))
            require(name.endswith('-writer') and command[3] == '67108864', 'only original writer admitted')
            if writer_failure[0]:
                raise ValueError('mock original writer failure')
            d = name.split('-')[0]
            for n, p in items[d]['recovery']['build_inputs'].items():
                if n not in ('order', 'sq8'):
                    probe.copy_bytes(p['path'], b'abcd' if n == 'records' else payload)
            if changed_product[0]:
                Path(items[d]['recovery']['build_inputs']['canonical']['path']).write_bytes(b'changed')
        def fake_diagnostic(command, binary, pin, output, deadline, stages, check):
            calls.append('split-diagnostic')
            report = dict(schema='borsuk-constrained-split-diagnostic-v2', status='PASS' if pass_exit2[0] else 'INCONCLUSIVE', config_sha256=pin['sha256'],
                          requires_matching_supervisor_exit_receipt=True,
                          durability=dict(status='SUPERVISOR_EXIT_RECEIPT_REQUIRED', standalone_authority=False))
            probe.copy_bytes(command[-1], local.canonical(report))
            inner = local.write_json(output/'split-diagnostic-stage-receipt.json', dict(stages=[dict(exit_status=2 if pass_exit2[0] else 3)]))
            stages.append(dict(name='split-diagnostic', closed=True, unit_drained=True, native_receipt=inner))
        source_open = positive.open_input
        def no_panel(path):
            require(not any(n in str(path).lower() for n in ('request', 'truth', 'query', 'nomination')), 'source-only open sentinel')
            return source_open(path)
        stack.enter_context(patch.object(positive, 'open_input', side_effect=no_panel))
        stack.enter_context(patch.object(positive, 'raw_blocks', side_effect=lambda stream, *_: [stream.read()]))
        stack.enter_context(patch.object(sys.modules[__name__], 'evidence_inputs', return_value=evidence))
        stack.enter_context(patch.object(probe, 'qualify_role', return_value=dict(source_sha256={}, source_archive_support_sha256={})))
        stack.enter_context(patch.object(probe, 'archive_sources'))
        stack.enter_context(patch.object(probe, 'native_stage', side_effect=fake_writer))
        stack.enter_context(patch.object(sys.modules[__name__], 'diagnostic_stage', side_effect=fake_diagnostic))
        real_layout = probe.layout
        stack.enter_context(patch.object(probe, 'layout', side_effect=lambda p, b: real_layout(p, b, 2, 4)))
        for obj, name in ((probe, 'original_evidence'), (probe, 'restore_panel'), (probe, 'reconstruct_command'),
                          (probe, 'nomination_config'), (local, 'prepare'), (local, 'validate_diagnostic'),
                          (launcher, 'panel_inputs'), (launcher, 'probe_qualify')):
            stack.enter_context(patch.object(obj, name, side_effect=AssertionError('forbidden old flow: '+name)))
        def run(name):
            out = root/name; out.mkdir(); (out/'scratch').mkdir()
            pin = local.write_json(out/'config.json', config)
            return execute(config, pin, root, out, download, lambda **_: None, time.monotonic()+90)
        for name, flag in (('writer-failure', writer_failure), ('changed-product', changed_product)):
            flag[0] = True; calls.clear()
            try:
                run(name)
            except ValueError:
                pass
            else:
                raise AssertionError('failed writer/product reached diagnostic')
            require('split-diagnostic' not in calls and not original.exists(), 'pre-diagnostic failure closes original root')
            flag[0] = False
        calls.clear(); result = run('complete')
        exact(calls, ['relaion-writer', 'cohere-writer', 'split-diagnostic'], 'exactly one diagnostic after two writers')
        exact(result['status'], 'INCONCLUSIVE', 'preserved native scientific disposition')
        require(not original.exists() and all((root/'complete/retained'/d/'canonical.bin').read_bytes() == payload for d in probe.DATASETS), 'retain exact originals before cleanup')
        pass_exit2[0] = True
        try:
            run('pass-body-exit2')
        except ValueError:
            pass
        else:
            raise AssertionError('PASS-looking native body exit2 accepted')
        closed = local.read_json(local.identity(root/'pass-body-exit2/native-execution-receipt.json'))
        exact(closed['status'], 'INVALID', 'exit2 overrides PASS-looking report')
        exact(closed['complete'], False, 'exit2 cannot complete science')
        observed = local.read_json(local.identity(root/'pass-body-exit2/supervisor-receipt.json'))
        exact(observed['process_exit_code'], 2, 'failed native exit retained independently')
        original.mkdir()
        try:
            run('occupied')
        except ValueError:
            pass
        else:
            raise AssertionError('occupied original root accepted')
        require(original.exists(), 'never delete unowned original root')


def metadata_self_check():
    """Real committed qualification metadata; never hydrate corpus or layouts."""
    import gzip
    repo = Path(__file__).resolve().parents[1]
    base = '57b5258abed122a9cf30d0c092641f782656f74f'
    gate = probe.BASE.parent/'constrained-split/implementation-gates'
    paths = dict(recovery=probe.BASE/'global-leaf-layout-recovery-admission.json',
        source_inputs=Path('docs/research/native-union-20260928/source-completion-http-config.json'),
        original_authority=probe.BASE/'qualified-original-binary-authority.json',
        decision=gate/'a0002/decision.json', terminal=gate/'a0002/aws-terminal.json',
        receipt=gate/'a0002/workspace-receipt.json', qualification=gate/'a0002/source-qualification.json',
        termination=gate/'a0002/termination-recheck.json', contract=gate/'source-contract.json',
        preregistration=probe.BASE.parent/'implementation-review/constrained-split-falsifier-preregistration-v2-20261003.json',
        retained_terminal=probe.BASE/'global-leaf-probe/a0002/aws-terminal.json')
    with tempfile.TemporaryDirectory(prefix='split-metadata-') as tmp:
        target = Path(tmp)
        def hydrate(name):
            require('/retained/' not in str(name) and not str(name).endswith(('.bin', '.tar.gz')), 'metadata-only hydration')
            commit = '1bb62bc8' if str(name).endswith('/termination-recheck.json') else base
            result = subprocess.run(['git', 'show', commit+':'+str(name)], cwd=repo, capture_output=True, check=False)
            if result.returncode:
                result = subprocess.run(['git', 'show', commit+':'+str(name)+'.gz'], cwd=repo, capture_output=True, check=True)
                require(len(result.stdout) < 1 << 20, 'small compressed metadata')
                body = gzip.decompress(result.stdout)
            else:
                body = result.stdout
            require(len(body) <= 8 << 20, 'metadata body bound')
            probe.copy_bytes(target/name, body)
            return dict(local.identity(target/name), path=str(name))
        evidence_pins = {n: hydrate(p) for n, p in paths.items()}
        authority = probe.ref(target, evidence_pins['original_authority'])
        for p in authority['refs'].values():
            hydrate(p['path'])
        original_terminal = probe.ref(target, authority['refs']['terminal'])
        hydrate(Path(authority['refs']['terminal']['path']).parent/'source-qualification.json')
        hydrate(authority['native']['gate_log']['path'])
        recovery = probe.ref(target, evidence_pins['recovery'])
        for item in recovery['items'].values():
            hydrate(item['original_writer_config']['path'])
        for name in CODE:
            # Use working code hashes, but never change the frozen native sources.
            body = (repo/name).read_bytes() if (repo/name).exists() else subprocess.check_output(['git', 'show', base+':'+name], cwd=repo)
            probe.copy_bytes(target/name, body)
        terminal = probe.ref(target, evidence_pins['terminal'])
        retained_terminal = probe.ref(target, evidence_pins['retained_terminal'])
        layouts = {d: {n: dict(path=str(probe.BASE/'global-leaf-probe/a0002/screen/retained'/d/filename),
            **retained_terminal['artifacts'][f'screen/retained/{d}/{filename}'])
            for n, filename in (('root', 'manifest.json'), ('directories', 'directories.bin'), ('cells', 'cells.bin'))}
            for d in probe.DATASETS}
        log = dict(path=str(gate/'a0002/test.log'), key='metadata-only/diagnostic-log', **terminal['artifacts']['test.log'])
        hydrate(log['path'])
        config = dict(FIXED, authority_pending=False, run_id='a0001', evidence=evidence_pins, layouts=layouts,
            resources=RESOURCES, phase_seconds=dict(writer=180, diagnostic=180), admission=dict(authority_pending=True),
            code_sha256={n: local.identity(target/n)['sha256'] for n in CODE}, diagnostic=dict(
                source_archive=dict(path='assets/diagnostic-source.tar.gz', key='metadata-only/diagnostic-archive', bytes=1,
                                    sha256=terminal['source_archive_sha256']), gate_log=log,
                binary=dict(path='assets/diagnostic', key='metadata-only/diagnostic', **terminal['artifacts']['binaries/check_hierarchical_split_balance'])))
        (target/CONFIG).parent.mkdir(parents=True)
        local.write_json(target/CONFIG, config)
        _, proof, evidence = qualify(target, canary=True)
        exact(proof['source_file_count'], 402, 'actual committed qualification accepted')
        require(all(not (target/p['path']).exists() for p in layouts['cohere'].values()), 'preflight never opens retained layout bodies')
        # Use existing SDK transfer accounting with controlled small log streams.
        # The actual CLI runs; import/version/model/SDK boundaries are labelled mocks.
        import io
        from unittest.mock import Mock
        native_objects = objects(config, evidence); by_key = {p['key']: p for p in native_objects}
        logs = [authority['native']['gate_log'], log]
        log_bodies = {p['key']: (target/p['path']).read_bytes() for p in logs}
        def head(**kwargs):
            exact(kwargs['Bucket'], launcher.BUCKET, 'mock SDK bucket')
            return dict(ContentLength=by_key[kwargs['Key']]['bytes'])
        def get(**kwargs):
            require(kwargs['Key'] in log_bodies, 'canary dataset/native binary GET forbidden')
            return dict(ContentLength=len(log_bodies[kwargs['Key']]), Body=io.BytesIO(log_bodies[kwargs['Key']]))
        def operation(name):
            return SimpleNamespace(input_shape=SimpleNamespace(members={'Bucket': None, 'Key': None, 'IfNoneMatch': None}))
        client = Mock(head_object=Mock(side_effect=head), get_object=Mock(side_effect=get),
                      meta=SimpleNamespace(service_model=SimpleNamespace(operation_model=operation)))
        versions = dict(FIXED['versions'], **launcher.SDK_VERSIONS); calls = []; scratch = target/'canary-scratch'; scratch.mkdir()
        with patch.object(launcher.importlib, 'import_module'), patch.object(launcher.importlib.metadata, 'version', side_effect=lambda n: versions[n]):
            result = canary(config, evidence, client, calls, scratch, lambda: None, time.monotonic()+30)
        exact(result['status'], 'GO', 'source-only SDK canary control flow')
        exact([c['operation'] for c in calls], ['head_object']*12+['get_object']*2, 'no-GT canary transport roster')
        exact([c['key'] for c in calls[12:]], [p['key'] for p in logs], 'only two small log bodies fetched')
        before = Path.cwd()
        try:
            os.chdir(target)
            with profile():
                scientific = launcher.probe_user_data('a'*40, 'b'*64, 'source/mock', PREFIX+'a0001',
                    dict(proof, campaign_schema=CAMPAIGN_SCHEMA))
                infrastructure = launcher.probe_user_data('a'*40, 'b'*64, 'source/mock', CANARY_PREFIX+'a0002', proof, canary=True)
                require('--constrained-split-falsifier --stage ' in scientific and 'MemoryMax=2G' in scientific
                    and 'CPUQuota=200%' in scientific and 'MemorySwapMax=0' in scientific
                    and 'sync -f terminal.json' in scientific, 'scientific delegated bootstrap')
                require('--constrained-split-falsifier --stage-canary ' in infrastructure and 'MemoryMax=256M' in infrastructure
                    and 'CPUQuota=100%' in infrastructure, 'distinct bounded canary bootstrap')
        finally:
            os.chdir(before)
        # Absence of a decision state field must not change qualification.
        decision = probe.ref(target, evidence_pins['decision']); decision.pop('state', None)
        (target/paths['decision']).unlink()
        evidence_pins['decision'] = dict(local.write_json(target/paths['decision'], decision), path=str(paths['decision']))
        (target/CONFIG).unlink(); local.write_json(target/CONFIG, config); qualify(target, canary=True)
        termination = probe.ref(target, evidence_pins['termination'])
        termination['instances'][0]['instance_id'] = 'i-wrong'
        (target/paths['termination']).unlink()
        evidence_pins['termination'] = dict(local.write_json(target/paths['termination'], termination), path=str(paths['termination']))
        (target/CONFIG).unlink(); local.write_json(target/CONFIG, config)
        try:
            qualify(target, canary=True)
        except ValueError as error:
            require('SAME qualified gate' in str(error), 'observed termination instance binding')
        else:
            raise AssertionError('wrong qualified instance termination accepted')
    print('PASS actual committed native402/all6gates/26tests metadata-only preflight; '
          'decision without state accepted; wrong termination instance rejected; no archive/layout hydration')


def self_check():
    """Catches forbidden-flow entry, config/input drift, writer failure and false PASS."""
    require(hasattr(probe, 'restore_writer_inputs') and hasattr(probe, 'reconstruct_writer_command'),
            'writer-only helpers missing')
    module = sys.modules[__name__]
    require(hasattr(module, 'admit_report'), 'independent supervisor admission missing')
    check_limits = dict(memory_max_bytes=256 << 20, cpu_affinity=[0])
    local.resource_snapshot(check_limits)
    with tempfile.TemporaryDirectory(prefix='split-runner-') as tmp, ExitStack() as stack:
        root = Path(tmp); original = root/'original'
        stack.enter_context(patch.object(probe, 'ORIGINAL_ROOT', original))
        source_open = positive.open_input
        def no_panel(path):
            require(not any(n in str(path).lower() for n in ('request', 'truth', 'query', 'nomination')),
                    'forbidden body opened')
            return source_open(path)
        stack.enter_context(patch.object(positive, 'open_input', side_effect=no_panel))
        for obj, name in ((probe, 'original_evidence'), (probe, 'execute'), (probe, 'restore_panel'),
                          (probe, 'reconstruct_command'), (probe, 'verify_pair'), (probe, 'nomination_config'),
                          (local, 'prepare'), (local, 'validate_diagnostic'), (launcher, 'probe_qualify'),
                          (launcher, 'panel_inputs'), (launcher, 'reduce_diagnostic')):
            stack.enter_context(patch.object(obj, name, side_effect=AssertionError('forbidden flow: '+name)))
        payload = b'tiny original bytes'
        pins = {n: dict(key=n, bytes=len(payload), sha256=local.sha(payload))
                for n in ('source', 'order.u64', 'sq8.bin')}
        folder = original/'screen/scratch/cohere'
        inputs = {n: dict(path=str(folder/f), bytes=len(payload), sha256=local.sha(payload))
                  for n, f in (('raw', 'source'), ('order', 'order'), ('sq8', 'sq8'))}
        def download(pin, destination):
            exact(set(pin) >= {'key', 'bytes', 'sha256'}, True, 'SDK mock complete descriptor')
            probe.copy_bytes(destination, payload)
        item = dict(inputs=inputs)
        probe.restore_writer_inputs(dict(name='cohere', artifacts=pins), item, folder, download, lambda: None)
        exact((folder/'source').read_bytes(), payload, 'writer receives original raw')
        require(not (folder/'full-requests').exists(), 'no requests restored')
        shutil.rmtree(folder)
        changed = copy.deepcopy(item); changed['inputs']['raw']['sha256'] = 'a'*64
        try:
            probe.restore_writer_inputs(dict(name='cohere', artifacts=pins), changed, folder, download, lambda: None)
        except ValueError:
            pass
        else:
            raise AssertionError('changed writer input pin accepted')
        shutil.rmtree(folder)
        folder.mkdir()
        try:
            probe.restore_writer_inputs(dict(name='cohere', artifacts=pins), item, folder, download, lambda: None)
        except FileExistsError:
            pass
        else:
            raise AssertionError('occupied input folder accepted')
        measurement = original/'screen/measurement'; measurement.mkdir()
        writer_bytes = b'{"tiny":"original config bytes"}\n'
        writer_pin = probe.copy_bytes(measurement/'cohere-writer.json', writer_bytes)
        role = dict(binaries=dict(writer=dict(path=str(original/'screen/scratch/native-writer'),
                                             bytes=1, sha256='a'*64)))
        recovery = dict(original_writer_config=writer_pin, output_cell_path=str(measurement/'cohere-cells'))
        item.update(recovery=recovery, writer=local.decode(writer_bytes))
        command = probe.reconstruct_writer_command(item, role)
        exact(command[1], [role['binaries']['writer']['path'], str(measurement/'cohere-writer.json'),
                          writer_pin['sha256'], '67108864', str(measurement/'cohere-generation')],
              'exact original writer flags/paths')
        (measurement/'cohere-writer.json').write_bytes(writer_bytes+b' ')
        try:
            probe.reconstruct_writer_command(item, role)
        except ValueError:
            pass
        else:
            raise AssertionError('changed raw config accepted')
        body = local.canonical(dict(schema='borsuk-constrained-split-diagnostic-v2', status='PASS',
            config_sha256='a'*64, requires_matching_supervisor_exit_receipt=True,
            durability=dict(standalone_authority=False, status='SUPERVISOR_EXIT_RECEIPT_REQUIRED')))
        supervisor = dict(run_id='tiny-run', config_sha256='a'*64, report_sha256=local.sha(body),
                          process_exit_code=0, resources_closed=True)
        exact(admit_report(body, 'tiny-run', 'a'*64, supervisor), 'PASS', 'matching original supervisor')
        for key, value in (('process_exit_code', 2), ('resources_closed', False), ('run_id', 'wrong'),
                           ('config_sha256', 'b'*64), ('report_sha256', 'c'*64)):
            bad = dict(supervisor, **{key: value})
            try:
                admit_report(body, 'tiny-run', 'a'*64, bad)
            except ValueError:
                pass
            else:
                raise AssertionError('PASS admitted without matching '+key)
        for status, code in (('REJECT', 1), ('INCONCLUSIVE', 3), ('INPUT_UNAVAILABLE', 3), ('INVALID', 2)):
            report = local.decode(body); report['status'] = status; encoded = local.canonical(report)
            observed = dict(supervisor, process_exit_code=code, report_sha256=local.sha(encoded))
            exact(admit_report(encoded, 'tiny-run', 'a'*64, observed), status, 'preserve native disposition')
    pipeline_self_check()
    metadata_self_check()
    snapshot = local.resource_snapshot(check_limits)
    print('PASS source-only actual cgroup: memory.max='+snapshot['memory.max']+
          ', memory.peak='+snapshot['memory.peak']+', swap.peak='+snapshot['memory.swap.peak']+', CPU affinity=[0]')
    print('PASS tiny writer-only fixture/input and config drift/occupied path/writer failure; '
          'independent supervisor PASS-body-exit2 negatives; forbidden request/GT/old-flow sentinels; '
          'SDK/process mocks only; no corpus/native/network/science')


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--self-check']:
            self_check()
        else:
            cli(sys.argv[1:])
    except Exception as error:
        print('INVALID: '+str(error), file=sys.stderr)
        sys.exit(2)
