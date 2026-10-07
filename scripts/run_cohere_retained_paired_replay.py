#!/usr/bin/env python3
"""Run the retained paired Cohere replay serially: native retained publisher, then A1 B1 B2 A2.

CLI: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check
(the internal --owned-stage re-exec is served by run_cohere_native_preflight.py)

Thin local glue only. Every native call runs through the existing bounded systemd supervisor
(probe.native_stage / witness.owned_stage / local.run_stage) and the preflight closure check.
Python never opens the network and never parses ANN results or logs: the four result JSONL files and
every native log are pinned by stream SHA/length/EOF only. The native baseline owns the query seal and
ground-truth order; the root alone checks collected results and parity before any performance reading.
This helper makes no parity, quality, performance, S3 or vendor claim.
"""
import copy
import json
import os
from pathlib import Path
import re
import signal
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_cohere_native_preflight as base
from scripts import run_hierarchical_global_leaf_probe as probe
from scripts import run_source_witness_paired_coverage as witness

local, positive = probe.local, probe.positive
require, exact, fields = local.require, local.exact, local.fields
body = witness.body
SCHEMA = 'borsuk-cohere-retained-paired-replay-v1'
LEDGER_SCHEMA = 'borsuk-retained-cohere-replay-input-ledger-v1'
GIB = 1 << 30
ROLES = ('publisher', 'baseline_a', 'baseline_b')
CALLS = (('publish', 'publisher'), ('a1', 'baseline_a'), ('b1', 'baseline_b'), ('b2', 'baseline_b'), ('a2', 'baseline_a'))
ARMS = CALLS[1:]
ORDER = ['A1', 'B1', 'B2', 'A2']
# Gate rosters are terminated and all-zero: retained publisher 5 stages, original baseline 6, blocked scorer 7.
GATE_OF = dict(publisher='retained_publisher', baseline_a='original_baseline', baseline_b='blocked_scorer')
GATE_STAGES = dict(retained_publisher=5, original_baseline=6, blocked_scorer=7)
# Historical qualified source identities: A is the native-preflight admission + cohort/a0002 verification, B the exact-sq8/a0001
# terminal and root verification. The publisher gate is bound to the configured current source instead.
GATE_SOURCE = dict(original_baseline='d0e7029d484db02904505b7c83c57a13a3258ab9db804e54338ff3fca0f39676',
                   blocked_scorer='4ed25e42ab6caabb94c76e1c7a909cf82a31a15887f06e41a6801ddcda409e03')
RESOURCES = dict(base.QUERY, scratch_max_bytes=4 * GIB)  # CPU[0], 512MiB, noSwap; 4GiB covers inputs, binaries and outputs
CALL_MAX, DEADLINE_MAX = 300, 1800
RETAINED_PREFIX, DESTINATION_PREFIX = 'semantic/index', 'semantic/rebound'
MAX_SCRATCH = 64 << 20
CONFIG_SCHEMA = 'borsuk-two-bit-retained-local-publication-config-v1'
RECEIPT_SCHEMA = 'borsuk-two-bit-retained-local-publication-receipt-v1'
HEAD_PATH = 'store/semantic/index/head.json'
HEAD_SCHEMA = 'borsuk-two-bit-head-v2'
HEAD_KEYS = ('schema', 'epoch', 'generation', 'root_sha256', 'mutation', 'fence')  # the head's native key order, not the ledger's
ASSETS, ASSET_BYTES = 15, 551_480_739
REBASED = (('requests', 'path'), ('truth', 'path'), ('store_root',), ('scratch_parent',),
           ('generation_prefix',), ('generation_root_sha256',))
LEDGER_FIELDS = ('admission_pending body_authentication_pending bucket input_bytes_excluding_pending_publisher objects order '
                 'original_head_authenticated original_runner_config query_binaries retained_approval '
                 'runtime_rebound_root_pending schema status').split()
RELATIVE = re.compile(r'[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*')


def need(value, names, label):
    require(type(value) is dict and set(names.split() if type(names) is str else names) <= set(value), label + ' fields')


def walk(value, path):
    for part in path:
        value = value[part]
    return value


def neutral(config):
    """The authenticated original config with the six runtime-bound slots blanked."""
    result = copy.deepcopy(config)
    for path in REBASED:
        parent = walk(result, path[:-1])
        require(path[-1] in parent, 'rebased config slot')
        parent[path[-1]] = None
    return result


def check_ledger(ledger):
    """Authority only: the source ledger keeps both runtime notions pending; only actual output evidence closes them."""
    need(ledger, LEDGER_FIELDS, 'input ledger')
    exact(ledger['schema'], LEDGER_SCHEMA, 'ledger schema')
    exact(ledger['status'], 'PROSPECTIVE_NOT_LAUNCHED', 'ledger status')
    exact(ledger['body_authentication_pending'], True, 'ledger body authentication stays pending (never self-asserted)')
    exact(ledger['runtime_rebound_root_pending'], True, 'ledger rebound root stays pending (never self-asserted)')
    exact(ledger['order'], ORDER, 'ledger run order')
    objects = ledger['objects']
    require(type(objects) is list and len(objects) == ASSETS, 'exactly the fifteen retained assets')
    by_path = {}
    for item in objects:
        need(item, 'bytes path sha256', 'ledger asset')
        path = item['path']
        require(type(path) is str and RELATIVE.fullmatch(path) and not any(p in ('.', '..') for p in path.split('/')), 'asset relative path')
        require(path not in by_path, 'unique asset path')
        local.integer(item['bytes'], 1, GIB, 'asset bytes')
        local.digest(item['sha256'])
        by_path[path] = item
    exact(sum(item['bytes'] for item in objects), ASSET_BYTES, 'retained asset byte total')
    approval, head = ledger['retained_approval'], ledger['original_head_authenticated']
    need(approval, 'root_sha256 generation control_epoch sq8_object_key sq8_etag_runtime_pending', 'retained approval')
    exact(approval['sq8_etag_runtime_pending'], True, 'SQ8 ETag is derived at runtime only')
    local.digest(approval['root_sha256'])
    local.integer(approval['generation'], 1, 1 << 62, 'approval generation')
    local.integer(approval['control_epoch'], 1, 1 << 62, 'approval control epoch')
    need(head, 'schema epoch generation root_sha256 mutation fence', 'authenticated head')
    exact(head['schema'], HEAD_SCHEMA, 'head schema')
    exact(head['mutation'], None, 'head carries no mutation'); exact(head['fence'], None, 'head carries no fence')
    exact(head['root_sha256'], approval['root_sha256'], 'head root equals approval root')
    exact(head['generation'], approval['generation'], 'head generation equals approval generation')
    exact(head['epoch'], approval['control_epoch'], 'head epoch equals approval control epoch')
    # The 170-byte retained head is an ordinary asset: its exact ordered compact bytes are the pin, never recreated.
    head_body = json.dumps({key: head[key] for key in HEAD_KEYS}, separators=(',', ':')).encode()
    require(HEAD_PATH in by_path and by_path[HEAD_PATH]['bytes'] == len(head_body) and by_path[HEAD_PATH]['sha256'] == local.sha(head_body),
            'retained head asset equals the ordered compact authenticated head')
    sq8_key = approval['sq8_object_key']
    require(type(sq8_key) is str and re.fullmatch(r'semantic/objects/[0-9a-f]{64}', sq8_key), 'approval SQ8 object key')
    require('store/' + sq8_key in by_path and by_path['store/' + sq8_key]['sha256'] == sq8_key.rsplit('/', 1)[1], 'SQ8 object is a staged asset')
    manifest = 'store/%s/generations/%s/manifest.json' % (RETAINED_PREFIX, approval['root_sha256'])
    require(manifest in by_path and by_path[manifest]['sha256'] == approval['root_sha256'], 'root manifest is a staged asset')
    queries, truth = (by_path.get('prepared/' + name) for name in ('queries.f32', 'truth.u64'))
    require(queries is not None and truth is not None, 'prepared requests and truth assets')
    binaries = ledger['query_binaries']
    need(binaries, 'A B', 'ledger query binaries')
    for key in 'AB':
        need(binaries[key], 'bytes sha256', 'ledger query binary')
        local.integer(binaries[key]['bytes'], 1, 256 << 20, 'query binary bytes'); local.digest(binaries[key]['sha256'])
    exact(ledger['input_bytes_excluding_pending_publisher'], ASSET_BYTES + binaries['A']['bytes'] + binaries['B']['bytes'], 'ledger input byte total')
    original = ledger['original_runner_config']
    need(original, 'bytes sha256 config', 'original runner config')
    pinned = local.canonical(original['config'])
    require(len(pinned) == original['bytes'] and local.sha(pinned) == original['sha256'], 'original runner config bytes/SHA')
    config = original['config']
    for slot, asset in (('requests', queries), ('truth', truth)):
        exact(config[slot]['bytes'], asset['bytes'], 'original config ' + slot + ' bytes')
        exact(config[slot]['sha256'], asset['sha256'], 'original config ' + slot + ' SHA')
    exact(config['generation_root_sha256'], approval['root_sha256'], 'original config root')
    exact(config['native_source']['sq8_sha256'], sq8_key.rsplit('/', 1)[1], 'original config SQ8 identity')
    neutral(config)
    return SimpleNamespace(objects=objects, by_path=by_path, approval=approval, head_body=head_body, config=config,
                           binaries=binaries, sq8_path='store/' + sq8_key)


def qualify(config):
    fields(config, 'schema run_id source_identity_sha256 admission ledger work_root binaries publisher_limits phase_seconds deadline_seconds',
           'replay config')
    exact(config['schema'], SCHEMA + '-config', 'replay config schema')
    require(type(config['run_id']) is str and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', config['run_id']), 'run ID')
    local.digest(config['source_identity_sha256'])
    local.pointer(config['admission'], 64 << 10)
    local.pointer(config['ledger'], 1 << 20)
    require(type(config['work_root']) is str and Path(config['work_root']).is_absolute(), 'absolute work root')
    root = positive.regular_path(config['work_root'])
    require(root.is_dir(), 'work root directory')
    fields(config['binaries'], ' '.join(ROLES), 'three qualified binary roles')
    for role in ROLES:
        local.pointer(config['binaries'][role], 256 << 20)
        require(Path(config['binaries'][role]['path']).parent == root / 'bin', 'binary inside work_root/bin: ' + role)
    require(len({config['binaries'][r]['path'] for r in ROLES}) == len(ROLES), 'distinct binary paths')
    fields(config['publisher_limits'], ' '.join(base.PUBLISHER_LIMITS), 'explicit publisher limits')
    for value in config['publisher_limits'].values():
        local.integer(value, 0, 1 << 62, 'explicit native limit')
    require(config['publisher_limits']['max_memory_bytes'] <= RESOURCES['memory_max_bytes'], 'publisher memory within the 512MiB cgroup')
    fields(config['phase_seconds'], ' '.join(name for name, _ in CALLS), 'per-call deadlines')
    for value in config['phase_seconds'].values():
        local.integer(value, 6, CALL_MAX, 'call seconds within the 300s maximum')
    local.integer(config['deadline_seconds'], 6, DEADLINE_MAX, 'whole-run deadline within 1800s')
    return root


def admit(config, ledger):
    """Pinned root admission: permission and binary qualification only; it carries no runtime body/root claim."""
    admission = local.read_json(config['admission'], 64 << 10)
    fields(admission, 'schema status source_identity_sha256 gates roles', 'replay admission')
    exact(admission['schema'], SCHEMA + '-admission', 'admission schema')
    exact(admission['status'], 'READY_PAIRED_RETAINED_REPLAY', 'admission ready status')
    exact(admission['source_identity_sha256'], config['source_identity_sha256'], 'admission binds the current source identity')
    fields(admission['gates'], ' '.join(GATE_STAGES), 'admitted gates')
    for name, gate in admission['gates'].items():
        fields(gate, 'session instance_id source_identity_sha256 stage_count stage_exit_statuses terminated receipt', 'admitted gate ' + name)
        local.integer(gate['session'], 1, 1 << 40, 'gate session')
        require(type(gate['instance_id']) is str and re.fullmatch(r'[A-Za-z0-9-]{1,64}', gate['instance_id']), 'gate instance identity')
        local.digest(gate['source_identity_sha256'])
        if name == 'retained_publisher':  # the current-source publisher gate is bound to the configured source
            exact(gate['source_identity_sha256'], config['source_identity_sha256'], 'retained publisher gate source identity equals the configured current source')
        else:  # each historical gate keeps its own distinct qualified identity (never bound to the current source)
            exact(gate['source_identity_sha256'], GATE_SOURCE[name], 'historical gate source identity: ' + name)
        exact(gate['stage_count'], GATE_STAGES[name], 'complete gate stage roster: ' + name)
        require(type(gate['stage_exit_statuses']) is list and len(gate['stage_exit_statuses']) == GATE_STAGES[name]
                and all(type(code) is int and code == 0 for code in gate['stage_exit_statuses']), 'all admitted gate stages exit 0: ' + name)
        exact(gate['terminated'], True, 'original gate instance terminated: ' + name)
        local.authenticate(gate['receipt'], 1 << 20)
    fields(admission['roles'], ' '.join(ROLES), 'admitted binary roles')
    digests = set()
    for role in ROLES:
        entry = admission['roles'][role]
        fields(entry, 'gate binary', 'admitted role ' + role)
        exact(entry['gate'], GATE_OF[role], 'role/gate binding: ' + role)
        fields(entry['binary'], 'path bytes sha256 local_basename', 'admitted binary Artifact ' + role)
        require(type(entry['binary']['path']) is str and Path(entry['binary']['path']).is_absolute(), 'admitted original path')
        local.integer(entry['binary']['bytes'], 1, 256 << 20, 'admitted binary bytes'); local.digest(entry['binary']['sha256'])
        configured = config['binaries'][role]
        exact(body(configured), body(entry['binary']), 'configured binary bytes/SHA equal admitted role: ' + role)
        exact(Path(configured['path']).name, entry['binary']['local_basename'], 'root-chosen local basename for role: ' + role)
        digests.add(entry['binary']['sha256'])
    require(len(digests) == len(ROLES), 'distinct admitted binary roles')
    for role, key in (('baseline_a', 'A'), ('baseline_b', 'B')):
        exact(body(config['binaries'][role]), body(ledger.binaries[key]), 'ledger query binary ' + key + ' equals ' + role)
    return admission


def logs_within_cap(stages):
    """The shared exit classifier ignores the log cap: promote only when both retained logs fit it."""
    record = stages[-1]
    return all(type(record.get(key)) is dict and type(record[key].get('bytes')) is int and record[key]['bytes'] <= RESOURCES['max_log_bytes']
               for key in ('native_log', 'log'))


def tree(folder):
    """Sorted relative paths of regular files; links, devices and the like are protocol violations."""
    names = []
    for path in sorted(Path(folder).rglob('*')):
        require(not path.is_symlink(), 'no symlink in a staged tree')
        if path.is_dir():
            continue
        require(path.is_file(), 'regular staged file')
        names.append(path.relative_to(folder).as_posix())
    return names


def stamp(folder, names, links):
    result = {}
    for name in names:
        info = os.stat(Path(folder) / name, follow_symlinks=False)
        result[name] = (info.st_ino, info.st_size, info.st_mtime_ns) + ((info.st_ctime_ns, info.st_nlink) if links else ())
    return result


def identities(folder, names):
    return {name: body(local.identity(Path(folder) / name)) for name in names}


def execute(config_path, config_sha, output):
    output = positive.regular_path(output)
    require(not output.is_relative_to(probe.ORIGINAL_ROOT), 'new output outside shared legacy helper scratch')
    output.mkdir(parents=False, exist_ok=False)  # Never mutate or resume an old attempt.
    receipt = dict(schema=SCHEMA + '-receipt', output=str(output), status='INVALID', complete=False, stages=[], calls=[],
                   results={}, sampled_peak_scratch_bytes=0,
                   claims=dict(parity=False, quality=False, performance=False, s3=False),
                   page_cache='shared and uncontrolled; local-file replay')
    started = time.monotonic()
    previous_term = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('runner terminated')))
    try:
        config, pin = local.load_config(config_path, config_sha)
        root = qualify(config)
        require(output == root / 'output', 'new output is work_root/output')
        ledger = check_ledger(local.read_json(config['ledger'], 1 << 20))
        admission = admit(config, ledger)
        deadline = started + config['deadline_seconds']
        receipt['config'] = witness.retain(pin, output / 'config.json')
        receipt['admission'] = witness.retain(config['admission'], output / 'admission.json')
        receipt['ledger'] = witness.retain(config['ledger'], output / 'ledger.json')
        receipt['source_identity_sha256'] = config['source_identity_sha256']
        receipt['gates'] = {name: {k: gate[k] for k in ('session', 'instance_id', 'source_identity_sha256', 'receipt')}
                            for name, gate in admission['gates'].items()}
        binaries = receipt['binaries'] = config['binaries']
        inputs, scratch = root / 'input', output / 'scratch'
        for folder in ('configs', 'scratch'):
            (output / folder).mkdir()

        def check():
            used = local.directory_bytes(root)
            receipt['sampled_peak_scratch_bytes'] = max(receipt['sampled_peak_scratch_bytes'], used)
            require(used <= RESOURCES['scratch_max_bytes'], 'outer 4Gi scratch bound')
            require(time.monotonic() < deadline, 'outer run deadline')

        def call(name, role, argv, config_body):
            cfg_pin = probe.copy_bytes(output / 'configs' / (name + '.json'), config_body)
            binary = binaries[role]
            command = [binary['path'], *argv]
            budget = config['phase_seconds'][name]
            limits = dict(RESOURCES, timeout_seconds=budget)
            with patch.object(probe, '__file__', str(Path(base.__file__).resolve())):  # re-exec through the preflight --owned-stage entry
                record = probe.native_stage(name, command, binary, cfg_pin, output, limits, budget, deadline, receipt['stages'], check)
            base.closure(record, name, command, RESOURCES, budget)
            receipt['calls'].append(dict(name=name, role=role, command=command, config=cfg_pin,
                closure=local.identity(output / (name + '-closure.json')), log=record['native_log']))
            check()
            return record

        def empty_scratch():
            require(not any(scratch.iterdir()), 'native scratch empty between calls')

        # 1. Admission and body authentication happen before ANY native call.
        names = sorted(ledger.by_path)
        require(tree(inputs) == names, 'staged input roster is exactly the fifteen ledger assets')
        require(tree(root / 'bin') == sorted(Path(binaries[r]['path']).name for r in ROLES), 'staged binary roster is exactly the three roles')
        for item in ledger.objects:
            local.authenticate(dict(path=str(inputs / item['path']), bytes=item['bytes'], sha256=item['sha256']), GIB)
        for role in ROLES:
            local.authenticate(binaries[role], 256 << 20)
            require(os.access(binaries[role]['path'], os.X_OK), 'qualified binary executable: ' + role)
        exact(local.authenticate(dict(path=str(inputs / HEAD_PATH), **body(ledger.by_path[HEAD_PATH])), 4096, read=True), ledger.head_body,
              'staged head equals the authenticated head metadata')
        receipt['body_authentication'] = dict(complete=False, objects=[dict(path=i['path'], bytes=i['bytes'], sha256=i['sha256']) for i in ledger.objects],
                                              binaries={r: body(binaries[r]) for r in ROLES}, reauthenticated_after_chain=False)
        empty_scratch(); check()
        # 2. The runtime SQ8 ETag is derived only after complete object authentication.
        etag = base.local_etag(inputs / ledger.sq8_path)
        approval = ledger.approval
        before = stamp(inputs, names, False)

        # 3. One retained publication into the sibling destination prefix.
        publish_config = local.canonical(dict(schema=CONFIG_SCHEMA, store_root=str(inputs / 'store'), retained_prefix=RETAINED_PREFIX,
            original_root_sha256=approval['root_sha256'], original_generation=approval['generation'],
            original_control_epoch=approval['control_epoch'], sq8_object_key=approval['sq8_object_key'], sq8_etag=etag,
            destination_prefix=DESTINATION_PREFIX, scratch_parent=str(scratch), max_scratch_bytes=MAX_SCRATCH,
            limits=config['publisher_limits']))
        require(len(publish_config) <= 65536, 'native retained publisher config cap')
        publish_sha = local.sha(publish_config)
        published = output / 'publish-receipt.json'
        call('publish', 'publisher', ['--retained', str(output / 'configs/publish.json'), publish_sha, str(published)], publish_config)
        empty_scratch()
        native = local.read_json(local.identity(published), 64 << 10)
        fields(native, 'schema config_sha256 store_root retained_prefix original_root_sha256 original_generation original_control_epoch '
               'sq8_object_key sq8_etag destination_prefix head_key metadata_prefix root_sha256 generation control_epoch '
               'local_file_only performance_claim', 'native retained publication receipt')
        for key, expected in dict(schema=RECEIPT_SCHEMA, config_sha256=publish_sha, store_root=str(inputs / 'store'), retained_prefix=RETAINED_PREFIX,
                original_root_sha256=approval['root_sha256'], original_generation=approval['generation'],
                original_control_epoch=approval['control_epoch'], sq8_object_key=approval['sq8_object_key'], sq8_etag=etag,
                destination_prefix=DESTINATION_PREFIX, head_key=DESTINATION_PREFIX + '/head.json', generation=approval['generation'],
                control_epoch=approval['control_epoch'], local_file_only=True, performance_claim=False).items():
            exact(native[key], expected, 'native retained publication receipt: ' + key)
        prefix, new_root = native['metadata_prefix'], native['root_sha256']
        require(type(prefix) is str and prefix.startswith(DESTINATION_PREFIX + '/') and RELATIVE.fullmatch(prefix), 'rebound metadata prefix')
        local.digest(new_root)
        require(new_root != approval['root_sha256'], 'a rebound root differs from the original root')
        exact(local.identity(inputs / 'store' / prefix / 'manifest.json')['sha256'], new_root, 'rebound root identity')
        new_head = local.read_json(local.identity(inputs / 'store' / native['head_key']), 4096)
        need(new_head, 'root_sha256 generation epoch', 'rebound head')
        for key, expected in dict(root_sha256=new_root, generation=approval['generation'], epoch=approval['control_epoch']).items():
            exact(new_head[key], expected, 'rebound head: ' + key)
        rebound = inputs / 'store' / DESTINATION_PREFIX
        rebound_names = tree(rebound)
        tracked = sorted([*names, *('store/%s/%s' % (DESTINATION_PREFIX, n) for n in rebound_names)])
        require(tree(inputs) == tracked, 'publication added only the rebound prefix')
        require({n: v[:3] for n, v in stamp(inputs, names, True).items()} == before, 'original assets unchanged by publication')
        rebound_pins = identities(rebound, rebound_names)
        fingerprint = stamp(inputs, tracked, True)
        receipt['runtime_rebound_root'] = dict(root_sha256=new_root, metadata_prefix=prefix, head_key=native['head_key'],
            receipt=local.identity(published), roster=rebound_pins)

        def unchanged():
            require(tree(inputs) == tracked and stamp(inputs, tracked, True) == fingerprint, 'original and rebound assets unchanged')
            for earlier in receipt['results'].values():
                local.authenticate(earlier, 64 << 20)  # an arm never rewrites another arm's pinned result
            empty_scratch()

        # 4. Four serial arms on byte-identical query configs over the one rebound root. Results stay opaque.
        cfg = copy.deepcopy(ledger.config)
        cfg['requests']['path'], cfg['truth']['path'] = str(inputs / 'prepared/queries.f32'), str(inputs / 'prepared/truth.u64')
        cfg['store_root'], cfg['scratch_parent'] = str(inputs / 'store'), str(scratch)
        cfg['generation_prefix'], cfg['generation_root_sha256'] = prefix, new_root
        exact(neutral(cfg), neutral(ledger.config), 'baseline config differs from the authenticated original only in the six runtime slots')
        baseline_config = local.canonical(cfg)
        baseline_sha = local.sha(baseline_config)
        receipt['baseline_config_sha256'] = baseline_sha
        for name, role in ARMS:
            result = output / (name + '-result.jsonl')
            try:
                call(name, role, [str(output / 'configs' / (name + '.json')), baseline_sha, str(result)], baseline_config)
            except BaseException as error:
                code = base.nonzero_native_exit(receipt['stages'])
                if code is None:
                    raise
                # An oversized log is a resource failure even when the native exit survived the supervisor's cleanup.
                require(logs_within_cap(receipt['stages']), 'retained native and unit logs within the supervisor log cap')
                # Clean closed unit, plain native exit within the log cap: recorded verbatim, never interpreted; later arms never start.
                receipt.update(status='BASELINE_NONZERO_EXIT', complete=True, native_exit_status=code, failed_arm=name,
                               error=str(error), error_type=type(error).__name__)
                if result.exists():
                    receipt['results'][name] = local.identity(result)
                break
            receipt['results'][name] = local.identity(result)  # opaque bytes: SHA/length/EOF only
            unchanged()
        else:
            receipt['status'] = 'NATIVE_CHAIN_CLOSED'
        require(all(s['closed'] and s['unit_drained'] and s['cgroup_drained'] for s in receipt['stages']), 'all native units drained')
        # 5. Output evidence: stream-authenticate everything again after the chain.
        for item in ledger.objects:
            local.authenticate(dict(path=str(inputs / item['path']), bytes=item['bytes'], sha256=item['sha256']), GIB)
        for role in ROLES:
            local.authenticate(binaries[role], 256 << 20)
        exact(identities(rebound, rebound_names), rebound_pins, 'rebound roster unchanged after the chain')
        for name, pin_ in receipt['results'].items():
            local.authenticate(pin_, 64 << 20)
        unchanged()
        local.authenticate(receipt['config'], local.CONFIG_CAP)
        receipt['body_authentication'].update(complete=True, reauthenticated_after_chain=True)
        check()
        receipt['complete'] = True
    except BaseException as error:
        receipt.update(status='INVALID', complete=False, error=str(error), error_type=type(error).__name__)
    finally:
        signal.signal(signal.SIGTERM, previous_term)
    receipt.update(wall_seconds=time.monotonic() - started,
        cleanup=dict(native_units_drained=bool(receipt['stages']) and all(
            s.get('unit_drained', False) and s.get('cgroup_drained', False) for s in receipt['stages']),
            native_processes_concurrent_max=1 if receipt['stages'] else 0, partial_artifacts_preserved=True))
    with patch.object(base, 'SCHEMA', SCHEMA):
        return base.finish(output, receipt)


FIXTURE = r'''import hashlib, json, os, re, sys
from pathlib import Path
S = os.environ.get('REPLAY_FIXTURE', '')
role = Path(sys.argv[0]).name
a = sys.argv[1:]
h = lambda b: hashlib.sha256(b).hexdigest()
def emit(text): sys.stdout.write(text); sys.stdout.flush()
def fail(code): sys.exit(code)
def data(n, salt): return bytes((i * 7 + salt) % 251 for i in range(n))
def arm(): return Path(a[2]).name.removesuffix('-result.jsonl') if role != 'publisher' else 'publish'
if S == 'exit2:' + arm(): fail(2)
if S == 'edgeexit2:' + arm():  # exactly the cap: still a plain, classifiable exit
    sys.stdout.write('x' * (16 << 20)); sys.stdout.flush(); fail(2)
if S == 'bigexit2:' + arm():
    sys.stdout.write('x' * ((16 << 20) + 1)); sys.stdout.flush(); fail(2)
if S == 'exit3:publish' and role == 'publisher': fail(3)
if S == 'sleep:publish' and role == 'publisher':
    import time; time.sleep(5)
if role == 'publisher':
    assert a[0] == '--retained'
    raw = Path(a[1]).read_bytes(); assert h(raw) == a[2]
    cfg = json.loads(raw)
    assert set(cfg) == set('schema store_root retained_prefix original_root_sha256 original_generation original_control_epoch sq8_object_key sq8_etag destination_prefix scratch_parent max_scratch_bytes limits'.split())
    assert cfg['schema'] == 'borsuk-two-bit-retained-local-publication-config-v1' and cfg['max_scratch_bytes'] == 64 << 20
    assert (cfg['retained_prefix'], cfg['destination_prefix']) == ('semantic/index', 'semantic/rebound')
    assert set(cfg['limits']) == set('max_memory_bytes max_active_queries max_query_bytes max_query_gets max_parallel_gets max_source_bytes max_source_gets max_parallel_source_gets max_query_scratch_bytes already_pinned_bytes'.split())
    store, old_root = Path(cfg['store_root']), cfg['original_root_sha256']
    assert os.listdir(cfg['scratch_parent']) == []
    st = os.stat(store / cfg['sq8_object_key'])
    assert cfg['sq8_etag'] == '"%x-%x-%x"' % (st.st_ino, st.st_mtime_ns // 1000, st.st_size), 'etag formula'
    head = json.loads((store / 'semantic/index/head.json').read_bytes()); assert head['root_sha256'] == old_root
    dest = store / 'semantic/rebound'; assert not dest.exists()
    manifest = json.dumps(dict(fixture='rebound', etag=cfg['sq8_etag'])).encode(); new_root = h(manifest)
    old_gen = store / 'semantic/index/generations' / old_root
    gen = dest / 'generations' / new_root
    for rel in ('page_digests.bin', 'page_manifest.json', 'plane/manifest.json', 'plane/mean.bin', 'plane/page_digests.bin', 'router/membership.bin', 'router/root.bin'):
        (gen / rel).parent.mkdir(parents=True, exist_ok=True); (gen / rel).write_bytes((old_gen / rel).read_bytes())
    for rel in ('plane/records.bin', 'router/leaves.bin'):  # create-only LocalFileSystem copies are hard links
        (gen / rel).parent.mkdir(parents=True, exist_ok=True); os.link(old_gen / rel, gen / rel)
    (gen / 'manifest.json').write_bytes(manifest)
    (dest / 'head.json').write_bytes(json.dumps(dict(schema='borsuk-two-bit-head-v2', epoch=1, generation=1, root_sha256=h(manifest) if S != 'pub-head-root' else '0' * 64, mutation=None, fence=None), separators=(',', ':')).encode())
    receipt = dict(schema='borsuk-two-bit-retained-local-publication-receipt-v1', config_sha256=a[2], store_root=cfg['store_root'],
        retained_prefix=cfg['retained_prefix'], original_root_sha256=old_root, original_generation=cfg['original_generation'],
        original_control_epoch=cfg['original_control_epoch'], sq8_object_key=cfg['sq8_object_key'], sq8_etag=cfg['sq8_etag'],
        destination_prefix='semantic/rebound', head_key='semantic/rebound/head.json', metadata_prefix='semantic/rebound/generations/' + new_root,
        root_sha256=new_root, generation=1, control_epoch=1, local_file_only=True, performance_claim=False)
    if S == 'pub-same-root': receipt['root_sha256'] = old_root
    if S == 'pub-wrong-root': receipt['root_sha256'] = '1' * 64
    if S == 'pub-head-key': receipt['head_key'] = 'semantic/index/head.json'
    if S == 'pub-local-false': receipt['local_file_only'] = False
    if S == 'pub-perf-claim': receipt['performance_claim'] = True
    if S == 'pub-etag': receipt['sq8_etag'] = '"0-0-0"'
    if S == 'pub-extra': receipt['extra'] = 1
    if S == 'pub-mutate-original': os.utime(old_gen / 'plane/mean.bin', (1, 1))
    with open(a[3], 'x') as f: json.dump(receipt, f)
else:
    cfg_bytes = Path(a[0]).read_bytes(); assert h(cfg_bytes) == a[1]
    cfg = json.loads(cfg_bytes)
    store = Path(cfg['store_root'])
    assert cfg['generation_prefix'].startswith('semantic/rebound/generations/'), cfg['generation_prefix']
    assert h((store / cfg['generation_prefix'] / 'manifest.json').read_bytes()) == cfg['generation_root_sha256']
    assert os.listdir(cfg['scratch_parent']) == [] and Path(cfg['requests']['path']).is_file() and Path(cfg['truth']['path']).is_file()
    target = arm()
    if S == 'scratch:' + target: Path(cfg['scratch_parent'], 'leftover').write_bytes(b'x')
    if S == 'stray:' + target: Path(store, 'stray.bin').write_bytes(b'x')
    if S == 'mutate-rebound:' + target: Path(store, 'semantic/rebound/head.json').write_bytes(b'{}')
    if S == 'touch-original:' + target: os.utime(cfg['requests']['path'], (1, 1))
    if S == 'tamper-result:' + target: Path(a[2]).with_name('a1-result.jsonl').write_bytes(b'tampered after pinning\n')
    if S != 'missing-result:' + target:
        # Arm A and arm B emit DIFFERENT opaque bytes (stand-ins for timing/trace); the helper must not care.
        Path(a[2]).write_bytes(('{"phase":"opaque","role":"%s","arm":"%s","tail":"%s"}\n' % (role, target, 'x' * (7 if role == 'baseline_a' else 11))).encode())
    emit('{"status":"MEASURED","fixture":"%s"}\n' % role)
'''


def self_check():
    """Real orchestration and shared supervisor/run_stage; only systemd and cgroup files are mocked."""
    import hashlib
    import subprocess as subprocess_module
    from contextlib import ExitStack
    module = sys.modules[__name__]
    h = lambda b: hashlib.sha256(b).hexdigest()
    data = lambda n, salt: bytes((i * 7 + salt) % 251 for i in range(n))
    limits_ok = dict(max_memory_bytes=1 << 29, max_active_queries=1, max_query_bytes=16773120, max_query_gets=32, max_parallel_gets=16,
                     max_source_bytes=64 << 20, max_source_gets=128, max_parallel_source_gets=16, max_query_scratch_bytes=400000, already_pinned_bytes=0)
    with tempfile.TemporaryDirectory(prefix='cohere-replay-glue-') as folder, ExitStack() as stack:
        top = Path(folder)
        stack.enter_context(patch.dict(os.environ, dict(BORSUK_GLOBAL_LEAF_SLICE='borsuk-global-leaf-synthetic.slice', REPLAY_FIXTURE='', REPLAY_BIG_UNIT='')))
        stack.enter_context(patch.object(module, 'ASSET_BYTES', 0))  # the synthetic fifteen assets are tiny; build() sets their total
        group = {'path': '/synthetic', 'memory.peak': '1048576', 'memory.swap.max': '0', 'memory.swap.peak': '0',
                 'memory.events': 'oom 0\noom_kill 0\noom_group_kill 0', 'pids.max': '512', 'cgroup.procs': str(os.getpid()),
                 'cpu.stat': 'usage_usec 0'}
        stack.enter_context(patch.object(local, 'resource_snapshot', lambda limits: dict(group,
            **{'memory.max': str(limits['memory_max_bytes']), 'cpu_affinity': limits['cpu_affinity']})))
        stack.enter_context(patch.object(probe, 'cgroup_snapshot', lambda _p, memory, cpu, tasks=512: dict(group,
            **{'memory.max': str(memory), 'cpu.max': '%d 100000' % (cpu * 1000), 'cpu_affinity': []})))
        supervisors, drain = [], {'bad': False}

        def fake_popen(command, **kwargs):
            assert command[0] == 'systemd-run'
            supervisors.append(command)
            at = command.index('--owned-stage')
            _, spec = local.load_config(command[at + 1], command[at + 2])
            if os.environ.get('REPLAY_BIG_UNIT') == Path(command[at + 1]).name.removesuffix('-stage.json'):
                kwargs['stdout'].write(b'x' * ((16 << 20) + 1)); kwargs['stdout'].flush()  # an oversized supervisor (unit) log
            try:
                witness.owned_stage(spec, command[at + 3]); code = 0
            except BaseException:
                code = 1
            return SimpleNamespace(poll=lambda: code, wait=lambda timeout=None: code, pid=os.getpid())

        def fake_run(command, **kwargs):
            if command[1] in ('stop', 'kill'):
                return subprocess_module.CompletedProcess(command, 0)
            bad = drain['bad']
            return subprocess_module.CompletedProcess(command, 0, stdout='ActiveState=' + ('active' if bad else 'inactive') +
                '\nSubState=dead\nMainPID=' + ('1' if bad else '0') + '\nControlGroup=\nLoadState=loaded\n')
        stack.enter_context(patch.object(probe, 'subprocess', SimpleNamespace(Popen=fake_popen, run=fake_run, DEVNULL=subprocess_module.DEVNULL,
            STDOUT=subprocess_module.STDOUT, TimeoutExpired=subprocess_module.TimeoutExpired)))
        spy = []
        real_native_stage = probe.native_stage
        stack.enter_context(patch.object(probe, 'native_stage', lambda *args, **kwargs: (spy.append(args[0]), real_native_stage(*args, **kwargs))[1]))
        read_json_paths = []
        real_read_json = local.read_json
        stack.enter_context(patch.object(local, 'read_json', lambda value, cap=65536: (read_json_paths.append(value['path']), real_read_json(value, cap))[1]))

        def build(label, ledger_edit=None, adm_edit=None, cfg_edit=None, stage_edit=None):
            work = top / label
            (work / 'input').mkdir(parents=True); (work / 'bin').mkdir()
            bins = {}
            for role in ROLES:
                path = work / 'bin' / role
                probe.copy_bytes(path, ('#!' + sys.executable + '\n' + FIXTURE + '\n# role ' + role + '\n').encode()); path.chmod(0o700)
                bins[role] = local.identity(path)
            orig_manifest = b'{"fixture":"original-root"}'
            root = h(orig_manifest); sq8, canon = data(64, 20), data(48, 21)
            head = dict(schema=HEAD_SCHEMA, epoch=1, generation=1, root_sha256=root, mutation=None, fence=None)
            head_body = json.dumps({key: head[key] for key in HEAD_KEYS}, separators=(',', ':')).encode()
            gen = 'store/semantic/index/generations/%s/' % root
            files = {'prepared/queries.f32': data(32, 1), 'prepared/truth.u64': data(16, 2), gen + 'manifest.json': orig_manifest,
                gen + 'page_digests.bin': data(24, 3), gen + 'page_manifest.json': data(20, 4), gen + 'plane/manifest.json': data(22, 5),
                gen + 'plane/mean.bin': data(16, 6), gen + 'plane/page_digests.bin': data(28, 7), gen + 'plane/records.bin': data(64, 8),
                gen + 'router/leaves.bin': data(48, 9), gen + 'router/membership.bin': data(18, 10), gen + 'router/root.bin': data(30, 11),
                HEAD_PATH: head_body, 'store/semantic/objects/' + h(sq8): sq8, 'store/semantic/objects/' + h(canon): canon}
            module.ASSET_BYTES = sum(len(payload) for payload in files.values())
            for name, payload in files.items():
                probe.copy_bytes(work / 'input' / name, payload)
            orig = dict(schema='borsuk-cohere-native-baseline-config-v1', dataset='fixture', count=2, k=3, rows=6, dimensions=4,
                max_memory_bytes=536870912, generation_prefix='semantic/index/generations/' + root, generation_root_sha256=root,
                native_source=dict(source_sha256='1' * 64, sq8_sha256=h(sq8), source_order_sha256='2' * 64),
                requests=dict(bytes=32, path='/mnt/x/prepared/queries.f32', sha256=h(files['prepared/queries.f32'])),
                truth=dict(bytes=16, path='/mnt/x/prepared/truth.u64', sha256=h(files['prepared/truth.u64'])),
                scratch_parent='/mnt/x/scratch', store_root='/mnt/x/store')
            canon_cfg = local.canonical(orig)
            ledger = dict(schema=LEDGER_SCHEMA, status='PROSPECTIVE_NOT_LAUNCHED', bucket='b', admission_pending=True,
                body_authentication_pending=True, runtime_rebound_root_pending=True, order=list(ORDER),
                objects=[dict(bytes=len(p), key='k/' + n, path=n, sha256=h(p), head_bytes_verified=True) for n, p in files.items()],
                query_binaries=dict(A=body(bins['baseline_a']), B=body(bins['baseline_b'])),
                input_bytes_excluding_pending_publisher=sum(len(p) for p in files.values()) + bins['baseline_a']['bytes'] + bins['baseline_b']['bytes'],
                original_head_authenticated=head, retained_approval=dict(root_sha256=root, generation=1, control_epoch=1,
                    sq8_object_key='semantic/objects/' + h(sq8), sq8_etag_runtime_pending=True),
                original_runner_config=dict(bytes=len(canon_cfg), sha256=h(canon_cfg), key='k', config=orig,
                    authenticated_against_original_terminal=True))
            if ledger_edit: ledger_edit(ledger)
            gates = {}
            for name, count in GATE_STAGES.items():
                gates[name] = dict(session=80402, instance_id='i-' + name.replace('_', '-'), stage_count=count,
                    source_identity_sha256={'retained_publisher': 'a' * 64, **GATE_SOURCE}[name],
                    stage_exit_statuses=[0] * count, terminated=True, receipt=local.write_json(work / ('gate-' + name + '.json'), dict(gate=name)))
            admission = dict(schema=SCHEMA + '-admission', status='READY_PAIRED_RETAINED_REPLAY', source_identity_sha256='a' * 64, gates=gates,
                roles={r: dict(gate=GATE_OF[r], binary=dict(path='/original/' + r + '-built', bytes=bins[r]['bytes'], sha256=bins[r]['sha256'],
                    local_basename=r)) for r in ROLES})
            if adm_edit: adm_edit(admission)
            cfg = dict(schema=SCHEMA + '-config', run_id='synthetic-a0001', source_identity_sha256='a' * 64,
                admission=local.write_json(work / 'admission-in.json', admission), ledger=local.write_json(work / 'ledger-in.json', ledger),
                work_root=str(work), binaries=bins, publisher_limits=dict(limits_ok),
                phase_seconds=dict(publish=60, a1=60, b1=60, b2=60, a2=60), deadline_seconds=600)
            if cfg_edit: cfg_edit(cfg)
            if stage_edit: stage_edit(work, files)
            return work, local.write_json(work / 'config-in.json', cfg)

        # Every refusal is pinned to its intended reason so a case cannot pass by failing for an incidental one.
        reasons = {'adm-pending': 'admission ready status', 'adm-failed': 'admission ready status',
            'adm-source': 'admission binds the current source identity', 'adm-stages': 'complete gate stage roster: retained_publisher',
            'adm-stages-baseline': 'complete gate stage roster: original_baseline', 'adm-stages-blocked': 'complete gate stage roster: blocked_scorer',
            'adm-exit': 'all admitted gate stages exit 0', 'adm-exit-bool': 'all admitted gate stages exit 0',
            'adm-alive': 'original gate instance terminated', 'adm-gate-sha': 'artifact authentication', 'adm-gate-source': 'invalid SHA256',
            'adm-publisher-source': 'retained publisher gate source identity equals the configured current source',
            'adm-baseline-source': 'historical gate source identity: original_baseline', 'adm-blocked-source': 'historical gate source identity: blocked_scorer',
            'adm-role-gate': 'role/gate binding', 'adm-role-sha': 'configured binary bytes/SHA equal admitted role',
            'adm-basename': 'root-chosen local basename', 'adm-role-swap': 'configured binary bytes/SHA equal admitted role',
            'adm-unknown': 'replay admission fields', 'adm-missing-role': 'admitted binary roles fields', 'cfg-unknown': 'replay config fields',
            'cfg-limit-bool': 'invalid explicit native limit', 'cfg-limit-memory': 'publisher memory within the 512MiB cgroup',
            'cfg-phase-high': 'invalid call seconds', 'cfg-phase-low': 'invalid call seconds', 'cfg-deadline': 'invalid whole-run deadline',
            'cfg-work-root': 'absolute work root', 'cfg-binary-sha': 'configured binary bytes/SHA equal admitted role',
            'cfg-binary-path': 'distinct binary paths', 'ledger-sha': 'artifact authentication',
            'ledger-body-flag': 'ledger body authentication stays pending', 'ledger-root-flag': 'ledger rebound root stays pending',
            'ledger-status': 'ledger status', 'ledger-order': 'ledger run order', 'ledger-count': 'exactly the fifteen retained assets',
            'ledger-dup': 'unique asset path', 'ledger-bytes': 'ledger input byte total', 'ledger-etag-resolved': 'SQ8 ETag is derived at runtime only',
            'ledger-head-gen': 'head generation equals approval generation', 'ledger-head-asset': 'retained head asset equals',
            'ledger-head-root': 'head root equals approval root', 'ledger-config-drift': 'original runner config bytes/SHA',
            'ledger-binary-a': 'ledger query binary A equals baseline_a', 'stage-flip-truth': 'artifact authentication',
            'stage-flip-object': 'artifact authentication', 'stage-stray': 'staged input roster', 'stage-missing': 'staged input roster',
            'stage-extra-bin': 'staged binary roster', 'pub-exit3': 'owned native unit exit0', 'pub-same-root': 'a rebound root differs',
            'pub-wrong-root': 'rebound root identity', 'pub-head-key': 'receipt: head_key', 'pub-local-false': 'receipt: local_file_only',
            'pub-perf-claim': 'receipt: performance_claim', 'pub-etag': 'receipt: sq8_etag', 'pub-extra': 'native retained publication receipt fields',
            'pub-head-root': 'rebound head: root_sha256', 'pub-mutate-original': 'original assets unchanged by publication',
            'missing-result': 'b1-result.jsonl', 'scratch-left': 'native scratch empty between calls',
            'stray-store': 'original and rebound assets unchanged', 'mutate-rebound': 'original and rebound assets unchanged',
            'touch-original': 'original and rebound assets unchanged', 'bigexit2-a1': 'retained native and unit logs within the supervisor log cap',
            'bigexit2-b2': 'retained native and unit logs within the supervisor log cap',
            'bigunit-b1': 'retained native and unit logs within the supervisor log cap',
            'edgeexit2-b2': 'owned native unit exit0', 'tamper-result': 'artifact authentication', 'drain': 'owned native unit closure', 'deadline': 'owned native unit exit0',
            'scratch-cap': 'outer 4Gi scratch bound', **{'nonzero-' + arm: 'owned native unit exit0' for arm in ('a1', 'b1', 'b2', 'a2')}}

        def attempt(label, scenario='', expected='INVALID', count=None, spawned=None, bad_drain=False, big_unit='', **edits):
            os.environ['REPLAY_FIXTURE'] = scenario; os.environ['REPLAY_BIG_UNIT'] = big_unit
            drain['bad'] = bad_drain; supervisors.clear(); spy.clear()
            work, pin = build(label, **edits)
            terminal = execute(pin['path'], pin['sha256'], work / 'output')
            receipt = local.read_json(local.identity(work / 'output/execution-receipt.json'), 8 << 20)
            assert terminal['status'] == expected, (label, receipt.get('error'), terminal['status'])
            assert terminal['complete'] == (expected != 'INVALID') and terminal['execution_exit_code'] == (0 if expected == 'NATIVE_CHAIN_CLOSED' else 2)
            assert terminal['inventory'] == base.inventory(work / 'output') and 'terminal.json' not in terminal['inventory']
            assert (label in reasons) == (receipt.get('error') is not None), (label, receipt.get('error'))
            assert label not in reasons or reasons[label] in receipt['error'], (label, receipt['error'])
            names = [name for name, _ in CALLS]
            if count is not None:
                assert spy == names[:count], (label, spy)
            if spawned is not None or count is not None:
                assert len(supervisors) == (count if spawned is None else spawned), (label, len(supervisors))
            return work, receipt

        def mutate(path, value):
            def edit(target):
                for part in path[:-1]:
                    target = target[part]
                target[path[-1]] = value
            return edit

        work, receipt = attempt('success', expected='NATIVE_CHAIN_CLOSED', count=5)
        out = work / 'output'
        assert [c['name'] for c in receipt['calls']] == [n for n, _ in CALLS] and receipt['cleanup']['native_units_drained']
        for command in supervisors:
            shell = ' '.join(command)
            assert '--property=MemoryMax=536870912' in shell and '--property=MemorySwapMax=0' in shell and '--property=CPUQuota=100%' in shell
            assert ' taskset -c 0 ' in shell and str(Path(base.__file__).resolve()) in command, 'preflight --owned-stage re-exec'
        # Exact publisher argv/config and baseline argv/config identity.
        publish_cfg = json.loads((out / 'configs/publish.json').read_bytes())
        st = os.stat(work / 'input/store/semantic/objects' / Path(publish_cfg['sq8_object_key']).name)
        assert publish_cfg['sq8_etag'] == '"%x-%x-%x"' % (st.st_ino, st.st_mtime_ns // 1000, st.st_size)
        assert (publish_cfg['retained_prefix'], publish_cfg['destination_prefix'], publish_cfg['max_scratch_bytes'], publish_cfg['limits']) == (
            RETAINED_PREFIX, DESTINATION_PREFIX, 64 << 20, limits_ok)
        assert receipt['calls'][0]['command'][1] == '--retained' and receipt['calls'][0]['log']['bytes'] == 0, 'silent publisher log authenticated'
        arm_cfgs = [(out / 'configs' / (n + '.json')).read_bytes() for n, _ in ARMS]
        assert len(set(arm_cfgs)) == 1 and h(arm_cfgs[0]) == receipt['baseline_config_sha256']
        original = json.loads((work / 'ledger-in.json').read_bytes())['original_runner_config']['config']
        rebased = json.loads(arm_cfgs[0])
        assert neutral(rebased) == neutral(original) and rebased != original
        assert rebased['generation_prefix'] == receipt['runtime_rebound_root']['metadata_prefix'] and rebased['generation_root_sha256'] == receipt['runtime_rebound_root']['root_sha256']
        assert rebased['store_root'] == str(work / 'input/store') and rebased['scratch_parent'] == str(out / 'scratch')
        assert [c['name'] + ':' + c['role'] for c in receipt['calls']] == ['publish:publisher', 'a1:baseline_a', 'b1:baseline_b', 'b2:baseline_b', 'a2:baseline_a']
        # Results stay opaque: A and B bytes differ yet the chain closes; pins are whole-file SHA/length; nothing parsed them.
        assert (out / 'a1-result.jsonl').read_bytes() != (out / 'b1-result.jsonl').read_bytes() and set(receipt['results']) == {'a1', 'b1', 'b2', 'a2'}
        for name, pin in receipt['results'].items():
            assert pin == local.identity(out / (name + '-result.jsonl'))
        assert not any(p.endswith(('-result.jsonl', '.log')) for p in read_json_paths), 'ANN results and logs never parsed'
        assert receipt['body_authentication']['complete'] is True and receipt['body_authentication']['reauthenticated_after_chain'] is True
        assert len(receipt['body_authentication']['objects']) == ASSETS and receipt['claims'] == dict(parity=False, quality=False, performance=False, s3=False)
        assert receipt['runtime_rebound_root']['root_sha256'] != json.loads((work / 'ledger-in.json').read_bytes())['retained_approval']['root_sha256']
        assert set(receipt['runtime_rebound_root']['roster']) == set(tree(work / 'input/store/semantic/rebound')) and receipt['status'] == 'NATIVE_CHAIN_CLOSED'
        # Output collision: nothing is overwritten.
        before = base.inventory(out)
        os.environ['REPLAY_FIXTURE'] = ''
        pin = local.identity(work / 'config-in.json')
        try:
            execute(pin['path'], pin['sha256'], out)
        except FileExistsError:
            pass
        else:
            raise AssertionError('existing output overwritten')
        assert base.inventory(out) == before, 'nooverwrite original attempt'

        # Pre-native refusals: INVALID and no native stage is even entered (spy empty).
        def ledger_edit(path, value):
            return dict(ledger_edit=mutate(path, value))
        def adm_edit(path, value):
            return dict(adm_edit=mutate(path, value))
        def swap_roles(adm):
            adm['roles']['baseline_a']['binary'], adm['roles']['baseline_b']['binary'] = adm['roles']['baseline_b']['binary'], adm['roles']['baseline_a']['binary']
        def drop(key):
            return lambda value: value.pop(key)
        def flip(relative):
            def edit(work_, files):
                path = work_ / 'input' / relative; payload = bytearray(path.read_bytes()); payload[0] ^= 1
                path.write_bytes(bytes(payload))
            return edit
        def flip_object(work_, files):
            flip(next(n for n in files if n.startswith('store/semantic/objects/')))(work_, files)
        def stage_stray(work_, files):
            (work_ / 'input/stray.bin').write_bytes(b'x')
        def stage_missing(work_, files):
            (work_ / 'input/prepared/truth.u64').unlink()
        def stage_extra_bin(work_, files):
            (work_ / 'bin/extra').write_bytes(b'x')
        def head_wrong_gen(ledger):
            ledger['retained_approval']['generation'] = 2
        def head_entry_wrong(ledger):
            for item in ledger['objects']:
                if item['path'] == HEAD_PATH:
                    item['sha256'] = '0' * 64
        def dup_path(ledger):
            ledger['objects'][1]['path'] = ledger['objects'][0]['path']
        def drift_config(ledger):
            ledger['original_runner_config']['config']['max_memory_bytes'] = 1
        for label, edits in (
                ('adm-pending', adm_edit(['status'], 'PENDING')), ('adm-failed', adm_edit(['status'], 'FAILED')),
                ('adm-source', adm_edit(['source_identity_sha256'], 'c' * 64)),
                ('adm-stages', adm_edit(['gates', 'retained_publisher', 'stage_count'], 4)),
                ('adm-stages-baseline', adm_edit(['gates', 'original_baseline', 'stage_count'], 5)),
                ('adm-stages-blocked', adm_edit(['gates', 'blocked_scorer', 'stage_count'], 6)),
                ('adm-exit', adm_edit(['gates', 'blocked_scorer', 'stage_exit_statuses'], [0, 0, 0, 0, 0, 0, 1])),
                ('adm-exit-bool', adm_edit(['gates', 'original_baseline', 'stage_exit_statuses'], [False] * 6)),
                ('adm-alive', adm_edit(['gates', 'retained_publisher', 'terminated'], False)),
                ('adm-gate-sha', adm_edit(['gates', 'blocked_scorer', 'receipt', 'sha256'], '0' * 64)),
                ('adm-gate-source', adm_edit(['gates', 'blocked_scorer', 'source_identity_sha256'], 'zz')),
                ('adm-publisher-source', adm_edit(['gates', 'retained_publisher', 'source_identity_sha256'], 'c' * 64)),
                ('adm-baseline-source', adm_edit(['gates', 'original_baseline', 'source_identity_sha256'], 'e' * 64)),
                ('adm-blocked-source', adm_edit(['gates', 'blocked_scorer', 'source_identity_sha256'], 'f' * 64)),
                ('adm-role-gate', adm_edit(['roles', 'publisher', 'gate'], 'blocked_scorer')),
                ('adm-role-sha', adm_edit(['roles', 'baseline_b', 'binary', 'sha256'], 'd' * 64)),
                ('adm-basename', adm_edit(['roles', 'publisher', 'binary', 'local_basename'], 'other')),
                ('adm-role-swap', dict(adm_edit=swap_roles)), ('adm-unknown', adm_edit(['extra'], 1)),
                ('adm-missing-role', dict(adm_edit=lambda adm: adm['roles'].pop('baseline_a'))),
                ('cfg-unknown', dict(cfg_edit=mutate(['extra'], 1))), ('cfg-limit-bool', dict(cfg_edit=mutate(['publisher_limits', 'max_query_gets'], True))),
                ('cfg-limit-memory', dict(cfg_edit=mutate(['publisher_limits', 'max_memory_bytes'], (512 << 20) + 1))),
                ('cfg-phase-high', dict(cfg_edit=mutate(['phase_seconds', 'b1'], 301))), ('cfg-phase-low', dict(cfg_edit=mutate(['phase_seconds', 'a2'], 1))),
                ('cfg-deadline', dict(cfg_edit=mutate(['deadline_seconds'], 1801))),
                ('cfg-work-root', dict(cfg_edit=mutate(['work_root'], 'relative/work'))),
                ('cfg-binary-sha', dict(cfg_edit=mutate(['binaries', 'baseline_b', 'sha256'], '0' * 64))),
                ('cfg-binary-path', dict(cfg_edit=lambda cfg: cfg['binaries'].update(baseline_b=dict(cfg['binaries']['baseline_a'])))),
                ('ledger-sha', dict(cfg_edit=mutate(['ledger', 'sha256'], '0' * 64))),
                ('ledger-body-flag', ledger_edit(['body_authentication_pending'], False)),
                ('ledger-root-flag', ledger_edit(['runtime_rebound_root_pending'], False)),
                ('ledger-status', ledger_edit(['status'], 'LAUNCHED')), ('ledger-order', ledger_edit(['order'], ['A1', 'A2', 'B1', 'B2'])),
                ('ledger-count', dict(ledger_edit=lambda ledger: ledger['objects'].pop())), ('ledger-dup', dict(ledger_edit=dup_path)),
                ('ledger-bytes', ledger_edit(['input_bytes_excluding_pending_publisher'], 1)),
                ('ledger-etag-resolved', ledger_edit(['retained_approval', 'sq8_etag_runtime_pending'], False)),
                ('ledger-head-gen', dict(ledger_edit=head_wrong_gen)), ('ledger-head-asset', dict(ledger_edit=head_entry_wrong)),
                ('ledger-head-root', ledger_edit(['original_head_authenticated', 'root_sha256'], '3' * 64)),
                ('ledger-config-drift', dict(ledger_edit=drift_config)),
                ('ledger-binary-a', ledger_edit(['query_binaries', 'A', 'sha256'], '4' * 64)),
                ('stage-flip-truth', dict(stage_edit=flip('prepared/truth.u64'))), ('stage-flip-object', dict(stage_edit=flip_object)),
                ('stage-stray', dict(stage_edit=stage_stray)), ('stage-missing', dict(stage_edit=stage_missing)),
                ('stage-extra-bin', dict(stage_edit=stage_extra_bin))):
            attempt(label, count=0, **edits)
        # Native-side failures: exact stage reached, later stages never start.
        for label, scenario, count in (('pub-exit3', 'exit3:publish', 1), ('pub-same-root', 'pub-same-root', 1), ('pub-wrong-root', 'pub-wrong-root', 1),
                ('pub-head-key', 'pub-head-key', 1), ('pub-local-false', 'pub-local-false', 1), ('pub-perf-claim', 'pub-perf-claim', 1),
                ('pub-etag', 'pub-etag', 1), ('pub-extra', 'pub-extra', 1), ('pub-head-root', 'pub-head-root', 1),
                ('pub-mutate-original', 'pub-mutate-original', 1), ('missing-result', 'missing-result:b1', 3), ('scratch-left', 'scratch:b1', 3),
                ('stray-store', 'stray:a1', 2), ('mutate-rebound', 'mutate-rebound:b1', 3), ('touch-original', 'touch-original:b2', 4),
                ('tamper-result', 'tamper-result:b2', 4)):
            _, partial = attempt(label, scenario, count=count)
            assert partial['error'] and not (partial['status'] == 'NATIVE_CHAIN_CLOSED')
        # Stop at the first failing arm; raw native exit retained; later arms never start; no parity/quality interpretation.
        for arm_name, count in (('a1', 2), ('b1', 3), ('b2', 4), ('a2', 5)):
            _, partial = attempt('nonzero-' + arm_name, 'exit2:' + arm_name, expected='BASELINE_NONZERO_EXIT', count=count)
            assert partial['native_exit_status'] == 2 and partial['failed_arm'] == arm_name and partial['error']
            assert list(partial['results']) == [n for n, _ in ARMS][:count - 2], 'only arms before the failing arm have result pins'
        # A log of exactly the cap is within it: an ordinary clean exit 2 is still classified, not rejected.
        edge_work, edge = attempt('edgeexit2-b2', 'edgeexit2:b2', expected='BASELINE_NONZERO_EXIT', count=4)
        assert (edge_work / 'output/b2.log').stat().st_size == RESOURCES['max_log_bytes'] and edge['native_exit_status'] == 2
        # An oversized native log with a surviving plain exit 2 is a resource failure (INVALID), never BASELINE_NONZERO_EXIT.
        for arm_name, count in (('a1', 2), ('b2', 4)):
            big_work, big = attempt('bigexit2-' + arm_name, 'bigexit2:' + arm_name, count=count)
            assert (big_work / 'output' / (arm_name + '.log')).stat().st_size > RESOURCES['max_log_bytes'] and big['status'] == 'INVALID'
            assert base.nonzero_native_exit(big['stages']) == 2, 'the shared classifier alone would have promoted this exit'
        big_work, big = attempt('bigunit-b1', 'exit2:b1', count=3, big_unit='b1')
        assert (big_work / 'output/b1-unit.log').stat().st_size > RESOURCES['max_log_bytes'] and base.nonzero_native_exit(big['stages']) == 2
        attempt('drain', count=1, bad_drain=True)
        work_, _ = attempt('deadline', 'sleep:publish', count=1, cfg_edit=mutate(['phase_seconds', 'publish'], 6))
        inner = real_read_json(local.identity(work_ / 'output/publish-stage-receipt.json'), 1 << 20)
        assert inner['status'] == 'INVALID' and inner['stages'][0]['exit_status'] == -9, 'deadline killed the native process, not an incidental failure'
        with patch.object(module, 'RESOURCES', dict(RESOURCES, scratch_max_bytes=1000)):
            attempt('scratch-cap', count=0)
        # The output must be exactly work_root/output; nothing native starts otherwise.
        os.environ['REPLAY_FIXTURE'] = ''; supervisors.clear(); spy.clear()
        work_, pin_ = build('out-mismatch')
        terminal = execute(pin_['path'], pin_['sha256'], top / 'elsewhere-out')
        assert terminal['status'] == 'INVALID' and not spy and not supervisors
        # The publisher inherits only the explicit approval: unrecognised fixture scenario proves the fixture is the discriminator.
        attempt('control', 'unknown-scenario', expected='NATIVE_CHAIN_CLOSED', count=5)
    print('PASS retained paired replay glue: publisher + A1 B1 B2 A2 through the real supervisor/run_stage and preflight closure; '
          'admission 5/6/7 gates and role drift; ledger pending flags never self-asserted; fifteen assets incl. authenticated head; '
          'ETag formula; exact publisher/baseline configs (six slots only); opaque results; stop at first failing arm; '
          'changed original/rebound files, scratch, deadline, closure refusals; terminal-last. systemd/cgroup and natives are MOCKED; no Cargo/native/data/network.')


def main(args):
    try:
        if args == ['--self-check']:
            self_check(); return 0
        require(len(args) == 3, 'usage: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check')
        terminal = execute(*args)
        print(json.dumps(dict(status=terminal['status'], complete=terminal['complete'], output=str(args[2])), sort_keys=True))
        return terminal['execution_exit_code']
    except Exception as error:
        print(type(error).__name__ + ': ' + str(error), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
