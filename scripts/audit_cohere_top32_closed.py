#!/usr/bin/env python3
"""Read-only audit of the frozen, failed CoHere top32 coverage execution.

--self-check exercises closure admission with source-bound temporary bodies.
An audit never repairs missing authority or qualifies the original execution.
"""
import argparse
import gzip
import json
import math
from pathlib import Path
import resource
import signal
import struct
import subprocess
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import launch_cohere_top32_coverage_spot as controller
sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_semantic_binary_coverage as coverage

EVIDENCE = controller.ROOT / 'a0002'
DELETED = ('source-order.u64', 'source-root.json', 'seal-readback.json', 'decision.json')
EVIDENCE_COMMIT = '2838f04b5e1da3008222c600ff76e35f4d943882'
SOURCE_COMMIT = '2d178890da6aec550cbbc5f81ff2737c5bd68fc4'
SOURCE_ARCHIVE = 'f35b08806829df80c249dc52858154b3e67da4724cc40a1023601698f5510fd0'


def committed(repo, revision, name):
    controller.repo_path(repo, name)  # Reject escaped paths before Git lookup.
    return subprocess.check_output(['git', 'show', revision + ':' + name], cwd=repo, timeout=5)


def bind(path, pin):
    assert controller.artifact(path) == {k: pin[k] for k in ('bytes', 'sha256')}, 'body identity: ' + str(path)


def recount(nomination, recorded, truth, narrow, order):
    """Count source-ordinal membership independently of the original reducer."""
    assert len(truth) == 51200 and len(narrow) == 25600, 'truth64x100 widths'
    assert len(nomination['records']) == len(recorded['records']) == 64
    summary = {name: dict.fromkeys(('nomination_unit_hits10', 'nomination_unit_hits100',
        'page_closure_hits10', 'page_closure_hits100', 'within_current_walk_guard_queries'), 0)
        for name in coverage.POLICIES}
    for ordinal, (frozen, report) in enumerate(zip(nomination['records'], recorded['records'])):
        assert frozen['query_ordinal'] == report['query_ordinal'] == ordinal, 'coverage ordinal'
        assert frozen['source_ordinal'] == report['source_ordinal'], 'coverage source ordinal'
        assert set(frozen['policies']) == set(report['policies']) == set(coverage.POLICIES)
        gold = struct.unpack_from('<100q', truth, ordinal * 800)
        assert gold == struct.unpack_from('<100I', narrow, ordinal * 400), 'truth widening'
        assert len(set(gold)) == 100 and all(0 <= value < 1000000 for value in gold), 'truth range/uniqueness'
        old, new = (frozen['policies'][name] for name in coverage.POLICIES)
        assert old['selected_leaf_ids'] == new['selected_leaf_ids'][:len(old['selected_leaf_ids'])]
        assert set(old['units']) <= set(new['units']), 'same ranking containment'
        for name, maximum in zip(coverage.POLICIES, (16, 32)):
            selection, values = frozen['policies'][name], report['policies'][name]
            coverage.frozen_record(selection, 1000000, nomination['leaf_count'], maximum, 768)
            assert all(values[key] == value for key, value in selection.items()), 'nomination changed in report'
            assert values['selected_units'] == len(selection['units'])
            assert values['page_closure_pages'] == len(selection['page_closure'])
            assert values['root_bytes'] == nomination['root_bytes']
            assert values['root_plus_selected_leaf_bytes'] == nomination['root_bytes'] + selection['selected_leaf_bytes']
            for label, width, selected in (('nomination_unit', 32, selection['units']),
                    ('page_closure', 256, selection['page_closure'])):
                selected = set(selected)
                for k in (10, 100):
                    hit_key, rate_key = label + '_hits' + str(k), label + '_R' + str(k)
                    hits = sum(order[value] // width in selected for value in gold[:k]) if order is not None else values[hit_key]
                    assert type(hits) is int and 0 <= hits <= k and values[hit_key] == hits, 'truth membership: ' + hit_key
                    assert values[rate_key] == hits / k, 'per-query fraction'
                    summary[name][hit_key] += hits
            summary[name]['within_current_walk_guard_queries'] += selection['within_current_walk_guard']
    for values in summary.values():
        for label in ('nomination_unit', 'page_closure'):
            for k in (10, 100):
                values[label + '_R' + str(k)] = values[label + '_hits' + str(k)] / (64 * k)
    assert summary == recorded['summary'], 'independent summary differs'
    assert recorded['threshold'] == dict(denominator10=640, hits10_minimum=608), 'frozen608/640 threshold'
    status = 'GO-for-native-investigation' if summary[coverage.POLICIES[1]]['page_closure_hits10'] >= 608 else 'FAIL'
    assert recorded['status'] == status, 'frozen gate status'
    return summary, status


def audit(repo, evidence, order_file=None, source_root_file=None):
    evidence = Path(evidence).resolve()
    # Independent root receipts are anchored to the committed closed evidence.
    receipts = {}
    for name in ('aws-terminal.json', 'aws-reservation.json', 'aws-launch.json', 'aws-closeout.json'):
        body = (evidence / name).read_bytes()
        assert body == committed(repo, EVIDENCE_COMMIT, str(EVIDENCE / name)), 'closed receipt drift: ' + name
        receipts[name] = json.loads(body)
    terminal, reservation, launch, closeout = (receipts[n] for n in (
        'aws-terminal.json', 'aws-reservation.json', 'aws-launch.json', 'aws-closeout.json'))
    proof = reservation['qualification']
    assert closeout['state'] == 'terminated' and closeout['nodes'] == launch['nodes']
    owned = [node['instance_id'] for node in launch['nodes'].values()]
    assert len(set(owned)) == len(owned) == 1 and terminal['instance_id'] == launch['instance_id'] == owned[0]
    for receipt in (terminal, reservation, launch):
        assert receipt['source_commit'] == SOURCE_COMMIT and receipt['source_archive_sha256'] == SOURCE_ARCHIVE
    assert terminal['schema'] == reservation['schema'] == controller.SCHEMA
    assert terminal['status'] == 'failed' and terminal['phase'] == 'coverage'
    assert terminal['exit_code'] == terminal['original_exit_code'] == 1, 'original execution remains FAIL'
    assert launch['prefix'] == controller.PREFIX + 'a0002'
    expected = set(controller.ARTIFACTS) - {'screen/' + n for n in DELETED}
    assert set(terminal['artifacts']) == expected and len(expected) == 34, 'closed34 body roster'
    for name, pin in terminal['artifacts'].items():
        assert set(pin) == {'bytes', 'sha256'} and type(pin['bytes']) is int and 0 < pin['bytes'] <= 16 << 20
        bind(controller.repo_path(evidence, name), pin)
        compressed = evidence / (name + '.gz')
        if compressed.exists():
            with gzip.open(compressed, 'rb') as stream:
                assert stream.read(pin['bytes'] + 1) == (evidence / name).read_bytes(), 'compressed body drift'
    def read(name):
        return json.loads((evidence / name).read_bytes())
    config, helper = read('config.json'), read('helper-config.json')
    assert set(config) == set(controller.FIXED) | {'authority_pending', 'controller_code_sha256', 'helper_config'}
    assert config['authority_pending'] is False
    assert all(controller.encoded(config[k]) == controller.encoded(v) for k, v in controller.FIXED.items())
    assert config['controller_code_sha256'] == proof['code_sha256'] and set(proof['code_sha256']) == set(controller.CODE)
    assert config['helper_config'] == proof['helper_config']
    assert proof['config_sha256'] == reservation['config_sha256'] == controller.artifact(evidence / 'config.json')['sha256']
    bind(evidence / 'helper-config.json', proof['helper_config'])
    assert proof['helper_config_sha256'] == proof['helper_config']['sha256']
    assert helper['refs'] == proof['refs'] and len(helper['refs']) == 46
    assert helper['authority_pending'] is False and set(helper['code_sha256']) == set(controller.HELPER_CODE)
    assert all(proof['code_sha256'][n] == digest for n, digest in helper['code_sha256'].items())
    for field, value in (('code_identity_sha256', proof['code_sha256']), ('refs_identity_sha256', helper['refs']),
            ('artifact_roster_sha256', controller.ARTIFACTS)):
        assert proof[field] == controller.sha(controller.encoded(value)), 'frozen identity: ' + field
    for key in controller.TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal proof: ' + key
    assert read('source-qualification.json') == dict(proof, source_commit=SOURCE_COMMIT, source_archive_sha256=SOURCE_ARCHIVE)
    for name, digest in proof['code_sha256'].items():
        assert controller.sha(committed(repo, SOURCE_COMMIT, name)) == digest, 'historical source: ' + name
    for pin in helper['refs'].values():
        body = committed(repo, SOURCE_COMMIT, pin['path'])
        assert len(body) == pin['bytes'] and controller.sha(body) == pin['sha256'], 'historical ref: ' + pin['path']
    assurance = read('archived-builder-assurance.json')
    assert assurance == read('screen/source-qualification.json')
    assert controller.sha(controller.encoded(assurance)) == proof['builder_assurance_sha256']
    for field, identity in (('original_native_source_sha256', 'original_source_identity_sha256'),
            ('archived_quality_native_source_sha256', 'archived_quality_source_identity_sha256')):
        assert len(assurance[field]) == assurance['source_file_count'] == 399
        assert controller.sha(controller.encoded(assurance[field])) == assurance[identity] == proof[identity]
    assert assurance['current_native_tree_qualified'] is assurance['current_whole_tree_full_execution'] is assurance['native_rebuilt'] is False
    assert assurance['original_full_workspace_execution_reused'] is True and assurance['scorer_invocations'] == 0
    builder = assurance['builder_binary']
    assert builder['sha256'] == proof['builder_binary_sha256']
    with gzip.open(controller.repo_path(repo, builder['archived_path']), 'rb') as stream:
        binary = stream.read(builder['bytes'] + 1)
    assert len(binary) == builder['bytes'] and controller.sha(binary) == builder['sha256'], 'archived binary'
    for pin in assurance['archived_assurance_refs'].values():
        path = controller.repo_path(repo, pin['path'])
        if path.exists():
            bind(path, pin)
        else:
            archive = controller.repo_path(repo, pin['archived_path'])
            assert archive.is_file(), 'archived assurance body required'
            with gzip.open(archive, 'rb') as stream:
                body = stream.read(pin['bytes'] + 1)
            assert len(body) == pin['bytes'] and controller.sha(body) == pin['sha256'], 'archived assurance identity'
    failure, closure = read('failure.json'), read('coverage-closure.json')
    assert failure['status'] == 'failed' and failure['helper_exit_code'] == closure['helper_exit_code'] == 0
    assert failure['error_type'] == 'IndexError' and failure['error'] == 'list index out of range'
    assert set(failure['cleanup']['removed']) == set(DELETED) and failure['replacement_allowed'] is False
    assert closure['closed'] is closure['process_cleanup'] is True and closure['helper_invocations'] == 1
    assert closure['prefix'] == launch['prefix'] and closure['scorer_invocations'] == 0
    assert closure['config_sha256'] == proof['config_sha256'] and closure['helper_config_sha256'] == proof['helper_config_sha256']
    assert closure['source_commit'] == SOURCE_COMMIT and closure['source_archive_sha256'] == SOURCE_ARCHIVE
    assert 0 <= closure['wall_seconds'] <= controller.WORKER_SECONDS
    controller.validate_cgroup(read('profile-cgroup.json'))
    timing = (evidence / 'profile-resources.txt').read_text()
    assert int(timing.split('Exit status: ', 1)[1].splitlines()[0]) == 1
    assert 0 <= int(timing.split('Maximum resident set size (kbytes): ', 1)[1].splitlines()[0]) * 1024 <= controller.MEMORY
    for name in ('resources.json', 'final-resources.json'):
        report = read('screen/' + name)
        assert report['passed'] is True and report['build_invocations'] == report['oracle_invocations'] == 1
        assert report['scorer_invocations'] == 0 and report['resource_failure'] is None
        assert 0 <= report['aggregate_memory_peak_bytes'] <= controller.MEMORY
        assert 0 <= report['peak_scratch_bytes'] <= controller.SCRATCH and 0 <= report['wall_seconds'] <= 1800
        assert all(int(report['memory_events'].get(k, 0)) == 0 for k in ('max', 'oom', 'oom_kill'))
    nomination, recorded = read('screen/nomination.json'), read('screen/coverage.json')
    nominate_config, reduce_config = read('screen/nominate-config.json'), read('screen/reduce-config.json')
    assert nomination['schema'] == coverage.FREEZE_SCHEMA and recorded['schema'] == coverage.RESULT_SCHEMA
    assert nomination['phase'] == 'nomination_frozen_before_truth' and nomination['truth_opened'] is False
    assert nomination['count'] == 64 and nomination['rows'] == 1000000 and nomination['dimensions'] == 768
    assert nomination['unit_rows'] == 32 and nomination['page_rows'] == 256 and nomination['profile'] == 'fresh1m'
    assert nomination['policies'] == list(coverage.POLICIES) and nomination['claims'] == recorded['claims'] == coverage.CLAIMS
    assert nomination['inputs'] == nominate_config == recorded['nomination_inputs']
    assert recorded['inputs'] == reduce_config and reduce_config['truth_id_space'] == 'source_ordinal'
    assert recorded['phase'] == 'truth_reduction_after_immutable_nomination'
    bind(evidence / 'screen/nominate-config.json', nomination['config'])
    bind(evidence / 'screen/reduce-config.json', recorded['config'])
    for field, name in (('nomination', 'nomination.json'), ('truth', 'truth.i64'), ('panel', 'panel.json'),
            ('requests', 'requests.jsonl'), ('protocol', 'prospective-protocol.json')):
        bind(evidence / 'screen' / name, reduce_config[field])
        if field in ('panel', 'requests', 'protocol'):
            assert reduce_config[field] == nominate_config[field]
    assert reduce_config['order'] == nominate_config['order']
    assert nomination['provenance'] == nominate_config['provenance'] == recorded['provenance']
    assert recorded['nomination_code_identity'] == recorded['reduction_code_identity'] == nomination['code_identity']
    for name, pin in nomination['code_identity']['files'].items():
        path = 'crates/borsuk/src/' + name if name.endswith('.rs') else 'scripts/' + name
        body = committed(repo, SOURCE_COMMIT, path)
        assert len(body) == pin['bytes'] and controller.sha(body) == pin['sha256']
        if name.endswith('.py'):
            bind(repo / path, pin)  # Executed validation primitives match the frozen source.
    provenance = nomination['provenance']
    assert provenance['builder_binary_sha256'] == builder['sha256'] and provenance['builder_commit'] == assurance['builder_commit']
    assert provenance['source_commit'] == assurance['source_commit'] and provenance['source_archive_sha256'] == assurance['source_archive_sha256']
    bind(evidence / 'screen/source-qualification.json', provenance['source_identity'])
    bind(evidence / 'screen/build-resources.json', provenance['resource_metadata'])
    seal = read('screen/nomination-seal.json')['artifacts']['nomination.json']
    bind(evidence / 'screen/nomination.json', seal)
    assert seal['authenticated_readback'] is True and seal['key'] == launch['prefix'] + '/nomination/sealed/nomination.json'
    assert seal['sha256'] == reduce_config['nomination']['sha256']
    oracle = read('screen/oracle.json')
    assert oracle['passed'] is True and oracle['ground_truth_constructions'] == 1
    assert oracle['rows'] == 1000000 and oracle['queries'] == 64 and oracle['k'] == 100
    assert oracle['nomination'] == reduce_config['nomination'] and oracle['truth_i64'] == reduce_config['truth']
    bind(evidence / 'screen/truth.u32', oracle['truth_u32'])
    relocated = {field: dict(reduce_config[field], path=str(evidence / 'screen' / name))
        for field, name in (('panel', 'panel.json'), ('requests', 'requests.jsonl'), ('protocol', 'prospective-protocol.json'))}
    coverage.load_protocol(relocated['protocol'])
    _, source_ids, panel = coverage.load_panel(relocated, 768)
    assert panel['selected_sha256'] == nomination['panel_selected_sha256']
    assert [row['source_ordinal'] for row in nomination['records']] == source_ids
    requests = [json.loads(line) for line in (evidence / 'screen/requests.jsonl').read_bytes().splitlines()]
    raw = (evidence / 'screen/queries.raw').read_bytes()
    assert len(raw) == 64 * 768 * 4
    for i, request in enumerate(requests):
        assert request['ordinal'] == i and all(math.isfinite(v) for v in request['query'])
        assert struct.pack('<768f', *request['query']) == raw[i * 3072:(i + 1) * 3072], 'query raw/request binding'
    order_pin = reduce_config['order']
    assert {k: order_pin[k] for k in ('bytes', 'sha256')} == {k: helper['corpus']['order'][k] for k in ('bytes', 'sha256')}
    assert panel['corpus']['order'] == helper['corpus']['order']
    order = None
    if order_file is not None:
        bind(order_file, order_pin)
        order = coverage.load_order(dict(order_pin, path=str(order_file)), 1000000)
    source_root_pin = helper['corpus']['root_manifest']
    if source_root_file is not None:
        bind(source_root_file, source_root_pin)
        root = json.loads(Path(source_root_file).read_bytes())
        generation = read('screen/generation-manifest.json')
        assert root['canonical']['rows'] == 1000000 and root['canonical']['dimensions'] == 768
        assert root['canonical']['bytes'] == 1000000 * (768 * 4 + 8)
        assert {k: root['canonical'][k] for k in ('rows', 'dimensions', 'bytes', 'sha256')} == {
            k: generation['canonical'][k] for k in ('rows', 'dimensions', 'bytes', 'sha256')}, 'canonical physical row identity'
        assert root['page_manifest_sha256'] == generation['page_manifest_sha256'], 'canonical page layout identity'
        assert root['sq8_object_sha256'] == generation['sq8_object_sha256'] == helper['corpus']['sq8']['sha256']
        assert root['low'] == generation['low'] and root['step'] == generation['step'], 'SQ8 quantizer identity'
        assert len(root['low']) == len(root['step']) == 768
        assert all(math.isfinite(v) for v in root['low']) and all(math.isfinite(v) and v > 0 for v in root['step'])
    summary, status = recount(nomination, recorded, (evidence / 'screen/truth.i64').read_bytes(),
        (evidence / 'screen/truth.u32').read_bytes(), order)
    missing = [name for name in DELETED if not (evidence / 'screen' / name).exists()]
    assert set(missing) == set(DELETED), 'deleted authority must remain absent'
    return dict(schema='borsuk-cohere-top32-closed-offline-audit-v1', evidence_commit=EVIDENCE_COMMIT,
        source_commit=SOURCE_COMMIT, source_archive_sha256=SOURCE_ARCHIVE, source_archive_body_authenticated=False,
        historical_code_files_authenticated=len(proof['code_sha256']), historical_refs_authenticated=46,
        terminal_bodies_authenticated=34, owned_instance_ids=owned, all_owned_instances_terminated=True,
        root_cause='stage parsed enclosing GNU time before its final timing fields were written',
        original_execution_status='FAIL', original_exit_status=1, helper_exit_status=0,
        original_execution_qualified=False, scientific_report_qualified=False, missing_closure_artifacts=missing,
        original_source_root_missing=True, final_seal_and_decision_authority_missing=True,
        truth_queries=64, truth_neighbors=100, query_ordinal_and_source_binding_verified=True,
        nomination_seal_verified=True, nomination_before_truth_source_and_receipt_bound=True,
        ground_truth_recomputed=False, nomination_recomputed=False, builder_reexecuted=False,
        source_order_retrieval=dict(path=str(order_file), original_key=helper['corpus']['order']['key'],
            bytes=order_pin['bytes'], sha256=order_pin['sha256'], original_closure_restored=False) if order is not None else None,
        source_order_bijection_verified=order is not None, truth_membership_recount_verified=order is not None,
        source_root_retrieval=dict(path=str(source_root_file), original_key=source_root_pin['key'],
            bytes=source_root_pin['bytes'], sha256=source_root_pin['sha256'], original_closure_restored=False)
            if source_root_file is not None else None,
        source_root_manifest_verified=source_root_file is not None,
        canonical_row_and_page_manifest_identity_bound=source_root_file is not None,
        physical_page_manifest_body_verified=False,
        recount_scope='independent truth membership' if order is not None else 'reported per-query arithmetic only; source-order body unavailable',
        summary=summary, scientific_report_status=status, frozen_threshold=recorded['threshold'],
        coverage_only=True, old_fail_preserved=True, returned_recall_measured=False,
        cold_http_measured=False, physical_s3_query_gets_measured=False, source_sq8_admission_qualified=False,
        native_qualification_claim=False, current_whole_tree_full_execution=False, complete_historical_coverage=False,
        limitations=['Original source-root, final seal and decision are missing; physical page-manifest body is unavailable. Measured report remains unqualified.',
            'Counts concern frozen nomination unit/page coverage; returned quality, latency and new-source admission remain unmeasured.'])


def self_check(repo):
    import copy
    import shutil
    import tempfile
    from types import SimpleNamespace
    from unittest.mock import patch

    # Synthetic source ordinals and physical mapping; no new experimental truth.
    selections = {}
    for name, units in zip(coverage.POLICIES, ([0], [0, 1])):
        selections[name] = dict(selected_leaf_ids=list(units), units=list(units), page_closure=[0],
            selected_leaf_bytes=len(units) * 1540, selected_source_rows=len(units) * 32,
            page_closure_rows=256, seed_page=0, seed_additions=list(range(len(units), 8)),
            prospective_walk_units=8, within_current_walk_guard=True)
    frozen = dict(root_bytes=512, leaf_count=489, records=[dict(query_ordinal=i,
        source_ordinal=1000000+i, policies=copy.deepcopy(selections)) for i in range(64)])
    for misses in (32, 33):
        records, gold_bytes, narrow_bytes = [], b'', b''
        for i in range(64):
            miss = int(i < misses)
            gold = [256 if miss else 0, *range(1, 100)]
            gold_bytes += struct.pack('<100q', *gold); narrow_bytes += struct.pack('<100I', *gold)
            policies = copy.deepcopy(selections)
            for name, values in policies.items():
                values.update(selected_units=len(values['units']), page_closure_pages=1, root_bytes=512,
                    root_plus_selected_leaf_bytes=512+values['selected_leaf_bytes'])
                for label, k, hits in (('nomination_unit', 10, 10-miss),
                        ('nomination_unit', 100, len(values['units'])*32-miss),
                        ('page_closure', 10, 10-miss), ('page_closure', 100, 100-miss)):
                    values[label+'_hits'+str(k)] = hits
                    values[label+'_R'+str(k)] = hits/k
            records.append(dict(query_ordinal=i, source_ordinal=1000000+i, policies=policies))
        summary = {}
        for name in coverage.POLICIES:
            summary[name] = dict(within_current_walk_guard_queries=64)
            for label in ('nomination_unit', 'page_closure'):
                for k in (10, 100):
                    hits = sum(row['policies'][name][label+'_hits'+str(k)] for row in records)
                    summary[name][label+'_hits'+str(k)] = hits
                    summary[name][label+'_R'+str(k)] = hits/(64*k)
        report = dict(records=records, summary=summary, threshold=dict(denominator10=640, hits10_minimum=608),
            status='GO-for-native-investigation' if misses==32 else 'FAIL')
        order = list(range(257))
        assert recount(frozen, report, gold_bytes, narrow_bytes, order) == (summary, report['status'])
        order[0], order[256] = order[256], order[0]
        try:
            recount(frozen, report, gold_bytes, narrow_bytes, order)
        except AssertionError:
            pass
        else:
            raise AssertionError('source-ordinal/physical-order mutation admitted')
    evidence = repo / EVIDENCE
    proof = json.loads((evidence / 'aws-reservation.json').read_bytes())['qualification']
    counters = json.loads((evidence / 'profile-cgroup.json').read_bytes())
    assurance = json.loads((evidence / 'archived-builder-assurance.json').read_bytes())
    source = json.loads((evidence / 'coverage-closure.json').read_bytes())
    helper = SimpleNamespace(prior=SimpleNamespace(terminate=lambda *_: None),
        offline_modules=lambda _: (None, None), load_inputs=lambda *_: (None, None),
        builder_authority=lambda *_: (None, assurance),
        replay=lambda *_: dict(passed=True, coverage_only=True, remote_objects_reopened=False,
            ground_truth_reexecuted=False, builder_reexecuted=False),
        cleanup_failure=lambda _: dict(synthetic=True))
    with tempfile.TemporaryDirectory(prefix='cohere-closure-synthetic-') as directory:
        out = Path(directory)
        shutil.copyfile(evidence / 'cpu.txt', out / 'cpu.txt')
        # GNU time creates this file before its enclosed process exits.
        controller.write(out / 'profile-resources.txt', b'')
        def prepared(*_):
            screen = out / 'screen'; screen.mkdir()
            for name in controller.HELPER_OUTPUTS:
                if name in DELETED:
                    value = dict(status='FAIL', prefix=source['prefix']) if name == 'decision.json' else dict(synthetic=True)
                    controller.write(screen / name, value)
                else:
                    shutil.copyfile(evidence / 'screen' / name, screen / name)
            (screen / 'nomination.json').chmod(0o444)
            return dict(exit_status=0, process_cleanup=True)
        environment = dict(BORSUK_COVERAGE_SOURCE_COMMIT=source['source_commit'],
            BORSUK_COVERAGE_ARCHIVE_SHA256=source['source_archive_sha256'])
        with patch.dict(controller.os.environ, environment), \
             patch.object(controller, 'qualify', return_value=proof), \
             patch.object(controller, 'helper_module', return_value=helper), \
             patch.object(controller, 'tools', return_value=json.loads((evidence / 'tool-versions.json').read_bytes())), \
             patch.object(controller, 'capture_cgroup', return_value=counters['after']), \
             patch.object(controller, 'run_process', side_effect=prepared):
            closure = controller.stage(repo, out, source['prefix'])
            assert closure['closed'] and closure['helper_exit_code'] == 0
            assert json.loads((out / 'failure.json').read_bytes())['status'] == 'complete'
            assert controller.validate_coverage(out, proof, source['prefix'], repo, collected=False) == 'FAIL'
            for timing in (b'', b'Maximum resident set size (kbytes): 42\nExit status: 1\n',
                    f'Maximum resident set size (kbytes): {controller.MEMORY // 1024 + 1}\nExit status: 0\n'.encode()):
                controller.write(out / 'profile-resources.txt', timing)
                try:
                    controller.validate_coverage(out, proof, source['prefix'], repo)
                except (AssertionError, IndexError):
                    pass
                else:
                    raise AssertionError('invalid final timing accepted')
            controller.write(out / 'profile-resources.txt', b'Maximum resident set size (kbytes): 42\nExit status: 0\n')
            assert controller.validate_coverage(out, proof, source['prefix'], repo) == 'FAIL'
            changed = json.loads((out / 'profile-cgroup.json').read_bytes())
            changed['after']['memory.events'] = 'max 1\noom 0\noom_kill 0\n'
            controller.write(out / 'profile-cgroup.json', changed)
            try:
                controller.validate_coverage(out, proof, source['prefix'], repo, collected=False)
            except AssertionError:
                pass
            else:
                raise AssertionError('stage admitted memory max event')
    unqualified = audit(repo, evidence)
    assert unqualified['terminal_bodies_authenticated'] == 34 and unqualified['scientific_report_status'] == 'FAIL'
    assert unqualified['truth_membership_recount_verified'] is unqualified['scientific_report_qualified'] is False
    with tempfile.TemporaryDirectory(prefix='cohere-audit-mutation-') as directory:
        fixture = Path(directory) / 'closed'
        shutil.copytree(evidence, fixture)
        truth = fixture / 'screen/truth.i64'
        truth.write_bytes(truth.read_bytes() + b'changed')
        try:
            audit(repo, fixture)
        except AssertionError:
            pass
        else:
            raise AssertionError('changed terminal body admitted')
        for option in ('--order-file', '--source-root-file'):
            command = [sys.executable, str(Path(__file__).resolve()), option, str(truth),
                '--output-file', str(Path(directory) / 'rejected.json')]
            run = subprocess.run(command, capture_output=True, timeout=10)
            assert run.returncode != 0 and not (Path(directory) / 'rejected.json').exists(), 'unpinned external input admitted'
        destination = Path(directory) / 'audit.json'; destination.write_bytes(b'existing audit')
        run = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--output-file', str(destination)],
            capture_output=True, timeout=10)
        assert run.returncode != 0 and destination.read_bytes() == b'existing audit', 'audit overwrote an existing file'
    print('PASS source-bound synthetic stage closure, collected timing/cgroup rejection, order mapping and608/607 gate; closed missing-order unqualified; body/external-pin mutations and no-overwrite. No data/build/GT/cloud execution')


def main():
    resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
    signal.alarm(55)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-check', action='store_true')
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--order-file', type=Path, help='external, immutable order body; must match the frozen input pin')
    parser.add_argument('--source-root-file', type=Path, help='external, immutable source-root manifest; must match the frozen corpus pin')
    parser.add_argument('--output-file', type=Path, help='new root audit JSON; existing files are never overwritten')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    if args.self_check:
        assert not any((args.evidence_dir, args.order_file, args.source_root_file, args.output_file)), 'self-check takes no audit inputs'
        self_check(repo)
    else:
        if args.output_file is None:
            parser.error('--output-file is required')
        report = audit(repo, args.evidence_dir or repo / EVIDENCE, args.order_file, args.source_root_file)
        with args.output_file.open('xb') as stream:
            stream.write(controller.encoded(report) + b'\n')
            stream.flush(); controller.os.fsync(stream.fileno())
        print(json.dumps(dict(original_execution_status=report['original_execution_status'],
            truth_membership_recount_verified=report['truth_membership_recount_verified'],
            scientific_report_status=report['scientific_report_status'],
            scientific_report_qualified=report['scientific_report_qualified'], output=str(args.output_file))))
    signal.alarm(0)


if __name__ == '__main__':
    main()
