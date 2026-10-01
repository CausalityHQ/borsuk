#!/usr/bin/env python3
"""Paired serial/offered metadata wave research adapter; no launch authority.

CLI: CONFIG CONFIG_SHA CONTROL_BINARY CONTROL_PROOF CANDIDATE_BINARY
     CANDIDATE_PROOF NEW_OUTPUT | --self-check

Config schema borsuk-native-semantic-metadata-waves-cold-v1 uses the existing fixed
64/k10 FIRST100k semantic panel envelopes. Both item.arms MUST be identical,
including index, head, generation, metadata, leaf and reference identities.
native_arms.{control,candidate} each contains source_manifest, binary and proof
{path,bytes,sha256} pointers. checker_authority is another such pointer to the
committed frozen authority. code_sha256 covers exactly CODE. Original CPU,
memory, deadlines and campaign caps are mandatory. Proofs require completed
affected/release/Clippy/test-compilation/full-workspace gates; pending is fatal.

All consumed manifests, proofs, frozen checkers and panel bodies are saved for
independent offline replay. No concurrency, retry, publication or native build.
execution_gate_passed covers execution/quality/resources/cleanup; paired_gate_passed
also requires candidate p90 < control and p95 <= control on BOTH datasets.
CLI exit 0 requires the paired gate and authenticated closed output.

Offered schema borsuk-native-semantic-metadata-waves-cold-offered-v1 keeps the
same native_arms/checker/panel contract, omits blocks, uses OFFERED_CODE and
OFFERED_FIXED: rates [.25,.5,1,2,4,8], 64 offers/cell, six workers, base18080,
125ms dispatch lateness, 90s cleanup reserve, ann_queries1536. The shared
rate-major 24-cell scheduler retains individual ledgers/markers after drain
and fsync. main(..., on_cell_closed=callback) reports (marker, paths).
Offered exit0 requires valid identity/resources/cleanup/ledger closure even
when offered_gate_passed is false; fatal execution exits1. Full-span QPS
includes validation/cleanup; response tails are conditioned on success and
exclude cleanup. --offered-self-check runs only mocks; no builds or cloud.
"""
from contextlib import contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import threading
import types

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_native_semantic_router_cold as worker

stats, old = worker.stats, worker.old
ROOT = Path(__file__).resolve().parents[1]
SCHEMA = 'borsuk-native-semantic-metadata-waves-cold-v1'
RESULT_SCHEMA = 'borsuk-native-semantic-metadata-waves-cold-result-v1'
OFFERED_SCHEMA = 'borsuk-native-semantic-metadata-waves-cold-offered-v1'
OFFERED_RESULT_SCHEMA = 'borsuk-native-semantic-metadata-waves-cold-offered-result-v1'
ROLES = ('control', 'candidate')
CODE = (*worker.CODE, 'scripts/run_native_semantic_metadata_cold.py',
        'scripts/check_native_semantic_metadata_cold.py')
OFFERED_CODE = (*CODE, *(n for n in worker.OFFERED_CODE if n not in worker.CODE))
AUTHORITY_SHA = 'c90ecbee9443ea46b75ede73b034040fdfcfcb61e0329e2d060235bb0b61a0b0'
SOURCE_IDS = dict(control='b095ba7d738a65a31faefa0d705ce34e12cb83aeaa153c25079564c53b129c70',
                  candidate='714794a10c2d886f7a53a9926a4bb63e093cec16858676268e31833aeead2681')
BINARY_IDS = dict(control=dict(bytes=16189792, sha256='82c02967f3b2de8c7bf1f2dcfc0e94496882ba6e749b7195f1fca824ab2b7ec1'),
                  candidate=dict(bytes=16259024, sha256='e3516453797add1f8e76daddcc97a8fb5e4c1c3467749ae9cb732934b3bc2c0e'))
NATIVE_DELTA = {'crates/borsuk/src/object_native_generation.rs', 'crates/borsuk/src/two_bit_generation.rs'}
FIXED = dict(count=64, k=10, ann_queries=512, native_memory_bytes=536870912,
             profile_memory_bytes=8589934592, profile_swap_bytes=0, native_rlimit_as_bytes=4294967296,
             namespace_connect_deadline_seconds=45, native_process_limit_seconds=60,
             query_payload_timeout_seconds=5, worker_limit_seconds=3000, machine_limit_seconds=3600,
             client_cpu_affinity=[4, 5], native_cpu_affinity=[0, 1, 2, 3],
             dataset_order=list(worker.DATASETS), blocks=list(worker.BLOCKS))
OFFERED_FIXED = dict(FIXED, ann_queries=1536, offered_qps=worker.RATES, workers=6,
                     base_port=18080, max_dispatch_lateness_ns=125000000, cleanup_reserve_seconds=90)
OFFERED_FIXED.pop('blocks')
PROOF_GATES = ('green_status', 'release_status', 'clippy_status',
               'workspace_test_compilation_status', 'full_suite_status')


@contextmanager
def scoped(**changes):
    """Install hooks before calls start; restore after synchronous work or drain."""
    saved = {name: getattr(worker, name) for name in changes}
    try:
        for name, value in changes.items():
            setattr(worker, name, value)
        yield
    finally:
        for name, value in saved.items():
            setattr(worker, name, value)


def sha_body(body):
    return hashlib.sha256(body).hexdigest()


def pointer(identity):
    stats.require(type(identity['path']) is str and bool(identity['path']), 'artifact path')
    stats.integer(identity['bytes'], 'artifact bytes', 1)
    stats.digest(identity['sha256'])


def read_bound(identity, path=None):
    pointer(identity)
    body = Path(path or identity['path']).read_bytes()
    stats.require(len(body) == identity['bytes'] and sha_body(body) == identity['sha256'], 'artifact identity: ' + identity['path'])
    return body


def authenticate(config, saved=None):
    """Bind full inventories and decompress only the authenticated checker bodies."""
    out = Path(saved) if saved is not None else None
    authority_body = read_bound(config['checker_authority'], out / 'checker-authority.json' if out else None)
    stats.require(sha_body(authority_body) == AUTHORITY_SHA, 'frozen checker authority identity')
    authority = json.loads(authority_body)
    stats.require(authority['schema'] == 'borsuk-semantic-metadata-paired-checker-authority-v1'
                  and set(authority['files']) == set(ROLES), 'checker authority schema/roles')
    manifests, checkers, bodies = {}, {}, {'checker-authority.json': authority_body}
    for role in ROLES:
        stats.require(authority[role + '_native_identity'] == SOURCE_IDS[role], 'checker/native role')
        entry = authority['files'][role]
        archive = (out / (role + '-checker.py.gz') if out else ROOT / entry['archived_path']).read_bytes()
        stats.require(sha_body(archive) == stats.digest(entry['archived_sha256']), 'archived checker identity')
        body = gzip.decompress(archive)
        stats.require(len(body) == entry['bytes'] and sha_body(body) == entry['sha256'], 'checker body identity')
        module = types.ModuleType('frozen_metadata_checker_' + role)
        exec(compile(body, entry['archived_path'], 'exec'), module.__dict__)
        checkers[role] = module
        manifest_body = read_bound(config['native_arms'][role]['source_manifest'],
                                  out / (role + '-source.json') if out else None)
        manifest = json.loads(manifest_body)
        inventory = manifest['source_sha256']
        stats.require(manifest['schema'] == 'borsuk-native-semantic-router-source-manifest-v1'
                      and type(manifest['source_file_count']) is int
                      and manifest['source_file_count'] == len(inventory) == 399, 'complete 399 source inventory')
        for name, digest in inventory.items():
            stats.require(type(name) is str and not Path(name).is_absolute()
                          and '..' not in Path(name).parts
                          and (name.endswith('.rs') or Path(name).name in ('Cargo.toml', 'Cargo.lock')), 'native source path')
            stats.digest(digest)
        stats.require(sha_body(worker.encoded(inventory).encode()) == manifest['source_identity_sha256']
                      == SOURCE_IDS[role], 'native source aggregate/role')
        manifests[role] = manifest
        bodies[role + '-source.json'] = manifest_body
        bodies[role + '-checker.py.gz'] = archive
    a, b = (manifests[r]['source_sha256'] for r in ROLES)
    stats.require(set(a) == set(b) and {name for name in a if a[name] != b[name]} == NATIVE_DELTA, 'two-file native intervention')
    return dict(authority=authority, manifests=manifests, checkers=checkers, bodies=bodies)


def validate_config(config):
    offered = config['schema'] == OFFERED_SCHEMA
    stats.require(config['schema'] in (SCHEMA, OFFERED_SCHEMA) and config.get('authority_pending', False) is False, 'config schema/authority')
    for name, value in (OFFERED_FIXED if offered else FIXED).items():
        stats.require(type(config[name]) is type(value) and config[name] == value, 'fixed serial contract: ' + name)
    if offered:
        stats.require(all(type(v) in (int, float) for v in config['offered_qps']), 'offered rate types')
    stats.require(config['credential_protocol'] == stats.CREDENTIAL_PROTOCOL, 'credential protocol')
    stats.require(set(config['code_sha256']) == set(OFFERED_CODE if offered else CODE), 'complete runtime code closure')
    for name, digest in config['code_sha256'].items():
        stats.require(old.sha(ROOT / name) == stats.digest(digest), 'runtime code identity: ' + name)
    stats.require(all(type(config[k]) is str and config[k] for k in ('bucket', 'region')), 'store location')
    stats.require(set(config['native_arms']) == set(ROLES), 'native roles')
    for role, arm in config['native_arms'].items():
        stats.require(set(arm) == {'source_manifest', 'binary', 'proof'}, 'native arm pointers')
        for identity in arm.values():
            pointer(identity)
        stats.require({k: arm['binary'][k] for k in ('bytes', 'sha256')} == BINARY_IDS[role], 'fixed role binary')
    pointer(config['checker_authority'])
    stats.require([i['dataset'] for i in config['items']] == list(worker.DATASETS), 'dataset order')
    for item in config['items']:
        stats.require(type(item['rows']) is type(item['dimensions']) is int
                      and (item['rows'], item['dimensions'], item['metric']) == (100000, 768, 'cosine'), 'FIRST100k D768 cosine')
        stats.require(item['query_split'] == 'FIRST100k D768 cosine consumed development ordinals0..63', 'frozen development split')
        stats.require(set(item['inputs']) == {'requests', 'truth'} and set(item['arms']) == set(ROLES), 'panel inputs/roles')
        identity = item['source_identity']
        stats.require(set(identity) == worker.SOURCE_IDENTITIES, 'source/SQ8 identities')
        for value in identity.values():
            stats.digest(value)
        stats.require(identity['queries_sha256'] == worker.input_sha(item['inputs']['requests'])
                      and identity['truth_sha256'] == worker.input_sha(item['inputs']['truth']), 'consumed panel identity')
        stats.require(item['arms']['control'] == item['arms']['candidate'], 'same immutable semantic index/head/reference')
        arm = item['arms']['control']
        stats.require(arm['discovery'] == 'semantic', 'both roles must use semantic discovery')
        stats.require(set(arm['authority']) == {'root_sha256', 'generation', 'control_epoch'}, 'generation authority')
        stats.digest(arm['authority']['root_sha256'])
        for key in ('generation', 'control_epoch'):
            stats.integer(arm['authority'][key], key, 1)
        stats.require(set(arm['indexes']) == {'10'} and type(arm['indexes']['10']) is str and arm['indexes']['10'], 'k10 index')
        stats.validate_roster(arm)
        stats.require(arm['metadata_sha256']['plane/mean.bin'] == identity['mean_sha256'], 'mean identity')
        stats.require(set(arm['inputs']) == {'reference-k10'}, 'fixed k10 reference')
        worker.input_sha(arm['inputs']['reference-k10'])


def validate_runtime(config, binaries, proofs, evidence):
    stats.require(set(binaries) == set(proofs) == set(ROLES), 'runtime roles')
    qualified = {}
    for role in ROLES:
        arm = config['native_arms'][role]
        proof = json.loads(read_bound(arm['proof'], proofs[role]))
        stats.require(proof['qualified'] is True and proof['full_workspace_execution_pending'] is False
                      and proof['current_full_suite_pass_claim'] is True, 'completed qualified role proof')
        for name in PROOF_GATES:
            stats.require(type(proof[name]) is int and proof[name] == 0, 'typed completed proof gate: ' + name)
        binary = Path(binaries[role])
        stats.require(binary.stat().st_size == proof['binary_bytes'] == arm['binary']['bytes']
                      and type(proof['binary_bytes']) is int
                      and old.sha(binary) == proof['binary_sha256'] == arm['binary']['sha256'], 'qualified role binary identity')
        inventory = evidence['manifests'][role]['source_sha256']
        stats.require(type(proof['source_file_count']) is int and proof['source_file_count'] == len(inventory) == 399
                      and proof['source_identity_sha256'] == SOURCE_IDS[role], 'proof source identity/role')
        compiled = {n: d for n, d in inventory.items() if n in ('Cargo.toml', 'Cargo.lock', 'crates/borsuk/Cargo.toml')
                    or n.startswith(('crates/borsuk/src/', 'crates/borsuk/examples/'))}
        stats.require(proof['compiled_native_sha256'] == compiled, 'complete compiled native subset')
        qualified[role] = proof
    return qualified


def binding(config, config_sha, role, evidence):
    arm = config['native_arms'][role]
    return dict(native_role=role, config_sha256=config_sha, binary_sha256=arm['binary']['sha256'],
                binary_bytes=arm['binary']['bytes'], proof_sha256=arm['proof']['sha256'],
                source_manifest_sha256=arm['source_manifest']['sha256'], native_source_identity_sha256=SOURCE_IDS[role],
                checker_sha256=evidence['authority']['files'][role]['sha256'], checker_authority_sha256=AUTHORITY_SHA)


def prepare(config, output, evidence, *, fetch=None):
    panels = worker.prepare(config, output, fetch=fetch)
    for panel in panels.values():
        a, b = (panel['arms'][role] for role in ROLES)
        stats.require(a['bodies'] == b['bodies'] and a['references'] == b['references'], 'identical consumed role requests/references')
        for role in ROLES:
            arm = panel['arms'][role]
            for row in arm['references']:
                evidence['checkers'][role].validate_query(dict(row, authority=arm['arm']['authority']), arm['arm'], telemetry=False)
            arm['arm']['native_role'] = role
    return panels


def reduce_run(records, panels, config, config_sha, evidence, before, after):
    validate_record = worker.validate_record

    def checked(record, *args, **kwargs):
        with scoped(stats=evidence['checkers'][record['arm']]):
            return validate_record(record, *args, **kwargs)

    for record in records:
        stats.integer(record['block'], 'ABBA block', maximum=3)
        stats.integer(record['query_ordinal'], 'query ordinal', maximum=63)
        role = record['arm']
        stats.require(all(record.get(k) == v for k, v in binding(config, config_sha, role, evidence).items()), 'row role/execution identity')
        if record['outcome'] == 'aborted':
            continue
        stats.require(record['port'] == 8080, 'serial owned port')
        actual = worker.offered_resources(before, record['cgroup_after'], [record], config)
        stats.require(record['resource_gate'] == actual, 'raw resource gate parity')
        stats.require(record['cleanup_gate_passed'] == worker.cleanup_confirmed(record), 'raw cleanup gate parity')
        if record['outcome'] == 'success':
            stats.require(actual['passed'] and record['cleanup_gate_passed'], 'per-call resource/cleanup gate')
        else:
            panel, q = panels[record['dataset']], record['query_ordinal']
            arm = panel['arms'][role]
            evidence['checkers'][role].validate_failed_record(record, config, arm['arm'], arm['bodies'][q],
                                                            arm['references'][q], panel['truths'][q])
    with scoped(validate_record=checked):
        summary = worker.reduce_run(records, panels, config)
    resource_gate = worker.offered_resources(before, after, [r for r in records if r['outcome'] != 'aborted'], config)
    summary.update(schema=RESULT_SCHEMA, control='fresh matched qualified metadata-HEAD binary on the identical immutable semantic generation',
                   checker_authority_sha256=AUTHORITY_SHA, config_sha256=config_sha,
                   native_arms={role: binding(config, config_sha, role, evidence) for role in ROLES},
                   code_sha256=config['code_sha256'], resource_gate=resource_gate,
                   campaign_cgroup_before=before, campaign_cgroup_after=after,
                   bounded_memory_gate_passed=resource_gate['passed'],
                   bounded_memory_evidence='strict shared cgroup baseline and every call snapshot; per-native GNU time RSS',
                   paired_metadata_only=True, historical_matched_control=False,
                   speedup_claim=False, sustainable_qps='UNKNOWN', cost='UNKNOWN', vendor_comparison='UNKNOWN')
    summary.pop('latency_improvement')
    for dataset, value in summary['datasets'].items():
        value.pop('latency_improvement')
        value['quality_delta_percentage_points'] = (100 * value['quality_delta_at_10']
            if value['all_256_calls_successful'] else 'UNMEASURED')
        control, candidate = (value['pooled'][r]['latency_ms']['whole_cold'] for r in ROLES)
        value['latency_gate_passed'] = (value['all_256_calls_successful']
            and candidate['p90'] < control['p90'] and candidate['p95'] <= control['p95'])
        for group, aggregates in (('blocks', value['blocks']), ('pooled', value['pooled'])):
            for name, aggregate in aggregates.items():
                role = 'candidate' if name.startswith('candidate') else 'control'
                rows = [r for r in records if r['dataset'] == dataset and r['arm'] == role
                        and (group == 'pooled' or worker.BLOCKS[r['block']] == name) and r['outcome'] == 'success']
                metadata_tails(aggregate, rows, role)
    summary['latency_gate_passed'] = all(v['latency_gate_passed'] for v in summary['datasets'].values())
    summary['execution_gate_passed'] = (summary['all_calls_successful'] and summary['quality_gate_passed']
                                        and summary['process_cleanup_complete'] and resource_gate['passed'])
    summary['paired_gate_passed'] = summary['execution_gate_passed'] and summary['latency_gate_passed']
    return summary


def run(config, binaries, panels, output, config_sha, evidence, before):
    measured, reducer, encoded = worker.measured_call, worker.reduce_run, worker.encoded

    def measure(unused, config, arm, body, expected, truth):
        role = arm['native_role']
        with scoped(stats=evidence['checkers'][role]):
            record = measured(binaries[role], dict(config, schema=worker.OFFERED_SCHEMA), arm, body, expected, truth)
        record.update(port=8080, **binding(config, config_sha, role, evidence))
        record['resource_gate'] = worker.offered_resources(before, record['cgroup_after'], [record], config)
        record['cleanup_gate_passed'] = worker.cleanup_confirmed(record)
        if record['outcome'] == 'success' and (not record['resource_gate']['passed'] or not record['cleanup_gate_passed']):
            record.update(outcome='failed', error_type='ValueError',
                          error='resource/cleanup gate: ' + record['resource_gate'].get('error', 'cleanup incomplete'))
        return record

    # Reuse the only serial scheduler. Restore all hooks even if reduction fails.
    def reduce(records, panels, config):
        with scoped(reduce_run=reducer):
            return reduce_run(records, panels, config, config_sha, evidence, before, worker.offered_cgroup_snapshot())

    def bound_row(value):
        if isinstance(value, dict) and {'dataset', 'block', 'arm', 'query_ordinal', 'outcome'} <= value.keys():
            value.update(binding(config, config_sha, value['arm'], evidence))
        return encoded(value)

    with scoped(measured_call=measure, reduce_run=reduce, encoded=bound_row):
        return worker.run(config, None, panels, output)


def input_receipts(panels):
    # Paths in scientific receipts are relative to the retained output bundle.
    return {d: dict(common={n: dict(v, path=f'inputs/{d}/{n}') for n, v in p['inputs'].items()},
                    arms={a: {n: dict(v, path=f'inputs/{d}/{a}/{n}') for n, v in arm['inputs'].items()}
                          for a, arm in p['arms'].items()}) for d, p in panels.items()}


def metadata_tails(aggregate, rows, role):
    aggregate['metadata_object_wait_sum_ms'] = {k: aggregate['latency_ms'].pop(k) for k in
        ('metadata_HEAD', 'metadata_GET_headers', 'metadata_stream_and_output', 'metadata_awaited_writes')}
    aggregate['latency_ms']['metadata_wave_critical'] = (worker.tails([
        sum({m['metadata_wave']: m['metadata_wave_wall_ns'] for m in
             r['native_header']['remote_open_stats']['metadata']}.values()) / 1e6
        for r in rows if r['outcome'] == 'success']) if role == 'candidate' else 'UNMEASURED')
    aggregate['metadata_payload_buffer_bound_bytes'] = max(
        (r['startup_accounting']['metadata']['payload_buffer_bound_bytes'] for r in rows
         if r['outcome'] == 'success'), default='UNMEASURED')


def native_transport_failure(row, config, arm, body, expected, truth, checker, port):
    """Only a coherent raw search failure entirely accounted by physical errors."""
    try:
        stats.require(row['outcome'] == 'failed' and row['http_status'] == 502
                      and row['raw_response_complete'] is True
                      and not row['telemetry_validation_errors'], 'accounted native failure')
        checker.validate_failed_record(row, config, arm, body, expected, truth, port=port)
        response = row['response']
        stats.require(response['error'] == 'search_failed', 'native search failure')
        accounting = checker.validate_outcome(row['native_header'], response, arm, False)
        stats.require(row['accounting'] == accounting, 'raw failure accounting')
        before, after = accounting['startup_transport'], accounting['final_process_transport']
        previous = dict(before['status_counts'])
        errors = {status: count - previous.get(status, 0) for status, count in after['status_counts']
                  if not 200 <= status < 300 and count > previous.get(status, 0)}
        stats.require(set(errors) <= {408, 429, 500, 502, 503, 504}, 'non-transient native status')
        physical = (sum(errors.values()) + after['transport_failures'] - before['transport_failures']
                    + after['stream_failures'] - before['stream_failures'])
        logical = sum(response[p + 'failed_gets'] for p in ('', 'source_', 'router_'))
        return physical > 0 and physical == logical
    except (ValueError, KeyError, TypeError, ArithmeticError):
        return False


@contextmanager
def offered_hooks(config, config_sha, evidence, binaries=None, proofs=None):
    """Install hooks once; each thread selects its frozen checker locally."""
    local = threading.local()
    class Checkers:
        def __getattr__(self, name):
            return getattr(evidence['checkers'][getattr(local, 'role', 'candidate')], name)
    measured, reducer, transport = worker.measured_call, worker.reduce_offered_cell, worker.transport_failure

    def measure(unused, config, arm, body, expected, truth, *, port):
        local.role = arm['native_role']
        try:
            row = measured(binaries[local.role], dict(config, schema=worker.OFFERED_SCHEMA),
                           arm, body, expected, truth, port=port)
            row.update(binding(config, config_sha, local.role, evidence))
            row['authenticated_native_transport_failure'] = False
            if row['outcome'] == 'failed':
                row['authenticated_native_transport_failure'] = native_transport_failure(
                    row, config, arm, body, expected, truth, evidence['checkers'][local.role], port)
            return row
        finally:
            del local.role

    def reduce_cell(rows, panel, config):
        role = rows[0]['arm']
        local.role = role
        try:
            for row in rows:
                expected = binding(config, config_sha, role, evidence)
                # Unattempted offers receive the same authority before ledger closure.
                if 'native_role' not in row and (row['port'] is None or row.get('failure_stage') == 'thread_start'):
                    row.update(expected)
                stats.require(all(row.get(k) == v for k, v in expected.items()), 'offered row role identity')
                if row['outcome'] == 'failed' and row.get('failure_stage') != 'thread_start':
                    q, arm = row['query_ordinal'], panel['arms'][role]
                    actual = native_transport_failure(row, config, arm['arm'], arm['bodies'][q],
                        arm['references'][q], panel['truths'][q], evidence['checkers'][role], row['port'])
                    stats.require(row.get('authenticated_native_transport_failure') is actual,
                                  'native transport classification parity')
            cell = reducer(rows, panel, config)
            metadata_tails(cell, rows, role)
            cell.update(success_conditioned_latency=True,
                        response_tail_boundary='wire response completion; excludes validation and cleanup')
            return cell
        finally:
            del local.role

    def qualify(*unused):
        return validate_runtime(config, binaries, proofs, authenticate(config))

    with scoped(stats=Checkers(), measured_call=measure, reduce_offered_cell=reduce_cell,
                transport_failure=lambda row: transport(row) or row.get('authenticated_native_transport_failure') is True,
                closed_cell_summary=lambda cell, cfg, sha: closed_cell_summary(cell, cfg, sha, evidence),
                validate_config=validate_config, validate_runtime=qualify):
        yield


def closed_cell_summary(cell, config, digest, evidence):
    return dict(cell, schema='borsuk-native-semantic-metadata-waves-cold-offered-cell-v1',
                **binding(config, digest, cell['arm'], evidence), code_sha256=config['code_sha256'],
                qualification_sha256=config['native_arms'][cell['arm']]['proof']['sha256'],
                native_source_file_count=399, closed=cell['process_cleanup_complete'],
                bounded_memory_gate_passed=cell['resource_gate']['passed'],
                records=dict(bytes=cell['records_bytes'], sha256=cell['records_sha256']))


def reduce_offered(records, panels, config, config_sha, evidence, before, after):
    with offered_hooks(config, config_sha, evidence):
        summary = worker.reduce_offered(records, panels, config)
    return offered_summary(summary, config, config_sha, evidence, before, after)


def offered_summary(summary, config, config_sha, evidence, before, after):
    resource = worker.offered_resources(before, after, [], config)
    summary.update(schema=OFFERED_RESULT_SCHEMA, config_sha256=config_sha,
                   checker_authority_sha256=AUTHORITY_SHA,
                   native_arms={r: binding(config, config_sha, r, evidence) for r in ROLES},
                   code_sha256=config['code_sha256'], campaign_cgroup_before=before, campaign_cgroup_after=after,
                   resource_gate=resource, bounded_memory_gate_passed=summary['bounded_memory_gate_passed'] and resource['passed'],
                   paired_metadata_only=True, historical_matched_control=False, speedup_claim=False)
    summary['execution_gate_passed'] = (summary['execution_gate_passed'] and
        summary['identity_gate_passed'] and resource['passed'])
    summary.pop('latency_improvement')
    return summary


def run_offered(config, binaries, proofs, panels, output, config_sha, evidence, before,
                *, worker_started_ns, on_cell_closed=None):
    def closed(marker, paths):
        stats.require(marker['config_sha256'] == config_sha, 'cell config binding')
        if on_cell_closed is not None:
            on_cell_closed(marker, paths)
    with offered_hooks(config, config_sha, evidence, binaries, proofs):
        summary = worker.run_offered(config, None, panels, output, worker_started_ns=worker_started_ns,
            config_sha=config_sha, proof_path=proofs, campaign_cgroup_before=before, on_cell_closed=closed)
    return offered_summary(summary, config, config_sha, evidence, before, worker.offered_cgroup_snapshot())


def main(argv=None, *, on_cell_closed=None):
    worker_started_ns = worker.time.monotonic_ns()
    args = sys.argv[1:] if argv is None else argv
    if args == ['--self-check']:
        from scripts.check_native_semantic_metadata_cold import self_check
        self_check()
        return 0
    if args == ['--offered-self-check']:
        from scripts.check_native_semantic_metadata_cold import self_check
        self_check(offered=True)
        return 0
    stats.require(len(args) == 7, 'usage: CONFIG CONFIG_SHA CONTROL_BINARY CONTROL_PROOF CANDIDATE_BINARY CANDIDATE_PROOF NEW_OUTPUT')
    config_path, digest, cb, cp, nb, np, output = args
    stats.require(old.sha(config_path) == stats.digest(digest), 'config identity')
    config = json.loads(Path(config_path).read_text())
    offered = config['schema'] == OFFERED_SCHEMA
    validate_config(config)
    evidence = authenticate(config)
    binaries, proofs = dict(control=cb, candidate=nb), dict(control=cp, candidate=np)
    validate_runtime(config, binaries, proofs, evidence)  # Pending qualification stops BEFORE preparation.
    stats.require(sorted(os.sched_getaffinity(0)) == config['client_cpu_affinity'], 'client CPU affinity')
    stats.require(resource.getrlimit(resource.RLIMIT_AS) == (config['native_rlimit_as_bytes'],) * 2, 'inherited native address-space cap')
    stats.require(os.environ.get('AWS_MAX_ATTEMPTS') == '1', 'one AWS attempt')
    out = Path(output).absolute()
    out.mkdir(exist_ok=False)
    (out / 'config.json').write_bytes(Path(config_path).read_bytes())
    for name, body in evidence['bodies'].items():
        (out / name).write_bytes(body)
    for role in ROLES:
        (out / (role + '-proof.json')).write_bytes(read_bound(config['native_arms'][role]['proof'], proofs[role]))
    summary = dict(schema=OFFERED_RESULT_SCHEMA if offered else RESULT_SCHEMA, config_sha256=digest, identity_gate_passed=False,
                   execution_gate_passed=False, paired_gate_passed=False, closed=False)
    try:
        before = worker.offered_cgroup_snapshot()
        gate = worker.offered_resources(before, before, [], config)
        summary.update(campaign_cgroup_before=before, pre_admission_resource_gate=gate)
        stats.require(gate['passed'], 'pre-admission resource proof: ' + gate.get('error', ''))
        panels = prepare(config, out, evidence)
        summary = (run_offered(config, binaries, proofs, panels, out, digest, evidence, before,
                              worker_started_ns=worker_started_ns, on_cell_closed=on_cell_closed)
                   if offered else run(config, binaries, panels, out, digest, evidence, before))
        for panel in panels.values():
            for identity in [*panel['inputs'].values(), *(v for arm in panel['arms'].values() for v in arm['inputs'].values())]:
                stats.require(Path(identity['path']).stat().st_size == identity['bytes'] and old.sha(identity['path']) == identity['sha256'], 'prepared input changed')
        stats.require(old.sha(config_path) == digest, 'config changed')
        validate_config(config)
        validate_runtime(config, binaries, proofs, authenticate(config))
        validate_runtime(config, binaries, {r: out / (r + '-proof.json') for r in ROLES}, authenticate(config, out))
        summary.update(inputs=input_receipts(panels), identity_gate_passed=summary.get('identity_gate_passed', True),
                       closed=summary['process_cleanup_complete'])
    except Exception as error:
        summary.update(terminal_error=dict(type=type(error).__name__, message=str(error)),
                       identity_gate_passed=False, execution_gate_passed=False, paired_gate_passed=False)
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
    print(worker.encoded({k: summary.get(k, False) for k in ('closed', 'execution_gate_passed', 'paired_gate_passed', 'identity_gate_passed')}))
    if offered:
        return 0 if summary.get('execution_gate_passed') and summary.get('identity_gate_passed') and summary.get('closed') else 1
    return 0 if summary.get('paired_gate_passed') and summary.get('identity_gate_passed') and summary.get('closed') else 1


if __name__ == '__main__':
    raise SystemExit(main())
