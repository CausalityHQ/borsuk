#!/usr/bin/env python3
"""Fixed FIRST100k ABBA cold HTTP runtime; this is not launch authority.

CLI: python3 -m scripts.run_native_semantic_router_cold CONFIG CONFIG_SHA BINARY PROOF NEW_OUTPUT
     python3 -m scripts.run_native_semantic_router_cold --self-check

Config contract (all SHA256 values are lowercase hexadecimal):
  schema=borsuk-native-semantic-router-cold-v1; count=64; k=10;
  dataset_order=[ReLAION,CoHere]; blocks=[control0,candidate1,candidate2,control3];
  bucket, region, native_memory_bytes; credential_protocol=instance-imdsv2;
  client_cpu_affinity=[4,5];
  native_cpu_affinity=[0,1,2,3]; binary={bytes,sha256}; qualification_sha256;
  code_sha256={every CODE path:SHA}; native_source_file_count;
  native_source_identity_sha256 (SHA of sorted compact JSON native_inventory()).
  items, in dataset order: {dataset,rows:100000,dimensions:768,metric:cosine,
    query_split,inputs:{requests,truth},source_identity:{source_sha256,
      source_order_sha256,mean_sha256,records_sha256,sq8_sha256,
      sq8_codec_tables_sha256,queries_sha256,truth_sha256},
    arms:{control:ARM,candidate:ARM}}.
  ARM={discovery:graph|semantic,authority:{root_sha256,generation,control_epoch},
    indexes:{"10":PREFIX},head_file:{bytes,sha256},metadata_files:{path:bytes},
    metadata_sha256:{path:SHA},inputs:{reference-k10,optional actual-k100}};
    semantic additionally leaf_object={bytes,sha256}. See validate_roster for
    the exact v7 startup roster: source records and selected leaves are excluded.
  Each input uses the existing fetch identity: {key,bytes,sha256}, optionally
    {range_start,range_bytes,range_sha256}. Requests are 64 JSONL objects with
    query_ordinal=0..63 and query[768]. Truth is exactly 64*100 little-endian u32.
  Each reference is 66 JSONL objects: header {top_k,declared_panel_count:64,
    rows:100000,dimensions:768,metric:cosine,discovery,authority,query_split,
    source_identity}; 64 rows {query_ordinal,...PARITY}; footer {count:64}.
    actual-k100 rows contain 100 actual ordered ids (not inferred from k10);
    its header also has measurement=actual-k100 and execution_sha256.
  PROOF is SHA-pinned by config and uses the existing qualified/green_status/
    release_status/binary_sha256/compiled_native_sha256/source_file_count/
    source_identity_sha256 contract. Both arms run the SAME qualified binary.

Preparation fetches only pinned panel/reference envelopes. No provisioning,
controller, qualification build, retry, cap expansion or offered-load estimate.
The first failed call or complete candidate block below 608/640 aborts every
remaining fixed position. records.jsonl still contains the exact 512-position
roster; attempted HTTP/process counts are actual, including zero-HTTP failures.
Process transport includes the shared SDK connector's PUT token and two GET
credential calls. Credential payload bytes are inferred by subtracting verified
S3 startup bytes from the ready total; credential values are never read here.
"""
from pathlib import Path
import sys
from contextlib import contextmanager
from decimal import Decimal
import base64
import hashlib
import io
import json
import math
import os
import struct
from types import SimpleNamespace

if not __debug__:
    raise RuntimeError('the reused cold-call harness requires Python assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_native_cold_first_query as old
from scripts import check_native_semantic_router_stats as stats

CODE = (*old.CODE, 'scripts/check_native_semantic_router_stats.py',
        'scripts/run_native_semantic_router_cold.py')
SCHEMA = 'borsuk-native-semantic-router-cold-v1'
DATASETS = ('ReLAION', 'CoHere')
BLOCKS = ('control0', 'candidate1', 'candidate2', 'control3')
SOURCE_IDENTITIES = {'source_sha256', 'source_order_sha256', 'mean_sha256', 'records_sha256',
                     'sq8_sha256', 'sq8_codec_tables_sha256', 'queries_sha256', 'truth_sha256'}
_cold_call, _post = old.cold_call, old.post


def execution_order():
    for dataset in DATASETS:
        for block, arm in enumerate(('control', 'candidate', 'candidate', 'control')):
            for ordinal in range(64):
                yield dataset, block, arm, ordinal


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def input_sha(identity):
    for name in ('sha256', *(['range_sha256'] if 'range_bytes' in identity else [])):
        stats.digest(identity[name])
    stats.integer(identity['bytes'], 'input bytes', 1)
    stats.require(type(identity['key']) is str and bool(identity['key']), 'input key')
    if 'range_bytes' in identity:
        stats.integer(identity['range_start'], 'input range start')
        stats.integer(identity['range_bytes'], 'input range length', 1)
        stats.require(identity['range_start'] + identity['range_bytes'] <= identity['bytes'], 'input range')
    return identity.get('range_sha256', identity['sha256'])


def native_inventory():
    paths = set(Path('.').rglob('*.rs')) | set(Path('.').rglob('Cargo.toml')) | set(Path('.').rglob('Cargo.lock'))
    return {str(path): old.sha(path) for path in sorted(paths)
            if not {'.git', 'target'}.intersection(path.parts)}


def validate_config(config):
    stats.require(config['schema'] == SCHEMA, 'config schema')
    stats.require(config.get('credential_protocol') == stats.CREDENTIAL_PROTOCOL, 'unsupported credential protocol')
    stats.require(type(config['count']) is type(config['k']) is int
                  and (config['count'], config['k']) == (64, 10), 'fixed 64/k10 panel')
    stats.require(config['dataset_order'] == list(DATASETS) and config['blocks'] == list(BLOCKS),
                  'fixed dataset/ABBA order; old 32-request blocks are incompatible')
    stats.require(config['client_cpu_affinity'] == [4, 5]
                  and config['native_cpu_affinity'] == [0, 1, 2, 3], 'cold-call CPU contract')
    stats.integer(config['native_memory_bytes'], 'native memory admission', 1)
    stats.integer(config['binary']['bytes'], 'binary bytes', 1)
    stats.digest(config['binary']['sha256'])
    stats.digest(config['qualification_sha256'])
    stats.digest(config['native_source_identity_sha256'])
    stats.integer(config['native_source_file_count'], 'source roster count', 1)
    stats.require(set(config['code_sha256']) == set(CODE), 'complete runtime code closure')
    for name, digest in config['code_sha256'].items():
        stats.require(old.sha(name) == stats.digest(digest), 'runtime code identity: ' + name)
    stats.require(all(type(config[k]) is str and config[k] for k in ('bucket', 'region')), 'store location')
    stats.require([i['dataset'] for i in config['items']] == list(DATASETS), 'dataset items')
    for item in config['items']:
        stats.require((item['rows'], item['dimensions'], item['metric']) == (100000, 768, 'cosine')
                      and type(item['rows']) is type(item['dimensions']) is int, 'FIRST100k D768 cosine')
        stats.require(type(item['query_split']) is str and bool(item['query_split']), 'query split')
        stats.require(set(item['inputs']) == {'requests', 'truth'} and set(item['arms']) == {'control', 'candidate'},
                      'panel inputs/arms')
        identity = item['source_identity']
        stats.require(set(identity) == SOURCE_IDENTITIES, 'scorer identity roster')
        for digest in identity.values():
            stats.digest(digest)
        stats.require(identity['queries_sha256'] == input_sha(item['inputs']['requests'])
                      and identity['truth_sha256'] == input_sha(item['inputs']['truth']), 'consumed panel identity')
        for name, arm in item['arms'].items():
            stats.require(arm['discovery'] == ('graph' if name == 'control' else 'semantic'), 'arm discovery')
            stats.require(set(arm['authority']) == {'root_sha256', 'generation', 'control_epoch'}, 'authority fields')
            stats.digest(arm['authority']['root_sha256'])
            for key in ('generation', 'control_epoch'):
                stats.integer(arm['authority'][key], key, 1)
            stats.require(set(arm['indexes']) == {'10'} and type(arm['indexes']['10']) is str
                          and bool(arm['indexes']['10']), 'pinned k10 index')
            stats.validate_roster(arm)
            stats.require(set(arm['inputs']) in ({'reference-k10'}, {'reference-k10', 'actual-k100'}), 'arm references')
            for reference in arm['inputs'].values():
                input_sha(reference)
            stats.require(arm['metadata_sha256']['plane/mean.bin'] == identity['mean_sha256'], 'mean identity')
        control, candidate = item['arms']['control'], item['arms']['candidate']
        stats.require(control['authority']['root_sha256'] != candidate['authority']['root_sha256']
                      and control['indexes']['10'] != candidate['indexes']['10'], 'distinct graph/semantic roots')
        for name in stats.COMMON_FILES - {'manifest.json'}:
            stats.require(control['metadata_files'][name] == candidate['metadata_files'][name]
                          and control['metadata_sha256'][name] == candidate['metadata_sha256'][name],
                          'unchanged source/SQ8 metadata: ' + name)


def validate_runtime(config, binary, proof_path):
    stats.require(old.sha(proof_path) == config['qualification_sha256'], 'qualification identity')
    proof = json.loads(Path(proof_path).read_text())
    stats.require(proof['qualified'] is True
                  and type(proof['green_status']) is type(proof['release_status']) is int
                  and proof['green_status'] == proof['release_status'] == 0, 'unqualified binary')
    stats.require(proof['binary_sha256'] == old.sha(binary) == config['binary']['sha256']
                  and Path(binary).stat().st_size == config['binary']['bytes'], 'same qualified binary')
    identities = native_inventory()
    digest = hashlib.sha256(encoded(identities).encode()).hexdigest()
    stats.require(len(identities) == proof['source_file_count'] == config['native_source_file_count']
                  and type(proof['source_file_count']) is int, 'current source roster count')
    stats.require(digest == proof['source_identity_sha256'] == config['native_source_identity_sha256'], 'native source identity')
    stats.require(bool(proof['compiled_native_sha256']), 'compiled source roster missing')
    for name, digest in proof['compiled_native_sha256'].items():
        stats.require(identities.get(name) == stats.digest(digest), 'compiled source: ' + name)
    return proof


def reference_envelope(path, item, arm, k):
    rows = [json.loads(line) for line in Path(path).read_text().splitlines()]
    stats.require(len(rows) == 66, 'reference envelope length')
    header, queries, tail = rows[0], rows[1:-1], rows[-1]
    expected = dict(top_k=k, declared_panel_count=64, rows=100000, dimensions=768, metric='cosine',
                    discovery=arm['discovery'], authority=arm['authority'], query_split=item['query_split'],
                    source_identity=item['source_identity'])
    stats.require(all(header[key] == value for key, value in expected.items()) and tail['count'] == 64,
                  'arm reference/scorer envelope')
    stats.require([r['query_ordinal'] for r in queries] == list(range(64))
                  and all(type(r['query_ordinal']) is int for r in queries), 'reference ordinals')
    if k == 100:
        stats.require(header['measurement'] == 'actual-k100', 'R100 needs actual k100 output')
        stats.digest(header['execution_sha256'])
        for row in queries:
            stats.require(len(row['ids']) == len(set(row['ids'])) == 100, 'actual k100 roster')
            for value in row['ids']:
                stats.integer(value, 'actual k100 ID', maximum=99999)
    else:
        for row in queries:
            stats.validate_query(dict(row, authority=arm['authority']), arm, telemetry=False)
    return queries


def prepare(config, output, *, fetch=None):
    """Reusable envelope preparation; does not start a native process or query."""
    if fetch is None:
        def fetch(bucket, identity, path):
            saved = os.environ.get('AWS_MAX_ATTEMPTS')
            try:
                os.environ['AWS_MAX_ATTEMPTS'] = '1'
                return old.fetch(bucket, identity, path)
            finally:
                if saved is None:
                    os.environ.pop('AWS_MAX_ATTEMPTS', None)
                else:
                    os.environ['AWS_MAX_ATTEMPTS'] = saved
    panels = {}
    for item in config['items']:
        dataset = item['dataset']
        directory = Path(output) / 'inputs' / dataset
        inputs = {name: fetch(config['bucket'], identity, directory / name)
                  for name, identity in item['inputs'].items()}
        requests = [json.loads(line) for line in Path(inputs['requests']['path']).read_text().splitlines()]
        stats.require(len(requests) == 64 and [r['query_ordinal'] for r in requests] == list(range(64))
                      and all(type(r['query_ordinal']) is int for r in requests), 'request ordinals')
        for request in requests:
            query = request['query']
            stats.require(len(query) == 768 and all(type(v) in (int, float) and math.isfinite(v) for v in query)
                          and any(v != 0 for v in query), 'query vector')
        truth_bytes = Path(inputs['truth']['path']).read_bytes()
        stats.require(len(truth_bytes) == 25600, '64 actual truth-at-100 rows')
        truths = [list(struct.unpack_from('<100I', truth_bytes, q * 400)) for q in range(64)]
        stats.require(all(len(set(t)) == 100 and max(t) < 100000 for t in truths), 'FIRST100k truth roster')
        panel = dict(item=item, inputs=inputs, truths=truths, arms={})
        for name, arm in item['arms'].items():
            references = {key: fetch(config['bucket'], identity, directory / name / key)
                          for key, identity in arm['inputs'].items()}
            rows = reference_envelope(references['reference-k10']['path'], item, arm, 10)
            bodies = [encoded(dict(query=r['query'], k=10, **arm['authority'])).encode() for r in requests]
            panel['arms'][name] = dict(arm=dict(arm, dataset=dataset), inputs=references,
                                       references=rows, bodies=bodies,
                                       actual_k100=(reference_envelope(references['actual-k100']['path'], item, arm, 100)
                                                    if 'actual-k100' in references else None))
        panels[dataset] = panel
    return panels


@contextmanager
def scoped_runner(config, arm, observed):
    spawn = old.subprocess.Popen

    def popen(*args, **kwargs):
        observed['namespace_start_attempted'] = True
        process = spawn(*args, **kwargs)
        observed['native_process_started'] = True
        return process

    def checked(response, expected, truth, authority):
        stats.require(authority == arm['authority'], 'scoped authority')
        return stats.validate_query(response, arm, expected=expected, truth=truth)['returned_hits']

    def startup(value, files, wall):
        stats.require(files == arm['metadata_files'], 'scoped metadata roster')
        return stats.validate_startup(value, arm, wall)

    def post(client, body):
        observed['http_attempts'] = 1
        status, raw = _post(client, body)
        # Retain the bytes without JSON/UTF-8 work before old.cold_call marks wire completion.
        observed.update(http_status=status, raw_response=raw)
        return status, raw

    replacements = dict(checked_response=checked, validate=startup, post=post,
                        subprocess=SimpleNamespace(Popen=popen, STDOUT=old.subprocess.STDOUT))
    saved = {name: getattr(old, name) for name in replacements}
    environment = {'BORSUK_NATIVE_MEMORY_BYTES': str(config['native_memory_bytes']), 'AWS_MAX_ATTEMPTS': '1'}
    saved_environment = {name: os.environ.get(name) for name in environment}
    try:
        for name, value in replacements.items():
            setattr(old, name, value)
        os.environ.update(environment)
        yield
    finally:
        for name, value in saved.items():
            setattr(old, name, value)
        for name, value in saved_environment.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def resources(log, limit):
    fields = {}
    for line in log.splitlines():
        for label, name in (('Maximum resident set size (kbytes):', 'rss_peak_bytes'),
                            ('User time (seconds):', 'cpu_user_seconds'), ('System time (seconds):', 'cpu_system_seconds')):
            if label in line:
                stats.require(name not in fields, 'duplicate native resource field')
                raw = line.split(label, 1)[1].strip()
                if name == 'rss_peak_bytes':
                    fields[name] = stats.integer(int(raw), name, 1) * 1024
                else:
                    value = Decimal(raw)
                    stats.require(value.is_finite() and value >= 0, 'native CPU time')
                    fields[name] = str(value)
    stats.require(set(fields) == {'rss_peak_bytes', 'cpu_user_seconds', 'cpu_system_seconds'}, 'native resource telemetry')
    stats.require(fields['rss_peak_bytes'] <= limit, 'native RSS cap')
    return dict(fields, native_memory_admission_bytes=limit, temporary_storage_peak_bytes='UNMEASURED')


def cgroup_snapshot():
    # Process-group totals can overlap the client; these are not per-query deltas.
    try:
        entry = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
        root = Path('/sys/fs/cgroup').resolve()
        directory = (root / entry.lstrip('/')).resolve()
        stats.require(directory.is_relative_to(root), 'cgroup path')
        files = {name: (directory / name).read_text().strip() for name in
                 ('memory.current', 'memory.peak', 'memory.events', 'memory.swap.current', 'cpu.stat', 'io.stat')}
        return dict(path=str(directory), scope='shared runtime cgroup cumulative snapshot', files=files)
    except (OSError, StopIteration, ValueError):
        return 'UNMEASURED'


def measured_call(binary, config, arm, body, expected, truth):
    failures, observed, record = io.StringIO(), dict(namespace_start_attempted=False, native_process_started=False, http_attempts=0), None
    before = cgroup_snapshot()
    try:
        with scoped_runner(config, arm, observed):
            record = _cold_call(binary, config, arm, body, expected, truth, failures)
        observed['raw_response_base64'] = base64.b64encode(observed['raw_response']).decode('ascii')
        observed['raw_response'] = observed['raw_response'].decode(errors='replace')
        record.update(outcome='success', **observed)
        record.pop('response')
        record['response'] = json.loads(record['raw_response'], parse_constant=lambda value: stats.require(False, 'nonfinite JSON: ' + value))
        stats.require(record['native_close']['intentional_stop'] is True, 'native cleanup')
        stats.require(record['returned_hits'] == stats.validate_query(record['response'], arm,
                      expected=expected, truth=truth)['returned_hits'], 'returned hit parity')
        record['accounting'] = stats.validate_outcome(record['native_header'], record['response'], arm, True)
        record['startup_accounting'] = stats.validate_ready(record['native_header'], arm)
        record['resources'] = resources(record['native_time_log'], config['native_memory_bytes'])
    except Exception as error:
        if isinstance(observed.get('raw_response'), bytes):
            observed['raw_response_base64'] = base64.b64encode(observed['raw_response']).decode('ascii')
            observed['raw_response'] = observed['raw_response'].decode(errors='replace')
        if record is None:
            rows = [json.loads(line) for line in failures.getvalue().splitlines()]
            stats.require(len(rows) <= 1, 'duplicate raw failure')
            record = rows[0] if rows else dict(http_attempts=0, native_close='UNMEASURED')
        record.update(outcome='failed', error_type=type(error).__name__, error=str(error), **observed)
        telemetry_errors = []
        try:
            headers = [json.loads(line) for line in record.get('native_server_log', '').splitlines() if line.startswith('{')]
            stats.require(len(headers) == 1, 'missing/ambiguous ready telemetry')
            record['native_header'] = headers[0]
            record['startup_accounting'] = stats.validate_ready(headers[0], arm)
            record.pop('response', None)
            response = json.loads(record.get('raw_response', ''), parse_constant=lambda value: stats.require(False, 'nonfinite JSON: ' + value))
            record['response'] = response
            record['accounting'] = stats.validate_outcome(headers[0], response, arm, False)
        except (ValueError, KeyError, TypeError) as telemetry_error:
            telemetry_errors.append(str(telemetry_error))
        record['telemetry_validation_errors'] = telemetry_errors
    record.update(cgroup_before=before, cgroup_after=cgroup_snapshot(), temporary_directory_cleanup=True,
                  expected_authority=arm['authority'], reference_response={k: expected[k] for k in stats.PARITY},
                  truth_at_10=list(truth[:10]), http_retry=False,
                  request_sha256=hashlib.sha256(body).hexdigest(), request_bytes=len(body),
                  raw_response_complete='raw_response' in observed)
    if 'resources' not in record:
        try:
            record['resources'] = resources(record.get('native_time_log', ''), config['native_memory_bytes'])
        except (ValueError, ArithmeticError) as error:
            record['resources'] = 'UNMEASURED'
            record['resource_validation_error'] = str(error)
    if record['outcome'] == 'success':
        try:
            validate_record(record, config, arm, body, expected, truth)
        except (ValueError, KeyError, TypeError) as error:
            record.update(outcome='failed', error_type=type(error).__name__, error=str(error),
                          telemetry_validation_errors=[str(error)])
    return record


def tails(values):
    return ({label: old.quantile(values, p) for label, p in (('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99))}
            if values else 'UNMEASURED')


def reduce_calls(records):
    successful = [r for r in records if r['outcome'] == 'success']
    metrics = {name: [] for name in ('whole_cold', 'http_first_query', 'incoming_http', 'cold_open', 'head_read',
               'metadata_staging', 'metadata_decode', 'metadata_HEAD', 'metadata_GET_headers',
               'metadata_stream_and_output', 'metadata_awaited_writes', 'source_head', 'router_head',
               'native_query', *stats.STAGES)}
    for record in successful:
        header, response = record['native_header'], record['response']
        for label, key in (('whole_cold', 'cold_start_to_first_http_response_ns'),
                           ('http_first_query', 'first_post_to_response_ns'), ('incoming_http', 'incoming_http_wall_ns')):
            metrics[label].append(record[key] / 1e6)
        metrics['cold_open'].append(header['remote_open_wall_ns'] / 1e6)
        metrics['head_read'].append(header['head_read_wall_ns'] / 1e6)
        for label, key in (('metadata_staging', 'staging_wall_ns'), ('metadata_decode', 'decode_wall_ns'),
                           ('source_head', 'source_head_wall_ns'), ('router_head', 'router_head_wall_ns')):
            metrics[label].append(header['remote_open_stats'][key] / 1e6)
        for label, key in (('metadata_HEAD', 'head_wall_ns'), ('metadata_GET_headers', 'get_wall_ns'),
                           ('metadata_stream_and_output', 'stream_wall_ns'), ('metadata_awaited_writes', 'write_wall_ns')):
            metrics[label].append(sum(row[key] for row in header['remote_open_stats']['metadata']) / 1e6)
        metrics['native_query'].append(response['native_wall_ns'] / 1e6)
        for name in stats.STAGES:
            stage = response['query_stages'][name]
            metrics[name].append((stage['end_ns'] - stage['start_ns']) / 1e6)
    hits = sum(r['returned_hits'] for r in successful)
    accounted = [r for r in records if 'accounting' in r]
    opened = [r for r in records if 'startup_accounting' in r]
    native_resources = [r['resources'] for r in records if isinstance(r.get('resources'), dict)]
    transport = [r['accounting']['final_process_transport'] if 'accounting' in r else
                 r['startup_accounting']['startup_transport'] for r in opened]
    known_totals = {name: sum(t[name] for t in transport) for name in
                    ('attempts', 'consumed_payload_bytes', 'transport_failures', 'stream_failures', 'dropped_error_bodies')}
    known_totals['method_counts'] = [sum(t['method_counts'][i] for t in transport) for i in range(10)]
    logical = {prefix + name: sum(r['response'].get(prefix + name, 0) for r in accounted)
               for prefix in ('source_', '', 'router_') for name in stats.COUNTERS}
    logical.update(metadata_GETs=sum(r['startup_accounting']['metadata']['logical_metadata_get_requests'] for r in opened),
        metadata_HEADs=sum(r['startup_accounting']['metadata']['logical_metadata_head_requests'] for r in opened),
        metadata_bytes=sum(r['startup_accounting']['metadata']['metadata_bytes'] for r in opened),
        authority_head_JSON_GETs=len(opened), source_HEADs=len(opened),
        router_HEADs=sum(r['startup_accounting']['metadata']['router_head_requests'] for r in opened))
    return dict(count=len(records), successes=len(successful), failures=sum(r['outcome'] == 'failed' for r in records),
                aborted=sum(r['outcome'] == 'aborted' for r in records),
                failed_calls=sum(r['outcome'] == 'failed' for r in records),
                returned_hits=hits, recall_at_10=(hits / (10 * len(records)) if len(successful) == len(records) else 'UNMEASURED'),
                success_conditioned_recall_at_10=(hits / (10 * len(successful)) if successful else 'UNMEASURED'),
                per_query_recall_at_10_p05=(old.quantile([r['returned_hits'] / 10 for r in successful], .05)
                                           if successful else 'UNMEASURED'),
                success_conditioned_latency=len(successful) != len(records),
                latency_ms={name: tails(values) for name, values in metrics.items()},
                accounting_observations=len(accounted), accounting_complete=len(accounted) == len(records),
                startup_accounting_observations=len(opened),
                transport_totals_scope='sum latest validated process snapshot; incomplete final snapshots are lower bounds',
                known_logical_charge_totals=logical, known_process_transport_totals=known_totals,
                credential_transport_totals=dict(protocol=stats.CREDENTIAL_PROTOCOL,
                    declared_credential_submissions=sum(r['startup_accounting']['declared_credential_submissions'] for r in opened),
                    inferred_credential_consumed_bytes=sum(r['startup_accounting']['inferred_credential_consumed_bytes'] for r in opened),
                    payload_attribution=stats.CREDENTIAL_PAYLOAD_ATTRIBUTION),
                native_resource_observations=len(native_resources),
                native_peak_RSS_bytes=max((r['rss_peak_bytes'] for r in native_resources), default='UNMEASURED'),
                native_CPU_seconds={name: str(sum((Decimal(r['cpu_' + name + '_seconds']) for r in native_resources), Decimal(0)))
                                    for name in ('user', 'system')},
                cgroup_peak_scope='shared runtime cgroup cumulative snapshots in raw records',
                temporary_storage_peak_bytes='UNMEASURED',
                fetch_decode_substages='UNMEASURED beyond the exposed native stage intervals',
                fetch_waves='UNMEASURED', confirmed_wire_requests='UNMEASURED')


def validate_record(record, config, arm, body, expected, truth):
    stats.require(record['http_status'] == 200 and record['http_attempts'] == record['valid_ann_requests'] == 1,
                  'one successful HTTP query')
    stats.require(record['native_close']['intentional_stop'] is True
                  and record['temporary_directory_cleanup'] is True and record['http_retry'] is False, 'cleanup/no retry')
    stats.require(record['namespace_start_attempted'] is record['native_process_started'] is record['raw_response_complete'] is True,
                  'one observed namespace/complete response')
    stats.require(record['expected_authority'] == arm['authority']
                  and record['reference_response'] == {k: expected[k] for k in stats.PARITY}
                  and record['truth_at_10'] == truth[:10], 'record reference binding')
    stats.require(record['returned_hits'] == stats.validate_query(record['response'], arm,
                  expected=expected, truth=truth)['returned_hits'], 'record parity/recall')
    raw = base64.b64decode(record['raw_response_base64'], validate=True)
    stats.require(record['raw_response'] == raw.decode(errors='replace')
                  and record['response'] == json.loads(raw), 'raw query outcome')
    headers = [json.loads(line) for line in record['native_server_log'].splitlines() if line.startswith('{')]
    stats.require(headers == [record['native_header']] and headers[0]['listen'] == '127.0.0.1:8080', 'raw ready outcome')
    accounting = stats.validate_outcome(headers[0], record['response'], arm, True)
    stats.require(record['accounting'] == accounting and record['metadata'] == accounting['metadata'], 'accounting receipt')
    stats.require(record['startup_accounting'] == stats.validate_ready(headers[0], arm), 'startup accounting receipt')
    stats.require(record['resources'] == resources(record['native_time_log'], config['native_memory_bytes']), 'resource receipt')
    for name in ('started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns'):
        stats.integer(record[name], name, maximum=2**128 - 1)
    start, attempt, connected, end = (record[k] for k in ('started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns'))
    stats.require(start <= attempt <= connected <= end, 'cold-call clock order')
    for name, elapsed in (('cold_start_to_first_http_response_ns', end - start),
                          ('before_successful_connect_attempt_ns', attempt - start),
                          ('successful_tcp_connect_ns', connected - attempt),
                          ('first_post_to_response_ns', end - connected), ('incoming_http_wall_ns', end - attempt)):
        stats.require(type(record[name]) is int and record[name] == elapsed, 'cold-call timing identity')
    stats.require(end - start >= headers[0]['remote_open_wall_ns'] + headers[0]['head_read_wall_ns']
                  and record['response']['native_wall_ns'] <= end - connected, 'native/HTTP timing bounds')
    stats.require(record['request_bytes'] == len(body)
                  and record['request_sha256'] == hashlib.sha256(body).hexdigest()
                  and record['response_bytes'] == len(raw), 'wire body identity')


def reduce_run(records, panels, config):
    stats.require([(r['dataset'], r['block'], r['arm'], r['query_ordinal']) for r in records]
                  == list(execution_order()), 'exact 512-call order')
    previous, abort, block_hits = 0, None, {}
    for record in records:
        stats.integer(record['http_attempts'], 'actual HTTP attempts', maximum=1)
        stats.require(type(record['namespace_start_attempted']) is type(record['native_process_started']) is bool
                      and (not record['native_process_started'] or record['namespace_start_attempted']), 'actual namespace attempts')
        if record['outcome'] == 'success':
            stats.require(abort is None, 'native call after abort')
            panel, q = panels[record['dataset']], record['query_ordinal']
            arm = panel['arms'][record['arm']]
            validate_record(record, config, arm['arm'], arm['bodies'][q], arm['references'][q], panel['truths'][q])
            stats.require(record['started_ns'] >= previous, 'overlapping cold calls')
            previous = record['completed_ns']
            key = record['dataset'], record['block']
            block_hits[key] = block_hits.get(key, 0) + record['returned_hits']
            if record['arm'] == 'candidate' and record['query_ordinal'] == 63 and block_hits[key] < 608:
                abort = dict(dataset=record['dataset'], block=record['block'], query_ordinal=63,
                             reason='candidate block quality below 608/640')
        elif record['outcome'] == 'failed':
            stats.require(abort is None and record['error_type'] and record['error'], 'raw first failure receipt')
            abort = dict(dataset=record['dataset'], block=record['block'], query_ordinal=record['query_ordinal'], reason='first failed call')
        else:
            stats.require(record['outcome'] == 'aborted' and abort is not None
                          and record['abort_after'] == abort and record['http_attempts'] == 0
                          and not record['namespace_start_attempted'] and not record['native_process_started'],
                          'explicit ordered unattempted position')
    results = {}
    for dataset in DATASETS:
        rows = [r for r in records if r['dataset'] == dataset]
        blocks = {BLOCKS[b]: reduce_calls([r for r in rows if r['block'] == b]) for b in range(4)}
        for block in ('candidate1', 'candidate2'):
            value = blocks[block]
            value['quality_status'] = ('UNMEASURED' if value['successes'] != 64 else
                                       'PASS' if value['returned_hits'] >= 608 else 'FAIL')
            value['quality_gate_passed'] = value['quality_status'] == 'PASS'
        pooled = {arm: reduce_calls([r for r in rows if r['arm'] == arm]) for arm in ('control', 'candidate')}
        quality = all(blocks[BLOCKS[b]]['quality_gate_passed'] for b in (1, 2))
        all_success = all(r['outcome'] == 'success' for r in rows)
        r100 = {}
        for arm in ('control', 'candidate'):
            actual = panels[dataset]['arms'][arm]['actual_k100']
            r100[arm] = ('UNMEASURED' if actual is None else
                         sum(len(set(r['ids']) & set(t)) for r, t in zip(actual, panels[dataset]['truths'])) / 6400)
        latency = all_success and all(pooled['candidate']['latency_ms']['whole_cold'][tail]
                                     < pooled['control']['latency_ms']['whole_cold'][tail] for tail in ('p90', 'p95'))
        results[dataset] = dict(blocks=blocks, pooled=pooled, all_256_calls_successful=all_success,
            candidate_quality_gate_passed=quality, candidate_quality_survivor=quality,
            candidate_quality_status=('FAIL' if any(blocks[BLOCKS[b]]['quality_status'] == 'FAIL' for b in (1, 2))
                                      else 'PASS' if quality else 'UNMEASURED'),
            latency_improvement=latency and quality,
            quality_delta_at_10=(pooled['candidate']['recall_at_10'] - pooled['control']['recall_at_10']
                                 if all_success else 'UNMEASURED'), R100=r100,
            R100_scope='authenticated separate actual-k100 reference; this HTTP run is k10',
            quality_delta_at_100=(r100['candidate'] - r100['control']
                                  if all(type(v) is float for v in r100.values()) else 'UNMEASURED'))
    return dict(schema='borsuk-native-semantic-router-cold-result-v1', datasets=results,
        fixed_positions=512, namespace_starts_attempted=sum(r.get('namespace_start_attempted', False) for r in records),
        namespace_processes_started=sum(r.get('native_process_started', False) for r in records),
        ann_calls_attempted=sum(r.get('http_attempts', 0) for r in records),
        failed_calls=sum(r['outcome'] == 'failed' for r in records),
        aborted_calls=sum(r['outcome'] == 'aborted' for r in records), dropped_calls=0, abort_after=abort,
        ann_calls_successful=sum(r['outcome'] == 'success' for r in records),
        quality_gate_passed=all(r['candidate_quality_gate_passed'] for r in results.values()),
        latency_improvement=all(r['latency_improvement'] for r in results.values()),
        all_calls_successful=all(r['all_256_calls_successful'] for r in results.values()),
        process_and_store_client_cold=True, application_cache=False, s3_service_cache='uncontrolled',
        process_tls_coldness='fresh process/client; differs from vendor namespace coldness',
        control='contemporaneously rebuilt graph, fresh process for each call',
        matched_vendor_measured=False, population='FIRST100k D768 cosine development ordinals 0..63',
        cold_call_boundary='preencoded request; process launch through first HTTP response; refused connects included',
        transport='one loopback plain HTTP ANN request per namespace; native object transport uses TLS',
        source_parallel_cap=16, sq8_get_cap=32, router_get_cap=16,
        bounded_memory_gate_passed=all(r['outcome'] == 'success' for r in records),
        bounded_memory_evidence='native modeled admission and GNU time RSS; shared cgroup snapshots retained',
        process_cleanup_complete=all(isinstance(r.get('native_close'), dict)
            and type(r['native_close'].get('returncode')) is int and r['temporary_directory_cleanup'] is True
            for r in records if r.get('native_process_started')),
        caps=dict(source_GETs=128, source_bytes=67108864, source_parallel=16,
                  SQ8_GETs=32, SQ8_bytes=16773120, router_GETs=16, router_bytes=2097152),
        observed_source_peak_inflight='UNMEASURED', observed_sq8_peak_inflight='UNMEASURED',
        generation_pin_concurrency='UNMEASURED (serial calls)', launch_authority=False,
        sustainable_qps='UNMEASURED', cost='UNMEASURED', offered_load_measured=False)


def run(config, binary, panels, output):
    records, abort, block_hits = [], None, {}
    with (Path(output) / 'records.jsonl').open('x') as stream:
        for dataset, block, arm_name, q in execution_order():
            panel, arm = panels[dataset], panels[dataset]['arms'][arm_name]
            if abort is not None:
                record = dict(outcome='aborted', abort_after=abort, http_attempts=0,
                              namespace_start_attempted=False, native_process_started=False)
            else:
                record = measured_call(binary, config, arm['arm'], arm['bodies'][q], arm['references'][q], panel['truths'][q])
                if record['outcome'] == 'failed':
                    abort = dict(dataset=dataset, block=block, query_ordinal=q, reason='first failed call')
                else:
                    key = dataset, block
                    block_hits[key] = block_hits.get(key, 0) + record['returned_hits']
                    if arm_name == 'candidate' and q == 63 and block_hits[key] < 608:
                        abort = dict(dataset=dataset, block=block, query_ordinal=q, reason='candidate block quality below 608/640')
            record.update(dataset=dataset, block=block, arm=arm_name, query_ordinal=q)
            stream.write(encoded(record) + '\n')
            stream.flush()
            records.append(record)
    return reduce_run(records, panels, config)


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    stats.require(len(args) == 5, 'usage: CONFIG CONFIG_SHA BINARY PROOF NEW_OUTPUT')
    config_path, digest, binary, proof_path, output = args
    stats.require(old.sha(config_path) == stats.digest(digest), 'config identity')
    config = json.loads(Path(config_path).read_text())
    validate_config(config)
    proof = validate_runtime(config, binary, proof_path)
    stats.require(sorted(os.sched_getaffinity(0)) == config['client_cpu_affinity'], 'client CPU affinity')
    out = Path(output).absolute()
    out.mkdir(exist_ok=False)
    (out / 'config.json').write_text(Path(config_path).read_text())
    (out / 'qualification.json').write_bytes(Path(proof_path).read_bytes())
    try:
        panels = prepare(config, out)
        summary = run(config, binary, panels, out)
        # Recheck every input and execution identity; no refetch or query retry.
        for panel in panels.values():
            inputs = [*panel['inputs'].values(), *(i for arm in panel['arms'].values() for i in arm['inputs'].values())]
            for item in inputs:
                stats.require(Path(item['path']).stat().st_size == item['bytes'] and old.sha(item['path']) == item['sha256'],
                              'prepared input changed')
        stats.require(old.sha(config_path) == digest, 'config changed')
        validate_config(config)
        validate_runtime(config, binary, proof_path)
        summary.update(config_sha256=digest, binary_sha256=config['binary']['sha256'],
                       qualification_sha256=config['qualification_sha256'],
                       native_source_identity_sha256=config['native_source_identity_sha256'],
                       native_source_file_count=config['native_source_file_count'], code_sha256=config['code_sha256'],
                       inputs={d: dict(common=p['inputs'], arms={a: v['inputs'] for a, v in p['arms'].items()})
                               for d, p in panels.items()}, identity_gate_passed=True, closed=summary['process_cleanup_complete'])
    except Exception as error:
        summary = locals().get('summary', dict(schema='borsuk-native-semantic-router-cold-result-v1'))
        summary.update(closed=summary.get('process_cleanup_complete', False), terminal_error=dict(type=type(error).__name__, message=str(error)),
                       identity_gate_passed=False, all_calls_successful=False, latency_improvement=False)
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
    print(encoded({k: summary.get(k, False) for k in ('closed', 'all_calls_successful', 'quality_gate_passed', 'latency_improvement')}))
    return 0 if summary['all_calls_successful'] and summary.get('identity_gate_passed') and summary.get('quality_gate_passed') else 1


def self_check():
    assert callable(globals().get('execution_order')), 'fixed execution order missing'
    expected = [(dataset, block, arm, q) for dataset in ('ReLAION', 'CoHere')
                for block, arm in enumerate(('control', 'candidate', 'candidate', 'control'))
                for q in range(64)]
    assert list(execution_order()) == expected and len(expected) == 512
    import copy
    from contextlib import ExitStack
    import tempfile
    from unittest.mock import Mock, patch

    def rejected(call):
        try:
            call()
        except (ValueError, KeyError, AssertionError):
            return
        raise AssertionError('invalid evidence accepted')

    def sha_body(body):
        return hashlib.sha256(body).hexdigest()

    requests = b''.join((encoded(dict(query_ordinal=q, query=[q + 1] + [0.] * 767)) + '\n').encode() for q in range(64))
    truth = struct.pack('<100I', *range(100)) * 64
    common = {'manifest.json': 5000, 'page_manifest.json': 1000, 'page_digests.bin': 12512,
              'plane/manifest.json': 500, 'plane/mean.bin': 3072, 'plane/page_digests.bin': 100000}
    hashes = {name: sha_body(name.encode()) for name in common}

    def query_response(arm):
        semantic = arm['discovery'] == 'semantic'
        stages = {name: dict(start_ns=i * 10 + 1, end_ns=i * 10 + 9) for i, name in enumerate(stats.STAGES)}
        stages['leaf_peak_inflight'] = 8 if semantic else 0
        return dict(authority=arm['authority'], ids=list(range(10)), ranges=[[0, 199680]],
            planned_bytes=199680, submitted_gets=1, verified_bytes=199680, failed_gets=0,
            source_submitted_gets=1, source_verified_bytes=6400, source_failed_gets=0,
            router_submitted_gets=8 if semantic else 0, router_verified_bytes=6400 if semantic else 0,
            router_failed_gets=0, query_stages=stages, native_wall_ns=40)

    def transport(gets, heads, payload, puts=1):
        return dict(schema='borsuk-native-transport-v1', scope='process_all_native_s3_readers', per_query_delta=False,
            attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests',
            method_order=stats.METHODS, status_counts_format='[http_status,count] nonzero entries',
            payload_measurement='consumed response data frames, including unauthenticated payload',
            dropped_error_body_consumed_bytes=0, unknown=stats.UNKNOWN,
            totals=dict(attempts=gets + heads + puts, method_counts=[gets, heads, puts] + [0] * 7,
                status_counts=[[200, gets + heads + puts]], transport_failures=0, stream_failures=0,
                consumed_payload_bytes=payload, dropped_error_bodies=0))

    def ready(arm):
        rows = [dict(name=name, bytes=size, chunks=1, head_wall_ns=1, get_wall_ns=0 if size > 4194304 else 1,
                     stream_wall_ns=2, write_wall_ns=1, logical_head_requests=1,
                     logical_get_requests=(size + 4194303) // 4194304,
                     payload_buffer_bound_bytes=min(size, 8 * 4194304)) for name, size in arm['metadata_files'].items()]
        router_head = int(arm['discovery'] == 'semantic')
        header = dict(phase='ready', listen='127.0.0.1:8080', authority=arm['authority'],
            head_read_wall_ns=2, remote_open_wall_ns=130,
            remote_open_stats=dict(metadata=rows, staging_wall_ns=100, decode_wall_ns=20,
                                  source_head_requests=1, source_head_wall_ns=5,
                                  router_head_requests=router_head, router_head_wall_ns=5 * router_head))
        header['transport'] = transport(sum(r['logical_get_requests'] for r in rows) + 3,
                                        len(rows) + 1 + router_head, sum(arm['metadata_files'].values()) + 200 + 37)
        return header

    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        binary, cfg, qualification = (directory / name for name in ('binary', 'config.json', 'proof.json'))
        binary.write_bytes(b'mock-qualified-binary-never-executed')
        inventory = native_inventory()
        source_digest = sha_body(encoded(inventory).encode())
        proof = dict(qualified=True, green_status=0, release_status=0, binary_sha256=old.sha(binary),
                     compiled_native_sha256=inventory, source_file_count=len(inventory), source_identity_sha256=source_digest)
        qualification.write_text(encoded(proof) + '\n')
        config = dict(schema=SCHEMA, count=64, k=10, dataset_order=list(DATASETS), blocks=list(BLOCKS),
            bucket='synthetic', region='synthetic', native_memory_bytes=1073741824,
            credential_protocol='instance-imdsv2',
            client_cpu_affinity=[4, 5], native_cpu_affinity=[0, 1, 2, 3],
            binary=dict(bytes=binary.stat().st_size, sha256=old.sha(binary)), qualification_sha256=old.sha(qualification),
            native_source_file_count=len(inventory), native_source_identity_sha256=source_digest,
            code_sha256={name: old.sha(name) for name in CODE}, items=[])
        bodies, by_index = {}, {}

        def bind(key, body):
            bodies[key] = body
            return dict(key=key, bytes=len(body), sha256=sha_body(body))

        for dataset in DATASETS:
            identity = {name: sha_body(name.encode()) for name in SOURCE_IDENTITIES}
            identity.update(mean_sha256=hashes['plane/mean.bin'], queries_sha256=sha_body(requests), truth_sha256=sha_body(truth))
            item = dict(dataset=dataset, rows=100000, dimensions=768, metric='cosine', query_split='development-0..63',
                source_identity=identity, inputs=dict(requests=bind(dataset + '/requests', requests), truth=bind(dataset + '/truth', truth)), arms={})
            for name, mode in (('control', 'graph'), ('candidate', 'semantic')):
                root = sha_body((dataset + name).encode())
                files = dict(common)
                if mode == 'graph':
                    files.update({'centroids.bin': 4800032, 'graph.bin': 1100, 'diverse_graph.bin': 1100})
                else:
                    files.update({'router/manifest.json': 80000, 'router/membership.bin': 12500})
                roster_sha = dict(hashes, **{key: sha_body(key.encode()) for key in files if key not in hashes})
                roster_sha['manifest.json'] = root
                arm = dict(discovery=mode, authority=dict(root_sha256=root, generation=1, control_epoch=1),
                    indexes={'10': dataset + '/' + name}, metadata_files=files, metadata_sha256=roster_sha,
                    head_file=dict(bytes=200, sha256=sha_body(b'mock-head')), inputs={})
                if mode == 'semantic':
                    arm['leaf_object'] = dict(bytes=4812500, sha256=sha_body(b'mock-leaves'))
                header = dict(top_k=10, declared_panel_count=64, rows=100000, dimensions=768, metric='cosine',
                    discovery=mode, authority=arm['authority'], query_split=item['query_split'], source_identity=identity)
                reference = [header, *[dict(query_ordinal=q, **{k: query_response(arm)[k] for k in stats.PARITY}) for q in range(64)], dict(count=64)]
                body = ''.join(encoded(r) + '\n' for r in reference).encode()
                arm['inputs']['reference-k10'] = bind(dataset + '/' + name + '/reference', body)
                item['arms'][name] = arm
                by_index[arm['indexes']['10']] = dict(arm, dataset=dataset)
            config['items'].append(item)
        for item in config['items']:
            for arm in item['arms'].values():
                header = ready(arm)
                startup = stats.validate_ready(header, arm)
                assert startup['credential_protocol'] == 'instance-imdsv2'
                assert startup['declared_credential_submissions'] == 3
                assert startup['inferred_credential_consumed_bytes'] == 37
                for mutation in ('missing_GET', 'missing_PUT', 'extra_GET', 'extra_PUT', 'DELETE',
                                 'status', 'zero_payload', 'short_payload', 'saturated'):
                    bad = copy.deepcopy(header)
                    totals = bad['transport']['totals']
                    if mutation in ('missing_GET', 'missing_PUT', 'extra_GET', 'extra_PUT', 'DELETE'):
                        method = 0 if mutation.endswith('GET') else 3 if mutation == 'DELETE' else 2
                        change = -1 if mutation.startswith('missing') else 1
                        totals['method_counts'][method] += change
                        totals['attempts'] += change
                        totals['status_counts'][0][1] += change
                    elif mutation == 'status': totals['status_counts'][0][0] = 403
                    elif mutation in ('zero_payload', 'short_payload'):
                        totals['consumed_payload_bytes'] -= 37 + int(mutation == 'short_payload')
                    else: totals['consumed_payload_bytes'] = 2**64 - 1
                    rejected(lambda: stats.validate_ready(bad, arm))
        cfg.write_text(encoded(config) + '\n')
        validate_config(config)
        validate_runtime(config, binary, qualification)
        for mutation in ('blocks', 'count', 'roster', 'identity', 'code', 'rootcap', 'membership', 'leafstaging',
                         'credential_protocol', 'missing_protocol'):
            bad = copy.deepcopy(config)
            if mutation == 'blocks': bad['blocks'] = ['control0', 'candidate1', 'candidate2', 'control3-32']
            elif mutation == 'count': bad['count'] = 32
            elif mutation == 'roster': bad['native_source_file_count'] = True
            elif mutation == 'identity': bad['items'][0]['source_identity']['queries_sha256'] = '0' * 64
            elif mutation == 'code': bad['code_sha256'].pop(CODE[-1])
            elif mutation == 'rootcap': bad['items'][0]['arms']['candidate']['metadata_files']['manifest.json'] = 65537
            elif mutation == 'membership': bad['items'][0]['arms']['candidate']['metadata_files']['router/membership.bin'] += 1
            elif mutation == 'leafstaging': bad['items'][0]['arms']['candidate']['metadata_files']['router/leaves.bin'] = 4812500
            elif mutation == 'credential_protocol': bad['credential_protocol'] = 'static'
            else: del bad['credential_protocol']
            rejected(lambda: validate_config(bad))
        bad = dict(config, native_source_file_count=len(inventory) + 1)
        rejected(lambda: validate_runtime(bad, binary, qualification))
        binary.write_bytes(b'tampered')
        rejected(lambda: validate_runtime(config, binary, qualification))
        binary.write_bytes(b'mock-qualified-binary-never-executed')

        def fetch(bucket, identity, path):
            assert bucket == 'synthetic'
            body = bodies[identity['key']]
            assert identity['bytes'] == len(body) and identity['sha256'] == sha_body(body)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            return dict(path=str(path), bytes=len(body), sha256=sha_body(body))

        state = dict(arm=None, fault=None, clock=1000000000)
        commands, seen, temporary_paths = [], [], []
        server = Mock()
        server.poll.return_value = None

        def spawn(command, **kwargs):
            assert command[4:11] == ['timeout', '--signal=TERM', '--kill-after=5', '60', 'taskset', '-c', '0-3']
            assert os.environ['BORSUK_NATIVE_MEMORY_BYTES'] == str(config['native_memory_bytes'])
            assert os.environ['AWS_MAX_ATTEMPTS'] == '1'
            state['arm'] = by_index[command[14]]
            server.poll.return_value = 123 if state['fault'] == 'startup' else None
            assert command[11] == str(binary) and command[15] == state['arm']['authority']['root_sha256']
            header = ready(state['arm'])
            if state['fault'] == 'ready': del header['transport']
            kwargs['stdout'].write(encoded(header) + '\n')
            kwargs['stdout'].flush()
            Path(command[3]).write_text('Maximum resident set size (kbytes): 1\nUser time (seconds): 0.01\nSystem time (seconds): 0.00\n')
            commands.append(command)
            temporary_paths.append(Path(command[3]).parent)
            return server

        def post(client, body):
            arm = state['arm']
            q = json.loads(body)['query'][0] - 1
            seen.append((arm['dataset'], 'candidate' if arm['discovery'] == 'semantic' else 'control', q))
            response = query_response(arm)
            ready_totals = ready(arm)['transport']['totals']
            query_gets = sum(response[p + 'submitted_gets'] for p in ('', 'source_', 'router_'))
            query_bytes = sum(response[p + 'verified_bytes'] for p in ('', 'source_', 'router_'))
            response['transport'] = transport(ready_totals['method_counts'][0] + query_gets,
                ready_totals['method_counts'][1], ready_totals['consumed_payload_bytes'] + query_bytes)
            fault = state['fault']
            if fault == 'parity': response['ids'].reverse()
            elif fault == 'telemetry': del response['transport']
            elif fault == 'stage': response['query_stages']['source']['end_ns'] = 41
            elif fault == 'wire': response['transport']['unknown'] = []
            elif fault in ('retry', 'credential_refresh'):
                response['transport']['totals']['attempts'] += 1
                response['transport']['totals']['method_counts'][0 if fault == 'retry' else 2] += 1
                response['transport']['totals']['status_counts'][0][1] += 1
            elif fault == 'nonmonotonic':
                response['transport']['totals']['consumed_payload_bytes'] = ready_totals['consumed_payload_bytes'] - 1
            elif fault == 'reject': return 400, encoded(dict(error='invalid_request', transport=ready(arm)['transport'])).encode()
            elif fault == 'http':
                response.pop('ids'); response.pop('ranges'); response.pop('planned_bytes')
                response['error'] = 'search_failed'
                return 502, encoded(response).encode()
            elif fault == 'binarybody': return 502, b'\xff\x00failed'
            return 200, encoded(response).encode()

        def clock():
            state['clock'] += 8000000 if state['arm'] and state['arm']['discovery'] == 'semantic' else 10000000
            return state['clock']

        saved = {name: getattr(old, name) for name in ('checked_response', 'validate', 'post', 'subprocess')}
        environment = {name: os.environ.get(name) for name in ('AWS_MAX_ATTEMPTS', 'BORSUK_NATIVE_MEMORY_BYTES')}
        argv = sys.argv
        with ExitStack() as stack:
            stack.enter_context(patch.object(old.subprocess, 'Popen', side_effect=spawn))
            stack.enter_context(patch.object(old.http.client, 'HTTPConnection', side_effect=lambda *a, **k: Mock()))
            stack.enter_context(patch.object(old.time, 'monotonic_ns', side_effect=clock))
            posted = stack.enter_context(patch.object(sys.modules[__name__], '_post', side_effect=post))
            stopped = stack.enter_context(patch.object(old, 'stop', return_value=dict(intentional_stop=True, returncode=143)))
            fetched = stack.enter_context(patch.object(old, 'fetch', side_effect=fetch))
            stack.enter_context(patch.object(old.os, 'sched_getaffinity', return_value={4, 5}))
            stack.enter_context(patch.object(sys.modules[__name__], 'cgroup_snapshot', return_value='UNMEASURED'))
            output = directory / 'output'
            status = main([str(cfg), old.sha(cfg), str(binary), str(qualification), str(output)])
            assert status == 0, json.loads((output / 'summary.json').read_text())
            result = json.loads((output / 'summary.json').read_text())
            records = [json.loads(line) for line in (output / 'records.jsonl').read_text().splitlines()]
            assert len(commands) == posted.call_count == stopped.call_count == 512 and fetched.call_count == 8
            assert seen == [(d, a, q) for d, _, a, q in expected]
            assert result['all_calls_successful'] and result['quality_gate_passed'] and result['latency_improvement']
            assert all(d['pooled']['candidate']['count'] == d['pooled']['control']['count'] == 128
                       and d['R100'] == {'control': 'UNMEASURED', 'candidate': 'UNMEASURED'} for d in result['datasets'].values())
            for dataset in result['datasets'].values():
                for pooled in dataset['pooled'].values():
                    credentials = pooled['credential_transport_totals']
                    assert credentials['protocol'] == 'instance-imdsv2'
                    assert credentials['declared_credential_submissions'] == 3 * 128
                    assert credentials['inferred_credential_consumed_bytes'] == 37 * 128
                    assert pooled['known_process_transport_totals']['method_counts'][2] == 128
            assert all(not path.exists() for path in temporary_paths)
            audited = stats.check_saved(output, old.sha(cfg), str(binary))
            assert audited == dict(all_calls_successful=True, quality_gate_passed=True, latency_improvement=True, records=512)
            bad_saved = copy.deepcopy(result)
            bad_saved['datasets']['ReLAION']['pooled']['candidate']['latency_ms']['whole_cold']['p95'] += 1
            (output / 'summary.json').write_text(encoded(bad_saved))
            rejected(lambda: stats.check_saved(output, old.sha(cfg), str(binary)))
            (output / 'summary.json').write_text(encoded(result))
            panels = prepare(config, directory / 'prepared')
            for fault in ('parity', 'telemetry', 'stage', 'ready', 'wire', 'retry', 'credential_refresh',
                          'nonmonotonic', 'http', 'binarybody', 'reject'):
                state['fault'] = fault
                arm = panels['ReLAION']['arms']['candidate']
                before_calls = posted.call_count, stopped.call_count
                failed = measured_call(str(binary), config, arm['arm'], arm['bodies'][0], arm['references'][0], panels['ReLAION']['truths'][0])
                assert failed['outcome'] == 'failed' and failed['http_attempts'] == 1
                assert failed['native_close']['intentional_stop'] and failed['native_server_log'] and failed['raw_response']
                assert posted.call_count == before_calls[0] + 1 and stopped.call_count == before_calls[1] + 1
                assert failed['temporary_directory_cleanup'] and not temporary_paths[-1].exists()
                if fault == 'binarybody': assert base64.b64decode(failed['raw_response_base64']) == b'\xff\x00failed'
                if fault in ('http', 'reject'): assert failed['telemetry_validation_errors'] == []
                assert sys.argv is argv and all(getattr(old, name) is value for name, value in saved.items())
                assert all(os.environ.get(name) == value for name, value in environment.items())
            state['fault'] = None
            for fault, http_calls in (('telemetry', 1), ('startup', 0), ('reject', 1), ('credential_refresh', 1)):
                state['fault'] = fault
                before_calls = len(commands), posted.call_count, stopped.call_count
                failed_output = directory / ('fail-fast-' + fault)
                assert main([str(cfg), old.sha(cfg), str(binary), str(qualification), str(failed_output)]) == 1
                partial = json.loads((failed_output / 'summary.json').read_text())
                partial_rows = [json.loads(line) for line in (failed_output / 'records.jsonl').read_text().splitlines()]
                assert partial['identity_gate_passed'] and partial['namespace_starts_attempted'] == partial['namespace_processes_started'] == 1
                assert partial['ann_calls_attempted'] == http_calls and partial['failed_calls'] == 1 and partial['aborted_calls'] == 511
                assert not partial['all_calls_successful'] and not partial['latency_improvement'] and not partial['bounded_memory_gate_passed']
                assert len(commands) == before_calls[0] + 1 and posted.call_count == before_calls[1] + http_calls
                assert stopped.call_count == before_calls[2] + 1
                assert [(r['dataset'], r['block'], r['arm'], r['query_ordinal']) for r in partial_rows] == expected
                assert partial_rows[0]['outcome'] == 'failed' and all(r['outcome'] == 'aborted' for r in partial_rows[1:])
                assert partial_rows[0]['native_close']['intentional_stop'] and not temporary_paths[-1].exists()
                assert not stats.check_saved(failed_output, old.sha(cfg), str(binary))['all_calls_successful']
            state['fault'] = None
            # Both candidate blocks must meet the integer 608/640 boundary.
            boundary_panels, boundary_records = copy.deepcopy(panels), copy.deepcopy(records)
            for dataset in DATASETS:
                for q in range(32, 64):
                    boundary_panels[dataset]['truths'][q][9] = 100
            for row in boundary_records:
                q = row['query_ordinal']
                row['truth_at_10'] = boundary_panels[row['dataset']]['truths'][q][:10]
                row['returned_hits'] = 9 if q >= 32 else 10
            boundary = reduce_run(boundary_records, boundary_panels, config)
            assert boundary['quality_gate_passed'] and all(
                d['blocks'][block]['returned_hits'] == 608 for d in boundary['datasets'].values() for block in ('candidate1', 'candidate2'))
            for dataset in DATASETS:
                boundary_panels[dataset]['truths'][31][9] = 100
            for row in boundary_records:
                if row['query_ordinal'] == 31:
                    row['returned_hits'] = 9
                    row['truth_at_10'] = boundary_panels[row['dataset']]['truths'][31][:10]
            quality_abort = dict(dataset='ReLAION', block=1, query_ordinal=63, reason='candidate block quality below 608/640')
            for index in range(128, 512):
                row = boundary_records[index]
                boundary_records[index] = dict(dataset=row['dataset'], block=row['block'], arm=row['arm'], query_ordinal=row['query_ordinal'],
                    outcome='aborted', abort_after=quality_abort, http_attempts=0, namespace_start_attempted=False, native_process_started=False)
            quality_failure = reduce_run(boundary_records, boundary_panels, config)
            assert not quality_failure['quality_gate_passed'] and quality_failure['ann_calls_attempted'] == 128
            assert quality_failure['datasets']['ReLAION']['blocks']['candidate1']['quality_status'] == 'FAIL'
            assert quality_failure['datasets']['ReLAION']['blocks']['candidate2']['quality_status'] == 'UNMEASURED'
            quality_output = directory / 'quality-abort'
            quality_output.mkdir()
            before_calls = len(commands), posted.call_count, stopped.call_count
            quality_run = run(config, str(binary), boundary_panels, quality_output)
            assert quality_run['ann_calls_attempted'] == quality_run['namespace_starts_attempted'] == 128
            assert quality_run['failed_calls'] == 0 and quality_run['aborted_calls'] == 384
            assert quality_run['abort_after'] == quality_abort and not quality_run['quality_gate_passed']
            assert len(commands) == before_calls[0] + 128 and posted.call_count == before_calls[1] + 128 and stopped.call_count == before_calls[2] + 128
            actual_config = copy.deepcopy(config)
            for item in actual_config['items']:
                for arm_name, arm in item['arms'].items():
                    header = dict(top_k=100, declared_panel_count=64, rows=100000, dimensions=768, metric='cosine',
                        discovery=arm['discovery'], authority=arm['authority'], query_split=item['query_split'],
                        source_identity=item['source_identity'], measurement='actual-k100', execution_sha256='a' * 64)
                    actual_rows = [header, *[dict(query_ordinal=q, ids=list(range(100))) for q in range(64)], dict(count=64)]
                    key = item['dataset'] + '/' + arm_name + '/actual-k100'
                    arm['inputs']['actual-k100'] = bind(key, ''.join(encoded(r) + '\n' for r in actual_rows).encode())
            actual_panels = prepare(actual_config, directory / 'actual-prepared')
            actual_summary = reduce_run(records, actual_panels, actual_config)
            assert all(d['R100'] == {'control': 1., 'candidate': 1.} and d['quality_delta_at_100'] == 0.
                       for d in actual_summary['datasets'].values())
            bad_records = copy.deepcopy(records)
            bad_records[64]['returned_hits'] = 0
            rejected(lambda: reduce_run(bad_records, panels, config))
            bad_records = copy.deepcopy(records)
            bad_records[64]['outcome'] = 'failed'
            bad_records[64].update(error_type='SyntheticFailure', error='kept once')
            failed_abort = dict(dataset='ReLAION', block=1, query_ordinal=0, reason='first failed call')
            for index in range(65, 512):
                row = bad_records[index]
                bad_records[index] = dict(dataset=row['dataset'], block=row['block'], arm=row['arm'], query_ordinal=row['query_ordinal'],
                    outcome='aborted', abort_after=failed_abort, http_attempts=0, namespace_start_attempted=False, native_process_started=False)
            bad_summary = reduce_run(bad_records, panels, config)
            assert not bad_summary['all_calls_successful'] and not bad_summary['latency_improvement']
            assert not bad_summary['datasets']['ReLAION']['candidate_quality_gate_passed']
            assert bad_summary['datasets']['CoHere']['candidate_quality_status'] == 'UNMEASURED'
            slower = copy.deepcopy(records)
            offset = 0
            for row in slower:
                for key in ('started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns'):
                    row[key] += offset
                if row['block'] == 2 and row['query_ordinal'] >= 56:
                    row['completed_ns'] += 20000000
                    for key in ('cold_start_to_first_http_response_ns', 'first_post_to_response_ns', 'incoming_http_wall_ns'):
                        row[key] += 20000000
                    offset += 20000000
            slower_summary = reduce_run(slower, panels, config)
            assert slower_summary['quality_gate_passed'] and not slower_summary['latency_improvement']
            for dataset in slower_summary['datasets'].values():
                candidate = dataset['pooled']['candidate']['latency_ms']['whole_cold']
                control = dataset['pooled']['control']['latency_ms']['whole_cold']
                assert candidate['p90'] < control['p90'] and candidate['p95'] > control['p95']
        assert sys.argv is argv and all(getattr(old, name) is value for name, value in saved.items())
        assert all(os.environ.get(name) == value for name, value in environment.items())
    stats.self_check()
    print('PASS 512 fixed ABBA calls; exact IMDSv2 startup and GET-only query delta; fail-fast/511 aborted and zero-HTTP startup failure; 608/607 block gates; raw failures, cleanup and restoration; native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        raise SystemExit(main())
