#!/usr/bin/env python3
"""Fixed FIRST100k ABBA cold HTTP runtime; this is not launch authority.

CLI: python3 -m scripts.run_native_semantic_router_cold CONFIG CONFIG_SHA BINARY PROOF NEW_OUTPUT
     python3 -m scripts.run_native_semantic_router_cold --self-check
     python3 -m scripts.run_native_semantic_router_cold --offered-self-check

The offered schema uses 24 rate-major dataset/arm cells, 64 offers each, six
cleanup-owned ports, no queue or retry. main(..., on_cell_closed=callback)
passes (cell_summary, {'records': Path, 'summary': Path}) only after drain,
immutable file fsync and valid identity/resource/cleanup proof. The controller
owns uploads. execution_gate_passed records valid experimental closeout;
offered_gate_passed requires BOTH candidate 8-QPS cells to qualify. Matched
latency comparisons additionally require complete valid control cells.

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
S3 startup bytes from the ready total: head.json, the generation manifest read
to authenticate authority, and all staged metadata. The manifest is fetched again
during staging; both reads are charged. Credential values are never read here.
"""
from pathlib import Path
import sys
from decimal import Decimal
import base64
import hashlib
import io
import json
import math
import os
import struct
import time

if not __debug__:
    raise RuntimeError('the reused cold-call harness requires Python assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_native_cold_first_query as old
from scripts import check_native_semantic_router_stats as stats

CODE = (*old.CODE, 'scripts/check_native_semantic_router_stats.py',
        'scripts/run_native_semantic_router_cold.py')
SCHEMA = 'borsuk-native-semantic-router-cold-v1'
OFFERED_SCHEMA = 'borsuk-native-semantic-router-cold-offered-v1'
OFFERED_RESULT_SCHEMA = 'borsuk-native-semantic-router-cold-offered-result-v1'
OFFERED_CODE = (*CODE, 'scripts/run_native_metadata_ranges_cold.py',
                'scripts/check_native_metadata_ranges_stats.py', 'scripts/run_native_cold_offered.py')
RATES = [.25, .5, 1, 2, 4, 8]
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
    stats.require(config['schema'] in (SCHEMA, OFFERED_SCHEMA), 'config schema')
    offered = config['schema'] == OFFERED_SCHEMA
    stats.require(config.get('authority_pending', False) is False, 'config authority pending')
    if offered:
        validate_offered_config(config)
    stats.require(config.get('credential_protocol') == stats.CREDENTIAL_PROTOCOL, 'unsupported credential protocol')
    stats.require(type(config['count']) is type(config['k']) is int
                  and (config['count'], config['k']) == (64, 10), 'fixed 64/k10 panel')
    stats.require(config['dataset_order'] == list(DATASETS) and (offered or config['blocks'] == list(BLOCKS)),
                  'fixed dataset/ABBA order; old 32-request blocks are incompatible')
    stats.require(config['client_cpu_affinity'] == [4, 5]
                  and config['native_cpu_affinity'] == [0, 1, 2, 3], 'cold-call CPU contract')
    stats.integer(config['native_memory_bytes'], 'native memory admission', 1)
    stats.integer(config['binary']['bytes'], 'binary bytes', 1)
    stats.digest(config['binary']['sha256'])
    stats.digest(config['qualification_sha256'])
    stats.digest(config['native_source_identity_sha256'])
    stats.integer(config['native_source_file_count'], 'source roster count', 1)
    stats.require(set(config['code_sha256']) == set(OFFERED_CODE if offered else CODE), 'complete runtime code closure')
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
            if offered:
                stats.require(set(arm['inputs']) == {'reference-k10'}, 'eight offered input bodies')
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
            if config.get('schema') == OFFERED_SCHEMA:
                stats.require(os.environ.get('AWS_MAX_ATTEMPTS') == '1', 'offered fetch retry environment')
                return old.fetch(bucket, identity, path)
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


def cgroup_snapshot(extra_files=()):
    # Process-group totals can overlap the client; these are not per-query deltas.
    snapshot = dict(path='UNMEASURED', scope='shared runtime cgroup cumulative snapshot', files={}, diagnostics={})
    field = '/proc/self/cgroup'
    try:
        entry = next(line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
        field = 'cgroup path'
        root = Path('/sys/fs/cgroup').resolve()
        directory = (root / entry.lstrip('/')).resolve()
        stats.require(directory.is_relative_to(root), 'cgroup path')
        snapshot['path'] = str(directory)
    except (OSError, StopIteration, ValueError) as error:
        snapshot['diagnostics'][field] = dict(type=type(error).__name__)
        if isinstance(error, OSError): snapshot['diagnostics'][field]['errno'] = error.errno
        return snapshot
    for name in ('memory.current', 'memory.peak', 'memory.events', 'memory.swap.current', 'cpu.stat', *extra_files, 'io.stat'):
        try:
            snapshot['files'][name] = (directory / name).read_text().strip()
        except (OSError, ValueError) as error:
            snapshot['files'][name] = 'UNMEASURED'
            snapshot['diagnostics'][name] = dict(type=type(error).__name__)
            if isinstance(error, OSError): snapshot['diagnostics'][name]['errno'] = error.errno
    return snapshot


def measured_call(binary, config, arm, body, expected, truth, *, port=8080):
    failures, observed, record = io.StringIO(), dict(namespace_start_attempted=False, native_process_started=False, http_attempts=0), None
    spawn = old.subprocess.Popen

    def popen(*args, **kwargs):
        observed['namespace_start_attempted'] = True
        process = spawn(*args, **kwargs)
        observed['native_process_started'] = True
        return process

    def checked(response, expected, truth, authority):
        stats.require(authority == arm['authority'], 'call authority')
        return stats.validate_query(response, arm, expected=expected, truth=truth)['returned_hits']

    def startup(value, files, wall):
        stats.require(files == arm['metadata_files'], 'call metadata roster')
        return stats.validate_startup(value, arm, wall)

    def post(client, body):
        observed['http_attempts'] = 1
        status, raw = _post(client, body)
        # Retain bytes before the shared cold call marks wire completion.
        observed.update(http_status=status, raw_response=raw)
        return status, raw

    environment = dict(os.environ, BORSUK_NATIVE_MEMORY_BYTES=str(config['native_memory_bytes']),
                       AWS_MAX_ATTEMPTS='1', TOKIO_WORKER_THREADS='4')
    snapshot = offered_cgroup_snapshot if config.get('schema') == OFFERED_SCHEMA else cgroup_snapshot
    before = snapshot()
    try:
        record = _cold_call(binary, config, arm, body, expected, truth, failures, port=port,
                            response_check=checked, startup_check=startup, post_call=post, spawn=popen, env=environment)
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
    record.update(cgroup_before=before, cgroup_after=snapshot(), temporary_directory_cleanup=True,
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
            validate_record(record, config, arm, body, expected, truth, port=port)
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
        authority_head_JSON_GETs=sum(r['startup_accounting']['authority_head_JSON_GETs'] for r in opened),
        authority_head_JSON_bytes=sum(r['startup_accounting']['authority_head_JSON_bytes'] for r in opened),
        authority_generation_root_GETs=sum(r['startup_accounting']['authority_generation_root_GETs'] for r in opened),
        authority_generation_root_bytes=sum(r['startup_accounting']['authority_generation_root_bytes'] for r in opened),
        source_HEADs=len(opened),
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


def validate_record(record, config, arm, body, expected, truth, *, port=8080):
    stats.require(type(port) is int and 1024 <= port <= 65535, 'port must be an integer in 1024..65535')
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
    stats.require(headers == [record['native_header']] and headers[0]['listen'] == f'127.0.0.1:{port}', 'raw ready outcome')
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


def validate_offered_config(config):
    from scripts import run_native_cold_offered as offered
    stats.require(set(OFFERED_CODE) == set(CODE) | set(offered.CODE), 'scheduler dependency closure')
    fixed = dict(offered_qps=RATES, workers=6, base_port=18080, max_dispatch_lateness_ns=125000000,
                 cleanup_reserve_seconds=90, worker_limit_seconds=3000, machine_limit_seconds=3600,
                 native_memory_bytes=536870912, profile_memory_bytes=8589934592, profile_swap_bytes=0,
                 native_rlimit_as_bytes=4294967296, namespace_connect_deadline_seconds=45,
                 native_process_limit_seconds=60, query_payload_timeout_seconds=5, ann_queries=1536,
                 native_source_file_count=399)
    for name, value in fixed.items():
        stats.require(config[name] == value and (type(config[name]) is int if type(value) is int else
                      type(config[name]) is list), 'fixed offered contract: ' + name)
    stats.require(all(type(v) in (int, float) for v in config['offered_qps']), 'offered rate types')
    stats.require(config['native_source_identity_sha256'] ==
                  '92085e6e40ac9324ea7a4fc8daab58995dc84680a5c2391eb426dd430230e520', '399-source identity')
    stats.require(config['binary'] == dict(bytes=16191384, sha256=
                  'c00b766f65f8f0ae0adb5fcca786cb33c0daf046ff4b8f9a8bbcab39e1263533'), 'qualified offered binary')
    stats.require(config['qualification_sha256'] ==
                  '335b9f0776a50c0a92afd37f4e6cff8a6fb402e8068d835073e1526bf300e107', 'qualified offered proof')


def offered_order():
    for index, rate in enumerate(RATES):
        for dataset in DATASETS:
            for arm in ('control', 'candidate'):
                yield index, rate, dataset, arm


def offered_name(index, dataset, arm):
    return f'rate{index}-{dataset.lower()}-{arm}-records.jsonl'


def offered_cgroup_snapshot():
    snapshot = cgroup_snapshot(('memory.max', 'memory.swap.max', 'memory.swap.peak'))
    snapshot['observed_ns'] = time.monotonic_ns()
    return snapshot


def offered_resources(before, after, records, config, *, cell_before=None):
    """Missing proof is a failed gate, retained for offline closeout."""
    try:
        snapshots = [before, *([cell_before] if cell_before is not None else []), *(s for r in records if r['port'] is not None
                               for s in (r.get('cgroup_before'), r.get('cgroup_after'))), after]
        baseline = None
        peak = 0
        for snapshot in snapshots:
            stats.require(isinstance(snapshot, dict), 'shared cgroup proof')
            missing = {k: v for k, v in snapshot.get('diagnostics', {}).items() if k != 'io.stat'}
            stats.require(not missing, 'shared cgroup mandatory fields: ' + encoded(missing))
            stats.require(snapshot['path'] != 'UNMEASURED' and snapshot['path'] == before['path'], 'shared cgroup path')
            files = snapshot['files']
            stats.require(files.get('cpu.stat', 'UNMEASURED') != 'UNMEASURED', 'shared cgroup cpu.stat')
            stats.require(int(files['memory.max']) == config['profile_memory_bytes']
                          and int(files['memory.swap.max']) == config['profile_swap_bytes'] == 0, 'shared cgroup limits')
            memory = [int(files[k]) for k in ('memory.current', 'memory.peak')]
            swap = [int(files[k]) for k in ('memory.swap.current', 'memory.swap.peak')]
            stats.require(0 <= memory[0] <= memory[1] <= config['profile_memory_bytes']
                          and swap == [0, 0], 'shared memory/swap peak')
            events = dict(line.split() for line in files['memory.events'].splitlines())
            counters = [stats.integer(int(events[k]), k) for k in ('oom', 'oom_kill', 'oom_group_kill')]
            stats.require(counters == [0, 0, 0], 'shared OOM events')
            if baseline is None: baseline = counters
            stats.require(counters == baseline, 'shared OOM increments')
            peak = max(peak, memory[1])
        native = [r for r in records if r.get('native_process_started')]
        stats.require(all(isinstance(r.get('resources'), dict) and r['resources'] ==
                          resources(r['native_time_log'], config['native_memory_bytes']) for r in native), 'per-native resource proof')
        return dict(passed=True, shared_peak_bytes=peak, shared_swap_peak_bytes=0, oom_event_increments=[0, 0, 0])
    except (ValueError, KeyError, TypeError, ArithmeticError) as error:
        return dict(passed=False, error=str(error), shared_peak_bytes='UNMEASURED',
                    shared_swap_peak_bytes='UNMEASURED', oom_event_increments='UNMEASURED')


def cleanup_confirmed(record):
    return (record.get('temporary_directory_cleanup') is True and
            (record.get('namespace_start_attempted') is False or
             isinstance(record.get('native_close'), dict) and
             type(record['native_close'].get('intentional_stop')) is bool and
             type(record['native_close'].get('returncode')) is int))


def transport_failure(record):
    return (record.get('error_type') in ('TimeoutError', 'ConnectionResetError', 'ConnectionAbortedError',
                 'ConnectionRefusedError', 'BrokenPipeError', 'RemoteDisconnected', 'IncompleteRead') or
                 record.get('error') == 'namespace process closed before first connection')


def offered_call(binary, config, panel, arm, q, port, *, campaign_cgroup_before=None):
    record = measured_call(binary, config, arm['arm'], arm['bodies'][q], arm['references'][q], panel['truths'][q], port=port)
    record['cleanup_confirmed'] = cleanup_confirmed(record)
    transport = transport_failure(record)
    record['failure_kind'] = ('transport' if transport else 'native') if record['outcome'] == 'failed' else None
    record['abort_admissions'] = (record['outcome'] == 'failed' and not transport or not record['cleanup_confirmed'])
    if not record['cleanup_confirmed'] and record['outcome'] == 'success':
        record.update(outcome='failed', failure_kind='native', error_type='ValueError', error='native cleanup unconfirmed')
    before = record['cgroup_before'] if campaign_cgroup_before is None else campaign_cgroup_before
    if not offered_resources(before, record['cgroup_after'], [dict(record, port=port)], config)['passed']:
        record['abort_admissions'] = True
        if record['outcome'] == 'success':
            record.update(outcome='failed', failure_kind='native', error_type='ValueError', error='shared resource gate')
    if record['outcome'] == 'failed':
        try:
            stats.require(record['native_header']['listen'] == f'127.0.0.1:{port}', 'failed ready port')
        except (ValueError, KeyError, TypeError) as error:
            record.setdefault('telemetry_validation_errors', []).append(str(error))
    return record


def all_offer_tails(records):
    finite = sorted((r['completed_ns'] - r['scheduled_ns']) / 1e6 for r in records if r['outcome'] == 'success')
    # Same interpolation as the serial quantile; its upper endpoint must be finite.
    result = {}
    for name, p in (('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99)):
        position = (len(records) - 1) * p
        lo, hi = math.floor(position), math.ceil(position)
        result[name] = (finite[lo] + (finite[hi] - finite[lo]) * (position - lo)
                        if hi < len(finite) else 'UNBOUNDED')
    return result


def reduce_offered_cell(records, panel, config):
    stats.require(len(records) == 64 and [r['query_ordinal'] for r in records] == list(range(64)), '64 offered ordinals')
    receipt = records[0]['cell_receipt']
    epoch, terminal, deadline = (stats.integer(receipt[k], k, maximum=2**128 - 1)
                                 for k in ('epoch_ns', 'terminal_ns', 'admission_deadline_ns'))
    stats.integer(receipt['worker_started_ns'], 'worker start', maximum=2**128 - 1)
    stats.require(type(receipt['cell_started']) is bool, 'cell start receipt')
    stats.require(terminal > epoch and deadline == receipt['worker_started_ns'] +
                  (config['worker_limit_seconds'] - config['cleanup_reserve_seconds']) * 10**9, 'offered clock/deadline')
    index, rate, dataset, arm_name = (records[0][k] for k in ('rate_index', 'offered_qps', 'dataset', 'arm'))
    arm = panel['arms'][arm_name]
    intervals, delays = [], []
    for q, record in enumerate(records):
        stats.integer(record['query_ordinal'], 'offered ordinal', maximum=63)
        stats.integer(record['rate_index'], 'rate index', maximum=5)
        stats.integer(record['scheduled_ns'], 'scheduled time', maximum=2**128 - 1)
        stats.require(type(record['offered_qps']) in (int, float) and record['offered_qps'] == RATES[record['rate_index']], 'fixed offered rate')
        stats.require((record['rate_index'], record['offered_qps'], record['dataset'], record['arm']) ==
                      (index, rate, dataset, arm_name), 'cell identity')
        stats.require(record['scheduled_ns'] == epoch + round(q * 1e9 / rate), 'absolute offer schedule')
        stats.integer(record['terminal_ns'], 'offer terminal', maximum=2**128 - 1)
        stats.require(epoch <= record['terminal_ns'] <= terminal, 'offer cleanup clock')
        stats.integer(record['http_attempts'], 'one HTTP attempt', maximum=1)
        stats.integer(record['valid_ann_requests'], 'valid ANN requests', maximum=1)
        stats.require(type(record['namespace_start_attempted']) is type(record['native_process_started']) is bool
                      and (not record['native_process_started'] or record['namespace_start_attempted']), 'observed process attempts')
        outcome, port = record['outcome'], record['port']
        stats.require(outcome in ('success', 'failed', 'capacity_drop', 'aborted'), 'offered outcome')
        if record['dispatched_ns'] is not None:
            stats.integer(record['dispatched_ns'], 'dispatch time', maximum=2**128 - 1)
            stats.require(record['scheduled_ns'] <= record['dispatched_ns'] <= record['terminal_ns']
                          and record['dispatched_ns'] < deadline, 'dispatch order/admission deadline')
            delays.append((record['dispatched_ns'] - record['scheduled_ns']) / 1e6)
        if port is None:
            stats.require(outcome in ('capacity_drop', 'aborted') and record['http_attempts'] == 0
                          and record['valid_ann_requests'] == 0
                          and not record['namespace_start_attempted'] and not record['native_process_started']
                          and record['started_ns'] is record['completed_ns'] is None, 'unattempted offer')
            stats.require(not any(k in record for k in ('native_header', 'native_close', 'response',
                          'accounting', 'startup_accounting', 'resources', 'raw_response_base64')), 'telemetry on unattempted offer')
            if outcome == 'capacity_drop':
                stats.require(record['dispatched_ns'] is not None and record['terminal_ns'] == record['dispatched_ns'], 'immediate drop')
            else:
                stats.require(record['dispatched_ns'] is None and record['abort_after'] == receipt['abort_after']
                              and isinstance(receipt['abort_after'], dict), 'explicit abort receipt')
        else:
            stats.integer(port, 'owned port', 18080, 18085)
            stats.require(outcome in ('success', 'failed') and record['dispatched_ns'] is not None, 'admitted offer')
            intervals.append((record['dispatched_ns'], record['terminal_ns'], port, record))
            if receipt['abort_after'] is not None:
                stats.require(record['dispatched_ns'] <= receipt['abort_after']['observed_ns'], 'admission after abort')
            stats.require(record['cleanup_confirmed'] == cleanup_confirmed(record), 'cleanup receipt')
            if record.get('started_ns') is not None:
                stats.require(record['dispatched_ns'] <= record['started_ns'] <= record['terminal_ns'], 'launch clock')
            if record.get('completed_ns') is not None:
                stats.require(record['started_ns'] <= record['completed_ns'] <= record['terminal_ns'], 'response clock')
            if outcome == 'success':
                stats.require(record['abort_admissions'] is False and record['failure_kind'] is None, 'successful admission flags')
                validate_record(record, config, arm['arm'], arm['bodies'][q], arm['references'][q], panel['truths'][q], port=port)
            else:
                stats.require(record['failure_kind'] == ('transport' if transport_failure(record) else 'native'), 'failure classification')
                expected_abort = (not transport_failure(record) or not record['cleanup_confirmed'] or
                    not offered_resources(receipt['campaign_cgroup_before'], record.get('cgroup_after'), [record], config)['passed'])
                stats.require(record['abort_admissions'] is expected_abort, 'fatal failure admission flag')
                stats.validate_failed_record(record, config, arm['arm'], arm['bodies'][q], arm['references'][q], panel['truths'][q], port=port)
            if record['abort_admissions']:
                stats.require(receipt['abort_after'] is not None, 'missing fatal abort receipt')
    if receipt['abort_after'] is not None and receipt['cell_started']:
        abort = receipt['abort_after']
        q = stats.integer(abort['query_ordinal'], 'abort ordinal', maximum=63)
        stats.integer(abort['observed_ns'], 'abort clock', maximum=2**128 - 1)
        stats.require(epoch <= abort['observed_ns'] <= terminal, 'abort observation clock')
        if abort['reason'] == 'admission deadline':
            stats.require(abort['observed_ns'] >= deadline and records[q]['outcome'] == 'aborted', 'deadline abort')
        else:
            stats.require(abort['reason'] in ('fatal call failure', 'cleanup unconfirmed')
                          and records[q].get('abort_admissions') is True
                          and abort['observed_ns'] == records[q]['terminal_ns'], 'fatal abort origin')
    peak = 0
    active = []
    for start, end, port, record in sorted(intervals, key=lambda value: value[0]):
        stats.require(all(port != p or stop <= start and cleanup for _, stop, p, cleanup in active), 'early/poisoned port reuse')
        active = [value for value in active if value[1] > start or not value[3]]
        active.append((start, end, port, record['cleanup_confirmed']))
        peak = max(peak, len(active))
        stats.require(peak <= config['workers'], 'ownership over six')
    for record in records:
        if record['outcome'] == 'capacity_drop':
            at = record['dispatched_ns']
            stats.require(sum(start <= at < end for start, end, _, _ in intervals) == config['workers'], 'drop without six owners')
    reduced = reduce_calls(records)
    admitted = len(intervals)
    successes = reduced['successes']
    resource = offered_resources(receipt['campaign_cgroup_before'], receipt['cgroup_after'], records, config,
                                 cell_before=receipt['cgroup_before'])
    cleanup = all(r['cleanup_confirmed'] for _, _, _, r in intervals)
    timing = all(delay <= config['max_dispatch_lateness_ns'] / 1e6 for delay in delays)
    complete = successes == 64
    quality = complete and reduced['returned_hits'] >= 608
    span = terminal - epoch
    name = offered_name(index, dataset, arm_name)
    body = ''.join(encoded(r) + '\n' for r in records).encode()
    identity = (not any(r.get('failure_kind') == 'native' for r in records) and
                (receipt['abort_after'] is None or receipt['abort_after']['reason'] not in
                 ('fatal call failure', 'cleanup unconfirmed', 'shared resource gate')))
    return dict(reduced, rate_index=index, offered_qps=rate, dataset=dataset, arm=arm_name, records_file=name,
                records_sha256=hashlib.sha256(body).hexdigest(), records_bytes=len(body),
                offered=64, admitted=admitted, admitted_completed=sum(r['cleanup_confirmed'] for _, _, _, r in intervals),
                capacity_drops=sum(r['outcome'] == 'capacity_drop' for r in records), aborted_offers=reduced['aborted'],
                success_fraction=successes / 64, all_offer_recall_at_10=reduced['returned_hits'] / 640,
                epoch_ns=epoch, terminal_ns=terminal, full_span_ns=span, planned_offer_window_ns=round(64e9 / rate),
                last_scheduled_offset_ns=round(63e9 / rate), successful_full_span_qps=successes * 1e9 / span,
                admitted_completed_full_span_qps=sum(r['cleanup_confirmed'] for _, _, _, r in intervals) * 1e9 / span,
                scheduled_to_response_ms=tails([(r['completed_ns'] - r['scheduled_ns']) / 1e6 for r in records if r['outcome'] == 'success']),
                all_offer_scheduled_to_valid_response_ms=all_offer_tails(records), dispatch_delay_ms=tails(delays),
                dispatch_timing_gate_passed=timing, quality_gate_passed=quality, all_offers_successful=complete,
                resource_gate=resource, process_cleanup_complete=cleanup, peak_port_ownership=peak,
                identity_gate_passed=identity,
                abort_after=receipt['abort_after'], cell_started=receipt['cell_started'],
                qualification_gate_passed=quality and timing and cleanup and resource['passed'])


def reduce_offered(records, panels, config):
    roster = [(i, d, a, q) for i, _, d, a in offered_order() for q in range(64)]
    stats.require([(r['rate_index'], r['dataset'], r['arm'], r['query_ordinal']) for r in records] == roster,
                  'exact rate-major 24-cell/1536-position roster')
    cells = [reduce_offered_cell(records[i:i + 64], panels[records[i]['dataset']], config) for i in range(0, 1536, 64)]
    campaign_stop, escalation_stops, previous, worker_start = None, {}, 0, None
    campaign_cgroup_before = records[0]['cell_receipt']['campaign_cgroup_before']
    for index, cell in enumerate(cells):
        rows = records[index * 64:(index + 1) * 64]
        receipt = rows[0]['cell_receipt']
        stats.require(receipt['campaign_cgroup_before'] == campaign_cgroup_before, 'campaign resource baseline')
        if worker_start is None: worker_start = receipt['worker_started_ns']
        stats.require(receipt['worker_started_ns'] == worker_start and cell['epoch_ns'] >= previous, 'drained cell order')
        previous = cell['terminal_ns']
        key = cell['dataset'] + '/' + cell['arm']
        escalation_stop = escalation_stops.get(key)
        stop = campaign_stop or (escalation_stop if escalation_stop and cell['rate_index'] > escalation_stop['rate_index'] else None)
        if stop is not None:
            stats.require(not cell['cell_started'] and all(r['outcome'] == 'aborted' and r['abort_after'] == stop for r in rows), 'remaining cells explicitly aborted')
        else:
            stats.require(cell['cell_started'] is True, 'unexplained unstarted cell')
            if cell['abort_after'] is not None:
                campaign_stop = cell['abort_after']
            elif not cell['resource_gate']['passed']:
                campaign_stop = dict(rate_index=cell['rate_index'], dataset=cell['dataset'], arm=cell['arm'],
                                     reason='shared resource gate', observed_ns=cell['terminal_ns'])
            elif not cell['qualification_gate_passed'] and escalation_stop is None:
                escalation_stops[key] = dict(rate_index=cell['rate_index'], dataset=cell['dataset'], arm=cell['arm'],
                                            reason='arm rate qualification failed', observed_ns=cell['terminal_ns'])
    passing = {dataset: {arm: max([0] + [c['offered_qps'] for c in cells if c['dataset'] == dataset and c['arm'] == arm
                                       and c['qualification_gate_passed']]) for arm in ('control', 'candidate')} for dataset in DATASETS}
    comparisons = []
    for index, rate in enumerate(RATES):
        for dataset in DATASETS:
            control, candidate = [next(c for c in cells if (c['rate_index'], c['dataset'], c['arm']) == (index, dataset, arm))
                                  for arm in ('control', 'candidate')]
            valid = all(c['qualification_gate_passed'] for c in (control, candidate))
            comparisons.append(dict(rate_index=index, offered_qps=rate, dataset=dataset, valid_matched_cells=valid,
                candidate_minus_control_ms={boundary: {tail: candidate[boundary][tail] - control[boundary][tail] for tail in ('p90', 'p95')}
                    for boundary in ('scheduled_to_response_ms', 'all_offer_scheduled_to_valid_response_ms')} if valid else 'UNMEASURED',
                candidate_minus_control_cold_ms={tail: candidate['latency_ms']['whole_cold'][tail] - control['latency_ms']['whole_cold'][tail]
                                                for tail in ('p90', 'p95')} if valid else 'UNMEASURED'))
    return dict(schema=OFFERED_RESULT_SCHEMA, cells=cells, fixed_positions=1536, planned_offers=1536, offered=1536,
                workers=6, base_port=18080, offered_qps=RATES, k=10,
                epoch_ns=cells[0]['epoch_ns'], terminal_ns=cells[-1]['terminal_ns'],
                full_span_ns=cells[-1]['terminal_ns'] - cells[0]['epoch_ns'],
                successful_full_span_qps=sum(c['successes'] for c in cells) * 1e9 / (cells[-1]['terminal_ns'] - cells[0]['epoch_ns']),
                admitted_completed_full_span_qps=sum(c['admitted_completed'] for c in cells) * 1e9 / (cells[-1]['terminal_ns'] - cells[0]['epoch_ns']),
                planned_offer_window_ns=sum(c['planned_offer_window_ns'] for c in cells),
                admitted=sum(c['admitted'] for c in cells), admitted_completed=sum(c['admitted_completed'] for c in cells),
                namespace_starts_attempted=sum(r['namespace_start_attempted'] for r in records),
                namespace_processes_started=sum(r['native_process_started'] for r in records),
                ann_calls_attempted=sum(r['http_attempts'] for r in records), ann_calls_successful=sum(c['successes'] for c in cells),
                failed_calls=sum(c['failures'] for c in cells), dropped_calls=sum(c['capacity_drops'] for c in cells),
                aborted_calls=sum(c['aborted_offers'] for c in cells), all_calls_successful=all(c['all_offers_successful'] for c in cells),
                quality_gate_passed=all(c['quality_gate_passed'] for c in cells), qualification_gate_passed=all(c['qualification_gate_passed'] for c in cells),
                bounded_memory_gate_passed=all(c['resource_gate']['passed'] for c in cells),
                process_cleanup_complete=all(c['process_cleanup_complete'] for c in cells),
                dispatch_timing_gate_passed=all(c['dispatch_timing_gate_passed'] for c in cells),
                execution_gate_passed=(campaign_stop is None and all(c['process_cleanup_complete'] and c['resource_gate']['passed'] for c in cells)),
                identity_gate_passed=all(c['identity_gate_passed'] for c in cells),
                largest_passing_tested_offered_qps=passing, matched_comparisons=comparisons,
                eight_qps_attained=all(c['qualification_gate_passed'] for c in cells if c['offered_qps'] == 8 and c['arm'] == 'candidate'),
                offered_gate_passed=all(c['qualification_gate_passed'] for c in cells if c['offered_qps'] == 8 and c['arm'] == 'candidate'),
                abort_after=campaign_stop, escalation_stopped_after=escalation_stops,
                latency_improvement=all(c['valid_matched_cells'] and all(v < 0 for v in c['candidate_minus_control_cold_ms'].values())
                                        for c in comparisons),
                full_span_boundary='first scheduled offer through all admitted calls, validation and cleanup',
                success_conditioned_tails=True, all_offer_tail_unsuccessful_completion='UNBOUNDED',
                process_and_store_client_cold=True, application_cache=False, s3_service_cache='uncontrolled',
                transport='one loopback HTTP ANN request per fresh namespace; native object transport uses TLS',
                population='FIRST100k D768 cosine development ordinals 0..63', matched_vendor_measured=False,
                sustainable_qps='UNMEASURED', cost='UNMEASURED', launch_authority=False, offered_load_measured=True)


def closed_cell_summary(cell, config, config_sha):
    return dict(cell, schema='borsuk-native-semantic-router-cold-offered-cell-v1',
                config_sha256=config_sha, binary_sha256=config['binary']['sha256'],
                qualification_sha256=config['qualification_sha256'], code_sha256=config['code_sha256'],
                native_source_file_count=config['native_source_file_count'],
                native_source_identity_sha256=config['native_source_identity_sha256'],
                closed=cell['process_cleanup_complete'],
                bounded_memory_gate_passed=(cell['resource_gate']['passed'] and
                    (cell['abort_after'] is None or cell['abort_after']['reason'] != 'shared resource gate')),
                records=dict(bytes=cell['records_bytes'], sha256=cell['records_sha256']))


def run_offered(config, binary, panels, output, *, worker_started_ns=None, on_cell_closed=None, config_sha=None,
                proof_path=None, campaign_cgroup_before=None):
    from scripts import run_native_cold_offered as offered
    worker_started_ns = time.monotonic_ns() if worker_started_ns is None else worker_started_ns
    config_sha = hashlib.sha256((encoded(config) + '\n').encode()).hexdigest() if config_sha is None else config_sha
    if campaign_cgroup_before is None: campaign_cgroup_before = offered_cgroup_snapshot()
    deadline = worker_started_ns + (config['worker_limit_seconds'] - config['cleanup_reserve_seconds']) * 10**9
    records, campaign_stop, escalation_stops = [], None, {}
    for index, rate, dataset, arm_name in offered_order():
        key = dataset + '/' + arm_name
        escalation_stop = escalation_stops.get(key)
        stop = campaign_stop or (escalation_stop if escalation_stop and index > escalation_stop['rate_index'] else None)
        before = offered_cgroup_snapshot()
        if stop is None:
            panel, arm = panels[dataset], panels[dataset]['arms'][arm_name]
            rows, epoch, terminal, abort = offered.schedule_offers(
                lambda q, port: offered_call(binary, config, panel, arm, q, port, campaign_cgroup_before=campaign_cgroup_before), rate,
                workers=config['workers'], base_port=config['base_port'], deadline_ns=deadline)
        else:
            epoch = time.monotonic_ns()
            rows = [dict(query_ordinal=q, offered_qps=rate, scheduled_ns=epoch + round(q * 1e9 / rate),
                         dispatched_ns=None, started_ns=None, completed_ns=None, port=None, outcome='aborted',
                         namespace_start_attempted=False, native_process_started=False, http_attempts=0, valid_ann_requests=0,
                         abort_after=stop, terminal_ns=time.monotonic_ns()) for q in range(64)]
            terminal, abort = time.monotonic_ns(), stop
        for row in rows:
            row.update(rate_index=index, dataset=dataset, arm=arm_name)
            if row.get('failure_stage') == 'thread_start':
                q = row['query_ordinal']
                body, reference = arm['bodies'][q], arm['references'][q]
                row.update(temporary_directory_cleanup=True, expected_authority=arm['arm']['authority'],
                           reference_response={k: reference[k] for k in stats.PARITY}, truth_at_10=panel['truths'][q][:10],
                           request_bytes=len(body), request_sha256=hashlib.sha256(body).hexdigest(),
                           raw_response_complete=False, http_retry=False, failure_kind='native',
                           cgroup_before=before, cgroup_after=offered_cgroup_snapshot(),
                           telemetry_validation_errors=['thread creation failed before native start'])
        rows[0]['cell_receipt'] = dict(epoch_ns=epoch, terminal_ns=terminal, abort_after=abort,
                                      cgroup_before=before, cgroup_after=offered_cgroup_snapshot(),
                                      campaign_cgroup_before=campaign_cgroup_before,
                                      worker_started_ns=worker_started_ns, admission_deadline_ns=deadline, cell_started=stop is None)
        cell = reduce_offered_cell(rows, panels[dataset], config)
        validate_config(config)
        stats.require(proof_path is not None, 'offered qualification proof path')
        validate_runtime(config, binary, proof_path)
        path = Path(output) / cell['records_file']
        with path.open('x') as stream:
            stream.write(''.join(encoded(r) + '\n' for r in rows))
            stream.flush()
            os.fsync(stream.fileno())
        records.extend(rows)
        if stop is None:
            if abort is not None:
                campaign_stop = abort
            elif not cell['resource_gate']['passed']:
                campaign_stop = dict(rate_index=index, dataset=dataset, arm=arm_name, reason='shared resource gate', observed_ns=terminal)
            elif not cell['qualification_gate_passed'] and escalation_stop is None:
                escalation_stops[key] = dict(rate_index=index, dataset=dataset, arm=arm_name,
                                            reason='arm rate qualification failed', observed_ns=terminal)
        summary_path = path.with_name(path.name.replace('-records.jsonl', '-summary.json'))
        marker = closed_cell_summary(cell, config, config_sha)
        with summary_path.open('x') as stream:
            stream.write(encoded(marker) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        if on_cell_closed is not None and marker['closed'] and marker['identity_gate_passed'] and marker['bounded_memory_gate_passed']:
            on_cell_closed(marker, dict(records=path, summary=summary_path))
    return reduce_offered(records, panels, config)


def main(argv=None, *, on_cell_closed=None):
    worker_started_ns = time.monotonic_ns()
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
    campaign_cgroup_before = None
    try:
        campaign_cgroup_before = offered_cgroup_snapshot() if config['schema'] == OFFERED_SCHEMA else None
        if config['schema'] == OFFERED_SCHEMA:
            pre_admission_resource_gate = offered_resources(campaign_cgroup_before, campaign_cgroup_before, [], config)
            stats.require(pre_admission_resource_gate['passed'], 'pre-admission resource proof: ' + pre_admission_resource_gate.get('error', ''))
        panels = prepare(config, out)
        summary = (run_offered(config, binary, panels, out, worker_started_ns=worker_started_ns,
                               on_cell_closed=on_cell_closed, config_sha=digest, proof_path=proof_path,
                               campaign_cgroup_before=campaign_cgroup_before)
                   if config['schema'] == OFFERED_SCHEMA else run(config, binary, panels, out))
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
                               for d, p in panels.items()}, identity_gate_passed=summary.get('identity_gate_passed', True),
                       closed=summary['process_cleanup_complete'])
    except Exception as error:
        summary = locals().get('summary', dict(schema=(OFFERED_RESULT_SCHEMA if config['schema'] == OFFERED_SCHEMA
                                                      else 'borsuk-native-semantic-router-cold-result-v1')))
        summary.update(closed=summary.get('process_cleanup_complete', False), terminal_error=dict(type=type(error).__name__, message=str(error)),
                       identity_gate_passed=False, all_calls_successful=False, latency_improvement=False)
        if config['schema'] == OFFERED_SCHEMA:
            summary.update(campaign_cgroup_before=campaign_cgroup_before,
                           pre_admission_resource_gate=locals().get('pre_admission_resource_gate', 'UNMEASURED'))
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')
    print(encoded({k: summary.get(k, False) for k in ('closed', 'all_calls_successful', 'quality_gate_passed', 'latency_improvement')}))
    if config['schema'] == OFFERED_SCHEMA:
        return 0 if summary.get('execution_gate_passed') and summary.get('identity_gate_passed') and summary.get('closed') else 1
    return 0 if summary['all_calls_successful'] and summary.get('identity_gate_passed') and summary.get('quality_gate_passed') else 1


def offered_self_check():
    import copy
    import tempfile
    import threading
    from contextlib import ExitStack
    from unittest.mock import Mock, patch
    from scripts import run_native_cold_offered as offered
    from scripts.check_native_semantic_concurrency import fixture

    def rejected(call):
        try:
            call()
        except (ValueError, KeyError, TypeError, AssertionError):
            return
        raise AssertionError('invalid offered evidence accepted')

    config = json.loads((Path(__file__).resolve().parents[1] /
                        'docs/research/performance-architecture-20260930/semantic-cold/config.json').read_text())
    config.update(schema=OFFERED_SCHEMA, offered_qps=RATES, workers=6, base_port=18080,
                  max_dispatch_lateness_ns=125000000, cleanup_reserve_seconds=90,
                  worker_limit_seconds=3000, machine_limit_seconds=3600, native_memory_bytes=536870912,
                  profile_memory_bytes=8589934592, profile_swap_bytes=0, native_rlimit_as_bytes=4294967296,
                  namespace_connect_deadline_seconds=45, native_process_limit_seconds=60,
                  query_payload_timeout_seconds=5, ann_queries=1536, authority_pending=False,
                  code_sha256={name: old.sha(name) for name in OFFERED_CODE})
    bodies, fixtures = {}, {}

    def bind(key, body):
        bodies[key] = body
        return dict(key=key, bytes=len(body), sha256=hashlib.sha256(body).hexdigest())

    requests = b''.join((encoded(dict(query_ordinal=q, query=[q + 1.] + [0.] * 767)) + '\n').encode() for q in range(64))
    truth = struct.pack('<100I', *range(100)) * 64
    for item in config['items']:
        dataset = item['dataset']
        item['inputs'] = dict(requests=bind(dataset + '/requests', requests), truth=bind(dataset + '/truth', truth))
        item['source_identity'].update(queries_sha256=hashlib.sha256(requests).hexdigest(), truth_sha256=hashlib.sha256(truth).hexdigest(),
                                       mean_sha256=hashlib.sha256(b'plane/mean.bin').hexdigest())
        for name, mode in (('control', 'graph'), ('candidate', 'semantic')):
            call = fixture(mode, 18080)
            rows = call['header']['remote_open_stats']['metadata']
            skipped = 0
            for row in rows:
                if row['name'] in stats.EXACT_LENGTH_FILES:
                    skipped += row['logical_head_requests']
                    row['logical_head_requests'] = row['head_wall_ns'] = 0
            for report in (call['header'], call['response']):
                totals = report['transport']['totals']
                totals['attempts'] -= skipped
                totals['method_counts'][1] -= skipped
                totals['status_counts'] = [[200, totals['attempts']]]
            arm = call['arm']
            arm.update(dataset=dataset, indexes={'10': dataset + '/' + name})
            arm['metadata_sha256'] = {k: hashlib.sha256(k.encode()).hexdigest() for k in arm['metadata_files']}
            arm['metadata_sha256']['manifest.json'] = arm['authority']['root_sha256']
            call['response']['ids'] = list(range(10))
            call['expected'].update({k: copy.deepcopy(call['response'][k]) for k in stats.PARITY})
            call['truth'] = list(range(100))
            header = dict(top_k=10, declared_panel_count=64, rows=100000, dimensions=768, metric='cosine',
                          discovery=mode, authority=arm['authority'], query_split=item['query_split'], source_identity=item['source_identity'])
            reference = [header, *[dict(query_ordinal=q, **{k: call['response'][k] for k in stats.PARITY}) for q in range(64)], dict(count=64)]
            arm['inputs'] = {'reference-k10': bind(dataset + '/' + name + '/reference',
                                                ''.join(encoded(r) + '\n' for r in reference).encode())}
            item['arms'][name] = arm
            fixtures[arm['indexes']['10']] = call
    validate_config(config)
    for mutation in ('pending', 'dependency', 'rate', 'port', 'deadline', 'source', 'binary'):
        bad = copy.deepcopy(config)
        if mutation == 'pending': bad['authority_pending'] = True
        elif mutation == 'dependency': bad['code_sha256'].pop('scripts/check_native_metadata_ranges_stats.py')
        elif mutation == 'rate': bad['offered_qps'] = [8]
        elif mutation == 'port': bad['base_port'] = 8080
        elif mutation == 'deadline': bad['cleanup_reserve_seconds'] = 0
        elif mutation == 'source': bad['native_source_file_count'] = 395
        else: bad['binary']['sha256'] = '0' * 64
        rejected(lambda: validate_config(bad))

    cgroup = dict(path='/synthetic-cgroup', files={'memory.max': '8589934592', 'memory.current': '1048576',
        'memory.peak': '2097152', 'memory.swap.max': '0', 'memory.swap.current': '0', 'memory.swap.peak': '0',
        'memory.events': 'oom 0\noom_kill 0\noom_group_kill 0', 'cpu.stat': 'usage_usec 1', 'io.stat': ''})

    def fetch(bucket, identity, path):
        body = bodies[identity['key']]
        assert identity['bytes'] == len(body) and identity['sha256'] == hashlib.sha256(body).hexdigest()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return dict(path=str(path), bytes=len(body), sha256=identity['sha256'])

    with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
        directory = Path(temporary)
        panels = prepare(config, directory / 'prepared', fetch=fetch)
        active, calls, paths, state = set(), {}, [], dict(fault=None, overlap=False, spawned=0, peak=0)
        lock, barrier = threading.Lock(), threading.Barrier(6, timeout=2)
        environment, argv = dict(os.environ), sys.argv
        saved = {k: getattr(old, k) for k in ('checked_response', 'validate', 'post')}

        def spawn(command, **kwargs):
            port = int(command[-1].split(':')[-1])
            with lock:
                assert port not in active, 'native port reused before cleanup'
                active.add(port)
                state['peak'] = max(state['peak'], len(active))
                call = copy.deepcopy(fixtures[command[14]])
                call.update(port=port, initial=state['spawned'] < 6, fault=state['fault'])
                state['spawned'] += 1
                calls[port] = call
            assert kwargs['env'] == dict(environment, BORSUK_NATIVE_MEMORY_BYTES='536870912', AWS_MAX_ATTEMPTS='1', TOKIO_WORKER_THREADS='4')
            header = call['header']
            header['listen'] = f'127.0.0.1:{port}'
            if call['fault'] == 'port': header['listen'] = '127.0.0.1:8080'
            if call['fault'] == 'authority': header['authority'] = dict(header['authority'], generation=2)
            kwargs['stdout'].write(encoded(header) + '\n')
            kwargs['stdout'].flush()
            rss = 524289 if call['fault'] == 'RSS' else 1
            Path(command[3]).write_text(f'Maximum resident set size (kbytes): {rss}\nUser time (seconds): 0.01\nSystem time (seconds): 0.00\n')
            paths.append(Path(command[3]).parent)
            process = Mock(port=port)
            process.poll.return_value = None
            return process

        class Connection:
            def __init__(self, host, port, timeout):
                assert host == '127.0.0.1' and timeout == 5
                self.call = calls[port]

            def connect(self):
                if state['overlap'] and self.call['initial']: barrier.wait()

            def request(self, method, path, body, headers):
                assert method == 'POST' and path == '/search'
                self.call['body'] = body

            def getresponse(self):
                response = copy.deepcopy(self.call['response'])
                if self.call['fault'] == 'extraGET':
                    totals = response['transport']['totals']
                    totals['attempts'] += 1
                    totals['method_counts'][0] += 1
                    totals['status_counts'][0][1] += 1
                return Mock(status=200, read=lambda: encoded(response).encode())

            def close(self): pass

        def stop(process):
            if state['overlap'] and calls[process.port]['initial']: time.sleep(.025)
            with lock: active.remove(process.port)
            return dict(intentional_stop=calls[process.port]['fault'] != 'cleanup',
                        returncode=None if calls[process.port]['fault'] == 'cleanup' else 143)

        stack.enter_context(patch.object(old.subprocess, 'Popen', side_effect=spawn))
        stack.enter_context(patch.object(old.http.client, 'HTTPConnection', Connection))
        stack.enter_context(patch.object(old, 'stop', side_effect=stop))
        stack.enter_context(patch.object(sys.modules[__name__], 'offered_cgroup_snapshot', side_effect=lambda: copy.deepcopy(cgroup)))
        state.update(overlap=True, spawned=0)
        panel = panels['ReLAION']
        real, epoch, terminal, abort = offered.schedule_offers(
            lambda q, port: offered_call('unused', config, panel, panel['arms']['candidate' if q % 2 else 'control'], q, port), 1000)
        assert abort is None and real[6]['outcome'] == 'capacity_drop' and state['peak'] == 6
        assert len(real) == 64 and not active and max(r['started_ns'] for r in real[:6]) < min(r['completed_ns'] for r in real[:6])
        assert terminal >= max(r['terminal_ns'] for r in real) > max(r['completed_ns'] for r in real if r['port'] is not None)
        for port in range(18080, 18086):
            owned = [r for r in real if r['port'] == port]
            assert all(a['terminal_ns'] <= b['dispatched_ns'] for a, b in zip(owned, owned[1:]))
        assert all(getattr(old, k) is v for k, v in saved.items()) and dict(os.environ) == environment and sys.argv is argv
        state['overlap'] = False
        arm = panel['arms']['candidate']
        for fault in ('authority', 'port', 'extraGET', 'RSS', 'cleanup'):
            state['fault'] = fault
            failed, _, _, after = offered.schedule_offers(
                lambda q, port: offered_call('unused', config, panel, arm, q, port), 1000)
            assert after is not None and failed[0]['outcome'] == 'failed'
            assert any(r['outcome'] == 'aborted' for r in failed)
            assert sum(r['http_attempts'] for r in failed) <= 6
            assert not active
        state['fault'] = None
        expired, _, _, after = offered.schedule_offers(lambda q, port: (_ for _ in ()).throw(AssertionError('late call')), 1000, deadline_ns=0)
        assert after['reason'] == 'admission deadline' and all(r['outcome'] == 'aborted' for r in expired)
        templates = {}
        for dataset in DATASETS:
            for name in ('control', 'candidate'):
                panel = panels[dataset]
                templates[dataset, name] = offered_call('unused', config, panel, panel['arms'][name], 0, 18080)
                assert templates[dataset, name]['outcome'] == 'success'
        fault_templates = {}
        for mode, fault in (('native', 'authority'), ('cleanup', 'cleanup')):
            state['fault'] = fault
            panel = panels['ReLAION']
            fault_templates[mode] = offered_call('unused', config, panel, panel['arms']['control'], 0, 18080)
            assert fault_templates[mode]['outcome'] == 'failed'
        state['fault'] = None
        assert all(not path.exists() for path in paths)

        clock, mode = [1000000000], [None]
        def fake_schedule(call_one, rate, **kwargs):
            # Synthetic remaining cells exercise real preparation, reducer, files and auditor.
            closure = dict(zip(call_one.__code__.co_freevars, (cell.cell_contents for cell in call_one.__closure__)))
            dataset = closure['panel']['item']['dataset']
            name = 'control' if closure['arm']['arm']['discovery'] == 'graph' else 'candidate'
            index = RATES.index(rate) * 4 + DATASETS.index(dataset) * 2 + int(name == 'candidate')
            epoch = clock[0]
            rows = []
            for q in range(64):
                row = copy.deepcopy(templates[dataset, name])
                dispatch = epoch + round(q * 1e9 / rate) + 1000000
                start, attempt, connected, end = dispatch + 1000000, dispatch + 2000000, dispatch + 3000000, dispatch + 5000000
                row.update(query_ordinal=q, offered_qps=rate, scheduled_ns=epoch + round(q * 1e9 / rate), dispatched_ns=dispatch,
                    port=18080, started_ns=start, successful_connect_attempt_ns=attempt, connected_ns=connected, completed_ns=end,
                    terminal_ns=end + 20000000, cold_start_to_first_http_response_ns=end-start,
                    before_successful_connect_attempt_ns=attempt-start, successful_tcp_connect_ns=connected-attempt,
                    first_post_to_response_ns=end-connected, incoming_http_wall_ns=end-attempt)
                body = panels[dataset]['arms'][name]['bodies'][q]
                row.update(request_bytes=len(body), request_sha256=hashlib.sha256(body).hexdigest())
                rows.append(row)
            if mode[0] == 'timing' and index == 4:
                for key in ('dispatched_ns', 'started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns', 'terminal_ns'):
                    rows[0][key] += 125000001
            if mode[0] == 'resource' and index == 4:
                rows[0]['cgroup_after'] = copy.deepcopy(cgroup)
                rows[0]['cgroup_after']['files']['memory.peak'] = 'UNMEASURED'
                rows[0]['cgroup_after']['diagnostics'] = {'memory.peak': dict(type='FileNotFoundError', errno=2)}
            if mode[0] == 'deadline' and index == 4:
                epoch = kwargs['deadline_ns']
                abort = dict(query_ordinal=0, reason='admission deadline', observed_ns=epoch)
                rows = [dict(query_ordinal=q, offered_qps=rate, scheduled_ns=epoch + round(q * 1e9 / rate),
                         dispatched_ns=None, started_ns=None, completed_ns=None, port=None, outcome='aborted',
                         namespace_start_attempted=False, native_process_started=False, http_attempts=0, valid_ann_requests=0,
                         abort_after=abort, terminal_ns=epoch) for q in range(64)]
                clock[0] = epoch + 1
                return rows, epoch, epoch + 1, abort
            if mode[0] == 'drop' and index == 4:
                for q in range(6):
                    rows[q]['port'] = 18080 + q
                    rows[q]['native_header']['listen'] = f'127.0.0.1:{18080 + q}'
                    rows[q]['native_server_log'] = encoded(rows[q]['native_header']) + '\n'
                    rows[q]['terminal_ns'] = epoch + 14000000000
                rows[6] = dict(query_ordinal=6, offered_qps=rate, scheduled_ns=epoch + round(6e9 / rate),
                               dispatched_ns=epoch + round(6e9 / rate) + 1000000,
                               terminal_ns=epoch + round(6e9 / rate) + 1000000, port=None, started_ns=None,
                               completed_ns=None, outcome='capacity_drop', http_attempts=0, valid_ann_requests=0,
                               namespace_start_attempted=False, native_process_started=False)
            if mode[0] == 'transport' and index == 4:
                row = rows[0]
                row.update(outcome='failed', error_type='TimeoutError', error='wire timeout', failure_kind='transport',
                           abort_admissions=False, completed_ns=None, http_status=None, raw_response_complete=False,
                           telemetry_validation_errors=['missing response telemetry'])
                for key in ('accounting', 'response', 'raw_response', 'raw_response_base64'): row.pop(key, None)
            abort = None
            if mode[0] in ('native', 'cleanup') and index == 4:
                failure = copy.deepcopy(fault_templates[mode[0]])
                failure.update(query_ordinal=0, offered_qps=rate, scheduled_ns=epoch, dispatched_ns=epoch + 1000000,
                               started_ns=epoch + 2000000, completed_ns=epoch + 5000000,
                               terminal_ns=epoch + 25000000, port=18080, valid_ann_requests=failure.get('valid_ann_requests', 0))
                abort = dict(query_ordinal=0, reason='cleanup unconfirmed' if mode[0] == 'cleanup' else 'fatal call failure',
                             observed_ns=failure['terminal_ns'])
                rows = [failure, *[dict(query_ordinal=q, offered_qps=rate, scheduled_ns=epoch + round(q * 1e9 / rate),
                         dispatched_ns=None, started_ns=None, completed_ns=None, port=None, outcome='aborted',
                         namespace_start_attempted=False, native_process_started=False, http_attempts=0, valid_ann_requests=0,
                         abort_after=abort, terminal_ns=failure['terminal_ns']) for q in range(1, 64)]]
            terminal = max(r['terminal_ns'] for r in rows) + 1000000
            clock[0] = terminal + 1000000
            return rows, epoch, terminal, abort

        checkpoints = []
        def checkpoint(cell, paths):
            assert cell == json.loads(paths['summary'].read_text())
            assert cell['records'] == dict(bytes=paths['records'].stat().st_size, sha256=old.sha(paths['records']))
            assert cell['closed'] and cell['identity_gate_passed'] and cell['bounded_memory_gate_passed']
            checkpoints.append((cell, paths))

        cfg, proof = directory / 'config.json', directory / 'proof.json'
        cfg.write_text(encoded(config) + '\n')
        proof.write_text('{}\n')
        stack.enter_context(patch.object(offered, 'schedule_offers', side_effect=fake_schedule))
        def tick():
            clock[0] += 1
            return clock[0]
        stack.enter_context(patch.object(time, 'monotonic_ns', side_effect=tick))
        stack.enter_context(patch.object(sys.modules[__name__], 'validate_runtime', return_value={}))
        from scripts import run_native_semantic_router_cold as audit_runtime
        if audit_runtime is not sys.modules[__name__]:
            stack.enter_context(patch.object(audit_runtime, 'validate_runtime', return_value={}))
        stack.enter_context(patch.object(old, 'fetch', side_effect=fetch))
        stack.enter_context(patch.object(os, 'sched_getaffinity', return_value={4, 5}))
        stack.enter_context(patch.dict(os.environ, AWS_MAX_ATTEMPTS='1'))
        out = directory / 'complete'
        assert main([str(cfg), old.sha(cfg), 'unused', str(proof), str(out)], on_cell_closed=checkpoint) == 0
        summary = json.loads((out / 'summary.json').read_text())
        assert len(checkpoints) == 24 and summary['ann_calls_successful'] == summary['offered'] == 1536
        assert summary['eight_qps_attained'] and summary['execution_gate_passed']
        assert stats.check_saved(out, old.sha(cfg), 'unused')['records'] == 1536
        changed_summary = copy.deepcopy(summary)
        changed_summary['offered_gate_passed'] = False
        (out / 'summary.json').write_text(encoded(changed_summary))
        rejected(lambda: stats.check_saved(out, old.sha(cfg), 'unused'))
        (out / 'summary.json').write_text(encoded(summary))
        all_rows = [json.loads(line) for _, paths in checkpoints for line in paths['records'].read_text().splitlines()]
        for mutation in ('authority', 'port', 'extraGET', 'RSS', 'cleanup', 'schedule', 'reuse', 'deadline', 'summary'):
            bad = copy.deepcopy(all_rows[:64])
            row = bad[0]
            if mutation == 'authority': row['expected_authority']['generation'] += 1
            elif mutation == 'port': row['port'] = 18081
            elif mutation == 'extraGET': row['accounting']['final_process_transport']['attempts'] += 1
            elif mutation == 'RSS': row['native_time_log'] = row['native_time_log'].replace('kbytes): 1', 'kbytes): 524289')
            elif mutation == 'cleanup': row['native_close']['returncode'] = None
            elif mutation == 'schedule': row['scheduled_ns'] += 1
            elif mutation == 'reuse': bad[1]['dispatched_ns'] = row['dispatched_ns']
            elif mutation == 'deadline': row['cell_receipt']['admission_deadline_ns'] = row['dispatched_ns']
            else: row['cell_receipt']['terminal_ns'] -= 1000000000000
            rejected(lambda: reduce_offered_cell(bad, panels['ReLAION'], config))
        boundary_panel, boundary = copy.deepcopy(panels['ReLAION']), copy.deepcopy(all_rows[:64])
        for q in range(32, 64):
            boundary_panel['truths'][q][9] = 100
            boundary[q]['truth_at_10'][9] = 100
            boundary[q]['returned_hits'] = 9
        cell = reduce_offered_cell(boundary, boundary_panel, config)
        assert cell['returned_hits'] == 608 and cell['quality_gate_passed']
        boundary_panel['truths'][31][9] = boundary[31]['truth_at_10'][9] = 100
        boundary[31]['returned_hits'] = 9
        assert not reduce_offered_cell(boundary, boundary_panel, config)['quality_gate_passed']
        for keep in (0, 32, 63):
            population = copy.deepcopy(all_rows[:64])
            for row in population[keep:]: row['outcome'] = 'capacity_drop'
            assert all_offer_tails(population)['p99'] == 'UNBOUNDED'
        for mutation in ('missing', 'limit', 'swap', 'oom'):
            bad = copy.deepcopy(cgroup)
            if mutation == 'missing': del bad['files']['memory.swap.peak']
            elif mutation == 'limit': bad['files']['memory.max'] = 'max'
            elif mutation == 'swap': bad['files']['memory.swap.peak'] = '1'
            else: bad['files']['memory.events'] = 'oom 1\noom_kill 0\noom_group_kill 0'
            assert not offered_resources(cgroup, bad, [], config)['passed']
        for campaign_mode, status in (('timing', 0), ('drop', 0), ('transport', 0), ('deadline', 1), ('resource', 1), ('native', 1), ('cleanup', 1)):
            mode[0] = campaign_mode
            checkpoints.clear()
            failed_out = directory / campaign_mode
            actual_status = main([str(cfg), old.sha(cfg), 'unused', str(proof), str(failed_out)], on_cell_closed=checkpoint)
            assert actual_status == status, json.loads((failed_out / 'summary.json').read_text())
            result = json.loads((failed_out / 'summary.json').read_text())
            assert not result['qualification_gate_passed'] and result['aborted_calls'] > 0
            assert result['largest_passing_tested_offered_qps']['ReLAION']['control'] == .25
            assert len(list(failed_out.glob('rate*-records.jsonl'))) == len(list(failed_out.glob('rate*-summary.json'))) == 24
            assert len(checkpoints) == (24 if status == 0 or campaign_mode == 'deadline' else 4)
            if campaign_mode == 'resource':
                assert 'memory.peak' in result['cells'][4]['resource_gate']['error']
                failed_row = json.loads((failed_out / 'rate1-relaion-control-records.jsonl').read_text().splitlines()[0])
                assert failed_row['raw_response_base64'] and failed_row['cgroup_after']['diagnostics']['memory.peak']['errno'] == 2
            if campaign_mode in ('drop', 'transport'):
                assert result['execution_gate_passed'] and result['offered_gate_passed']
                assert result['cells'][4]['all_offer_scheduled_to_valid_response_ms']['p99'] == 'UNBOUNDED'
                assert not result['matched_comparisons'][2]['valid_matched_cells']
            assert stats.check_saved(failed_out, old.sha(cfg), 'unused')['records'] == 1536
        assert not active
    print('PASS offered: six-call barrier/seventh drop/cleanup ownership; 24 cells/1536 positions; raw authority/port/GET/RSS/cleanup faults; 608/607, dispatch/deadline/resources/all-offer tails; immutable markers/offline replay; native UNRUN')


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
        headless = {'page_digests.bin', 'plane/mean.bin', 'plane/page_digests.bin',
                    'router/manifest.json', 'router/membership.bin'}
        order = ['manifest.json', 'page_manifest.json', 'page_digests.bin']
        if arm['discovery'] == 'graph':
            order += ['centroids.bin', 'graph.bin', 'diverse_graph.bin']
        order += ['plane/manifest.json', 'plane/mean.bin', 'plane/page_digests.bin']
        if arm['discovery'] == 'semantic':
            order += ['router/manifest.json', 'router/membership.bin']
        rows = []
        for i, name in enumerate(order):
            size = arm['metadata_files'][name]
            wave = 0 if i == 0 else (i - 1) // 4 + 1
            width = 1 if wave == 0 else min(4, len(order) - 1 - (wave - 1) * 4)
            heads = int(name not in headless)
            rows.append(dict(name=name, bytes=size, chunks=1, head_wall_ns=heads,
                             get_wall_ns=int(size <= 4194304), stream_wall_ns=2, write_wall_ns=1,
                             logical_head_requests=heads, logical_get_requests=(size + 4194303) // 4194304,
                             payload_buffer_bound_bytes=min(size, (8 // width) * 4194304),
                             metadata_wave=wave, metadata_wave_wall_ns=10))
        router_head = int(arm['discovery'] == 'semantic')
        header = dict(phase='ready', listen='127.0.0.1:8080', authority=arm['authority'],
            head_read_wall_ns=2, remote_open_wall_ns=130,
            remote_open_stats=dict(metadata=rows, staging_wall_ns=100, decode_wall_ns=20,
                                  source_head_requests=1, source_head_wall_ns=5,
                                  router_head_requests=router_head, router_head_wall_ns=5 * router_head))
        header['transport'] = transport(sum(r['logical_get_requests'] for r in rows) + 4,
                                        sum(r['logical_head_requests'] for r in rows) + 1 + router_head,
                                        sum(arm['metadata_files'].values())
                                        + arm['head_file']['bytes'] + arm['metadata_files']['manifest.json'] + 37)
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
                assert startup['authority_head_JSON_GETs'] == startup['authority_generation_root_GETs'] == 1
                assert startup['authority_head_JSON_bytes'] == 200
                assert startup['authority_generation_root_bytes'] == 5000
                assert startup['metadata']['logical_metadata_head_requests'] == (3 if arm['discovery'] == 'semantic' else 6)
                for row in header['remote_open_stats']['metadata']:
                    bad = copy.deepcopy(header)
                    changed = next(r for r in bad['remote_open_stats']['metadata'] if r['name'] == row['name'])
                    changed['logical_head_requests'] = 1 - row['logical_head_requests']
                    rejected(lambda: stats.validate_ready(bad, arm))
                    if row['logical_head_requests'] == 0:
                        changed['logical_head_requests'] = 0
                        changed['head_wall_ns'] = 1
                        rejected(lambda: stats.validate_ready(bad, arm))
                for mutation in ('missing_GET', 'missing_HEAD', 'missing_PUT', 'extra_GET', 'extra_HEAD', 'extra_PUT', 'DELETE',
                                 'status', 'transport_failure', 'stream_failure', 'zero_payload', 'short_payload', 'saturated'):
                    bad = copy.deepcopy(header)
                    totals = bad['transport']['totals']
                    if mutation.startswith(('missing_', 'extra_')) or mutation == 'DELETE':
                        method = stats.METHODS.index(mutation.split('_')[-1])
                        change = -1 if mutation.startswith('missing') else 1
                        totals['method_counts'][method] += change
                        totals['attempts'] += change
                        totals['status_counts'][0][1] += change
                    elif mutation == 'status': totals['status_counts'][0][0] = 403
                    elif mutation == 'transport_failure':
                        totals['transport_failures'] = 1
                        totals['status_counts'][0][1] -= 1
                    elif mutation == 'stream_failure': totals['stream_failures'] = 1
                    elif mutation in ('zero_payload', 'short_payload'):
                        totals['consumed_payload_bytes'] -= 37 + int(mutation == 'short_payload')
                    else: totals['consumed_payload_bytes'] = 2**64 - 1
                    rejected(lambda: stats.validate_ready(bad, arm))
                bad_arm = copy.deepcopy(arm)
                bad_arm['metadata_files']['manifest.json'] += 1
                rejected(lambda: stats.validate_ready(header, bad_arm))
                for prefix in ('', 'source_', 'router_'):
                    if prefix == 'router_' and arm['discovery'] == 'graph':
                        continue
                    bad_query = query_response(arm)
                    bad_query[prefix + 'failed_gets'] = 1
                    rejected(lambda: stats.validate_query(bad_query, arm))
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
            assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES'] == str(config['native_memory_bytes'])
            assert kwargs['env']['AWS_MAX_ATTEMPTS'] == '1'
            assert kwargs['env']['TOKIO_WORKER_THREADS'] == '4'
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
                    charges = pooled['known_logical_charge_totals']
                    assert charges['authority_head_JSON_GETs'] == charges['authority_generation_root_GETs'] == 128
                    assert charges['authority_head_JSON_bytes'] == 200 * 128
                    assert charges['authority_generation_root_bytes'] == 5000 * 128
            assert all(not path.exists() for path in temporary_paths)
            audited = stats.check_saved(output, old.sha(cfg), str(binary))
            assert audited == dict(all_calls_successful=True, quality_gate_passed=True, latency_improvement=True, records=512)
            bad_saved = copy.deepcopy(result)
            bad_saved['datasets']['ReLAION']['pooled']['candidate']['latency_ms']['whole_cold']['p95'] += 1
            (output / 'summary.json').write_text(encoded(bad_saved))
            rejected(lambda: stats.check_saved(output, old.sha(cfg), str(binary)))
            (output / 'summary.json').write_text(encoded(result))
            panels = prepare(config, directory / 'prepared')
            for field in ('authority_generation_root_GETs', 'authority_generation_root_bytes',
                          'inferred_credential_consumed_bytes'):
                bad_records = copy.deepcopy(records)
                bad_records[0]['startup_accounting'][field] += 1
                rejected(lambda: reduce_run(bad_records, panels, config))
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
    print('PASS 512 fixed ABBA calls; exact head/root/metadata/IMDSv2 startup and GET-only query delta; extra/missing GET/HEAD/PUT, root/credential receipts and failed GETs rejected; fail-fast/511 aborted and zero-HTTP startup failure; 608/607 block gates; raw failures, cleanup and restoration; native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--offered-self-check']:
        offered_self_check()
    elif sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        raise SystemExit(main())
