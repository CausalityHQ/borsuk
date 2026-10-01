#!/usr/bin/env python3
"""Strict semantic/graph cold HTTP accounting; no native requests or retries.

CLI: python3 -m scripts.check_native_semantic_router_stats OUTPUT CONFIG_SHA BINARY
     python3 -m scripts.check_native_semantic_router_stats --self-check
The auditor reuses only saved panel envelopes and validates every raw ready/query
outcome, then recomputes gates and tails. A failed call or quality gate exits 1.
"""

STAGES = ('discovery', 'source', 'planning', 'sq8')
COUNTERS = ('submitted_gets', 'verified_bytes', 'failed_gets')
PARITY = ('ids', 'ranges', 'planned_bytes', *COUNTERS,
          *('source_' + name for name in COUNTERS),
          *('router_' + name for name in COUNTERS))
COMMON_FILES = {'manifest.json', 'page_manifest.json', 'page_digests.bin',
                'plane/manifest.json', 'plane/mean.bin', 'plane/page_digests.bin'}
METHODS = ['GET', 'HEAD', 'PUT', 'DELETE', 'POST', 'PATCH', 'OPTIONS', 'CONNECT', 'TRACE', 'other']
CREDENTIAL_PROTOCOL = 'instance-imdsv2'
CREDENTIAL_PAYLOAD_ATTRIBUTION = 'inferred: ready consumed payload minus authenticated S3 startup bytes; no credential values read'
UNKNOWN = ['unread_response_payload_bytes', 'response_header_bytes',
           'request_wire_bytes', 'kernel_tls_wire_bytes']


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, name, minimum=0, maximum=2**64 - 1):
    require(type(value) is int and minimum <= value <= maximum, name)
    return value


def digest(value):
    require(type(value) is str and len(value) == 64
            and all(c in '0123456789abcdef' for c in value), 'SHA256')
    return value


def validate_roster(arm):
    mode, files, hashes = arm['discovery'], arm['metadata_files'], arm['metadata_sha256']
    require(mode in ('graph', 'semantic'), 'discovery mode')
    additions = ({'centroids.bin', 'graph.bin', 'diverse_graph.bin'} if mode == 'graph'
                 else {'router/manifest.json', 'router/membership.bin'})
    require(set(files) == COMMON_FILES | additions == set(hashes), 'explicit startup roster')
    for name, size in files.items():
        integer(size, name, 1)
        digest(hashes[name])
    require(files['manifest.json'] <= 65536, 'generation root cap')
    require(files['page_manifest.json'] <= 65536 and files['plane/manifest.json'] <= 65536,
            'metadata envelope cap')
    require(files['plane/mean.bin'] == 768 * 4
            and files['plane/page_digests.bin'] == 3125 * 32
            and files['page_digests.bin'] == 391 * 32, 'FIRST100k metadata geometry')
    require(hashes['manifest.json'] == arm['authority']['root_sha256'], 'root roster identity')
    integer(arm['head_file']['bytes'], 'head bytes', 1, 65536)
    digest(arm['head_file']['sha256'])
    if mode == 'semantic':
        require(files['router/manifest.json'] <= 1048576, 'router root cap')
        require(files['router/membership.bin'] == 3125 * 4, 'membership bound')
        require(arm['leaf_object']['bytes'] == 3125 * (4 + 768 * 2), 'whole-leaf object geometry')
        digest(arm['leaf_object']['sha256'])
    return files


def validate_startup(stats, arm, wall_ns):
    files = validate_roster(arm)
    integer(wall_ns, 'remote open wall', 1, 2**128 - 1)
    rows = stats['metadata']
    require(len(rows) == len(files) and len({r['name'] for r in rows}) == len(rows)
            and {r['name']: r['bytes'] for r in rows} == files, 'startup roster/bytes')
    for row in rows:
        for name in ('bytes', 'chunks', 'head_wall_ns', 'get_wall_ns', 'stream_wall_ns',
                     'write_wall_ns', 'logical_head_requests', 'logical_get_requests',
                     'payload_buffer_bound_bytes'):
            integer(row[name], name)
        require(row['chunks'] > 0 and row['logical_head_requests'] == 1, 'metadata HEAD/chunks')
        require(row['logical_get_requests'] == (row['bytes'] + 4194303) // 4194304,
                'metadata logical GETs')
        require(row['payload_buffer_bound_bytes'] == min(row['bytes'], 8 * 4194304),
                'metadata payload bound')
        require(row['write_wall_ns'] <= row['stream_wall_ns'], 'nested write timing')
        if row['bytes'] > 4194304:
            require(row['get_wall_ns'] == 0, 'range headers already included in stream')
    for name in ('staging_wall_ns', 'decode_wall_ns', 'source_head_wall_ns', 'router_head_wall_ns'):
        integer(stats[name], name, maximum=2**128 - 1)
    require(sum(r['head_wall_ns'] + r['get_wall_ns'] + r['stream_wall_ns'] for r in rows)
            <= stats['staging_wall_ns'], 'metadata staging timing')
    require(integer(stats['source_head_requests'], 'source HEAD') == 1, 'source HEAD count')
    router_heads = int(arm['discovery'] == 'semantic')
    require(integer(stats['router_head_requests'], 'router HEAD') == router_heads, 'router HEAD count')
    if not router_heads:
        require(stats['router_head_wall_ns'] == 0, 'graph router HEAD time')
    require(sum(stats[k] for k in ('staging_wall_ns', 'decode_wall_ns', 'source_head_wall_ns',
                                 'router_head_wall_ns')) <= wall_ns, 'startup stage bounds')
    return dict(metadata_objects=len(rows), metadata_bytes=sum(files.values()),
                logical_metadata_get_requests=sum(r['logical_get_requests'] for r in rows),
                logical_metadata_head_requests=len(rows), source_head_requests=1,
                router_head_requests=router_heads,
                payload_buffer_bound_bytes=max(r['payload_buffer_bound_bytes'] for r in rows),
                staged_selected_leaf_bytes=0)


def validate_stages(stages, wall_ns, mode, router_gets, success):
    integer(wall_ns, 'native wall', maximum=2**128 - 1)
    require(set(stages) == {*STAGES, 'leaf_peak_inflight'}, 'query stage fields')
    previous, stopped, result = 0, False, {}
    for name in STAGES:
        stage = stages[name]
        require(set(stage) == {'start_ns', 'end_ns'}, 'stage interval fields')
        start = integer(stage['start_ns'], name + ' start', maximum=2**128 - 1)
        end = integer(stage['end_ns'], name + ' end', maximum=2**128 - 1)
        if start == 0:
            require(end == 0 and not success, 'unentered stage')
            stopped = True
        else:
            require(not stopped and previous <= start <= end <= wall_ns, 'query stage bounds/order')
            previous = end
        result[name + '_ms'] = (end - start) / 1e6
    peak = integer(stages['leaf_peak_inflight'], 'leaf peak', maximum=16)
    require(peak <= router_gets and (mode != 'graph' or peak == router_gets == 0), 'leaf concurrency')
    if success and mode == 'semantic':
        require(peak > 0, 'missing leaf concurrency')
    return dict(result, leaf_peak_inflight=peak)


def validate_query(response, arm, success=True, expected=None, truth=None, *, telemetry=True):
    require(response['authority'] == arm['authority'], 'response authority')
    for prefix, gets_cap, bytes_cap in (('source_', 128, 67108864), ('', 32, 16773120),
                                       ('router_', 16, 2097152)):
        gets = integer(response[prefix + 'submitted_gets'], prefix + 'GETs', maximum=gets_cap)
        size = integer(response[prefix + 'verified_bytes'], prefix + 'bytes', maximum=bytes_cap)
        failed = integer(response[prefix + 'failed_gets'], prefix + 'failures', maximum=gets)
        require((gets > 0 or size == 0) and (not success or failed == 0), 'query read accounting')
        if success and prefix != 'router_':
            require(gets > 0 and size > 0, 'missing source/SQ8 reads')
    if arm['discovery'] == 'graph':
        require(all(response['router_' + k] == 0 for k in COUNTERS), 'graph leaf charges')
    elif success:
        require(8 <= response['router_submitted_gets'] <= 16
                and response['router_verified_bytes'] > 0, 'fixed eight-leaf/boundary policy')
    stages = (validate_stages(response['query_stages'], response['native_wall_ns'], arm['discovery'],
                             response['router_submitted_gets'], success) if telemetry else None)
    if not success:
        return dict(stages=stages)
    ids, ranges = response['ids'], response['ranges']
    require(len(ids) == len(set(ids)) == 10, 'top-10 roster')
    for value in ids:
        integer(value, 'returned ID', maximum=99999)
    require(len(ranges) == response['submitted_gets'], 'SQ8 range count')
    previous, size = 0, 0
    for start, end in ranges:
        integer(start, 'range start', maximum=78000000)
        integer(end, 'range end', 1, 78000000)
        require(previous <= start < end and start % 199680 == 0
                and (end % 199680 == 0 or end == 78000000), 'SQ8 ordered page ranges')
        previous, size = end, size + end - start
    require(size == integer(response['planned_bytes'], 'planned bytes') == response['verified_bytes'],
            'SQ8 plan/verified bytes')
    if expected is not None:
        require(all(response[k] == expected[k] for k in PARITY), 'source/scorer ordered-ID/range/counter parity')
    hits = None if truth is None else len(set(ids) & set(truth[:10]))
    return dict(stages=stages, returned_hits=hits)


def validate_transport(report, success):
    require(report['schema'] == 'borsuk-native-transport-v1'
            and report['scope'] == 'process_all_native_s3_readers'
            and report['per_query_delta'] is False, 'process transport scope')
    require(report['attempt_measurement'] == 'submitted HttpService calls, not confirmed wire or S3 requests'
            and report['method_order'] == METHODS, 'transport attempt definition')
    require(report['status_counts_format'] == '[http_status,count] nonzero entries'
            and report['payload_measurement'] == 'consumed response data frames, including unauthenticated payload',
            'transport status/payload definition')
    require(report['unknown'] == UNKNOWN
            and integer(report['dropped_error_body_consumed_bytes'], 'dropped error bytes') == 0,
            'unknown wire/unread payload distinction')
    totals = report['totals']
    for name in ('attempts', 'transport_failures', 'stream_failures', 'consumed_payload_bytes', 'dropped_error_bodies'):
        integer(totals[name], name, maximum=2**64 - 2)
    methods = totals['method_counts']
    require(len(methods) == 10, 'method count shape')
    for value in methods:
        integer(value, 'method count', maximum=2**64 - 2)
    require(sum(methods) == totals['attempts'] and not any(methods[3:]), 'native read/IMDS method totals')
    statuses = totals['status_counts']
    require(len({pair[0] for pair in statuses}) == len(statuses), 'duplicate HTTP status')
    for status, count in statuses:
        integer(status, 'status', 100, 999)
        integer(count, 'status count', 1, 2**64 - 2)
    require(sum(count for _, count in statuses) + totals['transport_failures'] == totals['attempts'],
            'transport outcome totals')
    require(totals['stream_failures'] <= totals['attempts']
            and totals['dropped_error_bodies'] <= sum(n for s, n in statuses if not 200 <= s < 300),
            'stream/error-body totals')
    if success:
        require(totals['transport_failures'] == totals['stream_failures'] == totals['dropped_error_bodies'] == 0
                and all(200 <= s < 300 for s, _ in statuses), 'successful native transport')
    return totals


def validate_ready(header, arm):
    require(header['phase'] == 'ready' and header['authority'] == arm['authority'], 'ready identity')
    metadata = validate_startup(header['remote_open_stats'], arm, header['remote_open_wall_ns'])
    integer(header['head_read_wall_ns'], 'head read wall', maximum=2**128 - 1)
    ready = validate_transport(header['transport'], True)
    heads = metadata['logical_metadata_head_requests'] + 1 + metadata['router_head_requests']
    gets = metadata['logical_metadata_get_requests'] + 1  # Authenticated head.json is a separate GET.
    # object_store 0.14.1 shares NativeConnector with its instance provider:
    # PUT token, GET role, GET credentials, before the first authenticated S3 call.
    require(ready['method_counts'] == [gets + 2, heads, 1] + [0] * 7, 'startup S3/IMDS attempts / no hidden retries')
    credential_bytes = ready['consumed_payload_bytes'] - metadata['metadata_bytes'] - arm['head_file']['bytes']
    integer(credential_bytes, 'inferred credential payload', 1, 2**64 - 2)
    return dict(metadata=metadata, startup_transport=ready, credential_protocol=CREDENTIAL_PROTOCOL,
                declared_credential_submissions=3, inferred_credential_consumed_bytes=credential_bytes,
                credential_payload_attribution=CREDENTIAL_PAYLOAD_ATTRIBUTION)


def validate_outcome(header, response, arm, success):
    startup = validate_ready(header, arm)
    ready = startup['startup_transport']
    final = validate_transport(response['transport'], success)
    for name in ('attempts', 'transport_failures', 'stream_failures', 'consumed_payload_bytes', 'dropped_error_bodies'):
        require(final[name] >= ready[name], 'nonmonotonic process transport')
    ready_status, final_status = dict(ready['status_counts']), dict(final['status_counts'])
    require(all(final_status.get(s, 0) >= n for s, n in ready_status.items()), 'nonmonotonic statuses')
    if 'query_stages' in response:
        validate_query(response, arm, success)
        query_gets = sum(response[p + 'submitted_gets'] for p in ('source_', '', 'router_'))
        verified = sum(response[p + 'verified_bytes'] for p in ('source_', '', 'router_'))
    else:
        require(not success and response.get('error') in ('invalid_request', 'query_capacity'),
                'missing query telemetry')
        query_gets, verified = 0, 0
    delta_methods = [b - a for a, b in zip(ready['method_counts'], final['method_counts'])]
    require(delta_methods == [query_gets, 0] + [0] * 8, 'query submissions / no hidden retries')
    payload = final['consumed_payload_bytes'] - ready['consumed_payload_bytes']
    require(payload >= verified and (not success or payload == verified), 'consumed vs verified payload')
    return dict(**startup, final_process_transport=final,
                query_transport_submissions=query_gets, query_consumed_payload_bytes=payload,
                unknown=UNKNOWN, confirmed_wire_requests='UNMEASURED', fetch_waves='UNMEASURED')


def self_check():
    # A missing stage validator must fail before any runtime is written.
    assert callable(globals().get('validate_stages')), 'stage validator missing'
    stages = {name: dict(start_ns=i * 10 + 1, end_ns=i * 10 + 9)
              for i, name in enumerate(('discovery', 'source', 'planning', 'sq8'))}
    stages['leaf_peak_inflight'] = 8
    assert validate_stages(stages, 40, 'semantic', 8, True)['source_ms'] == 8 / 1e6
    import copy
    for mutation in ('missing', 'overlap', 'wall', 'peak', 'boolean', 'gap'):
        bad = copy.deepcopy(stages)
        if mutation == 'missing': del bad['source']
        elif mutation == 'overlap': bad['source']['start_ns'] = 1
        elif mutation == 'wall': bad['sq8']['end_ns'] = 41
        elif mutation == 'peak': bad['leaf_peak_inflight'] = 17
        elif mutation == 'boolean': bad['source']['start_ns'] = True
        else: bad['source'] = dict(start_ns=0, end_ns=0)
        try:
            validate_stages(bad, 40, 'semantic', 8, True)
        except (ValueError, KeyError):
            pass
        else:
            raise AssertionError(mutation)
    partial = copy.deepcopy(stages)
    for name in ('planning', 'sq8'):
        partial[name] = dict(start_ns=0, end_ns=0)
    validate_stages(partial, 40, 'semantic', 8, False)
    print('PASS semantic query stage bounds, missing telemetry and partial failure stages')


def check_saved(output, config_sha, binary):
    import base64
    import json
    from pathlib import Path
    from scripts import run_native_semantic_router_cold as runtime
    out = Path(output).absolute()
    require(runtime.old.sha(out / 'config.json') == digest(config_sha), 'saved config identity')
    config = json.loads((out / 'config.json').read_text())
    runtime.validate_config(config)
    runtime.validate_runtime(config, binary, out / 'qualification.json')

    def saved_input(bucket, identity, path):
        size = identity.get('range_bytes', identity['bytes'])
        sha = runtime.input_sha(identity)
        require(path.stat().st_size == size and runtime.old.sha(path) == sha, 'saved input identity: ' + str(path))
        return dict(path=str(path), bytes=size, sha256=sha)

    panels = runtime.prepare(config, out, fetch=saved_input)
    records = [json.loads(line) for line in (out / 'records.jsonl').read_text().splitlines()]
    for record in records:
        if record['outcome'] == 'failed':
            # Malformed/missing telemetry is preserved as a failure, never upgraded.
            arm = panels[record['dataset']]['arms'][record['arm']]['arm']
            if record['raw_response_complete']:
                raw = base64.b64decode(record['raw_response_base64'], validate=True)
                require(record['raw_response'] == raw.decode(errors='replace'), 'raw failure byte identity')
            if 'startup_accounting' in record:
                require(record['startup_accounting'] == validate_ready(record['native_header'], arm), 'failed startup accounting receipt')
            if 'accounting' in record:
                require(record['accounting'] == validate_outcome(record['native_header'], record['response'], arm, False),
                        'failed query accounting receipt')
            try:
                header = record['native_header']
                require(header['phase'] == 'ready' and header['authority'] == arm['authority'], 'failed ready identity')
                validate_startup(header['remote_open_stats'], arm, header['remote_open_wall_ns'])
                validate_transport(header['transport'], True)
                validate_outcome(header, record['response'], arm, False)
            except (ValueError, KeyError, TypeError):
                require(bool(record['telemetry_validation_errors']), 'unreported failed telemetry')
    actual = runtime.reduce_run(records, panels, config)
    summary = json.loads((out / 'summary.json').read_text())
    require(summary.get('identity_gate_passed') is True, 'terminal identity gate failed')
    require(summary['config_sha256'] == config_sha, 'summary config binding')
    require(all(summary.get(name) == value for name, value in actual.items()), 'saved summary/gates/tails')
    return dict(all_calls_successful=actual['all_calls_successful'], quality_gate_passed=actual['quality_gate_passed'],
                latency_improvement=actual['latency_improvement'], records=len(records))


if __name__ == '__main__':
    import sys
    if not __debug__:
        raise RuntimeError('self-checks require Python assertions enabled')
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv) == 4:
        if __package__ in (None, ''):
            from pathlib import Path
            sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        import json
        result = check_saved(*sys.argv[1:])
        print(json.dumps(result, sort_keys=True))
        raise SystemExit(0 if result['all_calls_successful'] and result['quality_gate_passed'] else 1)
    else:
        raise SystemExit('usage: OUTPUT CONFIG_SHA BINARY | --self-check')
