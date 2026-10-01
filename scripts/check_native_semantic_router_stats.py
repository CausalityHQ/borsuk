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
EXACT_LENGTH_FILES = {'page_digests.bin', 'plane/mean.bin', 'plane/page_digests.bin',
                      'router/manifest.json', 'router/membership.bin'}
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
    order = ['manifest.json', 'page_manifest.json', 'page_digests.bin']
    if arm['discovery'] == 'graph':
        order += ['centroids.bin', 'graph.bin', 'diverse_graph.bin']
    order += ['plane/manifest.json', 'plane/mean.bin', 'plane/page_digests.bin']
    if arm['discovery'] == 'semantic':
        order += ['router/manifest.json', 'router/membership.bin']
    require([r['name'] for r in rows] == order, 'metadata roster order')
    waves = []
    for row in rows:
        wave = integer(row['metadata_wave'], 'metadata wave')
        integer(row['metadata_wave_wall_ns'], 'metadata wave wall', 1, 2**128 - 1)
        if not waves or wave != waves[-1][0]['metadata_wave']:
            require(wave == len(waves), 'metadata wave order')
            waves.append([])
        waves[-1].append(row)
    require(len(waves[0]) == 1, 'metadata root wave')
    for batch in waves:
        require(len(batch) <= 4, 'metadata wave width')
        wave_wall = batch[0]['metadata_wave_wall_ns']
        for row in batch:
            for name in ('bytes', 'chunks', 'logical_head_requests', 'logical_get_requests',
                         'payload_buffer_bound_bytes'):
                integer(row[name], name)
            for name in ('head_wall_ns', 'get_wall_ns', 'stream_wall_ns', 'write_wall_ns'):
                integer(row[name], name, maximum=2**128 - 1)
            require(row['metadata_wave_wall_ns'] == wave_wall, 'metadata wave repeated wall')
            require(row['chunks'] > 0, 'metadata chunks')
            heads = int(row['name'] not in EXACT_LENGTH_FILES)
            require(row['logical_head_requests'] == heads, 'metadata HEAD count')
            if not heads:
                require(row['head_wall_ns'] == 0, 'skipped metadata HEAD time')
            require(row['logical_get_requests'] == (row['bytes'] + 4194303) // 4194304,
                    'metadata logical GETs')
            require(row['payload_buffer_bound_bytes'] == min(row['bytes'], (8 // len(batch)) * 4194304),
                    'metadata payload bound')
            require(row['write_wall_ns'] <= row['stream_wall_ns'], 'nested write timing')
            require(row['head_wall_ns'] + row['get_wall_ns'] + row['stream_wall_ns'] <= wave_wall,
                    'metadata object/wave timing')
            if row['bytes'] > 4194304:
                require(row['get_wall_ns'] == 0, 'range headers already included in stream')
        require(sum(r['payload_buffer_bound_bytes'] for r in batch) <= 8 * 4194304,
                'metadata wave payload bound')
    for name in ('staging_wall_ns', 'decode_wall_ns', 'source_head_wall_ns', 'router_head_wall_ns'):
        integer(stats[name], name, maximum=2**128 - 1)
    require(sum(batch[0]['metadata_wave_wall_ns'] for batch in waves)
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
                logical_metadata_head_requests=sum(r['logical_head_requests'] for r in rows), source_head_requests=1,
                router_head_requests=router_heads,
                payload_buffer_bound_bytes=max(sum(r['payload_buffer_bound_bytes'] for r in batch) for batch in waves),
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
    # read_two_bit_head authenticates head.json and its generation manifest;
    # open_remote then stages that same manifest as part of the metadata roster.
    root_bytes = arm['metadata_files']['manifest.json']
    gets = metadata['logical_metadata_get_requests'] + 2
    # object_store 0.14.1 shares NativeConnector with its instance provider:
    # PUT token, GET role, GET credentials, before the first authenticated S3 call.
    require(ready['method_counts'] == [gets + 2, heads, 1] + [0] * 7, 'startup S3/IMDS attempts / no hidden retries')
    credential_bytes = ready['consumed_payload_bytes'] - metadata['metadata_bytes'] - arm['head_file']['bytes'] - root_bytes
    integer(credential_bytes, 'inferred credential payload', 1, 2**64 - 2)
    return dict(metadata=metadata, startup_transport=ready, credential_protocol=CREDENTIAL_PROTOCOL,
                authority_head_JSON_GETs=1, authority_head_JSON_bytes=arm['head_file']['bytes'],
                authority_generation_root_GETs=1, authority_generation_root_bytes=root_bytes,
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
    # Wave walls are critical intervals; per-object overlapping waits are not additive.
    files = {'manifest.json': 5000, 'page_manifest.json': 1000, 'page_digests.bin': 12512,
             'plane/manifest.json': 500, 'plane/mean.bin': 3072, 'plane/page_digests.bin': 100000,
             'router/manifest.json': 80000, 'router/membership.bin': 12500}
    arm = dict(discovery='semantic', metadata_files=files,
               metadata_sha256={name: 'a' * 64 for name in files},
               authority=dict(root_sha256='a' * 64), head_file=dict(bytes=200, sha256='a' * 64),
               leaf_object=dict(bytes=4812500, sha256='a' * 64))
    rows = [dict(name=name, bytes=size, chunks=1, head_wall_ns=int(name not in EXACT_LENGTH_FILES),
                 get_wall_ns=3, stream_wall_ns=5, write_wall_ns=1,
                 logical_head_requests=int(name not in EXACT_LENGTH_FILES), logical_get_requests=1,
                 payload_buffer_bound_bytes=size, metadata_wave=0 if i == 0 else (i - 1) // 4 + 1,
                 metadata_wave_wall_ns=10) for i, (name, size) in enumerate(files.items())]
    startup = dict(metadata=rows, staging_wall_ns=30, decode_wall_ns=5,
                   source_head_requests=1, source_head_wall_ns=2,
                   router_head_requests=1, router_head_wall_ns=2)
    assert sum(r['head_wall_ns'] + r['get_wall_ns'] + r['stream_wall_ns'] for r in rows) > 30
    validate_startup(startup, arm, 40)
    for mutation in ('missing_wave', 'missing_wall', 'root', 'gap', 'width', 'order',
                     'boolean', 'negative', 'overflow', 'unequal_wall', 'sum_wall', 'object_wall', 'buffer'):
        bad = copy.deepcopy(startup)
        changed = bad['metadata']
        if mutation == 'missing_wave': del changed[1]['metadata_wave']
        elif mutation == 'missing_wall': del changed[1]['metadata_wave_wall_ns']
        elif mutation == 'root': changed[1]['metadata_wave'] = 0
        elif mutation == 'gap':
            for row in changed[1:]: row['metadata_wave'] += 1
        elif mutation == 'width':
            for row in changed[1:]: row['metadata_wave'] = 1
        elif mutation == 'order': changed[1], changed[2] = changed[2], changed[1]
        elif mutation == 'boolean': changed[1]['metadata_wave'] = True
        elif mutation == 'negative': changed[1]['metadata_wave'] = -1
        elif mutation == 'overflow': changed[1]['metadata_wave_wall_ns'] = 2**128
        elif mutation == 'unequal_wall': changed[1]['metadata_wave_wall_ns'] += 1
        elif mutation == 'sum_wall': bad['staging_wall_ns'] = 29
        elif mutation == 'object_wall': changed[1]['get_wall_ns'] = 20
        else: changed[1]['payload_buffer_bound_bytes'] += 1
        try:
            validate_startup(bad, arm, 40)
        except (ValueError, KeyError): pass
        else: raise AssertionError('invalid metadata wave accepted: ' + mutation)
    # Large graph objects exercise the divided range-buffer bound, not just small files.
    graph = copy.deepcopy(arm)
    graph['discovery'] = 'graph'
    graph['metadata_files'] = {name: size for name, size in files.items() if not name.startswith('router/')}
    graph['metadata_files'].update({name: 20 * 4194304 for name in ('centroids.bin', 'graph.bin', 'diverse_graph.bin')})
    graph['metadata_sha256'] = {name: 'a' * 64 for name in graph['metadata_files']}
    order = ['manifest.json', 'page_manifest.json', 'page_digests.bin', 'centroids.bin', 'graph.bin',
             'diverse_graph.bin', 'plane/manifest.json', 'plane/mean.bin', 'plane/page_digests.bin']
    large = copy.deepcopy(startup)
    large['router_head_requests'] = large['router_head_wall_ns'] = 0
    large['metadata'] = []
    for i, name in enumerate(order):
        size = graph['metadata_files'][name]
        large['metadata'].append(dict(name=name, bytes=size, chunks=1,
            head_wall_ns=int(name not in EXACT_LENGTH_FILES), logical_head_requests=int(name not in EXACT_LENGTH_FILES),
            get_wall_ns=int(size <= 4194304), stream_wall_ns=5, write_wall_ns=1,
            logical_get_requests=(size + 4194303) // 4194304,
            payload_buffer_bound_bytes=min(size, (8 if i == 0 else 2) * 4194304),
            metadata_wave=0 if i == 0 else (i - 1) // 4 + 1, metadata_wave_wall_ns=10))
    bound = validate_startup(large, graph, 40)['payload_buffer_bound_bytes']
    assert 2 * 2 * 4194304 <= bound <= 8 * 4194304
    large['metadata'][3]['payload_buffer_bound_bytes'] = 8 * 4194304
    try:
        validate_startup(large, graph, 40)
    except ValueError as error: assert str(error) == 'metadata payload bound'
    else: raise AssertionError('undivided range buffer accepted')
    # Immutable closed a0003 evidence: posthoc accounting must not upgrade FAIL.
    import gzip
    import hashlib
    import json
    from pathlib import Path
    evidence = Path(__file__).resolve().parents[1] / 'docs/research/performance-architecture-20260930/semantic-cold/a0003/screen'
    bodies = {}
    for name, expected in (
            ('config.json.gz', 'a02532d2e00111d137afe1afb712cd3f51de015883aa617299a6a219ad4c4e4f'),
            ('records.jsonl.gz', '16a382104ec7bb6a9bd1388f9875f1e53f72b463ca475082da9c150cd0cd9b5f')):
        bodies[name] = (evidence / name).read_bytes()
        assert hashlib.sha256(bodies[name]).hexdigest() == expected
    config = json.loads(gzip.decompress(bodies['config.json.gz']))
    records = [json.loads(line) for line in gzip.decompress(bodies['records.jsonl.gz']).splitlines()]
    first, arm = records[0], config['items'][0]['arms']['control']
    header = first['native_header']
    assert first['outcome'] == 'failed' and first['http_status'] == 200 and first['returned_hits'] == 10
    assert first['error'] == 'startup S3/IMDS attempts / no hidden retries'
    assert all(r['outcome'] == 'aborted' for r in records[1:]) and len(records) == 512
    totals = header['transport']['totals']
    # The old head-only accounting expected 13 GETs and rejected this 14-GET ready header.
    assert totals['method_counts'] != [13, 10, 1] + [0] * 7
    assert totals['method_counts'] == [14, 10, 1] + [0] * 7 and totals['attempts'] == 25
    # This checker qualifies the new HEAD roster only. Historical telemetry
    # remains tied to its frozen source and cannot be silently requalified.
    try:
        validate_ready(header, arm)
    except KeyError as error:
        assert error.args == ('metadata_wave',)
    else:
        raise AssertionError('historical metadata wave telemetry accepted')
    validate_query(first['response'], arm, expected=first['reference_response'], truth=first['truth_at_10'])
    assert first['outcome'] == 'failed' and 'startup_accounting' not in first
    print('PASS closed a0003 historical metadata wave telemetry rejected; saved FAIL preserved')
    print('PASS semantic query stage bounds, missing telemetry and partial failure stages')


def validate_failed_record(record, config, arm, body, expected, truth, *, port=8080):
    import base64
    import hashlib
    import json
    require(record['outcome'] == 'failed' and bool(record['error_type']) and type(record['error']) is str, 'raw failure receipt')
    integer(record['http_attempts'], 'failed HTTP attempts', maximum=1)
    require(record['expected_authority'] == arm['authority']
            and record['reference_response'] == {k: expected[k] for k in PARITY}
            and record['truth_at_10'] == truth[:10]
            and record['request_bytes'] == len(body)
            and record['request_sha256'] == hashlib.sha256(body).hexdigest()
            and record['http_retry'] is False, 'failed input binding')
    if record['raw_response_complete']:
        raw = base64.b64decode(record['raw_response_base64'], validate=True)
        require(record['raw_response'] == raw.decode(errors='replace'), 'raw failure byte identity')
        if 'response' in record:
            require(record['response'] == json.loads(raw), 'failed response/raw identity')
    if 'native_header' in record:
        headers = [json.loads(line) for line in record['native_server_log'].splitlines() if line.startswith('{')]
        require(headers == [record['native_header']], 'failed raw ready identity')
    if 'startup_accounting' in record:
        require(record['startup_accounting'] == validate_ready(record['native_header'], arm), 'failed startup receipt')
    if 'accounting' in record:
        require(record['accounting'] == validate_outcome(record['native_header'], record['response'], arm, False), 'failed accounting receipt')
    # A failed record remains failed even if its available telemetry is coherent.
    # Missing/invalid telemetry must be explicitly reported, never silently totaled.
    try:
        header = record['native_header']
        require(header['phase'] == 'ready' and header['authority'] == arm['authority']
                and header['listen'] == f'127.0.0.1:{port}', 'failed ready authority/port')
        validate_outcome(header, record['response'], arm, False)
    except (ValueError, KeyError, TypeError):
        require(bool(record['telemetry_validation_errors']), 'unreported failed telemetry')


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
    if config['schema'] == runtime.OFFERED_SCHEMA:
        records, saved_cells = [], []
        for index, rate, dataset, arm in runtime.offered_order():
            path = out / runtime.offered_name(index, dataset, arm)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            require(path.read_bytes() == ''.join(runtime.encoded(r) + '\n' for r in rows).encode(), 'canonical immutable cell ledger')
            records.extend(rows)
            saved_cells.append(json.loads(path.with_name(path.name.replace('-records.jsonl', '-summary.json')).read_text()))
        actual = runtime.reduce_offered(records, panels, config)
        for cell, saved in zip(actual['cells'], saved_cells):
            require(saved == runtime.closed_cell_summary(cell, config, config_sha), 'closed cell marker identity/parity')
        summary = json.loads((out / 'summary.json').read_text())
        require(summary['config_sha256'] == config_sha, 'offered summary config identity')
        require(all(summary.get(name) == value for name, value in actual.items()), 'saved offered summary/gates/tails')
        require(summary['binary_sha256'] == config['binary']['sha256']
                and summary['qualification_sha256'] == config['qualification_sha256']
                and summary['native_source_identity_sha256'] == config['native_source_identity_sha256']
                and summary['native_source_file_count'] == config['native_source_file_count']
                and summary['code_sha256'] == config['code_sha256'], 'offered terminal execution identities')
        require(summary['closed'] == actual['process_cleanup_complete'], 'offered terminal cleanup')
        expected_inputs = {d: dict(common=p['inputs'], arms={a: v['inputs'] for a, v in p['arms'].items()}) for d, p in panels.items()}
        require(summary['inputs'] == expected_inputs, 'offered consumed input receipts')
        return dict(all_calls_successful=actual['all_calls_successful'], quality_gate_passed=actual['quality_gate_passed'],
                    qualification_gate_passed=actual['qualification_gate_passed'], execution_gate_passed=actual['execution_gate_passed'],
                    latency_improvement=actual['latency_improvement'], records=len(records))
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
        raise SystemExit(0 if result.get('execution_gate_passed', result['all_calls_successful'] and result['quality_gate_passed']) else 1)
    else:
        raise SystemExit('usage: OUTPUT CONFIG_SHA BINARY | --self-check')
