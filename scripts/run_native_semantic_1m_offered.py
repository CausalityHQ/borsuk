"""Retained v8 FIRST1M, six finite offered cells; no build or publication.

Root freezes CONFIG with FIXED, authority_pending=False, exact CODE hashes,
cold_config (bytes/SHA pointer), cold_run (exact closed cold artifact roster),
cold_source_authority (root-reviewed original config/code/source/qualification),
cold_fail_disposition (null for complete campaigns; approved failure pointer),
measurement_prefix, and prices (bytes/SHA pointer to root price provenance).
main(CONFIG, SHA, REPO, NEW_OUTPUT, *, on_cell_closed=None) refuses pending
authority before creating output. The launcher owns cloud and offline replay.
"""
import base64
import builtins
import http.client
import io
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import struct
import subprocess
import sys
import threading
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_native_semantic_1m_cold as native
from scripts import launch_native_semantic_1m_cold_spot as cold_spot
from scripts import run_native_cold_offered as offered

cold, panel, ids, stats = native.cold, native.panel, native.ids, native.stats
encoded, sha, artifact, write, read, identity = (native.encoded, native.sha,
    native.artifact, native.write, native.read, native.identity)
ROOT = native.ROOT.parent / 'offered-1m'
CONFIG = ROOT / 'config.json'
PREFIX = 'research/semantic-router/20261002/fresh1m-offered-'
RATES = [.25, .5, 1, 2, 4, 8]
MEMORY, NATIVE, THREAD_ENV = native.MEMORY, native.NATIVE, native.THREAD_ENV
FIXED = dict(native.FIXED, schema='borsuk-semantic-1m-offered-v1',
    build_invocations=0, publication_invocations=0, cold_invocations=384,
    concurrency=6, workers=6, base_port=18080, offered_qps=RATES,
    max_dispatch_lateness_ns=125000000, cleanup_reserve_seconds=90,
    worker_limit_seconds=3000, service_limit_seconds=3000,
    machine_limit_seconds=3600, compute_cap_usd=.5)
for _name in ('preparation_limit_seconds', 'build_limit_seconds',
              'publication_limit_seconds', 'cold_limit_seconds', 'publisher_memory_bytes'):
    FIXED.pop(_name)
CODE = tuple(sorted(set((*native.CODE, *offered.CODE,
    'scripts/run_native_semantic_1m_offered.py', 'scripts/launch_native_semantic_1m_offered_spot.py'))))
COLD_ROSTER = (*cold_spot.ARTIFACTS, 'aws-reservation.json', 'aws-launch.json',
               'aws-closeout.json', 'aws-terminal.json')
CELL_FILES = tuple(f'rate{i}-{suffix}' for i in range(6)
                   for suffix in ('records.jsonl', 'summary.json'))
OUTPUTS = ('source-qualification.json', 'config.json', 'tool-versions.json',
    'input-hashes.json', 'records.jsonl', 'failures.jsonl', 'summary.json',
    'resources.json', 'offered-cgroup.json', 'cleanup.json', *CELL_FILES)
_cgroup_lock = threading.Lock()


def validate_cgroup(report):
    # The inherited checker scopes a module constant. Serialize those scopes.
    with _cgroup_lock:
        native.validate_cgroup(report)
    for snapshot in (report['before'], report['after']):
        assert int(snapshot['memory.current']) >= 0
        memory = dict(line.split() for line in snapshot['memory.stat'].splitlines())
        assert int(memory['anon']) >= 0 and int(memory['file']) >= 0
        if snapshot['io.stat'] == 'UNMEASURED':
            assert snapshot['io.stat_unavailable'] == dict(type='FileNotFoundError',errno=2)


def qualify(config_path, expected_sha, repo):
    repo, config_path = Path(repo).resolve(), Path(config_path).absolute()
    assert config_path == config_path.resolve() == repo/CONFIG and not config_path.is_symlink()
    assert config_path.stat().st_size <= 65536
    body = config_path.read_bytes()
    assert sha(body) == expected_sha, 'config identity'
    config = json.loads(body)
    assert config['authority_pending'] is False, 'root freeze pending'
    assert set(config) == set(FIXED) | {'authority_pending', 'code_sha256',
        'cold_config', 'cold_run', 'cold_source_authority', 'cold_fail_disposition', 'measurement_prefix', 'prices'}
    assert all(type(config[n]) is type(v) and config[n] == v for n, v in FIXED.items()), 'fixed offered protocol'
    assert set(config['code_sha256']) == set(CODE), 'exact transitive code closure'
    assert all(artifact(panel.repo_path(repo, n))['sha256'] == d
               for n, d in config['code_sha256'].items()), 'code identity'
    assert panel.re.fullmatch(panel.re.escape(PREFIX)+r'a[0-9]{4}', config['measurement_prefix'])
    pointer = config['cold_config']
    read(repo, pointer)
    run = config['cold_run']
    assert set(run) == {'directory', 'files'} and set(run['files']) == set(COLD_ROSTER)
    assert panel.re.fullmatch(panel.re.escape(str(native.ROOT))+r'/a[0-9]{4}', run['directory'])
    # Only closed authenticated runs are eligible; no inspection of active results.
    for name, p in run['files'].items():
        assert p['path'] == str(Path(run['directory'])/name)
    closed = json.loads(read(repo, run['files']['aws-closeout.json']))
    terminal = json.loads(read(repo, run['files']['aws-terminal.json']))
    assert closed['state'] == 'terminated', 'closed cold prerequisite'
    assert pointer['path'] in (str(native.CONFIG), str(Path(run['directory'])/'screen/config.json'))
    for name,p in run['files'].items():
        if p['bytes']==0:
            assert name in ('screen/publication.log','screen/failures.jsonl'), 'only optional cold logs may be empty'
            assert artifact(panel.repo_path(repo,p['path']))==dict(bytes=0,sha256=p['sha256']), 'empty cold body identity'
        else: read(repo,p)
    assert type(config['cold_source_authority']) is dict, 'root cold source authority pointer required'
    checked = cold_spot.qualify_measurement(repo/run['directory'],repo,config['cold_source_authority'])
    assert all(checked[n] is True for n in ('measurement_gate_passed','quality_gate','identity_gate','resource_gate','cleanup_gate'))
    assert checked['valid_calls'] == checked['actual_http_attempts'] == 64
    cold_spot.fail_disposition(repo,run,config['cold_fail_disposition'],checked)
    cold_config, cold_proof = checked['cold_config'],checked['qualification']
    cold_proof = {n:v for n,v in cold_proof.items()
        if n not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256')}
    assert identity(run['files']['screen/config.json']) == identity(pointer)
    assert not cold_config['namespace_prefix'].startswith(config['measurement_prefix'])
    prices = json.loads(read(repo, config['prices']))
    assert type(prices) is dict and prices, 'root frozen price provenance'
    proof = dict(cold_proof, config_path=str(CONFIG), config_sha256=expected_sha,
        code_identity_sha256=sha(encoded(config['code_sha256'])),
        refs_identity_sha256=sha(encoded({n:config[n] for n in ('cold_config','cold_run','cold_source_authority','cold_fail_disposition','prices')})),
        measurement_prefix=config['measurement_prefix'], prices=identity(config['prices']),
        cold_terminal_sha256=run['files']['aws-terminal.json']['sha256'])
    return config, proof


def inputs(repo, config):
    """Small sealed envelopes only; never open raw vectors, SQ8 or an oracle."""
    cfg = json.loads(read(repo, config['cold_config']))
    files = config['cold_run']['files']
    publication = json.loads(read(repo, files['screen/publication.json']))
    arm = publication['arm']; native.validate_roster(arm)
    assert arm['indexes'] == {'10':cfg['namespace_prefix']}
    assert arm['authority'] == dict(root_sha256=publication['head']['root_sha256'], generation=1, control_epoch=1)
    remote, manifest = publication['remote_sq8'], publication['manifest']
    assert publication['retained_for_offered_gate'] is True
    assert remote['backend'] == 'AmazonS3' and remote['remote_readback_authenticated'] is True
    assert (manifest['sq8_object_key'], manifest['sq8_etag']) == (remote['key'], remote['etag'])
    assert remote['etag'] and remote['key'].startswith(cfg['namespace_prefix']+'/objects/')
    qcfg = json.loads(read(repo, cfg['quality_config']))
    original = read(repo, qcfg['panel']['files']['screen/requests.jsonl'])
    derived = read(repo, files['screen/publisher-requests.jsonl'])
    assert derived == native.publisher_requests(original)
    truth_body = read(repo, qcfg['panel']['files']['screen/truth.i64'])
    assert len(truth_body) == 64*100*8
    truth = [list(struct.unpack_from('<100q', truth_body, q*800)) for q in range(64)]
    assert all(len(set(t)) == 100 and all(type(n) is int and 0 <= n < 1000000 for n in t) for t in truth)
    reference = native.source_reference(read(repo, cfg['quality_run']['files']['screen/records.jsonl']))
    pointer = files['screen/publication-reference.jsonl']
    reference_path = panel.repo_path(repo, pointer['path'])
    assert reference_path.read_bytes() == read(repo, pointer), 'authenticated materialized publisher reference'
    assert publication['reference'] == identity(pointer)
    assert publication['validation'] == native.publication_reference(reference_path, arm, reference)
    requests = [native.http_request(json.loads(line)['query'], arm['authority']) for line in derived.splitlines()]
    return dict(arm=arm, requests=requests, references=[dict(r, ids=r['ids'][:10]) for r in reference],
        truth=truth, publication=publication, binary=cfg['binaries']['http'],
        identities=dict(publication=identity(files['screen/publication.json']),
            reference=identity(files['screen/publication-reference.jsonl']), requests=identity(files['screen/publisher-requests.jsonl']),
            truth=identity(qcfg['panel']['files']['screen/truth.i64']), quality_reference=identity(cfg['quality_run']['files']['screen/records.jsonl']),
            native_proof=identity(cfg['native_proofs']['http']), binary=identity(cfg['binaries']['http']),
            prices=config['prices'], remote_sq8=remote))


def measured_call(binary, config, evidence, q, port):
    """cold.stop is installed once by the campaign, before any worker starts."""
    arm, body, expected, truth = (evidence['arm'], evidence['requests'][q],
        evidence['references'][q], evidence['truth'][q])
    failures, observed = io.StringIO(), dict(native_process_started=False, namespace_start_attempted=False, http_attempts=0)
    stage = 'spawn'
    def spawn(args, **kwargs):
        nonlocal stage
        observed['namespace_start_attempted'] = True
        command = list(args); index = command.index('taskset')
        command[index:index] = ['prlimit', '--as=4294967296:4294967296']
        process = subprocess.Popen(command, **kwargs)
        observed.update(native_process_started=True, wrapper_pid=process.pid)
        stage = 'connect'
        return process
    def post(client, payload):
        nonlocal stage
        stage = 'cpu_before'
        before = native.native_cpu(observed['wrapper_pid'], binary)
        stage = 'transport'; observed['http_attempts'] = 1
        status, raw = cold.post(client, payload)
        wire = time.monotonic_ns()  # Before CPU attribution, JSON, GT or cleanup.
        observed.update(first_wire_completed_ns=wire, http_status=status,
                        raw_response_base64=base64.b64encode(raw).decode())
        stage = 'cpu_after'; after = native.native_cpu(observed['wrapper_pid'], binary)
        assert (before['pid'], before['start_ticks']) == (after['pid'], after['start_ticks'])
        ticks = sum(after[n]-before[n] for n in ('user_ticks','system_ticks'))
        assert ticks >= 0
        observed['query_cpu'] = dict(before=before, after=after, ticks=ticks,
            ticks_per_second=os.sysconf('SC_CLK_TCK'), scope='native process CPU sampled around one client POST; tick resolution')
        stage = 'response'
        return status, raw
    row = {}
    try:
        row = cold.cold_call(str(binary), config, dict(arm, dataset='ReLAION'), body, expected, truth, failures,
            port=port, response_check=lambda r,e,t,a:native.validate_query(r,arm,e,t),
            startup_check=lambda v,f,w:native.validate_startup(v,arm,w), post_call=post, spawn=spawn,
            env=dict(os.environ, BORSUK_NATIVE_MEMORY_BYTES=str(NATIVE), AWS_MAX_ATTEMPTS='1'))
        row.update(observed)
        row['completed_ns'] = observed['first_wire_completed_ns']
        for key, start in (('cold_start_to_first_http_response_ns','started_ns'),
            ('first_post_to_response_ns','connected_ns'), ('incoming_http_wall_ns','successful_connect_attempt_ns')):
            row[key] = row['completed_ns']-row[start]
        stage = 'accounting'; row['accounting'] = native.transport(row['native_header'], row['response'], arm)
        stage = 'resources'; row['resources'] = native.telemetry.resources(row['native_time_log'], NATIVE)
        stage = 'cleanup'
        assert row['native_close']['intentional_stop'] is True and row['native_close']['process_group_closed'] is True
        row.update(outcome='success', failure_kind=None, abort_admissions=False)
    except Exception as error:
        raw = failures.getvalue()
        failed = [json.loads(line) for line in raw.splitlines()]
        if not row and failed: row = failed[0]
        row.update(observed, failure_stream_raw=raw, failure_stage=stage,
                   error_type=type(error).__name__, error=str(error) or type(error).__name__)
        transport_error = transport_failure(row)
        row.update(outcome='failed', failure_kind='transport' if transport_error else 'fatal',
                   abort_admissions=not transport_error)
        # Failed payloads and any final counters survive; absent totals stay unknown.
        row['failed_final_transport'] = 'UNMEASURED'
        try:
            response = json.loads(base64.b64decode(row['raw_response_base64'], validate=True))
            row['failed_final_transport'] = stats.validate_transport(response['transport'], False)
        except (KeyError, ValueError, AssertionError, TypeError):
            pass
        if row['native_process_started']:
            try: row['resources'] = native.telemetry.resources(row['native_time_log'], NATIVE)
            except Exception as resource_error:
                row.update(failure_kind='fatal', abort_admissions=True, resource_error=str(resource_error))
    if 'first_wire_completed_ns' in observed:
        row['completed_ns'] = observed['first_wire_completed_ns']
        if row.get('started_ns') is not None:
            row['cold_start_to_first_http_response_ns'] = row['completed_ns']-row['started_ns']
    row['cleanup_confirmed'] = (not row['native_process_started'] or
        row.get('native_close', {}).get('process_group_closed') is True)
    if not row['cleanup_confirmed']: row.update(failure_kind='fatal', abort_admissions=True)
    row.update(query_ordinal=q, expected_authority=arm['authority'], request_sha256=sha(body),
               request_bytes=len(body), http_retry=False)
    return row


def transport_failure(row):
    stage, name, message = row['failure_stage'], row['error_type'], row['error']
    kind = getattr(http.client, name, getattr(builtins, name, None))
    return ((stage in ('connect','transport') and isinstance(kind,type) and issubclass(kind,(OSError,http.client.HTTPException))) or
        (stage == 'connect' and name == 'RuntimeError' and message == 'namespace process closed before first connection') or
        (stage == 'response' and name == 'AssertionError' and message == 'first and only ANN request failed; no HTTP retry'))


def validate_success(row, evidence, q):
    arm, response = evidence['arm'], row['response']
    assert row['http_status'] == 200 and row['http_attempts'] == row['valid_ann_requests'] == 1
    assert row['native_process_started'] is row['namespace_start_attempted'] is True
    assert row['response'] == json.loads(base64.b64decode(row['raw_response_base64'], validate=True))
    assert len(base64.b64decode(row['raw_response_base64'], validate=True)) == row['response_bytes']
    headers = [json.loads(line) for line in row['native_server_log'].splitlines() if line.startswith('{')]
    assert headers == [row['native_header']] and headers[0]['listen'] == f"127.0.0.1:{row['port']}"
    assert row['accounting'] == native.transport(headers[0], response, arm)
    assert row['returned_hits'] == native.validate_query(response, arm, evidence['references'][q], evidence['truth'][q])
    assert row['resources'] == native.telemetry.resources(row['native_time_log'], NATIVE)
    assert row['native_close']['intentional_stop'] is row['native_close']['process_group_closed'] is True
    assert type(row['native_close']['returncode']) is int
    cpu = row['query_cpu']; before, after = cpu['before'], cpu['after']
    assert (before['pid'], before['start_ticks']) == (after['pid'], after['start_ticks'])
    assert all(c['address_space_limit_bytes'] == 4*1024**3 and c['cpu_affinity'] == [0,1,2,3] for c in (before,after))
    assert cpu['scope'] == 'native process CPU sampled around one client POST; tick resolution'
    stats.integer(cpu['ticks_per_second'], 'tick resolution', 1)
    assert cpu['ticks'] == sum(after[n]-before[n] for n in ('user_ticks','system_ticks')) >= 0
    assert row['started_ns'] <= row['successful_connect_attempt_ns'] <= row['connected_ns'] <= row['completed_ns'] <= row['terminal_ns']
    assert row['completed_ns'] == row['first_wire_completed_ns']
    for key, start, end in (('cold_start_to_first_http_response_ns','started_ns','completed_ns'),
        ('before_successful_connect_attempt_ns','started_ns','successful_connect_attempt_ns'),
        ('successful_tcp_connect_ns','successful_connect_attempt_ns','connected_ns'),
        ('first_post_to_response_ns','connected_ns','completed_ns'),
        ('incoming_http_wall_ns','successful_connect_attempt_ns','completed_ns')):
        assert row[key] == row[end]-row[start]
    assert row['cold_start_to_first_http_response_ns'] >= headers[0]['remote_open_wall_ns']+headers[0]['head_read_wall_ns']


def reduce_cell(records, evidence, config):
    assert len(records) == 64 and [r['query_ordinal'] for r in records] == list(range(64))
    receipt = records[0]['cell_receipt']
    epoch, terminal = receipt['epoch_ns'], receipt['terminal_ns']
    assert type(epoch) is type(terminal) is int and terminal > epoch
    assert receipt['admission_deadline_ns'] == receipt['worker_started_ns']+(3000-90)*10**9
    index = records[0]['rate_index']; stats.integer(index, 'cell', 0, 5)
    rate = RATES[index]; active, intervals, delays, good = [], [], [], []
    fatal = False
    # This is deliberately after drain, never a per-call sibling PID check.
    validate_cgroup(dict(before=receipt['campaign_cgroup_before'], after=receipt['cgroup_after'], closed=True))
    validate_cgroup(dict(before=receipt['cgroup_before'], after=receipt['cgroup_after'], closed=True))
    assert receipt['resource_errors'] == [], 'shared resource observation failure'
    for q, row in enumerate(records):
        assert type(row['query_ordinal']) is int and type(row['rate_index']) is int
        assert row['rate_index'] == index and row['offered_qps'] == rate and row['dataset'] == 'ReLAION'
        assert row['scheduled_ns'] == epoch+round(q*1e9/rate)
        assert epoch <= row['terminal_ns'] <= terminal
        outcome, port = row['outcome'], row['port']
        assert outcome in ('success','failed','capacity_drop','aborted')
        if row['dispatched_ns'] is not None:
            assert receipt['cell_started'] is True
            assert row['scheduled_ns'] <= row['dispatched_ns'] <= row['terminal_ns']
            assert row['dispatched_ns'] < receipt['admission_deadline_ns']
            delays.append(row['dispatched_ns']-row['scheduled_ns'])
        if port is None:
            assert outcome in ('capacity_drop','aborted') and row['http_attempts'] == row['valid_ann_requests'] == 0
            assert row['native_process_started'] is row['namespace_start_attempted'] is False
            assert row['started_ns'] is row['completed_ns'] is None
            assert not any(n in row for n in ('response','native_close','raw_response_base64','native_header'))
            if outcome == 'capacity_drop': assert row['terminal_ns'] == row['dispatched_ns']
            else:
                assert row['dispatched_ns'] is None and row['abort_after'] == receipt['abort_after']
                assert isinstance(row['abort_after'], dict)
            continue
        stats.integer(port, 'port', 18080, 18085)
        assert outcome in ('success','failed') and row['dispatched_ns'] is not None
        assert row['request_sha256'] == sha(evidence['requests'][q]) and row['request_bytes'] == len(evidence['requests'][q])
        assert row['expected_authority'] == evidence['arm']['authority'] and row['http_retry'] is False
        assert row['cleanup_confirmed'] is (not row['native_process_started'] or row.get('native_close',{}).get('process_group_closed') is True)
        assert row['cleanup_confirmed'] is True, 'unclosed native process group'
        if row['started_ns'] is not None: assert row['dispatched_ns'] <= row['started_ns'] <= row['terminal_ns']
        if row['completed_ns'] is not None:
            assert row['started_ns'] <= row['completed_ns'] <= row['terminal_ns']
            assert row['completed_ns'] == row['first_wire_completed_ns']
        if receipt['abort_after'] is not None: assert row['dispatched_ns'] <= receipt['abort_after']['observed_ns']
        intervals.append((row['dispatched_ns'], row['terminal_ns'], port))
        if outcome == 'success':
            assert row['failure_kind'] is None and row['abort_admissions'] is False
            validate_success(row, evidence, q); good.append(row)
        else:
            assert row['error_type'] and row['error'] and row['failure_kind'] in ('transport','fatal')
            expected_kind = 'transport' if transport_failure(row) and 'resource_error' not in row else 'fatal'
            assert row['failure_kind'] == expected_kind, 'failure classification'
            assert row['abort_admissions'] is (row['failure_kind'] == 'fatal')
            assert type(row['failure_stream_raw']) is str
            assert row['http_attempts'] in (0,1) and type(row['http_attempts']) is int
            if row['failure_stream_raw']:
                failures = [json.loads(line) for line in row['failure_stream_raw'].splitlines()]
                assert len(failures) == 1
                assert row['error_type'] == failures[0]['error_type']
                assert row['error'] == (failures[0]['error'] or failures[0]['error_type'])
                for key in ('started_ns','native_close','native_time_log','native_server_log'):
                    assert row[key] == failures[0][key]
            if row['failure_kind'] == 'transport' and row['failure_stage'] == 'response':
                assert row['http_attempts'] == 1 and row['http_status'] != 200
            if 'raw_response_base64' in row:
                raw = base64.b64decode(row['raw_response_base64'],validate=True)
                assert type(row['http_status']) is int and row['first_wire_completed_ns'] <= row['terminal_ns']
                if row['failed_final_transport'] != 'UNMEASURED':
                    assert row['failed_final_transport'] == stats.validate_transport(json.loads(raw)['transport'],False)
            if row['native_process_started']:
                assert row['resources'] == native.telemetry.resources(row['native_time_log'], NATIVE)
            fatal |= row['failure_kind'] == 'fatal'
    peak = 0
    for start, end, port in sorted(intervals):
        active = [(s,e,p) for s,e,p in active if e > start]
        assert all(p != port for _,_,p in active), 'early port reuse'
        active.append((start,end,port)); peak = max(peak,len(active))
        assert peak <= 6
    for row in records:
        if row['outcome'] == 'capacity_drop':
            assert sum(s <= row['dispatched_ns'] < e for s,e,_ in intervals) == 6, 'drop without six owners'
    abort = receipt['abort_after']
    if receipt['cell_started'] and abort is not None:
        assert epoch <= abort['observed_ns'] <= terminal
        if abort['reason'] == 'admission deadline': assert abort['observed_ns'] >= receipt['admission_deadline_ns']
        else:
            origin = records[abort['query_ordinal']]
            assert origin['abort_admissions'] is True and origin['terminal_ns'] == abort['observed_ns']
            fatal = True
    counts = dict(planned=64, dispatched=sum(r['dispatched_ns'] is not None for r in records),
        admitted=len(intervals), terminal_completed=len(intervals), successful=len(good))
    hits = sum(r['returned_hits'] for r in good)
    timing = all(d <= 125000000 for d in delays)
    complete = len(good) == 64
    attained = complete and hits >= 608 and timing
    span = terminal-epoch
    started = receipt['cell_started']; assert type(started) is bool
    tails = dict(cold=offered.tails([r['cold_start_to_first_http_response_ns'] for r in good]),
        scheduled_response=offered.tails([r['completed_ns']-r['scheduled_ns'] for r in good]),
        dispatch=offered.tails(delays), all_offers=native.telemetry.all_offer_tails(records))
    return dict(schema='borsuk-semantic-1m-offered-cell-v1', rate_index=index, offered_qps=rate,
        dataset='ReLAION', cell_started=started, epoch_ns=epoch, terminal_ns=terminal,
        full_span_ns=span if started else 'UNMEASURED', counts=counts,
        full_span_qps={n:v*1e9/span if started else 'UNMEASURED' for n,v in counts.items()},
        capacity_drops=sum(r['outcome']=='capacity_drop' for r in records),
        errors=sum(r['outcome']=='failed' for r in records), aborted=sum(r['outcome']=='aborted' for r in records),
        returned_hits=hits, all_offer_recall_at_10=hits/640, quality_gate_passed=complete and hits>=608,
        dispatch_timing_gate_passed=timing, attainment=attained, execution_gate_passed=not fatal,
        peak_port_ownership=peak, closed=True, cleanup_confirmed=True,
        latency_ms=tails if started else 'UNMEASURED',
        latency_population='successful responses only; all-offer unsuccessful positions UNBOUNDED',
        successful_process_transport_totals={n:sum(r['accounting']['final_process_transport'][n] for r in good)
            for n in ('attempts','consumed_payload_bytes','transport_failures','stream_failures')},
        successful_process_method_counts=[sum(r['accounting']['final_process_transport']['method_counts'][i] for r in good) for i in range(10)],
        successful_process_status_counts={str(status):sum(dict(r['accounting']['final_process_transport']['status_counts']).get(status,0) for r in good)
            for status in sorted({s for r in good for s,_ in r['accounting']['final_process_transport']['status_counts']})},
        transport_scope='submitted process HttpService calls and consumed frames; startup plus query; includes IMDS below, not confirmed S3 requests',
        logical_query_totals={p+n:sum(r['response'][p+n] for r in good) for p in ('','source_','router_') for n in stats.COUNTERS},
        imds_token_PUTs=len(good), imds_credential_GETs=2*len(good),
        failed_transport_population='raw records; missing totals UNMEASURED',
        total_failed_call_transport='UNMEASURED' if any(r['outcome']=='failed' for r in records) else 0,
        wire_bytes='UNMEASURED', sustainable_qps='UNMEASURED', matched_vendor_comparison=False)


def reduce_records(records, evidence, config):
    assert len(records) == 384 and [(r['rate_index'],r['query_ordinal']) for r in records] == [(i,q) for i in range(6) for q in range(64)]
    cells = [reduce_cell(records[i:i+64], evidence, config) for i in range(0,384,64)]
    stop, previous = None, 0
    first = records[0]['cell_receipt']
    for index, cell in enumerate(cells):
        receipt = records[index*64]['cell_receipt']
        assert receipt['worker_started_ns'] == first['worker_started_ns']
        assert receipt['campaign_cgroup_before'] == first['campaign_cgroup_before']
        assert cell['epoch_ns'] >= previous; previous = cell['terminal_ns']
        if stop is None:
            assert cell['cell_started'] is True
            if not cell['attainment']:
                stop = dict(rate_index=index, reason='rate attainment failed', observed_ns=cell['terminal_ns'])
        else:
            assert cell['cell_started'] is False and receipt['abort_after'] == stop and cell['aborted'] == 64
    counts = {n:sum(c['counts'][n] for c in cells) for n in cells[0]['counts']}
    span = cells[-1]['terminal_ns']-cells[0]['epoch_ns']
    return dict(schema='borsuk-semantic-1m-offered-result-v1', closed=True, cells=cells, counts=counts,
        full_span_ns=span, full_span_qps={n:v*1e9/span for n,v in counts.items()},
        execution_gate_passed=all(c['execution_gate_passed'] for c in cells),
        offered_gate_passed=all(c['attainment'] for c in cells),
        largest_passing_tested_offered_qps=max([0]+[c['offered_qps'] for c in cells if c['attainment']]),
        sustainable_qps='UNMEASURED', matched_vendor_comparison=False, escalation_stop=stop)


def aborted_rows(index, epoch, stop):
    return [dict(query_ordinal=q, rate_index=index, dataset='ReLAION', offered_qps=RATES[index],
        scheduled_ns=epoch+round(q*1e9/RATES[index]), dispatched_ns=None, started_ns=None, completed_ns=None,
        port=None, outcome='aborted', abort_after=stop, terminal_ns=epoch,
        namespace_start_attempted=False, native_process_started=False, http_attempts=0, valid_ann_requests=0) for q in range(64)]


def close_cell(output, records, result, proof, on_cell_closed):
    index = records[0]['rate_index']
    path = output/f'rate{index}-records.jsonl'
    write(path, b''.join(encoded(r)+b'\n' for r in records))
    marker = dict(result, records=artifact(path), config_sha256=proof['config_sha256'],
        code_identity_sha256=proof['code_identity_sha256'],
        refs_identity_sha256=proof['refs_identity_sha256'], binary_sha256=proof['binary_sha256']['http'])
    target = output/f'rate{index}-summary.json'; write(target, marker)
    if on_cell_closed is not None and marker['closed'] and marker['cleanup_confirmed']:
        on_cell_closed(marker, dict(records=path, summary=target))
    return marker


def main(config_path, expected_sha, repo, output, *, on_cell_closed=None):
    started_ns = time.monotonic_ns()
    repo, output = Path(repo).resolve(), Path(output).absolute()
    config, proof = qualify(config_path, expected_sha, repo)
    evidence = inputs(repo, config)
    assert not output.exists() and not output.is_symlink() and not output.resolve().is_relative_to(repo)
    assert all(os.environ.get(n) == '2' for n in THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS') == '1'
    assert os.sched_getaffinity(0) == {4,5}
    baseline = native.capture(); validate_cgroup(dict(before=baseline,after=baseline,closed=True))
    source = {k:os.environ['BORSUK_OFFERED_'+n] for k,n in (('source_commit','SOURCE_COMMIT'),('source_archive_sha256','ARCHIVE_SHA256'))}
    assert panel.re.fullmatch('[0-9a-f]{40}', source['source_commit']) and panel.re.fullmatch('[0-9a-f]{64}', source['source_archive_sha256'])
    output.mkdir(parents=True); scratch = output/'scratch'; scratch.mkdir()
    records, cells, resource_errors = [], [], []
    sample_report = dict(samples=0, memory_current_peak_bytes=0, anonymous_peak_bytes=0, page_cache_peak_bytes=0)
    failure, stop_after, summary = None, None, None
    counters = dict(before=baseline, closed=False)
    stop, termination = threading.Event(), threading.Event()
    def sample():
        while not stop.is_set():
            try:
                snap = native.capture()
                # Validate limits/events with its own PID set; siblings are live owners.
                validate_cgroup(dict(before=snap,after=snap,closed=True))
                mem = dict(line.split() for line in snap['memory.stat'].splitlines())
                sample_report.update(samples=sample_report['samples']+1,
                    memory_current_peak_bytes=max(sample_report['memory_current_peak_bytes'],int(snap['memory.current'])),
                    anonymous_peak_bytes=max(sample_report['anonymous_peak_bytes'],int(mem['anon'])),
                    page_cache_peak_bytes=max(sample_report['page_cache_peak_bytes'],int(mem['file'])))
            except Exception as error:
                resource_errors.append(dict(type=type(error).__name__,message=str(error))); return
            stop.wait(.25)
    monitor = threading.Thread(target=sample, daemon=True)
    # A signal cannot unwind schedule_offers while its threads still own ports.
    def terminated(signum, frame): termination.set()
    previous = {s:signal.signal(s, terminated) for s in (signal.SIGTERM,signal.SIGINT)}
    try:
        write(output/'config.json', Path(config_path).read_bytes())
        write(output/'source-qualification.json', dict(proof, **source))
        write(output/'input-hashes.json', evidence['identities'])
        write(output/'tool-versions.json', dict(ids.tool_versions(), thread_environment={n:os.environ[n] for n in THREAD_ENV}))
        binary = scratch/'http'; write(binary, read(repo, evidence['binary'])); binary.chmod(0o500)
        assert artifact(binary) == identity(evidence['binary'])
        monitor.start()
        deadline = started_ns+(3000-90)*10**9
        # One hook for the whole concurrent campaign, restored only after drain.
        with patch.object(cold, 'stop', native.close_native):
            for index, rate in enumerate(RATES):
                before = native.capture()
                if stop_after is None:
                    assert artifact(binary) == identity(evidence['binary']), 'HTTP binary drift'
                    def call(q, port):
                        assert not resource_errors and not termination.is_set(), 'shared resource or termination failure'
                        return measured_call(binary, config, evidence, q, port)
                    rows, epoch, terminal, abort = offered.schedule_offers(call, rate, deadline_ns=deadline)
                else:
                    epoch = time.monotonic_ns(); rows = aborted_rows(index, epoch, stop_after)
                    terminal, abort = time.monotonic_ns(), stop_after
                for row in rows:
                    row.update(rate_index=index, dataset='ReLAION')
                    # The scheduler can fail to start a thread before the callback.
                    if row.get('failure_stage') in ('thread_start','callback'):
                        q = row['query_ordinal']
                        row.update(expected_authority=evidence['arm']['authority'], http_retry=False,
                            request_sha256=sha(evidence['requests'][q]), request_bytes=len(evidence['requests'][q]),
                            failure_kind='fatal', failure_stream_raw='')
                rows[0]['cell_receipt'] = dict(epoch_ns=epoch, terminal_ns=terminal, abort_after=abort,
                    cell_started=stop_after is None, worker_started_ns=started_ns, admission_deadline_ns=deadline,
                    campaign_cgroup_before=baseline, cgroup_before=before, cgroup_after=native.capture(),
                    resource_errors=list(resource_errors))
                records.extend(rows)
                result = reduce_cell(rows, evidence, config)
                cells.append(close_cell(output, rows, result, proof, on_cell_closed))
                if not result['attainment'] and stop_after is None:
                    stop_after = dict(rate_index=index, reason='rate attainment failed', observed_ns=terminal)
        summary = reduce_records(records, evidence, config)
        assert summary['execution_gate_passed'] and not termination.is_set(), 'fatal identity/resource/cleanup failure'
    except BaseException as error:
        failure = error
        for index in range(len(records)//64, 6):
            epoch = time.monotonic_ns()
            records.extend(aborted_rows(index, epoch, dict(reason='fatal '+type(error).__name__, observed_ns=epoch)))
        summary = dict(schema='borsuk-semantic-1m-offered-result-v1', closed=True,
            execution_gate_passed=False, offered_gate_passed=False, error_type=type(error).__name__,
            error=str(error), planned_positions=384, closed_cells=cells)
    finally:
        stop.set()
        if monitor.ident is not None: monitor.join(timeout=5)
        cleanup_error = None
        try:
            assert not monitor.is_alive()
            shutil.rmtree(scratch); assert not scratch.exists()
            counters.update(after=native.capture(), closed=True); validate_cgroup(counters)
            assert not resource_errors and not termination.is_set() and time.monotonic_ns()-started_ns <= 3000*10**9
        except BaseException as error:
            cleanup_error = error
            summary.update(execution_gate_passed=False, offered_gate_passed=False, cleanup_error=str(error))
        for name, value in (
            ('records.jsonl', b''.join(encoded(r)+b'\n' for r in records)),
            ('failures.jsonl', b''.join(encoded(r)+b'\n' for r in records if r['outcome']=='failed')),
            ('summary.json', summary), ('offered-cgroup.json', counters),
            ('cleanup.json', dict(valid=cleanup_error is None, scratch_removed=not scratch.exists(),
                observer_stopped=not monitor.is_alive(), build_invocations=0, publication_invocations=0)),
            ('resources.json', dict(wall_seconds=(time.monotonic_ns()-started_ns)/1e9,
                process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                observations=sample_report, resource_errors=resource_errors,
                native_invocations=sum(r['native_process_started'] for r in records),
                http_attempts=sum(r['http_attempts'] for r in records),
                memory_bytes=MEMORY, swap_bytes=0, native_memory_bytes=NATIVE,
                native_rlimit_as_bytes=4*1024**3, server_query_slots=native.SERVER_QUERY_SLOTS,
                native_budget_model=native.native_budget_model(evidence['arm']['metadata_files']),
                build_invocations=0, publication_invocations=0,
                scratch_http_binary_bytes=evidence['binary']['bytes'],
                retained_publication=config['cold_run']['files']['screen/publication.json'],
                prices=config['prices'], cost_measured=False, billing='root reduction of actual instance lifetime and request/storage populations; caps are not bills'))):
            write(output/name, value)
        for signum, handler in previous.items(): signal.signal(signum, handler)
        if cleanup_error is not None: raise cleanup_error
    if failure is not None: raise failure
    return summary


if __name__ == '__main__':
    assert len(sys.argv) == 5, 'CONFIG SHA REPO NEW_OUTPUT'
    main(*sys.argv[1:])
