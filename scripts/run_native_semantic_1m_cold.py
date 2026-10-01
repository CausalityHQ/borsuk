"""One root-frozen FIRST1M v8 publication and serial fresh-process HTTP gate.

CLI: CONFIG CONFIG_SHA REPO NEW_OUTPUT. No build of executables or cloud launch.
quality_config authenticates the existing quality config; quality_run pins its
exact terminal artifact roster plus aws-{reservation,launch,closeout,terminal}.
native_proofs.{http,publisher} bind the same unchanged 399-file production tree,
binary bytes/SHA, completed release/Clippy/test-compilation and reused original
full-suite source identity. Root supplies these authorities; pending is rejected.
The launcher --self-check uses synthetic bodies/processes only.
"""
import base64
import importlib.metadata
import io
import json
import math
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

from scripts import run_native_semantic_1m_quality as quality
from scripts import launch_native_semantic_1m_quality_spot as quality_spot
from scripts import run_native_cold_first_query as cold
from scripts import run_native_semantic_router_cold as telemetry

panel, ids, stats = quality.panel, quality.ids, telemetry.stats
encoded, sha, artifact, write, read, identity = (quality.encoded, quality.sha,
    quality.artifact, quality.write, quality.read, quality.identity)
ROOT = quality.ROOT.parent / 'cold-1m'
CONFIG = ROOT / 'config.json'
MEMORY, PAYLOAD = 8 * 1024**3, 512 * 1024**2
THREAD_ENV = quality.THREAD_ENV
FIXED = dict(quality.FIXED, schema='borsuk-semantic-1m-cold-v1', k=10,
    score_invocations=0, publication_invocations=1, cold_invocations=64,
    physical_s3_measured=True, cold_http_measured=True, memory_bytes=MEMORY,
    publication_limit_seconds=1800, cold_limit_seconds=900,
    native_memory_bytes=PAYLOAD, native_rlimit_as_bytes=4 * 1024**3,
    namespace_connect_deadline_seconds=45, query_payload_timeout_seconds=5,
    native_process_limit_seconds=60, native_cpu_affinity=[0,1,2,3],
    client_cpu_affinity=[4,5], credential_protocol=stats.CREDENTIAL_PROTOCOL,
    query_split='FIRST1M fresh64 ordinals0..63 reservoir1000..1063',
    s3_service_cache='uncontrolled', process_cache='empty', concurrency=1,
    filters=None, context_p90_ms=444, matched_vendor_comparison=False)
FIXED.pop('score_limit_seconds')
CODE = tuple(sorted(set((*quality.CODE, *telemetry.CODE,
    'scripts/run_native_semantic_1m_cold.py', 'scripts/launch_native_semantic_1m_cold_spot.py'))))
STARTUP = ('manifest.json','page_manifest.json','page_digests.bin',
    'plane/manifest.json','plane/mean.bin','plane/page_digests.bin',
    'router/root.bin','router/membership.bin')
EXACT = set(STARTUP) - {'manifest.json','page_manifest.json','plane/manifest.json'}
PARITY = ('ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets',
    'source_submitted_gets','source_verified_bytes','source_failed_gets')
OUTPUTS = ('source-qualification.json','config.json','tool-versions.json','input-hashes.json',
    'sq8-ordinal-check.json','remote-sq8.json','builder-config.json','build.log',
    'build-resources.txt','preparation.log','extraction-resources.txt',
    'publisher-requests.jsonl','request-derivative.json','publication.log',
    'publication-resources.txt','publication-reference.jsonl','publication.json',
    'records.jsonl','failures.jsonl','summary.json','resources.json','cold-cgroup.json','cleanup.json')
QUALITY_ROSTER = (*quality_spot.ARTIFACTS,'aws-reservation.json','aws-launch.json',
                  'aws-closeout.json','aws-terminal.json')


def publisher_requests(body, rows=64, dimensions=768):
    requests = [json.loads(line) for line in body.splitlines()]
    assert len(requests)==rows
    result = []
    for ordinal, request in enumerate(requests):
        assert set(request)=={'ordinal','query'} and type(request['ordinal']) is int and request['ordinal']==ordinal
        query = request['query']
        assert len(query)==dimensions and all(type(x) in (int,float) and math.isfinite(x) for x in query)
        bits = struct.pack('<'+'f'*dimensions,*query)
        derivative = dict(query_ordinal=ordinal,query=query)
        row = encoded(derivative)
        assert struct.pack('<'+'f'*dimensions,*json.loads(row)['query'])==bits
        assert dict(ordinal=derivative['query_ordinal'],query=derivative['query'])==request
        result.append(row+b'\n')
    return b''.join(result)


def qualify(config_path, expected_sha, repo):
    repo, config_path = Path(repo).resolve(), Path(config_path).resolve()
    assert config_path==repo/CONFIG and not config_path.is_symlink() and config_path.stat().st_size<=65536
    body = config_path.read_bytes()
    assert sha(body)==expected_sha, 'config identity'
    config = json.loads(body)
    assert config['authority_pending'] is False, 'root freeze pending'
    assert set(config)==set(FIXED)|{'authority_pending','code_sha256','quality_config',
        'quality_run','native_proofs','binaries','namespace_prefix'}
    assert all(type(config[k]) is type(v) and config[k]==v for k,v in FIXED.items()), 'fixed cold/resource protocol'
    assert set(config['code_sha256'])==set(CODE), 'exact transitive code closure'
    assert all(artifact(panel.repo_path(repo,n))['sha256']==d for n,d in config['code_sha256'].items()), 'code identity'
    qpointer = config['quality_config']
    assert qpointer['path']==str(quality.CONFIG)
    read(repo,qpointer)
    qconfig, qproof = quality.qualify(repo/quality.CONFIG,qpointer['sha256'],repo)
    run = config['quality_run']
    assert set(run)=={'directory','files'} and run['directory']==str(quality.ROOT/'a0001')
    assert set(run['files'])==set(QUALITY_ROSTER), 'exact completed quality roster'
    for n,p in run['files'].items():
        assert p['path']==str(Path(run['directory'])/n)
        read(repo,p)
    assert quality_spot.replay(repo/run['directory'])['executed'] is True
    record_body = read(repo,run['files']['screen/records.jsonl'])
    quality.score_summary(repo/run['directory']/'screen/records.jsonl',qproof,
        run['files']['screen/scorer-config.json']['sha256'])
    assert set(config['binaries'])==set(config['native_proofs'])=={'http','publisher'}
    for n,p in config['binaries'].items():
        read(repo,p)
        assert type(p['key']) is str and p['key'] and '\n' not in p['key']
        native_proof(repo,config['native_proofs'][n],p,qproof)
    prefix = config['namespace_prefix']
    assert panel.re.fullmatch(r'research/semantic-router/20261001/fresh1m-cold-a[0-9]{4}/native',prefix), 'fresh owned namespace'
    proof = dict(qproof,config_path=str(CONFIG),config_sha256=expected_sha,
        code_identity_sha256=sha(encoded(config['code_sha256'])),
        refs_identity_sha256=sha(encoded(dict(quality_config=qpointer,quality_run=run,native_proofs=config['native_proofs']))),
        binary_sha256=dict(qproof['binary_sha256'],**{n:p['sha256'] for n,p in config['binaries'].items()}),
        physical_s3_measured=True,quality_reference_sha256=sha(record_body),
        native_rebuilt=False,namespace_prefix=prefix)
    return config, proof


def native_proof(repo, pointer, binary, quality_proof):
    proof = json.loads(read(repo,pointer))
    assert proof['qualified'] is True and proof['authority_pending'] is False
    assert proof['production_library_unchanged'] is True and proof['current_whole_tree_full_execution'] is False
    assert proof['source_file_count']==399 and proof['native_source_sha256']==quality_proof['native_source_sha256']
    assert proof['source_identity_sha256']==quality_proof['source_identity_sha256']
    assert proof['original_full_source_identity_sha256']==quality_proof['original_source_identity_sha256']
    assert dict(bytes=proof['binary_bytes'],sha256=proof['binary_sha256'])==identity(binary)
    for n in ('release_status','clippy_status','workspace_test_compilation_status'):
        assert type(proof[n]) is int and proof[n]==0, 'completed native gate: '+n
    assert proof['oom_kills']==proof['swap_peak_bytes']==0
    return proof


def validate_cgroup(report):
    with patch.object(panel,'MEMORY',MEMORY):
        panel.validate_cgroup(report)


def capture():
    snapshot = ids.capture_cgroup()
    group = Path(snapshot['cgroup'])
    for name in ('memory.current','memory.stat'):
        snapshot[name]=(group/name).read_text()
    try:
        snapshot['io.stat']=(group/'io.stat').read_text()
    except FileNotFoundError as error:
        snapshot['io.stat']='UNMEASURED'
        snapshot['io.stat_unavailable']=dict(type=type(error).__name__,errno=error.errno)
    return snapshot


def source_reference(body):
    frozen = [json.loads(line) for line in body.splitlines() if json.loads(line)['phase']=='frozen_query']
    assert [r['ordinal'] for r in frozen]==list(range(64))
    refs = []
    for row in frozen:
        refs.append(dict(query_ordinal=row['ordinal'],ids=row['returned_ids'],
            ranges=[[r['start'],r['end']] for r in row['ranges']],
            planned_bytes=row['sq8_bytes'],submitted_gets=row['sq8_gets'],
            verified_bytes=row['sq8_bytes'],failed_gets=0,
            source_submitted_gets=row['source_gets'],source_verified_bytes=row['source_bytes'],source_failed_gets=0,
            router_submitted_gets=row['leaf_gets'],router_verified_bytes=row['leaf_bytes'],router_failed_gets=0))
    return refs


def validate_query(response, arm, expected, truth=None, telemetry_required=True):
    assert response['authority']==arm['authority'], 'query authority'
    assert len(response['ids'])==len(set(response['ids']))==len(expected['ids'])
    assert all(type(n) is int and 0<=n<1_000_000 for n in response['ids'])
    for k in PARITY:
        assert response[k]==expected[k], 'source/scorer ID/plan/counter parity: '+k
    for prefix,gets,size in (('',32,16773120),('source_',128,67108864),('router_',16,2097152)):
        for key,cap in (('submitted_gets',gets),('verified_bytes',size)):
            stats.integer(response[prefix+key],prefix+key,1,cap)
        stats.integer(response[prefix+'failed_gets'],prefix+'failed_gets',0,0)
        if prefix=='router_':
            assert response['router_submitted_gets']>=8
            assert all(response[prefix+k]==expected[prefix+k] for k in stats.COUNTERS), 'logical leaf parity'
    previous, total = 0, 0
    for start,end in response['ranges']:
        assert type(start) is type(end) is int and previous<=start<end<=780000000
        assert start%199680==0 and (end%199680==0 or end==780000000)
        previous, total=end,total+end-start
    assert len(response['ranges'])==response['submitted_gets'] and total==response['planned_bytes']==response['verified_bytes']
    if telemetry_required:
        stats.validate_stages(response['query_stages'],response['native_wall_ns'],'semantic',response['router_submitted_gets'],True)
    return None if truth is None else len(set(response['ids']) & set(truth[:10]))


def validate_roster(arm):
    files, hashes = arm['metadata_files'],arm['metadata_sha256']
    assert set(files)==set(hashes)==set(STARTUP), 'v8 eight-object startup roster'
    for n,size in files.items():
        stats.integer(size,n,1,PAYLOAD); stats.digest(hashes[n])
    assert hashes['manifest.json']==arm['authority']['root_sha256']
    assert files['plane/mean.bin']==3072 and files['plane/page_digests.bin']==1_000_000
    assert files['page_digests.bin']==3907*32 and files['router/membership.bin']==125000
    assert all(files[n]<=65536 for n in STARTUP if n.endswith('.json'))
    assert 512<=files['router/root.bin']<=4*1024**2
    assert arm['leaf_object']['bytes']==31_250*1540
    identity(arm['leaf_object']); identity(arm['head_file'])
    assert arm['head_file']['bytes']<=65536
    return files


def validate_startup(value, arm, wall):
    files = validate_roster(arm)
    rows = value['metadata']
    assert [r['name'] for r in rows]==list(STARTUP) and {r['name']:r['bytes'] for r in rows}==files
    waves = []
    for i,row in enumerate(rows):
        for name in ('bytes','chunks','logical_head_requests','logical_get_requests',
            'payload_buffer_bound_bytes','metadata_wave'):
            stats.integer(row[name],name)
        wave = 0 if i==0 else (i-1)//4+1
        assert row['metadata_wave']==wave
        if len(waves)==wave: waves.append([])
        waves[wave].append(row)
    for batch in waves:
        duration=stats.integer(batch[0]['metadata_wave_wall_ns'],'wave wall',1,2**128-1)
        for row in batch:
            assert row['metadata_wave_wall_ns']==duration
            for n in ('head_wall_ns','get_wall_ns','stream_wall_ns','write_wall_ns'):
                stats.integer(row[n],n,maximum=2**128-1)
            assert row['logical_head_requests']==int(row['name'] not in EXACT)
            if row['name'] in EXACT: assert row['head_wall_ns']==0
            assert row['logical_get_requests']==(row['bytes']+4194303)//4194304
            assert row['payload_buffer_bound_bytes']==min(row['bytes'],(8//len(batch))*4194304)
            assert type(row['chunks']) is int and row['chunks']>0
            assert row['write_wall_ns']<=row['stream_wall_ns']
            assert sum(row[n] for n in ('head_wall_ns','get_wall_ns','stream_wall_ns'))<=duration
            if row['bytes']>4194304: assert row['get_wall_ns']==0
        assert sum(r['payload_buffer_bound_bytes'] for r in batch)<=8*4194304
    for n in ('staging_wall_ns','decode_wall_ns','source_head_wall_ns','router_head_wall_ns'):
        stats.integer(value[n],n,maximum=2**128-1)
    assert sum(b[0]['metadata_wave_wall_ns'] for b in waves)<=value['staging_wall_ns']
    stats.integer(value['source_head_requests'],'source HEAD',1,1)
    stats.integer(value['router_head_requests'],'router HEAD',1,1)
    assert sum(value[n] for n in ('staging_wall_ns','decode_wall_ns','source_head_wall_ns','router_head_wall_ns'))<=wall
    return dict(metadata_objects=8,metadata_bytes=sum(files.values()),
        logical_metadata_get_requests=sum(r['logical_get_requests'] for r in rows),
        logical_metadata_head_requests=3,source_head_requests=1,router_head_requests=1,
        staged_selected_leaf_bytes=0,staged_full_plane_bytes=0)


def transport(header, response, arm):
    assert header['phase']=='ready' and header['authority']==arm['authority']
    opened=validate_startup(header['remote_open_stats'],arm,header['remote_open_wall_ns'])
    ready=stats.validate_transport(header['transport'],True)
    final=stats.validate_transport(response['transport'],True)
    assert ready['method_counts']==[opened['logical_metadata_get_requests']+4,5,1]+[0]*7, 'startup S3/IMDS / no retries'
    credential_bytes=ready['consumed_payload_bytes']-opened['metadata_bytes']-arm['head_file']['bytes']-arm['metadata_files']['manifest.json']
    stats.integer(credential_bytes,'inferred credential bytes',1)
    query_gets=sum(response[p+'submitted_gets'] for p in ('','source_','router_'))
    query_bytes=sum(response[p+'verified_bytes'] for p in ('','source_','router_'))
    for n in ('attempts','transport_failures','stream_failures','consumed_payload_bytes','dropped_error_bodies'):
        assert final[n]>=ready[n], 'nonmonotonic transport'
    assert all(dict(final['status_counts']).get(s,0)>=n for s,n in ready['status_counts'])
    assert [b-a for a,b in zip(ready['method_counts'],final['method_counts'])]==[query_gets]+[0]*9, 'query transport / no retries'
    assert final['consumed_payload_bytes']-ready['consumed_payload_bytes']==query_bytes
    return dict(metadata=opened,startup_transport=ready,final_process_transport=final,
        credential_protocol=stats.CREDENTIAL_PROTOCOL,imds_token_PUTs=1,imds_credential_GETs=2,
        inferred_credential_consumed_bytes=credential_bytes,credential_payload_attribution=stats.CREDENTIAL_PAYLOAD_ATTRIBUTION,
        query_transport_submissions=query_gets,query_consumed_payload_bytes=query_bytes,
        confirmed_wire_requests='UNMEASURED',unknown=stats.UNKNOWN)


def close_native(server):
    """Preserve the original PGID even if the timed wrapper has already exited."""
    result = dict(returncode=None,intentional_stop=False,process_group_closed=False)
    try:
        result.update(cold_stop(server))
    except Exception as error:
        result['stop_error']=dict(type=type(error).__name__,message=str(error))
    finally:
        for sig in (signal.SIGTERM,signal.SIGKILL):
            try: os.killpg(server.pid,sig)
            except ProcessLookupError: pass
        try: server.wait(timeout=5)
        except subprocess.TimeoutExpired: result['wait_timeout']=True
        deadline=time.monotonic()+5
        while True:
            try: os.killpg(server.pid,0)
            except ProcessLookupError:
                result['process_group_closed']=True; break
            if time.monotonic()>=deadline: break
            time.sleep(.01)
        result['returncode']=server.returncode
    return result


cold_stop = cold.stop


def native_cpu(server_pid, binary):
    """Process CPU ticks (all threads), excluding the time/timeout wrappers."""
    pending=[server_pid]; seen=set(); matches=[]
    while pending:
        pid=pending.pop()
        assert pid not in seen; seen.add(pid)
        proc=Path('/proc')/str(pid)
        pending.extend(map(int,(proc/'task'/str(pid)/'children').read_text().split()))
        if (proc/'exe').resolve()==Path(binary).resolve():
            fields=(proc/'stat').read_text().rsplit(')',1)[1].split()
            limits=[line.split() for line in (proc/'limits').read_text().splitlines() if line.startswith('Max address space')]
            assert len(limits)==1 and limits[0][3:5]==[str(4*1024**3)]*2, 'native AS ceiling'
            affinity=sorted(os.sched_getaffinity(pid))
            assert affinity==[0,1,2,3], 'native CPU affinity'
            matches.append(dict(pid=pid,start_ticks=int(fields[19]),user_ticks=int(fields[11]),system_ticks=int(fields[12]),
                address_space_limit_bytes=4*1024**3,cpu_affinity=affinity))
    assert len(matches)==1, 'one native HTTP descendant'
    return matches[0]


def http_request(query, authority):
    return encoded(dict(query=query,k=10,**authority))


def measured_call(binary, config, arm, body, expected, truth):
    failures, observed = io.StringIO(), dict(native_process_started=False,http_attempts=0)
    spawn = subprocess.Popen
    def popen(*args,**kwargs):
        command=list(args[0]); index=command.index('taskset')
        command[index:index]=['prlimit','--as=4294967296:4294967296']
        process=spawn(command,**kwargs); observed['native_process_started']=True
        observed['wrapper_pid']=process.pid
        return process
    def post(client,payload):
        cpu_before=native_cpu(observed['wrapper_pid'],binary)
        observed['http_attempts']=1
        status,raw=cold.post(client,payload)
        # Preserve wire completion before CPU attribution, validation, GT and cleanup.
        wire_ns=time.monotonic_ns()
        observed.update(http_status=status,raw_response_base64=base64.b64encode(raw).decode(),
            first_wire_completed_ns=wire_ns)
        cpu_after=native_cpu(observed['wrapper_pid'],binary)
        assert (cpu_before['pid'],cpu_before['start_ticks'])==(cpu_after['pid'],cpu_after['start_ticks'])
        ticks=sum(cpu_after[n]-cpu_before[n] for n in ('user_ticks','system_ticks'))
        assert ticks>=0
        observed.update(query_cpu=dict(before=cpu_before,after=cpu_after,
                ticks=ticks,ticks_per_second=os.sysconf('SC_CLK_TCK'),
                scope='native process CPU sampled around one client POST; tick resolution'))
        return status,raw
    item=dict(arm,dataset='ReLAION')
    env=dict(os.environ,BORSUK_NATIVE_MEMORY_BYTES=str(PAYLOAD),AWS_MAX_ATTEMPTS='1')
    try:
        with patch.object(cold,'stop',close_native):
            row=cold.cold_call(str(binary),config,item,body,expected,truth,failures,
                response_check=lambda r,e,t,a:validate_query(r,arm,e,t),
                startup_check=lambda v,f,w:validate_startup(v,arm,w),post_call=post,spawn=popen,env=env)
        row.update(outcome='success',**observed)
        row['completed_ns']=observed['first_wire_completed_ns']
        for key,start in (('cold_start_to_first_http_response_ns','started_ns'),
            ('first_post_to_response_ns','connected_ns'),('incoming_http_wall_ns','successful_connect_attempt_ns')):
            row[key]=row['completed_ns']-row[start]
        row['accounting']=transport(row['native_header'],row['response'],arm)
        row['resources']=telemetry.resources(row['native_time_log'],PAYLOAD)
        assert row['native_close']['intentional_stop'] is True and row['native_close']['process_group_closed'] is True
    except Exception as error:
        failed=[json.loads(line) for line in failures.getvalue().splitlines()]
        row=locals().get('row',failed[0] if failed else {})
        row.update(outcome='failed',error_type=type(error).__name__,error=str(error),**observed)
    row.update(query_ordinal=expected['query_ordinal'],terminal_ns=time.monotonic_ns(),
        expected_authority=arm['authority'],request_sha256=sha(body),request_bytes=len(body),http_retry=False)
    return row


def scientific_gate(hits):
    stats.integer(hits,'hits10',maximum=640)
    return dict(returned_hits=hits,recall_at_10=hits/640,quality_gate_passed=hits>=608,
        status='PASS' if hits>=608 else 'FAIL')


def sq8_authority(head, local, expected):
    assert head['ContentLength']==expected['bytes'] and type(head['ETag']) is str and head['ETag']
    assert head['ETag']!=quality.local_head(local)['etag'], 'LocalFileSystem ETag forbidden for remote serving'
    return head['ETag']


def reduce_records(records, arm, requests, references, truth, start_ns, end_ns):
    assert len(records)==64 and [r['query_ordinal'] for r in records]==list(range(64)), 'closed ledger roster'
    successful, aborted = [], False
    for q,row in enumerate(records):
        if row['outcome']=='aborted':
            aborted=True; assert type(row['reason']) is str and row['reason']
            assert row['http_attempts']==0 and row['native_process_started'] is False
            continue
        assert not aborted and row['outcome'] in ('success','failed'), 'ledger closure order'
        assert row['request_sha256']==sha(requests[q]) and row['request_bytes']==len(requests[q])
        assert row['expected_authority']==arm['authority'] and row['http_retry'] is False
        if row['outcome']=='failed':
            aborted=True; assert row['error_type'] and row['error']
            continue
        assert row['http_status']==200 and row['http_attempts']==row['valid_ann_requests']==1
        assert row['response']==json.loads(base64.b64decode(row['raw_response_base64'],validate=True))
        header=[json.loads(line) for line in row['native_server_log'].splitlines() if line.startswith('{')]
        assert header==[row['native_header']] and row['native_header']['listen']=='127.0.0.1:8080'
        assert row['accounting']==transport(row['native_header'],row['response'],arm)
        assert row['returned_hits']==validate_query(row['response'],arm,references[q],truth[q])
        assert row['resources']==telemetry.resources(row['native_time_log'],PAYLOAD)
        assert row['native_close']['intentional_stop'] is True and row['native_close']['process_group_closed'] is True
        assert type(row['native_close']['returncode']) is int
        cpu=row['query_cpu']; before,after=cpu['before'],cpu['after']
        assert (before['pid'],before['start_ticks'])==(after['pid'],after['start_ticks'])
        assert all(c['address_space_limit_bytes']==4*1024**3 and c['cpu_affinity']==[0,1,2,3] for c in (before,after))
        assert cpu['scope']=='native process CPU sampled around one client POST; tick resolution'
        stats.integer(cpu['ticks_per_second'],'CPU tick resolution',1)
        assert cpu['ticks']==sum(after[n]-before[n] for n in ('user_ticks','system_ticks'))>=0
        assert row['started_ns']<=row['completed_ns']<=row['terminal_ns']
        assert row['cold_start_to_first_http_response_ns']==row['completed_ns']-row['started_ns']
        if successful: assert successful[-1]['terminal_ns']<=row['started_ns']
        successful.append(row)
    valid=len(successful)==64
    metrics={n:telemetry.tails([r[n]/1e6 for r in successful]) for n in (
        'cold_start_to_first_http_response_ns','before_successful_connect_attempt_ns',
        'successful_tcp_connect_ns','first_post_to_response_ns')}
    for n in ('remote_open_wall_ns','head_read_wall_ns'):
        metrics[n]=telemetry.tails([r['native_header'][n]/1e6 for r in successful])
    metrics['native_query_wall_ns']=telemetry.tails([r['response']['native_wall_ns']/1e6 for r in successful])
    for n in stats.STAGES:
        metrics[n]=telemetry.tails([(r['response']['query_stages'][n]['end_ns']-
            r['response']['query_stages'][n]['start_ns'])/1e6 for r in successful])
    assert end_ns>=start_ns and all(start_ns<=r['started_ns']<=r['terminal_ns']<=end_ns for r in successful)
    scientific=scientific_gate(sum(r['returned_hits'] for r in successful)) if valid else dict(
        status='EXECUTION_FAILED',quality_gate_passed=False,returned_hits='UNMEASURED',recall_at_10='UNMEASURED')
    return dict(scientific,schema='borsuk-semantic-1m-cold-result-v1',closed=True,
        execution_gate_passed=valid,records=64,valid_calls=len(successful),
        failed_calls=sum(r['outcome']=='failed' for r in records),aborted_calls=sum(r['outcome']=='aborted' for r in records),
        actual_process_starts=sum(r['native_process_started'] for r in records),
        actual_http_attempts=sum(r['http_attempts'] for r in records),
        latency_ms=metrics,context_p90_attained=valid and metrics['cold_start_to_first_http_response_ns']['p90']<444,
        serial_full_span_ns=end_ns-start_ns,
        serial_full_span_completions_per_second=len(successful)*1e9/(end_ns-start_ns) if end_ns>start_ns else 0,
        sustainable_qps='UNMEASURED',matched_vendor_comparison=False,
        serial_start_ns=start_ns,serial_end_ns=end_ns,
        transport_population='process startup plus one query; successful calls only',
        process_transport_totals={k:sum(r['accounting']['final_process_transport'][k] for r in successful)
            for k in ('attempts','consumed_payload_bytes','transport_failures','stream_failures')},
        logical_query_totals={p+k:sum(r['response'][p+k] for r in successful)
            for p in ('','source_','router_') for k in stats.COUNTERS},
        imds_token_PUTs=len(successful),imds_credential_GETs=2*len(successful),wire_bytes='UNMEASURED',
        exact_native_query_cpu_ns='UNMEASURED',
        sampled_http_interval_cpu_ns=sum(r['query_cpu']['ticks']*1e9/r['query_cpu']['ticks_per_second'] for r in successful),
        native_peak_rss_bytes=max((r['resources']['rss_peak_bytes'] for r in successful),default=0))


def publication_arm(root, prefix, head_body):
    manifest=json.loads((root/'manifest.json').read_bytes())
    assert manifest['schema']=='borsuk-two-bit-generation-v8' and manifest['generation']==1 and manifest['base_epoch']==0
    assert manifest['discovery']['mode']=='semantic' and manifest['discovery']['profile']=='fresh1m'
    root_sha=artifact(root/'manifest.json')['sha256']
    head=json.loads(head_body)
    assert head['schema']=='borsuk-two-bit-head-v2' and head['generation']==head['epoch']==1
    assert head['root_sha256']==root_sha and head['mutation'] is head['fence'] is None
    files={n:artifact(root/n) for n in STARTUP}
    arm=dict(discovery='semantic',authority=dict(root_sha256=root_sha,generation=1,control_epoch=1),
        indexes={'10':prefix},metadata_files={n:files[n]['bytes'] for n in STARTUP},
        metadata_sha256={n:files[n]['sha256'] for n in STARTUP},
        head_file=dict(bytes=len(head_body),sha256=sha(head_body)),leaf_object=artifact(root/'router/leaves.bin'))
    validate_roster(arm)
    discovery=manifest['discovery']
    assert files['router/root.bin']==dict(bytes=discovery['root_bytes'],sha256=discovery['root_sha256'])
    assert files['router/membership.bin']==dict(bytes=discovery['membership_bytes'],sha256=discovery['membership_sha256'])
    assert arm['leaf_object']==dict(bytes=discovery['leaves_bytes'],sha256=discovery['leaves_sha256'])
    return arm,manifest


def publication_reference(path, arm, expected):
    events=[json.loads(line) for line in Path(path).read_bytes().splitlines()]
    assert len(events)==65 and events[0]['phase']=='startup'
    header,rows=events[0],events[1:]
    assert header['top_k']==100 and header['declared_panel_count']==64
    assert all(header[k]==v for k,v in arm['authority'].items())
    assert [r['query_ordinal'] for r in rows]==list(range(64)) and all(r['phase']=='query' for r in rows)
    for row,reference in zip(rows,expected):
        assert all(row[k]==reference[k] for k in PARITY), 'publication/sealed scorer parity'
    return dict(validated_queries=64,top_k=100,source_scorer_parity=True,
        publish_wall_ns=header['publish_wall_ns'],remote_open_wall_ns=header['remote_open_wall_ns'])


def aborted_rows(reason, first=0):
    return [dict(query_ordinal=q,outcome='aborted',reason=reason,http_attempts=0,native_process_started=False)
            for q in range(first,64)]


def main(config_path, expected_sha, repo, output):
    repo,output=Path(repo).resolve(),Path(output).absolute()
    assert not output.is_symlink() and not output.exists() and not output.resolve().is_relative_to(repo)
    output.mkdir(parents=True)
    scratch=output/'scratch'; scratch.mkdir()
    started=time.monotonic(); counters={}; records=[]; failure=None; summary=None
    report=dict(stages={},build_invocations=0,publication_invocations=0,cold_invocations=0,
        scratch_peak_observed_bytes=0,scratch_sample_interval_seconds=1)
    def terminated(signum,frame):
        raise TimeoutError('service termination signal')
    previous_term=signal.signal(signal.SIGTERM,terminated)
    stop=threading.Event()
    def sample():
        while not stop.is_set():
            try: used=sum(p.stat().st_size for p in scratch.rglob('*') if p.is_file() and not p.is_symlink())
            except FileNotFoundError: continue
            report['scratch_peak_observed_bytes']=max(report['scratch_peak_observed_bytes'],used)
            stop.wait(1)
    monitor=threading.Thread(target=sample,daemon=True); monitor.start()
    try:
        config,proof=qualify(config_path,expected_sha,repo)
        qconfig,_=quality.qualify(repo/quality.CONFIG,config['quality_config']['sha256'],repo)
        counters=dict(before=capture(),closed=False)
        validate_cgroup(dict(counters,after=counters['before'],closed=True))
        assert all(os.environ.get(n)=='2' for n in THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS')=='1'
        assert os.sched_getaffinity(0)=={4,5}, 'client CPU affinity'
        tools=ids.tool_versions()
        versions={n:importlib.metadata.version(n) for n in ('numpy','pyarrow')}
        assert versions=={n:FIXED['versions'][n] for n in versions}
        write(output/'tool-versions.json',dict(tools,**versions,thread_environment={n:os.environ[n] for n in THREAD_ENV}))
        source={k:os.environ['BORSUK_COLD_'+n] for k,n in (('source_commit','SOURCE_COMMIT'),('source_archive_sha256','ARCHIVE_SHA256'))}
        assert panel.re.fullmatch('[0-9a-f]{40}',source['source_commit']) and panel.re.fullmatch('[0-9a-f]{64}',source['source_archive_sha256'])
        write(output/'source-qualification.json',dict(proof,**source)); write(output/'config.json',Path(config_path).read_bytes())
        write(output/'preparation.log',b'Authenticated FIRST1M preparation and S3 SQ8 staging.\n')
        stage=time.monotonic()
        def remaining(limit):
            left=min(limit-(time.monotonic()-stage),7200-(time.monotonic()-started))
            assert left>0, 'stage/service deadline'
            return left
        def run(args,log,limit,timing=None):
            if args[0] in (str(scratch/'binaries/builder'),str(scratch/'binaries/publisher')):
                args=['taskset','-c','0-3',*args]
            result=quality.run_process(args,output/log,remaining(limit),output/timing if timing else None)
            assert result['exit_status']==0 and result['process_cleanup'] is True, 'stage process failure'
            return result
        binaries=dict(builder=qconfig['binaries']['builder'],**config['binaries'])
        for kind,pointers in (('inputs',qconfig['inputs']),('binaries',binaries)):
            (scratch/kind).mkdir()
            for n,p in pointers.items():
                target=scratch/kind/n
                run(['aws','s3','cp',f"s3://{config['bucket']}/{p['key']}",str(target),'--only-show-errors'],'preparation.log',1800)
                assert artifact(target)==identity(p), 'download body: '+n
                if kind=='binaries': target.chmod(0o500)
        inputs=scratch/'inputs'; executable=scratch/'binaries'
        raw_sha=json.loads(read(repo,qconfig['refs']['input_authorities']))['raw']['sha256']
        result=run([sys.executable,'-c','from pathlib import Path; import sys; from scripts import seal_v36_rank16_fresh_1m as seal; seal.source_raw(Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[3])',
            str(inputs/'parquet'),str(scratch/'raw'),raw_sha],'preparation.log',1800,'extraction-resources.txt')
        report['stages']['extraction']=dict(result,cgroup=capture())
        assert artifact(scratch/'raw')==dict(bytes=3_072_000_000,sha256=raw_sha)
        check=quality.ordinal_check(inputs/'sq8',inputs/'order'); write(output/'sq8-ordinal-check.json',check)
        coefficients=json.loads((inputs/'builder').read_bytes())
        assert (coefficients['rows'],coefficients['dimensions'])==(1_000_000,768)
        assert coefficients['raw_sha256']==raw_sha and coefficients['sq8_sha256']==check['sq8']['sha256']
        assert all(len(coefficients[n])==768 and all(math.isfinite(v) for v in coefficients[n]) for n in ('low','step'))
        sq8_key=config['namespace_prefix']+'/objects/'+check['sq8']['sha256']
        head_path=scratch/'sq8-head.json'
        # Conditional PUT rejects accidental reuse, then exact HEAD and full authenticated GET.
        run(['aws','s3api','put-object','--bucket',config['bucket'],'--key',sq8_key,
            '--body',str(inputs/'sq8'),'--if-none-match','*'],'preparation.log',1800)
        result=quality.run_process(['aws','s3api','head-object','--bucket',config['bucket'],'--key',sq8_key],head_path,remaining(1800))
        assert result['exit_status']==0
        head=json.loads(head_path.read_bytes())
        sq8_authority(head,inputs/'sq8',check['sq8'])
        readback=scratch/'sq8-readback'
        run(['aws','s3api','get-object','--bucket',config['bucket'],'--key',sq8_key,'--if-match',head['ETag'],str(readback)],'preparation.log',1800)
        assert artifact(readback)==check['sq8']; readback.unlink()
        write(output/'remote-sq8.json',dict(key=sq8_key,**check['sq8'],etag=head['ETag'],remote_readback_authenticated=True,
            backend='AmazonS3',generation_version=head.get('VersionId','UNMEASURED')))
        builder=dict(discovery='semantic',semantic_profile='fresh1m',
            order=dict(path=str(inputs/'order'),sha256=check['order']['sha256']),raw=str(scratch/'raw'),raw_sha256=raw_sha,
            sq8=str(inputs/'sq8'),sq8_sha256=check['sq8']['sha256'],rows=1_000_000,dimensions=768,generation=1,base_epoch=0,
            low=coefficients['low'],step=coefficients['step'],sq8_object_key=sq8_key,sq8_etag=head['ETag'])
        write(output/'builder-config.json',builder)
        request_body=read(repo,qconfig['panel']['files']['screen/requests.jsonl'])
        derived=publisher_requests(request_body); write(output/'publisher-requests.jsonl',derived)
        write(output/'request-derivative.json',dict(original=identity(qconfig['panel']['files']['screen/requests.jsonl']),
            derivative=artifact(output/'publisher-requests.jsonl'),only_ordinal_key_changed=True,f32_bits_equal=True))
        truth_body=read(repo,qconfig['panel']['files']['screen/truth.i64'])
        assert len(truth_body)==64*100*8
        truth=[list(struct.unpack_from('<100q',truth_body,q*800)) for q in range(64)]
        assert all(len(set(t))==100 and all(0<=n<1_000_000 for n in t) for t in truth)
        reference=source_reference(read(repo,config['quality_run']['files']['screen/records.jsonl']))
        write(output/'input-hashes.json',dict(inputs=qconfig['inputs'],raw=artifact(scratch/'raw'),
            panel=qconfig['panel'],binaries=binaries,quality_run=config['quality_run']))
        report['stages']['preparation']=dict(wall_seconds=time.monotonic()-stage,cgroup=capture())
        stage=time.monotonic(); report['build_invocations']+=1; root=scratch/'generation'
        result=run([str(executable/'builder'),str(output/'builder-config.json'),artifact(output/'builder-config.json')['sha256'],str(PAYLOAD),str(root)],
            'build.log',3600,'build-resources.txt')
        report['stages']['build']=dict(result,cgroup=capture())
        root_sha=artifact(root/'manifest.json')['sha256']
        assert (output/'build.log').read_text().splitlines()[-1]==root_sha
        assert json.loads((root/'manifest.json').read_bytes())['sq8_etag']==head['ETag']
        stage=time.monotonic(); report['publication_invocations']+=1
        env_before=os.environ.get('BORSUK_NATIVE_MEMORY_BYTES'); os.environ['BORSUK_NATIVE_MEMORY_BYTES']=str(PAYLOAD)
        try:
            result=run([str(executable/'publisher'),str(root),root_sha,str(output/'publisher-requests.jsonl'),sha(derived),
                str(output/'publication-reference.jsonl'),'0','64','--live-s3',config['bucket'],config['region'],config['namespace_prefix'],
                '--panel-count','64','--top-k','100'],'publication.log',1800,'publication-resources.txt')
        finally:
            if env_before is None: os.environ.pop('BORSUK_NATIVE_MEMORY_BYTES',None)
            else: os.environ['BORSUK_NATIVE_MEMORY_BYTES']=env_before
        head_remote=scratch/'head.json'
        run(['aws','s3','cp',f"s3://{config['bucket']}/{config['namespace_prefix']}/head.json",str(head_remote),'--only-show-errors'],'publication.log',1800)
        arm,manifest=publication_arm(root,config['namespace_prefix'],head_remote.read_bytes())
        for n in STARTUP:
            target=scratch/'metadata-readback'
            run(['aws','s3','cp',f"s3://{config['bucket']}/{config['namespace_prefix']}/generations/{root_sha}/{n}",str(target),'--only-show-errors'],'publication.log',1800)
            assert artifact(target)==dict(bytes=arm['metadata_files'][n],sha256=arm['metadata_sha256'][n]); target.unlink()
        validated=publication_reference(output/'publication-reference.jsonl',arm,reference)
        assert manifest['sq8_object_key']==sq8_key and manifest['sq8_etag']==head['ETag']
        write(output/'publication.json',dict(arm=arm,manifest=manifest,remote_sq8= json.loads((output/'remote-sq8.json').read_bytes()),
            head= json.loads(head_remote.read_bytes()),head_body_base64=base64.b64encode(head_remote.read_bytes()).decode(),
            reference=artifact(output/'publication-reference.jsonl'),validation=validated,retained_for_offered_gate=True))
        report['stages']['publication']=dict(result,wall_seconds=time.monotonic()-stage,cgroup=capture())
        stage=time.monotonic(); span_start=time.monotonic_ns()
        requests=[http_request(json.loads(line)['query'],arm['authority']) for line in derived.splitlines()]
        expected=[dict(r,ids=r['ids'][:10]) for r in reference]
        write(output/'failures.jsonl',b'')
        for q in range(64):
            remaining(900)
            assert all(artifact(executable/n)==identity(p) for n,p in binaries.items()), 'binary bytes drift'
            report['cold_invocations']+=1
            row=measured_call(executable/'http',config,arm,requests[q],expected[q],truth[q])
            records.append(row)
            if row['outcome']=='failed':
                with (output/'failures.jsonl').open('ab') as stream:
                    stream.write(encoded(row)+b'\n'); stream.flush(); os.fsync(stream.fileno())
                records.extend(aborted_rows('fatal cold call',q+1)); break
            validate_cgroup(dict(counters,after=capture(),closed=True))
        span_end=time.monotonic_ns()
        remaining(900)
        report['stages']['cold']=dict(wall_seconds=time.monotonic()-stage,cgroup=capture())
        summary=reduce_records(records,arm,requests,expected,truth,span_start,span_end)
        assert summary['execution_gate_passed'], 'cold execution failed'
    except BaseException as error:
        failure=error
        records.extend(aborted_rows('fatal '+type(error).__name__,len(records)))
        summary=dict(schema='borsuk-semantic-1m-cold-result-v1',status='EXECUTION_FAILED',
            execution_gate_passed=False,closed=True,error_type=type(error).__name__,error=str(error),records=64)
    finally:
        stop.set(); monitor.join(timeout=5)
        cleanup_error=None
        try:
            assert not monitor.is_alive(), 'scratch observer remains'
            shutil.rmtree(scratch)
            assert not scratch.exists()
            if counters:
                counters.update(after=capture(),closed=True); validate_cgroup(counters)
            assert time.monotonic()-started<=7200, 'whole service deadline'
        except BaseException as error:
            cleanup_error=error
            summary=dict(summary or {},status='EXECUTION_FAILED',execution_gate_passed=False,
                cleanup_error_type=type(error).__name__,cleanup_error=str(error))
        report.update(wall_seconds=time.monotonic()-started,process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        for n,value in (('resources.json',report),('cold-cgroup.json',counters),
            ('cleanup.json',dict(scratch_removed=not scratch.exists(),observer_stopped=not monitor.is_alive(),
                build_invocations=report['build_invocations'],publication_invocations=report['publication_invocations'],cold_invocations=report['cold_invocations'],
                valid=cleanup_error is None)),('summary.json',summary),
            ('records.jsonl',b''.join(encoded(r)+b'\n' for r in records))):
            write(output/n,value)
        signal.signal(signal.SIGTERM,previous_term)
        if cleanup_error is not None: raise cleanup_error
    if failure is not None: raise failure
    return summary


if __name__=='__main__':
    assert len(sys.argv)==5, 'usage: CONFIG CONFIG_SHA REPO NEW_OUTPUT'
    main(*sys.argv[1:])
