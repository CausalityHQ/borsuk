"""Root-owned retained FIRST1M gate: aNNNN | --self-check | --replay DIR.

--worker CONFIG SHA REPO NEW_OUTPUT PREFIX is the bounded remote entrypoint.
The shared lifecycle fsyncs every ACK and terminates/waits before collection.
Only --self-check is authorized during adapter development.
"""
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import Mock, patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import run_native_semantic_1m_offered as worker

native, panel, ids = worker.native, worker.panel, worker.ids
shared_quality = native.quality_spot
ROOT, CONFIG, PREFIX, CODE = worker.ROOT, worker.CONFIG, worker.PREFIX, worker.CODE
NAME = ''
SCHEMA = 'borsuk-semantic-1m-offered-spot-v1'
TOKEN_PREFIX, TAG = 'semantic-1m-offered-', 'borsuk-semantic-1m-offered'
WALL = 3600
INSTANCE_TYPE, IMAGE_ID = panel.INSTANCE_TYPE, panel.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = panel.ROOT_DEVICE_NAME, panel.SUBNET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, .50
AWSCLI_VERSION, AWSCLI_SHA256 = panel.AWSCLI_VERSION, panel.AWSCLI_SHA256
SDK_PACKAGES = ('boto3==1.40.72', 'botocore==1.40.72', 's3transfer==0.14.0',
    'jmespath==1.0.1', 'python-dateutil==2.9.0', 'six==1.17.0', 'urllib3==2.6.3')
ARTIFACTS = ('test-resources.txt', 'run-closed.log', *('screen/'+n for n in worker.OUTPUTS))
TERMINAL_IDENTITIES = (*shared_quality.TERMINAL_IDENTITIES, 'quality_reference_sha256',
    'namespace_prefix', 'measurement_prefix', 'cold_terminal_sha256')


def qualify(base=Path('.')):
    base = Path(base).resolve()
    _, proof = worker.qualify(base/CONFIG, worker.artifact(base/CONFIG)['sha256'], base)
    return dict(proof, campaign_schema=SCHEMA, artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=base,text=True).strip(), 'dirty source'
    return proof


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert prefix == qualification['measurement_prefix']
    assert not qualification['namespace_prefix'].startswith(prefix), 'measurement/publication separation'
    with patch.multiple(shared_quality, CONFIG=CONFIG, SCHEMA=SCHEMA, PREFIX=PREFIX,
        ARTIFACTS=ARTIFACTS, TERMINAL_IDENTITIES=TERMINAL_IDENTITIES, WALL=WALL):
        body = shared_quality.user_data(commit, archive_sha, archive_key, prefix, qualification)
    body = body.replace('numpy==2.3.3 pyarrow==24.0.0\n',
        'numpy==2.3.3 pyarrow==24.0.0 '+' '.join(SDK_PACKAGES)+'\n'
        'PYTHONPATH="$root/repo" "$root/venv/bin/python" -c \'import numpy, pyarrow; '
        'from scripts import launch_native_semantic_1m_offered_spot as launch; launch.ids.lifecycle()\'\n')
    body = body.replace('MemoryMax=1G', 'MemoryMax=8G -p IOAccounting=yes')
    body = body.replace('RuntimeMaxSec=7200', 'RuntimeMaxSec=3000').replace('--kill-after=5 7200', '--kill-after=90 3000')
    body = body.replace('BORSUK_QUALITY_', 'BORSUK_OFFERED_').replace('semantic-1m-quality', 'semantic-1m-offered')
    body = body.replace('"$root/venv/bin/python" -m scripts.run_native_semantic_1m_quality',
        'taskset -c 4-5 "$root/venv/bin/python" -m scripts.launch_native_semantic_1m_offered_spot --worker')
    body = body.replace('"$root/repo" "$root/screen"\n', f'"$root/repo" "$root/screen" {prefix}\n')
    body = body.replace('if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi',
        'if [ "$name" = screen/failures.jsonl ]; then test -f "$name"; elif [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi')
    subprocess.run(['bash','-n'], input=body, text=True, check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0], '<terminal>', 'exec')
    assert len(body.encode()) < 16384 and 'IOAccounting=yes' in body
    assert all(n not in body for n in ('cargo','rustup','--publish','run_native_semantic_1m_cold'))
    return body


def poll(ec2, s3, prefix, instance_id, started):
    shared, _ = ids.lifecycle()
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def checkpoint(marker, paths, config, proof, output, uploaded, put):
    assert set(paths) == {'records','summary'}
    index = marker['rate_index']; assert type(index) is int and 0 <= index < 6 and index not in uploaded
    assert marker['closed'] is marker['cleanup_confirmed'] is True
    for name, suffix in (('records','records.jsonl'),('summary','summary.json')):
        path = Path(paths[name])
        assert not path.is_symlink() and path.is_file()
        assert path.resolve() == output.resolve()/f'rate{index}-{suffix}'
    assert json.loads(paths['summary'].read_bytes()) == marker
    assert worker.artifact(paths['records']) == marker['records']
    for n in ('config_sha256','code_identity_sha256','refs_identity_sha256'):
        assert marker[n] == proof[n]
    assert marker['binary_sha256'] == proof['binary_sha256']['http']
    rows = [json.loads(line) for line in paths['records'].read_bytes().splitlines()]
    assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
    assert all(r['rate_index'] == index for r in rows)
    for name in ('records','summary'):
        path = paths[name]
        put(['aws','s3api','put-object','--if-none-match','*','--bucket',config['bucket'],
            '--key',config['measurement_prefix']+'/cells/'+path.name,'--body',str(path),
            '--region',config['region'],'--cli-connect-timeout','5','--cli-read-timeout','15','--no-cli-pager'])
    uploaded.add(index)


def remote_worker(args):
    assert len(args) == 5
    config_path, digest, repo, output, prefix = args
    config, proof = worker.qualify(config_path, digest, repo)
    assert prefix == config['measurement_prefix']
    _, bootstrap = ids.lifecycle()
    bootstrap._check_checkpoint_cli()
    uploaded = set()
    return worker.main(config_path, digest, repo, output,
        on_cell_closed=lambda marker,paths:checkpoint(marker,paths,config,proof,Path(output),uploaded,bootstrap._checkpoint_cli_call))


def replay(out):
    """Authenticated terminated artifacts only; independent raw response/GT replay."""
    out = Path(out)
    launch, closed, reservation, terminal = (json.loads((out/n).read_bytes()) for n in (
        'aws-launch.json','aws-closeout.json','aws-reservation.json','aws-terminal.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes']
    assert terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', launch['prefix'])
    for n in ('source_commit','source_archive_sha256'):
        assert terminal[n] == reservation[n] == launch[n]
    repo = Path(__file__).resolve().parents[1]
    proof = reservation['qualification']; assert proof == qualify(repo)
    assert proof['measurement_prefix'] == launch['prefix']
    for n in TERMINAL_IDENTITIES: assert terminal[n] == proof[n]
    assert reservation['wall_seconds'] == WALL and reservation['instance_type'] == INSTANCE_TYPE
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    for n, pointer in terminal['artifacts'].items(): assert worker.artifact(out/n) == pointer
    complete = terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['status'] == ('complete' if complete else 'failed')
    if not complete:
        if 'screen/records.jsonl' in terminal['artifacts']:
            rows = [json.loads(line) for line in (out/'screen/records.jsonl').read_bytes().splitlines()]
            assert [(r['rate_index'],r['query_ordinal']) for r in rows] == [(i,q) for i in range(6) for q in range(64)]
        return dict(executed=False, execution_gate_passed=False)
    assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(ARTIFACTS)
    screen = out/'screen'
    assert worker.artifact(screen/'config.json')['sha256'] == proof['config_sha256']
    config = json.loads((screen/'config.json').read_bytes()); evidence = worker.inputs(repo, config)
    source = {n:terminal[n] for n in ('source_commit','source_archive_sha256')}
    assert json.loads((screen/'source-qualification.json').read_bytes()) == {
        n:v for n,v in dict(proof,**source).items() if n not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256')}
    assert json.loads((screen/'input-hashes.json').read_bytes()) == evidence['identities']
    records = [json.loads(line) for line in (screen/'records.jsonl').read_bytes().splitlines()]
    assert (screen/'records.jsonl').read_bytes() == b''.join(worker.encoded(r)+b'\n' for r in records)
    summary = worker.reduce_records(records, evidence, config)
    assert json.loads((screen/'summary.json').read_bytes()) == summary and summary['execution_gate_passed'] is True
    for index, cell in enumerate(summary['cells']):
        rows = records[index*64:(index+1)*64]
        path = screen/f'rate{index}-records.jsonl'
        assert path.read_bytes() == b''.join(worker.encoded(r)+b'\n' for r in rows)
        expected = dict(cell, records=worker.artifact(path), config_sha256=proof['config_sha256'],
            code_identity_sha256=proof['code_identity_sha256'], refs_identity_sha256=proof['refs_identity_sha256'],
            binary_sha256=proof['binary_sha256']['http'])
        assert json.loads((screen/f'rate{index}-summary.json').read_bytes()) == expected
    assert (screen/'failures.jsonl').read_bytes() == b''.join(worker.encoded(r)+b'\n' for r in records if r['outcome']=='failed')
    worker.validate_cgroup(json.loads((screen/'offered-cgroup.json').read_bytes()))
    clean, report = (json.loads((screen/n).read_bytes()) for n in ('cleanup.json','resources.json'))
    assert clean['valid'] is clean['scratch_removed'] is clean['observer_stopped'] is True
    assert clean['build_invocations'] == clean['publication_invocations'] == report['build_invocations'] == report['publication_invocations'] == 0
    assert 0 <= report['wall_seconds'] <= 3000 and report['resource_errors'] == []
    assert report['native_invocations'] == sum(r['native_process_started'] for r in records)
    assert report['http_attempts'] == sum(r['http_attempts'] for r in records)
    assert report['prices'] == config['prices'] and report['cost_measured'] is False
    assert report['native_memory_bytes']==worker.NATIVE and report['server_query_slots']==4
    assert report['memory_bytes']==worker.MEMORY and report['swap_bytes']==0
    assert report['native_rlimit_as_bytes']==4*1024**3
    assert report['native_budget_model']==native.native_budget_model(evidence['arm']['metadata_files'])
    return dict(executed=True, execution_gate_passed=True, offered_gate_passed=summary['offered_gate_passed'],
                largest_passing_tested_offered_qps=summary['largest_passing_tested_offered_qps'])


def collect(s3, prefix, out, instance_id, commit, digest):
    launch, closed = (json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes']
    assert launch['prefix'] == prefix and launch['instance_id'] == instance_id
    with patch.multiple(ids, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, replay=replay):
        return ids.collect(s3, prefix, out, instance_id, commit, digest)


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}', attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()
        lifecycle, _ = ids.lifecycle()
        return lifecycle.main(attempt, campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """One stdlib synthetic check. No dataset, binary, SDK or cloud calls."""
    start = time.monotonic()
    def rejects(action):
        try: action()
        except (AssertionError,ValueError,KeyError,FileNotFoundError,FileExistsError,RuntimeError): return
        raise AssertionError('invalid evidence accepted')
    sizes = [2000,1000,3907*32,500,3072,1000000,1024,125000]
    arm = dict(discovery='semantic', authority=dict(root_sha256='a'*64,generation=1,control_epoch=1),
        indexes={'10':'retained/native'},metadata_files=dict(zip(native.STARTUP,sizes)),
        metadata_sha256={n:'a'*64 for n in native.STARTUP},head_file=dict(bytes=200,sha256='a'*64),
        leaf_object=dict(bytes=31250*1540,sha256='a'*64))
    metadata = [dict(name=n,bytes=size,chunks=1,metadata_wave=0 if i==0 else (i-1)//4+1,
        metadata_wave_wall_ns=10,logical_head_requests=int(n not in native.EXACT),logical_get_requests=1,
        head_wall_ns=int(n not in native.EXACT),get_wall_ns=2,stream_wall_ns=3,write_wall_ns=1,
        payload_buffer_bound_bytes=size) for i,(n,size) in enumerate(zip(native.STARTUP,sizes))]
    startup = dict(metadata=metadata,staging_wall_ns=30,decode_wall_ns=2,source_head_requests=1,
        router_head_requests=1,source_head_wall_ns=2,router_head_wall_ns=2)
    def transport(methods, payload):
        return dict(schema='borsuk-native-transport-v1',scope='process_all_native_s3_readers',per_query_delta=False,
            attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests',method_order=worker.stats.METHODS,
            status_counts_format='[http_status,count] nonzero entries',
            payload_measurement='consumed response data frames, including unauthenticated payload',
            unknown=worker.stats.UNKNOWN,dropped_error_body_consumed_bytes=0,
            totals=dict(attempts=sum(methods),method_counts=methods,status_counts=[[200,sum(methods)]],
                transport_failures=0,stream_failures=0,dropped_error_bodies=0,consumed_payload_bytes=payload))
    header = dict(phase='ready',authority=arm['authority'],listen='127.0.0.1:18080',remote_open_stats=startup,
        remote_open_wall_ns=40,head_read_wall_ns=10,transport=transport([12,5,1]+[0]*7,sum(sizes)+2500))
    expected = dict(query_ordinal=0,ids=list(range(10)),ranges=[[0,199680]],planned_bytes=199680,
        submitted_gets=1,verified_bytes=199680,failed_gets=0,source_submitted_gets=1,source_verified_bytes=100,
        source_failed_gets=0,router_submitted_gets=8,router_verified_bytes=500,router_failed_gets=0)
    stages = {n:dict(start_ns=i*10+1,end_ns=i*10+8) for i,n in enumerate(worker.stats.STAGES)}
    stages['leaf_peak_inflight'] = 8
    response = dict(expected,authority=arm['authority'],query_stages=stages,native_wall_ns=40,
        transport=transport([22,5,1]+[0]*7,header['transport']['totals']['consumed_payload_bytes']+200280))
    request = native.http_request([1]*768,arm['authority'])
    evidence = dict(arm=arm,requests=[request]*64,references=[dict(expected,query_ordinal=q) for q in range(64)],
        truth=[list(range(100)) for q in range(64)])
    log = 'User time (seconds): 0.01\nSystem time (seconds): 0.01\nMaximum resident set size (kbytes): 42\n'
    process = Mock(pid=1234,returncode=143); process.poll.return_value = None
    def spawn(args, **kwargs):
        assert '--as=4294967296:4294967296' in args and kwargs['start_new_session'] is True
        assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES'] == str(worker.NATIVE)
        kwargs['stdout'].write(json.dumps(header)+'\n'); kwargs['stdout'].flush()
        Path(args[3]).write_text(log)
        return process
    cpu = dict(pid=1235,start_ticks=100,user_ticks=1,system_ticks=1,address_space_limit_bytes=4*1024**3,cpu_affinity=[0,1,2,3])
    def killpg(pid, sig):
        assert pid == 1234
        if sig == 0: raise ProcessLookupError()
    original_stop = worker.cold.stop
    with patch.object(worker.subprocess,'Popen',side_effect=spawn), patch.object(worker.cold.http.client,'HTTPConnection'), \
        patch.object(worker.cold,'post',return_value=(200,worker.encoded(response))), \
        patch.object(native,'native_cpu',side_effect=[cpu,dict(cpu,user_ticks=2)]), \
        patch.object(native,'cold_stop',return_value=dict(intentional_stop=True,returncode=143)), \
        patch.object(native.os,'killpg',side_effect=killpg), patch.object(worker.cold,'stop',native.close_native):
        template = worker.measured_call('mock',worker.FIXED,evidence,0,18080)
    assert worker.cold.stop is original_stop and template['outcome'] == 'success', template
    for status, payload in ((503,b'{"error":"unavailable"}'),(200,worker.encoded(dict(response,authority={})))):
        with patch.object(worker.subprocess,'Popen',side_effect=spawn), patch.object(worker.cold.http.client,'HTTPConnection'), \
            patch.object(worker.cold,'post',return_value=(status,payload)),patch.object(native,'native_cpu',return_value=cpu), \
            patch.object(native,'cold_stop',return_value=dict(intentional_stop=True,returncode=143)), \
            patch.object(native.os,'killpg',side_effect=killpg),patch.object(worker.cold,'stop',native.close_native):
            failed=worker.measured_call('mock',worker.FIXED,evidence,0,18080)
        assert failed['outcome']=='failed' and failed['cleanup_confirmed'] is True
        assert worker.base64.b64decode(failed['raw_response_base64'])==payload and failed['failure_stream_raw']
        assert failed['completed_ns']==failed['first_wire_completed_ns']
        assert failed['failure_kind']==('transport' if status==503 else 'fatal')
        if status==503: assert failed['failed_final_transport']=='UNMEASURED'
    # Hold six completed responses through cleanup. The seventh cannot spawn/POST.
    owners, events, held, release = set(), [], threading.Event(), threading.Event()
    lock = threading.Lock()
    def call(q, port):
        with lock:
            assert port not in owners; owners.add(port); events.append(('spawn',q,port))
            if len(owners) == 6: held.set()
        if q < 6: assert release.wait(2)
        with lock:
            events.append(('cleanup',q,port)); owners.remove(port)
        return dict(outcome='success',cleanup_confirmed=True,abort_admissions=False)
    def unlock():
        assert held.wait(2)
        time.sleep(.025); release.set()
    unlocker = threading.Thread(target=unlock); unlocker.start()
    with patch.object(worker.offered,'scheduled_offsets_ns',return_value=[q*4000000 for q in range(64)]):
        held_rows, _, _, _ = worker.offered.schedule_offers(call,250)
    unlocker.join(2)
    assert held_rows[6]['outcome'] == 'capacity_drop' and not any(e[0]=='spawn' and e[1]==6 for e in events)
    assert any(r['outcome']=='success' for r in held_rows[12:]) and not owners
    snapshot = dict(cgroup='/mock',observer_pid=1,process_ids=[1],**{
        'memory.max':str(worker.MEMORY),'memory.peak':'100000','memory.swap.max':'0','memory.swap.peak':'0',
        'memory.events':'oom 0\noom_kill 0\noom_group_kill 0\nmax 0\n','memory.swap.events':'max 0\n',
        'cpu.max':'200000 100000','cpu.stat':'usage_usec 1\n','pids.max':'512','pids.current':'1','pids.events':'max 0\n',
        'memory.current':'100000','memory.stat':'anon 1000\nfile 99000\n','io.stat':'UNMEASURED',
        'io.stat_unavailable':dict(type='FileNotFoundError',errno=2)})
    config = dict(worker.FIXED)
    siblings=dict(snapshot,process_ids=[1,2,3],**{'pids.current':'3'})
    worker.validate_cgroup(dict(before=siblings,after=siblings,closed=True))
    rejects(lambda:worker.validate_cgroup(dict(before=snapshot,after=siblings,closed=True)))
    def ledger():
        rows=[]; epoch=10**12
        for index,rate in enumerate(worker.RATES):
            for q in range(64):
                row=copy.deepcopy(template); at=epoch+round(q*1e9/rate)
                row.update(query_ordinal=q,rate_index=index,dataset='ReLAION',offered_qps=rate,scheduled_ns=at,
                    dispatched_ns=at,started_ns=at+1,successful_connect_attempt_ns=at+100,
                    connected_ns=at+200,completed_ns=at+1000,first_wire_completed_ns=at+1000,
                    terminal_ns=at+2000,port=18080,cold_start_to_first_http_response_ns=999,
                    before_successful_connect_attempt_ns=99,successful_tcp_connect_ns=100,
                    first_post_to_response_ns=800,incoming_http_wall_ns=900)
                rows.append(row)
            terminal=rows[-1]['terminal_ns']+1
            rows[-64]['cell_receipt']=dict(epoch_ns=epoch,terminal_ns=terminal,abort_after=None,cell_started=True,
                worker_started_ns=10**12,admission_deadline_ns=10**12+(3000-90)*10**9,
                campaign_cgroup_before=snapshot,cgroup_before=snapshot,cgroup_after=copy.deepcopy(snapshot),resource_errors=[])
            epoch=terminal+1
        return rows
    rows=ledger(); result=worker.reduce_records(rows,evidence,config)
    assert result['counts']==dict(planned=384,dispatched=384,admitted=384,terminal_completed=384,successful=384)
    assert result['offered_gate_passed'] and result['largest_passing_tested_offered_qps']==8
    assert native.scientific_gate(607)['quality_gate_passed'] is False and native.scientific_gate(608)['quality_gate_passed'] is True
    # Independent truth recount, including the boundary rather than reported hits.
    boundary=copy.deepcopy(evidence)
    for q in range(32): boundary['truth'][q]=list(range(9))+list(range(100,191))
    cell=copy.deepcopy(rows[:64])
    for q in range(32): cell[q]['returned_hits']=9
    assert worker.reduce_cell(cell,boundary,config)['attainment'] is True
    boundary['truth'][32]=list(range(9))+list(range(100,191)); cell[32]['returned_hits']=9
    assert worker.reduce_cell(cell,boundary,config)['attainment'] is False
    # A real capacity drop needs six simultaneous owners and stops higher rates.
    dropped=copy.deepcopy(rows)
    until=dropped[6]['scheduled_ns']+1000000
    for q in range(6):
        dropped[q]['terminal_ns']=until; dropped[q]['port']=18080+q
        dropped[q]['native_header']['listen']=f'127.0.0.1:{18080+q}'
        dropped[q]['native_server_log']=json.dumps(dropped[q]['native_header'])+'\n'
    at=dropped[6]['scheduled_ns']
    dropped[6]=dict(worker.aborted_rows(0,rows[0]['scheduled_ns'],{})[6],outcome='capacity_drop',
        dispatched_ns=at,terminal_ns=at)
    stopped=dict(rate_index=0,reason='rate attainment failed',observed_ns=rows[0]['cell_receipt']['terminal_ns'])
    for index in range(1,6):
        receipt=copy.deepcopy(rows[index*64]['cell_receipt'])
        receipt.update(cell_started=False,abort_after=stopped)
        dropped[index*64:(index+1)*64]=worker.aborted_rows(index,receipt['epoch_ns'],stopped)
        dropped[index*64]['cell_receipt']=receipt
    stopped_result=worker.reduce_records(dropped,evidence,config)
    assert stopped_result['execution_gate_passed'] and not stopped_result['offered_gate_passed']
    assert stopped_result['cells'][0]['capacity_drops']==1 and stopped_result['cells'][0]['latency_ms']['all_offers']['p99']=='UNBOUNDED'
    assert all(c['full_span_ns']=='UNMEASURED' for c in stopped_result['cells'][1:])
    for kind in ('authority','hits','resource','cleanup','port','transport','startup','request'):
        bad=copy.deepcopy(rows[:64])
        if kind=='authority': bad[0]['expected_authority']={}
        elif kind=='hits': bad[0]['returned_hits']=9
        elif kind=='resource': bad[0]['cell_receipt']['cgroup_after']['memory.max']='1024'
        elif kind=='cleanup': bad[0]['native_close']['process_group_closed']=False
        elif kind=='port': bad[0]['terminal_ns']=bad[1]['terminal_ns']
        elif kind=='transport': bad[0]['accounting']['imds_token_PUTs']=2
        elif kind=='startup': bad[0]['native_header']['remote_open_stats']['metadata'][6]['name']='router/manifest.json'
        else: bad[0]['request_sha256']='0'*64
        rejects(lambda:worker.reduce_cell(bad,evidence,config))
    with tempfile.TemporaryDirectory() as directory:
        tmp=Path(directory); output=tmp/'screen'; output.mkdir()
        proof=dict(config_sha256='b'*64,code_identity_sha256='c'*64,refs_identity_sha256='d'*64,binary_sha256={'http':'e'*64})
        config.update(bucket='mock',region='mock',measurement_prefix=PREFIX+'a0001')
        uploads=[]; uploaded=set()
        marker=worker.close_cell(output,rows[:64],result['cells'][0],proof,
            lambda m,p:checkpoint(m,p,config,proof,output,uploaded,lambda cmd:uploads.append(cmd[cmd.index('--key')+1])))
        assert uploads==[PREFIX+'a0001/cells/rate0-records.jsonl',PREFIX+'a0001/cells/rate0-summary.json']
        paths=dict(records=output/'rate0-records.jsonl',summary=output/'rate0-summary.json')
        rejects(lambda:checkpoint(marker,paths,config,proof,output,uploaded,lambda cmd:None))
        rejects(lambda:worker.close_cell(output,rows[:64],result['cells'][0],proof,None))
        # Pending/missing config fails before output or subprocess effects.
        repo=tmp/'repo'; target=repo/CONFIG; target.parent.mkdir(parents=True)
        target.write_bytes(worker.encoded(dict(authority_pending=True)))
        with patch.object(worker.subprocess,'Popen',side_effect=AssertionError('unexpected process')):
            rejects(lambda:worker.main(target,worker.artifact(target)['sha256'],repo,tmp/'must-not-exist'))
        assert not (tmp/'must-not-exist').exists()
        # Real closed a0004 publication: valid 66 events, failed HTTP authority.
        here=Path(__file__).resolve().parents[1]
        closed=here/native.ROOT/'a0004'
        terminal=json.loads((closed/'aws-terminal.json').read_bytes())
        closeout=json.loads((closed/'aws-closeout.json').read_bytes())
        assert closeout['state']=='terminated' and terminal['status']=='failed'
        assert terminal['exit_code']==terminal['original_exit_code']==1
        assert worker.artifact(closed/'screen/publication-reference.jsonl')==terminal['artifacts']['screen/publication-reference.jsonl']
        frozen=json.loads((closed/'screen/config.json').read_bytes())
        retained=dict(prices=dict(path='fixture-price-provenance'),cold_config=dict(path=str((closed/'screen/config.json').relative_to(here)),
            **terminal['artifacts']['screen/config.json']),
            cold_run=dict(files={n:dict(path=str((closed/n).relative_to(here)),**p)
                for n,p in terminal['artifacts'].items()}))
        real_evidence=worker.inputs(here,retained)
        assert real_evidence['publication']['validation']['validated_queries']==64
        assert len(real_evidence['requests'])==len(real_evidence['references'])==64
        model=native.native_budget_model(real_evidence['arm']['metadata_files'])
        assert model['modeled_remote_payload_bytes']==870733975<worker.NATIVE
        assert model['server_query_slots']==4
        # Qualification still refuses this failed closed run before offered effects.
        for name in CODE:
            path=repo/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes((here/name).read_bytes())
        candidate=dict(worker.FIXED,authority_pending=False,
            code_sha256={n:worker.artifact(repo/n)['sha256'] for n in CODE},
            cold_config=dict(path=str(native.CONFIG),**worker.artifact(here/native.CONFIG)),
            cold_run=dict(directory=str(closed.relative_to(here)),files={n:dict(
                path=str((closed/n).relative_to(here)),**worker.artifact(closed/n)) for n in worker.COLD_ROSTER}),
            measurement_prefix=PREFIX+'a0001',prices={})
        for field,value in (('native_memory_bytes',native.PAYLOAD),('server_query_slots',1),('native_budget_model',{})):
            target.write_bytes(worker.encoded(dict(candidate,**{field:value})))
            with patch.object(worker,'read') as reads,patch.object(worker.subprocess,'Popen') as processes:
                rejects(lambda:worker.main(target,worker.artifact(target)['sha256'],repo,tmp/'undersized'))
                reads.assert_not_called(); processes.assert_not_called()
            assert not (tmp/'undersized').exists()
        target.write_bytes(worker.encoded(candidate))
        with patch.object(worker,'read',side_effect=lambda _,p:native.read(here,p)), \
            patch.object(native,'qualify',return_value=(frozen,{})), \
            patch.object(worker.cold_spot,'replay') as cold_replay,patch.object(worker.subprocess,'Popen') as processes:
            rejects(lambda:worker.main(target,worker.artifact(target)['sha256'],repo,tmp/'failed-cold'))
            cold_replay.assert_not_called(); processes.assert_not_called()
        assert not (tmp/'failed-cold').exists()
        assert json.loads((closed/'verification.json').read_bytes())['status']=='EXECUTION_FAILED'
        # Exercise main's exact files, scoped stop hook, callback and offline replay.
        binary=b'synthetic-http'
        runtime_evidence=dict(evidence,binary=dict(path='http',bytes=len(binary),sha256=worker.sha(binary)),identities={'mock':'sealed'})
        runtime_config=dict(config,prices=dict(path='prices',bytes=2,sha256=worker.sha(b'{}')),
            cold_run={'files':{'screen/publication.json':dict(path='publication',bytes=2,sha256=worker.sha(b'{}'))}})
        target.write_bytes(worker.encoded(runtime_config))
        runproof={n:'1'*64 for n in TERMINAL_IDENTITIES if n not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256')}
        runproof.update(proof,config_sha256=worker.artifact(target)['sha256'],config_path=str(CONFIG),
            measurement_prefix=PREFIX+'a0001',namespace_prefix='retained/native')
        campaign_proof=dict(runproof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
            awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
        virtual=[10**12]; scheduled=[0]; uploaded=set(); checkpoints=[]
        def scheduler(call, rate, **kwargs):
            assert worker.cold.stop is native.close_native
            index=scheduled[0]; scheduled[0]+=1
            batch=copy.deepcopy(rows[index*64:(index+1)*64]); receipt=batch[0].pop('cell_receipt')
            assert rate==worker.RATES[index]
            virtual[0]=receipt['terminal_ns']
            return batch,receipt['epoch_ns'],receipt['terminal_ns'],None
        environment=dict.fromkeys(worker.THREAD_ENV,'2')
        environment.update(AWS_MAX_ATTEMPTS='1',BORSUK_OFFERED_SOURCE_COMMIT='a'*40,BORSUK_OFFERED_ARCHIVE_SHA256='b'*64)
        collected=tmp/'collected'; collected.mkdir(); screen=collected/'screen'
        module=sys.modules[__name__]
        with patch.object(worker,'qualify',return_value=(runtime_config,runproof)), \
            patch.object(worker,'inputs',return_value=runtime_evidence),patch.object(worker,'read',return_value=binary), \
            patch.object(native,'capture',side_effect=lambda:copy.deepcopy(snapshot)),patch.object(ids,'tool_versions',return_value={'mock':True}), \
            patch.object(worker.os,'sched_getaffinity',return_value={4,5}),patch.dict(os.environ,environment), \
            patch.object(worker.time,'monotonic_ns',side_effect=lambda:virtual[0]),patch.object(worker.offered,'schedule_offers',side_effect=scheduler):
            actual=worker.main(target,runproof['config_sha256'],repo,screen,
                on_cell_closed=lambda m,p:checkpoint(m,p,runtime_config,runproof,screen,uploaded,
                    lambda cmd:checkpoints.append(cmd[cmd.index('--key')+1])))
        assert actual==result and worker.cold.stop is original_stop and len(checkpoints)==12
        assert set(p.name for p in screen.iterdir())==set(worker.OUTPUTS)
        source=dict(source_commit='a'*40,source_archive_sha256='b'*64)
        nodes={'0':{'instance_id':'i-mock'}}
        launch=dict(source,instance_id='i-mock',nodes=nodes,prefix=PREFIX+'a0001')
        terminal=dict(source,schema=SCHEMA,instance_id='i-mock',phase='complete',status='complete',exit_code=0,original_exit_code=0,
            **{n:campaign_proof[n] for n in TERMINAL_IDENTITIES})
        for name in ('test-resources.txt','run-closed.log'): worker.write(collected/name,b'mock\n')
        terminal['artifacts']={n:worker.artifact(collected/n) for n in ARTIFACTS}
        for name,value in (('aws-launch.json',launch),('aws-closeout.json',dict(state='terminated',nodes=nodes)),
            ('aws-reservation.json',dict(source,schema=SCHEMA,qualification=campaign_proof,wall_seconds=WALL,instance_type=INSTANCE_TYPE)),
            ('aws-terminal.json',terminal)):
            worker.write(collected/name,value)
        with patch.object(module,'qualify',return_value=campaign_proof),patch.object(worker,'inputs',return_value=runtime_evidence):
            assert replay(collected)['offered_gate_passed'] is True
            (collected/'aws-closeout.json').write_bytes(worker.encoded(dict(state='running',nodes=nodes)))
            rejects(lambda:replay(collected))
    # Load only a mocked SDK to exercise the real shared lifecycle and shell.
    from types import ModuleType
    sdk,botocore,exceptions=(ModuleType(n) for n in ('boto3','botocore','botocore.exceptions'))
    class SDKError(Exception):
        def __init__(self,**kwargs): super().__init__('synthetic SDK')
    for n in ('ClientError','EndpointConnectionError','ReadTimeoutError'): setattr(exceptions,n,SDKError)
    sdk.Session=Mock(side_effect=AssertionError('cloud forbidden')); botocore.exceptions=exceptions
    with patch.dict(sys.modules,{'boto3':sdk,'botocore':botocore,'botocore.exceptions':exceptions}):
        lifecycle,_=ids.lifecycle()
        frozen=dict.fromkeys(TERMINAL_IDENTITIES,'1'*64)
        frozen.update(config_path=str(CONFIG),campaign_schema=SCHEMA,awscli_version=AWSCLI_VERSION,
            awscli_sha256=AWSCLI_SHA256,namespace_prefix='retained/native',measurement_prefix=PREFIX+'a0001')
        body=user_data('0'*40,'1'*64,'source/key',PREFIX+'a0001',frozen)
        install=next(line for line in body.splitlines() if ' -m pip install ' in line)
        assert install.split('--no-deps ',1)[1].split()==[
            'numpy==2.3.3','pyarrow==24.0.0','boto3==1.40.72','botocore==1.40.72',
            's3transfer==0.14.0','jmespath==1.0.1','python-dateutil==2.9.0','six==1.17.0','urllib3==2.6.3'], 'bootstrap dependency closure'
        assert '--only-binary=:all:' in install
        assert body.index('launch.ids.lifecycle()') < body.index('systemd-run --unit=semantic-1m-offered')
        assert all(n in body for n in ('MemoryMax=8G','MemorySwapMax=0','CPUQuota=200%','TasksMax=512',
            'RuntimeMaxSec=3000','--on-active=3600s','taskset -c 4-5','--worker','IOAccounting=yes'))
        command=body.split('systemd-run --unit=semantic-1m-offered',1)[1].split('\nfor name',1)[0]
        argv=subprocess.check_output(['bash','-c','systemd-run() { printf "%s\\n" "$@"; }; root=/mock; systemd-run --unit=semantic-1m-offered'+command],text=True).splitlines()
        assert argv[-6]=='--worker' and argv[-1]==PREFIX+'a0001',argv
        with patch.object(sys.modules[__name__],'qualify',side_effect=AssertionError('pending')):
            rejects(lambda:main('a0001'))
        sdk.Session.assert_not_called()
        with panel.contextlib.redirect_stdout(worker.io.StringIO()): lifecycle.self_check(lifecycle_only=True)
    # Imports plus the inherited cold proof's explicitly pinned helper closure.
    import ast
    here=Path(__file__).resolve().parents[1]; seen=set()
    def closure(name):
        if name in seen: return
        seen.add(name)
        for node in ast.walk(ast.parse((here/name).read_bytes())):
            targets=[]
            if isinstance(node,ast.ImportFrom):
                targets=[a.name for a in node.names] if node.module=='scripts' else ([node.module[8:]] if node.module and node.module.startswith('scripts.') else [])
            elif isinstance(node,ast.Import): targets=[a.name[8:] for a in node.names if a.name.startswith('scripts.')]
            for target in targets:
                path='scripts/'+target.replace('.','/')+'.py'
                if (here/path).is_file(): closure(path)
    closure('scripts/launch_native_semantic_1m_offered_spot.py')
    assert seen | set(native.CODE) == set(CODE), 'import/evidence code closure drift'
    assert time.monotonic()-start < 55
    assert worker.resource.getrusage(worker.resource.RUSAGE_SELF).ru_maxrss*1024 <= 200*1024**2
    print('PASS authenticated closed a0004 66-event inputs (FAIL authority rejected), four-slot 1GiB/static budget, synthetic cold-call, six ports/seventh drop, 384 replay, 607/608, failure negatives, runtime/closed replay, records-before-marker, shell and shared ACK/fsync/termination; no native/cloud/data')


if __name__ == '__main__':
    args = sys.argv[1:]
    if args == ['--self-check']:
        self_check()
    elif len(args) == 2 and args[0] == '--replay':
        print(json.dumps(replay(args[1]), sort_keys=True))
    elif args and args[0] == '--worker':
        remote_worker(args[1:])
    else:
        assert len(args) == 1, 'aNNNN | --self-check | --replay DIR'
        with open('/tmp/borsuk-semantic-1m-offered-launch.lock','a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(args[0])
