"""Root-frozen FIRST1M cold gate: aNNNN | --self-check | --replay DIRECTORY."""
from pathlib import Path
import fcntl
import json
import os
import re
import subprocess
import sys
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')

from scripts import run_native_semantic_1m_cold as worker

panel,ids,shared_quality = worker.panel,worker.ids,worker.quality_spot
ROOT,CONFIG = worker.ROOT,worker.CONFIG
NAME = ''
SCHEMA = 'borsuk-semantic-1m-cold-spot-v1'
PREFIX = 'research/semantic-router/20261001/fresh1m-cold-'
TOKEN_PREFIX,TAG = 'semantic-1m-cold-','borsuk-semantic-1m-cold'
WALL = 9000
INSTANCE_TYPE,IMAGE_ID = panel.INSTANCE_TYPE,panel.IMAGE_ID
ROOT_DEVICE_NAME,SUBNET = panel.ROOT_DEVICE_NAME,panel.SUBNET
SPOT_MAX_USD_PER_HOUR,COMPUTE_CAP = .50,1.25
AWSCLI_VERSION,AWSCLI_SHA256 = panel.AWSCLI_VERSION,panel.AWSCLI_SHA256
CODE = worker.CODE
ARTIFACTS = ('test-resources.txt','run-closed.log',*('screen/'+n for n in worker.OUTPUTS))
TERMINAL_IDENTITIES = (*shared_quality.TERMINAL_IDENTITIES,'quality_reference_sha256','namespace_prefix')


def qualify(base=Path('.')):
    base=Path(base).resolve()
    _,proof=worker.qualify(base/CONFIG,worker.artifact(base/CONFIG)['sha256'],base)
    return dict(proof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
        awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof=qualify(base)
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=base,text=True).strip(), 'dirty source'
    return proof


def user_data(commit,archive_sha,archive_key,prefix,qualification):
    assert qualification['namespace_prefix']==prefix+'/native', 'campaign namespace binding'
    # Reuse the checked shared bootstrap/trap; replace only the bounded service command.
    with patch.multiple(shared_quality,CONFIG=CONFIG,SCHEMA=SCHEMA,PREFIX=PREFIX,
        ARTIFACTS=ARTIFACTS,TERMINAL_IDENTITIES=TERMINAL_IDENTITIES):
        body=shared_quality.user_data(commit,archive_sha,archive_key,prefix,qualification)
    body=body.replace('MemoryMax=1G','MemoryMax=8G -p IOAccounting=yes')
    body=body.replace('BORSUK_QUALITY_','BORSUK_COLD_')
    body=body.replace('semantic-1m-quality','semantic-1m-cold')
    body=body.replace('scripts.run_native_semantic_1m_quality','scripts.run_native_semantic_1m_cold')
    body=body.replace('if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi',
        'if [ "$name" = screen/failures.jsonl ]; then test -f "$name"; elif [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi')
    body=body.replace('"$root/venv/bin/python" -m scripts.run_native_semantic_1m_cold',
        'taskset -c 4-5 "$root/venv/bin/python" -m scripts.run_native_semantic_1m_cold')
    subprocess.run(['bash','-n'],input=body,text=True,check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0],'<terminal>','exec')
    assert len(body.encode())<16384 and 'MemoryMax=8G' in body
    return body


def poll(ec2,s3,prefix,instance_id,started):
    return panel.poll(ec2,s3,prefix,instance_id,started)


def replay(out):
    """Read only terminated, authenticated terminal artifacts; never reissue queries."""
    out=Path(out)
    launch,closed,reservation,terminal=(json.loads((out/n).read_bytes()) for n in (
        'aws-launch.json','aws-closeout.json','aws-reservation.json','aws-terminal.json'))
    assert closed['state']=='terminated' and closed['nodes']==launch['nodes']
    assert terminal['instance_id']==launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema']==reservation['schema']==SCHEMA
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}',launch['prefix'])
    for k in ('source_commit','source_archive_sha256'):
        assert terminal[k]==reservation[k]==launch[k]
    proof=reservation['qualification']
    assert proof==qualify(Path(__file__).resolve().parents[1]), 'frozen local authority'
    assert proof['namespace_prefix']==launch['prefix']+'/native'
    for k in TERMINAL_IDENTITIES: assert terminal[k]==proof[k], 'terminal authority: '+k
    assert set(terminal['artifacts'])<=set(ARTIFACTS)
    for n,p in terminal['artifacts'].items(): assert worker.artifact(out/n)==p, 'terminal body: '+n
    complete=terminal['phase']=='complete' and terminal['exit_code']==0
    assert terminal['status']==('complete' if complete else 'failed')
    if not complete:
        if 'screen/records.jsonl' in terminal['artifacts']:
            rows=[json.loads(line) for line in (out/'screen/records.jsonl').read_bytes().splitlines()]
            assert [r['query_ordinal'] for r in rows]==list(range(64))
        return dict(executed=False,execution_gate_passed=False)
    assert terminal['original_exit_code']==0 and set(terminal['artifacts'])==set(ARTIFACTS)
    screen=out/'screen'
    assert worker.artifact(screen/'config.json')['sha256']==proof['config_sha256']
    source={k:terminal[k] for k in ('source_commit','source_archive_sha256')}
    assert json.loads((screen/'source-qualification.json').read_bytes())==dict(
        (k,v) for k,v in dict(proof,**source).items()
        if k not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256'))
    counters=json.loads((screen/'cold-cgroup.json').read_bytes()); worker.validate_cgroup(counters)
    report,clean,summary=(json.loads((screen/n).read_bytes()) for n in ('resources.json','cleanup.json','summary.json'))
    assert clean['valid'] is clean['scratch_removed'] is clean['observer_stopped'] is True
    assert clean['build_invocations']==clean['publication_invocations']==1 and clean['cold_invocations']==64
    assert report['build_invocations']==report['publication_invocations']==1 and report['cold_invocations']==64
    assert 0<=report['wall_seconds']<=7200
    for n,limit in (('preparation',1800),('build',3600),('publication',1800),('cold',900)):
        assert 0<=report['stages'][n]['wall_seconds']<=limit
    assert report['stages']['build']['exit_status']==report['stages']['publication']['exit_status']==0
    ordinal=json.loads((screen/'sq8-ordinal-check.json').read_bytes())
    assert ordinal['rows_checked']==1_000_000 and ordinal['record_bytes']==780
    assert ordinal['id_matches_order'] is ordinal['complete_bijection'] is True
    repo=Path(__file__).resolve().parents[1]
    config=json.loads((screen/'config.json').read_bytes())
    assert report['phase_admission']==dict(builder_memory_bytes=worker.PAYLOAD,
        publisher_memory_bytes=config['publisher_memory_bytes'],native_memory_bytes=worker.PAYLOAD,
        memory_bytes=worker.MEMORY,swap_bytes=0), 'phase memory admission'
    qconfig=json.loads(worker.read(repo,config['quality_config']))
    original=worker.read(repo,qconfig['panel']['files']['screen/requests.jsonl'])
    derivative=worker.publisher_requests(original)
    assert (screen/'publisher-requests.jsonl').read_bytes()==derivative
    expected_derivative=dict(original=worker.identity(qconfig['panel']['files']['screen/requests.jsonl']),
        derivative=worker.artifact(screen/'publisher-requests.jsonl'),only_ordinal_key_changed=True,f32_bits_equal=True)
    assert json.loads((screen/'request-derivative.json').read_bytes())==expected_derivative
    truth_body=worker.read(repo,qconfig['panel']['files']['screen/truth.i64'])
    truth=[list(worker.struct.unpack_from('<100q',truth_body,q*800)) for q in range(64)]
    refs=worker.source_reference(worker.read(repo,config['quality_run']['files']['screen/records.jsonl']))
    publication=json.loads((screen/'publication.json').read_bytes()); arm=publication['arm']
    worker.validate_roster(arm)
    assert arm['indexes']=={'10':config['namespace_prefix']}
    assert publication['head']['root_sha256']==arm['authority']['root_sha256']
    head=worker.base64.b64decode(publication['head_body_base64'],validate=True)
    assert json.loads(head)==publication['head'] and dict(bytes=len(head),sha256=worker.sha(head))==arm['head_file']
    remote=json.loads((screen/'remote-sq8.json').read_bytes())
    assert publication['remote_sq8']==remote and remote['backend']=='AmazonS3' and remote['remote_readback_authenticated'] is True
    assert remote['key']==config['namespace_prefix']+'/objects/'+qconfig['inputs']['sq8']['sha256']
    assert worker.identity(remote)==worker.identity(qconfig['inputs']['sq8'])
    manifest=publication['manifest']
    assert manifest['sq8_object_key']==remote['key'] and manifest['sq8_etag']==remote['etag']
    builder=json.loads((screen/'builder-config.json').read_bytes())
    assert builder['sq8_object_key']==remote['key'] and builder['sq8_etag']==remote['etag']
    assert publication['reference']==worker.artifact(screen/'publication-reference.jsonl')
    assert publication['validation']==worker.publication_reference(screen/'publication-reference.jsonl',arm,refs)
    records=[json.loads(line) for line in (screen/'records.jsonl').read_bytes().splitlines()]
    assert (screen/'records.jsonl').read_bytes()==b''.join(worker.encoded(r)+b'\n' for r in records), 'canonical closed ledger'
    requests=[worker.http_request(json.loads(line)['query'],arm['authority']) for line in derivative.splitlines()]
    expected=[dict(r,ids=r['ids'][:10]) for r in refs]
    start_ns=summary['serial_start_ns']; end_ns=summary['serial_end_ns']
    assert summary==worker.reduce_records(records,arm,requests,expected,truth,start_ns,end_ns)
    assert summary['execution_gate_passed'] is True
    assert (screen/'failures.jsonl').read_bytes()==b''
    return dict(executed=True,execution_gate_passed=True,quality_gate_passed=summary['quality_gate_passed'],
        context_p90_attained=summary['context_p90_attained'])


def collect(s3,prefix,out,instance_id,commit,digest):
    launch,closed=(json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state']=='terminated' and closed['nodes']==launch['nodes']
    assert launch['prefix']==prefix and launch['instance_id']==instance_id
    with patch.multiple(ids,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS,replay=replay):
        return ids.collect(s3,prefix,out,instance_id,commit,digest)


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}',attempt)
    before=Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()
        lifecycle,_=ids.lifecycle()
        return lifecycle.main(attempt,campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    import copy
    import io
    import tempfile
    from types import ModuleType
    from unittest.mock import Mock
    module=sys.modules[__name__]
    here=Path(__file__).resolve().parents[1]
    def rejects(action):
        try: action()
        except (AssertionError,ValueError,KeyError,FileNotFoundError,FileExistsError,RuntimeError): return
        raise AssertionError('invalid evidence accepted')
    source = b'{"ordinal":0,"query":[1.0,-0.0]}\n'
    derivative = worker.publisher_requests(source, rows=1, dimensions=2)
    assert derivative == b'{"query":[1.0,-0.0],"query_ordinal":0}\n'
    assert worker.scientific_gate(607)['quality_gate_passed'] is False
    assert worker.scientific_gate(608)['quality_gate_passed'] is True
    for body in (b'{"ordinal":1,"query":[1,0]}',b'{"query_ordinal":0,"query":[1,0]}',
                 b'{"ordinal":0,"query":[1]}',b'{"ordinal":false,"query":[1,0]}',
                 b'{"ordinal":0,"query":[1,NaN]}'):
        rejects(lambda:worker.publisher_requests(body,rows=1,dimensions=2))
    assert worker.http_request([1,-0.0],dict(root_sha256='a'*64,generation=1,control_epoch=1))==worker.encoded(
        dict(query=[1,-0.0],k=10,root_sha256='a'*64,generation=1,control_epoch=1))
    with tempfile.TemporaryDirectory() as directory:
        tmp=Path(directory)
        # Existing source/binary/config/ordinal/run_process tests remain shared.
        with worker.panel.contextlib.redirect_stdout(io.StringIO()): shared_quality.self_check()
        local=tmp/'sq8'; local.write_bytes(b'packed')
        head=dict(ContentLength=6,ETag='"remote-s3-etag"')
        assert worker.sq8_authority(head,local,worker.artifact(local))==head['ETag']
        rejects(lambda:worker.sq8_authority(dict(head,ETag=worker.quality.local_head(local)['etag']),local,worker.artifact(local)))
        rejects(lambda:worker.sq8_authority(dict(head,ContentLength=5),local,worker.artifact(local)))
        proof=dict(qualified=True,authority_pending=False,production_library_unchanged=True,
            current_whole_tree_full_execution=False,source_file_count=399,native_source_sha256={'a.rs':'a'*64},
            source_identity_sha256='b'*64,original_full_source_identity_sha256='c'*64,
            binary_bytes=6,binary_sha256=worker.sha(b'packed'),release_status=0,clippy_status=0,
            workspace_test_compilation_status=0,oom_kills=0,swap_peak_bytes=0)
        qproof=dict(native_source_sha256=proof['native_source_sha256'],source_identity_sha256='b'*64,
            original_source_identity_sha256='c'*64)
        with patch.object(worker,'read',return_value=worker.encoded(proof)):
            worker.native_proof(tmp,{},worker.artifact(local),qproof)
            rejects(lambda:worker.native_proof(tmp,{},dict(worker.artifact(local),sha256='0'*64),qproof))
            rejects(lambda:worker.native_proof(tmp,{},worker.artifact(local),dict(qproof,native_source_sha256={})))
        for field,value in (('authority_pending',True),('release_status',None),('clippy_status',False),
            ('workspace_test_compilation_status',101),('oom_kills',1),('swap_peak_bytes',1)):
            with patch.object(worker,'read',return_value=worker.encoded(dict(proof,**{field:value}))):
                rejects(lambda:worker.native_proof(tmp,{},worker.artifact(local),qproof))
        # Dynamic v8 startup geometry, independent of the old 100k/v7 checker.
        sizes=[2000,1000,3907*32,500,3072,1_000_000,1024,125000]
        arm=dict(discovery='semantic',authority=dict(root_sha256='a'*64,generation=1,control_epoch=1),
            indexes={'10':PREFIX+'a0001/native'},metadata_files=dict(zip(worker.STARTUP,sizes)),
            metadata_sha256={n:'a'*64 for n in worker.STARTUP},head_file=dict(bytes=200,sha256='a'*64),
            leaf_object=dict(bytes=31_250*1540,sha256='a'*64))
        worker.validate_roster(json.loads(worker.encoded(arm)))
        metadata=[]
        for i,(name,size) in enumerate(zip(worker.STARTUP,sizes)):
            metadata.append(dict(name=name,bytes=size,chunks=1,metadata_wave=0 if i==0 else (i-1)//4+1,
                metadata_wave_wall_ns=10,logical_head_requests=int(name not in worker.EXACT),logical_get_requests=1,
                head_wall_ns=int(name not in worker.EXACT),get_wall_ns=2,stream_wall_ns=3,write_wall_ns=1,
                payload_buffer_bound_bytes=size))
        startup=dict(metadata=metadata,staging_wall_ns=30,decode_wall_ns=2,source_head_requests=1,
            router_head_requests=1,source_head_wall_ns=2,router_head_wall_ns=2)
        opened=worker.validate_startup(startup,arm,40)
        assert opened['metadata_objects']==8 and opened['logical_metadata_head_requests']==3
        for kind in ('v7','payload','length','hash','wave','head','buffer','order'):
            bad_arm=copy.deepcopy(arm); bad=copy.deepcopy(startup)
            if kind=='v7': bad_arm['metadata_files']['router/manifest.json']=bad_arm['metadata_files'].pop('router/root.bin')
            elif kind=='payload': bad_arm['metadata_files']['plane/records.bin']=100
            elif kind=='length': bad_arm['metadata_files']['page_digests.bin']=12512
            elif kind=='hash': bad_arm['metadata_sha256']['manifest.json']='0'*64
            elif kind=='wave': bad['metadata'][1]['metadata_wave']=0
            elif kind=='head': bad['metadata'][6]['logical_head_requests']=1
            elif kind=='buffer': bad['metadata'][6]['payload_buffer_bound_bytes']+=1
            else: bad['metadata'][1],bad['metadata'][2]=bad['metadata'][2],bad['metadata'][1]
            rejects(lambda:worker.validate_startup(bad,bad_arm,40))
        def transport_report(methods,payload):
            return dict(schema='borsuk-native-transport-v1',scope='process_all_native_s3_readers',per_query_delta=False,
                attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests',method_order=worker.stats.METHODS,
                status_counts_format='[http_status,count] nonzero entries',
                payload_measurement='consumed response data frames, including unauthenticated payload',
                unknown=worker.stats.UNKNOWN,dropped_error_body_consumed_bytes=0,
                totals=dict(attempts=sum(methods),method_counts=methods,status_counts=[[200,sum(methods)]],
                    transport_failures=0,stream_failures=0,dropped_error_bodies=0,consumed_payload_bytes=payload))
        ready=dict(phase='ready',authority=arm['authority'],listen='127.0.0.1:8080',remote_open_stats=startup,
            remote_open_wall_ns=40,head_read_wall_ns=10,
            transport=transport_report([12,5,1]+[0]*7,sum(sizes)+200+2000+300))
        expected=dict(query_ordinal=0,ids=list(range(10)),ranges=[[0,199680]],planned_bytes=199680,
            submitted_gets=1,verified_bytes=199680,failed_gets=0,source_submitted_gets=1,
            source_verified_bytes=100,source_failed_gets=0,router_submitted_gets=8,router_verified_bytes=500,router_failed_gets=0)
        stages={n:dict(start_ns=i*10+1,end_ns=i*10+8) for i,n in enumerate(worker.stats.STAGES)}
        stages['leaf_peak_inflight']=8
        response=dict(expected,authority=arm['authority'],query_stages=stages,native_wall_ns=40,
            transport=transport_report([22,5,1]+[0]*7,ready['transport']['totals']['consumed_payload_bytes']+200280))
        worker.validate_query(response,arm,expected,list(range(100)))
        accounting=worker.transport(ready,response,arm)
        assert accounting['imds_token_PUTs']==1 and accounting['imds_credential_GETs']==2
        assert accounting['inferred_credential_consumed_bytes']==300
        for kind in ('GET','HEAD','PUT','credential','root','queryGET','payload','counter','status'):
            h,r=copy.deepcopy(ready),copy.deepcopy(response)
            if kind in ('GET','HEAD','PUT'):
                index={'GET':0,'HEAD':1,'PUT':2}[kind]; m=h['transport']['totals']['method_counts']; m[index]+=1
                h['transport']['totals']['attempts']+=1; h['transport']['totals']['status_counts'][0][1]+=1
            elif kind=='credential': h['transport']['totals']['consumed_payload_bytes']-=300
            elif kind=='root': h['authority']['root_sha256']='0'*64
            elif kind=='queryGET':
                r['transport']['totals']['method_counts'][0]+=1; r['transport']['totals']['attempts']+=1
                r['transport']['totals']['status_counts'][0][1]+=1
            elif kind=='payload': r['transport']['totals']['consumed_payload_bytes']+=1
            elif kind=='counter': r['submitted_gets']+=1
            else: r['transport']['totals']['status_counts'][0][0]=500
            rejects(lambda:worker.transport(h,r,arm))
        time_log='User time (seconds): 0.01\nSystem time (seconds): 0.01\nMaximum resident set size (kbytes): 42\n'
        # The real cold_call executes against a mocked socket/native process.
        process=Mock(pid=1234,returncode=143); process.poll.return_value=None
        def spawn(args,**kwargs):
            assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES']==str(512 * 1024**2)
            assert args[args.index('prlimit')+1]=='--as=4294967296:4294967296'
            kwargs['stdout'].write(json.dumps(ready)+'\n'); kwargs['stdout'].flush()
            Path(args[3]).write_text(time_log)
            return process
        def killpg(pid,sig):
            assert pid==1234
            if sig==0: raise ProcessLookupError()
        cpu_before=dict(pid=1235,start_ticks=100,user_ticks=1,system_ticks=1,address_space_limit_bytes=4*1024**3,cpu_affinity=[0,1,2,3])
        cpu_after=dict(cpu_before,user_ticks=2)
        request=worker.http_request([1]*768,arm['authority'])
        with patch.object(worker.subprocess,'Popen',side_effect=spawn),patch.object(worker.cold.http.client,'HTTPConnection') as connection,\
             patch.object(worker.cold,'post',return_value=(200,worker.encoded(response))),\
             patch.object(worker,'native_cpu',side_effect=[cpu_before,cpu_after]),\
             patch.object(worker,'cold_stop',return_value=dict(intentional_stop=True,returncode=143)),\
             patch.object(worker.os,'killpg',side_effect=killpg):
            row=worker.measured_call('synthetic',dict(bucket='mock',region='mock'),arm,request,expected,list(range(100)))
        assert row['outcome']=='success',row
        assert row['completed_ns']==row['first_wire_completed_ns'] and row['native_close']['process_group_closed']
        assert row['query_cpu']['ticks']==1 and row['http_attempts']==1
        process.poll.return_value=124
        with patch.object(worker.subprocess,'Popen',side_effect=spawn),\
             patch.object(worker,'cold_stop',return_value=dict(intentional_stop=False,returncode=124)),\
             patch.object(worker.os,'killpg',side_effect=killpg):
            startup_failure=worker.measured_call('synthetic',dict(bucket='mock',region='mock'),arm,request,expected,list(range(100)))
        assert startup_failure['outcome']=='failed' and startup_failure['http_attempts']==0
        assert startup_failure['native_process_started'] and startup_failure['native_server_log']
        assert startup_failure['native_close']['process_group_closed']
        process.poll.return_value=None
        with patch.object(worker,'cold_stop',return_value=dict(intentional_stop=False,returncode=124)),\
             patch.object(worker.os,'killpg',side_effect=killpg) as killed:
            assert worker.close_native(process)['process_group_closed']
        assert [c.args for c in killed.call_args_list]==[(1234,worker.signal.SIGTERM),(1234,worker.signal.SIGKILL),(1234,0)]
        # Independent reduction sees exact 64 rows; incomplete or changed ledgers fail.
        records=[]; cursor=0
        for q in range(64):
            r=copy.deepcopy(row); r.update(query_ordinal=q,started_ns=cursor,completed_ns=cursor+1000000,
                terminal_ns=cursor+2000000,cold_start_to_first_http_response_ns=1000000)
            records.append(r); cursor+=3000000
        requests=[request]*64; refs=[dict(expected,query_ordinal=q) for q in range(64)]; truth=[list(range(100))]*64
        reduced=worker.reduce_records(records,arm,requests,refs,truth,0,cursor)
        assert reduced['execution_gate_passed'] and reduced['returned_hits']==640
        assert reduced['context_p90_attained'] and reduced['sustainable_qps']=='UNMEASURED'
        for kind in ('ledger','cleanup','resource','raw','identity','timing'):
            bad=copy.deepcopy(records)
            if kind=='ledger': bad.pop()
            elif kind=='cleanup': bad[1]['native_close']['process_group_closed']=False
            elif kind=='resource': bad[1]['resources']['rss_peak_bytes']=worker.PAYLOAD+1
            elif kind=='raw': bad[1]['raw_response_base64']=worker.base64.b64encode(b'{}').decode()
            elif kind=='identity': bad[1]['request_sha256']='0'*64
            else: bad[1]['started_ns']=0
            rejects(lambda:worker.reduce_records(bad,arm,requests,refs,truth,0,cursor))
        failed=dict(records[0],outcome='failed',error_type='RuntimeError',error='synthetic failure')
        broken=[failed,*worker.aborted_rows('fatal call',1)]
        assert not worker.reduce_records(broken,arm,requests,refs,truth,0,cursor)['execution_gate_passed']
        # Config pending and identity rejects precede all source/cloud access.
        repo=tmp/'repo'; repo.mkdir()
        for name in CODE:
            path=repo/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes((here/name).read_bytes())
        config=dict(worker.FIXED,authority_pending=True,code_sha256={n:worker.artifact(repo/n)['sha256'] for n in CODE},
            quality_config={},quality_run={},native_proofs={},binaries={},namespace_prefix=PREFIX+'a0001/native')
        target=repo/CONFIG; target.parent.mkdir(parents=True); target.write_bytes(worker.encoded(config))
        rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        rejects(lambda:worker.qualify(target,'0'*64,repo))
        config['authority_pending']=False
        for field,value in (('rows',100000),('k',100),('memory_bytes',worker.MEMORY+1),
                            ('publisher_memory_bytes',512 * 1024**2)):
            target.write_bytes(worker.encoded(dict(config,**{field:value})))
            with patch.object(worker,'read') as reads,patch.object(worker.quality,'run_process') as processes:
                rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
                reads.assert_not_called(); processes.assert_not_called()
        changed=dict(config,code_sha256=dict(config['code_sha256'],**{CODE[0]:'0'*64}))
        target.write_bytes(worker.encoded(changed)); rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        # Runtime qualification failures still produce the full aborted population and cleanup.
        target.write_bytes(worker.encoded(config))
        out=tmp/'fatal'
        with patch.object(worker,'qualify',side_effect=AssertionError('pending authority')):
            rejects(lambda:worker.main(target,'0'*64,repo,out))
        assert not (out/'scratch').exists()
        saved=[json.loads(line) for line in (out/'records.jsonl').read_bytes().splitlines()]
        assert [r['query_ordinal'] for r in saved]==list(range(64)) and all(r['outcome']=='aborted' for r in saved)
        assert json.loads((out/'cleanup.json').read_bytes())['valid'] is True
        assert not json.loads((out/'summary.json').read_bytes())['execution_gate_passed']
        # Full runtime and closed offline replay, with synthetic transfers/processes.
        request_body=b''.join(worker.encoded(dict(ordinal=q,query=[1.0]*768))+b'\n' for q in range(64))
        frozen_queries=[dict(phase='frozen_query',ordinal=q,returned_ids=list(range(100)),
            ranges=[dict(start=0,end=199680)],sq8_gets=1,sq8_bytes=199680,source_gets=1,source_bytes=100,
            leaf_gets=8,leaf_bytes=500) for q in range(64)]
        quality_records=b''.join(worker.encoded(r)+b'\n' for r in frozen_queries)
        coefficient_body=worker.encoded(dict(rows=1_000_000,dimensions=768,raw_sha256='5'*64,
            sq8_sha256=worker.sha(b'packed'),low=[0]*768,step=[1]*768))
        blobs=dict(parquet=b'parquet',sq8=b'packed',order=b'order',coefficients=coefficient_body,
            builder=b'builder',http=b'http',publisher=b'publisher')
        def object_pointer(name):
            return dict(key='synthetic/'+name,bytes=len(blobs[name]),sha256=worker.sha(blobs[name]))
        def authority_pointer(name,body): return dict(path=name,bytes=len(body),sha256=worker.sha(body))
        requests_pointer=authority_pointer('synthetic/requests.jsonl',request_body)
        records_pointer=authority_pointer('synthetic/records.jsonl',quality_records)
        qconfig=dict(inputs={n:object_pointer('coefficients' if n=='builder' else n) for n in ('parquet','sq8','order','builder')},
            binaries={'builder':object_pointer('builder')},refs={'input_authorities':dict(path='synthetic/raw-authority.json')},
            panel={'files':{'screen/requests.jsonl':requests_pointer}})
        runconfig=dict(config,quality_config=dict(path=str(worker.quality.CONFIG),bytes=1,sha256='6'*64),
            quality_run=dict(files={'screen/records.jsonl':records_pointer}),
            binaries={n:object_pointer(n) for n in ('http','publisher')})
        runproof=dict(config_path=str(CONFIG),config_sha256=worker.sha(worker.encoded(runconfig)),
            code_identity_sha256='1'*64,refs_identity_sha256='2'*64,panel_identity_sha256='3'*64,
            inputs_identity_sha256='4'*64,source_identity_sha256='a'*64,original_source_identity_sha256='b'*64,
            quality_reference_sha256=worker.sha(quality_records),namespace_prefix=PREFIX+'a0001/native',binary_sha256={})
        counters={'cgroup':'synthetic','observer_pid':42,'process_ids':[42],
            'memory.max':str(worker.MEMORY),'memory.peak':str(worker.MEMORY+4096),'memory.current':'100',
            'memory.stat':'anon 40\nfile 60\n','io.stat':'synthetic','memory.swap.max':'0','memory.swap.peak':'0',
            'memory.events':'max 1\noom 0\noom_kill 0\n','memory.swap.events':'max 0\n',
            'cpu.max':'200000 100000','cpu.stat':'usage_usec 1','pids.max':'512','pids.current':'1','pids.events':'max 0\n'}
        # IO observation may be absent; required resource safety counters still fail closed.
        group=tmp/'cgroup'; group.mkdir()
        for name in ('memory.current','memory.stat'): (group/name).write_text(counters[name])
        captured={k:v for k,v in counters.items() if k not in ('memory.current','memory.stat','io.stat')}
        captured['cgroup']=str(group)
        with patch.object(worker.ids,'capture_cgroup',side_effect=lambda:dict(captured)):
            missing=worker.capture()
            assert missing['io.stat']=='UNMEASURED'
            assert missing['io.stat_unavailable']==dict(type='FileNotFoundError',errno=2)
            worker.validate_cgroup(dict(before=missing,after=missing,closed=True))
            (group/'io.stat').write_text('259:0 rbytes=123 wbytes=456 rios=1 wios=2\n')
            present=worker.capture()
            assert present['io.stat']=='259:0 rbytes=123 wbytes=456 rios=1 wios=2\n'
            assert 'io.stat_unavailable' not in present
            for name in ('memory.current','memory.stat'):
                (group/name).unlink()
                rejects(worker.capture)
                (group/name).write_text(counters[name])
        for name in ('memory.max','cpu.stat','memory.events'):
            unsafe=dict(missing); del unsafe[name]
            rejects(lambda:worker.validate_cgroup(dict(before=missing,after=unsafe,closed=True)))
        # Fake only the huge raw/leaf lengths; all retained small bodies authenticate normally.
        original_artifact=worker.artifact
        def runtime_artifact(path):
            if Path(path).name=='raw': return dict(bytes=3_072_000_000,sha256='5'*64)
            actual=original_artifact(path)
            if str(path).endswith('/router/leaves.bin'): actual['bytes']=31_250*1540
            return actual
        generated={}; current_truth=[None]; current_mode=[None]
        def synthetic_run(args,log,seconds,timing=None):
            assert seconds>0
            if args[0]=='taskset':
                assert args[:3]==['taskset','-c','0-3']; args=args[3:]
            Path(log).parent.mkdir(parents=True,exist_ok=True)
            if timing: Path(timing).write_text(time_log)
            result=dict(command=args,exit_status=0,process_cleanup=True,wall_seconds=.001,process_peak_rss_kib=42)
            if args[0]=='aws':
                if args[1:3]==['s3','cp']:
                    key=args[3].split('/',3)[-1]
                    if key.startswith('synthetic/'): data=blobs[key.rsplit('/',1)[-1]]
                    elif key.endswith('/head.json'): data=generated['head']
                    else: data=(generated['root']/key.split('/generations/',1)[1].split('/',1)[1]).read_bytes()
                    Path(args[4]).write_bytes(data)
                elif args[2]=='head-object':
                    Path(log).write_bytes(worker.encoded(dict(ContentLength=6,ETag='"s3-current-etag"')))
                    return result
                elif args[2]=='get-object':
                    assert args[args.index('--if-match')+1]=='"s3-current-etag"'
                    Path(args[-1]).write_bytes(b'packed')
                else:
                    assert args[2]=='put-object' and args[-2:]==['--if-none-match','*']
            elif args[0]==sys.executable:
                Path(args[-2]).write_bytes(b'raw')
            elif args[0].endswith('/binaries/builder'):
                assert args[3]==str(512 * 1024**2)
                assert os.environ.get('BORSUK_NATIVE_MEMORY_BYTES')==environment.get('BORSUK_NATIVE_MEMORY_BYTES')
                if current_mode[0]=='build-failure': raise RuntimeError('synthetic build failed')
                builder=json.loads(Path(args[1]).read_bytes())
                assert builder['sq8_etag']=='"s3-current-etag"' and builder['semantic_profile']=='fresh1m'
                root=Path(args[-1]); root.mkdir()
                for name,size in zip(worker.STARTUP,sizes):
                    if name=='manifest.json': continue
                    path=root/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(bytes(size))
                (root/'router/leaves.bin').write_bytes(b'synthetic leaves')
                discovery=dict(mode='semantic',profile='fresh1m')
                for label,name in (('root','root'),('membership','membership'),('leaves','leaves')):
                    descriptor=runtime_artifact(root/('router/'+name+'.bin'))
                    discovery[label+'_bytes']=descriptor['bytes']; discovery[label+'_sha256']=descriptor['sha256']
                manifest=dict(schema='borsuk-two-bit-generation-v8',generation=1,base_epoch=0,
                    discovery=discovery,sq8_object_key=builder['sq8_object_key'],sq8_etag=builder['sq8_etag'])
                (root/'manifest.json').write_bytes(worker.encoded(manifest))
                root_sha=runtime_artifact(root/'manifest.json')['sha256']
                generated.update(root=root,head=worker.encoded(dict(schema='borsuk-two-bit-head-v2',generation=1,epoch=1,
                    root_sha256=root_sha,mutation=None,fence=None)))
                Path(log).write_text(root_sha+'\n'); return result
            else:
                assert args[0].endswith('/binaries/publisher')
                assert os.environ['BORSUK_NATIVE_MEMORY_BYTES']==str(1024**3), 'publisher admission'
                if current_mode[0]=='publication-failure': raise RuntimeError('synthetic publication failed')
                assert args[-4:]==['--panel-count','64','--top-k','100']
                refs=worker.source_reference(quality_records)
                startup_event=dict(phase='startup',root_sha256=args[2],generation=1,control_epoch=1,
                    top_k=100,declared_panel_count=64,publish_wall_ns=1,remote_open_wall_ns=40)
                events=[startup_event,*[dict(phase='query',**{k:r[k] for k in ('query_ordinal',*worker.PARITY)}) for r in refs]]
                Path(args[5]).write_bytes(b''.join(worker.encoded(r)+b'\n' for r in events))
            with Path(log).open('ab') as stream: stream.write(b'synthetic transfer/process\n')
            return result
        def runtime_read(_,pointer):
            name=pointer['path']
            if name=='synthetic/requests.jsonl': return request_body
            if name=='synthetic/records.jsonl': return quality_records
            if name=='synthetic/truth.i64': return current_truth[0]
            if name=='synthetic/raw-authority.json': return worker.encoded(dict(raw=dict(sha256='5'*64)))
            if name==str(worker.quality.CONFIG): return worker.encoded(qconfig)
            raise AssertionError(name)
        def synthetic_call(binary,cfg,published,body,ref,t):
            assert cfg['native_memory_bytes']==512 * 1024**2
            assert os.environ.get('BORSUK_NATIVE_MEMORY_BYTES')==environment.get('BORSUK_NATIVE_MEMORY_BYTES')
            q=ref['query_ordinal']; result=copy.deepcopy(row)
            now=worker.time.monotonic_ns()
            result.update(query_ordinal=q,started_ns=now-100000,completed_ns=now-50000,terminal_ns=now,
                cold_start_to_first_http_response_ns=50000,expected_authority=published['authority'],
                request_sha256=worker.sha(body),request_bytes=len(body))
            # Build startup telemetry from the actually generated frozen metadata.
            m=copy.deepcopy(metadata)
            for entry in m:
                entry['bytes']=published['metadata_files'][entry['name']]
                entry['payload_buffer_bound_bytes']=entry['bytes']
            h=dict(ready,authority=published['authority'],remote_open_stats=dict(startup,metadata=m))
            h['transport']=transport_report([12,5,1]+[0]*7,sum(published['metadata_files'].values())+
                published['head_file']['bytes']+published['metadata_files']['manifest.json']+300)
            r=dict(response,**ref,authority=published['authority'])
            r['transport']=transport_report([22,5,1]+[0]*7,h['transport']['totals']['consumed_payload_bytes']+200280)
            result.update(native_header=h,native_server_log=json.dumps(h)+'\n',response=r,
                raw_response_base64=worker.base64.b64encode(worker.encoded(r)).decode(),
                returned_hits=worker.validate_query(r,published,ref,t),accounting=worker.transport(h,r,published))
            if current_mode[0]=='call-failure' and q==1:
                result.update(outcome='failed',error_type='TimeoutError',error='synthetic timeout')
            return result
        environment=dict.fromkeys(worker.THREAD_ENV,'2')
        environment.update(AWS_MAX_ATTEMPTS='1',BORSUK_COLD_SOURCE_COMMIT='0'*40,BORSUK_COLD_ARCHIVE_SHA256='1'*64)
        for mode in ('complete','threshold608','scientific-fail','build-failure','publication-failure','call-failure','cleanup-failure'):
            current_mode[0]=mode
            environment['BORSUK_NATIVE_MEMORY_BYTES']='prior admission'
            if mode=='complete': environment.pop('BORSUK_NATIVE_MEMORY_BYTES')
            expected_truth=[]
            for q in range(64):
                t=list(range(100))
                if mode in ('scientific-fail','threshold608') and q<(33 if mode=='scientific-fail' else 32): t[9]=999
                expected_truth.extend(t)
            current_truth[0]=worker.struct.pack('<'+'q'*6400,*expected_truth)
            qconfig['panel']['files']['screen/truth.i64']=authority_pointer('synthetic/truth.i64',current_truth[0])
            target.write_bytes(worker.encoded(runconfig)); runproof['config_sha256']=worker.artifact(target)['sha256']
            out=tmp/mode
            real_remove=worker.shutil.rmtree
            def remove(path,*args,**kwargs):
                if mode=='cleanup-failure': raise OSError('synthetic cleanup failure')
                return real_remove(path,*args,**kwargs)
            with patch.object(worker,'qualify',return_value=(runconfig,runproof)),\
                 patch.object(worker.quality,'qualify',return_value=(qconfig,{})),\
                 patch.object(worker,'read',side_effect=runtime_read),patch.object(worker,'artifact',side_effect=runtime_artifact),\
                 patch.object(worker,'capture',return_value=counters),patch.object(worker.ids,'tool_versions',return_value={}),\
                 patch.object(worker.importlib.metadata,'version',side_effect=lambda n:worker.FIXED['versions'][n]),\
                 patch.object(worker.os,'sched_getaffinity',return_value={4,5}),\
                 patch.object(worker.quality,'run_process',side_effect=synthetic_run),\
                 patch.object(worker.quality,'ordinal_check',side_effect=lambda sq8,order:dict(rows_checked=1_000_000,record_bytes=780,
                    id_matches_order=True,complete_bijection=True,sq8=runtime_artifact(sq8),order=runtime_artifact(order))),\
                 patch.object(worker,'measured_call',side_effect=synthetic_call),patch.dict(os.environ,environment),\
                 patch.object(worker.shutil,'rmtree',side_effect=remove):
                if mode=='complete': os.environ.pop('BORSUK_NATIVE_MEMORY_BYTES',None)
                try: result=worker.main(target,runproof['config_sha256'],repo,out)
                except (AssertionError,RuntimeError,OSError):
                    assert mode in ('build-failure','publication-failure','call-failure','cleanup-failure'),mode
                else:
                    assert mode in ('complete','threshold608','scientific-fail')
                    assert result['execution_gate_passed'] and result['quality_gate_passed']==(mode!='scientific-fail')
                    assert result['returned_hits']==dict(complete=640,threshold608=608,**{'scientific-fail':607})[mode]
                assert os.environ.get('BORSUK_NATIVE_MEMORY_BYTES')==environment.get('BORSUK_NATIVE_MEMORY_BYTES')
            admission=json.loads((out/'resources.json').read_bytes())['phase_admission']
            assert admission==dict(builder_memory_bytes=512 * 1024**2,publisher_memory_bytes=1024**3,
                native_memory_bytes=512 * 1024**2,memory_bytes=8 * 1024**3,swap_bytes=0)
            if mode=='cleanup-failure': real_remove(out/'scratch')
            assert not (out/'scratch').exists()
            saved=[json.loads(line) for line in (out/'records.jsonl').read_bytes().splitlines()]
            assert [r['query_ordinal'] for r in saved]==list(range(64))
            if mode in ('build-failure','publication-failure'): assert all(r['outcome']=='aborted' for r in saved)
            if mode=='call-failure': assert saved[1]['outcome']=='failed' and all(r['outcome']=='aborted' for r in saved[2:])
            if mode in ('complete','threshold608','scientific-fail'):
                collected=tmp/('collected-'+mode); collected.mkdir(); worker.shutil.copytree(out,collected/'screen')
                for name in ('test-resources.txt','run-closed.log'): (collected/name).write_bytes(b'synthetic outer service log')
                receipt=dict(runproof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
                    awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
                launch=dict(instance_id='i-owned',nodes={'0':dict(instance_id='i-owned')},prefix=PREFIX+'a0001',
                    source_commit='0'*40,source_archive_sha256='1'*64)
                reservation=dict(schema=SCHEMA,qualification=receipt,source_commit='0'*40,source_archive_sha256='1'*64)
                terminal=dict(schema=SCHEMA,instance_id='i-owned',status='complete',phase='complete',exit_code=0,original_exit_code=0,
                    source_commit='0'*40,source_archive_sha256='1'*64,**{n:receipt[n] for n in TERMINAL_IDENTITIES},
                    artifacts={n:worker.artifact(collected/n) for n in ARTIFACTS})
                for name,value in (('aws-launch.json',launch),('aws-closeout.json',dict(nodes=launch['nodes'],state='terminated')),
                    ('aws-reservation.json',reservation),('aws-terminal.json',terminal)):
                    (collected/name).write_bytes(worker.encoded(value))
                with patch.object(module,'qualify',return_value=receipt),patch.object(worker,'read',side_effect=runtime_read):
                    replayed=replay(collected)
                    assert replayed['executed'] and replayed['quality_gate_passed']==(mode!='scientific-fail')
                    s3=Mock()
                    def get(**kwargs):
                        key=kwargs['Key']; path=collected/'aws-terminal.json' if key.endswith('/terminal.json') else collected/key.split('/artifacts/',1)[1]
                        return {'Body':io.BytesIO(path.read_bytes())}
                    s3.get_object.side_effect=get
                    collect(s3,launch['prefix'],collected,'i-owned','0'*40,'1'*64)
                    assert s3.get_object.call_count==len(ARTIFACTS)+1
                    (collected/'aws-closeout.json').write_bytes(worker.encoded(dict(state='running',nodes=launch['nodes'])))
                    s3.reset_mock(); rejects(lambda:collect(s3,launch['prefix'],collected,'i-owned','0'*40,'1'*64))
                    s3.get_object.assert_not_called()
        # Keep lifecycle tests synthetic, including this adapter's exact cloud geometry.
        sdk,botocore,exceptions=(ModuleType(n) for n in ('boto3','botocore','botocore.exceptions'))
        class SDKError(Exception):
            def __init__(self,**kwargs): super().__init__('synthetic SDK')
        for n in ('ClientError','EndpointConnectionError','ReadTimeoutError'): setattr(exceptions,n,SDKError)
        sdk.Session=Mock(side_effect=AssertionError('cloud forbidden')); botocore.exceptions=exceptions
        with patch.dict(sys.modules,{'boto3':sdk,'botocore':botocore,'botocore.exceptions':exceptions}):
            lifecycle,_=ids.lifecycle()
            frozen=dict.fromkeys(TERMINAL_IDENTITIES,'1'*64)
            frozen.update(config_path=str(CONFIG),campaign_schema=SCHEMA,awscli_version=AWSCLI_VERSION,
                awscli_sha256=AWSCLI_SHA256,namespace_prefix=PREFIX+'a0001/native')
            body=user_data('0'*40,'1'*64,'source/key',PREFIX+'a0001',frozen)
            assert all(x in body for x in ('MemoryMax=8G','MemorySwapMax=0','CPUQuota=200%','TasksMax=512',
                'RuntimeMaxSec=7200','--on-active=9000s','taskset -c 4-5','PYTHONPATH="$root/repo"'))
            assert 'ulimit -v' not in body, 'AS ceiling applies to native serving only'
            assert 'scripts.run_native_semantic_1m_cold' in body and 'scripts.run_native_semantic_1m_quality' not in body
            command=body.split('systemd-run --unit=semantic-1m-cold',1)[1].split('\nfor name',1)[0]
            shell='systemd-run() { printf "%s\\n" "$@"; }; root=/synthetic; systemd-run --unit=semantic-1m-cold'+command
            argv=subprocess.check_output(['bash','-c',shell],text=True).splitlines()
            assert 'IOAccounting=yes' in argv
            assert '--setenv=PYTHONPATH=/synthetic/repo' in argv
            assert all('--setenv='+n+'=2' in argv for n in worker.THREAD_ENV)
            assert argv[-7:][1:3]==['-m','scripts.run_native_semantic_1m_cold'],argv
            with patch.object(module,'qualify',side_effect=AssertionError('pending')):
                rejects(lambda:main('a0001'))
            sdk.Session.assert_not_called()
            with patch.dict(sys.modules,{lifecycle.__name__:lifecycle}),worker.panel.contextlib.redirect_stdout(io.StringIO()):
                lifecycle.self_check(lifecycle_only=True)
    print('PASS FIRST1M v8 contract, publisher1GiB/builder-HTTP512MiB admission and env restoration, ordinal/f32 parity, S3 ETag, dynamic startup/IMDS/transport, cold wire boundary, 607/608, ledger/resources/cleanup, original PGID and shared ACK/fsync/termination closure; native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv)==3 and sys.argv[1]=='--replay':
        print(json.dumps(replay(sys.argv[2]),sort_keys=True))
    else:
        assert len(sys.argv)==2, 'usage: aNNNN | --self-check | --replay DIRECTORY'
        with open('/tmp/borsuk-semantic-1m-cold-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(sys.argv[1])
