"""No AWS: exercise cold-call cleanup, fixed parity and the actual worker loop."""
import hashlib
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
from unittest.mock import Mock, patch

from scripts import run_native_cold_first_query as worker


def response(authority):
    return dict(authority=authority, ids=list(range(10)), ranges=[[0,1]],
                planned_bytes=1, submitted_gets=1, verified_bytes=1, failed_gets=0)


def call_check():
    authority = dict(root_sha256='a'*64,generation=1,control_epoch=1)
    expected = response(authority)
    expected['query_ordinal'] = 0
    header = dict(phase='ready',listen='127.0.0.1:8080',authority=authority,
        remote_open_wall_ns=5,head_read_wall_ns=1,remote_open_stats=dict(
            staging_wall_ns=3,decode_wall_ns=2,metadata=[dict(name='manifest.json',bytes=1,
            chunks=1,get_wall_ns=1,stream_wall_ns=2,write_wall_ns=1)]))
    item = dict(dataset='synthetic',authority=authority,indexes={'10':'fixed/index'},metadata_files={'manifest.json':1})
    config = dict(bucket='fixed',region='eu-central-1')
    server = Mock()
    server.poll.return_value = None
    def spawn(command,**kwargs):
        assert command[4:12] == ['timeout','--signal=TERM','--kill-after=5','60','taskset','-c','0-3','fixed-binary']
        kwargs['stdout'].write(json.dumps(header)+'\n')
        kwargs['stdout'].flush()
        Path(command[3]).write_text('Maximum resident set size (kbytes): 1\n')
        return server
    for failure in (False,True):
        clients = [Mock(),Mock()]
        clients[0].connect.side_effect = ConnectionRefusedError()
        raw = response(authority)
        if failure: raw['authority'] = dict(authority,generation=2)
        clock = iter(range(100,1000,10))
        failed_records = io.StringIO()
        with patch.object(worker.subprocess,'Popen',side_effect=spawn), \
             patch.object(worker.http.client,'HTTPConnection',side_effect=clients), \
             patch.object(worker.time,'monotonic_ns',side_effect=lambda:next(clock)), \
             patch.object(worker.time,'sleep'), \
             patch.object(worker,'post',return_value=(200,json.dumps(raw).encode())) as post, \
             patch.object(worker,'stop',return_value=dict(intentional_stop=True,returncode=124)) as stop:
            if failure:
                try: worker.cold_call('fixed-binary',config,item,b'{}',expected,list(range(100)),failed_records)
                except AssertionError: pass
                else: raise AssertionError('wrong authority accepted')
                failed = json.loads(failed_records.getvalue())
                assert failed['outcome']=='failed' and failed['native_close']['intentional_stop']
                assert failed['native_server_log'] and failed['raw_response']
            else:
                record=worker.cold_call('fixed-binary',config,item,b'{}',expected,list(range(100)))
                assert record['connection_refused_attempts']==record['http_attempts']==1
                assert record['returned_hits']==10
                assert record['cold_start_to_first_http_response_ns']==sum(record[key] for key in
                    ('before_successful_connect_attempt_ns','successful_tcp_connect_ns','first_post_to_response_ns'))
            post.assert_called_once()
            stop.assert_called_once_with(server)
            assert all(client.close.call_count==1 for client in clients)
    for mutation in ('ids','authority','budget'):
        raw = response(authority)
        ref = response(authority)
        if mutation=='ids': raw['ids'].reverse()
        elif mutation=='authority': raw['authority']=dict(authority,control_epoch=2)
        else:
            raw['submitted_gets']=ref['submitted_gets']=33
            raw['ranges']=ref['ranges']=[[0,1]]*33
        try: worker.checked_response(raw,ref,list(range(100)),authority)
        except AssertionError: pass
        else: raise AssertionError('invalid '+mutation+' accepted')


def loop_check():
    authority = dict(root_sha256='a'*64,generation=1,control_epoch=1)
    with tempfile.TemporaryDirectory() as tmp:
        base=Path(tmp)
        binary=base/'binary';binary.write_bytes(b'abc')
        qualified=base/'qualified.json'
        qualified.write_text(json.dumps(dict(qualified=True,green_status=0,release_status=0,
            binary_sha256=worker.sha(binary),compiled_native_sha256={})))
        config=dict(schema='borsuk-native-cold-first-query-v1',count=64,k=10,
            dataset_order=['ReLAION','CoHere'],bucket='synthetic',region='eu-central-1',
            code_sha256={name:worker.sha(name) for name in worker.CODE},
            binary=dict(bytes=3,sha256=worker.sha(binary)),items=[])
        for dataset in config['dataset_order']:
            config['items'].append(dict(dataset=dataset,rows=1000000,dimensions=768,
                authority=authority,query_split='synthetic0–63',inputs={name:{} for name in
                ('requests','reference-k10','truth')}))
        def fetch(bucket,identity,path):
            path.parent.mkdir(parents=True,exist_ok=True)
            if path.name=='requests':
                value=[dict(query_ordinal=q,query=[1.]+[0.]*767) for q in range(64)]
                data=''.join(json.dumps(row)+'\n' for row in value).encode()
            elif path.name=='reference-k10':
                value=[dict(top_k=10,declared_panel_count=64,**authority),
                       *[dict(query_ordinal=q,**response(authority)) for q in range(64)],dict(count=64)]
                data=''.join(json.dumps(row)+'\n' for row in value).encode()
            else: data=struct.pack('<100I',*range(100))*64
            path.write_bytes(data)
            return dict(path=str(path),bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
        calls=[]
        def cold(binary,config,item,body,expected,truth,**kwargs):
            ordinal=len(calls);calls.append(item['dataset'])
            start=ordinal*4_000_000_000
            return dict(started_ns=start,completed_ns=start+3_000_000_000,
                cold_start_to_first_http_response_ns=3_000_000_000,incoming_http_wall_ns=50_000_000,
                http_status=200,http_attempts=1,valid_ann_requests=1,returned_hits=10,
                response=response(authority),metadata=dict(metadata_objects=9,metadata_bytes=256000000))
        cfg=base/'config.json';cfg.write_text(json.dumps(config))
        out=base/'out'
        with patch.object(sys,'argv',['worker',str(cfg),worker.sha(cfg),str(binary),str(qualified),str(out)]), \
             patch.object(worker.os,'sched_getaffinity',return_value={4,5}), \
             patch.object(worker,'fetch',side_effect=fetch) as fetched, \
             patch.object(worker,'cold_call',side_effect=cold):
            worker.main()
        assert calls==['ReLAION']*64+['CoHere']*64 and fetched.call_count==6
        summary=json.loads((out/'summary.json').read_text())
        assert summary['ann_queries']==summary['namespace_starts']==128
        assert summary['quality_gate_passed'] and not summary['published_context_gate_passed']
        for dataset in config['dataset_order']:
            rows=[json.loads(line) for line in (out/(dataset.lower()+'-records.jsonl')).read_text().splitlines()]
            assert worker.reduce_panel(rows)['returned_hits']==640
            rows[-1]['returned_hits']=0
            rows[-2]['returned_hits']=0
            rows[-3]['returned_hits']=0
            rows[-4]['returned_hits']=0
            assert not worker.reduce_panel(rows)['quality_gate_passed']


if __name__=='__main__':
    call_check()
    loop_check()
    print('cold first-query worker checks PASS (cleanup/parity/physical cap/128 calls/frozen gates)')
