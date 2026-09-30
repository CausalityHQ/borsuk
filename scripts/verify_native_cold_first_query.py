"""Authenticate closed cold-call receipts and independently reduce each response."""
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import struct
import sys
import tarfile
import copy

import boto3
from scripts.check_native_startup_stats import validate
from scripts.launch_native_peer_1m_spot import BUCKET, REGION

ROOT = Path('docs/research/native-union-20260928')


def sha(body): return hashlib.sha256(body).hexdigest()


def percentile(values,p):
    values=sorted(values)
    position=(len(values)-1)*p
    low,high=math.floor(position),math.ceil(position)
    return values[low]+(values[high]-values[low])*(position-low)


def validate_record(row,request,reference,truth,item,port=8080):
    assert row['dataset']==item['dataset']
    assert row['http_status']==200 and row['http_attempts']==row['valid_ann_requests']==1
    for key in ('started_ns','successful_connect_attempt_ns','connected_ns','completed_ns',
                'cold_start_to_first_http_response_ns','before_successful_connect_attempt_ns',
                'successful_tcp_connect_ns','first_post_to_response_ns','incoming_http_wall_ns',
                'connection_refused_attempts'):
        assert type(row[key]) is int and row[key]>=0
    start,attempt,connect,end=[row[key] for key in
        ('started_ns','successful_connect_attempt_ns','connected_ns','completed_ns')]
    assert start<=attempt<=connect<=end
    assert row['cold_start_to_first_http_response_ns']==end-start
    assert row['before_successful_connect_attempt_ns']==attempt-start
    assert row['successful_tcp_connect_ns']==connect-attempt
    assert row['first_post_to_response_ns']==end-connect
    assert row['incoming_http_wall_ns']==end-attempt
    body=json.dumps(dict(query=request['query'],k=10,**item['authority']),
                    separators=(',',':'),allow_nan=False).encode()
    assert row['request_sha256']==sha(body) and row['request_bytes']==len(body)
    response=row['response']
    assert response['authority']==item['authority']
    for key in ('ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets'):
        assert response[key]==reference[key]
    assert len(response['ids'])==len(set(response['ids']))==10
    assert all(type(v) is int and 0<=v<1000000 for v in response['ids'])
    assert len(response['ranges'])==response['submitted_gets']<=32
    assert response['planned_bytes']==response['verified_bytes']<=16773120
    assert sum(end-begin for begin,end in response['ranges'])==response['planned_bytes']
    assert response['failed_gets']==0
    assert type(response['native_wall_ns']) is int and 0<=response['native_wall_ns']<=row['incoming_http_wall_ns']
    assert row['returned_hits']==len(set(response['ids']) & set(truth[:10]))
    headers=[json.loads(line) for line in row['native_server_log'].splitlines() if line.startswith('{')]
    assert len(headers)==1 and headers[0]==row['native_header']
    header=headers[0]
    assert header['phase']=='ready' and header['listen']==f'127.0.0.1:{port}' and header['authority']==item['authority']
    assert row['metadata']==validate(header['remote_open_stats'],item['metadata_files'],header['remote_open_wall_ns'])
    assert end-start>=header['remote_open_wall_ns']+header['head_read_wall_ns']
    assert row['native_close']['intentional_stop'] is True
    assert row['native_close']['returncode'] in (124,143,-15)
    metrics={line.strip().split(':',1)[0]:line.strip().split(':',1)[1].strip()
             for line in row['native_time_log'].splitlines() if ':' in line}
    peak=int(metrics['Maximum resident set size (kbytes)'])*1024
    assert 0<peak<4*1024**3 and int(metrics['Swaps'])==0
    return peak


def reduce_records(records,requests,references,truth,item):
    assert len(records)==len(requests)==len(references)==len(truth)==64
    assert [r['query_ordinal'] for r in records]==list(range(64))
    previous=0
    native_peaks=[]
    for q,row in enumerate(records):
        assert previous<=row['started_ns']
        native_peaks.append(validate_record(row,requests[q],references[q],truth[q],item))
        previous=row['completed_ns']
    hits=sum(row['returned_hits'] for row in records)
    cold={name:percentile([r['cold_start_to_first_http_response_ns']/1e6 for r in records],p)
          for name,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}
    http={name:percentile([r['incoming_http_wall_ns']/1e6 for r in records],p)
          for name,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}
    span=records[-1]['completed_ns']-records[0]['started_ns']
    result=dict(count=64,returned_hits=hits,recall_at_10=hits/640,quality_gate_passed=hits>=608,
        published_context_gate_passed=hits>=608 and cold['p90']<444,
        cold_start_to_first_http_response_ms=cold,incoming_http_ms=http,
        serial_cold_calls_per_second=64e9/span,serial_span_ns=span,
        query_submitted_gets=sum(r['response']['submitted_gets'] for r in records),
        query_verified_bytes=sum(r['response']['verified_bytes'] for r in records),
        query_failed_gets=0,metadata_objects=sum(r['metadata']['metadata_objects'] for r in records),
        metadata_bytes=sum(r['metadata']['metadata_bytes'] for r in records))
    return result,max(native_peaks)


def self_check():
    # Use an already closed real metadata receipt; query/timing values below are synthetic.
    directory=ROOT/'arm-sha-startup/a0001'
    raw_log=gzip.decompress((directory/'screen/candidate1/cell0-server.log.gz').read_bytes()).decode()
    header=next(json.loads(line) for line in raw_log.splitlines() if line.startswith('{'))
    time_log=gzip.decompress((directory/'screen/candidate1/cell0-server.time.gz').read_bytes()).decode()
    item=json.loads((ROOT/'cold-first-query-config.json').read_text())['items'][0]
    response=dict(authority=item['authority'],ids=list(range(10)),ranges=[[0,1]],
                  planned_bytes=1,submitted_gets=1,verified_bytes=1,failed_gets=0,native_wall_ns=1000000)
    records,requests,references,truth=[],[],[],[]
    load=header['head_read_wall_ns']+header['remote_open_wall_ns']
    for q in range(64):
        request=dict(query_ordinal=q,query=[1.]+[0.]*767)
        body=json.dumps(dict(query=request['query'],k=10,**item['authority']),separators=(',',':')).encode()
        start=(q+1)*10000000000
        attempt=start+load
        connected=attempt+1000000
        end=connected+100000000
        records.append(dict(query_ordinal=q,dataset=item['dataset'],http_status=200,http_attempts=1,
            valid_ann_requests=1,started_ns=start,successful_connect_attempt_ns=attempt,connected_ns=connected,
            completed_ns=end,cold_start_to_first_http_response_ns=end-start,
            before_successful_connect_attempt_ns=attempt-start,successful_tcp_connect_ns=connected-attempt,
            first_post_to_response_ns=end-connected,incoming_http_wall_ns=end-attempt,connection_refused_attempts=10,
            request_sha256=sha(body),request_bytes=len(body),response=copy.deepcopy(response),returned_hits=10,
            native_server_log=raw_log,native_header=header,native_close=dict(intentional_stop=True,returncode=143),
            native_time_log=time_log,metadata=validate(header['remote_open_stats'],item['metadata_files'],header['remote_open_wall_ns'])))
        requests.append(request);references.append(copy.deepcopy(response));truth.append(list(range(100)))
    result,_=reduce_records(records,requests,references,truth,item)
    assert result['quality_gate_passed'] and not result['published_context_gate_passed']
    for mutation in ('timing','ids','hits','cleanup'):
        bad=copy.deepcopy(records)
        if mutation=='timing':bad[0]['incoming_http_wall_ns']-=1
        elif mutation=='ids':bad[0]['response']['ids'].reverse()
        elif mutation=='hits':bad[0]['returned_hits']-=1
        else:bad[0]['native_close']['intentional_stop']=False
        try:reduce_records(bad,requests,references,truth,item)
        except AssertionError:pass
        else:raise AssertionError('invalid '+mutation+' accepted')
    print('independent cold reducer PASS (synthetic; timing/IDs/hits/cleanup negative guards)')


def main(attempt):
    from scripts import launch_native_cold_first_query_spot as campaign
    directory=ROOT/'cold-first-query'/attempt
    launch=json.loads((directory/'aws-launch.json').read_text())
    close=json.loads((directory/'aws-closeout.json').read_text())
    session=boto3.Session(profile_name='causality',region_name=REGION)
    s3,ec2=session.client('s3'),session.client('ec2')
    def remote(key): return s3.get_object(Bucket=BUCKET,Key=key)['Body'].read()
    prefix=launch['prefix']
    assert prefix=='research/native-union/20260929/cold-first-query-'+attempt
    assert json.loads(remote(prefix+'/launch.json'))==launch
    terminal_body=remote(prefix+'/terminal.json')
    assert terminal_body==(directory/'aws-terminal.json').read_bytes()
    terminal=json.loads(terminal_body)
    reservation=json.loads(remote(prefix+'/reservation.json'))
    assert reservation==json.loads((directory/'aws-reservation.json').read_text())
    assert terminal['schema']==reservation['schema']==campaign.SCHEMA
    assert terminal['status']=='complete' and terminal['exit_code']==0
    assert terminal['instance_id']==launch['instance_id']
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key]==reservation[key]==launch[key]
    archive=remote('research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')
    assert sha(archive)==launch['source_archive_sha256']
    artifacts={}
    assert set(terminal['artifacts'])==set(campaign.ARTIFACTS)
    for name,identity in terminal['artifacts'].items():
        body=remote(prefix+'/artifacts/'+name)
        assert len(body)==identity['bytes'] and sha(body)==identity['sha256']
        assert body==gzip.decompress((directory/(name+'.gz')).read_bytes())
        artifacts[name]=body
    with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
        def archived(name): return tar.extractfile(name).read()
        config_body=archived(str(campaign.CONFIG))
        assert config_body==campaign.CONFIG.read_bytes()
        config=json.loads(config_body)
        assert config['bucket']==BUCKET and config['region']==REGION
        assert sha(config_body)==reservation['config_sha256']
        assert config['schema']=='borsuk-native-cold-first-query-v1'
        assert config['count']==64 and config['k']==10 and config['dataset_order']==['ReLAION','CoHere']
        assert config['gates']==dict(recall_at_10_minimum=.95,all_calls_success=True,
            source_scorer_ordered_id_physical_parity=True,cold_start_to_first_http_response_p90_ms_exclusive_maximum=444)
        qualification=reservation['qualification']
        assert json.loads(artifacts['source-qualification.json'])==qualification
        for name,digest in qualification['code_sha256'].items(): assert sha(archived(name))==digest
        for name,digest in config['code_sha256'].items(): assert sha(archived(name))==digest
        for name in ('scripts/launch_native_cold_first_query_spot.py','scripts/check_native_startup_build.py'):
            assert sha(Path(name).read_bytes())==qualification['code_sha256'][name]
        for name in ('scripts/launch_native_peer_1m_spot.py','scripts/launch_native_startup_profile_spot.py',
                     'scripts/launch_v174_relaid_bind_compile_spot.py'):
            assert Path(name).read_bytes()==archived(name)
        frozen=config['frozen_native_qualification']
        frozen_body=archived(frozen['path'])
        assert sha(frozen_body)==frozen['sha256']
        proof=json.loads(frozen_body)
        assert proof['valid_diagnostic'] and proof['diagnostic_gate_passed'] and proof['state']=='terminated'
        for key in ('source_commit','source_archive_sha256','terminal_sha256'): assert frozen[key]==proof[key]
        native_prefix='research/native-union/20260929/arm-sha-startup-a0001'
        native_terminal_body=remote(native_prefix+'/terminal.json')
        assert sha(native_terminal_body)==frozen['terminal_sha256']
        native_terminal=json.loads(native_terminal_body)
        assert native_terminal['status']=='complete' and native_terminal['exit_code']==0
        for name in ('binaries/two_bit_http','boundary-check.json','compiled-source.json'):
            assert sha(artifacts[name])==native_terminal['artifacts'][name]['sha256']
            assert len(artifacts[name])==native_terminal['artifacts'][name]['bytes']
        boundary=json.loads(artifacts['boundary-check.json'])
        assert boundary['qualified'] and boundary['green_status']==boundary['release_status']==0
        assert not boundary['current_full_suite_pass_claim']
        assert boundary['binary_sha256']==sha(artifacts['binaries/two_bit_http'])==config['binary']['sha256']
        assert config['binary']['bytes']==len(artifacts['binaries/two_bit_http'])
        assert boundary['compiled_native_sha256']==proof['compiled_native_sha256']==json.loads(artifacts['compiled-source.json'])
        for name,digest in boundary['compiled_native_sha256'].items(): assert sha(archived(name))==digest
        hashes={m.name:sha(archived(m.name)) for m in tar.getmembers() if m.isfile() and
                (m.name.endswith('.rs') or Path(m.name).name in ('Cargo.toml','Cargo.lock'))}
        identity=sha(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode())
        assert identity==boundary['source_identity_sha256']==qualification['source_identity_sha256']
        assert len(hashes)==boundary['source_file_count']==qualification['source_file_count']==395
        assert qualification['compiled_native_sha256']==proof['compiled_native_sha256']
        assert qualification['current_full_suite_pass_claim'] is False
        original=json.loads(archived(str(ROOT/'peer-1m-config.json')))
        metadata=json.loads(archived(str(ROOT/'startup-profile-config.json')))
        for item,old in zip(config['items'],original['items']):
            assert {key:value for key,value in item.items() if key!='metadata_files'}==old
            assert item['metadata_files']==next(x['metadata_files'] for x in metadata['items'] if x['dataset']==item['dataset'])
    expected_user_data=campaign.user_data(launch['source_commit'],sha(archive),
        'research/native-library-check/sources/'+sha(archive)+'.tar.gz',prefix,qualification)
    assert expected_user_data==(directory/'aws-user-data.sh').read_text()
    group=json.loads(artifacts['profile-cgroup.json'])
    assert int(group['memory.max'])==8*1024**3 and int(group['memory.peak'])<8*1024**3
    assert int(group['memory.swap.max'])==int(group['memory.swap.peak'])==0
    events=dict(line.split() for line in group['memory.events'].splitlines())
    assert events['oom']==events['oom_kill']=='0'
    assert group['cpu_affinity']==[0,1,2,3] and group['rlimit_as_bytes']==[4*1024**3]*2
    summary=json.loads(artifacts['screen/summary.json'])
    assert summary['ann_queries']==summary['namespace_starts']==128 and summary['k']==10
    assert summary['source_scorer_ordered_id_physical_parity'] and summary['namespace_cold_start_included']
    assert not summary['application_sq8_cache'] and summary['s3_service_cache']=='uncontrolled'
    assert summary['transport']=='loopback plain HTTP' and summary['client_cpu_affinity']==[4,5]
    assert summary['native_cpu_affinity']==[0,1,2,3]
    assert not summary['serial_cold_qps_is_offered_or_saturation_qps'] and not summary['matched_vendor_measured']
    panels={}
    peaks={}
    for item in config['items']:
        dataset=item['dataset']
        bodies={}
        for name in ('requests','reference-k10','truth'):
            ident=item['inputs'][name]
            body=remote(ident['key'])
            assert len(body)==ident['bytes'] and sha(body)==ident['sha256']
            if 'range_bytes' in ident:
                body=body[ident['range_start']:ident['range_start']+ident['range_bytes']]
                assert len(body)==ident['range_bytes'] and sha(body)==ident['range_sha256']
            bodies[name]=body
        requests=[json.loads(line) for line in bodies['requests'].splitlines()]
        reference=[json.loads(line) for line in bodies['reference-k10'].splitlines()]
        assert len(requests)==64 and len(reference)==66 and len(bodies['truth'])==25600
        assert reference[0]['top_k']==10 and reference[0]['declared_panel_count']==reference[-1]['count']==64
        for key,value in item['authority'].items(): assert reference[0][key]==value
        assert [r['query_ordinal'] for r in requests]==[r['query_ordinal'] for r in reference[1:-1]]==list(range(64))
        truth=[struct.unpack_from('<100I',bodies['truth'],q*400) for q in range(64)]
        assert all(len(set(row))==100 and max(row)<1000000 for row in truth)
        records=[json.loads(line) for line in artifacts['screen/'+dataset.lower()+'-records.jsonl'].splitlines()]
        result,peak=reduce_records(records,requests,reference[1:-1],truth,item)
        assert all(summary['panels'][dataset][key]==value for key,value in result.items())
        assert summary['panels'][dataset]['split']==item['query_split']
        panels[dataset]=dict(result,split=item['query_split'])
        peaks[dataset]=peak
    quality=all(p['quality_gate_passed'] for p in panels.values())
    context=all(p['published_context_gate_passed'] for p in panels.values())
    assert summary['quality_gate_passed']==quality and summary['published_context_gate_passed']==context
    instance=ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name']==close['state']=='terminated' and instance['InstanceLifecycle']=='spot'
    assert instance['InstanceType']=='c7g.2xlarge' and instance['Placement']['AvailabilityZone']==reservation['availability_zone']
    report=dict(valid_measurement=True,state='terminated',instance_id=launch['instance_id'],
        source_commit=launch['source_commit'],source_archive_sha256=sha(archive),config_sha256=sha(config_body),
        terminal_sha256=sha(terminal_body),quality_gate_passed=quality,published_context_gate_passed=context,
        panels=panels,native_peak_rss_bytes=peaks,cgroup_peak_bytes=int(group['memory.peak']),swap_peak_bytes=0,
        namespace_cold_start_included=True,matched_vendor_measured=False,offered_or_saturation_qps_measured=False)
    (directory/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


if __name__=='__main__':
    if sys.argv[1:]==['--self-check']:self_check()
    else:main(sys.argv[1] if len(sys.argv)>1 else 'a0001')
