"""Independent schedules, port ownership, cold responses and all-offer denominators."""
import copy
import gzip
import io
import json
from pathlib import Path
import struct
import sys
import tarfile
import tempfile

import boto3
from scripts.verify_native_cold_first_query import sha, percentile, validate_record
from scripts.check_native_metadata_ranges_stats import validate as transfer

RATES = [.25,.5,1,2,4,8]


def tails(values):
    return {name:percentile([v/1e6 for v in values],p) if values else None
            for name,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}


def reduce_cell(rows,cell,requests,references,truth,item,index):
    rate = RATES[index]
    assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
    assert cell['rate_index'] == index and cell['dataset'] == item['dataset']
    assert cell['offered_qps'] == rate and cell['split'] == item['query_split']
    epoch,end = cell['epoch_ns'],cell['terminal_ns']
    assert type(epoch) is type(end) is int and 0 < epoch < end
    previous_dispatch = epoch
    peak = 0
    success = [];admitted = [];drops = 0
    ownership = []
    for q,row in enumerate(rows):
        assert row['rate_index'] == index and row['dataset'] == item['dataset']
        assert row['offered_qps'] == rate and row['scheduled_ns'] == epoch+round(q*1e9/rate)
        dispatched,terminal = row['dispatched_ns'],row['terminal_ns']
        assert type(dispatched) is type(terminal) is int
        assert previous_dispatch <= dispatched <= terminal <= end
        assert row['scheduled_ns'] <= dispatched
        previous_dispatch = dispatched
        if row['port'] is None:
            assert row['outcome'] == 'capacity_drop'
            assert row['started_ns'] is row['completed_ns'] is None
            assert row['http_attempts'] == row['valid_ann_requests'] == 0
            assert 'response' not in row and 'native_header' not in row
            drops += 1
            continue
        port = row['port']
        assert type(port) is int and 18080 <= port <= 18085
        for previous_port,begin,finish in ownership:
            if previous_port == port: assert finish <= dispatched
        ownership.append((port,dispatched,terminal));admitted.append(row)
        assert type(row['started_ns']) is int and dispatched <= row['started_ns'] <= terminal
        if row['outcome'] == 'success':
            assert row['completed_ns'] <= terminal
            peak = max(peak,validate_record(row,requests[q],references[q],truth[q],item,port=port))
            assert row['transfer_accounting'] == transfer(row['native_header'],item['metadata_files'],'candidate')
            success.append(row)
        else:
            assert row['outcome'] == 'transport_error'
            raw = row['failure_stream_raw']
            assert raw and json.loads(raw) == row['failure_record']
            failed = row['failure_record']
            assert failed['query_ordinal'] == q and failed['dataset'] == item['dataset']
            assert failed['outcome'] == 'failed' and failed['error_type'] == row['error_type']
            assert failed['started_ns'] == row['started_ns']
            assert type(failed['native_close']['returncode']) is int
    for _,begin,_ in ownership:
        assert sum(a <= begin < b for _,a,b in ownership) <= 6
    n = len(success);hits = sum(r['returned_hits'] for r in success)
    span = end-epoch
    quality = n == 64 and hits >= 608
    service = tails([r['cold_start_to_first_http_response_ns'] for r in success])
    result = dict(offered_qps=rate,offered=64,admitted=len(admitted),accepted_completed=len(admitted),
        successful=n,capacity_drops=drops,errors=len(admitted)-n,aborted_offers=0,arm_failed=False,
        success_fraction=n/64,epoch_ns=epoch,terminal_ns=end,
        planned_offer_window_ns=round(64*1e9/rate),full_span_ns=span,
        successful_full_span_qps=n*1e9/span,accepted_completed_full_span_qps=len(admitted)*1e9/span,
        returned_hits=hits,quality_success_count=n,success_conditioned_recall_at_10=hits/(10*n) if n else None,
        latency_population='successful offers only; drops/errors/aborts remain in all-offer denominators',
        cold_start_to_first_http_response_ms=service,
        scheduled_to_response_ms=tails([r['completed_ns']-r['scheduled_ns'] for r in success]),
        dispatch_delay_ms=tails([r['dispatched_ns']-r['scheduled_ns'] for r in rows]),
        all_offers_successful=n==64,quality_gate_passed=quality,
        published_context_gate_passed=quality and service['p90'] < 444,
        counter_scope='successful calls only; unsuccessful raw records retained',
        query_submitted_gets=sum(r['response']['submitted_gets'] for r in success),
        query_verified_bytes=sum(r['response']['verified_bytes'] for r in success),
        query_failed_gets=sum(r['response']['failed_gets'] for r in success),
        logical_metadata_head_requests=sum(r['transfer_accounting']['logical_metadata_head_requests'] for r in success),
        logical_metadata_get_requests=sum(r['transfer_accounting']['logical_metadata_get_requests'] for r in success))
    result.update(rate_index=index,dataset=item['dataset'],split=item['query_split'],
        records_file=f'rate{index}-{item["dataset"].lower()}-records.jsonl')
    assert result == cell
    return result,peak


def main(attempt):
    from scripts import launch_native_cold_offered_spot as campaign
    directory = campaign.ROOT/campaign.NAME/attempt
    session = boto3.Session(profile_name='causality',region_name=campaign.peer.REGION)
    s3,ec2 = session.client('s3'),session.client('ec2')
    def remote(key): return s3.get_object(Bucket=campaign.peer.BUCKET,Key=key)['Body'].read()
    launch = json.loads((directory/'aws-launch.json').read_bytes())
    prefix = launch['prefix'];assert prefix == campaign.PREFIX+attempt
    assert json.loads(remote(prefix+'/launch.json')) == launch
    reservation = json.loads(remote(prefix+'/reservation.json'))
    assert reservation == json.loads((directory/'aws-reservation.json').read_bytes())
    terminal_body = remote(prefix+'/terminal.json')
    assert terminal_body == (directory/'aws-terminal.json').read_bytes()
    terminal = json.loads(terminal_body)
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['schema'] == reservation['schema'] == campaign.SCHEMA
    assert terminal['instance_id'] == launch['instance_id']
    for key in ('source_commit','source_archive_sha256'): assert terminal[key] == reservation[key] == launch[key]
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS)
    bodies = {}
    for name,identity in terminal['artifacts'].items():
        value = remote(prefix+'/artifacts/'+name)
        assert len(value) == identity['bytes'] and sha(value) == identity['sha256']
        assert value == gzip.decompress((directory/(name+'.gz')).read_bytes())
        bodies[name] = value
    proof = json.loads(bodies['source-qualification.json'])
    assert proof == reservation['qualification'] and proof['native_rebuilt'] is False
    assert reservation['wall_seconds'] == campaign.WALL and reservation['compute_cap_usd'] == campaign.COMPUTE_CAP
    expected_userdata = campaign.user_data(launch['source_commit'],launch['source_archive_sha256'],
        'research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz',prefix,proof)
    assert (directory/'aws-user-data.sh').read_text() == expected_userdata
    assert b'aarch64' in bodies['cpu.txt'] and b'sha2' in bodies['cpu.txt']
    archive = remote('research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')
    assert sha(archive) == launch['source_archive_sha256']
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        with tarfile.open(fileobj=io.BytesIO(gzip.decompress(archive))) as tar: tar.extractall(repo,filter='data')
        assert campaign.preflight(base=repo) == proof
        config_body = (repo/campaign.CONFIG).read_bytes()
        assert sha(config_body) == proof['config_sha256'] == reservation['config_sha256']
        config = json.loads(config_body)
        for name,identity in proof['frozen_binary_artifacts'].items():
            assert bodies['frozen/'+name] == gzip.decompress((repo/campaign.FROZEN/(name+'.gz')).read_bytes())
        for name in ('binaries/two_bit_http','boundary-check.json','compiled-source.json'):
            assert bodies[name] == bodies['frozen/'+name]
        assert bodies['binaries/two_bit_http'] and sha(bodies['binaries/two_bit_http']) == campaign.BINARY_SHA
        summary = json.loads(bodies['screen/summary.json'])
        assert summary['schema'] == 'borsuk-native-cold-offered-result-v1' and summary['complete']
        assert summary['arm_failed'] is False and len(summary['cells']) == 12
        assert summary['offered_qps'] == RATES and summary['workers'] == 6 and summary['base_port'] == 18080
        assert summary['namespace_cold_start_included'] is True and summary['application_sq8_cache'] is False
        assert summary['matched_vendor_measured'] is summary['current_full_suite_pass_claim'] is summary['native_rebuilt'] is False
        assert summary['client_cpu_affinity'] == [4,5] and summary['native_cpu_affinity'] == [0,1,2,3]
        assert summary['s3_service_cache'] == 'uncontrolled' and summary['transport'] == 'loopback plain HTTP'
        assert summary['native_tokio_threads'] == 4 and summary['native_budget_bytes'] == 1024**3
        inputs = {}
        for item in config['items']:
            dataset = item['dataset'];values = {}
            for name in ('requests','reference-k10','truth'):
                identity = item['inputs'][name]
                value = remote(identity['key'])
                assert len(value) == identity['bytes'] and sha(value) == identity['sha256']
                if 'range_bytes' in identity:
                    value = value[identity['range_start']:identity['range_start']+identity['range_bytes']]
                    assert len(value) == identity['range_bytes'] and sha(value) == identity['range_sha256']
                observed = summary['inputs'][dataset][name]
                assert observed['bytes'] == len(value) and observed['sha256'] == sha(value)
                values[name] = value
            requests = [json.loads(line) for line in values['requests'].splitlines()]
            reference_rows = [json.loads(line) for line in values['reference-k10'].splitlines()]
            assert len(requests) == 64 and len(reference_rows) == 66 and reference_rows[-1]['count'] == 64
            assert reference_rows[0]['declared_panel_count'] == 64 and reference_rows[0]['top_k'] == 10
            for key,value in item['authority'].items(): assert reference_rows[0][key] == value
            references = reference_rows[1:-1]
            assert [r['query_ordinal'] for r in requests] == [r['query_ordinal'] for r in references] == list(range(64))
            assert len(values['truth']) == 64*400
            truth = [struct.unpack_from('<100I',values['truth'],q*400) for q in range(64)]
            assert all(len(set(row)) == 100 and max(row) < 1000000 for row in truth)
            inputs[dataset] = requests,references,truth
        cells = [];peaks = {};previous = 0
        for index,rate in enumerate(RATES):
            for item in config['items']:
                cell = summary['cells'][len(cells)]
                assert previous <= cell['epoch_ns'];previous = cell['terminal_ns']
                rows = [json.loads(line) for line in bodies['screen/'+cell['records_file']].splitlines()]
                result,peak = reduce_cell(rows,cell,*inputs[item['dataset']],item,index)
                cells.append(result);peaks[cell['records_file']] = peak
        for key in ('offered','admitted','successful','accepted_completed','capacity_drops','errors'):
            assert summary[key] == sum(c[key] for c in cells)
        assert summary['offered'] == summary['planned_offers'] == 768
        for key in ('quality_gate_passed','published_context_gate_passed'):
            assert summary[key] == all(c[key] for c in cells)
        largest = max([0]+[rate for rate in RATES if all(c['quality_gate_passed'] for c in cells if c['offered_qps'] == rate)])
        assert summary['largest_passing_tested_offered_qps'] == largest
        assert summary['eight_qps_attained'] == all(c['quality_gate_passed'] for c in cells if c['offered_qps'] == 8)
    group = json.loads(bodies['profile-cgroup.json'])
    assert int(group['memory.swap.peak']) == 0
    assert 0 < int(group['memory.peak']) <= 7*1024**3
    assert int(group['memory.max']) == 7*1024**3 and int(group['memory.swap.max']) == 0
    events = dict(line.split() for line in group['memory.events'].splitlines())
    assert events['oom'] == events['oom_kill'] == '0'
    assert group['cpu_affinity'] == [0,1,2,3] and group['rlimit_as_bytes'] == [4*1024**3]*2
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    close = json.loads((directory/'aws-closeout.json').read_bytes())
    assert instance['State']['Name'] == close['state'] == 'terminated'
    assert close['nodes'] == {'0':{'instance_id':launch['instance_id']}}
    assert instance['InstanceLifecycle'] == 'spot' and instance['InstanceType'] == 'c7g.2xlarge'
    assert instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    report = dict(valid_measurement=True,state='terminated',instance_id=launch['instance_id'],
        source_commit=launch['source_commit'],source_archive_sha256=sha(archive),config_sha256=sha(config_body),
        terminal_sha256=sha(terminal_body),native_rebuilt=False,current_full_suite_pass_claim=False,
        matched_vendor_measured=False,total_cost_measured=False,cgroup_peak_bytes=int(group['memory.peak']),
        native_peak_rss_bytes=peaks,cells=cells,largest_passing_tested_offered_qps=largest,
        eight_qps_attained=summary['eight_qps_attained'])
    (directory/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


def self_check():
    from scripts import launch_native_cold_offered_spot as campaign
    from scripts.run_native_cold_offered import reduce_cell as worker_reduce
    config = json.loads((campaign.ROOT/'metadata-ranges-config.json').read_bytes())
    item = config['items'][0]
    original = json.loads(gzip.decompress((campaign.FROZEN/'screen/block1-records.jsonl.gz').read_bytes()).splitlines()[0])
    requests = [dict(query_ordinal=q,query=[1.]+[0.]*767) for q in range(64)]
    references = [copy.deepcopy(original['response']) for _ in range(64)]
    ids = references[0]['ids']
    truth = [ids+[q for q in range(100) if q not in ids][:90] for _ in range(64)]
    epoch = 10**12
    rows = []
    for q in range(64):
        row = copy.deepcopy(original)
        row.update(query_ordinal=q,rate_index=0,offered_qps=.25,scheduled_ns=epoch+q*4*10**9,
            dispatched_ns=epoch+q*4*10**9+10**6,port=18080,outcome='success')
        delta = row['dispatched_ns']+10**6-row['started_ns']
        for key in ('started_ns','successful_connect_attempt_ns','connected_ns','completed_ns'): row[key] += delta
        row['terminal_ns'] = row['completed_ns']+50*10**6
        body = json.dumps(dict(query=requests[q]['query'],k=10,**item['authority']),separators=(',',':')).encode()
        row.update(request_sha256=sha(body),request_bytes=len(body),returned_hits=10)
        row['native_header']['listen']='127.0.0.1:18080'
        row['native_server_log']=row['native_server_log'].replace('127.0.0.1:8080','127.0.0.1:18080')
        row['transfer_accounting']=transfer(row['native_header'],item['metadata_files'],'candidate')
        rows.append(row)
    def expected(values):
        result=worker_reduce(values,.25,epoch,max(r['terminal_ns'] for r in values)+1,False)
        return dict(result,rate_index=0,dataset=item['dataset'],split=item['query_split'],records_file='rate0-relaion-records.jsonl')
    reduce_cell(rows,expected(rows),requests,references,truth,item,0)
    for outcome in ('capacity_drop','transport_error'):
        subset=copy.deepcopy(rows)
        last=subset[-1]
        if outcome=='capacity_drop':
            subset[-1]=dict(query_ordinal=63,dataset=item['dataset'],rate_index=0,offered_qps=.25,
                scheduled_ns=last['scheduled_ns'],dispatched_ns=last['dispatched_ns'],terminal_ns=last['terminal_ns'],
                started_ns=None,completed_ns=None,port=None,outcome=outcome,http_attempts=0,valid_ann_requests=0)
        else:
            raw=dict(query_ordinal=63,dataset=item['dataset'],started_ns=last['started_ns'],
                outcome='failed',error_type='OSError',native_close=last['native_close'])
            last.update(outcome=outcome,error_type='OSError',failure_record=raw,failure_stream_raw=json.dumps(raw))
        reduced=expected(subset)
        assert reduced['successful']==63 and not reduced['quality_gate_passed']
        reduce_cell(subset,reduced,requests,references,truth,item,0)
    for mutation in ('schedule','port','hits','buffer','summary'):
        bad = copy.deepcopy(rows);cell=expected(bad)
        if mutation=='schedule':bad[1]['scheduled_ns']+=1
        elif mutation=='port':bad[1]['port']=18086
        elif mutation=='hits':bad[1]['returned_hits']-=1
        elif mutation=='buffer':bad[1]['transfer_accounting']['payload_buffer_bound_bytes']+=1
        else:cell['successful']-=1
        try:reduce_cell(bad,cell,requests,references,truth,item,0)
        except AssertionError:pass
        else:raise AssertionError('accepted '+mutation)
    print('independent cold offered reducer PASS (synthetic64; schedule/port/GT/buffer/denominator negatives)')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    else: main(sys.argv[1] if len(sys.argv)==2 else 'a0001')
