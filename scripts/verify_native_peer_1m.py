"""Authenticate closed two-role peer receipts and independently reduce every offer."""
from collections import Counter
import gzip
import hashlib
import json
import math
from pathlib import Path
import struct
import sys

import boto3
from scripts import launch_native_peer_1m_spot as campaign


def sha(body):
    return hashlib.sha256(body).hexdigest()


def percentile(values, probability):
    values = sorted(values)
    position = (len(values) - 1) * probability
    low, high = math.floor(position), math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def main(attempt):
    config_body = campaign.CONFIG.read_bytes()
    config = json.loads(config_body)
    campaign.preflight(config)
    directory = campaign.ROOT / 'peer-1m' / attempt
    launch = json.loads((directory / 'aws-launch.json').read_text())
    close = json.loads((directory / 'aws-closeout.json').read_text())
    session = boto3.Session(profile_name='causality', region_name=config['region'])
    s3, ec2 = session.client('s3'), session.client('ec2')
    def remote(key):
        return s3.get_object(Bucket=config['bucket'], Key=key)['Body'].read()
    prefix = launch['prefix']
    assert json.loads(remote(prefix + '/launch.json')) == launch
    reservation = json.loads(remote(prefix + '/reservation.json'))
    assert reservation == json.loads((directory / 'aws-reservation.json').read_text())
    assert launch['config_sha256'] == reservation['config_sha256'] == sha(config_body)
    assert launch['source_commit'] == reservation['source_commit']
    assert launch['source_archive_sha256'] == reservation['source_archive_sha256']
    assert sha(remote('research/native-library-check/sources/' + launch['source_archive_sha256'] + '.tar.gz')) == launch['source_archive_sha256']
    assert launch['server']['instance_id'] != launch['client']['instance_id']
    assert launch['server']['private_ip'] != launch['client']['private_ip']
    assert close['nodes'] == {role: launch[role] for role in ('server', 'client')}
    assert close['state'] == 'terminated'
    artifacts, summaries = {}, {}
    for role in ('server', 'client'):
        body = remote(prefix + '/' + role + '/terminal.json')
        assert body == (directory / (role + '-terminal.json')).read_bytes()
        terminal = json.loads(body)
        assert terminal['schema'] == campaign.SCHEMA and terminal['status'] == 'complete' and terminal['exit_code'] == 0
        assert terminal['instance_id'] == launch[role]['instance_id']
        assert terminal['source_commit'] == launch['source_commit']
        assert terminal['source_archive_sha256'] == launch['source_archive_sha256']
        artifacts[role] = {}
        for name, ident in terminal['artifacts'].items():
            body = remote(prefix + '/' + role + '/artifacts/' + name)
            assert len(body) == ident['bytes'] and sha(body) == ident['sha256']
            assert body == gzip.decompress((directory / role / (name + '.gz')).read_bytes())
            artifacts[role][name] = body
        summary = json.loads(artifacts[role]['screen/summary.json'])
        summaries[role] = summary
        assert summary['identity'] == launch[role] and summary['role'] == role
        assert summary['namespace_cold_start_included'] is False and summary['matched_vendor_measured'] is False
        limit = (8 * 1024**3 if role == 'server' else 512 * 1024**2)
        group = summary['cgroup']
        assert int(group['memory.max']) == limit and int(group['memory.peak']) < limit
        assert int(group['memory.swap.max']) == int(group['memory.swap.peak']) == 0
        assert 'oom_kill 0' in group['memory.events'] and 'oom 0' in group['memory.events']
        address = (4 * 1024**3 if role == 'server' else 1024**3)
        assert summary['address_space_limit'] == [address, address]
        assert summary['cpu_affinity'] == ([0, 1, 2, 3] if role == 'server' else [0, 1])
        instance = ec2.describe_instances(InstanceIds=[launch[role]['instance_id']])['Reservations'][0]['Instances'][0]
        assert instance['State']['Name'] == 'terminated' and instance['InstanceLifecycle'] == 'spot'
        if 'PrivateIpAddress' in instance:
            assert instance['PrivateIpAddress'] == launch[role]['private_ip']
        # Terminated EC2 responses can omit IPs; authenticated IMDS role receipts above retain them.
        assert instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    assert summaries['server']['decisions'] == summaries['client']['decisions']
    decisions = summaries['client']['decisions']
    assert [row['cell'] for row in decisions] == list(range(len(decisions))) and 0 < len(decisions) <= 8
    runs = []
    for decision in decisions:
        cell = decision['cell']
        item = config['items'][cell // 4]
        k = config['setting_order'][cell % 4]
        assert decision['k'] == k and decision['dataset'] == item['dataset']
        def artifact(role, name):
            return json.loads(artifacts[role]['screen/' + name])
        ready = artifact('server', f'ready{cell}.json')
        assert ready == artifact('client', f'ready{cell}.json') == json.loads(remote(prefix + f'/ready/{cell}.json'))
        assert ready['authority'] == item['authority'] and ready['cell'] == cell and ready['k'] == k
        assert ready['dataset'] == item['dataset'] and ready['instance_id'] == launch['server']['instance_id']
        assert ready['endpoint'] == 'http://' + launch['server']['private_ip'] + ':8080' and ready['namespace_ready_ms'] > 0
        headers = [json.loads(line) for line in artifacts['server'][f'screen/cell{cell}-server.log'].splitlines()
                   if line.startswith(b'{')]
        assert len(headers) == 1 and headers[0]['phase'] == 'ready'
        header = headers[0]
        assert header['authority'] == item['authority']
        assert header['listen'] == launch['server']['private_ip'] + ':8080'
        assert header['head_read_wall_ns'] > 0 and header['remote_open_wall_ns'] > 0
        done = artifact('client', f'done{cell}.json')
        assert done == json.loads(remote(prefix + f'/done/{cell}.json'))
        closed = artifact('server', f'closed{cell}.json')
        assert closed == json.loads(remote(prefix + f'/closed/{cell}.json'))
        assert done['cell'] == closed['cell'] == cell
        assert done['instance_id'] == launch['client']['instance_id'] and closed['instance_id'] == launch['server']['instance_id']
        assert closed['intentional_stop'] is True
        body = artifacts['client'][f'screen/cell{cell}/result.json']
        assert sha(body) == done['result_sha256']
        result = json.loads(body)
        assert result['authority'] == item['authority'] and result['identity_parity_valid']
        assert result['endpoint'] == ready['endpoint'] and result['split'] == item['query_split']
        assert result['offered_count'] == 64 and result['offered_qps'] == 8
        assert result['scheduled_duration_ns'] == 8000000000 and result['elapsed_including_drain_ns'] >= 8000000000
        peer = artifact('client', f'config{cell}.json')
        assert peer['authority'] == item['authority'] and peer['k'] == k
        assert peer['code_sha256'] == config['code_sha256'] and peer['count'] == 64
        for name, source in [('requests', 'requests'), ('reference', 'reference-k' + str(k))]:
            ident = item['inputs'][source]
            body = remote(ident['key'])
            assert len(body) == ident['bytes'] and sha(body) == ident['sha256']
            assert peer['inputs'][name]['bytes'] == ident['bytes'] and peer['inputs'][name]['sha256'] == ident['sha256']
            if name == 'reference':
                refs = [json.loads(line) for line in body.splitlines()][1:-1]
            else:
                assert len(body.splitlines()) == 64
        assert len(refs) == 64
        ident = item['inputs']['truth']
        head = s3.head_object(Bucket=config['bucket'], Key=ident['key'])
        assert head['ContentLength'] == ident['bytes'] and head['Metadata']['sha256'] == ident['sha256']
        truth_body = s3.get_object(Bucket=config['bucket'], Key=ident['key'], Range='bytes=0-25599', IfMatch=head['ETag'])['Body'].read()
        assert len(truth_body) == 25600 and sha(truth_body) == ident['range_sha256']
        assert peer['inputs']['truth']['bytes'] == 25600 and peer['inputs']['truth']['sha256'] == sha(truth_body)
        truth = [struct.unpack_from('<100I', truth_body, q * 400) for q in range(64)]
        samples = [json.loads(line) for line in artifacts['client'][f'screen/cell{cell}/http.jsonl'].splitlines()]
        assert len(samples) == 64 and dict(Counter(row['outcome'] for row in samples)) == result['outcomes']
        hits = gets = count_bytes = failed = 0
        times = []
        for q, row in enumerate(samples):
            assert row['query_ordinal'] == q and row['integrity_error'] is None
            response = row.get('response', {})
            gets += response.get('submitted_gets') or 0
            count_bytes += response.get('verified_bytes') or 0
            failed += response.get('failed_gets') or 0
            if row['outcome'] != 'success':
                continue
            assert row['status'] == 200 and row['physical_counters_complete']
            assert response['authority'] == item['authority']
            for field in ('ids', 'ranges', 'planned_bytes', 'submitted_gets', 'verified_bytes', 'failed_gets'):
                assert response[field] == refs[q][field]
            assert row['returned_hits'] == len(set(response['ids']) & set(truth[q][:k]))
            hits += row['returned_hits']
            times.append((row['completed_ns'] - row['started_ns']) / 1e6)
        assert result['successful_count'] == len(times) and result['mean_offered_recall'] == hits / (64 * k)
        assert result['achieved_successful_qps'] == len(times) * 1e9 / result['elapsed_including_drain_ns']
        assert (gets, count_bytes, failed) == (result['known_submitted_gets'], result['known_verified_bytes'], result['known_failed_gets'])
        for name, p in [('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99)]:
            if times:
                assert abs(result['successful_incoming_http_ms'][name] - percentile(times, p)) < 1e-9
        passed = k != 10 or (len(times) == 64 and hits / (64 * k) >= .95 and result['successful_incoming_http_ms']['p90'] < 444 and result['achieved_successful_qps'] >= 8)
        assert done['gate_passed'] == decision['gate_passed'] == passed
        runs.append(dict(cell=cell, dataset=item['dataset'], k=k, hits=hits, denominator=64*k,
            successful_count=len(times), incoming_http_ms=result['successful_incoming_http_ms'], successful_qps=result['achieved_successful_qps'],
            namespace_ready_ms=ready['namespace_ready_ms'], head_read_ms=header['head_read_wall_ns']/1e6,
            remote_open_ms=header['remote_open_wall_ns']/1e6, data_gets=gets, verified_data_bytes=count_bytes))
    passed = len(decisions) == 8 and all(row['gate_passed'] for row in decisions)
    assert summaries['client']['all_cells_completed'] == summaries['server']['all_cells_completed'] == (len(decisions) == 8)
    assert passed or decisions[-1]['gate_passed'] is False
    report = dict(valid_measurement=True, decision='GO' if passed else 'FAIL', runs=runs,
        state='terminated', matched_vendor_measured=False, namespace_cold_start_included=False,
        config_sha256=sha(config_body), source_archive_sha256=launch['source_archive_sha256'],
        all_offered_requests=64*len(runs), all_successful_requests=sum(run['successful_count'] for run in runs),
        physical_data_gets=sum(run['data_gets'] for run in runs), verified_data_bytes=sum(run['verified_data_bytes'] for run in runs),
        role_resources={role: dict(cgroup_peak_bytes=int(summary['cgroup']['memory.peak']), swap_peak_bytes=0,
            address_space_limit=summary['address_space_limit'], cpu_affinity=summary['cpu_affinity'])
            for role, summary in summaries.items()},
        compute_cost_estimate_usd=close['compute_cost_estimate_usd'], cost_scope=close['cost_scope'])
    (directory / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'a0001')
