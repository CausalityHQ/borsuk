"""Independent fixed-block reduction; source/remote authority is a separate gate."""
import copy
import gzip
import json
import hashlib
import io
import struct
import tarfile
import re

import boto3
from pathlib import Path
import sys

from scripts.verify_native_cold_first_query import reduce_records
from scripts.check_native_startup_stats import validate

ROOT = Path('docs/research/native-union-20260928')
BLOCKS = (('control', 0, 32), ('candidate', 0, 32),
          ('candidate', 32, 64), ('control', 32, 64))


def reduce_blocks(blocks, inputs, items):
    assert len(blocks) == 4
    records = {arm: {item['dataset']: [] for item in items} for arm in ('control', 'candidate')}
    previous = 0
    for block, (arm, begin, end) in enumerate(BLOCKS):
        rows = blocks[block]
        expected = [(item['dataset'], q) for item in items for q in range(begin, end)]
        assert len(rows) == 64
        assert [(row['dataset'], row['query_ordinal']) for row in rows] == expected
        for row in rows:
            assert row['block'] == block and row['arm'] == arm
            assert previous <= row['started_ns'] <= row['completed_ns']
            previous = row['completed_ns']
            records[arm][row['dataset']].append(row)
    panels, peaks = {}, {}
    for arm, datasets in records.items():
        panels[arm], peaks[arm] = {}, {}
        for item in items:
            dataset = item['dataset']
            rows = datasets[dataset]
            result, peak = reduce_records(rows, *inputs[dataset], item)
            result.pop('serial_span_ns'); result.pop('serial_cold_calls_per_second')
            result['split'] = item['query_split']
            heads, gets, buffers = 0, 0, []
            for row in rows:
                header = row['native_header']
                stats = header['remote_open_stats']
                accounting = dict(row['metadata'])
                if arm == 'control':
                    accounting.update(logical_metadata_head_requests=0,
                        logical_metadata_get_requests=9, payload_buffer_bound_bytes=None)
                    gets += 9
                else:
                    nhead, nget, bounds = 0, 0, []
                    for entry in stats['metadata']:
                        for field in ('head_wall_ns', 'logical_head_requests', 'logical_get_requests', 'payload_buffer_bound_bytes'):
                            assert type(entry[field]) is int and entry[field] >= 0
                        assert entry['logical_head_requests'] == 1
                        size = entry['bytes']
                        count = (size+8388607)//8388608
                        bound = min(size, 33554432)
                        assert entry['logical_get_requests'] == count
                        assert entry['payload_buffer_bound_bytes'] == bound
                        if size > 8388608: assert entry['get_wall_ns'] == 0
                        nhead += 1; nget += count; bounds.append(bound)
                    assert sum(e['head_wall_ns']+e['get_wall_ns']+e['stream_wall_ns'] for e in stats['metadata']) <= stats['staging_wall_ns']
                    accounting.update(logical_metadata_head_requests=nhead,
                        logical_metadata_get_requests=nget, payload_buffer_bound_bytes=max(bounds))
                    heads += nhead; gets += nget; buffers.append(max(bounds))
                assert accounting == row['transfer_accounting']
            result.update(logical_metadata_head_requests=heads, logical_metadata_get_requests=gets,
                payload_buffer_bound_bytes=max(buffers) if buffers else None)
            panels[arm][dataset] = result
            peaks[arm][dataset] = peak
    quality = all(p['quality_gate_passed'] for datasets in panels.values() for p in datasets.values())
    deltas = {item['dataset']: panels['candidate'][item['dataset']]['cold_start_to_first_http_response_ms']['p90']-
        panels['control'][item['dataset']]['cold_start_to_first_http_response_ms']['p90'] for item in items}
    paired = {item['dataset']: [records['candidate'][item['dataset']][q]['cold_start_to_first_http_response_ns']-
        records['control'][item['dataset']][q]['cold_start_to_first_http_response_ns'] for q in range(64)] for item in items}
    span = blocks[-1][-1]['completed_ns']-blocks[0][0]['started_ns']
    return dict(panels=panels, native_peak_rss_bytes=peaks, quality_gate_passed=quality,
        candidate_minus_control_cold_p90_ms=deltas, paired_candidate_minus_control_cold_ns=paired,
        diagnostic_gate_passed=quality and all(delta < 0 for delta in deltas.values()),
        published_context_gate_passed=all(p['published_context_gate_passed'] for p in panels['candidate'].values()),
        serial_campaign_span_ns=span, serial_campaign_cold_calls_per_second=256e9/span)


def self_check():
    # Closed original records supply valid shape/quality only. Block clocks and
    # candidate transfer counters below are synthetic, not a new measurement.
    config = json.loads((ROOT/'cold-first-query-config.json').read_text())
    original = {item['dataset']: [json.loads(line) for line in gzip.decompress(
        (ROOT/f"cold-first-query/a0001/screen/{item['dataset'].lower()}-records.jsonl.gz").read_bytes()).splitlines()]
        for item in config['items']}
    inputs = {}
    for item in config['items']:
        rows = original[item['dataset']]
        # Use each response's IDs as synthetic GT. Actual closed quality is not
        # recomputed or claimed by this synthetic test.
        requests = [dict(query=[1.]+[0.]*767) for _ in rows]
        references = [row['response'] for row in rows]
        truth = [row['response']['ids']+list(range(100, 190)) for row in rows]
        inputs[item['dataset']] = requests, references, truth
    clock = 1
    blocks = []
    for block, (arm, begin, end) in enumerate(BLOCKS):
        rows = []
        for item in config['items']:
            for q in range(begin, end):
                row = copy.deepcopy(original[item['dataset']][q])
                offset = clock-row['started_ns']
                for key in ('started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns'): row[key] += offset
                if arm == 'control':
                    for key in ('successful_connect_attempt_ns', 'connected_ns', 'completed_ns',
                                'before_successful_connect_attempt_ns', 'cold_start_to_first_http_response_ns'): row[key] += 10000000
                clock = row['completed_ns']+1000000
                row.update(block=block, arm=arm, returned_hits=10)
                request = json.dumps(dict(query=inputs[item['dataset']][0][q]['query'], k=10, **item['authority']), separators=(',', ':')).encode()
                from scripts.verify_native_cold_first_query import sha
                row.update(request_sha256=sha(request), request_bytes=len(request))
                stats = row['native_header']['remote_open_stats']
                if arm == 'candidate':
                    for entry in stats['metadata']:
                        entry.update(head_wall_ns=1, logical_head_requests=1,
                            logical_get_requests=(entry['bytes']+8388607)//8388608,
                            payload_buffer_bound_bytes=min(entry['bytes'],33554432))
                        if entry['bytes'] > 8388608: entry['get_wall_ns'] = 0
                    row['metadata'] = validate(stats, item['metadata_files'], row['native_header']['remote_open_wall_ns'])
                row['native_server_log'] = json.dumps(row['native_header'])+'\n'
                row['transfer_accounting'] = dict(row['metadata'],
                    logical_metadata_head_requests=9 if arm == 'candidate' else 0,
                    logical_metadata_get_requests=37 if arm == 'candidate' else 9,
                    payload_buffer_bound_bytes=33554432 if arm == 'candidate' else None)
                rows.append(row)
        blocks.append(rows)
    result = reduce_blocks(blocks, inputs, config['items'])
    assert result['quality_gate_passed'] and result['diagnostic_gate_passed'] and not result['published_context_gate_passed']
    assert all(delta == -10 for delta in result['candidate_minus_control_cold_p90_ms'].values())
    for mutation in ('order', 'arm', 'buffer', 'timing'):
        bad = copy.deepcopy(blocks)
        if mutation == 'order': bad[0][0], bad[0][1] = bad[0][1], bad[0][0]
        elif mutation == 'arm': bad[0][0]['arm'] = 'candidate'
        elif mutation == 'buffer': bad[1][0]['native_header']['remote_open_stats']['metadata'][-1]['payload_buffer_bound_bytes'] += 1
        else: bad[3][0]['started_ns'] = 0
        try: reduce_blocks(bad, inputs, config['items'])
        except AssertionError: pass
        else: raise AssertionError('invalid '+mutation+' accepted')
    print('independent paired reducer PASS (synthetic256/order/arm/buffer/timing guards)')


def sha(body): return hashlib.sha256(body).hexdigest()


def main(attempt):
    from scripts import launch_native_metadata_ranges_cold_spot as campaign
    from scripts.launch_native_peer_1m_spot import BUCKET, REGION
    directory = ROOT/'metadata-ranges-cold'/attempt
    launch = json.loads((directory/'aws-launch.json').read_text())
    close = json.loads((directory/'aws-closeout.json').read_text())
    session = boto3.Session(profile_name='causality', region_name=REGION)
    s3, ec2 = session.client('s3'), session.client('ec2')
    def remote(key): return s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    prefix = launch['prefix']
    assert prefix == 'research/native-union/20260930/metadata-ranges-cold-'+attempt
    assert json.loads(remote(prefix+'/launch.json')) == launch
    reservation = json.loads(remote(prefix+'/reservation.json'))
    assert reservation == json.loads((directory/'aws-reservation.json').read_text())
    terminal_body = remote(prefix+'/terminal.json')
    assert terminal_body == (directory/'aws-terminal.json').read_bytes()
    terminal = json.loads(terminal_body)
    assert terminal['schema'] == reservation['schema'] == campaign.SCHEMA
    assert terminal['phase'] == terminal['status'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == launch['instance_id']
    assert close['nodes'] == {'0': {'instance_id': launch['instance_id']}}
    for key in ('source_commit', 'source_archive_sha256'):
        assert terminal[key] == reservation[key] == launch[key]
    archive = remote('research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')
    assert sha(archive) == launch['source_archive_sha256']
    artifacts = {}
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS)
    for name, identity in terminal['artifacts'].items():
        body = remote(prefix+'/artifacts/'+name)
        assert sha(body) == identity['sha256'] and len(body) == identity['bytes']
        assert body == gzip.decompress((directory/(name+'.gz')).read_bytes())
        artifacts[name] = body
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar:
        def archived(name): return tar.extractfile(name).read()
        config_body = archived(str(campaign.CONFIG))
        assert config_body == campaign.CONFIG.read_bytes()
        assert sha(config_body) == reservation['config_sha256']
        config = json.loads(config_body)
        assert config['schema'] == 'borsuk-native-metadata-ranges-cold-v1'
        assert config['count'] == 64 and config['k'] == 10 and config['ann_queries'] == 256
        assert config['dataset_order'] == ['ReLAION', 'CoHere']
        assert config['blocks'] == [dict(arm=a, begin=b, end=e) for a,b,e in BLOCKS]
        assert config['bucket'] == BUCKET and config['region'] == REGION
        assert config['worker_limit_seconds'] == 1500 and config['machine_limit_seconds'] == 4200
        assert config['gates'] == dict(recall_at_10_minimum=.95, all_calls_success=True,
            source_scorer_ordered_id_physical_parity=True, candidate_cold_p90_lower_than_control=True,
            cold_start_to_first_http_response_p90_ms_exclusive_maximum=444)
        qualification = reservation['qualification']
        assert qualification == json.loads(artifacts['source-qualification.json'])
        assert qualification['config_sha256'] == sha(config_body)
        assert qualification['artifact_roster_sha256'] == sha(json.dumps(campaign.ARTIFACTS, separators=(',', ':')).encode())
        assert set(qualification['code_sha256']) == set(config['code_sha256']) | {
            'scripts/launch_native_metadata_ranges_cold_spot.py', 'scripts/check_native_metadata_ranges_build.py'}
        for name, digest in qualification['code_sha256'].items(): assert sha(archived(name)) == digest
        for name, digest in config['code_sha256'].items(): assert sha(archived(name)) == digest
        for name in ('scripts/launch_native_metadata_ranges_cold_spot.py',
                     'scripts/check_native_metadata_ranges_build.py', 'scripts/check_native_startup_build.py',
                     'scripts/launch_native_peer_1m_spot.py', 'scripts/launch_native_startup_profile_spot.py',
                     'scripts/launch_v174_relaid_bind_compile_spot.py'):
            assert archived(name) == Path(name).read_bytes()
        frozen = config['frozen_native_qualification']
        assert sha(archived(frozen['path'])) == frozen['sha256']
        prior = json.loads(archived(frozen['path']))
        assert prior['valid_diagnostic'] and prior['diagnostic_gate_passed'] and prior['state'] == 'terminated'
        for key in ('source_commit', 'source_archive_sha256', 'terminal_sha256'): assert prior[key] == frozen[key]
        control_terminal_body = remote('research/native-union/20260929/arm-sha-startup-a0001/terminal.json')
        assert sha(control_terminal_body) == frozen['terminal_sha256']
        control_terminal = json.loads(control_terminal_body)
        assert control_terminal['status'] == 'complete' and control_terminal['exit_code'] == 0
        for name in campaign.FROZEN_FILES:
            identity = control_terminal['artifacts'][name]
            assert sha(artifacts['control/'+name]) == identity['sha256']
            assert len(artifacts['control/'+name]) == identity['bytes']
        control = json.loads(artifacts['control/boundary-check.json'])
        control_compiled = json.loads(artifacts['control/compiled-source.json'])
        assert control_compiled == prior['compiled_native_sha256'] == control['compiled_native_sha256']
        assert control['qualified'] and control['green_status'] == control['release_status'] == 0
        assert control['binary_sha256'] == sha(artifacts['control/binaries/two_bit_http']) == config['control_binary']['sha256']
        assert len(artifacts['control/binaries/two_bit_http']) == config['control_binary']['bytes']
        old_qualified = json.loads(artifacts['control/source-qualification.json'])
        for name, digest in old_qualified['code_sha256'].items(): assert sha(archived(name)) == digest
        hashes = {m.name: sha(archived(m.name)) for m in tar.getmembers() if m.isfile() and
            (m.name.endswith('.rs') or Path(m.name).name in ('Cargo.toml', 'Cargo.lock'))}
        identity = sha(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode())
        assert len(hashes) == qualification['source_file_count'] == control['source_file_count'] == 395
        assert identity == qualification['source_identity_sha256']
        stage = 'crates/borsuk/src/object_native_generation.rs'
        delta = config['reviewed_native_delta_sha256']
        assert delta == qualification['reviewed_native_delta_sha256'] == {stage: hashes[stage]}
        assert hashes[stage] != control_compiled[stage]
        baseline = dict(hashes); baseline[stage] = control_compiled[stage]
        baseline_identity = sha(json.dumps(baseline, sort_keys=True, separators=(',', ':')).encode())
        assert baseline_identity == control['source_identity_sha256'] == qualification['control_source_identity_sha256']
        candidate = json.loads(artifacts['boundary-check.json'])
        compiled = json.loads(artifacts['compiled-source.json'])
        assert set(compiled) == set(control_compiled)
        assert compiled == qualification['compiled_native_sha256'] == candidate['compiled_native_sha256']
        assert candidate['qualified'] and candidate['green_status'] == candidate['release_status'] == 0
        assert candidate['source_identity_sha256'] == identity and candidate['source_file_count'] == 395
        assert candidate['reviewed_native_delta_sha256'] == delta and candidate['native_rebuilt'] is True
        assert candidate['current_full_suite_pass_claim'] is qualification['current_full_suite_pass_claim'] is False
        assert candidate['binary_sha256'] == sha(artifacts['binaries/two_bit_http'])
        assert candidate['compiled_http_sha256'] == compiled['crates/borsuk/examples/two_bit_http.rs'] == control['compiled_http_sha256']
        for name, digest in compiled.items():
            assert hashes[name] == sha(artifacts['compiled-source/'+name]) == digest
        assert config['items'] == json.loads(archived(str(ROOT/'cold-first-query-config.json')))['items']
    expected = campaign.user_data(launch['source_commit'], sha(archive),
        'research/native-library-check/sources/'+sha(archive)+'.tar.gz', prefix, qualification)
    assert expected == (directory/'aws-user-data.sh').read_text()
    for name in ('object-native', 'generation', 'http', 'source'):
        assert b'0 failed;' in artifacts[name+'.log'] and b'ok. 0 passed;' not in artifacts[name+'.log']
    for name in ('authentication_backend_matches_sha256_known_answers',
                 'streams_source_records_and_rejects_wrong_identity_order_budget_and_overwrite',
                 'opens_authenticated_plane_and_rejects_wrong_generation_corruption_and_budget'):
        assert ('test '+name+' ... ok').encode() in artifacts['source.log']
    for name in ('rustc-version.txt', 'cargo-version.txt'):
        assert artifacts[name] == artifacts['control/'+name]
    for name in ('arm-feature-tree.txt', 'x86-feature-tree.txt'):
        def normalized(body): return [re.sub(r'\(/[^)]*/repo/', '(__repo__/', line).removesuffix(' (*)') for line in body.decode().splitlines()]
        assert normalized(artifacts[name]) == normalized(artifacts['control/'+name])
    assert b'sha2 feature "asm"' in artifacts['arm-feature-tree.txt'] and b'force-soft' not in artifacts['arm-feature-tree.txt']
    assert b'sha2 feature "asm"' not in artifacts['x86-feature-tree.txt']
    features = [line.split(b':',1)[1].split() for line in artifacts['cpuinfo.txt'].splitlines() if line.split(b':',1)[0].strip() == b'Features']
    assert features and all(b'sha2' in flags for flags in features)
    groups = {}
    for name, limit in [('boundary-cgroup.json',10*1024**3),('profile-cgroup.json',8*1024**3)]:
        group = json.loads(artifacts[name]); groups[name] = group
        assert int(group['memory.max']) == limit and 0 < int(group['memory.peak']) < limit
        assert int(group['memory.swap.max']) == int(group['memory.swap.peak']) == 0
        events = dict(line.split() for line in group['memory.events'].splitlines())
        assert events['oom'] == events['oom_kill'] == '0' and group['cpu_affinity'] == [0,1,2,3]
    assert groups['profile-cgroup.json']['rlimit_as_bytes'] == [4*1024**3]*2
    inputs = {}
    for item in config['items']:
        bodies = {}
        for name in ('requests','reference-k10','truth'):
            ident = item['inputs'][name]; body = remote(ident['key'])
            assert len(body) == ident['bytes'] and sha(body) == ident['sha256']
            if 'range_bytes' in ident:
                body = body[ident['range_start']:ident['range_start']+ident['range_bytes']]
                assert len(body) == ident['range_bytes'] and sha(body) == ident['range_sha256']
            bodies[name] = body
        requests = [json.loads(line) for line in bodies['requests'].splitlines()]
        refs = [json.loads(line) for line in bodies['reference-k10'].splitlines()]
        assert len(requests) == 64 and len(refs) == 66 and len(bodies['truth']) == 25600
        assert refs[0]['top_k'] == 10 and refs[0]['declared_panel_count'] == refs[-1]['count'] == 64
        for key, value in item['authority'].items(): assert refs[0][key] == value
        assert [r['query_ordinal'] for r in requests] == [r['query_ordinal'] for r in refs[1:-1]] == list(range(64))
        truth = [struct.unpack_from('<100I', bodies['truth'], q*400) for q in range(64)]
        assert all(len(set(row)) == 100 and max(row) < 1000000 for row in truth)
        inputs[item['dataset']] = requests, refs[1:-1], truth
    blocks = [[json.loads(line) for line in artifacts[f'screen/block{b}-records.jsonl'].splitlines()] for b in range(4)]
    result = reduce_blocks(blocks, inputs, config['items'])
    summary = json.loads(artifacts['screen/summary.json'])
    for key, value in result.items():
        if key != 'native_peak_rss_bytes': assert summary[key] == value, key
    assert summary['ann_queries'] == summary['namespace_starts'] == 256 and summary['k'] == 10
    assert summary['blocks'] == config['blocks'] and summary['source_scorer_ordered_id_physical_parity'] is True
    assert summary['namespace_cold_start_included'] is True and summary['application_sq8_cache'] is False
    assert summary['cold_call_boundary'] == 'preencoded request; process launch through first HTTP response; refused TCP connects included'
    assert summary['s3_service_cache'] == 'uncontrolled' and summary['transport'] == 'loopback plain HTTP'
    assert summary['client_cpu_affinity'] == [4,5] and summary['native_cpu_affinity'] == [0,1,2,3]
    assert summary['matched_vendor_measured'] is summary['serial_cold_qps_is_offered_or_saturation_qps'] is False
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name'] == close['state'] == 'terminated' and instance['InstanceLifecycle'] == 'spot'
    assert instance['InstanceType'] == 'c7g.2xlarge' and instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    report = dict(valid_measurement=True, state='terminated', instance_id=launch['instance_id'],
        source_commit=launch['source_commit'], source_archive_sha256=sha(archive), config_sha256=sha(config_body),
        terminal_sha256=sha(terminal_body), reviewed_native_delta_sha256=delta,
        build_peak_bytes=int(groups['boundary-cgroup.json']['memory.peak']),
        cgroup_peak_bytes=int(groups['profile-cgroup.json']['memory.peak']), swap_peak_bytes=0,
        current_full_suite_pass_claim=False, matched_vendor_measured=False, offered_or_saturation_qps_measured=False, **result)
    (directory/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    else: main(sys.argv[1] if len(sys.argv) == 2 else 'a0001')
