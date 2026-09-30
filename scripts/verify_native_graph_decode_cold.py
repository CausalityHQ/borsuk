"""Authenticate paired native builds and independently reduce paged cold records."""
import copy
import gzip
import json
from pathlib import Path
import struct
import sys

from scripts import verify_native_paged_cold_first_query as paged
from scripts import run_native_graph_decode_cold as worker
from scripts.check_native_startup_build import source_identity

sha = paged.base.sha
BLOCKS = worker.paired.BLOCKS


def validate_manifest(archived, config):
    pointer = config['native_manifest']; body = archived[pointer['path']]
    assert sha(body) == pointer['sha256']
    manifest = json.loads(body)
    hashes = {name: sha(body) for name,body in archived.items()
              if name.endswith('.rs') or Path(name).name in ('Cargo.toml','Cargo.lock')}
    assert hashes == manifest['source_sha256'] and len(hashes) == manifest['source_file_count'] == 395
    assert source_identity(hashes) == manifest['candidate_identity']
    control_body = archived[manifest['control_graph']['path']]
    assert sha(control_body) == manifest['control_graph']['sha256']
    control = dict(hashes, **{worker.GRAPH:sha(control_body)})
    assert source_identity(control) == manifest['control_identity']
    assert {name for name in hashes if hashes[name] != control[name]} == {worker.GRAPH}
    assert len(config['compiled_sha256']) == 20
    assert config['compiled_sha256'] == {name:hashes[name] for name in config['compiled_sha256']}
    pointer = config['items_source']; body = archived[pointer['path']]
    assert sha(body) == pointer['sha256'] and config['items'] == json.loads(body)['items']
    assert config['schema'] == worker.SCHEMA and config['source_caps'] == worker.paged.SOURCE_CAPS
    return manifest, hashes, control


def reduce_blocks(blocks, inputs, items):
    assert len(blocks) == 4
    records = {arm:{item['dataset']:[] for item in items} for arm in ('control','candidate')}
    previous = 0
    for block,(arm,begin,end) in enumerate(BLOCKS):
        rows = blocks[block]
        assert [(r['dataset'],r['query_ordinal']) for r in rows] == [(i['dataset'],q) for i in items for q in range(begin,end)]
        for row in rows:
            assert row['block'] == block and row['arm'] == arm
            assert previous <= row['started_ns'] <= row['completed_ns']; previous = row['completed_ns']
            assert row['transfer_accounting'] == row['metadata']
            records[arm][row['dataset']].append(row)
    panels,peaks,decode = {},{},{}
    for arm,datasets in records.items():
        panels[arm],peaks[arm],decode[arm] = {},{},{}
        for item in items:
            dataset = item['dataset']; rows = datasets[dataset]
            result,peak = paged.reduce_records(rows,*inputs[dataset],item)
            result.pop('serial_span_ns');result.pop('serial_cold_calls_per_second')
            result.update(split=item['query_split'], payload_buffer_bound_bytes=max(r['metadata']['payload_buffer_bound_bytes'] for r in rows))
            panels[arm][dataset] = result;peaks[arm][dataset] = peak
            decode[arm][dataset] = {label:paged.base.percentile([r['native_header']['remote_open_stats']['decode_wall_ns']/1e6 for r in rows],p) for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}
    quality = all(p['quality_gate_passed'] for datasets in panels.values() for p in datasets.values())
    deltas = {i['dataset']:panels['candidate'][i['dataset']]['cold_start_to_first_http_response_ms']['p90']-panels['control'][i['dataset']]['cold_start_to_first_http_response_ms']['p90'] for i in items}
    paired = {i['dataset']:[records['candidate'][i['dataset']][q]['cold_start_to_first_http_response_ns']-records['control'][i['dataset']][q]['cold_start_to_first_http_response_ns'] for q in range(64)] for i in items}
    span = blocks[-1][-1]['completed_ns']-blocks[0][0]['started_ns']
    decode_gate = all(decode['candidate'][i['dataset']]['p50'] < decode['control'][i['dataset']]['p50'] for i in items)
    diagnostic = quality and all(delta < 0 for delta in deltas.values())
    return dict(panels=panels,native_peak_rss_bytes=peaks,decode_ms=decode,quality_gate_passed=quality,
        candidate_minus_control_cold_p90_ms=deltas,paired_candidate_minus_control_cold_ns=paired,
        diagnostic_gate_passed=diagnostic,decode_gate_passed=decode_gate,go=diagnostic and decode_gate,
        published_context_gate_passed=all(p['published_context_gate_passed'] for p in panels['candidate'].values()),
        serial_campaign_span_ns=span,serial_campaign_cold_calls_per_second=256e9/span)


def inputs_from_remote(config, remote, observed):
    inputs = {}
    for item in config['items']:
        bodies = {}
        for name in ('requests','reference-k10','truth'):
            ident = item['inputs'][name]; body = remote(ident['key'])
            assert len(body) == ident['bytes'] and sha(body) == ident['sha256']
            if 'range_bytes' in ident:
                body = body[ident['range_start']:ident['range_start']+ident['range_bytes']]
                assert len(body) == ident['range_bytes'] and sha(body) == ident['range_sha256']
            assert observed[item['dataset']][name]['bytes'] == len(body)
            assert observed[item['dataset']][name]['sha256'] == sha(body)
            bodies[name] = body
        requests = [json.loads(line) for line in bodies['requests'].splitlines()]
        refs = [json.loads(line) for line in bodies['reference-k10'].splitlines()]
        assert len(requests) == 64 and len(refs) == 66 and len(bodies['truth']) == 25600
        assert refs[0]['top_k'] == 10 and refs[0]['declared_panel_count'] == refs[-1]['count'] == 64
        assert [r['query_ordinal'] for r in requests] == [r['query_ordinal'] for r in refs[1:-1]] == list(range(64))
        for key,value in item['authority'].items(): assert refs[0][key] == value
        truth = [struct.unpack_from('<100I',bodies['truth'],q*400) for q in range(64)]
        assert all(len(set(row)) == 100 and max(row) < 1000000 for row in truth)
        inputs[item['dataset']] = requests,refs[1:-1],truth
    return inputs


def main(attempt):
    import boto3
    from scripts import launch_native_graph_decode_spot as campaign
    directory = campaign.ROOT/campaign.NAME/attempt
    session = boto3.Session(profile_name='causality',region_name=paged.base.REGION)
    s3,ec2 = session.client('s3'),session.client('ec2')
    def remote(key): return s3.get_object(Bucket=paged.base.BUCKET,Key=key)['Body'].read()
    launch,reservation,terminal,artifacts,archived,config = paged.authenticate_closed(directory,campaign,remote,validate_manifest)
    manifest,hashes,control = validate_manifest(archived,config)
    proof = reservation['qualification']
    assert proof['source_identity_sha256'] == manifest['candidate_identity']
    assert proof['control_source_identity_sha256'] == manifest['control_identity']
    assert proof['compiled_native_sha256'] == config['compiled_sha256']
    assert proof['control_compiled_native_sha256'] == {name:control[name] for name in campaign.COMPILED}
    assert proof['artifact_roster_sha256'] == sha(json.dumps(campaign.ARTIFACTS,separators=(',',':')).encode())
    for key in ('config_sha256','manifest_sha256','artifact_roster_sha256'): assert terminal[key] == proof[key]
    assert reservation['wall_seconds'] == campaign.WALL and reservation['compute_cap_usd'] == campaign.COMPUTE_CAP
    for arm,ids,prefix in [('candidate',hashes,''),('control',control,'control/')]:
        boundary = json.loads(artifacts[prefix+'boundary-check.json'])
        compiled = json.loads(artifacts[prefix+'compiled-source.json'])
        assert compiled == boundary['compiled_native_sha256'] == {name:ids[name] for name in campaign.COMPILED}
        assert boundary['source_identity_sha256'] == source_identity(ids) and boundary['source_file_count'] == 395
        assert boundary['qualified'] is True and boundary['green_status'] == boundary['release_status'] == 0
        assert boundary['arm'] == arm and boundary['same_worker_toolchain'] is True
        assert boundary['native_rebuilt'] is True and boundary['no_corpus_query'] is True
        assert boundary['focused_tests'] == [name for name,_ in campaign.CHECKS]
        for key,name in [('rustc_sha256','rustc-version.txt'),('cargo_sha256','cargo-version.txt'),('cpuinfo_sha256','cpuinfo.txt')]:
            assert boundary['sha_backend'][key] == sha(artifacts[prefix+name])
        if arm == 'control': assert boundary['current_full_suite_pass_claim'] is False
        assert boundary['binary_sha256'] == sha(artifacts[prefix+'binaries/two_bit_http'])
        assert boundary['binary_bytes'] == len(artifacts[prefix+'binaries/two_bit_http'])
        for name,digest in compiled.items(): assert sha(artifacts[prefix+'compiled-source/'+name]) == digest
        for name,_ in campaign.CHECKS:
            log = artifacts[prefix+name+'.log']
            assert b'0 failed;' in log and b'ok. 0 passed;' not in log
        assert ('test result: ok. '+str(8 if arm == 'candidate' else 7)+' passed; 0 failed;').encode() in artifacts[prefix+'graph.log']
        assert boundary['sha_backend']['arm_asm_selected'] is boundary['sha_backend']['cpu_sha2_capable'] is True
        assert boundary['sha_backend']['x86_asm_selected'] is False
        if arm == 'candidate':
            assert boundary['full_suite_status'] == 0 and boundary['current_full_suite_pass_claim'] is True
            command = boundary['full_suite_command']
            assert command[1:6] == ['test','--release','--locked','--workspace','--all-targets']
            assert command[6] == '--manifest-path' and command[7].endswith('/repo/Cargo.toml')
            assert command[8] == '--target-dir' and command[9].endswith('/target')
            assert command[10:] == ['--jobs','4'] and boundary['full_suite_runs'] == 1
            status = json.loads(artifacts['full-suite-status.json'])
            assert status['status'] == 0 and status['runs'] == 1 and status['arm'] == 'candidate'
            assert status['command'] == command and status['scope'] == boundary['full_suite_scope']
            assert status['current_full_suite_pass_claim'] is True
            log = artifacts['full-suite.log'];assert b'0 failed;' in log and b'FAILED' not in log
    for name in ('rustc-version.txt','cargo-version.txt','cpuinfo.txt','arm-feature-tree.txt','x86-feature-tree.txt'):
        assert artifacts[name] == artifacts['control/'+name]
    for name,limit in [('boundary-cgroup.json',10*1024**3),('profile-cgroup.json',8*1024**3)]:
        group = json.loads(artifacts[name])
        assert int(group['memory.max']) == limit and 0 < int(group['memory.peak']) < limit
        assert int(group['memory.swap.max']) == int(group['memory.swap.peak']) == 0
        events = dict(line.split() for line in group['memory.events'].splitlines())
        assert events['oom'] == events['oom_kill'] == '0' and group['cpu_affinity'] == [0,1,2,3]
        if name == 'profile-cgroup.json': assert group['rlimit_as_bytes'] == [4*1024**3]*2
    assert campaign.user_data(launch['source_commit'],launch['source_archive_sha256'],
        'research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz',launch['prefix'],proof) == (directory/'aws-user-data.sh').read_text()
    blocks = [[json.loads(line) for line in artifacts[f'screen/block{b}-records.jsonl'].splitlines()] for b in range(4)]
    summary = json.loads(artifacts['screen/summary.json'])
    result = reduce_blocks(blocks,inputs_from_remote(config,remote,summary['inputs']),config['items'])
    for key,value in result.items():
        if key not in ('native_peak_rss_bytes','go'): assert summary[key] == value,key
    assert summary['ann_queries'] == summary['namespace_starts'] == 256 and summary['k'] == 10
    assert summary['blocks'] == config['blocks'] and summary['source_caps'] == worker.paged.SOURCE_CAPS
    assert summary['client_cpu_affinity'] == [4,5] and summary['native_cpu_affinity'] == [0,1,2,3]
    assert summary['namespace_cold_start_included'] is True and summary['application_sq8_cache'] is False
    assert summary['s3_service_cache'] == 'uncontrolled' and summary['transport'] == 'loopback plain HTTP'
    assert summary['cold_call_boundary'] == 'preencoded request; process launch through first HTTP response; refused TCP connects included'
    assert summary['source_scorer_ordered_id_physical_parity'] is True
    assert summary['matched_vendor_measured'] is summary['serial_cold_qps_is_offered_or_saturation_qps'] is False
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    close = json.loads((directory/'aws-closeout.json').read_bytes())
    assert instance['State']['Name'] == close['state'] == 'terminated'
    assert close['nodes'] == {'0':dict(instance_id=launch['instance_id'])}
    assert instance['InstanceLifecycle'] == 'spot' and instance['InstanceType'] == 'c7g.2xlarge'
    assert instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    report = dict(valid_measurement=True,state='terminated',source_commit=launch['source_commit'],
        source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=sha((directory/'aws-terminal.json').read_bytes()),
        config_sha256=reservation['config_sha256'],candidate_native_identity=manifest['candidate_identity'],
        control_native_identity=manifest['control_identity'],candidate_full_suite_verified=True,
        matched_vendor_measured=False,total_cost_measured=False,offered_or_saturation_qps_measured=False,**result)
    (directory/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))


def runtime_check():
    import os
    import tempfile
    from unittest.mock import patch
    path = Path('docs/research/source-paging-20260930/decode/config.json')
    config = json.loads(path.read_bytes())
    manifest = json.loads(Path(config['native_manifest']['path']).read_bytes())
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp); binary = root/'binary'; binary.write_bytes(b'synthetic fixture binary')
        proofs = {}
        for arm in ('candidate','control'):
            compiled = dict(config['compiled_sha256'])
            if arm == 'control': compiled[worker.GRAPH] = manifest['control_graph']['sha256']
            proofs[arm] = dict(qualified=True,green_status=0,release_status=0,arm=arm,
                same_worker_toolchain=True,source_identity_sha256=manifest[arm+'_identity'],
                source_file_count=395,binary_sha256=sha(binary.read_bytes()),binary_bytes=binary.stat().st_size,
                compiled_native_sha256=compiled,sha_backend=dict(arm_asm_selected=True,
                    cpu_sha2_capable=True,x86_asm_selected=False))
        for mutation in (None,'compiled','control','binary','backend','env'):
            shaped = copy.deepcopy(proofs)
            if mutation == 'compiled': shaped['candidate']['compiled_native_sha256'] = {}
            elif mutation == 'control': shaped['control']['source_identity_sha256'] = '0'*64
            elif mutation == 'binary': shaped['candidate']['binary_bytes'] += 1
            elif mutation == 'backend': shaped['candidate']['sha_backend']['x86_asm_selected'] = True
            for arm,proof in shaped.items(): (root/(arm+'.json')).write_text(json.dumps(proof))
            argv = ['worker',str(path),sha(path.read_bytes()),str(binary),str(root/'candidate.json'),
                str(binary),str(root/'control.json'),str(root/'screen')]
            with patch.object(sys,'argv',argv),patch.object(worker.os,'sched_getaffinity',return_value={4,5}), \
                 patch.dict(os.environ,TOKIO_WORKER_THREADS='8' if mutation == 'env' else '4',
                    AWS_MAX_ATTEMPTS='1',BORSUK_NATIVE_MEMORY_BYTES='1073741824'),patch.object(worker,'run') as run:
                if mutation is None:
                    worker.main();run.assert_called_once()
                else:
                    try: worker.main()
                    except AssertionError: pass
                    else: raise AssertionError('invalid runtime proof accepted: '+mutation)
                    run.assert_not_called()
    print('PASS paired runtime proof/env boundary before profiling')


def self_check():
    from unittest.mock import patch
    worker.self_check()
    runtime_check()
    paged.authentication_check()  # Original manifest path remains unchanged.
    config = json.loads(Path('docs/research/source-paging-20260930/decode/config.json').read_bytes())
    manifest = json.loads(Path(config['native_manifest']['path']).read_bytes())
    names = [*manifest['source_sha256'],config['native_manifest']['path'],manifest['control_graph']['path'],config['items_source']['path']]
    archived = {name:Path(name).read_bytes() for name in names}
    validate_manifest(archived,config)
    for name in (worker.GRAPH,manifest['control_graph']['path'],config['native_manifest']['path']):
        changed = dict(archived);changed[name] += b'changed'
        try: validate_manifest(changed,config)
        except AssertionError: pass
        else: raise AssertionError('changed native authority accepted: '+name)
    original = {i['dataset']:[json.loads(line) for line in gzip.decompress(Path(
        'docs/research/source-paging-20260930/cold/a0004/screen/'+i['dataset'].lower()+'-records.jsonl.gz').read_bytes()).splitlines()] for i in config['items']}
    inputs = {}
    for item in config['items']:
        rows = original[item['dataset']]
        inputs[item['dataset']] = ([dict(query=[1.]+[0.]*767) for _ in rows],
            [r['response'] for r in rows],[r['response']['ids']+list(range(100)) for r in rows])
    blocks = []; clock = 1
    for block,(arm,begin,end) in enumerate(BLOCKS):
        rows = []
        for item in config['items']:
            for q in range(begin,end):
                row = copy.deepcopy(original[item['dataset']][q])
                delta = clock-row['started_ns']
                for key in ('started_ns','successful_connect_attempt_ns','connected_ns','completed_ns'): row[key] += delta
                clock = row['completed_ns']+1
                request = json.dumps(dict(query=inputs[item['dataset']][0][q]['query'],k=10,**item['authority']),separators=(',',':')).encode()
                row.update(block=block,arm=arm,request_sha256=sha(request),request_bytes=len(request),
                    returned_hits=10,truth_at_10=row['response']['ids'],transfer_accounting=row['metadata'])
                rows.append(row)
        blocks.append(rows)
    result = reduce_blocks(blocks,inputs,config['items'])
    assert result['quality_gate_passed'] and not result['go'] and not result['decode_gate_passed']
    assert all(delta == 0 for delta in result['candidate_minus_control_cold_p90_ms'].values())
    for kind in ('order','arm','source','truth','cleanup'):
        bad = copy.deepcopy(blocks)
        if kind == 'order': bad[0][0],bad[0][1] = bad[0][1],bad[0][0]
        elif kind == 'arm': bad[0][0]['arm'] = 'candidate'
        elif kind == 'source': bad[0][0]['response']['source_verified_bytes'] = 67108865
        elif kind == 'truth': bad[0][0]['truth_at_10'] = []
        else: bad[0][0]['native_close']['intentional_stop'] = False
        try: reduce_blocks(bad,inputs,config['items'])
        except AssertionError: pass
        else: raise AssertionError('accepted '+kind)
    # Compare panels against the actual runtime reducer under the paged scope.
    with worker.paged.scoped_runner(sys.argv):
        for arm in ('control','candidate'):
            for item in config['items']:
                rows = [r for b in blocks for r in b if r['arm'] == arm and r['dataset'] == item['dataset']]
                panel = worker.paired.cold.reduce_panel(rows)
                panel.pop('serial_span_ns');panel.pop('serial_cold_calls_per_second')
                expected = result['panels'][arm][item['dataset']]
                assert all(expected[key] == value for key,value in panel.items())
    print('PASS independent ABBA paged reducer; unchanged control is FAIL; order/arm/source/truth/cleanup guards')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    else:
        assert len(sys.argv) == 2
        main(sys.argv[1])
