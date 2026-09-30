"""Authenticate a terminal two-build geometry campaign before reducing records."""
import gzip
import io
import json
from pathlib import Path
import re
import struct
import sys
import tarfile

import boto3

from scripts.check_native_startup_build import FOCUSED_ARM, SOURCE_TESTS, sha, source_identity
from scripts.verify_native_metadata_ranges_cold import reduce_blocks
from scripts.launch_native_peer_1m_spot import BUCKET, REGION

ROOT = Path('docs/research/native-union-20260928')
STAGE = 'crates/borsuk/src/object_native_generation.rs'
COMPILED = (*FOCUSED_ARM, 'crates/borsuk/src/unit_centroid_graph.rs')
CONTROL_COMMIT = '1224634bfb121eef533789a422f1c06fd7adf142'
CONTROL_IDENTITY = '6566a30c7ccfbf5ec8a8d4481b2568fc020b67e831e5d186a94f5025ab156ec2'


def main(attempt):
    from scripts import launch_native_metadata_geometry_spot as campaign
    directory = ROOT/'metadata-geometry'/attempt
    launch = json.loads((directory/'aws-launch.json').read_text())
    close = json.loads((directory/'aws-closeout.json').read_text())
    session = boto3.Session(profile_name='causality', region_name=REGION)
    s3, ec2 = session.client('s3'), session.client('ec2')
    def remote(key): return s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    prefix = launch['prefix']
    assert prefix == campaign.PREFIX+attempt
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
    archive_key = 'research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz'
    archive = remote(archive_key)
    assert sha(archive) == launch['source_archive_sha256']
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS)
    artifacts = {}
    for name, identity in terminal['artifacts'].items():
        body = remote(prefix+'/artifacts/'+name)
        assert len(body) == identity['bytes'] and sha(body) == identity['sha256'], name
        assert body == gzip.decompress((directory/(name+'.gz')).read_bytes()), name
        artifacts[name] = body
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar:
        def archived(name): return tar.extractfile(name).read()
        config_body = archived(str(campaign.CONFIG))
        assert config_body == campaign.CONFIG.read_bytes()
        assert sha(config_body) == reservation['config_sha256']
        config = json.loads(config_body)
        assert config['schema'] == 'borsuk-native-metadata-geometry-v1'
        assert (config['bucket'], config['region'], config['count'], config['k'], config['ann_queries']) == (BUCKET, REGION, 64, 10, 256)
        assert config['dataset_order'] == ['ReLAION', 'CoHere']
        assert config['staging'] == dict(control=dict(range_bytes=8388608, parallel_gets=4),
                                       candidate=dict(range_bytes=4194304, parallel_gets=8))
        assert config['gates'] == dict(recall_at_10_minimum=.95, all_calls_success=True,
            source_scorer_ordered_id_physical_parity=True, candidate_cold_p90_lower_than_control=True,
            cold_start_to_first_http_response_p90_ms_exclusive_maximum=444)
        assert config['worker_limit_seconds'] == 1500 and config['machine_limit_seconds'] == 4200
        qualification = reservation['qualification']
        assert qualification == json.loads(artifacts['source-qualification.json'])
        assert qualification['config_sha256'] == sha(config_body)
        assert qualification['artifact_roster_sha256'] == sha(json.dumps(campaign.ARTIFACTS, separators=(',', ':')).encode())
        assert set(qualification['code_sha256']) == set(campaign.CODE)
        assert set(config['code_sha256']) == set(campaign.CODE)-{
            'scripts/launch_native_metadata_geometry_spot.py', 'scripts/check_native_metadata_geometry_build.py'}
        for name, digest in qualification['code_sha256'].items(): assert sha(archived(name)) == digest
        for name, digest in config['code_sha256'].items(): assert sha(archived(name)) == digest
        for name in (*campaign.CODE, 'scripts/check_native_startup_build.py',
                     'scripts/launch_native_metadata_ranges_cold_spot.py',
                     'scripts/launch_native_peer_1m_spot.py', 'scripts/launch_native_startup_profile_spot.py',
                     'scripts/launch_v174_relaid_bind_compile_spot.py',
                     'scripts/verify_native_metadata_ranges_cold.py', 'scripts/verify_native_metadata_geometry.py'):
            assert archived(name) == Path(name).read_bytes(), name
        hashes = {m.name: sha(archived(m.name)) for m in tar.getmembers() if m.isfile() and
            (m.name.endswith('.rs') or Path(m.name).name in ('Cargo.toml', 'Cargo.lock'))}
        assert len(hashes) == config['candidate_native']['source_file_count'] == 395
        assert source_identity(hashes) == config['candidate_native']['source_identity_sha256']
        assert hashes[STAGE] == config['candidate_native']['stage_sha256']
        stage_body = archived(config['control_stage']['path'])
        assert sha(stage_body) == config['control_stage']['sha256'] == config['control_native']['stage_sha256']
        control_hashes = dict(hashes, **{STAGE: sha(stage_body)})
        assert config['control_native']['source_commit'] == CONTROL_COMMIT
        assert source_identity(control_hashes) == config['control_native']['source_identity_sha256'] == CONTROL_IDENTITY
        assert config['control_native']['source_file_count'] == 395
        assert {name for name in hashes if hashes[name] != control_hashes[name]} == {STAGE}
        assert config['compiled_sha256'] == {name: hashes[name] for name in COMPILED}
        for arm, arm_hashes, path in [('candidate', hashes, ''), ('control', control_hashes, 'control/')]:
            proof = json.loads(artifacts[path+'boundary-check.json'])
            compiled = json.loads(artifacts[path+'compiled-source.json'])
            assert compiled == proof['compiled_native_sha256'] == {name: arm_hashes[name] for name in COMPILED}
            assert proof['qualified'] and proof['green_status'] == proof['release_status'] == 0
            assert proof['current_full_suite_pass_claim'] is False
            assert proof['source_file_count'] == 395 and proof['source_identity_sha256'] == source_identity(arm_hashes)
            assert proof['compiled_http_sha256'] == compiled['crates/borsuk/examples/two_bit_http.rs']
            assert proof['binary_sha256'] == sha(artifacts[path+'binaries/two_bit_http'])
            assert proof['binary_bytes'] == len(artifacts[path+'binaries/two_bit_http'])
            for name, digest in compiled.items(): assert sha(artifacts[path+'compiled-source/'+name]) == digest
            for name in ('object-native', 'generation', 'http', 'source', 'graph'):
                log = artifacts[path+name+'.log']
                assert b'0 failed;' in log and b'ok. 0 passed;' not in log, (arm, name)
            assert re.search(rb'test result: ok\. 7 passed; 0 failed;', artifacts[path+'graph.log'])
            for name in SOURCE_TESTS: assert ('test '+name+' ... ok').encode() in artifacts[path+'source.log']
        original_items = archived(config['items_source']['path'])
        assert sha(original_items) == config['items_source']['sha256']
        assert config['items'] == json.loads(original_items)['items']
    expected = campaign.user_data(launch['source_commit'], sha(archive), archive_key, prefix, qualification)
    assert expected == (directory/'aws-user-data.sh').read_text()
    for name in ('rustc-version.txt', 'cargo-version.txt', 'cpuinfo.txt', 'arm-feature-tree.txt', 'x86-feature-tree.txt'):
        assert artifacts[name] == artifacts['control/'+name], name
    assert b'sha2 feature "asm"' in artifacts['arm-feature-tree.txt'] and b'force-soft' not in artifacts['arm-feature-tree.txt']
    assert b'sha2 feature "asm"' not in artifacts['x86-feature-tree.txt']
    features = [line.split(b':', 1)[1].split() for line in artifacts['cpuinfo.txt'].splitlines()
                if line.split(b':', 1)[0].strip() == b'Features']
    assert features and all(b'sha2' in flags for flags in features)
    groups = {}
    for name, limit in [('boundary-cgroup.json', 10*1024**3), ('profile-cgroup.json', 8*1024**3)]:
        group = json.loads(artifacts[name]); groups[name] = group
        assert int(group['memory.max']) == limit and 0 < int(group['memory.peak']) < limit
        assert int(group['memory.swap.max']) == int(group['memory.swap.peak']) == 0
        events = dict(line.split() for line in group['memory.events'].splitlines())
        assert events['oom'] == events['oom_kill'] == '0' and group['cpu_affinity'] == [0, 1, 2, 3]
    assert groups['profile-cgroup.json']['rlimit_as_bytes'] == [4*1024**3]*2
    inputs = {}
    for item in config['items']:
        bodies = {}
        for name in ('requests', 'reference-k10', 'truth'):
            identity = item['inputs'][name]; body = remote(identity['key'])
            assert len(body) == identity['bytes'] and sha(body) == identity['sha256']
            if 'range_bytes' in identity:
                body = body[identity['range_start']:identity['range_start']+identity['range_bytes']]
                assert len(body) == identity['range_bytes'] and sha(body) == identity['range_sha256']
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
    result = reduce_blocks(blocks, inputs, config['items'], config['staging'])
    summary = json.loads(artifacts['screen/summary.json'])
    for key, value in result.items():
        if key != 'native_peak_rss_bytes': assert summary[key] == value, key
    assert summary['ann_queries'] == summary['namespace_starts'] == 256 and summary['k'] == 10
    assert summary['blocks'] == config['blocks'] and summary['source_scorer_ordered_id_physical_parity'] is True
    assert summary['namespace_cold_start_included'] is True and summary['application_sq8_cache'] is False
    assert summary['cold_call_boundary'] == 'preencoded request; process launch through first HTTP response; refused TCP connects included'
    assert summary['client_cpu_affinity'] == [4, 5] and summary['native_cpu_affinity'] == [0, 1, 2, 3]
    assert summary['matched_vendor_measured'] is summary['serial_cold_qps_is_offered_or_saturation_qps'] is False
    assert summary['s3_service_cache'] == 'uncontrolled' and summary['transport'] == 'loopback plain HTTP'
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name'] == close['state'] == 'terminated' and instance['InstanceLifecycle'] == 'spot'
    assert instance['InstanceType'] == 'c7g.2xlarge' and instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    report = dict(valid_measurement=True, state='terminated', instance_id=launch['instance_id'],
        source_commit=launch['source_commit'], source_archive_sha256=sha(archive), config_sha256=sha(config_body),
        terminal_sha256=sha(terminal_body), control_native=config['control_native'], candidate_native=config['candidate_native'],
        staging=config['staging'], build_peak_bytes=int(groups['boundary-cgroup.json']['memory.peak']),
        cgroup_peak_bytes=int(groups['profile-cgroup.json']['memory.peak']), swap_peak_bytes=0,
        current_full_suite_pass_claim=False, matched_vendor_measured=False, offered_or_saturation_qps_measured=False, **result)
    (directory/'verification.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    assert len(sys.argv) == 2
    main(sys.argv[1])
