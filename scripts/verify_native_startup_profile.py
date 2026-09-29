"""Authenticate closed startup profiling receipts; no incomplete artifact reads."""
import gzip
import io
import json
from pathlib import Path
import sys
import tarfile

import boto3
from scripts.check_native_startup_stats import validate
from scripts.launch_native_peer_1m_spot import sha, BUCKET, REGION

ROOT = Path('docs/research/native-union-20260928')
FOCUSED = ('crates/borsuk/examples/two_bit_http.rs', 'crates/borsuk/src/object_native_generation.rs',
           'crates/borsuk/src/two_bit_generation.rs', 'crates/borsuk/tests/two_bit_generation.rs')


def main(attempt):
    from scripts import launch_native_startup_profile_spot as campaign
    directory = ROOT / 'startup-profile' / attempt
    launch = json.loads((directory/'aws-launch.json').read_text())
    close = json.loads((directory/'aws-closeout.json').read_text())
    session = boto3.Session(profile_name='causality',region_name=REGION)
    s3,ec2 = session.client('s3'),session.client('ec2')
    def remote(key):
        return s3.get_object(Bucket=BUCKET,Key=key)['Body'].read()
    prefix = launch['prefix']
    assert json.loads(remote(prefix+'/launch.json')) == launch
    terminal_body = remote(prefix+'/terminal.json')
    reservation_body = remote(prefix+'/reservation.json')
    assert terminal_body == (directory/'aws-terminal.json').read_bytes()
    terminal,reservation = json.loads(terminal_body),json.loads(reservation_body)
    assert reservation == json.loads((directory/'aws-reservation.json').read_text())
    assert terminal['schema'] == reservation['schema'] == campaign.SCHEMA
    assert terminal['status'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == launch['instance_id']
    assert terminal['source_commit'] == reservation['source_commit'] == launch['source_commit']
    assert terminal['source_archive_sha256'] == reservation['source_archive_sha256'] == launch['source_archive_sha256']
    archive = remote('research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')
    assert sha(archive) == launch['source_archive_sha256']
    with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
        def archived(name): return tar.extractfile(name).read()
        config_body = archived(str(ROOT/'startup-profile-config.json'))
        assert config_body == (ROOT/'startup-profile-config.json').read_bytes()
        config = json.loads(config_body)
        assert sha(config_body) == reservation['config_sha256']
        assert config['dataset_order'] == ['ReLAION','CoHere']*3 and config['ann_queries'] == 0
        for name,digest in config['code_sha256'].items(): assert sha(archived(name)) == digest
        source_hashes = {name:sha(archived(name)) for name in FOCUSED}
        qualification = reservation['qualification']
        assert qualification['compiled_native_sha256'] == source_hashes
        assert qualification['current_full_suite_pass_claim'] is False
        for name,digest in qualification['code_sha256'].items(): assert sha(archived(name)) == digest
        prior_body = archived(str(ROOT/'source-completion-integration/a0002/verification.json'))
        assert sha(prior_body) == reservation['qualification']['prior_assurance_sha256']
        prior = json.loads(prior_body)
        assert prior['valid_check'] and prior['state'] == 'terminated'
        changed = {name for name,digest in prior['compiled_native_sha256'].items()
                   if '/src/bin/' not in name and sha(archived(name)) != digest}
        assert changed == set(FOCUSED),changed
        for item in config['items']:
            proof_body = archived(item['closed_dev_verification_path'])
            assert sha(proof_body) == item['closed_dev_verification_sha256']
            proof = json.loads(proof_body)
            assert proof['valid_measurement'] and proof['state'] == 'terminated'
            original = json.loads(archived(str(ROOT/('fresh-rank16-dev64-config.json'
                if item['dataset']=='ReLAION' else 'fresh-cohere-dev64-config.json'))))
            assert item['authority'] == dict(root_sha256=original['root_sha256'],generation=1,control_epoch=1)
            assert len(item['metadata_files']) == 9
            assert item['metadata_files'] == {name:original['generation_artifacts']['generation/'+name]['bytes']
                                               for name in item['metadata_files']}
    artifacts = {}
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS)
    for name,identity in terminal['artifacts'].items():
        body = remote(prefix+'/artifacts/'+name)
        assert len(body) == identity['bytes'] and sha(body) == identity['sha256']
        assert body == gzip.decompress((directory/(name+'.gz')).read_bytes())
        artifacts[name] = body
    assert json.loads(artifacts['source-qualification.json']) == reservation['qualification']
    qualified = json.loads(artifacts['boundary-check.json'])
    assert qualified['qualified'] and qualified['no_corpus_query'] and qualified['full_workspace_repeated'] is False
    assert qualified['green_status'] == qualified['release_status'] == 0
    assert qualified['current_full_suite_pass_claim'] is False
    assert qualified['compiled_native_sha256'] == source_hashes
    assert qualified['compiled_http_sha256'] == source_hashes[FOCUSED[0]]
    assert qualified['binary_sha256'] == sha(artifacts['binaries/two_bit_http'])
    assert json.loads(artifacts['compiled-source.json']) == source_hashes
    for name,digest in source_hashes.items(): assert sha(artifacts['compiled-source/'+name]) == digest
    for name in ('object-native','generation','http'):
        assert b'0 failed;' in artifacts[name+'.log'] and b'ok. 0 passed;' not in artifacts[name+'.log']
    group = json.loads(artifacts['boundary-cgroup.json'])
    assert int(group['memory.max']) == 10*1024**3 and int(group['memory.peak']) < 10*1024**3
    assert int(group['memory.swap.peak']) == 0
    build_events = dict(line.split() for line in group['memory.events'].splitlines())
    assert build_events['oom'] == build_events['oom_kill'] == '0'
    profile_group = json.loads(artifacts['profile-cgroup.json'])
    assert int(profile_group['memory.max']) == 8*1024**3
    assert int(profile_group['memory.peak']) < 8*1024**3
    assert int(profile_group['memory.swap.max']) == int(profile_group['memory.swap.peak']) == 0
    events = dict(line.split() for line in profile_group['memory.events'].splitlines())
    assert events['oom'] == events['oom_kill'] == '0'
    assert profile_group['cpu_affinity'] == [0,1,2,3]
    assert profile_group['rlimit_as_bytes'] == [4*1024**3,4*1024**3]
    summary = json.loads(artifacts['screen/summary.json'])
    assert summary['ann_queries'] == 0 and summary['first_query_measured'] is False and summary['matched_vendor_measured'] is False
    assert len(summary['records']) == 6
    records = []
    for cell,dataset in enumerate(config['dataset_order']):
        item = next(item for item in config['items'] if item['dataset']==dataset)
        headers = [json.loads(line) for line in artifacts[f'screen/cell{cell}-server.log'].splitlines() if line.startswith(b'{')]
        assert len(headers)==1
        header = headers[0]
        assert header['phase']=='ready' and header['authority']==item['authority'] and header['listen']=='127.0.0.1:8080'
        reduced = validate(header['remote_open_stats'],item['metadata_files'],header['remote_open_wall_ns'])
        record = json.loads(artifacts[f'screen/cell{cell}-profile.json'])
        assert record == summary['records'][cell] and record['cell']==cell and record['dataset']==dataset
        assert all(record[key]==value for key,value in reduced.items())
        assert record['remote_open_ms']==header['remote_open_wall_ns']/1e6
        assert record['head_read_ms']==header['head_read_wall_ns']/1e6
        assert record['namespace_ready_ms'] >= record['remote_open_ms']+record['head_read_ms']
        assert json.loads(artifacts[f'screen/cell{cell}-close.json'])['intentional_stop'] is True
        records.append(record)
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name']==close['state']=='terminated' and instance['InstanceLifecycle']=='spot'
    assert instance['InstanceType']=='c7g.2xlarge' and instance['Placement']['AvailabilityZone']==reservation['availability_zone']
    report = dict(valid_diagnostic=True,ann_queries=0,first_query_measured=False,matched_vendor_measured=False,
        state='terminated',instance_id=launch['instance_id'],source_archive_sha256=sha(archive),
        source_commit=launch['source_commit'],config_sha256=sha(config_body),terminal_sha256=sha(terminal_body),
        compiled_native_sha256=source_hashes,records=records,
        build_cgroup_peak_bytes=int(group['memory.peak']),profile_cgroup_peak_bytes=int(profile_group['memory.peak']),
        swap_peak_bytes=0,profile_cpu_affinity=profile_group['cpu_affinity'],
        profile_address_space_bytes=profile_group['rlimit_as_bytes'])
    (directory/'verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv)>1 else 'a0001')
