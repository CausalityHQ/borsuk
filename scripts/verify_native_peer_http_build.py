"""Independently authenticate a closed ARM example qualification and termination."""
import gzip
import io
import json
from pathlib import Path
import sys
import tarfile

import boto3
from scripts import launch_native_peer_http_build as build
from scripts.launch_native_peer_1m_spot import sha, BUCKET, REGION


def main(attempt):
    directory = build.ROOT / 'peer-http-build' / attempt
    launch = json.loads((directory / 'aws-launch.json').read_text())
    close = json.loads((directory / 'aws-closeout.json').read_text())
    session = boto3.Session(profile_name='causality', region_name=REGION)
    s3, ec2 = session.client('s3'), session.client('ec2')
    def remote(key):
        return s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    prefix = launch['prefix']
    terminal_body = remote(prefix + '/terminal.json')
    assert terminal_body == (directory / 'aws-terminal.json').read_bytes()
    terminal = json.loads(terminal_body)
    reservation_body = remote(prefix + '/reservation.json')
    reservation = json.loads(reservation_body)
    assert reservation == json.loads((directory / 'aws-reservation.json').read_text())
    assert terminal['schema'] == reservation['schema'] == build.SCHEMA
    assert terminal['status'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == launch['instance_id'] == close['nodes']['build']['instance_id']
    assert terminal['source_commit'] == reservation['source_commit'] == launch['source_commit']
    assert terminal['source_archive_sha256'] == reservation['source_archive_sha256'] == launch['source_archive_sha256']
    archive = remote('research/native-library-check/sources/' + launch['source_archive_sha256'] + '.tar.gz')
    assert sha(archive) == launch['source_archive_sha256']
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar:
        source = tar.extractfile('crates/borsuk/examples/two_bit_http.rs').read()
        assurance_body = tar.extractfile('docs/research/native-union-20260928/source-completion-integration/a0002/verification.json').read()
        assert sha(assurance_body) == reservation['prior_assurance_sha256']
        assurance = json.loads(assurance_body)
        assert assurance['valid_check'] and assurance['state'] == 'terminated'
        for name, digest in assurance['compiled_native_sha256'].items():
            if '/src/bin/' in name or name == 'crates/borsuk/examples/two_bit_http.rs':
                continue
            assert sha(tar.extractfile(name).read()) == digest, name
    artifacts = {}
    assert set(terminal['artifacts']) == set(build.ARTIFACTS)
    for name, ident in terminal['artifacts'].items():
        body = remote(prefix + '/artifacts/' + name)
        assert len(body) == ident['bytes'] and sha(body) == ident['sha256']
        assert body == gzip.decompress((directory / (name + '.gz')).read_bytes())
        artifacts[name] = body
    report = json.loads(artifacts['boundary-check.json'])
    assert report['qualified'] and report['no_corpus_query'] and report['full_workspace_repeated'] is False
    assert report['green_status'] == report['release_status'] == 0 and report['query_slots'] == 4
    assert report['compiled_http_sha256'] == sha(source) == sha(artifacts['http.fixed.rs'])
    assert report['binary_sha256'] == sha(artifacts['binaries/two_bit_http'])
    assert b'ip.is_private()' in source and b'const QUERY_SLOTS: usize = 4;' in source
    assert b'0 failed;' in artifacts['green.log'] and b'listener_boundary ... ok' in artifacts['listener.log']
    group = json.loads(artifacts['boundary-cgroup.json'])
    assert int(group['memory.max']) == 10 * 1024**3
    assert int(group['memory.peak']) < int(group['memory.max']) and int(group['memory.swap.peak']) == 0
    assert 'oom_kill 0' in group['memory.events'] and 'oom 0' in group['memory.events']
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name'] == close['state'] == 'terminated'
    assert instance['InstanceLifecycle'] == 'spot' and instance['InstanceType'] == 'c7g.2xlarge'
    assert instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    output = dict(valid_check=True, qualification='private-listener HTTP example only', no_corpus_query=True,
        source_commit=launch['source_commit'], source_archive_sha256=sha(archive), terminal_sha256=sha(terminal_body),
        reservation_sha256=sha(reservation_body), instance_id=launch['instance_id'], state='terminated',
        compiled_http_sha256=sha(source), binary=dict(terminal['artifacts']['binaries/two_bit_http'],
            key=prefix + '/artifacts/binaries/two_bit_http'), library_assurance_reused=2696,
        cgroup_peak_bytes=int(group['memory.peak']), swap_peak_bytes=0,
        observed_elapsed_s=close['observed_elapsed_s'],
        compute_cost_estimate_usd=round(close['observed_elapsed_s']/3600*float(reservation['spot_price_observed_usd_per_hour']),4),
        cost_scope='conservative controller elapsed estimate, excludes EBS/S3, not invoice')
    (directory / 'verification.json').write_text(json.dumps(output, indent=2)+'\n')
    print(json.dumps(output))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'a0001')
