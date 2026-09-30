"""Independent paged cold record reduction; terminal authentication precedes use."""
import copy
import gzip
import io
import json
from pathlib import Path
import tarfile
import tempfile
import struct
import sys
from types import SimpleNamespace
from unittest.mock import patch
from unittest.mock import Mock

from scripts import verify_native_cold_first_query as base
from scripts.check_native_paged_source_stats import validate_response, validate_startup


def authenticate_closed(directory, campaign, remote, manifest_validator=None):
    """Authenticate remote terminal, archive and every locally collected body."""
    directory = Path(directory)
    launch = json.loads((directory/'aws-launch.json').read_bytes())
    reservation = json.loads((directory/'aws-reservation.json').read_bytes())
    terminal_body = (directory/'aws-terminal.json').read_bytes()
    terminal = json.loads(terminal_body)
    prefix = launch['prefix']
    assert prefix == campaign.PREFIX + directory.name
    assert json.loads(remote(prefix+'/launch.json')) == launch
    assert json.loads(remote(prefix+'/reservation.json')) == reservation
    assert remote(prefix+'/terminal.json') == terminal_body
    assert terminal['schema'] == reservation['schema'] == campaign.SCHEMA
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == launch['instance_id']
    for key in ('source_commit', 'source_archive_sha256'):
        assert terminal[key] == reservation[key] == launch[key]
    archive = remote('research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')
    assert base.sha(archive) == launch['source_archive_sha256']
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS)
    artifacts = {}
    for name, identity in terminal['artifacts'].items():
        body = remote(prefix+'/artifacts/'+name)
        assert len(body) == identity['bytes'] and base.sha(body) == identity['sha256'], name
        assert body == gzip.decompress((directory/(name+'.gz')).read_bytes()), name
        artifacts[name] = body
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar:
        archived = {m.name: tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    config_body = archived[str(campaign.CONFIG)]
    assert config_body == campaign.CONFIG.read_bytes()
    assert base.sha(config_body) == reservation['config_sha256']
    config = json.loads(config_body)
    qualification = reservation['qualification']
    built_qualification = json.loads(artifacts['source-qualification.json'])
    assert all(built_qualification[key] == value for key, value in qualification.items())
    assert qualification['config_sha256'] == base.sha(config_body)
    assert set(qualification['code_sha256']) == set(campaign.CODE)
    for name, digest in qualification['code_sha256'].items():
        assert base.sha(archived[name]) == digest == base.sha(Path(name).read_bytes()), name
    for name, digest in config['code_sha256'].items(): assert base.sha(archived[name]) == digest
    if manifest_validator is not None:
        manifest_validator(archived, config)
        return launch, reservation, terminal, artifacts, archived, config
    pointer = config['native_source_manifest']
    manifest_body = archived[pointer['path']]
    assert base.sha(manifest_body) == pointer['sha256']
    manifest = json.loads(manifest_body)
    hashes = {name: base.sha(body) for name, body in archived.items()
              if name.endswith('.rs') or Path(name).name in ('Cargo.toml', 'Cargo.lock')}
    assert hashes == manifest['source_sha256']
    assert len(hashes) == manifest['source_file_count'] == config['native_source_file_count'] == 395
    identity = base.sha(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode())
    assert identity == manifest['source_identity_sha256'] == config['native_source_identity_sha256']
    return launch, reservation, terminal, artifacts, archived, config


def validate_build(artifacts, archived, config, reservation, terminal, campaign):
    proof = json.loads(artifacts['boundary-check.json'])
    resolved_body = artifacts['resolved-config.json']; resolved = json.loads(resolved_body)
    binary = artifacts['binaries/two_bit_http']
    binary_identity = dict(sha256=base.sha(binary), bytes=len(binary))
    assert config['binary'] is None and resolved == dict(config, binary=binary_identity)
    assert terminal['resolved_config_sha256'] == proof['resolved_config_sha256'] == base.sha(resolved_body)
    assert proof['original_config_sha256'] == proof['config_sha256'] == reservation['config_sha256']
    assert proof['binary_sha256'] == binary_identity['sha256'] and proof['binary_bytes'] == len(binary)
    assert proof['qualified'] is True and proof['green_status'] == proof['release_status'] == 0
    assert proof['native_rebuilt'] is True and proof['current_full_suite_pass_claim'] is False
    assert proof['source_file_count'] == 395
    assert proof['source_identity_sha256'] == config['native_source_identity_sha256']
    compiled = {name: base.sha(archived[name]) for name in campaign.COMPILED}
    assert compiled == json.loads(artifacts['compiled-source.json']) == proof['compiled_native_sha256']
    assert compiled == reservation['qualification']['compiled_native_sha256']
    for name, digest in compiled.items(): assert base.sha(artifacts['compiled-source/'+name]) == digest
    for name, _ in campaign.CHECKS:
        text = artifacts[name+'.log'].decode()
        assert '0 failed;' in text and 'test result: ok. 0 passed;' not in text, name
    assert b'sha2 feature "asm"' in artifacts['arm-feature-tree.txt']
    assert b'force-soft' not in artifacts['arm-feature-tree.txt']
    assert b'sha2 feature "asm"' not in artifacts['x86-feature-tree.txt']
    flags = [line.split(b':', 1)[1].split() for line in artifacts['cpuinfo.txt'].splitlines()
             if line.split(b':', 1)[0].strip() == b'Features']
    assert flags and all(b'sha2' in row for row in flags)
    for name, limit in [('boundary-cgroup.json', 10*1024**3), ('profile-cgroup.json', 8*1024**3)]:
        group = json.loads(artifacts[name])
        assert int(group['memory.max']) == limit and 0 < int(group['memory.peak']) < limit
        assert int(group['memory.swap.max']) == int(group['memory.swap.peak']) == 0
        events = dict(line.split() for line in group['memory.events'].splitlines())
        assert events['oom'] == events['oom_kill'] == '0' and group['cpu_affinity'] == [0, 1, 2, 3]
        if name == 'profile-cgroup.json': assert group['rlimit_as_bytes'] == [4*1024**3]*2
    return resolved


def reduce_records(records, requests, references, truth, item):
    # Reuse the existing timing, request-byte, ordered-hit, RSS and cleanup guards.
    with patch.object(base, 'validate', validate_startup):
        result, peak = base.reduce_records(records, requests, references, truth, item)
    for q, row in enumerate(records):
        validate_response(row['response'])
        assert row['reference_response'] == {key: references[q][key] for key in
            ('ids', 'ranges', 'planned_bytes', 'submitted_gets', 'verified_bytes', 'failed_gets')}
        assert row['truth_at_10'] == list(truth[q][:10])
        assert row['expected_authority'] == item['authority']
        assert row['metadata_files'] == item['metadata_files']
    for suffix in ('submitted_gets', 'verified_bytes', 'failed_gets'):
        source = sum(r['response']['source_' + suffix] for r in records)
        sq8 = sum(r['response'][suffix] for r in records)
        result.update({'source_' + suffix: source, 'sq8_' + suffix: sq8,
                       'combined_' + suffix: source + sq8})
    result.update(source_head_requests=sum(r['metadata']['source_head_requests'] for r in records),
        logical_metadata_head_requests=sum(r['metadata']['logical_metadata_head_requests'] for r in records),
        logical_metadata_get_requests=sum(r['metadata']['logical_metadata_get_requests'] for r in records),
        metadata_payload_buffer_bound_bytes=max(r['metadata']['payload_buffer_bound_bytes'] for r in records),
        source_head_wall_ns=sum(r['native_header']['remote_open_stats']['source_head_wall_ns'] for r in records),
        source_head_ms=sum(r['metadata']['source_head_ms'] for r in records),
        head_read_wall_ns=sum(r['native_header']['head_read_wall_ns'] for r in records))
    return result, peak


def self_check():
    from scripts import run_native_paged_cold_first_query_selfcheck as fixture
    records, requests, references, truth = [], [], [], []
    item = dict(dataset='synthetic', authority=fixture.AUTHORITY, metadata_files=fixture.FILES)
    for q in range(64):
        row = fixture.record(q)
        request = dict(query=[1.] + [0.] * 767)
        body = json.dumps(dict(query=request['query'], k=10, **item['authority']),
                          separators=(',', ':')).encode()
        end = row['completed_ns']; connect = end - 50_000_000
        row.update(successful_connect_attempt_ns=connect, connected_ns=connect,
            before_successful_connect_attempt_ns=connect-row['started_ns'],
            successful_tcp_connect_ns=0, first_post_to_response_ns=50_000_000,
            connection_refused_attempts=0, request_sha256=base.sha(body), request_bytes=len(body),
            native_server_log=json.dumps(row['native_header'])+'\n',
            native_time_log='Maximum resident set size (kbytes): 1024\nSwaps: 0\n')
        row['response']['native_wall_ns'] = 1
        records.append(row); requests.append(request)
        references.append(copy.deepcopy(row['response'])); truth.append(list(range(100)))
    result, peak = reduce_records(records, requests, references, truth, item)
    assert result['combined_submitted_gets'] == 10240 and peak == 1048576
    assert result['quality_gate_passed'] and not result['published_context_gate_passed']
    for mutate in (
        lambda r: r['response'].update(source_submitted_gets=129),
        lambda r: r.update(truth_at_10=list(reversed(r['truth_at_10']))),
        lambda r: r.update(incoming_http_wall_ns=1),
        lambda r: r['native_close'].update(intentional_stop=False),
        lambda r: r['metadata'].update(logical_metadata_get_requests=0)):
        bad = copy.deepcopy(records); mutate(bad[0])
        try: reduce_records(bad, requests, references, truth, item)
        except AssertionError: pass
        else: raise AssertionError('tampered receipt accepted')
    print('PASS independent paged reduction: caps, truth, timing, cleanup, metadata tamper guards')
    authentication_check()


def authentication_check():
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)/'a0001'; directory.mkdir()
        config_path = Path(tmp)/'config.json'
        source = {f'crates/fixture/{n}.rs': b'fixture' for n in range(395)}
        hashes = {n: base.sha(b) for n, b in source.items()}
        identity = base.sha(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode())
        manifest = json.dumps(dict(source_sha256=hashes, source_file_count=395,
                                   source_identity_sha256=identity)).encode()
        config = dict(native_source_manifest=dict(path='manifest.json', sha256=base.sha(manifest)),
            native_source_file_count=395, native_source_identity_sha256=identity, code_sha256={})
        config_path.write_text(json.dumps(config)); config_body = config_path.read_bytes()
        archived = dict(source, **{'manifest.json': manifest, str(config_path): config_body})
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode='w:gz') as tar:
            for name, body in archived.items():
                member = tarfile.TarInfo(name); member.size = len(body)
                tar.addfile(member, io.BytesIO(body))
        archive = stream.getvalue(); prefix = 'fixture-a0001'
        campaign = SimpleNamespace(PREFIX='fixture-', SCHEMA='fixture', CONFIG=config_path,
                                   CODE=(), ARTIFACTS=('source-qualification.json',))
        qualification = dict(config_sha256=base.sha(config_body), code_sha256={})
        artifact = json.dumps(qualification).encode()
        launch = dict(prefix=prefix, instance_id='fixture-instance', source_commit='fixture',
                      source_archive_sha256=base.sha(archive))
        reservation = dict(launch, schema='fixture', config_sha256=base.sha(config_body),
                           qualification=qualification)
        terminal = dict(launch, schema='fixture', phase='complete', status='complete', exit_code=0,
            artifacts={'source-qualification.json': dict(bytes=len(artifact), sha256=base.sha(artifact))})
        objects = {'research/native-library-check/sources/'+base.sha(archive)+'.tar.gz': archive,
                   prefix+'/artifacts/source-qualification.json': artifact}
        for name, value in [('launch', launch), ('reservation', reservation), ('terminal', terminal)]:
            body = json.dumps(value).encode(); objects[prefix+'/'+name+'.json'] = body
            (directory/('aws-'+name+'.json')).write_bytes(body)
        (directory/'source-qualification.json.gz').write_bytes(gzip.compress(artifact))
        authenticate_closed(directory, campaign, objects.__getitem__)
        for key in (prefix+'/terminal.json', prefix+'/artifacts/source-qualification.json',
                    'research/native-library-check/sources/'+base.sha(archive)+'.tar.gz'):
            bad = dict(objects); bad[key] += b'tamper'
            try: authenticate_closed(directory, campaign, bad.__getitem__)
            except AssertionError: pass
            else: raise AssertionError('tampered closed authority accepted')
        campaign.COMPILED = ('crates/fixture/0.rs',); campaign.CHECKS = (('source', []),)
        config['binary'] = None
        binary = b'fixture executable'
        resolved_body = json.dumps(dict(config, binary=dict(sha256=base.sha(binary), bytes=len(binary)))).encode()
        compiled = {name: hashes[name] for name in campaign.COMPILED}
        reservation['qualification']['compiled_native_sha256'] = compiled
        proof = dict(resolved_config_sha256=base.sha(resolved_body), original_config_sha256=reservation['config_sha256'],
            config_sha256=reservation['config_sha256'], binary_sha256=base.sha(binary), binary_bytes=len(binary),
            qualified=True, green_status=0, release_status=0, native_rebuilt=True,
            current_full_suite_pass_claim=False, source_file_count=395,
            source_identity_sha256=identity, compiled_native_sha256=compiled)
        terminal['resolved_config_sha256'] = base.sha(resolved_body)
        artifacts = {'boundary-check.json': json.dumps(proof).encode(), 'resolved-config.json': resolved_body,
            'binaries/two_bit_http': binary, 'compiled-source.json': json.dumps(compiled).encode(),
            'compiled-source/crates/fixture/0.rs': source['crates/fixture/0.rs'],
            'source.log': b'test result: ok. 1 passed; 0 failed;',
            'arm-feature-tree.txt': b'sha2 feature "asm"', 'x86-feature-tree.txt': b'sha2',
            'cpuinfo.txt': b'Features : sha2\n'}
        for name, limit in [('boundary-cgroup.json', 10*1024**3), ('profile-cgroup.json', 8*1024**3)]:
            artifacts[name] = json.dumps({'memory.max': str(limit), 'memory.peak': '1024',
                'memory.swap.max': '0', 'memory.swap.peak': '0', 'memory.events': 'oom 0\noom_kill 0',
                'cpu_affinity': [0,1,2,3], 'rlimit_as_bytes': [4*1024**3]*2}).encode()
        validate_build(artifacts, archived, config, reservation, terminal, campaign)
        for key in ('binaries/two_bit_http', 'compiled-source/crates/fixture/0.rs', 'resolved-config.json'):
            bad = dict(artifacts); bad[key] += b' '
            try: validate_build(bad, archived, config, reservation, terminal, campaign)
            except AssertionError: pass
            else: raise AssertionError('tampered build authority accepted')
    print('PASS closed authentication: terminal, artifact and archive tamper rejection')
    print('PASS build authority: binary, compiled snapshot and resolved config tamper rejection')


def integration_check():
    import boto3
    from scripts import launch_native_paged_source_cold_spot as campaign
    config = json.loads(campaign.CONFIG.read_bytes()); remote = {}; panels = {}
    for item in config['items']:
        requests = [dict(query_ordinal=q, query=[1.]+[0.]*767) for q in range(64)]
        refs = [dict(item['authority'], top_k=10, declared_panel_count=64)]
        refs += [dict(query_ordinal=q) for q in range(64)] + [dict(count=64)]
        bodies = {'requests': '\n'.join(map(json.dumps, requests)).encode(),
                  'reference-k10': '\n'.join(map(json.dumps, refs)).encode(),
                  'truth': struct.pack('<100I', *range(100))*64}
        item['inputs'] = {name: dict(key=item['dataset']+'/'+name, bytes=len(body), sha256=base.sha(body))
                          for name, body in bodies.items()}
        remote.update({item['inputs'][name]['key']: body for name, body in bodies.items()})
        panels[item['dataset']] = dict(count=64, quality_gate_passed=True,
                                      published_context_gate_passed=False, split=item['query_split'])
    summary = dict(panels=panels, original_config_sha256=base.sha(b'{}'), ann_queries=128,
        namespace_starts=128, k=10, client_cpu_affinity=[4,5], native_cpu_affinity=[0,1,2,3],
        matched_vendor_measured=False, matched_control_latency_measured=False,
        serial_cold_qps_is_offered_or_saturation_qps=False, application_sq8_cache=False,
        namespace_cold_start_included=True, s3_service_cache='uncontrolled',
        source_scorer_ordered_id_physical_parity=True, quality_gate_passed=True,
        published_context_gate_passed=False)
    artifacts = {'resolved-config.json': b'{}', 'screen/summary.json': json.dumps(summary).encode()}
    for item in config['items']: artifacts['screen/'+item['dataset'].lower()+'-records.jsonl'] = b'{}\n'
    launch = dict(instance_id='i-fixture', source_commit='fixture', source_archive_sha256='a'*64, prefix='fixture')
    reservation = dict(config_sha256='b'*64, qualification={}, availability_zone='fixture-az')
    terminal = dict(resolved_config_sha256=base.sha(b'{}'))
    ec2, s3, session = Mock(), Mock(), Mock()
    session.client.side_effect = [s3, ec2]
    s3.get_object.side_effect = lambda Bucket, Key: {'Body': io.BytesIO(remote[Key])}
    ec2.describe_instances.return_value = {'Reservations': [{'Instances': [dict(State={'Name':'terminated'},
        InstanceLifecycle='spot', InstanceType='c7g.2xlarge', Placement={'AvailabilityZone':'fixture-az'})]}]}
    with tempfile.TemporaryDirectory() as tmp, patch.object(campaign, 'ROOT', Path(tmp)), \
            patch.object(campaign, 'user_data', return_value='fixture'), \
            patch.object(boto3, 'Session', return_value=session), \
            patch(__name__+'.authenticate_closed', return_value=(launch,reservation,terminal,artifacts,{},config)), \
            patch(__name__+'.validate_build', return_value=config), \
            patch(__name__+'.reduce_records', side_effect=lambda rows,req,ref,truth,item: (
                {k:v for k,v in panels[item['dataset']].items() if k!='split'}, 1024)):
        directory = Path(tmp)/campaign.NAME/'a0001'; directory.mkdir(parents=True)
        (directory/'aws-user-data.sh').write_text('fixture')
        (directory/'aws-terminal.json').write_text('{}')
        (directory/'aws-closeout.json').write_text(json.dumps(dict(state='terminated', nodes={'0':{'instance_id':'i-fixture'}})))
        main('a0001')
        assert json.loads((directory/'verification.json').read_bytes())['valid_measurement'] is True
    print('PASS verifier entry point wiring (mocked authority/reducer; no AWS/native execution)')


def main(attempt):
    import boto3
    from scripts import launch_native_paged_source_cold_spot as campaign
    directory = campaign.ROOT/campaign.NAME/attempt
    session = boto3.Session(profile_name='causality', region_name=base.REGION)
    s3, ec2 = session.client('s3'), session.client('ec2')
    def remote(key): return s3.get_object(Bucket=base.BUCKET, Key=key)['Body'].read()
    launch, reservation, terminal, artifacts, archived, config = authenticate_closed(directory, campaign, remote)
    resolved = validate_build(artifacts, archived, config, reservation, terminal, campaign)
    expected = campaign.user_data(launch['source_commit'], launch['source_archive_sha256'],
        'research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz',
        launch['prefix'], reservation['qualification'])
    assert expected == (directory/'aws-user-data.sh').read_text()
    close = json.loads((directory/'aws-closeout.json').read_bytes())
    assert close['nodes'] == {'0': {'instance_id': launch['instance_id']}}
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name'] == close['state'] == 'terminated'
    assert instance['InstanceLifecycle'] == 'spot' and instance['InstanceType'] == 'c7g.2xlarge'
    assert instance['Placement']['AvailabilityZone'] == reservation['availability_zone']
    summary = json.loads(artifacts['screen/summary.json']); panels, peaks = {}, {}
    assert summary['original_config_sha256'] == base.sha(artifacts['resolved-config.json'])
    assert summary['ann_queries'] == summary['namespace_starts'] == 128 and summary['k'] == 10
    assert summary['client_cpu_affinity'] == [4,5] and summary['native_cpu_affinity'] == [0,1,2,3]
    for key in ('matched_vendor_measured', 'matched_control_latency_measured',
                'serial_cold_qps_is_offered_or_saturation_qps', 'application_sq8_cache'):
        assert summary[key] is False
    assert summary['namespace_cold_start_included'] is True and summary['s3_service_cache'] == 'uncontrolled'
    assert summary['source_scorer_ordered_id_physical_parity'] is True
    for item in resolved['items']:
        bodies = {}
        for name in ('requests', 'reference-k10', 'truth'):
            ident = item['inputs'][name]; body = remote(ident['key'])
            assert len(body) == ident['bytes'] and base.sha(body) == ident['sha256']
            if 'range_bytes' in ident:
                body = body[ident['range_start']:ident['range_start']+ident['range_bytes']]
                assert len(body) == ident['range_bytes'] and base.sha(body) == ident['range_sha256']
            bodies[name] = body
        requests = [json.loads(line) for line in bodies['requests'].splitlines()]
        refs = [json.loads(line) for line in bodies['reference-k10'].splitlines()]
        assert len(requests) == 64 and len(refs) == 66 and len(bodies['truth']) == 25600
        assert refs[0]['top_k'] == 10 and refs[0]['declared_panel_count'] == refs[-1]['count'] == 64
        for key, value in item['authority'].items(): assert refs[0][key] == value
        assert [r['query_ordinal'] for r in requests] == [r['query_ordinal'] for r in refs[1:-1]] == list(range(64))
        truth = [struct.unpack_from('<100I', bodies['truth'], q*400) for q in range(64)]
        assert all(len(set(row)) == 100 and max(row) < 1000000 for row in truth)
        records = [json.loads(line) for line in artifacts['screen/'+item['dataset'].lower()+'-records.jsonl'].splitlines()]
        result, peak = reduce_records(records, requests, refs[1:-1], truth, item)
        assert all(summary['panels'][item['dataset']][key] == value for key, value in result.items())
        assert summary['panels'][item['dataset']]['split'] == item['query_split']
        panels[item['dataset']] = dict(result, split=item['query_split']); peaks[item['dataset']] = peak
    quality = all(p['quality_gate_passed'] for p in panels.values())
    context = all(p['published_context_gate_passed'] for p in panels.values())
    assert summary['quality_gate_passed'] == quality and summary['published_context_gate_passed'] == context
    report = dict(valid_measurement=True, state='terminated', instance_id=launch['instance_id'],
        source_commit=launch['source_commit'], source_archive_sha256=launch['source_archive_sha256'],
        original_config_sha256=reservation['config_sha256'], resolved_config_sha256=terminal['resolved_config_sha256'],
        terminal_sha256=base.sha((directory/'aws-terminal.json').read_bytes()),
        quality_gate_passed=quality, published_context_gate_passed=context, panels=panels,
        native_peak_rss_bytes=peaks, matched_vendor_measured=False, matched_control_latency_measured=False,
        offered_or_saturation_qps_measured=False, current_full_suite_pass_claim=False)
    report['qualified_binary_reused'] = reservation['qualification'].get('qualified_binary_reused', False)
    report['build_evidence_is_historical'] = report['qualified_binary_reused']
    (directory/'verification.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


if __name__ == '__main__':
    if sys.argv[1:] in ([], ['--self-check']):
        self_check(); integration_check()
    else:
        assert len(sys.argv) == 2
        main(sys.argv[1])
