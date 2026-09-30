"""Independent paged cold record reduction; terminal authentication precedes use."""
import copy
import gzip
import io
import json
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from scripts import verify_native_cold_first_query as base
from scripts.check_native_paged_source_stats import validate_response, validate_startup


def authenticate_closed(directory, campaign, remote):
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
    assert qualification == json.loads(artifacts['source-qualification.json'])
    assert qualification['config_sha256'] == base.sha(config_body)
    assert set(qualification['code_sha256']) == set(campaign.CODE)
    for name, digest in qualification['code_sha256'].items():
        assert base.sha(archived[name]) == digest == base.sha(Path(name).read_bytes()), name
    for name, digest in config['code_sha256'].items(): assert base.sha(archived[name]) == digest
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
    print('PASS closed authentication: terminal, artifact and archive tamper rejection')


if __name__ == '__main__':
    self_check()
