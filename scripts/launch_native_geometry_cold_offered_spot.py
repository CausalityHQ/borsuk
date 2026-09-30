"""Reuse a closed qualified native binary and the existing owned-Spot lifecycle."""
import fcntl
import gzip
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from scripts import launch_native_metadata_ranges_cold_spot as ranges
from scripts import launch_native_cold_first_query_spot as cold
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.check_native_startup_build import FOCUSED_ARM, source_hashes, source_identity

ROOT = peer.ROOT
NAME = 'geometry-cold-offered'
PREFIX = 'research/native-union/20260930/geometry-cold-offered-'
TOKEN_PREFIX = 'geometry-cold-offered-'
TAG = 'borsuk-geometry-cold-offered'
CONFIG = ROOT / 'geometry-cold-offered-config.json'
SCHEMA = 'borsuk-native-geometry-cold-offered-spot-v1'
WALL = 2700
COMPUTE_CAP = .225
FROZEN = ROOT / 'metadata-geometry/a0002'
PROOF_SHA = 'cbc369cfe714653173525b6557af49c0462b458bf29a9c9ce5a4aacce318452c'
BINARY_SHA = '256dcf6c9d3f893d5c709564a099bf440b482e617800598ddd6538e1ec0cbb21'
TERMINAL_SHA = '37268e9f0125ef0a3f770271458a0378d27b018571cb4c701965833b9fa1e826'
GRAPH = 'crates/borsuk/src/unit_centroid_graph.rs'
COMPILED = (*FOCUSED_ARM, GRAPH)
SOURCE_IDENTITY = '4bbc6c332e78e5a068001913597cdc82d6c13bd147acd96e019149c190a39a1c'
ITEMS_SOURCE = dict(path=str(ROOT/'metadata-geometry-config.json'),
    sha256='410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3')
FROZEN_FILES = (*cold.FROZEN_FILES, 'compiled-source/'+GRAPH)
ARTIFACTS = ('source-qualification.json', 'boundary-check.json', 'compiled-source.json',
    'binaries/two_bit_http', 'cpu.txt', 'test.log', 'run-closed.log',
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json',
    *(f'screen/rate{rate}-{dataset.lower()}-records.jsonl' for rate in range(6) for dataset in ('ReLAION','CoHere')),
    *('frozen/'+name for name in FROZEN_FILES))


def preflight(base=Path('.')):
    from scripts import run_native_geometry_cold_offered as worker
    base = Path(base)
    body = (base/CONFIG).read_bytes()
    config = json.loads(body)
    worker.validate_config(config, base)
    verification_body = (base/FROZEN/'verification.json').read_bytes()
    assert peer.sha(verification_body) == PROOF_SHA
    verified = json.loads(verification_body)
    assert verified['valid_measurement'] and verified['diagnostic_gate_passed'] and verified['state'] == 'terminated'
    terminal_body = (base/FROZEN/'aws-terminal.json').read_bytes()
    assert peer.sha(terminal_body) == TERMINAL_SHA == verified['terminal_sha256']
    terminal = json.loads(terminal_body)
    assert terminal['schema'] == 'borsuk-native-metadata-geometry-spot-v1' and terminal['status'] == terminal['phase'] == 'complete'
    assert terminal['exit_code'] == 0 and terminal['instance_id'] == verified['instance_id']
    assert len(terminal['artifacts']) == 60
    for key in ('source_commit','source_archive_sha256'): assert terminal[key] == verified[key]
    close = json.loads((base/FROZEN/'aws-closeout.json').read_bytes())
    assert close['state'] == 'terminated' and close['nodes'] == {'0':{'instance_id':verified['instance_id']}}
    bodies = {}
    for name,identity in terminal['artifacts'].items():
        value = gzip.decompress((base/FROZEN/(name+'.gz')).read_bytes())
        assert len(value) == identity['bytes'] and peer.sha(value) == identity['sha256'],name
        if name in FROZEN_FILES: bodies[name] = value
    boundary = json.loads(bodies['boundary-check.json'])
    compiled = json.loads(bodies['compiled-source.json'])
    qualified = json.loads(bodies['source-qualification.json'])
    assert boundary['qualified'] and boundary['green_status'] == boundary['release_status'] == 0
    assert boundary['current_full_suite_pass_claim'] is False
    assert boundary['no_corpus_query'] is True
    assert boundary['arm'] == 'candidate' and boundary['same_worker_toolchain'] is True
    assert boundary['focused_tests'] == ['object-native', 'generation', 'http', 'source', 'graph']
    assert boundary['sha_backend']['arm_asm_selected'] is True
    assert boundary['sha_backend']['x86_asm_selected'] is False
    assert boundary['sha_backend']['cpu_sha2_capable'] is True
    assert boundary['sha_backend']['toolchain_parity_asserted'] is True
    assert qualified['current_full_suite_pass_claim'] is False
    identities = source_hashes(base)
    assert len(identities) == boundary['source_file_count'] == qualified['source_file_count'] == 395
    assert source_identity(identities) == boundary['source_identity_sha256'] == qualified['source_identity_sha256'] == SOURCE_IDENTITY
    assert set(compiled) == set(COMPILED)
    assert compiled == boundary['compiled_native_sha256'] == qualified['compiled_native_sha256']
    assert boundary['compiled_http_sha256'] == compiled['crates/borsuk/examples/two_bit_http.rs']
    for name,digest in compiled.items(): assert identities[name] == peer.sha(bodies['compiled-source/'+name]) == digest,name
    assert peer.sha(bodies['binaries/two_bit_http']) == boundary['binary_sha256'] == BINARY_SHA
    assert len(bodies['binaries/two_bit_http']) == boundary['binary_bytes'] == 12470768
    assert config['items_source'] == ITEMS_SOURCE
    reference_body = (base/ITEMS_SOURCE['path']).read_bytes()
    assert peer.sha(reference_body) == ITEMS_SOURCE['sha256'] == verified['config_sha256'] == qualified['config_sha256']
    assert qualified['config_path'] == ITEMS_SOURCE['path']
    reference = json.loads(reference_body)
    assert config['items'] == reference['items']
    assert config['binary'] == terminal['artifacts']['binaries/two_bit_http']
    frozen = dict(path=str(FROZEN/'verification.json'),sha256=PROOF_SHA,
        source_commit=verified['source_commit'],source_archive_sha256=verified['source_archive_sha256'],
        terminal_sha256=TERMINAL_SHA)
    assert config['frozen_native_qualification'] == frozen
    extras = ('scripts/launch_native_geometry_cold_offered_spot.py','scripts/launch_native_metadata_ranges_cold_spot.py',
        'scripts/launch_native_cold_first_query_spot.py','scripts/check_native_startup_build.py')
    return dict(config_sha256=peer.sha(body),code_sha256=dict(config['code_sha256'],
        **{name:peer.sha((base/name).read_bytes()) for name in extras}),
        frozen_native_qualification=frozen, frozen_binary_artifacts={name:terminal['artifacts'][name] for name in FROZEN_FILES},
        source_identity_sha256=source_identity(identities),source_file_count=395,compiled_native_sha256=compiled,
        native_rebuilt=False,native_source_commit=verified['source_commit'],current_full_suite_pass_claim=False,
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS,separators=(',',':')).encode()),
        config_path=str(CONFIG),campaign_schema=SCHEMA)


def extract_frozen(out):
    out = Path(out)
    assert json.loads((out/'source-qualification.json').read_bytes()) == preflight()
    for name in FROZEN_FILES:
        value = gzip.decompress((FROZEN/(name+'.gz')).read_bytes())
        path = out/'frozen'/name
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(value)
        if name in ('boundary-check.json','compiled-source.json','binaries/two_bit_http'):
            target = out/name
            target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes(value)
    (out/'binaries/two_bit_http').chmod(0o755)
    print(json.dumps(dict(native_rebuilt=False,binary_sha256=BINARY_SHA)))


def poll(ec2,s3,prefix,instance_id,started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2,s3,prefix,instance_id,started)


def collect(s3,prefix,out,instance_id,commit,digest):
    with patch.multiple(ranges, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        return ranges.collect(s3,prefix,out,instance_id,commit,digest)


def user_data(commit,archive_sha,archive_key,prefix,qualification):
    with patch.multiple(runner,WALL_SECONDS=WALL,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS):
        body = runner.user_data(commit,archive_sha,archive_key,prefix)
    body = body.replace('v174-relaid-bind-compile','native-geometry-cold-offered')
    roster = json.dumps(ARTIFACTS,separators=(',',':'))
    body = body.replace(' '.join(ARTIFACTS), "$(python3 -c 'import json; print(\" \".join(json.load(open(\"artifact-roster.json\"))))')")
    body = body.replace(repr(ARTIFACTS), 'json.loads(Path("artifact-roster.json").read_text())')
    body = body.replace('phase=bootstrap', "cat >artifact-roster.json <<'ROSTER'\n"+roster+"\nROSTER\nphase=bootstrap")
    start,end = body.index('phase=install\n'),body.index('phase=complete')
    proof = json.dumps(qualification,sort_keys=True,separators=(',',':'))
    command = f'''phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
cat >source-qualification.json <<'QUALIFICATION'
{proof}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
(cd repo; PYTHONPATH=. python3.12 -m scripts.launch_native_geometry_cold_offered_spot --extract-frozen "$root") >test.log 2>&1
phase=profile
systemd-run --unit=native-geometry-cold-offered --wait --pipe -p MemoryMax=7G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_geometry_cold_offered {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" 7516192768 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    command += '\n'.join('test -s "$root/'+name+'"' for name in ARTIFACTS if name.startswith('screen/'))+'\n'
    body = body[:start]+command+body[end:]
    subprocess.run(['bash','-n'],input=body,text=True,check=True)
    assert len(body.encode()) < 16384
    return body


def lifecycle_self_check():
    """Exercise the real shared lifecycle through this campaign; AWS is mocked."""
    from datetime import datetime, timezone
    import tempfile
    from unittest.mock import Mock, patch
    module = sys.modules[__name__]
    tokens = []
    for failure in (None, 'fsync', 'upload', 'poll', 'interrupt', 'multi-ack'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2,s3,session = Mock(),Mock(),Mock()
            session.client.side_effect = [ec2,s3]
            ec2.describe_instances.return_value = {'Reservations':[]}
            ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[
                {'SpotPrice':'.1','Timestamp':datetime.now(timezone.utc)}]}
            instance_ids = ['i-owned', 'i-extra'] if failure == 'multi-ack' else ['i-owned']
            ec2.run_instances.return_value = {'Instances':[{'InstanceId':value} for value in instance_ids]}
            events=[]
            ec2.terminate_instances.side_effect = lambda **kwargs: events.append('terminated')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kwargs: events.append('wait')
            def collected(*args):
                assert events == ['terminated','wait']
                events.append('collected')
                return dict(status='complete',phase='complete',exit_code=0,artifacts={name:{} for name in ARTIFACTS})
            proof={'config_sha256':'a'*64}
            error = KeyboardInterrupt() if failure == 'interrupt' else RuntimeError('mock failure')
            writes=[None,None,OSError('upload')] if failure == 'upload' else [None,None,None]
            with patch.object(module,'ROOT',Path(tmp)),patch.object(module,'preflight',return_value=proof), \
                    patch.object(module,'user_data',return_value='mock'),patch.object(module,'poll',side_effect=error if failure in ('poll','interrupt') else None), \
                    patch.object(module,'collect',side_effect=collected),patch.object(ranges.boto3,'Session',return_value=session), \
                    patch.object(ranges.subprocess,'check_output',side_effect=['','0'*40,b'archive']), \
                    patch.object(peer,'missing',return_value=True),patch.object(peer,'put_if_absent',side_effect=writes), \
                    patch.object(ranges.os,'fsync',side_effect=OSError('persist') if failure == 'fsync' else None):
                try: ranges.main('a0002' if failure == 'multi-ack' else 'a0001',campaign=module)
                except (OSError,RuntimeError,KeyboardInterrupt): assert failure is not None
                else: assert failure in (None, 'multi-ack')
            assert events == ['terminated','wait','collected']
            ec2.terminate_instances.assert_called_once_with(InstanceIds=instance_ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=instance_ids)
            ec2.run_instances.assert_called_once()
            assert ec2.run_instances.call_args.kwargs['ClientToken'].startswith(TOKEN_PREFIX)
            attempt = 'a0002' if failure == 'multi-ack' else 'a0001'
            tokens.append(ec2.run_instances.call_args.kwargs['ClientToken'])
            assert ec2.run_instances.call_args.kwargs['InstanceType'] == 'c7g.2xlarge'
            reservation=json.loads((Path(tmp)/NAME/attempt/'aws-reservation.json').read_bytes())
            assert reservation['schema']==SCHEMA and reservation['wall_seconds']==WALL
            assert reservation['compute_cap_usd']==COMPUTE_CAP and reservation['ebs_s3_allowance_usd']==.15
    assert tokens[0] != tokens[-1] and all(len(token) <= 64 for token in tokens)
    print('geometry controller ACK/unique-token/persistence/upload/poll/interrupt/terminate-before-collect PASS')


def collection_self_check():
    import io
    import tempfile
    from unittest.mock import Mock
    originals = (ranges.SCHEMA, ranges.ARTIFACTS)
    values = {name: ('authenticated '+name).encode() for name in ARTIFACTS}
    for mutation in (None, 'instance', 'archive', 'roster', 'body', 'length'):
        terminal = dict(schema=SCHEMA, instance_id='i-owned', source_commit='0'*40,
            source_archive_sha256='1'*64, status='complete', phase='complete', exit_code=0,
            artifacts={name: dict(bytes=len(value), sha256=peer.sha(value)) for name, value in values.items()})
        if mutation == 'instance': terminal['instance_id'] = 'i-other'
        if mutation == 'archive': terminal['source_archive_sha256'] = '2'*64
        if mutation == 'roster': terminal['artifacts']['unowned'] = dict(bytes=0, sha256=peer.sha(b''))
        if mutation == 'length': terminal['artifacts']['boundary-check.json']['bytes'] += 1
        def get_object(**kwargs):
            key = kwargs['Key']
            value = (json.dumps(terminal).encode() if key.endswith('/terminal.json')
                     else values[key.split('/artifacts/', 1)[1]])
            if mutation == 'body' and key.endswith('/artifacts/boundary-check.json'): value += b'changed'
            return dict(Body=io.BytesIO(value))
        s3 = Mock()
        s3.get_object.side_effect = get_object
        with tempfile.TemporaryDirectory() as tmp:
            try: collect(s3, 'mock', Path(tmp), 'i-owned', '0'*40, '1'*64)
            except AssertionError: assert mutation is not None
            else:
                assert mutation is None
                assert all(gzip.decompress((Path(tmp)/(name+'.gz')).read_bytes()) == value
                           for name, value in values.items())
        assert originals == (ranges.SCHEMA, ranges.ARTIFACTS)
    print('geometry closed-artifact identity/body/roster/length authentication PASS')


def self_check():
    """Authenticate real frozen bodies; mutate local copies; never use live AWS."""
    import contextlib
    import copy
    import io
    import os
    import shutil
    import tempfile
    from scripts import run_native_geometry_cold_offered as worker
    module = sys.modules[__name__]
    original = Path.cwd()
    with tempfile.TemporaryDirectory() as tmp:
        base, out = Path(tmp)/'repo', Path(tmp)/'out'
        def write(name, value):
            target = base/name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(value)
        for name in source_hashes(original): write(name, (original/name).read_bytes())
        extras = ('scripts/launch_native_geometry_cold_offered_spot.py',
            'scripts/launch_native_metadata_ranges_cold_spot.py',
            'scripts/launch_native_cold_first_query_spot.py', 'scripts/check_native_startup_build.py')
        for name in (*worker.CODE, *extras): write(name, (original/name).read_bytes())
        write(ITEMS_SOURCE['path'], (original/ITEMS_SOURCE['path']).read_bytes())
        shutil.copytree(original/FROZEN, base/FROZEN)
        # Synthetic campaign config, real immutable protocol/items/native authority.
        config = json.loads((original/ROOT/'cold-offered-config.json').read_bytes())
        verified = json.loads((base/FROZEN/'verification.json').read_bytes())
        config.update(schema=worker.SCHEMA, staging=worker.STAGING, items_source=ITEMS_SOURCE,
            items=json.loads((base/ITEMS_SOURCE['path']).read_bytes())['items'],
            binary=dict(sha256=BINARY_SHA, bytes=12470768),
            frozen_native_qualification=dict(path=str(FROZEN/'verification.json'), sha256=PROOF_SHA,
                source_commit=verified['source_commit'], source_archive_sha256=verified['source_archive_sha256'],
                terminal_sha256=TERMINAL_SHA),
            code_sha256={name: peer.sha((base/name).read_bytes()) for name in worker.CODE})
        write(CONFIG, json.dumps(config).encode())
        qualification = preflight(base)
        assert len(FROZEN_FILES) == 19 and len(ARTIFACTS) == 42 and len(set(ARTIFACTS)) == 42
        assert len(qualification['compiled_native_sha256']) == 9
        assert len(qualification['code_sha256']) == 15 and qualification['native_rebuilt'] is False
        originals = (runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS)
        body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', qualification)
        assert originals == (runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS)
        for token in ('--on-active=2700s', 'MemoryMax=7G', 'MemorySwapMax=0',
            'RuntimeMaxSec=2430', 'ulimit -v 4194304', 'TOKIO_WORKER_THREADS=4',
            'BORSUK_NATIVE_MEMORY_BYTES=1073741824', 'AWS_MAX_ATTEMPTS=1',
            'taskset -c 4-5', 'taskset -c 0-3', '--kill-after=30 2400',
            'scripts.run_native_geometry_cold_offered', '--extract-frozen', '7516192768'):
            assert token in body, token
        assert not any(token in body for token in ('rustup', 'cargo test', 'cargo build', 'phase=test'))
        for key, value in [('schema', 'borsuk-native-cold-offered-v1'), ('count', 63),
            ('k', 100), ('workers', 7), ('offered_qps', [1]), ('base_port', 18081),
            ('query_payload_timeout_seconds', 6), ('client_cpu_affinity', [0, 1]),
            ('worker_memory_bytes', 8*1024**3),
            ('staging', dict(candidate=dict(range_bytes=4194304, parallel_gets=4))),
            ('binary', dict(config['binary'], bytes=12471000)),
            ('frozen_native_qualification', dict(config['frozen_native_qualification'], source_commit='0'*40)),
            ('items_source', dict(ITEMS_SOURCE, sha256='0'*64)),
            ('items', [dict(config['items'][0], metadata_files={}), config['items'][1]])]:
            write(CONFIG, json.dumps(dict(config, **{key: value})).encode())
            try: preflight(base)
            except AssertionError: pass
            else: raise AssertionError('changed config accepted: '+key)
        write(CONFIG, json.dumps(config).encode())
        for name in ('Cargo.toml', GRAPH, worker.CODE[-1],
            FROZEN/'verification.json', FROZEN/'aws-terminal.json',
            FROZEN/'control/binaries/two_bit_http.gz', FROZEN/'screen/block0-records.jsonl.gz'):
            before = (base/name).read_bytes()
            write(name, (gzip.compress(gzip.decompress(before)+b'changed', mtime=0)
                         if str(name).endswith('.gz') else before+b'changed'))
            try: preflight(base)
            except AssertionError: pass
            else: raise AssertionError('changed file accepted: '+str(name))
            finally: write(name, before)
        out.mkdir()
        (out/'source-qualification.json').write_text(json.dumps(qualification))
        try:
            os.chdir(base)
            with contextlib.redirect_stdout(io.StringIO()): extract_frozen(out)
            assert (out/'binaries/two_bit_http').stat().st_mode & 0o111
            assert all(peer.sha((out/'frozen'/name).read_bytes()) == qualification['frozen_binary_artifacts'][name]['sha256']
                       for name in FROZEN_FILES)
            proof_path, binary = out/'boundary-check.json', out/'binaries/two_bit_http'
            proof = json.loads(proof_path.read_text())
            for mutation in (None, 'geometry', 'binary', 'proof', 'compiled', 'source', 'env'):
                shaped = copy.deepcopy(config)
                before = proof_path.read_bytes()
                if mutation == 'geometry': shaped['staging']['candidate']['parallel_gets'] = 16
                if mutation == 'binary': shaped['binary']['sha256'] = '0'*64
                if mutation == 'proof': proof_path.write_text(json.dumps(dict(proof, qualified=False)))
                if mutation == 'compiled': proof_path.write_text(json.dumps(dict(proof, compiled_native_sha256={})))
                if mutation == 'source': write('Cargo.toml', (base/'Cargo.toml').read_bytes()+b'changed')
                write(CONFIG, json.dumps(shaped).encode())
                with patch.object(worker.sys, 'argv', ['worker', str(CONFIG), worker.cold.sha(CONFIG),
                        str(binary), str(proof_path), str(out/'screen')]), \
                     patch.object(worker.os, 'sched_getaffinity', return_value={4, 5}), \
                     patch.dict(os.environ, TOKIO_WORKER_THREADS='8' if mutation == 'env' else '4',
                         BORSUK_NATIVE_MEMORY_BYTES='1073741824', AWS_MAX_ATTEMPTS='1'), \
                     patch.object(worker, 'run', return_value=dict(offered=768, successful=768, capacity_drops=0, errors=0)) as run, \
                     contextlib.redirect_stdout(io.StringIO()):
                    try: worker.main()
                    except AssertionError:
                        assert mutation is not None
                        run.assert_not_called()
                    else:
                        assert mutation is None
                        run.assert_called_once()
                proof_path.write_bytes(before)
                if mutation == 'source': write('Cargo.toml', (original/'Cargo.toml').read_bytes())
            assert worker.run is worker.offered.run
        finally: os.chdir(original)
        print('geometry frozen60/protocol/source/binary/runtime guards PASS; user-data bytes='+str(len(body.encode())))
    lifecycle_self_check()
    collection_self_check()


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    elif sys.argv[1:2] == ['--extract-frozen']:
        assert len(sys.argv) == 3
        extract_frozen(sys.argv[2])
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_geometry_cold_offered_spot aNNNN'
        with open('/tmp/borsuk-native-geometry-cold-offered-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ranges.main(sys.argv[1],campaign=sys.modules[__name__])
