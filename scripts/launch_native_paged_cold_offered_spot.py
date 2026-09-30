"""Reuse the closed paged binary with the shared bounded owned-Spot lifecycle."""
import base64
import fcntl
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from scripts import launch_native_metadata_ranges_cold_spot as ranges
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts import run_native_paged_cold_offered as worker
from scripts.check_native_startup_build import FOCUSED_ARM, source_hashes, source_identity

ROOT = Path('docs/research/source-paging-20260930')
NAME = 'offered'
CONFIG = ROOT / 'offered-config.json'
CONFIG_SHA = '422465367366c909b061e8d211a30386e10ad2f56c70032fe38aa954ca05534d'
SCHEMA = 'borsuk-native-paged-cold-offered-spot-v1'
PREFIX = 'research/source-paging/20260930/offered-'
TOKEN_PREFIX = 'paged-cold-offered-'
TAG = 'borsuk-paged-cold-offered'
SUBNET = 'subnet-00243d923761c047c'
WALL = 2700
COMPUTE_CAP = .225
FROZEN = ROOT / 'cold/a0004'
FROZEN_AUTHORITY = dict(path=str(FROZEN/'verification.json'),
    sha256='6f7c10c0186045fb2aa0108a669c7535a188698e8420afbbc7d73e0994164dd4',
    source_commit='24342b9e5a740049cdb7f02e859af04b8eb9fa32',
    source_archive_sha256='3ea1f9903194747d7f741753fc66f48a21c89f22985e6bc9a4660b1f347abca9',
    terminal_sha256='ce8ddf306f932908ce66fb69f6daeae204936d843f2cff324ee3e72deef00c05')
BINARY = dict(sha256='99cf800558a6cebc710a3da28f6f4afa5743680efc8839a0b7b849b60c11eace', bytes=12544008)
BINARY_SHA = BINARY['sha256']
BOUNDARY = dict(sha256='870e160718fc989f54b66344db81e2ab09e24a34e358a6065adfd70c0b294403', bytes=3981)
ITEMS_SOURCE = dict(path=str(ROOT/'cold-config.json'),
    sha256='92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9')
MANIFEST = ROOT / 'native-source-manifest.json'
MANIFEST_SHA = 'e23787927669acf07cbf0d5e687e99ef5a09b24f27e42e76de4c2070991a744c'
SOURCE_IDENTITY = '3f05bdfd2c399fb4f93ab5c73827c7b55d8fd31f73ed967e6ac235976a7d7f64'
NATIVE_COMMIT = '2b0827369965ede5414a23e7be3abfefb92166b9'
COMPILED = (*FOCUSED_ARM, *('crates/borsuk/src/'+name for name in (
    'sq8_s3_range.rs', 'sq8_page_authority.rs', 'two_bit_source.rs', 'two_bit_build.rs',
    'two_bit_index.rs', 'unit_centroid_graph.rs', 'bin/build_two_bit_graph_variant.rs',
    'bin/two_bit_plan_demo.rs', 'bin/two_bit_union_nomination.rs', 'bin/two_bit_walk_nomination.rs')),
    'crates/borsuk/tests/two_bit_application_ids.rs', 'crates/borsuk/tests/two_bit_gc_delayed_delete.rs')
TOOLCHAIN = ('rustc-version.txt', 'cargo-version.txt', 'cpuinfo.txt', 'arm-feature-tree.txt', 'x86-feature-tree.txt')
EXTRAS = ('scripts/launch_native_paged_cold_offered_spot.py', 'scripts/check_native_startup_build.py',
    'scripts/launch_native_metadata_ranges_cold_spot.py', 'scripts/launch_native_startup_profile_spot.py',
    'scripts/launch_native_peer_1m_spot.py', 'scripts/launch_v174_relaid_bind_compile_spot.py',
    'scripts/launch_v157_primary_feasibility_spot.py')
CODE = (*worker.CODE, *EXTRAS)
FROZEN_FILES = ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json',
    'source-qualification.json', 'run-closed.log', *TOOLCHAIN, *('compiled-source/'+name for name in COMPILED))
ARTIFACTS = ('source-qualification.json', 'boundary-check.json', 'compiled-source.json',
    'binaries/two_bit_http', 'cpu.txt', 'test.log', 'run-closed.log', 'profile.log',
    'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json',
    *(f'screen/rate{rate}-{dataset}-records.jsonl' for rate in range(6) for dataset in ('relaion','cohere')),
    *('frozen/'+name for name in FROZEN_FILES))


def preflight(base=Path('.')):
    base = Path(base)
    body = (base/CONFIG).read_bytes()
    assert peer.sha(body) == CONFIG_SHA
    config = json.loads(body)
    worker.validate_config(config, base)
    assert config['items_source'] == ITEMS_SOURCE
    assert config['binary'] == BINARY and config['boundary_proof'] == BOUNDARY
    assert config['native_source_manifest'] == dict(path=str(MANIFEST), sha256=MANIFEST_SHA)
    assert config['frozen_native_qualification'] == FROZEN_AUTHORITY
    manifest_body = (base/MANIFEST).read_bytes()
    assert peer.sha(manifest_body) == MANIFEST_SHA
    manifest = json.loads(manifest_body)
    identities = source_hashes(base)
    assert manifest['schema'] == 'borsuk-native-paged-source-manifest-v1'
    assert identities == manifest['source_sha256']
    assert len(identities) == manifest['source_file_count'] == config['native_source_file_count'] == 395
    assert source_identity(identities) == manifest['source_identity_sha256'] == config['native_source_identity_sha256'] == SOURCE_IDENTITY
    assert manifest['native_source_commit'] == NATIVE_COMMIT
    assert manifest['current_full_suite_pass_claim'] is False
    verified_body = (base/FROZEN_AUTHORITY['path']).read_bytes()
    assert peer.sha(verified_body) == FROZEN_AUTHORITY['sha256']
    verified = json.loads(verified_body)
    assert verified['valid_measurement'] is True and verified['state'] == 'terminated'
    terminal_body = (base/FROZEN/'aws-terminal.json').read_bytes()
    assert peer.sha(terminal_body) == FROZEN_AUTHORITY['terminal_sha256'] == verified['terminal_sha256']
    terminal = json.loads(terminal_body)
    assert terminal['schema'] == 'borsuk-native-paged-source-cold-spot-v1'
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == verified['instance_id']
    for key in ('source_commit', 'source_archive_sha256'):
        assert terminal[key] == verified[key] == FROZEN_AUTHORITY[key]
    assert len(terminal['artifacts']) == 51
    close = json.loads((base/FROZEN/'aws-closeout.json').read_bytes())
    assert close['state'] == 'terminated' and close['nodes'] == {'0': dict(instance_id=verified['instance_id'])}
    bodies = {}
    for name, identity in terminal['artifacts'].items():
        raw = gzip.decompress((base/FROZEN/(name+'.gz')).read_bytes())
        assert identity == dict(bytes=len(raw), sha256=peer.sha(raw)), name
        bodies[name] = raw
    boundary = json.loads(bodies['boundary-check.json'])
    compiled = json.loads(bodies['compiled-source.json'])
    historical = json.loads(bodies['source-qualification.json'])
    assert terminal['artifacts']['boundary-check.json'] == BOUNDARY
    assert terminal['artifacts']['binaries/two_bit_http'] == BINARY
    assert boundary['qualified'] is True and boundary['green_status'] == boundary['release_status'] == 0
    assert boundary['no_corpus_query'] is True and boundary['arm'] == 'candidate'
    assert boundary['current_full_suite_pass_claim'] is historical['current_full_suite_pass_claim'] is False
    assert boundary['focused_tests'] == ['source', 'source-walk', 'sq8-transport', 'object-native',
        'graph', 'generation', 'application-ids', 'gc', 'http']
    assert boundary['sha_backend']['arm_asm_selected'] is boundary['sha_backend']['cpu_sha2_capable'] is True
    assert boundary['sha_backend']['x86_asm_selected'] is False
    assert compiled == boundary['compiled_native_sha256'] == historical['compiled_native_sha256']
    assert set(compiled) == set(COMPILED) and len(compiled) == 20
    assert boundary['compiled_http_sha256'] == compiled['crates/borsuk/examples/two_bit_http.rs']
    for proof in (boundary, historical):
        assert proof['source_identity_sha256'] == SOURCE_IDENTITY and proof['source_file_count'] == 395
        assert proof['manifest_sha256'] == MANIFEST_SHA and proof['native_source_commit'] == NATIVE_COMMIT
        assert proof['binary_sha256'] == BINARY['sha256'] and proof['binary_bytes'] == BINARY['bytes']
        assert proof['config_sha256'] == proof['original_config_sha256'] == ITEMS_SOURCE['sha256']
        assert proof['resolved_config_sha256'] == terminal['resolved_config_sha256'] == verified['resolved_config_sha256']
    assert historical['config_path'] == ITEMS_SOURCE['path']
    assert terminal['config_sha256'] == terminal['original_config_sha256'] == verified['original_config_sha256'] == ITEMS_SOURCE['sha256']
    reference = json.loads((base/ITEMS_SOURCE['path']).read_bytes())
    assert json.loads(bodies['resolved-config.json']) == dict(reference, binary=BINARY)
    for name, digest in compiled.items():
        assert identities[name] == peer.sha(bodies['compiled-source/'+name]) == digest, name
    for key, name in (('rustc_sha256', 'rustc-version.txt'), ('cargo_sha256', 'cargo-version.txt'), ('cpuinfo_sha256', 'cpuinfo.txt')):
        assert peer.sha(bodies[name]) == boundary['sha_backend'][key]
    return dict(config_sha256=CONFIG_SHA, config_path=str(CONFIG), campaign_schema=SCHEMA,
        code_sha256={name: peer.sha((base/name).read_bytes()) for name in CODE},
        manifest_path=str(MANIFEST), manifest_sha256=MANIFEST_SHA,
        source_identity_sha256=SOURCE_IDENTITY, source_file_count=395,
        compiled_native_sha256=compiled, native_source_commit=NATIVE_COMMIT,
        frozen_native_qualification=FROZEN_AUTHORITY,
        frozen_binary_artifacts={name: terminal['artifacts'][name] for name in FROZEN_FILES},
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()),
        native_rebuilt=False, current_full_suite_pass_claim=False, build_evidence_is_historical=True,
        matched_control_latency_measured=False, matched_vendor_measured=False)


def extract_frozen(repo, out):
    repo, out = Path(repo), Path(out)
    proof = json.loads((out/'source-qualification.json').read_bytes())
    assert proof == preflight(repo)
    for name, identity in proof['frozen_binary_artifacts'].items():
        raw = gzip.decompress((repo/FROZEN/(name+'.gz')).read_bytes())
        assert identity == dict(bytes=len(raw), sha256=peer.sha(raw)), name
        path = out/'frozen'/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        if name in ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json'):
            destination = out/name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
    (out/'binaries/two_bit_http').chmod(0o755)
    print(json.dumps(dict(native_rebuilt=False, binary_sha256=BINARY['sha256'])))


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def collect(s3, prefix, out, instance_id, commit, digest):
    with patch.multiple(ranges, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        return ranges.collect(s3, prefix, out, instance_id, commit, digest)


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    with patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace('v174-relaid-bind-compile', 'native-paged-cold-offered')
    roster = json.dumps(ARTIFACTS, separators=(',', ':'))
    body = body.replace(' '.join(ARTIFACTS), "$(python3 -c 'import json; print(\" \".join(json.load(open(\"artifact-roster.json\"))))')")
    body = body.replace(repr(ARTIFACTS), 'json.loads(Path("artifact-roster.json").read_text())')
    body = body.replace('phase=bootstrap', "cat >artifact-roster.json <<'ROSTER'\n"+roster+"\nROSTER\nphase=bootstrap")
    start, end = body.index('phase=install\n'), body.index('phase=complete')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':')).encode()
    encoded = base64.b64encode(gzip.compress(proof, mtime=0)).decode()
    command = f'''phase=install
dnf install -y -q tar gzip time python3.12
python3.12 -m ensurepip
python3.12 -m pip install -q boto3
python3.12 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("{encoded}")))'
phase=binary-qualification
lscpu >cpu.txt
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_paged_cold_offered_spot --extract-frozen "$root/repo" "$root" >test.log 2>&1
phase=profile
systemd-run --unit=native-paged-cold-offered --wait --pipe -p MemoryMax=7G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_paged_cold_offered {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" 7516192768 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    command += '\n'.join('test -s "$root/'+name+'"' for name in ARTIFACTS if name.startswith('screen/'))+'\n'
    body = body[:start]+command+body[end:]
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
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
            launch = ec2.run_instances.call_args.kwargs
            assert launch['MinCount'] == launch['MaxCount'] == 1
            assert launch['NetworkInterfaces'][0]['SubnetId'] == SUBNET
            assert launch['NetworkInterfaces'][0]['AssociatePublicIpAddress'] is True
            assert launch['InstanceMarketOptions']['SpotOptions'] == dict(
                InstanceInterruptionBehavior='terminate', SpotInstanceType='one-time', MaxPrice='0.30')
            assert launch['BlockDeviceMappings'][0]['Ebs'] == dict(
                DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType='gp3')
            reservation=json.loads((Path(tmp)/NAME/attempt/'aws-reservation.json').read_bytes())
            assert reservation['schema']==SCHEMA and reservation['wall_seconds']==WALL
            assert reservation['compute_cap_usd']==COMPUTE_CAP and reservation['ebs_s3_allowance_usd']==.15
    assert tokens[0] != tokens[-1] and all(len(token) == 64 for token in tokens)
    print('paged offered controller ACK/unique-token/persistence/upload/poll/interrupt/terminate-before-collect PASS')


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
    print('paged offered closed-artifact identity/body/roster/length authentication PASS')


def self_check():
    """Real frozen authority and CLI; synthetic failures; no live AWS/native work."""
    import contextlib
    import copy
    import io
    import shutil
    import tempfile
    original = Path.cwd()
    proof = preflight()
    assert len(CODE) == len(set(CODE)) == 20 and len(worker.CODE) == 13
    assert len(COMPILED) == 20 and len(FROZEN_FILES) == 30
    assert len(ARTIFACTS) == len(set(ARTIFACTS)) == 53
    assert proof['native_rebuilt'] is proof['current_full_suite_pass_claim'] is False
    assert proof['build_evidence_is_historical'] is True
    originals = (runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS)
    archive_sha = 'a'*64
    archive_key = 'research/native-library-check/sources/'+archive_sha+'.tar.gz'
    body = user_data('0'*40, archive_sha, archive_key, PREFIX+'a0001', proof)
    assert originals == (runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS)
    for token in ('--on-active=2700s', 'MemoryMax=7G', 'MemorySwapMax=0',
        'RuntimeMaxSec=2430', 'ulimit -v 4194304', 'TOKIO_WORKER_THREADS=4',
        'AWS_MAX_ATTEMPTS=1', 'BORSUK_NATIVE_MEMORY_BYTES=1073741824',
        '--kill-after=30 2400', 'taskset -c 4-5', 'taskset -c 0-3', '7516192768',
        'scripts.run_native_paged_cold_offered',
        'PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_paged_cold_offered_spot --extract-frozen "$root/repo" "$root"'):
        assert token in body, token
    assert not any(token in body for token in ('rustup', 'cargo test', 'cargo build', 'phase=test'))
    encoded = body.split('base64.b64decode("', 1)[1].split('"', 1)[0]
    assert json.loads(gzip.decompress(base64.b64decode(encoded))) == proof
    def rejected(call):
        try: call()
        except AssertionError: pass
        else: raise AssertionError('tampering accepted')
    with tempfile.TemporaryDirectory() as tmp:
        base, out = Path(tmp)/'repo', Path(tmp)/'out'
        def write(name, raw):
            path = base/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        for name in (*source_hashes(original), *CODE, ITEMS_SOURCE['path'], MANIFEST, CONFIG):
            write(name, (original/name).read_bytes())
        shutil.copytree(original/FROZEN, base/FROZEN)
        assert preflight(base) == proof
        config_body = (base/CONFIG).read_bytes()
        config = json.loads(config_body)
        for key, value in [('schema', 'borsuk-native-cold-offered-v1'), ('count', 63), ('k', 100),
            ('offered_qps', list(reversed(worker.RATES))), ('workers', 7), ('base_port', 18081),
            ('worker_limit_seconds', 2401), ('machine_limit_seconds', 2701),
            ('query_payload_timeout_seconds', 6), ('namespace_connect_deadline_seconds', 46),
            ('native_process_limit_seconds', 61), ('client_cpu_affinity', [0, 1]),
            ('worker_memory_bytes', 8*1024**3), ('native_memory_bytes', 2*1024**3),
            ('staging', dict(candidate=dict(range_bytes=4194304, parallel_gets=4))),
            ('binary', dict(BINARY, bytes=BINARY['bytes']+1)),
            ('boundary_proof', dict(BOUNDARY, sha256='0'*64)),
            ('frozen_native_qualification', dict(FROZEN_AUTHORITY, source_commit='0'*40)),
            ('items_source', dict(ITEMS_SOURCE, sha256='0'*64)),
            ('items', [dict(config['items'][0], metadata_files={}), config['items'][1]])]:
            write(CONFIG, json.dumps(dict(config, **{key:value})).encode())
            rejected(lambda: preflight(base))
        write(CONFIG, config_body)
        for name in ('Cargo.toml', COMPILED[-1], worker.CODE[-1], MANIFEST,
            ITEMS_SOURCE['path'], FROZEN/'verification.json', FROZEN/'aws-terminal.json',
            FROZEN/'aws-closeout.json', FROZEN/'binaries/two_bit_http.gz',
            FROZEN/'source-qualification.json.gz', FROZEN/'boundary-check.json.gz',
            FROZEN/'compiled-source.json.gz', FROZEN/'rustc-version.txt.gz',
            FROZEN/'screen/relaion-records.jsonl.gz'):
            before = (base/name).read_bytes()
            if str(name).endswith('.gz'):
                changed = gzip.compress(gzip.decompress(before)+b'changed', mtime=0)
            elif str(name).endswith('aws-closeout.json'):
                changed = json.dumps(dict(json.loads(before), state='running')).encode()
            else: changed = before+b'changed'
            write(name, changed)
            rejected(lambda: preflight(base))
            write(name, before)
        out.mkdir()
        (out/'source-qualification.json').write_text(json.dumps(proof))
        subprocess.run([sys.executable, '-B', '-m', 'scripts.launch_native_paged_cold_offered_spot',
            '--extract-frozen', str(base), str(out)], cwd=out,
            env=dict(os.environ, PYTHONPATH=str(base)), check=True, stdout=subprocess.PIPE)
        assert (out/'binaries/two_bit_http').stat().st_mode & 0o111
        for name, identity in proof['frozen_binary_artifacts'].items():
            raw = (out/'frozen'/name).read_bytes()
            assert identity == dict(bytes=len(raw), sha256=peer.sha(raw))
        assert (out/'frozen/source-qualification.json').read_bytes() != (out/'source-qualification.json').read_bytes()
        (out/'source-qualification.json').write_text(json.dumps(dict(proof,
            frozen_native_qualification=dict(FROZEN_AUTHORITY, terminal_sha256='0'*64))))
        rejected(lambda: extract_frozen(base, out))
        (out/'source-qualification.json').write_text(json.dumps(proof))
        boundary_path = out/'boundary-check.json'
        boundary_body = boundary_path.read_bytes()
        binary_path = out/'binaries/two_bit_http'
        binary_body = binary_path.read_bytes()
        (out/'screen').mkdir()
        try:
            os.chdir(base)
            for mutation in (None, 'config', 'binary', 'boundary', 'compiled', 'source', 'env'):
                shaped = copy.deepcopy(config)
                if mutation == 'config': shaped['offered_qps'] = list(reversed(worker.RATES))
                if mutation == 'binary': binary_path.write_bytes(binary_body+b'changed')
                if mutation in ('boundary', 'compiled'):
                    boundary = json.loads(boundary_body)
                    boundary.update(qualified=False) if mutation == 'boundary' else boundary.update(compiled_native_sha256={})
                    boundary_path.write_text(json.dumps(boundary))
                if mutation == 'source': write('Cargo.toml', (original/'Cargo.toml').read_bytes()+b'changed')
                write(CONFIG, json.dumps(shaped).encode())
                with patch.object(worker.sys, 'argv', ['worker', str(CONFIG), worker.offered.cold.sha(CONFIG),
                        str(binary_path), str(boundary_path), str(out/'screen')]), \
                     patch.object(worker.os, 'sched_getaffinity', return_value={4,5}), \
                     patch.dict(os.environ, TOKIO_WORKER_THREADS='8' if mutation == 'env' else '4',
                        BORSUK_NATIVE_MEMORY_BYTES='1073741824', AWS_MAX_ATTEMPTS='1'), \
                     patch.object(worker.offered, 'run', return_value=dict(offered=768,
                        successful=768, capacity_drops=0, errors=0)) as run, \
                     contextlib.redirect_stdout(io.StringIO()):
                    if mutation is None:
                        worker.main()
                        run.assert_called_once()
                    else:
                        rejected(worker.main)
                        run.assert_not_called()
                binary_path.write_bytes(binary_body)
                boundary_path.write_bytes(boundary_body)
                write('Cargo.toml', (original/'Cargo.toml').read_bytes())
        finally: os.chdir(original)
    lifecycle_self_check()
    collection_self_check()
    print('PASS frozen51/whole395/compiled20/toolchain5/config/runtime/realCLI guards; user-data bytes='+str(len(body.encode())))


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    elif sys.argv[1:2] == ['--extract-frozen']:
        assert len(sys.argv) == 4
        extract_frozen(*sys.argv[2:])
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_paged_cold_offered_spot aNNNN'
        with open('/tmp/borsuk-native-paged-cold-offered-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ranges.main(sys.argv[1], campaign=sys.modules[__name__])
