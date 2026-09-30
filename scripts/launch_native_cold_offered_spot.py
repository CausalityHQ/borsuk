"""Reuse a closed qualified native binary and the existing owned-Spot lifecycle."""
import gzip
import json
from pathlib import Path
import subprocess
import sys

from scripts import launch_native_metadata_ranges_cold_spot as ranges
from scripts import launch_native_cold_first_query_spot as cold
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts.check_native_startup_build import source_hashes, source_identity

ROOT = peer.ROOT
NAME = 'cold-offered'
PREFIX = 'research/native-union/20260930/cold-offered-'
TOKEN_PREFIX = 'cold-offered-'
TAG = 'borsuk-cold-offered'
CONFIG = ROOT / 'cold-offered-config.json'
SCHEMA = 'borsuk-native-cold-offered-spot-v1'
WALL = 2700
COMPUTE_CAP = .225
FROZEN = ROOT / 'metadata-ranges-cold/a0001'
PROOF_SHA = 'e24ddfbb646f5bca185429b2d027799dd9e7f8c9c4de3db8c7fbf7b5f4ee5826'
BINARY_SHA = '3d96aa35461bea6985d7e990f623aab9643ec55316be64af214c3c23dccc192d'
TERMINAL_SHA = '91ae24f6bc0f1a6680777d45e35e66f21906e81b639b1a86701de71fbf503235'
FROZEN_FILES = cold.FROZEN_FILES
ARTIFACTS = ('source-qualification.json', 'boundary-check.json', 'compiled-source.json',
    'binaries/two_bit_http', 'cpu.txt', 'test.log', 'run-closed.log',
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json',
    *(f'screen/rate{rate}-{dataset.lower()}-records.jsonl' for rate in range(6) for dataset in ('ReLAION','CoHere')),
    *('frozen/'+name for name in FROZEN_FILES))


def preflight(base=Path('.')):
    from scripts import run_native_cold_offered as worker
    body = (base/CONFIG).read_bytes()
    config = json.loads(body)
    assert config['schema'] == 'borsuk-native-cold-offered-v1'
    assert config['offered_qps'] == [.25,.5,1,2,4,8] and config['workers'] == 6
    assert config['count'] == 64 and config['k'] == 10 and config['base_port'] == 18080
    assert config['dataset_order'] == ['ReLAION','CoHere']
    assert config['client_cpu_affinity'] == [4,5] and config['native_cpu_affinity'] == [0,1,2,3]
    assert config['worker_limit_seconds'] == 2400 and config['machine_limit_seconds'] == WALL
    assert config['worker_memory_bytes'] == 7*1024**3 and config['native_memory_bytes'] == 1024**3
    assert config['gates'] == dict(recall_at_10_minimum=.95, all_offers_success=True,
        source_scorer_ordered_id_physical_parity=True, cold_p90_ms_exclusive_maximum=444)
    assert config['namespace_cold_start_included'] and config['previous_observed_development_panel']
    assert config['matched_vendor_measured'] is config['application_sq8_cache'] is False
    assert config['s3_service_cache'] == 'uncontrolled' and config['transport'] == 'loopback plain HTTP'
    assert (config['bucket'],config['region']) == (peer.BUCKET,peer.REGION)
    assert set(config['code_sha256']) == set(worker.CODE)
    for name,digest in config['code_sha256'].items(): assert peer.sha((base/name).read_bytes()) == digest,name
    verification_body = (base/FROZEN/'verification.json').read_bytes()
    assert peer.sha(verification_body) == PROOF_SHA
    verified = json.loads(verification_body)
    assert verified['valid_measurement'] and verified['diagnostic_gate_passed'] and verified['state'] == 'terminated'
    terminal_body = (base/FROZEN/'aws-terminal.json').read_bytes()
    assert peer.sha(terminal_body) == TERMINAL_SHA == verified['terminal_sha256']
    terminal = json.loads(terminal_body)
    assert terminal['schema'] == ranges.SCHEMA and terminal['status'] == terminal['phase'] == 'complete'
    assert terminal['exit_code'] == 0 and terminal['instance_id'] == verified['instance_id']
    assert set(terminal['artifacts']) == set(ranges.ARTIFACTS)
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
    assert boundary['qualified'] and boundary['green_status'] == boundary['release_status'] == 0
    assert boundary['current_full_suite_pass_claim'] is False
    identities = source_hashes(base)
    assert len(identities) == boundary['source_file_count'] == 395
    assert source_identity(identities) == boundary['source_identity_sha256']
    assert compiled == boundary['compiled_native_sha256']
    for name,digest in compiled.items(): assert identities[name] == peer.sha(bodies['compiled-source/'+name]) == digest,name
    assert peer.sha(bodies['binaries/two_bit_http']) == boundary['binary_sha256'] == BINARY_SHA
    assert len(bodies['binaries/two_bit_http']) == 12471000
    reference_body = (base/ROOT/'metadata-ranges-config.json').read_bytes()
    assert peer.sha(reference_body) == verified['config_sha256']
    reference = json.loads(reference_body)
    assert config['items'] == reference['items']
    assert config['binary'] == terminal['artifacts']['binaries/two_bit_http']
    frozen = dict(path=str(FROZEN/'verification.json'),sha256=PROOF_SHA,
        source_commit=verified['source_commit'],source_archive_sha256=verified['source_archive_sha256'],
        terminal_sha256=TERMINAL_SHA)
    assert config['frozen_native_qualification'] == frozen
    extras = ('scripts/launch_native_cold_offered_spot.py','scripts/launch_native_metadata_ranges_cold_spot.py',
        'scripts/launch_native_cold_first_query_spot.py','scripts/check_native_startup_build.py')
    return dict(config_sha256=peer.sha(body),code_sha256=dict(config['code_sha256'],
        **{name:peer.sha((base/name).read_bytes()) for name in extras}),
        frozen_native_qualification=frozen, frozen_binary_artifacts={name:terminal['artifacts'][name] for name in FROZEN_FILES},
        source_identity_sha256=source_identity(identities),source_file_count=395,compiled_native_sha256=compiled,
        native_rebuilt=False,native_source_commit=verified['source_commit'],current_full_suite_pass_claim=False)


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
    original = startup.WALL
    startup.WALL = WALL
    try: startup.poll(ec2,s3,prefix,instance_id,started)
    finally: startup.WALL = original


def collect(s3,prefix,out,instance_id,commit,digest):
    raw = s3.get_object(Bucket=peer.BUCKET,Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS)
    for name,identity in terminal['artifacts'].items():
        value = s3.get_object(Bucket=peer.BUCKET,Key=prefix+'/artifacts/'+name)['Body'].read()
        assert len(value) == identity['bytes'] and peer.sha(value) == identity['sha256'],name
        path = out/(name+'.gz');path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(gzip.compress(value,mtime=0))
    return terminal


def user_data(commit,archive_sha,archive_key,prefix,qualification):
    runner.WALL_SECONDS,runner.SCHEMA,runner.ARTIFACTS = WALL,SCHEMA,ARTIFACTS
    body = runner.user_data(commit,archive_sha,archive_key,prefix).replace('v174-relaid-bind-compile','native-cold-offered')
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
(cd repo; PYTHONPATH=. python3.12 -m scripts.launch_native_cold_offered_spot --extract-frozen "$root") >test.log 2>&1
phase=profile
systemd-run --unit=native-cold-offered --wait --pipe -p MemoryMax=7G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_cold_offered {CONFIG} {qualification['config_sha256']} "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" 7516192768 || exit 96; exit "$code"' _ "$root" >profile.log 2>&1
'''
    command += '\n'.join('test -s "$root/'+name+'"' for name in ARTIFACTS if name.startswith('screen/'))+'\n'
    body = body[:start]+command+body[end:]
    subprocess.run(['bash','-n'],input=body,text=True,check=True)
    assert len(body.encode()) < 16384
    return body


def self_check():
    """Exercise the real shared lifecycle through this campaign; AWS is mocked."""
    from datetime import datetime, timezone
    import tempfile
    from unittest.mock import Mock, patch
    module = sys.modules[__name__]
    for failure in (None, 'fsync', 'upload', 'poll', 'interrupt'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2,s3,session = Mock(),Mock(),Mock()
            session.client.side_effect = [ec2,s3]
            ec2.describe_instances.return_value = {'Reservations':[]}
            ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[
                {'SpotPrice':'.1','Timestamp':datetime.now(timezone.utc)}]}
            ec2.run_instances.return_value = {'Instances':[{'InstanceId':'i-owned'}]}
            events=[]
            def terminate(client,nodes):
                assert client is ec2 and nodes == {'0':{'instance_id':'i-owned'}}
                events.append('terminated')
            def collected(*args):
                assert events == ['terminated']
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
                    patch.object(ranges.os,'fsync',side_effect=OSError('persist') if failure == 'fsync' else None), \
                    patch.object(startup,'terminate_owned',side_effect=terminate):
                try: ranges.main('a0001',campaign=module)
                except (OSError,RuntimeError,KeyboardInterrupt): assert failure is not None
                else: assert failure is None
            assert events == ['terminated','collected']
            ec2.run_instances.assert_called_once()
            assert ec2.run_instances.call_args.kwargs['ClientToken'].startswith(TOKEN_PREFIX)
            reservation=json.loads((Path(tmp)/NAME/'a0001/aws-reservation.json').read_bytes())
            assert reservation['schema']==SCHEMA and reservation['wall_seconds']==WALL
            assert reservation['compute_cap_usd']==COMPUTE_CAP
    print('cold offered controller PASS (mock ACK/persistence/upload/poll/interrupt/termination-before-collection)')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    elif sys.argv[1] == '--extract-frozen': extract_frozen(sys.argv[2])
    else: ranges.main(sys.argv[1],campaign=sys.modules[__name__])
