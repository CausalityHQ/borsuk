import datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import tarfile

root = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-control-build-a0001')
protocol = json.loads((root / 'protocol.json').read_text())
job = json.loads((root / 'active-job.json').read_text())
terminal = json.loads((root / 'terminal.json').read_text())
assert job['status'] == 'TERMINATED_COLLECTED_NOT_YET_VERIFIED'
assert json.loads((root / 'wait.json').read_text())['exit'] == 0
assert any(item['InstanceId'] == job['instance_id'] and item['CurrentState']['Name'] in ('shutting-down', 'terminated') for item in json.loads((root / 'termination.json').read_text())['TerminatingInstances'])
assert terminal['schema'] == 'borsuk-two-bit-four-row-control-build-v1'
assert terminal['instance_id'] == job['instance_id']
assert terminal['source_commit'] == terminal['candidate_commit'] == protocol['candidate']
assert terminal['original_exit'] == terminal['exit'] == terminal['native_exit'] == 0
assert terminal['native_source_identity_sha256'] == protocol['source']['native_identity']
assert terminal['control_native_identity_sha256'] == protocol['source']['control']['native_identity']
assert terminal['control_library_source'] == protocol['source']['control']['library_source']
assert terminal['source_archive_sha256'] == protocol['source']['archive_sha256']
archive = root / 'evidence.tar.gz'
assert archive.stat().st_size == terminal['evidence']['bytes'] < 64 * 1024 * 1024
assert hashlib.sha256(archive.read_bytes()).hexdigest() == terminal['evidence']['sha256']
pins = {}
for line in (root / 'artifacts.sha256').read_text().splitlines():
    digest, name = line.split('  ', 1)
    name = name.removeprefix('./')
    assert name not in pins
    pins[name] = digest
data, total = {}, 0
with tarfile.open(archive) as tar:
    for member in tar:
        if member.isdir(): continue
        name = member.name.removeprefix('./')
        assert member.isfile() and name in pins and name not in data
        assert not name.startswith('/') and '..' not in PurePosixPath(name).parts
        total += member.size
        assert member.size <= 64 * 1024 * 1024 and total <= 128 * 1024 * 1024
        body = tar.extractfile(member).read()
        assert hashlib.sha256(body).hexdigest() == pins[name]
        data[name] = body
assert set(data) == set(pins)
for name, pin in protocol['inputs'].items():
    assert len(data[name]) == pin['bytes'] and hashlib.sha256(data[name]).hexdigest() == pin['sha256']
for stem, pin_name in [('source', 'source-files.sha256'), ('control-source', 'control-source-files.sha256')]:
    mapping = dict((line.split('  ', 1)[1], line.split('  ', 1)[0]) for line in data[pin_name].decode().splitlines())
    assert len(mapping) == protocol['source']['file_count']
    for suffix in ('before', 'after'):
        assert data[stem + '-' + suffix + '.log'].decode().splitlines() == [name + ': OK' for name in mapping]
    native_name = 'native-source.json' if stem == 'source' else 'control-native-source.json'
    native = json.loads(data[native_name])
    assert len(native) == protocol['source']['native_file_count']
    assert all(mapping[name] == digest for name, digest in native.items())
    identity = hashlib.sha256(b''.join(name.encode() + b'\0' + digest.encode() + b'\n' for name, digest in sorted(native.items()))).hexdigest()
    assert identity == (protocol['source']['native_identity'] if stem == 'source' else protocol['source']['control']['native_identity'])
stage = protocol['serial_stages'][0]
assert len(protocol['serial_stages']) == 1 and stage['name'] == 'control-release'
actual = [shlex.split(line)[1:] for line in data['gates.sh'].decode().splitlines() if line.startswith(' stage ')]
assert actual == [[stage['name'], *stage['argv']]]
assert data['control-release.command'].decode().strip() == ' '.join(stage['argv'])
for name in ('control-release.native-exit', 'control-release.tee-exit', 'gates-exit', 'final-exit'):
    assert int(data[name]) == 0
assert re.search(r'Exit status: 0\s*$', data['control-release.time'].decode())
assert data['control-target-admission.txt'].strip() == b'candidate and control targets absent; isolated control target absent before Cargo'
assert not data['compiler-env.txt'].strip()
assert data['cargo-config-admission.txt'].strip() == b'no ancestor/source/CARGO_HOME/root Cargo configuration overrides'
assert data['toolchain.txt'].decode().splitlines()[0].startswith('rustc 1.98.0 ')
assert data['toolchain.txt'].decode().splitlines()[1].startswith('cargo 1.98.0 ')
profile = [line.split('  ', 1) for line in data['matched-profile-inputs.sha256'].decode().splitlines()]
assert len(profile) == 4 and profile[0][0] == profile[2][0] and profile[1][0] == profile[3][0]
prior = root.parent / 'two-bit-four-row-qualification-a0004'
with tarfile.open(prior / 'evidence.tar.gz') as tar:
    member = next(member for member in tar if member.name.removeprefix('./') == 'matched-profile-inputs.sha256')
    assert tar.extractfile(member).read() == data['matched-profile-inputs.sha256']
assert data['cpu.max.before'].strip() == b'200000 100000'
assert data['memory.max.before'].strip() == b'8589934592'
assert data['memory.swap.max.before'].strip() == b'0'
assert data['pids.max.before'].strip() == b'512'
for prefix in ('', 'global-'):
    events = dict(line.split() for line in data[prefix + 'memory.events.after'].decode().splitlines())
    assert int(events['oom']) == int(events['oom_kill']) == 0
    assert int(data[prefix + 'memory.peak.after']) <= 8589934592
    assert int(data[prefix + 'memory.swap.peak.after']) == 0
assert int(data['global-pids.current.after']) == 0
assert b'ActiveState=inactive' in data['global-systemd-after.txt']
assert b'MainPID=0' in data['systemd-after.txt'] and b'ExecMainStatus=0' in data['systemd-after.txt']
records = []
for line in data['control-release.log'].decode().splitlines():
    if line.startswith('{'):
        item = json.loads(line)
        if item.get('reason') == 'compiler-artifact' and item.get('manifest_path') == '/mnt/borsuk-http/control-source/crates/borsuk/Cargo.toml':
            records.append(item)
library = [item for item in records if item['target']['name'] == 'borsuk' and item['target']['kind'] == ['lib']]
assert len(library) == 1 and library[0]['fresh'] is False and library[0]['features'] == ['default']
assert library[0]['target']['src_path'] == '/mnt/borsuk-http/control-source/crates/borsuk/src/lib.rs'
selected = {}
for role, kind, directory, relative in [('control-check_cohere_native_baseline', ['bin'], '/mnt/borsuk-http/target-control-isolated/release', 'src/bin/check_cohere_native_baseline.rs'), ('control-two_bit_http', ['example'], '/mnt/borsuk-http/target-control-isolated/release/examples', 'examples/two_bit_http.rs')]:
    name = role.removeprefix('control-')
    matches = [item for item in records if item['target']['name'] == name and item['target']['kind'] == kind]
    assert len(matches) == 1
    item = matches[0]
    assert item['fresh'] is False and item['profile']['test'] is False
    assert item['target']['src_path'] == '/mnt/borsuk-http/control-source/crates/borsuk/' + relative
    path = PurePosixPath(item['executable'])
    assert str(path.parent) == directory and path.name == name
    pin = json.loads(data[role + '.binary.json'])
    assert pin['role'] == role and type(pin['bytes']) is int and pin['bytes'] > 0
    assert re.fullmatch('[0-9a-f]{64}', pin['sha256'])
    assert int(data[role + '.binary-upload-exit']) == 0
    candidate_pin = json.loads((prior / (role.replace('control-', 'candidate-') + '.binary.json')).read_text())
    assert pin['sha256'] != candidate_pin['sha256'], 'paired binaries must differ before stack/performance audit'
    selected[role] = item
    (root / (role + '.binary.json')).write_bytes(data[role + '.binary.json'])
assert json.loads(data['control-compiler-artifacts.json']) == {'library': library[0], 'serving': selected}
receipt = {'status': 'ISOLATED_CONTROL_BUILD_VERIFIED_SERVING_STACK_PENDING', 'instance_id': job['instance_id'], 'candidate': protocol['candidate'], 'control_library_source': terminal['control_library_source'], 'control_native_identity_sha256': terminal['control_native_identity_sha256'], 'artifact_count': len(data), 'stage_native_exit': 0, 'stage_tee_exit': 0, 'fresh_control_library_and_two_serving_artifacts': True, 'candidate_and_control_serving_hashes_differ': True, 'matched_toolchain_profile_inputs': True, 'source_before_after_equal': True, 'terminated_waited_and_drained': True, 'serving_stack_qualified': False, 'primitive_allowed': False, 'performance_claim': False, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
(root / 'independent-verification.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
