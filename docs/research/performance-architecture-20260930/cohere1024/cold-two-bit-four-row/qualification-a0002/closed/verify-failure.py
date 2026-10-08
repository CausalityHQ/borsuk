import pathlib, json, hashlib, tarfile, re, shlex, datetime

root = pathlib.Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0002')
protocol = json.loads((root / 'protocol.json').read_text())
job = json.loads((root / 'active-job.json').read_text())
terminal = json.loads((root / 'terminal.json').read_text())
assert job['status'] == 'TERMINATED_COLLECTED_NOT_YET_VERIFIED'
assert json.loads((root / 'wait.json').read_text())['exit'] == 0
termination = json.loads((root / 'termination.json').read_text())
assert any(x['InstanceId'] == job['instance_id'] and x['CurrentState']['Name'] in ('shutting-down', 'terminated')
           for x in termination['TerminatingInstances'])
assert terminal['instance_id'] == job['instance_id']
assert terminal['candidate_commit'] == terminal['source_commit'] == protocol['candidate']
assert terminal['schema'] == 'borsuk-two-bit-four-row-native-qualification-v1'
assert terminal['native_source_identity_sha256'] == protocol['source']['native_identity']
assert terminal['native_source_file_count'] == protocol['source']['native_file_count']
assert terminal['source_archive_sha256'] == protocol['source']['archive_sha256']
userdata = (root / 'user-data.sh').read_bytes()
assert len(userdata) == protocol['userdata']['bytes']
assert hashlib.sha256(userdata).hexdigest() == protocol['userdata']['sha256']
archive = root / 'evidence.tar.gz'
assert archive.stat().st_size == terminal['evidence']['bytes'] < 64 * 1024 * 1024
assert hashlib.sha256(archive.read_bytes()).hexdigest() == terminal['evidence']['sha256']
pins = {}
for line in (root / 'artifacts.sha256').read_text().splitlines():
    digest, name = line.split('  ', 1)
    name = name.removeprefix('./')
    assert name not in pins
    pins[name] = digest
data = {}
total = 0
with tarfile.open(archive) as tar:
    for member in tar:
        if member.isdir():
            continue
        assert member.isfile()
        name = member.name.removeprefix('./')
        assert name in pins and name not in data
        assert name and not name.startswith('/') and '..' not in pathlib.PurePosixPath(name).parts
        total += member.size
        assert member.size <= 64 * 1024 * 1024 and total <= 128 * 1024 * 1024
        body = tar.extractfile(member).read()
        assert hashlib.sha256(body).hexdigest() == pins[name]
        data[name] = body
assert set(data) == set(pins)
for name, pin in protocol['inputs'].items():
    assert len(data[name]) == pin['bytes'] and hashlib.sha256(data[name]).hexdigest() == pin['sha256']
native = json.loads(data['native-source.json'])
assert native == json.loads((root / 'native-source.json').read_text())
identity = hashlib.sha256(b''.join(p.encode() + b'\0' + h.encode() + b'\n'
                                  for p, h in sorted(native.items()))).hexdigest()
assert identity == protocol['source']['native_identity']
source_map = data['source-files.sha256'].decode()
assert source_map == (root / 'source-files.sha256').read_text()
support = {line.split('  ', 1)[1]: line.split('  ', 1)[0] for line in source_map.splitlines()}
assert len(support) == protocol['source']['file_count']
assert all(support[p] == h for p, h in native.items())
for name in ('source-before', 'source-after'):
    assert data[name + '.log'].decode().splitlines() == [p + ': OK' for p in support]
commands = {}
for line in data['gates.sh'].decode().splitlines():
    if line.startswith(' stage '):
        words = shlex.split(line)
        assert words[1] not in commands
        commands[words[1]] = ' '.join(words[2:])
assert list(commands) == ['runner-debug', 'planner-debug', 'codec-debug', 'probe-compile', 'codec-release', 'planner-release', 'runner-release', 'integration', 'release', 'control-release', 'clippy', 'test-build', 'scalar-control-release']
assert commands == {stage['name']: ' '.join(stage['argv']) for stage in protocol['serial_stages']}
assert not data['compiler-env.txt'].strip()
toolchain = data['toolchain.txt'].decode().splitlines()
assert len(toolchain) == 2
assert toolchain[0].startswith('rustc 1.98.0 ') and toolchain[1].startswith('cargo 1.98.0 ')
assert re.search(rb'^flags.*\bavx2\b', data['cpuinfo.txt'], re.M)
required = json.loads(data['mandatory-tests.json'])
stages = []
for name, command in commands.items():
    assert data[name + '.command'].decode().strip() == command
    assert int(data[name + '.native-exit']) == int(data[name + '.tee-exit']) == 0
    started = datetime.datetime.fromisoformat(data[name + '.started'].decode().strip().replace('Z','+00:00'))
    finished = datetime.datetime.fromisoformat(data[name + '.finished'].decode().strip().replace('Z','+00:00'))
    assert finished >= started
    timing = data[name + '.time'].decode()
    assert re.search(r'Exit status: 0\s*$', timing)
    log = data[name + '.log'].decode()
    passed = re.findall(r'^test ([\w:]+) \.\.\. ok$', log, re.M)
    for test in required.get(name, []):
        assert passed.count(test) == 1, (name, test)
    assert not re.search(r'^test .* \.\.\. FAILED$', log, re.M)
    if name in required:
        summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', log)
        assert summaries and all(int(count) > 0 and failed == '0' for count, failed in summaries)
    stages.append({'name': name, 'command': command, 'native_exit': 0, 'tee_exit': 0,
                   'mandatory_count': len(required.get(name, []))})
assert terminal['original_exit'] == terminal['exit'] == terminal['native_exit'] == 0
assert int(data['gates-exit']) == int(data['final-exit']) == 0
assert data['cpu.max.before'].strip() == b'200000 100000'
assert data['memory.max.before'].strip() == b'8589934592'
assert data['memory.swap.max.before'].strip() == b'0'
assert data['pids.max.before'].strip() == b'512'
events = dict(line.split() for line in data['memory.events.after'].decode().splitlines())
assert int(events.get('oom', 0)) == int(events.get('oom_kill', 0)) == 0
assert int(data['memory.peak.after']) <= 8589934592 and int(data['memory.swap.peak.after']) == 0
for scope in ('', 'global-'):
    scope_events = dict(line.split() for line in data[scope + 'memory.events.after'].decode().splitlines())
    assert int(scope_events.get('oom', 0)) == int(scope_events.get('oom_kill', 0)) == 0
    assert int(data[scope + 'memory.peak.after']) <= 8589934592
    assert int(data[scope + 'memory.swap.peak.after']) == 0
    # The service snapshot is taken by its shell plus cat; the later aggregate
    # snapshot follows systemctl stop and must prove actual drain.
    assert int(data[scope + 'pids.current.after']) <= 512
    if scope + 'pids.peak.after' in data:
        assert int(data[scope + 'pids.peak.after']) <= 512
assert int(data['global-pids.current.after']) == 0
assert b'ActiveState=inactive' in data['global-systemd-after.txt']
assert b'MainPID=0' in data['systemd-after.txt']
assert b'ExecMainStatus=0' in data['systemd-after.txt']
artifact = json.loads(data['probe-artifact.json'])
retained = json.loads(data['probe-libtest.binary.json'])
assert retained['bytes'] == artifact['bytes'] and retained['sha256'] == artifact['sha256']
assert int(data['probe-libtest.binary-upload-exit']) == 0
screen = json.loads(data['early-codegen-screen.json'])
assert screen['status'] == 'SCREEN_PASS_AUDIT_PENDING'
assert screen['packed_add_present'] is True and screen['forbidden_mnemonics'] == []
assert screen['qualification'] is False and screen['primitive_run'] is False
before_digest, before_path = data['probe-elf-before-tests.sha256'].decode().strip().split('  ', 1)
assert before_digest == retained['sha256'] and before_path == artifact['path']
assert data['probe-elf-after-tests.log'].decode().strip() == artifact['path'] + ': OK'
assert commands['scalar-control-release'].startswith('env CARGO_TARGET_DIR=/mnt/borsuk-http/target-scalar-control cargo ')
for stage_name in ('codec-release', 'planner-release'):
    assert pathlib.PurePosixPath(artifact['path']).name in data[stage_name + '.log'].decode()

assert artifact['status'] == 'RELEASE_COMPILED_CODEGEN_AUDIT_PENDING'
assert artifact['ordered_sum_codegen_qualified'] is False and artifact['serving_stack_qualified'] is False
assert terminal['control_library_source'] == protocol['source']['control']['library_source']
assert terminal['control_native_identity_sha256'] == protocol['source']['control']['native_identity']
control_native = json.loads(data['control-native-source.json'])
control_identity = hashlib.sha256(b''.join(p.encode() + b'\0' + h.encode() + b'\n' for p,h in sorted(control_native.items()))).hexdigest()
assert control_identity == terminal['control_native_identity_sha256']
control_map = {line.split('  ',1)[1]:line.split('  ',1)[0] for line in data['control-source-files.sha256'].decode().splitlines()}
assert len(control_map) == len(support)
assert all(control_map[p] == h for p,h in control_native.items())
assert {p for p in support if support[p] != control_map[p]} == {'crates/borsuk/src/rotated_two_bit.rs','crates/borsuk/src/two_bit_generation.rs'}
for suffix in ('before','after'):
    assert data['control-source-'+suffix+'.log'].decode().splitlines() == [p+': OK' for p in control_map]
assert data['cargo-config-admission.txt'].decode().strip() == 'no ancestor/source/CARGO_HOME/root Cargo configuration overrides'
profiles = [line.split('  ',1) for line in data['matched-profile-inputs.sha256'].decode().splitlines()]
assert len(profiles) == 4 and profiles[0][0] == profiles[2][0] and profiles[1][0] == profiles[3][0]
assert data['rustc-verbose.txt'].decode().startswith('rustc 1.98.0 ')
roles = ['probe-libtest','candidate-check_cohere_native_baseline','candidate-two_bit_http','control-check_cohere_native_baseline','control-two_bit_http','scalar-control-libtest']
for role in roles:
    pin = json.loads(data[role+'.binary.json'])
    assert pin['role'] == role and type(pin['bytes']) is int and pin['bytes'] > 0
    assert re.fullmatch('[0-9a-f]{64}',pin['sha256'])
    assert int(data[role+'.binary-upload-exit']) == 0
scalar = json.loads(data['scalar-control-artifact.json'])
assert scalar['reason'] == 'compiler-artifact' and scalar['target']['name'] == 'borsuk'
assert scalar['target']['kind'] == ['lib'] and scalar['profile']['test'] is True
assert 'scalar-control' in scalar['features']
assert pathlib.PurePosixPath(scalar['executable']).parent == pathlib.PurePosixPath('/mnt/borsuk-http/target-scalar-control/release/deps')
assert terminal['resolved_lock_sha256'] == terminal['resolved_source_map_sha256'] == 'null'
assert profiles[0][0] == support['Cargo.toml'] and profiles[1][0] == support['Cargo.lock']
candidate_lines = data['candidate-serving-before-control.sha256'].decode().splitlines()
assert len(candidate_lines) == 2
for line in candidate_lines:
    digest,path = line.split('  ',1)
    role = pathlib.PurePosixPath(path).name
    assert digest == json.loads(data[role+'.binary.json'])['sha256']
assert data['candidate-serving-after-control.log'].decode().splitlines() == [line.split('  ',1)[1]+': OK' for line in candidate_lines]
receipt = {'status': 'FULL_NATIVE_CORRECTNESS_VERIFIED_SERVING_STACK_PENDING',
           'candidate': protocol['candidate'], 'instance_id': job['instance_id'],
           'native_file_count': len(native), 'support_file_count': len(support),
           'native_identity': identity, 'full_source_before_after_equal': True,
           'artifact_count': len(data), 'stages': stages, 'release_elf': retained,
           'early_emission_screen': screen, 'release_elf_before_after_equal': True, 'scalar_control_target_separate': True,
           'codegen_qualified': False, 'primitive_run': False,
           'workspace_clippy_and_testbuild': 'PASS_EXACT_SOURCE', 'production_integrated': False,
           'performance_claim': False, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
(root / 'independent-verification.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
