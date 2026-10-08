import ast
import hashlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
from unittest import mock

root = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-control-build-a0001')
protocol = json.loads((root / 'protocol.json').read_text())
contract = json.loads((root / 'candidate-contract.json').read_text())
assert protocol['candidate'] == contract['commit'] == 'fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b'
assert protocol['paid_launch_authorized'] is False
def sha(body):
    return hashlib.sha256(body).hexdigest()
for name, pin in protocol['inputs'].items():
    body = (root / name).read_bytes()
    assert len(body) == pin['bytes'] and sha(body) == pin['sha256']
assert sha((root / 'user-data.sh').read_bytes()) == protocol['userdata']['sha256']
native = json.loads((root / 'native-source.json').read_text())
control = json.loads((root / 'control-native-source.json').read_text())
support = {line.split('  ', 1)[1]: line.split('  ', 1)[0]
           for line in (root / 'source-files.sha256').read_text().splitlines()}
control_support = {line.split('  ', 1)[1]: line.split('  ', 1)[0]
                   for line in (root / 'control-source-files.sha256').read_text().splitlines()}
assert len(support) == protocol['source']['file_count'] == len(control_support)
assert len(native) == len(control) == 415
assert all(support[path] == digest for path, digest in native.items())
assert all(control_support[path] == digest for path, digest in control.items())
delta = {path for path in support if support[path] != control_support[path]}
assert delta == set(protocol['source']['control']['native_delta_paths']) == {
    'crates/borsuk/src/rotated_two_bit.rs', 'crates/borsuk/src/two_bit_generation.rs'}
def identity(mapping):
    return sha(b''.join(path.encode() + b'\0' + digest.encode() + b'\n'
                        for path, digest in sorted(mapping.items())))
assert identity(native) == protocol['source']['native_identity']
assert identity(control) == protocol['source']['control']['native_identity']
for path, expected in contract['owned_sha256'].items():
    assert expected == native[path]
    assert sha(subprocess.check_output(['/usr/bin/git', 'show', protocol['candidate'] + ':' + path])) == expected
digest = hashlib.sha256()
with (root / 'source.tar.gz').open('rb') as stream:
    while block := stream.read(65536):
        digest.update(block)
assert digest.hexdigest() == protocol['source']['archive_sha256']
assert (root / 'source.tar.gz').stat().st_size == protocol['source']['archive_bytes']
seen, owned = set(), {}
with tarfile.open(root / 'source.tar.gz') as archive:
    for entry in archive:
        assert entry.isfile() and entry.name in support and entry.name not in seen
        assert not entry.name.startswith('/') and '..' not in Path(entry.name).parts
        body = archive.extractfile(entry).read()
        assert sha(body) == support[entry.name]
        if entry.name in contract['owned_paths']:
            owned[entry.name] = body.decode()
        seen.add(entry.name)
assert seen == set(support)
for name in ('gates.sh', 'user-data.sh'):
    subprocess.run(['bash', '-n', str(root / name)], check=True)
for name in ('select-probe.py', 'select-scalar-control.py'):
    ast.parse((root / name).read_text())
gates = (root / 'gates.sh').read_text()
actual = [shlex.split(line)[1:] for line in gates.splitlines() if line.startswith(' stage ')]
assert actual == [[stage['name'], *stage['argv']] for stage in protocol['serial_stages']]
assert len(actual) == 1 and not any('--ignored' in argv for argv in actual)
assert 'unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS' in gates
required = json.loads((root / 'mandatory-tests.json').read_text())
for name, names in required.items():
    assert names and len(names) == len(set(names))
    path = 'crates/borsuk/src/' + ('bin/check_cohere_native_baseline.rs' if name.startswith('runner')
                                 else 'two_bit_generation.rs' if name.startswith('planner') else 'rotated_two_bit.rs')
    for test in names:
        assert 'fn ' + test.rsplit('::', 1)[1] + '(' in owned[path]
assert required == {}
assert 'CARGO_TARGET_DIR=/mnt/borsuk-http/target-control-isolated' in actual[0]
assert '--message-format=json' in actual[0]
assert 'test ! -e /mnt/borsuk-http/target-control-isolated' in gates
for path, filename in (
    ('crates/borsuk/src/rotated_two_bit.rs', 'control-rotated_two_bit.rs'),
    ('crates/borsuk/src/two_bit_generation.rs', 'control-two_bit_generation.rs'),
):
    body = (root / filename).read_bytes()
    assert sha(body) == control[path]
    assert body == subprocess.check_output(['/usr/bin/git', 'show', protocol['source']['control']['library_source'] + ':' + path])
# Exercise the actual shell transfer/patch/hash sequence on tiny source-bound fixtures.
with tempfile.TemporaryDirectory(prefix='borsuk-four-row-control-canary-') as temporary:
    parent = Path(temporary)
    candidate_dir, control_dir = parent / 'candidate', parent / 'control'
    candidate_dir.mkdir()
    fixture = {'Cargo.toml': b'[workspace]\n', 'Cargo.lock': b'opaque lock fixture\n',
               'runner.rs': b'fixed codec +512+trace; fixture only\n',
               'rotated.rs': b'candidate-vector\n', 'generation.rs': b'candidate-planner\n'}
    for name, body in fixture.items():
        (candidate_dir / name).write_bytes(body)
    with tarfile.open(parent / 'source.tar.gz', 'w:gz') as archive:
        for name in fixture:
            archive.add(candidate_dir / name, arcname=name)
    control_fixture = {**fixture, 'rotated.rs': b'original-scalar\n', 'generation.rs': b'original-planner\n'}
    for name in ('rotated.rs', 'generation.rs'):
        (parent / ('control-' + name)).write_bytes(control_fixture[name])
    (parent / 'control.sha256').write_text(''.join(sha(body) + '  ' + name + '\n' for name, body in sorted(control_fixture.items())))
    subprocess.run(['bash', '-c', 'set -euo pipefail; test ! -e "$1/control"; mkdir "$1/control"; tar -xzf "$1/source.tar.gz" -C "$1/control"; cp "$1/control-rotated.rs" "$1/control/rotated.rs"; cp "$1/control-generation.rs" "$1/control/generation.rs"; cd "$1/control"; sha256sum --check "$1/control.sha256"', 'control-canary', str(parent)], check=True, stdout=subprocess.PIPE)
    assert all((candidate_dir / name).read_bytes() == body for name, body in fixture.items())
    assert all((control_dir / name).read_bytes() == body for name, body in control_fixture.items())

selector = (root / 'select-probe.py').read_text()
with tempfile.TemporaryDirectory(prefix='borsuk-isolated-control-selector-') as temporary:
    base = Path(temporary)
    (base / 'evidence').mkdir()
    (base / 'retained').mkdir()
    script = selector.replace("pathlib.Path('/mnt/borsuk-http')", 'pathlib.Path(' + repr(temporary) + ')')
    items = []
    for name, kind, relative, executable in (
        ('borsuk', ['lib'], 'src/lib.rs', None),
        ('check_cohere_native_baseline', ['bin'], 'src/bin/check_cohere_native_baseline.rs', '/mnt/borsuk-http/target-control-isolated/release/check_cohere_native_baseline'),
        ('two_bit_http', ['example'], 'examples/two_bit_http.rs', '/mnt/borsuk-http/target-control-isolated/release/examples/two_bit_http'),
    ):
        items.append({'reason': 'compiler-artifact', 'manifest_path': '/mnt/borsuk-http/control-source/crates/borsuk/Cargo.toml', 'target': {'name': name, 'kind': kind, 'src_path': '/mnt/borsuk-http/control-source/crates/borsuk/' + relative}, 'fresh': False, 'profile': {'test': False}, 'features': ['default'], 'executable': executable})
    for label in ('positive', 'fresh-library', 'fresh-binary', 'wrong-manifest', 'wrong-source', 'wrong-target', 'duplicate', 'missing', 'wrong-feature'):
        value = json.loads(json.dumps(items))
        if label == 'fresh-library': value[0]['fresh'] = True
        if label == 'fresh-binary': value[1]['fresh'] = True
        if label == 'wrong-manifest': value[0]['manifest_path'] = '/mnt/borsuk-http/source/crates/borsuk/Cargo.toml'
        if label == 'wrong-source': value[1]['target']['src_path'] = '/mnt/borsuk-http/source/crates/borsuk/src/bin/check_cohere_native_baseline.rs'
        if label == 'wrong-target': value[2]['executable'] = '/mnt/borsuk-http/source/target/release/examples/two_bit_http'
        if label == 'duplicate': value.append(value[-1])
        if label == 'missing': value.pop()
        if label == 'wrong-feature': value[0]['features'] = ['default', 'scalar-control']
        (base / 'evidence/control-release.log').write_text(''.join(json.dumps(item) + '\n' for item in value))
        with mock.patch('pathlib.Path.is_file', return_value=True), mock.patch('shutil.copyfile') as copied:
            try:
                exec(compile(script, 'actual-control-selector-fixture', 'exec'), {})
                assert label == 'positive' and copied.call_count == 2
            except AssertionError:
                assert label != 'positive' and copied.call_count == 0

prior = root.parent / 'two-bit-four-row-qualification-a0004'
assert json.loads((prior / 'independent-verification.json').read_text())['status'] == 'FULL_NATIVE_CORRECTNESS_VERIFIED_SERVING_STACK_PENDING'
assert json.loads((prior / 'control-build-provenance-invalid.json').read_text())['status'] == 'CONTROL_SERVING_BINARY_PROVENANCE_INVALID_BUILD_REUSE'
assert json.loads((prior / 'wait.json').read_text())['exit'] == 0
for name in ('independent-verification.json', 'control-build-provenance-invalid.json'):
    assert (root / ('prior-' + name)).read_bytes() == (prior / name).read_bytes()
assert not Path('/mnt/borsuk-http/FROZEN_FOUR_ROW_SOURCE_AND_ROSTER').exists()
refusal = subprocess.run(['bash', str(root / 'gates.sh')], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
assert refusal.returncode == 98
receipt = {'status': 'PASS_BOUNDED_SOURCE_AND_STAGING_CANARY', 'candidate': protocol['candidate'], 'native_identity': identity(native), 'control_native_identity': identity(control), 'native_count': len(native), 'support_count': len(support), 'stages': len(actual), 'real': ['all exact archive/source/input pins', 'source-body admission', 'control two-file substitution and unchanged candidate', 'Bash syntax', 'missing marker refusal', 'prior13-stage completion and control-build INVALID pins'], 'mocked': ['actual isolated-control compiler-artifact selector positive and eight provenance refusals; no copying before complete admission'], 'native_Cargo_rustc_execution': False, 'AWS': False, 'performance_claim': False, 'primitive_allowed': False}
(root / 'local-admission-canary.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
