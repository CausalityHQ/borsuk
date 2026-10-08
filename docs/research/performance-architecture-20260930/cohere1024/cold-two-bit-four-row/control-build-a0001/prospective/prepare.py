import hashlib
import json
from pathlib import Path
import shutil

old = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0004')
root = old.parent / 'two-bit-four-row-control-build-a0001'
root.mkdir(exist_ok=False)
protocol = json.loads((old / 'protocol.json').read_text())
for name in [*protocol['inputs'], 'candidate-contract.json', 'bundle.json', 'source.tar.gz', 'root-source-review.json']:
    shutil.copyfile(old / name, root / name)
protocol['prefix'] = 'research/semantic-router/20261008/cold-two-bit-four-row-control-build-a0001'
protocol['status'] = 'SOURCE_ADMISSION_PENDING_CONTROL_BUILD_REPAIR_ONLY'
protocol['paid_launch_authorized'] = False
protocol['repair_of'] = {'commit': '39ec911b93dec90c9a62c47d81cd6e4e5a01511c', 'attempt': 'qualification-a0004', 'receipt_sha256': hashlib.sha256((old / 'control-build-provenance-invalid.json').read_bytes()).hexdigest()}
protocol['scope'] = 'One isolated control serving build; previously passed candidate tests/Clippy/testbuild are not repeated. No native ANN, primitive or performance execution.'
protocol['serial_stages'] = [{'name': 'control-release', 'argv': ['env', 'CARGO_TARGET_DIR=/mnt/borsuk-http/target-control-isolated', 'cargo', 'build', '--locked', '--release', '--manifest-path', '/mnt/borsuk-http/control-source/Cargo.toml', '-p', 'borsuk', '--bin', 'check_cohere_native_baseline', '--example', 'two_bit_http', '--message-format=json']}]
(root / 'mandatory-tests.json').write_text('{}\n')
gates = (old / 'gates.sh').read_text()
start = gates.index('set +e\n(\n')
end = gates.index('\nstatus=$?\n', start)
replacement = '''set +e
(
 set -e
 test ! -e /mnt/borsuk-http/source/target
 test ! -e /mnt/borsuk-http/control-source/target
 test ! -e /mnt/borsuk-http/target-control-isolated
 printf '%s\\n' 'candidate and control targets absent; isolated control target absent before Cargo' > "$evidence/control-target-admission.txt"
 stage control-release env CARGO_TARGET_DIR=/mnt/borsuk-http/target-control-isolated cargo build --locked --release --manifest-path /mnt/borsuk-http/control-source/Cargo.toml -p borsuk --bin check_cohere_native_baseline --example two_bit_http --message-format=json
 python3 /mnt/borsuk-http/select-probe.py
 (cd /mnt/borsuk-http/control-source && sha256sum --check /mnt/borsuk-http/control-source-files.sha256) > "$evidence/control-source-after.log"
)'''
(root / 'gates.sh').write_text(gates[:start] + replacement + gates[end:])
selector = '''import json, pathlib, shutil, hashlib
root = pathlib.Path('/mnt/borsuk-http')
evidence = root / 'evidence'
records = []
for line in (evidence / 'control-release.log').read_text().splitlines():
    if line.startswith('{'):
        item = json.loads(line)
        if item.get('reason') == 'compiler-artifact' and item.get('manifest_path') == '/mnt/borsuk-http/control-source/crates/borsuk/Cargo.toml':
            records.append(item)
library = [item for item in records if item['target']['name'] == 'borsuk' and item['target']['kind'] == ['lib']]
assert len(library) == 1 and library[0]['fresh'] is False and library[0]['profile']['test'] is False
assert library[0]['target']['src_path'] == '/mnt/borsuk-http/control-source/crates/borsuk/src/lib.rs'
assert library[0]['features'] == ['default']
selected = {}
for role, name, kind, directory in [('control-check_cohere_native_baseline', 'check_cohere_native_baseline', ['bin'], '/mnt/borsuk-http/target-control-isolated/release'), ('control-two_bit_http', 'two_bit_http', ['example'], '/mnt/borsuk-http/target-control-isolated/release/examples')]:
    matches = [item for item in records if item['target']['name'] == name and item['target']['kind'] == kind]
    assert len(matches) == 1
    item = matches[0]
    assert item['fresh'] is False and item['profile']['test'] is False
    expected_source = '/mnt/borsuk-http/control-source/crates/borsuk/' + ('src/bin/check_cohere_native_baseline.rs' if kind == ['bin'] else 'examples/two_bit_http.rs')
    assert item['target']['src_path'] == expected_source
    path = pathlib.Path(item['executable'])
    assert str(path.parent) == directory and path.name == name and path.is_file()
    selected[role] = item
# Validate the complete roster before copying either result.
assert len(selected) == 2
for role, item in selected.items():
    shutil.copyfile(item['executable'], root / 'retained' / role)
(evidence / 'control-compiler-artifacts.json').write_text(json.dumps({'library': library[0], 'serving': selected}, indent=2) + '\\n')
'''
(root / 'select-probe.py').write_text(selector)
userdata = (old / 'user-data.sh').read_text().replace('cold-two-bit-four-row-qualification-a0004', 'cold-two-bit-four-row-control-build-a0001')
userdata = userdata.replace('for role in probe-libtest candidate-check_cohere_native_baseline candidate-two_bit_http control-check_cohere_native_baseline control-two_bit_http scalar-control-libtest; do', 'for role in control-check_cohere_native_baseline control-two_bit_http; do')
userdata = userdata.replace('borsuk-two-bit-four-row-native-qualification-v1', 'borsuk-two-bit-four-row-control-build-v1')
userdata = userdata.replace('exact native tests and full compiler gates plus matched-profile serving artifacts; production stack and primitive/cold timing pending', 'one fresh isolated unchanged control build; candidate correctness qualified previously; production stack and timing pending')
userdata = userdata.replace('test "$(jq length mandatory-tests.json)" = 7', 'test "$(jq length mandatory-tests.json)" = 0')
for name, pin in protocol['inputs'].items():
    body = (root / name).read_bytes()
    new_digest = hashlib.sha256(body).hexdigest()
    userdata = userdata.replace(pin['sha256'], new_digest)
    pin.update(bytes=len(body), sha256=new_digest)
(root / 'user-data.sh').write_text(userdata)
protocol['userdata'] = {'bytes': len(userdata.encode()), 'sha256': hashlib.sha256(userdata.encode()).hexdigest()}
protocol['control_target'] = '/mnt/borsuk-http/target-control-isolated'
protocol['expected_serving_roles'] = ['control-check_cohere_native_baseline', 'control-two_bit_http']
protocol['candidate_correctness_receipt'] = {'path': str(old / 'independent-verification.json'), 'bytes': (old / 'independent-verification.json').stat().st_size, 'sha256': hashlib.sha256((old / 'independent-verification.json').read_bytes()).hexdigest()}
for name in ['independent-verification.json', 'control-build-provenance-invalid.json']:
    shutil.copyfile(old / name, root / ('prior-' + name))
(root / 'protocol.json').write_text(json.dumps(protocol, indent=2) + '\n')
print(root)
