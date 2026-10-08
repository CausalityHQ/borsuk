import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

candidate = 'fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b'
control_library = 'c49a2e6d2a035bfaf42358fbf7fb60abf11e4222'
prior = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-gate-a0004')
out = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0002')
out.mkdir(exist_ok=True)
contract = json.loads(Path('/tmp/borsuk-two-bit-four-row-native-contract.json').read_text())
assert contract['commit'] == candidate
owned = {path: subprocess.check_output(['/usr/bin/git', 'show', candidate + ':' + path])
         for path in contract['owned_paths']}
assert {path: hashlib.sha256(body).hexdigest() for path, body in owned.items()} == contract['owned_sha256']
prior_pins = {line.split('  ', 1)[1]: line.split('  ', 1)[0]
              for line in (prior / 'source-files.sha256').read_text().splitlines()}
prior_archive = json.loads((prior / 'protocol.json').read_text())['source']
assert (prior / 'source.tar.gz').stat().st_size == prior_archive['archive_bytes']
sha = hashlib.sha256()
with (prior / 'source.tar.gz').open('rb') as stream:
    while block := stream.read(65536):
        sha.update(block)
assert sha.hexdigest() == prior_archive['archive_sha256']
pins = {}
with tarfile.open(prior / 'source.tar.gz') as source, (out / 'source.tar.gz').open('wb') as raw:
    with gzip.GzipFile(fileobj=raw, mode='wb', mtime=0) as zipped, tarfile.open(fileobj=zipped, mode='w|') as target:
        for entry in source:
            assert entry.isfile() and entry.name in prior_pins and entry.name not in pins
            assert not entry.name.startswith('/') and '..' not in Path(entry.name).parts
            body = source.extractfile(entry).read()
            assert hashlib.sha256(body).hexdigest() == prior_pins[entry.name]
            body = owned.get(entry.name, body)
            pins[entry.name] = hashlib.sha256(body).hexdigest()
            item = tarfile.TarInfo(entry.name)
            item.size, item.mode, item.mtime = len(body), entry.mode, 0
            target.addfile(item, io.BytesIO(body))
assert set(pins) == set(prior_pins)
native = {path: pins[path] for path in json.loads((prior / 'native-source.json').read_text())}
all_native = subprocess.check_output(['/usr/bin/git', 'ls-tree', '-r', '--name-only', candidate], text=True).splitlines()
all_native = {path for path in all_native if path.endswith('.rs') or Path(path).name in ('Cargo.toml', 'Cargo.lock')}
assert set(native) == all_native and len(native) == 415
for path, expected in native.items():
    assert hashlib.sha256(subprocess.check_output(['/usr/bin/git', 'show', candidate + ':' + path])).hexdigest() == expected
control_pins = dict(pins)
control = dict(native)
for path, name in (
    ('crates/borsuk/src/rotated_two_bit.rs', 'control-rotated_two_bit.rs'),
    ('crates/borsuk/src/two_bit_generation.rs', 'control-two_bit_generation.rs'),
):
    body = subprocess.check_output(['/usr/bin/git', 'show', control_library + ':' + path])
    (out / name).write_bytes(body)
    control[path] = control_pins[path] = hashlib.sha256(body).hexdigest()
def identity(mapping):
    return hashlib.sha256(b''.join(path.encode() + b'\0' + digest.encode() + b'\n'
                                   for path, digest in sorted(mapping.items()))).hexdigest()
for name, mapping in (('source-files.sha256', pins), ('control-source-files.sha256', control_pins)):
    (out / name).write_text(''.join(digest + '  ' + path + '\n' for path, digest in sorted(mapping.items())))
for name, mapping in (('native-source.json', native), ('control-native-source.json', control)):
    (out / name).write_text(json.dumps(mapping, indent=2) + '\n')
assert {path for path in native if native[path] != control[path]} == {
    'crates/borsuk/src/rotated_two_bit.rs', 'crates/borsuk/src/two_bit_generation.rs'}
assert native['crates/borsuk/src/bin/check_cohere_native_baseline.rs'] == control['crates/borsuk/src/bin/check_cohere_native_baseline.rs']
archive = out / 'source.tar.gz'
sha = hashlib.sha256()
with archive.open('rb') as stream:
    while block := stream.read(65536):
        sha.update(block)
receipt = {
    'status': 'SOURCE_BUNDLE_ONLY_NO_LAUNCH', 'candidate': candidate,
    'file_count': len(pins), 'native_file_count': len(native), 'native_identity': identity(native),
    'archive_bytes': archive.stat().st_size, 'archive_sha256': sha.hexdigest(),
    'control': {'library_source': control_library, 'native_identity': identity(control),
                'derivation': 'exact candidate archive with only the two library files replaced by authenticated c49a2e6d blobs; identical repaired runner and other native/support bytes',
                'native_delta_paths': sorted(path for path in native if native[path] != control[path])},
    'changed_native_paths': contract['owned_paths'], 'performance_claim': False,
}
(out / 'bundle.json').write_text(json.dumps(receipt, indent=2) + '\n')
(out / 'candidate-contract.json').write_text(json.dumps(contract, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
