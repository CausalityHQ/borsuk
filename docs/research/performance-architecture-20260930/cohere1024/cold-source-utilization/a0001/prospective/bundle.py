"""Exact experiment packaging only; reads immutable Git source, not vector data."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile

candidate, output = sys.argv[1:]
assert len(candidate) == 40 and all(c in '0123456789abcdef' for c in candidate)
repo = Path('/home/rb/worktrees/borsuk-prod-ready-v9')
prior = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0004')
out = Path(output)
assert not out.exists(), 'create-only output'
contract_body = Path('/tmp/borsuk-cold-source-utilization-native-contract.json').read_bytes()
contract = json.loads(contract_body)
assert contract['commit'] == candidate
owned = 'crates/borsuk/examples/compare_native_replay.rs'
body = subprocess.check_output(['/usr/bin/git', 'show', candidate + ':' + owned], cwd=repo)
assert hashlib.sha256(body).hexdigest() == contract['file_sha256']
tracked = set(subprocess.check_output(['/usr/bin/git', 'ls-tree', '-r', '--name-only', candidate], cwd=repo, text=True).splitlines())
native = {p for p in tracked if p.endswith('.rs') or Path(p).name in ('Cargo.toml', 'Cargo.lock')}
support = {line.split('  ', 1)[1] for line in (prior / 'source-files.sha256').read_text().splitlines()}
assert support <= tracked, 'prior support path absent at candidate'
selected = support | native
assert {'scripts/check_rust_test_build.sh', 'Cargo.toml', 'Cargo.lock', owned} <= selected
assert all(not p.startswith('/') and '..' not in Path(p).parts for p in selected)
out.mkdir()
pins = {}
archive = subprocess.Popen(['/usr/bin/git', 'archive', '--format=tar', candidate, '--', *sorted(selected)], cwd=repo, stdout=subprocess.PIPE)
with (out / 'source.tar.gz').open('xb') as raw, gzip.GzipFile(fileobj=raw, mode='wb', mtime=0) as zipped, tarfile.open(fileobj=zipped, mode='w|') as target, tarfile.open(fileobj=archive.stdout, mode='r|') as source:
    for entry in source:
        if entry.isdir():
            continue
        assert entry.isfile() and entry.name in selected and entry.name not in pins
        assert entry.size <= 16 * 1024 * 1024, 'bounded source member'
        body = source.extractfile(entry).read()
        assert len(body) == entry.size
        pins[entry.name] = hashlib.sha256(body).hexdigest()
        item = tarfile.TarInfo(entry.name)
        item.size, item.mode, item.mtime = len(body), entry.mode, 0
        target.addfile(item, io.BytesIO(body))
assert archive.wait() == 0 and set(pins) == selected
native_map = {p: pins[p] for p in sorted(native)}
assert pins[owned] == contract['file_sha256']
identity = hashlib.sha256(b''.join(p.encode() + b'\0' + h.encode() + b'\n' for p, h in native_map.items())).hexdigest()
(out / 'source-files.sha256').write_text(''.join(h + '  ' + p + '\n' for p, h in sorted(pins.items())))
(out / 'native-source.json').write_text(json.dumps(native_map, indent=2) + '\n')
(out / 'candidate-contract.json').write_bytes(contract_body)
receipt = {'status': 'SOURCE_BUNDLE_ONLY_NOT_LAUNCHED', 'candidate': candidate, 'file_count': len(pins), 'native_file_count': len(native_map), 'native_identity': identity, 'archive_bytes': (out / 'source.tar.gz').stat().st_size, 'archive_sha256': hashlib.sha256((out / 'source.tar.gz').read_bytes()).hexdigest(), 'owned_file': owned, 'owned_sha256': pins[owned], 'performance_claim': False}
(out / 'bundle.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
