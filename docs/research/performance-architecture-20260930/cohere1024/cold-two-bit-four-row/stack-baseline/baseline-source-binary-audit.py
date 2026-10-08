import hashlib
import json
from pathlib import Path
import subprocess

root = Path('/data/target/borsuk-cold-membership-native')
prior = root / 'qualification-a0003'
baseline = json.loads((root / 'two-bit-four-row-gate-a0004/native-source.json').read_text())
parent = 'c49a2e6d2a035bfaf42358fbf7fb60abf11e4222'
for path in ('crates/borsuk/src/rotated_two_bit.rs', 'crates/borsuk/src/two_bit_generation.rs'):
    body = subprocess.check_output(['/usr/bin/git', 'show', parent + ':' + path])
    baseline[path] = hashlib.sha256(body).hexdigest()
assert baseline == json.loads((prior / 'native-source.json').read_text())
identity = hashlib.sha256(b''.join(path.encode() + b'\0' + sha.encode() + b'\n' for path, sha in sorted(baseline.items()))).hexdigest()
verified = json.loads((prior / 'independent-verification.json').read_text())
assert verified['native_source_identity_sha256'] == identity
assert verified['status'] == 'NATIVE_QUALIFIED' and verified['terminated'] is True
binaries = {}
for role in ('check_cohere_native_baseline', 'two_bit_http'):
    path = prior / (role + '.binary')
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    pin = verified['binaries'][role]
    assert path.stat().st_size == pin['bytes'] and digest.hexdigest() == pin['sha256']
    binaries[role] = {'path': str(path), **pin}
toolchain = (prior / 'extracted/toolchain.txt').read_text()
assert 'rustc 1.98.0 (88d9e12ae 2026-08-18)' in toolchain
receipt = {
    'status': 'BASELINE_SOURCE_AND_BINARIES_AUTHENTICATED_STACK_AUDIT_PENDING',
    'baseline_commit': parent, 'qualified_source': verified['source_commit'],
    'native_count': len(baseline), 'native_identity': identity,
    'all_native_files_byte_identical': True, 'binaries': binaries,
    'toolchain': toolchain, 'release_command': (prior / 'extracted/release.command').read_text().strip(),
    'compiler_environment_receipt_present': (prior / 'extracted/compiler-env.txt').exists(),
    'limitations': ['No matched candidate production ELF yet.', 'Full compiler-environment equivalence and production caller/kernel stack including realignment remain unproved.'],
    'stack_qualified': False, 'primitive_allowed': False, 'native_execution': False,
}
Path('/tmp/borsuk-four-row-baseline-source-binary-audit.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
