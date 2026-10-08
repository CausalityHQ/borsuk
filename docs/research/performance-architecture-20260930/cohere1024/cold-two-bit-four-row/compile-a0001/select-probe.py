import json, pathlib, subprocess, hashlib

root = pathlib.Path('/mnt/borsuk-http/evidence')
artifacts = []
for line in (root / 'probe-compile.log').read_text().splitlines():
    try:
        item = json.loads(line)
    except ValueError:
        continue
    if (isinstance(item, dict) and item.get('reason') == 'compiler-artifact'
        and item.get('target', {}).get('name') == 'borsuk'
        and item['target'].get('kind') == ['lib']
        and item.get('profile', {}).get('test') is True and item.get('executable')):
        artifacts.append(item)
assert len(artifacts) == 1, 'release libtest missing/ambiguous'
binary = pathlib.Path(artifacts[0]['executable'])
digest = hashlib.sha256()
with binary.open('rb') as source:
    while block := source.read(1048576):
        digest.update(block)
symbols = subprocess.check_output(['nm', '-S', '-C', str(binary)], text=True)
(root / 'probe-nm.txt').write_text(symbols)
matches = []
for line in symbols.splitlines():
    fields = line.split(maxsplit=3)
    if len(fields) == 4 and fields[-1] in ('borsuk::rotated_two_bit::PreparedTwoBit::score_four', '<borsuk::rotated_two_bit::PreparedTwoBit>::score_four'):
        matches.append(fields)
assert len(matches) == 1, 'four-row kernel missing/ambiguous'
address, size = int(matches[0][0], 16), int(matches[0][1], 16)
assert 0 < size <= 65536, 'kernel disassembly bound'
assembly = subprocess.check_output(['objdump', '-d', f'--start-address={address}',
                                   f'--stop-address={address + size}', str(binary)], text=True)
(root / 'four-row-disassembly.txt').write_text(assembly)
(root / 'probe-executable.txt').write_text(str(binary) + '\n')
(root / 'probe-artifact.json').write_text(json.dumps({
    'status': 'RELEASE_COMPILED_CODEGEN_AUDIT_PENDING', 'path': str(binary),
    'bytes': binary.stat().st_size, 'sha256': digest.hexdigest(),
    'cargo_artifact': artifacts[0], 'kernel_address': address, 'kernel_bytes': size,
    'ordered_sum_codegen_qualified': False, 'serving_stack_qualified': False,
    'primitive_run': False, 'performance_claim': False,
}, indent=2) + '\n')
