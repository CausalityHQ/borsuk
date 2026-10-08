import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess

root = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0004')
verified = json.loads((root / 'independent-verification.json').read_text())
assert verified['status'] == 'FULL_NATIVE_CORRECTNESS_VERIFIED_SERVING_STACK_PENDING'
receipts = []
for role in ('probe-libtest', 'candidate-check_cohere_native_baseline', 'candidate-two_bit_http'):
    pin = json.loads((root / (role + '.binary.json')).read_text())
    binary = root / role
    digest = hashlib.sha256()
    with binary.open('rb') as stream:
        while block := stream.read(65536): digest.update(block)
    assert binary.stat().st_size == pin['bytes'] and digest.hexdigest() == pin['sha256']
    symbols = subprocess.check_output(['nm', '-S', '-C', str(binary)], text=True)
    matches = [line.split(maxsplit=3) for line in symbols.splitlines() if len(line.split(maxsplit=3)) == 4 and line.split(maxsplit=3)[-1] in ('<borsuk::rotated_two_bit::PreparedTwoBit>::score_four', 'borsuk::rotated_two_bit::PreparedTwoBit::score_four')]
    assert len(matches) == 1
    address, size = (int(value, 16) for value in matches[0][:2])
    assert size == 695
    assembly = subprocess.check_output(['objdump', '-d', '--start-address=' + hex(address), '--stop-address=' + hex(address + size), str(binary)], text=True)
    mnemonics = re.findall(r'^\s*[0-9a-f]+:\s+(?:[0-9a-f]{2}\s+)+([a-z][a-z0-9]*)\b', assembly, re.M)
    assert mnemonics.count('addpd') == 2
    assert not any('fmadd' in name or 'fmsub' in name or 'fnmadd' in name or 'fnmsub' in name or name in ('haddpd', 'vhaddpd', 'haddps', 'vhaddps', 'hsubpd', 'vhsubpd', 'hsubps', 'vhsubps') for name in mnemonics)
    assert re.search(r'addpd\s+%xmm10,%xmm9', assembly)
    assert re.search(r'addpd\s+%xmm10,%xmm4', assembly)
    assert re.search(r'unpcklpd\s+%xmm11,%xmm10', assembly)
    assert re.search(r'movhpd\s+\(%rsi,%r10,8\),%xmm10', assembly)
    assert re.search(r'inc\s+%rax', assembly)
    assert re.search(r'add\s+\$0x100,%rbp', assembly)
    assert re.search(r'mov\s+%rbp,%r10', assembly)
    assert re.search(r'cmp\s+%rax,%r11', assembly)
    assert re.search(r'jne\s+' + format(address + 0x90, 'x') + r'\b', assembly)
    initializer = re.search(r'movapd[^\n]*,%xmm9\s+# ([0-9a-f]+)', assembly)
    assert initializer
    constant_address = int(initializer[1], 16)
    constant = subprocess.check_output(['objdump', '-s', '--start-address=' + hex(constant_address), '--stop-address=' + hex(constant_address + 16), str(binary)], text=True)
    assert '00000000 00000080 00000000 00000080' in constant
    assert mnemonics.count('addsd') == 4
    assert mnemonics.count('mulsd') == 12
    assert len(re.findall(r'unpckhpd', assembly)) == 2
    assert len(re.findall(r'\bpush\s', assembly)) == 6
    assert re.search(r'sub\s+\$0x18,%rsp', assembly)
    assert not re.search(r'and\s+.*%rsp', assembly)
    (root / (role + '.ordered-kernel-disassembly.txt')).write_text(assembly)
    (root / (role + '.ordered-kernel-initializer.txt')).write_text(constant)
    receipts.append({'role': role, 'elf': pin, 'symbol': matches[0][-1], 'address': hex(address), 'kernel_bytes': size, 'disassembly_sha256': hashlib.sha256(assembly.encode()).hexdigest(), 'initializer_address': hex(constant_address), 'initializer_sha256': hashlib.sha256(constant.encode()).hexdigest(), 'standalone_kernel_entry_stack_bytes': 80, 'stack_realignment_bytes': 0})
receipt = {'status': 'ORDERED_CANDIDATE_KERNEL_EMISSION_PASS_MATCHED_SERVING_STACK_PENDING', 'candidate': verified['candidate'], 'qualification_instance_id': verified['instance_id'], 'binaries': receipts, 'manual_loop_audit': {'index': 'rax increments once per packed byte; r11 bounds; r10 table offset increments256', 'row01': 'row0 and row1 indexed loads packed with unpcklpd; single addpd into xmm9 per byte', 'row23': 'row2 and row3 indexed loads packed with movsd/movhpd; single addpd into xmm4 per byte', 'initializers': 'both accumulators start at exact f64 negative zero in both lanes', 'sum_order': 'one increasing-byte addition chain per lane; no split accumulators, horizontal reduction or FMA', 'finalizers': 'row0 finite check precedes row1 high-lane extraction, then row2 finite check precedes row3 high-lane extraction; each unchanged scalar multiply/add/multiply/multiply', 'lookup_validation': 'authenticated geometry and valid record lengths make bounds/panic calls unreachable on the measured valid-score path'}, 'paired_control_serving_build': 'prior control shared-target provenance INVALID; isolated control repair running separately', 'production_serving_stack_qualified': False, 'primitive_allowed': False, 'primitive_run': False, 'performance_claim': False, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat()}
(root / 'independent-ordered-kernel-audit.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
