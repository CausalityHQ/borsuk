import datetime
import hashlib
import json
from pathlib import Path
import re
import subprocess

root = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-gate-a0004')
verified = json.loads((root / 'independent-verification.json').read_text())
assert verified['status'] == 'COMPILE_AND_SYNTHETIC_CORRECTNESS_VERIFIED_CODEGEN_PENDING'
pin = verified['release_elf']
binary = root / 'probe-libtest'
digest = hashlib.sha256()
with binary.open('rb') as stream:
    while block := stream.read(65536):
        digest.update(block)
assert binary.stat().st_size == pin['bytes']
assert digest.hexdigest() == pin['sha256']
artifact = json.loads((root / 'probe-artifact.json').read_text())
symbols = subprocess.check_output(['nm', '-S', '-C', str(binary)], text=True)
names = {'borsuk::rotated_two_bit::PreparedTwoBit::score_four',
         '<borsuk::rotated_two_bit::PreparedTwoBit>::score_four'}
matches = [line.split(maxsplit=3) for line in symbols.splitlines()
           if len(line.split(maxsplit=3)) == 4 and line.split(maxsplit=3)[-1] in names]
assert len(matches) == 1
address, size = (int(value, 16) for value in matches[0][:2])
assert address == artifact['kernel_address'] and size == artifact['kernel_bytes'] == 695
assembly = subprocess.check_output(['objdump', '-d', f'--start-address={address}',
                                   f'--stop-address={address + size}', str(binary)], text=True)
retained = (root / 'four-row-disassembly.txt').read_text()
def instructions(text):
    return [line.strip() for line in text.splitlines()
            if re.match(r'^\s*[0-9a-f]+:\s', line)]
assert instructions(assembly) == instructions(retained)
(root / 'independent-kernel-disassembly.txt').write_text(assembly)
mnemonics = re.findall(r'^\s*[0-9a-f]+:\s+(?:[0-9a-f]{2}\s+)+([a-z][a-z0-9]*)\b', assembly, re.M)
assert mnemonics.count('addpd') == 2
assert not any('fmadd' in name or 'fmsub' in name or 'fnmadd' in name or 'fnmsub' in name
               or name in ('haddpd', 'vhaddpd', 'haddps', 'vhaddps', 'hsubpd', 'vhsubpd', 'hsubps', 'vhsubps')
               for name in mnemonics)
assert re.search(r'addpd\s+%xmm10,%xmm9', assembly)
assert re.search(r'addpd\s+%xmm10,%xmm4', assembly)
assert re.search(r'inc\s+%rax', assembly)
assert re.search(r'add\s+\$0x100,%rbp', assembly)
assert re.search(r'mov\s+%rbp,%r10', assembly)
assert re.search(r'cmp\s+%rax,%r11', assembly)
assert re.search(r'jne\s+19d0720', assembly)
constant = subprocess.check_output(['objdump', '-s', '--start-address=0x4b8990',
                                    '--stop-address=0x4b89a0', str(binary)], text=True)
assert '00000000 00000080 00000000 00000080' in constant
(root / 'independent-kernel-initializer.txt').write_text(constant)
success = assembly.split('19d08e5:')[0]
assert len(re.findall(r'\bpush\s', success)) == 6
assert re.search(r'sub\s+\$0x18,%rsp', success)
assert not re.search(r'and\s+.*%rsp', success)
receipt = {
    'status': 'ORDERED_KERNEL_EMISSION_PASS_SERVING_STACK_PENDING',
    'candidate': verified['candidate'], 'instance_id': verified['instance_id'],
    'elf': {'path': str(binary), **pin}, 'symbol': matches[0][-1],
    'address': hex(address), 'bytes': size,
    'independent_disassembly_matches_authenticated_remote_bytes': True,
    'disassembly_sha256': hashlib.sha256(assembly.encode()).hexdigest(),
    'manual_loop_audit': {
        'byte_index': 'rax advances by one; r11 is packed byte count; one backward branch to 0x19d0720',
        'lookup_offset': 'r10 advances by 256 table entries per byte; four checked row-indexed scalar loads',
        'lane_0_1': 'row0/row1 loads packed with unpcklpd into xmm10, then added only into xmm9',
        'lane_2_3': 'row2/row3 loads packed with movsd/movhpd into xmm10, then added only into xmm4',
        'initializer': 'both lanes of xmm9 and xmm4 start at literal f64 negative zero at 0x4b8990',
        'sum_order': 'one addition per lane per increasing byte; no split partial sums or cross-lane reduction',
        'finalizers': 'after the loop, row0 then row1 then row2 then row3 use unchanged scalar multiply/add/multiply/multiply and finite checks; upper-lane extraction occurs only after each preceding finalizer',
        'packed_additions_per_byte': 2, 'fma': False, 'horizontal_arithmetic': False,
    },
    'standalone_kernel_success_entry_frame_bytes': 80,
    'standalone_kernel_stack_realignment_bytes': 0,
    'production_serving_stack_qualified': False,
    'primitive_allowed': False, 'primitive_run': False, 'performance_claim': False,
    'remaining': [
        'Candidate production serving ELF and matched-profile control caller/kernel frame accounting are not available from this compile-only job.',
        'Cold runner currently supplies only codec+trace scratch and selects scalar fallback; prospective +512 admission and native dispatch regression are required.',
        'Workspace Clippy and real unshimmed test compilation are not part of this seven-stage job.',
    ],
    'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
}
(root / 'independent-kernel-audit.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
