from pathlib import Path
import json
import tempfile

script = Path('/tmp/borsuk-four-row-select-probe-repaired-a0004.py').read_text()
binary = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-gate-a0002/probe-libtest')
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    artifact = {'reason': 'compiler-artifact', 'target': {'name': 'borsuk', 'kind': ['lib']}, 'profile': {'test': True}, 'executable': str(binary)}
    (root / 'probe-compile.log').write_text(json.dumps(artifact) + '\n')
    script = script.replace("pathlib.Path('/mnt/borsuk-http/evidence')", 'pathlib.Path(' + repr(directory) + ')')
    status = 0
    try:
        exec(compile(script, 'retained-ELF-selector-check', 'exec'), {})
    except SystemExit as error:
        status = error.code
    assert status == 97
    artifact = json.loads((root / 'probe-artifact.json').read_text())
    assert artifact['sha256'] == 'bbfa40b5cc2daca732fb0de19d87a059982ec11fd3fa41add19a7c96706ebb8a'
    assert artifact['kernel_address'] == 0x1b6a700 and artifact['kernel_bytes'] == 723
    assert json.loads((root / 'early-codegen-screen.json').read_text())['status'] == 'CODEGEN_GATE_UNMET_NO_TIMING'
    receipt = {'status': 'PASS_REAL_RETAINED_ELF_METADATA_SELECTOR_REPAIR', 'elf_sha256': artifact['sha256'], 'symbol_resolved': True, 'old_scalar_screen_exit': status, 'old_disposition_unchanged': True, 'native_execution': False}
    Path('/tmp/borsuk-four-row-selector-a0004-real-elf-check.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))
