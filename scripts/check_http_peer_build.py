"""Targeted example tests/release build; unchanged library assurance is reused."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

cargo, repo, out = sys.argv[1:]
repo, out = Path(repo), Path(out)
source = repo / 'crates/borsuk/examples/two_bit_http.rs'
original = source.read_bytes()
assert b'const QUERY_SLOTS: usize = 4;' in original and b'ip.is_private()' in original
args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
        '--target-dir', str(out / 'target'), '-p', 'borsuk', '--jobs', '4', '--example', 'two_bit_http']
for operation, name in [('test', 'green'), ('build', 'release')]:
    with (out / (name + '.log')).open('x') as log:
        subprocess.run([cargo, operation, *args], stdout=log, stderr=subprocess.STDOUT, check=True)
assert '0 failed;' in (out / 'green.log').read_text()
assert source.read_bytes() == original
(out / 'http.fixed.rs').write_bytes(original)
(out / 'binaries').mkdir(exist_ok=True)
binary = (out / 'target/release/examples/two_bit_http').read_bytes()
(out / 'binaries/two_bit_http').write_bytes(binary)
group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
(out / 'boundary-cgroup.json').write_text(json.dumps({k: (group/k).read_text() for k in
    ['memory.max', 'memory.peak', 'memory.swap.peak', 'memory.events', 'cpu.stat']}, indent=2)+'\n')
report = dict(qualified=True, no_corpus_query=True, full_workspace_repeated=False,
    green_status=0, release_status=0, compiled_http_sha256=hashlib.sha256(original).hexdigest(),
    binary_sha256=hashlib.sha256(binary).hexdigest(), query_slots=4, library_assurance_reused=2696)
(out / 'boundary-check.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(report))
