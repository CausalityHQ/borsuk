"""Run the packaged Rust example, using only Python's standard library for inputs."""
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys

campaign = Path(sys.argv[1]).resolve()
binary = campaign.parent / 'rust-repo/target/debug/examples/build_sq8_source'
raw = campaign / 'demo.raw'
raw.write_bytes(struct.pack('<4f', 2.0, 0.0, 0.0, 3.0))
order = campaign / 'demo.order'
order.write_bytes(struct.pack('<2Q', 1, 0))
normalized = campaign / 'demo.normalized'
sq8 = campaign / 'demo.sq8'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

normalization = json.loads(subprocess.check_output([
    str(binary), 'normalize', str(raw), sha(raw), '2', '2', '1048576', str(normalized)
], text=True, timeout=10))
assert normalization['normalized_sha256'] == sha(normalized)
assert normalized.read_bytes() == struct.pack('<4f', 1.0, 0.0, 0.0, 1.0)
encoding = json.loads(subprocess.check_output([
    str(binary), str(normalized), sha(normalized), '2', str(order), sha(order),
    '1048576', str(sq8)
], text=True, timeout=10))
assert encoding['sq8_sha256'] == sha(sq8)
assert len(sq8.read_bytes()) == 28
assert struct.unpack_from('<q', sq8.read_bytes(), 0)[0] == 1
assert struct.unpack_from('<q', sq8.read_bytes(), 14)[0] == 0
print(json.dumps({'normalization': normalization, 'encoding': encoding,
                  'scope': 'packaged example normalization and SQ8 source creation; not search latency'}))
