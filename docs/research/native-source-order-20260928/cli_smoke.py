"""Exercise normalization -> native order -> SQ8 through the Rust example."""
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
binary = root.parent / 'rust-repo/target/debug/examples/build_sq8_source'
rows = 513
raw = root / 'raw.f32'
raw.write_bytes(b''.join(struct.pack('<2f', *[(2., 0.), (0., 3.), (-4., 0.)][i % 3]) for i in range(rows)))
normalized, order, sq8 = (root / name for name in ['normalized.f32', 'order.u64', 'sq8.bin'])

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def run(*args):
    return json.loads(subprocess.check_output([str(binary), *map(str, args)], text=True, timeout=30))

normalization = run('normalize', raw, sha(raw), rows, 2, 1048576, normalized)
assert normalization['normalized_sha256'] == sha(normalized)
fitting = run('fit', normalized, sha(normalized), rows, 2, 1048576, order)
assert fitting['order_sha256'] == sha(order)
ordinals = struct.unpack('<' + 'Q' * rows, order.read_bytes())
assert sorted(ordinals) == list(range(rows))
rejected = subprocess.run([str(binary), 'fit', str(normalized), sha(normalized), str(rows), '2', '1048576', str(order)], capture_output=True, timeout=30)
assert rejected.returncode != 0 and fitting['order_sha256'] == sha(order)
encoding = run(normalized, sha(normalized), 2, order, sha(order), 1048576, sq8)
assert encoding['sq8_sha256'] == sha(sq8)
body = sq8.read_bytes()
assert len(body) == rows * 14
assert [struct.unpack_from('<q', body, i * 14)[0] for i in range(rows)] == list(ordinals)
print(json.dumps({'rows': rows, 'dimensions': 2, 'normalize': normalization, 'fit': fitting, 'sq8': encoding,
                  'overwrite_rejected': True, 'scope': 'native CLI geometry/identity smoke; not corpus quality or serving performance'}))
