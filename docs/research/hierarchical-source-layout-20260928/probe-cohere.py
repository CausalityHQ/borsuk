"""Frozen source-only fit and development-only extent containment diagnostic."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import numpy as np

base = Path(sys.argv[1]).resolve()
output = base / 'hierarchical-fit-a0001'
output.mkdir()
config = json.loads((base / 'native-pipeline-quality-a0001/config.json').read_text())
item = next(x for x in config if x['name'] == 'cohere')
source = base / 'native-pipeline-quality-a0001/cohere/normalized.f32'
prep = json.loads((source.parent / 'preparation.json').read_text())

def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 << 20), b''):
            digest.update(block)
    return digest.hexdigest()

source_sha = prep['normalization']['normalized_sha256']
assert sha(source) == source_sha
truth_path = Path(item['truth'])
assert sha(truth_path) == item['truth_sha256']
order_path = output / 'order.u64'
with (output / 'fit.json').open('x') as stream:
    subprocess.run(['/usr/bin/time', '-v', '-o', str(output / 'fit.time'),
                    str(base / 'rust-repo/target/release/examples/build_sq8_source'),
                    'hier-fit', str(source), source_sha, '100000', '768',
                    '268435456', str(order_path)], stdout=stream, check=True)
receipt = json.loads((output / 'fit.json').read_text())
assert receipt['recipe'] == 'borsuk-hierarchical-extents-chacha8-v1'
assert receipt['query_or_truth_used'] is False
assert receipt['order_sha256'] == sha(order_path)
order = np.fromfile(order_path, dtype='<u8')
assert len(order) == 100000 and np.array_equal(np.sort(order), np.arange(100000))
extents = receipt['extents']
assert extents[0][0] == 0 and extents[-1][1] == 100000
extent_of = np.empty(100000, dtype=np.int32)
last = 0
for index, (start, end) in enumerate(extents):
    assert start == last and 0 < end - start <= 1024
    extent_of[order[start:end]] = index
    last = end
truth = np.fromfile(truth_path, dtype='<u4').reshape(-1, 100)[:64]
assert truth.shape == (64, 100) and np.max(truth) < 100000
panels = {}
for cap in (32, 21):
    hits = [int(np.sort(np.bincount(extent_of[q], minlength=len(extents)))[-cap:].sum()) for q in truth]
    panels[str(cap)] = dict(hits=hits, mean_percent=float(np.mean(hits)),
                            p05_percent=int(sorted(hits)[int(np.ceil(.05 * len(hits))) - 1]))
upper, witness = panels['32'], panels['21']
passes = lambda row: row['mean_percent'] >= 98.9 and row['p05_percent'] >= 96
verdict = 'KILL' if not passes(upper) else ('GO_TO_NOMINATION_ONLY' if passes(witness) else 'HOLD_BYTE_KNAPSACK')
result = dict(dataset='CoHere first100k', dimensions=768, metric='cosine', k=100,
              split='development queries 0-63 (previously used)', queries=64,
              source_sha256=source_sha, truth_sha256=item['truth_sha256'],
              order_sha256=receipt['order_sha256'], extent_count=len(extents),
              max_extent_rows=max(b-a for a,b in extents), panels=panels,
              decision=verdict, diagnostic_only=True,
              actual_nomination_recall_latency_and_vendor_win_unmeasured=True)
with (output / 'terminal.json').open('x') as stream:
    json.dump(result, stream, indent=2)
    stream.write('\n')
print(json.dumps({k:v for k,v in result.items() if k != 'panels'}))
print(json.dumps({k:{a:b for a,b in v.items() if a != 'hits'} for k,v in panels.items()}))
