"""Frozen raw-input native preparation and offline paired quality gate."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import numpy as np
from scripts.native_two_bit_cosine_development import score_panel

root = Path(sys.argv[1]).resolve()
config_path = root / 'config.json'
config = json.loads(config_path.read_text())
resume = len(sys.argv) == 3 and sys.argv[2] == '--score-prepared'
assert len(sys.argv) == 2 or resume
binaries = root.parent / 'rust-repo/target/release'

def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 << 20), b''):
            digest.update(block)
    return digest.hexdigest()

def write_json(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')

def timed(directory, phase, args):
    with (directory / (phase + '.stdout')).open('x') as stream:
        subprocess.run(['/usr/bin/time', '-v', '-o', str(directory / (phase + '.time')),
                        'timeout', '--kill-after=10s', '900', *map(str, args)],
                       stdout=stream, check=True)
    return (directory / (phase + '.stdout')).read_text().strip()

prepared = []
for item in config:
    directory = root / item['name']
    if resume:
        receipt = json.loads((directory / 'preparation.json').read_text())
        manifest, sq8 = directory / 'generation/manifest.json', directory / 'sq8.bin'
        assert sha(item['raw']) == item['raw_sha256']
        assert sha(directory / 'normalized.f32') == receipt['normalization']['normalized_sha256']
        assert sha(directory / 'order.u64') == receipt['fit']['order_sha256']
        assert sha(sq8) == receipt['encoding']['sq8_sha256']
        assert sha(manifest) == receipt['root_sha256']
        prepared.append((item, directory, manifest, receipt['root_sha256'], sq8))
        continue
    directory.mkdir()
    raw = Path(item['raw'])
    assert sha(raw) == item['raw_sha256']
    assert raw.stat().st_size == 307200000
    normalized, order, sq8 = (directory / name for name in ['normalized.f32', 'order.u64', 'sq8.bin'])
    tool = binaries / 'examples/build_sq8_source'
    normalization = json.loads(timed(directory, 'normalize', [tool, 'normalize', raw, item['raw_sha256'], 100000, 768, 268435456, normalized]))
    assert normalization['normalized_sha256'] == sha(normalized)
    fitting = json.loads(timed(directory, 'fit', [tool, 'fit', normalized, sha(normalized), 100000, 768, 268435456, order]))
    assert fitting['order_sha256'] == sha(order)
    encoding = json.loads(timed(directory, 'encode', [tool, normalized, sha(normalized), 768, order, sha(order), 268435456, sq8]))
    assert encoding['sq8_sha256'] == sha(sq8)
    builder = dict(raw=str(raw), raw_sha256=item['raw_sha256'], sq8=str(sq8),
                   sq8_sha256=encoding['sq8_sha256'], rows=100000, dimensions=768,
                   generation=1, low=encoding['low'], step=encoding['step'],
                   sq8_object_key='native-candidate/objects/' + sha(sq8), sq8_etag='offline-not-published')
    builder_path = directory / 'builder.json'
    write_json(builder_path, builder)
    generation = directory / 'generation'
    root_sha = timed(directory, 'build', [binaries / 'build_two_bit_generation', builder_path, sha(builder_path), 268435456, generation])
    manifest = generation / 'manifest.json'
    assert root_sha == sha(manifest)
    write_json(directory / 'preparation.json', dict(normalization=normalization, fit=fitting, encoding=encoding, root_sha256=root_sha))
    prepared.append((item, directory, manifest, root_sha, sq8))

results = []
for first, count in [(0, 64), (256, 744)]:
    for item, directory, manifest, root_sha, sq8 in prepared:
        label = 'development' if first == 0 else 'validation'
        requests_path, truth_path = Path(item['requests']), Path(item['truth'])
        assert sha(requests_path) == item['requests_sha256']
        assert sha(truth_path) == item['truth_sha256']
        plans_path = directory / (label + '-plans.jsonl')
        timed(directory, label + '-plan', [binaries / 'two_bit_plan_demo', manifest.parent, root_sha, requests_path, item['requests_sha256'], plans_path, first, count])
        requests = [json.loads(line) for line in requests_path.read_text().splitlines()]
        plans = [json.loads(line) for line in plans_path.read_text().splitlines()]
        assert len(requests) == 1000 and len(plans) == count
        assert [r['query_ordinal'] for r in requests] == list(range(1000))
        truth = np.fromfile(truth_path, dtype='<u4').reshape(1000, 100)
        assert (truth < 100000).all()
        body = json.loads(manifest.read_text())
        assert body['sq8_object_sha256'] == sha(sq8)
        dtype = np.dtype([('id', '<i8'), ('norm', '<f4'), ('code', 'u1', (768,))])
        data = np.memmap(sq8, mode='r', dtype=dtype, shape=(100000,))
        assert np.array_equal(np.sort(data['id']), np.arange(100000))
        low, step = (np.asarray(body[key], dtype=np.float32) for key in ['low', 'step'])
        metrics, samples, passed = score_panel(requests[first:first+count], plans, truth, data, low, step, first, count)
        result = dict(dataset=item['name'], rows=100000, dimensions=768, metric='cosine', k=100,
                      first=first, queries=count, root_sha256=root_sha, sq8_sha256=sha(sq8),
                      plans_sha256=sha(plans_path), metrics=metrics, samples=samples,
                      max_gets=max(s['gets'] for s in samples), max_bytes=max(s['bytes'] for s in samples),
                      pass_gate=passed, qualification=False,
                      scope='native raw-input preparation and Rust plans; established Python f32 SQ8 mirror/paired flat; no cloud transport or serving latency')
        write_json(directory / (label + '-score.json'), result)
        results.append({key: value for key, value in result.items() if key != 'samples'})
        write_json(root / ('checkpoint-' + item['name'] + '-' + label + '.json'), results[-1])
        if not passed:
            write_json(root / 'decision.json', dict(decision='KILL', first_failing_dataset=item['name'], split=label, results=results))
            sys.exit(0)
write_json(root / 'decision.json', dict(decision='GO paired offline quality; HOLD release/cloud/vendor/scale', results=results))
