"""Frozen64-query diagnostic for the corrected cosine API; never qualification."""
import hashlib
import json
import math
from pathlib import Path

import numpy as np
from scripts.v291_two_stage_development import rust_sq8_scores


def checked(path, digest):
    body = Path(path).read_bytes()
    if hashlib.sha256(body).hexdigest() != digest:
        raise ValueError(f"identity: {path}")
    return body


def normalize(query):
    query = np.asarray(query, dtype=np.float32)
    norm2 = sum(float(x) ** 2 for x in query)
    if not np.isfinite(query).all() or norm2 <= 0:
        raise ValueError("query")
    return query if abs(norm2 - 1) <= 1e-6 else np.asarray(
        [float(x) / math.sqrt(norm2) for x in query], dtype=np.float32)


def run():
    root = Path('.borsuk-scratch/v283')
    manifest = json.loads(checked(root / 'native-two-bit-generation/manifest.json',
        'b2420db4aab045979d191b68ab9b61b149b01b6d84782a0cbac0be8779224219'))
    requests = [json.loads(x) for x in checked(root / 'cohere-dev64-requests.jsonl',
        '1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4').splitlines()]
    truth = np.frombuffer(checked(root / 'cohere-truth.u32',
        '06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8'), dtype='<u4').reshape(1000, 100)
    receipt = json.loads(Path('docs/research/native-two-bit-query-check.json').read_text())
    plans = [json.loads(x) for x in checked(
        'docs/research/native-two-bit-query-development-plans.jsonl', receipt['replay']['plans_sha256']).splitlines()]
    dtype = np.dtype([('id', '<i8'), ('norm', '<f4'), ('code', 'u1', (768,))])
    sq8 = np.frombuffer(checked(root / 'cohere-layout/sq8.bin',
        '301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58'), dtype=dtype)
    assert sq8.shape == (100000,) and np.array_equal(np.sort(sq8['id']), np.arange(100000))
    assert len(plans) == len(requests) == 64
    low, step = (np.asarray(manifest[k], dtype=np.float32) for k in ('low', 'step'))
    samples = []
    for i, (request, plan) in enumerate(zip(requests, plans)):
        assert request['query_ordinal'] == plan['query_ordinal'] == i
        query = normalize(request['query'])
        assert query.shape == (768,)
        ranges = plan['ranges']
        assert 0 < len(ranges) <= 32
        assert all(0 <= start < end <= 78000000 and start % 780 == end % 780 == 0 for start, end in ranges)
        assert all(ranges[j][1] < ranges[j+1][0] for j in range(len(ranges)-1))
        size = sum(end-start for start, end in ranges)
        assert size == plan['planned_bytes'] <= 16773120
        physical = np.concatenate([np.arange(start//780, end//780) for start, end in ranges])
        scores = rust_sq8_scores(sq8, query, low, step)
        assert np.isfinite(scores).all()
        hits = lambda ids: int(np.isin(ids, truth[i]).sum())
        fetched = sq8[physical]
        returned = fetched['id'][np.lexsort((fetched['id'], scores[physical]))[:100]]
        flat = sq8['id'][np.lexsort((sq8['id'], scores))[:100]]
        samples.append(dict(query_ordinal=i, fetched_hits=hits(fetched['id']),
            returned_hits=hits(returned), flat_hits=hits(flat), gets=len(ranges), bytes=size))
    metrics = {f'{stat}_{key}': reducer([s[key] for s in samples])
        for stat, reducer in [('mean', lambda xs: sum(xs)/64), ('p05', lambda xs: sorted(xs)[3])]
        for key in ['fetched_hits', 'returned_hits', 'flat_hits']}
    passes = metrics['mean_returned_hits'] >= 98 and metrics['p05_returned_hits'] >= 95 and metrics['mean_flat_hits']-metrics['mean_returned_hits'] <= .5
    result = dict(schema='borsuk-cosine-correction-development-v1', dataset='CoHere first100k',
        dimensions=768, metric='cosine', k=100, split='development0–63', queries=64,
        metrics=metrics, max_gets=max(s['gets'] for s in samples), max_bytes=max(s['bytes'] for s in samples),
        development_screen_pass=passes, qualification=False, samples=samples)
    output = Path('.borsuk-scratch/v283/native-cosine-development-score.json')
    with output.open('x') as f:
        json.dump(result, f, indent=2); f.write('\n')
    print(json.dumps({k:v for k,v in result.items() if k!='samples'}))


if __name__ == '__main__':
    assert np.allclose(normalize([5e29, 2.5e29]), normalize([5e-31, 2.5e-31]))
    run()
