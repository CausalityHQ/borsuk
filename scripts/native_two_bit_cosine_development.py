"""Source-pinned cosine quality diagnostic; never serving/vendor qualification."""
import argparse
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


def score_panel(requests, plans, truth, sq8, low, step, first, count, *, local_ordinals=False):
    samples = []
    for i, (request, plan) in enumerate(zip(requests, plans)):
        assert request['query_ordinal'] == i + first
        assert plan['query_ordinal'] == (i if local_ordinals else i + first)
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
        hits = lambda ids: int(np.isin(ids, truth[i + first]).sum())
        fetched = sq8[physical]
        returned = fetched['id'][np.lexsort((fetched['id'], scores[physical]))[:100]]
        flat = sq8['id'][np.lexsort((sq8['id'], scores))[:100]]
        samples.append(dict(query_ordinal=i + first, fetched_hits=hits(fetched['id']),
            returned_hits=hits(returned), flat_hits=hits(flat), gets=len(ranges), bytes=size))
    metrics = {f'{stat}_{key}': reducer([s[key] for s in samples])
        for stat, reducer in [('mean', lambda xs: sum(xs)/count), ('p05', lambda xs: sorted(xs)[math.ceil(.05 * count)-1])]
        for key in ['fetched_hits', 'returned_hits', 'flat_hits']}
    passes = metrics['mean_returned_hits'] >= 98 and metrics['p05_returned_hits'] >= 95 and metrics['mean_flat_hits']-metrics['mean_returned_hits'] <= .5
    return metrics, samples, passes


def run(validation_plan_sha=None, full_validation_plan_sha=None):
    root = Path('.borsuk-scratch/v283')
    manifest = json.loads(checked(root / 'native-two-bit-generation/manifest.json',
        'b2420db4aab045979d191b68ab9b61b149b01b6d84782a0cbac0be8779224219'))
    first = 256 if validation_plan_sha or full_validation_plan_sha else 0
    count = 744 if full_validation_plan_sha else 64
    if full_validation_plan_sha:
        gate = json.loads(Path('docs/research/native-cosine-full-validation-gate.json').read_text())
        assert gate['first'] == first and gate['count'] == count and not gate['qualification']
        request_path = root / 'cohere-requests.jsonl'
        requests_sha = '86d9406486a2bb27aa2e603f019e078dd3ecaed47f79ec685558ba3536433812'
        plans_path = root / 'native-cosine-full-validation-plans.jsonl'
        plans_sha = full_validation_plan_sha
    elif validation_plan_sha:
        gate = json.loads(Path('docs/research/native-cosine-validation-gate.json').read_text())
        assert gate['first'] == first and gate['count'] == 64 and not gate['qualification']
        request_path = root / 'native-cosine-validation64-requests.jsonl'
        requests_sha = gate['requests_sha256']
        plans_path = root / 'native-cosine-validation64-plans.jsonl'
        plans_sha = validation_plan_sha
    else:
        request_path = root / 'cohere-dev64-requests.jsonl'
        requests_sha = '1de0122f73d1b72e54498640b9701ce6d156b513629596447580c85fac302ba4'
        receipt = json.loads(Path('docs/research/native-two-bit-query-check.json').read_text())
        plans_path = Path('docs/research/native-two-bit-query-development-plans.jsonl')
        plans_sha = receipt['replay']['plans_sha256']
    requests = [json.loads(x) for x in checked(request_path, requests_sha).splitlines()]
    if full_validation_plan_sha:
        assert len(requests) == 1000
        assert [r["query_ordinal"] for r in requests] == list(range(1000))
        requests = requests[first:first + count]
    truth = np.frombuffer(checked(root / 'cohere-truth.u32',
        '06cd59b31962d4190367b54d7abf24dd4e018d3c4ac8da0b2b528d21a5a7cbb8'), dtype='<u4').reshape(1000, 100)
    plans = [json.loads(x) for x in checked(plans_path, plans_sha).splitlines()]
    dtype = np.dtype([('id', '<i8'), ('norm', '<f4'), ('code', 'u1', (768,))])
    sq8 = np.frombuffer(checked(root / 'cohere-layout/sq8.bin',
        '301696df05ca03122951b66ad8a9bedb5d5f1e675c6fc66f6019abbce3fcda58'), dtype=dtype)
    assert sq8.shape == (100000,) and np.array_equal(np.sort(sq8['id']), np.arange(100000))
    assert len(plans) == len(requests) == count
    low, step = (np.asarray(manifest[k], dtype=np.float32) for k in ('low', 'step'))
    metrics, samples, passes = score_panel(requests, plans, truth, sq8, low, step,
        first, count, local_ordinals=not full_validation_plan_sha)
    result = dict(schema='borsuk-cosine-correction-screen-v1', dataset='CoHere first100k',
        dimensions=768, metric='cosine', k=100, split='validation256–999' if full_validation_plan_sha else ('validation256–319' if validation_plan_sha else 'development0–63'), queries=count,
        metrics=metrics, max_gets=max(s['gets'] for s in samples), max_bytes=max(s['bytes'] for s in samples),
        screen_pass=passes, full_split_pass=bool(full_validation_plan_sha and passes), qualification=False, samples=samples)
    output = root / ('native-cosine-full-validation-score.json' if full_validation_plan_sha else ('native-cosine-validation-score.json' if validation_plan_sha else 'native-cosine-development-score.json'))
    with output.open('x') as f:
        json.dump(result, f, indent=2); f.write('\n')
    print(json.dumps({k:v for k,v in result.items() if k!='samples'}))


if __name__ == '__main__':
    assert np.allclose(normalize([5e29, 2.5e29]), normalize([5e-31, 2.5e-31]))
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--validation-plan-sha")
    group.add_argument("--full-validation-plan-sha")
    args = parser.parse_args()
    run(args.validation_plan_sha, args.full_validation_plan_sha)
