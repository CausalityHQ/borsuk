"""One authenticated100k D768 cosine quality panel, with a paired flat control."""
import argparse
import json
from pathlib import Path
import numpy as np
from scripts.native_two_bit_cosine_development import checked, score_panel


def evaluate(config_path, config_sha, plans_path, plans_sha):
    config = json.loads(checked(config_path, config_sha))
    first, count = config['first'], config['count']
    if (first, count) not in [(0, 64), (256, 64), (256, 744)]:
        raise ValueError('unsupported frozen split')
    root = Path(config['root'])
    manifest = json.loads(checked(root / 'manifest.json', config['root_sha256']))
    assert manifest['schema'] == 'borsuk-two-bit-generation-v1'
    plane = json.loads(checked(root / 'plane/manifest.json', manifest['plane_manifest_sha256']))
    assert (plane['rows'], plane['dimensions']) == (100000, 768)
    assert manifest['sq8_object_sha256'] == config['sq8_sha256']
    requests = [json.loads(x) for x in checked(config['requests'], config['requests_sha256']).splitlines()]
    assert len(requests) == 1000 and [r['query_ordinal'] for r in requests] == list(range(1000))
    requests = requests[first:first+count]
    plans = [json.loads(x) for x in checked(plans_path, plans_sha).splitlines()]
    assert len(plans) == count
    truth = np.frombuffer(checked(config['truth'], config['truth_sha256']), dtype='<u4').reshape(1000, 100)
    assert (truth < 100000).all()
    assert all(np.unique(row).size == 100 for row in truth[first:first+count])
    dtype = np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    sq8 = np.frombuffer(checked(config['sq8'], config['sq8_sha256']), dtype=dtype)
    assert sq8.shape == (100000,) and np.array_equal(np.sort(sq8['id']), np.arange(100000))
    assert np.isfinite(sq8['norm']).all() and (sq8['norm'] > 0).all()
    low, step = (np.asarray(manifest[k],dtype=np.float32) for k in ['low','step'])
    assert low.shape == step.shape == (768,) and np.isfinite(low).all()
    assert np.isfinite(step).all() and (step > 0).all()
    metrics, samples, passes = score_panel(requests, plans, truth, sq8, low, step, first, count)
    return dict(schema='borsuk-authenticated-cosine-quality-panel-v1', dataset=config['dataset'],
        rows=100000, dimensions=768, metric='cosine', k=100, first=first, queries=count,
        metrics=metrics, max_gets=max(s['gets'] for s in samples), max_bytes=max(s['bytes'] for s in samples),
        quality_gate_pass=passes, qualification=False, config_sha256=config_sha,
        plans_sha256=plans_sha, samples=samples)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    for arg in ['config','config-sha','plans','plans-sha','output']:
        parser.add_argument('--'+arg,required=True)
    args = parser.parse_args()
    result = evaluate(args.config,args.config_sha,args.plans,args.plans_sha)
    with Path(args.output).open('x') as f:
        json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({k:v for k,v in result.items() if k!='samples'}))
