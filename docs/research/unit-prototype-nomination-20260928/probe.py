"""Unit-prototype cosine geometry falsifier; not a scalable query route."""
import hashlib
import json
import math
from pathlib import Path
import sys
import numpy as np

base = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(base / 'quality-repo'))
from scripts.native_two_bit_cosine_development import normalize, score_panel

output = base / 'unit-prototype-nomination-a0001'
output.mkdir()
config = json.loads((base / 'native-pipeline-quality-a0001/config.json').read_text())

def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 << 20), b''):
            digest.update(block)
    return digest.hexdigest()

def unit_centroids(centroids):
    norms = np.linalg.norm(centroids.astype(np.float64),axis=1)
    if not np.isfinite(centroids).all() or not np.isfinite(norms).all() or np.any(norms<=0):
        raise ValueError('invalid prototype direction')
    return np.asarray(centroids/norms[:,None],dtype=np.float32), norms

def nominate(centroids, parents, extent_count, query, cap):
    # Exact distance reference in f64; no graph/approximation confounder.
    scores = np.sum((centroids.astype(np.float64) - query) ** 2, axis=1)
    best = np.full(extent_count, np.inf)
    np.minimum.at(best, parents, scores)
    return np.lexsort((np.arange(extent_count), best))[:cap]

assert nominate(np.asarray([[1.,0.],[0.,1.],[1.,0.]],dtype=np.float32), [0,0,1], 2, [1.,0.], 2).tolist() == [0,1]
assert nominate(np.asarray([[1.,0.],[0.,1.],[.5,.5]],dtype=np.float32), [0,0,1], 2, [0.,1.], 1).tolist() == [0]
biased = np.asarray([[.2,.01],[.9,.4]],dtype=np.float32)
assert nominate(biased,[0,1],2,[1.,0.],1).tolist()==[1]
assert nominate(unit_centroids(biased)[0],[0,1],2,[1.,0.],1).tolist()==[0]
assert nominate(unit_centroids(biased*np.asarray([[5.],[.1]]))[0],[0,1],2,[1.,0.],1).tolist()==[0]
try:
    unit_centroids(np.zeros((1,2),dtype=np.float32))
except ValueError:
    pass
else:
    raise AssertionError('zero direction admitted')
results = []
for dataset in ('cohere','relaion'):
    item = next(x for x in config if x['name'] == dataset)
    prepdir = base / 'native-pipeline-quality-a0001' / dataset
    prep = json.loads((prepdir / 'preparation.json').read_text())
    layoutdir = base / ('hierarchical-fit-a0001' if dataset == 'cohere' else 'hierarchical-fit-relaion-a0001')
    bound = json.loads((layoutdir / 'terminal.json').read_text())
    receipt = json.loads((layoutdir / 'fit.json').read_text())
    assert bound['decision'] == 'GO_TO_NOMINATION_ONLY'
    assert sha(prepdir / 'normalized.f32') == bound['source_sha256'] == prep['normalization']['normalized_sha256']
    assert sha(layoutdir / 'order.u64') == bound['order_sha256'] == receipt['order_sha256']
    assert sha(item['requests']) == item['requests_sha256']
    assert sha(item['truth']) == item['truth_sha256'] == bound['truth_sha256']
    order = np.fromfile(layoutdir / 'order.u64',dtype='<u8')
    assert np.array_equal(np.sort(order), np.arange(100000))
    extents = receipt['extents']
    source = np.memmap(prepdir / 'normalized.f32',dtype='<f4',mode='r',shape=(100000,768))
    summaries, parents = [], []
    for extent,(a,b) in enumerate(extents):
        for first in range(a,b,32):
            summaries.append(np.mean(source[order[first:min(first+32,b)]],axis=0,dtype=np.float64))
            parents.append(extent)
    centroids = np.asarray(summaries,dtype=np.float32)
    del summaries
    parents = np.asarray(parents,dtype=np.int32)
    assert np.isfinite(centroids).all()
    centroids, prototype_norms = unit_centroids(centroids)
    assert np.allclose(np.linalg.norm(centroids,axis=1),1,atol=2e-7)
    truth = np.fromfile(item['truth'],dtype='<u4').reshape(-1,100)
    requests = [json.loads(line) for line in Path(item['requests']).read_text().splitlines()][:64]
    extent_of = np.empty(100000,dtype=np.int32)
    last = 0
    for i,(a,b) in enumerate(extents):
        assert a == last and 0 < b-a <= 1024
        extent_of[order[a:b]] = i
        last = b
    assert last == 100000
    samples, plans = [], []
    for ordinal,request in enumerate(requests):
        chosen = nominate(centroids,parents,len(extents),normalize(request['query']),21)
        ranges = []
        for index in sorted(chosen):
            a,b = extents[index]
            if ranges and ranges[-1][1] == a*780:
                ranges[-1][1] = b*780
            else:
                ranges.append([a*780,b*780])
        size = sum(b-a for a,b in ranges)
        assert len(ranges)<=21 and size<=16773120
        samples.append(dict(query_ordinal=ordinal,selected_extents=chosen.tolist(),
                            fetched_hits=int(np.isin(extent_of[truth[ordinal]],chosen).sum()),
                            gets=len(ranges),bytes=size))
        plans.append(dict(query_ordinal=ordinal,ranges=ranges,planned_bytes=size))
    counts = [s['fetched_hits'] for s in samples]
    mean,p05 = sum(counts)/64,sorted(counts)[math.ceil(.05*64)-1]
    passes = mean>=98.9 and p05>=96
    result = dict(dataset=dataset,split='development0-63 (previously used)',rows=100000,
                  dimensions=768,k=100,metric='cosine',source_sha256=bound['source_sha256'],
                  order_sha256=bound['order_sha256'],requests_sha256=item['requests_sha256'],
                  truth_sha256=item['truth_sha256'],mean_fetched_percent=mean,p05_fetched_percent=p05,
                  max_gets=max(s['gets'] for s in samples),max_bytes=max(s['bytes'] for s in samples),
                  samples=samples,prototype_count=len(centroids),prototype_f32_bytes=centroids.nbytes,unnormalized_prototype_norm_quantiles=np.quantile(prototype_norms,[0,.05,.5,.95,1]).tolist(),actual_s3_and_latency_unmeasured=True)
    if passes:
        assert sha(prepdir / 'sq8.bin') == prep['encoding']['sq8_sha256']
        dtype = np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))])
        original = np.memmap(prepdir / 'sq8.bin',dtype=dtype,mode='r')
        assert len(original)==100000 and np.array_equal(np.sort(original['id']),np.arange(100000))
        position = np.empty(100000,dtype=np.int64)
        position[original['id']] = np.arange(100000)
        reordered = original[position[order]]
        metrics,quality,passes = score_panel(requests,plans,truth,reordered,
            np.asarray(prep['encoding']['low'],dtype=np.float32),
            np.asarray(prep['encoding']['step'],dtype=np.float32),0,64)
        result.update(sq8_sha256=prep['encoding']['sq8_sha256'],quality_metrics=metrics,quality_samples=quality)
        result['first_failing_layer'] = None if passes else 'returned SQ8 / nomination interaction'
    else:
        result['first_failing_layer'] = 'exact unit-prototype extent nomination coverage'
        result['sq8_scoring_skipped'] = True
    result['decision'] = 'GO_DEVELOPMENT_ONLY' if passes else 'KILL_NOMINATION_GEOMETRY'
    with (output / (dataset+'.json')).open('x') as stream:
        json.dump(result,stream,indent=2);stream.write('\n')
    results.append({k:v for k,v in result.items() if k not in ('samples','quality_samples')})
    if not passes:
        break
terminal = dict(results=results,advance=len(results)==2 and all(r['decision']=='GO_DEVELOPMENT_ONLY' for r in results),
                diagnostic_only=True,source_revision='9c0f62c8')
with (output/'terminal.json').open('x') as stream:
    json.dump(terminal,stream,indent=2);stream.write('\n')
print(json.dumps(terminal))
