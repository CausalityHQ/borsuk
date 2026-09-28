"""GT-aware32-page containment bound; not a query router or recall result."""
import hashlib,json,math,sys
from pathlib import Path
import numpy as np
base=Path(sys.argv[1]); output=Path(sys.argv[2])
config=json.loads(Path(sys.argv[3]).read_text())
roots={'relaion':'b8f2ec97c0ad9633d1f3bc672b90ff88e32cb80b0d6ecbeb12f2ddbf74a48ff2','cohere':'7b02d4810002c02c842b2fcad454ebe9dd3d4fc5c889ddf6d046fb5a1f2cc7df'}
def checked(path,digest):
    data=path.read_bytes(); assert hashlib.sha256(data).hexdigest()==digest; return data
results=[]
random=np.random.default_rng(20260928).permutation(100000)
for item in config:
    name=item['name']; root=base/'native-pipeline-quality-a0001'/name
    checked(root/'generation/manifest.json',roots[name])
    receipt=json.loads((root/'preparation.json').read_text())
    order=np.frombuffer(checked(root/'order.u64',receipt['fit']['order_sha256']),dtype='<u8')
    assert np.array_equal(np.sort(order),np.arange(100000))
    truth=np.frombuffer(checked(Path(item['truth']),item['truth_sha256']),dtype='<u4').reshape(1000,100)[:64]
    assert (truth<100000).all() and all(len(np.unique(row))==100 for row in truth)
    arms=[]
    for label,permutation in [('native',order),('random',random)]:
        page_of=np.empty(100000,dtype=np.int64); page_of[permutation]=np.arange(100000)//256
        samples=[]
        for ordinal,neighbors in enumerate(truth):
            counts=np.bincount(page_of[neighbors],minlength=391)
            samples.append(dict(query_ordinal=ordinal,top32_hits=int(np.sort(counts)[-32:].sum()),gt_pages=int(np.count_nonzero(counts)),max_hits_per_page=int(counts.max())))
        hits=sorted(s['top32_hits'] for s in samples)
        arms.append(dict(arm=label,order_sha256=hashlib.sha256(np.asarray(permutation,dtype='<u8').tobytes()).hexdigest(),mean_top32_hits=sum(hits)/64,p05_top32_hits=hits[math.ceil(.05*64)-1],mean_gt_pages=sum(s['gt_pages'] for s in samples)/64,samples=samples))
    results.append(dict(dataset=name,rows=100000,dimensions=768,metric='cosine',k=100,split='development0-63',queries=64,max_pages=32,max_logical_gets=32,max_bytes=32*256*780,arms=arms))
result=dict(qualification=False,scope='GT-aware optimistic32-page upper bound versus random order; tighter byte budget than frozen84-page gate; no source fitting, returned recall, transport, latency or100M qualification',results=results)
with output.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
print(json.dumps([{**r,'arms':[{k:v for k,v in a.items() if k!='samples'} for a in r['arms']]} for r in results]))
