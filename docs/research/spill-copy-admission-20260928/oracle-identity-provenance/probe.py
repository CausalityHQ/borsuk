"""Query-blind epsilon-closure replication cost admission; not recall."""
import hashlib
import json
from pathlib import Path
import sys
import numpy as np

base=Path(sys.argv[1]).resolve()
output=base/'spill-copy-admission-a0001'
output.mkdir()
layoutdir=base/'hierarchical-fit-a0001'
receipt=json.loads((layoutdir/'fit.json').read_text())
bound=json.loads((layoutdir/'terminal.json').read_text())
source_path=base/'native-pipeline-quality-a0001/cohere/normalized.f32'

def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(4<<20),b''):
            digest.update(block)
    return digest.hexdigest()

def alternate_owners(distances,primary):
    assert distances.ndim==2 and len(distances)==len(primary)
    nearest=np.min(distances,axis=1)
    scores=distances.copy()
    scores[np.arange(len(scores)),primary]=np.inf
    nearest_alt=np.argsort(scores,axis=1,kind='stable')[:,:2]
    valid=np.take_along_axis(scores,nearest_alt,axis=1)<=1.15*nearest[:,None]
    return nearest_alt,valid

alts,valid=alternate_owners(np.asarray([[.1,.1,.2],[0.,0.,1.],[.1,.11,.5]]),np.asarray([2,0,2]))
assert alts.tolist()==[[0,1],[1,2],[0,1]] and valid.tolist()==[[True,True],[True,False],[True,True]]
assert sha(source_path)==bound['source_sha256']
assert sha(layoutdir/'order.u64')==receipt['order_sha256']==bound['order_sha256']
order=np.fromfile(layoutdir/'order.u64',dtype='<u8')
assert np.array_equal(np.sort(order),np.arange(100000))
extents=receipt['extents']
source=np.memmap(source_path,dtype='<f4',mode='r',shape=(100000,768))
owner=np.empty(100000,dtype=np.int32)
centers=[]
last=0
for index,(a,b) in enumerate(extents):
    assert a==last and 0<b-a<=1024
    owner[order[a:b]]=index
    mean=np.mean(source[order[a:b]],axis=0,dtype=np.float64)
    norm=np.linalg.norm(mean)
    assert np.isfinite(mean).all() and np.isfinite(norm) and norm>0
    centers.append(mean/norm)
    last=b
assert last==100000
centers=np.asarray(centers)
center_norms=np.sum(centers**2,axis=1)
populations=np.zeros(len(extents),dtype=np.int64)
degrees=np.zeros(3,dtype=np.int64)
processed=copies=0
for first in range(0,100000,256):
    last=min(first+256,100000)
    rows=np.asarray(source[first:last],dtype=np.float64)
    norms=np.sum(rows**2,axis=1)
    assert np.isfinite(rows).all() and np.max(np.abs(norms-1))<=2e-4
    distances=np.maximum(norms[:,None]+center_norms[None,:]-2*rows@centers.T,0)
    alt,valid=alternate_owners(distances,owner[first:last])
    assert np.all(alt[valid]!=np.broadcast_to(owner[first:last,None],valid.shape)[valid])
    populations+=np.bincount(owner[first:last],minlength=len(extents))
    populations+=np.bincount(alt[valid],minlength=len(extents))
    degree=1+np.sum(valid,axis=1)
    degrees+=np.bincount(degree,minlength=4)[1:4]
    copies+=int(np.sum(degree))
    processed=last
    if copies+(100000-processed)>200000:
        break
assert int(np.sum(populations))==copies and int(np.sum(degrees))==processed
lower=(copies+100000-processed)/100000
passed=processed==100000 and lower<=2
result=dict(dataset='CoHere first100k',rows=100000,dimensions=768,source_only=True,
    source_sha256=bound['source_sha256'],order_sha256=bound['order_sha256'],
    processed_rows=processed,observed_assignments=copies,degree_counts_processed=degrees.tolist(),
    full_rho=lower if processed==100000 else None,full_rho_lower_bound=lower,
    owner_populations_processed=populations.tolist(),
    hypothetical_extent_count_full=int(np.sum((populations+1023)//1024)) if processed==100000 else None,
    decision='GO_COPY_COST_ONLY' if passed else 'KILL_COPY_AMPLIFICATION',
    recall_latency_vendor_and_100m_unmeasured=True)
with (output/'terminal.json').open('x') as stream:
    json.dump(result,stream,indent=2);stream.write('\n')
print(json.dumps({k:v for k,v in result.items() if k!='owner_populations_processed'}))
