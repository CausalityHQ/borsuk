"""Development-only exhaustive centroid diagnostic; not a serving candidate."""
import hashlib,json,struct,sys
from pathlib import Path
import numpy as np
base=Path(sys.argv[1]); out=Path(sys.argv[2])
config=json.loads(Path(sys.argv[3]).read_text())[0]
root=base/'native-pipeline-quality-a0001/relaion/generation'
def checked(path,digest):
    data=path.read_bytes(); assert hashlib.sha256(data).hexdigest()==digest; return data
manifest=json.loads(checked(root/'manifest.json','b8f2ec97c0ad9633d1f3bc672b90ff88e32cb80b0d6ecbeb12f2ddbf74a48ff2'))
blob=checked(root/'centroids.bin',manifest['centroids_sha256'])
assert blob[:8]==b'BORSUCP1' and struct.unpack('<QIIII',blob[8:32])==(100000,768,32,256,0)
centers=np.frombuffer(blob,offset=32,dtype='<f2').astype(np.float32).reshape(3125,768)
truth=np.frombuffer(checked(Path(config['truth']),config['truth_sha256']),dtype='<u4').reshape(1000,100)
sq8=np.frombuffer(checked(base/'native-pipeline-quality-a0001/relaion/sq8.bin',manifest['sq8_object_sha256']),dtype=np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))]))
assert np.array_equal(np.sort(sq8['id']),np.arange(100000))
page_of=np.empty(100000,dtype=np.int64); page_of[sq8['id']]=np.arange(100000)//256
requests=list(map(json.loads,checked(Path(config['requests']),config['requests_sha256']).splitlines()))
traces=list(map(json.loads,(out/'traces.jsonl').read_text().splitlines()))
assert [x['query_ordinal'] for x in traces]==list(range(64))
samples=[]
for ordinal,trace in enumerate(traces):
    q=np.asarray(requests[ordinal]['query'],dtype=np.float32)
    squared_norm=np.cumsum(q.astype(np.float64)**2)[-1]
    if abs(squared_norm-1)>1e-6: q=(q.astype(np.float64)/np.sqrt(squared_norm)).astype(np.float32)
    squares=(centers-q)**2
    distances=np.cumsum(squares,axis=1,dtype=np.float32)[:,-1]
    page_dist=np.full(391,np.inf,dtype=np.float32)
    np.minimum.at(page_dist,np.arange(3125)//8,distances)
    flat=np.lexsort((np.arange(391),page_dist))[:159]
    graph=trace['ranked_candidate_pages']; assert len(graph)==len(set(graph))==159
    gtpages=page_of[truth[ordinal]]
    samples.append(dict(query_ordinal=ordinal,graph_hits=int(np.isin(gtpages,graph).sum()),flat_centroid_hits=int(np.isin(gtpages,flat).sum()),page_overlap=len(set(graph)&set(flat))))
result=dict(dataset='ReLAION first100k',split='development0-63',rows=100000,dimensions=768,k=100,queries=64,samples=samples,qualification=False,scope='Exhaustive nearest-unit centroid page ranking diagnostic, not a new route or latency claim',means={key:sum(s[key] for s in samples)/64 for key in ['graph_hits','flat_centroid_hits','page_overlap']})
with (out/'result.json').open('x') as f: json.dump(result,f,indent=2); f.write('\n')
print(json.dumps(result['means']))
