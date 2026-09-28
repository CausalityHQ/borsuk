"""Single two-dataset development falsifier; existing SQ8 scoring mirror."""
import hashlib,json,subprocess,sys
from pathlib import Path
import numpy as np
from scripts.native_two_bit_cosine_development import score_panel
base=Path(sys.argv[1]); out=Path(sys.argv[2]); config=json.loads((base/'config.json').read_text())
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
baseline_hashes={'relaion': 'c7fb31345107195efe1bbe82a0f49a6aeaef58cd9386a6a60e327592899f03b2', 'cohere': '1ec96cd7644b9bd06d9091cacb86a6e64cd505d6d35f3a1abba76160cca99cd5'}
root_hashes={'relaion': 'b8f2ec97c0ad9633d1f3bc672b90ff88e32cb80b0d6ecbeb12f2ddbf74a48ff2', 'cohere': '7b02d4810002c02c842b2fcad454ebe9dd3d4fc5c889ddf6d046fb5a1f2cc7df'}
results=[]
for item in config:
    name=item['name']; original=base/'native-pipeline-quality-a0001'/name
    root=original/'generation'; prep=json.loads((original/'preparation.json').read_text())
    root_sha=root_hashes[name]; assert prep['root_sha256']==root_sha and sha(root/'manifest.json')==root_sha
    requests_path=Path(item['requests']); truth_path=Path(item['truth'])
    assert sha(requests_path)==item['requests_sha256'] and sha(truth_path)==item['truth_sha256']
    plans_path=out/(name+'-plans.jsonl')
    subprocess.run([str(base/'rust-repo/target/release/two_bit_plan_demo'),str(root),root_sha,str(requests_path),item['requests_sha256'],str(plans_path),'0','64','--page-score'],check=True)
    manifest=json.loads((root/'manifest.json').read_text()); sq8_path=original/'sq8.bin'
    assert sha(sq8_path)==manifest['sq8_object_sha256']
    sq8=np.memmap(sq8_path,mode='r',dtype=np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))]),shape=(100000,))
    requests=list(map(json.loads,requests_path.read_text().splitlines()))
    plans=list(map(json.loads,plans_path.read_text().splitlines()))
    assert len(requests)==1000 and [r['query_ordinal'] for r in requests]==list(range(1000))
    assert len(plans)==64 and [r['query_ordinal'] for r in plans]==list(range(64))
    assert np.array_equal(np.sort(sq8['id']),np.arange(100000))
    truth=np.fromfile(truth_path,dtype='<u4').reshape(1000,100)
    page_of=np.empty(100000,dtype=np.int64); page_of[sq8['id']]=np.arange(100000)//256
    for plan in plans:
        candidates=plan['ranked_candidate_pages']
        assert len(candidates)==len(set(candidates))==159
        assert all(0<=page<391 for page in candidates)
        assert sum(min(256,100000-page*256) for page in candidates)<=40704
    low,step=(np.asarray(manifest[key],dtype=np.float32) for key in ['low','step'])
    metrics,samples,passed=score_panel(requests[:64],plans,truth,sq8,low,step,0,64)
    assert sha(original/'development-score.json')==baseline_hashes[name]
    for plan,sample in zip(plans,samples):
        sample['discovered_hits']=int(np.isin(page_of[truth[sample['query_ordinal']]],plan['ranked_candidate_pages']).sum())
    metrics['mean_discovered_hits']=sum(x['discovered_hits'] for x in samples)/64
    baseline=json.loads((original/'development-score.json').read_text())['metrics']
    noninferior=all(metrics[key]>=baseline[key] for key in ['mean_fetched_hits','mean_returned_hits'])
    causal=(name!='relaion' or metrics['mean_fetched_hits']>=baseline['mean_fetched_hits']+.1)
    result=dict(dataset=name,rows=100000,dimensions=768,metric='cosine',k=100,split='development0-63',queries=64,metrics=metrics,baseline=baseline,samples=samples,pass_quality=passed,noninferior=noninferior,causal_gain=causal,root_sha256=root_sha,plans_sha256=sha(plans_path),max_logical_gets=max(x['gets'] for x in samples),max_planned_bytes=max(x['bytes'] for x in samples))
    results.append(result)
    if not(passed and noninferior and causal): break
result=dict(decision='DEVELOPMENT_PASS' if len(results)==2 and all(r['pass_quality'] and r['noninferior'] and r['causal_gain'] for r in results) else 'KILL',qualification=False,results=results)
with (out/'decision.json').open('x') as f: json.dump(result,f,indent=2);f.write('\n')
print(json.dumps({**result,'results':[{k:v for k,v in r.items() if k!='samples'} for r in results]}))
