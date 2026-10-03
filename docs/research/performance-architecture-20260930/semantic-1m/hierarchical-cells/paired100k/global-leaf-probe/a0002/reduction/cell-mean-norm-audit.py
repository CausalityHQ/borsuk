import json,pathlib,struct,math,statistics
from scripts import reduce_hierarchical_global_leaf_coverage as r
p=pathlib.Path('docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k/global-leaf-probe/a0002/reduction');c=json.loads((p/'config.json').read_text());out={'schema':'borsuk-closed-cell-mean-norm-audit-v1','scope':'authenticated directory metadata only; no query/GT reads or rerouting','datasets':{}}
for d in r.DATASETS:
 root,leaves,_=r.layout(c['datasets'][d]['candidate_root'],c['datasets'][d]['directories'])
 vals=[]
 for i,v in leaves.items():
  x=[struct.unpack('<f',struct.pack('<I',b))[0] for b in v['prototype_bits']];vals.append(math.fsum(y*y for y in x))
 out['datasets'][d]={'cells':len(vals),'mean_squared_norm':statistics.mean(vals),'min_squared_norm':min(vals),'median_squared_norm':statistics.median(vals),'max_squared_norm':max(vals),'root_sha256':c['datasets'][d]['candidate_root']['sha256'],'directories_sha256':c['datasets'][d]['directories']['sha256']}
print(json.dumps(out,sort_keys=True,indent=2))
