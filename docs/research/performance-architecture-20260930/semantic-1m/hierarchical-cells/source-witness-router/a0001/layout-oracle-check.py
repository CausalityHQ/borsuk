import array,collections,hashlib,json,math,struct,time
from pathlib import Path
base=Path('/data/orchestration/borsuk-source-witness-closed-audit-a0001/native')
out=Path('/data/orchestration/borsuk-source-witness-closed-audit-a0001/layout-oracle.json');assert not out.exists()
receipt=json.loads((base/'execution-receipt.json').read_bytes());results={};started=time.monotonic()
def auth(p,pin):
 h=hashlib.sha256();n=0
 with p.open('rb') as f:
  while b:=f.read(65536):h.update(b);n+=len(b)
 assert n==pin['bytes'] and h.hexdigest()==pin['sha256']
for dataset in ('relaion','cohere'):
 pins=receipt['inputs'][dataset]
 for name in ('manifest.json','directories.bin','cells.bin'):
  auth(base/'inputs'/dataset/name,pins[name])
 auth(base/'panels'/dataset/'truth64',pins['truth64'])
 root=json.loads((base/'inputs'/dataset/'manifest.json').read_bytes())
 assert root['rows']==100000 and root['dimensions']==768
 width=8+root['input']['records']['bytes']//root['rows'];assert width==208
 membership=array.array('I',[0xffffffff])*100000;cell_sizes={};visited=set();pending=[root['root_directory']]
 with (base/'inputs'/dataset/'directories.bin').open('rb') as dirs,(base/'inputs'/dataset/'cells.bin').open('rb') as cells:
  while pending:
   span=pending.pop();key=(span['offset'],span['bytes']);assert key not in visited;visited.add(key)
   dirs.seek(span['offset']);body=dirs.read(span['bytes']);assert hashlib.sha256(body).hexdigest()==span['sha256']
   page=json.loads(body)
   for node in page['children']:
    target=node['target']
    if target['kind']=='directory':pending.append(target['span']);continue
    cell=target['cell'];n=node['rows'];assert n<=512
    s=cell['source'];assert s['bytes']==n*width
    w=cell['whole'];assert w['bytes']==n*(width+780)
    cells.seek(w['offset']);h=hashlib.sha256();left=w['bytes']
    while left:
     b=cells.read(min(left,65536));assert b;h.update(b);left-=len(b)
    assert h.hexdigest()==w['sha256']
    cells.seek(s['offset']);body=cells.read(s['bytes']);assert hashlib.sha256(body).hexdigest()==s['sha256']
    cid=cell['id'];assert cid not in cell_sizes;cell_sizes[cid]=w['bytes']
    for row in range(n):
     sid=struct.unpack_from('<q',body,row*width)[0];assert 0<=sid<100000 and membership[sid]==0xffffffff;membership[sid]=cid
 assert 0xffffffff not in membership and len(cell_sizes)==root['build']['cells']
 truth=(base/'panels'/dataset/'truth64').read_bytes();assert len(truth)==64*100*4
 observations=[]
 for q in range(64):
  ids=struct.unpack_from('<100I',truth,q*400);assert len(set(ids))==100 and all(0<=i<100000 for i in ids)
  counts=collections.Counter(membership[i] for i in ids)
  ranked=sorted(counts,key=lambda c:(-counts[c],c))
  row=dict(ordinal=q,distinct_truth_cells=len(counts),oracle={})
  for k in (24,32):
   selected=ranked[:k];size=sum(cell_sizes[c] for c in selected);assert size<=16<<20
   row['oracle'][str(k)]=dict(hits=sum(counts[c] for c in selected),bytes=size,gets=len(selected),cells=selected)
  observations.append(row)
 stats={}
 for k in (24,32):
  vals=sorted(r['oracle'][str(k)]['hits'] for r in observations)
  stats[str(k)]=dict(mean_coverage=sum(vals)/6400,p05_hits=vals[math.floor(.05*63)],min_hits=vals[0],max_hypothetical_bytes=max(r['oracle'][str(k)]['bytes'] for r in observations),queries_below95=sum(x<95 for x in vals),queries=64)
 results[dataset]=dict(root=pins['manifest.json'],truth=pins['truth64'],membership_complete=True,cells=len(cell_sizes),ceilings=stats,observations=observations)
result=dict(schema='borsuk-posthoc-retained-layout-oracle-v1',purpose='Truth-informed upper bound only; NOT deployable ANN or fresh quality qualification',source_native_receipt_sha256=hashlib.sha256((base/'execution-receipt.json').read_bytes()).hexdigest(),native_rerun=False,new_ann_queries=0,split='consumed64 FIRST100k D768 cosine k100',budgets=dict(max_gets=32,max_bytes=16<<20),method='Complete authenticated source-ID cell bijection; choose cells with largest exact-GT counts. Cardinality optimum fits byte cap, so top counts are an achievable oracle upper bound.',results=results,wall_seconds=time.monotonic()-started)
out.write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
print(json.dumps({d:r['ceilings'] for d,r in results.items()},sort_keys=True))
