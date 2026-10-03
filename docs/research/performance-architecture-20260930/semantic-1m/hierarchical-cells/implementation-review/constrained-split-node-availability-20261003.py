import pathlib,json,hashlib
p=pathlib.Path('docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k/global-leaf-probe/a0002/screen/retained');out={}
for dataset in ['relaion','cohere']:
 root=json.loads((p/dataset/'manifest.json').read_text());body=(p/dataset/'directories.bin').read_bytes();assert hashlib.sha256(body).hexdigest()==root['directory_sha256'];root_sha=hashlib.sha256((p/dataset/'manifest.json').read_bytes()).hexdigest();pending=[root['root_directory']];eligible=[];allnodes=0
 while pending:
  span=pending.pop();part=body[span['offset']:span['offset']+span['bytes']];assert hashlib.sha256(part).hexdigest()==span['sha256'];page=json.loads(part);nodes=page['children'];allnodes+=1
  if len(nodes)==2:
   counts=[x['rows'] for x in nodes];m=sum(counts)
   if 513<=m<=2048 and abs(counts[0]-counts[1])<=1:
    eligible.append({'offset':span['offset'],'rows':m,'child_rows':counts,'hash_order_key':hashlib.sha256(bytes.fromhex(root_sha)+span['offset'].to_bytes(8,'little')).hexdigest()})
  for n in nodes:
   if n['target']['kind']=='directory':pending.append(n['target']['span'])
 selected=sorted(eligible,key=lambda x:x['hash_order_key'])[:8];out[dataset]={'root_sha256':root_sha,'directory_sha256':root['directory_sha256'],'internal_nodes':allnodes,'eligible_equal_population_nodes':len(eligible),'first8_prospective_byte_encoding':selected,'fallback_replay_not_yet_verified':True}
print(json.dumps({'schema':'borsuk-constrained-split-node-availability-v1','selection_encoding':'root SHA raw32 bytes concatenated LEu64 directory offset; prospective, final native contract must match','query_or_truth_opened':False,'datasets':out},sort_keys=True,indent=2))
