import sys,pathlib,json,hashlib,subprocess,os
src=pathlib.Path(sys.argv[1]);rev=sys.argv[2];gates=pathlib.Path(sys.argv[3])
p=pathlib.Path(sys.argv[4])
assert (gates/'terminal').read_text().strip()=='PASS'
assert (gates/'manager.exit').read_text().strip()=='0'
assert subprocess.check_output(['git','-C',str(src),'cat-file','-t',rev],text=True).strip()=='commit'
assert rev=='a0463ed097296c19539d05529ddad615be760fdd'
def sha(b):return hashlib.sha256(b).hexdigest()
old=pathlib.Path('/mnt/borsuk-workspace/evidence/source-block32-packed-full1000-abba-a0002')
assert (old/'A1.exit').read_text().strip()=='0'
c=json.loads((old/'A1.reader-config.json').read_bytes())
paths={
'runner_source_sha256':'crates/borsuk/src/bin/check_cohere_native_baseline.rs',
'generation_source_sha256':'crates/borsuk/src/two_bit_generation.rs',
'router_source_sha256':'crates/borsuk/src/semantic_unit_router.rs',
'codec_source_sha256':'crates/borsuk/src/rotated_two_bit.rs',
'source_plane_source_sha256':'crates/borsuk/src/two_bit_source.rs',
'source_capsule_source_sha256':'crates/borsuk/src/source_capsule.rs',
'sq8_range_source_sha256':'crates/borsuk/src/sq8_s3_range.rs',
'returned_source_sha256':'crates/borsuk/src/returned_sq8.rs'}
sources={k:sha(subprocess.check_output(['git','-C',str(src),'show',rev+':'+v])) for k,v in paths.items()}
binpath=gates/'artifacts/check_cohere_native_baseline'
binary_sha=sha(binpath.read_bytes())
(p/'frozen-headers').mkdir()
for leaf in [48,40]:
 cfg=p/'configs'/f'full.leaf{leaf}.json';b=cfg.read_bytes();conf=json.loads(b)
 assert conf['semantic_leaf_limit']==leaf and conf['execution']=={'mode':'full'}
 assert conf['source_authentication']=='inline' and conf['serving']=={'mode':'baseline'}
 assert conf['sq8_range_split'] is None and conf.get('shared_byte_window') is None
 assert conf['s3_read_retry']=='one_retry_100ms'
 i=dict(c['expected_identity']);bound=dict(c['expected_bound_inputs'])
 assert i['execution']==bound['execution']=={'mode':'full'}
 for k in ['dataset','revision','rows','count','k','profile','backend','generation_prefix','generation_root_sha256','metric','tie_rule','corpus_intervals','reserved_query_interval','producer_authority','max_memory_bytes','source_get_budget','source_byte_budget','sq8_get_budget','sq8_byte_budget','fetch_parallelism','source_layout']:
  assert bound[k]==conf[k],k
 for key in ['requests','truth']:
  for prop in ['bytes','sha256']: assert bound[key+'_'+prop]==conf[key][prop]
 assert bound['cohort_receipt_sha256']==conf['cohort_receipt']['sha256']
 assert bound['derivation_receipt_sha256']==conf['derivation_receipt']['sha256']
 assert bound['native_source_sha256']==conf['native_source']['source_sha256']
 assert bound['native_sq8_sha256']==conf['native_source']['sq8_sha256']
 assert bound['native_order_sha256']==conf['native_source']['source_order_sha256']
 i.update(sources);i['binary_sha256']=binary_sha;i['config_sha256']=sha(b)
 for h in [i,bound]:
  h['source_authentication']='inline';h['semantic_leaf_limit']=leaf
 out={'schema':c['schema'],'expected_identity':i,'expected_bound_inputs':bound}
 target=p/'frozen-headers'/f'leaf{leaf}.json'
 with target.open('x') as f:json.dump(out,f,sort_keys=True,indent=2);f.write('\n')
manifest={'revision':rev,'binary_sha256':binary_sha,'source_hashes':sources,'header_freeze_before_any_query':True}
with (p/'header-authority.json').open('x') as f:json.dump(manifest,f,sort_keys=True,indent=2);f.write('\n')
print(json.dumps(manifest))
