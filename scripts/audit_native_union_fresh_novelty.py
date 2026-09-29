"""Metadata-only prior-query overlap witness; never reads the new sealed cohort."""
import ast,hashlib,io,json,tarfile
from pathlib import Path
import boto3
root=Path('docs/research/native-union-20260928');out=root/'fresh-novelty';out.mkdir(exist_ok=True)
config=json.loads((root/'fresh-config.json').read_text());item=config['items'][0];seal=json.loads((root/'fresh/a0002/verification.json').read_text());assert seal['valid_construction'] and seal['sealed_data_opened'] is False
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3');bucket=config['bucket'];prefix='research/v189-predicted-interval-source/ae0b160e0b48c419a6bac1b6d9e5964f7fd4b63b/runs/a0002';terminal_sha='ec2a845a4471afacf48ec8e78cb16fdafff75bb0852a7323e67d9ea9db2f8605'
terminal=s3.get_object(Bucket=bucket,Key=prefix+'/terminal.json')['Body'].read();assert hashlib.sha256(terminal).hexdigest()==terminal_sha;t=json.loads(terminal);assert t['exit_code']==0 and t['status']=='complete'
def artifact(name):
    b=s3.get_object(Bucket=bucket,Key=prefix+'/artifacts/'+name)['Body'].read();ident=t['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
features=artifact('out/features.jsonl');rows=[json.loads(s) for s in features.splitlines()];first,count=item['query_source_first'],item['query_source_count'];assert (first,count)==(100000,1000)
witnesses=[dict(prior_ordinal=r['ordinal'],source_id=r['source_id'],source_row=r['source_row'],prospective_ordinal=r['source_row']-first) for r in rows if first<=r['source_row']<first+count];assert witnesses==[dict(prior_ordinal=2578,source_id=83606072,source_row=100903,prospective_ordinal=903)]
commit=t.get('source_commit',t.get('source_base_commit'));source_sha=t['source_archive_sha256'];archive_key='research/v189-predicted-interval-source/'+commit+'/sources/'+source_sha+'.tar.gz';archive=s3.get_object(Bucket=bucket,Key=archive_key)['Body'].read();assert hashlib.sha256(archive).hexdigest()==source_sha
names=['scripts/launch_v189_predicted_interval_source_spot.py','scripts/v189_predicted_interval_source.py','scripts/v177_source_candidate_ceiling.py','scripts/v164_smooth_layout_1m.py','scripts/native_geometric_layout_screen.py'];code={}
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for name in names:code[name]=tar.extractfile(name).read()
source_ident=item['chunks'][0];assert source_ident['sha256']=='2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86' and source_ident['bytes']==1458450077
inputs=next(n.value for n in ast.parse(code[names[0]]).body if isinstance(n,ast.Assign) and any(isinstance(target,ast.Name) and target.id=='INPUTS' for target in n.targets))
old_source=ast.literal_eval(inputs.elts[0]);assert old_source[1:]==(source_ident['uri'].removeprefix('s3://'+bucket+'/'),source_ident['bytes'],source_ident['sha256'])
assert b'query = np.asarray(vectors[source_row], dtype=np.float32)' in code[names[1]] and b'source_ids, vectors, _ = source_arrays(args.source)' in code[names[2]]
assert b'stable_ids, vectors = _source_arrays(source_path, authority())' in code[names[3]]
helper=code[names[4]];start=helper.index(b'def _source_arrays(');end=helper.index(b'\ndef _stable_id(',start);mapping=helper[start:end];assert b'pq.read_table(path, columns=["feature_row_id", "embedding"])' in mapping and b'return stable_ids, vectors' in mapping and b'sort' not in mapping
report=dict(decision='KILL ReLAION prospective cohort novelty; construction/GT seal remains valid',dataset='ReLAION',index_rows=100000,prospective_source_first=first,prospective_queries=count,minimum_prior_query_overlap=len(witnesses),witnesses=witnesses,prior_dataset_rows=1000000,prior_split='V189 consumed source pseudoquery holdout ranks2561-2688',prior_terminal_key=prefix+'/terminal.json',prior_terminal_sha256=terminal_sha,prior_features=t['artifacts']['out/features.jsonl'],prior_source_archive_key=archive_key,prior_source_archive_sha256=source_sha,source_parquet_identity=source_ident,prior_source_code_sha256={name:hashlib.sha256(body).hexdigest() for name,body in code.items()},source_mapping_preserves_parquet_row_order=True,new_sealed_data_opened=False,ann_quality_measured=False,complete_prior_query_audit=False,qualification=False,action='Do not use this ReLAION panel as unused qualification evidence; do not remove the witness and silently relabel remaining999. Preserve seal. A replacement cohort must be entirely preregistered and audited before quality access. CoHere novelty remains uncertified.')
(out/'witness.json').write_text(json.dumps(report,indent=2)+'\n');(out/'prior-terminal.json').write_bytes(terminal);(out/'prior-prepare-seal.json').write_bytes(artifact('out/prepare-seal.json'));print(json.dumps(report))
