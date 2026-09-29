import concurrent.futures,hashlib,json,re
from pathlib import Path
import boto3
root=Path('/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928')
manifest=json.loads((root/'fresh-history-prefix-inventory.json').read_text())
s3=boto3.Session(profile_name='causality',region_name='eu-central-1').client('s3')
bucket=manifest['bucket']
def scan(prefix):
    keys=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix):
        keys.extend(r['Key'] for r in page.get('Contents',[]) if r['Key'].rsplit('/',1)[-1] in
                    ['terminal.json','ATTEMPT_COMPLETE.json','ATTEMPT_FAILED.json','attempt-complete.json','complete.json'])
    rows=[]
    for key in keys:
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
        value=json.loads(body)
        artifacts=value.get('artifacts',{})
        relevant={name:ident for name,ident in artifacts.items() if re.search(r'query|queries|request|feature|prepare|population|seal',name,re.I)} if isinstance(artifacts,dict) else {}
        rows.append(dict(key=key,sha256=hashlib.sha256(body).hexdigest(),bytes=len(body),
                         schema=value.get('schema',value.get('format')),status=value.get('status'),exit_code=value.get('exit_code'),
                         source_archive_sha256=value.get('source_archive_sha256'),
                         source_commit=value.get('source_commit',value.get('source_base_commit')),
                         relevant_artifact_identities=relevant))
    return rows
rows=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    for result in pool.map(scan,manifest['versioned_prefixes']):rows.extend(result)
(root/'fresh-history-terminal-inventory.json').write_text(json.dumps(dict(metadata_only=True,bucket=bucket,versioned_prefixes_scanned=len(manifest['versioned_prefixes']),terminal_metadata_count=len(rows),terminal_metadata=rows,complete_prior_query_audit=False,query_gt_or_sealed_bodies_opened=False,scope='Versioned campaign terminal identity and named metadata locators only; archive-bound lineage still required; nonversioned campaign inventory still pending'),indent=2)+'\n')
print(json.dumps(dict(prefixes=len(manifest['versioned_prefixes']),terminals=len(rows),with_query_locators=sum(bool(r['relevant_artifact_identities']) for r in rows))))
