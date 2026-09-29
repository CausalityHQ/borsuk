import concurrent.futures,hashlib,io,json,re,tarfile
from pathlib import Path
import boto3
root=Path('/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/native-union-20260928')
items=json.loads((root/'fresh-history-terminal-inventory.json').read_text())['terminal_metadata']
selected=[r for r in items if any('/v'+str(n)+'-' in r['key'] for n in [151,152,154,272,278,279,281])]
s3=boto3.Session(profile_name='causality',region_name='eu-central-1').client('s3')
bucket='borsuk-bench-453182569524-euc1'
out=root/'fresh-history-source-inventory';out.mkdir(exist_ok=True)
def scan(row):
    campaign=row['key'].split('/')[1]
    saved=out/(campaign+'-'+row['source_commit'][:8]+'.json')
    if saved.exists(): return dict(campaign=campaign,already_authenticated=True,proof=str(saved))
    key='/'.join(['research',campaign,row['source_commit'],'sources',row['source_archive_sha256']+'.tar.gz'])
    if campaign.startswith(('v151-', 'v152-', 'v154-')):
        candidates=json.loads((out/'older-archive-locators.json').read_text())
        matches=[r for r in candidates if r['terminal_key']==row['key']]
        assert len(matches)==1 and len(matches[0]['candidate_metadata_locators'])==1
        key=matches[0]['candidate_metadata_locators'][0]['key']
    assert s3.head_object(Bucket=bucket,Key=key)['ContentLength'] <= 128*1024*1024
    archive=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
    assert hashlib.sha256(archive).hexdigest()==row['source_archive_sha256']
    number=campaign.split('-')[0]
    code={}
    with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.startswith('scripts/') and member.name.endswith('.py') and (Path(member.name).name.startswith(number+'_') or Path(member.name).name.startswith('launch_'+number+'_')):
                body=tar.extractfile(member).read()
                code[member.name]=dict(sha256=hashlib.sha256(body).hexdigest(),lines=[dict(line=i,text=line) for i,line in enumerate(body.decode().splitlines(),1) if re.search(r'quer|fresh|train|parquet|source|\.f32|\.raw',line,re.I)])
    result=dict(terminal_key=row['key'],terminal_sha256=row['sha256'],terminal_status=row['status'],archive_key=key,archive_sha256=row['source_archive_sha256'],source_code=code,complete_prior_query_audit=False,query_gt_or_sealed_bodies_opened=False)
    path=out/(campaign+'-'+row['source_commit'][:8]+'.json');path.write_text(json.dumps(result,indent=2)+'\n')
    return dict(campaign=campaign,source_files=len(code),proof=str(path))
results=[]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    jobs=[(row,pool.submit(scan,row)) for row in selected]
    for row,job in jobs:
        try: value=job.result()
        except Exception as error: value=dict(campaign=row['key'].split('/')[1],source_commit=row['source_commit'],authenticated=False,error=type(error).__name__+': '+str(error))
        results.append(value);print(json.dumps(value),flush=True)
(out/'collection-status.json').write_text(json.dumps(results,indent=2)+'\n')
