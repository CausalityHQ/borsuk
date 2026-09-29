"""Verify closed source/seal metadata and HEAD identities without opening sealed data."""
import gzip,hashlib,io,json,sys,tarfile
from pathlib import Path
import boto3
out=Path(sys.argv[1]);root=out.parents[1]
launch=json.loads((out/'aws-launch.json').read_text());raw=(out/'aws-terminal.json').read_bytes();terminal=json.loads(raw);close=json.loads((out/'aws-closeout.json').read_text());reservation=json.loads((out/'aws-reservation.json').read_text())
assert hashlib.sha256(raw).hexdigest()==(out/'aws-terminal.sha256').read_text().split()[0]
assert all(terminal[k]==launch[k] for k in ['source_archive_sha256','source_base_commit','instance_id']) and terminal['schema']=='borsuk-native-union-fresh-v1'
assert close['state']=='terminated' and close['instance_id']==launch['instance_id']
session=boto3.Session(profile_name='causality',region_name='eu-central-1');s3=session.client('s3')
assert session.client('ec2').describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]['State']['Name']=='terminated'
def get(name):
    b=gzip.decompress((out/(name+'.gz')).read_bytes());ident=terminal['artifacts'][name];assert len(b)==ident['bytes'] and hashlib.sha256(b).hexdigest()==ident['sha256'];return b
def obj(name):return json.loads(get(name))
for name in terminal['artifacts']:get(name)
archive=s3.get_object(Bucket=launch['bucket'],Key='research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')['Body'].read();assert hashlib.sha256(archive).hexdigest()==launch['source_archive_sha256']
matched=[];native=0
with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
    for member in tar.getmembers():
        path=Path(member.name);is_native=path.suffix=='.rs' or path.name in ['Cargo.toml','Cargo.lock']
        if is_native or member.name in [str(root/x) for x in ['fresh-config.json','fresh-preregister.md']]+['scripts/seal_native_union_fresh.py']:
            assert path.read_bytes()==tar.extractfile(member).read(),member.name;matched.append(member.name);native+=is_native
assert native==392
report=dict(instance_id=launch['instance_id'],state='terminated',source_archive_sha256=launch['source_archive_sha256'],terminal_sha256=hashlib.sha256(raw).hexdigest(),native_files_matched=native,source_files_matched=len(matched),artifacts_verified=len(terminal['artifacts']),qualification=False,sealed_data_opened=False)
if terminal['status']!='complete' or terminal['exit_code']!=0:
    report.update(valid_construction=False,phase=terminal['phase'],exit_code=terminal['exit_code']);(out/'invalid-verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));sys.exit(0)
config=json.loads((root/'fresh-config.json').read_text());assert hashlib.sha256((root/'fresh-config.json').read_bytes()).hexdigest()==reservation['config_sha256']
decision=obj('screen/decision.json');parities=[]
for item in config['items'][:len(decision['source_parity'])]:
    r=obj('screen/'+item['name']+'/source-parity.json');assert r['expected_source_raw_sha256']==item['source_raw_sha256'] and r['source_parity']==(r['source_raw_sha256']==item['source_raw_sha256']);assert r['query_count']==1000 and r['source_query_intervals_disjoint'];parities.append(r)
assert parities==decision['source_parity']
cohorts=[]
if decision['sealed']:
    assert len(parities)==2 and all(r['source_parity'] for r in parities) and decision['ann_quality_measured'] is False
    for item in config['items']:
        r=obj('screen/'+item['name']+'/cohort.json');assert r['source_raw_sha256']==item['source_raw_sha256'] and r['source_parity'] and r['oracle_scalar_check'] and r['query_source_first']==100000 and r['query_source_count']==1000 and r['ann_queries_or_scoring']==0 and r['quality_peek'] is False and r['exhaustive_oracle_queries']==1000
        assert r['oracle']==config['oracle'] and r['query_cohort']==config['query_cohort'] and 0<=r['minimum_100_101_cosine_margin']<=2 and 0<=r['gt_cutoff_exact_ties']<=1000
        assert r['artifacts']['queries.raw']['bytes']==3072000 and r['artifacts']['truth.u32']['bytes']==400000
        for name,ident in r['artifacts'].items():
            assert ident['key']==launch['prefix']+'/sealed/'+item['name']+'/'+name
            h=s3.head_object(Bucket=config['bucket'],Key=ident['key']);assert h['ContentLength']==ident['bytes'] and h['Metadata']['sha256']==ident['sha256']
        cohorts.append(r)
    assert cohorts==decision['cohorts']
else:assert decision['decision']=='INVALID prospective cohort source mismatch' and any(not r['source_parity'] for r in parities)
cgroup=obj('screen/cgroup.json');assert int(cgroup['memory.swap.peak'])==0 and all(int(s.split()[1])==0 for s in cgroup['memory.events'].splitlines() if s.split()[0] in ['oom','oom_kill'])
report.update(valid_construction=decision['sealed'],decision=decision['decision'],source_parity=parities,cohorts=cohorts,cgroup=cgroup,ann_quality_measured=False)
(out/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
