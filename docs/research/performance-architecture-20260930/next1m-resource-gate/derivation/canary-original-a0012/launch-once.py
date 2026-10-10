import datetime,hashlib,json,os,pathlib,re,subprocess,time
# Prospective frozen config: the root fills exactly these three values at freeze time (no actual request, epoch or job is baked in here).
LAUNCH_DIR='/data/target/borsuk-cold-membership-native/scale-maintenance-qualification-a0002/next1m-derivation-draft/canary-original-a0012';RUN_PREFIX='research/semantic-router/20261010/actual1m-scale-chain-canary-a0012';WATCH_UNIT='borsuk-next1m-canary-a0012-watch'
assert 'PENDING' not in LAUNCH_DIR+RUN_PREFIX+WATCH_UNIT and re.fullmatch(r'research/semantic-router/[0-9]{8}/[a-z0-9-]+',RUN_PREFIX) and re.fullmatch(r'borsuk-next1m-canary-a[0-9]{4}-watch',WATCH_UNIT)
root=pathlib.Path(LAUNCH_DIR)
a=json.loads((root/'launch-admission.json').read_bytes())
r=json.loads((root/'launch-request.json').read_bytes())
env=dict(os.environ,AWS_MAX_ATTEMPTS='1',AWS_RETRY_MODE='standard',AWS_PAGER='')
instance=None;watch_started=False
def aws(name,args,limit=25,keep=False):
    cmd=['aws','--profile','causality','--region','eu-central-1','--cli-connect-timeout','5','--cli-read-timeout',str(max(1,min(15,int(limit)))),*args]
    (root/(name+'.command.json')).write_text(json.dumps(cmd)+'\n')
    try:
        result=subprocess.run(cmd,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=min(25,limit))
    except subprocess.TimeoutExpired as e:
        if keep:
            # observation calls only: retain the capped partial output and an explicit timeout outcome under the SAME name, then propagate (no retry)
            (root/(name+'.stdout')).write_bytes((e.stdout or b'')[:65536]);(root/(name+'.stderr')).write_bytes((e.stderr or b'')[:65536]);(root/(name+'.exit')).write_text('TIMEOUT\n')
        raise
    (root/(name+'.stdout')).write_bytes(result.stdout);(root/(name+'.stderr')).write_bytes(result.stderr)
    (root/(name+'.exit')).write_text(str(result.returncode)+'\n')
    return result
def checked(name,args):
    x=aws(name,args)
    if x.returncode:raise RuntimeError(name+' failed')
    return json.loads(x.stdout)
def epoch(t):
    # timezone-aware ISO 8601 only; naive, malformed, non-string or nonpositive timestamps are refused
    assert type(t) is str
    d=datetime.datetime.fromisoformat(t);assert d.tzinfo is not None and d.utcoffset() is not None
    e=int(d.timestamp());assert e>0
    return e
def scratch_proof(started,zone,instance,info,described,now):
    # Pure ownership proof for the scratch volume; every missing, malformed, mistyped or stale fact raises AssertionError (no defaults, no truthiness).
    mappings=info['BlockDeviceMappings'];assert type(mappings) is list and len(mappings)==2
    ebs={x['DeviceName']:x['Ebs'] for x in mappings};assert set(ebs)=={'/dev/sda1','/dev/sdf'}
    assert all(e['DeleteOnTermination'] is True for e in ebs.values())
    scratch,rootvol=ebs['/dev/sdf']['VolumeId'],ebs['/dev/sda1']['VolumeId']
    assert scratch!=rootvol and info['InstanceId']==instance and info['Placement']['AvailabilityZone']==zone
    volumes=described['Volumes'];assert type(volumes) is list and len(volumes)==1;v=volumes[0]
    assert v['VolumeId']==scratch and type(v.get('SnapshotId')) is str and v['SnapshotId']=='' and type(v['Size']) is int and v['Size']==40
    assert v['VolumeType']=='gp3' and v['Encrypted'] is True and v.get('MultiAttachEnabled') is False and v['AvailabilityZone']==zone and v['State']=='in-use'
    att=v['Attachments'];assert type(att) is list and len(att)==1 and att[0]['InstanceId']==instance and att[0]['Device']=='/dev/sdf' and att[0]['State']=='attached' and att[0]['VolumeId']==scratch
    launched,created,attached=epoch(info['LaunchTime']),epoch(v['CreateTime']),epoch(att[0]['AttachTime'])
    assert started-60<=launched<=min(now+60,started+240)
    assert launched-60<=created<=launched+60 and created-5<=attached<=launched+120
    return {'scratch':scratch,'root':rootvol,'launched':launched,'created':created,'attached':attached}
def settle(observe,volume,instance,clock,sleep,window=30,spacing=2,limit=12):
    # Finite observation of ONLY the scratch attachment: one DescribeVolumes per observation (an AWS error is never retried), every call clamped to the time left,
    # each raw body distinct and <=64 KiB. 'attaching' is transient and is never accepted as final; anything malformed or foreign is refused immediately.
    start=clock()
    for n in range(1,limit+1):
        left=window-(clock()-start);assert left>=3
        rc,body=observe(n,min(25,left));assert clock()-start<=window   # a call that finished after the window can never succeed
        assert rc==0 and type(body) is bytes and 0<len(body)<=65536
        volumes=json.loads(body)['Volumes'];assert type(volumes) is list and len(volumes)==1 and volumes[0]['VolumeId']==volume
        att=volumes[0]['Attachments'];assert type(att) is list and len(att)==1 and att[0]['InstanceId']==instance and att[0]['Device']=='/dev/sdf' and att[0]['VolumeId']==volume
        if att[0]['State']=='attached':
            assert clock()-start<=window   # and re-checked after parsing, immediately before success
            return n,body,json.loads(body)
        assert att[0]['State']=='attaching'
        sleep(min(spacing,max(0,window-(clock()-start)-3)))
    raise AssertionError('scratch attachment did not settle')
def settle_instance(observe,instance,token,clock,sleep,window=30,spacing=2,limit=12):
    # Finite observation of ONLY this instance (same id and client token) until BOTH expected mappings exist. A pending instance may still be missing one of them (transient);
    # a foreign or duplicate device, wrong identity, token or state, a malformed shape or an AWS error is refused at once. Timing rules exactly as in settle().
    start=clock()
    for n in range(1,limit+1):
        left=window-(clock()-start);assert left>=3
        rc,body=observe(n,min(25,left));assert clock()-start<=window   # a call that finished after the window can never succeed
        assert rc==0 and type(body) is bytes and 0<len(body)<=65536
        doc=json.loads(body);res=doc['Reservations'];assert type(res) is list and len(res)==1
        found=res[0]['Instances'];assert type(found) is list and len(found)==1;info=found[0]
        assert info['InstanceId']==instance and info['ClientToken']==token and info['State']['Name'] in ('pending','running')
        maps=info['BlockDeviceMappings'];assert type(maps) is list
        names=[m['DeviceName'] for m in maps]
        assert all(type(x) is str for x in names) and len(set(names))==len(names) and set(names)<={'/dev/sda1','/dev/sdf'}
        assert all(type(m['Ebs']) is dict and type(m['Ebs']['VolumeId']) is str and re.fullmatch(r'vol-[0-9a-f]{8,17}',m['Ebs']['VolumeId']) for m in maps)
        if set(names)=={'/dev/sda1','/dev/sdf'}:
            assert clock()-start<=window   # and re-checked immediately before success
            return n,body,doc
        assert info['State']['Name']=='pending'
        sleep(min(spacing,max(0,window-(clock()-start)-3)))
    raise AssertionError('instance block device mappings did not settle')
assert time.time()-a['started_epoch']<180
assert hashlib.sha256((root/'user-data.sh').read_bytes()).hexdigest()==a['user_data_sha256']
assert hashlib.sha256((root/'watch-original.sh').read_bytes()).hexdigest()==a['watcher_sha256']
assert a['client_token']==r['ClientToken']
with (root/'launch-once.claim').open('x') as f:f.write(str(time.time())+'\n')
try:
    dry=aws('dry-run',['ec2','run-instances','--cli-input-json','file://'+str(root/'launch-request.json'),'--dry-run'])
    assert dry.returncode!=0 and b'(DryRunOperation)' in dry.stderr
    original=checked('run-instances',['ec2','run-instances','--cli-input-json','file://'+str(root/'launch-request.json')])
    assert len(original['Instances'])==1
    instance=original['Instances'][0]['InstanceId'];assert re.fullmatch(r'i-[0-9a-f]+',instance)
    (root/'instance-id').write_text(instance+'\n')
    seen_instance,raw_instances,seen_doc=settle_instance(lambda i,left:(lambda x:(x.returncode,x.stdout))(aws('describe-original-%02d'%i,['ec2','describe-instances','--instance-ids',instance],left,True)),instance,a['client_token'],time.monotonic,time.sleep)
    instance_evidence='describe-original-%02d.stdout'%seen_instance;assert (root/instance_evidence).read_bytes()==raw_instances
    info=seen_doc['Reservations'][0]['Instances'][0]
    mappings=info['BlockDeviceMappings'];assert len(mappings)==2
    volumes=[x['Ebs']['VolumeId'] for x in mappings];assert len(set(volumes))==2
    assert all(x['Ebs']['DeleteOnTermination'] is True for x in mappings)
    scratch=next(x['Ebs']['VolumeId'] for x in mappings if x['DeviceName']=='/dev/sdf')
    (root/'volume-ids.json').write_text(json.dumps(volumes)+'\n')
    # ---- API gate BEFORE the binding is published (single attempt, no retry loop; any failure leaves no binding and the cleanup below terminates the instance)
    zone=r['Placement']['AvailabilityZone']
    seen,raw_volumes,described=settle(lambda i,left:(lambda x:(x.returncode,x.stdout))(aws('describe-scratch-volume-%02d'%i,['ec2','describe-volumes','--volume-ids',scratch],left,True)),scratch,instance,time.monotonic,time.sleep)
    final_evidence='describe-scratch-volume-%02d.stdout'%seen;assert (root/final_evidence).read_bytes()==raw_volumes
    proof=scratch_proof(a['started_epoch'],zone,instance,info,described,int(time.time()));assert proof['scratch']==scratch
    binding={'schema':'borsuk-scratch-launch-binding-v2','instance_id':instance,'volume_id':scratch,'root_volume_id':proof['root'],'device':'/dev/sdf','size_bytes':42949672960,'availability_zone':zone,
             'volume_type':'gp3','encrypted':True,'multi_attach':False,'snapshot_empty':True,'state':'in-use','attached_device':'/dev/sdf','delete_on_termination':True,
             'create_time_epoch':proof['created'],'attach_time_epoch':proof['attached'],'launch_time_epoch':proof['launched'],
             'describe_instances_sha256':hashlib.sha256(raw_instances).hexdigest(),'describe_volumes_sha256':hashlib.sha256(raw_volumes).hexdigest()}
    raw=(json.dumps(binding,sort_keys=True,separators=(',',':'))+'\n').encode();assert len(raw)<=4096
    (root/'scratch-launch-binding.json').write_bytes(raw)
    (root/'scratch-launch-binding.json.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  scratch-launch-binding.json\n')
    for name in ['scratch-launch-binding.json','scratch-launch-binding.json.sha256']:
        checked('put-'+name,['s3api','put-object','--bucket','borsuk-bench-453182569524-euc1','--key',RUN_PREFIX+'/inputs/'+name,'--body',str(root/name),'--if-none-match','*'])
    cmd=['systemd-run','--user','--unit='+WATCH_UNIT,'-p','CPUQuota=100%','-p','AllowedCPUs=0','-p','MemoryMax=256M','-p','MemorySwapMax=0','-p','TasksMax=128','-p','RuntimeMaxSec=3900s','-p','Environment=AWS_MAX_ATTEMPTS=1','-p','Environment=AWS_PAGER=','bash',str(root/'watch-original.sh'),instance,str(a['started_epoch']),a['user_data_sha256']]
    subprocess.run(cmd,check=True,timeout=10)
    watch_started=True
    (root/'launch-root-result.json').write_text(json.dumps({'instance_id':instance,'volumes':volumes,'watcher_unit':WATCH_UNIT+'.service','watcher_started':True,'instance_final_evidence':instance_evidence,'scratch_volume_final_evidence':final_evidence,'performance_claim':False},indent=2)+'\n')
    print(instance)
except BaseException:
    if instance is None:
        lookup=checked('ambiguous-token-lookup',['ec2','describe-instances','--filters','Name=client-token,Values='+a['client_token']])
        found=[i for reservation in lookup['Reservations'] for i in reservation['Instances']]
        assert len(found)<=1
        if found:instance=found[0]['InstanceId']
    if instance and not watch_started:
        checked('emergency-terminate',['ec2','terminate-instances','--instance-ids',instance])
    raise
