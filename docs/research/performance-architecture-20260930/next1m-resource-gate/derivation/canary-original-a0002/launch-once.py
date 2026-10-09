import hashlib,json,os,pathlib,re,subprocess,time
root=pathlib.Path('/data/target/borsuk-cold-membership-native/scale-maintenance-qualification-a0002/next1m-derivation-draft/canary-original-a0002')
a=json.loads((root/'launch-admission.json').read_bytes())
r=json.loads((root/'launch-request.json').read_bytes())
env=dict(os.environ,AWS_MAX_ATTEMPTS='1',AWS_RETRY_MODE='standard',AWS_PAGER='')
instance=None;watch_started=False
def aws(name,args):
    cmd=['aws','--profile','causality','--region','eu-central-1','--cli-connect-timeout','5','--cli-read-timeout','15',*args]
    (root/(name+'.command.json')).write_text(json.dumps(cmd)+'\n')
    result=subprocess.run(cmd,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25)
    (root/(name+'.stdout')).write_bytes(result.stdout);(root/(name+'.stderr')).write_bytes(result.stderr)
    (root/(name+'.exit')).write_text(str(result.returncode)+'\n')
    return result
def checked(name,args):
    x=aws(name,args)
    if x.returncode:raise RuntimeError(name+' failed')
    return json.loads(x.stdout)
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
    info=checked('describe-original',['ec2','describe-instances','--instance-ids',instance])['Reservations'][0]['Instances'][0]
    assert info['InstanceId']==instance and info['ClientToken']==a['client_token']
    mappings=info['BlockDeviceMappings'];assert len(mappings)==2
    volumes=[x['Ebs']['VolumeId'] for x in mappings];assert len(set(volumes))==2
    assert all(x['Ebs']['DeleteOnTermination'] for x in mappings)
    scratch=next(x['Ebs']['VolumeId'] for x in mappings if x['DeviceName']=='/dev/sdf')
    (root/'volume-ids.json').write_text(json.dumps(volumes)+'\n')
    binding={'schema':'borsuk-scratch-launch-binding-v1','instance_id':instance,'volume_id':scratch,'device':'/dev/sdf','size_bytes':42949672960}
    raw=(json.dumps(binding,separators=(',',':'))+'\n').encode()
    (root/'scratch-launch-binding.json').write_bytes(raw)
    (root/'scratch-launch-binding.json.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  scratch-launch-binding.json\n')
    for name in ['scratch-launch-binding.json','scratch-launch-binding.json.sha256']:
        checked('put-'+name,['s3api','put-object','--bucket','borsuk-bench-453182569524-euc1','--key','research/semantic-router/20261009/actual1m-scale-chain-canary-a0002/inputs/'+name,'--body',str(root/name),'--if-none-match','*'])
    cmd=['systemd-run','--user','--unit=borsuk-next1m-canary-a0002-watch','-p','CPUQuota=100%','-p','AllowedCPUs=0','-p','MemoryMax=256M','-p','MemorySwapMax=0','-p','TasksMax=128','-p','RuntimeMaxSec=3900s','-p','Environment=AWS_MAX_ATTEMPTS=1','-p','Environment=AWS_PAGER=','bash',str(root/'watch-original.sh'),instance,str(a['started_epoch']),a['user_data_sha256']]
    subprocess.run(cmd,check=True,timeout=10)
    watch_started=True
    (root/'launch-root-result.json').write_text(json.dumps({'instance_id':instance,'volumes':volumes,'watcher_unit':'borsuk-next1m-canary-a0002-watch.service','watcher_started':True,'performance_claim':False},indent=2)+'\n')
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
