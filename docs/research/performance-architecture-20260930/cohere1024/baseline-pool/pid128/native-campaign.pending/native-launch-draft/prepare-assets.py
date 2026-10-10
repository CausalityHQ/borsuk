"""Opaque source preparation only; no experiment entrypoint or AWS call."""
import hashlib,json,pathlib,subprocess,sys,uuid
assert len(sys.argv)==3
revision,dest=sys.argv[1:]
assert len(revision)==40 and all(c in '0123456789abcdef' for c in revision)
out=pathlib.Path(dest); assert str(out).startswith('/tmp/borsuk-pid128-native-ec2-') and not out.exists()
prefix='docs/research/performance-architecture-20260930/cohere1024/baseline-pool/pid128/native-campaign.pending/'
def blob(name):return subprocess.check_output(['git','show',revision+':'+prefix+name])
out.mkdir(mode=0o700);(out/'assets').mkdir()
roster=json.loads(blob('native-launch-draft/support-roster.json'))
assert len(roster)==19
for row in roster:
 name=row['name']; assert '/' not in name
 if name=='transport-native-inputs.sh':folder='native-launch-draft/'
 elif name.endswith('.json'):folder='native-input-draft/'
 else:folder='review-repair-r7/'
 body=blob(folder+name)
 assert len(body)==row['bytes'] and hashlib.sha256(body).hexdigest()==row['sha256']
 (out/'assets'/name).write_bytes(body)
for name in ['native-worker.sh','native-watch.sh','request.pending.json','resource-plan.pending.json','spot-quote.json']:
 (out/name).write_bytes(blob('native-launch-draft/'+name))
(out/'support-roster.json').write_text(json.dumps(roster,indent=2)+'\n')
tag='borsuk-pid128-native-a0001-'+uuid.uuid4().hex[:16]
root='research/semantic-router/20261010/'+tag
(out/'asset-plan.json').write_text(json.dumps({'source_revision':revision,'client_token':tag,'prefix':root+'/run','assets':root+'/assets','watch_unit':tag+'.service','launch_authority':False},indent=2)+'\n')
print('Prepared pinned source assets only:',out)
