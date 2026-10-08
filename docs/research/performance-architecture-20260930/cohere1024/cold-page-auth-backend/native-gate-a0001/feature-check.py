import json,pathlib
r=pathlib.Path('/mnt/borsuk-http/evidence')
def metadata(name):
 objs=[]
 for line in (r/(name+'.log')).read_text().splitlines():
  try: x=json.loads(line)
  except ValueError: continue
  if isinstance(x,dict) and 'packages' in x and 'resolve' in x: objs.append(x)
 assert len(objs)==1
 x=objs[0]; pk={p['id']:p for p in x['packages']}
 return {(p['name'],p['version'],p['source']):sorted(n['features']) for n in x['resolve']['nodes'] if (p:=pk[n['id']])['source'] is not None}
a,b=metadata('baseline-metadata'),metadata('candidate-metadata')
assert a==b, 'resolved external packages/features changed'
(r/'feature-admission.json').write_text(json.dumps({'status':'PASS','external_package_count':len(a),'same_packages_and_features':True})+'\n')
