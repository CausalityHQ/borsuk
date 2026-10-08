import json,pathlib,subprocess,hashlib
r=pathlib.Path('/mnt/borsuk-http/evidence');xs=[]
for line in (r/'probe-compile.log').read_text().splitlines():
 try:x=json.loads(line)
 except ValueError:continue
 if isinstance(x,dict) and x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')=='borsuk' and x['target'].get('kind')==['lib'] and x.get('profile',{}).get('test') is True and x.get('executable'):xs.append(x)
assert len(xs)==1,'release libtest missing/ambiguous'
p=pathlib.Path(xs[0]['executable']);h=hashlib.sha256()
with p.open('rb') as f:
 while b:=f.read(1048576):h.update(b)
nm=subprocess.check_output(['nm','-C',str(p)],text=True);(r/'probe-nm.txt').write_text(nm)
rows=[line for line in nm.splitlines() if len(line.split(maxsplit=2)) == 3 and line.split(maxsplit=2)[-1] in ('borsuk::rotated_two_bit::PreparedTwoBit::gather_dot', '<borsuk::rotated_two_bit::PreparedTwoBit>::gather_dot')];assert len(rows)==1,'gather kernel missing/ambiguous'
a=int(rows[0].split()[0],16);dis=subprocess.check_output(['objdump','-d',f'--start-address={a}',f'--stop-address={a+8192}',str(p)],text=True);(r/'gather-disassembly.txt').write_text(dis)
assert 'vgatherdpd' in dis,'selected release kernel lacks AVX2 gather'
(r/'probe-executable.txt').write_text(str(p)+'\n');(r/'probe-artifact.json').write_text(json.dumps({'status':'RELEASE_COMPILED_CODEGEN_AUDIT_PENDING','path':str(p),'bytes':p.stat().st_size,'sha256':h.hexdigest(),'cargo_artifact':xs[0],'gather_instruction_present':True,'ordered_sum_codegen_qualified':False,'performance_claim':False},indent=2)+'\n')
