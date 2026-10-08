import json,pathlib,subprocess,hashlib
r=pathlib.Path('/mnt/borsuk-http/evidence'); xs=[]
for line in (r/'release-probe-compile.log').read_text().splitlines():
 try:x=json.loads(line)
 except ValueError:continue
 if isinstance(x,dict) and x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')=='borsuk' and x['target'].get('kind')==['lib'] and x.get('profile',{}).get('test') is True and x.get('executable'):xs.append(x)
assert len(xs)==1, 'exact release libtest artifact missing/ambiguous'
p=pathlib.Path(xs[0]['executable']); h=hashlib.sha256()
with p.open('rb') as f:
 while b:=f.read(1048576):h.update(b)
nm=subprocess.check_output(['nm','-S','-C',str(p)],text=True); (r/'probe-nm.txt').write_text(nm)
rows=[s for s in nm.splitlines() if s.endswith(' sha2::sha256::x86::digest_blocks')];assert len(rows)==1
address=int(rows[0].split()[0],16); size=int(rows[0].split()[1],16); assert size>0
assert xs[0]['profile'].get('opt_level')=='3' and xs[0]['profile'].get('debug_assertions') is False
dis=subprocess.check_output(['objdump','-d',f'--start-address={address}',f'--stop-address={address+size}',str(p)],text=True)
assert 'sha256rnds2' in dis
assert 'aws_lc_0_45_0_sha256_block_data_order_hw' in nm
(r/'probe-disassembly.txt').write_text(dis)
(r/'probe-executable.txt').write_text(str(p)+'\n')
(r/'probe-artifact.json').write_text(json.dumps({'status':'PASS','path':str(p),'bytes':p.stat().st_size,'sha256':h.hexdigest(),'cargo_artifact':xs[0],'sha_instructions_present':True,'aws_lc_hardware_symbol_present':True},indent=2)+'\n')
