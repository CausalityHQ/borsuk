#!/usr/bin/env python3
"""Root-only metadata preparation; never opens payloads or launches native code."""
import copy, hashlib, json
from pathlib import Path
E0=Path('/mnt/borsuk-workspace/evidence/fp32-concurrency-screen-root-a0001')
E=Path('/mnt/borsuk-workspace/evidence/fp32-concurrent-full1000-root-a0001')
P=Path(__file__).resolve().parent
NAMES=['admit-A','admit-B','canary-A','canary-B','A1','B1','B2','A2']
MEASURED=NAMES[4:]
RUNNER='afcbbfbd616d7c35aa29beb71968435f4b0e6142c526f11051796ccb21076648'
READER='cea18d35d66862cc4723d3c4b25aa8abe5bb2bc87e6577c5f2f9a6941d0fb545'
PINS={
 'qualified-native-authority.json':'dd1e8fcbb7e94e06094b42605aad18ceb1d76b3049e1eab70c9c2286bc5c0e10',
 'run-screen-v2.py':'c2fe5e2ca591444dd0f2ab111855f27438f556d51a1dbf6768506a72c2811809',
 'run-controller-v2.sh':'742252eb108811feb1b0b4e49889ee3527b7746059bbd1a8608943f9fadc2ac8',
 'oracle-reused.py':'6a8f9e381ff11c6c9a1e6a6f40987cee155d7d9b8dcce36894194f2f4e807f2a',
 'Q1-config.json':'1e9fe42fb0c0eea74940c7d2db5552cb442b8f0b20b7a596d8a32f45549c1990',
 'Q2-config.json':'3480e00cd1cdd9fe5a239eec6ba608f4203ec74a33fdfd2abb7d4b00acbe0990',
 'Q1-expected.UNBOUND.json':'22d4c5b344b6d49c92e5000f6bd43e03ea80d7d3a586f105744ebb227fa36118',
 'Q2-expected.UNBOUND.json':'5226a4ea493a060deeb2e299387be8abb41ac749315652198656e4a96a56ca4d'}
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encode(value):return (json.dumps(value,sort_keys=True,indent=2)+'\n').encode()
def active(name):return 1 if name.endswith('-A') or name.startswith('A') else 2
def count(name):return 8 if name.startswith('admit-') else 2 if name.startswith('canary-') else 1000
def retained():
 for name,pin in PINS.items():assert sha((E0/name).read_bytes())==pin,name
 a=json.loads((E0/'qualified-native-authority.json').read_bytes())
 assert a['qualification_complete'] is True and a['all_owned_compiler_processes_terminal'] is True
 assert a['runner_sha256']==RUNNER and a['reader_sha256']==READER
 assert a['commit']=='0cddd79569db2f92bf2079f1d9bbc3b90759a895'
 return a
def root_authority():
 original=retained()
 a=json.loads((E/'qualified-native-authority.json').read_bytes())
 assert a['instance_id']=='i-09f8571d0edf296e9'
 assert a['main_commit']=='afab20a8c2f58d326c4502065a80e61f2a9be680'
 assert a['main_crates_tree']=='ab0730bfc49fe9587705275a464cb54f3813c1d2'
 for key,value in original.items():
  if key!='instance_id':assert a[key]==value,key
 return a
def make_cell(name,a):
 assert name in NAMES
 n=count(name);q=active(name)
 c=json.loads((E0/f'Q{q}-config.json').read_bytes())
 c['execution']={'mode':'full'} if n==1000 else {'mode':'diagnostic_panel','ordinals':list(range(n)),'trace':False}
 c['scratch_parent']=str(E/('scratch-'+name))
 raw=encode(c)
 ex=json.loads((E0/f'Q{q}-expected.UNBOUND.json').read_bytes())
 assert ex['identity']['binary_sha256'] is None and ex['identity']['runner_source_sha256'] is None
 ex['identity'].update(binary_sha256=a['runner_sha256'],runner_source_sha256=a['runner_source_sha256'],config_sha256=sha(raw),execution=copy.deepcopy(c['execution']))
 ex['bound_inputs'].update(execution=copy.deepcopy(c['execution']),selected_count=n)
 return raw,encode(ex)
def files_to_create():
 a=root_authority()
 authority=json.loads((E/'gold-authority.json').read_bytes())
 assert authority['schema']=='borsuk-fp32-full1000-gold-authority-v1'
 assert authority['gold_path']==str(E/'gold-full1000.json') and authority['records']==1000 and authority['ordinals']=='0..999'
 assert authority['source_sha256']=='69b56a0464d9db152625b32051438fdc644785f6dc18564f68e0980a06e6b8b1'
 assert authority['gold_sha256']=='955ef2319cf831710048fbec2697ad2fa7f1c399358a5b0fea817e5977abe5b0'
 assert authority['source_completed_measured'] is True
 files={'oracle-reused.py':(E0/'oracle-reused.py').read_bytes()}
 for name in NAMES:
  c,ex=make_cell(name,a);files[name+'-config.json']=c;files[name+'-expected.json']=ex
  assert not (E/(name+'.jsonl')).exists() and not (E/('scratch-'+name)).exists()
 for name in ['prepare.py','run-full1000.py','run-controller.sh','launch.sh','README.md','selfcheck.py']:
  files[name]=(P/name).read_bytes()
 files['preparation.json']=encode({'schema':'borsuk-fp32-concurrent-full1000-preparation-v1','retained_sha256':PINS,'files_sha256':{n:sha(b) for n,b in files.items()},'gold_authority_sha256':sha((E/'gold-authority.json').read_bytes()),'native_authority_sha256':sha((E/'qualified-native-authority.json').read_bytes()),'payloads_opened':False})
 return files
def prepare():
 files=files_to_create()
 assert all(not (E/name).exists() for name in files),'fresh destination required; no overwrite/resume'
 for name,raw in files.items():
  with (E/name).open('xb') as f:f.write(raw)
 print('PREPARED_METADATA_ONLY',len(files))
if __name__=='__main__':prepare()
