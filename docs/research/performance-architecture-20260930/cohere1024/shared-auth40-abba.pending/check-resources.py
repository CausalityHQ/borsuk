from pathlib import Path
import json,sys
p=Path(__file__).parent
def validate(rows,stage,arm):
 authority=json.loads((p/'resource-authority.json').read_bytes())
 expected=json.loads((p/('frozen-headers/'+arm+'.json' if stage=='full' else 'gate-headers/'+stage+'.'+arm+'.json')).read_bytes())
 seen=set()
 for row in rows:
  phase=row.get('phase')
  if phase in ('identity','bound_inputs'):
   assert row==expected['expected_'+phase], phase
   seen.add(phase)
  elif phase=='startup':
   assert {k:row[k] for k in authority['startup'][stage]}==authority['startup'][stage]
   assert row['source_authentication_extra_library_bytes']==authority['extra_library_bytes'][arm]
   assert row['library_cap_bytes']==2147483648
   seen.add(phase)
  elif phase=='diagnostic_admission':
   assert row['source_authentication_extra_library_bytes']==authority['extra_library_bytes'][arm]
   expected_admission=dict(authority['diagnostic_admission'][stage],source_authentication_extra_library_bytes=authority['extra_library_bytes'][arm])
   assert row==expected_admission
   seen.add(phase)
 assert seen==({'identity','bound_inputs','startup'} | ({'diagnostic_admission'} if stage!='full' else set()))
if __name__=='__main__':
 with open(sys.argv[1]) as f:validate((json.loads(line) for line in f),sys.argv[2],sys.argv[3])
