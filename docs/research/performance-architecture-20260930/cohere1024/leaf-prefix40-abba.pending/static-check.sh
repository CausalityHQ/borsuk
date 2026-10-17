#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
for script in ./*.sh; do bash -n "$script"; done
/mnt/borsuk-workspace/tools/shellcheck -S warning -e SC1091 ./*.sh
python3 - <<'CHECK'
import ast,json,pathlib,hashlib
p=pathlib.Path('.')
for f in p.glob('*.py'):ast.parse(f.read_text())
for f in p.rglob('*.json'):json.loads(f.read_bytes())
configs=json.loads((p/'config-pins.unselected.json').read_bytes())['configs']
for row in configs:
 b=(p/'configs'/row['name']).read_bytes()
 assert len(b)==row['bytes'] and hashlib.sha256(b).hexdigest()==row['sha256']
for stage in ('admission','smoke','full'):
 a=json.loads((p/'configs'/f'{stage}.leaf48.json').read_bytes())
 b=json.loads((p/'configs'/f'{stage}.leaf40.json').read_bytes())
 assert a.pop('semantic_leaf_limit')==48 and b.pop('semantic_leaf_limit')==40 and a==b
 assert a['count']==1000 and a['fetch_parallelism']==64 and a['s3_read_retry']=='one_retry_100ms'
 if stage=='smoke':assert a['execution']['ordinals']==[32,161]
 if stage=='full':assert a['execution']=={'mode':'full'}
 for k in ('requests','truth','cohort_receipt','derivation_receipt'):
  q=pathlib.Path(a[k]['path']); assert q.stat().st_size==a[k]['bytes'] and hashlib.sha256(q.read_bytes()).hexdigest()==a[k]['sha256']
for leaf in (40,48):
 h=json.loads((p/'frozen-headers'/f'leaf{leaf}.json').read_bytes())
 assert h['expected_identity']['config_sha256']==hashlib.sha256((p/'configs'/f'full.leaf{leaf}.json').read_bytes()).hexdigest()
 assert h['expected_bound_inputs']['selected_count']==1000 and h['expected_bound_inputs']['source_cache']=='off'
 c=json.loads((p/'canonical'/f'leaf{leaf}.json').read_bytes())
 assert set(map(int,c['queries']))==set(map(int,c['hits']))==set(range(1000))
 assert sum(c['hits'].values())==c['recall_numerator']
old=json.loads(pathlib.Path('/mnt/borsuk-workspace/evidence/leaf-prefix-abba-preparation-a0001/header-authority.json').read_bytes())
assert json.loads((p/'header-authority.json').read_bytes())==old
print('PASS: Bash syntax, ShellCheck, Python AST, JSON, one causal config change, actual local input hashes, full headers, canonical counts and source authority')
CHECK
