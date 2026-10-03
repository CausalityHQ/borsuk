from pathlib import Path
import json
from scripts import run_hierarchical_global_leaf_probe as p
root=Path.cwd();out=Path('docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k/global-leaf-probe/a0002/screen')
c=json.loads((out/'config.json').read_text());seal=json.loads((out/'paired-seal.json').read_text());e=p.original_evidence(root,c)
r=p.verify_execution(out,c,seal,e)
assert r['complete'] and r['truth_opened'] is False
print('PASS relative execution closure, all6stages/actualhost/cleanup/retainedroots; no GT')
