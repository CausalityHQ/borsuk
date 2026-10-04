import hashlib
import json
import sys
import time
from pathlib import Path

repo = Path('/home/rb/worktrees/borsuk-prod-ready-v9')
sys.path.insert(0, str(repo))
from scripts import prepare_hierarchical_cells_100k as helper

started = time.monotonic()
base = Path('docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells')
root = repo/base/'source-witness-router'
authority = json.loads((root/'retained-input-authority.json').read_text())
retained = Path('/home/rb/worktrees/borsuk-partitioner-paired-science-a0001-frozen')/base/'capacity-constrained-partitioner/paired100k/a0001/screen/retained'
results = {}
for dataset in ('relaion', 'cohere'):
    expected = authority['datasets'][dataset]['requests64']
    pin = dict(path=str(retained/dataset/'partitioner/requests64'), bytes=expected['bytes'], sha256=expected['sha256'])
    helper.validate_requests(pin, (100000, 768, 64, 100))
    results[dataset] = dict(pin=pin, requests=64, dimensions=768, authenticated=True, strict_schema_passed=True)
report = dict(schema='borsuk-source-witness-real-request-preflight-v1', results=results,
    helper_sha256=hashlib.sha256((repo/'scripts/prepare_hierarchical_cells_100k.py').read_bytes()).hexdigest(),
    module_sha256=hashlib.sha256((repo/'crates/borsuk/src/hierarchical_semantic_cells.rs').read_bytes()).hexdigest(),
    binary_source_sha256=hashlib.sha256((repo/'crates/borsuk/src/bin/hierarchical_semantic_cells.rs').read_bytes()).hexdigest(),
    wall_seconds=time.monotonic()-started, vectors_renormalized=False, truth_opened=False,
    native_search_executed=False, native_runtime_request_admission_proven=False, scientific_measurement=False)
Path('/tmp/borsuk-source-witness-real-request-preflight.json').write_text(json.dumps(report, sort_keys=True, indent=2)+'\n')
print(json.dumps(dict(strict_real_request_panels_passed=list(results),wall_seconds=report['wall_seconds'],truth_opened=False,native_search_executed=False)))
