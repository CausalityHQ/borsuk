"""Pinned metadata-only locality falsifier; no corpus, request, truth or S3 reads."""
import hashlib
import itertools
import json
import math
import pathlib
import statistics

ROOT = pathlib.Path('/mnt/borsuk-workspace')
OUT = ROOT / 'evidence/fp32-coalescing-native-a0001'
BUDGETS = (16, 32, 64, 128, 256, 512, 1024)

def authenticated(path, digest, cap):
    with path.open('rb') as f:
        data = f.read(cap + 1)
    assert len(data) <= cap
    assert hashlib.sha256(data).hexdigest() == digest
    return data

def frontier(ordinals):
    rows = sorted(ordinals)
    assert rows and len(rows) == len(set(rows))
    assert all(type(x) is int and x >= 0 for x in rows)
    gaps = sorted((b-a-1, a, i) for i,(a,b) in enumerate(zip(rows, rows[1:])))
    covered = len(rows)
    result = {len(rows): covered}
    for index, (gap, _, _) in enumerate(gaps, 1):
        covered += gap
        result[len(rows)-index] = covered
    return result

def brute(rows):
    best = {}
    for mask in range(1 << (len(rows)-1)):
        starts = [0]+[i+1 for i in range(len(rows)-1) if mask & (1 << i)]
        ends = starts[1:]+[len(rows)]
        covered = sum(rows[b-1]-rows[a]+1 for a,b in zip(starts,ends))
        count = len(starts)
        best[count] = min(best.get(count, covered), covered)
    return best

checks = 0
for n in range(1, 8):
    for rows in itertools.combinations(range(10), n):
        assert frontier(rows) == brute(rows)
        checks += 1
for rows in [(0, 2**64-1), (0, 1, 2**64-2, 2**64-1)]:
    assert frontier(rows) == brute(rows)
    checks += 1

data = authenticated(ROOT/'evidence/fp32-serving-full1000-abba-a0002/B1.jsonl',
    '2bda08997e598964b556ced9b5993271696045855bf9026b736d84c3ef260407',64*1024*1024)
manifest = json.loads(authenticated(ROOT/'data/fp32-rerank-serving-real1m-a0001/generation/manifest.json',
    '14c8cbba54b3299ad681a40ce0455caac78f46c316dd1714da6b475733c5fde5',1024*1024))
geometry = manifest['canonical']
record_bytes = 8+4*geometry['dimensions']
assert geometry['bytes'] == geometry['rows']*record_bytes
plans = {b:[] for b in BUDGETS}
full = {g:[] for g in range(1,17)}
for line in data.splitlines():
    stage = json.loads(line).get('canonical_stage')
    if stage is None:
        continue
    assert stage['record_bytes'] == record_bytes
    ordinals = [row['ordinal'] for row in stage['shortlist']]
    assert len(ordinals) == 16 and all(0 <= x < geometry['rows'] for x in ordinals)
    curve = frontier(ordinals)
    for gets, covered in curve.items():
        full[gets].append(covered)
    for budget in BUDGETS:
        gets = min(g for g,c in curve.items() if g <= 16 and c <= budget)
        covered = curve[gets]
        plans[budget].append((gets, covered))
assert len(plans[16]) == 1000

def summary(values):
    ordered = sorted(values)
    return dict(total=sum(values), median=statistics.median(values),
                p95=ordered[math.ceil(len(ordered)*0.95)-1], max=max(values))

report = {
    'schema':'fp32-coalescing-metadata-frontier-v1',
    'scope':'cost counterfactual only; no speed claim',
    'evidence_sha256': hashlib.sha256(data).hexdigest(),
    'manifest_sha256':'14c8cbba54b3299ad681a40ce0455caac78f46c316dd1714da6b475733c5fde5',
    'geometry':{k:geometry[k] for k in ('rows','dimensions','bytes')},
    'record_bytes':record_bytes, 'queries':1000, 'selected_rows_per_query':16, 'get_cap':16,
    'optimality':'Each joined adjacent gap removes one GET and adds its missing records. Choosing smallest gaps minimizes covered records for each GET count; ties use lower physical ordinal.',
    'quantiles':'median; nearest-rank p95',
    'bruteforce_cases':checks,
    'budgets':[],
    'minimum_covered_records_by_get_count':{g:summary(v) for g,v in full.items()},
}
for budget, p in plans.items():
    report['budgets'].append({
        'covered_record_budget':budget,
        'gets':summary([g for g,c in p]),
        'covered_records':summary([c for g,c in p]),
        'covered_bytes':summary([c*record_bytes for g,c in p]),
        'bridge_records':summary([c-16 for g,c in p]),
        'bridge_bytes':summary([(c-16)*record_bytes for g,c in p]),
        'saved_gets':summary([16-g for g,c in p]),
        'queries_with_savings':sum(g<16 for g,c in p),
    })
report['continue_implementation'] = any(b['saved_gets']['total']>0 for b in report['budgets'])
(OUT/'frontier.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
print(json.dumps({k:report[k] for k in ('queries','record_bytes','bruteforce_cases','continue_implementation')}))
for b in report['budgets']:
    print(json.dumps({k:b[k] for k in ('covered_record_budget','gets','covered_records','saved_gets','queries_with_savings')}))
