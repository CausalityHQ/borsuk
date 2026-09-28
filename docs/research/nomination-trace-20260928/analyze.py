"""Postmortem stage coverage; no selection change or quality qualification."""
import hashlib
import json
from pathlib import Path
import sys
import numpy as np

campaign = Path(sys.argv[1]).resolve()
original = campaign.parent / 'native-pipeline-quality-a0001'
manifest = original / 'relaion/generation/manifest.json'
assert hashlib.sha256(manifest.read_bytes()).hexdigest() == 'b8f2ec97c0ad9633d1f3bc672b90ff88e32cb80b0d6ecbeb12f2ddbf74a48ff2'
body = json.loads(manifest.read_text())
sq8_path = original / 'relaion/sq8.bin'
assert hashlib.sha256(sq8_path.read_bytes()).hexdigest() == body['sq8_object_sha256']
truth_path = campaign.parent / 'truth.u32'
assert hashlib.sha256(truth_path.read_bytes()).hexdigest() == '4bd3ac79fce3919f85359ce3e305491663991f6cc0890ab91682cda34247a24e'
truth = np.fromfile(truth_path, dtype='<u4').reshape(1000, 100)
dtype = np.dtype([('id', '<i8'), ('norm', '<f4'), ('code', 'u1', (768,))])
data = np.memmap(sq8_path, mode='r', dtype=dtype, shape=(100000,))
old_plans = {p['query_ordinal']:p for p in map(json.loads, (original/'relaion/validation-plans.jsonl').read_text().splitlines())}
old_samples = {s['query_ordinal']:s for s in json.loads((original/'relaion/validation-score.json').read_text())['samples']}
traces = list(map(json.loads, (campaign/'traces.jsonl').read_text().splitlines()))
assert len(traces) == 64 and [t['query_ordinal'] for t in traces] == list(range(256,320))
samples = []
for trace in traces:
    ordinal = trace['query_ordinal']
    old = old_plans[ordinal]
    assert trace['ranges'] == old['ranges'] and trace['planned_bytes'] == old['planned_bytes']
    ranking, selected = trace['ranked_candidate_pages'], trace['selected_pages']
    assert len(ranking) == len(set(ranking)) == 159
    assert len(selected) == len(set(selected)) and set(selected).issubset(ranking)
    def hits(pages):
        positions = np.concatenate([np.arange(p*256,min((p+1)*256,100000)) for p in pages])
        return int(np.isin(data['id'][positions], truth[ordinal]).sum())
    samples.append(dict(query_ordinal=ordinal, discovered_hits=hits(ranking),
                        nominal_top84_hits=hits(ranking[:84]), selected_hits=hits(selected),
                        selected_pages=len(selected),
                        **{key:old_samples[ordinal][key] for key in ['fetched_hits','returned_hits','flat_hits','gets','bytes']}))
metrics = {key:sum(s[key] for s in samples)/64 for key in ['discovered_hits','nominal_top84_hits','selected_hits','fetched_hits','returned_hits','flat_hits','selected_pages']}
result = dict(dataset='ReLAION first100k', dimensions=768, metric='cosine', k=100,
              split='already consumed validation256-319 postmortem', queries=64,
              plan_parity=True, metrics=metrics, samples=samples, qualification=False,
              scope='GT coverage of existing stage sets; top84 ignores GET bridging; sets are not nested and losses are not additive; no serving latency or new route')
with (campaign/'analysis.json').open('x') as stream:
    json.dump(result,stream,indent=2);stream.write('\n')
print(json.dumps({k:v for k,v in result.items() if k!='samples'}))
