"""Reconcile closed development receipts; no queries, fit, or benchmark run."""
from pathlib import Path
import hashlib, json
root = Path(__file__).resolve().parents[1]
paths = [root/'centroid-discovery-diagnostic-20260928/result.json',
         root/'centroid-discovery-diagnostic-20260928/traces.jsonl',
         root/'native-pipeline-quality-20260928/relaion/development-score.json']
raw = [p.read_bytes() for p in paths]
discovery, score = json.loads(raw[0]), json.loads(raw[2])
traces = [json.loads(line) for line in raw[1].splitlines()]
assert len(discovery['samples']) == len(score['samples']) == len(traces) == 64
for candidate, fetched, trace in zip(discovery['samples'], score['samples'], traces):
    assert candidate['query_ordinal'] == fetched['query_ordinal'] == trace['query_ordinal']
    assert len(set(trace['ranked_candidate_pages'])) == 159
    assert set(trace['selected_pages']) <= set(trace['ranked_candidate_pages'])
    assert candidate['graph_hits'] == fetched['fetched_hits']
report = {'dataset':'ReLAION first100k D768 cosine k100', 'split':'reused development0-63',
          'queries':64, 'candidate_vs_fetched_equal_queries':64,
          'candidate_gt_coverage_percent':discovery['means']['graph_hits'],
          'exhaustive_other_candidate_set_gt_coverage_percent':discovery['means']['flat_centroid_hits'],
          'current_unit_center_arrays_bytes_per_row_projection':(768*4+4)/32,
          'scope':'Closed receipt reconciliation and source-layout arithmetic; no new quality, RSS, latency or vendor measurement',
          'inputs':[{ 'path':str(p.relative_to(root)), 'sha256':hashlib.sha256(data).hexdigest()}
                    for p,data in zip(paths,raw)]}
print(json.dumps(report, indent=2))
