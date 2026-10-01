"""Authenticate closed gzip bodies, replay relocated ledger, apply preregistered gate."""
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
from scripts import launch_native_semantic_metadata_cold_spot as controller

ROOT = Path(__file__).resolve().parent

def run():
    terminal = json.loads((ROOT/'aws-terminal.json').read_bytes())
    assert terminal['exit_code'] == terminal['original_exit_code'] == 0
    assert terminal['status'] == terminal['phase'] == 'complete'
    assert set(terminal['artifacts']) == set(controller.ARTIFACTS)
    with tempfile.TemporaryDirectory() as temporary:
        out = Path(temporary)
        for name in ('aws-reservation.json', 'aws-closeout.json', 'aws-terminal.json'):
            (out/name).write_bytes((ROOT/name).read_bytes())
        for name, identity in terminal['artifacts'].items():
            body = gzip.decompress((ROOT/(name+'.gz')).read_bytes())
            assert len(body) == identity['bytes'] and hashlib.sha256(body).hexdigest() == identity['sha256'], name
            path = out/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        replay = controller.replay(out)
        assert replay['records'] == 512 and replay['execution_gate_passed']
        summary = json.loads((out/'screen/summary.json').read_bytes())
        rows = [json.loads(line) for line in (out/'screen/records.jsonl').read_bytes().splitlines()]
        by_query = {}
        for row in rows:
            metadata = row['startup_accounting']['metadata']
            assert metadata['logical_metadata_head_requests'] == (8 if row['arm']=='control' else 3)
            assert row['outcome'] == 'success'
            by_query.setdefault((row['dataset'],row['query_ordinal']), []).append(row)
        for group in by_query.values():
            assert len(group)==4
            control=next(r for r in group if r['arm']=='control')
            for row in group:
                assert row['reference_response']==control['reference_response']
                for key in ('logical_metadata_get_requests','metadata_bytes'):
                    assert row['startup_accounting']['metadata'][key]==control['startup_accounting']['metadata'][key]
                counts=row['accounting']['final_process_transport']['method_counts']
                expected=control['accounting']['final_process_transport']['method_counts'].copy()
                if row['arm']=='candidate': expected[1]-=5
                assert counts==expected, 'five HEAD reduction without extra submissions'
        datasets={}
        for dataset, value in summary['datasets'].items():
            pooled=value['pooled']; c,n=(pooled[r] for r in ('control','candidate'))
            ct,nt=(x['latency_ms']['whole_cold'] for x in (c,n))
            assert value['quality_delta_at_10']==0 and value['candidate_quality_gate_passed']
            passed=nt['p90']<ct['p90'] and nt['p95']<=ct['p95']
            datasets[dataset]=dict(gate_passed=passed,recall_at_10=n['recall_at_10'],quality_delta_pp=0,
                control_cold_ms=ct,candidate_cold_ms=nt,p90_delta_ms=nt['p90']-ct['p90'],
                p90_improvement_percent=100*(1-nt['p90']/ct['p90']),
                control_RSS_peak_bytes=c['native_peak_RSS_bytes'],candidate_RSS_peak_bytes=n['native_peak_RSS_bytes'],
                per_arm_observations=128,unique_queries=64)
        return dict(schema='borsuk-semantic-metadata-head-decision-v1',decision='GO' if all(x['gate_passed'] for x in datasets.values()) else 'FAIL',
            authenticated_artifacts=61,closed_replayed_calls=512,original_controller_exit_status=0,
            instance_id=terminal['instance_id'],state='terminated',datasets=datasets,
            resource_gate=summary['resource_gate'],GET_and_payload_parity=True,metadata_HEADs={'control':8,'candidate':3},
            sustainable_QPS='UNKNOWN',matched_vendor_measured=False,total_billed_lifecycle_cost='UNKNOWN')

if __name__=='__main__':
    print(json.dumps(run(),sort_keys=True,indent=2))
