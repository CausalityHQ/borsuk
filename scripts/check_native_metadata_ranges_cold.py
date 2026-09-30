"""No AWS: exercise the actual fixed ABBA worker and both diagnostic outcomes."""
import json
from pathlib import Path
import struct
import tempfile
from unittest.mock import patch

from scripts import run_native_metadata_ranges_cold as worker


def main():
    authority = dict(root_sha256='a'*64, generation=1, control_epoch=1)
    response = dict(authority=authority, ids=list(range(10)), ranges=[[0, 1]],
        planned_bytes=1, submitted_gets=1, verified_bytes=1, failed_gets=0)
    config = dict(bucket='synthetic', items=[dict(dataset=dataset, rows=1000000,
        dimensions=768, authority=authority, query_split='synthetic', metadata_files={'manifest.json':1},
        inputs={name: {} for name in ('requests', 'reference-k10', 'truth')})
        for dataset in ('ReLAION', 'CoHere')])
    def fetch(bucket, identity, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.name == 'requests':
            values = [dict(query_ordinal=q, query=[1.]+[0.]*767) for q in range(64)]
            body = ''.join(json.dumps(value)+'\n' for value in values).encode()
        elif path.name == 'reference-k10':
            values = [dict(top_k=10, declared_panel_count=64, **authority),
                *[dict(query_ordinal=q, **response) for q in range(64)], dict(count=64)]
            body = ''.join(json.dumps(value)+'\n' for value in values).encode()
        else: body = struct.pack('<100I', *range(100))*64
        path.write_bytes(body)
        return dict(path=str(path), sha256=worker.cold.sha(path), bytes=len(body))
    for slower_cohere in (False, True):
        calls = []
        def call(binary, config, item, body, reference, truth, **kwargs):
            start = len(calls)*5_000_000_000
            calls.append((binary, item['dataset'], reference['query_ordinal']))
            duration = 3_000_000_000 if binary == 'control' else 1_000_000_000
            if slower_cohere and binary == 'candidate' and item['dataset'] == 'CoHere': duration = 4_000_000_000
            assert 'failure_stream' in kwargs and len(truth) == 100
            stat = dict(name='manifest.json',bytes=1,chunks=1,get_wall_ns=1,stream_wall_ns=1,write_wall_ns=1)
            if binary == 'candidate': stat.update(head_wall_ns=1,logical_head_requests=1,logical_get_requests=1,payload_buffer_bound_bytes=1)
            header = dict(remote_open_wall_ns=7,remote_open_stats=dict(metadata=[stat],staging_wall_ns=4,decode_wall_ns=2))
            return dict(native_header=header,started_ns=start, completed_ns=start+duration,
                cold_start_to_first_http_response_ns=duration, incoming_http_wall_ns=50_000_000,
                http_status=200, http_attempts=1, valid_ann_requests=1, returned_hits=10,
                response=response, metadata=dict(metadata_objects=9, metadata_bytes=256000000))
        with tempfile.TemporaryDirectory() as tmp, patch.object(worker.cold, 'fetch', side_effect=fetch) as fetched, \
                patch.object(worker.cold, 'cold_call', side_effect=call), \
                patch.object(worker.os, 'sched_getaffinity', return_value={4, 5}):
            output = Path(tmp)/'screen'
            summary = worker.run(config, dict(control='control', candidate='candidate'), output)
            expected = [(arm, dataset, q) for arm, begin, end in worker.BLOCKS
                for dataset in ('ReLAION', 'CoHere') for q in range(begin, end)]
            assert calls == expected and fetched.call_count == 6
            assert summary['ann_queries'] == summary['namespace_starts'] == 256
            assert summary['quality_gate_passed'] and summary['diagnostic_gate_passed'] is (not slower_cohere)
            assert not summary['published_context_gate_passed']
            for block in range(4):
                rows = [json.loads(line) for line in (output/f'block{block}-records.jsonl').read_text().splitlines()]
                assert len(rows) == 64 and all(row['block'] == block for row in rows)
            assert all(panel['returned_hits'] == 640 for panels in summary['panels'].values() for panel in panels.values())
            assert len(summary['paired_candidate_minus_control_cold_ns']['ReLAION']) == 64
            assert summary['candidate_minus_control_cold_p90_ms']['ReLAION'] == -2000
            assert summary['candidate_minus_control_cold_p90_ms']['CoHere'] == (1000 if slower_cohere else -2000)
    calls.clear()
    with tempfile.TemporaryDirectory() as tmp, patch.object(worker.cold, 'fetch', side_effect=fetch), \
            patch.object(worker.cold, 'cold_call', side_effect=call), \
            patch.object(worker, 'validate_transfer', side_effect=AssertionError('synthetic bad counter')):
        output = Path(tmp)/'screen'
        try: worker.run(config, dict(control='control', candidate='candidate'), output)
        except AssertionError: pass
        else: raise AssertionError('invalid transfer accepted')
        failed = json.loads((output/'block0-records.jsonl').read_text())
        assert failed['outcome'] == 'invalid_transfer_accounting' and failed['native_header']
        assert not (output/'summary.json').exists() and len(calls) == 1
    print('paired cold worker PASS (synthetic256-call ABBA/input count/quality/context/one-dataset slowdown rejection)')


if __name__ == '__main__': main()
