"""Independent fixed-block reduction; source/remote authority is a separate gate."""
import copy
import gzip
import json
from pathlib import Path
import sys

from scripts.verify_native_cold_first_query import reduce_records
from scripts.check_native_startup_stats import validate

ROOT = Path('docs/research/native-union-20260928')
BLOCKS = (('control', 0, 32), ('candidate', 0, 32),
          ('candidate', 32, 64), ('control', 32, 64))


def reduce_blocks(blocks, inputs, items):
    assert len(blocks) == 4
    records = {arm: {item['dataset']: [] for item in items} for arm in ('control', 'candidate')}
    previous = 0
    for block, (arm, begin, end) in enumerate(BLOCKS):
        rows = blocks[block]
        expected = [(item['dataset'], q) for item in items for q in range(begin, end)]
        assert len(rows) == 64
        assert [(row['dataset'], row['query_ordinal']) for row in rows] == expected
        for row in rows:
            assert row['block'] == block and row['arm'] == arm
            assert previous <= row['started_ns'] <= row['completed_ns']
            previous = row['completed_ns']
            records[arm][row['dataset']].append(row)
    panels, peaks = {}, {}
    for arm, datasets in records.items():
        panels[arm], peaks[arm] = {}, {}
        for item in items:
            dataset = item['dataset']
            rows = datasets[dataset]
            result, peak = reduce_records(rows, *inputs[dataset], item)
            result.pop('serial_span_ns'); result.pop('serial_cold_calls_per_second')
            result['split'] = item['query_split']
            heads, gets, buffers = 0, 0, []
            for row in rows:
                header = row['native_header']
                stats = header['remote_open_stats']
                accounting = dict(row['metadata'])
                if arm == 'control':
                    accounting.update(logical_metadata_head_requests=0,
                        logical_metadata_get_requests=9, payload_buffer_bound_bytes=None)
                    gets += 9
                else:
                    nhead, nget, bounds = 0, 0, []
                    for entry in stats['metadata']:
                        for field in ('head_wall_ns', 'logical_head_requests', 'logical_get_requests', 'payload_buffer_bound_bytes'):
                            assert type(entry[field]) is int and entry[field] >= 0
                        assert entry['logical_head_requests'] == 1
                        size = entry['bytes']
                        count = (size+8388607)//8388608
                        bound = min(size, 33554432)
                        assert entry['logical_get_requests'] == count
                        assert entry['payload_buffer_bound_bytes'] == bound
                        if size > 8388608: assert entry['get_wall_ns'] == 0
                        nhead += 1; nget += count; bounds.append(bound)
                    assert sum(e['head_wall_ns']+e['get_wall_ns']+e['stream_wall_ns'] for e in stats['metadata']) <= stats['staging_wall_ns']
                    accounting.update(logical_metadata_head_requests=nhead,
                        logical_metadata_get_requests=nget, payload_buffer_bound_bytes=max(bounds))
                    heads += nhead; gets += nget; buffers.append(max(bounds))
                assert accounting == row['transfer_accounting']
            result.update(logical_metadata_head_requests=heads, logical_metadata_get_requests=gets,
                payload_buffer_bound_bytes=max(buffers) if buffers else None)
            panels[arm][dataset] = result
            peaks[arm][dataset] = peak
    quality = all(p['quality_gate_passed'] for datasets in panels.values() for p in datasets.values())
    deltas = {item['dataset']: panels['candidate'][item['dataset']]['cold_start_to_first_http_response_ms']['p90']-
        panels['control'][item['dataset']]['cold_start_to_first_http_response_ms']['p90'] for item in items}
    paired = {item['dataset']: [records['candidate'][item['dataset']][q]['cold_start_to_first_http_response_ns']-
        records['control'][item['dataset']][q]['cold_start_to_first_http_response_ns'] for q in range(64)] for item in items}
    span = blocks[-1][-1]['completed_ns']-blocks[0][0]['started_ns']
    return dict(panels=panels, native_peak_rss_bytes=peaks, quality_gate_passed=quality,
        candidate_minus_control_cold_p90_ms=deltas, paired_candidate_minus_control_cold_ns=paired,
        diagnostic_gate_passed=quality and all(delta < 0 for delta in deltas.values()),
        published_context_gate_passed=all(p['published_context_gate_passed'] for p in panels['candidate'].values()),
        serial_campaign_span_ns=span, serial_campaign_cold_calls_per_second=256e9/span)


def self_check():
    # Closed original records supply valid shape/quality only. Block clocks and
    # candidate transfer counters below are synthetic, not a new measurement.
    config = json.loads((ROOT/'cold-first-query-config.json').read_text())
    original = {item['dataset']: [json.loads(line) for line in gzip.decompress(
        (ROOT/f"cold-first-query/a0001/screen/{item['dataset'].lower()}-records.jsonl.gz").read_bytes()).splitlines()]
        for item in config['items']}
    inputs = {}
    for item in config['items']:
        rows = original[item['dataset']]
        # Use each response's IDs as synthetic GT. Actual closed quality is not
        # recomputed or claimed by this synthetic test.
        requests = [dict(query=[1.]+[0.]*767) for _ in rows]
        references = [row['response'] for row in rows]
        truth = [row['response']['ids']+list(range(100, 190)) for row in rows]
        inputs[item['dataset']] = requests, references, truth
    clock = 1
    blocks = []
    for block, (arm, begin, end) in enumerate(BLOCKS):
        rows = []
        for item in config['items']:
            for q in range(begin, end):
                row = copy.deepcopy(original[item['dataset']][q])
                offset = clock-row['started_ns']
                for key in ('started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns'): row[key] += offset
                if arm == 'control':
                    for key in ('successful_connect_attempt_ns', 'connected_ns', 'completed_ns',
                                'before_successful_connect_attempt_ns', 'cold_start_to_first_http_response_ns'): row[key] += 10000000
                clock = row['completed_ns']+1000000
                row.update(block=block, arm=arm, returned_hits=10)
                request = json.dumps(dict(query=inputs[item['dataset']][0][q]['query'], k=10, **item['authority']), separators=(',', ':')).encode()
                from scripts.verify_native_cold_first_query import sha
                row.update(request_sha256=sha(request), request_bytes=len(request))
                stats = row['native_header']['remote_open_stats']
                if arm == 'candidate':
                    for entry in stats['metadata']:
                        entry.update(head_wall_ns=1, logical_head_requests=1,
                            logical_get_requests=(entry['bytes']+8388607)//8388608,
                            payload_buffer_bound_bytes=min(entry['bytes'],33554432))
                        if entry['bytes'] > 8388608: entry['get_wall_ns'] = 0
                    row['metadata'] = validate(stats, item['metadata_files'], row['native_header']['remote_open_wall_ns'])
                row['native_server_log'] = json.dumps(row['native_header'])+'\n'
                row['transfer_accounting'] = dict(row['metadata'],
                    logical_metadata_head_requests=9 if arm == 'candidate' else 0,
                    logical_metadata_get_requests=37 if arm == 'candidate' else 9,
                    payload_buffer_bound_bytes=33554432 if arm == 'candidate' else None)
                rows.append(row)
        blocks.append(rows)
    result = reduce_blocks(blocks, inputs, config['items'])
    assert result['quality_gate_passed'] and result['diagnostic_gate_passed'] and not result['published_context_gate_passed']
    assert all(delta == -10 for delta in result['candidate_minus_control_cold_p90_ms'].values())
    for mutation in ('order', 'arm', 'buffer', 'timing'):
        bad = copy.deepcopy(blocks)
        if mutation == 'order': bad[0][0], bad[0][1] = bad[0][1], bad[0][0]
        elif mutation == 'arm': bad[0][0]['arm'] = 'candidate'
        elif mutation == 'buffer': bad[1][0]['native_header']['remote_open_stats']['metadata'][-1]['payload_buffer_bound_bytes'] += 1
        else: bad[3][0]['started_ns'] = 0
        try: reduce_blocks(bad, inputs, config['items'])
        except AssertionError: pass
        else: raise AssertionError('invalid '+mutation+' accepted')
    print('independent paired reducer PASS (synthetic256/order/arm/buffer/timing guards)')


if __name__ == '__main__':
    assert sys.argv[1:] == ['--self-check'], 'closed authority main pending; no launch authority yet'
    self_check()
