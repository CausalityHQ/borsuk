"""Same-host ABBA cold calls; reuse the frozen first-response protocol."""
import json
import math
import os
from pathlib import Path
import struct
import sys

from scripts import run_native_cold_first_query as cold
from scripts.check_native_metadata_ranges_stats import validate as validate_transfer

BLOCKS = (('control', 0, 32), ('candidate', 0, 32),
          ('candidate', 32, 64), ('control', 32, 64))
CODE = (*cold.CODE, 'scripts/run_native_metadata_ranges_cold.py',
        'scripts/check_native_metadata_ranges_stats.py')


def prepare(config, output):
    panels = {}
    for item in config['items']:
        dataset = item['dataset']
        assert (item['rows'], item['dimensions']) == (1000000, 768)
        sources = {name: cold.fetch(config['bucket'], item['inputs'][name],
            output/'inputs'/dataset/name) for name in ('requests', 'reference-k10', 'truth')}
        requests = [json.loads(line) for line in Path(sources['requests']['path']).read_text().splitlines()]
        references = [json.loads(line) for line in Path(sources['reference-k10']['path']).read_text().splitlines()]
        truth = Path(sources['truth']['path']).read_bytes()
        assert len(requests) == 64 and len(references) == 66 and len(truth) == 25600
        assert references[0]['top_k'] == 10 and references[0]['declared_panel_count'] == references[-1]['count'] == 64
        for key, value in item['authority'].items(): assert references[0][key] == value
        assert [r['query_ordinal'] for r in requests] == [r['query_ordinal'] for r in references[1:-1]] == list(range(64))
        values = []
        for q, request in enumerate(requests):
            query = request['query']
            assert len(query) == 768 and all(math.isfinite(v) for v in query) and any(v != 0 for v in query)
            gt = struct.unpack_from('<100I', truth, q*400)
            assert len(set(gt)) == 100 and max(gt) < 1000000
            body = json.dumps(dict(query=query, k=10, **item['authority']),
                separators=(',', ':'), allow_nan=False).encode()
            values.append((body, references[q+1], gt))
        panels[dataset] = (item, sources, values)
    assert list(panels) == ['ReLAION', 'CoHere']
    return panels


def run(config, binaries, output):
    output.mkdir(exist_ok=False)
    inputs = prepare(config, output)
    records = {arm: {dataset: [] for dataset in inputs} for arm in binaries}
    for block, (arm, begin, end) in enumerate(BLOCKS):
        with (output/f'block{block}-records.jsonl').open('x') as stream:
            for dataset, (item, _, values) in inputs.items():
                for q in range(begin, end):
                    body, reference, truth = values[q]
                    row = dict(block=block, arm=arm, dataset=dataset, query_ordinal=q,
                        **cold.cold_call(binaries[arm], config, item, body, reference, truth,
                            failure_stream=stream))
                    try:
                        row['transfer_accounting'] = validate_transfer(row['native_header'], item['metadata_files'], arm)
                    except Exception as error:
                        row.update(outcome='invalid_transfer_accounting', error_type=type(error).__name__, error=str(error))
                        stream.write(json.dumps(row, sort_keys=True, allow_nan=False)+'\n')
                        stream.flush()
                        raise
                    stream.write(json.dumps(row, sort_keys=True, allow_nan=False)+'\n')
                    stream.flush()
                    records[arm][dataset].append(row)
    panels = {arm: {dataset: dict(**cold.reduce_panel(rows),
        split=inputs[dataset][0]['query_split']) for dataset, rows in datasets.items()}
        for arm, datasets in records.items()}
    for datasets in panels.values():
        for panel in datasets.values():
            panel.pop('serial_cold_calls_per_second')
            panel.pop('serial_span_ns')
    for arm, datasets in records.items():
        for dataset, rows in datasets.items():
            panels[arm][dataset].update(
                logical_metadata_head_requests=sum(r['transfer_accounting']['logical_metadata_head_requests'] for r in rows),
                logical_metadata_get_requests=sum(r['transfer_accounting']['logical_metadata_get_requests'] for r in rows),
                payload_buffer_bound_bytes=max(r['transfer_accounting']['payload_buffer_bound_bytes'] for r in rows) if arm == 'candidate' else None)
    campaign_span = records['control']['CoHere'][-1]['completed_ns']-records['control']['ReLAION'][0]['started_ns']
    quality = all(p['quality_gate_passed'] for datasets in panels.values() for p in datasets.values())
    deltas = {dataset: panels['candidate'][dataset]['cold_start_to_first_http_response_ms']['p90']-
        panels['control'][dataset]['cold_start_to_first_http_response_ms']['p90'] for dataset in inputs}
    paired = {dataset: [records['candidate'][dataset][q]['cold_start_to_first_http_response_ns']-
        records['control'][dataset][q]['cold_start_to_first_http_response_ns'] for q in range(64)]
        for dataset in inputs}
    summary = dict(panels=panels, blocks=[dict(arm=arm, begin=begin, end=end) for arm, begin, end in BLOCKS],
        inputs={dataset: value[1] for dataset, value in inputs.items()},
        ann_queries=256, namespace_starts=256, k=10,
        serial_campaign_span_ns=campaign_span, serial_campaign_cold_calls_per_second=256e9/campaign_span,
        candidate_minus_control_cold_p90_ms=deltas, paired_candidate_minus_control_cold_ns=paired,
        quality_gate_passed=quality, diagnostic_gate_passed=quality and all(delta < 0 for delta in deltas.values()),
        published_context_gate_passed=all(p['published_context_gate_passed'] for p in panels['candidate'].values()),
        source_scorer_ordered_id_physical_parity=True, namespace_cold_start_included=True,
        application_sq8_cache=False, s3_service_cache='uncontrolled', transport='loopback plain HTTP',
        cold_call_boundary='preencoded request; process launch through first HTTP response; refused TCP connects included',
        client_cpu_affinity=sorted(os.sched_getaffinity(0)), native_cpu_affinity=[0, 1, 2, 3],
        serial_cold_qps_is_offered_or_saturation_qps=False, matched_vendor_measured=False)
    (output/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False)+'\n')
    print(json.dumps(dict(closed=True, ann_queries=256, quality_gate_passed=quality,
        diagnostic_gate_passed=summary['diagnostic_gate_passed'],
        published_context_gate_passed=summary['published_context_gate_passed'])))
    return summary


def main():
    config_path, digest, candidate, candidate_proof, control, control_proof, output = sys.argv[1:]
    assert cold.sha(config_path) == digest
    config = json.loads(Path(config_path).read_text())
    assert config['schema'] == 'borsuk-native-metadata-ranges-cold-v1'
    assert config['count'] == 64 and config['k'] == 10
    assert config['dataset_order'] == ['ReLAION', 'CoHere'] and sorted(os.sched_getaffinity(0)) == [4, 5]
    assert set(config['code_sha256']) == set(CODE)
    for name, expected in config['code_sha256'].items(): assert cold.sha(name) == expected
    for arm, binary, path in [('candidate', candidate, candidate_proof), ('control', control, control_proof)]:
        proof = json.loads(Path(path).read_text())
        assert proof['qualified'] and proof['green_status'] == proof['release_status'] == 0
        assert cold.sha(binary) == proof['binary_sha256']
        assert cold.sha('crates/borsuk/examples/two_bit_http.rs') == proof['compiled_http_sha256']
        if arm == 'candidate':
            for name, expected in proof['compiled_native_sha256'].items(): assert cold.sha(name) == expected
        else:
            assert proof['binary_sha256'] == config['control_binary']['sha256']
            assert Path(binary).stat().st_size == config['control_binary']['bytes']
    run(config, dict(candidate=candidate, control=control), Path(output))


if __name__ == '__main__':
    assert len(sys.argv) == 8
    main()
