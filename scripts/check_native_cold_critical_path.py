"""Replay authenticated CLOSED cold records; no queries or latency predictions."""
import json
from pathlib import Path
import sys

from scripts.check_native_source_paging_replay import artifact_at, require, sha
from scripts.run_native_cold_first_query import quantile

ROOT = Path('docs/research/source-paging-20260930/decode/cold/a0004')
VERIFICATION_SHA = 'f99bcbfa495bea6cb70787f2aac03894f050d29ebc528ea33f6029e01f371366'


def measures(row):
    header = row['native_header']
    stats = header['remote_open_stats']
    metadata = stats['metadata']
    cold = row['cold_start_to_first_http_response_ns']
    opened = header['remote_open_wall_ns']
    decoded = stats['decode_wall_ns']
    head = header['head_read_wall_ns']
    http = row['incoming_http_wall_ns']
    require(opened >= stats['staging_wall_ns'] + decoded + stats['source_head_wall_ns'],
            'overlapping or invalid remote-open intervals')
    require(cold >= opened + head + http, 'invalid cold partition')
    response = row['response']
    return dict(cold_ns=cold, head_ns=head, staging_ns=stats['staging_wall_ns'],
        metadata_head_ns=sum(m['head_wall_ns'] for m in metadata),
        metadata_get_header_ns=sum(m['get_wall_ns'] for m in metadata),
        metadata_stream_output_ns=sum(m['stream_wall_ns'] for m in metadata),
        decode_ns=decoded, source_head_ns=stats['source_head_wall_ns'], http_ns=http,
        residual_ns=cold-opened-head-http,
        erase_decode_ns=cold-decoded, erase_remote_open_ns=cold-opened,
        source_bytes=response['source_verified_bytes'], sq8_bytes=response['verified_bytes'],
        query_bytes=response['source_verified_bytes'] + response['verified_bytes'],
        source_gets=response['source_submitted_gets'], sq8_gets=response['submitted_gets'],
        metadata_gets=sum(m['logical_get_requests'] for m in metadata),
        metadata_heads=sum(m['logical_head_requests'] for m in metadata),
        metadata_bytes=sum(m['bytes'] for m in metadata))


def reduce(rows):
    values = [measures(row) for row in rows]
    return {(key[:-3] + '_ms' if key.endswith('_ns') else key): {label: quantile([v[key] / (1e6 if key.endswith('_ns') else 1)
                                 for v in values], p)
                  for label, p in [('min', 0), ('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99), ('max', 1)]}
            for key in values[0]}


def replay():
    body = (ROOT / 'verification.json').read_bytes()
    require(sha(body) == VERIFICATION_SHA, 'changed accepted verification')
    verification = json.loads(body)
    require(verification['valid_measurement'] and verification['state'] == 'terminated',
            'campaign not independently verified closed')
    terminal_body = (ROOT / 'aws-terminal.json').read_bytes()
    require(sha(terminal_body) == verification['terminal_sha256'], 'changed terminal')
    terminal = json.loads(terminal_body)
    require(terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0,
            'campaign not closed successfully')
    identities, rows = {}, []
    for block in range(4):
        name = f'screen/block{block}-records.jsonl'
        records = [json.loads(line) for line in artifact_at(ROOT, terminal, name, identities).splitlines()]
        require(len(records) == 64 and all(r['block'] == block for r in records), 'block roster')
        rows.extend(records)
    panels = {}
    for arm in ('control', 'candidate'):
        panels[arm] = {}
        for dataset in ('ReLAION', 'CoHere'):
            selected = [r for r in rows if r['arm'] == arm and r['dataset'] == dataset]
            require(sorted(r['query_ordinal'] for r in selected) == list(range(64)), 'panel roster')
            panels[arm][dataset] = reduce(selected)
    return dict(schema='borsuk-authenticated-cold-critical-path-v1',
        verification_sha256=VERIFICATION_SHA, terminal_sha256=verification['terminal_sha256'],
        authenticated_records=256, artifacts=identities, panels=panels,
        duration_units='ms for keys ending _ms; bytes/counts otherwise',
        scope='Observed phase distributions; no sum of marginal quantiles',
        counterfactual_scope='erase_* subtract each observed query interval before reducing; '
            'conditional trace arithmetic, not measurements or predictions of a changed system',
        unknowns=['query source/SQ8/CPU phase split', 'current offered QPS',
                  '10M/100M scale', 'lifecycle cost', 'new architecture quality'])


def self_check():
    row = dict(cold_start_to_first_http_response_ns=100, incoming_http_wall_ns=20,
        native_header=dict(remote_open_wall_ns=60, head_read_wall_ns=10,
            remote_open_stats=dict(staging_wall_ns=30, decode_wall_ns=20, source_head_wall_ns=5,
                metadata=[dict(head_wall_ns=3, get_wall_ns=2, stream_wall_ns=25,
                    logical_get_requests=2, logical_head_requests=1, bytes=100)])),
        response=dict(source_verified_bytes=50, verified_bytes=25,
            source_submitted_gets=4, submitted_gets=2))
    result = measures(row)
    assert result['residual_ns'] == 10 and result['erase_decode_ns'] == 80
    assert result['erase_remote_open_ns'] == 40 and result['query_bytes'] == 75
    row['cold_start_to_first_http_response_ns'] = 80
    try:
        measures(row)
    except ValueError:
        pass
    else:
        raise AssertionError('overlapping partition accepted')
    print('PASS cold partition and per-query subtraction')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.check_native_cold_critical_path OUTPUT.json'
        Path(sys.argv[1]).write_text(json.dumps(replay(), sort_keys=True, indent=2) + '\n')
