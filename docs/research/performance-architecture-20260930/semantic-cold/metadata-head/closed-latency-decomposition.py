"""Derive timing means from authenticated, complete offered-a3 1QPS cells."""
import gzip
import hashlib
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[1] / 'offered/a0003'
EXACT = {'page_digests.bin', 'plane/mean.bin', 'plane/page_digests.bin',
         'router/manifest.json', 'router/membership.bin'}

def run():
    terminal = json.loads((ROOT / 'aws-terminal.json').read_bytes())
    cells = {}
    for dataset in ('relaion', 'cohere'):
        name = f'screen/rate2-{dataset}-candidate-records.jsonl'
        body = gzip.decompress((ROOT / (name + '.gz')).read_bytes())
        assert len(body) == terminal['artifacts'][name]['bytes']
        assert hashlib.sha256(body).hexdigest() == terminal['artifacts'][name]['sha256']
        rows = [json.loads(line) for line in body.splitlines()]
        assert len(rows) == 64 and {r['query_ordinal'] for r in rows} == set(range(64))
        assert all(r['outcome'] == 'success' and r['offered_qps'] == 1 for r in rows)
        def mean(read):
            return statistics.mean(read(r) for r in rows) / 1e6
        def metadata(read):
            return mean(lambda r: sum(read(x) for x in r['native_header']['remote_open_stats']['metadata']))
        cells[dataset] = dict(
            whole_cold_mean_ms=mean(lambda r:r['cold_start_to_first_http_response_ns']),
            authority_head_read_mean_ms=mean(lambda r:r['native_header']['head_read_wall_ns']),
            remote_open_mean_ms=mean(lambda r:r['native_header']['remote_open_wall_ns']),
            metadata_HEAD_mean_ms=metadata(lambda x:x['head_wall_ns']),
            metadata_GET_header_mean_ms=metadata(lambda x:x['get_wall_ns']),
            metadata_stream_output_mean_ms=metadata(lambda x:x['stream_wall_ns']),
            metadata_decode_mean_ms=mean(lambda r:r['native_header']['remote_open_stats']['decode_wall_ns']),
            native_query_mean_ms=mean(lambda r:r['response']['native_wall_ns']),
            five_known_HEAD_observed_mean_ms=metadata(lambda x:x['head_wall_ns'] if x['name'] in EXACT else 0),
            estimated_speedup_ms='UNMEASURED',
            query_mean={key: statistics.mean(r['response'][key] for r in rows) for key in
                        ('source_submitted_gets', 'source_verified_bytes', 'router_submitted_gets',
                         'router_verified_bytes', 'submitted_gets', 'verified_bytes')},
            input_sha256=hashlib.sha256(body).hexdigest())
    return dict(schema='borsuk-closed-cold-timing-decomposition-v1',
                population='FIRST100k D768 cosine development0..63 k10, semantic, 1 offered QPS',
                cells=cells, note='Observed interval means; nested intervals overlap. Do not add percentiles. Removed HEAD waits do not establish candidate speedup.')

if __name__ == '__main__':
    print(json.dumps(run(), sort_keys=True, indent=2))
