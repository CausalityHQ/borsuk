"""Independent offered schedules and paged source accounting."""
import copy
import sys
from unittest.mock import patch

from scripts import verify_native_cold_offered as base
from scripts import verify_native_cold_first_query as cold
from scripts.check_native_paged_source_stats import validate_response, validate_startup
from scripts.run_native_paged_cold_offered import RATES

EXTRA = ('source_submitted_gets', 'source_verified_bytes', 'source_failed_gets',
         'sq8_submitted_gets', 'sq8_verified_bytes', 'sq8_failed_gets',
         'combined_submitted_gets', 'combined_verified_bytes', 'combined_failed_gets', 'source_head_requests')
_reduce = base.reduce_cell


def reduce_cell(rows, cell, requests, references, truth, item, index, geometry=None):
    legacy = {key: value for key, value in cell.items() if key not in EXTRA}
    with patch.object(base, 'RATES', RATES), patch.object(cold, 'validate', validate_startup), \
            patch.object(base, 'transfer', side_effect=lambda header, files, arm, geometry:
                validate_startup(header['remote_open_stats'], files, header['remote_open_wall_ns'])):
        result, peak = _reduce(rows, legacy, requests, references, truth, item, index, geometry)
    successful = [r for r in rows if r['outcome'] == 'success']
    for row in successful:
        validate_response(row['response'])
        q = row['query_ordinal']
        assert row['truth_at_10'] == list(truth[q][:10])
        assert row['expected_authority'] == item['authority']
        assert row['metadata_files'] == item['metadata_files']
        assert row['reference_response'] == {key: references[q][key] for key in
            ('ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets')}
    for suffix in ('submitted_gets', 'verified_bytes', 'failed_gets'):
        source = sum(r['response']['source_'+suffix] for r in successful)
        sq8 = sum(r['response'][suffix] for r in successful)
        result.update({'source_'+suffix: source, 'sq8_'+suffix: sq8, 'combined_'+suffix: source+sq8})
    result['source_head_requests'] = sum(r['metadata']['source_head_requests'] for r in successful)
    assert result == cell
    return result, peak


def main(attempt):
    from scripts import launch_native_paged_cold_offered_spot as campaign
    # Existing main authenticates terminal/archive/frozen binary/current source,
    # inputs, cgroup bounds and termination, then calls the independent reducer.
    with patch.object(base, 'RATES', RATES), patch.object(base, 'reduce_cell', reduce_cell), \
            patch.object(cold, 'validate', validate_startup), \
            patch.object(base, 'transfer', side_effect=lambda header, files, arm, geometry:
                validate_startup(header['remote_open_stats'], files, header['remote_open_wall_ns'])):
        base.main(attempt, campaign=campaign)


def self_check():
    from scripts import run_native_paged_cold_offered as worker
    from scripts.run_native_paged_cold_first_query_selfcheck import gates_check
    gates_check()
    epoch = 1_000_000; item = dict(dataset='synthetic', query_split='synthetic')
    rows = [dict(query_ordinal=q, rate_index=0, dataset='synthetic', offered_qps=8,
        scheduled_ns=epoch+round(q*1e9/8), dispatched_ns=epoch+round(q*1e9/8),
        terminal_ns=epoch+round(q*1e9/8), port=None, outcome='capacity_drop',
        started_ns=None, completed_ns=None, http_attempts=0, valid_ann_requests=0) for q in range(64)]
    result = worker.reduce_cell(rows, 8, epoch, epoch+8_000_000_000, False)
    cell = dict(result, rate_index=0, dataset='synthetic', split='synthetic',
                records_file='rate0-synthetic-records.jsonl')
    checked, _ = reduce_cell(rows, cell, [], [], [], item, 0)
    assert checked['successful'] == 0 and not checked['quality_gate_passed']
    for key in ('successful', 'source_verified_bytes', 'quality_gate_passed'):
        bad = dict(cell); bad[key] = True if key == 'quality_gate_passed' else 1
        try: reduce_cell(rows, bad, [], [], [], item, 0)
        except AssertionError: pass
        else: raise AssertionError('drop-biased or charged summary accepted')
    bad = copy.deepcopy(rows); bad[0]['scheduled_ns'] += 1
    try: reduce_cell(bad, cell, [], [], [], item, 0)
    except AssertionError: pass
    else: raise AssertionError('changed schedule accepted')
    # Replay a closed successful call under synthetic schedules; no performance claim.
    import gzip, json
    from pathlib import Path
    config = json.loads(Path('docs/research/source-paging-20260930/offered-config.json').read_bytes())
    item = config['items'][0]
    original = json.loads(gzip.decompress(Path('docs/research/source-paging-20260930/cold/a0004/screen/relaion-records.jsonl.gz').read_bytes()).splitlines()[0])
    requests = [dict(query=[1.]+[0.]*767) for _ in range(64)]
    reference = original['response']
    references = [reference for _ in range(64)]
    truth = [reference['ids']+list(range(100)) for _ in range(64)]
    rows = []
    for q in range(64):
        row = copy.deepcopy(original)
        dispatched = epoch+q*4*10**9
        delta = dispatched-row['started_ns']
        for key in ('started_ns','successful_connect_attempt_ns','connected_ns','completed_ns'): row[key] += delta
        row.update(query_ordinal=q,rate_index=1,offered_qps=.25,scheduled_ns=dispatched,
            dispatched_ns=dispatched,terminal_ns=row['completed_ns']+1,port=18080,outcome='success',
            returned_hits=10,truth_at_10=truth[q][:10])
        body = json.dumps(dict(query=requests[q]['query'],k=10,**item['authority']),separators=(',',':')).encode()
        row.update(request_sha256=cold.sha(body),request_bytes=len(body),transfer_accounting=row['metadata'])
        row['native_header']['listen'] = '127.0.0.1:18080'
        row['native_server_log'] = json.dumps(row['native_header'])+'\n'
        rows.append(row)
    result = worker.reduce_cell(rows,.25,epoch,rows[-1]['terminal_ns']+1,False)
    cell = dict(result,rate_index=1,dataset=item['dataset'],split=item['query_split'],records_file='rate1-relaion-records.jsonl')
    reduce_cell(rows,cell,requests,references,truth,item,1)
    bad = copy.deepcopy(rows); bad[0]['response']['source_verified_bytes'] += 1
    try: reduce_cell(bad,cell,requests,references,truth,item,1)
    except AssertionError: pass
    else: raise AssertionError('changed successful source charge accepted')
    from types import ModuleType
    campaign = ModuleType('scripts.launch_native_paged_cold_offered_spot')
    previous = base.RATES, base.reduce_cell, cold.validate
    def invoked(attempt, campaign):
        assert attempt == 'fixture' and base.RATES == RATES
        assert base.reduce_cell is reduce_cell and cold.validate is validate_startup
        raise RuntimeError('synthetic cancellation')
    with patch.dict(sys.modules, {campaign.__name__: campaign}), patch.object(base, 'main', side_effect=invoked):
        try: main('fixture')
        except RuntimeError: pass
        else: raise AssertionError('cancelled verification accepted')
    assert (base.RATES, base.reduce_cell, cold.validate) == previous
    print('PASS independent offered reducer: schedule, drops, quality and source charges')


if __name__ == '__main__':
    if sys.argv[1:] in ([], ['--self-check']): self_check()
    else:
        assert len(sys.argv) == 2
        main(sys.argv[1])
