"""Bounded open-loop offers, each admitted offer owning a fresh cold namespace."""
import hashlib
import http.client
import io
import json
import math
import os
from pathlib import Path
import queue
import sys
import threading
import time

from scripts import run_native_metadata_ranges_cold as ranges
from scripts.rest_coexistence_load import scheduled_offsets_ns

cold = ranges.cold
CODE = (*ranges.CODE, 'scripts/run_native_cold_offered.py')
RATES = [0.25, 0.5, 1, 2, 4, 8]
BINARY_SHA = '3d96aa35461bea6985d7e990f623aab9643ec55316be64af214c3c23dccc192d'
SOURCE_IDENTITY = '1720c277b493c1097f68aa89ea44c8555bf5222bf0de69de28144f900fdee969'


def tails(values):
    return {label: cold.quantile([v/1e6 for v in values], p) if values else None
            for label, p in [('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99)]}


def reduce_cell(records, offered_qps, epoch, terminal, arm_failed):
    successful = [r for r in records if r['outcome'] == 'success']
    admitted = [r for r in records if r['port'] is not None]
    dropped = sum(r['outcome'] == 'capacity_drop' for r in records)
    aborted = sum(r['outcome'] == 'arm_aborted' for r in records)
    errors = len(admitted)-len(successful)
    hits = sum(r['returned_hits'] for r in successful)
    span = terminal-epoch
    service = tails([r['cold_start_to_first_http_response_ns'] for r in successful])
    scheduled = tails([r['completed_ns']-r['scheduled_ns'] for r in successful])
    complete = len(successful) == len(records) == 64 and dropped == errors == aborted == 0
    quality = complete and hits >= 608
    return dict(offered_qps=offered_qps, offered=len(records), admitted=len(admitted),
        accepted_completed=len(admitted), successful=len(successful), capacity_drops=dropped,
        errors=errors, aborted_offers=aborted, arm_failed=arm_failed,
        success_fraction=len(successful)/len(records), epoch_ns=epoch, terminal_ns=terminal,
        planned_offer_window_ns=round(len(records)*1e9/offered_qps), full_span_ns=span,
        successful_full_span_qps=len(successful)*1e9/span,
        accepted_completed_full_span_qps=len(admitted)*1e9/span,
        returned_hits=hits, quality_success_count=len(successful),
        success_conditioned_recall_at_10=hits/(10*len(successful)) if successful else None,
        latency_population='successful offers only; drops/errors/aborts remain in all-offer denominators',
        cold_start_to_first_http_response_ms=service, scheduled_to_response_ms=scheduled,
        dispatch_delay_ms=tails([r['dispatched_ns']-r['scheduled_ns'] for r in records
                                 if r['dispatched_ns'] is not None]),
        all_offers_successful=complete, quality_gate_passed=quality,
        published_context_gate_passed=quality and service['p90'] < 444,
        counter_scope='successful calls only; unsuccessful raw records retained',
        query_submitted_gets=sum(r['response']['submitted_gets'] for r in successful),
        query_verified_bytes=sum(r['response']['verified_bytes'] for r in successful),
        query_failed_gets=sum(r['response']['failed_gets'] for r in successful),
        logical_metadata_head_requests=sum(r['transfer_accounting']['logical_metadata_head_requests'] for r in successful),
        logical_metadata_get_requests=sum(r['transfer_accounting']['logical_metadata_get_requests'] for r in successful))


def measure(binary, config, item, values, offered_qps, workers=6, base_port=18080):
    if type(workers) is not int or not 1 <= workers <= 6:
        raise ValueError('workers must be in 1..6')
    if type(base_port) is not int or not 1024 <= base_port <= 65536-workers:
        raise ValueError('worker ports must be in 1024..65535')
    if not math.isfinite(offered_qps) or offered_qps <= 0:
        raise ValueError('offered_qps must be finite and positive')
    assert len(values) == 64 and [v[1]['query_ordinal'] for v in values] == list(range(64))
    offsets = scheduled_offsets_ns(offered_qps, 64/offered_qps)
    assert len(offsets) == 64
    ports = queue.Queue()
    for port in range(base_port, base_port+workers): ports.put(port)
    failed = threading.Event()
    records = [None]*64
    threads = []
    epoch = time.monotonic_ns()

    def call(q, row):
        body, reference, truth = values[q]
        failure_stream = io.StringIO()
        stage = 'cold_call'
        try:
            row.update(cold.cold_call(binary, config, item, body, reference, truth,
                                     failure_stream=failure_stream, port=row['port']))
            stage = 'ann_identity'
            assert row['http_status'] == 200 and row['http_attempts'] == row['valid_ann_requests'] == 1
            assert row['native_close']['intentional_stop'] is True
            assert row['native_header']['listen'] == f"127.0.0.1:{row['port']}"
            assert row['scheduled_ns'] <= row['dispatched_ns'] <= row['started_ns'] <= row['completed_ns']
            assert row['cold_start_to_first_http_response_ns'] == row['completed_ns']-row['started_ns']
            assert row['returned_hits'] == cold.checked_response(row['response'], reference, truth, item['authority'])
            stage = 'transfer_accounting'
            row['transfer_accounting'] = ranges.validate_transfer(row['native_header'], item['metadata_files'], 'candidate')
            row['outcome'] = 'success'
        except Exception as error:
            raw = failure_stream.getvalue()
            row.update(error_type=type(error).__name__, error=str(error), failure_stage=stage,
                       failure_stream_raw=raw)
            if raw:
                row['failure_record'] = json.loads(raw)
                for key in ('started_ns', 'completed_ns', 'http_status', 'http_attempts', 'native_close'):
                    if key in row['failure_record']: row[key] = row['failure_record'][key]
            transport = stage == 'cold_call' and (
                isinstance(error, (OSError, http.client.HTTPException)) or
                isinstance(error, RuntimeError) and str(error) == 'namespace process closed before first connection' or
                isinstance(error, AssertionError) and str(error) == 'first and only ANN request failed; no HTTP retry')
            row['outcome'] = 'transport_error' if transport else 'invalid_ann'
            if not transport: failed.set()
        finally:
            # cold_call returns/raises only after owned process cleanup.
            row['terminal_ns'] = time.monotonic_ns()
            records[q] = row
            ports.put(row['port'])

    for q, offset in enumerate(offsets):
        scheduled = epoch+offset
        row = dict(dataset=item['dataset'], query_ordinal=q, offered_qps=offered_qps,
                   scheduled_ns=scheduled, dispatched_ns=None, started_ns=None,
                   completed_ns=None, port=None)
        if not failed.is_set():
            delay = (scheduled-time.monotonic_ns())/1e9
            if delay > 0: time.sleep(delay)
        if failed.is_set():
            row.update(outcome='arm_aborted', terminal_ns=time.monotonic_ns(),
                       http_attempts=0, valid_ann_requests=0)
            records[q] = row
            continue
        row['dispatched_ns'] = time.monotonic_ns()
        try:
            row['port'] = ports.get_nowait()
        except queue.Empty:
            row.update(outcome='capacity_drop', terminal_ns=time.monotonic_ns(),
                       http_attempts=0, valid_ann_requests=0)
            records[q] = row
            continue
        # A thread is created only after owning a port; there is no executor/client queue.
        thread = threading.Thread(target=call, args=(q, row))
        try:
            thread.start()
        except Exception as error:
            row.update(outcome='invalid_ann', error_type=type(error).__name__, error=str(error),
                       failure_stage='thread_start', terminal_ns=time.monotonic_ns())
            records[q] = row
            ports.put(row['port'])
            failed.set()
        else:
            threads.append(thread)
    for thread in threads: thread.join()
    terminal = time.monotonic_ns()
    return records, reduce_cell(records, offered_qps, epoch, terminal, failed.is_set())


def run(config, binary, output):
    output = Path(output)
    output.mkdir(exist_ok=False)
    inputs = ranges.prepare(config, output)  # Six immutable fetches, before every cell timer.
    cells = []
    summary = dict(schema='borsuk-native-cold-offered-result-v1', cells=cells,
        inputs={dataset: panel[1] for dataset, panel in inputs.items()},
        offered_qps=RATES, workers=6, base_port=18080, k=10, planned_offers=768,
        namespace_cold_start_included=True, application_sq8_cache=False,
        s3_service_cache='uncontrolled', transport='loopback plain HTTP',
        client_cpu_affinity=sorted(os.sched_getaffinity(0)), native_cpu_affinity=[0, 1, 2, 3],
        native_tokio_threads=4, native_budget_bytes=1024**3,
        matched_vendor_measured=False, current_full_suite_pass_claim=False, native_rebuilt=False,
        cold_call_boundary='preencoded request; process launch through first HTTP response; refused TCP connects included',
        scheduled_response_boundary='scheduled absolute offer time through full response bytes; successful offers only',
        full_span_boundary='first scheduled offer through all admitted calls and cleanup',
        arm_failed=False, complete=False)
    for index, rate in enumerate(RATES):
        for dataset, (item, _, values) in inputs.items():
            records, result = measure(binary, config, item, values, rate)
            name = f'rate{index}-{dataset.lower()}-records.jsonl'
            with (output/name).open('x') as stream:
                for row in records:
                    row['rate_index'] = index
                    stream.write(json.dumps(row, sort_keys=True, allow_nan=False)+'\n')
                stream.flush()
            cells.append(dict(result, rate_index=index, dataset=dataset,
                              split=item['query_split'], records_file=name))
            if result['arm_failed']:
                summary['arm_failed'] = True
                (output/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False)+'\n')
                raise RuntimeError('invalid ANN identity/accounting; terminal cell records preserved')
    summary.update(complete=True, offered=sum(c['offered'] for c in cells),
        admitted=sum(c['admitted'] for c in cells), successful=sum(c['successful'] for c in cells),
        accepted_completed=sum(c['accepted_completed'] for c in cells),
        capacity_drops=sum(c['capacity_drops'] for c in cells), errors=sum(c['errors'] for c in cells),
        quality_gate_passed=all(c['quality_gate_passed'] for c in cells),
        published_context_gate_passed=all(c['published_context_gate_passed'] for c in cells),
        largest_passing_tested_offered_qps=max([0]+[rate for rate in RATES
            if all(c['quality_gate_passed'] for c in cells if c['offered_qps'] == rate)]),
        eight_qps_attained=all(c['quality_gate_passed'] for c in cells if c['offered_qps'] == 8))
    (output/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False)+'\n')
    return summary


def main():
    config_path, digest, binary, proof_path, output = sys.argv[1:]
    assert cold.sha(config_path) == digest
    config = json.loads(Path(config_path).read_text())
    assert config['schema'] == 'borsuk-native-cold-offered-v1'
    assert (config['count'], config['k']) == (64, 10)
    assert config['offered_qps'] == RATES and config['workers'] == 6 and config['base_port'] == 18080
    assert config['dataset_order'] == ['ReLAION', 'CoHere']
    assert sorted(os.sched_getaffinity(0)) == config['client_cpu_affinity'] == [4, 5]
    assert config['native_cpu_affinity'] == [0, 1, 2, 3]
    assert os.environ['TOKIO_WORKER_THREADS'] == '4'
    assert os.environ['BORSUK_NATIVE_MEMORY_BYTES'] == '1073741824'
    assert set(config['code_sha256']) == set(CODE)
    for name, expected in config['code_sha256'].items(): assert cold.sha(name) == expected
    proof = json.loads(Path(proof_path).read_text())
    assert proof['qualified'] and proof['green_status'] == proof['release_status'] == 0
    assert proof['current_full_suite_pass_claim'] is False
    assert proof['sha_backend']['arm_asm_selected'] is True
    assert proof['sha_backend']['x86_asm_selected'] is False
    assert proof['sha_backend']['cpu_sha2_capable'] is True
    assert proof['binary_sha256'] == cold.sha(binary) == config['binary']['sha256'] == BINARY_SHA
    assert Path(binary).stat().st_size == config['binary']['bytes'] == 12471000
    assert proof['compiled_http_sha256'] == cold.sha('crates/borsuk/examples/two_bit_http.rs')
    assert len(proof['compiled_native_sha256']) == 8
    for name, expected in proof['compiled_native_sha256'].items(): assert cold.sha(name) == expected
    # Match the existing native proof identity without importing build/controller code.
    repo = Path('.')
    paths = set(repo.rglob('*.rs')) | set(repo.rglob('Cargo.toml')) | set(repo.rglob('Cargo.lock'))
    identities = {str(p): cold.sha(p) for p in sorted(paths) if not {'.git', 'target'}.intersection(p.parts)}
    identity = hashlib.sha256(json.dumps(identities, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert len(identities) == proof['source_file_count'] == 395
    assert identity == proof['source_identity_sha256'] == SOURCE_IDENTITY
    summary = run(config, binary, Path(output))
    print(json.dumps(dict(closed=True, offered=summary['offered'], successful=summary['successful'],
                          capacity_drops=summary['capacity_drops'], errors=summary['errors'])))


if __name__ == '__main__':
    assert len(sys.argv) == 6
    main()
