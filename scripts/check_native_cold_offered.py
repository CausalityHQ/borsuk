"""Stdlib-only checks: real scheduler/threads, mocked cold calls and virtual time."""
import copy
import io
import json
from pathlib import Path
import struct
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import run_native_cold_offered as worker


AUTHORITY = dict(root_sha256='a'*64, generation=1, control_epoch=1)
ITEM = dict(dataset='ReLAION', authority=AUTHORITY, indexes={'10': 'fixed/index'},
            metadata_files={'manifest.json': 1}, query_split='synthetic')
CONFIG = dict(bucket='synthetic', region='eu-central-1')
RESPONSE = dict(authority=AUTHORITY, ids=list(range(10)), ranges=[[0, 1]],
                planned_bytes=1, submitted_gets=1, verified_bytes=1, failed_gets=0)
TRANSFER = dict(logical_metadata_head_requests=1, logical_metadata_get_requests=1)
VALUES = [(b'{}', dict(RESPONSE, query_ordinal=q), list(range(100))) for q in range(64)]


class Scenario:
    def __init__(self, failures=None, hold_ns=1_000_000_000):
        self.now = 1_000_000_000
        self.hold_ns = hold_ns
        self.failures = failures or {}
        self.threads = []
        self.active = set()
        self.peak = 0
        self.calls = []
        self.streams = []
        scenario = self

        class Thread(threading.Thread):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.q = kwargs['args'][0]
                self.registered = threading.Event()
                self.release = threading.Event()
                self.cleanup_at = None
                scenario.threads.append(self)

            def start(self):
                super().start()
                assert self.registered.wait(5), 'mock call did not start'

            def join(self):
                scenario.advance(max(t.cleanup_at for t in scenario.threads))
                threading.Thread.join(self, 5)
                assert not self.is_alive(), 'mock call did not finish'

        self.Thread = Thread

    def monotonic_ns(self):
        return self.now

    def sleep(self, seconds):
        # Fixed 7ms dispatch delay makes scheduled and service tails distinct.
        self.advance(self.now+round(seconds*1e9)+7_000_000)

    def advance(self, when):
        self.now = max(self.now, when)
        due = [t for t in self.threads if t.cleanup_at <= self.now]
        for thread in due: thread.release.set()
        for thread in due:
            threading.Thread.join(thread, 5)
            assert not thread.is_alive(), 'cleanup/release did not finish'

    def cold(self, binary, config, item, body, reference, truth, *, failure_stream, port):
        q = reference['query_ordinal']
        thread = next(t for t in self.threads if t.q == q)
        assert port not in self.active
        self.active.add(port)
        self.peak = max(self.peak, len(self.active))
        self.calls.append((q, port))
        self.streams.append(failure_stream)
        started = self.now+1_000_000
        completed = started+50_000_000
        thread.cleanup_at = self.now+self.hold_ns
        thread.registered.set()
        assert thread.release.wait(5), 'virtual scheduler did not release mock call'
        self.active.remove(port)
        row = dict(started_ns=started, completed_ns=completed,
            cold_start_to_first_http_response_ns=50_000_000, http_status=200,
            http_attempts=1, valid_ann_requests=1, returned_hits=10, response=copy.deepcopy(RESPONSE),
            native_header=dict(listen=f'127.0.0.1:{port}'),
            native_close=dict(intentional_stop=True, returncode=124))
        if q in self.failures:
            failure = dict(row, outcome='failed', raw_response='raw failure body', native_server_log='raw log')
            failure_stream.write(json.dumps(failure)+'\n')
            raise self.failures[q]
        return row

    def measure(self, bad_transfer=False, geometry=None):
        with patch.object(worker, 'time', self), \
             patch.object(worker, 'threading', SimpleNamespace(Event=threading.Event, Thread=self.Thread)), \
             patch.object(worker.cold, 'cold_call', side_effect=self.cold), \
             patch.object(worker.ranges, 'validate_transfer', return_value=TRANSFER,
                          side_effect=AssertionError('bad transfer') if bad_transfer else None) as transfer:
            config = dict(CONFIG, staging={'candidate':geometry}) if geometry is not None else CONFIG
            rows, result = worker.measure('qualified-binary', config, ITEM, VALUES, 8)
        assert not self.active
        if not bad_transfer:
            assert transfer.call_count == result['successful']
            assert all(call.args[-1] == geometry for call in transfer.call_args_list)
        assert len({id(stream) for stream in self.streams}) == len(self.calls)
        return rows, result


def scheduler_check():
    Scenario().measure(geometry=dict(range_bytes=4194304, parallel_gets=8))
    scenario = Scenario()
    rows, result = scenario.measure()
    assert scenario.peak == 6
    assert {port for _, port in scenario.calls} == set(range(18080, 18086))
    assert len(scenario.calls) > 6 and len(scenario.calls) < 64
    assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
    assert result['admitted'] == result['accepted_completed'] == result['successful'] == len(scenario.calls)
    assert result['capacity_drops'] == 64-len(scenario.calls) and result['errors'] == 0
    assert not result['all_offers_successful'] and not result['quality_gate_passed']
    assert not result['published_context_gate_passed']
    assert result['success_fraction'] == len(scenario.calls)/64
    assert result['cold_start_to_first_http_response_ms'] == dict(p50=50., p90=50., p95=50., p99=50.)
    assert result['scheduled_to_response_ms']['p90'] == 58.
    assert result['successful_full_span_qps'] == len(scenario.calls)*1e9/result['full_span_ns']
    for row in rows:
        assert row['scheduled_ns'] == result['epoch_ns']+round(row['query_ordinal']*1e9/8)
        if row['outcome'] == 'capacity_drop':
            assert row['port'] is row['started_ns'] is row['completed_ns'] is None
            assert row['http_attempts'] == row['valid_ann_requests'] == 0
        else:
            assert row['started_ns']-row['dispatched_ns'] == 1_000_000
            assert row['terminal_ns'] > row['completed_ns']
    for port in range(18080, 18086):
        calls = [r for r in rows if r['port'] == port]
        assert all(a['terminal_ns'] <= b['dispatched_ns'] for a, b in zip(calls, calls[1:]))

    transport = Scenario({0: TimeoutError('wire timeout')})
    rows, result = transport.measure()
    assert not result['arm_failed'] and result['errors'] == 1
    assert rows[0]['outcome'] == 'transport_error' and rows[0]['failure_record']['raw_response'] == 'raw failure body'
    assert json.loads(rows[0]['failure_stream_raw']) == rows[0]['failure_record']
    assert rows[0]['native_close']['intentional_stop'] is True
    assert len(transport.calls) > 6  # Remaining offers continue, with no retry of ordinal0.
    assert sum(q == 0 for q, _ in transport.calls) == 1

    invalid = Scenario({0: AssertionError('ordered-ID/source/scorer/physical parity')})
    rows, result = invalid.measure()
    assert result['arm_failed'] and rows[0]['outcome'] == 'invalid_ann'
    assert rows[0]['failure_record']['native_server_log'] == 'raw log'
    assert result['aborted_offers'] > 0 and len(invalid.calls) == 6
    assert result['successful']+result['errors']+result['capacity_drops']+result['aborted_offers'] == 64

    rows, result = Scenario().measure(bad_transfer=True)
    assert result['arm_failed'] and result['successful'] == 0
    assert rows[0]['outcome'] == 'invalid_ann' and rows[0]['failure_stage'] == 'transfer_accounting'
    assert rows[0]['response'] == RESPONSE and rows[0]['native_close']['intentional_stop'] is True
    assert result['success_conditioned_recall_at_10'] is None
    assert all(value is None for value in result['cold_start_to_first_http_response_ms'].values())
    assert all(value is None for value in result['scheduled_to_response_ms'].values())

    complete = Scenario(hold_ns=60_000_000)
    rows, result = complete.measure()
    assert result['successful'] == 64 and result['returned_hits'] == 640
    assert result['quality_gate_passed'] and result['published_context_gate_passed']
    rows[0]['returned_hits'] = 0
    rows[1]['returned_hits'] = 0
    rows[2]['returned_hits'] = 0
    rows[3]['returned_hits'] = 0
    result = worker.reduce_cell(rows, 8, result['epoch_ns'], result['terminal_ns'], False)
    assert result['success_conditioned_recall_at_10'] == .9375 and not result['quality_gate_passed']


def port_check():
    cold = worker.cold
    header = dict(phase='ready', listen='127.0.0.1:8080', authority=AUTHORITY,
        remote_open_wall_ns=5, head_read_wall_ns=1, remote_open_stats=dict(
            staging_wall_ns=3, decode_wall_ns=2, metadata=[dict(name='manifest.json', bytes=1,
            chunks=1, get_wall_ns=1, stream_wall_ns=2, write_wall_ns=1)]))
    for port in (8080, 18085, 1024, 65535):
        for invalid_header in (False, True):
            stream = io.StringIO()
            server = Mock()
            server.poll.return_value = None
            def spawn(command, **kwargs):
                assert command[-1] == f'127.0.0.1:{port}'
                hdr = dict(header, listen=f'127.0.0.1:{port}')
                if invalid_header: hdr['authority'] = dict(AUTHORITY, generation=2)
                kwargs['stdout'].write(json.dumps(hdr)+'\n')
                kwargs['stdout'].flush()
                Path(command[3]).write_text('mock native time log\n')
                return server
            clock = iter(range(100, 1000, 10))
            with patch.object(cold.subprocess, 'Popen', side_effect=spawn), \
                 patch.object(cold.http.client, 'HTTPConnection') as connection, \
                 patch.object(cold.time, 'monotonic_ns', side_effect=lambda: next(clock)), \
                 patch.object(cold, 'post', return_value=(200, json.dumps(RESPONSE).encode())) as post, \
                 patch.object(cold, 'stop', return_value=dict(intentional_stop=True)) as stop:
                kwargs = {} if port == 8080 else dict(port=port)
                if invalid_header:
                    try: cold.cold_call('fixed', CONFIG, ITEM, b'{}', VALUES[0][1], VALUES[0][2], stream, **kwargs)
                    except AssertionError: pass
                    else: raise AssertionError('invalid header accepted')
                    failure = json.loads(stream.getvalue())
                    assert failure['native_server_log'] and failure['raw_response'] and failure['native_close']['intentional_stop']
                else:
                    row = cold.cold_call('fixed', CONFIG, ITEM, b'{}', VALUES[0][1], VALUES[0][2], **kwargs)
                    assert row['native_header']['listen'] == f'127.0.0.1:{port}'
                connection.assert_called_once_with('127.0.0.1', port, timeout=5)
                post.assert_called_once()
                stop.assert_called_once_with(server)
    with patch.object(cold.subprocess, 'Popen') as spawn:
        for port in (1023, 65536, True, 18080.0):
            try: cold.cold_call('fixed', CONFIG, ITEM, b'{}', VALUES[0][1], VALUES[0][2], port=port)
            except ValueError: pass
            else: raise AssertionError('invalid port accepted')
        spawn.assert_not_called()


def run_check():
    config = dict(CONFIG, items=[dict(ITEM, dataset=dataset, rows=1000000, dimensions=768,
                  inputs={name: {} for name in ('requests', 'reference-k10', 'truth')})
                  for dataset in ('ReLAION', 'CoHere')])
    calls = []
    completed_rows, completed_result = Scenario(hold_ns=60_000_000).measure()
    def fetch(bucket, identity, path):
        assert not calls, 'immutable fetch inside cell timing'
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.name == 'requests':
            rows = [dict(query_ordinal=q, query=[1.]+[0.]*767) for q in range(64)]
        elif path.name == 'reference-k10':
            rows = [dict(top_k=10, declared_panel_count=64, **AUTHORITY),
                    *[dict(RESPONSE, query_ordinal=q) for q in range(64)], dict(count=64)]
        else:
            path.write_bytes(struct.pack('<100I', *range(100))*64)
            return dict(path=str(path))
        path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
        return dict(path=str(path))
    def measure(binary, config, item, values, rate):
        calls.append((rate, item['dataset']))
        assert len(values) == 64
        rows, result = copy.deepcopy((completed_rows, completed_result))
        result['offered_qps'] = rate
        for row in rows: row.update(dataset=item['dataset'], offered_qps=rate)
        return rows, result
    with tempfile.TemporaryDirectory() as tmp, \
         patch.object(worker.cold, 'fetch', side_effect=fetch) as fetched, \
         patch.object(worker, 'measure', side_effect=measure):
        out = Path(tmp)/'out'
        summary = worker.run(config, 'qualified', out)
        assert fetched.call_count == 6
        assert calls == [(rate, dataset) for rate in worker.RATES for dataset in ('ReLAION', 'CoHere')]
        assert summary['complete'] and summary['offered'] == summary['successful'] == 768
        assert summary['eight_qps_attained'] and summary['largest_passing_tested_offered_qps'] == 8
        assert len(list(out.glob('rate*-records.jsonl'))) == 12
        for cell in summary['cells']:
            rows = [json.loads(line) for line in (out/cell['records_file']).read_text().splitlines()]
            assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
        assert json.loads((out/'summary.json').read_text()) == summary
    calls.clear()
    with tempfile.TemporaryDirectory() as tmp, \
         patch.object(worker.cold, 'fetch', side_effect=fetch), \
         patch.object(worker, 'measure', return_value=Scenario({0: AssertionError('bad ANN')}).measure()):
        out = Path(tmp)/'failed'
        try: worker.run(config, 'qualified', out)
        except RuntimeError: pass
        else: raise AssertionError('invalid arm continued')
        summary = json.loads((out/'summary.json').read_text())
        assert summary['arm_failed'] and not summary['complete'] and len(summary['cells']) == 1
        rows = [json.loads(line) for line in (out/'rate0-relaion-records.jsonl').read_text().splitlines()]
        assert rows[0]['failure_record']['raw_response'] == 'raw failure body'


if __name__ == '__main__':
    port_check()
    scheduler_check()
    run_check()
    print('cold offered checks PASS (bounded ports/overlap/drop/no queue/raw failures/tails/12 cells/6 fetches)')
