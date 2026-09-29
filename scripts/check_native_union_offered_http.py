"""AWS-only real-socket protocol checks; synthetic IDs, no ANN/data/quality claim."""
import hashlib
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import http.client

from scripts import run_native_union_offered_http as driver

AUTHORITY = dict(root_sha256='a' * 64, generation=1, control_epoch=1)


def reference(k):
    return dict(ids=list(range(k)), ranges=[[0, 200]], planned_bytes=200,
                submitted_gets=1, verified_bytes=200, failed_gets=0)


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        mode = body['query'][0]
        response = dict(authority=AUTHORITY, **reference(body['k']))
        status = 503 if mode == 2 else 200
        if mode == 3:
            response['authority'] = dict(AUTHORITY, control_epoch=2)
        if mode == 4:
            time.sleep(.15)
        raw = json.dumps(response).encode()
        try:
            self.send_response(status)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass


def check():
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    original = driver.connection
    driver.connection = lambda: http.client.HTTPConnection(*server.server_address, timeout=5)
    cases = []
    try:
        def run(modes, k=10, rate=8, workers=8, timeout=5):
            requests = [dict(query_ordinal=q, query=[mode]) for q, mode in enumerate(modes)]
            return driver.measure(requests, [reference(k) for _ in modes],
                                  [list(range(100)) for _ in modes], AUTHORITY,
                                  k=k, offered_qps=rate, workers=workers, timeout_seconds=timeout)

        rows, result = run([1] * 10)
        assert result['offered_count'] == result['successful_count'] == 10
        assert result['outcomes'] == {'success': 10}
        assert result['mean_successful_recall'] == result['mean_offered_recall'] == 1
        assert result['known_submitted_gets'] == 10 and result['known_verified_bytes'] == 2000
        assert result['physical_counters_complete'] and result['identity_parity_valid']
        assert result['scheduled_duration_ns'] == 1250000000
        assert result['achieved_successful_qps'] <= 8
        assert [r['scheduled_ns'] - rows[0]['scheduled_ns'] for r in rows] == [q * 125000000 for q in range(10)]
        assert all(r['completed_ns'] >= r['started_ns'] >= r['dispatched_ns'] >= r['scheduled_ns'] for r in rows)
        assert all(r['server_admission_ns'] is None for r in rows)
        assert not result['qualification'] and not result['matched_vendor_measured']
        cases.append('absolute8QPS_full10panel_k10_wire_parity_counters_denominator')

        rows, result = run([1], k=100)
        assert len(rows[0]['response']['ids']) == 100 and result['successful_count'] == 1
        cases.append('actual_k100_reference_geometry')

        rows, result = run([1, 2, 3, 4], timeout=.03)
        assert result['outcomes'] == dict(success=1, rejected_503=1, invalid_response=1, timeout=1), result['outcomes']
        assert result['offered_count'] == 4 and result['successful_count'] == 1
        assert result['mean_successful_recall'] == 1 and result['mean_offered_recall'] == .25
        assert not result['physical_counters_complete'] and not result['identity_parity_valid']
        assert all(value is not None for value in result['all_offered_terminal_ms'].values())
        cases.append('rejection_bad_head_timeout_all_offers_retained_no_false_counter_completeness')

        rows, result = run([4] * 8, rate=100, workers=1)
        assert result['outcomes'].get('client_capacity_drop', 0) > 0
        assert sum(result['outcomes'].values()) == 8
        assert all(r['started_ns'] is None for r in rows if r['outcome'] == 'client_capacity_drop')
        assert result['elapsed_including_drain_ns'] > result['scheduled_duration_ns']
        assert result['achieved_successful_qps'] == result['successful_count'] * 1e9 / result['elapsed_including_drain_ns']
        cases.append('bounded_nonqueued_client_overload_drops_drain_successQPS')

        for rate in [0, float('nan'), float('inf'), 1001]:
            try:
                run([1], rate=rate)
            except ValueError:
                pass
            else:
                raise AssertionError('invalid rate accepted')
        try:
            driver.measure([dict(query_ordinal=0, query=[1])], [reference(100)],
                           [list(range(100))], AUTHORITY, k=10, offered_qps=8)
        except ValueError:
            pass
        else:
            raise AssertionError('k100 prefix masquerading as actual k10 reference accepted')
        cases.append('invalid_rate_and_wrong_reference_rejected_before_dispatch')
    finally:
        driver.connection = original
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()
    return dict(passed=True, cases=cases, checks=len(cases), synthetic_protocol_only=True,
                ann_queries=0, dataset_quality_measured=False, full_native_assurance_repeated=False,
                source_sha256={name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in
                               ['scripts/run_native_union_offered_http.py', 'scripts/check_native_union_offered_http.py',
                                'scripts/rest_coexistence_load.py', 'scripts/run_native_union_http.py']})


if __name__ == '__main__':
    result = check()
    group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    result['cgroup'] = {name: (group / name).read_text() for name in
                       ['memory.peak', 'memory.swap.peak', 'memory.events', 'cpu.stat'] if (group / name).exists()}
    Path(sys.argv[1]).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
