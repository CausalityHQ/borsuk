"""AWS-only bounded offered-load measurement of the authenticated native HTTP API.

Server lifecycle, frozen inputs, RSS and infrastructure cost belong to the caller.
No retries, no client queue, no quality or vendor acceptance decision here.
"""
import concurrent.futures
import hashlib
import json
import math
import socket
import threading
import time
from collections import Counter

from scripts.rest_coexistence_load import scheduled_offsets_ns
from scripts.run_native_union_http import connection, post, quantile


def measure(requests, references, truth, authority, *, k, offered_qps,
            workers=8, timeout_seconds=5, connection_factory=None):
    connect = connection if connection_factory is None else connection_factory
    if not callable(connect):
        raise ValueError("callable HTTP connection factory required")
    count = len(requests)
    if k not in (10, 100) or not 1 <= count <= 10000:
        raise ValueError('k10/k100 and bounded nonempty panel required')
    if not math.isfinite(offered_qps) or not 0 < offered_qps <= 1000:
        raise ValueError('finite positive offered rate <=1000 required')
    if not 1 <= workers <= 128 or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError('bounded workers and finite positive timeout required')
    if len(references) != count or len(truth) != count:
        raise ValueError('one actual native reference and exact truth per offered query required')
    bodies = []
    for q, (request, reference, neighbors) in enumerate(zip(requests, references, truth)):
        if request['query_ordinal'] != q or len(reference['ids']) != k or len(set(reference['ids'])) != k:
            raise ValueError('ordered panel/native reference geometry')
        if len(neighbors) < k or len(set(neighbors[:k])) != k:
            raise ValueError('exact truth geometry')
        bodies.append(json.dumps(dict(query=request['query'], k=k, **authority),
                                 separators=(',', ':'), allow_nan=False).encode())
    offsets = scheduled_offsets_ns(offered_qps, count / offered_qps)
    if len(offsets) != count:
        raise ValueError('schedule must contain exactly the declared panel')
    permits = threading.BoundedSemaphore(workers)
    samples = [None] * count
    epoch = time.monotonic_ns() + 100_000_000

    def request_one(q, scheduled, dispatched):
        row = dict(query_ordinal=q, scheduled_ns=scheduled, dispatched_ns=dispatched,
                   started_ns=time.monotonic_ns(), server_admission_ns=None,
                   request_sha256=hashlib.sha256(bodies[q]).hexdigest(),
                   request_bytes=len(bodies[q]), status=None, outcome='transport_error',
                   integrity_error=None, physical_counters_complete=False)
        client = None
        try:
            client = connect()
            client.timeout = timeout_seconds
            status, raw = post(client, bodies[q])
            row.update(status=status, response_bytes=len(raw), outcome='http_error')
            # Preserve the wire completion time before JSON/parity/recall processing.
            row['completed_ns'] = time.monotonic_ns()
            if status == 200:
                response = json.loads(raw)
                expected = references[q]
                if response['authority'] != authority or any(response[key] != expected[key] for key in
                        ['ids', 'ranges', 'planned_bytes', 'submitted_gets', 'verified_bytes', 'failed_gets']):
                    raise ValueError('head/source/scorer/ordered-ID/physical reference parity')
                if (len(response['ids']) != k or len(set(response['ids'])) != k or
                        len(response['ranges']) != response['submitted_gets'] or
                        response['submitted_gets'] > 32 or
                        response['planned_bytes'] != response['verified_bytes'] or
                        response['verified_bytes'] > 16773120 or response['failed_gets'] != 0):
                    raise ValueError('native physical/identity bounds')
                row.update(outcome='success', returned_hits=len(set(response['ids']) & set(truth[q][:k])),
                           response=response, physical_counters_complete=True)
            elif status == 503:
                # Current authenticated boundary rejects before allocation/ANN/GET.
                row.update(outcome='rejected_503', physical_counters_complete=True)
            elif status == 502:
                response = json.loads(raw)
                row['response'] = response
        except (ValueError, KeyError, TypeError) as error:
            row.update(outcome='invalid_response', integrity_error=str(error))
        except (OSError, TimeoutError) as error:
            row.update(outcome='timeout' if isinstance(error, (TimeoutError, socket.timeout)) else 'transport_error',
                       transport_error=type(error).__name__)
        except Exception as error:
            row.update(outcome='client_error', integrity_error=type(error).__name__ + ': ' + str(error))
        finally:
            row.setdefault('completed_ns', time.monotonic_ns())
            if client is not None:
                client.close()
            samples[q] = row
            permits.release()
        return row

    # ponytail: one bounded thread pool; late dispatch and drops stay in the evidence.
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = []
        for q, offset in enumerate(offsets):
            scheduled = epoch + offset
            delay = (scheduled - time.monotonic_ns()) / 1e9
            if delay > 0:
                time.sleep(delay)
            dispatched = time.monotonic_ns()
            if permits.acquire(blocking=False):
                futures.append(pool.submit(request_one, q, scheduled, dispatched))
            else:
                samples[q] = dict(query_ordinal=q, scheduled_ns=scheduled, dispatched_ns=dispatched,
                                  started_ns=None, completed_ns=dispatched, server_admission_ns=None,
                                  status=None, outcome='client_capacity_drop',
                                  physical_counters_complete=True, integrity_error=None)
        for future in futures:
            future.result()
    duration_ns = round(count / offered_qps * 1e9)
    elapsed_ns = max(duration_ns, max(row['completed_ns'] for row in samples) - epoch)
    successful = [row for row in samples if row['outcome'] == 'success']
    outcomes = dict(Counter(row['outcome'] for row in samples))

    def tails(values):
        return {name: quantile(values, p) if values else None for name, p in
                [('p50', .5), ('p90', .9), ('p95', .95), ('p99', .99)]}

    result = dict(schema='borsuk-native-offered-http-v1', k=k, offered_count=count,
                  offered_qps=offered_qps, workers=workers, timeout_seconds=timeout_seconds,
                  scheduled_duration_ns=duration_ns, elapsed_including_drain_ns=elapsed_ns,
                  outcomes=outcomes, successful_count=len(successful),
                  achieved_successful_qps=len(successful) * 1e9 / elapsed_ns,
                  successful_completions_in_window=sum(row['completed_ns'] < epoch + duration_ns for row in successful),
                  mean_successful_recall=sum(row['returned_hits'] for row in successful) / (k * len(successful)) if successful else None,
                  mean_offered_recall=sum(row['returned_hits'] for row in successful) / (k * count),
                  successful_incoming_http_ms=tails([(row['completed_ns'] - row['started_ns']) / 1e6 for row in successful]),
                  successful_scheduled_to_completion_ms=tails([(row['completed_ns'] - row['scheduled_ns']) / 1e6 for row in successful]),
                  all_offered_terminal_ms=tails([(row['completed_ns'] - row['scheduled_ns']) / 1e6 for row in samples]),
                  dispatch_lag_ms=tails([(row['dispatched_ns'] - row['scheduled_ns']) / 1e6 for row in samples]),
                  client_queue_delay_ms=tails([(row['started_ns'] - row['dispatched_ns']) / 1e6 for row in samples if row['started_ns'] is not None]),
                  known_submitted_gets=sum(row.get('response', {}).get('submitted_gets') or 0 for row in samples),
                  known_verified_bytes=sum(row.get('response', {}).get('verified_bytes') or 0 for row in samples),
                  known_failed_gets=sum(row.get('response', {}).get('failed_gets') or 0 for row in samples),
                  physical_counters_complete=all(row['physical_counters_complete'] for row in samples),
                  identity_parity_valid=not any(row['integrity_error'] for row in samples),
                  server_admission_measured=False, server_rss_bytes=None, infrastructure_cost_usd=None,
                  percentile_method='linear interpolation at (n-1)*p', qualification=False,
                  matched_vendor_measured=False)
    return samples, result
