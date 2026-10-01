#!/usr/bin/env python3
"""Offline paired metadata wave audit; no native/cloud calls.

CLI: OUTPUT CONFIG_SHA CONTROL_BINARY CANDIDATE_BINARY | --self-check
Uses the retained per-role manifests, frozen checkers, completed proofs and
input bodies, then revalidates raw records and exact saved-summary parity.
"""
import json
from pathlib import Path
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def check_saved(output, config_sha, control_binary, candidate_binary):
    from scripts import run_native_semantic_metadata_cold as runtime
    worker, stats = runtime.worker, runtime.stats
    out = Path(output).absolute()
    stats.require(runtime.old.sha(out / 'config.json') == stats.digest(config_sha), 'saved config identity')
    config = json.loads((out / 'config.json').read_text())
    runtime.validate_config(config)
    evidence = runtime.authenticate(config, out)
    runtime.validate_runtime(config, dict(control=control_binary, candidate=candidate_binary),
                             {r: out / (r + '-proof.json') for r in runtime.ROLES}, evidence)

    def saved_input(bucket, identity, path):
        size, digest = identity.get('range_bytes', identity['bytes']), worker.input_sha(identity)
        stats.require(path.stat().st_size == size and runtime.old.sha(path) == digest, 'saved input identity: ' + str(path))
        return dict(path=str(path), bytes=size, sha256=digest)

    panels = runtime.prepare(config, out, evidence, fetch=saved_input)
    offered = config['schema'] == runtime.OFFERED_SCHEMA
    records, markers = [], []
    paths = ([out / worker.offered_name(i, d, a) for i, _, d, a in worker.offered_order()]
             if offered else [out / 'records.jsonl'])
    for path in paths:
        raw = path.read_bytes()
        rows = [json.loads(line) for line in raw.splitlines()]
        stats.require(raw == ''.join(worker.encoded(r) + '\n' for r in rows).encode(), 'canonical exact raw ledger')
        records.extend(rows)
        if offered:
            markers.append(json.loads(path.with_name(path.name.replace('-records.jsonl', '-summary.json')).read_bytes()))
    saved = json.loads((out / 'summary.json').read_text())
    actual = (runtime.reduce_offered if offered else runtime.reduce_run)(records, panels, config, config_sha, evidence,
                                saved['campaign_cgroup_before'], saved['campaign_cgroup_after'])
    actual.update(inputs=runtime.input_receipts(panels), identity_gate_passed=actual.get('identity_gate_passed', True),
                  closed=actual['process_cleanup_complete'])
    if offered:
        for cell, marker in zip(actual['cells'], markers):
            stats.require(marker == runtime.closed_cell_summary(cell, config, config_sha, evidence), 'cell marker parity')
        stats.require(saved == actual, 'saved offered summary/gates/tails/identities parity')
        return {k: actual[k] for k in ('execution_gate_passed', 'offered_gate_passed', 'identity_gate_passed',
            'process_cleanup_complete', 'bounded_memory_gate_passed', 'quality_gate_passed', 'all_calls_successful')} | dict(records=len(records))
    stats.require(saved == actual, 'saved summary/gates/tails/identities parity')
    return dict(records=len(records), execution_gate_passed=actual['execution_gate_passed'],
                paired_gate_passed=actual['paired_gate_passed'], latency_gate_passed=actual['latency_gate_passed'],
                all_calls_successful=actual['all_calls_successful'], quality_gate_passed=actual['quality_gate_passed'],
                process_cleanup_complete=actual['process_cleanup_complete'], resource_gate=actual['resource_gate'])


def self_check(offered=False):
    from scripts import run_native_semantic_metadata_cold as runtime
    assert callable(getattr(runtime, 'run', None)), 'serial paired adapter missing'
    import base64
    import copy
    from contextlib import ExitStack, redirect_stdout
    import io
    import os
    import shutil
    import struct
    import tempfile
    from unittest.mock import Mock, patch
    from scripts.check_native_semantic_concurrency import fixture

    worker, old, stats = runtime.worker, runtime.old, runtime.stats
    root = runtime.ROOT / 'docs/research/performance-architecture-20260930/semantic-cold'
    originals = {n: getattr(worker, n) for n in ('stats', 'measured_call', 'reduce_run', 'validate_record', 'encoded')}
    parent_environment = dict(os.environ)

    def rejected(call):
        try:
            call()
        except (ValueError, KeyError, TypeError, OSError, AssertionError):
            return
        raise AssertionError('invalid evidence accepted')

    def ptr(path):
        return dict(path=str(path), bytes=path.stat().st_size, sha256=old.sha(path))

    def dump(path, value):
        path.write_text(worker.encoded(value) + '\n')

    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        config = dict(runtime.FIXED, schema=runtime.SCHEMA, credential_protocol=stats.CREDENTIAL_PROTOCOL,
                      bucket='synthetic-never-fetched', region='synthetic', items=[], native_arms={},
                      checker_authority=ptr(root / 'metadata-waves/checker-authority.json'),
                      code_sha256={name: old.sha(runtime.ROOT / name) for name in runtime.CODE})
        binaries, proofs, mock_binary_ids = {}, {}, {}
        for role, source in (('control', root / 'metadata-head/native-source-manifest.json'),
                             ('candidate', root / 'metadata-waves/native-source-manifest.json')):
            binary, proof_path = directory / (role + '-binary'), directory / (role + '-proof.json')
            binary.write_bytes(('synthetic ' + role + ' binary NEVER EXECUTED').encode())
            manifest = json.loads(source.read_text())
            inventory = manifest['source_sha256']
            compiled = {n: d for n, d in inventory.items() if n in ('Cargo.toml', 'Cargo.lock', 'crates/borsuk/Cargo.toml')
                        or n.startswith(('crates/borsuk/src/', 'crates/borsuk/examples/'))}
            proof = dict(qualified=True, full_workspace_execution_pending=False, current_full_suite_pass_claim=True,
                         **{n: 0 for n in runtime.PROOF_GATES}, binary_bytes=binary.stat().st_size,
                         binary_sha256=old.sha(binary), source_file_count=399,
                         source_identity_sha256=manifest['source_identity_sha256'], compiled_native_sha256=compiled)
            dump(proof_path, proof)
            config['native_arms'][role] = dict(source_manifest=ptr(source), binary=ptr(binary), proof=ptr(proof_path))
            binaries[role], proofs[role] = str(binary), str(proof_path)
            mock_binary_ids[role] = {n: ptr(binary)[n] for n in ('bytes', 'sha256')}

        bodies = {}

        def bind(key, body):
            bodies[key] = body
            return dict(key=key, bytes=len(body), sha256=runtime.sha_body(body))

        call_fixture = fixture('semantic', 8080)
        # Both roles have three metadata HEADs. Only the candidate emits waves.
        requests = ''.join(worker.encoded(dict(query_ordinal=q, query=[q + 1] + [0.] * 767)) + '\n' for q in range(64)).encode()
        truth = struct.pack('<100I', *range(10, 110)) * 64
        for dataset in worker.DATASETS:
            arm = copy.deepcopy(call_fixture['arm'])
            arm.pop('dataset')
            arm['indexes'] = {'10': dataset + '/same-semantic-generation'}
            identity = {n: runtime.sha_body(n.encode()) for n in worker.SOURCE_IDENTITIES}
            identity.update(mean_sha256=arm['metadata_sha256']['plane/mean.bin'],
                            queries_sha256=runtime.sha_body(requests), truth_sha256=runtime.sha_body(truth))
            item = dict(dataset=dataset, rows=100000, dimensions=768, metric='cosine',
                        query_split='FIRST100k D768 cosine consumed development ordinals0..63',
                        source_identity=identity, inputs=dict(requests=bind(dataset + '/requests', requests),
                                                             truth=bind(dataset + '/truth', truth)))
            header = dict(top_k=10, declared_panel_count=64, rows=100000, dimensions=768, metric='cosine',
                          discovery='semantic', authority=arm['authority'], query_split=item['query_split'], source_identity=identity)
            references = [header, *[dict(query_ordinal=q, **{n: call_fixture['response'][n] for n in stats.PARITY})
                                    for q in range(64)], dict(count=64)]
            reference = ''.join(worker.encoded(r) + '\n' for r in references).encode()
            arm['inputs'] = {'reference-k10': bind(dataset + '/same-reference', reference)}
            item['arms'] = {role: copy.deepcopy(arm) for role in runtime.ROLES}
            config['items'].append(item)

        def fetch(bucket, identity, path):
            assert bucket == config['bucket']
            body = bodies[identity['key']]
            assert len(body) == identity['bytes'] and runtime.sha_body(body) == identity['sha256']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            return dict(path=str(path), bytes=len(body), sha256=runtime.sha_body(body))

        snapshot = dict(path='/synthetic/cgroup', observed_ns=1, diagnostics={'io.stat': {'type': 'OSError'}},
                        files={'memory.max': '8589934592', 'memory.swap.max': '0', 'memory.current': '64',
                               'memory.peak': '128', 'memory.swap.current': '0', 'memory.swap.peak': '0',
                               'memory.events': 'oom 0\noom_kill 0\noom_group_kill 0', 'cpu.stat': 'usage_usec 1'})
        calls, paths, running, stopped, clients_closed = [], [], [], [], []
        fault = None
        campaign_calls = 0
        clock = 0

        def tick():
            nonlocal clock
            step = 100000 if running and running[0].role == 'candidate' else 200000
            clock += 300000 if fault == 'latency' else step
            return clock

        def sample():
            value = copy.deepcopy(snapshot)
            if fault == 'resource-after' and len(calls) > campaign_calls:
                value['files']['memory.swap.peak'] = '1'
            return value

        def profile(role):
            value = copy.deepcopy(call_fixture)
            metadata = value['header']['remote_open_stats']
            if role == 'control' or fault == 'missing-waves':
                for row in metadata['metadata']:
                    del row['metadata_wave'], row['metadata_wave_wall_ns']
            else:
                # Real overlapping waits exceed staging when added per object.
                metadata['staging_wall_ns'] = 30
                value['header']['remote_open_wall_ns'] = 60
                for row in metadata['metadata']:
                    row['stream_wall_ns'] = 8
            return value

        def spawn(command, **kwargs):
            assert not running, 'native calls overlap'
            role = next(r for r in runtime.ROLES if command[11] == binaries[r])
            dataset = command[14].split('/')[0]
            assert command[4:11] == ['timeout', '--signal=TERM', '--kill-after=5', '60', 'taskset', '-c', '0-3']
            assert command[14] == dataset + '/same-semantic-generation'
            assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES'] == '536870912'
            assert kwargs['env']['AWS_MAX_ATTEMPTS'] == '1' and kwargs['env']['TOKIO_WORKER_THREADS'] == '4'
            value = profile(role)
            kwargs['stdout'].write(worker.encoded(value['header']) + '\n')
            kwargs['stdout'].flush()
            path = Path(command[3])
            path.write_text('Maximum resident set size (kbytes): 1\nUser time (seconds): 0.01\nSystem time (seconds): 0.00\n')
            paths.append(path.parent)
            server = Mock(role=role, dataset=dataset, value=value)
            server.poll.return_value = None
            running.append(server)
            return server

        def stop(server):
            assert running.pop() is server
            stopped.append(server.role)
            return dict(intentional_stop=True, returncode=None if fault == 'cleanup' else 143)

        class Connection:
            def __init__(self, host, port, timeout):
                assert (host, port, timeout) == ('127.0.0.1', 8080, 5)
                self.server = running[0]

            def connect(self):
                pass

            def request(self, method, path, body, headers):
                assert (method, path) == ('POST', '/search')
                request = json.loads(body)
                assert request['k'] == 10 and {n: request[n] for n in call_fixture['arm']['authority']} == call_fixture['arm']['authority']
                calls.append((self.server.dataset, self.server.role, request['query'][0] - 1))

            def getresponse(self):
                raw = b'\xff\x00raw fatal' if fault == 'raw' else worker.encoded(self.server.value['response']).encode()
                return Mock(status=502 if fault == 'raw' else 200, read=lambda: raw)

            def close(self):
                clients_closed.append(self.server.role)

        with ExitStack() as stack:
            stack.enter_context(patch.object(runtime, 'BINARY_IDS', mock_binary_ids))
            stack.enter_context(patch.object(old, 'fetch', side_effect=fetch))
            stack.enter_context(patch.object(old.subprocess, 'Popen', side_effect=spawn))
            stack.enter_context(patch.object(old.http.client, 'HTTPConnection', Connection))
            stack.enter_context(patch.object(old, 'stop', side_effect=stop))
            stack.enter_context(patch.object(old.time, 'monotonic_ns', side_effect=tick))
            stack.enter_context(patch.object(worker, 'offered_cgroup_snapshot', side_effect=sample))
            stack.enter_context(patch.object(runtime.os, 'sched_getaffinity', return_value={4, 5}))
            stack.enter_context(patch.object(runtime.resource, 'getrlimit', return_value=(4294967296,) * 2))
            stack.enter_context(patch.dict(os.environ, AWS_MAX_ATTEMPTS='1'))
            runtime.validate_config(config)
            evidence = runtime.authenticate(config)
            runtime.validate_runtime(config, binaries, proofs, evidence)
            for role in runtime.ROLES:
                header = profile(role)['header']
                arm = config['items'][0]['arms'][role]
                ready = evidence['checkers'][role].validate_ready(header, arm)
                assert ready['metadata']['logical_metadata_head_requests'] == 3
                rejected(lambda: evidence['checkers']['candidate' if role == 'control' else 'control'].validate_ready(header, arm))
            stats.validate_ready(profile('candidate')['header'], config['items'][0]['arms']['candidate'])
            rejected(lambda: stats.validate_ready(profile('control')['header'], config['items'][0]['arms']['control']))
            for mutation in ('wave', 'wall', 'buffer', 'gets', 'heads'):
                header = profile('candidate')['header']
                row = header['remote_open_stats']['metadata'][1]
                field = dict(wave='metadata_wave', wall='metadata_wave_wall_ns',
                             buffer='payload_buffer_bound_bytes', gets='logical_get_requests',
                             heads='logical_head_requests')[mutation]
                row[field] += 1
                rejected(lambda: evidence['checkers']['candidate'].validate_ready(header, config['items'][0]['arms']['candidate']))
            for field in runtime.FIXED:
                bad = copy.deepcopy(config)
                bad[field] = None
                rejected(lambda: runtime.validate_config(bad))
            for mutation in ('index', 'graph', 'code', 'binary-role', 'checker', 'split'):
                bad = copy.deepcopy(config)
                if mutation == 'index': bad['items'][0]['arms']['candidate']['indexes']['10'] += '/different'
                elif mutation == 'graph':
                    for arm in bad['items'][0]['arms'].values(): arm['discovery'] = 'graph'
                elif mutation == 'code': bad['code_sha256'].pop(next(iter(bad['code_sha256'])))
                elif mutation == 'binary-role': bad['native_arms']['candidate']['binary'] = bad['native_arms']['control']['binary']
                elif mutation == 'checker': bad['checker_authority']['sha256'] = '0' * 64
                else: bad['items'][0]['query_split'] = 'holdout'
                rejected(lambda: (runtime.validate_config(bad), runtime.authenticate(bad)))
            for role in runtime.ROLES:
                proof_path = Path(proofs[role])
                original = proof_path.read_bytes()
                for field, value in [*( (n, False) for n in runtime.PROOF_GATES),
                                      ('full_workspace_execution_pending', True), ('current_full_suite_pass_claim', False),
                                      ('binary_sha256', '0' * 64), ('source_identity_sha256', '0' * 64),
                                      ('compiled_native_sha256', {}), ('binary_bytes', 1), ('qualified', False)]:
                    bad = copy.deepcopy(config)
                    proof = json.loads(original)
                    proof[field] = value
                    dump(proof_path, proof)
                    bad['native_arms'][role]['proof'] = ptr(proof_path)
                    rejected(lambda: runtime.validate_runtime(bad, binaries, proofs, evidence))
                proof_path.write_bytes(original)
            # Rebind a changed manifest: full aggregate authentication still rejects it.
            changed = directory / 'changed-source.json'
            manifest = copy.deepcopy(evidence['manifests']['control'])
            manifest['source_sha256']['Cargo.lock'] = '0' * 64
            dump(changed, manifest)
            bad = copy.deepcopy(config)
            bad['native_arms']['control']['source_manifest'] = ptr(changed)
            rejected(lambda: runtime.authenticate(bad))
            cfg = directory / 'config.json'
            dump(cfg, config)
            digest = old.sha(cfg)

            def launch(name):
                nonlocal campaign_calls
                campaign_calls = len(calls)
                out = directory / name
                with redirect_stdout(io.StringIO()):
                    status = runtime.main([str(cfg), old.sha(cfg), binaries['control'], proofs['control'],
                                           binaries['candidate'], proofs['candidate'], str(out)])
                return out, status

            # Pending/missing proof never reaches even the mocked preparation.
            pp = Path(proofs['candidate'])
            original = pp.read_bytes()
            pending = json.loads(original)
            pending['full_workspace_execution_pending'] = True
            dump(pp, pending)
            bad = copy.deepcopy(config)
            bad['native_arms']['candidate']['proof'] = ptr(pp)
            dump(cfg, bad)
            count = len(calls)
            rejected(lambda: launch('pending'))
            assert not (directory / 'pending').exists() and len(calls) == count
            pp.write_bytes(original)
            dump(cfg, config)
            missing = dict(proofs, candidate=str(directory / 'missing-proof'))
            rejected(lambda: runtime.validate_runtime(config, binaries, missing, evidence))

            out, status = launch('success')
            assert status == 0
            expected_order = [(d, a, q) for d, b, a, q in worker.execution_order()]
            assert calls == expected_order and len(calls) == len(stopped) == len(clients_closed) == 512
            assert not running and all(not p.exists() for p in paths), 'native/client/temp cleanup'
            assert all(getattr(worker, n) is v for n, v in originals.items()), 'worker hooks leaked'
            result = check_saved(out, digest, *[binaries[r] for r in runtime.ROLES])
            assert result['paired_gate_passed'] and result['records'] == 512
            relocated = directory / 'relocated-offline-copy'
            shutil.copytree(out, relocated)
            assert check_saved(relocated, digest, *[binaries[r] for r in runtime.ROLES])['execution_gate_passed']
            summary = json.loads((out / 'summary.json').read_text())
            assert summary['speedup_claim'] is False and 'latency_improvement' not in summary
            for dataset in worker.DATASETS:
                pooled = summary['datasets'][dataset]['pooled']
                assert pooled['control']['known_logical_charge_totals']['metadata_HEADs'] == 128 * 3
                assert pooled['candidate']['known_logical_charge_totals']['metadata_HEADs'] == 128 * 3
                assert summary['datasets'][dataset]['quality_delta_at_10'] == 0
                assert summary['datasets'][dataset]['quality_delta_percentage_points'] == 0
                assert pooled['candidate']['latency_ms']['metadata_wave_critical']['p90'] == 30 / 1e6
                assert pooled['control']['latency_ms']['metadata_wave_critical'] == 'UNMEASURED'
                assert 'metadata_stream_and_output' not in pooled['candidate']['latency_ms']
                assert pooled['candidate']['metadata_object_wait_sum_ms']['metadata_stream_and_output']['p90'] == 64 / 1e6

            ledger = out / 'records.jsonl'
            original_ledger = ledger.read_bytes()
            rows = [json.loads(line) for line in original_ledger.splitlines()]
            if offered:
                templates = [next(r for r in rows if r['dataset'] == d and r['arm'] == a)
                             for d in worker.DATASETS for a in runtime.ROLES]
                del rows, original_ledger
                offered_environment = dict(os.environ)
                offered_self_check(runtime, config, binaries, proofs, evidence, templates, directory, rejected)
                assert not running and all(not p.exists() for p in paths)
                assert dict(os.environ) == offered_environment
                return
            panels = runtime.prepare(config, out, evidence, fetch=fetch)
            # The exact decision admits equal p95, but never equal p90 or a loss on either dataset.
            for failure in (None, 'equal-p90', 'p95-regression', 'CoHere-slower'):
                timed = copy.deepcopy(rows)
                for i, row in enumerate(timed):
                    duration = 600000
                    if row['arm'] == 'candidate':
                        duration = 600000 if row['query_ordinal'] >= 59 else 300000
                        if failure == 'equal-p90': duration = 600000
                        if failure == 'p95-regression' and row['query_ordinal'] >= 59: duration = 900000
                        if failure == 'CoHere-slower' and row['dataset'] == 'CoHere': duration = 900000
                    start = (i + 1) * 10**9
                    row.update(started_ns=start, successful_connect_attempt_ns=start + 1000,
                        connected_ns=start + 2000, completed_ns=start + duration,
                        cold_start_to_first_http_response_ns=duration, before_successful_connect_attempt_ns=1000,
                        successful_tcp_connect_ns=1000, first_post_to_response_ns=duration - 2000,
                        incoming_http_wall_ns=duration - 1000)
                reduced = runtime.reduce_run(timed, panels, config, digest, evidence, snapshot, snapshot)
                assert reduced['execution_gate_passed'] and reduced['paired_gate_passed'] == (failure is None)
            del timed, panels
            for mutation in ('role', 'checker', 'proof', 'source', 'raw-ready', 'raw-query', 'cap', 'resource', 'cleanup', 'order',
                             'bool-ordinal', 'bool-block'):
                bad = copy.deepcopy(rows)
                row = bad[0]
                if mutation in ('role', 'checker', 'proof', 'source'):
                    field = {'role': 'native_role', 'checker': 'checker_sha256', 'proof': 'proof_sha256', 'source': 'native_source_identity_sha256'}[mutation]
                    row[field] = 'wrong'
                elif mutation == 'raw-ready': row['native_server_log'] += worker.encoded(row['native_header']) + '\n'
                elif mutation == 'raw-query': row['raw_response_base64'] = base64.b64encode(b'{}').decode()
                elif mutation == 'cap': row['response']['source_submitted_gets'] = 129
                elif mutation == 'resource': row['cgroup_after']['files']['memory.swap.peak'] = '1'
                elif mutation == 'cleanup': row['temporary_directory_cleanup'] = False
                elif mutation == 'order': bad[0], bad[1] = bad[1], bad[0]
                elif mutation == 'bool-ordinal': bad[1]['query_ordinal'] = True
                else: bad[64]['block'] = True
                ledger.write_text(''.join(worker.encoded(r) + '\n' for r in bad))
                rejected(lambda: check_saved(out, digest, *[binaries[r] for r in runtime.ROLES]))
            ledger.write_bytes(original_ledger)
            for file in ('candidate-checker.py.gz', 'candidate-proof.json', 'control-source.json', 'inputs/ReLAION/truth'):
                path = out / file
                original = path.read_bytes()
                path.write_bytes(original + b'corrupt')
                rejected(lambda: check_saved(out, digest, *[binaries[r] for r in runtime.ROLES]))
                path.write_bytes(original)
            saved_summary = (out / 'summary.json').read_bytes()
            summary['datasets']['ReLAION']['pooled']['candidate']['latency_ms']['whole_cold']['p95'] += 1
            dump(out / 'summary.json', summary)
            rejected(lambda: check_saved(out, digest, *[binaries[r] for r in runtime.ROLES]))
            (out / 'summary.json').write_bytes(saved_summary)
            assert check_saved(out, digest, *[binaries[r] for r in runtime.ROLES])['execution_gate_passed']
            del rows, bad, original_ledger

            for failure in ('raw', 'missing-waves', 'cleanup', 'resource-after', 'resource'):
                fault = failure
                if failure == 'resource': snapshot['files']['memory.swap.peak'] = '1'
                offset = len(calls)
                failed, status = launch(failure)
                assert status == 1
                if failure == 'resource':
                    assert len(calls) == offset and not (failed / 'records.jsonl').exists(), 'pre-admission must precede preparation/native'
                    snapshot['files']['memory.swap.peak'] = '0'
                    continue
                failed_rows = [json.loads(line) for line in (failed / 'records.jsonl').read_text().splitlines()]
                n = 65 if failure == 'missing-waves' else 1
                assert len(calls) - offset == n and len(failed_rows) == 512
                assert failed_rows[n - 1]['outcome'] == 'failed'
                assert all(r['outcome'] == 'aborted' and r['http_attempts'] == 0 for r in failed_rows[n:])
                assert all(r['native_role'] == r['arm'] and r['config_sha256'] == digest for r in failed_rows)
                if failure == 'raw': assert base64.b64decode(failed_rows[0]['raw_response_base64']) == b'\xff\x00raw fatal'
                result = check_saved(failed, digest, *[binaries[r] for r in runtime.ROLES])
                assert not result['execution_gate_passed']
                assert all(getattr(worker, n) is v for n, v in originals.items())
            fault = 'latency'
            slow, status = launch('latency-fail')
            result = check_saved(slow, digest, *[binaries[r] for r in runtime.ROLES])
            assert status == 1 and result['execution_gate_passed'] and not result['paired_gate_passed']
            fault = None
            # Quality failure is an explicit abort at the end of the first candidate block.
            bad = copy.deepcopy(config)
            for item in bad['items']:
                new_truth = struct.pack('<100I', *range(100)) * 64
                item['inputs']['truth'] = bind(item['dataset'] + '/low-quality-truth', new_truth)
                item['source_identity']['truth_sha256'] = runtime.sha_body(new_truth)
                for arm in item['arms'].values():
                    reference_key = arm['inputs']['reference-k10']['key']
                    reference = [json.loads(line) for line in bodies[reference_key].splitlines()]
                    reference[0]['source_identity'] = item['source_identity']
                    arm['inputs']['reference-k10'] = bind(reference_key + '-low-quality', ''.join(worker.encoded(r) + '\n' for r in reference).encode())
            dump(cfg, bad)
            offset = len(calls)
            low, status = launch('low-quality')
            assert status == 1 and len(calls) - offset == 128
            low_rows = [json.loads(line) for line in (low / 'records.jsonl').read_text().splitlines()]
            assert low_rows[128]['abort_after']['reason'] == 'candidate block quality below 608/640'
            assert not check_saved(low, old.sha(cfg), *[binaries[r] for r in runtime.ROLES])['quality_gate_passed']
            # Exercise finally restoration on an exception, independently of valid reductions.
            try:
                with runtime.scoped(stats=evidence['checkers']['control'], measured_call=lambda: None):
                    raise RuntimeError('deliberate scoped failure')
            except RuntimeError:
                pass
            assert all(getattr(worker, n) is v for n, v in originals.items())
            assert not running and len(stopped) == len(clients_closed) == len(calls)
            assert all(not p.exists() for p in paths)
    assert dict(os.environ) == parent_environment
    print('paired metadata waves self-check PASS: 512 serial calls, both 3 HEADs, wave overlap/bounds, p90/p95 decision, fail/abort/cleanup and offline tamper replay; native/cloud UNRUN')


def offered_self_check(runtime, config, binaries, proofs, evidence, serial_rows, directory, rejected):
    """Real scheduler/role dispatch; virtual-time campaigns retain real reducers."""
    import base64
    import copy
    import gzip
    import io
    import threading
    import time
    from contextlib import redirect_stdout
    from unittest.mock import patch
    from scripts import run_native_cold_offered as scheduler
    from scripts import launch_native_semantic_metadata_cold_spot as controller
    worker, old = runtime.worker, runtime.old
    config = dict(config, **runtime.OFFERED_FIXED)
    config.pop('blocks')
    config.update(schema=runtime.OFFERED_SCHEMA, code_sha256={n: old.sha(runtime.ROOT/n) for n in runtime.OFFERED_CODE})
    cfg = directory/'offered-config.json'
    cfg.write_text(worker.encoded(config)+'\n')
    digest = old.sha(cfg)
    for field in runtime.OFFERED_FIXED:
        bad = copy.deepcopy(config)
        bad[field] = None
        rejected(lambda: runtime.validate_config(bad))
    templates = {(d, r): next(row for row in serial_rows if row['dataset'] == d and row['arm'] == r)
                 for d in worker.DATASETS for r in runtime.ROLES}
    panels = runtime.prepare(config, directory/'prepared-offered', evidence)
    mode, clock = [None], [10**9]
    historical = runtime.ROOT/'docs/research/performance-architecture-20260930/semantic-cold/offered/a0003/screen/rate3-cohere-candidate-records.jsonl.gz'
    historical_body = gzip.decompress(historical.read_bytes())
    terminal = json.loads((historical.parent.parent/'aws-terminal.json').read_bytes())
    assert terminal['artifacts']['screen/rate3-cohere-candidate-records.jsonl'] == dict(
        bytes=len(historical_body), sha256=runtime.sha_body(historical_body))
    prior = next(json.loads(line) for line in historical_body.splitlines()
                 if json.loads(line)['outcome'] == 'failed')
    assert dict(prior['response']['transport']['totals']['status_counts'])[503] == 1

    def mock_measured(binary, cfg, arm, body, expected, truth, *, port):
        role = arm['native_role']
        assert binary == binaries[role]
        dataset = arm['indexes']['10'].split('/')[0]
        row = copy.deepcopy(templates[dataset, role])
        row['native_header']['listen'] = f'127.0.0.1:{port}'
        row.update(native_server_log=worker.encoded(row['native_header'])+'\n',
                   request_bytes=len(body), request_sha256=runtime.sha_body(body))
        # Exercise checker selection inside the actual measured-call thread.
        worker.stats.validate_ready(row['native_header'], arm)
        if mode[0] in ('503', 'forged503', 'corrupt503') and dataset == 'CoHere' and role == 'candidate':
            response = row['response']
            for name in ('ids', 'ranges', 'planned_bytes'):
                response.pop(name)
            response.update(error=prior['response']['error'], router_failed_gets=1)
            final = response['transport']['totals']
            final['status_counts'][0][1] -= 1
            final['status_counts'].append([503, 1])
            final['dropped_error_bodies'] = 1
            row.update(outcome='failed', error_type='AssertionError', error=prior['error'], http_status=502,
                       valid_ann_requests=0, telemetry_validation_errors=[])
            row['accounting'] = worker.stats.validate_outcome(row['native_header'], response, arm, False)
            row['raw_response'] = worker.encoded(response)
            row['raw_response_base64'] = base64.b64encode(row['raw_response'].encode()).decode()
            if mode[0] == 'forged503': row['accounting']['final_process_transport']['status_counts'][-1][1] += 1
            if mode[0] == 'corrupt503': row['raw_response_base64'] = base64.b64encode(b'{}').decode()
        if mode[0] == 'cleanup' and dataset == 'CoHere' and role == 'candidate':
            row['native_close']['returncode'] = None
        if mode[0] == 'resource' and dataset == 'CoHere' and role == 'candidate':
            row['cgroup_after']['files']['memory.swap.peak'] = '1'
        return row

    def fake_schedule(call_one, rate, **kwargs):
        epoch = clock[0]
        rows, abort = [], None
        for q in range(64):
            dispatch = epoch + round(q*1e9/rate) + 1000000
            if abort is not None:
                rows.append(dict(query_ordinal=q, offered_qps=rate, scheduled_ns=epoch+round(q*1e9/rate),
                    dispatched_ns=None, started_ns=None, completed_ns=None, port=None, outcome='aborted',
                    namespace_start_attempted=False, native_process_started=False, http_attempts=0,
                    valid_ann_requests=0, abort_after=abort, terminal_ns=clock[0]))
                continue
            row = call_one(q, 18080)
            row.update(query_ordinal=q, offered_qps=rate, scheduled_ns=epoch+round(q*1e9/rate),
                dispatched_ns=dispatch, port=18080, started_ns=dispatch+1000000,
                successful_connect_attempt_ns=dispatch+2000000, connected_ns=dispatch+3000000,
                completed_ns=dispatch+5000000, terminal_ns=dispatch+25000000,
                cold_start_to_first_http_response_ns=4000000, before_successful_connect_attempt_ns=1000000,
                successful_tcp_connect_ns=1000000, first_post_to_response_ns=2000000, incoming_http_wall_ns=3000000)
            clock[0] = row['terminal_ns']
            if mode[0] == 'timing' and row['native_role'] == 'candidate' and row['response'].get('ids'):
                # Same valid response; an excessive dispatch delay fails capacity qualification.
                for key in ('dispatched_ns', 'started_ns', 'successful_connect_attempt_ns', 'connected_ns', 'completed_ns', 'terminal_ns'):
                    row[key] += 125000001
                clock[0] = row['terminal_ns']
            if row['abort_admissions']:
                abort = dict(query_ordinal=q, reason='cleanup unconfirmed' if not row['cleanup_confirmed']
                             else 'fatal call failure', observed_ns=row['terminal_ns'])
            rows.append(row)
        clock[0] += 1000000
        return rows, epoch, clock[0], abort

    def tick():
        clock[0] += 1000
        return clock[0]

    with patch.object(worker, 'measured_call', side_effect=mock_measured), \
            patch.object(old.time, 'monotonic_ns', side_effect=tick):
        # Six concurrent calls alternate frozen checker roles without global mutation.
        barrier = threading.Barrier(6, timeout=3)
        errors, owned = [], []
        def overlap(q, port):
            role = runtime.ROLES[q % 2]
            try:
                arm = panels['ReLAION']['arms'][role]
                row = worker.offered_call(None, config, panels['ReLAION'], arm, q, port)
                owned.append(port)
                if q < 6:
                    barrier.wait()
                    threading.Event().wait(.025)
                return row
            except Exception as error:
                errors.append(error)
                raise
        with runtime.offered_hooks(config, digest, evidence, binaries, proofs):
            # Compress only schedule offsets for this ownership stress check.
            previous_stack = threading.stack_size(256*1024)
            try:
                with patch.object(scheduler, 'scheduled_offsets_ns', return_value=list(range(64))), patch.object(scheduler.time, 'sleep'):
                    concurrent, _, _, _ = scheduler.schedule_offers(overlap, 1000)
            finally:
                threading.stack_size(previous_stack)
        assert not errors and len(set(owned[:6])) == 6 and any(r['outcome'] == 'capacity_drop' for r in concurrent)
        assert all(r['cleanup_confirmed'] for r in concurrent if r['port'] is not None)
        for failure in (None, 'timing', '503', 'cleanup', 'resource', 'forged503', 'corrupt503', 'callback'):
            mode[0], clock[0] = failure, 10**9
            out = directory/('offered-'+str(failure))
            markers = []
            def closed(marker, paths):
                if failure == 'callback': raise RuntimeError('conditional PUT failed')
                assert paths['records'].parent == out
                controller._cell_bodies(marker, paths, dict(config, config_sha256=digest))
                markers.append(marker)
            with patch.object(scheduler, 'schedule_offers', side_effect=fake_schedule), redirect_stdout(io.StringIO()):
                status = runtime.main([str(cfg), digest, binaries['control'], proofs['control'],
                                       binaries['candidate'], proofs['candidate'], str(out)], on_cell_closed=closed)
            if failure in ('callback', 'forged503', 'corrupt503'):
                assert status == 1
                continue
            assert status == (1 if failure in ('cleanup', 'resource') else 0), (failure, json.loads((out/'summary.json').read_bytes()))
            result = check_saved(out, digest, *[binaries[r] for r in runtime.ROLES])
            summary = json.loads((out/'summary.json').read_bytes())
            assert status == (1 if failure in ('cleanup', 'resource') else 0), (failure, summary.get('terminal_error'))
            assert result['records'] == 1536 and result['offered_gate_passed'] == (failure is None)
            if failure == '503':
                assert summary['execution_gate_passed'] and summary['failed_calls'] == 64
                assert summary['largest_passing_tested_offered_qps']['ReLAION']['candidate'] == 8
                assert summary['largest_passing_tested_offered_qps']['CoHere']['control'] == 8
                assert summary['aborted_calls'] == 5*64 and len(markers) == 24
            first = summary['cells'][0]
            assert first['successful_full_span_qps'] == first['successes']*1e9/first['full_span_ns']
            assert first['full_span_ns'] > 63e9/.25 and first['response_tail_boundary'].endswith('cleanup')
            ledger = out/first['records_file']
            original = ledger.read_bytes()
            changed = [json.loads(line) for line in original.splitlines()]
            changed[0]['proof_sha256'] = '0'*64
            ledger.write_text(''.join(worker.encoded(r)+'\n' for r in changed))
            rejected(lambda: check_saved(out, digest, *[binaries[r] for r in runtime.ROLES]))
            ledger.write_bytes(original)
    print('offered metadata PASS: both frozen roles, six-slot ownership, 24 cells/1536 records, full-span cleanup QPS, valid503 arm stop, forged/corrupt fatal, callback/offline parity; native/cloud UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif sys.argv[1:] == ['--offered-self-check']:
        self_check(offered=True)
    elif len(sys.argv) == 5:
        result = check_saved(*sys.argv[1:])
        print(json.dumps(result, sort_keys=True))
        raise SystemExit(0 if result.get('offered_gate_passed', result.get('paired_gate_passed')) else 1)
    else:
        raise SystemExit('usage: OUTPUT CONFIG_SHA CONTROL_BINARY CANDIDATE_BINARY | --self-check')
