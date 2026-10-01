#!/usr/bin/env python3
"""Offline paired metadata HEAD audit; no native/cloud calls.

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
    raw = (out / 'records.jsonl').read_bytes()
    records = [json.loads(line) for line in raw.splitlines()]
    stats.require(raw == ''.join(worker.encoded(r) + '\n' for r in records).encode(), 'canonical exact raw ledger')
    saved = json.loads((out / 'summary.json').read_text())
    actual = runtime.reduce_run(records, panels, config, config_sha, evidence,
                                saved['campaign_cgroup_before'], saved['campaign_cgroup_after'])
    actual.update(inputs=runtime.input_receipts(panels), identity_gate_passed=True,
                  closed=actual['process_cleanup_complete'])
    stats.require(saved == actual, 'saved summary/gates/tails/identities parity')
    return dict(records=len(records), execution_gate_passed=actual['execution_gate_passed'],
                all_calls_successful=actual['all_calls_successful'], quality_gate_passed=actual['quality_gate_passed'],
                process_cleanup_complete=actual['process_cleanup_complete'], resource_gate=actual['resource_gate'])


def self_check():
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
                      checker_authority=ptr(root / 'metadata-head/checker-authority.json'),
                      code_sha256={name: old.sha(runtime.ROOT / name) for name in runtime.CODE})
        binaries, proofs, mock_binary_ids = {}, {}, {}
        for role, source in (('control', root / 'native-source-manifest.json'),
                             ('candidate', root / 'metadata-head/native-source-manifest.json')):
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

        def sample():
            value = copy.deepcopy(snapshot)
            if fault == 'resource-after' and len(calls) > campaign_calls:
                value['files']['memory.swap.peak'] = '1'
            return value

        def profile(role):
            value = copy.deepcopy(call_fixture)
            if role == 'candidate' and fault != 'old-heads':
                exact = evidence['checkers']['candidate'].EXACT_LENGTH_FILES
                for row in value['header']['remote_open_stats']['metadata']:
                    if row['name'] in exact:
                        row.update(logical_head_requests=0, head_wall_ns=0)
                for report in (value['header']['transport'], value['response']['transport']):
                    totals = report['totals']
                    totals['method_counts'][1] -= 5
                    totals['attempts'] -= 5
                    totals['status_counts'][0][1] -= 5
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
                assert ready['metadata']['logical_metadata_head_requests'] == (8 if role == 'control' else 3)
                rejected(lambda: evidence['checkers']['candidate' if role == 'control' else 'control'].validate_ready(header, arm))
            # The parent has not integrated the native slice at this worker base.
            # Whatever its default profile, the adapter must leave it untouched.
            default_role = 'candidate' if hasattr(stats, 'EXACT_LENGTH_FILES') else 'control'
            stats.validate_ready(profile(default_role)['header'], config['items'][0]['arms'][default_role])
            rejected(lambda: stats.validate_ready(profile('control' if default_role == 'candidate' else 'candidate')['header'],
                                                 config['items'][0]['arms'][default_role]))
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
            assert result['execution_gate_passed'] and result['records'] == 512
            relocated = directory / 'relocated-offline-copy'
            shutil.copytree(out, relocated)
            assert check_saved(relocated, digest, *[binaries[r] for r in runtime.ROLES])['execution_gate_passed']
            summary = json.loads((out / 'summary.json').read_text())
            assert summary['speedup_claim'] is False and 'latency_improvement' not in summary
            for dataset in worker.DATASETS:
                pooled = summary['datasets'][dataset]['pooled']
                assert pooled['control']['known_logical_charge_totals']['metadata_HEADs'] == 128 * 8
                assert pooled['candidate']['known_logical_charge_totals']['metadata_HEADs'] == 128 * 3
                assert summary['datasets'][dataset]['quality_delta_at_10'] == 0

            ledger = out / 'records.jsonl'
            original_ledger = ledger.read_bytes()
            rows = [json.loads(line) for line in original_ledger.splitlines()]
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

            for failure in ('raw', 'old-heads', 'cleanup', 'resource-after', 'resource'):
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
                n = 65 if failure == 'old-heads' else 1
                assert len(calls) - offset == n and len(failed_rows) == 512
                assert failed_rows[n - 1]['outcome'] == 'failed'
                assert all(r['outcome'] == 'aborted' and r['http_attempts'] == 0 for r in failed_rows[n:])
                assert all(r['native_role'] == r['arm'] and r['config_sha256'] == digest for r in failed_rows)
                if failure == 'raw': assert base64.b64decode(failed_rows[0]['raw_response_base64']) == b'\xff\x00raw fatal'
                result = check_saved(failed, digest, *[binaries[r] for r in runtime.ROLES])
                assert not result['execution_gate_passed']
                assert all(getattr(worker, n) is v for n, v in originals.items())
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
    print('paired metadata self-check PASS: 512 serial calls, frozen role checkers, fail/abort/cleanup and offline tamper replay; native/cloud UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv) == 5:
        result = check_saved(*sys.argv[1:])
        print(json.dumps(result, sort_keys=True))
        raise SystemExit(0 if result['execution_gate_passed'] else 1)
    else:
        raise SystemExit('usage: OUTPUT CONFIG_SHA CONTROL_BINARY CANDIDATE_BINARY | --self-check')
