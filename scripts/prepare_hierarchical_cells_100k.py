#!/usr/bin/env python3
"""Local paired100k preparation; root owns native qualification and execution.

CLI: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check
The closed contract is described by validate_config/validate_proof below.
No downloads, corpus decoding, tuning, or control-performance comparison.
Truth is opened only by the existing diagnostic, after its durable trace freeze.
Exit 0 means complete DIAGNOSTIC evidence, never scientific PASS.
"""

import copy
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import resource
import re
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.check_semantic_router_coverage import canonical, decode, digest, fields, f32, integer, require, sha
from scripts.prepare_semantic_positive_inputs import open_input, stamp, digest as stream_digest, regular_path, output_file
from scripts.benchmark_with_resources import directory_bytes, sample_process_tree

SCHEMA = "borsuk-hierarchical-100k-local-v1"
PROOF_SCHEMA = "borsuk-hierarchical-local-native-proof-v1"
BINDING_SCHEMA = "borsuk-hierarchical-consumed-panel-binding-v1"
GEOMETRY = (100000, 768, 64, 100)
CONFIG_CAP = 128 << 10  # Both pinned coefficient pairs already exceed 64KiB.
POLICY = dict(fetch_policy="whole_cell", primary_beam=8, boundary_beam=24,
              blocks_per_cell=4, max_cells=32, max_cell_gets=32, max_cell_bytes=16 << 20,
              max_source_gets=1, max_source_bytes=0, max_refinement_gets=1, max_refinement_bytes=0)
TESTS = {
    "hierarchical-cell-tests": tuple("hierarchical_semantic_cells::tests::" + name for name in (
        "semantic_cells_do_not_close_over_old_pages_and_keep_unchanged_ranking",
        "identical_geometry_is_bounded_and_reproducible_without_truth",
        "source_id_binding_budgets_and_corruption_fail_closed_with_charges",
        "loss_receipt_separates_boundary_recovery_nomination_and_final_ranking",
        "cell_local_block_nomination_omits_other_blocks_even_for_tied_codes",
        "resident_directory_preload_is_admitted_charged_and_has_no_query_reads",
        "resident_preload_rejects_corrupt_unused_interior_page",
        "whole_cell_matches_two_stage_with_one_wave_and_full_payload_charges",
        "nonunit_query_nonzero_mean_preserves_native_sq2_scoring_and_nomination")),
    "hierarchical-cell-bin-tests": ("tests::configurations_reject_unknown_fields_and_truth_in_requests",
        "tests::created_outputs_close_invalid_on_bad_truth_or_output_cap_without_overwrite"),
}
GATES = {
    "hierarchical-cell-tests": ["cargo", "test", "--locked", "-p", "borsuk", "--lib", "hierarchical_semantic_cells::"],
    "hierarchical-cell-bin-tests": ["cargo", "test", "--locked", "-p", "borsuk", "--bin", "hierarchical_semantic_cells"],
    "generation-integration": ["cargo", "test", "--locked", "-p", "borsuk", "--test", "two_bit_generation"],
    "release": ["cargo", "build", "--release", "--locked", "-p", "borsuk", "--bin", "hierarchical_semantic_cells", "--example", "two_bit_http", "--bin", "build_two_bit_generation", "--bin", "check_semantic_router_scorer"],
    "clippy": ["cargo", "clippy", "--locked", "--workspace", "--all-targets", "--", "-D", "clippy::correctness", "-D", "clippy::suspicious"],
    "test-build": ["env", "-u", "BORSUK_TEST_BUILD_COMMAND", "bash", "scripts/check_rust_test_build.sh"],
}
RESOURCE_FIELDS = ("memory_max_bytes timeout_seconds scratch_max_bytes writer_max_memory_bytes "
                   "build_max_payload_bytes build_max_output_bytes max_resident_directory_payload_bytes "
                   "max_evaluator_payload_bytes max_query_payload_bytes max_result_bytes max_log_bytes")
INPUTS = "raw order sq8 mean records requests truth"
SOURCE_FILES = dict(module="crates/borsuk/src/hierarchical_semantic_cells.rs",
                    binary="crates/borsuk/src/bin/hierarchical_semantic_cells.rs",
                    writer="crates/borsuk/src/bin/build_two_bit_generation.rs",
                    gates="scripts/check_hierarchical_cells_implementation.sh")
EVIDENCE_ROOT = "docs/research/performance-architecture-20260930/semantic-1m/fixed48/architecture-decision-20261002/"
EVIDENCE = (
    (EVIDENCE_ROOT + "next100k-control-preparation-draft.json", 103958, "1d141feeca62476c8cc96db3f01af51ef6b0cb5fd72f226154db2528b5982a36"),
    (EVIDENCE_ROOT + "next100k-codec-input-inventory.json", 3445, "4153107bd812d45c0a5b8210abab468c1ee9d9f507b3794a86f1387e36cc84ee"),
    ("docs/research/native-union-20260928/source-completion-http-config.json", 16015, "06c52df531f8bdb7c30b746cd1a480830e1f2b074ab4d92e45a1c73199ee0a6e"),
)


def exact(value, expected, name):
    require(type(value) is type(expected) and value == expected, name)


def pointer(value, cap):
    fields(value, "path bytes sha256", "artifact")
    require(type(value['path']) is str and Path(value['path']).is_absolute(), "absolute artifact path")
    integer(value['bytes'], 1, cap, "artifact bytes")
    digest(value['sha256'])
    regular_path(value['path'])
    return value


def identity(path):
    with open_input(path) as stream:
        return dict(path=str(regular_path(path)), **stream_digest(stream))


def authenticate(value, cap, read=False):
    pointer(value, cap)
    with open_input(value['path']) as stream:
        require(stream_digest(stream) == {k: value[k] for k in ('bytes', 'sha256')},
                "artifact authentication: " + value['path'])
        if read:
            before = stamp(stream)
            body = stream.read(value['bytes'] + 1)
            require(stamp(stream) == before and len(body) == value['bytes'] and sha(body) == value['sha256'],
                    "artifact changed after authentication")
            return body


def read_json(value, cap=65536):
    return decode(authenticate(value, cap, read=True))


def load_config(path, expected_sha):
    digest(expected_sha)
    with open_input(path) as stream:
        size = os.fstat(stream.fileno()).st_size
    pin = dict(path=str(regular_path(path)), bytes=size, sha256=expected_sha)
    return read_json(pin, CONFIG_CAP), pin


def write_json(path, value):
    identities = {}
    with output_file(path, identities) as write:
        write(canonical(value))
    return identities[path.name]


def validate_config(config, geometry):
    rows, dimensions, count, k = geometry
    fields(config, "schema qualification items resources", "config")
    exact(config['schema'], SCHEMA, "config schema")
    pointer(config['qualification'], 65536)
    limits = config['resources']
    fields(limits, RESOURCE_FIELDS + ' cpu_affinity', "resource config")
    for name in RESOURCE_FIELDS.split():
        integer(limits[name], 1, (1 << 63) - 1, name)
    for name, cap in dict(max_evaluator_payload_bytes=256 << 20,
                          max_query_payload_bytes=512 << 20, max_result_bytes=512 << 20).items():
        require(limits[name] <= cap, "native API cap: " + name)
    require(limits['max_log_bytes'] <= 64 << 20 and limits['max_result_bytes'] <= limits['scratch_max_bytes'],
            "log/output admission")
    affinity = limits['cpu_affinity']
    require(type(affinity) is list and 0 < len(affinity) <= 256, "CPU affinity")
    for cpu in affinity:
        integer(cpu, 0, 65535, "CPU ID")
    require(affinity == sorted(set(affinity)), "ordered unique CPU affinity")
    require(type(config['items']) is list and len(config['items']) == 2, "paired item roster")
    paths = set()
    sizes = dict(raw=rows*dimensions*4, order=rows*8, sq8=rows*(dimensions+12),
                 mean=dimensions*4, records=rows*(8+(dimensions+3)//4),
                 truth=count*k*4)
    for dataset, item in zip(('relaion', 'cohere'), config['items']):
        fields(item, "dataset inputs canonical writer panel_binding", "item")
        exact(item['dataset'], dataset, "fixed dataset order")
        pointer(item['panel_binding'], 65536)
        fields(item['canonical'], "bytes sha256", "retained canonical identity")
        exact(item['canonical']['bytes'], rows*(dimensions*4+8), "retained canonical geometry")
        digest(item['canonical']['sha256'])
        fields(item['inputs'], INPUTS, "input roster")
        for name, pin in item['inputs'].items():
            pointer(pin, sizes.get(name, 32 << 20))
            if name in sizes:
                exact(pin['bytes'], sizes[name], name + " geometry")
            require(pin['path'] not in paths, "distinct input paths, including truth")
            paths.add(pin['path'])
        writer = item['writer']
        fields(writer, "generation base_epoch low step sq8_object_key sq8_etag", "writer")
        exact(writer['generation'], 1, "generation")
        exact(writer['base_epoch'], 0, "base epoch")
        for name in ('low', 'step'):
            values = writer[name]
            require(type(values) is list and len(values) == dimensions, "coefficient geometry")
            values = [f32(v) for v in values]
            require(name != 'step' or all(v > 0 for v in values), "positive f32 step")
        require(type(writer['sq8_object_key']) is str and
                writer['sq8_object_key'].endswith('/' + item['inputs']['sq8']['sha256']) and
                all(p and p not in ('.', '..') for p in writer['sq8_object_key'].split('/')), "SQ8 object binding")
        require(type(writer['sq8_etag']) is str and 0 < len(writer['sq8_etag']) <= 256, "SQ8 ETag")


def validate_proof(value):
    proof = read_json(value)
    fields(proof, "schema source_commit source_archive sources binaries gate_log", "native proof")
    exact(proof['schema'], PROOF_SCHEMA, "native proof schema; completed proof required")
    require(type(proof['source_commit']) is str and re.fullmatch('[0-9a-f]{40}', proof['source_commit']),
            "proof source commit")
    authenticate(proof['source_archive'], 1 << 30)
    fields(proof['sources'], ' '.join(SOURCE_FILES), "proof source roster")
    for name, source in proof['sources'].items():
        authenticate(source, 8 << 20)
        local = identity(Path(__file__).resolve().parents[1] / SOURCE_FILES[name])
        exact(source['sha256'], local['sha256'], "proof source differs from this checkout: " + name)
    fields(proof['binaries'], "writer cells", "proof binary roster")
    for pin in proof['binaries'].values():
        authenticate(pin, 256 << 20)
        require(os.access(pin['path'], os.X_OK), "qualified binary executable")
    transcript = authenticate(proof['gate_log'], 64 << 20, read=True)
    require(transcript.endswith(b'\n'), "proof gate log final newline")
    events, passed, summaries = [], {}, []
    for line in transcript.splitlines():
        test = re.fullmatch(rb'test (\S+) \.\.\. ok', line)
        if test:
            name = test[1].decode()
            passed[name] = passed.get(name, 0) + 1
        summary = re.fullmatch(rb'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; .*', line)
        if summary:
            summaries.append((int(summary[1]), int(summary[2])))
        if not line.startswith(b'{'):
            continue
        event = decode(line)
        if type(event) is not dict or event.get('schema') != 'borsuk-hierarchical-cells-implementation-stage-v1':
            continue
        fields(event, "schema stage started_at finished_at exit_status gate_status tests_run required_test_passes command", "proof stage")
        position = len(events)
        require(position < 12, "proof duplicate gate")
        stage = list(GATES)[position // 2]
        exact(event['stage'], stage, "proof ordered gates")
        exact(event['command'], GATES[stage], "proof exact native command")
        require(type(event['started_at']) is str and event['started_at'], "proof start time")
        if position % 2 == 0:
            for key in ('finished_at', 'exit_status', 'gate_status', 'tests_run', 'required_test_passes'):
                exact(event[key], None, "proof started gate: " + key)
            passed, summaries = {}, []
        else:
            exact(event['started_at'], events[-1]['started_at'], "proof start/completion binding")
            require(type(event['finished_at']) is str and event['finished_at'], "proof completion required")
            exact(event['exit_status'], 0, "proof completed exit0")
            exact(event['gate_status'], 0, "proof completed gate0")
            expected = dict.fromkeys(TESTS.get(stage, ()), 1)
            exact(event['required_test_passes'], expected, "proof named test roster")
            for name in expected:
                exact(event['required_test_passes'][name], 1, "proof integer named pass")
            require(all(passed.get(name) == 1 for name in expected), "proof missing real named test lines")
            if stage in ('release', 'clippy', 'test-build'):
                exact(event['tests_run'], None, "proof compilation gate")
            else:
                integer(event['tests_run'], 1, 1000000, "proof executed test count")
                require(summaries and sum(p for p, _ in summaries) == event['tests_run'] and
                        all(f == 0 for _, f in summaries), "proof executed test summaries")
        events.append(event)
    require(len(events) == 12, "proof requires all six native gates completed0")
    return proof


def validate_binding(item, geometry):
    _, dimensions, count, k = geometry
    binding = read_json(item['panel_binding'])
    fields(binding, "schema dataset source_reference source_requests source_truth requests_sha256 truth_sha256 "
           "raw_sha256 order_sha256 sq8_sha256 canonical_sha256 low_f32_hex step_f32_hex first count k metric consumed", "panel binding")
    for name, expected in dict(schema=BINDING_SCHEMA, dataset=item['dataset'], first=0,
                               count=count, k=k, metric='cosine', consumed=True).items():
        exact(binding[name], expected, "panel binding: " + name)
    require(type(binding['source_reference']) is str and 0 < len(binding['source_reference']) <= 4096,
            "separate source-bound quality reference")
    for name in ('source_requests', 'source_truth'):
        fields(binding[name], "bytes sha256", "source panel identity")
        integer(binding[name]['bytes'], 1, 64 << 20, name + " bytes")
        digest(binding[name]['sha256'])
    for name in ('requests', 'truth', 'raw', 'order', 'sq8', 'canonical'):
        expected = item['canonical'] if name == 'canonical' else item['inputs'][name]
        exact(binding[name + '_sha256'], expected['sha256'], "panel input binding: " + name)
    for name in ('low', 'step'):
        bits = struct.pack('<' + str(dimensions) + 'f', *[f32(v) for v in item['writer'][name]]).hex()
        exact(binding[name + '_f32_hex'], bits, "coefficient provenance: " + name)
    return binding


def retained_pins(config):
    """Immutable source metadata is provenance, never native gate authority."""
    repo = Path(__file__).resolve().parents[1]
    pins = [dict(path=str(repo/path), bytes=size, sha256=hash_value) for path, size, hash_value in EVIDENCE]
    draft, inventory, source = [read_json(pin, CONFIG_CAP) for pin in pins]
    for item, expected, retained, original in zip(config['items'], draft['items'], inventory['datasets'], source['items']):
        dataset, inputs, writer = item['dataset'], item['inputs'], item['writer']
        require(dataset == expected['dataset'] == retained['dataset'] == original['name'], "retained dataset binding")
        exact(inputs['raw']['sha256'], expected['decoded_raw_sha256'], "pinned original raw identity")
        for name, asset in (('order', 'order.u64'), ('sq8', 'sq8.bin'), ('mean', 'plane-mean.bin'), ('records', 'plane-records.bin')):
            for field, recorded in (('bytes', 'bytes'), ('sha256', 'expected_sha256')):
                exact(inputs[name][field], retained['assets'][asset][recorded], "pinned retained " + name)
        for name in ('low', 'step'):
            actual = struct.pack('<768f', *[f32(v) for v in writer[name]])
            required = struct.pack('<768f', *[f32(v) for v in expected['current_writer_config_draft'][name]])
            exact(actual, required, "pinned retained coefficient bits")
        for name in ('sq8_object_key', 'sq8_etag'):
            exact(writer[name], expected['current_writer_config_draft'][name], "pinned retained " + name)
        binding = validate_binding(item, GEOMETRY)
        for name, source_name in (('source_requests', 'requests'), ('source_truth', 'truth')):
            exact(binding[name], {key: original['artifacts'][source_name][key] for key in ('bytes', 'sha256')},
                  "pinned consumed source panel: " + name)
    return pins


def validate_mapping(inputs, geometry, canonical_pin=None):
    rows, dimensions, _, _ = geometry
    order = authenticate(inputs['order'], rows * 8, read=True)
    ids = struct.unpack('<' + str(rows) + 'Q', order)
    require(set(ids) == set(range(rows)), "physical-to-source order permutation")
    with ExitStack() as stack:
        sq8 = stack.enter_context(open_input(inputs['sq8']['path']))
        canonical_rows = stack.enter_context(open_input(canonical_pin['path'])) if canonical_pin else None
        for ordinal in ids:
            s = sq8.read(dimensions + 12)
            require(len(s) == dimensions + 12 and struct.unpack('<q', s[:8])[0] == ordinal,
                    "SQ8 IDs must equal order[p]")
            if canonical_rows:
                c = canonical_rows.read(dimensions * 4 + 8)
                require(len(c) == dimensions * 4 + 8 and struct.unpack('<q', c[:8])[0] == ordinal,
                        "canonical IDs must equal order[p]")
        require(not sq8.read(1) and (not canonical_rows or not canonical_rows.read(1)), "mapping exact EOF")


def validate_requests(pin, geometry):
    _, dimensions, count, _ = geometry
    body = authenticate(pin, 32 << 20, read=True)
    require(body.endswith(b'\n') and len(body.splitlines()) == count, "exact consumed request panel")
    for ordinal, line in enumerate(body.splitlines()):
        require(len(line) <= 65536, "request line cap")
        request = decode(line)
        fields(request, "ordinal query", "request")
        exact(request['ordinal'], ordinal, "request ordinal")
        require(type(request['query']) is list and len(request['query']) == dimensions, "request dimension")
        values = [f32(v) for v in request['query']]
        require(sum(v*v for v in values) > 0, "nonzero cosine query")


def resource_snapshot(limits):
    entry = Path('/proc/self/cgroup').read_text().strip()
    require(entry.startswith('0::/') and '\n' not in entry, "cgroup v2 required")
    group = Path('/sys/fs/cgroup') / entry[3:].lstrip('/')
    snapshot = {name: (group / name).read_text().strip() for name in
                ('memory.max', 'memory.swap.max', 'memory.peak', 'memory.swap.peak', 'memory.events', 'cpu.stat')}
    require(snapshot['memory.max'] != 'max' and 0 < int(snapshot['memory.max']) <= limits['memory_max_bytes']
            and int(snapshot['memory.peak']) <= limits['memory_max_bytes']
            and snapshot['memory.swap.max'] == '0' and int(snapshot['memory.swap.peak']) == 0,
            "root-supplied memory/no-swap cgroup required")
    require(sorted(os.sched_getaffinity(0)) == limits['cpu_affinity'], "exact root-supplied CPU affinity")
    snapshot.update(path=str(group), cpu_affinity=limits['cpu_affinity'])
    return snapshot


def run_stage(name, command, binary, config_pin, output, limits, deadline, stages):
    record = dict(name=name, command=command, binary=binary, config=config_pin,
                  exit_status=None, cleanup_complete=False,
                  cgroup_before=resource_snapshot(limits), sampled_peak_rss_bytes=0, sampled_peak_scratch_bytes=0,
                  resource_gate_passed=False)
    stages.append(record)
    log = output / (name + '.log')
    started, usage = time.monotonic(), resource.getrusage(resource.RUSAGE_CHILDREN)
    process = None
    error = None
    try:
        authenticate(binary, 256 << 20)
        authenticate(config_pin, 65536)
        exact(command[0], binary['path'], "exact qualified binary command")
        with log.open('xb') as stream:
            process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                       start_new_session=True, env=dict(os.environ, TMPDIR=str(output),
                                                                        BORSUK_BUILD_SCRATCH_DIR=str(output)))
            while True:
                scratch = directory_bytes(output)
                sample = sample_process_tree(process.pid)
                record['sampled_peak_scratch_bytes'] = max(record['sampled_peak_scratch_bytes'], scratch)
                if sample:
                    record['sampled_peak_rss_bytes'] = max(record['sampled_peak_rss_bytes'], sample[1])
                require(scratch <= limits['scratch_max_bytes'], "scratch byte cap")
                require(log.stat().st_size <= limits['max_log_bytes'], "stage log byte cap")
                if time.monotonic() >= deadline:
                    raise TimeoutError("root runtime cap")
                status = process.poll()
                if status is not None:
                    record['exit_status'] = status
                    require(status == 0, name + ' exit ' + str(status))
                    break
                time.sleep(0.05)
            stream.flush()
            os.fsync(stream.fileno())
        authenticate(binary, 256 << 20)
        authenticate(config_pin, 65536)
    except BaseException as failure:
        error = failure
    finally:
        if process is not None:
            # Kill the process group even after leader exit; no orphan descendants.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            record['exit_status'] = process.wait()
        record['cleanup_complete'] = True
        record['wall_seconds'] = time.monotonic() - started
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        record['child_user_seconds'] = after.ru_utime - usage.ru_utime
        record['child_system_seconds'] = after.ru_stime - usage.ru_stime
        record['child_lifetime_hwm_bytes'] = after.ru_maxrss * 1024
        record['log'] = identity(log) if log.exists() else None
        try:
            record['cgroup_after'] = resource_snapshot(limits)
            before_events = dict(line.split() for line in record['cgroup_before']['memory.events'].splitlines())
            after_events = dict(line.split() for line in record['cgroup_after']['memory.events'].splitlines())
            no_oom = all(after_events.get(k) == before_events.get(k) for k in ('oom', 'oom_kill', 'oom_group_kill'))
        except Exception as closure_error:
            record['cgroup_after'] = dict(error=str(closure_error))
            no_oom = False
            if error is None:
                error = closure_error
        record['resource_gate_passed'] = (error is None and no_oom and directory_bytes(output) <= limits['scratch_max_bytes'])
        if not no_oom and error is None:
            error = ValueError("cgroup OOM resource failure")
    if error is not None:
        raise error
    require(record['resource_gate_passed'], "stage resource closure")
    return authenticate(record['log'], limits['max_log_bytes'], read=True)


def current_generation(item, folder, geometry):
    rows, dimensions, _, _ = geometry
    pins = {name: identity(folder / relative) for name, relative in dict(
        generation='manifest.json', plane='plane/manifest.json', mean='plane/mean.bin',
        records='plane/records.bin', canonical='canonical.bin').items()}
    root, plane = read_json(pins['generation']), read_json(pins['plane'])
    exact(root.get('schema'), 'borsuk-two-bit-generation-v8', "current generation required")
    exact(plane.get('schema'), 'borsuk-two-bit-plane-v3', "current plane required; no legacy reader")
    inputs = item['inputs']
    for name in ('mean', 'records', 'canonical'):
        expected = item['canonical'] if name == 'canonical' else inputs[name]
        require(all(pins[name][key] == expected[key] for key in ('bytes', 'sha256')),
                "retained byte parity: " + name)
    for name, expected in dict(rows=rows, dimensions=dimensions, seed=20260923,
                               record_bytes=8+(dimensions+3)//4, source_sha256=inputs['raw']['sha256'],
                               sq8_sha256=inputs['sq8']['sha256'], source_order_sha256=inputs['order']['sha256'],
                               mean_sha256=pins['mean']['sha256'], records_sha256=pins['records']['sha256'],
                               query_or_truth_used=False).items():
        exact(plane.get(name), expected, "current plane binding: " + name)
    for name, expected in dict(plane_manifest_sha256=pins['plane']['sha256'], sq8_object_sha256=inputs['sq8']['sha256'],
                               generation=1, base_epoch=0, sq8_object_key=item['writer']['sq8_object_key'],
                               sq8_etag=item['writer']['sq8_etag']).items():
        exact(root.get(name), expected, "current generation binding: " + name)
    require(root.get('discovery', {}).get('mode') == 'semantic' and
            root['discovery'].get('profile') == 'native100k', "current semantic/native100k generation")
    for name in ('low', 'step'):
        values = root.get(name)
        require(type(values) is list and len(values) == dimensions, "current coefficient geometry")
        exact(struct.pack('<' + str(dimensions) + 'f', *[f32(v) for v in values]),
              struct.pack('<' + str(dimensions) + 'f', *[f32(v) for v in item['writer'][name]]),
              "current coefficient bit binding")
    require(all(root.get('canonical', {}).get(name) == expected for name, expected in dict(
        rows=rows, dimensions=dimensions, bytes=pins['canonical']['bytes'], sha256=pins['canonical']['sha256']).items()),
        "current canonical binding")
    validate_mapping(inputs, geometry, pins['canonical'])
    return dict(pins, order=inputs['order'], sq8=inputs['sq8'])


def validate_diagnostic(pin, config, config_pin, proof, limits):
    # Stream frozen diagnostics; do not read GT or feed losses into search.
    pointer(pin, limits['max_result_bytes'])
    queries, losses, frozen, terminal, prefix = 0, 0, False, False, hashlib.sha256()
    prefix_bytes = 0
    with open_input(pin['path']) as stream:
        for index, line in enumerate(stream):
            require(not terminal, "event after terminal")
            require(len(line) <= 8 << 20 and line.endswith(b'\n'), "diagnostic event cap/newline")
            event = decode(line)
            phase = event.get('phase')
            if index == 0:
                for name, expected in dict(phase='identity', schema='borsuk-hierarchical-cells-diagnostic-v3',
                    config_sha256=config_pin['sha256'], candidate_root_sha256=config['candidate_root']['sha256'],
                    requests_sha256=config['requests']['sha256'], module_source_sha256=proof['sources']['module']['sha256'],
                    binary_source_sha256=proof['sources']['binary']['sha256'], first=0, count=config['count'],
                    top_k=config['top_k'], options=config['options']).items():
                    exact(event.get(name), expected, "diagnostic identity: " + name)
            elif phase == 'query_frozen':
                require(not frozen and queries < config['count'], "query after freeze/overflow")
                exact(event.get('ordinal'), queries, "frozen query ordinal")
                exact(event.get('truth_opened'), False, "truth-free query")
                queries += 1
            elif phase == 'all_queries_frozen':
                require(not frozen and queries == config['count'], "complete freeze required")
                for name, expected in dict(count=queries, first=0, trace_prefix_bytes=prefix_bytes,
                                           trace_prefix_sha256=prefix.hexdigest(), truth_opened=False).items():
                    exact(event.get(name), expected, "synced freeze binding: " + name)
                frozen = True
            elif phase == 'loss_attribution':
                require(frozen and not terminal and losses < queries, "loss only after complete freeze")
                exact(event.get('ordinal'), losses, "loss ordinal")
                exact(event.get('truth_sha256'), config['truth']['sha256'], "native-authenticated truth identity")
                losses += 1
            elif phase == 'terminal':
                require(frozen and losses == queries and not terminal, "complete single terminal")
                for name, expected in dict(status='DIAGNOSTIC', complete=True, queries=queries, truth_opened=True,
                    scientific_qualification=False, quality_or_performance_claim=False).items():
                    exact(event.get(name), expected, "diagnostic terminal: " + name)
                terminal = True
            else:
                raise ValueError("unexpected diagnostic phase")
            if not frozen:
                prefix.update(line)
                prefix_bytes += len(line)
    require(terminal, "no partial diagnostic completion")
    authenticate(pin, limits['max_result_bytes'])


def prepare(config, config_identity, output, geometry=GEOMETRY, before_diagnostics=None):
    output = regular_path(output)
    require(not os.path.lexists(output), "output exists")
    validate_config(config, geometry)
    require(read_json(config_identity, CONFIG_CAP) == config, "authenticated config binding")
    limits = config['resources']
    before = resource_snapshot(limits)
    deadline = time.monotonic() + limits['timeout_seconds']
    receipt = dict(schema=SCHEMA, status='INVALID', complete=False, config=config_identity,
                   adapter=identity(__file__),
                   quality_or_performance_claim=False, physical_s3_measured=False, scientific_qualification=False,
                   resource_limits=limits, cgroup_before=before, stages=[], cleanup_complete=False,
                   qualification_attestation='authenticated root archive/source/binary binding and completed native gate transcript',
                   memory_measurement='cgroup lifetime peak; sampled process RSS is a lower bound; child HWM is lifetime',
                   truth_policy='only existing native diagnostic opens/authenticates GT after its synced query freeze')
    old_handler = signal.getsignal(signal.SIGALRM)
    def expired(_signal, _frame):
        raise TimeoutError("root runtime cap")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, limits['timeout_seconds'])
    created = False
    try:
        proof = validate_proof(config['qualification'])
        receipt['proof'] = proof
        if geometry == GEOMETRY:
            receipt['retained_source_metadata'] = retained_pins(config)
        bindings = {}
        for item in config['items']:
            bindings[item['dataset']] = validate_binding(item, geometry)
            for name, pin in item['inputs'].items():
                if name != 'truth':  # Native authenticates GT after its durable freeze.
                    authenticate(pin, max(pin['bytes'], 1))
            validate_mapping(item['inputs'], geometry)
            validate_requests(item['inputs']['requests'], geometry)
        receipt['panel_bindings'] = bindings
        output.mkdir(mode=0o700)
        created = True
        receipt['frozen_config'] = write_json(output/'frozen-config.json', config)
        current = {}
        for item in config['items']:
            dataset, inputs = item['dataset'], item['inputs']
            writer = dict(item['writer'], discovery='semantic', semantic_profile='native100k',
                          raw=inputs['raw']['path'], raw_sha256=inputs['raw']['sha256'],
                          sq8=inputs['sq8']['path'], sq8_sha256=inputs['sq8']['sha256'],
                          rows=geometry[0], dimensions=geometry[1],
                          order={k: inputs['order'][k] for k in ('path', 'sha256')})
            pin = write_json(output/(dataset+'-writer.json'), writer)
            folder = output/(dataset+'-generation')
            command = [proof['binaries']['writer']['path'], pin['path'], pin['sha256'],
                       str(limits['writer_max_memory_bytes']), str(folder)]
            log = run_stage(dataset+'-writer', command, proof['binaries']['writer'], pin,
                            output, limits, deadline, receipt['stages'])
            current[dataset] = current_generation(item, folder, geometry)
            exact(log.strip().decode(), current[dataset]['generation']['sha256'], "writer emitted root identity")
        receipt['current_inputs'] = current
        # Both source-only builds precede any diagnostic's GT evaluation.
        candidates = {}
        for item in config['items']:
            dataset = item['dataset']
            build = dict(current[dataset], schema='borsuk-hierarchical-cells-build-v1', cell_rows=512,
                         sample_rows=256, max_depth=32, max_build_payload_bytes=limits['build_max_payload_bytes'],
                         max_output_bytes=limits['build_max_output_bytes'])
            pin = write_json(output/(dataset+'-build.json'), build)
            folder = output/(dataset+'-cells')
            log = run_stage(dataset+'-build', [proof['binaries']['cells']['path'], 'build', pin['path'], pin['sha256'],
                                             str(folder)], proof['binaries']['cells'], pin,
                            output, limits, deadline, receipt['stages'])
            result = decode(log)
            for key, expected in dict(status='BUILT_UNQUALIFIED', config_sha256=pin['sha256'],
                                      module_source_sha256=proof['sources']['module']['sha256']).items():
                exact(result.get(key), expected, "build source/config identity")
            candidates[dataset] = identity(folder/'manifest.json')
        receipt['candidates'] = candidates
        results, diagnostic_plans = {}, []
        for item in config['items']:
            dataset, inputs = item['dataset'], item['inputs']
            diagnostic = dict(schema='borsuk-hierarchical-cells-diagnostic-v3', candidate_root=candidates[dataset],
                requests=inputs['requests'], first=0, count=geometry[2], top_k=geometry[3],
                options=dict(POLICY, max_query_payload_bytes=limits['max_query_payload_bytes']),
                max_resident_directory_payload_bytes=limits['max_resident_directory_payload_bytes'],
                truth=inputs['truth'], truth_width=geometry[3],
                max_evaluator_payload_bytes=limits['max_evaluator_payload_bytes'], max_result_bytes=limits['max_result_bytes'])
            pin = write_json(output/(dataset+'-diagnose.json'), diagnostic)
            target = output/(dataset+'-diagnostic.jsonl')
            diagnostic_plans.append((dataset, diagnostic, pin, target))
        # Every builder and query configuration is sealed before the first GT open.
        receipt['diagnostic_configs'] = {dataset: pin for dataset, _, pin, _ in diagnostic_plans}
        if before_diagnostics is not None:
            before_diagnostics(copy.deepcopy(diagnostic_plans), copy.deepcopy(proof),
                               copy.deepcopy(limits), deadline, receipt)
            # Admission may create disposable evidence, but cannot change the arm.
            for _, diagnostic, pin, _ in diagnostic_plans:
                require(read_json(pin) == diagnostic, "admission changed sealed diagnostic")
        for dataset, diagnostic, pin, target in diagnostic_plans:
            run_stage(dataset+'-diagnose', [proof['binaries']['cells']['path'], 'diagnose', pin['path'], pin['sha256'],
                                           str(target)], proof['binaries']['cells'], pin,
                      output, limits, deadline, receipt['stages'])
            results[dataset] = identity(target)
            validate_diagnostic(results[dataset], diagnostic, pin, proof, limits)
        for item in config['items']:
            for name, pin in item['inputs'].items():
                if name != 'truth':
                    authenticate(pin, pin['bytes'])
        authenticate(config_identity, CONFIG_CAP)
        for pin in proof['sources'].values():
            authenticate(pin, 8 << 20)
        authenticate(config['qualification'], 65536)
        receipt['cgroup_after'] = resource_snapshot(limits)
        receipt.update(status='DIAGNOSTIC', complete=True, results=results)
        receipt['cleanup_complete'] = True
        receipt['final_output_bytes'] = directory_bytes(output)
        for _ in range(3):
            receipt['final_output_bytes'] = directory_bytes(output) + len(canonical(receipt))
        require(len(canonical(receipt)) <= 256 << 10 and
                receipt['final_output_bytes'] <= limits['scratch_max_bytes'], "final receipt/scratch byte cap")
    except BaseException as error:
        receipt.update(status='INVALID', complete=False)
        receipt['error'] = type(error).__name__ + ': ' + str(error)
        if created:
            # Only directories under this newly created output belong to this run.
            for entry in output.iterdir():
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry)
                elif entry.is_symlink():
                    entry.unlink()
        raise
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        if created:
            receipt['cleanup_complete'] = True
            if not receipt['complete']:
                try:
                    receipt['cgroup_after'] = resource_snapshot(limits)
                except Exception as closure_error:
                    receipt['cgroup_after'] = dict(error=str(closure_error))
                receipt['final_output_bytes'] = directory_bytes(output)
            write_json(output/'receipt.json', receipt)
            fd = os.open(output, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    return receipt


def self_check():
    """Tiny authenticated fixtures and mock executables; no real native or GT run."""
    with tempfile.TemporaryDirectory(prefix="hierarchical-local-self-check-") as directory:
        base = Path(directory)
        geometry = (4, 3, 2, 2)

        def put(path, body):
            path.write_bytes(body)
            return dict(path=str(path), bytes=len(body), sha256=sha(body))

        repo = Path(__file__).resolve().parents[1]
        sources = {name: put(base / name, (repo / path).read_bytes())
                   for name, path in SOURCE_FILES.items()}
        # These are synthetic completed transcripts, used only for helper checks.
        log = bytearray()
        for stage, command in GATES.items():
            event = dict(schema="borsuk-hierarchical-cells-implementation-stage-v1", stage=stage,
                         started_at="2026-10-03T00:00:00Z", finished_at=None,
                         exit_status=None, gate_status=None, tests_run=None,
                         required_test_passes=None, command=command)
            log.extend(canonical(event))
            for name in TESTS.get(stage, ()):
                log.extend(("test " + name + " ... ok\n").encode())
            n = len(TESTS.get(stage, ())) or 1
            if stage not in ("release", "clippy", "test-build"):
                log.extend(f"test result: ok. {n} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n".encode())
            event.update(finished_at="2026-10-03T00:00:01Z", exit_status=0, gate_status=0,
                         tests_run=n if stage not in ("release", "clippy", "test-build") else None,
                         required_test_passes=dict.fromkeys(TESTS.get(stage, ()), 1))
            log.extend(canonical(event))
        gate_log = put(base / "gate.log", bytes(log))
        mock = b'''#!/usr/bin/env python3
import hashlib,json,sys,struct
from pathlib import Path
def h(b):return hashlib.sha256(b).hexdigest()
def body(p):return Path(p).read_bytes()
def enc(v):return (json.dumps(v,sort_keys=True,separators=(',',':'))+'\\n').encode()
base=Path(__file__).parent
if sys.argv[1] not in ('build','diagnose'):
 c=json.loads(body(sys.argv[1])); out=Path(sys.argv[4]);out.mkdir();(out/'plane').mkdir()
 if (base/'fail').exists(): (out/'partial').write_text('partial');sys.exit(7)
 if (base/'hang').exists():
  import time
  time.sleep(120)
 if (base/'large-log').exists(): print('X'*8192);sys.stdout.flush()
 mean=b'bad' if (base/'mismatch').exists() else struct.pack('<3f',0,0,0)
 records=b'R'*36
 order=body(c['order']['path']);ids=struct.unpack('<4Q',order)
 can=b''.join(struct.pack('<q3f',i,1,0,0) for i in ids)
 if (base/'bad_id').exists(): can=struct.pack('<q',3)+can[8:]
 (out/'plane/mean.bin').write_bytes(mean);(out/'plane/records.bin').write_bytes(records)
 (out/'canonical.bin').write_bytes(can)
 plane=dict(schema='borsuk-two-bit-plane-v3',rows=4,dimensions=3,seed=20260923,record_bytes=9,
 source_sha256=c['raw_sha256'],sq8_sha256=c['sq8_sha256'],source_order_sha256=h(order),
 mean_sha256=h(mean),records_sha256=h(records),page_rows=32,page_digest_sha256='a'*64,query_or_truth_used=False)
 (out/'plane/manifest.json').write_bytes(enc(plane))
 root=dict(schema='borsuk-two-bit-generation-v8',generation=c['generation'],base_epoch=c['base_epoch'],
 plane_manifest_sha256=h(enc(plane)),sq8_object_sha256=c['sq8_sha256'],low=c['low'],step=c['step'],
 sq8_object_key=c['sq8_object_key'],sq8_etag=c['sq8_etag'],discovery=dict(mode='semantic',profile='native100k'),
 canonical=dict(rows=4,dimensions=3,bytes=len(can),sha256=h(can),object_key='objects/'+h(can)))
 (out/'manifest.json').write_bytes(enc(root));print(h(enc(root)))
else:
 mode,config,config_sha,output=sys.argv[1:];c=json.loads(body(config));out=Path(output)
 if mode=='build':
  out.mkdir();(out/'manifest.json').write_bytes(enc(dict(schema='borsuk-hierarchical-cells-resident-v3')))
  print(json.dumps(dict(status='BUILT_UNQUALIFIED',config_sha256=config_sha,
   module_source_sha256=h(body(base/'module')),receipt={})))
 else:
  identity=dict(phase='identity',schema='borsuk-hierarchical-cells-diagnostic-v3',config_sha256=config_sha,
   candidate_root_sha256=c['candidate_root']['sha256'],requests_sha256=c['requests']['sha256'],
   module_source_sha256=h(body(base/'module')),binary_source_sha256=h(body(base/'binary')),
   first=c['first'],count=c['count'],top_k=c['top_k'],options=c['options'])
  events=[identity]+[dict(phase='query_frozen',ordinal=i,truth_opened=False,trace={}) for i in range(c['count'])]
  prefix=b''.join(enc(e) for e in events)
  freeze=dict(phase='all_queries_frozen',count=c['count'],first=0,trace_prefix_bytes=len(prefix),trace_prefix_sha256=h(prefix),truth_opened=False)
  with out.open('xb') as f:
   f.write(prefix+enc(freeze));f.flush()
   truth=body(c['truth']['path']);assert h(truth)==c['truth']['sha256']
   for i in range(c['count']):f.write(enc(dict(phase='loss_attribution',ordinal=i,truth_sha256=h(truth),loss={})))
   f.write(enc(dict(phase='terminal',status='DIAGNOSTIC',complete=True,queries=c['count'],truth_opened=True,scientific_qualification=False,quality_or_performance_claim=False)))
  if (base/'bad-identity').exists():
   out.write_bytes(out.read_bytes().replace(identity['binary_source_sha256'].encode(),b'0'*64,1))
  if (base/'partial-diagnostic').exists():
   out.write_bytes(b'\\n'.join(out.read_bytes().splitlines()[:-1])+b'\\n')
  print(json.dumps(dict(status='DIAGNOSTIC',queries=c['count'],result_bytes=out.stat().st_size)))
'''
        binaries = {name: put(base / (name + "-mock"), mock) for name in ("writer", "cells")}
        for descriptor in binaries.values():
            Path(descriptor['path']).chmod(0o700)
        proof = dict(schema=PROOF_SCHEMA, source_commit="a" * 40,
                     source_archive=put(base / "archive", b"synthetic archive"), sources=sources,
                     binaries=binaries, gate_log=gate_log)
        proof_pin = put(base / "proof.json", canonical(proof))
        items = []
        for dataset in ("relaion", "cohere"):
            folder = base / dataset
            folder.mkdir()
            ids = (2, 0, 3, 1)
            bodies = dict(raw=struct.pack('<12f', *([1, 0, 0] * 4)), order=struct.pack('<4Q', *ids),
                          sq8=b''.join(struct.pack('<qf3B', i, 1, 7, 8, 9) for i in ids),
                          mean=struct.pack('<3f', 0, 0, 0), records=b'R' * 36,
                          canonical=b''.join(struct.pack('<q3f', i, 1, 0, 0) for i in ids),
                          requests=b''.join(canonical(dict(ordinal=i, query=[1, 0, 0])) for i in range(2)),
                          truth=struct.pack('<4I', 0, 1, 2, 3))
            inputs = {name: put(folder / name, body) for name, body in bodies.items()}
            canonical_pin = inputs.pop('canonical')
            binding = dict(schema=BINDING_SCHEMA, dataset=dataset, source_reference="synthetic quality reference",
                           source_requests=dict(bytes=100, sha256="b" * 64), source_truth=dict(bytes=100, sha256="c" * 64),
                           **{name + '_sha256': inputs[name]['sha256'] for name in ('requests', 'truth', 'raw', 'order', 'sq8')},
                           canonical_sha256=canonical_pin['sha256'],
                           low_f32_hex=struct.pack('<3f', 0, 0, 0).hex(), step_f32_hex=struct.pack('<3f', 1, 1, 1).hex(),
                           first=0, count=2, k=2, metric="cosine", consumed=True)
            items.append(dict(dataset=dataset, inputs=inputs, canonical={key: canonical_pin[key] for key in ('bytes', 'sha256')},
                              panel_binding=put(folder / 'binding.json', canonical(binding)),
                              writer=dict(generation=1, base_epoch=0, low=[0] * 3, step=[1] * 3,
                                          sq8_object_key='objects/' + inputs['sq8']['sha256'], sq8_etag='"synthetic"')))
        resources = dict.fromkeys(RESOURCE_FIELDS.split(), 128 << 20)
        resources.update(memory_max_bytes=256 << 20, timeout_seconds=30, scratch_max_bytes=16 << 20,
                         max_result_bytes=1 << 20, max_log_bytes=1 << 20,
                         cpu_affinity=sorted(os.sched_getaffinity(0)))
        config = dict(schema=SCHEMA, qualification=proof_pin, items=items, resources=resources)
        put(base / 'config.json', canonical(config))
        # Paired production coefficients alone are >64KiB; keep helper admission usable.
        padded = put(base/'padded-config.json', canonical(config) + b' ' * 65536)
        assert load_config(padded['path'], padded['sha256'])[0] == config
        # Authenticate production provenance using metadata only and tiny bindings.
        # Declared 100k payloads below are never opened or passed to a subprocess.
        draft, inventory, source = [decode((repo/path).read_bytes()) for path, _, _ in EVIDENCE]
        production = copy.deepcopy(config)
        for item, planned, retained, original in zip(production['items'], draft['items'], inventory['datasets'], source['items']):
            writer = planned['current_writer_config_draft']
            item['writer'] = {key: writer[key] for key in item['writer']}
            item['inputs']['raw'].update(bytes=307200000, sha256=planned['decoded_raw_sha256'])
            for name, asset in (('order', 'order.u64'), ('sq8', 'sq8.bin'), ('mean', 'plane-mean.bin'), ('records', 'plane-records.bin')):
                item['inputs'][name].update(bytes=retained['assets'][asset]['bytes'], sha256=retained['assets'][asset]['expected_sha256'])
            item['inputs']['truth']['bytes'] = 25600
            item['canonical']['bytes'] = 308000000
            binding = read_json(item['panel_binding'])
            binding.update(count=64, k=100)
            for name in ('raw', 'order', 'sq8'):
                binding[name+'_sha256'] = item['inputs'][name]['sha256']
            for name in ('low', 'step'):
                binding[name+'_f32_hex'] = struct.pack('<768f', *writer[name]).hex()
            for name, origin in (('source_requests', 'requests'), ('source_truth', 'truth')):
                binding[name] = {key: original['artifacts'][origin][key] for key in ('bytes', 'sha256')}
            item['panel_binding'] = put(base/(item['dataset']+'-production-binding.json'), canonical(binding))
        validate_config(production, GEOMETRY)
        assert len(retained_pins(production)) == 3
        production['items'][0]['inputs']['mean']['sha256'] = '0'*64
        try:
            retained_pins(production)
        except ValueError as error:
            assert 'pinned retained mean' in str(error)
        else:
            raise AssertionError('altered retained provenance')

        def run(c, name):
            pin = put(base / (name + '.json'), canonical(c))
            # Assert the helper never opens truth, including authentication.
            from unittest.mock import patch
            original = open_input
            def guarded(path):
                require(Path(path).name != 'truth', "helper opened truth before freeze")
                return original(path)
            with patch(__name__ + '.open_input', side_effect=guarded):
                return prepare(c, pin, base / name, geometry)

        result = run(config, 'success')
        assert result['status'] == 'DIAGNOSTIC' and result['complete'] is True
        assert result['quality_or_performance_claim'] is False
        assert len(result['stages']) == 6
        for dataset in ('relaion', 'cohere'):
            writer = decode((base / 'success' / (dataset + '-writer.json')).read_bytes())
            assert writer['discovery'] == 'semantic' and writer['semantic_profile'] == 'native100k'
            build = decode((base / 'success' / (dataset + '-build.json')).read_bytes())
            assert (build['cell_rows'], build['sample_rows'], build['max_depth']) == (512, 256, 32)
            diagnostic = decode((base / 'success' / (dataset + '-diagnose.json')).read_bytes())
            assert diagnostic['options'] == dict(POLICY, max_query_payload_bytes=resources['max_query_payload_bytes'])

        def rejects(c, name, text):
            try:
                run(c, name)
            except (ValueError, OSError, TimeoutError) as error:
                assert text in str(error), (name, str(error))
            else:
                raise AssertionError('accepted ' + name)

        before = (base / 'success' / 'receipt.json').read_bytes()
        rejects(config, 'success', 'exists')
        assert (base / 'success' / 'receipt.json').read_bytes() == before
        for name, mutate, expected in (
            ('unknown', lambda c: c.update(truth=[0]), 'fields'),
            ('altered', lambda c: c['items'][0]['inputs']['raw'].update(sha256='0'*64), 'binding'),
            ('wrong-size', lambda c: c['items'][0]['inputs']['records'].update(bytes=1), 'geometry'),
            ('wrong-binding', lambda c: c['items'][0]['writer']['low'].__setitem__(0, 2), 'coefficient'),
        ):
            changed = copy.deepcopy(config)
            mutate(changed)
            rejects(changed, name, expected)
        raw_path = Path(config['items'][0]['inputs']['raw']['path'])
        raw_body = raw_path.read_bytes()
        raw_path.write_bytes(b'X' + raw_body[1:])
        rejects(config, 'altered-body', 'authentication')
        raw_path.write_bytes(raw_body)
        for marker, expected in (('mismatch', 'byte parity'), ('bad_id', 'byte parity'), ('fail', 'exit 7')):
            (base / marker).touch()
            rejects(config, marker + '-run', expected)
            (base / marker).unlink()
            out = base / (marker + '-run')
            receipt = decode((out / 'receipt.json').read_bytes())
            assert receipt['status'] == 'INVALID' and not receipt['complete'] and receipt['cleanup_complete']
            assert not any(p.is_dir() for p in out.iterdir())
            assert len(receipt['stages']) == 1 and receipt['stages'][0]['exit_status'] in (0, 7)
            assert not any('build' in s['name'] for s in receipt['stages'])
        for marker, cap, expected in (
            ('hang', ('timeout_seconds', 1), 'runtime cap'),
            ('large-log', ('max_log_bytes', 4096), 'log byte cap'),
            ('bad-identity', None, 'diagnostic identity'),
            ('partial-diagnostic', None, 'partial diagnostic'),
        ):
            changed = copy.deepcopy(config)
            if cap:
                changed['resources'][cap[0]] = cap[1]
            (base/marker).touch()
            rejects(changed, marker+'-run', expected)
            (base/marker).unlink()
            out = base/(marker+'-run')
            receipt = decode((out/'receipt.json').read_bytes())
            assert receipt['status'] == 'INVALID' and receipt['complete'] is False and receipt['cleanup_complete']
            assert all(stage['cleanup_complete'] for stage in receipt['stages'])
            assert not any(p.is_dir() for p in out.iterdir())
        # Pending proof, missing native gate, and a lying test roster all fail.
        for name, mutate in (
            ('pending-proof', lambda p: p.update(schema='SOURCE_POLICY_SELECTED_PENDING_NATIVE')),
            ('missing-gate', lambda p: p.update(gate_log=put(base/'missing.log', bytes(log).splitlines(keepends=True)[0]))),
            ('failed-gate', lambda p: p.update(gate_log=put(base/'failed.log', bytes(log).replace(b'"gate_status":0', b'"gate_status":1', 1)))),
            ('missing-test', lambda p: p.update(gate_log=put(base/'missing-test.log', bytes(log).replace(('test '+TESTS['hierarchical-cell-tests'][0]+' ... ok\n').encode(), b'')))),
        ):
            changed_proof = copy.deepcopy(proof)
            mutate(changed_proof)
            changed = copy.deepcopy(config)
            changed['qualification'] = put(base/(name+'-proof.json'), canonical(changed_proof))
            rejects(changed, name, 'proof')
        try:
            load_config(base/'config.json', '0'*64)
        except ValueError:
            pass
        else:
            raise AssertionError('unauthenticated config')
        print('self-check: authenticated paired fixtures, parity/ID refusal, completed gates, exact policy, failure cleanup, no overwrite')


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--self-check']:
            limits = dict(memory_max_bytes=256 << 20, cpu_affinity=sorted(os.sched_getaffinity(0)))
            require(len(limits['cpu_affinity']) == 1, "self-check CPU1 required")
            resource_snapshot(limits)
            signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('120-second self-check cap')))
            signal.alarm(120)
            self_check()
        else:
            require(len(sys.argv) == 4, "usage: CONFIG CONFIG_SHA256 NEW_OUTPUT | --self-check")
            config, pin = load_config(sys.argv[1], sys.argv[2])
            result = prepare(config, pin, sys.argv[3])
            print(json.dumps(dict(status=result['status'], receipt=str(Path(sys.argv[3]).absolute()/'receipt.json'))))
    except Exception as error:
        print('INVALID: ' + str(error), file=sys.stderr)
        sys.exit(2)
