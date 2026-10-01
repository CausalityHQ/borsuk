#!/usr/bin/env python3
"""Stage authenticated packages and publish through qualified two_bit_plan_demo.

CLI: python3 -m scripts.prepare_native_semantic_publication CONFIG CONFIG_SHA PUBLISHER NEW_OUTPUT
     python3 -m scripts.prepare_native_semantic_publication --self-check
run(config, config_sha, publisher, out) accepts the CONFIG path and returns a
receipt, or raises after saving a failed receipt. No cold performance claim.

The existing cold config is extended with publication={publisher:{bytes,sha256},
assets:{key,bytes,sha256}, asset_manifest:{path,bytes,sha256},
qualification:{path,bytes,sha256}}. The publisher qualification must explicitly
have qualified=true and integer green_status=release_status=full_suite_status=0,
binary_sha256 plus compiled_native_sha256/source_file_count/source_identity_sha256.
The qualification is supplied by the root AFTER completed full-suite execution.

Asset manifest: schema=borsuk-native-semantic-publication-assets-v1;
files={relative_tar_path:{bytes,sha256}};
items=[{dataset,canonical:{key,bytes,sha256},arms:{control:{directory:DATASET/control},
candidate:{directory:DATASET/candidate}}}], ordered ReLAION then CoHere.
Tar contains ONLY regular files with these exact names, no directory entries:
DATASET/ARM/<each committed preparation publication_roster file>, plus candidate
centroids.bin copied from the control identity; DATASET/requests, DATASET/truth,
DATASET/ARM/reference-k10. Canonical and SQ8 bodies MUST NOT be in the tar.
Panel/reference identities match cold config (the consumed range if specified).
canonical is the unchanged original root object key/SHA/bytes, downloaded once
per dataset and hardlinked into both arm directories. No SQ8 download/refit.
Asset archives are prepared by the root; this module has no pack command.
"""
import argparse
from contextlib import closing
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import signal
import subprocess
import sys
import tarfile
import tempfile
import time

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import package_semantic_native_generation as files
from scripts import run_native_semantic_router_cold as cold
from scripts import check_native_semantic_router_stats as stats

require, sha, regular, write_new = files.require, files.sha, files.regular, files.write_new
CHUNK = 65536
CAP = 4 * 1024 * 1024
EVIDENCE = files.EVIDENCE
PREPARATION = EVIDENCE / 'semantic-native-publication-preparation.json'
REFERENCES = EVIDENCE / 'semantic-native-reference-envelopes.json'
PREPARATION_SHA = '7f80419636603afb9cc5b6b87518eb9f9db6ba109ae5db187c6ea1238e45081e'
REFERENCES_SHA = 'b25dae1d9bfa9e3f94aa918879fe4a20d95aaeda2bccdc594bd6df66dea54b91'
SCHEMA = 'borsuk-native-semantic-publication-assets-v1'
KNOWN_PARITY = tuple(k for k in stats.PARITY if not k.startswith('router_'))
UNKNOWN_NATIVE = ['router_submitted_gets', 'router_verified_bytes', 'router_failed_gets',
                  'native_publication_logical_requests', 'native_transport_submissions',
                  'confirmed_wire_requests', 'wire_bytes', 'credential_payload_bytes']


def json_body(value):
    return (cold.encoded(value) + '\n').encode()


def identity(value):
    require(set(value) == {'bytes', 'sha256'}, 'body identity fields')
    stats.integer(value['bytes'], 'body bytes', 1)
    stats.digest(value['sha256'])
    return value


def object_identity(value):
    require(set(value) == {'key', 'bytes', 'sha256'}, 'object identity fields')
    require(type(value['key']) is str and bool(value['key']), 'object key')
    return identity({k: value[k] for k in ('bytes', 'sha256')})


def body_identity(value):
    return {'bytes': value.get('range_bytes', value['bytes']),
            'sha256': value.get('range_sha256', value['sha256'])}


def small(path, cap=CAP):
    with regular(path) as source:
        size = os.fstat(source.fileno()).st_size
        require(0 < size <= cap, 'local metadata cap')
        body = source.read(size + 1)
    require(len(body) == size, 'local metadata length')
    return body


def authenticated_json(descriptor, cap=CAP):
    require(set(descriptor) == {'path', 'bytes', 'sha256'}, 'local descriptor fields')
    identity({k: descriptor[k] for k in ('bytes', 'sha256')})
    body, value = files.metadata(descriptor['path'], descriptor['sha256'],
                                 cap=cap, length=descriptor['bytes'])
    return body, value


def transfer(source, descriptor, destination):
    """Authenticate exactly one body with reads bounded to 64 KiB."""
    identity(descriptor)
    digest, total = hashlib.sha256(), 0
    with open(destination, 'xb', buffering=0) as target:
        while total < descriptor['bytes']:
            chunk = source.read(min(CHUNK, descriptor['bytes'] - total))
            require(chunk and len(chunk) <= min(CHUNK, descriptor['bytes'] - total), 'stream length')
            digest.update(chunk)
            require(target.write(chunk) == len(chunk), 'short stream write')
            total += len(chunk)
        require(source.read(1) == b'' and digest.hexdigest() == descriptor['sha256'], 'stream identity')
        os.fsync(target.fileno())
    return total


def relative(name):
    require(type(name) is str and bool(name) and '\\' not in name and '\x00' not in name,
            'archive path')
    path = PurePosixPath(name)
    require(not path.is_absolute() and all(p not in ('', '.', '..') for p in name.split('/'))
            and path.as_posix() == name, 'archive path traversal')
    return path


def safe_extract(archive, roster, output):
    for name, descriptor in roster.items():
        relative(name)
        identity(descriptor)
    output = Path(output)
    output.mkdir()  # Fresh private directory; no preexisting symlink ancestors inside it.
    seen = set()
    with regular(archive) as source, tarfile.open(fileobj=source, mode='r|*') as tar:
        for entry in tar:
            relative(entry.name)
            require(entry.isfile() and not entry.issparse(), 'archive entry must be a regular file')
            require(entry.name in roster and entry.name not in seen, 'unrostered/duplicate archive entry')
            expected = roster[entry.name]
            require(entry.size == expected['bytes'], 'archive entry length')
            path = output / entry.name
            path.parent.mkdir(parents=True, exist_ok=True)
            with closing(tar.extractfile(entry)) as body:
                transfer(body, expected, path)
            seen.add(entry.name)
    require(seen == set(roster), 'missing archive entries')


def asset_roster(config, preparation):
    """Exact archive roster for the root packer/controller; no data bodies read."""
    roster = {}
    require([i['dataset'] for i in preparation['items']] == list(cold.DATASETS), 'preparation datasets')
    for item, prepared in zip(config['items'], preparation['items']):
        dataset = item['dataset']
        require(prepared['dataset'] == dataset, 'preparation order')
        for name in ('requests', 'truth'):
            roster[f'{dataset}/{name}'] = body_identity(item['inputs'][name])
        for name in ('control', 'candidate'):
            arm, frozen = item['arms'][name], prepared['arms'][name]
            require(arm['authority']['root_sha256'] == frozen['root_sha256'], 'frozen root identity')
            require(arm['authority']['generation'] == arm['authority']['control_epoch'] == 1,
                    'initial publication authority')
            for path, descriptor in frozen['publication_roster'].items():
                roster[f'{dataset}/{name}/{path}'] = identity(descriptor)
            for path, size in arm['metadata_files'].items():
                descriptor = frozen['publication_roster'][path]
                require(descriptor == {'bytes': size, 'sha256': arm['metadata_sha256'][path]},
                        'cold startup identity')
            if name == 'candidate':
                require(arm['leaf_object'] == frozen['publication_roster']['router/leaves.bin'], 'leaf identity')
                roster[f'{dataset}/{name}/centroids.bin'] = prepared['arms']['control']['publication_roster']['centroids.bin']
            roster[f'{dataset}/{name}/reference-k10'] = body_identity(arm['inputs']['reference-k10'])
    return roster


def validate_manifest(config, manifest, preparation, reference_authority):
    require(set(manifest) == {'schema', 'files', 'items'} and manifest['schema'] == SCHEMA, 'asset manifest schema')
    require(manifest['files'] == asset_roster(config, preparation), 'exact authenticated asset roster')
    require([i['dataset'] for i in manifest['items']] == list(cold.DATASETS), 'asset dataset order')
    prefixes = []
    for item, entry, prepared, refs in zip(config['items'], manifest['items'], preparation['items'],
                                          reference_authority['items']):
        dataset = item['dataset']
        require(set(entry) == {'dataset', 'canonical', 'arms'} and set(entry['arms']) == {'control', 'candidate'},
                'asset item fields')
        object_identity(entry['canonical'])
        require(entry['canonical']['bytes'] == 100000 * (768 * 4 + 8), 'canonical frozen geometry')
        for key in cold.SOURCE_IDENTITIES - {'queries_sha256', 'truth_sha256'}:
            require(item['source_identity'][key] == refs['source_identity'][key], 'frozen source/scorer identity')
        for name, arm in item['arms'].items():
            require(entry['arms'][name] == {'directory': f'{dataset}/{name}'}, 'arm directory')
            require(entry['canonical']['key'] == prepared['arms'][name]['source_payload_keys']['canonical'],
                    'original canonical key')
            require(arm['authority'] == refs['arms'][name]['authority'], 'reference authority')
            prefixes.append(arm['indexes']['10'])
            prefix = relative(arm['indexes']['10'])
            require(prefix.as_posix() == arm['indexes']['10'], 'index prefix')
            stats.integer(arm['head_file']['bytes'], 'HEAD bytes', 1, 4096)
    require(len(prefixes) == len(set(prefixes)) == 4, 'four distinct fresh namespaces')


def validate_publisher(config, publisher):
    descriptor = identity(config['publication']['publisher'])
    files.stream_copy(publisher, descriptor['bytes'], descriptor['sha256'])
    require(os.access(publisher, os.X_OK), 'publisher executable')
    _, proof = authenticated_json(config['publication']['qualification'])
    require(proof['qualified'] is True, 'publisher not qualified')
    for key in ('green_status', 'release_status', 'full_suite_status'):
        require(type(proof[key]) is int and proof[key] == 0, 'pending/failed publisher qualification')
    require(proof['binary_sha256'] == descriptor['sha256'], 'qualified publisher identity')
    inventory = cold.native_inventory()
    compiled = proof['compiled_native_sha256']
    require(type(compiled) is dict and bool(compiled), 'compiled source roster missing')
    require(all(inventory.get(name) == stats.digest(digest) for name, digest in compiled.items()),
            'compiled source identity')
    if 'source_sha256' in proof:
        require(proof['source_sha256'] == inventory, 'complete source snapshot')
    require(type(proof['source_file_count']) is int
            and proof['source_file_count'] == config['native_source_file_count'] == len(inventory), 'source file count')
    digest = sha(cold.encoded(inventory).encode())
    require(digest == proof['source_identity_sha256'] == config['native_source_identity_sha256'], 'source authority')
    return proof


def saved_identity(path):
    with regular(path) as source:
        digest, size = hashlib.sha256(), 0
        for chunk in iter(lambda: source.read(CHUNK), b''):
            digest.update(chunk)
            size += len(chunk)
    return {'bytes': size, 'sha256': digest.hexdigest()}


def sdk_client(region):
    import boto3
    from botocore.config import Config
    return boto3.client('s3', region_name=region,
                        config=Config(retries={'max_attempts': 0}, connect_timeout=30, read_timeout=30))


def sdk_call(client, calls, method, key, bucket):
    record = {'operation': method, 'key': key, 'outcome': 'failed'}
    calls.append(record)
    start = time.monotonic_ns()
    try:
        response = getattr(client, method)(Bucket=bucket, Key=key)
        record['outcome'] = 'returned'
        if 'ContentLength' in response:
            record['declared_bytes'] = response['ContentLength']
        return response
    finally:
        record['elapsed_ns'] = time.monotonic_ns() - start


def fresh(client, calls, bucket, prefix):
    try:
        sdk_call(client, calls, 'head_object', prefix + '/head.json', bucket)
    except Exception as error:
        code = getattr(error, 'response', {}).get('Error', {}).get('Code')
        require(code in ('404', 'NoSuchKey', 'NotFound'), 'fresh HEAD check failed')
        calls[-1]['outcome'] = 'absent'
        return
    raise ValueError('existing HEAD: publication forbidden')


def download(client, calls, bucket, descriptor, path):
    expected = object_identity(descriptor)
    response = sdk_call(client, calls, 'get_object', descriptor['key'], bucket)
    with closing(response['Body']) as body:
        require(type(response['ContentLength']) is int and response['ContentLength'] == expected['bytes'], 'SDK body length')
        count = transfer(body, expected, path)
    calls[-1].update(verified_bytes=count, verified_sha256=expected['sha256'])


def stage(config, manifest, directory, client, calls):
    archive = directory / 'assets.tar'
    download(client, calls, config['bucket'], config['publication']['assets'], archive)
    staged = directory / 'staged'
    safe_extract(archive, manifest['files'], staged)
    panels = cold.prepare(config, directory, fetch=lambda bucket, descriptor, path:
                          {'path': str(staged / str(path.relative_to(directory / 'inputs'))),
                           **body_identity(descriptor)})
    # Every panel/reference was authenticated by the exact tar roster. prepare
    # validates query/truth geometry and all 66 reference envelope records.
    for item, entry in zip(config['items'], manifest['items']):
        dataset = item['dataset']
        for name, arm in item['arms'].items():
            root = json.loads(small(staged / dataset / name / 'manifest.json', CHUNK))
            canonical = root['canonical']
            require(root['schema'] == 'borsuk-two-bit-generation-v7' and root['base_epoch'] == 0
                    and root['generation'] == arm['authority']['generation']
                    and root['discovery']['mode'] == arm['discovery'], 'local root authority')
            require(entry['canonical'] == {'key': canonical['object_key'], 'bytes': canonical['bytes'],
                                          'sha256': canonical['sha256']}, 'canonical descriptor/root identity')
            require(root['sq8_object_sha256'] == item['source_identity']['sq8_sha256'], 'unchanged SQ8 identity')
            prepared = files.metadata(PREPARATION, PREPARATION_SHA, cap=CAP)[1]['items'][list(cold.DATASETS).index(dataset)]
            require(root['sq8_object_key'] == prepared['arms'][name]['source_payload_keys']['sq8'], 'original SQ8 key')
        canonical_path = staged / dataset / 'canonical.bin'
        download(client, calls, config['bucket'], entry['canonical'], canonical_path)
        for name in ('control', 'candidate'):
            os.link(canonical_path, staged / dataset / name / 'canonical.bin', follow_symlinks=False)
    return staged, panels


def native_output(path, item, arm, expected):
    rows = [json.loads(line) for line in small(path).splitlines()]
    require(len(rows) == 66, 'closed native output must have startup,64 queries,summary')
    start, queries, end = rows[0], rows[1:-1], rows[-1]
    require(start['phase'] == 'startup' and end['phase'] == 'summary', 'native output phases')
    require({k: start[k] for k in arm['authority']} == arm['authority'], 'native startup authority')
    for key in ('generation', 'control_epoch'):
        stats.integer(start[key], key, 1)
    require(start['top_k'] == end['top_k'] == 10 and start['declared_panel_count'] == end['count'] == 64,
            'native fixed panel')
    for key in ('publish_wall_ns', 'head_read_wall_ns', 'remote_open_wall_ns'):
        stats.integer(start[key], key, maximum=2**128 - 1)
    stats.integer(end['measurement_wall_ns'], 'native validation wall', maximum=2**128 - 1)
    require([q['query_ordinal'] for q in queries] == list(range(64)), 'native query order')
    for query, reference in zip(queries, expected):
        require(query['phase'] == 'query' and type(query['query_ordinal']) is int, 'native successful query')
        require(all(query[k] == reference[k] for k in KNOWN_PARITY), 'native ordered-ID/range/source/SQ8 parity')
        for key in ('query_wall_ns', 'query_process_cpu_ns'):
            stats.integer(query[key], key, maximum=2**128 - 1)
        # Validate known geometry using already authenticated reference router
        # fields, without claiming the CLI observed router counters.
        checked = dict(reference, **{k: query[k] for k in KNOWN_PARITY}, authority=arm['authority'])
        stats.validate_query(checked, arm, telemetry=False)
    return {'startup': start, 'summary': end, 'validated_queries': 64,
            'validated_fields': list(KNOWN_PARITY), 'unknown': list(UNKNOWN_NATIVE)}


def resources(path):
    text = small(path, CHUNK).decode()
    result = {}
    for name, key in (('Maximum resident set size (kbytes)', 'max_rss_kib'),
                      ('User time (seconds)', 'user_seconds'), ('System time (seconds)', 'system_seconds')):
        values = [line.strip().split(': ', 1)[1] for line in text.splitlines()
                  if line.strip().startswith(name + ': ')]
        require(len(values) == 1, 'missing/duplicate native resource field')
        result[key] = int(values[0]) if key == 'max_rss_kib' else float(values[0])
        require(math.isfinite(result[key]) and result[key] >= 0, 'native resource value')
    require(0 < result['max_rss_kib'] <= 524288, 'native publication RSS ceiling')
    return result


def invoke(command, stdout, stderr):
    # timeout owns the 240s process envelope; the outer watchdog kills the whole
    # session if even the timeout wrapper fails to close. Neither path retries.
    with subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True) as process:
        try:
            return process.wait(timeout=250)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise


def publish_arm(config, publisher, staged, item, name, references, client, receipt, directory):
    arm = item['arms'][name]
    destination = directory / item['dataset'] / name
    destination.mkdir(parents=True)
    row = {'dataset': item['dataset'], 'arm': name, 'authority': arm['authority'],
           'prefix': arm['indexes']['10'], 'outcome': 'failed', 'native_invocations': 0}
    receipt['arms'].append(row)
    fresh(client, receipt['logical_sdk_calls'], config['bucket'], arm['indexes']['10'])
    output = destination / 'native.jsonl'
    root = staged / item['dataset'] / name
    command = ['/usr/bin/time', '-v', '-o', str(destination / 'resources.txt'),
               'timeout', '--signal=TERM', '--kill-after=5', '240', 'taskset', '-c', '0-3',
               str(publisher), str(root), arm['authority']['root_sha256'],
               str(staged / item['dataset'] / 'requests'), cold.input_sha(item['inputs']['requests']),
               str(output), '0', '64', '--live-s3', config['bucket'], config['region'],
               arm['indexes']['10'], '--panel-count', '64', '--top-k', '10']
    row['argv'] = command
    start = time.monotonic_ns()
    saved = {key: os.environ.get(key) for key in ('BORSUK_NATIVE_MEMORY_BYTES', 'AWS_MAX_ATTEMPTS')}
    try:
        os.environ.update(BORSUK_NATIVE_MEMORY_BYTES='536870912', AWS_MAX_ATTEMPTS='1')
        with open(destination / 'stdout.log', 'xb') as stdout, open(destination / 'stderr.log', 'xb') as stderr:
            row['native_invocations'] = 1
            row['returncode'] = invoke(command, stdout, stderr)
    finally:
        row['elapsed_ns'] = time.monotonic_ns() - start
        row['artifacts'] = {p.name: saved_identity(p) for p in destination.iterdir() if p.is_file()}
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    require(row['returncode'] == 0, 'native publisher failed; no advancement')
    row['resources'] = resources(destination / 'resources.txt')
    row['validation'] = native_output(output, item, arm, references)
    response = sdk_call(client, receipt['logical_sdk_calls'], 'get_object', arm['indexes']['10'] + '/head.json', config['bucket'])
    with closing(response['Body']) as body:
        require(type(response['ContentLength']) is int and response['ContentLength'] == arm['head_file']['bytes'] <= 4096,
                'actual HEAD length')
        transfer(body, arm['head_file'], destination / 'head.json')
    head = json.loads(small(destination / 'head.json', 4096))
    require(head == {'schema': 'borsuk-two-bit-head-v2', 'epoch': arm['authority']['control_epoch'],
                     'generation': arm['authority']['generation'], 'root_sha256': arm['authority']['root_sha256'],
                     'mutation': None, 'fence': None}, 'actual HEAD complete authority')
    row['artifacts']['head.json'] = arm['head_file']
    receipt['logical_sdk_calls'][-1].update(verified_bytes=arm['head_file']['bytes'],
                                           verified_sha256=arm['head_file']['sha256'])
    row.update(outcome='published-and-validated', head=head,
               authenticated_publication_payload_bytes={
                   'canonical': (root / 'canonical.bin').stat().st_size,
                   'metadata': sum(arm['metadata_files'].values()) + (root / 'plane/records.bin').stat().st_size
                               + (arm['leaf_object']['bytes'] if name == 'candidate' else 0)},
               publication_payload_scope='authenticated body sizes; native upload attempts and wire bytes unknown')
    return row


def run(config, config_sha, publisher, out):
    config_path = Path(config).absolute()
    config_sha = stats.digest(config_sha)
    body, config = files.metadata(config_path, config_sha, cap=CAP)
    cold.validate_config(config)
    require(config['native_memory_bytes'] == 536870912, 'fixed publication memory admission')
    publisher = Path(publisher).absolute()
    validate_publisher(config, publisher)
    pub = config['publication']
    object_identity(pub['assets'])
    manifest_body, manifest = authenticated_json(pub['asset_manifest'])
    preparation_body, preparation = files.metadata(PREPARATION, PREPARATION_SHA, cap=CAP)
    references_body, references = files.metadata(REFERENCES, REFERENCES_SHA, cap=CAP)
    validate_manifest(config, manifest, preparation, references)
    output = Path(out).absolute()
    output.mkdir()  # No resumptions or reuse of partial publication outputs.
    write_new(output / 'config.json', body)
    write_new(output / 'asset-manifest.json', manifest_body)
    receipt = {'schema': 'borsuk-native-semantic-publication-receipt-v1', 'outcome': 'failed',
               'config_sha256': config_sha, 'publication': pub,
               'adapter_code': {str(p.relative_to(files.REPO)): saved_identity(p)
                                for p in (Path(__file__), Path(files.__file__))}, 'native_source_identity_sha256': config['native_source_identity_sha256'],
               'native_source_file_count': config['native_source_file_count'],
               'preparation_sha256': sha(preparation_body), 'reference_authority_sha256': sha(references_body),
               'source_identities': {i['dataset']: i['source_identity'] for i in config['items']},
               'asset_files': manifest['files'], 'canonical_objects': {i['dataset']: i['canonical'] for i in manifest['items']},
               'logical_sdk_calls': [], 'arms': [], 'unknown_native': list(UNKNOWN_NATIVE),
               'copy_buffer_bytes': CHUNK, 'cold_performance_measured': False,
               'canonical_uploads': 'existing library publisher reuploads canonical once per arm; charged separately',
               'declared_canonical_upload_bytes': sum(i['canonical']['bytes'] * 2 for i in manifest['items']),
               'credential_values_recorded': False}
    start = time.monotonic_ns()
    client = None
    try:
        client = sdk_client(config['region'])
        # Reject ANY preexisting namespace before data transfer/native work.
        for item in config['items']:
            for arm in item['arms'].values():
                fresh(client, receipt['logical_sdk_calls'], config['bucket'], arm['indexes']['10'])
        staged, panels = stage(config, manifest, output, client, receipt['logical_sdk_calls'])
        for item in config['items']:
            for name in ('control', 'candidate'):
                publish_arm(config, publisher, staged, item, name,
                            panels[item['dataset']]['arms'][name]['references'], client, receipt, output)
        receipt['outcome'] = 'published-and-validated'
        return receipt
    except BaseException as error:
        # Do not serialize SDK exception strings (they can contain credentials).
        receipt['error_type'] = type(error).__name__
        raise
    finally:
        if client is not None:
            client.close()
        receipt['elapsed_ns'] = time.monotonic_ns() - start
        receipt['logical_sdk_call_count'] = len(receipt['logical_sdk_calls'])
        write_new(output / 'publication-receipt.json', json_body(receipt))


def self_check():
    import copy
    from contextlib import ExitStack
    from types import SimpleNamespace
    from unittest.mock import Mock, patch

    def rejected(action, expected=(ValueError, KeyError, FileExistsError)):
        try:
            action()
        except expected:
            return
        raise AssertionError('unsafe operation was accepted')

    def bind(body):
        return {'bytes': len(body), 'sha256': sha(body)}

    def tar_bytes(entries, compressed=False):
        out = io.BytesIO()
        with tarfile.open(fileobj=out, mode='w:gz' if compressed else 'w') as tar:
            for name, body, kind in entries:
                entry = tarfile.TarInfo(name)
                entry.type = kind
                entry.size = len(body) if kind == tarfile.REGTYPE else 0
                if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                    entry.linkname = 'safe'
                tar.addfile(entry, io.BytesIO(body) if kind == tarfile.REGTYPE else None)
        return out.getvalue()

    class Bounded(io.BytesIO):
        def read(self, count=-1):
            assert 0 <= count <= CHUNK, 'unbounded stream read'
            return super().read(count)

    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        payload = bytes(range(251)) * 900 + b'tail'
        transfer(Bounded(payload), bind(payload), directory / 'stream')
        assert (directory / 'stream').read_bytes() == payload
        for descriptor, body in ((dict(bind(payload), sha256='0' * 64), payload),
                                 (bind(payload), payload[:-1]), (bind(payload), payload + b'x')):
            rejected(lambda: transfer(Bounded(body), descriptor, directory / ('bad-stream-' + str(len(list(directory.iterdir()))))))
        good = [('deep/safe', payload, tarfile.REGTYPE)]
        for compressed in (False, True):
            archive = directory / ('assets-' + str(compressed))
            archive.write_bytes(tar_bytes(good, compressed))
            output = directory / ('extract-' + str(compressed))
            safe_extract(archive, {'deep/safe': bind(payload)}, output)
            assert (output / 'deep/safe').read_bytes() == payload
            rejected(lambda: safe_extract(archive, {'deep/safe': bind(payload)}, output))
        bad_entries = [[(name, b'x', kind)] for name, kind in (
            ('/absolute', tarfile.REGTYPE), ('../escape', tarfile.REGTYPE),
            ('deep/../escape', tarfile.REGTYPE), ('deep//safe', tarfile.REGTYPE),
            ('deep/./safe', tarfile.REGTYPE), ('deep/safe', tarfile.SYMTYPE),
            ('deep/safe', tarfile.LNKTYPE), ('deep/safe', tarfile.DIRTYPE),
            ('deep/safe', tarfile.FIFOTYPE), ('unexpected', tarfile.REGTYPE))]
        bad_entries.extend([good + good, [], [('deep/safe', payload[:-1], tarfile.REGTYPE)],
                            [('deep/safe', b'z' * len(payload), tarfile.REGTYPE)]])
        for index, entries in enumerate(bad_entries):
            archive = directory / ('unsafe-' + str(index))
            archive.write_bytes(tar_bytes(entries))
            rejected(lambda: safe_extract(archive, {'deep/safe': bind(payload)}, directory / ('bad-extract-' + str(index))))
        fifo = directory / 'fifo'
        os.mkfifo(fifo)
        rejected(lambda: small(fifo))
        link = directory / 'link'
        link.symlink_to(directory / 'stream')
        rejected(lambda: small(link), (OSError,))

        client = Mock()
        no_key = RuntimeError('secret that must not be recorded')
        no_key.response = {'Error': {'Code': '404'}}
        client.head_object.side_effect = no_key
        calls = []
        fresh(client, calls, 'synthetic', 'fresh/prefix')
        assert calls[0]['outcome'] == 'absent' and 'secret' not in json.dumps(calls)
        client.head_object.side_effect = None
        client.head_object.return_value = {'ContentLength': 1}
        rejected(lambda: fresh(client, calls, 'synthetic', 'used/prefix'))
        denied = RuntimeError('credential values')
        denied.response = {'Error': {'Code': 'AccessDenied'}}
        client.head_object.side_effect = denied
        rejected(lambda: fresh(client, calls, 'synthetic', 'denied/prefix'))
        client.get_object.return_value = {'ContentLength': len(payload), 'Body': Bounded(payload)}
        download(client, calls, 'synthetic', dict(key='object', **bind(payload)), directory / 'download')
        assert calls[-1]['verified_bytes'] == len(payload) and (directory / 'download').read_bytes() == payload
        client.get_object.return_value = {'ContentLength': len(payload) + 1, 'Body': Bounded(payload)}
        rejected(lambda: download(client, calls, 'synthetic', dict(key='object', **bind(payload)), directory / 'bad-download'))
        constructed = Mock()
        with patch.dict(sys.modules, boto3=SimpleNamespace(client=constructed),
                        **{'botocore.config': SimpleNamespace(Config=lambda **k: k)}):
            sdk_client('region')
        assert constructed.call_args.kwargs['config']['retries'] == {'max_attempts': 0}

        config = {'bucket': 'synthetic', 'region': 'synthetic-region', 'items': []}
        native_rows, panels, roster, bodies = {}, {}, {}, {}
        prep = {'items': []}
        ref_authority = {'items': []}
        manifest = {'schema': SCHEMA, 'files': roster, 'items': []}
        for dataset in cold.DATASETS:
            source = {key: 'c' * 64 for key in cold.SOURCE_IDENTITIES}
            item = {'dataset': dataset, 'rows': 100000, 'dimensions': 768, 'metric': 'cosine',
                    'query_split': 'consumed development0..63', 'source_identity': source,
                    'inputs': {}, 'arms': {}}
            config['items'].append(item)
            panels[dataset] = {'arms': {}}
            canonical = b'synthetic-canonical-' + dataset.encode()
            entry = {'dataset': dataset, 'canonical': dict(key=dataset + '/canonical', **bind(canonical)),
                     'arms': {}}
            manifest['items'].append(entry)
            prepared = {'dataset': dataset, 'arms': {}}
            prep['items'].append(prepared)
            ref_authority['items'].append({'dataset': dataset, 'source_identity': dict(source), 'arms': {}})
            requests = b''.join(json_body({'query_ordinal': q, 'query': [1] * 768}) for q in range(64))
            import struct
            truth = struct.pack('<100I', *range(100)) * 64
            for name, body in (('requests', requests), ('truth', truth)):
                item['inputs'][name] = dict(key=dataset + '/' + name, **bind(body))
                source['queries_sha256' if name == 'requests' else 'truth_sha256'] = sha(body)
                roster[dataset + '/' + name] = bind(body)
                bodies[dataset + '/' + name] = body
            for name in ('control', 'candidate'):
                mode = 'graph' if name == 'control' else 'semantic'
                root = {'schema': 'borsuk-two-bit-generation-v7', 'generation': 1, 'base_epoch': 0,
                        'discovery': {'mode': mode},
                        'canonical': {'object_key': entry['canonical']['key'], **bind(canonical)},
                        'sq8_object_key': dataset + '/sq8', 'sq8_object_sha256': source['sq8_sha256']}
                root_body = json_body(root)
                authority = {'root_sha256': sha(root_body), 'generation': 1, 'control_epoch': 1}
                head = json_body({'schema': 'borsuk-two-bit-head-v2', 'epoch': 1, 'generation': 1,
                                  'root_sha256': authority['root_sha256'], 'mutation': None, 'fence': None})
                metadata = {'manifest.json': root_body, 'page_manifest.json': b'{}',
                            'page_digests.bin': b'x' * 12512, 'plane/manifest.json': b'{}',
                            'plane/mean.bin': b'x' * 3072, 'plane/page_digests.bin': b'x' * 100000}
                source['mean_sha256'] = sha(metadata['plane/mean.bin'])
                if name == 'control':
                    metadata.update({'centroids.bin': b'synthetic-centroids',
                                     'graph.bin': b'synthetic-graph', 'diverse_graph.bin': b'synthetic-diverse'})
                else:
                    metadata.update({'router/manifest.json': b'{}', 'router/membership.bin': b'x' * 12500})
                arm = {'discovery': mode, 'authority': authority, 'indexes': {'10': dataset + '/' + name + '/index'},
                       'head_file': bind(head), 'metadata_files': {k: len(v) for k, v in metadata.items()},
                       'metadata_sha256': {k: sha(v) for k, v in metadata.items()}, 'inputs': {}}
                item['arms'][name] = arm
                entry['arms'][name] = {'directory': dataset + '/' + name}
                ref_authority['items'][-1]['arms'][name] = {'authority': authority}
                all_files = dict(metadata, **{'plane/records.bin': b'synthetic-records'})
                if name == 'candidate':
                    all_files['router/leaves.bin'] = b'x' * 4812500
                    all_files['centroids.bin'] = b'synthetic-centroids'
                    arm['leaf_object'] = bind(all_files['router/leaves.bin'])
                prepared['arms'][name] = {'root_sha256': authority['root_sha256'],
                    'publication_roster': {k: bind(v) for k, v in all_files.items()
                                           if name == 'control' or k != 'centroids.bin'},
                    'source_payload_keys': {'canonical': entry['canonical']['key'], 'sq8': dataset + '/sq8'}}
                expected = {'ids': list(range(10)), 'ranges': [[0, 199680]], 'planned_bytes': 199680,
                            'submitted_gets': 1, 'verified_bytes': 199680, 'failed_gets': 0,
                            'source_submitted_gets': 1, 'source_verified_bytes': 200, 'source_failed_gets': 0,
                            'router_submitted_gets': 0 if name == 'control' else 8,
                            'router_verified_bytes': 0 if name == 'control' else 123,
                            'router_failed_gets': 0}
                reference_rows = [dict(expected, query_ordinal=q) for q in range(64)]
                panels[dataset]['arms'][name] = {'references': reference_rows}
                header = {'top_k': 10, 'declared_panel_count': 64, 'rows': 100000, 'dimensions': 768,
                          'metric': 'cosine', 'discovery': mode, 'authority': authority,
                          'query_split': item['query_split'], 'source_identity': source}
                reference_body = b''.join(json_body(r) for r in [header, *reference_rows, {'count': 64}])
                arm['inputs']['reference-k10'] = dict(key=dataset + '/' + name + '/reference', **bind(reference_body))
                all_files['reference-k10'] = reference_body
                for path, body in all_files.items():
                    roster[dataset + '/' + name + '/' + path] = bind(body)
                    bodies[dataset + '/' + name + '/' + path] = body
                startup = dict(phase='startup', **authority, top_k=10, declared_panel_count=64,
                               publish_wall_ns=1, head_read_wall_ns=1, remote_open_wall_ns=1)
                rows = [startup, *[dict(phase='query', query_ordinal=q, query_wall_ns=1,
                                       query_process_cpu_ns=1, **{k: expected[k] for k in KNOWN_PARITY}) for q in range(64)],
                        {'phase': 'summary', 'count': 64, 'top_k': 10, 'measurement_wall_ns': 64}]
                native_rows[arm['indexes']['10']] = rows
                bodies[arm['indexes']['10'] + '/head.json'] = head
            bodies[entry['canonical']['key']] = canonical
        for item, ref in zip(config['items'], ref_authority['items']):
            ref['source_identity'] = dict(item['source_identity'])
        assert asset_roster(config, prep) == roster
        full_manifest = copy.deepcopy(manifest)
        for entry in full_manifest['items']:
            entry['canonical']['bytes'] = 308000000
        validate_manifest(config, full_manifest, prep, ref_authority)
        for mutation in ('extra', 'missing-centroids', 'wrong-root', 'duplicate-prefix', 'canonical-key'):
            cfg, man = copy.deepcopy(config), copy.deepcopy(full_manifest)
            if mutation == 'extra': man['files']['extra'] = bind(b'extra')
            elif mutation == 'missing-centroids': del man['files']['CoHere/candidate/centroids.bin']
            elif mutation == 'wrong-root': cfg['items'][0]['arms']['control']['authority']['root_sha256'] = '0' * 64
            elif mutation == 'duplicate-prefix': cfg['items'][1]['arms']['control']['indexes']['10'] = cfg['items'][0]['arms']['control']['indexes']['10']
            else: man['items'][0]['canonical']['key'] = 'foreign-key'
            rejected(lambda: validate_manifest(cfg, man, prep, ref_authority))

        # Exercise real stage extraction/envelope validation and hardlinks with
        # small synthetic canonical bodies; never read the real datasets.
        archive_body = tar_bytes([(k, v, tarfile.REGTYPE) for k, v in bodies.items() if k in roster])
        config['publication'] = {'assets': dict(key='assets', **bind(archive_body))}
        bodies['assets'] = archive_body
        stage_client = Mock()
        stage_client.get_object.side_effect = lambda **k: {'ContentLength': len(bodies[k['Key']]), 'Body': Bounded(bodies[k['Key']])}
        staged_output = directory / 'stage'
        staged_output.mkdir()
        prep_path = directory / 'preparation'
        prep_path.write_bytes(json_body(prep))
        with patch.multiple(sys.modules[__name__], PREPARATION=prep_path, PREPARATION_SHA=sha(json_body(prep))):
            staged, real_panels = stage(config, manifest, staged_output, stage_client, [])
        assert len(real_panels) == 2 and all(len(p['truths']) == 64 for p in real_panels.values())
        assert stage_client.get_object.call_count == 3
        assert [c.kwargs['Key'] for c in stage_client.get_object.call_args_list] == ['assets', 'ReLAION/canonical', 'CoHere/canonical']
        for dataset in cold.DATASETS:
            a, b, shared = (staged / dataset / p for p in ('control/canonical.bin', 'candidate/canonical.bin', 'canonical.bin'))
            assert a.stat().st_ino == b.stat().st_ino == shared.stat().st_ino and shared.stat().st_nlink == 3

        item, arm = config['items'][0], config['items'][0]['arms']['control']
        native = directory / 'native'
        rows = native_rows[arm['indexes']['10']]
        native.write_bytes(b''.join(json_body(r) for r in rows))
        assert native_output(native, item, arm, real_panels['ReLAION']['arms']['control']['references'])['validated_queries'] == 64
        for mutation in ('partial', 'ordinal', 'ids', 'ranges', 'source', 'sq8', 'authority', 'boolean-epoch', 'failed-query'):
            bad = copy.deepcopy(rows)
            if mutation == 'partial': bad.pop()
            elif mutation == 'ordinal': bad[1]['query_ordinal'] = 1
            elif mutation == 'ids': bad[1]['ids'].reverse()
            elif mutation == 'ranges': bad[1]['ranges'][0][1] *= 2
            elif mutation == 'source': bad[1]['source_submitted_gets'] += 1
            elif mutation == 'sq8': bad[1]['verified_bytes'] -= 1
            elif mutation == 'authority': bad[0]['root_sha256'] = '0' * 64
            elif mutation == 'boolean-epoch': bad[0]['control_epoch'] = True
            else: bad[1]['phase'] = 'query-error'
            native.write_bytes(b''.join(json_body(r) for r in bad))
            rejected(lambda: native_output(native, item, arm, real_panels['ReLAION']['arms']['control']['references']))
        native.write_bytes(b''.join(json_body(r) for r in rows))

        state = {'fault': None}
        native_client = Mock()
        native_client.head_object.side_effect = no_key
        def get_head(**kwargs):
            body = bodies[kwargs['Key']]
            if state['fault'] == 'head-tamper': body = b'z' * len(body)
            return {'ContentLength': len(body), 'Body': Bounded(body)}
        native_client.get_object.side_effect = get_head
        def mock_invoke(command, stdout, stderr):
            assert command[:3] == ['/usr/bin/time', '-v', '-o']
            assert command[4:11] == ['timeout', '--signal=TERM', '--kill-after=5', '240', 'taskset', '-c', '0-3']
            assert command[17:19] == ['0', '64'] and command[-4:] == ['--panel-count', '64', '--top-k', '10']
            assert os.environ['BORSUK_NATIVE_MEMORY_BYTES'] == '536870912' and os.environ['AWS_MAX_ATTEMPTS'] == '1'
            assert command[13] == native_rows[command[22]][0]['root_sha256']
            assert command[15] == sha((staged / Path(command[14]).relative_to(staged)).read_bytes())
            stdout.write(b'synthetic stdout'); stderr.write(b'synthetic stderr')
            Path(command[3]).write_text('Maximum resident set size (kbytes): 1\nUser time (seconds): 0.01\nSystem time (seconds): 0.01\n')
            output_rows = native_rows[command[22]]
            if state['fault'] == 'partial': output_rows = output_rows[:20]
            Path(command[16]).write_bytes(b''.join(json_body(r) for r in output_rows))
            if state['fault'] == 'timeout': raise subprocess.TimeoutExpired(command, 250)
            return 1 if state['fault'] == 'exit' else 0
        saved_environment = {k: os.environ.get(k) for k in ('AWS_MAX_ATTEMPTS', 'BORSUK_NATIVE_MEMORY_BYTES')}
        with patch.object(sys.modules[__name__], 'invoke', side_effect=mock_invoke) as invoked:
            receipt = {'arms': [], 'logical_sdk_calls': []}
            pub_out = directory / 'publish'
            pub_out.mkdir()
            for dataset_item in config['items']:
                for name in ('control', 'candidate'):
                    row = publish_arm(config, directory / 'publisher', staged, dataset_item, name,
                                      real_panels[dataset_item['dataset']]['arms'][name]['references'],
                                      native_client, receipt, pub_out)
                    assert row['outcome'] == 'published-and-validated' and row['native_invocations'] == 1
                    assert row['artifacts']['head.json'] == dataset_item['arms'][name]['head_file']
            assert invoked.call_count == 4 and native_client.get_object.call_count == 4
            assert all(r['validation']['unknown'] == UNKNOWN_NATIVE for r in receipt['arms'])
            for fault in ('exit', 'partial', 'timeout', 'head-tamper', 'fresh'):
                state['fault'] = fault
                failure_out = directory / ('failed-' + fault)
                failure_out.mkdir()
                failed = {'arms': [], 'logical_sdk_calls': []}
                before = invoked.call_count, native_client.get_object.call_count
                if fault == 'fresh': native_client.head_object.side_effect = None
                rejected(lambda: publish_arm(config, directory / 'publisher', staged, item, 'control',
                         real_panels['ReLAION']['arms']['control']['references'], native_client, failed, failure_out),
                         (ValueError, subprocess.TimeoutExpired))
                assert failed['arms'][0]['outcome'] == 'failed'
                assert invoked.call_count == before[0] + int(fault != 'fresh')
                assert native_client.get_object.call_count == before[1] + int(fault == 'head-tamper')
                native_client.head_object.side_effect = no_key
            assert all(os.environ.get(k) == v for k, v in saved_environment.items())
        state['fault'] = None
        process = Mock()
        process.__enter__ = Mock(return_value=process)
        process.__exit__ = Mock(return_value=False)
        process.pid = 123456
        process.wait.side_effect = [subprocess.TimeoutExpired(['synthetic'], 250), 0]
        with patch.object(subprocess, 'Popen', return_value=process), patch.object(os, 'killpg') as killed:
            rejected(lambda: invoke(['synthetic'], None, None), (subprocess.TimeoutExpired,))
            killed.assert_called_once_with(123456, signal.SIGKILL)

        # Verify the root qualification gate with a tiny synthetic source roster.
        binary = directory / 'publisher'
        binary.write_bytes(b'qualified-never-executed'); binary.chmod(0o700)
        inventory = {'synthetic.rs': 'a' * 64}
        inventory_sha = sha(cold.encoded(inventory).encode())
        qualification = {'qualified': True, 'green_status': 0, 'release_status': 0, 'full_suite_status': 0,
                         'binary_sha256': sha(binary.read_bytes()), 'compiled_native_sha256': inventory,
                         'source_file_count': 1, 'source_identity_sha256': inventory_sha}
        proof_path = directory / 'qualification'
        proof_path.write_bytes(json_body(qualification))
        config.update(native_source_file_count=1, native_source_identity_sha256=inventory_sha)
        config['publication'].update(publisher=bind(binary.read_bytes()), qualification=dict(path=str(proof_path), **bind(json_body(qualification))))
        with patch.object(cold, 'native_inventory', return_value=inventory):
            validate_publisher(config, binary)
            for key in ('qualified', 'green_status', 'release_status', 'full_suite_status', 'binary_sha256',
                        'source_file_count', 'compiled_native_sha256', 'source_identity_sha256', 'source_sha256'):
                bad = dict(qualification)
                bad[key] = (False if key == 'qualified' else 1 if key.endswith('status') else
                            2 if key == 'source_file_count' else {} if key in ('compiled_native_sha256', 'source_sha256') else '0' * 64)
                proof_path.write_bytes(json_body(bad))
                config['publication']['qualification'] = dict(path=str(proof_path), **bind(json_body(bad)))
                rejected(lambda: validate_publisher(config, binary))
            changed = dict(qualification, compiled_native_sha256={'synthetic.rs': '0' * 64})
            proof_path.write_bytes(json_body(changed))
            config['publication']['qualification'] = dict(path=str(proof_path), **bind(json_body(changed)))
            rejected(lambda: validate_publisher(config, binary))
            proof_path.write_bytes(json_body(qualification))
            config['publication']['qualification'] = dict(path=str(proof_path), **bind(json_body(qualification)))
            binary.write_bytes(b'tampered')
            rejected(lambda: validate_publisher(config, binary))
            binary.write_bytes(b'qualified-never-executed')

        # Run fail-fast orchestration using the independently checked stage and
        # native interfaces. The synthetic publisher and cloud are never run.
        config.update(native_memory_bytes=536870912)
        manifest_path = directory / 'manifest'
        manifest_path.write_bytes(json_body(full_manifest))
        config['publication']['asset_manifest'] = dict(path=str(manifest_path), **bind(json_body(full_manifest)))
        config_path = directory / 'config'
        config_path.write_bytes(json_body(config))
        ref_path = directory / 'reference-authority'
        ref_path.write_bytes(json_body(ref_authority))
        with ExitStack() as stack:
            stack.enter_context(patch.object(cold, 'validate_config'))
            stack.enter_context(patch.object(cold, 'native_inventory', return_value=inventory))
            stack.enter_context(patch.multiple(sys.modules[__name__], PREPARATION=prep_path, PREPARATION_SHA=sha(json_body(prep)),
                                               REFERENCES=ref_path, REFERENCES_SHA=sha(json_body(ref_authority))))
            stack.enter_context(patch.object(sys.modules[__name__], 'stage', return_value=(staged, real_panels)))
            stack.enter_context(patch.object(sys.modules[__name__], 'sdk_client', return_value=native_client))
            invoked = stack.enter_context(patch.object(sys.modules[__name__], 'invoke', side_effect=mock_invoke))
            output = directory / 'run'
            result = run(config_path, sha(json_body(config)), binary, output)
            assert result['outcome'] == 'published-and-validated' and invoked.call_count == 4
            assert result['cold_performance_measured'] is False and result['logical_sdk_call_count'] == 12
            assert len(json.loads((output / 'publication-receipt.json').read_text())['arms']) == 4
            rejected(lambda: run(config_path, '0' * 64, binary, directory / 'wrong-config'))
            rejected(lambda: run(config_path, sha(json_body(config)), binary, output))
            for fault in ('exit', 'partial', 'timeout', 'head-tamper', 'fresh'):
                state['fault'] = fault
                if fault == 'fresh': native_client.head_object.side_effect = None
                out = directory / ('run-failed-' + fault)
                before = invoked.call_count
                rejected(lambda: run(config_path, sha(json_body(config)), binary, out),
                         (ValueError, subprocess.TimeoutExpired))
                result = json.loads((out / 'publication-receipt.json').read_text())
                assert result['outcome'] == 'failed' and len(result['arms']) == int(fault != 'fresh')
                assert invoked.call_count == before + int(fault != 'fresh')
                assert 'secret' not in json.dumps(result)
                native_client.head_object.side_effect = no_key
    print('PASS authenticated tar/stream bounds; exact roster; canonical hardlinks; qualification; 4 mocked arms; HEAD/parity/fresh/timeout/partial fail-fast; native/cloud UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument('config')
        parser.add_argument('config_sha')
        parser.add_argument('publisher')
        parser.add_argument('out')
        args = parser.parse_args()
        try:
            run(args.config, args.config_sha, args.publisher, args.out)
        except Exception as error:
            print('publication failed: ' + type(error).__name__, file=sys.stderr)
            raise SystemExit(1)
