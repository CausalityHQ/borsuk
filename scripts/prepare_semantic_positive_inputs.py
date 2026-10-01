"""Prepare authenticated ReLAION positive-control inputs; never normalize/refit.

CLI: python scripts/prepare_semantic_positive_inputs.py CONFIG CONFIG_SHA NEW_OUTPUT
     python scripts/prepare_semantic_positive_inputs.py --self-check

Closed config (no extra fields):
  schema="borsuk-semantic-positive-inputs-v1", dataset="relaion",
  rows=100000, dimensions=768, count=64, k=100,
  versions={"numpy":"2.3.3","pyarrow":"24.0.0"},
  raw={"bytes":307200000,"sha256":RAW_SHA256},
  inputs={source_parquet, requests, truth_u32, order, sq8, binding,
          coefficient_manifest}.
Each input is {"path":absolute local regular path,"bytes":N,"sha256":SHA}.
Requests are exactly the consumed first64 historical JSONL records, with
query_ordinal and query; historical extra fields are discarded. order is LEu64,
SQ8 is packed i64/f32/768u8. coefficient_manifest contains low and step;
its SHA must match a candidate/control root in binding. binding must assert
coefficient_f32_bits_exact=true and per_id_sq8_payload_exact=true and pin order.

Run under a cgroup v2 MemoryMax<=200M, MemorySwapMax=0 and RuntimeMaxSec=120;
set OPENBLAS_NUM_THREADS=1 and OMP_NUM_THREADS=1. Arrow mappings require an
actual memory cap rather than RLIMIT_AS. The CLI verifies/records that cap
and also sets a 120-second alarm before importing NumPy/Arrow. Root owns real
data execution. Outputs are raw.f32, requests.jsonl, truth.i64, preparation.json.
Truth IDs remain source ordinals; the pinned SQ8 IDs MUST equal order[p].
"""

import hashlib
from contextlib import ExitStack, contextmanager
import json
import math
import os
from pathlib import Path
import signal
import stat
import struct
import sys
import tempfile
import time


RAW_SHA256 = "0d55a09756f41361c89f0ca355a3bc98da6799b4dc7ea39ab34a905844a758c2"
SCHEMA = "borsuk-semantic-positive-inputs-v1"
ROLES = ("source_parquet", "requests", "truth_u32", "order", "sq8", "binding", "coefficient_manifest")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse_json(body):
    def unique(pairs):
        require(len({key for key, _ in pairs}) == len(pairs), "duplicate JSON field")
        return dict(pairs)

    def invalid(value):
        raise ValueError("nonfinite JSON constant: " + value)

    return json.loads(body, object_pairs_hook=unique, parse_constant=invalid)


def regular_path(path):
    path = Path(os.path.abspath(path))
    for component in reversed((path, *path.parents)):
        require(not component.is_symlink(), "symlink path: " + str(component))
    return path


def open_input(path):
    path = regular_path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    stream = os.fdopen(descriptor, "rb")
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        stream.close()
        raise ValueError("input must be a regular file")
    return stream


def stamp(stream):
    metadata = os.fstat(stream.fileno())
    return (metadata.st_dev, metadata.st_ino, metadata.st_size,
            metadata.st_mtime_ns, metadata.st_ctime_ns)


def digest(stream):
    before = stamp(stream)
    stream.seek(0)
    hasher, size = hashlib.sha256(), 0
    while block := stream.read(1 << 20):
        hasher.update(block)
        size += len(block)
    require(stamp(stream) == before, "input changed during authentication")
    stream.seek(0)
    return dict(bytes=size, sha256=hasher.hexdigest())


def valid_sha(value):
    return (type(value) is str and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def validate_config(config, geometry):
    rows, dimensions, count, k = geometry
    expected = dict(schema=SCHEMA, dataset="relaion", rows=rows, dimensions=dimensions,
                    count=count, k=k, versions=dict(numpy="2.3.3", pyarrow="24.0.0"))
    require(type(config) is dict and set(config) == set(expected) | {'inputs', 'raw'}, "closed config fields")
    require(all(config[key] == value for key, value in expected.items()), "fixed ReLAION configuration")
    require(all(type(config[key]) is int for key in ('rows', 'dimensions', 'count', 'k')), "integer geometry")
    raw = config['raw']
    require(type(raw) is dict and set(raw) == {'bytes', 'sha256'}
            and type(raw['bytes']) is int and raw['bytes'] == rows * dimensions * 4
            and valid_sha(raw['sha256']), "raw identity/geometry")
    if geometry == (100000, 768, 64, 100):
        require(raw['sha256'] == RAW_SHA256, "fixed original ReLAION raw SHA")
    require(type(config['inputs']) is dict and set(config['inputs']) == set(ROLES), "closed input roster")
    sizes = dict(truth_u32=count*k*4, order=rows*8, sq8=rows*(12+dimensions))
    maxima = dict(source_parquet=160_000_000, requests=8 << 20, binding=128 << 10, coefficient_manifest=128 << 10)
    for role, ident in config['inputs'].items():
        require(type(ident) is dict and set(ident) == {'path', 'bytes', 'sha256'}, "input pointer fields: " + role)
        require(type(ident['path']) is str and Path(ident['path']).is_absolute()
                and type(ident['bytes']) is int and ident['bytes'] > 0
                and valid_sha(ident['sha256']), "input pointer: " + role)
        require(ident['bytes'] == sizes[role] if role in sizes else ident['bytes'] <= maxima[role],
                "input byte geometry: " + role)


def f32(values, dimensions):
    import numpy as np
    require(type(values) is list and len(values) == dimensions
            and all(type(value) in (int, float) and math.isfinite(value) for value in values), "finite numeric vector geometry")
    with np.errstate(over='ignore', invalid='ignore'):
        vector = np.asarray(values, dtype='<f4')
    require(np.isfinite(vector).all(), "nonfinite f32 vector")
    return vector


@contextmanager
def output_file(path, identities):
    """Exclusive regular writes; SHA covers precisely the bytes sent to disk."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        hasher, size = hashlib.sha256(), 0

        def write(body):
            nonlocal size
            require(stream.write(body) == len(body), "short output write")
            hasher.update(body)
            size += len(body)

        yield write
        stream.flush()
        os.fsync(stream.fileno())
        identities[path.name] = dict(path=str(path), bytes=size, sha256=hasher.hexdigest())


def raw_blocks(stream, rows, dimensions):
    """Same original-f32 producer as run_native_source_precision_http:17-28."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    parquet = pq.ParquetFile(stream)
    schema = parquet.schema_arrow
    require(schema.names.count('embedding') == 1, "source embedding column")
    field = schema.field('embedding').type
    require(pa.types.is_fixed_size_list(field) and field.list_size == dimensions
            and field.value_type == pa.float32(), "source embedding schema")
    remaining = rows
    for batch in parquet.iter_batches(batch_size=256, columns=['embedding'], use_threads=False):
        col = batch.column(0)
        require(col.type == field and not col.null_count and not col.values.null_count,
                "null/schema original source")
        values = np.asarray(col.values.slice(col.offset*dimensions, len(col)*dimensions)
                            .to_numpy(zero_copy_only=False), dtype='<f4').reshape(len(col), dimensions)[:remaining]
        require(np.isfinite(values).all(), "nonfinite original source")
        yield values.tobytes()
        remaining -= len(values)
        if not remaining:
            break
    require(remaining == 0, "short original source")


def validate_mapping(inputs, identities, rows, dimensions):
    import numpy as np
    order = np.frombuffer(inputs['order'].read(), dtype='<u8')
    require(order.shape == (rows,) and np.array_equal(np.sort(order), np.arange(rows, dtype='<u8')),
            "order must be physical-to-source-ordinal permutation")
    stride = 12 + dimensions
    dtype = np.dtype([('id', '<i8'), ('norm', '<f4'), ('code', 'u1', (dimensions,))])
    require(dtype.itemsize == stride, "packed SQ8 record geometry")
    for start in range(0, rows, 256):
        count = min(256, rows-start)
        body = inputs['sq8'].read(count*stride)
        require(len(body) == count*stride, "short SQ8")
        records = np.frombuffer(body, dtype=dtype)
        require(np.array_equal(records['id'], order[start:start+count]), "SQ8 ID differs from order[p]")
    require(not inputs['sq8'].read(1), "trailing SQ8 records")
    binding = parse_json(inputs['binding'].read())
    require(type(binding) is dict and binding.get('coefficient_f32_bits_exact') is True
            and binding.get('per_id_sq8_payload_exact') is True
            and binding.get('source_order_sha256') == identities['order']['sha256'], "binding order/parity authority")
    require(all(valid_sha(binding.get(arm + '_root_sha256')) for arm in ('candidate', 'control')), "binding root identities")
    roots = [arm for arm in ('candidate', 'control')
             if binding[arm + '_root_sha256'] == identities['coefficient_manifest']['sha256']]
    require(roots, "coefficient manifest differs from binding candidate/control roots")
    manifest = parse_json(inputs['coefficient_manifest'].read())
    require(type(manifest) is dict and {'low', 'step', 'sq8_object_sha256'} <= set(manifest)
            and manifest['sq8_object_sha256'] == identities['sq8']['sha256'], "manifest SQ8/coefficient authority")
    coefficients = {name + '_le_f32_hex': f32(manifest[name], dimensions).tobytes().hex() for name in ('low', 'step')}
    return coefficients, roots


def request_bodies(stream, count, dimensions, bits):
    for ordinal in range(count):
        line = stream.readline(65537)
        require(0 < len(line) <= 65536, "request line length/count")
        request = parse_json(line)
        require(type(request) is dict and {'query_ordinal', 'query'} <= set(request)
                and type(request['query_ordinal']) is int and request['query_ordinal'] == ordinal,
                "historical request ordinal")
        vector = f32(request['query'], dimensions)
        body = (json.dumps(dict(ordinal=ordinal, query=vector.tolist()),
                           separators=(',', ':'), allow_nan=False) + '\n').encode()
        require(f32(parse_json(body)['query'], dimensions).tobytes() == vector.tobytes(), "request f32 bit parity")
        bits.update(vector.tobytes())
        yield body
    require(not stream.read(1), "expected exactly consumed64 request records")


def truth_bodies(stream, rows, count, k):
    for _ in range(count):
        body = stream.read(k*4)
        require(len(body) == k*4, "short truth row")
        ids = struct.unpack('<' + str(k) + 'I', body)
        require(max(ids) < rows and len(set(ids)) == k, "truth must contain unique source ordinals below rows")
        wide = struct.pack('<' + str(k) + 'q', *ids)
        require(struct.unpack('<' + str(k) + 'q', wide) == ids, "truth zero-extension parity")
        yield wide
    require(not stream.read(1), "trailing truth")


def prepare(config, output, geometry=(100000, 768, 64, 100), config_identity=None, limits=None):
    import numpy as np
    import pyarrow as pa
    started = time.monotonic()
    validate_config(config, geometry)
    require((np.__version__, pa.__version__) == ('2.3.3', '24.0.0'), "pinned NumPy/Arrow versions")
    rows, dimensions, count, k = geometry
    output = regular_path(output)
    with ExitStack() as stack:
        inputs = {}
        stamps = {}
        for role in ROLES:
            ident = config['inputs'][role]
            stream = stack.enter_context(open_input(ident['path']))
            require(digest(stream) == {key: ident[key] for key in ('bytes', 'sha256')}, "input authentication: " + role)
            inputs[role], stamps[role] = stream, stamp(stream)
        coefficients, roots = validate_mapping(inputs, config['inputs'], rows, dimensions)
        # Validate the small consumed request/truth roster before creating output.
        for _ in request_bodies(inputs['requests'], count, dimensions, hashlib.sha256()):
            pass
        for _ in truth_bodies(inputs['truth_u32'], rows, count, k):
            pass
        inputs['requests'].seek(0)
        inputs['truth_u32'].seek(0)
        output.mkdir(mode=0o700)
        identities = {}
        with output_file(output / 'raw.f32', identities) as write:
            for body in raw_blocks(inputs['source_parquet'], rows, dimensions):
                write(body)
        require({key: identities['raw.f32'][key] for key in ('bytes', 'sha256')} == config['raw'],
                "original ReLAION raw length/SHA differs")
        bits = hashlib.sha256()
        with output_file(output / 'requests.jsonl', identities) as write:
            for body in request_bodies(inputs['requests'], count, dimensions, bits):
                write(body)
        with output_file(output / 'truth.i64', identities) as write:
            for body in truth_bodies(inputs['truth_u32'], rows, count, k):
                write(body)
        require(identities['truth.i64']['bytes'] == count*k*8, "widened truth byte geometry")
        for role in ROLES:
            require(stamp(inputs[role]) == stamps[role], "input modified during preparation: " + role)
            # Reauthenticate retained descriptors before sealing the new identities.
            require(digest(inputs[role]) == {key: config['inputs'][role][key] for key in ('bytes', 'sha256')},
                    "input changed during preparation: " + role)
        receipt = dict(schema='borsuk-semantic-positive-preparation-v1', dataset='relaion',
                       rows=rows, dimensions=dimensions, count=count, k=k,
                       versions=config['versions'], config=config_identity, inputs=config['inputs'],
                       outputs=dict(identities), raw_normalized=False, queries_normalized=False,
                       requests_f32_sha256=bits.hexdigest(), binding=dict(coefficients, manifest_bound_arms=roots,
                                                                                coefficient_f32_bits_exact=True,
                                                                                per_id_sq8_payload_exact=True),
                       mapping=dict(order_dtype='LEu64', order_semantics='physical_position -> source_ordinal',
                                    order_sha256=config['inputs']['order']['sha256'],
                                    sq8_sha256=config['inputs']['sq8']['sha256'], sq8_record_bytes=12+dimensions,
                                    sq8_ids_equal_order=True, sq8_norm_and_code_bytes='original immutable input'),
                       truth_contract=dict(input_dtype='LEu32', output_dtype='LEi64', ids='source_ordinals',
                                           zero_extended=True, remapped=False, unique_per_row=True),
                       resources=limits, elapsed_seconds=time.monotonic()-started)
        with open_input(__file__) as stream:
            receipt['adapter'] = dict(path=str(Path(__file__).absolute()), **digest(stream))
        with output_file(output / 'preparation.json', {}) as write:
            write((json.dumps(receipt, sort_keys=True, allow_nan=False) + '\n').encode())
        descriptor = os.open(output, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return receipt


def resource_cap():
    entry = Path('/proc/self/cgroup').read_text().strip()
    require(entry.startswith('0::/') and '\n' not in entry, "cgroup v2 required")
    cgroup = Path('/sys/fs/cgroup') / entry[3:].lstrip('/')
    memory = (cgroup / 'memory.max').read_text().strip()
    swap = (cgroup / 'memory.swap.max').read_text().strip()
    require(memory != 'max' and 0 < int(memory) <= 200*1024*1024 and swap == '0', "200MiB/no-swap cgroup required")
    require(os.environ.get('OPENBLAS_NUM_THREADS') == '1' and os.environ.get('OMP_NUM_THREADS') == '1', "single-thread numeric libraries required")
    return dict(enforcement='cgroup-v2 actual memory (no RLIMIT_AS)', path=str(cgroup),
                memory_max_bytes=int(memory), memory_swap_max_bytes=0, timeout_seconds=120)


def read_config(path, expected_sha):
    require(valid_sha(expected_sha), "config SHA format")
    with open_input(path) as stream:
        require(os.fstat(stream.fileno()).st_size <= 65536, "bounded config size")
        ident = dict(path=str(regular_path(path)), **digest(stream))
        require(ident['sha256'] == expected_sha, "config authentication")
        body = stream.read()
        require(dict(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
                == {key: ident[key] for key in ('bytes', 'sha256')}, "config changed after authentication")
        return parse_json(body), ident


def main():
    limits = resource_cap()

    def expired(_signal, _frame):
        raise TimeoutError("120-second preparation cap")

    signal.signal(signal.SIGALRM, expired)
    signal.alarm(120)
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        require(len(sys.argv) == 4, "usage: CONFIG CONFIG_SHA NEW_OUTPUT | --self-check")
        path, expected_sha, output = sys.argv[1:]
        config, ident = read_config(path, expected_sha)
        receipt = prepare(config, output, config_identity=ident, limits=limits)
        print(json.dumps(dict(preparation=str(Path(output).absolute() / 'preparation.json'),
                              outputs=receipt['outputs']), sort_keys=True))
    signal.alarm(0)


def self_check():
    """One end-to-end tiny fixture, with corruption at the actual boundaries."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    with tempfile.TemporaryDirectory(prefix="semantic-positive-self-check-") as directory:
        base = Path(directory)
        rows, dim, count, k = 4, 3, 2, 2
        raw = struct.pack("<12f", -0.0, 1.0, -2.0, 3.0, 0.0, 4.0,
                          5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
        values = np.frombuffer(raw, dtype="<f4")
        source = pa.FixedSizeListArray.from_arrays(pa.array(values), dim)
        parquet = base / "source.parquet"
        pq.write_table(pa.table({"embedding": source}), parquet, row_group_size=2)
        requests = base / "requests"
        requests.write_text(''.join(json.dumps(dict(query_ordinal=i,
                            query=[-0.0, 1.0000001192092896, -2.5],
                            historical="discard")) + "\n" for i in range(count)))
        (base / "truth_u32").write_bytes(struct.pack("<4I", 0, 3, 2, 1))
        (base / "order").write_bytes(struct.pack("<4Q", 2, 0, 3, 1))
        sq8 = b''.join(struct.pack("<qf3B", i, 1.0, 7, 8, 9) for i in (2, 0, 3, 1))
        (base / "sq8").write_bytes(sq8)
        manifest = dict(low=[-0.0, 1.0, 2.0], step=[1.0]*dim,
                        sq8_object_sha256=hashlib.sha256(sq8).hexdigest())
        (base / "coefficient_manifest").write_text(json.dumps(manifest))
        (base / "binding").write_text(json.dumps(dict(
            coefficient_f32_bits_exact=True, per_id_sq8_payload_exact=True,
            source_order_sha256=hashlib.sha256((base / 'order').read_bytes()).hexdigest(),
            candidate_root_sha256=hashlib.sha256((base / 'coefficient_manifest').read_bytes()).hexdigest(),
            control_root_sha256='b'*64)))

        def identity(path):
            body = path.read_bytes()
            return dict(path=str(path), bytes=len(body), sha256=hashlib.sha256(body).hexdigest())

        config = dict(schema=SCHEMA, dataset="relaion", rows=rows, dimensions=dim,
                      count=count, k=k, versions=dict(numpy="2.3.3", pyarrow="24.0.0"),
                      raw=dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()),
                      inputs={name: identity(parquet if name == "source_parquet" else base/name)
                              for name in ROLES})
        config_path = base / 'config.json'
        config_path.write_text(json.dumps(config))
        parsed, config_ident = read_config(config_path, identity(config_path)['sha256'])
        assert parsed == config
        try:
            read_config(config_path, '0'*64)
        except ValueError:
            pass
        else:
            raise AssertionError('unauthenticated config')
        before = {name: Path(ident['path']).read_bytes() for name, ident in config['inputs'].items()}
        out = base / "good"
        receipt = prepare(config, out, geometry=(rows, dim, count, k), config_identity=config_ident)
        assert (out / "raw.f32").read_bytes() == raw, "raw normalization/bit mutation"
        assert (out / "truth.i64").read_bytes() == struct.pack("<4q", 0, 3, 2, 1), "truth remapping/widening"
        records = [json.loads(line) for line in (out / "requests.jsonl").read_text().splitlines()]
        assert [record['ordinal'] for record in records] == [0, 1]
        assert all(set(record) == {'ordinal', 'query'} for record in records)
        assert all(struct.pack("<3f", *record['query']) == struct.pack("<3f", -0.0, 1.0000001192092896, -2.5)
                   for record in records), "query signed-zero/f32 parity"
        assert receipt == json.loads((out / "preparation.json").read_text())
        assert receipt['mapping']['sq8_ids_equal_order'] is True
        assert f32([3.4028235e38, -0.0, 1e-45], dim).tobytes() == struct.pack('<3I', 0x7f7fffff, 0x80000000, 1)
        assert all(Path(config['inputs'][name]['path']).read_bytes() == body for name, body in before.items())

        failures = []

        def reject(label, name=None, body=None, authenticated=True, changed=None):
            candidate = json.loads(json.dumps(config))
            if name:
                path = Path(candidate['inputs'][name]['path'])
                path.write_bytes(body)
                if authenticated:
                    candidate['inputs'][name] = identity(path)
            if changed:
                changed(candidate)
            target = base / label
            try:
                prepare(candidate, target, geometry=(rows, dim, count, k))
            except (ValueError, FileExistsError, OSError):
                assert not (target / "preparation.json").exists(), label
                if not authenticated:
                    assert not target.exists(), "tampered input created output"
                failures.append(label)
            else:
                raise AssertionError("accepted " + label)
            finally:
                if name:
                    path.write_bytes(before[name])

        reject("tamper", "truth_u32", struct.pack("<4I", 1, 3, 2, 1), authenticated=False)
        reject("config_extra", changed=lambda candidate: candidate.update(unexpected=True))
        reject("config_versions", changed=lambda candidate: candidate['versions'].update(numpy='0'))
        reject("config_geometry", changed=lambda candidate: candidate.update(dimensions=dim+1))
        reject("truth_range", "truth_u32", struct.pack("<4I", 0, 4, 2, 1))
        reject("truth_duplicate", "truth_u32", struct.pack("<4I", 0, 0, 2, 1))
        reject("truth_short", "truth_u32", struct.pack("<3I", 0, 3, 2))
        reject("order_duplicate", "order", struct.pack("<4Q", 2, 0, 2, 1))
        reject("order_range", "order", struct.pack("<4Q", 2, 0, 4, 1))
        reject("sq8_id", "sq8", struct.pack("<q", 1) + sq8[8:])
        reject("sq8_short", "sq8", sq8[:-1])
        reject("binding_order", "binding", before['binding'].replace(
            config['inputs']['order']['sha256'].encode(), b'0'*64))
        reject("binding_parity", "binding", before['binding'].replace(b'true', b'false'))
        reject("manifest_root", "coefficient_manifest", before['coefficient_manifest'] + b' ')
        for field, coefficient_values in (('low', [1e100, 0, 0]), ('step', [1, 1])):
            # Keep the binding authenticated and cross-bound to isolate coefficient validation.
            altered = dict(manifest, **{field: coefficient_values})
            body = json.dumps(altered).encode()
            binding_path = base / 'binding'
            updated_binding = json.loads(before['binding'])
            updated_binding['candidate_root_sha256'] = hashlib.sha256(body).hexdigest()
            binding_path.write_text(json.dumps(updated_binding))
            old_pointer = config['inputs']['binding']
            config['inputs']['binding'] = identity(binding_path)
            try:
                reject('coefficient_' + field, 'coefficient_manifest', body)
            finally:
                binding_path.write_bytes(before['binding'])
                config['inputs']['binding'] = old_pointer
        reject("ordinal", "requests", before['requests'].replace(b'"query_ordinal": 1', b'"query_ordinal": 0'))
        reject("ordinal_bool", "requests", before['requests'].replace(b'"query_ordinal": 0', b'"query_ordinal": false'))
        reject("query_nonfinite", "requests", before['requests'].replace(b'-2.5', b'1e100'))
        reject("query_dimension", "requests", before['requests'].replace(b', -2.5', b''))
        reject("query_null", "requests", before['requests'].replace(b'-2.5', b'null'))
        reject("duplicate_json", "requests", before['requests'].replace(b'"query_ordinal": 0', b'"query_ordinal": 0, "query_ordinal": 0'))
        reject("request_count", "requests", before['requests'] + before['requests'].splitlines(keepends=True)[0])
        reject("raw_sha", changed=lambda candidate: candidate['raw'].update(sha256='0'*64))
        for label, column in (
            ("source_null", pa.array([None, [3., 0., 4.], [5., 6., 7.], [8., 9., 10.]], type=source.type)),
            ("value_null", pa.FixedSizeListArray.from_arrays(pa.array([None] + list(range(11)), type=pa.float32()), dim)),
            ("source_nonfinite", pa.FixedSizeListArray.from_arrays(pa.array([float('inf')] + list(range(11)), type=pa.float32()), dim)),
            ("source_short", source.slice(0, rows-1)),
            ("source_schema", pa.FixedSizeListArray.from_arrays(pa.array(values, type=pa.float64()), dim)),
        ):
            other = base / "bad.parquet"
            pq.write_table(pa.table({"embedding": column}), other)
            reject(label, "source_parquet", other.read_bytes())
        link = base / "input-link"
        link.symlink_to(base / "truth_u32")
        reject("input_symlink", changed=lambda candidate: candidate['inputs']['truth_u32'].update(path=str(link)))
        output_link = base / 'output_symlink'
        output_link.symlink_to(base / 'unused')
        try:
            prepare(config, output_link, geometry=(rows, dim, count, k))
        except ValueError:
            assert not (base / 'unused').exists()
        else:
            raise AssertionError('symlink output')
        try:
            prepare(config, out, geometry=(rows, dim, count, k))
        except FileExistsError:
            pass
        else:
            raise AssertionError("reused output directory")
        print(json.dumps(dict(self_check="passed", negative_checks=failures,
                              synthetic_only=True, resources=resource_cap())))


if __name__ == "__main__":
    main()
