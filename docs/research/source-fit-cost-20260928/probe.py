"""AWS-only source fitter cost check; no query/truth input exists."""
import hashlib, json, os, subprocess, sys
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1048576), b''):
            digest.update(chunk)
    return digest.hexdigest()

def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    root, tool = Path(sys.argv[2]), Path(sys.argv[3])
    root.mkdir(exist_ok=False)
    pa.set_cpu_count(4)
    pa.set_io_thread_count(2)
    results = {'query_or_truth_used': False, 'qualification': False,
               'affinity': sorted(os.sched_getaffinity(0)),
               'versions': {'numpy': np.__version__, 'pyarrow': pa.__version__}, 'cells': []}
    assert len(results['affinity']) == 4
    def save():
        (root / 'result.json').write_text(json.dumps(results, indent=2) + '\n')
    save()
    for source in config['sources']:
        n = source['rows']
        directory = root / str(n)
        directory.mkdir()
        parquet, raw, normalized = (directory / p for p in ['source.parquet', 'raw.f32', 'normalized.f32'])
        subprocess.run(['aws', 's3', 'cp', source['uri'], str(parquet), '--only-show-errors'], check=True)
        assert parquet.stat().st_size == source['bytes'] and sha(parquet) == source['sha256']
        pf = pq.ParquetFile(parquet)
        assert pf.metadata.num_rows == n
        count = 0
        with raw.open('xb') as output:
            for batch in pf.iter_batches(batch_size=8192, columns=['embedding']):
                column = batch.column(0)
                assert pa.types.is_fixed_size_list(column.type) and column.type.list_size == 768
                assert column.type.value_type == pa.float32() and column.null_count == column.values.null_count == 0
                vectors = np.asarray(column.flatten().to_numpy(zero_copy_only=False), dtype='<f4').reshape(len(column), 768)
                assert np.isfinite(vectors).all()
                output.write(vectors.tobytes())
                count += len(column)
        assert count == n and raw.stat().st_size == n * 768 * 4
        raw_sha = sha(raw)
        if 'raw_sha256' in source:
            assert raw_sha == source['raw_sha256']
        def run(label, args, seconds):
            with (directory / (label + '.stdout')).open('xb') as stdout, (directory / (label + '.stderr')).open('xb') as stderr:
                code = subprocess.run(['/usr/bin/time', '-v', '-o', str(directory / (label + '.time')),
                    'timeout', '--signal=TERM', '--kill-after=10', str(seconds), str(tool), *map(str, args)],
                    stdout=stdout, stderr=stderr).returncode
            return code
        assert run('normalize', ['normalize', raw, raw_sha, n, 768, 1073741824, normalized], 180) == 0
        norm_sha = sha(normalized)
        assert normalized.stat().st_size == raw.stat().st_size
        if 'normalized_sha256' in source:
            assert norm_sha == source['normalized_sha256']
        for mode in ['fit', 'hier-fit']:
            order = directory / (mode + '.u64')
            seconds = 120 if mode == 'fit' or n == 100000 else 600
            code = run(mode, [mode, normalized, norm_sha, n, 768, 1073741824, order], seconds)
            cell = {'rows': n, 'mode': mode, 'exit_code': code, 'timeout_seconds': seconds,
                    'source_sha256': source['sha256'], 'raw_sha256': raw_sha, 'normalized_sha256': norm_sha,
                    'status': 'pass' if code == 0 else 'failed-at-fixed-envelope',
                    'failure_kind': None if code == 0 else ('timeout' if code in (124, 137) else 'admission-or-process-error')}
            if code == 0:
                values = np.fromfile(order, dtype='<u8')
                assert len(values) == n and np.array_equal(np.sort(values), np.arange(n, dtype=np.uint64))
                cell['order_sha256'] = sha(order)
                receipt = json.loads((directory / (mode + '.stdout')).read_text())
                assert receipt['rows'] == n and receipt['order_sha256'] == cell['order_sha256'] and receipt['query_or_truth_used'] is False
                if mode == 'fit' and 'order_sha256' in source:
                    assert cell['order_sha256'] == source['order_sha256']
                if mode == 'hier-fit':
                    extents = receipt['extents']
                    assert extents[0][0] == 0 and extents[-1][1] == n
                    assert all(0 < b-a <= 1024 for a,b in extents)
                    assert all(extents[i][1] == extents[i+1][0] for i in range(len(extents)-1))
                    lengths = [b-a for a,b in extents]
                    cell['extents'] = {'count': len(lengths), 'min_rows': min(lengths), 'max_rows': max(lengths)}
            results['cells'].append(cell)
            save()
            if n == 100000 and mode == 'fit':
                assert code == 0, '100k baseline failed; stop before 1M'
        for path in [parquet, raw, normalized]:
            path.unlink()
    save()

if __name__ == '__main__':
    main()
