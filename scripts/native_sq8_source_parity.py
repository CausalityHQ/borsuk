"""Source-only export and exact representation check for the native SQ8 writer.

Diagnostic only; no layout fitting, query/truth access or artifact overwrites.
"""
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        while block := stream.read(4 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def checked(path, identity):
    if sha(path) != identity:
        raise ValueError(f'identity: {path}')


def prepare(source, source_sha, layout, layout_sha, output):
    import pyarrow.parquet as pq
    checked(source, source_sha)
    checked(layout, layout_sha)
    if output.exists():
        raise ValueError('output already exists')
    table = pq.read_table(source, columns=['feature_row_id', 'embedding'])
    ids = table['feature_row_id'].combine_chunks().to_numpy(zero_copy_only=False)
    vectors = table['embedding'].combine_chunks().values.to_numpy(zero_copy_only=False).reshape(100000, 768)
    order = np.load(layout, allow_pickle=False)
    if (vectors.dtype != np.float32 or not np.isfinite(vectors).all()
            or not np.array_equal(ids, np.arange(100000))
            or order.shape != (100000,) or order.dtype != np.int64
            or not np.array_equal(np.sort(order), np.arange(100000))):
        raise ValueError('frozen source/order geometry')
    output.mkdir()
    vectors.astype('<f4', copy=False).tofile(output / 'normalized.f32')
    order.astype('<u8').tofile(output / 'order.u64')
    receipt = dict(source_sha256=source_sha, layout_sha256=layout_sha,
        normalized_sha256=sha(output / 'normalized.f32'), order_sha256=sha(output / 'order.u64'),
        rows=100000, dimensions=768, query_or_truth_used=False)
    (output / 'export.json').write_text(json.dumps(receipt, indent=2)+'\n')
    return receipt


def compare(reference, reference_sha, root, root_sha, native, receipt_path, output):
    checked(reference, reference_sha)
    checked(root, root_sha)
    manifest = json.loads(root.read_text())
    receipt = json.loads(receipt_path.read_text())
    checked(native, receipt['sq8_sha256'])
    if (manifest['sq8_object_sha256'] != reference_sha
            or receipt['rows'] != 100000 or receipt['query_or_truth_used'] is not False):
        raise ValueError('reference/native binding')
    dtype = np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    if reference.stat().st_size != 78000000 or native.stat().st_size != 78000000:
        raise ValueError('body geometry')
    old, new = [np.memmap(p, mode='r', dtype=dtype, shape=(100000,)) for p in [reference, native]]
    codes_differ = int(np.count_nonzero(old['code'] != new['code']))
    ids_differ = int(np.count_nonzero(old['id'] != new['id']))
    low_same, step_same = [np.array_equal(np.asarray(manifest[k],np.float32),
        np.asarray(receipt[k],np.float32)) for k in ['low','step']]
    difference = np.abs(old['norm'].astype(np.float64)-new['norm'].astype(np.float64))
    max_difference = float(difference.max())
    finite_positive = bool(np.isfinite(new['norm']).all() and (new['norm'] > 0).all())
    result = dict(schema='borsuk-native-sq8-representation-parity-v1', rows=100000, dimensions=768,
        reference_sha256=reference_sha, root_sha256=root_sha, native_sha256=receipt['sq8_sha256'],
        differing_codes=codes_differ, differing_ids=ids_differ, low_equal=low_same, step_equal=step_same,
        differing_norms=int(np.count_nonzero(difference)), max_abs_norm_difference=max_difference,
        mean_abs_norm_difference=float(difference.mean()), finite_positive_norms=finite_positive,
        representation_gate_pass=bool(codes_differ == ids_differ == 0 and low_same and step_same
            and finite_positive and max_difference <= 2e-6),
        qualification=False, query_or_truth_used=False)
    with output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    return result


def replay(config, config_sha, output):
    from scripts.native_two_bit_cosine_development import score_panel
    checked(config, config_sha)
    cfg = json.loads(config.read_text())
    if (cfg['first'], cfg['count']) != (256, 744):
        raise ValueError('frozen validation split')
    for key in ['root', 'native', 'native_receipt', 'parity', 'requests', 'truth', 'plans']:
        checked(cfg[key], cfg[key+'_sha256'])
    parity = json.loads(Path(cfg['parity']).read_text())
    receipt = json.loads(Path(cfg['native_receipt']).read_text())
    if (not parity['representation_gate_pass']
            or parity['native_sha256'] != cfg['native_sha256']
            or parity['root_sha256'] != cfg['root_sha256']
            or receipt['sq8_sha256'] != cfg['native_sha256']):
        raise ValueError('native representation binding')
    requests = [json.loads(line) for line in Path(cfg['requests']).read_text().splitlines()]
    plans = [json.loads(line) for line in Path(cfg['plans']).read_text().splitlines()]
    truth = np.fromfile(cfg['truth'], dtype='<u4').reshape(1000, 100)
    if (len(requests) != 1000 or len(plans) != 744
            or [r['query_ordinal'] for r in requests] != list(range(1000))
            or (truth >= 100000).any()):
        raise ValueError('frozen query/plan/truth geometry')
    dtype = np.dtype([('id','<i8'),('norm','<f4'),('code','u1',(768,))])
    native = np.memmap(cfg['native'], mode='r', dtype=dtype, shape=(100000,))
    low, step = [np.asarray(receipt[key], np.float32) for key in ['low','step']]
    metrics, samples, passed = score_panel(requests[256:], plans, truth, native, low, step, 256, 744)
    result = dict(schema='borsuk-native-sq8-frozen-range-replay-v1', dataset=cfg['dataset'],
        rows=100000, dimensions=768, metric='cosine', k=100, first=256, queries=744,
        metrics=metrics, max_gets=max(s['gets'] for s in samples), max_bytes=max(s['bytes'] for s in samples),
        quality_gate_pass=passed, qualification=False, config_sha256=config_sha, samples=samples,
        limits='Native SQ8 scores on frozen semantic range plans; no live AWS or regenerated root serving')
    with output.open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    return {k:v for k,v in result.items() if k != 'samples'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='mode', required=True)
    prep = sub.add_parser('prepare')
    for arg in ['source','source-sha','layout','layout-sha','output']:
        prep.add_argument('--'+arg,required=True)
    comp = sub.add_parser('compare')
    for arg in ['reference','reference-sha','root','root-sha','native','receipt','output']:
        comp.add_argument('--'+arg,required=True)
    rep = sub.add_parser('replay')
    for arg in ['config','config-sha','output']:
        rep.add_argument('--'+arg,required=True)
    args = parser.parse_args()
    if args.mode == 'prepare':
        result = prepare(Path(args.source),args.source_sha,Path(args.layout),args.layout_sha,Path(args.output))
    elif args.mode == 'replay':
        result = replay(Path(args.config),args.config_sha,Path(args.output))
    else:
        result = compare(Path(args.reference),args.reference_sha,Path(args.root),args.root_sha,
            Path(args.native),Path(args.receipt),Path(args.output))
    print(json.dumps(result))
