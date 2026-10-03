import hashlib,json,struct,subprocess
from pathlib import Path

repo=Path('/home/rb/worktrees/borsuk-prod-ready-v9')
base=repo/'docs/research/performance-architecture-20260930'
results=[]
for dataset,attempt in [('relaion','a0002'),('cohere','a0001')]:
    authority=base/f'semantic-native-packaging-{dataset}-{attempt}.json'
    metadata=authority.read_bytes(); proof=json.loads(metadata)
    order_descriptor=json.loads((base/f'semantic-router-scorer-{dataset}-config.json').read_text())['inputs']['order']
    order_path=Path(order_descriptor['path']); order_body=order_path.read_bytes()
    assert len(order_body)==order_descriptor['bytes']==800000
    assert hashlib.sha256(order_body).hexdigest()==order_descriptor['sha256']==proof['files']['order.u64']['sha256']
    order=[x[0] for x in struct.iter_unpack('<Q',order_body)]
    assert sorted(order)==list(range(100000))
    canonical=Path(f'/tmp/borsuk-semantic-native-paired-{dataset}-{attempt}/graph/canonical.bin')
    assert canonical.is_file() and not canonical.is_symlink()
    pin=proof['files']['graph/canonical.bin']; hashed=hashlib.sha256(); id_hash=hashlib.sha256(); count=0
    with canonical.open('rb') as body:
        for physical,ordinal in enumerate(order):
            record=body.read(3080); assert len(record)==3080
            hashed.update(record); id_hash.update(record[:8]); count+=len(record)
            assert struct.unpack_from('<q',record)[0]==ordinal,(dataset,physical,ordinal)
        assert not body.read(1)
    assert count==pin['bytes']==308000000 and hashed.hexdigest()==pin['sha256']
    results.append(dict(dataset=dataset,rows=100000,dimensions=768,
        order_descriptor=order_descriptor,order_authenticated=True,order_bijection=True,
        canonical=dict(path=str(canonical),**pin),canonical_authenticated=True,
        all_canonical_ids_equal_order=True,canonical_id_sequence_sha256=id_hash.hexdigest(),
        packaging_metadata=dict(path=str(authority.relative_to(repo)),bytes=len(metadata),sha256=hashlib.sha256(metadata).hexdigest())))
result=dict(schema='borsuk-next100k-id-admission-v1',source_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
    status='PASS_ID_BINDING_ONLY',datasets=results,query_or_truth_bodies_opened=False,
    canonical_vectors_interpreted=False,canonical_bytes_streamed_for_authentication=True,
    original_stable_id_translation_tested=False,current_format_admission_tested=False,
    quality_or_performance_claim=False)
out=base/'semantic-1m/fixed48/architecture-decision-20261002/id-admission.json'
with out.open('x') as f:json.dump(result,f,sort_keys=True,indent=2);f.write('\n')
print(json.dumps(dict(status=result['status'],datasets=2,rows_checked=200000)))
