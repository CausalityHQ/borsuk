"""Small metadata/authority/decomposition/gate checks; no corpus required."""
import hashlib
import json
from scripts.native_two_bit_topology import (
    historical_control_fingerprint, validate_root_pair, validate_paired_roster,
    stage_sets, quality_gate, classify_failure,
)


def rejects(call):
    try:
        call()
    except ValueError:
        return
    raise AssertionError('invalid input accepted')


def fingerprint_check():
    # Preserve raw floating tokens, including exponent spelling; digest only.
    body = b'{"base_epoch":0,"canonical":{"rows":100000,"dimensions":768},"graph_sha256":"g","low":[1e-6,-0.0],"schema":"borsuk-two-bit-generation-v3"}'
    historical = b'{"graph_sha256":"g","low":[1e-6,-0.0],"schema":"borsuk-two-bit-generation-v1"}'
    assert historical_control_fingerprint(body) == hashlib.sha256(historical).hexdigest()
    rejects(lambda: historical_control_fingerprint(body.replace(b'v3', b'v1')))
    rejects(lambda: historical_control_fingerprint(body.replace(b'"base_epoch":0', b'"base_epoch":1')))
    rejects(lambda: historical_control_fingerprint(body.replace(b'"graph_sha256":"g"', b'"graph_sha256":"g","graph_sha256":"h"')))


def root_check():
    control = dict(schema='borsuk-two-bit-generation-v3',base_epoch=0,canonical=dict(rows=100000,dimensions=768),plane_manifest_sha256='plane',graph_sha256='a',graph_resident_bytes=100)
    candidate = dict(control,graph_sha256='b',graph_resident_bytes=101)
    validate_root_pair(control,candidate)
    rejects(lambda: validate_root_pair(control,dict(candidate,plane_manifest_sha256='other')))
    rejects(lambda: validate_root_pair(control,dict(candidate,schema='borsuk-two-bit-generation-v1')))
    rejects(lambda: validate_root_pair(control,control))


ROOTS = dict(control='a'*64,candidate='b'*64)
def roster():
    return [dict(query_ordinal=i,arm=arm,root_sha256=ROOTS[arm])
            for i in range(64) for arm in (['control','candidate'] if i%2==0 else ['candidate','control'])]


def roster_check():
    records = roster()
    validate_paired_roster(records,ROOTS)
    validate_paired_roster(records[:1],ROOTS,partial=True)
    rejects(lambda: validate_paired_roster(records[:-1],ROOTS))
    wrong = [dict(r) for r in records];wrong[1]['root_sha256']=ROOTS['control']
    rejects(lambda: validate_paired_roster(wrong,ROOTS))
    rejects(lambda: validate_paired_roster([records[1],records[0]]+records[2:],ROOTS))


def stages_check():
    plan = dict(seed_evaluated_units=[0],walk_evaluated_units=[8,16],ranked_candidate_pages=[0,1,2],selected_pages=[0,2],ranges=[[0,3*199680]])
    stages = stage_sets([0,1,2,3],dict(enumerate([0,8,16,24])),dict(enumerate([0,1,2,3])),plan,[0,2],[0,1,3])
    assert stages == dict(seed={0},walk={1,2},visited={0,1,2},candidate={0,1,2},nominated={0,2},physical={0,1,2},returned={0,2},flat={0,1,3})
    rejects(lambda: stage_sets([0,1,2,3],dict(enumerate([0,8,16,24])),dict(enumerate([0,1,2,3])),dict(plan,selected_pages=[3]),[0],[0]))
    rejects(lambda: stage_sets([0,1,2,3],dict(enumerate([0,8,16,24])),dict(enumerate([0,1,2,3])),plan,[3],[0]))


def samples(total):
    return [total//64+(i<total%64) for i in range(64)]


def gate_samples():
    control = [dict(query_ordinal=i,candidate_hits=f,fetched_hits=f,returned_hits=r,flat_hits=e,gets=32,bytes=16773120)
        for i,(f,r,e) in enumerate(zip(samples(6372),samples(6346),samples(6369)))]
    candidate = [dict(c) for c in control]
    for c in candidate[36:43]:c['candidate_hits']+=1;c['fetched_hits']+=1
    return control,candidate


def gate_check():
    control,candidate = gate_samples()
    assert quality_gate('relaion',control,candidate)
    rejects(lambda: quality_gate('relaion',control[:-1],candidate))
    assert not quality_gate('relaion',control,control)
    bad = [dict(c) for c in candidate];bad[0]['bytes']+=1
    assert not quality_gate('relaion',control,bad)
    bad = [dict(c) for c in candidate];bad[0]['returned_hits']-=1
    assert not quality_gate('relaion',control,bad)
    bad = [dict(c) for c in candidate]
    for c in bad:c['returned_hits']-=1
    assert not quality_gate('relaion',control,bad)


def failure_check():
    assert classify_failure(roster()[:1],ROOTS,1,'Error: Invalid("candidate geometry")') == 'scientific-kill'
    assert classify_failure([],ROOTS,1,'Error: Invalid("candidate geometry")') == 'invalid-control-or-runtime'
    assert classify_failure(roster()[:1],ROOTS,-15,'candidate geometry') == 'invalid-control-or-runtime'
    assert classify_failure(roster(),ROOTS,1,'disk full') == 'invalid-control-or-runtime'


def review_regressions():
    control,candidate = gate_samples()
    root = dict(schema='borsuk-two-bit-generation-v3',base_epoch=0,low=[0.0],graph_sha256='a',graph_resident_bytes=100)
    same = [dict(r,root_sha256=ROOTS['control']) for r in roster()]
    upper = dict(ROOTS,candidate='B'*64)
    upper_records = [dict(r,root_sha256=upper[r['arm']]) for r in roster()]
    plan = dict(seed_evaluated_units=[0],walk_evaluated_units=[8,16],ranked_candidate_pages=[0,1,2],selected_pages=[0,2],ranges=[[0,3*199680]])
    stage = lambda gt,ret,p: stage_sets(gt,dict(enumerate([0,8,16,24])),dict(enumerate([0,1,2,3])),p,ret,[0,1,3])
    change = lambda **fields: [dict(candidate[0],**fields)]+candidate[1:]
    cases = [
        ('signed-zero root',lambda: validate_root_pair(root,dict(root,low=[-0.0],graph_sha256='b'))),
        ('same root roster',lambda: validate_paired_roster(same,dict(control=ROOTS['control'],candidate=ROOTS['control']))),
        ('noncanonical root hash',lambda: validate_paired_roster(upper_records,upper)),
        ('fractional nominated page',lambda: stage([0,1,2,3],[0,2],dict(plan,selected_pages=[0,2.0]))),
        ('fractional GT',lambda: stage([0,1.9,2,3],[0,2],plan)),
        ('duplicate GT',lambda: stage([0,1,2,3,3],[0,2],plan)),
        ('negative returned ID',lambda: stage([0,1,2,3],[0,-1],plan)),
        ('fractional GET count',lambda: quality_gate('relaion',control,change(gets=1.5))),
        ('boolean GET count',lambda: quality_gate('relaion',control,change(gets=True))),
        ('sample ordinal mismatch',lambda: quality_gate('relaion',control,change(query_ordinal=63))),
        ('returned exceeds fetched',lambda: quality_gate('relaion',control,change(fetched_hits=0))),
    ]
    failed=[]
    for name,call in cases:
        try: rejects(call)
        except AssertionError: failed.append(name)
    if classify_failure(roster()[:1],ROOTS,1,'Error: Invalid("candidate geometry")\npanic: unrelated') != 'invalid-control-or-runtime':
        failed.append('extra failure stderr')
    for name in failed: print('review regression: '+name)
    assert not failed, f'{len(failed)} topology review regressions rejected incorrectly'
    print('twelve topology review regressions passed')


if __name__ == '__main__':
    checks = [fingerprint_check,root_check,roster_check,stages_check,gate_check,failure_check]
    failures = 0
    for check in checks:
        try:
            check()
        except NotImplementedError as error:
            failures+=1;print(check.__name__+': '+str(error))
    assert failures == 0, f'{failures} topology controller checks not implemented'
    review_regressions()
    print('six topology controller checks passed')
