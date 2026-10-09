"""Frozen-design arithmetic only; receipt checks are performed by the caller.
CPU intervals are approximate paired-quad Student/delta intervals, not exact
finite-sample bounds. Latency order-statistic coverage assumes independent quads.
"""
import math,statistics
from fractions import Fraction

def ratio_interval(on,off):
    assert len(on)==len(off)==32 and all(x>0 for x in on+off)
    estimate=sum(on)/sum(off)
    residuals=[a-estimate*b for a,b in zip(on,off)]
    se=statistics.stdev(residuals)/math.sqrt(32)/statistics.mean(off)
    return {'estimate':estimate,'lower':estimate-3*se,'upper':estimate+3*se,'clusters':32,'critical_multiplier':3,'scope':'approximate paired-quad delta/Student bound'}

def decision(runs):
    assert len(runs)==256
    arms={a:[] for a in ['A','B']}
    for q in range(64):
        block=runs[q*4:q*4+4];arm=block[0]['arm'];assert arm in arms
        assert all(x['arm']==arm and x['quad']==q for x in block)
        assert [x['trace'] for x in block] in [[False,True,True,False],[True,False,False,True]]
        ords=block[0]['ordinals'];assert len(ords)==len(set(ords))==64
        assert all(x['ordinals']==ords and len(x['wall_ns'])==len(x['cpu_ns'])==64 for x in block)
        assert all(type(n) is int and n>0 for x in block for key in ['wall_ns','cpu_ns'] for n in x[key])
        groups={t:[x for x in block if x['trace']==t] for t in [False,True]}
        values={t:{'query_cpu':sum(sum(x['cpu_ns']) for x in groups[t]),'os_cpu':float(sum((Fraction(x['os_user_seconds'])+Fraction(x['os_system_seconds']) for x in groups[t]),Fraction()))} for t in [False,True]}
        delta=statistics.median([(groups[True][0]['wall_ns'][i]+groups[True][1]['wall_ns'][i]-groups[False][0]['wall_ns'][i]-groups[False][1]['wall_ns'][i])/2 for i in range(64)])
        arms[arm].append({'on':values[True],'off':values[False],'median_added_wall_ns':delta})
    gates={}
    for arm,quads in arms.items():
        assert len(quads)==32
        query=ratio_interval([x['on']['query_cpu'] for x in quads],[x['off']['query_cpu'] for x in quads])
        # Two calls per state/quad, two truncated centisecond components/call.
        os_upper=ratio_interval([x['on']['os_cpu']+.04 for x in quads],[x['off']['os_cpu'] for x in quads])
        os_lower=ratio_interval([x['on']['os_cpu'] for x in quads],[x['off']['os_cpu']+.04 for x in quads])
        walls=sorted(x['median_added_wall_ns'] for x in quads)
        gates[arm]={'query_cpu':query,'whole_os_cpu':{'lower':os_lower['lower'],'upper':os_upper['upper'],'reported_ratio':sum(x['on']['os_cpu'] for x in quads)/sum(x['off']['os_cpu'] for x in quads),'truncation_interval_per_quad_seconds':.04},'latency':{'estimate_ns':statistics.median(walls),'lower_ns':walls[8],'upper_ns':walls[23],'upper_one_sided_coverage_under_independent_quads':sum(math.comb(32,k) for k in range(24))/2**32}}
    passed=all(g['query_cpu']['upper']<=1.02 and g['whole_os_cpu']['upper']<=1.02 and g['latency']['upper_ns']<=1000000 for g in gates.values())
    failed=any(g['query_cpu']['lower']>1.02 or g['whole_os_cpu']['lower']>1.02 or g['latency']['lower_ns']>1000000 for g in gates.values())
    return {'status':'OVERHEAD_GO' if passed else ('OVERHEAD_FAIL' if failed else 'INCONCLUSIVE'),'arms':gates,'receipt_validation_performed':False,'vendor_win_claim':False,'population_percentile_claim':False,'scope':'selected128ordinals, perarm process-quad observer-overhead gate only','confidence_limitations':'CPU intervals approximate; independent stable randomized-quad errors assumed; uncontrolled backend remains disclosed; no optional extension'}
