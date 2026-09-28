"""Current-v3 topology mechanism controller; offline, never vendor qualification."""
import hashlib
import json

PAGE_BYTES = 256 * 780
OBJECT_BYTES = 100000 * 780


def _unique_fields(pairs):
    result=dict(pairs)
    if len(result)!=len(pairs): raise ValueError('duplicate JSON key')
    return result


def historical_control_fingerprint(body, plane_body=None, order_sha256=None):
    if len(body) > 65536:
        raise ValueError('root cap')
    text = body.decode('utf-8')
    root = json.loads(text,object_pairs_hook=_unique_fields)
    if not isinstance(root, dict) or root.get('schema') != 'borsuk-two-bit-generation-v3' or type(root.get('base_epoch')) is not int or root['base_epoch'] != 0 or not isinstance(root.get('canonical'), dict):
        raise ValueError('current initial v3 root required')
    plane_sha=None
    if 'plane_manifest_sha256' in root:
        if plane_body is None or len(plane_body)>65536 or hashlib.sha256(plane_body).hexdigest()!=root['plane_manifest_sha256']:
            raise ValueError('authenticated current plane required')
        plane=json.loads(plane_body,object_pairs_hook=_unique_fields)
        keys=['schema','rows','dimensions','seed','record_bytes','source_sha256','sq8_sha256','source_order_sha256','mean_sha256','records_sha256','query_or_truth_used']
        if not isinstance(plane,dict) or set(plane)!=set(keys) or plane['schema']!='borsuk-two-bit-plane-v2' or plane['source_order_sha256']!=order_sha256 or plane['query_or_truth_used'] is not False:
            raise ValueError('current order-bound plane required')
        if any(type(plane[k]) is not int for k in ['rows','dimensions','seed','record_bytes']) or (plane['rows'],plane['dimensions'],plane['seed'],plane['record_bytes'])!=(100000,768,20260923,200):
            raise ValueError('plane fingerprint geometry')
        if any(not isinstance(plane[k],str) or len(plane[k])!=64 or any(c not in '0123456789abcdef' for c in plane[k]) for k in keys if k.endswith('_sha256')):
            raise ValueError('plane fingerprint hashes')
        archive_plane={k:plane[k] for k in keys if k!='source_order_sha256'}
        archive_plane['schema']='borsuk-two-bit-plane-v1'
        plane_sha=hashlib.sha256((json.dumps(archive_plane,separators=(',',':'))+'\n').encode()).hexdigest()
    elif plane_body is not None:
        raise ValueError('unexpected plane witness')
    # Use stdlib JSON decoding to retain each raw value token. This computes an
    # archive fingerprint only; no legacy artifact is written or loaded.
    decoder, fields, pos = json.JSONDecoder(), {}, 1
    def skip(index):
        while index < len(text) and text[index].isspace():
            index += 1
        return index
    pos = skip(pos)
    while pos < len(text) and text[pos] != '}':
        key, pos = decoder.raw_decode(text, pos)
        if not isinstance(key, str) or key in fields:
            raise ValueError('duplicate or invalid root key')
        pos = skip(pos)
        if text[pos] != ':':
            raise ValueError('root separator')
        start = skip(pos + 1)
        _, pos = decoder.raw_decode(text, start)
        fields[key] = text[start:pos]
        pos = skip(pos)
        if text[pos] == '}':
            break
        if text[pos] != ',':
            raise ValueError('root separator')
        pos = skip(pos + 1)
    if pos >= len(text) or text[pos] != '}' or text[pos + 1:].strip():
        raise ValueError('root suffix')
    del fields['canonical'], fields['base_epoch']
    fields['schema'] = json.dumps('borsuk-two-bit-generation-v1')
    if plane_sha is not None:
        fields['plane_manifest_sha256']=json.dumps(plane_sha)
    projected = '{' + ','.join(json.dumps(key) + ':' + fields[key] for key in sorted(fields)) + '}'
    return hashlib.sha256(projected.encode()).hexdigest()


def validate_root_pair(control, candidate):
    for root in [control, candidate]:
        if root.get('schema') != 'borsuk-two-bit-generation-v3' or type(root.get('base_epoch')) is not int or root['base_epoch'] != 0:
            raise ValueError('current initial v3 root required')
    a, b = (dict(root) for root in [control, candidate])
    for key in ['graph_sha256', 'graph_resident_bytes']:
        if key not in a or key not in b:
            raise ValueError('graph identity missing')
        del a[key], b[key]
    if json.dumps(a,sort_keys=True) != json.dumps(b,sort_keys=True):
        raise ValueError('non-topology root fields differ')
    if control['graph_sha256'] == candidate['graph_sha256']:
        raise ValueError('no topology intervention')


def validate_paired_roster(records, roots, partial=False):
    if set(roots) != {'control', 'candidate'} or len(records) > 128 or (not partial and len(records) != 128):
        raise ValueError('paired roster size')
    if roots['control'] == roots['candidate'] or any(not isinstance(sha,str) or len(sha) != 64 or any(c not in '0123456789abcdef' for c in sha) for sha in roots.values()):
        raise ValueError('distinct canonical root hashes required')
    expected = [(i, arm) for i in range(64) for arm in (['control','candidate'] if i % 2 == 0 else ['candidate','control'])]
    for record, (ordinal, arm) in zip(records, expected):
        if type(record.get('query_ordinal')) is not int or record['query_ordinal'] != ordinal or record.get('arm') != arm or record.get('root_sha256') != roots[arm]:
            raise ValueError('paired ordinal/arm/root identity')


def stage_sets(truth, unit_of, page_of, plan, returned, flat):
    for values in [truth,returned,flat]:
        if len(values) > 100 or any(type(v) is not int or not 0 <= v < 100000 for v in values) or len(set(values)) != len(values):
            raise ValueError('unique integer result/GT identities required')
    gt = set(truth)
    phases = []
    for field, cap in [('seed_evaluated_units',128), ('walk_evaluated_units',1272)]:
        values = plan[field]
        if len(values) > cap or len(set(values)) != len(values) or any(type(v) is not int or not 0 <= v < 3125 for v in values):
            raise ValueError('unit trace authority')
        phases.append(set(values))
    candidates, nominated = plan['ranked_candidate_pages'], plan['selected_pages']
    if any(type(p) is not int or not 0 <= p < 391 for p in candidates+nominated) or len(set(candidates)) != len(candidates) or len(set(nominated)) != len(nominated):
        raise ValueError('page trace authority')
    candidate_pages, nominated_pages = set(candidates), set(nominated)
    fetched = set()
    previous_end = -1
    for start, end in plan['ranges']:
        if type(start) is not int or type(end) is not int or not 0 <= start < end <= OBJECT_BYTES or start % PAGE_BYTES or (end % PAGE_BYTES and end != OBJECT_BYTES) or start <= previous_end:
            raise ValueError('range authority')
        fetched.update(range(start // PAGE_BYTES, (end - 1) // PAGE_BYTES + 1))
        previous_end = end
    if not nominated_pages <= candidate_pages or not nominated_pages <= fetched:
        raise ValueError('nomination authority')
    result = dict(seed={i for i in gt if unit_of[i] in phases[0]},walk={i for i in gt if unit_of[i] in phases[1]},
        candidate={i for i in gt if page_of[i] in candidate_pages},nominated={i for i in gt if page_of[i] in nominated_pages},
        physical={i for i in gt if page_of[i] in fetched},returned=gt.intersection(returned),flat=gt.intersection(flat))
    result['visited'] = result['seed'] | result['walk']
    if not result['returned'] <= result['physical']:
        raise ValueError('returned authority')
    return result


def quality_gate(dataset, control, candidate):
    floors = dict(relaion=(6379,6346),cohere=(6392,6342))
    if dataset not in floors or len(control) != 64 or len(candidate) != 64:
        raise ValueError('frozen dataset/split')
    for sample in control + candidate:
        if any(type(sample[key]) is not int or not 0 <= sample[key] <= 100 for key in ['candidate_hits','fetched_hits','returned_hits','flat_hits']):
            raise ValueError('hit counts')
        if sample['returned_hits'] > sample['fetched_hits'] or any(type(sample[key]) is not int for key in ['gets','bytes']):
            raise ValueError('physical/resource counts')
    for samples in [control,candidate]:
        if any(type(s.get('query_ordinal')) is not int or s['query_ordinal'] != i for i,s in enumerate(samples)):
            raise ValueError('sample ordinal identity')
    if any(c['flat_hits'] != a['flat_hits'] for c,a in zip(control,candidate)):
        raise ValueError('exhaustive control changed')
    total = lambda samples, field: sum(s[field] for s in samples)
    fetched_floor, returned_floor = floors[dataset]
    return (total(candidate,'candidate_hits') > total(control,'candidate_hits')
        and total(candidate,'fetched_hits') >= fetched_floor
        and total(candidate,'returned_hits') >= returned_floor
        and total(candidate,'returned_hits') >= total(control,'returned_hits')
        and total(candidate,'flat_hits') - total(candidate,'returned_hits') <= 32
        and sorted(s['returned_hits'] for s in candidate)[3] >= 95
        and all(0 < s['gets'] <= 32 and 0 < s['bytes'] <= 16773120 for s in candidate))


def classify_failure(records, roots, code, stderr):
    validate_paired_roster(records,roots,partial=True)
    if code == 1 and len(records) < 128 and stderr.strip() == 'Error: Invalid("candidate geometry")':
        ordinal = len(records) // 2
        next_arm = (['control','candidate'] if ordinal % 2 == 0 else ['candidate','control'])[len(records) % 2]
        if next_arm == 'candidate':
            return 'scientific-kill'
    return 'invalid-control-or-runtime'
