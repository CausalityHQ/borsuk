"""Authenticated local geometry replay; no source fetch, scores, or live claims.

Run: python3 scripts/check_native_source_paging_replay.py --self-check
     python3 scripts/check_native_source_paging_replay.py --output REPORT.json
"""
import argparse
import gzip
import hashlib
import json
import math
import tempfile
from bisect import bisect_right
from copy import deepcopy
from itertools import combinations
from pathlib import Path

BASE = Path('docs/research/native-union-20260928')
CAPS = (8, 16, 32, 64, 128, 256)
RECORD_BYTES = 200
SQ8_BOUND = 16773120
CAMPAIGNS = (
    ('source-completion-1m', 'plan-candidate', 'generation', 1000000,
     '2e3306d61c5ecc83a7bd95578e9a9f0dd1c163b500b76761796febb1b3e3e365', ('relaion',)),
    ('source-completion-http', 'core-plan', 'candidate', 100000,
     'bb027f633bce5293ba09b7568506b6f7d53cdb8a85480fcd98669644a8d7bbcd',
     ('relaion', 'cohere')),
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(body):
    return hashlib.sha256(body).hexdigest()


def authenticate(body, identity):
    require(len(body) == identity['bytes'] and sha(body) == identity['sha256'],
            'artifact body identity mismatch')
    return body


def terminal_at(directory, expected_sha):
    body = (directory / 'aws-terminal.json').read_bytes()
    require(sha(body) == expected_sha ==
            (directory / 'aws-terminal.sha256').read_text().split()[0],
            'terminal identity mismatch')
    terminal = json.loads(body)
    require(terminal['status'] == terminal['phase'] == 'complete' and
            terminal['exit_code'] == 0, 'campaign not closed successfully')
    return terminal


def artifact_at(directory, terminal, name, identities):
    # Only terminal-listed, already-local decompressed bodies are used.
    identity = terminal['artifacts'][name]
    body = authenticate(gzip.decompress((directory / (name + '.gz')).read_bytes()), identity)
    identities[name] = identity
    return body


def blocks(ids, block_rows, rows):
    require(type(rows) is int and rows > 0, 'invalid row count')
    require(all(type(i) is int and 0 <= i < math.ceil(rows / block_rows) for i in ids),
            'invalid block id')
    return [(i * block_rows * RECORD_BYTES,
             min((i + 1) * block_rows, rows) * RECORD_BYTES) for i in sorted(set(ids))]


def coalesce(intervals):
    merged = []
    for start, end in sorted(intervals):
        require(0 <= start < end, 'invalid interval')
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def cover_groups(groups, max_gets):
    """Minimum bytes under a shared GET cap, with mandatory group boundaries.

    Every cover starts at its first required byte and ends at its last.
    A split saves precisely its gap size: retain the largest available gaps.
    """
    groups = [coalesce(group) for group in groups if group]
    require(type(max_gets) is int and max_gets >= len(groups), 'GET cap cannot cover groups')
    gaps = [(right[0] - left[1], group_id, i)
            for group_id, group in enumerate(groups)
            for i, (left, right) in enumerate(zip(group, group[1:], strict=False))]
    cuts = {(group_id, i) for _, group_id, i in
            sorted(gaps, key=lambda gap: (-gap[0], gap[1], gap[2]))[:max_gets - len(groups)]}
    ranges = []
    for group_id, group in enumerate(groups):
        start = group[0][0]
        for i, (_, end) in enumerate(group):
            if i == len(group) - 1 or (group_id, i) in cuts:
                ranges.append((start, end))
                if i + 1 < len(group):
                    start = group[i + 1][0]
    return ranges


def byte_count(ranges):
    return sum(end - start for start, end in ranges)


def contains(ranges, interval):
    return any(start <= interval[0] and interval[1] <= end for start, end in ranges)


def unfetched_groups(required, fetched):
    """Partition completion blocks into holes; never refetch first-wave bytes."""
    ends = [end for _, end in fetched]
    groups = {}
    for interval in required:
        hole = bisect_right(ends, interval[0])
        if hole < len(fetched) and fetched[hole][0] <= interval[0]:
            require(interval[1] <= fetched[hole][1], 'partial cached block')
            continue
        require(hole == len(fetched) or interval[1] <= fetched[hole][0],
                'completion overlaps fetched boundary')
        groups.setdefault(hole, []).append(interval)
    return list(groups.values())


def two_wave(initial, completion, max_gets):
    """Replay all cap allocations for greedy initial covers, then exact hole covers.

    Cap allocation uses the recorded path. This is not a globally optimal joint
    scheduling claim: first-wave cuts minimize initial bytes without future scores.
    """
    best = None
    for first_cap in range(1, min(max_gets, len(coalesce(initial))) + 1):
        first = cover_groups([initial], first_cap)
        groups = unfetched_groups(completion, first)
        remaining_cap = max_gets - len(first)
        if len(groups) > remaining_cap:
            continue
        second = cover_groups(groups, remaining_cap)
        candidate = (byte_count(first) + byte_count(second), len(first) + len(second),
                     first, second)
        if best is None or candidate < best:
            best = candidate
    require(best is not None, 'no feasible two-wave replay')
    return best[2], best[3]


def unit_list(value, count):
    require(isinstance(value, list) and all(type(u) is int and 0 <= u < count for u in value)
            and len(value) == len(set(value)), 'invalid or duplicate units')
    return value


def validate_trace(query, rows):
    units = math.ceil(rows / 32)
    page_count = math.ceil(rows / 256)
    discoveries = query['discoveries']
    require(isinstance(discoveries, list) and 1 <= len(discoveries) <= 2, 'invalid discoveries')
    walked = set()
    for discovery in discoveries:
        seed = discovery['seed_page']
        require(type(seed) is int and 0 <= seed < page_count, 'invalid seed page')
        walk = unit_list(discovery['walk_evaluated_units'], units)
        # plan_inner substitutes the whole seed page when count == 1.
        if page_count == 1:
            require(not walk, 'single-page trace unexpectedly has a walk')
            walk = list(range(seed * 8, min((seed + 1) * 8, units)))
        require(0 < len(walk) <= 1272 and
                set(range(seed * 8, min((seed + 1) * 8, units))) <= set(walk),
                'walk misses seed or exceeds bound')
        walked.update(walk)
    initial = sorted(walked)
    nominated = unit_list(query['nomination_evaluated_units'], units)
    pages = {u // 8 for u in initial}
    closure = {u for page in pages for u in range(page * 8, min((page + 1) * 8, units))}
    require(nominated[:len(initial)] == initial, 'initial nomination is not sorted walk union')
    require(set(nominated) <= closure, 'nomination escapes initial page closure')
    require(len(nominated) == min(2544, len(closure)), 'incomplete or oversized nomination')
    completion = nominated[len(initial):]
    visited = set()
    offset = 0
    while offset < len(completion):
        page = completion[offset] // 8
        require(page not in visited, 'completion revisits page')
        visited.add(page)
        expected = [u for u in range(page * 8, min((page + 1) * 8, units)) if u not in walked]
        actual = completion[offset:offset + len(expected)]
        require(actual == expected[:len(actual)], 'invalid completion order')
        require(len(actual) == len(expected) or len(nominated) == 2544, 'premature completion stop')
        offset += len(actual)
    ranked = unit_list(query['ranked_candidate_pages'], page_count)
    require(ranked and len(ranked) <= 2 * min(page_count, 159) and set(ranked) <= pages,
            'invalid ranked candidate pages')
    require(query['primary_page'] == ranked[0], 'primary does not match ranking')
    selected = unit_list(query['selected_pages'], page_count)
    require(selected == sorted(selected) and set(selected) <= set(ranked), 'invalid selection')
    ranges = query['ranges']
    require(isinstance(ranges, list) and all(isinstance(r, list) and len(r) == 2 and
            all(type(n) is int for n in r) and 0 <= r[0] < r[1] <= rows * 780 for r in ranges),
            'invalid SQ8 ranges')
    require(all(left[1] < right[0] for left, right in zip(ranges, ranges[1:], strict=False)),
            'uncoalesced SQ8 ranges')
    require(type(query['planned_bytes']) is int and
            query['planned_bytes'] == byte_count(ranges) <= SQ8_BOUND, 'invalid SQ8 byte count')
    require(all(contains(ranges, (page * 256 * 780, min((page + 1) * 256, rows) * 780))
                for page in selected), 'SQ8 selection not covered')
    return initial, completion, sorted(pages), nominated


def summary(values):
    ordered = sorted(values)
    return {name: ordered[math.ceil(p * len(ordered)) - 1]
            for name, p in [('p50', .5), ('p90', .9), ('p95', .95), ('max', 1)]}


def validate_queries(queries):
    require(len(queries) == 64 and [q['query_ordinal'] for q in queries] == list(range(64)) and
            all(type(q['query_ordinal']) is int for q in queries), 'expected exactly queries 0-63')


def replay_dataset(directory, terminal, dataset, trace_name, folder, rows):
    identities = {}

    def body(name):
        return artifact_at(directory, terminal, f'screen/{dataset}/{name}', identities)

    plane_body = body(f'{folder}/plane/manifest.json')
    page_body = body(f'{folder}/page_manifest.json')
    root_body = body(f'{folder}/manifest.json')
    plane, page, root = map(json.loads, (plane_body, page_body, root_body))
    require(plane['schema'] == 'borsuk-two-bit-plane-v2' and plane['rows'] == rows and
            plane['dimensions'] == 768 and plane['record_bytes'] == RECORD_BYTES and
            plane['query_or_truth_used'] is False, 'unexpected source geometry')
    require(page['rows'] == rows and page['dimensions'] == 768 and page['page_rows'] == 256 and
            root['plane_manifest_sha256'] == sha(plane_body) and
            root['page_manifest_sha256'] == sha(page_body) and
            root['sq8_object_sha256'] == plane['sq8_sha256'] == page['object_sha256'],
            'generation authority mismatch')
    records = terminal['artifacts'][f'screen/{dataset}/{folder}/plane/records.bin']
    require(records['bytes'] == RECORD_BYTES * rows and
            records['sha256'] == plane['records_sha256'], 'records identity/size mismatch')
    binding = json.loads(body('binding.json'))
    require(binding['source_order_sha256'] == plane['source_order_sha256'] and
            binding['source_precision_bits']['candidate'] == 2, 'source binding mismatch')
    if rows == 1000000:
        require(binding['raw_sha256'] == plane['source_sha256'] and
                binding['source_recipe']['rows'] == rows and
                binding['root_sha256']['candidate'] == sha(root_body), '1M binding mismatch')
    else:
        require(binding['source_only_recipe']['rows'] == rows and
                binding['candidate_root_sha256'] == sha(root_body), '100k binding mismatch')
    trace_body = body(trace_name + '.jsonl')
    queries = [json.loads(line) for line in trace_body.splitlines()]
    validate_queries(queries)
    samples = {'one_wave_page_closure': {cap: [] for cap in CAPS},
               'two_wave_unit_path': {cap: [] for cap in CAPS}}
    geometry = []
    for query in queries:
        initial, completion, pages, nominated = validate_trace(query, rows)
        first_blocks = blocks(initial, 32, rows)
        completion_blocks = blocks(completion, 32, rows)
        closure_blocks = blocks(pages, 256, rows)
        needed = byte_count(blocks(nominated, 32, rows))
        geometry.append(dict(query_ordinal=query['query_ordinal'], initial_units=len(initial),
                             completion_units=len(completion), initial_pages=len(pages),
                             source_payload_actual_needed_bytes=needed))
        for cap in CAPS:
            plans = {'one_wave_page_closure': (cover_groups([closure_blocks], cap), []),
                     'two_wave_unit_path': two_wave(first_blocks, completion_blocks, cap)}
            for name, (first, second) in plans.items():
                ranges = first + second
                source_bytes = byte_count(ranges)
                require(len(ranges) <= cap and
                        source_bytes == byte_count(coalesce(ranges)) <= records['bytes'] and
                        all(end <= records['bytes'] for _, end in ranges) and
                        all(contains(sorted(ranges), block) for block in blocks(nominated, 32, rows)),
                        'source replay lost coverage, refetched bytes, or exceeded object/cap')
                samples[name][cap].append(dict(
                    query_ordinal=query['query_ordinal'], source_gets=len(ranges),
                    first_wave_gets=len(first), second_wave_gets=len(second),
                    source_bytes=source_bytes, source_payload_actual_needed_bytes=needed,
                    source_overfetch_bytes=source_bytes - needed,
                    source_overfetch_ratio=round(source_bytes / needed, 6),
                    source_plus_sq8_buffer_bound_bytes=source_bytes + SQ8_BOUND))
    variants = {}
    for name, plans in samples.items():
        cells = []
        for cap, records_at_cap in plans.items():
            metrics = {field: summary([record[field] for record in records_at_cap]) for field in
                       ('source_gets', 'first_wave_gets', 'second_wave_gets', 'source_bytes',
                        'source_payload_actual_needed_bytes', 'source_overfetch_bytes',
                        'source_overfetch_ratio', 'source_plus_sq8_buffer_bound_bytes')}
            cells.append(dict(max_source_gets=cap, summary=metrics, samples=records_at_cap))
        variants[name] = dict(caps=cells, all_observed_rows_feasibility=[
            dict(max_source_gets=cap, max_source_mib=mib,
                 passing_queries=sum(record['source_gets'] <= cap and
                                     record['source_bytes'] <= mib * 1024 * 1024
                                     for record in plans[cap]),
                 query_count=64,
                 all_observed_rows=all(record['source_gets'] <= cap and
                                       record['source_bytes'] <= mib * 1024 * 1024
                                       for record in plans[cap]))
            for cap in (32, 64, 128) for mib in (32, 64)])
    return dict(dataset=dataset, rows=rows, dimensions=768, query_count=64,
                cohort='FIRST1M' if rows == 1000000 else 'FIRST100k',
                query_scope='consumed external development0-63; not fresh',
                authenticated_bodies=identities, records_object_identity=records,
                source_identity=plane, records_body_fetched_or_reauthenticated=False,
                geometry=geometry, variants=variants)


def make_report(repo):
    campaigns = []
    for run, trace, folder, rows, digest, datasets in CAMPAIGNS:
        directory = repo / BASE / run / 'a0001'
        terminal = terminal_at(directory, digest)
        campaigns.append(dict(
            local_directory=str(BASE / run / 'a0001'), terminal_sha256=digest,
            instance_id=terminal['instance_id'], source_base_commit=terminal['source_base_commit'],
            source_archive_sha256=terminal['source_archive_sha256'],
            datasets=[replay_dataset(directory, terminal, dataset, trace, folder, rows)
                      for dataset in datasets]))
    return dict(
        schema='borsuk-native-source-paging-replay-v1', status='REPLAYED',
        scope='Authenticated local trace geometry only; current rank_walked_source coverage.',
        live_measurements=False, latency_claim=False, recall_claim=False,
        new_quality_pass_claim=False, quality_tuned_arm_selected=False,
        caps=list(CAPS), quantiles='nearest rank, independently per metric',
        byte_ranges='half-open; 200 bytes/row; clip both 32-row and 256-row partial tails',
        fixed_cap_optimality='Contiguous cover retains the largest max_gets-1 byte gaps; '
                            'mandatory groups consume one GET each before allocating cuts.',
        one_wave_policy='Fetch full 256-row closure of ALL initial walk pages; '
                        'all recorded nomination units are asserted in that closure. '
                        'Only record units on the recorded path need be scored.',
        two_wave_policy='Fetch initial sorted union as 32-row blocks. Reuse fetched bytes, '
                        'then fetch actual completion units in unfetched holes. Each initial '
                        'cover and each conditional completion cover is byte-optimal under '
                        'its cap. Enumerate shared-cap allocations and minimize total bytes. '
                        'Allocation is recorded-path replay, not predictable before source '
                        'scores and not a globally optimal joint first/second-wave schedule.',
        nomination_preservation='Coverage feasibility conditional on identical authenticated '
                                'records and scorer, same initial sorted scoring and completion '
                                'order/cap. Source scores and selected-page equality are not '
                                'recomputed; no source records were read.',
        memory=dict(single_query_buffer_bound='source_bytes + 16773120 SQ8 bytes',
                    sq8_buffer_bound_bytes=SQ8_BOUND, host_rss_projection=False,
                    includes_other_runtime_allocations=False),
        unknown=[dict(dataset='cohere', rows=1000000, cohort='fresh', status='unknown',
                      reason='Current fresh 1M trace absent; 100k consumed trace is not a substitute.')],
        campaigns=campaigns)


def self_check():
    # Exhaust every partition of every subset of a small universe, including tails.
    for rows, block_rows in ((9, 2), (257, 32), (257, 256)):
        count = math.ceil(rows / block_rows)
        for mask in range(1, 1 << count):
            intervals = blocks([i for i in range(count) if mask & (1 << i)], block_rows, rows)
            for cap in range(1, len(intervals) + 1):
                result = cover_groups([intervals], cap)
                costs = []
                for cuts_count in range(cap):
                    for cuts in combinations(range(len(intervals) - 1), cuts_count):
                        boundaries = (-1, *cuts, len(intervals) - 1)
                        costs.append(sum(intervals[right][1] - intervals[left + 1][0]
                                         for left, right in zip(boundaries, boundaries[1:], strict=False)))
                assert byte_count(result) == min(costs)
                assert len(result) <= cap and all(contains(result, block) for block in intervals)
                assert all(0 <= start < end <= RECORD_BYTES * rows for start, end in result)
    assert blocks([8], 32, 257) == [(51200, 51400)]
    assert cover_groups([[(0, 1), (5, 6), (10, 11)]], 2) == [(0, 1), (5, 11)]
    first, second = two_wave(blocks([0, 3, 8], 32, 257), blocks([1, 2, 4, 7], 32, 257), 3)
    assert len(first + second) <= 3
    assert byte_count(first + second) == byte_count(coalesce(first + second)) <= 51400
    assert all(contains(first + second, block) for block in blocks([0, 1, 2, 3, 4, 7, 8], 32, 257))

    def rejected(action):
        try:
            action()
        except ValueError:
            return
        raise AssertionError('invalid input accepted')

    valid = dict(discoveries=[dict(seed_page=0, walk_evaluated_units=list(range(8)))],
                 nomination_evaluated_units=list(range(8)), ranked_candidate_pages=[0],
                 primary_page=0, selected_pages=[0], ranges=[[0, 199680]], planned_bytes=199680)
    validate_trace(valid, 512)
    for field, replacement in [('nomination_evaluated_units', [*range(8), 8]),
                               ('nomination_evaluated_units', [1, 0, *range(2, 8)]),
                               ('nomination_evaluated_units', list(range(7))),
                               ('planned_bytes', 1), ('ranges', [[0, 999999999]]),
                               ('ranked_candidate_pages', [1])]:
        invalid = deepcopy(valid)
        invalid[field] = replacement
        rejected(lambda query=invalid: validate_trace(query, 512))
    invalid = deepcopy(valid)
    invalid['discoveries'][0]['walk_evaluated_units'] = [0, 0]
    rejected(lambda: validate_trace(invalid, 512))
    completed = deepcopy(valid)
    completed['discoveries'][0]['walk_evaluated_units'] = list(range(9))
    completed['nomination_evaluated_units'] = list(range(16))
    validate_trace(completed, 512)
    completed['nomination_evaluated_units'][9:11] = [10, 9]
    rejected(lambda: validate_trace(completed, 512))
    queries = [dict(query_ordinal=i) for i in range(64)]
    validate_queries(queries)
    rejected(lambda: validate_queries(queries[:-1]))
    rejected(lambda: validate_queries(queries[::-1]))
    queries[0]['query_ordinal'] = False
    rejected(lambda: validate_queries(queries))
    identity = dict(bytes=4, sha256=sha(b'good'))
    assert authenticate(b'good', identity) == b'good'
    rejected(lambda: authenticate(b'evil', identity))
    rejected(lambda: authenticate(b'good!', identity))
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        body = json.dumps(dict(status='complete', phase='complete', exit_code=0)).encode()
        (path / 'aws-terminal.json').write_bytes(body)
        (path / 'aws-terminal.sha256').write_text(sha(body) + '  aws-terminal.json\n')
        assert terminal_at(path, sha(body))['exit_code'] == 0
        (path / 'aws-terminal.json').write_bytes(body + b' ')
        rejected(lambda: terminal_at(path, sha(body)))
        # A replaced sidecar cannot authorize a replaced terminal.
        (path / 'aws-terminal.sha256').write_text(sha(body + b' '))
        rejected(lambda: terminal_at(path, sha(body)))
    print('source paging self-check: PASS')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-check', action='store_true')
    parser.add_argument('--repo', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.self_check:
        self_check()
    if args.output:
        report = make_report(args.repo)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
        print(f'REPLAYED: 3 datasets, 192 consumed queries; report {args.output}')
    require(args.self_check or args.output is not None, 'choose --self-check or --output')


if __name__ == '__main__':
    main()
