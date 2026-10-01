"""Bounded stdlib regression checks; no native processes, cloud or queries."""
import copy
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch

from scripts import run_native_semantic_router_cold as runtime
from scripts import launch_native_semantic_router_cold_spot as controller


FILES = {'memory.max': '8589934592', 'memory.current': '1048576', 'memory.peak': '2097152',
         'memory.swap.max': '0', 'memory.swap.current': '0', 'memory.swap.peak': '0',
         'memory.events': 'oom 0\noom_kill 0\noom_group_kill 0', 'cpu.stat': 'usage_usec 1'}
CONFIG = dict(profile_memory_bytes=8589934592, profile_swap_bytes=0)


def check_sampler():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / 'cgroup'
        group = root / 'bounded'
        group.mkdir(parents=True)
        proc = Path(tmp) / 'proc-cgroup'
        proc.write_text('0::/bounded\n')
        for name, body in FILES.items():
            (group / name).write_text(body)

        def paths(value):
            return {'/proc/self/cgroup': proc, '/sys/fs/cgroup': root}.get(str(value), Path(value))

        with patch.object(runtime, 'Path', side_effect=paths):
            serial = runtime.cgroup_snapshot()
            assert isinstance(serial, dict), 'missing optional io.stat discarded mandatory evidence'
            assert serial['files']['memory.peak'] == '2097152' and serial['files']['cpu.stat'] == 'usage_usec 1'
            assert serial['files']['io.stat'] == 'UNMEASURED' and 'io.stat' in serial['diagnostics']
            assert 'memory.max' not in serial['files'], 'serial field roster changed'
            valid = runtime.offered_cgroup_snapshot()
            assert runtime.offered_resources(valid, valid, [], CONFIG)['passed']
            for field in FILES:
                (group / field).unlink()
                missing = runtime.offered_cgroup_snapshot()
                assert missing['files'][field] == 'UNMEASURED' and field in missing['diagnostics']
                assert missing['diagnostics'][field] == dict(type='FileNotFoundError', errno=2)
                gate = runtime.offered_resources(missing, missing, [], CONFIG)
                assert not gate['passed'] and field in gate['error'], field
                (group / field).write_text(FILES[field])
            for field, body in (('memory.max', 'max'), ('memory.peak', 'invalid'), ('memory.swap.peak', '1'),
                                ('memory.events', 'oom 1\noom_kill 0\noom_group_kill 0')):
                (group / field).write_text(body)
                bad = runtime.offered_cgroup_snapshot()
                assert not runtime.offered_resources(bad, bad, [], CONFIG)['passed'], field
                (group / field).write_text(FILES[field])
            for entry in ('0::/../outside\n', '1:memory:/bounded\n'):
                proc.write_text(entry)
                bad = runtime.offered_cgroup_snapshot()
                assert bad['diagnostics'] and not runtime.offered_resources(bad, bad, [], CONFIG)['passed']
    print('PASS sampler: optional IO preserved; mandatory fields/path/limits/swap/nonzero OOM rejected')


def check_admission():
    config = json.loads((Path(__file__).resolve().parents[1] / controller.OFFERED_CONFIG).read_bytes())
    snapshot = dict(path='/bounded', files=copy.deepcopy(FILES), diagnostics={})
    with tempfile.TemporaryDirectory() as tmp:
        cfg, proof = Path(tmp) / 'config.json', Path(tmp) / 'proof.json'
        cfg.write_text(json.dumps(config))
        proof.write_text('{}')
        for index, field in enumerate(('memory.max', 'memory.events', 'cpu.stat')):
            bad = copy.deepcopy(snapshot)
            bad['files'][field] = 'UNMEASURED'
            bad['diagnostics'][field] = dict(type='FileNotFoundError', errno=2)
            out = Path(tmp) / str(index)
            with patch.object(runtime, 'validate_config'), patch.object(runtime, 'validate_runtime'), \
                    patch.object(os, 'sched_getaffinity', return_value={4, 5}), \
                    patch.object(runtime, 'offered_cgroup_snapshot', return_value=bad), \
                    patch.object(runtime, 'prepare', side_effect=AssertionError('preparation reached')) as prepare, \
                    patch.object(runtime.old.subprocess, 'Popen', side_effect=AssertionError('native reached')) as spawn:
                assert runtime.main([str(cfg), runtime.old.sha(cfg), 'unused', str(proof), str(out)]) == 1
                assert prepare.call_count == spawn.call_count == 0, 'invalid proof reached preparation/native'
            summary = json.loads((out / 'summary.json').read_bytes())
            assert summary['campaign_cgroup_before'] == bad
            assert not summary['pre_admission_resource_gate']['passed']
            assert field in summary['terminal_error']['message']
    print('PASS pre-admission: failed resource proof retained; zero preparation/native starts')


def check_forensics():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        screen = out / 'screen'
        screen.mkdir()
        record = screen / 'rate0-relaion-control-records.jsonl'
        marker = screen / 'rate0-relaion-control-summary.json'
        raw = b'{"raw_response_base64":"/wBmYWlsZWQ=","cgroup_after":{"diagnostics":{"memory.max":{"type":"FileNotFoundError","errno":2}}}}\n'
        record.write_bytes(raw)
        marker.write_text('{"closed":true,"identity_gate_passed":false,"bounded_memory_gate_passed":false}')
        (screen / 'rate0-relaion-candidate-records.jsonl').symlink_to(record)
        (screen / 'rate0-relaion-candidate-summary.json').mkdir()
        (screen / 'unknown-records.jsonl').write_bytes(b'outside roster')
        names = controller._closed_artifacts(out)
        assert 'screen/' + record.name in names and 'screen/' + marker.name in names, 'invalid bodies lost'
        assert not any('candidate' in n for n in names if n.startswith('screen/rate'))
        assert not any('unknown' in n for n in names)
        assert record.read_bytes() == raw, 'forensic bytes changed'
        (out / 'elsewhere').mkdir()
        screen.rename(out / 'elsewhere' / 'screen')
        screen.symlink_to(out / 'elsewhere' / 'screen', target_is_directory=True)
        assert not any(n.startswith('screen/rate') for n in controller._closed_artifacts(out)), 'symlink directory followed'
    print('PASS forensic roster: invalid raw/diagnostic bodies retained; foreign names/symlinks/directories excluded')


if __name__ == '__main__':
    check_sampler()
    check_admission()
    check_forensics()
