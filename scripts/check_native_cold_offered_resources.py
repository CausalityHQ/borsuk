"""Declared cgroup limits and unchanged legacy defaults, without native/AWS work."""
import json
from pathlib import Path
import tempfile
from unittest.mock import patch
from scripts import check_native_startup_build as helper


def main():
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp);group=root/'group';group.mkdir()
        proc=root/'proc';proc.write_text('0::/\n')
        for key in ('memory.peak','memory.swap.max','memory.swap.peak'):(group/key).write_text('0')
        for key in ('memory.swap.events','memory.events','cpu.stat'):(group/key).write_text('oom 0\noom_kill 0\n')
        def mapped(path):
            if str(path)=='/sys/fs/cgroup':return group
            if str(path)=='/proc/self/cgroup':return proc
            return Path(path)
        with patch.object(helper,'Path',side_effect=mapped),patch.object(helper.os,'sched_getaffinity',return_value={0,1,2,3}),patch.object(helper.resource,'getrlimit',return_value=(4*1024**3,)*2):
            for gib,explicit,name in ((7,7*1024**3,'profile-cgroup.json'),(8,None,'profile-cgroup.json'),(10,None,'build.json')):
                (group/'memory.max').write_text(str(gib*1024**3))
                out=root/name;helper.capture_cgroup(out,explicit)
                assert int(json.loads(out.read_bytes())['memory.max'])==gib*1024**3
            (group/'memory.max').write_text(str(7*1024**3))
            try:helper.capture_cgroup(root/'profile-cgroup.json',8*1024**3)
            except AssertionError:pass
            else:raise AssertionError('wrong declared limit accepted')
    print('resource capture PASS (declared7GiB, legacy8/10GiB, mismatch rejected)')


if __name__=='__main__':main()
