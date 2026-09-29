"""One synthetic startup-worker control-flow check, no external processes/network."""
import io
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import Mock, patch

from scripts import run_native_startup_profile as worker


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        binary = root/'binary'; binary.write_bytes(b'synthetic')
        source = Path('crates/borsuk/examples/two_bit_http.rs')
        proof = root/'proof.json'; proof.write_text(json.dumps(dict(qualified=True, green_status=0,
            release_status=0, binary_sha256=worker.sha(binary), compiled_http_sha256=worker.sha(source))))
        authority = dict(root_sha256='a'*64,generation=1,control_epoch=1)
        items = [dict(dataset=name, authority=authority, index=name, metadata_files={'manifest.json':2})
                 for name in ['ReLAION','CoHere']]
        config = root/'config.json'; config.write_text(json.dumps(dict(schema='borsuk-native-startup-profile-v1',
            dataset_order=['ReLAION','CoHere']*3, code_sha256={}, items=items, bucket='synthetic',region='synthetic')))
        stats = dict(metadata=[dict(name='manifest.json',bytes=2,chunks=1,get_wall_ns=10,
            stream_wall_ns=30,write_wall_ns=20)], staging_wall_ns=50,decode_wall_ns=20)
        starts = []
        def popen(command, **kwargs):
            assert command[-1] == '127.0.0.1:8080'
            assert command[-5] == ['ReLAION','CoHere'][len(starts)%2]
            starts.append(command)
            kwargs['stdout'].write(json.dumps(dict(phase='ready',authority=authority,listen='127.0.0.1:8080',
                remote_open_stats=stats,remote_open_wall_ns=80,head_read_wall_ns=10))+'\n')
            kwargs['stdout'].flush()
            return Mock(poll=Mock(return_value=None))
        health = json.dumps(dict(authority=authority,dimensions=768)).encode()
        with patch.object(sys,'argv',['worker',str(config),worker.sha(config),str(binary),str(proof),str(root/'out')]), \
             patch.object(worker.subprocess,'Popen',side_effect=popen), \
             patch.object(worker,'stop',return_value=dict(intentional_stop=True,returncode=143)) as stop, \
             patch.object(worker.urllib.request,'urlopen',side_effect=lambda *a,**k:io.BytesIO(health)):
            worker.main()
            health = json.dumps(dict(authority=authority, dimensions=767)).encode()
            sys.argv[-1] = str(root/'bad')
            try:
                worker.main()
            except AssertionError:
                pass
            else:
                raise AssertionError('wrong health geometry accepted')
        summary = json.loads((root/'out/summary.json').read_text())
        assert len(starts) == stop.call_count == 7
        assert len(summary['records']) == 6
        assert not (root/'bad/summary.json').exists()
        assert summary['ann_queries'] == 0 and summary['first_query_measured'] is False
        assert [row['dataset'] for row in summary['records']] == ['ReLAION','CoHere']*3
        assert all(row['metadata_bytes']==2 for row in summary['records'])
    print('PASS synthetic startup worker: six interleaved starts, cleanup, accounting, no ANN queries')


if __name__ == '__main__':
    main()
