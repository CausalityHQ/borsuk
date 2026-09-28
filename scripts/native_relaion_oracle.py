"""One frozen ReLAION source/GT identity check and development page upper bound."""
import json
import sys
import time
from pathlib import Path
from scripts.v282_prepare_pair import digest, truth
from scripts.v283_page_oracle import page_oracle


def run(root):
    if (root / 'oracle-terminal.json').exists() or (root / 'truth.u32').exists():
        raise ValueError('closed or existing job; do not repeat')
    phase = 'identity'
    code = 1
    try:
        terminal = json.loads((root / 'terminal.json').read_text())
        assert terminal['exit_code'] == 0 and terminal['phase'] == 'complete'
        assert terminal['query_or_truth_used'] is False
        source_sha = 'efefaab5221333f23b21b3928914463cd89ef51c804ac1f3512c23c550918b58'
        layout_sha = '73cf76ca57f9e899c5157c7b92454400c8edc87a8671c496b5a3bbb04f63db73'
        truth_sha = '4bd3ac79fce3919f85359ce3e305491663991f6cc0890ab91682cda34247a24e'
        assert terminal['artifacts']['source/source.parquet']['sha256'] == source_sha
        assert terminal['artifacts']['layout/layout.npy']['sha256'] == layout_sha
        phase = 'truth'
        truth(root / 'source/source.parquet', source_sha, root / 'requests.jsonl',
              'b2485629b919614bf46877a779b16d678cd1690d1872b7d4f9c9cbe6ddd94eb0', root / 'truth.u32')
        assert digest(root / 'truth.u32') == truth_sha
        phase = 'oracle'
        result = page_oracle(root / 'layout/layout.npy', root / 'truth.u32',
                             layout_sha=layout_sha, truth_sha=truth_sha)
        result['split'] = 'ReLAION first100k development0–63'
        (root / 'oracle.json').write_text(json.dumps(result, sort_keys=True)+'\n')
        phase = 'complete'; code = 0
    finally:
        artifacts = {name: dict(sha256=digest(root / name), bytes=(root / name).stat().st_size)
                     for name in ['truth.u32','oracle.json'] if (root / name).is_file()}
        receipt = dict(schema='borsuk-native-relaion-oracle-terminal-v1', exit_code=code,
                       phase=phase, finished_epoch=int(time.time()), artifacts=artifacts,
                       code_sha256=digest(Path(__file__)), qualification=False)
        (root / 'oracle-terminal.json.pending').write_text(json.dumps(receipt,sort_keys=True)+'\n')
        (root / 'oracle-terminal.json.pending').rename(root / 'oracle-terminal.json')


if __name__ == '__main__':
    run(Path(sys.argv[1]))
