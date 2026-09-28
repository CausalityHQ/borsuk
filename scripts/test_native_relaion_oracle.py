"""Small terminal/closed-job check; no corpus or scoring required."""
import json
import tempfile
from pathlib import Path
from scripts.native_relaion_oracle import run


def check():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / 'terminal.json').write_text('{"exit_code":1,"phase":"failed"}')
        try:
            run(root)
        except AssertionError:
            pass
        else:
            raise AssertionError('failed source accepted')
        original = (root / 'oracle-terminal.json').read_bytes()
        receipt = json.loads(original)
        assert receipt['exit_code'] == 1 and receipt['phase'] == 'identity'
        try:
            run(root)
        except ValueError:
            pass
        else:
            raise AssertionError('closed job repeated')
        assert (root / 'oracle-terminal.json').read_bytes() == original
    print('oracle identity failure and closed-job guard verified')


if __name__ == '__main__':
    check()
