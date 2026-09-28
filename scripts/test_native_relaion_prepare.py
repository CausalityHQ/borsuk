"""Small terminal/lock check without data, dependencies or remote execution."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile


def check():
    script = Path('scripts/run_native_relaion_prepare.sh').resolve()
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        fake = root / 'bin'; fake.mkdir()
        uv = fake / 'uv'; uv.write_text('#!/bin/sh\nexit 42\n'); uv.chmod(0o755)
        job = root / 'job'; job.mkdir()
        env = dict(os.environ, PATH=f'{fake}:{os.environ["PATH"]}', BORSUK_SOURCE_ARCHIVE_SHA='0'*64)
        result = subprocess.run([str(script), str(job)], env=env, capture_output=True)
        assert result.returncode == 42
        terminal = (job / 'terminal.json').read_bytes()
        receipt = json.loads(terminal)
        assert receipt['exit_code'] == 42 and receipt['phase'] == 'dependencies'
        assert receipt['query_or_truth_used'] is False
        assert subprocess.run([str(script), str(job)], env=env, capture_output=True).returncode == 76
        assert (job / 'terminal.json').read_bytes() == terminal
        other = root / 'other'; other.mkdir()
        with (other / 'prepare.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert subprocess.run([str(script), str(other)], env=env, capture_output=True).returncode == 75
            assert not (other / 'terminal.json').exists()
    print('failure terminal, closed-job refusal and live lock verified')


if __name__ == '__main__':
    check()
