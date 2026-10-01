"""Bounded controller checkpoint checks; no AWS, native code or credentials."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import traceback
from types import SimpleNamespace
from unittest.mock import Mock, patch

from scripts import launch_native_semantic_router_cold_spot as controller


def check():
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        cli = base/'aws'
        calls = base/'calls.jsonl'
        cli.write_text(f'''#!{sys.executable}
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
mode = os.environ.get('CHECKPOINT_FAULT', '')
with open(os.environ['CHECKPOINT_CALLS'], 'a') as log:
    log.write(json.dumps(dict(args=args, attempts=os.environ.get('AWS_MAX_ATTEMPTS'))) + '\\n')
if args == ['--version']:
    print('aws-cli/' + ('2.0.0' if mode == 'version' else '2.36.11') + ' Python/stub')
elif '--generate-cli-skeleton' in args:
    assert '--no-sign-request' in args
    print(json.dumps({{}} if mode == 'capability' else dict(IfNoneMatch='')))
else:
    assert args[:2] == ['s3api', 'put-object']
    assert args[args.index('--if-none-match') + 1] == '*'
    assert args[args.index('--cli-connect-timeout') + 1] == '5'
    assert args[args.index('--cli-read-timeout') + 1] == '15'
    assert '--no-cli-pager' in args
    assert os.environ['AWS_MAX_ATTEMPTS'] == '1'
    assert Path(args[args.index('--body') + 1]).is_file()
    if mode in ('412', '409') or (mode == 'marker' and args[args.index('--body') + 1].endswith('-summary.json')):
        print('synthetic-sensitive-cli-text', file=sys.stderr)
        sys.exit(int(mode) if mode != 'marker' else 1)
    print('{{}}')
''')
        cli.chmod(0o755)
        config = dict(schema=controller.OFFERED_RUNTIME_SCHEMA, region='eu-central-1',
            bucket='owned-bucket', binary=dict(sha256='b'*64), qualification_sha256='c'*64)
        cfg = base/'config.json'
        cfg.write_text(json.dumps(config))
        digest = controller.peer.sha(cfg.read_bytes())
        checked = dict(config, config_sha256=digest)
        output = base/'screen'
        argv = [str(cfg), digest, 'unused-binary', 'unused-proof', str(output)]
        prefix = 'research/semantic-router/20261001/offered-a0001'
        worker_calls = []
        fault = ''
        def runtime(args, *, on_cell_closed):
            worker_calls.append(args)
            if output.exists():
                for path in output.iterdir(): path.unlink()
            paths = controller._cell_fixture(output, 'rate0-relaion-control', checked)
            marker = json.loads(paths['summary'].read_bytes())
            if fault == 'symlink':
                target = output/'record-real'
                paths['records'].rename(target)
                paths['records'].symlink_to(target)
            elif fault == 'body': paths['records'].write_bytes(b'changed')
            on_cell_closed(marker, paths)
            if fault == 'duplicate': on_cell_closed(marker, paths)
            return 0
        env = dict(PATH=str(base)+os.pathsep+os.environ['PATH'], CHECKPOINT_CALLS=str(calls))
        # The old SDK rejects IfNoneMatch before any request; SDK writes must disappear.
        old = Mock()
        old.put_object.side_effect = RuntimeError('ParamValidationError: Unknown parameter IfNoneMatch')
        with patch.dict(os.environ, env), patch.object(controller.publication, 'sdk_client', return_value=old), \
                patch.object(controller, '_worker', return_value=SimpleNamespace(main=runtime)):
            assert controller._run_offered(argv, prefix) == 0
        events = [json.loads(line) for line in calls.read_text().splitlines()]
        uploads = [e for e in events if '--body' in e['args']]
        assert len(uploads) == 2 and len(worker_calls) == 1
        for event, suffix in zip(uploads, ('records.jsonl', 'summary.json')):
            args = event['args']
            assert args[args.index('--bucket')+1] == 'owned-bucket'
            assert args[args.index('--region')+1] == 'eu-central-1'
            assert args[args.index('--key')+1] == prefix+'/cells/rate0-relaion-control-'+suffix
            assert args[args.index('--body')+1] == str(output/('rate0-relaion-control-'+suffix))
        old.put_object.assert_not_called()
        with patch.dict(os.environ, env), patch.object(controller.publication, 'sdk_client', return_value=old), \
                patch.object(controller, '_worker', return_value=SimpleNamespace(main=runtime)):
            for mode in ('version', 'capability', '412', '409', 'marker', 'symlink', 'body', 'duplicate'):
                calls.write_text('')
                worker_calls.clear()
                fault = mode
                with patch.dict(os.environ, CHECKPOINT_FAULT=mode):
                    try: controller._run_offered(argv, prefix)
                    except (RuntimeError, AssertionError):
                        assert 'synthetic-sensitive-cli-text' not in traceback.format_exc()
                    else: raise AssertionError('checkpoint failure swallowed: '+mode)
                events = [json.loads(line) for line in calls.read_text().splitlines()]
                writes = [e['args'] for e in events if '--body' in e['args']]
                assert len(worker_calls) == (0 if mode in ('version', 'capability') else 1), mode
                assert len(writes) == dict(version=0, capability=0, symlink=0, body=0,
                                          duplicate=2, marker=2, **{'412': 1, '409': 1})[mode], mode
                if mode in ('412', '409'):
                    assert writes[0][writes[0].index('--body')+1].endswith('-records.jsonl')
            # Capability failure in remote stage stops before even qualifying binaries.
            (base/'source-qualification.json').write_text(json.dumps(dict(
                campaign_schema=controller.OFFERED_SCHEMA, config_path=str(controller.OFFERED_CONFIG))))
            for mode in ('version', 'capability'):
                with patch.dict(os.environ, CHECKPOINT_FAULT=mode), patch.object(controller, '_qualify') as qualify:
                    try: controller._stage(base, base)
                    except AssertionError: pass
                    else: raise AssertionError('stage accepted missing capability')
                    qualify.assert_not_called()
            # Use real children; inject only a wait fault and prove kill+reap on the record write.
            fault = ''
            spawn = subprocess.Popen
            for failure in (subprocess.TimeoutExpired(['aws'], 45, stderr=b'synthetic-sensitive-cli-text'), KeyboardInterrupt()):
                children, commands = [], []
                def start(command, **kwargs):
                    assert kwargs['stderr'] == subprocess.DEVNULL and kwargs['env']['AWS_MAX_ATTEMPTS'] == '1'
                    child = spawn(command, **kwargs)
                    child.communicate = Mock(side_effect=failure)
                    child.kill = Mock(wraps=child.kill)
                    child.wait = Mock(wraps=child.wait)
                    children.append(child)
                    commands.append(command)
                    return child
                with patch.object(controller, '_check_checkpoint_cli'), patch.object(subprocess, 'Popen', side_effect=start):
                    try: controller._run_offered(argv, prefix)
                    except (RuntimeError, KeyboardInterrupt) as error:
                        assert isinstance(error, KeyboardInterrupt) == isinstance(failure, KeyboardInterrupt)
                        assert 'synthetic-sensitive-cli-text' not in traceback.format_exc()
                        if isinstance(failure, subprocess.TimeoutExpired): assert 'TimeoutExpired' in str(error)
                    else: raise AssertionError('wait failure swallowed')
                assert len(children) == 1 and commands[0][commands[0].index('--body')+1].endswith('-records.jsonl')
                child = children[0]
                child.communicate.assert_called_once_with(timeout=45)
                child.kill.assert_called_once()
                child.wait.assert_called()
                assert child.returncode is not None and child.poll() is not None
                try: os.waitpid(child.pid, os.WNOHANG)
                except ChildProcessError: pass
                else: raise AssertionError('CLI child not reaped')
            old.put_object.assert_not_called()
        hidden = base/'hidden-aws'
        cli.rename(hidden)
        try:
            with patch.dict(os.environ, env | dict(PATH=str(base))), patch.object(controller, '_worker') as worker:
                try: controller._run_offered(argv, prefix)
                except RuntimeError as error: assert 'FileNotFoundError' in str(error)
                else: raise AssertionError('missing CLI accepted')
                worker.assert_not_called()
        finally:
            hidden.rename(cli)
    print('semantic checkpoint CLI check PASS; cloud/native UNRUN')


if __name__ == '__main__':
    check()
