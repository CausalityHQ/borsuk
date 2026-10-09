"""One-shot byte assembly only; no AWS calls or launch authority."""
import hashlib
import json
import pathlib
import re
import subprocess
import sys

REPO = '/home/rb/worktrees/borsuk-prod-ready-v9'
BASE = '66cd7d50d440c62a961c0cdb70b0413830fea066'
DIR = 'docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/'

def blob(revision, path):
    return subprocess.check_output(['git', '-C', REPO, 'show', revision + ':' + path])

def sha(body):
    return hashlib.sha256(body).hexdigest()

def main():
    if len(sys.argv) != 3:
        raise ValueError('CONFIG NEW_OUTPUT; assembly only')
    cfg = json.loads(pathlib.Path(sys.argv[1]).read_bytes())
    keys = {'prefix', 'support_sha256', 'started_epoch', 'candidate_commit',
            'tail_sha256', 'coordinator_sha256', 'wrapper_sha256', 'fragment_sha256'}
    if set(cfg) != keys or type(cfg['started_epoch']) is not int or cfg['started_epoch'] <= 0:
        raise ValueError('exact assembly fields and positive explicit epoch required')
    if not re.fullmatch(r'research/semantic-router/[0-9]{8}/[a-z0-9-]+', cfg['prefix']):
        raise ValueError('prefix')
    for key in keys - {'prefix', 'started_epoch', 'candidate_commit'}:
        if not re.fullmatch(r'[0-9a-f]{64}', cfg[key]):
            raise ValueError(key)
    if not re.fullmatch(r'[0-9a-f]{40}', cfg['candidate_commit']):
        raise ValueError('candidate commit')
    pins = json.loads(blob(BASE, DIR + 'canary-prefix-segments.pending.json'))
    source = blob(pins['source_revision'], pins['source_path'])
    if sha(source) != pins['source_sha256']:
        raise ValueError('original source hash')
    lines = source.splitlines(keepends=True)
    prefix = b''.join(lines[:159])
    if len(prefix) != pins['prefix_bytes'] or sha(prefix) != pins['prefix_sha256']:
        raise ValueError('original prefix')
    for segment in pins['segments']:
        part = b''.join(lines[segment['first_line'] - 1:segment['last_line']])
        if len(part) != segment['bytes'] or sha(part) != segment['sha256']:
            raise ValueError('preserved segment')
    for hook in reversed(pins['hooks']):
        number = hook.get('line', hook.get('insert_before_line')) - 1
        expected = hook.get('original_sha256', hook.get('anchor_sha256'))
        if sha(lines[number]) != expected:
            raise ValueError('hook anchor')
        replacement = hook['replacement_pending'].replace('PENDING_ROOT_STARTED_EPOCH', str(cfg['started_epoch'])) + '\n'
        if 'line' in hook:
            lines[number] = replacement.encode()
        else:
            lines.insert(number, replacement.encode())
    bodies = {}
    for name, field in [('canary-tail.sh', 'tail_sha256'), ('canary-coordinator.sh', 'coordinator_sha256')]:
        bodies[name] = blob(cfg['candidate_commit'], DIR + 'runtime-support.pending/' + name)
        if sha(bodies[name]) != cfg[field] or len(bodies[name]) > 65536:
            raise ValueError('candidate body ' + name)
    wrapper = blob(BASE, DIR + 'runtime-support.pending/wrapper-canary.sh')
    if sha(wrapper) != cfg['wrapper_sha256']:
        raise ValueError('wrapper')
    if ('coord_sha=' + cfg['coordinator_sha256']).encode() not in bodies['canary-tail.sh']:
        raise ValueError('tail coordinator binding')
    # H5 adds one line before the original cut; all other hooks replace one line.
    result = b''.join(lines[:160]).replace(b'PENDING_ROOT_FREEZE_RUN_PREFIX', cfg['prefix'].encode()).replace(b'PENDING_ROOT_FREEZE_SUPPORT_SHA256', cfg['support_sha256'].encode())
    stub = '\nphase=canary\n'
    for name, field in [('canary-tail.sh', 'tail_sha256'), ('canary-coordinator.sh', 'coordinator_sha256'), ('wrapper-canary.sh', 'wrapper_sha256')]:
        stub += f"get {name} 64\nprintf '%s  {name}\\n' {cfg[field]} | sha256sum --strict -c -\n"
    stub += f'bash "$root/canary-tail.sh" "$root" "$bucket" "$prefix" "$instance" "$boot_epoch" "$local_stop_epoch" {cfg["fragment_sha256"]}\nexit 99\n'
    result += stub.encode()
    if b'PENDING_' in result or len(result) > 16384:
        raise ValueError('pending marker or EC2 raw user-data size')
    with pathlib.Path(sys.argv[2]).open('xb') as output:
        output.write(result)
    print(json.dumps({'assembly_only': True, 'launch_authorized': False, 'bytes': len(result), 'sha256': sha(result)}))

if __name__ == '__main__':
    main()
