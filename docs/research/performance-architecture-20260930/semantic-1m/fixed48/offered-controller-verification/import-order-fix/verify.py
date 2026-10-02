import json
from pathlib import Path
import subprocess
import sys

logs = Path(__file__).parent
owned = Path('scripts/launch_cohere_fixed48_offered_http_spot.py')
source = owned.read_text()
old = source.replace('import io\n','import importlib\nimport io\n',1).replace('# Capture runtime CODE before preflight substitutes the larger cold.CODE closure.\nfrom scripts import run_cohere_fixed48_offered_http as offered_runtime\n','',1).replace('    return offered_runtime\n',"    return importlib.import_module(RUNTIME[:-3].replace('/', '.'))\n",1)
def run(label, args):
    result = subprocess.run([sys.executable,'-B',*args],capture_output=True,timeout=65)
    (logs/(label+'.stdout')).write_bytes(result.stdout)
    (logs/(label+'.stderr')).write_bytes(result.stderr)
    (logs/(label+'.status')).write_text(str(result.returncode)+'\n')
    print(label,result.returncode,result.stdout.decode().strip(),result.stderr.decode().strip(),flush=True)
    return result
try:
    owned.write_text(old)
    red = run('red-mutation',['-m','scripts.launch_cohere_fixed48_offered_http_spot','--self-check'])
    assert red.returncode == 2 and b'fresh default CLI preflight' in red.stderr and b'exact executor CODE closure' in red.stderr
finally:
    owned.write_text(source)
green = run('green-selfcheck',['-m','scripts.launch_cohere_fixed48_offered_http_spot','--self-check'])
assert green.returncode == 0 and json.loads(green.stdout)['fresh_cli_preflight'] is True
contract = run('contract',['-m','scripts.launch_cohere_fixed48_offered_http_spot','--contract'])
assert contract.returncode == 0
value = json.loads(contract.stdout)
assert len(value['CODE']) == 83 and len(value['runtime_CODE']) == 81 and value['launch_authorized'] is False
print('logs',logs,flush=True)
