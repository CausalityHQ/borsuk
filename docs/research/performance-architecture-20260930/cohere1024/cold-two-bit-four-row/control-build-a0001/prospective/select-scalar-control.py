import json, pathlib, shutil
root = pathlib.Path('/mnt/borsuk-http/evidence')
items = []
for line in (root / 'scalar-control-release.log').read_text().splitlines():
    try: item = json.loads(line)
    except ValueError: continue
    if (isinstance(item, dict) and item.get('reason') == 'compiler-artifact'
        and item.get('target', {}).get('name') == 'borsuk'
        and item['target'].get('kind') == ['lib'] and item.get('profile', {}).get('test') is True
        and 'scalar-control' in item.get('features', []) and item.get('executable')):
        items.append(item)
assert len(items) == 1, 'scalar-control release libtest missing/ambiguous'
source = pathlib.Path(items[0]['executable'])
assert source.is_file() and source.parent == pathlib.Path('/mnt/borsuk-http/target-scalar-control/release/deps')
shutil.copyfile(source, '/mnt/borsuk-http/retained/scalar-control-libtest')
(root / 'scalar-control-artifact.json').write_text(json.dumps(items[0], indent=2) + '\n')
