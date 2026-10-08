import json, pathlib, shutil, hashlib
root = pathlib.Path('/mnt/borsuk-http')
evidence = root / 'evidence'
records = []
for line in (evidence / 'control-release.log').read_text().splitlines():
    if line.startswith('{'):
        item = json.loads(line)
        if item.get('reason') == 'compiler-artifact' and item.get('manifest_path') == '/mnt/borsuk-http/control-source/crates/borsuk/Cargo.toml':
            records.append(item)
library = [item for item in records if item['target']['name'] == 'borsuk' and item['target']['kind'] == ['lib']]
assert len(library) == 1 and library[0]['fresh'] is False and library[0]['profile']['test'] is False
assert library[0]['target']['src_path'] == '/mnt/borsuk-http/control-source/crates/borsuk/src/lib.rs'
assert library[0]['features'] == ['default']
selected = {}
for role, name, kind, directory in [('control-check_cohere_native_baseline', 'check_cohere_native_baseline', ['bin'], '/mnt/borsuk-http/target-control-isolated/release'), ('control-two_bit_http', 'two_bit_http', ['example'], '/mnt/borsuk-http/target-control-isolated/release/examples')]:
    matches = [item for item in records if item['target']['name'] == name and item['target']['kind'] == kind]
    assert len(matches) == 1
    item = matches[0]
    assert item['fresh'] is False and item['profile']['test'] is False
    expected_source = '/mnt/borsuk-http/control-source/crates/borsuk/' + ('src/bin/check_cohere_native_baseline.rs' if kind == ['bin'] else 'examples/two_bit_http.rs')
    assert item['target']['src_path'] == expected_source
    path = pathlib.Path(item['executable'])
    assert str(path.parent) == directory and path.name == name and path.is_file()
    selected[role] = item
# Validate the complete roster before copying either result.
assert len(selected) == 2
for role, item in selected.items():
    shutil.copyfile(item['executable'], root / 'retained' / role)
(evidence / 'control-compiler-artifacts.json').write_text(json.dumps({'library': library[0], 'serving': selected}, indent=2) + '\n')
