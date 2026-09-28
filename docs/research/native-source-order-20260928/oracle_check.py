"""Development-only page upper bound for a completed native order artifact."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import numpy as np

campaign = Path(sys.argv[1]).resolve()
truth = Path(sys.argv[2]).resolve()
truth_sha = sys.argv[3]
receipt = json.loads((campaign / 'fit.json').read_text())
order_path = campaign / 'order.u64'
order_sha = hashlib.sha256(order_path.read_bytes()).hexdigest()
assert receipt['order_sha256'] == order_sha
assert receipt['recipe'] == 'borsuk-semantic-order-chacha8-f32-v1'
order = np.fromfile(order_path, dtype='<u8')
assert order.shape == (100000,) and np.array_equal(np.sort(order), np.arange(100000))
layout = campaign / 'layout.npy'
assert not layout.exists()
np.save(layout, order, allow_pickle=False)
layout_sha = hashlib.sha256(layout.read_bytes()).hexdigest()
spec = importlib.util.spec_from_file_location('frozen_page_oracle', campaign / 'v283_page_oracle.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
result = module.page_oracle(layout, truth, layout_sha=layout_sha, truth_sha=truth_sha)
result['source_only_order_receipt'] = receipt
result['scope'] = 'GT-aware optimistic upper bound on development0-63 only; not a serving route, recall or latency result'
(campaign / 'oracle.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
