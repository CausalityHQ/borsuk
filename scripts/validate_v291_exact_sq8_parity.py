"""Replay the sealed all-query relation between BLAS plans and exact Rust scores."""

import hashlib
import json
import math
import sys
from pathlib import Path


def validate(old_path, exact_path, rust_path, result_path):
    paths = (old_path, exact_path, rust_path, result_path)
    expected = (
        "82aa26a676f4b358e2de17ab48d7b5dba086cb5cb23963b8fa3fa437a240ea39",
        "432fa2b9754517ccf4637a71bd455fdf9b57b2ba755c3c14c5e5d9365508e3a7",
        "6e0e8eecd6c616dbf4fe76b5ae3653f44d51f9fba94855bc8de87b1842a84318",
        "3fed8fb0db99881e83608470cc895be1904b89f340467ad2922637a1e7ac3678",
    )
    assert tuple(hashlib.sha256(p.read_bytes()).hexdigest() for p in paths) == expected
    old = [json.loads(line) for line in old_path.read_text().splitlines()]
    exact = [json.loads(line) for line in exact_path.read_text().splitlines()]
    rust = json.loads(rust_path.read_text())
    result = json.loads(result_path.read_text())
    assert len(old) == len(exact) == 64 and rust["queries"] == 64
    assert rust["fetched_per_query_parity"] and rust["plans_sha256"] == expected[0]
    differences = {d["query_ordinal"]: d for d in rust["returned_differences"]}
    assert list(differences) == [18]
    for ordinal, (before, after) in enumerate(zip(old, exact)):
        assert before["query_ordinal"] == after["query_ordinal"] == ordinal
        assert before["pages"] == after["pages"]
        assert before["fetched_gt_hits"] == after["fetched_gt_hits"]
        if ordinal in differences:
            assert differences[ordinal]["python_hits"] == before["returned_gt_hits"]
            returned = differences[ordinal]["rust_hits"]
        else:
            returned = before["returned_gt_hits"]
        assert after["returned_gt_hits"] == returned
    hits = [p["returned_gt_hits"] for p in exact]
    mean = sum(hits)/64
    p05 = sorted(hits)[math.ceil(.05*64)-1]
    assert mean == rust["mean_returned_gt_hits"] == result["mean_returned_gt_hits"] == 98.1875
    assert p05 == rust["p05_returned_gt_hits"] == result["p05_returned_gt_hits"] == 96
    assert result["advance"]
    return {"per_query_exact_score_parity": True, "queries": 64,
            "mean_returned_gt_hits": mean, "p05_returned_gt_hits": p05}


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit("usage: validate_v291_exact_sq8_parity OLD_PLANS EXACT_PLANS RUST_RESULT EXACT_RESULT")
    print(json.dumps(validate(*(Path(path) for path in sys.argv[1:])), sort_keys=True))
